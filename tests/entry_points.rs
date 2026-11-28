//! Entry points in compressed data blocks (ADR-0040), through the public
//! paths of a store of 16 KiB blocks (blocks under 8 KiB carry none).
//!
//! - A table of entry points the writer got wrong, under a valid CRC, is
//!   `CorruptBlock` wherever its block is read whole: a scan, forward or
//!   reverse, a compaction. A point read, which may decode only part of
//!   the block, either fails or answers as the intact block does.
//! - A store whose tables are stripped (the fill a writer before
//!   ADR-0040 left) reads the same everywhere.
//! - A bit flipped in a cache's copy of a block with entry points, its
//!   table and count word included, never changes what a scan yields,
//!   forward or reverse, short or long, on any lend or only some: a
//!   flip the scan reads is a miss and a device read. The flips reach
//!   decodes that begin at an entry point.
mod common;

use common::{MemDevice, block_on};
use horton::cache::CheckedRead;
use horton::{CachePort, CacheStats, Config, Error, MemTable, Progress, ScanBlock, crc32};
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};

horton::db_types! {
    block: 16384,
    key_max: 64,
    val_max: 512,
    memtable_entries: 64,
    memtable_arena: 16384,
    levels: 4,
    tables_per_level: 7,
    bloom_bytes: 1024,
    cache_blocks: 2;
    type Db = KvDb;
    type Scan = KvScan;
    type RevScan = KvRevScan;
    type Compaction = KvCompaction;
}

const B: usize = 16_384;
const BODY: usize = B - 4;
const KEYS: usize = 8000;
type Dev = MemDevice<B>;
type Db = KvDb<Dev>;
type Page = MemTable<64, 16384, 64, 512>;
type Manifest = horton::Manifest<4, 7, 64>;
type Entries = Vec<(Vec<u8>, Vec<u8>)>;
type Failure = Error<core::convert::Infallible>;
/// A cache's images by table and block.
type Images = RefCell<HashMap<(u32, u64), Box<[u8; B]>>>;
/// Damage done to a block's body.
type Edit = Box<dyn Fn(&mut [u8])>;

fn key(i: usize) -> Vec<u8> {
    format!("key-{i:05}").into_bytes()
}

fn drain(db: &mut Db) {
    let mut scratch = Box::new(KvCompaction::new());
    while db.compaction_pending() {
        while block_on(db.compact_step(&mut scratch)).unwrap() == Progress::More {}
    }
}

const fn config() -> Config {
    Config::whole_device(Db::MIN_DEVICE_BLOCKS).with_compression_from_level(0)
}

/// A store of [`KEYS`] keys with compressible values, flushed and
/// compacted: its data blocks are compressed, and most carry a table.
fn build() -> Db {
    let mut db = KvDb::new(Dev::new(), config());
    block_on(db.open()).unwrap();
    for i in 0..KEYS {
        let val = format!("v{i:05}:{}", "abcdefghij".repeat(4 + i % 9)).into_bytes();
        for attempt in 0..2 {
            match block_on(db.put(&key(i * 7 % KEYS), &val)) {
                Ok(_) => break,
                Err(Error::TableFull | Error::ArenaFull | Error::WalFull) if attempt == 0 => {
                    block_on(db.flush()).unwrap();
                    drain(&mut db);
                }
                Err(e) => panic!("{e:?}"),
            }
        }
    }
    block_on(db.flush()).unwrap();
    drain(&mut db);
    db
}

/// The compressed data blocks of `manifest`'s tables that carry a table
/// of entry points: `(block, points)`.
fn with_points(dev: &Dev, manifest: &Manifest) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    for t in manifest.tables() {
        for b in t.data_first()..t.data_first() + t.data_blocks().unwrap() {
            let b = usize::try_from(b).unwrap();
            let img = &dev.blocks()[b];
            let flag = u16::from_le_bytes([img[BODY - 2], img[BODY - 1]]);
            let n = usize::from(u16::from_le_bytes([img[BODY - 4], img[BODY - 3]]));
            if flag & 0x8000 != 0 && n > 0 {
                out.push((b, n));
            }
        }
    }
    out
}

/// Block `b` of `dev` with `edit` applied to its body and its CRC made
/// good again: the damage a writer bug would leave.
fn rewritten(dev: &Dev, b: usize, edit: impl FnOnce(&mut [u8])) -> Dev {
    let mut dev = dev.clone();
    let block = &mut dev.blocks_mut()[b];
    edit(&mut block[..BODY]);
    let crc = crc32(&block[..BODY]);
    block[BODY..].copy_from_slice(&crc.to_le_bytes());
    dev
}

/// What one scan does: its direction, its start, how many entries it
/// takes (`None`: all), and whether it has a block pool.
#[derive(Clone, Copy, Debug)]
struct Run<'a> {
    rev: bool,
    start: &'a [u8],
    limit: Option<usize>,
    pool: bool,
}

/// The entries of run `r` over `manifest` on `dev`, through `cache`.
fn scan(
    dev: &Dev,
    manifest: &Manifest,
    cache: Option<&dyn CachePort<B>>,
    node: u32,
    r: Run<'_>,
) -> Result<Entries, Failure> {
    let page = Box::new(Page::new());
    let (mut k, mut v) = (vec![0u8; 64], vec![0u8; 512]);
    let mut pool: Vec<ScanBlock<B>> = (0..3).map(|_| ScanBlock::new()).collect();
    let mut out = Vec::new();
    let take = r.limit.unwrap_or(usize::MAX);
    if r.rev {
        let mut s = Box::new(KvRevScan::detached(&page, manifest, dev, cache, node));
        if r.pool {
            s.use_block_pool(&mut pool);
        }
        block_on(s.seek_prev(r.start, None, u64::MAX))?;
        while out.len() < take {
            let Some((kl, vl)) = block_on(s.prev(&mut k, &mut v))? else {
                break;
            };
            out.push((k[..kl].to_vec(), v[..vl].to_vec()));
        }
    } else {
        let mut s = Box::new(KvScan::detached(&page, manifest, dev, cache, node));
        if r.pool {
            s.use_block_pool(&mut pool);
        }
        block_on(s.seek(r.start, None, u64::MAX))?;
        while out.len() < take {
            let Some((kl, vl)) = block_on(s.next(&mut k, &mut v))? else {
                break;
            };
            out.push((k[..kl].to_vec(), v[..vl].to_vec()));
        }
    }
    Ok(out)
}

/// The whole store, forward or reverse, uncached.
fn whole(dev: &Dev, manifest: &Manifest, node: u32, rev: bool) -> Result<Entries, Failure> {
    let r = Run {
        rev,
        start: b"",
        limit: None,
        pool: false,
    };
    scan(dev, manifest, None, node, r)
}

/// The level of `db`'s table that holds device block `b`.
fn level_of(db: &Db, b: usize) -> usize {
    (0..4)
        .find(|&l| {
            db.manifest().level(l).is_some_and(|ts| {
                ts.iter().any(|t| {
                    let first = usize::try_from(t.data_first()).unwrap();
                    let n = usize::try_from(t.data_blocks().unwrap()).unwrap();
                    (first..first + n).contains(&b)
                })
            })
        })
        .unwrap()
}

/// Whether a compaction of `level` meets `CorruptBlock`.
fn compaction_is_corrupt(db: &mut Db, level: usize) -> bool {
    db.request_compaction(level).unwrap();
    let mut scratch = Box::new(KvCompaction::new());
    while db.compaction_pending() {
        loop {
            match block_on(db.compact_step(&mut scratch)) {
                Ok(Progress::More) => {}
                Ok(_) => break,
                Err(Error::CorruptBlock { .. }) => return true,
                Err(e) => panic!("{e:?}"),
            }
        }
    }
    false
}

/// The edits of a table of `n` points: each field of each point, by one
/// either way, and the count, one too high (it reads a point out of the
/// stream or the fill).
fn edits(n: usize) -> Vec<Edit> {
    let from = BODY - 4 - n * 6;
    let mut out: Vec<Edit> = Vec::new();
    for field in 0..3 * n {
        for delta in [-1i32, 1] {
            out.push(Box::new(move |body: &mut [u8]| {
                let at = from + field * 2;
                let v = i32::from(u16::from_le_bytes([body[at], body[at + 1]])) + delta;
                let v = u16::try_from(v.max(0)).unwrap();
                body[at..at + 2].copy_from_slice(&v.to_le_bytes());
            }));
        }
    }
    out.push(Box::new(move |body: &mut [u8]| {
        let c = u16::try_from(n + 1).unwrap();
        body[BODY - 4..BODY - 2].copy_from_slice(&c.to_le_bytes());
    }));
    out
}

/// Every whole path finds a wrong table corrupt, and a point read over
/// one answers truly or fails. Tables of blocks across the store are
/// damaged in each field of each point, by one either way, and in their
/// count.
#[test]
fn a_wrong_table_is_corrupt_whole_and_never_a_wrong_answer() {
    let db = build();
    let manifest = *db.manifest();
    let node = db.node_id();
    let want = whole(db.device(), &manifest, node, false).unwrap();
    assert_eq!(want.len(), KEYS);
    let truth: HashMap<Vec<u8>, Vec<u8>> = want.iter().cloned().collect();
    let dev = db.into_device();
    let blocks = with_points(&dev, &manifest);
    assert!(blocks.len() > 10, "{} blocks carry a table", blocks.len());
    let (mut tried, mut failed_reads, mut true_reads) = (0, 0, 0);
    for &(b, n) in blocks.iter().step_by(blocks.len() / 3).take(3) {
        for edit in edits(n) {
            let bad = rewritten(&dev, b, edit);
            if bad.blocks()[b] == dev.blocks()[b] {
                continue;
            }
            tried += 1;
            for rev in [false, true] {
                assert!(
                    matches!(
                        whole(&bad, &manifest, node, rev),
                        Err(Error::CorruptBlock { .. })
                    ),
                    "block {b}: a scan (rev {rev}) read a wrong table whole"
                );
            }
            // A store over the damaged device: point reads, then a
            // compaction of the damaged table's level, which reads its
            // tables whole.
            let mut db = KvDb::new(bad, config());
            block_on(db.open()).unwrap();
            let mut v = vec![0u8; 512];
            for i in (0..KEYS).step_by(3) {
                let k = key(i);
                match block_on(db.get(&k, &mut v)) {
                    Ok(Some(len)) => {
                        assert_eq!(v[..len], truth[&k][..], "block {b}: a wrong answer");
                        true_reads += 1;
                    }
                    Ok(None) => panic!("block {b}: {k:?} read as absent"),
                    Err(Error::CorruptBlock { .. }) => failed_reads += 1,
                    Err(e) => panic!("{e:?}"),
                }
            }
            let level = level_of(&db, b);
            assert!(
                compaction_is_corrupt(&mut db, level),
                "block {b}: a compaction read a wrong table whole"
            );
        }
    }
    assert!(tried >= 30, "{tried} edits");
    assert!(failed_reads > 0, "no point read met the damage");
    assert!(true_reads > 0);
}

/// Old images, sealed with no table, read as they always did: blocks
/// whose tables are zeroed out give every path the same entries.
#[test]
fn blocks_without_a_table_read_the_same() {
    let db = build();
    let manifest = *db.manifest();
    let node = db.node_id();
    let want = whole(db.device(), &manifest, node, false).unwrap();
    let dev = db.into_device();
    let mut plain = dev.clone();
    let mut stripped = 0;
    for (b, n) in with_points(&dev, &manifest) {
        plain = rewritten(&plain, b, |body| body[BODY - 4 - n * 6..BODY - 2].fill(0));
        stripped += 1;
    }
    assert!(stripped > 10);
    assert_eq!(whole(&plain, &manifest, node, false).unwrap(), want);
    let mut rev = whole(&plain, &manifest, node, true).unwrap();
    rev.reverse();
    assert_eq!(rev, want);
    let mut db = KvDb::new(plain, config());
    block_on(db.open()).unwrap();
    let mut v = vec![0u8; 512];
    for (k, val) in &want {
        let len = block_on(db.get(k, &mut v)).unwrap().unwrap();
        assert_eq!(&v[..len], &val[..]);
    }
}

/// When the cache's copy of a block lends with its bit flipped.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Fault {
    /// On every lend.
    Stuck,
    /// On the block's first lend only.
    Once,
    /// On every lend of the block but its first.
    Later,
}

/// A cache that checks ranges against a pristine copy, as a real one
/// checks them against its RAM sums, and lends data blocks with byte
/// `at` flipped by `mask`.
#[derive(Default)]
struct FlipCache {
    images: Images,
    targets: RefCell<HashSet<u64>>,
    flip: Cell<Option<(usize, u8, Fault)>>,
    flipped: RefCell<HashSet<u64>>,
    rejected: Cell<u64>,
    lent_flipped: Cell<u64>,
    /// Lends that read a block's table of entry points, and those whose
    /// decode began past the block's start, at one of its points.
    tabled: Cell<u64>,
    entered: Cell<u64>,
}

impl CachePort<B> for FlipCache {
    fn get_into(&self, t: u32, b: u64, out: &mut [u8; B]) -> bool {
        self.images
            .borrow()
            .get(&(t, b))
            .map(|i| *out = **i)
            .is_some()
    }
    fn get_range_into(&self, t: u32, b: u64, off: usize, out: &mut [u8]) -> bool {
        self.images
            .borrow()
            .get(&(t, b))
            .and_then(|i| i.get(off..off + out.len()))
            .map(|s| out.copy_from_slice(s))
            .is_some()
    }
    fn with_checked(&self, t: u32, b: u64, read: &mut CheckedRead<'_>) -> Option<bool> {
        let Some(pristine) = self.images.borrow().get(&(t, b)).cloned() else {
            return Some(false);
        };
        let mut lent = pristine.clone();
        if let Some((at, mask, fault)) = self.flip.get()
            && self.targets.borrow().contains(&b)
            && match fault {
                Fault::Stuck => true,
                Fault::Once => self.flipped.borrow_mut().insert(b),
                Fault::Later => !self.flipped.borrow_mut().insert(b),
            }
        {
            lent[at] ^= mask;
            self.lent_flipped.set(self.lent_flipped.get() + 1);
        }
        let mut ok = true;
        let mut covered = vec![false; BODY];
        read(&lent[..], &mut |from, to| {
            let good = ok && (to <= from || (to <= B && lent[from..to] == pristine[from..to]));
            ok &= good;
            for c in covered.iter_mut().take(to.min(BODY)).skip(from) {
                *c = true;
            }
            good
        });
        let n = usize::from(u16::from_le_bytes([pristine[BODY - 4], pristine[BODY - 3]]));
        let flag = u16::from_le_bytes([pristine[BODY - 2], pristine[BODY - 1]]);
        if flag & 0x8000 != 0 && n > 0 && covered[BODY - 4 - 6 * n..BODY - 2].iter().all(|&c| c) {
            self.tabled.set(self.tabled.get() + 1);
            if !covered[0] {
                self.entered.set(self.entered.get() + 1);
            }
        }
        if !ok {
            self.images.borrow_mut().remove(&(t, b));
            self.rejected.set(self.rejected.get() + 1);
        }
        Some(ok)
    }
    fn checks_ranges(&self) -> bool {
        true
    }
    fn put(&self, t: u32, b: u64, block: &[u8; B], _hot: bool) {
        self.images.borrow_mut().insert((t, b), Box::new(*block));
    }
    fn invalidate_table(&self, t: u32) {
        self.images.borrow_mut().retain(|k, _| k.0 != t);
    }
    fn stats(&self) -> CacheStats {
        CacheStats::default()
    }
}

/// Flips bytes of every cached data block in turn, under `fault`, for
/// scans forward and reverse, short and long, with and without a pool:
/// each yields what it yields uncached.
fn flip_scans(fault: Fault) {
    let db = build();
    let manifest = *db.manifest();
    let node = db.node_id();
    let dev = db.into_device();
    let starts: Vec<Vec<u8>> = [0usize, 1, 2222, 4444, 7999]
        .iter()
        .map(|&i| key(i))
        .collect();
    let mut runs = Vec::new();
    for s in &starts {
        for rev in [false, true] {
            for limit in [Some(1), Some(2), Some(17)] {
                for pool in [false, true] {
                    runs.push(Run {
                        rev,
                        start: s,
                        limit,
                        pool,
                    });
                }
            }
        }
    }
    let want: Vec<Entries> = runs
        .iter()
        .map(|&r| scan(&dev, &manifest, None, node, r).unwrap())
        .collect();
    let cache = FlipCache::default();
    let points: HashSet<u64> = with_points(&dev, &manifest)
        .into_iter()
        .map(|(b, _)| u64::try_from(b).unwrap())
        .collect();
    assert!(points.len() > 10);
    *cache.targets.borrow_mut() = points;
    for &r in &runs {
        scan(&dev, &manifest, Some(&cache), node, r).unwrap();
    }
    let pristine = cache.images.borrow().clone();
    let step = if cfg!(debug_assertions) { 509 } else { 37 };
    // Every 37th byte, and every byte of the table's end of the block.
    for at in (0..B).step_by(step).chain(BODY - 120..BODY) {
        let mask = 1u8 << ((at * 3) % 8);
        for (i, &r) in runs.iter().enumerate() {
            cache.images.borrow_mut().clone_from(&pristine);
            cache.flipped.borrow_mut().clear();
            cache.flip.set(Some((at, mask, fault)));
            let got = scan(&dev, &manifest, Some(&cache), node, r);
            assert_eq!(got.as_ref().ok(), Some(&want[i]), "byte {at} run {r:?}");
        }
    }
    assert!(cache.lent_flipped.get() > 0 && cache.rejected.get() > 0);
    assert!(
        cache.tabled.get() > 0 && cache.entered.get() > 0,
        "the flips reach decodes begun at an entry point: {} {}",
        cache.tabled.get(),
        cache.entered.get()
    );
}

#[test]
fn a_stuck_bit_in_a_block_with_entry_points_never_changes_a_scan() {
    flip_scans(Fault::Stuck);
}

#[test]
fn a_bit_flipped_on_one_lend_of_a_block_with_entry_points_never_changes_a_scan() {
    flip_scans(Fault::Once);
    flip_scans(Fault::Later);
}
