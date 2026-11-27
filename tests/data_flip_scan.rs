//! A scan reads a cached data block range by range, and keeps in its own
//! buffer only the grains it checked as it copied them (ADR-0035). A
//! detached scan, forward and reverse, short and long, with and without a
//! block pool, through a cache whose RAM copy of its data blocks has a
//! bit flipped yields exactly what an uncached scan yields, and never
//! `CorruptBlock`: a flip the scan reads fails its check and the block
//! is read from the device again.
//!
//! The flip is held for every lend (a stuck bit) or only for a block's
//! first lend (a bit that a refill, or the fault going away, clears).
//! The second is the one a scan that copied a whole image and checked
//! the rest later would get wrong: the later check passes over a clean
//! image while its copy keeps the flipped byte. A flip on every lend but
//! the first hits what a later lend adds to a buffer the first one left
//! part-filled: the rest of a raw block's grains, or the rest of a
//! compressed block's inflate (ADR-0039).
mod common;

use common::{Lcg, MemDevice, TestDb, block_on, test_config};
use horton::cache::CheckedRead;
use horton::{
    BlockDevice, CachePort, CacheStats, Compaction, Config, Error, MemTable, Progress, RevScan,
    Scan, ScanBlock,
};
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};

type TestCompaction = Compaction<4096, 256, 1024, 1024>;
type ScanOver<'d, D> = Scan<'d, D, 4096, 256, 1024, 64, 4096, 7, 4, 1024, 8>;
type RevScanOver<'d, D> = RevScan<'d, D, 4096, 256, 1024, 64, 4096, 7, 4, 1024, 8>;
type Page = MemTable<64, 4096, 256, 1024>;
type Entries = Vec<(Vec<u8>, Vec<u8>)>;
type Manifest = horton::Manifest<7, 4, 256>;
type State = horton::ScanState<4096, 256, 1024, 7, 4>;
type RevState = horton::RevScanState<4096, 256, 1024, 7, 4>;

thread_local! {
    /// The states every scan of a test is built in, when it keeps one.
    static KEPT: RefCell<Option<(Box<State>, Box<RevState>)>> = const { RefCell::new(None) };
    /// A twin of the store a test scans (see [`build_with`]): when set,
    /// every run over kept states first scans the twin through them,
    /// so the states hold blocks with the same ids and other bytes.
    static TWIN: RefCell<Option<TestDb<MemDevice<4096>>>> = const { RefCell::new(None) };
}

fn key(i: usize) -> Vec<u8> {
    format!("key-{i:05}").into_bytes()
}

fn drain<D: BlockDevice>(db: &mut TestDb<D>)
where
    D::Error: core::fmt::Debug,
{
    let mut scratch = Box::new(TestCompaction::new());
    while db.compaction_pending() {
        while block_on(db.compact_step(&mut scratch)).unwrap() == Progress::More {}
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
    images: RefCell<HashMap<(u32, u64), [u8; 4096]>>,
    /// The blocks a flip applies to.
    targets: RefCell<HashSet<u64>>,
    /// (byte, mask, fault).
    flip: Cell<Option<(usize, u8, Fault)>>,
    /// Blocks lent flipped since the flip was set.
    flipped: RefCell<HashSet<u64>>,
    rejected: Cell<u64>,
    lent_flipped: Cell<u64>,
    lookups: Cell<u64>,
    misses: Cell<u64>,
    /// Lends of a compressed block that checked only part of its stream:
    /// a seek's inflate that stopped short of the block's end.
    partial: Cell<u64>,
    /// Checks answer `true` and the lend's result says whether they
    /// held: a cache that verifies its sums once the read is done, as
    /// the `CachePort::with_checked` contract allows (`Some(false)`:
    /// whatever `read` made of the image is discarded).
    defer: Cell<bool>,
}

impl CachePort<4096> for FlipCache {
    fn get_into(&self, t: u32, b: u64, out: &mut [u8; 4096]) -> bool {
        self.images
            .borrow()
            .get(&(t, b))
            .map(|i| *out = *i)
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
        self.lookups.set(self.lookups.get() + 1);
        let Some(pristine) = self.images.borrow().get(&(t, b)).copied() else {
            self.misses.set(self.misses.get() + 1);
            return Some(false);
        };
        let mut lent = pristine;
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
        // The bytes checked, below the flag.
        let mut covered = vec![false; 4092];
        read(&lent, &mut |from, to| {
            let good = ok && (to <= from || (to <= 4096 && lent[from..to] == pristine[from..to]));
            ok &= good;
            for c in covered.iter_mut().take(to.min(4092)).skip(from) {
                *c = true;
            }
            good || self.defer.get()
        });
        let flag = u16::from_le_bytes([pristine[4090], pristine[4091]]);
        let clen = usize::from(flag & 0x7FFF);
        if flag & 0x8000 != 0 && covered[4090] && covered[..clen].contains(&false) {
            self.partial.set(self.partial.get() + 1);
        }
        if !ok {
            self.images.borrow_mut().remove(&(t, b));
            self.rejected.set(self.rejected.get() + 1);
            self.misses.set(self.misses.get() + 1);
        }
        Some(ok)
    }
    fn checks_ranges(&self) -> bool {
        true
    }
    fn put(&self, t: u32, b: u64, block: &[u8; 4096], _hot: bool) {
        self.images.borrow_mut().insert((t, b), *block);
    }
    fn invalidate_table(&self, t: u32) {
        self.images.borrow_mut().retain(|k, _| k.0 != t);
    }
    fn stats(&self) -> CacheStats {
        CacheStats::default()
    }
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

/// The source a scan reads.
struct Src<'a, D> {
    page: &'a Page,
    manifest: &'a Manifest,
    device: &'a D,
    node: u32,
}

/// One run `r` over `twin`, uncached, in the kept states: it leaves in
/// them the twin's blocks, under the ids the run that follows reads.
fn twin_scan(twin: &TestDb<MemDevice<4096>>, (fwd, rev): (&mut State, &mut RevState), r: Run<'_>) {
    let page = Box::new(Page::new());
    let manifest = *twin.manifest();
    let mut pool: Vec<ScanBlock<4096>> = (0..4).map(|_| ScanBlock::new()).collect();
    let (mut k, mut v) = (vec![0u8; 256], vec![0u8; 1024]);
    let take = r.limit.unwrap_or(usize::MAX);
    let device = twin.device();
    if r.rev {
        let mut scan =
            RevScanOver::detached_in(rev, &page, &manifest, device, None, twin.node_id());
        if r.pool {
            scan.use_block_pool(&mut pool);
        }
        block_on(scan.seek_prev(r.start, None, u64::MAX)).unwrap();
        for _ in 0..take {
            if block_on(scan.prev(&mut k, &mut v)).unwrap().is_none() {
                break;
            }
        }
    } else {
        let mut scan = ScanOver::detached_in(fwd, &page, &manifest, device, None, twin.node_id());
        if r.pool {
            scan.use_block_pool(&mut pool);
        }
        block_on(scan.seek(r.start, None, u64::MAX)).unwrap();
        for _ in 0..take {
            if block_on(scan.next(&mut k, &mut v)).unwrap().is_none() {
                break;
            }
        }
    }
}

fn run<D: BlockDevice>(
    src: &Src<'_, D>,
    cache: Option<&dyn CachePort<4096>>,
    r: Run<'_>,
) -> Result<Entries, Error<D::Error>> {
    // A kept pair of states, when the test set one: every scan of the
    // run is built in it (ADR-0037).
    let mut kept = KEPT.with(|k| k.borrow_mut().take());
    if let Some((fwd, rev)) = kept.as_mut() {
        TWIN.with(|t| {
            if let Some(twin) = t.borrow().as_ref() {
                twin_scan(twin, (fwd, rev), r);
            }
        });
    }
    let result = (|| -> Result<Entries, Error<D::Error>> {
        let mut pool: Vec<ScanBlock<4096>> = (0..4).map(|_| ScanBlock::new()).collect();
        let (mut k, mut v) = (vec![0u8; 256], vec![0u8; 1024]);
        let mut out = Vec::new();
        let take = r.limit.unwrap_or(usize::MAX);
        if r.rev {
            let mut scan = Box::new(match kept.as_mut() {
                Some((_, rev)) => RevScanOver::<D>::detached_in(
                    rev,
                    src.page,
                    src.manifest,
                    src.device,
                    cache,
                    src.node,
                ),
                None => {
                    RevScanOver::<D>::detached(src.page, src.manifest, src.device, cache, src.node)
                }
            });
            if r.pool {
                scan.use_block_pool(&mut pool);
            }
            block_on(scan.seek_prev(r.start, None, u64::MAX))?;
            while out.len() < take {
                let Some((kl, vl)) = block_on(scan.prev(&mut k, &mut v))? else {
                    break;
                };
                out.push((k[..kl].to_vec(), v[..vl].to_vec()));
            }
        } else {
            let mut scan = Box::new(match kept.as_mut() {
                Some((fwd, _)) => ScanOver::<D>::detached_in(
                    fwd,
                    src.page,
                    src.manifest,
                    src.device,
                    cache,
                    src.node,
                ),
                None => {
                    ScanOver::<D>::detached(src.page, src.manifest, src.device, cache, src.node)
                }
            });
            if r.pool {
                scan.use_block_pool(&mut pool);
            }
            block_on(scan.seek(r.start, None, u64::MAX))?;
            while out.len() < take {
                let Some((kl, vl)) = block_on(scan.next(&mut k, &mut v))? else {
                    break;
                };
                out.push((k[..kl].to_vec(), v[..vl].to_vec()));
            }
        }
        Ok(out)
    })();
    KEPT.with(|k| *k.borrow_mut() = kept);
    result
}

/// A database of several tables over 3000 keys, the last of them a
/// flushed table that overlaps the others, so a merge turns between
/// tables and a scan's one buffer is taken back and forth.
fn build(config: Config) -> TestDb<MemDevice<4096>> {
    build_with(config, false)
}

/// [`build`], with every value upper-cased when `upper`: the same keys,
/// lengths and writes, so the same tables in the same blocks, holding
/// other bytes.
fn build_with(config: Config, upper: bool) -> TestDb<MemDevice<4096>> {
    let mut db = TestDb::new(MemDevice::<4096>::new(), config);
    block_on(db.open()).unwrap();
    let mut rng = Lcg::new(0xDA7A_F11F);
    let put = |db: &mut TestDb<MemDevice<4096>>, rng: &mut Lcg| {
        let k = key(rng.next_bounded(3000));
        let mut fill = b"abcdefghijklmnopqrstuvwxyz"[rng.next_bounded(26)];
        if upper {
            fill = fill.to_ascii_uppercase();
        }
        let val = vec![fill; 20 + rng.next_bounded(100)];
        for attempt in 0..2 {
            match block_on(db.put(&k, &val)) {
                Ok(_) => break,
                Err(Error::TableFull | Error::ArenaFull | Error::WalFull) if attempt == 0 => {
                    block_on(db.flush()).unwrap();
                    drain(db);
                }
                Err(e) => panic!("{e:?}"),
            }
        }
    };
    for _ in 0..5000 {
        put(&mut db, &mut rng);
    }
    block_on(db.flush()).unwrap();
    drain(&mut db);
    for _ in 0..40 {
        put(&mut db, &mut rng);
    }
    block_on(db.flush()).unwrap();
    db
}

/// Every data block of every table.
fn data_blocks(m: &Manifest) -> HashSet<u64> {
    m.tables()
        .iter()
        .flat_map(|t| {
            (t.data_first()..t.data_first() + t.data_blocks().unwrap()).collect::<Vec<_>>()
        })
        .collect()
}

fn flip_every_byte(config: Config, fault: Fault) -> u64 {
    flip_every_byte_with(config, fault, false)
}

/// Flips each byte of every cached data block in turn, for every run;
/// returns how many lends of a compressed block checked only part of
/// its stream.
fn flip_every_byte_with(config: Config, fault: Fault, defer: bool) -> u64 {
    let db = build(config);
    let manifest = *db.manifest();
    let device = db.device().clone();
    let page = Box::new(Page::new());
    let src = Src {
        page: &page,
        manifest: &manifest,
        device: &device,
        node: db.node_id(),
    };
    assert!(
        manifest.tables().len() >= 2,
        "the merge turns between tables"
    );
    let starts: Vec<Vec<u8>> = [0usize, 1, 444, 1234, 1999, 2998]
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
    for rev in [false, true] {
        runs.push(Run {
            rev,
            start: b"",
            limit: None,
            pool: false,
        });
    }
    let want: Vec<Entries> = runs.iter().map(|&r| run(&src, None, r).unwrap()).collect();
    // A reverse scan from below the first key yields nothing; the rest do.
    assert!(want.iter().filter(|w| w.is_empty()).count() <= 8);
    let cache = FlipCache::default();
    cache.defer.set(defer);
    *cache.targets.borrow_mut() = data_blocks(&manifest);
    for &r in &runs {
        run(&src, Some(&cache), r).unwrap();
    }
    let pristine = cache.images.borrow().clone();
    let step = if cfg!(debug_assertions) { 97 } else { 7 };
    for at in (0..4096).step_by(step) {
        let mask = 1u8 << ((at * 3) % 8);
        for (i, &r) in runs.iter().enumerate() {
            cache.images.borrow_mut().clone_from(&pristine);
            cache.flipped.borrow_mut().clear();
            cache.flip.set(Some((at, mask, fault)));
            let got = run(&src, Some(&cache), r);
            assert_eq!(got.as_ref().ok(), Some(&want[i]), "byte {at} run {r:?}");
        }
    }
    assert!(cache.lent_flipped.get() > 0);
    assert!(cache.rejected.get() > 0);
    cache.partial.get()
}

#[test]
fn a_stuck_bit_in_a_raw_data_block_never_changes_a_scan() {
    flip_every_byte(test_config(), Fault::Stuck);
}

#[test]
fn a_bit_flipped_on_the_first_lend_only_never_changes_a_scan() {
    flip_every_byte(test_config(), Fault::Once);
}

/// A lend that returns `Some(false)` is discarded whole, even when
/// every check it was asked answered `true`. Found by mutation (r3,
/// mutant 9): `Blocks::complete` kept the grains of a lend that ended
/// `Some(false)` once they made the buffer whole.
#[test]
fn a_lend_that_ends_false_is_discarded_even_when_its_checks_held() {
    flip_every_byte_with(test_config(), Fault::Stuck, true);
}

/// Every scan of the run built in one kept pair of states, each after a
/// scan of a twin store through them: so each starts in buffers that
/// hold blocks of the same ids with other bytes, and grains an earlier
/// scan's lends checked. None of them is trusted, and no flip the
/// earlier scans saw or missed changes an answer (ADR-0035, ADR-0037).
#[test]
fn a_kept_scan_state_trusts_no_grain_an_earlier_scan_checked() {
    KEPT.with(|k| *k.borrow_mut() = Some((Box::new(State::new()), Box::new(RevState::new()))));
    TWIN.with(|t| *t.borrow_mut() = Some(build_with(test_config(), true)));
    flip_every_byte(test_config(), Fault::Once);
    flip_every_byte(test_config(), Fault::Stuck);
    TWIN.with(|t| t.borrow_mut().take());
    assert!(
        KEPT.with(|k| k.borrow_mut().take()).is_some(),
        "the states were kept"
    );
}

#[test]
fn a_bit_flipped_in_a_compressed_data_block_never_changes_a_scan() {
    let partial = flip_every_byte(test_config().with_compression_from_level(0), Fault::Stuck);
    assert!(partial > 0, "a seek inflated part of a block");
}

/// The seek's lend of a compressed block checks and inflates only the
/// start of its stream; a flip there, on that lend only, is a miss, and a
/// flip anywhere on a later lend (the one that goes on with the inflate)
/// is a miss too: what the first lend left is never mixed with an
/// unchecked byte (ADR-0039).
#[test]
fn a_bit_flipped_in_a_compressed_block_on_one_lend_never_changes_a_scan() {
    let config = test_config().with_compression_from_level(0);
    assert!(flip_every_byte(config, Fault::Once) > 0);
    assert!(flip_every_byte(config, Fault::Later) > 0);
    assert!(flip_every_byte_with(config, Fault::Stuck, true) > 0);
}

/// [`a_kept_scan_state_trusts_no_grain_an_earlier_scan_checked`] over
/// compressed blocks: a kept state's buffer may hold a twin block's
/// partial inflate under the same id, and none of it is gone on with.
#[test]
fn a_kept_scan_state_goes_on_with_no_inflate_an_earlier_scan_left() {
    let config = test_config().with_compression_from_level(0);
    KEPT.with(|k| *k.borrow_mut() = Some((Box::new(State::new()), Box::new(RevState::new()))));
    TWIN.with(|t| *t.borrow_mut() = Some(build_with(config, true)));
    assert!(flip_every_byte(config, Fault::Later) > 0);
    TWIN.with(|t| t.borrow_mut().take());
    assert!(KEPT.with(|k| k.borrow_mut().take()).is_some());
}

/// A limit-1 seek through an intact cache asks it once for the data
/// block it lands in, and reads no device block: the seek took the few
/// grains it reads, and the advance past the head finds the next entry
/// among them.
#[test]
fn a_short_seek_takes_one_lend_of_its_data_block() {
    short_seek_lends(test_config());
}

/// [`a_short_seek_takes_one_lend_of_its_data_block`] over compressed
/// blocks: the seek inflates the start of its block, past the entry after
/// the head, in its one lend (ADR-0039).
#[test]
fn a_short_seek_takes_one_lend_of_its_compressed_data_block() {
    short_seek_lends(test_config().with_compression_from_level(0));
}

fn short_seek_lends(config: Config) {
    let db = build(config);
    let manifest = *db.manifest();
    let device = db.device().clone();
    let page = Box::new(Page::new());
    let src = Src {
        page: &page,
        manifest: &manifest,
        device: &device,
        node: db.node_id(),
    };
    let cache = FlipCache::default();
    let whole = Run {
        rev: false,
        start: b"",
        limit: None,
        pool: false,
    };
    run(&src, Some(&cache), whole).unwrap();
    // The deepest level alone, so the seek reads one table.
    let deepest = (1..7)
        .rev()
        .find(|&l| manifest.level(l).is_some_and(|t| !t.is_empty()))
        .unwrap();
    let mut solo = Manifest::new();
    for t in manifest.level(deepest).unwrap() {
        solo.add_table_to_level::<()>(deepest, *t).unwrap();
    }
    let solo_src = Src {
        manifest: &solo,
        ..src
    };
    let data = data_blocks(&solo);
    let mut lookups = 0;
    for i in [3usize, 700, 1500, 2600] {
        let k = key(i);
        let before = (cache.lookups.get(), cache.misses.get());
        let r = Run {
            rev: false,
            start: &k,
            limit: Some(1),
            pool: false,
        };
        let got = run(&solo_src, Some(&cache), r).unwrap();
        assert_eq!(got, run(&solo_src, None, r).unwrap());
        // The index block, then one data block (and the next, when the
        // head is its block's last entry).
        let asked = cache.lookups.get() - before.0;
        assert!(asked == 2 || asked == 3, "seek {i}: {asked} lookups");
        lookups += asked;
        assert_eq!(cache.misses.get(), before.1, "seek {i}");
    }
    assert!(data.len() > 4);
    assert!(lookups < 4 * 3, "{lookups}");
}

/// A store of one table whose data blocks hold four 900-byte values and
/// several hundred bytes of zero padding.
fn padded() -> TestDb<MemDevice<4096>> {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    block_on(db.open()).unwrap();
    for i in 0..40usize {
        let val = vec![b"abcdefghijklmnopqrstuvwxyz"[i % 26]; 900];
        for attempt in 0..2 {
            match block_on(db.put(&key(i), &val)) {
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

/// A seek whose head is its block's last entry takes the padding after
/// it along with the head, so the step past the head finds the block's
/// end in the buffer: two data-block lookups (its block and the next),
/// not three. The table's last key costs one. Found in review: the
/// padding was not taken, so the step past the head asked the cache for
/// the rest of the block only to find zeros.
#[test]
fn a_seek_onto_a_blocks_last_entry_takes_its_padding() {
    let db = padded();
    let manifest = *db.manifest();
    let device = db.device().clone();
    let page = Box::new(Page::new());
    let (level, table) = (0..7)
        .flat_map(|l| {
            manifest
                .level(l)
                .unwrap_or(&[])
                .iter()
                .map(move |t| (l, *t))
        })
        .max_by_key(|(_, t)| t.data_blocks())
        .unwrap();
    assert!(table.data_blocks().unwrap() > 3);
    let mut solo = Manifest::new();
    solo.add_table_to_level::<()>(level, table).unwrap();
    let src = Src {
        page: &page,
        manifest: &solo,
        device: &device,
        node: db.node_id(),
    };
    let cache = FlipCache::default();
    let all = Run {
        rev: false,
        start: b"",
        limit: None,
        pool: false,
    };
    // Warm, and learn which keys end a block.
    let want = run(&src, Some(&cache), all).unwrap();
    for (n, (k, _)) in want.iter().enumerate() {
        let before = (cache.lookups.get(), cache.misses.get());
        let r = Run {
            rev: false,
            start: k,
            limit: Some(1),
            pool: false,
        };
        assert_eq!(run(&src, Some(&cache), r).unwrap(), want[n..=n]);
        assert_eq!(cache.misses.get(), before.1, "key {n}");
        // The index block, then the data blocks.
        let data = cache.lookups.get() - before.0 - 1;
        let last = n + 1 == want.len();
        assert!(
            data <= if last { 1 } else { 2 },
            "key {n}: {data} data-block lookups"
        );
    }
}

/// A seek takes the few grains of its data block it reads; the first
/// step past them takes the rest of that block in one lend, after which
/// every step over the block is parsed in the buffer. A scan through a
/// table from a seek therefore asks the cache for each data block at
/// most twice (the seek's block) or once (every later block), plus
/// index blocks. Found by mutation (r3, mutant 3): a step over a buffer
/// that held only part of its block lent the block again, partially,
/// on every step that went past what the buffer held.
#[test]
fn a_scan_takes_the_rest_of_its_seek_block_in_one_lend() {
    rest_in_one_lend(test_config());
}

/// [`a_scan_takes_the_rest_of_its_seek_block_in_one_lend`] over
/// compressed blocks: the step past a seek's partial inflate goes on
/// with it in one lend (ADR-0039).
#[test]
fn a_scan_inflates_the_rest_of_its_seek_block_in_one_lend() {
    rest_in_one_lend(test_config().with_compression_from_level(0));
}

fn rest_in_one_lend(config: Config) {
    let db = build(config);
    let manifest = *db.manifest();
    let device = db.device().clone();
    let page = Box::new(Page::new());
    let deepest = (1..7)
        .rev()
        .find(|&l| manifest.level(l).is_some_and(|t| !t.is_empty()))
        .unwrap();
    let mut solo = Manifest::new();
    for t in manifest.level(deepest).unwrap() {
        solo.add_table_to_level::<()>(deepest, *t).unwrap();
    }
    let src = Src {
        page: &page,
        manifest: &solo,
        device: &device,
        node: db.node_id(),
    };
    let cache = FlipCache::default();
    let all = Run {
        rev: false,
        start: b"",
        limit: None,
        pool: false,
    };
    let want = run(&src, Some(&cache), all).unwrap();
    for pool in [false, true] {
        for i in [3usize, 700, 1500] {
            let k = key(i);
            let r = Run {
                rev: false,
                start: &k,
                limit: Some(60),
                pool,
            };
            let before = (cache.lookups.get(), cache.misses.get());
            let got = run(&src, Some(&cache), r).unwrap();
            let from = want.iter().position(|(w, _)| *w >= k).unwrap();
            assert_eq!(got, want[from..from + 60]);
            assert_eq!(cache.misses.get(), before.1, "seek {i}");
            let asked = cache.lookups.get() - before.0;
            // 60 entries of 20-120 bytes span at most three 4 KiB
            // blocks: their lends, one more for the seek block's rest,
            // and the index blocks.
            assert!(asked <= 8, "pool {pool} seek {i}: {asked} lookups");
        }
    }
}
