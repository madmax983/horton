//! A device read that fails mid-scan leaves every cursor where it was:
//! the call returns `Error::Device`, and calling it again yields exactly
//! what an unfailed scan yields from there, with no entry lost,
//! repeated or given another entry's value.
//!
//! A cursor is moved to the block it parks in only once the park has
//! found its entry there. Before that fix a reverse cursor that stepped
//! back to an earlier block was moved first, so a failed read of that
//! block left the cursor's head in one block and its offset pointing
//! into another. A retried `prev` then copied another entry's value
//! (review of ADR-0035). A forward cursor crossing two blocks in one
//! park had the same flaw.
mod common;

use common::{Lcg, MemDevice, TestDb, block_on, test_config};
use horton::cache::BlockCache;
use horton::{
    BlockDevice, CachePort, Compaction, Error, MemTable, Progress, RevScan, Scan, ScanBlock,
};
use std::cell::{Cell, RefCell};
use std::task::{Context, Poll};

type TestCompaction = Compaction<4096, 256, 1024, 1024>;
type Page = MemTable<64, 4096, 256, 1024>;
type Manifest = horton::Manifest<7, 4, 256>;
type Entries = Vec<(Vec<u8>, Vec<u8>)>;

#[derive(Debug)]
struct Fail;

/// A device whose `n`th read (counted from when the count is reset)
/// fails once.
struct FailDevice {
    inner: MemDevice<4096>,
    reads: Cell<u64>,
    fail_at: Cell<Option<u64>>,
}

impl BlockDevice for FailDevice {
    type Error = Fail;
    const BLOCK: usize = 4096;
    fn poll_read_block(
        &self,
        cx: &mut Context<'_>,
        id: u64,
        buf: &mut [u8],
    ) -> Poll<Result<(), Fail>> {
        let n = self.reads.get();
        self.reads.set(n + 1);
        if self.fail_at.get() == Some(n) {
            self.fail_at.set(None);
            return Poll::Ready(Err(Fail));
        }
        self.inner
            .poll_read_block(cx, id, buf)
            .map(|r| r.map_err(|_| Fail))
    }
    fn poll_write_block(
        &mut self,
        _cx: &mut Context<'_>,
        _id: u64,
        _buf: &[u8],
    ) -> Poll<Result<(), Fail>> {
        Poll::Ready(Err(Fail))
    }
    fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Fail>> {
        Poll::Ready(Ok(()))
    }
}

fn key(i: usize) -> Vec<u8> {
    format!("key-{i:05}").into_bytes()
}

fn drain(db: &mut TestDb<MemDevice<4096>>) {
    let mut scratch = Box::new(TestCompaction::new());
    while db.compaction_pending() {
        while block_on(db.compact_step(&mut scratch)).unwrap() == Progress::More {}
    }
}

/// Writes `keys` in order, each with a value of its own, and compacts.
fn build(keys: impl Iterator<Item = usize>) -> TestDb<MemDevice<4096>> {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    block_on(db.open()).unwrap();
    for (n, i) in keys.enumerate() {
        let val = format!("value-{i:05}-{n:05}-").repeat(3).into_bytes();
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

/// The deepest level alone: one table per key range, several blocks each.
fn deepest(m: &Manifest) -> Manifest {
    let level = (1..7)
        .rev()
        .find(|&l| m.level(l).is_some_and(|t| !t.is_empty()))
        .unwrap();
    let mut solo = Manifest::new();
    for t in m.level(level).unwrap() {
        solo.add_table_to_level::<()>(level, *t).unwrap();
    }
    solo
}

/// One whole scan, retrying each call that fails with a device error
/// (at most three); `None` when the seek itself failed.
fn scan(
    page: &Page,
    manifest: &Manifest,
    dev: &FailDevice,
    cache: Option<&dyn CachePort<4096>>,
    node: u32,
    (rev, pooled, max_seq): (bool, bool, u64),
) -> Option<Entries> {
    let (mut k, mut v) = (vec![0u8; 256], vec![0u8; 1024]);
    let mut out = Vec::new();
    let mut pool: Vec<ScanBlock<4096>> = (0..3).map(|_| ScanBlock::new()).collect();
    let mut errs = 0;
    // `true` while the scan goes on.
    let mut step = |r: Result<Option<(usize, usize)>, Error<Fail>>,
                    k: &[u8],
                    v: &[u8],
                    out: &mut Entries| match r {
        Ok(Some((kl, vl))) => {
            out.push((k[..kl].to_vec(), v[..vl].to_vec()));
            true
        }
        Ok(None) => false,
        Err(Error::Device(Fail)) => {
            errs += 1;
            assert!(errs <= 3, "one failed read fails one call");
            true
        }
        Err(e) => panic!("{e:?}"),
    };
    if rev {
        let mut s = Box::new(
            RevScan::<_, 4096, 256, 1024, 64, 4096, 7, 4, 1024, 8>::detached(
                page, manifest, dev, cache, node,
            ),
        );
        if pooled {
            s.use_block_pool(&mut pool);
        }
        block_on(s.seek_prev(b"", None, max_seq)).ok()?;
        while {
            let r = block_on(s.prev(&mut k, &mut v));
            step(r, &k, &v, &mut out)
        } {}
    } else {
        let mut s = Box::new(
            Scan::<_, 4096, 256, 1024, 64, 4096, 7, 4, 1024, 8>::detached(
                page, manifest, dev, cache, node,
            ),
        );
        if pooled {
            s.use_block_pool(&mut pool);
        }
        block_on(s.seek(b"", None, max_seq)).ok()?;
        while {
            let r = block_on(s.next(&mut k, &mut v));
            step(r, &k, &v, &mut out)
        } {}
    }
    Some(out)
}

/// Fails each device read of a clean scan in turn, and checks that the
/// retried scan yields exactly the clean scan's entries.
fn every_read_fails_once(db: &TestDb<MemDevice<4096>>, max_seq: u64) {
    let solo = deepest(db.manifest());
    let page = Box::new(Page::new());
    let dev = FailDevice {
        inner: db.device().clone(),
        reads: Cell::new(0),
        fail_at: Cell::new(None),
    };
    for rev in [false, true] {
        for (cached, pooled) in [(false, false), (true, false), (false, true), (true, true)] {
            let cache = RefCell::new(BlockCache::<4096, 3>::new());
            let cache: Option<&dyn CachePort<4096>> = if cached { Some(&cache) } else { None };
            let run = (rev, pooled, max_seq);
            dev.reads.set(0);
            let want = scan(&page, &solo, &dev, cache, db.node_id(), run).unwrap();
            assert!(!want.is_empty());
            let reads = dev.reads.get();
            assert!(reads > 4, "the scan reads blocks from the device");
            for n in 0..reads {
                dev.reads.set(0);
                dev.fail_at.set(Some(n));
                let Some(got) = scan(&page, &solo, &dev, cache, db.node_id(), run) else {
                    continue;
                };
                assert_eq!(
                    got, want,
                    "read {n} failed: rev={rev} cached={cached} pooled={pooled}"
                );
            }
        }
    }
}

/// Random keys: every entry visible, so a park crosses at most one
/// block, but each reverse step back is a read.
#[test]
fn a_failed_read_mid_scan_changes_nothing_a_retry_yields() {
    let mut rng = Lcg::new(0xDA7A_F11F);
    let db = build((0..3000).map(|_| rng.next_bounded(3000)));
    every_read_fails_once(&db, u64::MAX);
}

/// Keys written in order, read at a snapshot that sees the first half:
/// blocks whose every entry is invisible lie past the visible ones, so a
/// park crosses several blocks before it finds the end.
#[test]
fn a_failed_read_while_crossing_invisible_blocks_changes_nothing() {
    let db = build(0..2400);
    every_read_fails_once(&db, 1200);
}
