//! v0.3: the open-time sweep reclaims orphaned table blocks, and flush
//! allocates from the free list first.
//!
//! Geometry: WAL `[8, 16)`, tables `[16, 26)` — 10 blocks. A hand-committed
//! manifest references a table at `[20, 25)`, leaving `[16, 20)` as
//! unreferenced orphans below the sweep's bump resume point. Bump-only
//! allocation would start at 25 and fail a 4-block table (`[25, 29)` past
//! `tbl_end`); the free list must serve it instead.
//!
//! Two tests: the first plants the orphans by hand; the second crashes a
//! real flush between the table-block writes and the manifest commit, so
//! the orphans are genuinely produced by the torn flush.

mod common;

use common::{block_on, CrashDevice, MemDevice, TestDb};
use horton::{BlockDevice, Config, Error, KeyBound, Manifest, TableRef};

use core::task::{Context, Poll};

type DevError = core::convert::Infallible;

/// Tiny geometry: manifest slots 0/1, WAL `[8, 16)`, tables `[16, 26)`.
const fn tiny_config() -> Config {
    Config::new(8, 16, 16, 26, 0, 1)
}

/// A live table at `[20, 25)` covering `m..=z`; blocks `[16, 20)` are
/// unreferenced orphans. Its blocks were never written — no read path in
/// this test may consult it (the test key sorts below `m`, so key-range
/// pruning skips it).
fn live_ref() -> TableRef<256> {
    TableRef {
        id: 0,
        first_block: 20,
        block_count: 5,
        first_key: KeyBound::from_slice(b"m").expect("bound"),
        last_key: KeyBound::from_slice(b"z").expect("bound"),
        max_seq: 0,
        entry_count: 0,
    }
}

fn get(db: &TestDb<MemDevice<4096>>, key: &[u8]) -> Option<Vec<u8>> {
    let mut buf = [0u8; 1024];
    block_on(db.get(key, &mut buf))
        .expect("get")
        .map(|n| buf[..n].to_vec())
}

/// A fresh device whose manifest references only the live `[20, 25)` table.
fn base_device() -> MemDevice<4096> {
    let mut dev = MemDevice::<4096>::new();
    let mut manifest = Manifest::<7, 4, 256>::new();
    manifest.set_wal_head(8);
    manifest
        .add_table_to_level::<DevError>(0, live_ref())
        .expect("place table");
    let mut scratch = [0u8; 4096];
    block_on(manifest.commit(&mut dev, &mut scratch, 0, 1)).expect("commit");
    dev
}

/// Records the block id of every write, in order.
struct IdLogDevice<D> {
    inner: D,
    ids: Vec<u64>,
}

impl<D: BlockDevice> BlockDevice for IdLogDevice<D> {
    type Error = D::Error;
    const BLOCK: usize = D::BLOCK;

    fn poll_read_block(
        &self,
        cx: &mut Context<'_>,
        id: u64,
        buf: &mut [u8],
    ) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_read_block(cx, id, buf)
    }

    fn poll_write_block(
        &mut self,
        cx: &mut Context<'_>,
        id: u64,
        buf: &[u8],
    ) -> Poll<Result<(), Self::Error>> {
        self.ids.push(id);
        self.inner.poll_write_block(cx, id, buf)
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_flush(cx)
    }
}

const PUTS: [(&[u8], &[u8]); 4] = [
    (b"a".as_slice(), b"1".as_slice()),
    (b"b".as_slice(), b"2".as_slice()),
    (b"c".as_slice(), b"3".as_slice()),
    (b"d".as_slice(), b"4".as_slice()),
];

/// The write index of the flush's manifest commit: the last write overall,
/// and it must target a manifest slot (0 or 1) — not a table or WAL block.
fn commit_write_index() -> usize {
    let dev = IdLogDevice {
        inner: base_device(),
        ids: Vec::new(),
    };
    let mut db = TestDb::new(dev, tiny_config());
    block_on(db.open()).expect("open");
    for (k, v) in PUTS {
        block_on(db.put(k, v)).expect("put");
    }
    block_on(db.flush()).expect("flush");
    let dev = db.into_device();
    let idx = dev
        .ids
        .iter()
        .rposition(|&id| id == 0 || id == 1)
        .expect("a manifest commit write");
    assert_eq!(
        idx,
        dev.ids.len() - 1,
        "the manifest commit is the flush's final write"
    );
    idx
}

#[test]
fn crashed_flush_orphans_are_reclaimed_and_reused() {
    // Crash the flush after its table blocks land but before the manifest
    // commit: the 4 reserved blocks become genuine orphans of a torn flush.
    let crash_at = commit_write_index();
    let dev = CrashDevice::<MemDevice<4096>, 4096>::new(base_device(), crash_at);
    let mut db = TestDb::new(dev, tiny_config());
    block_on(db.open()).expect("open");
    for (k, v) in PUTS {
        block_on(db.put(k, v)).expect("put");
    }
    // Returns `Ok` — the device reported success; the "crash" only dropped
    // the commit write.
    block_on(db.flush()).expect("flush");
    let dev = db.into_device().into_inner();

    // Reopen: the sweep reclaims the orphaned run, the WAL replay restores
    // the puts (durable before the crash), and retrying the flush must reuse
    // the reclaimed `[16, 20)` run — the bump at 25 cannot fit 4 blocks.
    let mut db = TestDb::new(dev, tiny_config());
    block_on(db.open()).expect("open");
    assert_eq!(get(&db, b"a"), Some(b"1".to_vec()));
    block_on(db.flush()).expect("retry flush reuses the reclaimed run");
    assert_eq!(get(&db, b"d"), Some(b"4".to_vec()));

    // Allocator exhausted again afterwards, and the failed flush is harmless.
    block_on(db.put(b"e", b"5")).expect("put");
    let err = block_on(db.flush()).expect_err("flush must fail");
    assert!(matches!(err, Error::NoSpace));
    assert_eq!(get(&db, b"e"), Some(b"5".to_vec()));

    // Reopening rebuilds the same allocator state.
    let dev = db.into_device();
    let mut db = TestDb::new(dev, tiny_config());
    block_on(db.open()).expect("open");
    assert_eq!(get(&db, b"d"), Some(b"4".to_vec()));
}

#[test]
fn sweep_reclaims_orphans_and_flush_reuses_them() {
    let mut db = TestDb::new(base_device(), tiny_config());
    block_on(db.open()).expect("open");

    // A 4-block table at the bump (25) would need `[25, 29)` — past
    // `tbl_end`. The reclaimed `[16, 20)` run serves it instead.
    block_on(db.put(b"a", b"1")).expect("put");
    block_on(db.flush()).expect("flush");
    assert_eq!(get(&db, b"a"), Some(b"1".to_vec()));

    // The free blocks are spent now: the next flush genuinely has nowhere
    // to go, and the failed flush changes nothing — "b" is still served
    // from the memtable.
    block_on(db.put(b"b", b"2")).expect("put");
    let err = block_on(db.flush()).expect_err("flush must fail");
    assert!(matches!(err, Error::NoSpace));
    assert_eq!(get(&db, b"b"), Some(b"2".to_vec()));

    // Reopening rebuilds the same allocator state: the bump still resumes
    // past the referenced table and the claimed blocks stay claimed.
    let dev = db.into_device();
    let mut db = TestDb::new(dev, tiny_config());
    block_on(db.open()).expect("open");
    assert_eq!(get(&db, b"a"), Some(b"1".to_vec()));
    block_on(db.put(b"c", b"3")).expect("put");
    let err = block_on(db.flush()).expect_err("flush must still fail");
    assert!(matches!(err, Error::NoSpace));
}
