//! Flush tests: memtable → `SSTable` → manifest, across reopens.

mod common;

use common::{MemDevice, TestDb, block_on, test_config};
use horton::{Config, Error};

fn open<D: horton::BlockDevice>(db: &mut TestDb<D>)
where
    D::Error: std::fmt::Debug,
{
    block_on(db.open()).unwrap();
}

fn get<D: horton::BlockDevice>(db: &TestDb<D>, key: &[u8]) -> Option<Vec<u8>>
where
    D::Error: std::fmt::Debug,
{
    let mut buf = [0u8; 2048];
    block_on(db.get(key, &mut buf))
        .unwrap()
        .map(|n| buf[..n].to_vec())
}

#[test]
fn flush_persists_across_reopen() {
    let dev = MemDevice::<4096>::new();
    let mut db = TestDb::new(dev, test_config());
    open(&mut db);
    for i in 0..20u8 {
        block_on(db.put(&[i], &[i, i])).unwrap();
    }
    block_on(db.flush()).unwrap();

    let dev = db.into_device();
    let mut db2 = TestDb::new(dev, test_config());
    let rep = block_on(db2.open()).unwrap();
    // Nothing replays: the flush advanced the WAL head past every record.
    assert_eq!(rep.recovered_records, 0);
    assert_eq!(rep.l0_tables, 1);
    assert_eq!(rep.max_seq, 20);
    for i in 0..20u8 {
        assert_eq!(get(&db2, &[i]), Some(vec![i, i]), "key {i}");
    }
    assert_eq!(get(&db2, b"missing"), None);
}

#[test]
fn flush_tombstone_hides() {
    let dev = MemDevice::<4096>::new();
    let mut db = TestDb::new(dev, test_config());
    open(&mut db);
    block_on(db.put(b"k", b"v")).unwrap();
    block_on(db.flush()).unwrap();
    block_on(db.delete(b"k")).unwrap();
    block_on(db.flush()).unwrap();

    let dev = db.into_device();
    let mut db2 = TestDb::new(dev, test_config());
    let rep = block_on(db2.open()).unwrap();
    assert_eq!(rep.l0_tables, 2);
    assert_eq!(get(&db2, b"k"), None);
}

#[test]
fn newest_sequence_wins_across_tables() {
    let dev = MemDevice::<4096>::new();
    let mut db = TestDb::new(dev, test_config());
    open(&mut db);
    block_on(db.put(b"k", b"v1")).unwrap();
    block_on(db.flush()).unwrap();
    // The memtable still shadows the older table before the second flush.
    block_on(db.put(b"k", b"v2")).unwrap();
    assert_eq!(get(&db, b"k"), Some(b"v2".to_vec()));
    block_on(db.flush()).unwrap();
    // Newest L0 table first: v2 wins after reopen too.
    let dev = db.into_device();
    let mut db2 = TestDb::new(dev, test_config());
    open(&mut db2);
    assert_eq!(get(&db2, b"k"), Some(b"v2".to_vec()));
}

#[test]
fn wal_head_advances_past_flushed_records() {
    let dev = MemDevice::<4096>::new();
    let mut db = TestDb::new(dev, test_config());
    open(&mut db);
    block_on(db.put(b"a", b"1")).unwrap();
    block_on(db.put(b"b", b"2")).unwrap();
    block_on(db.flush()).unwrap();
    block_on(db.put(b"c", b"3")).unwrap();

    // Only the post-flush record replays; the rest comes from the table.
    let dev = db.into_device();
    let mut db2 = TestDb::new(dev, test_config());
    let rep = block_on(db2.open()).unwrap();
    assert_eq!(rep.recovered_records, 1);
    assert_eq!(rep.max_seq, 3);
    assert_eq!(get(&db2, b"a"), Some(b"1".to_vec()));
    assert_eq!(get(&db2, b"b"), Some(b"2".to_vec()));
    assert_eq!(get(&db2, b"c"), Some(b"3".to_vec()));
    // The sequence counter resumes past both the table and the WAL.
    let s = block_on(db2.put(b"d", b"4")).unwrap();
    assert_eq!(s, 4);
}

#[test]
fn flush_empty_is_noop() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    block_on(db.flush()).unwrap();
    let dev = db.into_device();
    let mut db2 = TestDb::new(dev, test_config());
    let rep = block_on(db2.open()).unwrap();
    assert_eq!(rep.l0_tables, 0);
    assert_eq!(rep.recovered_records, 0);
}

#[test]
fn l0_full_errors() {
    // TestDb allows 4 L0 tables; the 5th flush must fail cleanly.
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    for i in 0..4u8 {
        block_on(db.put(&[i], b"v")).unwrap();
        block_on(db.flush()).unwrap();
    }
    block_on(db.put(b"x", b"v")).unwrap();
    let err = block_on(db.flush()).unwrap_err();
    assert!(matches!(err, Error::NoSpace));
    // The failed flush changed nothing: the key is still served from the
    // memtable and the four tables are intact.
    assert_eq!(get(&db, b"x"), Some(b"v".to_vec()));
    for i in 0..4u8 {
        assert_eq!(get(&db, &[i]), Some(b"v".to_vec()));
    }
}

#[test]
fn table_region_exhaustion() {
    // Table region [136, 140): exactly one 4-block table fits.
    let cfg = Config::new(8, 136, 136, 140, 0, 1);
    let mut db = TestDb::new(MemDevice::<4096>::new(), cfg);
    open(&mut db);
    block_on(db.put(b"a", b"1")).unwrap();
    block_on(db.flush()).unwrap();
    block_on(db.put(b"b", b"2")).unwrap();
    let err = block_on(db.flush()).unwrap_err();
    assert!(matches!(err, Error::NoSpace));
    // State is intact: b from the memtable, a from the table.
    assert_eq!(get(&db, b"a"), Some(b"1".to_vec()));
    assert_eq!(get(&db, b"b"), Some(b"2".to_vec()));
}

use std::task::{Context, Poll};

/// I/O error a test device can actually construct (`Infallible` cannot fail).
#[derive(Debug, Clone, Copy)]
struct TestIoError;

/// Block device that fails exactly one numbered write with a real error,
/// then behaves. Models a returned I/O error: the process lives on and may
/// retry, unlike the crash injector.
struct FailOnceDevice {
    blocks: Vec<[u8; 4096]>,
    fail_at: Option<usize>,
    writes: usize,
}

impl FailOnceDevice {
    const fn new(fail_at: usize) -> Self {
        Self {
            blocks: Vec::new(),
            fail_at: Some(fail_at),
            writes: 0,
        }
    }
}

impl horton::BlockDevice for FailOnceDevice {
    type Error = TestIoError;
    const BLOCK: usize = 4096;

    fn poll_read_block(
        &self,
        _cx: &mut Context<'_>,
        id: u64,
        buf: &mut [u8],
    ) -> Poll<Result<(), TestIoError>> {
        let Ok(id) = usize::try_from(id) else {
            return Poll::Ready(Err(TestIoError));
        };
        if buf.len() != 4096 {
            return Poll::Ready(Err(TestIoError));
        }
        // Never-written blocks read as zeros, like the real device.
        match self.blocks.get(id) {
            Some(blk) => buf.copy_from_slice(blk),
            None => buf.fill(0),
        }
        Poll::Ready(Ok(()))
    }

    fn poll_write_block(
        &mut self,
        _cx: &mut Context<'_>,
        id: u64,
        buf: &[u8],
    ) -> Poll<Result<(), TestIoError>> {
        let n = self.writes;
        self.writes += 1;
        if self.fail_at == Some(n) {
            self.fail_at = None;
            return Poll::Ready(Err(TestIoError));
        }
        let Ok(id) = usize::try_from(id) else {
            return Poll::Ready(Err(TestIoError));
        };
        if buf.len() != 4096 {
            return Poll::Ready(Err(TestIoError));
        }
        if id >= self.blocks.len() {
            self.blocks.resize(id + 1, [0u8; 4096]);
        }
        self.blocks[id].copy_from_slice(buf);
        Poll::Ready(Ok(()))
    }

    fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), TestIoError>> {
        Poll::Ready(Ok(()))
    }
}

#[test]
fn io_error_in_table_write_aborts_flush_cleanly() {
    // Write #2 is the table's first data block (WAL(a)=#0, WAL(b)=#1).
    let mut db = TestDb::new(FailOnceDevice::new(2), test_config());
    open(&mut db);
    block_on(db.put(b"a", b"1")).unwrap();
    block_on(db.put(b"b", b"2")).unwrap();
    let err = block_on(db.flush()).unwrap_err();
    assert!(matches!(err, Error::Device(TestIoError)));
    // Nothing was published: both keys still serve from the memtable.
    assert_eq!(get(&db, b"a"), Some(b"1".to_vec()));
    assert_eq!(get(&db, b"b"), Some(b"2".to_vec()));
    // Retry on the now-healthy device: one clean table, both keys.
    block_on(db.flush()).unwrap();
    let dev = db.into_device();
    let mut db2 = TestDb::new(dev, test_config());
    let rep = block_on(db2.open()).unwrap();
    assert_eq!(rep.l0_tables, 1);
    assert_eq!(rep.recovered_records, 0);
    assert_eq!(get(&db2, b"a"), Some(b"1".to_vec()));
    assert_eq!(get(&db2, b"b"), Some(b"2".to_vec()));
}

#[test]
fn io_error_in_manifest_write_aborts_flush_cleanly() {
    // Write #6 is the manifest commit: the table is fully written but the
    // atomic visibility point never lands.
    let mut db = TestDb::new(FailOnceDevice::new(6), test_config());
    open(&mut db);
    block_on(db.put(b"a", b"1")).unwrap();
    block_on(db.put(b"b", b"2")).unwrap();
    let err = block_on(db.flush()).unwrap_err();
    assert!(matches!(err, Error::Device(TestIoError)));
    assert_eq!(get(&db, b"a"), Some(b"1".to_vec()));
    // Retry: the staged manifest was discarded, so exactly one table
    // reference is published — no phantom duplicate, no leaked bump range.
    block_on(db.flush()).unwrap();
    let dev = db.into_device();
    let mut db2 = TestDb::new(dev, test_config());
    let rep = block_on(db2.open()).unwrap();
    assert_eq!(rep.l0_tables, 1);
    assert_eq!(rep.recovered_records, 0);
    assert_eq!(get(&db2, b"a"), Some(b"1".to_vec()));
    assert_eq!(get(&db2, b"b"), Some(b"2".to_vec()));
}
