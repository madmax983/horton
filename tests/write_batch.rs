//! `WriteBatch`: atomic multi-op writes.
//!
//! RED suite for v0.11: every op in a batch becomes durable and visible
//! together, or none does — including across crashes at every block-write
//! position.

mod common;

use std::collections::BTreeMap;
use std::task::{Context, Poll};

use common::{CrashDevice, MemDevice, TestDb, block_on, test_config};
use horton::{BlockDevice, Error, WriteBatch};

const BLOCK: usize = 4096;
const KEY_MAX: usize = 256;
const VAL_MAX: usize = 1024;

type Batch<const OPS: usize> = WriteBatch<KEY_MAX, VAL_MAX, OPS>;

fn get(db: &TestDb<MemDevice<BLOCK>>, key: &[u8]) -> Option<Vec<u8>> {
    let mut buf = [0u8; 2048];
    block_on(db.get(key, &mut buf))
        .unwrap()
        .map(|n| buf[..n].to_vec())
}

fn open_db() -> TestDb<MemDevice<BLOCK>> {
    let mut db = TestDb::new(MemDevice::<BLOCK>::new(), test_config());
    block_on(db.open()).unwrap();
    db
}

#[test]
fn write_batch_happy_path() {
    let mut db = open_db();
    let mut b = Batch::<8>::new();
    assert!(b.is_empty());
    b.put(b"k1", b"v1").unwrap();
    b.put(b"k2", b"v2").unwrap();
    b.delete(b"k3").unwrap();
    assert_eq!(b.len(), 3);

    let base = block_on(db.write(&b)).unwrap();
    assert_eq!(base, 1, "first batch takes seqs 1..=3");

    assert_eq!(get(&db, b"k1"), Some(b"v1".to_vec()));
    assert_eq!(get(&db, b"k2"), Some(b"v2".to_vec()));
    assert_eq!(get(&db, b"k3"), None, "delete lands as a tombstone");

    // Seqs are consecutive: the next op takes base + len.
    let s = block_on(db.put(b"k4", b"v4")).unwrap();
    assert_eq!(s, base + 3);
}

#[test]
fn write_batch_duplicate_keys_last_wins() {
    let mut db = open_db();
    let mut b = Batch::<8>::new();
    b.put(b"k", b"old").unwrap();
    b.put(b"k", b"new").unwrap();
    let base = block_on(db.write(&b)).unwrap();
    assert_eq!(get(&db, b"k"), Some(b"new".to_vec()));
    // Both ops consumed seqs, in order.
    let s = block_on(db.put(b"z", b"z")).unwrap();
    assert_eq!(s, base + 2);
}

#[test]
fn write_batch_empty_is_seq_conserving_noop() {
    let mut db = open_db();
    let b = Batch::<8>::new();
    let r = block_on(db.write(&b)).unwrap();
    assert_eq!(r, 0, "empty batch returns next_seq without consuming");
    let s = block_on(db.put(b"k", b"v")).unwrap();
    assert_eq!(s, 1, "no seq was consumed by the empty batch");
}

#[test]
fn write_batch_rejects_over_block_with_no_trace() {
    let mut db = open_db();
    // One max-size record is 23 + 256 + 1024 = 1303 bytes; four exceed one
    // 4096-byte WAL block, so the batch cannot commit atomically.
    let mut b = Batch::<4>::new();
    let big_k = [b'x'; KEY_MAX];
    let big_v = [b'y'; VAL_MAX];
    for _ in 0..4 {
        b.put(&big_k, &big_v).unwrap();
    }
    match block_on(db.write(&b)) {
        Err(Error::BatchTooLarge { bytes, max }) => {
            assert_eq!(bytes, 4 * (23 + KEY_MAX + VAL_MAX));
            assert_eq!(max, BLOCK);
        }
        other => panic!("expected BatchTooLarge, got {other:?}"),
    }
    // No trace: no WAL records, no staged bytes, no consumed seqs.
    let s = block_on(db.put(b"k", b"v")).unwrap();
    assert_eq!(s, 1);
    assert_eq!(get(&db, b"k"), Some(b"v".to_vec()));
}

#[test]
fn write_batch_rejected_when_memtable_full() {
    // TestDb CAP = 64 slots. Fill 63, then a 2-op batch must fail whole.
    let mut db = open_db();
    for i in 0..63u8 {
        block_on(db.put(&[i], &[i])).unwrap();
    }
    let mut b = Batch::<4>::new();
    b.put(b"x", b"1").unwrap();
    b.put(b"y", b"2").unwrap();
    match block_on(db.write(&b)) {
        Err(Error::TableFull) => {}
        other => panic!("expected TableFull, got {other:?}"),
    }
    // Nothing applied: batch keys absent, one slot still free for a put.
    assert_eq!(get(&db, b"x"), None);
    assert_eq!(get(&db, b"y"), None);
    block_on(db.put(b"z", b"3")).unwrap();
    assert_eq!(get(&db, b"z"), Some(b"3".to_vec()));
}

#[test]
fn write_batch_validates_at_build_time() {
    let mut b = Batch::<2>::new();
    assert_eq!(b.put(b"", b"v"), Err(Error::EmptyKey));
    assert_eq!(
        b.put(&[b'k'; KEY_MAX + 1], b"v"),
        Err(Error::KeyTooLarge {
            len: KEY_MAX + 1,
            max: KEY_MAX
        })
    );
    assert_eq!(
        b.put(b"k", &[b'v'; VAL_MAX + 1]),
        Err(Error::ValueTooLarge {
            len: VAL_MAX + 1,
            max: VAL_MAX
        })
    );
    b.put(b"a", b"1").unwrap();
    b.put(b"b", b"2").unwrap();
    assert_eq!(b.put(b"c", b"3"), Err(Error::BatchFull));
    assert_eq!(b.len(), 2);
    b.clear();
    assert!(b.is_empty());
    b.put(b"c", b"3").unwrap();
    assert_eq!(b.len(), 1);
}

#[test]
fn write_batch_then_flush_and_recover() {
    let mut db = open_db();
    let mut b = Batch::<8>::new();
    b.put(b"k1", b"v1").unwrap();
    b.put(b"k2", b"v2").unwrap();
    b.delete(b"k3").unwrap();
    block_on(db.write(&b)).unwrap();
    block_on(db.flush()).unwrap();
    let dev = db.into_device();

    let mut db2 = TestDb::new(dev, test_config());
    block_on(db2.open()).unwrap();
    assert_eq!(get(&db2, b"k1"), Some(b"v1".to_vec()));
    assert_eq!(get(&db2, b"k2"), Some(b"v2".to_vec()));
    assert_eq!(get(&db2, b"k3"), None);
}

/// Counts block writes; everything else passes through.
struct CountDevice<D> {
    inner: D,
    writes: usize,
}

impl<D: BlockDevice> BlockDevice for CountDevice<D> {
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
        self.writes += 1;
        self.inner.poll_write_block(cx, id, buf)
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_flush(cx)
    }
}

fn batch_script_writes() -> usize {
    let dev = CountDevice {
        inner: MemDevice::<BLOCK>::new(),
        writes: 0,
    };
    let mut db = TestDb::new(dev, test_config());
    block_on(db.open()).unwrap();
    block_on(db.put(b"a", b"1")).unwrap();
    let mut b = Batch::<8>::new();
    b.put(b"b", b"2").unwrap();
    b.put(b"c", b"3").unwrap();
    b.delete(b"d").unwrap();
    block_on(db.write(&b)).unwrap();
    db.into_device().writes
}

/// Runs open + put(a,1) + write(batch b/c/d) with writes `>= crash_at`
/// dropped; returns the surviving device.
fn run_crashed(crash_at: usize) -> MemDevice<BLOCK> {
    let dev: CrashDevice<MemDevice<BLOCK>, BLOCK> =
        CrashDevice::new(MemDevice::<BLOCK>::new(), crash_at);
    let mut db = TestDb::new(dev, test_config());
    block_on(db.open()).unwrap();
    block_on(db.put(b"a", b"1")).unwrap();
    let mut b = Batch::<8>::new();
    b.put(b"b", b"2").unwrap();
    b.put(b"c", b"3").unwrap();
    b.delete(b"d").unwrap();
    block_on(db.write(&b)).unwrap();
    db.into_device().into_inner()
}

/// Exhaustive: every crash point of put + batch-write.
///
/// Write order: WAL(a)=#0, WAL(batch)=#1 — the batch commits as a single
/// block write, so recovery sees all of it or none of it, never a prefix.
#[test]
fn write_batch_crash_is_atomic() {
    let w = batch_script_writes();
    assert_eq!(w, 2, "write count changed; oracle below needs updating");

    for crash_at in 0..=w {
        let dev = run_crashed(crash_at);
        let mut db = TestDb::new(dev, test_config());
        let rep = block_on(db.open()).unwrap();

        let mut want = BTreeMap::new();
        if crash_at >= 1 {
            want.insert(b"a".to_vec(), b"1".to_vec());
        }
        let want_seq = if crash_at >= 2 {
            // All or nothing: the batch lands whole.
            want.insert(b"b".to_vec(), b"2".to_vec());
            want.insert(b"c".to_vec(), b"3".to_vec());
            4u64
        } else {
            u64::from(crash_at >= 1)
        };
        assert_eq!(rep.max_seq, want_seq, "crash_at={crash_at}");

        let mut map = BTreeMap::new();
        for key in [b"a".as_slice(), b"b".as_slice(), b"c".as_slice()] {
            if let Some(v) = get(&db, key) {
                map.insert(key.to_vec(), v);
            }
        }
        assert_eq!(
            map, want,
            "crash_at={crash_at}: batch must be all-or-nothing"
        );

        // No seq reuse: the next write continues past every landed seq.
        let s = block_on(db.put(b"z", b"9")).unwrap();
        assert_eq!(s, want_seq + 1, "crash_at={crash_at}");
    }
}
