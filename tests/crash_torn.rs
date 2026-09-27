//! Crash injector, torn-write fault model (v0.15/v0.16 reliability pass).
//!
//! [`CrashDevice`](common::CrashDevice) drops whole block writes; this
//! file tears them instead: the boundary write lands only its leading
//! bytes and everything after it is lost, modelling power loss mid-block.
//! The headline case is the double-buffered manifest commit — a torn slot
//! must decode as corrupt and lose to the healthy slot, never as a torn
//! half-manifest — exercised around flush, plain compaction, and
//! range-tombstone compaction, plus the WAL torn-tail rule at the flush
//! boundary.

mod common;

use std::collections::BTreeMap;
use std::task::{Context, Poll};

use common::{MemDevice, TestDb, TornDevice, block_on, test_config};
use horton::{BlockDevice, Compaction, Manifest, Progress};

const BLOCK: usize = 4096;

type TestCompaction = Compaction<4096, 256, 1024, 1024>;
type TestManifest = Manifest<7, 4, 256>;

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

/// Drives exactly one compaction job to completion (no-op when idle).
fn drive_one<D: BlockDevice>(db: &mut TestDb<D>)
where
    D::Error: std::fmt::Debug,
{
    let mut c = TestCompaction::new();
    loop {
        match block_on(db.compact_step(&mut c)) {
            Ok(Progress::More) => {}
            Ok(Progress::Done) => break,
            Err(e) => panic!("unexpected compaction error: {e:?}"),
        }
    }
}

fn get(db: &TestDb<MemDevice<BLOCK>>, key: &[u8]) -> Option<Vec<u8>> {
    let mut buf = [0u8; 2048];
    block_on(db.get(key, &mut buf))
        .unwrap()
        .map(|n| buf[..n].to_vec())
}

/// Write census of the put(a,1) + put(b,2) + flush script.
fn count_flush_writes() -> usize {
    let dev = CountDevice {
        inner: MemDevice::<BLOCK>::new(),
        writes: 0,
    };
    let mut db = TestDb::new(dev, test_config());
    block_on(db.open()).unwrap();
    block_on(db.put(b"a", b"1")).unwrap();
    block_on(db.put(b"b", b"2")).unwrap();
    block_on(db.flush()).unwrap();
    db.into_device().writes
}

/// Runs the flush script with write `torn_at` torn to `torn_len` leading
/// bytes and every write after it dropped; returns the device.
fn run_torn_flush(torn_at: usize, torn_len: usize) -> MemDevice<BLOCK> {
    let dev: TornDevice<MemDevice<BLOCK>, BLOCK> =
        TornDevice::new(MemDevice::<BLOCK>::new(), torn_at, torn_len);
    let mut db = TestDb::new(dev, test_config());
    block_on(db.open()).unwrap();
    block_on(db.put(b"a", b"1")).unwrap();
    block_on(db.put(b"b", b"2")).unwrap();
    block_on(db.flush()).unwrap();
    db.into_device().into_inner()
}

/// Builds the pre-compaction database: 4 puts + 4 flushes = full L0.
fn build() -> MemDevice<BLOCK> {
    let mut db = TestDb::new(MemDevice::<BLOCK>::new(), test_config());
    block_on(db.open()).unwrap();
    for i in 0..4u8 {
        block_on(db.put(&[b'k', b'0' + i], &[b'v', b'0' + i])).unwrap();
        block_on(db.flush()).unwrap();
    }
    assert!(db.compaction_pending());
    db.into_device()
}

/// Builds the range-tombstone pre-compaction database: 4 puts across 4
/// flushes, with a range delete covering k1..k3 in the second flush.
fn build_with_rdel() -> MemDevice<BLOCK> {
    let mut db = TestDb::new(MemDevice::<BLOCK>::new(), test_config());
    block_on(db.open()).unwrap();
    for i in 0..4u8 {
        block_on(db.put(&[b'k', b'0' + i], &[b'v', b'0' + i])).unwrap();
        if i == 1 {
            block_on(db.delete_range(b"k1", b"k3")).unwrap();
        }
        block_on(db.flush()).unwrap();
    }
    assert!(db.compaction_pending());
    db.into_device()
}

/// Counts the block writes one L0→L1 job performs on `build()`.
fn count_compaction_writes() -> usize {
    let dev = CountDevice {
        inner: build(),
        writes: 0,
    };
    let mut db = TestDb::new(dev, test_config());
    block_on(db.open()).unwrap();
    drive_one(&mut db);
    db.into_device().writes
}

/// Counts the block writes one L0→L1 job performs on `build_with_rdel()`.
fn count_rdel_compaction_writes() -> usize {
    let dev = CountDevice {
        inner: build_with_rdel(),
        writes: 0,
    };
    let mut db = TestDb::new(dev, test_config());
    block_on(db.open()).unwrap();
    drive_one(&mut db);
    db.into_device().writes
}

/// Runs build + one compaction job with the last write (the manifest
/// commit) torn to `torn_len` leading bytes.
fn run_torn_compaction(torn_len: usize) -> MemDevice<BLOCK> {
    let w = count_compaction_writes();
    let dev: TornDevice<MemDevice<BLOCK>, BLOCK> = TornDevice::new(build(), w - 1, torn_len);
    let mut db = TestDb::new(dev, test_config());
    block_on(db.open()).unwrap();
    drive_one(&mut db);
    db.into_device().into_inner()
}

/// Runs the rdel build + one compaction job with the manifest commit torn.
fn run_torn_rdel_compaction(torn_len: usize) -> MemDevice<BLOCK> {
    let w = count_rdel_compaction_writes();
    let dev: TornDevice<MemDevice<BLOCK>, BLOCK> =
        TornDevice::new(build_with_rdel(), w - 1, torn_len);
    let mut db = TestDb::new(dev, test_config());
    block_on(db.open()).unwrap();
    drive_one(&mut db);
    db.into_device().into_inner()
}

/// Whether a tear at `torn_len` still covers the manifest's full logical
/// content (`magic | payload_len | payload | crc32`), i.e. the commit
/// observably landed. The manifest encoding is prefix-complete and
/// self-validating: validity is decided by the leading bytes plus the
/// CRC, so a tear past the CRC is a landed commit, not torn metadata.
/// The encoded manifest for these tiny test databases is ~216 bytes;
/// both regimes are probed far from the boundary, so the exact size is
/// not load-bearing.
const fn commit_landed(torn_len: usize) -> bool {
    torn_len >= 2048
}

fn live_map(db: &TestDb<MemDevice<BLOCK>>) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut map = BTreeMap::new();
    let mut buf = [0u8; 2048];
    for key in [b"a".as_slice(), b"b".as_slice()] {
        if let Some(n) = block_on(db.get(key, &mut buf)).unwrap() {
            map.insert(key.to_vec(), buf[..n].to_vec());
        }
    }
    for i in 0..4u8 {
        let key = [b'k', b'0' + i];
        if let Some(n) = block_on(db.get(&key, &mut buf)).unwrap() {
            map.insert(key.to_vec(), buf[..n].to_vec());
        }
    }
    map
}

/// Torn manifest slot during flush: the commit is the script's last write
/// (#6). A tear inside the logical content reads as a corrupt slot and
/// loses to the healthy one — the torn *first* commit, so recovery treats
/// the device as fresh and replays both WAL records (pre-flush state). A
/// tear past the CRC is a landed commit (post-flush state). Either way
/// both puts survive and no torn metadata remains: the next commit
/// overwrites the torn slot in full.
#[test]
fn torn_manifest_slot_during_flush() {
    let w = count_flush_writes();
    // Sanity on the assumed layout: 2 WAL writes + 4 table blocks + 1 manifest.
    assert_eq!(w, 7, "write count changed; oracle below needs updating");

    for torn_len in [0usize, 1, 8, 2048, 4095] {
        let dev = run_torn_flush(w - 1, torn_len);
        let mut db = TestDb::new(dev, test_config());
        let rep = block_on(db.open()).unwrap();
        let landed = commit_landed(torn_len);
        if landed {
            // The tear is past the CRC: the commit observably landed.
            assert_eq!(rep.l0_tables, 1, "torn_len={torn_len}");
            assert_eq!(rep.recovered_records, 0, "torn_len={torn_len}");
        } else {
            // The slot is corrupt: pre-flush state, both WAL records replay.
            assert_eq!(rep.l0_tables, 0, "torn_len={torn_len}");
            assert_eq!(rep.recovered_records, 2, "torn_len={torn_len}");
        }
        assert_eq!(rep.max_seq, 2, "torn_len={torn_len}");
        assert_eq!(get(&db, b"a"), Some(b"1".to_vec()), "torn_len={torn_len}");
        assert_eq!(get(&db, b"b"), Some(b"2".to_vec()), "torn_len={torn_len}");

        // Keep operating: the next commit must land cleanly on the torn
        // slot and the recovered state must stay exact.
        block_on(db.put(b"c", b"3")).unwrap();
        block_on(db.flush()).unwrap();
        let dev = db.into_device();
        let mut db = TestDb::new(dev, test_config());
        let rep = block_on(db.open()).unwrap();
        // Pre-flush regime: one table holding a, b, c. Landed regime: the
        // crashed flush's table plus the new one.
        assert_eq!(
            rep.l0_tables,
            if landed { 2 } else { 1 },
            "torn_len={torn_len}"
        );
        assert_eq!(rep.recovered_records, 0, "torn_len={torn_len}");
        assert_eq!(rep.max_seq, 3, "torn_len={torn_len}");
        for (k, v) in [(b"a", b"1"), (b"b", b"2"), (b"c", b"3")] {
            assert_eq!(
                get(&db, k),
                Some(v.to_vec()),
                "torn_len={torn_len} key={}",
                String::from_utf8_lossy(k)
            );
        }
        // Both manifest slots decode now: the torn bytes are gone.
        let mut dev = db.into_device();
        let mut scratch = [0u8; BLOCK];
        let (m, fresh) = block_on(TestManifest::recover(&mut dev, &mut scratch, 0, 4)).unwrap();
        assert!(!fresh, "torn_len={torn_len}");
        assert_eq!(m.seq(), if landed { 2 } else { 1 }, "torn_len={torn_len}");
        assert_eq!(
            m.l0().len(),
            if landed { 2 } else { 1 },
            "torn_len={torn_len}"
        );
    }
}

/// Torn WAL block during flush: the torn-tail rule stops recovery at the
/// first corrupt record, so the torn write's record and everything after
/// it are lost, but every fully-landed record before it replays.
#[test]
fn torn_wal_write_during_flush_stops_at_torn_tail() {
    // Tear WAL(a) (#0) to a single byte: the record header is destroyed.
    let dev = run_torn_flush(0, 1);
    let mut db = TestDb::new(dev, test_config());
    let rep = block_on(db.open()).unwrap();
    assert_eq!(rep.recovered_records, 0, "torn WAL(a)");
    assert_eq!(rep.l0_tables, 0, "torn WAL(a)");
    assert_eq!(get(&db, b"a"), None, "torn WAL(a)");
    assert_eq!(get(&db, b"b"), None, "torn WAL(a)");

    // Tear WAL(b) (#1): WAL(a) landed whole, so it replays alone.
    let dev = run_torn_flush(1, 1);
    let mut db = TestDb::new(dev, test_config());
    let rep = block_on(db.open()).unwrap();
    assert_eq!(rep.recovered_records, 1, "torn WAL(b)");
    assert_eq!(get(&db, b"a"), Some(b"1".to_vec()), "torn WAL(b)");
    assert_eq!(get(&db, b"b"), None, "torn WAL(b)");
}

/// Torn manifest slot during compaction: the four flushes committed
/// manifest seq 4 to slot A; the compaction commit (seq 5) tears slot B.
/// A tear inside the logical content loses to the healthy slot
/// (pre-compaction: L0 intact, all four keys present); a tear past the
/// CRC is a landed commit (post-compaction). The rerun job converges
/// from the pre-compaction state, and a reopen proves the torn slot was
/// overwritten in full.
#[test]
fn torn_manifest_slot_during_compaction() {
    let w = count_compaction_writes();
    assert_eq!(w, 5, "write count changed; oracle below needs updating");

    let mut want = BTreeMap::new();
    for i in 0..4u8 {
        want.insert(vec![b'k', b'0' + i], vec![b'v', b'0' + i]);
    }

    for torn_len in [1usize, 8, 2048, 4095] {
        let dev = run_torn_compaction(torn_len);
        let mut db = TestDb::new(dev, test_config());
        let rep = block_on(db.open()).unwrap();
        let landed = commit_landed(torn_len);
        assert_eq!(
            rep.l0_tables,
            if landed { 0 } else { 4 },
            "torn_len={torn_len}"
        );
        assert_eq!(rep.recovered_records, 0, "torn_len={torn_len}");
        assert_eq!(live_map(&db), want, "torn_len={torn_len}");

        // Rerun the job: a no-op when the commit landed, convergence
        // otherwise. Either way the torn slot is overwritten in full.
        drive_one(&mut db);
        assert!(!db.compaction_pending(), "torn_len={torn_len}");
        assert_eq!(live_map(&db), want, "torn_len={torn_len}");
        let dev = db.into_device();
        let mut db = TestDb::new(dev, test_config());
        let rep = block_on(db.open()).unwrap();
        assert_eq!(rep.l0_tables, 0, "torn_len={torn_len}");
        assert_eq!(live_map(&db), want, "torn_len={torn_len}");
    }
}

/// Torn manifest slot during a range-tombstone compaction: same two
/// regimes, but the pre-compaction state carries the range tombstone, so
/// k1 stays shadowed in every recovered view. The job is bottommost, so it
/// collects the tombstone and k1's hidden version (F16): the output is the
/// data-only table, and the landed state must still hide k1.
#[test]
fn torn_manifest_slot_during_rdel_compaction() {
    let w = count_rdel_compaction_writes();
    assert_eq!(w, 5, "write count changed; oracle below needs updating");

    let mut want = BTreeMap::new();
    want.insert(vec![b'k', b'0'], vec![b'v', b'0']);
    want.insert(vec![b'k', b'2'], vec![b'v', b'2']);
    want.insert(vec![b'k', b'3'], vec![b'v', b'3']);

    for torn_len in [1usize, 8, 2048, 4095] {
        let dev = run_torn_rdel_compaction(torn_len);
        let mut db = TestDb::new(dev, test_config());
        let rep = block_on(db.open()).unwrap();
        let landed = commit_landed(torn_len);
        assert_eq!(
            rep.l0_tables,
            if landed { 0 } else { 4 },
            "torn_len={torn_len}"
        );
        assert_eq!(rep.recovered_records, 0, "torn_len={torn_len}");
        assert_eq!(live_map(&db), want, "torn_len={torn_len}");

        drive_one(&mut db);
        assert!(!db.compaction_pending(), "torn_len={torn_len}");
        assert_eq!(live_map(&db), want, "torn_len={torn_len}");
    }
}
