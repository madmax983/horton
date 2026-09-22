//! Crash injector for flush: for every crash point in a put/put/flush
//! script, the recovered database must be exactly the pre-flush state
//! (old manifest + WAL replay) or the post-flush state (new manifest +
//! advanced WAL head) — never a mix, never a missing acknowledged write.

mod common;

use std::collections::BTreeMap;
use std::task::{Context, Poll};

use common::{CrashDevice, MemDevice, TestDb, block_on, test_config};
use horton::BlockDevice;

const BLOCK: usize = 4096;

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

fn get(db: &TestDb<MemDevice<BLOCK>>, key: &[u8]) -> Option<Vec<u8>> {
    let mut buf = [0u8; 2048];
    block_on(db.get(key, &mut buf))
        .unwrap()
        .map(|n| buf[..n].to_vec())
}

fn snapshot(db: &TestDb<MemDevice<BLOCK>>) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut map = BTreeMap::new();
    for key in [b"a".as_slice(), b"b".as_slice()] {
        if let Some(v) = get(db, key) {
            map.insert(key.to_vec(), v);
        }
    }
    map
}

/// Runs put(a,1) + put(b,2) + flush on a counting device: returns the total
/// number of block writes the script performs.
fn count_writes() -> usize {
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

/// Runs the script with writes `>= crash_at` dropped; returns the device.
fn run_crashed(crash_at: usize) -> MemDevice<BLOCK> {
    let dev: CrashDevice<MemDevice<BLOCK>, BLOCK> =
        CrashDevice::new(MemDevice::<BLOCK>::new(), crash_at);
    let mut db = TestDb::new(dev, test_config());
    block_on(db.open()).unwrap();
    block_on(db.put(b"a", b"1")).unwrap();
    block_on(db.put(b"b", b"2")).unwrap();
    block_on(db.flush()).unwrap();
    db.into_device().into_inner()
}

/// Exhaustive: every crash point of put/put/flush.
///
/// Write order: WAL(a)=#0, WAL(b)=#1, table blocks #2..#5, manifest=#6.
/// The manifest commit is the last write, so a crash either lands it
/// (post-flush: one L0 table, WAL head advanced, nothing replays) or drops
/// it (pre-flush: no tables, the WAL replays the landed puts).
#[test]
fn crash_during_flush_is_atomic() {
    let w = count_writes();
    // Sanity on the assumed layout: 2 WAL writes + 4 table blocks + 1 manifest.
    assert_eq!(w, 7, "write count changed; oracle below needs updating");

    for crash_at in 0..=w {
        let dev = run_crashed(crash_at);
        let mut db = TestDb::new(dev, test_config());
        let rep = block_on(db.open()).unwrap();
        let map = snapshot(&db);

        if crash_at == w {
            // Every write landed: post-flush state.
            assert_eq!(rep.l0_tables, 1, "crash_at={crash_at}");
            assert_eq!(rep.recovered_records, 0, "crash_at={crash_at}");
            assert_eq!(rep.max_seq, 2, "crash_at={crash_at}");
        } else {
            // Manifest commit dropped: pre-flush state, WAL replays.
            assert_eq!(rep.l0_tables, 0, "crash_at={crash_at}");
            let landed = crash_at.min(2);
            assert_eq!(rep.recovered_records, landed as u64, "crash_at={crash_at}");
        }

        // The key state is exactly the landed prefix in both cases: no
        // partial table is ever visible, no acknowledged write is lost.
        let mut want = BTreeMap::new();
        if crash_at >= 1 {
            want.insert(b"a".to_vec(), b"1".to_vec());
        }
        if crash_at >= 2 {
            want.insert(b"b".to_vec(), b"2".to_vec());
        }
        assert_eq!(map, want, "crash_at={crash_at}");
    }
}

/// Targeted: crash points across two flushes. After the second flush the
/// recovered state must be the full map regardless of which flush's
/// manifest commit survived.
#[test]
fn crash_across_two_flushes() {
    // Script: put(a,1), flush, put(b,2), flush.
    let run = |crash_at: usize| {
        let dev: CrashDevice<MemDevice<BLOCK>, BLOCK> =
            CrashDevice::new(MemDevice::<BLOCK>::new(), crash_at);
        let mut db = TestDb::new(dev, test_config());
        block_on(db.open()).unwrap();
        block_on(db.put(b"a", b"1")).unwrap();
        block_on(db.flush()).unwrap();
        block_on(db.put(b"b", b"2")).unwrap();
        block_on(db.flush()).unwrap();
        db.into_device().into_inner()
    };
    // Count the writes of the full script.
    let dev = CountDevice {
        inner: MemDevice::<BLOCK>::new(),
        writes: 0,
    };
    let mut db = TestDb::new(dev, test_config());
    block_on(db.open()).unwrap();
    block_on(db.put(b"a", b"1")).unwrap();
    block_on(db.flush()).unwrap();
    block_on(db.put(b"b", b"2")).unwrap();
    block_on(db.flush()).unwrap();
    let w = db.into_device().writes;
    // 1 WAL + 4 table + 1 manifest, twice.
    assert_eq!(w, 12, "write count changed; oracle below needs updating");

    // No crash: two tables, nothing replays.
    // Crash at w-1: second manifest commit dropped → first manifest wins,
    // b replays from the WAL. Crash at w-2: second table's footer dropped
    // too → same outcome. All three must expose exactly {a, b}.
    for crash_at in [w - 2, w - 1, w] {
        let dev = run(crash_at);
        let mut db = TestDb::new(dev, test_config());
        let rep = block_on(db.open()).unwrap();
        let map = snapshot(&db);
        let mut want = BTreeMap::new();
        want.insert(b"a".to_vec(), b"1".to_vec());
        want.insert(b"b".to_vec(), b"2".to_vec());
        assert_eq!(map, want, "crash_at={crash_at}");
        if crash_at == w {
            assert_eq!(rep.l0_tables, 2, "crash_at={crash_at}");
            assert_eq!(rep.recovered_records, 0, "crash_at={crash_at}");
        } else {
            assert_eq!(rep.l0_tables, 1, "crash_at={crash_at}");
            assert_eq!(rep.recovered_records, 1, "crash_at={crash_at}");
        }
    }
}
