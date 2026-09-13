//! v0.3: the WAL wraps instead of exhausting.
//!
//! Repeated put/flush cycles over the 128-block test WAL region must keep
//! working past the region's physical block count — the flush that fills
//! the WAL wraps it atomically — and reopening must skip the stale
//! pre-wrap blocks via the sequence floor.

mod common;

use common::{block_on, test_config, MemDevice, TestDb};

fn key(n: u64) -> Vec<u8> {
    format!("k{n:04}").into_bytes()
}

fn val(n: u64) -> Vec<u8> {
    format!("v{n}").into_bytes()
}

fn put(db: &mut TestDb<MemDevice<4096>>, n: u64) {
    block_on(db.put(&key(n), &val(n))).expect("put");
}

fn get(db: &TestDb<MemDevice<4096>>, n: u64) -> Option<Vec<u8>> {
    let mut buf = [0u8; 1024];
    block_on(db.get(&key(n), &mut buf))
        .expect("get")
        .map(|m| buf[..m].to_vec())
}

#[test]
fn wal_wraps_across_many_flush_cycles() {
    let mut db = TestDb::new(MemDevice::new(), test_config());
    block_on(db.open()).expect("open");

    // The memtable holds 64 entries and the WAL region is 128 blocks, so
    // two full memtables exactly fill the WAL: the second flush wraps it,
    // the third proves the wrapped region keeps working.
    for cycle in 0..3u64 {
        for i in 0..64u64 {
            put(&mut db, cycle * 64 + i);
        }
        block_on(db.flush()).expect("flush");
    }

    // All 192 keys readable straight through the wrap.
    for n in 0..192u64 {
        assert_eq!(get(&db, n), Some(val(n)), "n={n}");
    }

    // Reopen: the stale pre-wrap WAL blocks are skipped by the sequence
    // floor — nothing resurrects, the sequence counter resumes at 192.
    let dev = db.into_device();
    let mut db = TestDb::new(dev, test_config());
    let rep = block_on(db.open()).expect("open");
    assert_eq!(rep.recovered_records, 0);
    assert_eq!(rep.max_seq, 192);
    for n in (0..192u64).step_by(17) {
        assert_eq!(get(&db, n), Some(val(n)), "n={n}");
    }

    // The WAL is exactly full after the reopen scan, so the first flush
    // (empty memtable) wraps it; writes then resume without `NoSpace`.
    block_on(db.flush()).expect("wrap flush");
    for n in 192..200u64 {
        put(&mut db, n);
    }
    block_on(db.flush()).expect("flush");
    for n in 192..200u64 {
        assert_eq!(get(&db, n), Some(val(n)), "n={n}");
    }
    // And the pre-wrap data is still intact beside the new writes.
    for n in (0..192u64).step_by(23) {
        assert_eq!(get(&db, n), Some(val(n)), "n={n}");
    }
}
