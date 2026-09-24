//! RED tests for the confirmed defects in `docs/ARCHITECTURE_REVIEW.md`.
//!
//! Each test asserts the *correct* behavior and currently fails, so each
//! one is `#[ignore]`d with the finding it pins. `cargo test` stays green;
//! `cargo test --test review_findings -- --ignored` shows the failures.
//! When a fix lands, drop its `#[ignore]` in the same commit: the test
//! becomes the regression guard.

use horton::{Compaction, Config, Error, Progress, RevScan, Scan};

mod common;
use common::{MemDevice, TestDb, block_on, test_config};

/// Caller scratch for `compact_step`, matching the test database shape.
type TestCompaction = Compaction<4096, 256, 1024, 1024>;

/// Drives compaction until no level is full.
fn drain_compaction(db: &mut TestDb<MemDevice<4096>>, c: &mut TestCompaction) {
    while db.compaction_pending() {
        while block_on(db.compact_step(c)).unwrap() == Progress::More {}
    }
}

/// Flushes, compacting first whenever level 0 is full.
fn flush_retrying(db: &mut TestDb<MemDevice<4096>>, c: &mut TestCompaction) {
    loop {
        match block_on(db.flush()) {
            Ok(()) => return,
            Err(Error::NoSpace) if db.compaction_pending() => drain_compaction(db, c),
            Err(e) => panic!("flush: {e:?}"),
        }
    }
}

/// Puts, flushing whenever the memtable or the WAL region is full.
fn put_retrying(db: &mut TestDb<MemDevice<4096>>, c: &mut TestCompaction, k: &[u8], v: &[u8]) {
    loop {
        match block_on(db.put(k, v)) {
            Ok(_) => return,
            Err(Error::TableFull | Error::ArenaFull | Error::NoSpace) => flush_retrying(db, c),
            Err(e) => panic!("put: {e:?}"),
        }
    }
}

/// F1 — compaction's output-run reservation assumes a merge never needs
/// more data blocks than its inputs used. Greedy block packing breaks that:
/// four inputs that each pack into one tight block (3 × 1041 B + 965 B =
/// 4088 B) interleave into five output blocks. `TableWriter` never checks
/// the reservation, and the bump advances by the reservation only, so the
/// next allocation lands on the output table's footer and a live table is
/// overwritten.
#[test]
#[ignore = "F1: compaction output can outgrow its reserved run and overwrite a live table"]
fn f1_compaction_output_never_outgrows_its_reservation() {
    let mut db: TestDb<MemDevice<4096>> = TestDb::new(MemDevice::new(), test_config());
    block_on(db.open()).unwrap();
    let mut c = Box::new(TestCompaction::new());
    let big = [0x5A_u8; 1024]; // 13 + 4 + 1024 = 1041-byte entry
    let mid = [0xA5_u8; 948]; // 13 + 4 + 948 = 965-byte entry
    let big_key = |i: u8, j: u8| [b'a', b'0' + i, b'0' + j, b'_'];
    let mid_key = |i: u8| [b'm', b'0' + i, b'_', b'_'];
    for i in 0..4u8 {
        for j in 0..3u8 {
            block_on(db.put(&big_key(i, j), &big)).unwrap();
        }
        block_on(db.put(&mid_key(i), &mid)).unwrap();
        block_on(db.flush()).unwrap();
    }
    drain_compaction(&mut db, &mut c);
    // Drain the reclaimed inputs from the free list, then compact again:
    // the second job's run comes from the bump pointer.
    for i in 0..4u8 {
        block_on(db.put(&[b'z', b'0' + i], b"after")).unwrap();
        block_on(db.flush()).unwrap();
    }
    drain_compaction(&mut db, &mut c);

    // No two live tables may share a device block.
    let mut runs: Vec<(u64, u64)> = (0..7)
        .flat_map(|l| db.level_tables(l).unwrap().to_vec())
        .map(|t| (t.first_block, t.end_block()))
        .collect();
    runs.sort_unstable();
    for w in runs.windows(2) {
        assert!(w[0].1 <= w[1].0, "live tables overlap on device: {runs:?}");
    }
    let mut buf = [0u8; 1024];
    for i in 0..4u8 {
        for j in 0..3u8 {
            assert_eq!(block_on(db.get(&big_key(i, j), &mut buf)), Ok(Some(1024)));
        }
        assert_eq!(block_on(db.get(&mid_key(i), &mut buf)), Ok(Some(948)));
    }
}

/// Fills the WAL region with 128 versions of `x`, so the next flush wraps
/// it, leaving stale pre-wrap records behind.
fn fill_and_wrap_wal(db: &mut TestDb<MemDevice<4096>>, c: &mut TestCompaction) {
    for i in 0..128u32 {
        put_retrying(db, c, b"x", &i.to_le_bytes());
    }
    flush_retrying(db, c); // WAL exhausted: this flush wraps it
}

/// Deletes `x` in flushed tables until compaction carries the tombstone to
/// the bottom and drops it: afterwards no table holds any sequence.
/// Returns the last sequence number issued.
fn delete_x_and_compact_away(db: &mut TestDb<MemDevice<4096>>, c: &mut TestCompaction) -> u64 {
    for _ in 0..16 {
        let seq = block_on(db.delete(b"x")).unwrap();
        flush_retrying(db, c);
        drain_compaction(db, c);
        let tables: usize = (0..7).map(|l| db.level_tables(l).unwrap().len()).sum();
        if tables == 0 {
            return seq;
        }
    }
    panic!("setup: compaction never dropped every table");
}

/// F2 — the WAL replay floor and the resumed sequence counter both come
/// from `Manifest::max_seq()`, which is *derived* from live tables. When
/// compaction drops the tables holding the highest sequences, the floor
/// falls, stale pre-wrap WAL records replay, and a deleted key comes back.
#[test]
#[ignore = "F2: derived seq floor lets stale pre-wrap WAL records resurrect deleted data"]
fn f2_deleted_key_stays_deleted_across_reopen_after_wal_wrap() {
    let mut db: TestDb<MemDevice<4096>> = TestDb::new(MemDevice::new(), test_config());
    block_on(db.open()).unwrap();
    let mut c = Box::new(TestCompaction::new());
    fill_and_wrap_wal(&mut db, &mut c);
    // Advance through most of the wrapped WAL so only a few stale blocks
    // remain past the append position (few enough to fit the memtable).
    for i in 1000..1100u32 {
        put_retrying(&mut db, &mut c, b"x", &i.to_le_bytes());
    }
    let issued = delete_x_and_compact_away(&mut db, &mut c);
    let mut buf = [0u8; 16];
    assert_eq!(block_on(db.get(b"x", &mut buf)), Ok(None));

    let mut db = TestDb::new(db.into_device(), test_config());
    let report = block_on(db.open()).unwrap();
    assert_eq!(block_on(db.get(b"x", &mut buf)), Ok(None), "x resurrected");
    assert!(
        report.max_seq >= issued,
        "sequence counter regressed: resumed at {} after issuing {issued}",
        report.max_seq
    );
}

/// F2 (variant) — with more stale records than memtable slots, the same
/// replay overflows the memtable and `open()` fails outright.
#[test]
#[ignore = "F2: derived seq floor makes open() fail with CorruptWal"]
fn f2_open_succeeds_after_wal_wrap_and_full_compaction() {
    let mut db: TestDb<MemDevice<4096>> = TestDb::new(MemDevice::new(), test_config());
    block_on(db.open()).unwrap();
    let mut c = Box::new(TestCompaction::new());
    fill_and_wrap_wal(&mut db, &mut c);
    delete_x_and_compact_away(&mut db, &mut c);

    let mut db = TestDb::new(db.into_device(), test_config());
    let opened = block_on(db.open());
    assert!(opened.is_ok(), "open failed: {opened:?}");
    let mut buf = [0u8; 16];
    assert_eq!(block_on(db.get(b"x", &mut buf)), Ok(None));
}

/// F3 — the open-time sweep inserts every unreferenced block below the
/// highest live table into a `FREELIST`-entry list and fails on overflow.
/// Compaction reclaims whole input tables, so ordinary operation produces
/// a device that the same configuration can no longer open.
#[test]
#[ignore = "F3: an undersized FREELIST makes open() fail after ordinary compaction"]
fn f3_reopen_succeeds_after_compaction_with_small_freelist() {
    type SmallFreeDb<D> = horton::Db<D, 4096, 256, 1024, 64, 4096, 7, 4, 1024, 8, 8>;
    let cfg = Config::new(8, 136, 136, 4224, 0, 1);
    let mut db: SmallFreeDb<MemDevice<4096>> = SmallFreeDb::new(MemDevice::new(), cfg);
    block_on(db.open()).unwrap();
    for i in 0..4u8 {
        block_on(db.put(&[b'k', i], b"v")).unwrap();
        block_on(db.flush()).unwrap();
    }
    let mut c = Box::new(TestCompaction::new());
    while block_on(db.compact_step(&mut c)).unwrap() == Progress::More {}

    let mut db = SmallFreeDb::new(db.into_device(), cfg);
    let opened = block_on(db.open());
    assert!(opened.is_ok(), "open failed: {opened:?}");
    let mut buf = [0u8; 8];
    assert_eq!(block_on(db.get(&[b'k', 0], &mut buf)), Ok(Some(1)));
}

/// F4 — `Db::new` hands back a handle that accepts writes before `open()`.
/// The WAL append position starts at `wal_start`, so the write lands on a
/// live WAL block and an acknowledged, unflushed mutation is lost.
#[test]
#[ignore = "F4: writes before open() clobber the live WAL"]
fn f4_write_before_open_cannot_destroy_acknowledged_data() {
    let mut db: TestDb<MemDevice<4096>> = TestDb::new(MemDevice::new(), test_config());
    block_on(db.open()).unwrap();
    block_on(db.put(b"acked", b"1")).unwrap(); // durable in the WAL, unflushed

    let mut db = TestDb::new(db.into_device(), test_config());
    let _ = block_on(db.put(b"early", b"2")); // forgot open(): should be refused

    let mut db = TestDb::new(db.into_device(), test_config());
    block_on(db.open()).unwrap();
    let mut buf = [0u8; 4];
    assert_eq!(block_on(db.get(b"acked", &mut buf)), Ok(Some(1)));
}

/// Collects a full scan in the given direction as `(key, value)` pairs.
fn scan_all(db: &TestDb<MemDevice<4096>>, reverse: bool) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out = Vec::new();
    let mut key = [0u8; 256];
    let mut val = [0u8; 1024];
    if reverse {
        let mut s = Box::new(RevScan::new(db));
        block_on(s.seek_prev(b"", None, u64::MAX)).unwrap();
        while let Some((kl, vl)) = block_on(s.prev(&mut key, &mut val)).unwrap() {
            out.push((key[..kl].to_vec(), val[..vl].to_vec()));
        }
        out.reverse();
    } else {
        let mut s = Box::new(Scan::new(db));
        block_on(s.seek(b"", None, u64::MAX)).unwrap();
        while let Some((kl, vl)) = block_on(s.next(&mut key, &mut val)).unwrap() {
            out.push((key[..kl].to_vec(), val[..vl].to_vec()));
        }
    }
    out
}

/// F5 — memtable versions of a key are stored newest-first, and the
/// reverse memtable walk meets the *oldest* one first, so `RevScan` over
/// unflushed data yields stale values and resurrects deleted keys. The
/// reverse differential test flushes before every check, so it never
/// exercises the memtable path.
#[test]
#[ignore = "F5: RevScan yields the oldest memtable version (stale values, deleted keys return)"]
fn f5_reverse_scan_matches_forward_scan_over_the_memtable() {
    let mut db: TestDb<MemDevice<4096>> = TestDb::new(MemDevice::new(), test_config());
    block_on(db.open()).unwrap();
    block_on(db.put(b"a", b"old")).unwrap();
    block_on(db.put(b"a", b"new")).unwrap();
    block_on(db.put(b"b", b"v")).unwrap();
    block_on(db.delete(b"b")).unwrap();
    let forward = scan_all(&db, false);
    assert_eq!(forward, vec![(b"a".to_vec(), b"new".to_vec())]);
    assert_eq!(scan_all(&db, true), forward);
}
