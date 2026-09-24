//! Regression tests for the confirmed defects in
//! `docs/ARCHITECTURE_REVIEW.md`, one per finding.
//!
//! Each test asserts the *correct* behavior. They were written first, as
//! `#[ignore]`d RED tests pinning each defect; each fix dropped its
//! `#[ignore]` in the same commit, so the test is now the finding's
//! regression guard. CI fails if any test here is ignored again.

use horton::{Compaction, Error, Progress, RevScan, Scan};

mod common;
use common::{MemDevice, TestDb, block_on, noop_waker, test_config};

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

/// F1 — compaction's output-run reservation assumed a merge never needs
/// more data blocks than its inputs used. Greedy block packing breaks that:
/// four inputs that each pack into one tight block (3 × 1041 B + 965 B =
/// 4088 B) interleave into five output blocks, and the output used to run
/// past its reservation onto the next table. Every table now lives in its
/// own fixed slot and the writer is capped at the slot, so an output can
/// never reach another table's blocks.
#[test]
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

/// F2 (ingest) — an ingested table can carry sequences from another
/// history. The counter must resume above them, in session and across
/// reopen, or a later local write loses to the older ingested version
/// under highest-sequence-wins.
#[test]
fn f2_local_write_after_ingest_beats_the_ingested_version() {
    use core::task::{Context, Poll};
    use horton::BlockDevice;

    // Source database: 300 mutations, so its table carries seqs up to 300.
    let mut src: TestDb<MemDevice<4096>> = TestDb::new(MemDevice::new(), test_config());
    block_on(src.open()).unwrap();
    let mut c = Box::new(TestCompaction::new());
    for i in 0..300u32 {
        put_retrying(&mut src, &mut c, b"k", &i.to_le_bytes());
    }
    flush_retrying(&mut src, &mut c);
    let t = *src.level_tables(0).unwrap().last().unwrap();
    let sealed = src.archive_plan(0, t.id).unwrap().sealed();
    // Upload the table's blocks to a remote device at offset 0.
    let mut remote = MemDevice::<4096>::new();
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut buf = [0u8; 4096];
    for (k, id) in (t.first_block..t.end_block()).enumerate() {
        assert!(matches!(
            src.device().poll_read_block(&mut cx, id, &mut buf),
            Poll::Ready(Ok(()))
        ));
        let dst = u64::try_from(k).unwrap();
        assert!(matches!(
            remote.poll_write_block(&mut cx, dst, &buf),
            Poll::Ready(Ok(()))
        ));
    }

    // A fresh database ingests it, then writes the same key locally.
    let mut db: TestDb<MemDevice<4096>> = TestDb::new(MemDevice::new(), test_config());
    block_on(db.open()).unwrap();
    assert_eq!(block_on(db.ingest_table(&sealed, &remote, 0)), Ok(true));
    let seq = block_on(db.put(b"k", b"local")).unwrap();
    assert!(
        seq > sealed.max_seq,
        "local write got seq {seq} <= ingested {}",
        sealed.max_seq
    );
    let mut val = [0u8; 16];
    assert_eq!(block_on(db.get(b"k", &mut val)), Ok(Some(5)));
    assert_eq!(&val[..5], b"local");

    // Across reopen too (the write is still only in the WAL).
    let mut db = TestDb::new(db.into_device(), test_config());
    block_on(db.open()).unwrap();
    assert_eq!(block_on(db.get(b"k", &mut val)), Ok(Some(5)));
    assert_eq!(&val[..5], b"local");
}

/// F3 — the open-time sweep used to insert every unreferenced block below
/// the highest live table into a `FREELIST`-entry list and fail on
/// overflow, so ordinary compaction produced a device the same
/// configuration could no longer open. Allocation is now one table per
/// fixed slot: `open()` rebuilds the slot map from the manifest alone,
/// with no capacity to exceed, and arrives at the same state the live
/// session had.
#[test]
fn f3_reopen_succeeds_after_compaction() {
    let mut db: TestDb<MemDevice<4096>> = TestDb::new(MemDevice::new(), test_config());
    block_on(db.open()).unwrap();
    for i in 0..4u8 {
        block_on(db.put(&[b'k', i], b"v")).unwrap();
        block_on(db.flush()).unwrap();
    }
    let mut c = Box::new(TestCompaction::new());
    while block_on(db.compact_step(&mut c)).unwrap() == Progress::More {}
    let live = db.slot_stats();

    let mut db = TestDb::new(db.into_device(), test_config());
    let opened = block_on(db.open());
    assert!(opened.is_ok(), "open failed: {opened:?}");
    assert_eq!(db.slot_stats(), live, "reopen rebuilds the same slot map");
    assert_eq!(db.check_invariants(), Ok(()));
    let mut buf = [0u8; 8];
    assert_eq!(block_on(db.get(&[b'k', 0], &mut buf)), Ok(Some(1)));
}

/// F4 — `Db::new` hands back a handle that accepts writes before `open()`.
/// The WAL append position starts at `wal_start`, so the write lands on a
/// live WAL block and an acknowledged, unflushed mutation is lost.
#[test]
fn f4_write_before_open_cannot_destroy_acknowledged_data() {
    let mut db: TestDb<MemDevice<4096>> = TestDb::new(MemDevice::new(), test_config());
    block_on(db.open()).unwrap();
    block_on(db.put(b"acked", b"1")).unwrap(); // durable in the WAL, unflushed

    let mut db = TestDb::new(db.into_device(), test_config());
    // Forgot open(): every device-touching call is refused, nothing lands.
    assert_eq!(block_on(db.put(b"early", b"2")), Err(Error::NotOpen));
    assert_eq!(block_on(db.flush()), Err(Error::NotOpen));
    let mut probe = [0u8; 4];
    assert_eq!(block_on(db.get(b"acked", &mut probe)), Err(Error::NotOpen));
    assert_eq!(db.snapshot(), Err(Error::NotOpen));
    assert!(!db.is_open());

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

/// F5 (related cases from the scan sub-review): a newer put above a range
/// tombstone must win, and a snapshot `seek_prev` at `from` must still find
/// `from`'s older visible version when its newest one is above the
/// snapshot — both over unflushed data.
#[test]
fn f5_reverse_scan_range_delete_and_snapshot_seek() {
    let mut db: TestDb<MemDevice<4096>> = TestDb::new(MemDevice::new(), test_config());
    block_on(db.open()).unwrap();
    block_on(db.put(b"k", b"v1")).unwrap();
    block_on(db.delete_range(b"k", b"z")).unwrap();
    block_on(db.put(b"k", b"v3")).unwrap();
    assert_eq!(scan_all(&db, false), vec![(b"k".to_vec(), b"v3".to_vec())]);
    assert_eq!(scan_all(&db, true), scan_all(&db, false));

    let mut db: TestDb<MemDevice<4096>> = TestDb::new(MemDevice::new(), test_config());
    block_on(db.open()).unwrap();
    block_on(db.put(b"a", b"a1")).unwrap();
    block_on(db.put(b"k", b"k1")).unwrap();
    let snap = db.snapshot().unwrap();
    block_on(db.put(b"k", b"k2")).unwrap();
    let mut s = Box::new(RevScan::new(&db));
    block_on(s.seek_prev(b"k", None, snap)).unwrap();
    let mut key = [0u8; 8];
    let mut val = [0u8; 8];
    let mut got = Vec::new();
    while let Some((kl, vl)) = block_on(s.prev(&mut key, &mut val)).unwrap() {
        got.push((key[..kl].to_vec(), val[..vl].to_vec()));
    }
    assert_eq!(
        got,
        vec![
            (b"k".to_vec(), b"k1".to_vec()),
            (b"a".to_vec(), b"a1".to_vec())
        ]
    );
}

/// F12 — sequence 0 is reserved (the counter issues 1 and up), but an
/// externally built table can carry it and `ingest_table` accepts such a
/// table. Point reads already treat a seq-0 version as invisible; scans
/// yielded an empty key forever (release) or panicked (debug). Every read
/// path must agree: the version is invisible and the scan terminates.
#[test]
fn f12_seq0_entries_are_invisible_and_scans_terminate() {
    use horton::{KeyBound, SealedTable, SstEntry, bloom_k, write_table};

    let mut remote = MemDevice::<4096>::new();
    let entries = [
        SstEntry {
            key: b"a",
            val: b"x",
            seq: 0,
            tombstone: false,
            expire_at: 0,
        },
        SstEntry {
            key: b"b",
            val: b"y",
            seq: 7,
            tombstone: false,
            expire_at: 0,
        },
    ];
    let blocks = block_on(write_table::<_, 4096, 1024, 256>(
        &mut remote,
        0,
        bloom_k(1024 * 8, 2),
        entries.into_iter(),
        None,
    ))
    .unwrap();
    let sealed = SealedTable {
        id: 1000,
        block_count: u32::try_from(blocks).unwrap(),
        first_key: KeyBound::from_slice(b"a").unwrap(),
        last_key: KeyBound::from_slice(b"b").unwrap(),
        max_seq: 7,
        min_seq: 0,
        entry_count: 2,
        rdel_blocks: 0,
    };
    let mut db: TestDb<MemDevice<4096>> = TestDb::new(MemDevice::new(), test_config());
    block_on(db.open()).unwrap();
    assert_eq!(block_on(db.ingest_table(&sealed, &remote, 0)), Ok(true));

    let mut val = [0u8; 8];
    assert_eq!(block_on(db.get(b"a", &mut val)), Ok(None));
    assert_eq!(block_on(db.get(b"b", &mut val)), Ok(Some(1)));
    let want = vec![(b"b".to_vec(), b"y".to_vec())];
    for reverse in [false, true] {
        let mut key = [0u8; 8];
        let mut got = Vec::new();
        if reverse {
            let mut s = Box::new(RevScan::new(&db));
            block_on(s.seek_prev(b"", None, u64::MAX)).unwrap();
            while let Some((kl, vl)) = block_on(s.prev(&mut key, &mut val)).unwrap() {
                got.push((key[..kl].to_vec(), val[..vl].to_vec()));
                assert!(got.len() <= 4, "reverse scan does not terminate: {got:?}");
            }
        } else {
            let mut s = Box::new(Scan::new(&db));
            block_on(s.seek(b"", None, u64::MAX)).unwrap();
            while let Some((kl, vl)) = block_on(s.next(&mut key, &mut val)).unwrap() {
                got.push((key[..kl].to_vec(), val[..vl].to_vec()));
                assert!(got.len() <= 4, "forward scan does not terminate: {got:?}");
            }
        }
        assert_eq!(got, want, "reverse={reverse}");
    }
}

/// F13 — a data block failing its CRC read as "absent" on the point-read
/// path, so `get` fell through to an older version in a deeper table (a
/// silent stale read) while scans reported `CorruptBlock`. Both must
/// report the corruption.
#[test]
fn f13_corrupt_newer_block_is_an_error_not_a_stale_read() {
    let mut db: TestDb<MemDevice<4096>> = TestDb::new(MemDevice::new(), test_config());
    block_on(db.open()).unwrap();
    block_on(db.put(b"k", b"old")).unwrap();
    block_on(db.flush()).unwrap();
    block_on(db.put(b"k", b"new")).unwrap();
    block_on(db.flush()).unwrap();
    let newer = *db.level_tables(0).unwrap().last().unwrap();
    // The newer table's first data block (range-tombstone blocks, if any,
    // precede it in this layout; this table has none).
    assert_eq!(newer.rdel_blocks, 0);
    let data_block = usize::try_from(newer.first_block).unwrap();
    let mut dev = db.into_device();
    dev.blocks_mut()[data_block][20] ^= 0xFF; // bit rot
    let mut db = TestDb::new(dev, test_config());
    block_on(db.open()).unwrap();
    let mut buf = [0u8; 8];
    assert!(
        matches!(
            block_on(db.get(b"k", &mut buf)),
            Err(Error::CorruptBlock { .. })
        ),
        "a corrupt newer version must not silently fall back to the older one"
    );
    let mut s = Box::new(Scan::new(&db));
    assert!(matches!(
        block_on(s.seek(b"", None, u64::MAX)),
        Err(Error::CorruptBlock { .. })
    ));
}

/// F17 — found by the lifecycle fuzzer. WAL recovery reads blocks into the
/// writer's staging buffer but left the writer's "bytes past `dirty_to`
/// are zero" mark untouched. When recovery ends at the end of the WAL
/// region — routine once the WAL has wrapped and stale blocks fill it —
/// the buffer still holds a stale block, and the first block written
/// after the reopen carries the new record followed by that garbage. The
/// next recovery reads the garbage as a torn tail and stops there, losing
/// every acknowledged write after it.
#[test]
fn f17_writes_after_a_reopen_survive_the_next_reopen() {
    let mut db: TestDb<MemDevice<4096>> = TestDb::new(MemDevice::new(), test_config());
    block_on(db.open()).unwrap();
    let mut c = Box::new(TestCompaction::new());
    // Fill the WAL region so it wraps: stale pre-wrap blocks now fill it to
    // the end, so the next recovery scans all the way to `wal_end`.
    fill_and_wrap_wal(&mut db, &mut c);
    let mut db = TestDb::new(db.into_device(), test_config());
    block_on(db.open()).unwrap();
    for i in 0..5u8 {
        block_on(db.put(&[b'n', i], b"after-reopen")).unwrap();
    }
    let mut db = TestDb::new(db.into_device(), test_config());
    block_on(db.open()).unwrap();
    let mut val = [0u8; 16];
    for i in 0..5u8 {
        assert_eq!(
            block_on(db.get(&[b'n', i], &mut val)),
            Ok(Some(12)),
            "acknowledged put n{i} lost across the second reopen"
        );
    }
}

/// F14 — a compaction job used to only *peek* at its output run, so a
/// flush between two `compact_step` calls could allocate the same blocks:
/// the flushed table and the job's output overlapped on device, a key
/// written mid-job read as `CorruptBlock`, and compacted keys vanished.
/// The job now reserves its output slot, which flushes never take.
#[test]
fn f14_flush_during_inflight_compaction_keeps_tables_disjoint() {
    let mut db: TestDb<MemDevice<4096>> = TestDb::new(MemDevice::new(), test_config());
    block_on(db.open()).unwrap();
    let mut c = Box::new(TestCompaction::new());
    let v = [9u8; 1000];
    // Four disjoint L1 tables, each several data blocks.
    for round in 0..4u8 {
        for f in 0..4u8 {
            for i in 0..3u8 {
                block_on(db.put(&[b'a' + round, f, i], &v)).unwrap();
            }
            block_on(db.flush()).unwrap();
        }
        while block_on(db.compact_step(&mut c)).unwrap() == Progress::More {}
    }
    assert!(db.compaction_pending(), "setup: L1 should be full");
    // Start the L1 -> L2 job, then do real-time work mid-job.
    assert_eq!(block_on(db.compact_step(&mut c)), Ok(Progress::More));
    block_on(db.put(b"zz", b"mid-job")).unwrap();
    block_on(db.flush()).unwrap();
    while block_on(db.compact_step(&mut c)).unwrap() == Progress::More {}
    assert_eq!(db.check_invariants(), Ok(()));
    let mut buf = [0u8; 1024];
    assert_eq!(block_on(db.get(b"zz", &mut buf)), Ok(Some(7)));
    for round in 0..4u8 {
        for f in 0..4u8 {
            for i in 0..3u8 {
                assert_eq!(
                    block_on(db.get(&[b'a' + round, f, i], &mut buf)),
                    Ok(Some(1000)),
                    "key {round}/{f}/{i} lost"
                );
            }
        }
    }
}

/// F15 — compaction dropped a bottommost tombstone whenever nothing *deeper*
/// overlapped it, ignoring shallower tables. A re-ingested table sits at
/// L0 but can hold versions older than tombstones below it; dropping such
/// a tombstone resurrects the value it was hiding.
#[test]
fn f15_tombstone_drop_respects_older_ingested_tables() {
    use core::task::{Context, Poll};
    use horton::BlockDevice;

    let mut db: TestDb<MemDevice<4096>> = TestDb::new(MemDevice::new(), test_config());
    block_on(db.open()).unwrap();
    let mut c = Box::new(TestCompaction::new());
    // T1 holds b@1; archive it (upload, then forget locally).
    block_on(db.put(b"b", b"old")).unwrap();
    block_on(db.flush()).unwrap();
    let t1 = db.level_tables(0).unwrap()[0];
    let sealed = db.archive_plan(0, t1.id).unwrap().sealed();
    let mut remote = MemDevice::<4096>::new();
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut buf = [0u8; 4096];
    for (k, id) in (t1.first_block..t1.end_block()).enumerate() {
        assert!(matches!(
            db.device().poll_read_block(&mut cx, id, &mut buf),
            Poll::Ready(Ok(()))
        ));
        assert!(matches!(
            remote.poll_write_block(&mut cx, u64::try_from(k).unwrap(), &buf),
            Poll::Ready(Ok(()))
        ));
    }
    assert_eq!(block_on(db.archive_commit(0, t1.id)), Ok(true));

    // Delete b; a snapshot keeps the tombstone alive on its way into L1.
    block_on(db.delete(b"b")).unwrap();
    let snap = db.snapshot().unwrap();
    // Four compaction rounds of disjoint key ranges fill L1 with four
    // tables; the first (oldest) holds the tombstone.
    for round in 0..4u8 {
        for f in 0..4u8 {
            block_on(db.put(&[b'b', b'0' + round, b'0' + f], b"x")).unwrap();
            block_on(db.flush()).unwrap();
        }
        while block_on(db.compact_step(&mut c)).unwrap() == Progress::More {}
    }
    assert_eq!(db.level_tables(1).unwrap().len(), 4, "setup: L1 full");
    // Re-attach T1 at L0, release the snapshot, and let the L1 -> L2 job
    // (which does not include L0) run.
    assert_eq!(block_on(db.ingest_table(&sealed, &remote, 0)), Ok(true));
    db.release_snapshot(snap);
    let mut val = [0u8; 8];
    assert_eq!(block_on(db.get(b"b", &mut val)), Ok(None), "deleted before");
    while block_on(db.compact_step(&mut c)).unwrap() == Progress::More {}
    assert_eq!(
        block_on(db.get(b"b", &mut val)),
        Ok(None),
        "the delete must still hide the older ingested version"
    );
}
