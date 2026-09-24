//! Regression tests for the confirmed defects in
//! `docs/ARCHITECTURE_REVIEW.md`, one per finding.
//!
//! Each test asserts the *correct* behavior. They were written first, as
//! `#[ignore]`d RED tests pinning each defect; each fix dropped its
//! `#[ignore]` in the same commit, so the test is now the finding's
//! regression guard. CI fails if any test here is ignored again.

use horton::{Compaction, Error, Progress, RevScan, Scan};

mod common;
use common::{Lcg, MemDevice, TestDb, block_on, noop_waker, test_config, tight_config};

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

/// Writes 1000-byte values under 8-byte keys — sequential or random —
/// flushing and compacting whenever asked, until the database genuinely
/// refuses (no flush fits and no compaction job can run). Returns the keys
/// accepted, after reading a sample of them back.
fn fill_until_full(cfg: horton::Config, random: bool) -> Vec<u64> {
    let mut db: TestDb<MemDevice<4096>> = TestDb::new(MemDevice::new(), cfg);
    block_on(db.open()).unwrap();
    let mut c = Box::new(TestCompaction::new());
    let mut rng = Lcg::new(6);
    let mut keys = Vec::new();
    let mut i = 0u64;
    'fill: loop {
        let k = if random {
            rng.next() ^ (rng.next() << 31)
        } else {
            i
        };
        i += 1;
        loop {
            match block_on(db.put(&k.to_be_bytes(), &[0x5A; 1000])) {
                Ok(_) => break,
                Err(Error::TableFull | Error::ArenaFull | Error::NoSpace) => loop {
                    match block_on(db.flush()) {
                        Ok(()) => break,
                        Err(Error::NoSpace) if db.compaction_pending() => loop {
                            match block_on(db.compact_step(&mut c)) {
                                Ok(Progress::More) => {}
                                Ok(Progress::Done) => break,
                                Err(Error::NoSpace) => break 'fill,
                                Err(e) => panic!("compact_step: {e:?}"),
                            }
                        },
                        Err(Error::NoSpace) => break 'fill,
                        Err(e) => panic!("flush: {e:?}"),
                    }
                },
                Err(e) => panic!("put: {e:?}"),
            }
        }
        keys.push(k);
        assert!(keys.len() < 100_000, "never filled");
    }
    assert_eq!(db.check_invariants(), Ok(()));
    let mut buf = [0u8; 1024];
    for k in keys.iter().step_by(16) {
        assert_eq!(
            block_on(db.get(&k.to_be_bytes(), &mut buf)),
            Ok(Some(1000)),
            "key {k}"
        );
    }
    keys
}

/// F6 — compaction never split its output, level capacity was a table
/// count, and disjoint tables never merged, so writes stopped for good
/// with the table region ~4% used (340 sequential or 644 random 1000-byte
/// writes on `TestDb`'s 4,088-block region). Now outputs split at slot
/// size, the bottom level grows into whatever slots are free, and small
/// tables consolidate: both workloads fill most of the region before
/// flush refuses — and everything written reads back.
///
/// Measured on the 28 × 30-block geometry: ~58% (sequential) and ~63%
/// (random) of the region's bytes hold live values when writes stop; on
/// `test_config` (28 × 146 blocks) the figures are ~63% and ~73%. The
/// remainder is the two-slot compaction reserve, L0's partly filled
/// flushes, and consolidation's three-quarters threshold. Debug builds use
/// 16-block slots to keep the run short.
#[test]
fn f6_writes_continue_until_the_region_is_mostly_full() {
    let slot = if cfg!(debug_assertions) { 16 } else { 30 };
    let cfg = horton::Config::new(8, 136, 136, 136 + 28 * slot, 0, 1);
    let region_bytes = 28 * usize::try_from(slot).unwrap() * 4096;
    for random in [false, true] {
        let n = fill_until_full(cfg, random).len();
        let used = n * 1017; // key + value + entry header
        assert!(
            used * 2 >= region_bytes,
            "random={random}: only {n} writes ({}% of the region)",
            used * 100 / region_bytes
        );
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
/// The job now reserves its output slots, which flushes never take.
///
/// Driven with overlapping keys (every level-to-level job is a real merge)
/// under the tight slot geometry (outputs split every few blocks), one
/// compaction step at a time, flushing whenever a job is mid-flight.
#[test]
fn f14_flush_during_inflight_compaction_keeps_tables_disjoint() {
    use std::collections::BTreeMap;

    let mut db: TestDb<MemDevice<4096>> = TestDb::new(MemDevice::new(), tight_config());
    block_on(db.open()).unwrap();
    let mut c = Box::new(TestCompaction::new());
    let mut model = BTreeMap::new();
    let mut rng = Lcg::new(14);
    let mut mid_job_flushes = 0;
    for i in 0..1500u32 {
        let key = [b'k', u8::try_from(rng.next_bounded(160)).unwrap()];
        let val = vec![u8::try_from(i % 251).unwrap(); 200 + rng.next_bounded(600)];
        loop {
            match block_on(db.put(&key, &val)) {
                Ok(_) => break,
                Err(Error::TableFull | Error::ArenaFull | Error::NoSpace) => {
                    match block_on(db.flush()) {
                        Ok(()) => {}
                        Err(Error::NoSpace) => {
                            // L0 full (or no slot): run a step and retry.
                            let _ = block_on(db.compact_step(&mut c)).unwrap();
                        }
                        Err(e) => panic!("flush: {e:?}"),
                    }
                }
                Err(e) => panic!("put: {e:?}"),
            }
        }
        model.insert(key, val);
        // One compaction step per write; mid-job, try to flush.
        if db.compaction_pending() || db.slot_stats().reserved > 0 {
            let step = block_on(db.compact_step(&mut c)).unwrap();
            if step == Progress::More
                && db.slot_stats().reserved > 0
                && block_on(db.flush()).is_ok()
            {
                mid_job_flushes += 1;
            }
        }
        assert_eq!(db.check_invariants(), Ok(()), "after write {i}");
    }
    assert!(
        mid_job_flushes > 10,
        "only {mid_job_flushes} mid-job flushes"
    );
    drain_compaction(&mut db, &mut c);
    let mut db = TestDb::new(db.into_device(), tight_config());
    block_on(db.open()).unwrap();
    assert_eq!(db.check_invariants(), Ok(()));
    let mut buf = [0u8; 1024];
    for (k, v) in &model {
        let n = block_on(db.get(k, &mut buf)).unwrap().expect("key lost");
        assert_eq!(&buf[..n], &v[..], "key {k:?}");
    }
}

/// 3 levels of 3 tables, 9 table slots of 8 blocks. A table with one data
/// block (4 blocks) fills less than three quarters of a slot — compaction
/// rewrites it rather than moving it down — while one with three data
/// blocks (6 blocks) does not.
type NarrowDb<D> = horton::Db<D, 4096, 256, 1024, 64, 4096, 3, 3, 1024, 8>;

const fn narrow_config() -> horton::Config {
    horton::Config::new(8, 136, 136, 136 + 9 * 8, 0, 1)
}

/// One flush per key group, then the L0 -> L1 job they trigger.
fn l0_job(
    db: &mut NarrowDb<MemDevice<4096>>,
    c: &mut TestCompaction,
    groups: [&[&[u8]]; 3],
    v: &[u8],
) {
    for keys in groups {
        for k in keys {
            block_on(db.put(k, v)).unwrap();
        }
        block_on(db.flush()).unwrap();
    }
    while block_on(db.compact_step(c)).unwrap() == Progress::More {}
    assert_eq!(db.check_invariants(), Ok(()));
}

/// F15 — compaction dropped a bottommost tombstone whenever nothing *deeper*
/// overlapped it, ignoring shallower tables. A re-ingested table sits at
/// L0 but can hold versions older than tombstones below it; dropping such
/// a tombstone resurrects the value it was hiding.
#[test]
fn f15_tombstone_drop_respects_older_ingested_tables() {
    use core::task::{Context, Poll};
    use horton::BlockDevice;

    let mut db: NarrowDb<MemDevice<4096>> = NarrowDb::new(MemDevice::new(), narrow_config());
    block_on(db.open()).unwrap();
    let mut c = Box::new(TestCompaction::new());
    // T1 holds b@old; archive it (upload, then forget locally).
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

    // Two L1 tables of three data blocks each (1000-byte values), well
    // above b's range.
    let big = [7u8; 1000];
    let x: [&[u8]; 12] = [
        b"x0", b"x1", b"x2", b"x3", b"x4", b"x5", b"x6", b"x7", b"x8", b"x9", b"xa", b"xb",
    ];
    let y: [&[u8]; 12] = [
        b"y0", b"y1", b"y2", b"y3", b"y4", b"y5", b"y6", b"y7", b"y8", b"y9", b"ya", b"yb",
    ];
    l0_job(&mut db, &mut c, [&x[..4], &x[4..8], &x[8..]], &big);
    l0_job(&mut db, &mut c, [&y[..4], &y[4..8], &y[8..]], &big);
    // Delete b; a snapshot keeps the tombstone alive into L1, in a table
    // S of one data block: small, and the lowest key in a now-full L1.
    block_on(db.delete(b"b")).unwrap();
    let snap = db.snapshot().unwrap();
    l0_job(&mut db, &mut c, [&[b"c"], &[b"d"], &[b"e"]], b"v");
    assert_eq!(db.level_tables(1).unwrap().len(), 3, "setup: L1 full");
    assert!(db.compaction_pending());

    // Re-attach T1 at L0 and release the snapshot. The next job rewrites
    // S alone into L2 — the bottom of b's range — without L0.
    assert_eq!(block_on(db.ingest_table(&sealed, &remote, 0)), Ok(true));
    db.release_snapshot(snap);
    let mut val = [0u8; 8];
    assert_eq!(block_on(db.get(b"b", &mut val)), Ok(None), "deleted before");
    while block_on(db.compact_step(&mut c)).unwrap() == Progress::More {}
    assert_eq!(
        db.level_tables(2).unwrap().len(),
        1,
        "setup: S rewritten into L2"
    );
    assert_eq!(
        db.level_tables(0).unwrap().len(),
        1,
        "setup: T1 untouched in L0"
    );
    assert_eq!(
        block_on(db.get(b"b", &mut val)),
        Ok(None),
        "the delete must still hide the older ingested version"
    );
}
