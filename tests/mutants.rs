//! Mutation-testing killers: regression tests whose sole job is to kill
//! mutants that the feature test-suites miss.
//!
//! Each test is named `mut_<area>_<behavior>` and carries a comment naming
//! the mutant(s) it kills. These tests are part of the permanent suite: a
//! future refactor that breaks the asserted behavior fails here first.

mod common;

use core::convert::Infallible;

use horton::WriteBatch;
use horton::memtable::MemTable;
use horton::{BlockDevice, Compaction, Error, Progress, Scan};

use common::{MemDevice, TestDb, block_on, test_config};

const KEY_MAX: usize = 32;
const VAL_MAX: usize = 64;

/// Caller scratch for `compact_step`, matching the test database shape.
type TestCompaction = Compaction<4096, 256, 1024, 1024>;

fn open<D: BlockDevice>(db: &mut TestDb<D>)
where
    D::Error: std::fmt::Debug,
{
    block_on(db.open()).unwrap();
}

fn get<D: BlockDevice>(db: &TestDb<D>, key: &[u8]) -> Option<Vec<u8>>
where
    D::Error: std::fmt::Debug,
{
    let mut buf = [0u8; 2048];
    block_on(db.get(key, &mut buf))
        .unwrap()
        .map(|n| buf[..n].to_vec())
}

fn get_now<D: BlockDevice>(db: &TestDb<D>, key: &[u8], now: u64) -> Option<Vec<u8>>
where
    D::Error: std::fmt::Debug,
{
    let mut buf = [0u8; 2048];
    block_on(db.get_with_time(key, &mut buf, now))
        .unwrap()
        .map(|n| buf[..n].to_vec())
}

// ---------------------------------------------------------------------------
// src/batch.rs
// ---------------------------------------------------------------------------

/// Kills `replace WriteBatch::is_empty -> bool with true`: a batch holding
/// an op must report non-empty, and `clear()` must restore emptiness.
#[test]
fn mut_batch_is_empty_reflects_len() {
    let mut b = WriteBatch::<KEY_MAX, VAL_MAX, 4>::new();
    assert!(b.is_empty());
    b.put(b"k", b"v").unwrap();
    assert!(!b.is_empty());
    b.clear();
    assert!(b.is_empty());
}

/// Kills `replace WriteBatch::capacity -> usize with 0` and `... with 1`:
/// capacity is the `OPS` const, not a hardcoded small number.
#[test]
fn mut_batch_capacity_reports_ops() {
    assert_eq!(WriteBatch::<KEY_MAX, VAL_MAX, 1>::new().capacity(), 1);
    assert_eq!(WriteBatch::<KEY_MAX, VAL_MAX, 7>::new().capacity(), 7);
    assert_eq!(WriteBatch::<KEY_MAX, VAL_MAX, 64>::new().capacity(), 64);
}

// ---------------------------------------------------------------------------
// src/memtable.rs
// ---------------------------------------------------------------------------

/// Kills `replace MemTable::is_empty -> bool with true`: a table holding
/// slots must report non-empty. The feature suite asserts emptiness of a
/// fresh table but never the non-empty side.
#[test]
fn mut_memtable_is_empty_reflects_len() {
    let mut t = MemTable::<8, 256, 16, 32>::new();
    assert!(t.is_empty());
    t.insert::<Infallible>(b"k", b"v", 1, false).unwrap();
    assert!(!t.is_empty());
}

/// Kills `replace > with >= in MemTable::plan` (src/memtable.rs:262 for
/// keys, :273 for values): a key of exactly `KEY_MAX` bytes and a value of
/// exactly `VAL_MAX` bytes are both legal — the bounds are inclusive. The
/// mutants reject them with the absurd `KeyTooLarge { len: 16, max: 16 }` /
/// `ValueTooLarge { len: 32, max: 32 }`.
#[test]
fn mut_memtable_key_val_max_are_inclusive() {
    let mut t = MemTable::<8, 256, 16, 32>::new();
    t.insert::<Infallible>(&[9u8; 16], b"v", 1, false).unwrap();
    let e = t.get(&[9u8; 16]).unwrap();
    assert_eq!(e.val, b"v");
    t.insert::<Infallible>(b"k", &[7u8; 32], 2, false).unwrap();
    let e = t.get(b"k").unwrap();
    assert_eq!(e.val, &[7u8; 32]);
}

/// Kills `replace > with >= in MemTable::plan` (src/memtable.rs:288): an
/// insert whose bytes land exactly on the arena limit fits — the bound is
/// inclusive. The mutant rejects the last byte with `ArenaFull`.
#[test]
fn mut_memtable_arena_exact_fit_is_allowed() {
    let mut t = MemTable::<8, 256, 16, 32>::new();
    // 8 x (1 + 31) = 256 bytes: exactly the arena limit.
    for i in 0..8u8 {
        let k = [i];
        t.insert::<Infallible>(&k, &[i; 31], u64::from(i) + 1, false)
            .unwrap();
    }
    assert_eq!(t.len(), 8);
    assert_eq!(t.arena_len(), 256);
}

/// Kills five `plan_range_del` mutants (src/memtable.rs:311 `> with
/// ==/</>=`, :318 `>= with <`, :328 `>= with <`). The feature suite never
/// exercises range-tombstone validation directly, so the bound checks,
/// the empty/inverted-range rejection, and the table-full check were all
/// untested.
#[test]
fn mut_memtable_range_del_validates_bounds() {
    let mut t = MemTable::<8, 256, 16, 32>::new();
    // KEY_MAX-sized bounds are legal (inclusive).
    t.insert_range_del::<Infallible>(&[8u8; 16], &[9u8; 16], 1)
        .unwrap();
    // Oversized bound rejected.
    assert!(matches!(
        t.insert_range_del::<Infallible>(&[8u8; 17], &[9u8; 16], 2),
        Err(Error::KeyTooLarge { .. })
    ));
    // Empty bound rejected.
    assert!(matches!(
        t.insert_range_del::<Infallible>(b"", &[9u8; 16], 3),
        Err(Error::EmptyKey)
    ));
    // Inverted range rejected.
    assert!(matches!(
        t.insert_range_del::<Infallible>(b"b", b"a", 4),
        Err(Error::EmptyKey)
    ));
    // Empty range (start == end) rejected.
    assert!(matches!(
        t.insert_range_del::<Infallible>(b"a", b"a", 5),
        Err(Error::EmptyKey)
    ));
}

/// Kills seven `plan_range_del` arena-accounting mutants
/// (src/memtable.rs:331 `+ with -/*`, :332 `> with ==/</>=` and `+ with
/// -/*`). The feature suite never fills a memtable's arena via range
/// tombstones, so the size math was untested. Scenario A pins the
/// overflow-reject side; scenario B pins the exact-fit-allow side.
#[test]
fn mut_memtable_range_del_arena_accounting() {
    // Scenario A: 240 bytes used; a 32-byte range del must NOT fit.
    let mut t = MemTable::<8, 256, 16, 32>::new();
    for i in 0..5u8 {
        t.insert::<Infallible>(&[i; 16], &[i; 32], u64::from(i) + 1, false)
            .unwrap();
    }
    assert_eq!(t.arena_len(), 240);
    assert!(matches!(
        t.insert_range_del::<Infallible>(&[8u8; 16], &[9u8; 16], 100),
        Err(Error::ArenaFull)
    ));
    // Scenario B: 224 bytes used; a 32-byte range del fits exactly.
    let mut u = MemTable::<8, 256, 16, 32>::new();
    for i in 0..7u8 {
        u.insert_range_del::<Infallible>(&[i; 16], &[0x80 + i; 16], u64::from(i) + 1)
            .unwrap();
    }
    assert_eq!(u.arena_len(), 224);
    u.insert_range_del::<Infallible>(&[0x70; 16], &[0x90; 16], 100)
        .unwrap();
    assert_eq!(u.arena_len(), 256);
}

/// Kills `replace < with <= in MemTable::get_at` (src/memtable.rs:514):
/// the run-walk must stop at `self.len`. The mutant reads one slot past
/// the used region; on a full table that is `slots[CAP]` — an index-out-
/// of-bounds panic. The feature suite never snapshot-reads a full table
/// past the end of a max-key run.
#[test]
fn mut_memtable_get_at_stops_at_len() {
    let mut t = MemTable::<8, 256, 16, 32>::new();
    // Fill to CAP; the max key (b"h") has seq 100.
    for i in 0u8..8 {
        let k = [b'a' + i];
        let seq = if i == 7 { 100 } else { u64::from(i) + 1 };
        t.insert::<Infallible>(&k, b"v", seq, false).unwrap();
    }
    assert_eq!(t.len(), 8);
    // Snapshot older than b"h"'s only version: no match, walk runs to the end.
    assert!(t.get_at(b"h", 50).is_none());
    // Sanity: live view still finds it.
    assert!(t.get(b"h").is_some());
}

/// Kills `replace += with -=` and `replace += with *=` in
/// `MemTable::get_at` (src/memtable.rs:522): the run walk must skip
/// range tombstones by advancing. The `-=` mutant walks backwards
/// (underflow panic); the `*=` mutant never advances (infinite loop).
/// The feature suite never puts a range tombstone inside a looked-up
/// key's run.
#[test]
fn mut_memtable_get_at_skips_range_del() {
    let mut t = MemTable::<8, 256, 16, 32>::new();
    // Put first (older), then a range tombstone starting at "m" (newer).
    // The rdel sorts first in "m"'s run, so get_at must skip it.
    t.insert::<Infallible>(b"m", b"v", 1, false).unwrap();
    t.insert_range_del::<Infallible>(b"m", b"z", 2).unwrap();
    let e = t.get_at(b"m", u64::MAX).unwrap();
    assert_eq!(e.val, b"v");
}

/// Kills `replace MemTable::max_covering_rdel -> Option<u64> with None`
/// / `Some(0)` / `Some(1)` (src/memtable.rs:546) and the logic mutants in
/// its scan (:548 `||`→`&&`, `!` deletion, `>`→`==`/`<`/`>=`; :557
/// `&&`→`||`, `<=`→`>`, `<`→`==`/`>`/`<=`, `>`→`==`/`<` in the best-seq
/// update): the highest covering range tombstone's seq must be reported
/// accurately. The feature suite never queries `max_covering_rdel`
/// directly.
#[test]
fn mut_memtable_max_covering_rdel_reports_seq() {
    let mut t = MemTable::<8, 256, 16, 32>::new();
    assert_eq!(t.max_covering_rdel(b"m", u64::MAX), None);
    // A regular put must not be mistaken for a range del.
    t.insert::<Infallible>(b"m", b"v", 1, false).unwrap();
    assert_eq!(t.max_covering_rdel(b"m", u64::MAX), None);
    t.insert_range_del::<Infallible>(b"a", b"z", 7).unwrap();
    assert_eq!(t.max_covering_rdel(b"m", u64::MAX), Some(7));
    assert_eq!(t.max_covering_rdel(b"m", 7), Some(7));
    // Outside the range, or before the rdel's seq: no cover.
    assert_eq!(t.max_covering_rdel(b"zz", u64::MAX), None);
    assert_eq!(t.max_covering_rdel(b"m", 6), None);
    // Boundaries: start <= key < end.
    assert_eq!(t.max_covering_rdel(b"a", u64::MAX), Some(7));
    assert_eq!(t.max_covering_rdel(b"z", u64::MAX), None);
    // Two covering rdels with different starts (lower seq sorts first):
    // the highest seq wins.
    let mut u = MemTable::<8, 256, 16, 32>::new();
    u.insert_range_del::<Infallible>(b"a", b"z", 5).unwrap();
    u.insert_range_del::<Infallible>(b"b", b"y", 7).unwrap();
    assert_eq!(u.max_covering_rdel(b"m", u64::MAX), Some(7));
    assert_eq!(u.max_covering_rdel(b"m", 6), Some(5));
}

/// Kills `replace MemTable::check_insert -> Result with Ok(())` and
/// `replace MemTable::check_insert_range_del -> Result with Ok(())`
/// (src/memtable.rs:395, :499): the validation shims must actually
/// validate. The feature suite never calls them with invalid input
/// directly.
#[test]
fn mut_memtable_check_inserts_validate() {
    let t = MemTable::<8, 256, 16, 32>::new();
    assert!(
        t.check_insert::<Infallible>(&[0u8; 17], b"v", false)
            .is_err()
    );
    assert!(t.check_insert::<Infallible>(b"", b"v", false).is_err());
    assert!(t.check_insert_range_del::<Infallible>(b"b", b"a").is_err());
    assert!(
        t.check_insert_range_del::<Infallible>(&[0u8; 17], b"z")
            .is_err()
    );
    // Valid inputs still pass.
    t.check_insert::<Infallible>(b"k", b"v", false).unwrap();
    t.check_insert_range_del::<Infallible>(b"a", b"z").unwrap();
}

/// Kills `replace MemTable::insert_ttl -> Result with Ok(())`
/// (src/memtable.rs:444): a TTL insert must actually store the entry.
/// The feature suite never inserts via `insert_ttl` directly.
#[test]
fn mut_memtable_insert_ttl_stores_entry() {
    let mut t = MemTable::<8, 256, 16, 32>::new();
    t.insert_ttl::<Infallible>(b"k", b"v", 1, 999).unwrap();
    let e = t.get(b"k").unwrap();
    assert_eq!(e.val, b"v");
    assert_eq!(e.expire_at, 999);
}

/// Kills `replace MemTable::arena_len -> usize with 0` and `... with 1`:
/// `Db::write` relies on `arena_len()` for its up-front whole-batch arena
/// check ("the table is unchanged since the capacity check, so this cannot
/// fail"). With the check weakened, an overflowing batch commits to the
/// WAL, applies a prefix of its ops, then fails — breaking batch
/// atomicity. Correct code rejects the batch before touching anything.
#[test]
fn mut_db_write_arena_check_is_atomic() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    // Fill the arena to 4016 of 4096 bytes (8 puts x 502 bytes).
    for i in 0..8u8 {
        let key = [b'f', i];
        block_on(db.put(&key, &vec![i; 500])).unwrap();
    }
    // 22 + 102 = 124 bytes: fits the weakened check, overflows the real
    // arena after the first op lands (4016 + 22 + 102 > 4096).
    let mut b = WriteBatch::<256, 1024, 2>::new();
    b.put(b"a1", &[0u8; 20]).unwrap();
    b.put(b"b2", &[1u8; 100]).unwrap();
    assert!(matches!(block_on(db.write(&b)), Err(Error::ArenaFull)));
    assert_eq!(get(&db, b"a1"), None, "batch was not atomic");
    assert_eq!(get(&db, b"b2"), None, "batch was not atomic");
}

/// Kills the `slot_len`, `slot_view`, and `lower_bound` mutants
/// (src/memtable.rs:579,585,591,592,604): the memtable scan path must see
/// every resident entry. The feature suite only scans flushed (sstable)
/// data, so the memtable scan path was uncovered. Data stays in the
/// memtable (no flush): `slot_len -> 0/1` truncates the scan, `slot_view
/// -> None` empties it, `lower_bound -> 0/1` mispositions the seek.
#[test]
fn mut_memtable_scan_sees_resident_entries() {
    type TestScan<'d> = Scan<'d, MemDevice<4096>, 4096, 256, 1024, 64, 4096, 7, 4, 1024, 8>;
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    block_on(db.put(b"a", b"1")).unwrap();
    block_on(db.put(b"b", b"2")).unwrap();
    block_on(db.put(b"c", b"3")).unwrap();
    let mut scan = TestScan::new(&db);
    block_on(scan.seek(b"", None, u64::MAX)).unwrap();
    let mut kbuf = [0u8; 256];
    let mut vbuf = [0u8; 1024];
    let mut out = Vec::new();
    while let Some((klen, _)) = block_on(scan.next(&mut kbuf, &mut vbuf)).unwrap() {
        out.push(kbuf[..klen].to_vec());
    }
    assert_eq!(
        out,
        vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
        "memtable scan missed resident entries"
    );
    // Seek into the middle: lower_bound must position correctly.
    let mut scan = TestScan::new(&db);
    block_on(scan.seek(b"b", None, u64::MAX)).unwrap();
    let mut out = Vec::new();
    while let Some((klen, _)) = block_on(scan.next(&mut kbuf, &mut vbuf)).unwrap() {
        out.push(kbuf[..klen].to_vec());
    }
    assert_eq!(
        out,
        vec![b"b".to_vec(), b"c".to_vec()],
        "memtable scan seek mispositioned"
    );
}
// src/compact.rs
// ---------------------------------------------------------------------------

/// Kills `replace Compaction::reset with ()` (src/compact.rs:282): the
/// scratch is documented as reusable across jobs — a finished job leaves it
/// idle and the next `compact_step` selects a fresh job. With `reset` as a
/// no-op the second job skips selection on the stale `State::Merging` and
/// merges exhausted cursors, corrupting the manifest. The existing suite
/// always uses a fresh scratch per job (`drive_one`), so nothing else
/// covers this.
#[test]
fn mut_compact_scratch_reusable_across_jobs() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    let mut c = TestCompaction::new();
    for round in 0..2u8 {
        // Fill L0 (TABLES = 4): one flush per table.
        for t in 0..4u8 {
            let base = round * 4 + t;
            for i in 0..8u8 {
                let key = [b'k', base, i];
                block_on(db.put(&key, &[base, i])).unwrap();
            }
            block_on(db.flush()).unwrap();
        }
        // Drive exactly one job to completion with the SAME scratch.
        loop {
            match block_on(db.compact_step(&mut c)) {
                Ok(Progress::More) => {}
                Ok(Progress::Done) => break,
                Err(e) => panic!("round {round}: unexpected compaction error: {e:?}"),
            }
        }
        // The job must actually have drained L0: with `reset` as a no-op
        // the second round's step skips selection on the stale
        // `State::Merging`, "commits" the exhausted cursors, and leaves
        // the fresh L0 tables behind while reporting `Done`.
        assert!(
            !db.compaction_pending(),
            "round {round}: L0 still full after the job"
        );
    }
    // Both jobs' data must be intact and visible.
    for round in 0..2u8 {
        for t in 0..4u8 {
            let base = round * 4 + t;
            for i in 0..8u8 {
                let key = [b'k', base, i];
                assert_eq!(get(&db, &key), Some(vec![base, i]), "lost key {key:?}");
            }
        }
    }
}

/// Kills `replace || with && in merge_step` (src/compact.rs:368): the
/// per-key restart must fire when the key LENGTH changes, even when the
/// shared prefix bytes happen to match the stale tail of the key buffer.
/// Merge order here is `wy < x < xy < z`: after `x`, the buffer still
/// holds `wy`'s `y` at index 1, so with `&&` the mutant sees `xy` as a
/// continuation of `x`, never restarts `served`, and drops `xy`'s version
/// (the test observes `None`; correct code yields `Some("3")`).
#[test]
fn mut_compact_prefix_key_restarts_per_key_state() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    for (k, v) in [
        (b"wy".as_slice(), b"1".as_slice()),
        (b"x", b"2"),
        (b"xy", b"3"),
        (b"z", b"4"),
    ] {
        block_on(db.put(k, v)).unwrap();
        block_on(db.flush()).unwrap();
    }
    let mut c = TestCompaction::new();
    loop {
        match block_on(db.compact_step(&mut c)) {
            Ok(Progress::More) => {}
            Ok(Progress::Done) => break,
            Err(e) => panic!("unexpected compaction error: {e:?}"),
        }
    }
    assert_eq!(get(&db, b"wy"), Some(b"1".to_vec()));
    assert_eq!(get(&db, b"x"), Some(b"2".to_vec()));
    assert_eq!(
        get(&db, b"xy"),
        Some(b"3".to_vec()),
        "prefix key xy lost by the merge"
    );
    assert_eq!(get(&db, b"z"), Some(b"4".to_vec()));
}

/// Kills `replace <= with > in Compaction::merge_step`
/// (src/compact.rs:385): the bottommost-tombstone-drop decision must treat
/// a TTL value as a tombstone exactly when it is EXPIRED
/// (`expire_at <= purge_before`). The mutant inverts the test, so a
/// not-yet-expired value is dropped as if it were a tombstone — silent
/// data loss. The feature suite never compacts a live (unexpired) TTL
/// value through a bottommost compaction.
#[test]
fn mut_compact_ttl_unexpired_survives_bottommost() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    // Four tables in L0 to trigger compaction; TTL expires at 300 while
    // the purge cutoff is 200, so the values are NOT expired.
    for i in 0..4u8 {
        let k = [b'k', i];
        block_on(db.put_with_ttl(&k, b"v", 300)).unwrap();
        block_on(db.flush()).unwrap();
    }
    let mut c = TestCompaction::new();
    c.purge_before = 200;
    loop {
        match block_on(db.compact_step(&mut c)) {
            Ok(Progress::More) => {}
            Ok(Progress::Done) => break,
            Err(e) => panic!("unexpected compaction error: {e:?}"),
        }
    }
    // Still live at time 200: must survive bottommost compaction.
    for i in 0..4u8 {
        let k = [b'k', i];
        assert_eq!(
            get_now(&db, &k, 200),
            Some(b"v".to_vec()),
            "unexpired TTL value lost by compaction"
        );
    }
}

/// Kills `delete ! in Compaction::merge_step` (src/compact.rs:406):
/// the TTL purge must convert an expired value into a tombstone
/// (`!tombstone && expire_at != 0 && expire_at <= purge_before`). The
/// mutant disables the purge for values, so an expired value survives
/// compaction as a live value. A snapshot pins the pre-compaction seq so
/// the bottommost drop cannot hide the difference; the read is at a time
/// before expiry, where the purged tombstone (clean) hides the key but
/// the unpurged value (mutant) is still visible.
#[test]
fn mut_compact_ttl_purge_converts_expired_value() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    let k0 = [b'k', 0];
    // Expires at 100; the purge cutoff is 200, the read time is 50.
    block_on(db.put_with_ttl(&k0, b"v", 100)).unwrap();
    let snap = db.snapshot().unwrap();
    block_on(db.flush()).unwrap();
    for i in 1..4u8 {
        let k = [b'k', i];
        block_on(db.put_with_ttl(&k, b"v", 100)).unwrap();
        block_on(db.flush()).unwrap();
    }
    let mut c = TestCompaction::new();
    c.purge_before = 200;
    loop {
        match block_on(db.compact_step(&mut c)) {
            Ok(Progress::More) => {}
            Ok(Progress::Done) => break,
            Err(e) => panic!("unexpected compaction error: {e:?}"),
        }
    }
    // The expired value was purged to a tombstone: not visible even
    // before its expiry time, because the tombstone won the merge.
    let mut buf = [0u8; 2048];
    let r = block_on(db.get_at_with_time(&k0, &mut buf, snap, 50)).unwrap();
    assert_eq!(r, None, "expired value was not purged to a tombstone");
    db.release_snapshot(snap);
}

// ---------------------------------------------------------------------------
// src/wal.rs
// ---------------------------------------------------------------------------

/// Kills `src/wal.rs:174 replace < with == in decode_record`: a torn tail
/// shorter than `WAL_HEADER_LEN` at the end of a WAL block must stop
/// recovery cleanly. The weakened guard (`==`) walks past the length check
/// into the header indexing and panics out of bounds; the real guard
/// returns `None` (torn tail) for any short buffer.
#[test]
fn mut_wal_short_torn_tail_stops_cleanly() {
    use horton::wal::{Op, WalWriter};

    const BLOCK: usize = 512;
    // A 1-byte key + 2-byte value put encodes to 23 + 1 + 2 = 26 bytes, so
    // 19 records fill 494 bytes, leaving an 18-byte tail (< 19).
    let mut w: WalWriter<_, BLOCK> = WalWriter::new(MemDevice::<BLOCK>::new(), 0, 16);
    for i in 0..19u8 {
        block_on(w.append(u64::from(i) + 1, Op::Put, &[i], &[i, i])).unwrap();
    }
    block_on(w.commit()).unwrap();
    // Craft a torn tail: valid magic + tiny length so the weakened guard
    // walks into the header reads, then a valid op so it reaches the
    // key/value length indexing — past the end of the 18-byte tail.
    let tail: [u8; 18] = [
        0x73, 0x6C, // WAL_MAGIC ("ls")
        0x05, 0x00, 0x00, 0x00, // len = 5
        0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, // seq filler
        0x01, // op = Put
        0x00, 0x00, // key_len = 0
        0x00, // val_len low byte; the high byte is past the end
    ];
    let mut dev = w.into_device();
    dev.blocks_mut()[0][494..512].copy_from_slice(&tail);
    let mut w2: WalWriter<_, BLOCK> = WalWriter::new(dev, 0, 16);
    let mut t = MemTable::<32, 2048, 16, 32>::new();
    let st = block_on(w2.recover(&mut t)).unwrap();
    // The clean prefix replayed in full; the torn tail stopped the scan
    // without a panic and without an error.
    assert_eq!(st.records, 19);
}

/// Kills `src/wal.rs:301 replace staged_bytes -> usize with 0`: the
/// getter must report the bytes staged in RAM. `Db::write` drains a
/// non-empty stage before batching (`db.rs:686`), so a lying getter would
/// silently drop staged records from the atomicity protocol.
#[test]
fn mut_wal_staged_bytes_tracks_stage() {
    use horton::wal::{Op, WalWriter};

    const BLOCK: usize = 512;
    let mut w: WalWriter<_, BLOCK> = WalWriter::new(MemDevice::<BLOCK>::new(), 0, 16);
    assert_eq!(w.staged_bytes(), 0);
    block_on(w.append(1, Op::Put, b"k", b"v")).unwrap();
    // 23 overhead + 1 key + 1 value = 25 bytes staged, not yet durable.
    assert_eq!(w.staged_bytes(), 25);
    block_on(w.commit()).unwrap();
    assert_eq!(w.staged_bytes(), 0);
}

/// Kills `src/wal.rs:330 replace max_seq -> u64 with 0` and `with 1`:
/// the getter must track the highest sequence appended so far.
#[test]
fn mut_wal_max_seq_tracks_appends() {
    use horton::wal::{Op, WalWriter};

    const BLOCK: usize = 512;
    let mut w: WalWriter<_, BLOCK> = WalWriter::new(MemDevice::<BLOCK>::new(), 0, 16);
    assert_eq!(w.max_seq(), 0);
    block_on(w.append(5, Op::Put, b"k", b"v")).unwrap();
    assert_eq!(w.max_seq(), 5);
    block_on(w.append(3, Op::Put, b"k", b"v")).unwrap();
    assert_eq!(w.max_seq(), 5);
    block_on(w.append(7, Op::Put, b"k", b"v")).unwrap();
    assert_eq!(w.max_seq(), 7);
}

/// Kills `src/wal.rs:398 replace > with >= in append_inner`: a record
/// that exactly fills the staging block must pack into it, not trigger
/// a premature flush of the (empty) stage and waste a WAL block.
#[test]
fn mut_wal_exact_fit_packs_block() {
    use horton::wal::{Op, WalWriter};

    const BLOCK: usize = 512;
    let mut w: WalWriter<_, BLOCK> = WalWriter::new(MemDevice::<BLOCK>::new(), 0, 16);
    // 23 overhead + 1 key + 488 value = 512: exactly one block.
    let v = [0xAA; 488];
    block_on(w.append(1, Op::Put, b"k", &v)).unwrap();
    assert_eq!(w.next_block(), 0, "exact-fit record must pack, not flush");
    assert_eq!(w.staged_bytes(), 512);
    block_on(w.commit()).unwrap();
    assert_eq!(w.next_block(), 1);
}

/// Kills `src/wal.rs:549 replace > with >= in recover_from`: a record
/// with `seq == seq_floor` is stale (already flushed into a table) and
/// must be skipped, not replayed. Replaying the boundary would resurrect
/// superseded versions into the memtable.
#[test]
fn mut_wal_seq_floor_boundary_skips() {
    use horton::wal::{Op, WalWriter};

    const BLOCK: usize = 512;
    let mut w: WalWriter<_, BLOCK> = WalWriter::new(MemDevice::<BLOCK>::new(), 0, 16);
    block_on(w.append(1, Op::Put, b"a", b"1")).unwrap();
    block_on(w.append(2, Op::Put, b"b", b"2")).unwrap();
    block_on(w.commit()).unwrap();
    let dev = w.into_device();
    let mut w2: WalWriter<_, BLOCK> = WalWriter::new(dev, 0, 16);
    let mut t = MemTable::<32, 2048, 16, 32>::new();
    // seq_floor = 1: the seq-1 record is stale, the seq-2 record is live.
    let st = block_on(w2.recover_from(&mut t, 0, 1)).unwrap();
    assert_eq!(st.records, 1, "seq_floor boundary record must be skipped");
    assert_eq!(st.max_seq, 2);
}

// ---------------------------------------------------------------------------
// src/manifest.rs
// ---------------------------------------------------------------------------

/// Kills `src/manifest.rs:63 replace > with ==` and `> with >=` in
/// `KeyBound::from_slice`: a key of exactly `KEY_MAX` bytes is valid and
/// must be accepted; a longer key must be rejected, not panicked on.
#[test]
fn mut_manifest_keybound_from_slice_boundary() {
    use horton::KeyBound;

    let full = [0xAA; 8];
    let b = KeyBound::<8>::from_slice(&full);
    assert!(b.is_some(), "KEY_MAX-length key must be accepted");
    assert_eq!(b.unwrap().as_slice(), &full);
    assert!(
        KeyBound::<8>::from_slice(&[0xBB; 9]).is_none(),
        "overlong key must be rejected"
    );
    assert!(
        KeyBound::<8>::from_slice(&[0xCC; 3]).is_some(),
        "short key must be accepted"
    );
}

/// Kills `src/manifest.rs:90 replace < with ==` in `KeyBound::min`:
/// when the other bound is strictly smaller, `min` must return it.
#[test]
fn mut_manifest_keybound_min_picks_lesser() {
    use horton::KeyBound;

    let big = KeyBound::<8>::from_slice(b"bbbb").unwrap();
    let small = KeyBound::<8>::from_slice(b"aaaa").unwrap();
    assert_eq!(big.min(small), small);
    assert_eq!(small.min(big), small);
    // Equal bounds: either is the same key.
    assert_eq!(small.min(small).as_slice(), b"aaaa");
}

/// Kills `src/manifest.rs:229 replace next_table_id -> u32 with 0`:
/// the getter must track `bump_table_id`.
#[test]
fn mut_manifest_next_table_id_tracks_bumps() {
    use horton::Manifest;

    let mut m = Manifest::<2, 4, 32>::new();
    assert_eq!(m.next_table_id(), 0);
    m.bump_table_id::<Infallible>().unwrap();
    m.bump_table_id::<Infallible>().unwrap();
    assert_eq!(m.next_table_id(), 2);
}

/// Kills `src/manifest.rs:260 replace > with >=` in
/// `advance_next_table_id`: raising to the current value is a no-op; the
/// assignment must write the identical value back.
#[test]
fn mut_manifest_advance_next_table_id_noop_on_tie() {
    use horton::Manifest;

    let mut m = Manifest::<2, 4, 32>::new();
    m.bump_table_id::<Infallible>().unwrap();
    m.bump_table_id::<Infallible>().unwrap();
    assert_eq!(m.next_table_id(), 2);
    // Raising to the current counter value changes nothing.
    m.advance_next_table_id(2);
    assert_eq!(m.next_table_id(), 2);
    // Raising above advances; lowering never does.
    m.advance_next_table_id(9);
    assert_eq!(m.next_table_id(), 9);
    m.advance_next_table_id(4);
    assert_eq!(m.next_table_id(), 9);
}

/// Kills `src/manifest.rs:282 replace l0_is_full -> bool with false`:
/// level 0 holding `TABLES` tables must report full — flush and ingest
/// rely on this to fail fast with `NeedsCompaction` before doing I/O.
#[test]
fn mut_manifest_l0_is_full_reports_full() {
    use horton::{KeyBound, Manifest, TableRef};

    fn tref(id: u32) -> TableRef<32> {
        TableRef {
            id,
            first_block: u64::from(id) * 10,
            block_count: 4,
            first_key: KeyBound::from_slice(b"a").unwrap(),
            last_key: KeyBound::from_slice(b"z").unwrap(),
            max_seq: 10,
            min_seq: 0,
            entry_count: 5,
            rdel_blocks: 0,
        }
    }

    let mut m = Manifest::<2, 4, 32>::new();
    assert!(!m.l0_is_full());
    for i in 0..4u32 {
        m.add_l0_table::<Infallible>(tref(i)).unwrap();
    }
    assert!(m.l0_is_full(), "L0 with TABLES tables must report full");
}

/// Kills `src/manifest.rs:468 replace > with ==` in `Manifest::decode`:
/// a `total` past the block end must be rejected as corrupt — never read
/// out of bounds (the mutant turns the guard into a panic).
#[test]
fn mut_manifest_decode_rejects_oversized_total() {
    use horton::{MANIFEST_MAGIC, Manifest};

    const BLOCK: usize = 512;
    let mut buf = [0u8; BLOCK];
    buf[0..8].copy_from_slice(&MANIFEST_MAGIC.to_le_bytes());
    // payload_len chosen so total = 12 + len + 4 overflows the block.
    buf[8..12].copy_from_slice(&0xFFFF_FFFFu32.to_le_bytes());
    let res = Manifest::<2, 4, 32>::decode::<Infallible, BLOCK>(&buf);
    assert!(
        res.is_err(),
        "total > BLOCK must be CorruptManifest, never a panic"
    );
}

/// Boundary of the block length check in `check_block`: a manifest block
/// whose body and CRC fill it exactly (`24 + len + 4 == BLOCK`) is valid
/// and must decode — an off-by-one (`>=`) would reject it as corrupt.
#[test]
fn mut_manifest_decode_accepts_exact_fit() {
    use horton::crc::crc32;
    use horton::{MANIFEST_MAGIC, Manifest};

    fn w64(buf: &mut [u8], off: &mut usize, v: u64) {
        buf[*off..*off + 8].copy_from_slice(&v.to_le_bytes());
        *off += 8;
    }
    fn w32(buf: &mut [u8], off: &mut usize, v: u32) {
        buf[*off..*off + 4].copy_from_slice(&v.to_le_bytes());
        *off += 4;
    }
    fn w16(buf: &mut [u8], off: &mut usize, v: u16) {
        buf[*off..*off + 2].copy_from_slice(&v.to_le_bytes());
        *off += 2;
    }

    // Manifest<1, 1, 8> with one table whose bounds are 1-byte keys, as a
    // one-block copy: header 24 (magic, seq, index, count, len), body
    // 8 + 4 + 8 + 8 + 4 (fixed fields) + 4 (level count) + 46 (the ref)
    // = 82, total = 24 + 82 + 4 = 110.
    const BLOCK: usize = 110;
    const CRC_END: usize = 106;
    let mut buf = [0u8; BLOCK];
    buf[0..8].copy_from_slice(&MANIFEST_MAGIC.to_le_bytes());
    buf[8..16].copy_from_slice(&7u64.to_le_bytes()); // seq
    buf[16..18].copy_from_slice(&0u16.to_le_bytes()); // block index
    buf[18..20].copy_from_slice(&1u16.to_le_bytes()); // block count
    buf[20..24].copy_from_slice(&82u32.to_le_bytes()); // body bytes
    let mut off = 24;
    w64(&mut buf, &mut off, 0); // wal_head
    w32(&mut buf, &mut off, 0); // next_table_id
    w64(&mut buf, &mut off, 0); // flushed_seq
    w64(&mut buf, &mut off, 0); // seq_high
    w32(&mut buf, &mut off, 1); // nlevels
    w32(&mut buf, &mut off, 1); // level 0: one table
    w32(&mut buf, &mut off, 7); // id
    w64(&mut buf, &mut off, 100); // first_block
    w32(&mut buf, &mut off, 4); // block_count
    w16(&mut buf, &mut off, 1); // first_key len
    buf[off] = b'a';
    off += 1;
    w16(&mut buf, &mut off, 1); // last_key len
    buf[off] = b'z';
    off += 1;
    w64(&mut buf, &mut off, 10); // max_seq
    w64(&mut buf, &mut off, 3); // min_seq
    w32(&mut buf, &mut off, 5); // entry_count
    w32(&mut buf, &mut off, 0); // rdel_blocks
    assert_eq!(off, CRC_END);
    let crc = crc32(&buf[..CRC_END]);
    buf[CRC_END..CRC_END + 4].copy_from_slice(&crc.to_le_bytes());

    let m = Manifest::<1, 1, 8>::decode::<Infallible, BLOCK>(&buf)
        .expect("exact-fit manifest must decode");
    assert_eq!(m.seq(), 7);
    assert_eq!(m.l0().len(), 1);
}

/// Slot boundary: a table ending exactly at its slot's end fits; one block
/// more straddles into the next slot, which `open()` must treat as a
/// corrupt manifest rather than hand the neighbour's blocks out twice.
#[test]
fn mut_slot_of_end_boundary() {
    use horton::SlotMap;

    let m = SlotMap::layout(100, 140, 4, 1).expect("layout"); // 10 blocks each
    assert_eq!(m.slot_of(100, 10), Some(0), "ends exactly at the slot end");
    assert_eq!(m.slot_of(100, 11), None, "one block into slot 1");
    assert_eq!(m.slot_of(109, 1), Some(0));
    assert_eq!(m.slot_of(110, 1), Some(1));
}

/// Kills `src/manifest.rs:537 replace > with >=` in `decode_bound`:
/// a bound of exactly `KEY_MAX` bytes is legal (`from_slice` accepts it),
/// so a manifest carrying one must decode.
#[test]
fn mut_manifest_decode_bound_accepts_key_max() {
    use horton::{KeyBound, Manifest, TableRef};

    let mut m = Manifest::<2, 4, 8>::new();
    m.add_l0_table::<Infallible>(TableRef {
        id: 1,
        first_block: 50,
        block_count: 4,
        first_key: KeyBound::from_slice(&[0xAA; 8]).unwrap(),
        last_key: KeyBound::from_slice(&[0xBB; 8]).unwrap(),
        max_seq: 10,
        min_seq: 0,
        entry_count: 5,
        rdel_blocks: 0,
    })
    .unwrap();
    let mut buf = [0u8; 512];
    m.encode::<Infallible, 512>(&mut buf).unwrap();
    let back = Manifest::<2, 4, 8>::decode::<Infallible, 512>(&buf)
        .expect("KEY_MAX-length bound must decode");
    assert_eq!(back.l0().len(), 1);
}

/// Kills `src/manifest.rs:537 replace > with ==` in `decode_bound`:
/// a bound longer than `KEY_MAX` must be rejected as corrupt — never read
/// past the bound array (the mutant turns the guard into a panic).
#[test]
fn mut_manifest_decode_bound_rejects_overlong() {
    use horton::crc::crc32;
    use horton::{KeyBound, MANIFEST_MAGIC, Manifest, TableRef};

    let mut m = Manifest::<2, 4, 8>::new();
    m.add_l0_table::<Infallible>(TableRef {
        id: 1,
        first_block: 50,
        block_count: 4,
        first_key: KeyBound::from_slice(b"a").unwrap(),
        last_key: KeyBound::from_slice(b"z").unwrap(),
        max_seq: 10,
        min_seq: 0,
        entry_count: 5,
        rdel_blocks: 0,
    })
    .unwrap();
    let mut buf = [0u8; 512];
    m.encode::<Infallible, 512>(&mut buf).unwrap();
    // Patch first_key's u16 length to KEY_MAX + 1. Layout: header(12) +
    // seq(8) + wal_head(8) + next_table_id(4) + flushed_seq(8) +
    // seq_high(8) + nlevels(4) + count(4) + id(4) + first_block(8) +
    // block_count(4) = offset 72.
    assert_eq!(&buf[0..8], &MANIFEST_MAGIC.to_le_bytes());
    buf[72..74].copy_from_slice(&9u16.to_le_bytes());
    // Repair the CRC over the patched payload.
    let payload_len = u32::from_le_bytes(buf[8..12].try_into().unwrap()) as usize;
    let crc_end = 12 + payload_len;
    let crc = crc32(&buf[..crc_end]);
    buf[crc_end..crc_end + 4].copy_from_slice(&crc.to_le_bytes());

    let res = Manifest::<2, 4, 8>::decode::<Infallible, 512>(&buf);
    assert!(
        res.is_err(),
        "overlong bound must be CorruptManifest, never a panic"
    );
}

/// A manifest that fills its block exactly must encode as a one-block
/// copy — an off-by-one in the chunk arithmetic would report `ManifestFull` or
/// spill into a second block.
#[test]
fn mut_manifest_encode_accepts_exact_fit() {
    use horton::{KeyBound, Manifest, TableRef};

    // Same shape as `mut_manifest_decode_accepts_exact_fit`: total = 110.
    let mut m = Manifest::<1, 1, 8>::new();
    m.add_l0_table::<Infallible>(TableRef {
        id: 7,
        first_block: 100,
        block_count: 4,
        first_key: KeyBound::from_slice(b"a").unwrap(),
        last_key: KeyBound::from_slice(b"z").unwrap(),
        max_seq: 10,
        min_seq: 0,
        entry_count: 5,
        rdel_blocks: 0,
    })
    .unwrap();
    let mut buf = [0u8; 110];
    m.encode::<Infallible, 110>(&mut buf)
        .expect("exact-fit manifest must encode");
    assert_eq!(m.encoded_blocks::<110>(), 1, "exactly one block");
    assert_eq!(m.encoded_blocks::<109>(), 2, "one byte short spills over");
    // And it must round-trip through decode.
    let back = Manifest::<1, 1, 8>::decode::<Infallible, 110>(&buf)
        .expect("exact-fit manifest must decode");
    assert_eq!(back.seq(), 0);
    assert_eq!(back.l0().len(), 1);
}

/// A manifest larger than one block must fail the one-block `encode`
/// with `ManifestFull` — never write out of bounds (multi-block copies go
/// through `commit_to`).
#[test]
fn mut_manifest_encode_rejects_oversized() {
    use horton::{KeyBound, Manifest, TableRef};

    let mut m = Manifest::<2, 4, 256>::new();
    m.add_l0_table::<Infallible>(TableRef {
        id: 1,
        first_block: 50,
        block_count: 4,
        first_key: KeyBound::from_slice(&[0xAA; 256]).unwrap(),
        last_key: KeyBound::from_slice(&[0xBB; 256]).unwrap(),
        max_seq: 10,
        min_seq: 0,
        entry_count: 5,
        rdel_blocks: 0,
    })
    .unwrap();
    let mut buf = [0u8; 128];
    let res = m.encode::<Infallible, 128>(&mut buf);
    assert!(
        matches!(res, Err(horton::Error::ManifestFull)),
        "oversized manifest must be ManifestFull, never a panic"
    );
}
