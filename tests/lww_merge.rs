//! Replica-side LWW merging: per-table origin (`node_id`, `seal_wall`)
//! and the version-based read merge.
//!
//! A version is `(node_id, seal_wall, seq)`. Same `node_id` compares
//! sequence numbers — bit-identical to the legacy highest-seq-wins rule;
//! different nodes compare `(seal_wall, node_id)`, so the merge is
//! deterministic across primaries without any clock on the device.
//! Unstamped tables are `(0, 0)`: every comparison is same-node, which is
//! exactly today's behavior.

mod common;

use common::{MemDevice, TestDb, block_on, noop_waker, test_config, tight_config};
use core::task::{Context, Poll};
use horton::{BlockDevice, Compaction, Error, Progress};

const BLOCK: usize = 4096;

type TestCompaction = Compaction<4096, 256, 1024, 1024>;
/// Two levels, so L0 -> L1 is already the bottommost merge: the
/// tombstone-drop rule is exercisable without draining six levels.
type TwoLevelDb<D> = horton::Db<D, 4096, 256, 1024, 64, 4096, 2, 4, 1024, 8>;

/// Reads one block through the poll interface (test devices are `Ready`).
fn read_block<D: BlockDevice>(dev: &D, id: u64) -> [u8; BLOCK] {
    let mut buf = [0u8; BLOCK];
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    match dev.poll_read_block(&mut cx, id, &mut buf) {
        Poll::Ready(Ok(())) => buf,
        Poll::Ready(Err(_)) | Poll::Pending => panic!("test device read failed"),
    }
}

fn write_block<D: BlockDevice>(dev: &mut D, id: u64, buf: &[u8; BLOCK]) {
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    match dev.poll_write_block(&mut cx, id, buf) {
        Poll::Ready(Ok(())) => (),
        Poll::Ready(Err(_)) | Poll::Pending => panic!("test device write failed"),
    }
}

fn get<D>(db: &TestDb<D>, key: &[u8]) -> Option<Vec<u8>>
where
    D: BlockDevice,
    D::Error: core::fmt::Debug,
{
    let mut buf = [0u8; 1024];
    block_on(db.get(key, &mut buf))
        .expect("get")
        .map(|n| buf[..n].to_vec())
}

fn get2<D>(db: &TwoLevelDb<D>, key: &[u8]) -> Option<Vec<u8>>
where
    D: BlockDevice,
    D::Error: core::fmt::Debug,
{
    let mut buf = [0u8; 1024];
    block_on(db.get(key, &mut buf))
        .expect("get")
        .map(|n| buf[..n].to_vec())
}

/// Flushes the memtable, stamps the new table, and returns its id. The
/// test DBs run with `node_id` 0, so flush writes `(0, 0)` and the stamp
/// claims the legacy table — the same path a sweeper takes for a table
/// sealed by a foreign primary.
fn flush_stamp<D>(db: &mut TestDb<D>, node: u32, wall: u64) -> u32
where
    D: BlockDevice,
    D::Error: core::fmt::Debug,
{
    let before: Vec<u32> = db
        .tables(0)
        .map(|ts| ts.iter().map(|t| t.id).collect())
        .unwrap_or_default();
    block_on(db.flush()).expect("flush");
    let id = db
        .tables(0)
        .expect("l0")
        .iter()
        .map(|t| t.id)
        .find(|id| !before.contains(id))
        .expect("new table");
    assert!(block_on(db.stamp_table(id, node, wall)).expect("stamp"));
    id
}

/// Drives compaction jobs until none is selectable.
fn drain<D>(db: &mut TwoLevelDb<D>)
where
    D: BlockDevice,
    D::Error: core::fmt::Debug,
{
    let mut scratch = TestCompaction::new();
    while db.compaction_pending() {
        while block_on(db.compact_step(&mut scratch)).expect("step") == Progress::More {}
    }
}

// ---------------------------------------------------------------------------
// Read merge: version-based winner selection
// ---------------------------------------------------------------------------

/// Cross-node: the higher seal wall wins even with the lower sequence
/// number. Node 7 sealed at wall 100 (seq 1); node 9 sealed at wall 50
/// (seq 2). Deterministic LWW says node 7.
#[test]
fn lww_cross_node_higher_wall_wins_despite_lower_seq() {
    let mut db = TestDb::new(MemDevice::<BLOCK>::new(), test_config());
    block_on(db.open()).unwrap();
    block_on(db.put(b"k", b"v7")).unwrap();
    flush_stamp(&mut db, 7, 100);
    block_on(db.put(b"k", b"v9")).unwrap();
    flush_stamp(&mut db, 9, 50);
    assert_eq!(get(&db, b"k"), Some(b"v7".to_vec()));
}

/// Mirror image: node 9's wall is newer, so its higher-seq value wins.
#[test]
fn lww_cross_node_newer_wall_wins() {
    let mut db = TestDb::new(MemDevice::<BLOCK>::new(), test_config());
    block_on(db.open()).unwrap();
    block_on(db.put(b"k", b"v7")).unwrap();
    flush_stamp(&mut db, 7, 50);
    block_on(db.put(b"k", b"v9")).unwrap();
    flush_stamp(&mut db, 9, 100);
    assert_eq!(get(&db, b"k"), Some(b"v9".to_vec()));
}

/// Same wall on both sides: the higher node_id breaks the tie,
/// deterministically.
#[test]
fn lww_node_id_breaks_wall_tie() {
    let mut db = TestDb::new(MemDevice::<BLOCK>::new(), test_config());
    block_on(db.open()).unwrap();
    block_on(db.put(b"k", b"v7")).unwrap();
    flush_stamp(&mut db, 7, 100);
    block_on(db.put(b"k", b"v9")).unwrap();
    flush_stamp(&mut db, 9, 100);
    assert_eq!(get(&db, b"k"), Some(b"v9".to_vec()));
}

/// Same node: sequence order decides, exactly the legacy rule.
#[test]
fn lww_same_node_seq_order() {
    let mut db = TestDb::new(MemDevice::<BLOCK>::new(), test_config());
    block_on(db.open()).unwrap();
    block_on(db.put(b"k", b"v1")).unwrap();
    flush_stamp(&mut db, 7, 100);
    block_on(db.put(b"k", b"v2")).unwrap();
    flush_stamp(&mut db, 7, 50);
    assert_eq!(get(&db, b"k"), Some(b"v2".to_vec()));
}

/// An unstamped `(0, 0)` table sorts below every stamped table: the
/// stamp is what confers recency across nodes.
#[test]
fn lww_unstamped_sorts_below_stamped() {
    let mut db = TestDb::new(MemDevice::<BLOCK>::new(), test_config());
    block_on(db.open()).unwrap();
    block_on(db.put(b"k", b"stamped")).unwrap();
    flush_stamp(&mut db, 7, 100);
    // A later, unstamped flush: higher seq, but wall 0.
    block_on(db.put(b"k", b"unstamped")).unwrap();
    block_on(db.flush()).unwrap();
    assert_eq!(get(&db, b"k"), Some(b"stamped".to_vec()));
}

/// A foreign tombstone with an older wall does not shadow a value sealed
/// later: (100, 7) beats (50, 9).
#[test]
fn lww_foreign_tombstone_shadowed_by_newer_wall() {
    let mut db = TestDb::new(MemDevice::<BLOCK>::new(), test_config());
    block_on(db.open()).unwrap();
    block_on(db.put(b"k", b"v")).unwrap();
    flush_stamp(&mut db, 7, 100);
    block_on(db.delete(b"k")).unwrap();
    flush_stamp(&mut db, 9, 50);
    assert_eq!(get(&db, b"k"), Some(b"v".to_vec()));
}

/// A foreign tombstone with a newer wall hides the older value: (200, 9)
/// beats (100, 7).
#[test]
fn lww_foreign_tombstone_wins_with_newer_wall() {
    let mut db = TestDb::new(MemDevice::<BLOCK>::new(), test_config());
    block_on(db.open()).unwrap();
    block_on(db.put(b"k", b"v")).unwrap();
    flush_stamp(&mut db, 7, 100);
    block_on(db.delete(b"k")).unwrap();
    flush_stamp(&mut db, 9, 200);
    assert_eq!(get(&db, b"k"), None);
}

// ---------------------------------------------------------------------------
// stamp_table
// ---------------------------------------------------------------------------

/// Re-stamping with identical values is idempotent; any conflicting
/// re-stamp is a caller bug (`StampConflict`).
#[test]
fn stamp_table_idempotent_and_conflict() {
    let mut db = TestDb::new(MemDevice::<BLOCK>::new(), test_config());
    block_on(db.open()).unwrap();
    block_on(db.put(b"k", b"v")).unwrap();
    let id = flush_stamp(&mut db, 7, 100);
    // Identical: Ok(true), no write.
    assert!(block_on(db.stamp_table(id, 7, 100)).unwrap());
    // Different wall, different node: conflict.
    assert!(matches!(
        block_on(db.stamp_table(id, 7, 101)),
        Err(Error::StampConflict { id: got }) if got == id
    ));
    assert!(matches!(
        block_on(db.stamp_table(id, 8, 100)),
        Err(Error::StampConflict { id: got }) if got == id
    ));
    // The failed stamps changed nothing.
    let t = db.tables(0).unwrap().iter().find(|t| t.id == id).unwrap();
    assert_eq!((t.node_id, t.seal_wall), (7, 100));
}

/// Stamping a missing table id is Ok(false): safe to retry after the
/// table was archived or compacted away.
#[test]
fn stamp_table_missing_is_ok_false() {
    let mut db = TestDb::new(MemDevice::<BLOCK>::new(), test_config());
    block_on(db.open()).unwrap();
    assert!(!block_on(db.stamp_table(4242, 7, 100)).unwrap());
}

/// A flush output carries the configured node id with wall 0, and the
/// seal log can fill the wall in afterwards (no conflict).
#[test]
fn flush_carries_config_node_id_and_wall_fill() {
    let mut db = TestDb::new(MemDevice::<BLOCK>::new(), test_config().with_node_id(7));
    block_on(db.open()).unwrap();
    block_on(db.put(b"k", b"v")).unwrap();
    block_on(db.flush()).unwrap();
    let t = db.tables(0).unwrap().first().unwrap();
    assert_eq!((t.node_id, t.seal_wall), (7, 0));
    // The seal log fills in the wall: allowed, not a conflict.
    assert!(block_on(db.stamp_table(t.id, 7, 100)).unwrap());
    let t = db.tables(0).unwrap().first().unwrap();
    assert_eq!((t.node_id, t.seal_wall), (7, 100));
}

// ---------------------------------------------------------------------------
// Ingest preserves origin
// ---------------------------------------------------------------------------

/// A sealed table's origin rides the ingest descriptor into the
/// destination manifest unchanged.
#[test]
fn ingest_preserves_origin() {
    let mut src = TestDb::new(MemDevice::<BLOCK>::new(), test_config());
    block_on(src.open()).unwrap();
    block_on(src.put(b"k", b"v")).unwrap();
    let id = flush_stamp(&mut src, 7, 100);

    // Stream the sealed blocks to a remote device, like an upload would.
    let plan = src.archive_plan(0, id).unwrap();
    let sealed = plan.sealed();
    assert_eq!((sealed.node_id, sealed.seal_wall), (7, 100));
    let mut remote = MemDevice::<BLOCK>::new();
    for i in 0..sealed.block_count {
        let buf = read_block(src.device(), plan.table.first_block + u64::from(i));
        write_block(&mut remote, u64::from(i), &buf);
    }

    let mut dst = TestDb::new(MemDevice::<BLOCK>::new(), test_config());
    block_on(dst.open()).unwrap();
    assert!(block_on(dst.ingest_table(&sealed, &remote, 0)).unwrap());
    let t = dst.tables(0).unwrap().first().unwrap();
    assert_eq!((t.node_id, t.seal_wall), (7, 100));
    // The ingested value reads back under the version merge.
    assert_eq!(get(&dst, b"k"), Some(b"v".to_vec()));
}

// ---------------------------------------------------------------------------
// Compaction tombstone-drop rule under the version merge
// ---------------------------------------------------------------------------

/// Builds the derived unsound-drop scenario: node 1 seals a point
/// tombstone for `victim` at wall `t_wall` (seq 33); node 2 seals an
/// overlapping table at wall `f_wall` holding a newer value for
/// `victim` (seq 34, so `F.min_seq = 34 > 33`). Four L0 tables force
/// the merge.
fn drop_scenario(t_wall: u64, f_wall: u64) -> TwoLevelDb<MemDevice<BLOCK>> {
    let mut db = TwoLevelDb::new(MemDevice::<BLOCK>::new(), tight_config());
    block_on(db.open()).unwrap();
    // P1: filler, node 1.
    for i in 0..32u8 {
        block_on(db.put(&[b'k', i], b"x")).unwrap();
    }
    block_on(db.flush()).unwrap();
    let p1 = db.tables(0).unwrap().iter().map(|t| t.id).max().unwrap();
    assert!(block_on(db.stamp_table(p1, 1, t_wall)).unwrap());
    // T1: the tombstone, seq 33, node 1.
    block_on(db.delete(b"victim")).unwrap();
    block_on(db.flush()).unwrap();
    let t1 = db.tables(0).unwrap().iter().map(|t| t.id).max().unwrap();
    assert!(block_on(db.stamp_table(t1, 1, t_wall)).unwrap());
    // F: node 2, min_seq 34, overlapping range (it holds `victim`).
    block_on(db.put(b"victim", b"v2")).unwrap();
    for i in 0..31u8 {
        block_on(db.put(&[b'q', i], b"y")).unwrap();
    }
    block_on(db.flush()).unwrap();
    let f = db.tables(0).unwrap().iter().map(|t| t.id).max().unwrap();
    assert!(block_on(db.stamp_table(f, 2, f_wall)).unwrap());
    // P2: filler, node 1.
    for i in 0..32u8 {
        block_on(db.put(&[b'z', i], b"w")).unwrap();
    }
    block_on(db.flush()).unwrap();
    let p2 = db.tables(0).unwrap().iter().map(|t| t.id).max().unwrap();
    assert!(block_on(db.stamp_table(p2, 1, t_wall)).unwrap());
    assert_eq!(db.tables(0).unwrap().len(), 4);
    db
}

/// The sharp edge: the tombstone (node 1, wall 100, seq 101) still hides
/// F's versions ((100, 1) > (50, 2)), so the bottommost merge must keep
/// it even though `33 < F.min_seq = 34` — the legacy min_seq-only rule
/// would drop it and resurrect `v2`.
#[test]
fn tombstone_survives_compaction_against_newer_foreign_wall() {
    let mut db = drop_scenario(100, 50);
    // Sanity: the tombstone wins the read before compaction.
    assert_eq!(get2(&db, b"victim"), None);
    drain(&mut db);
    // The tombstone survived: `victim` is still deleted. Had the merge
    // dropped it, F's `v2` would have resurrected.
    assert_eq!(get2(&db, b"victim"), None);
    // And the output kept the job's origin.
    let l1 = db.tables(1).unwrap();
    assert_eq!(l1.len(), 1);
    assert_eq!((l1[0].node_id, l1[0].seal_wall), (1, 100));
    // 32 + 32 filler entries plus the kept tombstone: the drop did not
    // happen.
    assert_eq!(l1[0].entry_count, 65);
}

/// Positive control: when the foreign table's wall is newer ((50, 1) <
/// (100, 2)), the tombstone hides nothing and the merge may drop it —
/// `v2` was already the merge winner, so the read is unchanged.
#[test]
fn tombstone_drops_against_older_foreign_wall() {
    let mut db = drop_scenario(50, 100);
    // Sanity: `v2` wins the read before compaction.
    assert_eq!(get2(&db, b"victim"), Some(b"v2".to_vec()));
    drain(&mut db);
    assert_eq!(get2(&db, b"victim"), Some(b"v2".to_vec()));
    // The tombstone was actually dropped: 64 entries, not 65.
    let l1 = db.tables(1).unwrap();
    assert_eq!(l1.len(), 1);
    assert_eq!(l1[0].entry_count, 64);
}
