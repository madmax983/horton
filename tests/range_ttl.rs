//! Range deletes + TTL (v0.15).
//!
//! RED: `delete_range`, `put_with_ttl`, `get_with_time`,
//! `get_at_with_time`, `seek_with_time`, `seek_prev_with_time`, and
//! `Compaction::purge_before` do not exist yet.

mod common;

use std::collections::BTreeMap;

use common::{Lcg, MemDevice, TestDb, block_on, test_config};
use horton::model::{VersionTtl, model_visible_value};
use horton::{Compaction, Progress, RevScan, Scan};

type TestScan<'d> = Scan<'d, MemDevice<4096>, 4096, 256, 1024, 64, 4096, 7, 4, 1024, 8>;
type TestRevScan<'d> = RevScan<'d, MemDevice<4096>, 4096, 256, 1024, 64, 4096, 7, 4, 1024, 8>;

fn open(db: &mut TestDb<MemDevice<4096>>) {
    block_on(db.open()).unwrap();
}

/// Drives compaction to completion with the given TTL purge cutoff.
fn drive_purge(db: &mut TestDb<MemDevice<4096>>, purge_before: u64) {
    let mut c = Compaction::<4096, 256, 1024, 1024>::new();
    c.purge_before = purge_before;
    while block_on(db.compact_step(&mut c)).unwrap() == Progress::More {}
}

fn flush(db: &mut TestDb<MemDevice<4096>>) {
    match block_on(db.flush()) {
        Ok(()) => {}
        Err(horton::Error::NoSpace) => {
            drive_purge(db, 0);
            block_on(db.flush()).unwrap();
        }
        Err(e) => panic!("unexpected flush error: {e:?}"),
    }
}

/// Runs one mutating op, flushing and retrying once when the WAL fills
/// between the sparse explicit flushes. Each mutation burns a whole WAL
/// block (durable before return), so a flush that lands with the WAL
/// nearly full can be followed by enough ops to exhaust it before the
/// next flush — the database reports that as `NoSpace`, the documented
/// caller-manages-space contract (see `flush` above). A failed mutation
/// consumes no sequence number and leaves no WAL or memtable trace, so
/// the retry takes exactly the sequence the model expects; a second
/// `NoSpace` still panics.
fn mutate(
    db: &mut TestDb<MemDevice<4096>>,
    mut op: impl FnMut(
        &mut TestDb<MemDevice<4096>>,
    ) -> Result<u64, horton::Error<core::convert::Infallible>>,
) -> u64 {
    match op(db) {
        Ok(seq) => seq,
        Err(horton::Error::NoSpace) => {
            flush(db);
            op(db).unwrap()
        }
        Err(e) => panic!("unexpected mutation error: {e:?}"),
    }
}

fn get(db: &TestDb<MemDevice<4096>>, key: &[u8]) -> Option<Vec<u8>> {
    let mut buf = [0u8; 1024];
    block_on(db.get(key, &mut buf))
        .unwrap()
        .map(|n| buf[..n].to_vec())
}

fn get_now(db: &TestDb<MemDevice<4096>>, key: &[u8], now: u64) -> Option<Vec<u8>> {
    let mut buf = [0u8; 1024];
    block_on(db.get_with_time(key, &mut buf, now))
        .unwrap()
        .map(|n| buf[..n].to_vec())
}

fn put(db: &mut TestDb<MemDevice<4096>>, k: &[u8], v: &[u8]) {
    block_on(db.put(k, v)).unwrap();
}

#[test]
fn range_delete_shadows_keys_exclusive_end() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    for k in [b"a", b"b", b"c", b"d"] {
        put(&mut db, k, k);
    }
    block_on(db.delete_range(b"b", b"d")).unwrap();
    assert_eq!(get(&db, b"a"), Some(b"a".to_vec()));
    assert_eq!(get(&db, b"b"), None);
    assert_eq!(get(&db, b"c"), None);
    // Exclusive end: "d" itself is untouched.
    assert_eq!(get(&db, b"d"), Some(b"d".to_vec()));
}

#[test]
fn range_delete_empty_range_is_noop() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    put(&mut db, b"b", b"v");
    // Empty and inverted ranges are no-ops, not errors.
    block_on(db.delete_range(b"b", b"b")).unwrap();
    block_on(db.delete_range(b"d", b"b")).unwrap();
    assert_eq!(get(&db, b"b"), Some(b"v".to_vec()));
}

#[test]
fn range_delete_newer_put_wins() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    put(&mut db, b"k", b"old");
    block_on(db.delete_range(b"a", b"z")).unwrap();
    assert_eq!(get(&db, b"k"), None);
    // A newer put resurrects the key.
    put(&mut db, b"k", b"new");
    assert_eq!(get(&db, b"k"), Some(b"new".to_vec()));
}

#[test]
fn range_delete_snapshot_isolation() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    put(&mut db, b"k", b"v");
    let snap = db.snapshot().unwrap();
    block_on(db.delete_range(b"a", b"z")).unwrap();
    // Live view: shadowed. Snapshot view: still visible.
    assert_eq!(get(&db, b"k"), None);
    let mut buf = [0u8; 1024];
    assert!(block_on(db.get_at(b"k", &mut buf, snap)).unwrap().is_some());
    assert_eq!(&buf[..1], b"v");
}

#[test]
fn range_delete_survives_flush_and_compaction() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    for k in [b"a", b"b", b"c"] {
        put(&mut db, k, k);
    }
    block_on(db.delete_range(b"a", b"c")).unwrap();
    flush(&mut db);
    assert_eq!(get(&db, b"a"), None);
    assert_eq!(get(&db, b"b"), None);
    assert_eq!(get(&db, b"c"), Some(b"c".to_vec()));
    drive_purge(&mut db, 0);
    assert_eq!(get(&db, b"a"), None);
    assert_eq!(get(&db, b"b"), None);
    assert_eq!(get(&db, b"c"), Some(b"c".to_vec()));
}

#[test]
fn range_delete_scan_skips_covered_keys() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    for k in [b"a", b"b", b"c", b"d"] {
        put(&mut db, k, k);
    }
    block_on(db.delete_range(b"b", b"d")).unwrap();
    flush(&mut db);
    let mut scan = TestScan::new(&db);
    block_on(scan.seek(b"", None, u64::MAX)).unwrap();
    let mut kbuf = [0u8; 256];
    let mut vbuf = [0u8; 1024];
    let mut out = Vec::new();
    while let Some((klen, _)) = block_on(scan.next(&mut kbuf, &mut vbuf)).unwrap() {
        out.push(kbuf[..klen].to_vec());
    }
    assert_eq!(out, vec![b"a".to_vec(), b"d".to_vec()]);
}

#[test]
fn range_delete_revscan_skips_covered_keys() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    for k in [b"a", b"b", b"c", b"d"] {
        put(&mut db, k, k);
    }
    block_on(db.delete_range(b"b", b"d")).unwrap();
    flush(&mut db);
    let mut scan = TestRevScan::new(&db);
    block_on(scan.seek_prev(b"", None, u64::MAX)).unwrap();
    let mut kbuf = [0u8; 256];
    let mut vbuf = [0u8; 1024];
    let mut out = Vec::new();
    while let Some((klen, _)) = block_on(scan.prev(&mut kbuf, &mut vbuf)).unwrap() {
        out.push(kbuf[..klen].to_vec());
    }
    assert_eq!(out, vec![b"d".to_vec(), b"a".to_vec()]);
}

#[test]
fn range_delete_recovery_without_flush() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    put(&mut db, b"k", b"v");
    block_on(db.delete_range(b"a", b"z")).unwrap();
    // Drop without flushing: the range tombstone lives only in the WAL.
    let dev = db.into_device();
    let mut db2 = TestDb::new(dev, test_config());
    open(&mut db2);
    assert_eq!(get(&db2, b"k"), None);
    // And a newer put after recovery still wins.
    put(&mut db2, b"k", b"v2");
    assert_eq!(get(&db2, b"k"), Some(b"v2".to_vec()));
}

#[test]
fn ttl_recovery_without_flush() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    block_on(db.put_with_ttl(b"k", b"v", 100)).unwrap();
    let dev = db.into_device();
    let mut db2 = TestDb::new(dev, test_config());
    open(&mut db2);
    assert_eq!(get_now(&db2, b"k", 50), Some(b"v".to_vec()));
    assert_eq!(get_now(&db2, b"k", 100), None);
}

#[test]
fn ttl_suppresses_value_at_and_after_expiry() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    block_on(db.put_with_ttl(b"k", b"v", 100)).unwrap();
    assert_eq!(get_now(&db, b"k", 0), Some(b"v".to_vec()));
    assert_eq!(get_now(&db, b"k", 99), Some(b"v".to_vec()));
    assert_eq!(get_now(&db, b"k", 100), None);
    assert_eq!(get_now(&db, b"k", 10_000), None);
    // Plain get (now = 0) still sees it.
    assert_eq!(get(&db, b"k"), Some(b"v".to_vec()));
}

#[test]
fn ttl_zero_never_expires() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    block_on(db.put_with_ttl(b"k", b"v", 0)).unwrap();
    assert_eq!(get_now(&db, b"k", u64::MAX), Some(b"v".to_vec()));
}

#[test]
fn ttl_scan_skips_expired() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    block_on(db.put_with_ttl(b"a", b"1", 100)).unwrap();
    put(&mut db, b"b", b"2");
    flush(&mut db);
    let mut scan = TestScan::new(&db);
    block_on(scan.seek_with_time(b"", None, u64::MAX, 200)).unwrap();
    let mut kbuf = [0u8; 256];
    let mut vbuf = [0u8; 1024];
    let mut out = Vec::new();
    while let Some((klen, _)) = block_on(scan.next(&mut kbuf, &mut vbuf)).unwrap() {
        out.push(kbuf[..klen].to_vec());
    }
    assert_eq!(out, vec![b"b".to_vec()]);
    // Before expiry both are visible.
    let mut scan = TestScan::new(&db);
    block_on(scan.seek_with_time(b"", None, u64::MAX, 50)).unwrap();
    let mut out = Vec::new();
    while let Some((klen, _)) = block_on(scan.next(&mut kbuf, &mut vbuf)).unwrap() {
        out.push(kbuf[..klen].to_vec());
    }
    assert_eq!(out, vec![b"a".to_vec(), b"b".to_vec()]);
}

#[test]
fn ttl_compaction_converts_expired_to_tombstone() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    put(&mut db, b"k", b"v1");
    let snap = db.snapshot().unwrap();
    block_on(db.put_with_ttl(b"k", b"v2", 100)).unwrap();
    flush(&mut db);
    // Purge everything expiring at or before 200.
    drive_purge(&mut db, 200);
    // Live view: absent. Snapshot from before the TTL put: v1 intact.
    assert_eq!(get_now(&db, b"k", 200), None);
    let mut buf = [0u8; 1024];
    assert!(
        block_on(db.get_at_with_time(b"k", &mut buf, snap, 200))
            .unwrap()
            .is_some()
    );
    assert_eq!(&buf[..2], b"v1");
}

#[test]
fn ttl_compaction_bottommost_drop_removes_key() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    block_on(db.put_with_ttl(b"k", b"v", 100)).unwrap();
    flush(&mut db);
    drive_purge(&mut db, 200);
    // No snapshots: the expired value became a bottommost tombstone and
    // dropped. A newer put still works afterwards.
    assert_eq!(get_now(&db, b"k", 200), None);
    put(&mut db, b"k", b"v3");
    assert_eq!(get_now(&db, b"k", 200), Some(b"v3".to_vec()));
}

#[test]
fn range_delete_differential_against_model() {
    let mut rng = Lcg::new(0x005e_ed15);
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    // Oracle state: per-key version list (newest last) + range tombstones.
    let mut versions: BTreeMap<Vec<u8>, Vec<VersionTtl>> = BTreeMap::new();
    let mut rdels: Vec<(Vec<u8>, Vec<u8>, u64)> = Vec::new();
    let mut seq = 0u64;
    let keys: Vec<Vec<u8>> = (0u8..8).map(|i| vec![b'a' + i]).collect();
    for _ in 0..300 {
        seq += 1;
        match rng.next() % 5 {
            0 => {
                // put
                let k = keys[rng.next_bounded(keys.len())].clone();
                let ttl = if rng.next().is_multiple_of(3) {
                    50 + rng.next() % 200
                } else {
                    0
                };
                if ttl == 0 {
                    mutate(&mut db, |db| block_on(db.put(&k, b"v")));
                } else {
                    mutate(&mut db, |db| block_on(db.put_with_ttl(&k, b"v", ttl)));
                }
                versions.entry(k).or_default().push(VersionTtl {
                    seq,
                    tombstone: false,
                    expire_at: ttl,
                });
            }
            1 => {
                // delete
                let k = keys[rng.next_bounded(keys.len())].clone();
                mutate(&mut db, |db| block_on(db.delete(&k)));
                versions.entry(k).or_default().push(VersionTtl {
                    seq,
                    tombstone: true,
                    expire_at: 0,
                });
            }
            2 => {
                // range delete [lo, hi)
                let mut a = u8::try_from(rng.next() % 8).unwrap_or(0);
                let mut b = u8::try_from(rng.next() % 8).unwrap_or(0);
                if a > b {
                    core::mem::swap(&mut a, &mut b);
                }
                b += 1; // ensure hi > lo
                let (lo, hi) = (vec![b'a' + a], vec![b'a' + b]);
                mutate(&mut db, |db| block_on(db.delete_range(&lo, &hi)));
                rdels.push((lo, hi, seq));
            }
            3 => {
                flush(&mut db);
            }
            _ => {
                // point check against the model at a random view/time
                let k = keys[rng.next_bounded(keys.len())].clone();
                let max_seq = if rng.next().is_multiple_of(2) {
                    u64::MAX
                } else {
                    seq
                };
                let now = rng.next() % 400;
                let covering: Vec<u64> = rdels
                    .iter()
                    .filter(|(lo, hi, _)| *lo <= k && k < *hi)
                    .map(|(_, _, q)| *q)
                    .collect();
                let vs = versions.get(&k).map_or(&[][..], Vec::as_slice);
                let expect = model_visible_value(vs, &covering, max_seq, now);
                let mut buf = [0u8; 1024];
                let got = block_on(db.get_at_with_time(&k, &mut buf, max_seq, now)).unwrap();
                assert_eq!(
                    got.is_some(),
                    expect,
                    "key {k:?} max_seq {max_seq} now {now}"
                );
            }
        }
    }
}

#[test]
fn archive_refuses_range_tombstone_overlapping_deeper_table() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    // L1 table holding keys the range tombstone would shadow: four L0
    // flushes trip the compaction threshold, then the purge merges them
    // down a level.
    for _ in 0..4 {
        put(&mut db, b"m", b"v");
        flush(&mut db);
    }
    drive_purge(&mut db, 0); // L0 -> L1
    assert_eq!(db.level_tables(1).unwrap().len(), 1);
    // L0 table with a range tombstone covering "m".
    block_on(db.delete_range(b"a", b"z")).unwrap();
    put(&mut db, b"zz", b"v");
    flush(&mut db);
    assert_eq!(db.level_tables(0).unwrap().len(), 1);
    let tid = db.level_tables(0).unwrap()[0].id;
    let res = block_on(db.archive_commit(0, tid));
    assert!(
        matches!(res, Err(horton::Error::WouldResurrect { .. })),
        "overlapping range tombstone must refuse archival, got {res:?}"
    );
}

#[test]
fn range_delete_compaction_keeps_shadowed_tombstone_across_snapshot_gap() {
    // The older identical tombstone may only be dropped when the newer
    // shadowing tombstone is also at or below the oldest snapshot. A live
    // snapshot strictly between the two sequences still needs the older
    // one: dropping it would resurrect covered keys at that snapshot.
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    put(&mut db, b"k", b"v"); // seq 1
    flush(&mut db);
    block_on(db.delete_range(b"a", b"z")).unwrap(); // seq 2
    flush(&mut db);
    // Advance the sequence so the snapshot lands strictly above seq 2.
    for i in 0..8u8 {
        put(&mut db, &[b'x', i], b"v"); // seq 3..=10
    }
    flush(&mut db);
    let snap = db.snapshot().unwrap(); // seq 10
    block_on(db.delete_range(b"a", b"z")).unwrap(); // seq 11
    flush(&mut db);
    drive_purge(&mut db, 0); // L0 -> L1, bottommost; snapshot still live
    // Live view: the newer tombstone shadows.
    assert_eq!(get(&db, b"k"), None);
    // Snapshot@10 sits between the tombstone sequences: it must still see
    // k as deleted via tombstone@2.
    let mut buf = [0u8; 1024];
    assert!(
        block_on(db.get_at(b"k", &mut buf, snap)).unwrap().is_none(),
        "snapshot in the sequence gap must still see the older tombstone"
    );
}

#[test]
fn archive_accepts_disjoint_range_tombstone() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    for _ in 0..4 {
        put(&mut db, b"m", b"v");
        flush(&mut db);
    }
    drive_purge(&mut db, 0); // L0 -> L1 holds "m"
    assert_eq!(db.level_tables(1).unwrap().len(), 1);
    // Range tombstone over a disjoint range: provably shadows nothing.
    block_on(db.delete_range(b"a", b"c")).unwrap();
    put(&mut db, b"zz", b"v");
    flush(&mut db);
    assert_eq!(db.level_tables(0).unwrap().len(), 1);
    let tid = db.level_tables(0).unwrap()[0].id;
    assert!(block_on(db.archive_commit(0, tid)).unwrap());
}

#[test]
fn range_delete_kway_merge_across_four_l0_tables() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    for k in [b"a", b"b", b"c", b"d", b"e", b"f"] {
        put(&mut db, k, k);
    }
    flush(&mut db);
    // Four L0 tables with overlapping range tombstones: the purge merges
    // all four inputs' rdel sections through the k-way merge (ordering,
    // same-seq coalescing, cross-input duplicates).
    block_on(db.delete_range(b"a", b"d")).unwrap();
    flush(&mut db);
    block_on(db.delete_range(b"c", b"f")).unwrap();
    flush(&mut db);
    block_on(db.delete_range(b"b", b"e")).unwrap();
    flush(&mut db);
    block_on(db.delete_range(b"a", b"z")).unwrap();
    flush(&mut db);
    drive_purge(&mut db, 0); // L0 -> L1
    assert_eq!(db.level_tables(1).unwrap().len(), 1);
    // The newest [a,z) tombstone covers every seeded key.
    for k in [b"a", b"b", b"c", b"d", b"e", b"f"] {
        assert_eq!(get(&db, k), None, "key {k:?} should stay shadowed");
    }
    // A newer put resurrects through the merged tombstones.
    put(&mut db, b"c", b"new");
    flush(&mut db);
    assert_eq!(get(&db, b"c"), Some(b"new".to_vec()));
    assert_eq!(get(&db, b"d"), None);
}

#[test]
fn range_delete_compaction_keeps_older_overlap_for_snapshot() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    put(&mut db, b"k", b"v");
    flush(&mut db);
    block_on(db.delete_range(b"a", b"z")).unwrap();
    let snap = db.snapshot().unwrap();
    flush(&mut db);
    // A second, identical range delete at a newer sequence.
    block_on(db.delete_range(b"a", b"z")).unwrap();
    flush(&mut db);
    put(&mut db, b"q", b"w");
    flush(&mut db);
    drive_purge(&mut db, 0); // L0 -> L1, bottommost; snapshot still live
    assert_eq!(db.level_tables(1).unwrap().len(), 1);
    // Live view: the newer tombstone shadows.
    assert_eq!(get(&db, b"k"), None);
    // Snapshot view (watermark between the two tombstones): the older
    // tombstone must have survived the merge — keeping only the newest
    // identical range would wrongly resurrect "k" here.
    let mut buf = [0u8; 1024];
    assert!(
        block_on(db.get_at(b"k", &mut buf, snap)).unwrap().is_none(),
        "snapshot must still see the older range tombstone"
    );
}
