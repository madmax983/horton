//! Scan diffs: merge-join two scans into the changes that turn one view
//! into the other — between two snapshots of one database, or between a
//! database and its replica.

mod common;

use std::collections::BTreeMap;

use common::{Lcg, MemDevice, TestDb, block_on, test_config};
use horton::diff::{Change, Diff};
use horton::{Compaction, Error, Progress, Scan};

type TestScan<'d> = Scan<'d, MemDevice<4096>, 4096, 256, 1024, 64, 4096, 7, 4, 1024, 8>;

fn fresh() -> TestDb<MemDevice<4096>> {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    block_on(db.open()).unwrap();
    db
}

fn drive(db: &mut TestDb<MemDevice<4096>>) {
    let mut c = Compaction::<4096, 256, 1024, 1024>::new();
    while block_on(db.compact_step(&mut c)).unwrap() == Progress::More {}
}

fn flush(db: &mut TestDb<MemDevice<4096>>) {
    match block_on(db.flush()) {
        Ok(()) => {}
        Err(Error::NeedsCompaction) => {
            drive(db);
            block_on(db.flush()).unwrap();
        }
        Err(e) => panic!("unexpected flush error: {e:?}"),
    }
}

fn put(db: &mut TestDb<MemDevice<4096>>, k: &[u8], v: &[u8]) {
    match block_on(db.put(k, v)) {
        Ok(_) => {}
        Err(Error::TableFull | Error::ArenaFull | Error::WalFull) => {
            flush(db);
            block_on(db.put(k, v)).unwrap();
        }
        Err(e) => panic!("unexpected put error: {e:?}"),
    }
}

fn del(db: &mut TestDb<MemDevice<4096>>, k: &[u8]) {
    match block_on(db.delete(k)) {
        Ok(_) => {}
        Err(Error::TableFull | Error::ArenaFull | Error::WalFull) => {
            flush(db);
            block_on(db.delete(k)).unwrap();
        }
        Err(e) => panic!("unexpected delete error: {e:?}"),
    }
}

type Row = (Change, Vec<u8>, Option<Vec<u8>>, Option<Vec<u8>>);

/// Collects every change from `old` (at `old_seq`) to `new` (at `new_seq`)
/// over `[start, end)`.
fn changes(
    old: &TestDb<MemDevice<4096>>,
    old_seq: u64,
    new: &TestDb<MemDevice<4096>>,
    new_seq: u64,
    start: &[u8],
    end: Option<&[u8]>,
) -> Vec<Row> {
    let mut a = TestScan::new(old);
    let mut b = TestScan::new(new);
    block_on(a.seek(start, end, old_seq)).unwrap();
    block_on(b.seek(start, end, new_seq)).unwrap();
    let mut diff = Diff::new(&mut a, &mut b);
    let mut out = Vec::new();
    while let Some(change) = block_on(diff.next()).unwrap() {
        out.push((
            change,
            diff.key().to_vec(),
            diff.old_value().map(<[u8]>::to_vec),
            diff.new_value().map(<[u8]>::to_vec),
        ));
    }
    // Exhausted stays exhausted.
    assert_eq!(block_on(diff.next()).unwrap(), None);
    out
}

fn key(i: usize) -> [u8; 6] {
    let mut k = *b"k00000";
    let mut n = i;
    for b in k[1..].iter_mut().rev() {
        *b = b'0' + u8::try_from(n % 10).unwrap();
        n /= 10;
    }
    k
}

fn row(c: Change, k: &[u8], old: Option<&[u8]>, new: Option<&[u8]>) -> Row {
    (
        c,
        k.to_vec(),
        old.map(<[u8]>::to_vec),
        new.map(<[u8]>::to_vec),
    )
}

#[test]
fn identical_views_have_no_changes() {
    let mut db = fresh();
    for i in 0..20 {
        put(&mut db, &key(i), b"v");
    }
    assert!(changes(&db, u64::MAX, &db, u64::MAX, b"", None).is_empty());
    let empty = fresh();
    assert!(changes(&empty, u64::MAX, &empty, u64::MAX, b"", None).is_empty());
}

#[test]
fn snapshot_diff_reports_added_removed_and_changed() {
    let mut db = fresh();
    put(&mut db, b"a", b"1");
    put(&mut db, b"b", b"2");
    put(&mut db, b"c", b"3");
    let snap = db.snapshot().unwrap();
    del(&mut db, b"a");
    put(&mut db, b"b", b"two");
    put(&mut db, b"c", b"3"); // rewritten, same value: not a change
    put(&mut db, b"d", b"4");

    assert_eq!(
        changes(&db, snap, &db, u64::MAX, b"", None),
        vec![
            row(Change::Removed, b"a", Some(b"1"), None),
            row(Change::Changed, b"b", Some(b"2"), Some(b"two")),
            row(Change::Added, b"d", None, Some(b"4")),
        ]
    );
    // Reversed, every change inverts.
    assert_eq!(
        changes(&db, u64::MAX, &db, snap, b"", None),
        vec![
            row(Change::Added, b"a", None, Some(b"1")),
            row(Change::Changed, b"b", Some(b"two"), Some(b"2")),
            row(Change::Removed, b"d", Some(b"4"), None),
        ]
    );
}

#[test]
fn snapshot_diff_survives_flush_and_compaction() {
    let mut db = fresh();
    for i in 0..50 {
        put(&mut db, &key(i), b"v1");
    }
    let snap = db.snapshot().unwrap();
    for i in (0..50).step_by(10) {
        put(&mut db, &key(i), b"v2");
    }
    flush(&mut db);
    drive(&mut db);
    let got = changes(&db, snap, &db, u64::MAX, b"", None);
    let want: Vec<Row> = (0..50)
        .step_by(10)
        .map(|i| row(Change::Changed, &key(i), Some(b"v1"), Some(b"v2")))
        .collect();
    assert_eq!(got, want);
}

#[test]
fn replica_diff_names_exactly_what_diverged() {
    let mut a = fresh();
    let mut b = fresh();
    for i in 0..100 {
        put(&mut a, &key(i), &key(i));
        put(&mut b, &key(i), &key(i));
    }
    // Different layouts, mostly the same content.
    flush(&mut b);
    put(&mut b, &key(62), b"diverged");
    del(&mut b, &key(63));
    put(&mut b, b"zz", b"extra");

    assert_eq!(
        changes(&a, u64::MAX, &b, u64::MAX, b"", None),
        vec![
            row(Change::Changed, &key(62), Some(&key(62)), Some(b"diverged")),
            row(Change::Removed, &key(63), Some(&key(63)), None),
            row(Change::Added, b"zz", None, Some(b"extra")),
        ]
    );
    // Range-bounded: only the bucket asked about.
    assert_eq!(
        changes(&a, u64::MAX, &b, u64::MAX, &key(50), Some(&key(63))),
        vec![row(
            Change::Changed,
            &key(62),
            Some(&key(62)),
            Some(b"diverged")
        )]
    );
}

#[test]
fn one_side_empty_is_all_adds_or_all_removes() {
    let mut db = fresh();
    for i in 0..5 {
        put(&mut db, &key(i), b"v");
    }
    let empty = fresh();
    let adds = changes(&empty, u64::MAX, &db, u64::MAX, b"", None);
    assert_eq!(adds.len(), 5);
    assert!(adds.iter().all(|r| r.0 == Change::Added && r.2.is_none()));
    let removes = changes(&db, u64::MAX, &empty, u64::MAX, b"", None);
    assert_eq!(removes.len(), 5);
    assert!(
        removes
            .iter()
            .all(|r| r.0 == Change::Removed && r.3.is_none())
    );
}

#[test]
fn accessors_are_empty_before_the_first_change() {
    let db = fresh();
    let mut a = TestScan::new(&db);
    let mut b = TestScan::new(&db);
    let diff = Diff::new(&mut a, &mut b);
    assert!(diff.key().is_empty());
    assert_eq!(diff.old_value(), None);
    assert_eq!(diff.new_value(), None);
}

/// Differential: random writes around a snapshot, diffed against a
/// `BTreeMap` model of both views.
#[test]
fn random_snapshot_diff_matches_the_model() {
    for seed in 1..=8u64 {
        let mut rng = Lcg::new(seed);
        let mut db = fresh();
        let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let mut step = |db: &mut TestDb<MemDevice<4096>>, model: &mut BTreeMap<_, _>| {
            let k = key(rng.next_bounded(40)).to_vec();
            if rng.next_bounded(4) == 0 {
                del(db, &k);
                model.remove(&k);
            } else {
                let v = vec![
                    b'a' + u8::try_from(rng.next_bounded(3)).unwrap();
                    1 + rng.next_bounded(8)
                ];
                put(db, &k, &v);
                model.insert(k, v);
            }
        };
        for _ in 0..150 {
            step(&mut db, &mut model);
        }
        let snap = db.snapshot().unwrap();
        let before = model.clone();
        for i in 0..150 {
            step(&mut db, &mut model);
            if i == 75 {
                flush(&mut db);
            }
        }

        let mut want = Vec::new();
        let keys: std::collections::BTreeSet<_> = before.keys().chain(model.keys()).collect();
        for k in keys {
            match (before.get(k), model.get(k)) {
                (Some(o), None) => want.push(row(Change::Removed, k, Some(o), None)),
                (None, Some(n)) => want.push(row(Change::Added, k, None, Some(n))),
                (Some(o), Some(n)) if o != n => {
                    want.push(row(Change::Changed, k, Some(o), Some(n)));
                }
                _ => {}
            }
        }
        assert_eq!(
            changes(&db, snap, &db, u64::MAX, b"", None),
            want,
            "seed {seed}"
        );
    }
}
