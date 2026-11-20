//! Content fingerprints: a layout-independent digest of the visible
//! key-value set, mergeable across key ranges for anti-entropy.

mod common;

use common::{Lcg, MemDevice, TestDb, block_on, test_config};
use horton::fingerprint::{Fingerprint, fingerprint, fingerprint_buckets};
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

/// Puts, flushing (and compacting when needed) whenever the memtable
/// or WAL fills.
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

fn fp(db: &TestDb<MemDevice<4096>>, start: &[u8], end: Option<&[u8]>, seq: u64) -> Fingerprint {
    let mut scan = TestScan::new(db);
    block_on(scan.seek(start, end, seq)).unwrap();
    let (mut k, mut v) = ([0u8; 256], [0u8; 1024]);
    block_on(fingerprint(&mut scan, &mut k, &mut v)).unwrap()
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

#[test]
fn empty_database_has_the_empty_fingerprint() {
    let db = fresh();
    assert_eq!(fp(&db, b"", None, u64::MAX), Fingerprint::EMPTY);
    assert_eq!(Fingerprint::EMPTY.count(), 0);
}

#[test]
fn same_contents_same_fingerprint_whatever_the_layout() {
    // A: everything in the memtable, written once in order.
    let mut a = fresh();
    for i in 0..40 {
        put(&mut a, &key(i), &key(i * 7));
    }
    // B: reverse order, overwrites, deletes, flushes and compaction,
    // ending with the same visible set.
    let mut b = fresh();
    let mut rng = Lcg::new(7);
    for round in 0..3 {
        for i in (0..60).rev() {
            let junk = [u8::try_from(rng.next_bounded(256)).unwrap(); 9];
            put(&mut b, &key(i), &junk);
        }
        flush(&mut b);
        if round == 1 {
            drive(&mut b);
        }
    }
    for i in 40..60 {
        del(&mut b, &key(i));
    }
    flush(&mut b);
    for i in (0..40).rev() {
        put(&mut b, &key(i), &key(i * 7));
    }

    let fa = fp(&a, b"", None, u64::MAX);
    assert_eq!(fa.count(), 40);
    assert_eq!(fa, fp(&b, b"", None, u64::MAX));
}

#[test]
fn one_changed_byte_changes_the_fingerprint() {
    let mut db = fresh();
    for i in 0..20 {
        put(&mut db, &key(i), b"value");
    }
    let before = fp(&db, b"", None, u64::MAX);
    put(&mut db, &key(13), b"valuf");
    let after = fp(&db, b"", None, u64::MAX);
    assert_eq!(before.count(), after.count());
    assert_ne!(before, after);
}

#[test]
fn key_and_value_boundaries_are_not_confused() {
    // ("ab", "c") and ("a", "bc") concatenate to the same bytes.
    let mut x = fresh();
    put(&mut x, b"ab", b"c");
    let mut y = fresh();
    put(&mut y, b"a", b"bc");
    assert_ne!(fp(&x, b"", None, u64::MAX), fp(&y, b"", None, u64::MAX));
}

#[test]
fn snapshot_fingerprint_ignores_later_writes() {
    let mut db = fresh();
    for i in 0..10 {
        put(&mut db, &key(i), b"v1");
    }
    let snap = db.snapshot().unwrap();
    let pinned = fp(&db, b"", None, snap);
    for i in 0..10 {
        put(&mut db, &key(i), b"v2");
    }
    flush(&mut db);
    assert_eq!(fp(&db, b"", None, snap), pinned);
    assert_ne!(fp(&db, b"", None, u64::MAX), pinned);
}

#[test]
fn bucket_fingerprints_merge_into_the_whole_and_localize_a_diff() {
    let mut a = fresh();
    let mut b = fresh();
    for i in 0..100 {
        put(&mut a, &key(i), &key(i));
        put(&mut b, &key(i), &key(i));
    }
    flush(&mut b);
    put(&mut b, &key(62), b"diverged");

    let splits: [&[u8]; 3] = [&key(25), &key(50), &key(75)];
    let buckets = |db: &TestDb<MemDevice<4096>>| {
        let mut scan = TestScan::new(db);
        block_on(scan.seek(b"", None, u64::MAX)).unwrap();
        let (mut k, mut v) = ([0u8; 256], [0u8; 1024]);
        let mut out = [Fingerprint::EMPTY; 4];
        block_on(fingerprint_buckets(
            &mut scan, &splits, &mut out, &mut k, &mut v,
        ))
        .unwrap();
        out
    };
    let ba = buckets(&a);
    let bb = buckets(&b);

    let whole = ba.iter().fold(Fingerprint::EMPTY, |acc, f| acc.merge(*f));
    assert_eq!(whole, fp(&a, b"", None, u64::MAX));
    assert!(ba.iter().all(|f| f.count() == 25));
    // Only the bucket holding key 62 ([k00050, k00075)) disagrees.
    let differing: Vec<usize> = (0..4).filter(|&i| ba[i] != bb[i]).collect();
    assert_eq!(differing, vec![2]);
    // And a range fingerprint of that bucket matches the bucket.
    assert_eq!(fp(&a, &key(50), Some(&key(75)), u64::MAX), ba[2]);
}

#[test]
fn merge_is_order_independent() {
    let mut f1 = Fingerprint::EMPTY;
    f1.add(b"a", b"1");
    f1.add(b"b", b"2");
    let mut f2 = Fingerprint::EMPTY;
    f2.add(b"b", b"2");
    f2.add(b"a", b"1");
    assert_eq!(f1, f2);
    let mut lone = Fingerprint::EMPTY;
    lone.add(b"a", b"1");
    let mut other = Fingerprint::EMPTY;
    other.add(b"b", b"2");
    assert_eq!(lone.merge(other), f1);
    assert_eq!(other.merge(lone), f1);
}

#[test]
fn bad_bucket_arguments_are_errors() {
    let db = fresh();
    let mut scan = TestScan::new(&db);
    block_on(scan.seek(b"", None, u64::MAX)).unwrap();
    let (mut k, mut v) = ([0u8; 256], [0u8; 1024]);
    let mut out = [Fingerprint::EMPTY; 2];
    let two: [&[u8]; 2] = [b"a", b"b"];
    let r = block_on(fingerprint_buckets(
        &mut scan, &two, &mut out, &mut k, &mut v,
    ));
    assert!(matches!(r, Err(Error::BadBufferLen)));
    let mut out3 = [Fingerprint::EMPTY; 3];
    let unsorted: [&[u8]; 2] = [b"b", b"a"];
    let r = block_on(fingerprint_buckets(
        &mut scan, &unsorted, &mut out3, &mut k, &mut v,
    ));
    assert!(matches!(r, Err(Error::BadConfig)));
}

#[test]
fn wire_form_round_trips() {
    let mut f = Fingerprint::EMPTY;
    f.add(b"sensor/temp", b"21.5");
    f.add(b"sensor/rh", b"40");
    let b = f.to_bytes();
    assert_eq!(&b[..8], &2u64.to_be_bytes());
    assert_eq!(&b[8..], &f.digest().to_be_bytes());
    assert_eq!(Fingerprint::from_bytes(b), f);
    assert_eq!(Fingerprint::default(), Fingerprint::EMPTY);
}
