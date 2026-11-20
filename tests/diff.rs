//! Scan diffs: a merge-join of two positioned scans that yields exactly
//! the entries that differ — the repair list once `fingerprint_buckets`
//! has narrowed a replica mismatch to one bucket.

mod common;

use common::{Lcg, MemDevice, TestDb, block_on, test_config};
use horton::diff::{Delta, Diff, SortedEntries};
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

fn key(i: usize) -> [u8; 6] {
    let mut k = *b"k00000";
    let mut n = i;
    for b in k[1..].iter_mut().rev() {
        *b = b'0' + u8::try_from(n % 10).unwrap();
        n /= 10;
    }
    k
}

/// An owned copy of one delta, so tests can collect them.
#[derive(Debug, PartialEq, Eq)]
enum Owned {
    OnlyLeft(Vec<u8>, Vec<u8>),
    OnlyRight(Vec<u8>, Vec<u8>),
    Differs(Vec<u8>, Vec<u8>, Vec<u8>),
}

fn own(d: &Delta<'_>) -> Owned {
    match *d {
        Delta::OnlyLeft { key, val } => Owned::OnlyLeft(key.to_vec(), val.to_vec()),
        Delta::OnlyRight { key, val } => Owned::OnlyRight(key.to_vec(), val.to_vec()),
        Delta::Differs { key, left, right } => {
            Owned::Differs(key.to_vec(), left.to_vec(), right.to_vec())
        }
    }
}

fn collect<A, B>(left: &mut A, right: &mut B) -> Vec<Owned>
where
    A: SortedEntries,
    B: SortedEntries<Error = A::Error>,
    A::Error: core::fmt::Debug,
{
    let (mut lk, mut lv, mut rk, mut rv) = ([0u8; 256], [0u8; 1024], [0u8; 256], [0u8; 1024]);
    let mut diff = Diff::new(left, right, &mut lk, &mut lv, &mut rk, &mut rv);
    let mut out = Vec::new();
    while let Some(d) = block_on(diff.next()).unwrap() {
        out.push(own(&d));
    }
    out
}

fn diff_dbs(
    a: &TestDb<MemDevice<4096>>,
    b: &TestDb<MemDevice<4096>>,
    start: &[u8],
    end: Option<&[u8]>,
) -> Vec<Owned> {
    let mut sa = TestScan::new(a);
    let mut sb = TestScan::new(b);
    block_on(sa.seek(start, end, u64::MAX)).unwrap();
    block_on(sb.seek(start, end, u64::MAX)).unwrap();
    collect(&mut sa, &mut sb)
}

fn fp(db: &TestDb<MemDevice<4096>>) -> Fingerprint {
    let mut scan = TestScan::new(db);
    block_on(scan.seek(b"", None, u64::MAX)).unwrap();
    let (mut k, mut v) = ([0u8; 256], [0u8; 1024]);
    block_on(fingerprint(&mut scan, &mut k, &mut v)).unwrap()
}

#[test]
fn identical_contents_have_no_delta_whatever_the_layout() {
    let mut a = fresh();
    let mut b = fresh();
    for i in 0..50 {
        put(&mut a, &key(i), &key(i * 3));
    }
    let mut rng = Lcg::new(11);
    for i in (0..80).rev() {
        let junk = [u8::try_from(rng.next_bounded(256)).unwrap(); 7];
        put(&mut b, &key(i), &junk);
    }
    flush(&mut b);
    drive(&mut b);
    for i in 50..80 {
        del(&mut b, &key(i));
    }
    for i in 0..50 {
        put(&mut b, &key(i), &key(i * 3));
    }
    assert!(diff_dbs(&a, &b, b"", None).is_empty());
}

#[test]
fn every_kind_of_difference_is_reported_in_key_order() {
    let mut a = fresh();
    let mut b = fresh();
    put(&mut a, b"apple", b"red");
    put(&mut b, b"apple", b"red");
    put(&mut a, b"banana", b"yellow"); // only left
    put(&mut a, b"cherry", b"dark");
    put(&mut b, b"cherry", b"bright"); // differs
    put(&mut b, b"damson", b"purple"); // only right
    put(&mut a, b"elder", b"x");
    put(&mut b, b"elder", b"x");
    put(&mut b, b"fig", b"green"); // only right, at the tail
    flush(&mut b);

    assert_eq!(
        diff_dbs(&a, &b, b"", None),
        vec![
            Owned::OnlyLeft(b"banana".to_vec(), b"yellow".to_vec()),
            Owned::Differs(b"cherry".to_vec(), b"dark".to_vec(), b"bright".to_vec()),
            Owned::OnlyRight(b"damson".to_vec(), b"purple".to_vec()),
            Owned::OnlyRight(b"fig".to_vec(), b"green".to_vec()),
        ]
    );
}

#[test]
fn one_empty_side_yields_the_other_wholesale() {
    let mut a = fresh();
    let b = fresh();
    for i in 0..5 {
        put(&mut a, &key(i), b"v");
    }
    let d = diff_dbs(&a, &b, b"", None);
    assert_eq!(d.len(), 5);
    assert!(
        d.iter()
            .all(|x| matches!(x, Owned::OnlyLeft(_, v) if v == b"v"))
    );
    let d = diff_dbs(&b, &a, b"", None);
    assert_eq!(d.len(), 5);
    assert!(d.iter().all(|x| matches!(x, Owned::OnlyRight(..))));
}

#[test]
fn diff_respects_the_scan_range() {
    let mut a = fresh();
    let mut b = fresh();
    for i in 0..30 {
        put(&mut a, &key(i), b"same");
        put(&mut b, &key(i), b"same");
    }
    put(&mut b, &key(5), b"other");
    put(&mut b, &key(25), b"other");
    let d = diff_dbs(&a, &b, &key(20), Some(&key(30)));
    assert_eq!(
        d,
        vec![Owned::Differs(
            key(25).to_vec(),
            b"same".to_vec(),
            b"other".to_vec()
        )]
    );
}

#[test]
fn diff_is_fused_after_the_end() {
    let mut a = fresh();
    let b = fresh();
    put(&mut a, b"k", b"v");
    let mut sa = TestScan::new(&a);
    let mut sb = TestScan::new(&b);
    block_on(sa.seek(b"", None, u64::MAX)).unwrap();
    block_on(sb.seek(b"", None, u64::MAX)).unwrap();
    let (mut lk, mut lv, mut rk, mut rv) = ([0u8; 256], [0u8; 1024], [0u8; 256], [0u8; 1024]);
    let mut diff = Diff::new(&mut sa, &mut sb, &mut lk, &mut lv, &mut rk, &mut rv);
    assert!(block_on(diff.next()).unwrap().is_some());
    assert!(block_on(diff.next()).unwrap().is_none());
    assert!(block_on(diff.next()).unwrap().is_none());
}

#[test]
fn a_too_small_buffer_errors_and_a_retry_loses_nothing() {
    let mut a = fresh();
    let b = fresh();
    put(&mut a, b"k", &[7u8; 100]);
    let mut sa = TestScan::new(&a);
    let mut sb = TestScan::new(&b);
    block_on(sa.seek(b"", None, u64::MAX)).unwrap();
    block_on(sb.seek(b"", None, u64::MAX)).unwrap();
    let (mut lk, mut lv, mut rk, mut rv) = ([0u8; 256], [0u8; 10], [0u8; 256], [0u8; 1024]);
    {
        let mut diff = Diff::new(&mut sa, &mut sb, &mut lk, &mut lv, &mut rk, &mut rv);
        assert!(matches!(
            block_on(diff.next()),
            Err(Error::BufferTooSmall { .. })
        ));
    }
    // Same scans, bigger buffer: the entry is still there.
    let mut big = [0u8; 1024];
    let mut diff = Diff::new(&mut sa, &mut sb, &mut lk, &mut big, &mut rk, &mut rv);
    let d = block_on(diff.next()).unwrap().unwrap();
    assert_eq!(own(&d), Owned::OnlyLeft(b"k".to_vec(), vec![7u8; 100]));
}

/// The whole anti-entropy loop: fingerprint buckets find the bucket
/// that disagrees, the diff of that bucket alone names every repair, and
/// applying the repairs makes the replica's fingerprint match.
#[test]
fn fingerprint_then_diff_then_repair_converges() {
    let mut primary = fresh();
    let mut replica = fresh();
    for i in 0..100 {
        put(&mut primary, &key(i), &key(i));
        put(&mut replica, &key(i), &key(i));
    }
    flush(&mut replica);
    // Divergence inside [k00050, k00075): a lost write, a stale value
    // and a key the primary deleted.
    put(&mut primary, &key(60), b"fresh");
    del(&mut replica, &key(55));
    del(&mut primary, &key(70));
    assert_ne!(fp(&primary), fp(&replica));

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
    let (bp, br) = (buckets(&primary), buckets(&replica));
    let bad: Vec<usize> = (0..4).filter(|&i| bp[i] != br[i]).collect();
    assert_eq!(bad, vec![2]);

    let repairs = diff_dbs(&primary, &replica, splits[1], Some(splits[2]));
    assert_eq!(repairs.len(), 3);
    for r in repairs {
        match r {
            Owned::OnlyLeft(k, v) | Owned::Differs(k, v, _) => put(&mut replica, &k, &v),
            Owned::OnlyRight(k, _) => del(&mut replica, &k),
        }
    }
    assert_eq!(fp(&primary), fp(&replica));
    assert!(diff_dbs(&primary, &replica, b"", None).is_empty());
}

/// A sorted slice of pairs: any ordered source can stand on either side.
struct Pairs<'p> {
    items: &'p [(&'p [u8], &'p [u8])],
    at: usize,
}

impl SortedEntries for Pairs<'_> {
    type Error = Error<()>;

    async fn next_entry(
        &mut self,
        key_buf: &mut [u8],
        val_buf: &mut [u8],
    ) -> Result<Option<(usize, usize)>, Self::Error> {
        let Some(&(k, v)) = self.items.get(self.at) else {
            return Ok(None);
        };
        self.at += 1;
        key_buf[..k.len()].copy_from_slice(k);
        val_buf[..v.len()].copy_from_slice(v);
        Ok(Some((k.len(), v.len())))
    }
}

#[test]
fn any_sorted_source_can_be_diffed() {
    let left: [(&[u8], &[u8]); 3] = [(b"a", b"1"), (b"b", b"2"), (b"c", b"3")];
    let right: [(&[u8], &[u8]); 3] = [(b"a", b"1"), (b"c", b"4"), (b"d", b"5")];
    let mut l = Pairs {
        items: &left,
        at: 0,
    };
    let mut r = Pairs {
        items: &right,
        at: 0,
    };
    assert_eq!(
        collect(&mut l, &mut r),
        vec![
            Owned::OnlyLeft(b"b".to_vec(), b"2".to_vec()),
            Owned::Differs(b"c".to_vec(), b"3".to_vec(), b"4".to_vec()),
            Owned::OnlyRight(b"d".to_vec(), b"5".to_vec()),
        ]
    );
}

#[test]
fn delta_key_names_the_differing_key() {
    let d = Delta::OnlyLeft {
        key: b"x",
        val: b"1",
    };
    assert_eq!(d.key(), b"x");
    let d = Delta::OnlyRight {
        key: b"y",
        val: b"1",
    };
    assert_eq!(d.key(), b"y");
    let d = Delta::Differs {
        key: b"z",
        left: b"1",
        right: b"2",
    };
    assert_eq!(d.key(), b"z");
}
