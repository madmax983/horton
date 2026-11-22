//! Set-reconciliation sketches: an invertible Bloom lookup table over a
//! scan, so two replicas find every entry they disagree on from one
//! fixed-size message, without bisecting.

mod common;

use common::{Lcg, MemDevice, TestDb, block_on, test_config};
use horton::fingerprint::fingerprint;
use horton::sketch::{Sketch, entry_id, sketch};
use horton::{Compaction, Error, Progress, Scan};

type TestScan<'d> = Scan<'d, MemDevice<4096>, 4096, 256, 1024, 64, 4096, 7, 4, 1024, 8>;
type S = Sketch<64>;

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

fn of(entries: &[(&[u8], &[u8])]) -> S {
    let mut s = S::new();
    for (k, v) in entries {
        s.insert(k, v);
    }
    s
}

fn sketch_db(db: &TestDb<MemDevice<4096>>) -> S {
    let mut scan = TestScan::new(db);
    block_on(scan.seek(b"", None, u64::MAX)).unwrap();
    let (mut k, mut v) = ([0u8; 256], [0u8; 1024]);
    let mut s = S::new();
    block_on(sketch(&mut scan, &mut s, &mut k, &mut v)).unwrap();
    s
}

/// The visible entries of `db` whose [`entry_id`] is in `ids`: what a
/// decoded id list means, recovered by one rescan.
fn entries_with(db: &TestDb<MemDevice<4096>>, ids: &[u64]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut scan = TestScan::new(db);
    block_on(scan.seek(b"", None, u64::MAX)).unwrap();
    let (mut k, mut v) = ([0u8; 256], [0u8; 1024]);
    let mut out = Vec::new();
    while let Some((kl, vl)) = block_on(scan.next(&mut k, &mut v)).unwrap() {
        if ids.contains(&entry_id(&k[..kl], &v[..vl])) {
            out.push((k[..kl].to_vec(), v[..vl].to_vec()));
        }
    }
    out
}

fn sorted(mut ids: Vec<u64>) -> Vec<u64> {
    ids.sort_unstable();
    ids
}

#[test]
fn identical_sets_decode_to_no_differences() {
    let a = of(&[(b"a", b"1"), (b"b", b"2"), (b"c", b"3")]);
    let b = of(&[(b"c", b"3"), (b"a", b"1"), (b"b", b"2")]);
    let (mut l, mut r) = ([0u64; 4], [0u64; 4]);
    assert_eq!(a.subtract(&b).decode(&mut l, &mut r), Ok((0, 0)));
    assert!(a.subtract(&b).is_empty());
    assert!(S::new().is_empty());
    assert!(!a.is_empty());
}

#[test]
fn recovers_entries_on_each_side_and_changed_values() {
    let local = of(&[(b"a", b"1"), (b"b", b"2"), (b"c", b"3"), (b"d", b"4")]);
    let remote = of(&[(b"a", b"1"), (b"c", b"30"), (b"d", b"4"), (b"e", b"5")]);
    let (mut l, mut r) = ([0u64; 8], [0u64; 8]);
    let (nl, nr) = local.subtract(&remote).decode(&mut l, &mut r).unwrap();
    assert_eq!(
        sorted(l[..nl].to_vec()),
        sorted(vec![entry_id(b"b", b"2"), entry_id(b"c", b"3")])
    );
    assert_eq!(
        sorted(r[..nr].to_vec()),
        sorted(vec![entry_id(b"c", b"30"), entry_id(b"e", b"5")])
    );
}

#[test]
fn entry_ids_are_length_prefixed() {
    assert_ne!(entry_id(b"ab", b"c"), entry_id(b"a", b"bc"));
    assert_eq!(entry_id(b"ab", b"c"), entry_id(b"ab", b"c"));
}

#[test]
fn decodes_differences_up_to_a_large_fraction_of_the_cells() {
    // 64 cells peel 20 differences nearly always. Peeling is
    // probabilistic: a miss must say so, and a hit must be exact.
    let mut rng = Lcg::new(7);
    let mut decoded = 0;
    for _ in 0..200 {
        let mut ours = S::new();
        let mut theirs = S::new();
        let mut want_l = Vec::new();
        let mut want_r = Vec::new();
        for i in 0..500u64 {
            let k = i.to_be_bytes();
            let v = rng.next().to_be_bytes();
            ours.insert(&k, &v);
            theirs.insert(&k, &v);
        }
        for i in 0..20u64 {
            let k = (1000 + i).to_be_bytes();
            let v = rng.next().to_be_bytes();
            if rng.next().is_multiple_of(2) {
                ours.insert(&k, &v);
                want_l.push(entry_id(&k, &v));
            } else {
                theirs.insert(&k, &v);
                want_r.push(entry_id(&k, &v));
            }
        }
        let (mut left, mut right) = ([0u64; 20], [0u64; 20]);
        match ours.subtract(&theirs).decode(&mut left, &mut right) {
            Ok((nl, nr)) => {
                assert_eq!(sorted(left[..nl].to_vec()), sorted(want_l));
                assert_eq!(sorted(right[..nr].to_vec()), sorted(want_r));
                decoded += 1;
            }
            Err(e) => assert_eq!(e, Error::SketchTooSmall),
        }
    }
    assert!(decoded >= 196, "only {decoded} of 200 decoded");
}

#[test]
fn too_many_differences_is_reported_not_misdecoded() {
    let mut a = Sketch::<8>::new();
    for i in 0..200u32 {
        a.insert(&i.to_be_bytes(), b"v");
    }
    let (mut l, mut r) = ([0u64; 256], [0u64; 256]);
    assert_eq!(
        a.subtract(&Sketch::new()).decode(&mut l, &mut r),
        Err(Error::<core::convert::Infallible>::SketchTooSmall)
    );
}

#[test]
fn small_output_buffers_report_the_length_they_need() {
    let local = of(&[(b"a", b"1"), (b"b", b"2"), (b"c", b"3")]);
    let (mut l, mut r) = ([0u64; 1], [0u64; 1]);
    assert_eq!(
        local.subtract(&S::new()).decode(&mut l, &mut r),
        Err(Error::<core::convert::Infallible>::BufferTooSmall { need: 3 })
    );
}

#[test]
fn wire_form_round_trips_and_checks_its_length() {
    let s = of(&[(b"a", b"1"), (b"b", b"2")]);
    let mut wire = [0u8; S::WIRE_BYTES];
    assert_eq!(S::WIRE_BYTES, 64 * 20);
    s.to_bytes(&mut wire).unwrap();
    assert_eq!(S::from_bytes(&wire), Ok(s));
    let mut short = [0u8; S::WIRE_BYTES - 1];
    assert_eq!(
        s.to_bytes(&mut short),
        Err(Error::<core::convert::Infallible>::BadBufferLen)
    );
    assert_eq!(
        S::from_bytes(&short),
        Err(Error::<core::convert::Infallible>::BadBufferLen)
    );
}

#[test]
fn sketch_is_layout_independent_and_heals_two_replicas() {
    // Two replicas share 400 entries, then drift: the device overwrites,
    // deletes and adds; the ground misses some and has a stale extra.
    let mut device = fresh();
    let mut ground = fresh();
    for i in 0..400 {
        let v = [u8::try_from(i % 251).unwrap(); 40];
        put(&mut device, &key(i), &v);
        put(&mut ground, &key(i), &v);
    }
    flush(&mut device);
    drive(&mut device);
    for i in [3, 77, 250] {
        put(&mut device, &key(i), b"rewritten");
    }
    for i in [10, 11] {
        del(&mut device, &key(i));
    }
    for i in 400..405 {
        put(&mut device, &key(i), b"new");
    }
    put(&mut ground, &key(999), b"stale");

    // Ground station: send its sketch over the link.
    let mut wire = [0u8; S::WIRE_BYTES];
    sketch_db(&ground).to_bytes(&mut wire).unwrap();

    // Device: subtract, peel, and turn ids back into entries by rescanning.
    let diff = sketch_db(&device).subtract(&S::from_bytes(&wire).unwrap());
    let (mut mine, mut theirs) = ([0u64; 32], [0u64; 32]);
    let (nm, nt) = diff.decode(&mut mine, &mut theirs).unwrap();
    assert_eq!((nm, nt), (8, 6)); // 3 rewrites + 5 adds; 3 old + 2 deleted + 1 stale

    // Device ships its side; ground deletes keys it holds under `theirs`
    // ids, then applies the device's entries.
    let repairs = entries_with(&device, &mine[..nm]);
    let stale = entries_with(&ground, &theirs[..nt]);
    assert_eq!(repairs.len(), nm);
    assert_eq!(stale.len(), nt);
    for (k, _) in &stale {
        del(&mut ground, k);
    }
    for (k, v) in &repairs {
        put(&mut ground, k, v);
    }

    assert!(sketch_db(&device).subtract(&sketch_db(&ground)).is_empty());
    let fp = |db: &TestDb<MemDevice<4096>>| {
        let mut scan = TestScan::new(db);
        block_on(scan.seek(b"", None, u64::MAX)).unwrap();
        let (mut k, mut v) = ([0u8; 256], [0u8; 1024]);
        block_on(fingerprint(&mut scan, &mut k, &mut v)).unwrap()
    };
    assert_eq!(fp(&device), fp(&ground));
}

#[test]
fn sketches_of_disjoint_ranges_merge() {
    let whole = of(&[(b"a", b"1"), (b"m", b"2"), (b"z", b"3")]);
    let low = of(&[(b"a", b"1")]);
    let high = of(&[(b"m", b"2"), (b"z", b"3")]);
    assert_eq!(low.merge(&high), whole);
}
