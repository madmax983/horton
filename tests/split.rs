//! Equi-depth split keys: one pass over a sorted source, fixed memory,
//! and the split keys that cut it into buckets of near-equal size, so
//! `fingerprint_buckets` compares a replica bucket by bucket with no
//! bucket empty and none holding everything, however skewed the keys.

mod common;

use common::{Lcg, MemDevice, TestDb, block_on, test_config};
use horton::diff::SortedEntries;
use horton::fingerprint::{Fingerprint, fingerprint_buckets};
use horton::split::{Splitter, sample};
use horton::{Compaction, Error, Progress, Scan};

type TestScan<'d> = Scan<'d, MemDevice<4096>, 4096, 256, 1024, 64, 4096, 7, 4, 1024, 8>;

/// An in-memory ascending source, for exact counting.
struct Sorted<'v> {
    keys: &'v [Vec<u8>],
    at: usize,
}

impl SortedEntries for Sorted<'_> {
    type Error = Error<core::convert::Infallible>;

    async fn next_entry(
        &mut self,
        key_buf: &mut [u8],
        _val_buf: &mut [u8],
    ) -> Result<Option<(usize, usize)>, Self::Error> {
        let Some(k) = self.keys.get(self.at) else {
            return Ok(None);
        };
        if k.len() > key_buf.len() {
            return Err(Error::BufferTooSmall { need: k.len() });
        }
        key_buf[..k.len()].copy_from_slice(k);
        self.at += 1;
        Ok(Some((k.len(), 0)))
    }
}

fn keys(n: usize) -> Vec<Vec<u8>> {
    (0..n).map(|i| format!("k{i:08}").into_bytes()).collect()
}

/// Bucket sizes of `keys` under `splits`, the way `fingerprint_buckets`
/// assigns them.
fn bucket_sizes(keys: &[Vec<u8>], splits: &[&[u8]]) -> Vec<usize> {
    let mut out = vec![0; splits.len() + 1];
    for k in keys {
        out[splits.partition_point(|s| *s <= k.as_slice())] += 1;
    }
    out
}

fn sample_all<const K: usize, const S: usize>(ks: &[Vec<u8>]) -> Splitter<K, S> {
    let mut sp = Splitter::<K, S>::new();
    let mut src = Sorted { keys: ks, at: 0 };
    let (mut k, mut v) = ([0u8; K], [0u8; 1]);
    let n = block_on(sample(&mut src, &mut sp, &mut k, &mut v)).unwrap();
    assert_eq!(n, ks.len() as u64);
    assert_eq!(sp.seen(), ks.len() as u64);
    sp
}

#[test]
fn an_empty_source_has_no_splits() {
    let sp = sample_all::<16, 8>(&[]);
    let mut out = [&[][..]; 3];
    assert_eq!(sp.splits(&mut out), 0);
}

#[test]
fn few_keys_split_exactly() {
    let ks = keys(8);
    let sp = sample_all::<16, 8>(&ks);
    assert_eq!(sp.stride(), 1);
    let mut out = [&[][..]; 3];
    let n = sp.splits(&mut out);
    assert_eq!(n, 3);
    assert_eq!(bucket_sizes(&ks, &out[..n]), vec![2, 2, 2, 2]);
}

#[test]
fn fewer_keys_than_buckets_give_one_key_per_bucket() {
    let ks = keys(3);
    let sp = sample_all::<16, 8>(&ks);
    let mut out = [&[][..]; 7];
    let n = sp.splits(&mut out);
    assert_eq!(n, 2);
    assert_eq!(bucket_sizes(&ks, &out[..n]), vec![1, 1, 1]);
}

#[test]
fn many_keys_stay_in_fixed_memory_and_split_evenly() {
    for (total, buckets) in [(10_000, 4), (12_345, 7), (999, 16), (100_003, 9)] {
        let ks = keys(total);
        let sp = sample_all::<16, 64>(&ks);
        assert!(sp.stride() > 1, "the sampler thinned itself");
        let mut out = [&[][..]; 15];
        let n = sp.splits(&mut out[..buckets - 1]);
        assert_eq!(n, buckets - 1);
        let splits = &out[..n];
        assert!(splits.windows(2).all(|w| w[0] < w[1]), "strictly ascending");
        let sizes = bucket_sizes(&ks, splits);
        assert_eq!(sizes.iter().sum::<usize>(), total);
        // The documented bound: every bucket within two strides of ideal.
        let ideal = total / buckets;
        let slack = usize::try_from(2 * sp.stride()).unwrap();
        for s in &sizes {
            assert!(
                s.abs_diff(ideal) <= slack,
                "{total} keys / {buckets}: {sizes:?}, stride {}",
                sp.stride()
            );
        }
    }
}

#[test]
fn an_odd_slot_count_thins_correctly() {
    let ks = keys(5_000);
    let sp = sample_all::<16, 7>(&ks);
    let mut out = [&[][..]; 2];
    let n = sp.splits(&mut out);
    assert_eq!(n, 2);
    let sizes = bucket_sizes(&ks, &out[..n]);
    let slack = usize::try_from(2 * sp.stride()).unwrap();
    for s in &sizes {
        assert!(s.abs_diff(5_000 / 3) <= slack, "{sizes:?}");
    }
}

#[test]
fn an_oversized_key_is_refused_and_changes_nothing() {
    let mut sp = Splitter::<4, 8>::new();
    sp.observe(b"abc").unwrap();
    assert_eq!(
        sp.observe(b"abcde"),
        Err(Error::KeyTooLarge { len: 5, max: 4 })
    );
    assert_eq!(sp.seen(), 1);
    sp.observe(b"abd").unwrap();
    let mut out = [&[][..]; 1];
    assert_eq!(sp.splits(&mut out), 1);
    assert_eq!(out[0], b"abd");
}

#[test]
fn clear_starts_over() {
    let ks = keys(1_000);
    let mut sp = sample_all::<16, 8>(&ks);
    sp.clear();
    assert_eq!((sp.seen(), sp.stride()), (0, 1));
    let mut out = [&[][..]; 1];
    assert_eq!(sp.splits(&mut out), 0);
}

fn put(db: &mut TestDb<MemDevice<4096>>, k: &[u8], v: &[u8]) {
    match block_on(db.put(k, v)) {
        Ok(_) => {}
        Err(Error::TableFull | Error::ArenaFull | Error::WalFull) => {
            match block_on(db.flush()) {
                Ok(()) => {}
                Err(Error::NeedsCompaction) => {
                    let mut c = Compaction::<4096, 256, 1024, 1024>::new();
                    while block_on(db.compact_step(&mut c)).unwrap() == Progress::More {}
                    block_on(db.flush()).unwrap();
                }
                Err(e) => panic!("flush: {e:?}"),
            }
            block_on(db.put(k, v)).unwrap();
        }
        Err(e) => panic!("put: {e:?}"),
    }
}

/// The point of it all: a telemetry store whose keys share one prefix
/// and crowd one corner of the key space. Evenly spaced byte splits put
/// every key in one bucket; sampled splits balance the buckets, and a
/// one-entry divergence lands in exactly one of them.
#[test]
fn sampled_splits_balance_replica_buckets_over_skewed_keys() {
    let mut dev = TestDb::new(MemDevice::<4096>::new(), test_config());
    let mut replica = TestDb::new(MemDevice::<4096>::new(), test_config());
    block_on(dev.open()).unwrap();
    block_on(replica.open()).unwrap();
    let mut rng = Lcg::new(7);
    let mut stamp = 1_700_000_000u64;
    let mut written = Vec::new();
    for _ in 0..600 {
        stamp += 1 + rng.next() % 5;
        let k = format!("tlm/{stamp:012}").into_bytes();
        put(&mut dev, &k, b"reading");
        put(&mut replica, &k, b"reading");
        written.push(k);
    }
    // One divergence, two-thirds of the way along.
    put(&mut replica, &written[400], b"bitflip");

    // The device samples its own keys and ships the splits to the replica.
    let mut sp = Splitter::<256, 32>::new();
    let mut scan = TestScan::new(&dev);
    block_on(scan.seek(b"", None, u64::MAX)).unwrap();
    let (mut kbuf, mut vbuf) = ([0u8; 256], [0u8; 1024]);
    block_on(sample(&mut scan, &mut sp, &mut kbuf, &mut vbuf)).unwrap();
    let mut out = [&[][..]; 7];
    let n = sp.splits(&mut out);
    assert_eq!(n, 7);
    let splits = &out[..n];

    let bucketed = |db: &TestDb<MemDevice<4096>>| {
        let mut fps = [Fingerprint::EMPTY; 8];
        let mut scan = TestScan::new(db);
        block_on(scan.seek(b"", None, u64::MAX)).unwrap();
        let (mut k, mut v) = ([0u8; 256], [0u8; 1024]);
        block_on(fingerprint_buckets(
            &mut scan, splits, &mut fps, &mut k, &mut v,
        ))
        .unwrap();
        fps
    };
    let ours = bucketed(&dev);
    let theirs = bucketed(&replica);
    for fp in &ours {
        assert!(
            (50..=100).contains(&fp.count()),
            "balanced buckets: {:?}",
            ours.map(|f| f.count())
        );
    }
    let bad: Vec<usize> = (0..8).filter(|&i| ours[i] != theirs[i]).collect();
    assert_eq!(bad.len(), 1, "one bucket disagrees");
    let at = bad[0];
    let lo = if at == 0 { &b""[..] } else { splits[at - 1] };
    assert!(lo <= written[400].as_slice());
    assert!(at == 7 || written[400].as_slice() < splits[at]);
}
