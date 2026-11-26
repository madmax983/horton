//! `SSTable` tests: round trips, bloom behavior, and corruption semantics.

mod common;

use std::future::poll_fn;

use common::{MemDevice, block_on};
use horton::sstable::{SstEntry, TableReader, bloom_k, plan_table, write_table};
use horton::{BlockDevice, Error};

const BLOCK: usize = 4096;
const KEY_MAX: usize = 256;
const BLOOM_BYTES: usize = 1024;
const BASE: u64 = 100;

const fn entry<'a>(key: &'a [u8], val: &'a [u8], seq: u64) -> SstEntry<'a> {
    SstEntry {
        key,
        val,
        seq,
        tombstone: false,
        expire_at: 0,
    }
}

/// Writes `items` (key-ascending) as one table at `BASE`; returns
/// `(blocks_written, data_blocks)`.
fn write(items: &[SstEntry<'_>], dev: &mut MemDevice<BLOCK>) -> (u64, u64) {
    let plan =
        plan_table::<core::convert::Infallible, BLOCK, KEY_MAX>(items.iter().copied()).unwrap();
    let k = bloom_k(BLOOM_BYTES * 8, plan.entry_count);
    let n = block_on(
        write_table::<MemDevice<BLOCK>, BLOCK, BLOOM_BYTES, KEY_MAX>(
            dev,
            BASE,
            k,
            items.iter().copied(),
            None,
        ),
    )
    .unwrap();
    assert_eq!(n, plan.data_blocks + 3, "writer must emit plan + 3 blocks");
    (n, plan.data_blocks)
}

fn get(
    dev: &MemDevice<BLOCK>,
    footer: u64,
    key: &[u8],
) -> Result<Option<Vec<u8>>, Error<core::convert::Infallible>> {
    let mut scratch = [0u8; BLOCK];
    let mut decomp = [0u8; BLOCK];
    let mut buf = [0u8; 2048];
    let reader = block_on(TableReader::<MemDevice<BLOCK>, BLOCK, BLOOM_BYTES>::open(
        dev,
        &mut scratch,
        footer,
    ))?;
    let n = block_on(reader.get(&mut scratch, &mut decomp, key, &mut buf))?;
    Ok(n.map(|n| buf[..n].to_vec()))
}

fn read_block(dev: &MemDevice<BLOCK>, id: u64) -> [u8; BLOCK] {
    let mut buf = [0u8; BLOCK];
    block_on(poll_fn(|cx| dev.poll_read_block(cx, id, &mut buf))).unwrap();
    buf
}

fn write_block(dev: &mut MemDevice<BLOCK>, id: u64, buf: &[u8; BLOCK]) {
    let owned = *buf;
    block_on(poll_fn(|cx| dev.poll_write_block(cx, id, &owned))).unwrap();
}

#[test]
fn empty_table_round_trip() {
    let mut dev = MemDevice::<BLOCK>::new();
    let (n, data_blocks) = write(&[], &mut dev);
    assert_eq!(data_blocks, 0);
    assert_eq!(n, 3);
    let footer = BASE + n - 1;
    assert_eq!(get(&dev, footer, b"anything").unwrap(), None);
}

#[test]
fn single_entry_round_trip() {
    let mut dev = MemDevice::<BLOCK>::new();
    let items = [entry(b"k", b"v", 1)];
    let (n, _) = write(&items, &mut dev);
    let footer = BASE + n - 1;
    assert_eq!(get(&dev, footer, b"k").unwrap(), Some(b"v".to_vec()));
    assert_eq!(get(&dev, footer, b"kk").unwrap(), None);
    assert_eq!(get(&dev, footer, b"a").unwrap(), None);
}

#[test]
fn multi_block_restart_boundaries() {
    // ~200 B values x 40 entries: several data blocks, several restart
    // points per block — exercises index binary search and restart search.
    let mut dev = MemDevice::<BLOCK>::new();
    let keys: Vec<Vec<u8>> = (0..40u32)
        .map(|i| format!("key-{i:03}").into_bytes())
        .collect();
    let vals: Vec<Vec<u8>> = (0..40u32)
        .map(|i| vec![b'x' + (i % 3) as u8; 200])
        .collect();
    let items: Vec<SstEntry<'_>> = keys
        .iter()
        .zip(vals.iter())
        .enumerate()
        .map(|(i, (k, v))| entry(k, v, i as u64 + 1))
        .collect();
    let (n, data_blocks) = write(&items, &mut dev);
    assert!(
        data_blocks >= 2,
        "want multiple data blocks, got {data_blocks}"
    );
    let footer = BASE + n - 1;
    for (k, v) in keys.iter().zip(vals.iter()) {
        assert_eq!(get(&dev, footer, k).unwrap(), Some(v.clone()), "key {k:?}");
    }
    // Misses below, between, and above the key range.
    assert_eq!(get(&dev, footer, b"key-!").unwrap(), None);
    assert_eq!(get(&dev, footer, b"key-020a").unwrap(), None);
    assert_eq!(get(&dev, footer, b"key-999").unwrap(), None);
}

#[test]
fn tombstone_reads_as_missing() {
    let mut dev = MemDevice::<BLOCK>::new();
    let items = [
        entry(b"a", b"1", 1),
        SstEntry {
            key: b"b",
            val: b"",
            seq: 2,
            tombstone: true,
            expire_at: 0,
        },
        entry(b"c", b"3", 3),
    ];
    let (n, _) = write(&items, &mut dev);
    let footer = BASE + n - 1;
    assert_eq!(get(&dev, footer, b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(get(&dev, footer, b"b").unwrap(), None);
    assert_eq!(get(&dev, footer, b"c").unwrap(), Some(b"3".to_vec()));
}

#[test]
fn bloom_has_no_false_negatives() {
    let mut dev = MemDevice::<BLOCK>::new();
    let keys: Vec<Vec<u8>> = (0..200u32)
        .map(|i| format!("bloom-{i:04}").into_bytes())
        .collect();
    let items: Vec<SstEntry<'_>> = keys
        .iter()
        .enumerate()
        .map(|(i, k)| entry(k, b"v", i as u64 + 1))
        .collect();
    let (n, _) = write(&items, &mut dev);
    let footer = BASE + n - 1;
    // Every inserted key must survive the bloom gate: no false negatives.
    for k in &keys {
        assert_eq!(
            get(&dev, footer, k).unwrap(),
            Some(b"v".to_vec()),
            "false negative for {k:?}"
        );
    }
}

#[test]
fn bloom_k_is_sane() {
    // ~56 optimal probes for 100 keys in 8192 bits: clamped to 30.
    assert_eq!(bloom_k(8192, 100), 30);
    // ~5.6 optimal probes for 1000 keys.
    let k = bloom_k(8192, 1000);
    assert!((5..=6).contains(&k), "k = {k}");
    // Degenerate inputs still yield a usable probe count.
    assert_eq!(bloom_k(8192, 0), 1);
    assert_eq!(bloom_k(8, 1_000_000), 1);
}

#[test]
fn corrupt_footer_rejected() {
    let mut dev = MemDevice::<BLOCK>::new();
    let items = [entry(b"k", b"v", 1)];
    let (n, _) = write(&items, &mut dev);
    let footer = BASE + n - 1;
    // Flip a payload byte (CRC region untouched in layout, so the stored CRC
    // now mismatches).
    let mut blk = read_block(&dev, footer);
    blk[16] ^= 0xFF;
    write_block(&mut dev, footer, &blk);
    let mut scratch = [0u8; BLOCK];
    let res = block_on(TableReader::<MemDevice<BLOCK>, BLOCK, BLOOM_BYTES>::open(
        &dev,
        &mut scratch,
        footer,
    ));
    assert!(matches!(res, Err(Error::CorruptBlock { id }) if id == footer));
}

#[test]
fn corrupt_data_block_is_an_error() {
    let mut dev = MemDevice::<BLOCK>::new();
    let items = [entry(b"k", b"v", 1), entry(b"k2", b"v2", 2)];
    let (n, data_blocks) = write(&items, &mut dev);
    assert_eq!(data_blocks, 1);
    let footer = BASE + n - 1;
    // Corrupt the only data block: a committed table's bad CRC is media
    // corruption and must surface — reading it as absent would let an
    // older version in a deeper table win silently.
    let mut blk = read_block(&dev, BASE);
    blk[0] ^= 0xFF;
    write_block(&mut dev, BASE, &blk);
    assert!(matches!(get(&dev, footer, b"k"), Err(Error::CorruptBlock { id }) if id == BASE));
    assert!(matches!(get(&dev, footer, b"k2"), Err(Error::CorruptBlock { id }) if id == BASE));
}

#[test]
fn corrupt_index_is_an_error() {
    let mut dev = MemDevice::<BLOCK>::new();
    let items = [entry(b"k", b"v", 1)];
    let (n, data_blocks) = write(&items, &mut dev);
    let index_id = BASE + data_blocks + 1;
    let footer = BASE + n - 1;
    let mut blk = read_block(&dev, index_id);
    blk[0] ^= 0xFF;
    write_block(&mut dev, index_id, &blk);
    let err = get(&dev, footer, b"k").unwrap_err();
    assert!(matches!(err, Error::CorruptBlock { id } if id == index_id));
}

#[test]
fn corrupt_bloom_only_disables_the_gate() {
    let mut dev = MemDevice::<BLOCK>::new();
    let items = [entry(b"k", b"v", 1)];
    let (n, data_blocks) = write(&items, &mut dev);
    let bloom_id = BASE + data_blocks;
    let footer = BASE + n - 1;
    let mut blk = read_block(&dev, bloom_id);
    blk[0] ^= 0xFF;
    write_block(&mut dev, bloom_id, &blk);
    // The gate is advisory: the lookup still finds the key.
    assert_eq!(get(&dev, footer, b"k").unwrap(), Some(b"v".to_vec()));
    assert_eq!(get(&dev, footer, b"missing").unwrap(), None);
}

#[test]
fn empty_key_rejected() {
    let items = [entry(b"", b"v", 1)];
    let plan = plan_table::<core::convert::Infallible, BLOCK, KEY_MAX>(items.iter().copied());
    assert!(matches!(plan, Err(Error::EmptyKey)));
}

/// Recomputes a block's trailing CRC32 after test-controlled mutation, so
/// the block stays CRC-valid.
fn reseal(blk: &mut [u8; BLOCK]) {
    let crc = horton::crc32(&blk[..BLOCK - 4]);
    blk[BLOCK - 4..].copy_from_slice(&crc.to_le_bytes());
}

/// An index block's trailer (ADR-0034) that does not describe its
/// entries, under a valid CRC, is corruption, not a silent miss: here its
/// one restart offset points past the entry. The padding between the
/// entries and the trailer is never read, so garbage there changes no
/// answer.
#[test]
fn crc_valid_index_garbage_is_an_error() {
    let mut dev = MemDevice::<BLOCK>::new();
    let items = [entry(b"k", b"v", 1)];
    let (n, data_blocks) = write(&items, &mut dev);
    let index_id = BASE + data_blocks + 1;
    let footer = BASE + n - 1;
    // The trailer: one restart offset (0) and the entry count (1), ending
    // at the CRC.
    let body = BLOCK - 4;
    let good = read_block(&dev, index_id);
    assert_eq!(&good[body - 4..body], &[0, 0, 1, 0]);

    let mut blk = good;
    blk[body - 4] = 19; // past the single 19-byte entry
    reseal(&mut blk);
    write_block(&mut dev, index_id, &blk);
    let err = get(&dev, footer, b"k").unwrap_err();
    assert!(matches!(err, Error::CorruptBlock { id } if id == index_id));

    // The single index entry is 2 + 1 + 8 + 8 = 19 bytes; offset 100 is
    // inside the padding, which no search reads.
    let mut blk = good;
    assert_eq!(blk[100], 0);
    blk[100] = 0xAB;
    reseal(&mut blk);
    write_block(&mut dev, index_id, &blk);
    assert_eq!(get(&dev, footer, b"k").unwrap(), Some(b"v".to_vec()));
}

#[test]
fn crc_valid_data_garbage_is_an_error() {
    let mut dev = MemDevice::<BLOCK>::new();
    let items = [entry(b"k", b"v", 1)];
    let (n, data_blocks) = write(&items, &mut dev);
    assert_eq!(data_blocks, 1);
    let footer = BASE + n - 1;
    // The entry is 14 + 1 + 1 = 16 bytes; the 4-byte restart tail sits at
    // the end, so offset 100 is inside the zero padding. Nonzero garbage
    // there with a valid CRC must read as corruption, not as a miss.
    let mut blk = read_block(&dev, BASE);
    assert_eq!(blk[100], 0);
    blk[100] = 0xAB;
    reseal(&mut blk);
    write_block(&mut dev, BASE, &blk);
    // Break the bloom block's CRC so the gate is skipped and the scan runs;
    // look up a key past the entry so the scan walks into the garbage.
    let bloom_id = BASE + data_blocks;
    let mut bloom_blk = read_block(&dev, bloom_id);
    bloom_blk[0] ^= 0xFF;
    write_block(&mut dev, bloom_id, &bloom_blk);
    let err = get(&dev, footer, b"kk").unwrap_err();
    assert!(matches!(err, Error::CorruptBlock { id } if id == BASE));
}

#[test]
fn bloom_false_positive_rate_is_low() {
    use horton::sstable::bloom_maybe_contains;

    // 800 keys in 8192 filter bits: ~10.2 bits/key, k = 7, theory FPR ~0.7%.
    let n = 800u32;
    let keys: Vec<[u8; 8]> = (0..n).map(|i| (u64::from(i) * 2).to_be_bytes()).collect();
    let items: Vec<SstEntry<'_>> = keys
        .iter()
        .enumerate()
        .map(|(i, k)| entry(k, b"v", i as u64 + 1))
        .collect();
    let mut dev = MemDevice::<BLOCK>::new();
    let (_, data_blocks) = write(&items, &mut dev);
    let bloom_id = BASE + data_blocks;
    let blk = read_block(&dev, bloom_id);
    let filter = &blk[..BLOOM_BYTES];
    let k = bloom_k(BLOOM_BYTES * 8, u64::from(n));

    // No false negatives on the inserted (even) keys.
    for key in &keys {
        assert!(
            bloom_maybe_contains(filter, key, k),
            "false negative for {key:?}"
        );
    }
    // Measured FPR over 20k absent (odd) keys: theory ~0.7%, bound 5%
    // (~70 sigma of slack — a failure means the filter is broken).
    let probes = 20_000u32;
    let mut hits = 0u32;
    for i in 0..probes {
        let key = (u64::from(i) * 2 + 1).to_be_bytes();
        if bloom_maybe_contains(filter, &key, k) {
            hits += 1;
        }
    }
    let fpr = f64::from(hits) / f64::from(probes);
    assert!(
        fpr < 0.05,
        "measured bloom FPR {fpr:.4} (hits {hits}/{probes})"
    );
}

/// Point lookup with a `max_seq` watermark.
fn get_at(
    dev: &MemDevice<BLOCK>,
    footer: u64,
    key: &[u8],
    max_seq: u64,
) -> Result<Option<Vec<u8>>, Error<core::convert::Infallible>> {
    let mut scratch = [0u8; BLOCK];
    let mut decomp = [0u8; BLOCK];
    let mut buf = [0u8; 2048];
    let reader = block_on(TableReader::<MemDevice<BLOCK>, BLOCK, BLOOM_BYTES>::open(
        dev,
        &mut scratch,
        footer,
    ))?;
    let n = block_on(reader.get_at(&mut scratch, &mut decomp, key, &mut buf, max_seq))?;
    Ok(n.map(|n| buf[..n].to_vec()))
}

/// RED (v0.5 audit): a key's version run straddling data blocks. The index
/// must resolve to the FIRST block of the run (which holds the newest
/// versions), and a `max_seq` hiding every version in the first block must
/// continue the run into the next block — not report the key missing.
#[test]
fn version_run_crossing_blocks() {
    const VERSIONS: u64 = 500;
    let mut dev = MemDevice::<BLOCK>::new();
    // `a` (3 versions) then `k` (500 versions, seqs 1..=500) then `z`:
    // key-ascending, seq-descending within each key. All value bytes are
    // built first so the entries can borrow them immutably.
    let mut vals: Vec<Vec<u8>> = Vec::new();
    for s in (2000..=2002).rev() {
        vals.push(format!("a{s}").into_bytes());
    }
    for v in (1..=VERSIONS).rev() {
        vals.push(format!("k{v:03}").into_bytes());
    }
    vals.push(b"z1".to_vec());
    let mut items: Vec<SstEntry<'_>> = Vec::new();
    for (i, s) in (2000..=2002).rev().enumerate() {
        items.push(SstEntry {
            key: b"a",
            val: &vals[i],
            seq: s,
            tombstone: false,
            expire_at: 0,
        });
    }
    for (i, v) in (1..=VERSIONS).rev().enumerate() {
        items.push(SstEntry {
            key: b"k",
            val: &vals[3 + i],
            seq: v,
            tombstone: false,
            expire_at: 0,
        });
    }
    items.push(SstEntry {
        key: b"z",
        val: &vals[3 + 500], // 3 `a` values + 500 `k` versions precede `z`
        seq: 3000,
        tombstone: false,
        expire_at: 0,
    });
    let (n, data_blocks) = write(&items, &mut dev);
    assert!(
        data_blocks >= 2,
        "the run must straddle data blocks for this test to mean anything"
    );
    let footer = BASE + n - 1;
    let want = |v: u64| Some(format!("k{v:03}").into_bytes());

    // Live view: the newest version, which lives in the run's FIRST block.
    assert_eq!(
        get_at(&dev, footer, b"k", u64::MAX).unwrap(),
        want(VERSIONS)
    );
    // Watermarks hiding the first block's versions must continue the run
    // into later blocks.
    for s in [1u64, 7, 63, 120, 250, 499] {
        assert_eq!(
            get_at(&dev, footer, b"k", s).unwrap(),
            want(s),
            "max_seq={s}"
        );
    }
    // Neighbors are unaffected.
    assert_eq!(
        get_at(&dev, footer, b"a", u64::MAX).unwrap(),
        Some(b"a2002".to_vec())
    );
    assert_eq!(
        get_at(&dev, footer, b"z", u64::MAX).unwrap(),
        Some(b"z1".to_vec())
    );
    assert_eq!(get_at(&dev, footer, b"m", u64::MAX).unwrap(), None);
}

// ---------------------------------------------------------------------------
// v0.20: the bucketed bloom filter (`hrtsst03`).
// ---------------------------------------------------------------------------

/// Every probe of a key lies in one 64-byte bucket of the filter, and a
/// reader holding only that bucket decides exactly as one holding the
/// whole filter. This is what lets a point read fetch 64 bytes of the
/// bloom block instead of all of it.
#[test]
fn a_bloom_bucket_alone_answers_like_the_whole_filter() {
    use horton::sstable::{BLOOM_BUCKET_BYTES, BloomProbe, bloom_maybe_contains};

    let n = 600u32;
    let keys: Vec<Vec<u8>> = (0..n)
        .map(|i| format!("bucket-{:05}", i * 2).into_bytes())
        .collect();
    let items: Vec<SstEntry<'_>> = keys
        .iter()
        .enumerate()
        .map(|(i, k)| entry(k, b"v", i as u64 + 1))
        .collect();
    let mut dev = MemDevice::<BLOCK>::new();
    let (_, data_blocks) = write(&items, &mut dev);
    let blk = read_block(&dev, BASE + data_blocks);
    let filter = &blk[..BLOOM_BYTES];
    let k = bloom_k(BLOOM_BYTES * 8, u64::from(n));

    let mut used = std::collections::BTreeSet::new();
    for i in 0..n * 2 {
        let key = format!("bucket-{i:05}").into_bytes();
        let probe = BloomProbe::new(BLOOM_BYTES, &key);
        let r = probe.range();
        assert_eq!(r.len(), BLOOM_BUCKET_BYTES);
        assert_eq!(r.start % BLOOM_BUCKET_BYTES, 0, "buckets do not overlap");
        assert!(r.end <= BLOOM_BYTES);
        used.insert(r.start);
        let whole = bloom_maybe_contains(filter, &key, k);
        assert_eq!(probe.matches(&filter[r], k), whole, "key {i}");
        if i % 2 == 0 {
            assert!(whole, "false negative for inserted key {i}");
        }
    }
    // The keys spread over every bucket of the filter.
    assert_eq!(used.len(), BLOOM_BYTES / BLOOM_BUCKET_BYTES);
}

/// Filters smaller than a bucket, and sizes that are not a multiple of
/// one, keep the two bloom guarantees: probes stay inside the filter and
/// an inserted key is never reported absent. (The ESP32-S3 profile's
/// 64-byte filter is exactly one bucket.)
#[test]
fn odd_bloom_sizes_have_no_false_negatives() {
    use horton::sstable::{BloomProbe, bloom_maybe_contains};

    fn check<const BLOOM: usize>() {
        let keys: Vec<Vec<u8>> = (0..40u32)
            .map(|i| format!("odd-{i:03}").into_bytes())
            .collect();
        let items: Vec<SstEntry<'_>> = keys
            .iter()
            .enumerate()
            .map(|(i, k)| entry(k, b"v", i as u64 + 1))
            .collect();
        let mut dev = MemDevice::<BLOCK>::new();
        let plan =
            plan_table::<core::convert::Infallible, BLOCK, KEY_MAX>(items.iter().copied()).unwrap();
        let k = bloom_k(BLOOM * 8, plan.entry_count);
        let n = block_on(write_table::<MemDevice<BLOCK>, BLOCK, BLOOM, KEY_MAX>(
            &mut dev,
            BASE,
            k,
            items.iter().copied(),
            None,
        ))
        .unwrap();
        let blk = read_block(&dev, BASE + plan.data_blocks);
        for key in &keys {
            let probe = BloomProbe::new(BLOOM, key);
            assert!(probe.range().end <= BLOOM, "{BLOOM}-byte filter");
            assert!(probe.matches(&blk[probe.range()], k), "{BLOOM}-byte filter");
            assert!(bloom_maybe_contains(&blk[..BLOOM], key, k));
        }
        // And through a reader built for this filter size.
        let mut scratch = [0u8; BLOCK];
        let mut decomp = [0u8; BLOCK];
        let mut buf = [0u8; 64];
        let reader = block_on(TableReader::<MemDevice<BLOCK>, BLOCK, BLOOM>::open(
            &dev,
            &mut scratch,
            BASE + n - 1,
        ))
        .unwrap();
        for key in &keys {
            let got = block_on(reader.get(&mut scratch, &mut decomp, key, &mut buf)).unwrap();
            assert_eq!(got, Some(1), "{BLOOM}-byte filter lost {key:?}");
        }
    }
    check::<1>();
    check::<7>();
    check::<63>();
    check::<64>();
    check::<65>();
    check::<100>();
    check::<{ BLOCK - 4 }>();
}

/// A table written before v0.20 carries the unbucketed filter under the
/// magic "hrtsst02". Read by today's rule it would report keys it holds
/// as absent, so its footer is refused outright. So is "hrtsst03", an
/// unreleased v0.20 footer with no filter length in it, and "hrtsst04",
/// an unreleased v0.20 table whose index block has no trailer of
/// restart offsets (ADR-0034): an archive exported by such a build is
/// not ingested.
#[test]
fn a_table_with_the_old_magic_is_rejected() {
    let mut dev = MemDevice::<BLOCK>::new();
    let items = [entry(b"k", b"v", 1)];
    let (n, _) = write(&items, &mut dev);
    let footer = BASE + n - 1;
    assert_eq!(get(&dev, footer, b"k").unwrap(), Some(b"v".to_vec()));
    for old in [b"hrtsst04", b"hrtsst03", b"hrtsst02", b"lsmtable"] {
        let mut blk = read_block(&dev, footer);
        blk[..8].copy_from_slice(old);
        reseal(&mut blk);
        write_block(&mut dev, footer, &blk);
        let err = get(&dev, footer, b"k").unwrap_err();
        assert!(
            matches!(err, Error::CorruptBlock { id } if id == footer),
            "{:?}: {err:?}",
            core::str::from_utf8(old)
        );
    }
}

// ---------------------------------------------------------------------------
// v0.20: a filter may span blocks (`hrtsst04`).
// ---------------------------------------------------------------------------

mod spanning {
    use std::cell::RefCell;
    use std::task::{Context, Poll};

    use super::{BASE, BLOCK, KEY_MAX, entry, read_block, reseal, write_block};
    use crate::common::{MemDevice, block_on};
    use horton::sstable::{
        BLOOM_BITS_PER_KEY, BLOOM_BUCKET_BYTES, BloomShape, SstEntry, TableReader, TableWriter,
        write_table,
    };
    use horton::{BlockDevice, Error};

    /// Buckets in a 4 KiB bloom block.
    const PER_BLOCK: usize = (BLOCK - 4) / BLOOM_BUCKET_BYTES;
    /// A filter of up to eight blocks.
    const WIDE: usize = 8 * PER_BLOCK * BLOOM_BUCKET_BYTES;

    /// Records the id of every block read.
    struct Reads {
        inner: MemDevice<BLOCK>,
        log: RefCell<Vec<u64>>,
    }

    impl BlockDevice for Reads {
        type Error = core::convert::Infallible;
        const BLOCK: usize = BLOCK;

        fn poll_read_block(
            &self,
            cx: &mut Context<'_>,
            id: u64,
            buf: &mut [u8],
        ) -> Poll<Result<(), Self::Error>> {
            self.log.borrow_mut().push(id);
            self.inner.poll_read_block(cx, id, buf)
        }

        fn poll_write_block(
            &mut self,
            cx: &mut Context<'_>,
            id: u64,
            buf: &[u8],
        ) -> Poll<Result<(), Self::Error>> {
            self.inner.poll_write_block(cx, id, buf)
        }

        fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.inner.poll_flush(cx)
        }
    }

    fn key(i: u32) -> Vec<u8> {
        format!("w{i:06}").into_bytes()
    }

    /// What a table of `n` keys (the even numbers below `2 * n`) came
    /// out as.
    struct Written {
        dev: MemDevice<BLOCK>,
        blocks: u64,
        data_blocks: u64,
        shape: BloomShape,
    }

    impl Written {
        const fn footer(&self) -> u64 {
            BASE + self.blocks - 1
        }

        const fn bloom_first(&self) -> u64 {
            BASE + self.data_blocks
        }
    }

    fn write_wide<const BLOOM: usize>(n: u32) -> Written {
        let keys: Vec<Vec<u8>> = (0..n).map(|i| key(i * 2)).collect();
        let items: Vec<SstEntry<'_>> = keys
            .iter()
            .enumerate()
            .map(|(i, k)| entry(k, b"v", i as u64 + 1))
            .collect();
        let mut dev = MemDevice::<BLOCK>::new();
        let k = TableWriter::<BLOCK, BLOOM, KEY_MAX>::probes_for(u64::from(n));
        let blocks = block_on(write_table::<MemDevice<BLOCK>, BLOCK, BLOOM, KEY_MAX>(
            &mut dev,
            BASE,
            k,
            items.iter().copied(),
            None,
        ))
        .unwrap();
        let shape = BloomShape::for_entries(BLOOM, BLOCK, u64::from(n));
        let data_blocks = blocks - 2 - u64::from(shape.blocks);
        Written {
            dev,
            blocks,
            data_blocks,
            shape,
        }
    }

    type Reader<'d, D> = TableReader<'d, D, BLOCK, WIDE>;

    fn lookup<D: BlockDevice>(reader: &Reader<'_, D>, key: &[u8]) -> Result<bool, Error<D::Error>> {
        let mut scratch = [0u8; BLOCK];
        let mut decomp = [0u8; BLOCK];
        let mut buf = [0u8; 8];
        Ok(block_on(reader.get(&mut scratch, &mut decomp, key, &mut buf))?.is_some())
    }

    /// A table gets the fewest bloom blocks that give its entries
    /// `BLOOM_BITS_PER_KEY` bits each, the footer says how much filter
    /// that is, and a lookup reads one bloom block: the one holding the
    /// key's bucket. No key is lost, and the filter still filters.
    #[test]
    fn a_table_stores_the_filter_its_entry_count_calls_for() {
        let mut seen_blocks = std::collections::BTreeSet::new();
        for n in [0u32, 1, 500, 3_300, 8_000, 12_000, 28_000] {
            let t = write_wide::<WIDE>(n);
            seen_blocks.insert(t.shape.blocks);
            let dev = Reads {
                inner: t.dev.clone(),
                log: RefCell::new(Vec::new()),
            };
            let mut scratch = [0u8; BLOCK];
            let reader = block_on(Reader::open(&dev, &mut scratch, t.footer())).unwrap();
            assert_eq!(reader.entry_count(), u64::from(n));
            assert_eq!(reader.bloom_len(), t.shape.len, "{n} entries");
            assert_eq!(reader.bloom_blocks(), u64::from(t.shape.blocks));
            let bits = t.shape.len as u64 * 8;
            assert!(
                t.shape.blocks == 8 || bits >= u64::from(n) * BLOOM_BITS_PER_KEY,
                "{n} entries got {bits} bits"
            );

            let bloom = t.bloom_first()..t.bloom_first() + u64::from(t.shape.blocks);
            let index = bloom.end;
            // Every key is found, through exactly one bloom block.
            for i in (0..n).step_by((n as usize / 800).max(1)) {
                dev.log.borrow_mut().clear();
                assert!(
                    lookup(&reader, &key(i * 2)).unwrap(),
                    "{n} entries lost {i}"
                );
                let log = dev.log.borrow();
                assert_eq!(log.iter().filter(|id| bloom.contains(id)).count(), 1);
                assert!(log.contains(&index));
            }
            // Absent keys inside the table's range: the filter turns
            // nearly all of them away after that one block.
            let (mut passed, mut asked) = (0u32, 0u32);
            let mut touched = std::collections::BTreeSet::new();
            for i in (0..n).step_by((n as usize / 2_000).max(1)) {
                dev.log.borrow_mut().clear();
                assert!(!lookup(&reader, &key(i * 2 + 1)).unwrap());
                let log = dev.log.borrow();
                let blooms: Vec<u64> = log
                    .iter()
                    .copied()
                    .filter(|id| bloom.contains(id))
                    .collect();
                assert_eq!(blooms.len(), 1);
                touched.insert(blooms[0]);
                asked += 1;
                passed += u32::from(log.contains(&index));
            }
            if n >= 3_300 {
                assert!(
                    passed * 100 <= asked * 3,
                    "{n} entries: {passed} of {asked} absent keys passed the filter"
                );
                assert_eq!(
                    u32::try_from(touched.len()).unwrap(),
                    t.shape.blocks,
                    "keys spread over every bloom block"
                );
            }
        }
        // The sizes above span one block to the full eight.
        assert_eq!(seen_blocks.into_iter().collect::<Vec<_>>(), [1, 2, 3, 4, 8]);
    }

    /// The filter a wide configuration folds a table's keys into is, byte
    /// for byte, the filter a configuration of exactly that size builds.
    #[test]
    fn a_folded_table_filter_equals_the_one_built_at_that_size() {
        /// Two blocks' worth: what 6,000 entries fold an eight-block
        /// filter to.
        const TWO: usize = 2 * PER_BLOCK * BLOOM_BUCKET_BYTES;
        let wide = write_wide::<WIDE>(6_000);
        let exact = write_wide::<TWO>(6_000);
        assert_eq!(wide.shape.len, TWO);
        assert_eq!(wide.shape, exact.shape);
        assert_eq!(wide.blocks, exact.blocks);
        for i in 0..wide.blocks - 1 {
            // Every block but the footer, which names the same layout.
            assert_eq!(
                read_block(&wide.dev, BASE + i)[..],
                read_block(&exact.dev, BASE + i)[..],
                "block {i}"
            );
        }
        assert_eq!(
            read_block(&wide.dev, wide.footer())[..],
            read_block(&exact.dev, exact.footer())[..]
        );
    }

    /// A compaction job reuses one writer for all its outputs, restarted
    /// in place. Whatever the table before left in it (filter bits, index
    /// bytes, counters, a block limit), the next table comes out as a new
    /// writer would have written it.
    #[test]
    fn a_restarted_writer_writes_what_a_new_one_does() {
        fn push_keys(
            w: &mut TableWriter<BLOCK, WIDE, KEY_MAX>,
            dev: &mut MemDevice<BLOCK>,
            keys: impl Iterator<Item = u32>,
        ) -> u64 {
            for (seq, i) in keys.enumerate() {
                block_on(w.push(dev, entry(&key(i), b"v", seq as u64 + 1), None)).unwrap();
            }
            let done = block_on(w.finish(dev, None)).unwrap();
            done.data_blocks + u64::from(done.bloom_blocks) + 2
        }
        let k = TableWriter::<BLOCK, WIDE, KEY_MAX>::probes_for(4_000);

        let mut fresh_dev = MemDevice::<BLOCK>::new();
        let mut fresh = Box::new(TableWriter::<BLOCK, WIDE, KEY_MAX>::new(BASE, k));
        let blocks = push_keys(&mut fresh, &mut fresh_dev, (0..4_000).map(|i| i * 3));

        // The same table from a writer that has just written a larger
        // one, with other keys, another probe count and a block limit.
        let mut used_dev = MemDevice::<BLOCK>::new();
        let mut used = Box::new(TableWriter::<BLOCK, WIDE, KEY_MAX>::new(BASE + 5_000, 3));
        used.set_block_limit(1_000);
        push_keys(&mut used, &mut used_dev, (0..9_000).map(|i| i * 7 + 1));
        used.restart(BASE, k);
        // An empty index is its entry count: two bytes of trailer.
        assert_eq!(
            (used.entry_count(), used.data_blocks(), used.index_len()),
            (0, 0, horton::sstable::index_tail(0))
        );
        assert_eq!(
            push_keys(&mut used, &mut used_dev, (0..4_000).map(|i| i * 3)),
            blocks
        );
        for i in 0..blocks {
            assert_eq!(
                read_block(&used_dev, BASE + i)[..],
                read_block(&fresh_dev, BASE + i)[..],
                "block {i}"
            );
        }
        // The block limit did not carry over: a restarted writer is
        // unbounded until it is told otherwise.
        used.restart(BASE, k);
        used.set_block_limit(1);
        let mut err = None;
        for i in 0..4_000u32 {
            if let Err(e) = block_on(used.push(&mut used_dev, entry(&key(i), b"v", 1), None)) {
                err = Some(e);
                break;
            }
        }
        assert!(matches!(err, Some(Error::TableTooLarge)), "{err:?}");
    }

    /// A caller whose `finish_meta` future was dropped at a device wait
    /// calls it again (a resumed compaction step does). The second call
    /// must write the table the first would have: a filter folded twice
    /// has its bits in buckets no key hashes to, and reports keys the
    /// table holds as absent.
    #[test]
    fn finishing_a_table_twice_writes_the_same_table() {
        let n = 6_000u32;
        let once = write_wide::<WIDE>(n);
        assert_eq!(once.shape.blocks, 2, "a filter that is folded");

        let mut dev = MemDevice::<BLOCK>::new();
        let k = TableWriter::<BLOCK, WIDE, KEY_MAX>::probes_for(u64::from(n));
        let mut w = Box::new(TableWriter::<BLOCK, WIDE, KEY_MAX>::new(BASE, k));
        for i in 0..n {
            block_on(w.push(&mut dev, entry(&key(i * 2), b"v", u64::from(i) + 1), None)).unwrap();
        }
        block_on(w.seal_data(&mut dev, None)).unwrap();
        let first = block_on(w.finish_meta(&mut dev, 0, u64::MAX)).unwrap();
        let second = block_on(w.finish_meta(&mut dev, 0, u64::MAX)).unwrap();
        assert_eq!(first.bloom_blocks, second.bloom_blocks);
        for i in 0..once.blocks {
            assert_eq!(
                read_block(&dev, BASE + i)[..],
                read_block(&once.dev, BASE + i)[..],
                "block {i}"
            );
        }
        let mut scratch = [0u8; BLOCK];
        let reader = block_on(Reader::open(&dev, &mut scratch, once.footer())).unwrap();
        for i in (0..n).step_by(3) {
            assert!(lookup(&reader, &key(i * 2)).unwrap(), "lost key {i}");
        }
    }

    /// A bloom block that fails its CRC disables the filter for the keys
    /// whose bucket it holds and for no others. Nothing is reported
    /// absent that is present, and nothing errors.
    #[test]
    fn one_bad_bloom_block_only_stops_filtering_its_own_keys() {
        let n = 12_000u32;
        let mut t = write_wide::<WIDE>(n);
        assert_eq!(t.shape.blocks, 4);
        let bad = t.bloom_first() + 2;
        let mut blk = read_block(&t.dev, bad);
        blk[100] ^= 0x40;
        write_block(&mut t.dev, bad, &blk);

        let dev = Reads {
            inner: t.dev.clone(),
            log: RefCell::new(Vec::new()),
        };
        let mut scratch = [0u8; BLOCK];
        let reader = block_on(Reader::open(&dev, &mut scratch, t.footer())).unwrap();
        let index = t.bloom_first() + 4;
        for i in (0..n).step_by(11) {
            assert!(lookup(&reader, &key(i * 2)).unwrap(), "lost key {i}");
        }
        let (mut through_bad, mut filtered) = (0u32, 0u32);
        for i in (0..n).step_by(6) {
            dev.log.borrow_mut().clear();
            assert!(!lookup(&reader, &key(i * 2 + 1)).unwrap());
            let log = dev.log.borrow();
            if log.contains(&bad) {
                // Unfiltered: the lookup went on to the index.
                assert!(log.contains(&index));
                through_bad += 1;
            } else if !log.contains(&index) {
                filtered += 1;
            }
        }
        assert!(
            through_bad > 300,
            "{through_bad} absent keys hashed to the bad block"
        );
        assert!(
            filtered > 1_200,
            "{filtered} absent keys were still filtered"
        );
    }

    /// The footer's filter length and its two block ids must agree. A
    /// footer that claims more or less filter than the section holds is
    /// corrupt: a probe must never read an index or data block as filter.
    #[test]
    fn a_footer_whose_filter_length_disagrees_with_its_blocks_is_refused() {
        let t = write_wide::<WIDE>(6_000);
        assert_eq!(t.shape.blocks, 2);
        let footer = t.footer();
        let good = read_block(&t.dev, footer);
        assert_eq!(
            u32::from_le_bytes(good[45..49].try_into().unwrap()) as usize,
            t.shape.len
        );
        for claimed in [
            0u32,
            64,
            u32::try_from(PER_BLOCK * BLOOM_BUCKET_BYTES).unwrap(),
            u32::try_from(4 * PER_BLOCK * BLOOM_BUCKET_BYTES).unwrap(),
            u32::MAX,
        ] {
            let mut dev = t.dev.clone();
            let mut blk = good;
            blk[45..49].copy_from_slice(&claimed.to_le_bytes());
            reseal(&mut blk);
            write_block(&mut dev, footer, &blk);
            let mut scratch = [0u8; BLOCK];
            let err = block_on(Reader::open(&dev, &mut scratch, footer))
                .err()
                .unwrap_or_else(|| panic!("a footer claiming {claimed} filter bytes opened"));
            assert!(matches!(err, Error::CorruptBlock { id } if id == footer));
        }
        // And the index pointer moved off the end of the bloom section.
        let mut dev = t.dev;
        let mut blk = good;
        let index = u64::from_le_bytes(blk[8..16].try_into().unwrap());
        blk[8..16].copy_from_slice(&(index + 1).to_le_bytes());
        reseal(&mut blk);
        write_block(&mut dev, footer, &blk);
        let mut scratch = [0u8; BLOCK];
        assert!(matches!(
            block_on(Reader::open(&dev, &mut scratch, footer)),
            Err(Error::CorruptBlock { id }) if id == footer
        ));
    }
}
