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

#[test]
fn crc_valid_index_garbage_is_an_error() {
    let mut dev = MemDevice::<BLOCK>::new();
    let items = [entry(b"k", b"v", 1)];
    let (n, data_blocks) = write(&items, &mut dev);
    let index_id = BASE + data_blocks + 1;
    let footer = BASE + n - 1;
    // The single index entry is 2 + 1 + 8 + 8 = 19 bytes; offset 100 is
    // inside the zero padding. Nonzero garbage there with a valid CRC must
    // read as corruption, not as a silent end of the index.
    let mut blk = read_block(&dev, index_id);
    assert_eq!(blk[100], 0);
    blk[100] = 0xAB;
    reseal(&mut blk);
    write_block(&mut dev, index_id, &blk);
    let err = get(&dev, footer, b"k").unwrap_err();
    assert!(matches!(err, Error::CorruptBlock { id } if id == index_id));
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
