//! `SSTable` tests: round trips, bloom behavior, and corruption semantics.

mod common;

use std::future::poll_fn;

use common::{block_on, MemDevice};
use horton::sstable::{bloom_k, plan_table, write_table, SstEntry, TableReader};
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
    }
}

/// Writes `items` (key-ascending) as one table at `BASE`; returns
/// `(blocks_written, data_blocks)`.
fn write(items: &[SstEntry<'_>], dev: &mut MemDevice<BLOCK>) -> (u64, u64) {
    let plan =
        plan_table::<core::convert::Infallible, BLOCK, KEY_MAX>(items.iter().copied()).unwrap();
    let k = bloom_k(BLOOM_BYTES * 8, plan.entry_count);
    let mut data = [0u8; BLOCK];
    let mut index = [0u8; BLOCK];
    let mut bloom = [0u8; BLOOM_BYTES];
    let n = block_on(write_table::<MemDevice<BLOCK>, BLOCK, BLOOM_BYTES>(
        dev,
        BASE,
        k,
        items.iter().copied(),
        &mut data,
        &mut index,
        &mut bloom,
    ))
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
    let mut buf = [0u8; 2048];
    let reader = block_on(TableReader::<MemDevice<BLOCK>, BLOCK, BLOOM_BYTES>::open(
        dev,
        &mut scratch,
        footer,
    ))?;
    let n = block_on(reader.get(&mut scratch, key, &mut buf))?;
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
fn corrupt_data_block_reads_as_absent() {
    let mut dev = MemDevice::<BLOCK>::new();
    let items = [entry(b"k", b"v", 1), entry(b"k2", b"v2", 2)];
    let (n, data_blocks) = write(&items, &mut dev);
    assert_eq!(data_blocks, 1);
    let footer = BASE + n - 1;
    // Corrupt the only data block: the key is reported absent, not an error.
    let mut blk = read_block(&dev, BASE);
    blk[0] ^= 0xFF;
    write_block(&mut dev, BASE, &blk);
    assert_eq!(get(&dev, footer, b"k").unwrap(), None);
    assert_eq!(get(&dev, footer, b"k2").unwrap(), None);
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
