//! Decoder fuzzing (v0.9): structure-aware, deterministic, in-tree, no
//! dependencies.
//!
//! A seeded LCG mutates valid encoded inputs — WAL record streams, `SSTable`
//! blocks, manifest blocks — with bit flips, byte smears, splices, and
//! truncations. The contract under test: decoders never panic on arbitrary
//! bytes. Corrupt input must surface as a clean `Error` or, for WAL
//! recovery, as a torn tail that ends the scan. A panic fails the test,
//! which *is* the assertion; the unmutated corpus is verified intact first
//! so the fuzzer cannot pass vacuously.
//!
//! The `SSTable` mutations are CRC-aware: after mutating a block's payload
//! the trailing CRC32 is recomputed, so the mutated block *passes*
//! `check_block_crc` and the entry/index parsers actually run on garbage.
//! Without that, every mutation would bounce off the CRC gate and the
//! parsers would never be exercised.

mod common;

use std::future::poll_fn;

use common::{Lcg, MemDevice, block_on};
use horton::manifest::{KeyBound, Manifest, TableRef};
use horton::{
    BlockDevice, MemTable, Op, SstEntry, TableReader, WalWriter, bloom_k, crc32, write_table,
};

const BLOCK: usize = 4096;
/// Mutations per corpus. Deterministic: iteration `i` uses `Lcg(SEED + i)`.
const ITERS: usize = 300;
const SEED: u64 = 0x9E37_79B9_7F4A_7C15;

type DevError = core::convert::Infallible;
type TestManifest = Manifest<2, 4, 256>;

/// Applies one random structural mutation to the block list.
///
/// The `u64` → `usize`/`u8` casts below are PRNG output folded into
/// bounded ranges via `%`; truncation is intentional, not a bug.
#[allow(clippy::cast_possible_truncation)]
fn mutate(rng: &mut Lcg, bufs: &mut [[u8; BLOCK]], fix_crc: bool) {
    let n = bufs.len();
    if n == 0 {
        return;
    }
    let buf = &mut bufs[(rng.next() as usize) % n];
    match rng.next() % 4 {
        // Bit flips: 1..8 random bits anywhere in the block.
        0 => {
            let flips = 1 + (rng.next() % 8) as usize;
            for _ in 0..flips {
                let bit = (rng.next() as usize) % (BLOCK * 8);
                buf[bit / 8] ^= 1 << (bit % 8);
            }
        }
        // Byte smear: a random span filled with random bytes.
        1 => {
            let start = (rng.next() as usize) % BLOCK;
            let max_len = BLOCK - start;
            let len = 1 + (rng.next() as usize) % max_len;
            for b in &mut buf[start..start + len] {
                *b = rng.next() as u8;
            }
        }
        // Truncation: zero a random suffix (a torn write).
        2 => {
            let cut = (rng.next() as usize) % (BLOCK + 1);
            buf[cut..].fill(0);
        }
        // Splice: copy a random span onto a random offset.
        _ => {
            let src = (rng.next() as usize) % BLOCK;
            let dst = (rng.next() as usize) % BLOCK;
            let max_len = (BLOCK - src).min(BLOCK - dst);
            if max_len > 0 {
                let len = 1 + (rng.next() as usize) % max_len;
                let tmp: Vec<u8> = buf[src..src + len].to_vec();
                buf[dst..dst + len].copy_from_slice(&tmp);
            }
        }
    }
    if fix_crc {
        // Recompute the trailing CRC32 so the block passes `check_block_crc`
        // and the parsers run on the mutated payload.
        let crc = crc32(&buf[..BLOCK - 4]);
        buf[BLOCK - 4..].copy_from_slice(&crc.to_le_bytes());
    }
}

fn read_block(dev: &MemDevice<BLOCK>, id: u64) -> [u8; BLOCK] {
    let mut buf = [0u8; BLOCK];
    block_on(poll_fn(|cx| dev.poll_read_block(cx, id, &mut buf))).unwrap();
    buf
}

/// Builds a WAL with 24 records (puts of varying sizes plus two deletes)
/// over blocks `[8, 40)`; returns the device and the pristine blocks.
fn wal_corpus() -> (MemDevice<BLOCK>, Vec<[u8; BLOCK]>) {
    let mut wal = WalWriter::<MemDevice<BLOCK>, BLOCK>::new(MemDevice::new(), 8, 40);
    for i in 0..24u64 {
        let key = format!("key{i:02}");
        let op = if i % 11 == 10 { Op::Delete } else { Op::Put };
        // Varying value sizes to spread records across block offsets.
        let val: Vec<u8> = (0..(i % 17) as u8).collect();
        block_on(wal.append(i + 1, op, key.as_bytes(), &val)).unwrap();
    }
    block_on(wal.commit()).unwrap();
    let mut dev = wal.into_device();
    let pristine = dev.blocks_mut().clone();
    (dev, pristine)
}

/// The unmutated corpus must recover exactly: 24 records, max seq 24.
#[test]
fn wal_corpus_is_valid() {
    let (dev, _) = wal_corpus();
    let mut wal = WalWriter::<MemDevice<BLOCK>, BLOCK>::new(dev, 8, 40);
    let mut table = MemTable::<64, 4096, 256, 1024>::new();
    let state = block_on(wal.recover(&mut table)).unwrap();
    assert_eq!(state.records, 24);
    assert_eq!(state.max_seq, 24);
}

/// Mutated WAL blocks must never panic recovery. Corrupt records are the
/// expected torn tail: recovery stops there and returns `Ok`.
#[test]
fn wal_decoder_never_panics() {
    let (_, pristine) = wal_corpus();
    for i in 0..ITERS {
        let mut rng = Lcg::new(SEED.wrapping_add(i as u64));
        let mut blocks = pristine.clone();
        // 1..3 mutations per iteration; WAL record CRCs are never repaired.
        for _ in 0..=i % 3 {
            mutate(&mut rng, &mut blocks, false);
        }
        let mut dev = MemDevice::<BLOCK>::new();
        dev.blocks_mut().extend_from_slice(&blocks);
        let mut wal = WalWriter::<MemDevice<BLOCK>, BLOCK>::new(dev, 8, 40);
        let mut table = MemTable::<64, 4096, 256, 1024>::new();
        // No panic is the assertion; any clean outcome is acceptable.
        let _ = block_on(wal.recover(&mut table));
    }
}

/// Builds one `SSTable` with 40 entries (including tombstones) at `base`;
/// returns the device, the pristine blocks, and the block count.
fn sstable_corpus(base: u64) -> (MemDevice<BLOCK>, Vec<[u8; BLOCK]>, u64) {
    let mut dev = MemDevice::<BLOCK>::new();
    let mut keys: Vec<Vec<u8>> = Vec::new();
    let mut vals: Vec<Vec<u8>> = Vec::new();
    for i in 0..40u8 {
        keys.push(format!("skey{i:02}").into_bytes());
        vals.push(vec![i; usize::from(i % 13)]);
    }
    let entries = keys
        .iter()
        .zip(vals.iter())
        .enumerate()
        .map(|(i, (k, v))| SstEntry {
            key: k,
            val: v,
            seq: i as u64 + 1,
            tombstone: i % 13 == 12,
        });
    let k = bloom_k(1024 * 8, 40);
    let nblocks = block_on(write_table::<_, BLOCK, 1024, 256>(
        &mut dev, base, k, entries,
    ))
    .unwrap();
    let pristine = dev.blocks_mut().clone();
    (dev, pristine, nblocks)
}

/// The unmutated table must open and serve every live key.
#[test]
fn sstable_corpus_is_valid() {
    let base = 136u64;
    let (dev, _, nblocks) = sstable_corpus(base);
    let mut scratch = [0u8; BLOCK];
    let reader = block_on(TableReader::<MemDevice<BLOCK>, BLOCK, 1024>::open(
        &dev,
        &mut scratch,
        base + nblocks - 1,
    ))
    .unwrap();
    let mut val_buf = [0u8; 1024];
    for i in 0..40u8 {
        let key = format!("skey{i:02}");
        let got = block_on(reader.get(&mut scratch, key.as_bytes(), &mut val_buf));
        if i % 13 == 12 {
            assert_eq!(got.unwrap(), None, "tombstone {i}");
        } else {
            let n = got.unwrap().unwrap();
            assert_eq!(&val_buf[..n], &vec![i; usize::from(i % 13)][..]);
        }
    }
}

/// Mutated `SSTable` blocks must never panic open/lookup. Half the iterations
/// repair the block CRC so the parsers run on garbage; the rest bounce off
/// the CRC gate. Either way the outcome is `Ok` or a clean `Error`.
#[test]
fn sstable_decoder_never_panics() {
    let base = 136u64;
    let (_, pristine, nblocks) = sstable_corpus(base);
    let footer = base + nblocks - 1;
    for i in 0..ITERS {
        let mut rng = Lcg::new(SEED.wrapping_add(0x1000 + i as u64));
        let mut blocks = pristine.clone();
        let fix_crc = i % 2 == 0;
        for _ in 0..=i % 3 {
            mutate(&mut rng, &mut blocks, fix_crc);
        }
        let mut dev = MemDevice::<BLOCK>::new();
        dev.blocks_mut().extend_from_slice(&blocks);
        let mut scratch = [0u8; BLOCK];
        let mut val_buf = [0u8; 1024];
        let open = block_on(TableReader::<MemDevice<BLOCK>, BLOCK, 1024>::open(
            &dev,
            &mut scratch,
            footer,
        ));
        if let Ok(reader) = open {
            for j in 0..40u64 {
                let key = format!("skey{j:02}");
                // No panic is the assertion; any clean outcome is acceptable.
                let _ = block_on(reader.get(&mut scratch, key.as_bytes(), &mut val_buf));
            }
        }
    }
}

fn tref(id: u32, first_block: u64) -> TableRef<256> {
    TableRef {
        id,
        first_block,
        block_count: 4,
        first_key: KeyBound::from_slice(b"a").unwrap(),
        last_key: KeyBound::from_slice(b"z").unwrap(),
        max_seq: 10,
        entry_count: 5,
    }
}

/// Builds a manifest with L0/L1 tables, commits it to slot 0, and returns
/// the raw block plus a pristine copy.
fn manifest_corpus() -> ([u8; BLOCK], [u8; BLOCK]) {
    let mut dev = MemDevice::<BLOCK>::new();
    let mut m = TestManifest::new();
    m.set_wal_head(8);
    m.add_l0_table::<DevError>(tref(0, 136)).unwrap();
    m.add_l0_table::<DevError>(tref(1, 140)).unwrap();
    m.add_table_to_level::<DevError>(1, tref(2, 200)).unwrap();
    let mut scratch = [0u8; BLOCK];
    block_on(m.commit(&mut dev, &mut scratch, 0, 1)).unwrap();
    // seq goes 0 -> 1 (odd), so the commit lands in slot_b == 1.
    let blk = read_block(&dev, 1);
    (blk, blk)
}

/// The unmutated manifest block must decode exactly.
#[test]
fn manifest_corpus_is_valid() {
    let (blk, _) = manifest_corpus();
    let m = TestManifest::decode::<DevError, BLOCK>(&blk).unwrap();
    assert_eq!(m.l0().len(), 2);
    assert_eq!(m.wal_head(), 8);
}

/// Mutated manifest blocks must never panic the decoder.
#[test]
fn manifest_decoder_never_panics() {
    let (_, pristine) = manifest_corpus();
    for i in 0..ITERS {
        let mut rng = Lcg::new(SEED.wrapping_add(0x2000 + i as u64));
        let mut blocks = [pristine];
        for _ in 0..=i % 3 {
            mutate(&mut rng, &mut blocks, false);
        }
        // No panic is the assertion; any clean outcome is acceptable.
        let _ = TestManifest::decode::<DevError, BLOCK>(&blocks[0]);
    }
}
