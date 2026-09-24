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
//!
//! Reliability-pass extensions (v0.16+):
//!
//! - WAL: beyond never-panics, recovery must replay a *clean prefix* of
//!   the record stream (`records == max_seq`, memtable matches a replay of
//!   exactly that prefix) — never garbage past the tear. Targeted torn
//!   tails (corrupt magic, mid-record truncation, corrupt length/CRC)
//!   pin the exact stop point.
//! - LZ77 (v0.13 codec): mutated and pure-garbage streams must decode to
//!   `Ok(4092)` or a typed `DecompressError`, never a panic; valid streams
//!   round-trip byte-identically.
//! - `SSTable` with compression enabled: CRC-repaired mutations hammer the
//!   inflate path (`inflate_data_block`) on garbage. Targeted block surgery
//!   covers corrupt restart trailers, bogus compression flags, corrupt
//!   bloom blocks (CRC-broken vs CRC-repaired), corrupt index entries, a
//!   zeroed footer `k`, and a lying `rdel_blocks` count (must fail fast,
//!   never hang the range-tombstone scan).
//! - Bloom filter pure functions on arbitrary bytes/lengths/`k`.
//! - Memtable insert path: adversarial key/value lengths (empty, `KEY_MAX`
//!   boundaries, oversize), duplicate-key storms, tombstone interleavings,
//!   and adversarial range-tombstone bounds — typed errors only, accepted
//!   inserts read back exactly.
//! - Block cache (v0.16): a poisoned cache entry must not bypass the read
//!   path's checks — the same poisoned bytes served from the cache and read
//!   straight from the device must produce identical outcomes.
//!
//! Under Miri (`cfg!(miri)`) the iteration counts shrink to a smoke run:
//! Miri is 50–100x slower, so the full counts are a CI job, not a Miri
//! job. The seeds are unchanged, so the Miri run is still deterministic.

mod common;

use std::collections::BTreeMap;
use std::future::poll_fn;

use common::{Lcg, MemDevice, block_on};
use horton::RecoverState;
use horton::compress::{CompressScratch, decompress};
use horton::manifest::{KeyBound, Manifest, TableRef};
use horton::{
    BlockCache, BlockDevice, CachePort, Error, MemTable, Op, SstEntry, SstLookup, TableReader,
    WalWriter, bloom_k, bloom_maybe_contains, crc32, write_table,
};

const BLOCK: usize = 4096;
/// Mutations per corpus. Deterministic: iteration `i` uses `Lcg(SEED + i)`.
/// Under Miri this shrinks to a smoke run (see the module docs).
const ITERS: usize = if cfg!(miri) { 5 } else { 300 };
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
            expire_at: 0,
        });
    let k = bloom_k(1024 * 8, 40);
    let nblocks = block_on(write_table::<_, BLOCK, 1024, 256>(
        &mut dev, base, k, entries, None,
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
    let mut decomp = [0u8; BLOCK];
    let reader = block_on(TableReader::<MemDevice<BLOCK>, BLOCK, 1024>::open(
        &dev,
        &mut scratch,
        base + nblocks - 1,
    ))
    .unwrap();
    let mut val_buf = [0u8; 1024];
    for i in 0..40u8 {
        let key = format!("skey{i:02}");
        let got = block_on(reader.get(&mut scratch, &mut decomp, key.as_bytes(), &mut val_buf));
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
        let mut decomp = [0u8; BLOCK];
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
                let _ =
                    block_on(reader.get(&mut scratch, &mut decomp, key.as_bytes(), &mut val_buf));
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
        min_seq: 0,
        entry_count: 5,
        rdel_blocks: 0,
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

// ---------------------------------------------------------------------------
// WAL torn-tail property: recovery replays a clean prefix, never garbage.
// ---------------------------------------------------------------------------

/// Replays the first `n` records of the WAL corpus into a fresh memtable:
/// the expected post-recovery state when the tear falls after record `n`.
fn replay_wal_prefix(n: usize) -> MemTable<64, 4096, 256, 1024> {
    let mut table = MemTable::<64, 4096, 256, 1024>::new();
    for i in 0..n as u64 {
        let key = format!("key{i:02}");
        let val: Vec<u8> = (0..(i % 17) as u8).collect();
        table
            .insert::<DevError>(key.as_bytes(), &val, i + 1, i % 11 == 10)
            .unwrap();
    }
    table
}

/// Compares two memtables' views of every corpus key: `(val, seq,
/// tombstone)` must agree.
fn memtables_equal(a: &MemTable<64, 4096, 256, 1024>, b: &MemTable<64, 4096, 256, 1024>) -> bool {
    for i in 0..24u64 {
        let key = format!("key{i:02}");
        let la = a
            .get(key.as_bytes())
            .map(|l| (l.val.to_vec(), l.seq, l.tombstone));
        let lb = b
            .get(key.as_bytes())
            .map(|l| (l.val.to_vec(), l.seq, l.tombstone));
        if la != lb {
            return false;
        }
    }
    true
}

/// Recovers `blocks` (a full device image) and returns the recovery state
/// plus the replayed memtable.
fn recover_blocks(blocks: &[[u8; BLOCK]]) -> (RecoverState, MemTable<64, 4096, 256, 1024>) {
    let mut dev = MemDevice::<BLOCK>::new();
    dev.blocks_mut().extend_from_slice(blocks);
    let mut wal = WalWriter::<MemDevice<BLOCK>, BLOCK>::new(dev, 8, 40);
    let mut table = MemTable::<64, 4096, 256, 1024>::new();
    let state = block_on(wal.recover(&mut table)).unwrap();
    (state, table)
}

/// Mutated WAL recovery must replay a clean prefix of the record stream:
/// corpus seqs are dense `1..=24`, so any torn tail cuts a prefix —
/// `records == max_seq` — and the recovered memtable must match a replay
/// of exactly that prefix. Replaying garbage past the tear fails here.
#[test]
fn wal_recovery_replays_clean_prefix() {
    let (_, pristine) = wal_corpus();
    for i in 0..ITERS {
        let mut rng = Lcg::new(SEED.wrapping_add(0x3000 + i as u64));
        let mut blocks = pristine.clone();
        for _ in 0..=i % 3 {
            mutate(&mut rng, &mut blocks, false);
        }
        let (state, table) = recover_blocks(&blocks);
        assert_eq!(
            state.records, state.max_seq,
            "iter {i}: torn tail did not cut a clean prefix"
        );
        let expect = replay_wal_prefix(usize::try_from(state.records).unwrap());
        assert!(
            memtables_equal(&table, &expect),
            "iter {i}: recovered garbage past the tear"
        );
    }
}

/// Byte offset of each of the 24 corpus records inside WAL block 8.
/// Record `i` is `23 + 5 + (i % 17)` bytes: header/trailer + key + value.
fn wal_record_offsets() -> [usize; 24] {
    let mut offs = [0usize; 24];
    let mut off = 0usize;
    for (i, o) in offs.iter_mut().enumerate() {
        *o = off;
        off += 23 + 5 + (i % 17);
    }
    offs
}

/// Targeted torn tails: corrupt first-record magic, zero the whole block,
/// truncate mid-record, corrupt a length field, corrupt a record CRC.
/// Recovery must stop exactly at the tear and return `Ok`.
#[test]
fn wal_targeted_torn_tails() {
    let (_, pristine) = wal_corpus();
    let offs = wal_record_offsets();
    // Sanity: the first record really does start with the WAL magic.
    assert_eq!(
        u16::from_le_bytes([pristine[8][0], pristine[8][1]]),
        horton::wal::WAL_MAGIC
    );

    // 1. Corrupt magic of record 0: nothing is recoverable.
    let mut blocks = pristine.clone();
    blocks[8][0] ^= 0xFF;
    let (state, _) = recover_blocks(&blocks);
    assert_eq!((state.records, state.max_seq), (0, 0));

    // 2. Zero the whole WAL block: a fully torn write.
    let mut blocks = pristine.clone();
    blocks[8].fill(0);
    let (state, _) = recover_blocks(&blocks);
    assert_eq!((state.records, state.max_seq), (0, 0));

    // 3. Truncate 10 bytes into record 5: records 0..=4 replay, then stop.
    let mut blocks = pristine.clone();
    blocks[8][offs[5] + 10..].fill(0);
    let (state, table) = recover_blocks(&blocks);
    assert_eq!((state.records, state.max_seq), (5, 5));
    assert!(memtables_equal(&table, &replay_wal_prefix(5)));

    // 4. Corrupt the length field of record 2: stop after records 0..=1.
    let mut blocks = pristine.clone();
    blocks[8][offs[2] + 2..offs[2] + 6].copy_from_slice(&u32::MAX.to_le_bytes());
    let (state, _) = recover_blocks(&blocks);
    assert_eq!((state.records, state.max_seq), (2, 2));

    // 5. Corrupt the CRC of record 3 (last 4 bytes of the record): stop
    //    after records 0..=2.
    let mut blocks = pristine;
    let rec3_end = offs[3] + 23 + 5 + 3;
    blocks[8][rec3_end - 1] ^= 0xFF;
    let (state, _) = recover_blocks(&blocks);
    assert_eq!((state.records, state.max_seq), (3, 3));
}

// ---------------------------------------------------------------------------
// LZ77 codec fuzzing (v0.13): malformed streams must decode to Ok or a
// typed DecompressError — never a panic, never an OOB read.
// ---------------------------------------------------------------------------

/// Applies one random structural mutation to a byte slice: bit flips, byte
/// smears, truncation, or splices. The slice analogue of [`mutate`].
///
/// The `u64` → `usize`/`u8` casts below are PRNG output folded into
/// bounded ranges via `%`; truncation is intentional, not a bug.
#[allow(clippy::cast_possible_truncation)]
fn mutate_bytes(rng: &mut Lcg, buf: &mut Vec<u8>) {
    if buf.is_empty() {
        return;
    }
    match rng.next() % 5 {
        // Bit flips: 1..8 random bits anywhere.
        0 => {
            let flips = 1 + (rng.next() % 8) as usize;
            for _ in 0..flips {
                let bit = (rng.next() as usize) % (buf.len() * 8);
                buf[bit / 8] ^= 1 << (bit % 8);
            }
        }
        // Byte smear: a random span filled with random bytes.
        1 => {
            let start = (rng.next() as usize) % buf.len();
            let max_len = buf.len() - start;
            let len = 1 + (rng.next() as usize) % max_len;
            for b in &mut buf[start..start + len] {
                *b = rng.next() as u8;
            }
        }
        // Truncation: cut the stream short (a torn write).
        2 => {
            let cut = (rng.next() as usize) % (buf.len() + 1);
            buf.truncate(cut);
        }
        // Zero a random suffix (torn write that keeps the length).
        3 => {
            let cut = (rng.next() as usize) % (buf.len() + 1);
            buf[cut..].fill(0);
        }
        // Splice: copy a random span onto a random offset.
        _ => {
            let src = (rng.next() as usize) % buf.len();
            let dst = (rng.next() as usize) % buf.len();
            let max_len = (buf.len() - src).min(buf.len() - dst);
            if max_len > 0 {
                let len = 1 + (rng.next() as usize) % max_len;
                let tmp: Vec<u8> = buf[src..src + len].to_vec();
                buf[dst..dst + len].copy_from_slice(&tmp);
            }
        }
    }
}

/// Builds valid compressed streams by running patterned 4092-byte blocks
/// through the real encoder. Returns `(stream, original)` pairs.
#[allow(clippy::cast_possible_truncation)]
fn lz77_corpus() -> Vec<(Vec<u8>, [u8; 4092])> {
    let mut out = Vec::new();
    let mut cs = CompressScratch::<4096>::new();
    for pat in 0..6u8 {
        let mut block = [0u8; 4092];
        for (i, b) in block.iter_mut().enumerate() {
            let x = i as u8;
            *b = match pat {
                0 => 0,
                1 => x,
                2 => (i >> 3) as u8,
                3 => x.wrapping_mul(31).wrapping_add(pat),
                4 => {
                    if i % 2 == 0 {
                        0xAB
                    } else {
                        0xCD
                    }
                }
                _ => x ^ (x >> 4),
            };
        }
        if let Some(clen) = cs.compress(&block) {
            out.push((cs.compressed()[..clen].to_vec(), block));
        }
    }
    out
}

/// The unmutated LZ77 corpus must round-trip byte-identically.
#[test]
fn lz77_corpus_is_valid() {
    let corpus = lz77_corpus();
    assert!(
        !corpus.is_empty(),
        "no pattern compressed — the fuzz below would pass vacuously"
    );
    for (stream, original) in &corpus {
        let mut dst = [0u8; 4092];
        let n = decompress(stream, &mut dst).unwrap();
        assert_eq!(n, 4092);
        assert_eq!(&dst, original);
    }
}

/// Mutated LZ77 streams must never panic the decoder. A success always
/// fills the whole output buffer (`Ok(4092)`); anything else is a typed
/// `DecompressError`.
#[test]
fn lz77_decoder_never_panics() {
    let corpus = lz77_corpus();
    assert_ne!(corpus.len(), 0, "lz77 corpus must not be empty");
    for i in 0..ITERS {
        let mut rng = Lcg::new(SEED.wrapping_add(0x6000 + i as u64));
        let (stream, _) = &corpus[i % corpus.len()];
        let mut bytes = stream.clone();
        for _ in 0..=i % 3 {
            mutate_bytes(&mut rng, &mut bytes);
        }
        let mut dst = [0u8; 4092];
        // No panic is the main assertion; a success must fill the buffer.
        let r = decompress(&bytes, &mut dst);
        assert!(r.is_err() || r.unwrap() == 4092, "iter {i}");
    }
}

/// Pure-garbage streams — never near a valid encoding — must also decode
/// to `Err` (or, perversely, a full buffer), never panic.
#[test]
fn lz77_garbage_never_panics() {
    for i in 0..ITERS {
        let mut rng = Lcg::new(SEED.wrapping_add(0x7000 + i as u64));
        #[allow(clippy::cast_possible_truncation)]
        let len = (rng.next() % 300) as usize;
        let mut bytes = vec![0u8; len];
        for b in &mut bytes {
            #[allow(clippy::cast_possible_truncation)]
            {
                *b = rng.next() as u8;
            }
        }
        // The empty stream is the extreme torn write.
        let mut dst = [0u8; 4092];
        let r = decompress(&bytes, &mut dst);
        assert!(r.is_err() || r.unwrap() == 4092, "iter {i}");
    }
}

// ---------------------------------------------------------------------------
// SSTable decoder fuzzing with compression enabled (v0.13+): CRC-repaired
// mutations hammer inflate_data_block on garbage.
// ---------------------------------------------------------------------------

/// Builds a 200-entry table with compression enabled (repetitive values so
/// the trial compressor keeps the compressed form); returns the device,
/// the pristine blocks, and the block count.
#[allow(clippy::cast_possible_truncation)]
fn sstable_corpus_compressed(base: u64) -> (MemDevice<BLOCK>, Vec<[u8; BLOCK]>, u64) {
    let mut dev = MemDevice::<BLOCK>::new();
    let mut keys: Vec<Vec<u8>> = Vec::new();
    let mut vals: Vec<Vec<u8>> = Vec::new();
    for i in 0..200u64 {
        keys.push(format!("ckey{i:03}").into_bytes());
        // Repetitive values: the trial compressor keeps these blocks.
        vals.push(vec![(i % 7) as u8; 64]);
    }
    let entries = keys.iter().zip(vals.iter()).enumerate().map(|(i, (k, v))| {
        let seq = i as u64 + 1;
        SstEntry {
            key: k,
            val: v,
            seq,
            tombstone: seq.is_multiple_of(29),
            expire_at: if seq.is_multiple_of(29) || !seq.is_multiple_of(11) {
                0
            } else {
                1000 + seq
            },
        }
    });
    let k = bloom_k(1024 * 8, 200);
    let mut cs = CompressScratch::<BLOCK>::new();
    let nblocks = block_on(write_table::<_, BLOCK, 1024, 256>(
        &mut dev,
        base,
        k,
        entries,
        Some(&mut cs),
    ))
    .unwrap();
    let pristine = dev.blocks_mut().clone();
    (dev, pristine, nblocks)
}

/// The unmutated compressed table must open, serve every live key with the
/// right value/seq/expiry, and actually contain compressed blocks —
/// otherwise the fuzz below would never exercise the inflate path.
#[test]
#[allow(clippy::cast_possible_truncation)]
fn sstable_corpus_compressed_is_valid() {
    let base = 200u64;
    let (dev, pristine, nblocks) = sstable_corpus_compressed(base);
    let mut compressed = 0u32;
    for b in 0..nblocks - 3 {
        let blk = &pristine[(base + b) as usize];
        if u16::from_le_bytes([blk[BLOCK - 6], blk[BLOCK - 5]]) & 0x8000 != 0 {
            compressed += 1;
        }
    }
    assert!(
        compressed > 0,
        "no block kept its compressed form — inflate is untested"
    );
    let mut scratch = [0u8; BLOCK];
    let mut decomp = [0u8; BLOCK];
    let reader = block_on(TableReader::<MemDevice<BLOCK>, BLOCK, 1024>::open(
        &dev,
        &mut scratch,
        base + nblocks - 1,
    ))
    .unwrap();
    let mut val_buf = [0u8; 1024];
    for i in 1..=200u64 {
        let key = format!("ckey{:03}", i - 1);
        let got = block_on(reader.lookup(&mut scratch, &mut decomp, key.as_bytes(), &mut val_buf))
            .unwrap();
        if i % 29 == 0 {
            assert_eq!(got, SstLookup::Tombstone { seq: i }, "tombstone {i}");
        } else {
            let expect_val = [((i - 1) % 7) as u8; 64];
            let expect_expire = if i % 11 == 0 { 1000 + i } else { 0 };
            match got {
                SstLookup::Value {
                    len,
                    seq,
                    expire_at,
                } => {
                    assert_eq!(seq, i);
                    assert_eq!(expire_at, expect_expire);
                    assert_eq!(&val_buf[..len], &expect_val[..]);
                }
                other => panic!("key {key} should be live, got {other:?}"),
            }
        }
    }
}

/// Mutated compressed `SSTable` blocks must never panic open/lookup. The
/// CRC is always repaired so the inflate path runs on garbage: bad
/// compression flags, impossible lengths, and malformed LZ77 streams all
/// land in `inflate_data_block`. Outcomes are `Ok` or a clean `Error`.
#[test]
fn sstable_compressed_decoder_never_panics() {
    let base = 200u64;
    let (_, pristine, nblocks) = sstable_corpus_compressed(base);
    let footer = base + nblocks - 1;
    // Fewer iterations than the raw corpus: each one decompresses.
    let iters: usize = if cfg!(miri) { 3 } else { 200 };
    for i in 0..iters {
        let mut rng = Lcg::new(SEED.wrapping_add(0x8000 + i as u64));
        let mut blocks = pristine.clone();
        for _ in 0..=i % 3 {
            mutate(&mut rng, &mut blocks, true);
        }
        let mut dev = MemDevice::<BLOCK>::new();
        dev.blocks_mut().extend_from_slice(&blocks);
        let mut scratch = [0u8; BLOCK];
        let mut decomp = [0u8; BLOCK];
        let mut val_buf = [0u8; 1024];
        let open = block_on(TableReader::<MemDevice<BLOCK>, BLOCK, 1024>::open(
            &dev,
            &mut scratch,
            footer,
        ));
        if let Ok(reader) = open {
            for j in (0..200u64).step_by(17) {
                let key = format!("ckey{j:03}");
                // No panic is the assertion; any clean outcome is acceptable.
                let _ =
                    block_on(reader.get(&mut scratch, &mut decomp, key.as_bytes(), &mut val_buf));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Targeted SSTable block surgery: corrupt trailers, compression flags,
// bloom blocks, index entries, footer fields, rdel block counts.
// ---------------------------------------------------------------------------

/// Recomputes a block's trailing CRC32 in place.
fn fix_block_crc(blk: &mut [u8; BLOCK]) {
    let crc = crc32(&blk[..BLOCK - 4]);
    blk[BLOCK - 4..].copy_from_slice(&crc.to_le_bytes());
}

/// Fresh device from `pristine` with device block `id` replaced by the
/// result of `f` (which typically mutates bytes and repairs the CRC).
/// `pristine` covers device blocks `0..` densely (the corpora write
/// contiguous runs from an empty device), so `id` indexes it directly.
#[allow(clippy::cast_possible_truncation)]
fn device_with_block(
    pristine: &[[u8; BLOCK]],
    id: u64,
    f: impl FnOnce(&mut [u8; BLOCK]),
) -> MemDevice<BLOCK> {
    let mut blocks = pristine.to_vec();
    f(&mut blocks[id as usize]);
    let mut dev = MemDevice::<BLOCK>::new();
    dev.blocks_mut().extend_from_slice(&blocks);
    dev
}

/// Opens the table ending at `footer` and gets `key`, returning the raw
/// outcome (no unwrap: the tests assert on `Err` variants).
fn try_get(
    dev: &MemDevice<BLOCK>,
    footer: u64,
    key: &[u8],
) -> Result<Option<Vec<u8>>, Error<DevError>> {
    let mut scratch = [0u8; BLOCK];
    let mut decomp = [0u8; BLOCK];
    let mut val_buf = [0u8; 1024];
    let reader = block_on(TableReader::<MemDevice<BLOCK>, BLOCK, 1024>::open(
        dev,
        &mut scratch,
        footer,
    ))?;
    let n = block_on(reader.get(&mut scratch, &mut decomp, key, &mut val_buf))?;
    Ok(n.map(|len| val_buf[..len].to_vec()))
}

/// Targeted `SSTable` block surgery. Every case must resolve to `Ok` or a
/// typed `Error::CorruptBlock` — never a panic, never an OOB read, never
/// a hang.
#[test]
#[allow(clippy::too_many_lines)]
fn sstable_targeted_block_corruption() {
    // Plain (uncompressed) corpus: 1 data block, bloom, index, footer.
    let base = 136u64;
    let (_, pristine, nblocks) = sstable_corpus(base);
    assert_eq!(nblocks, 4);
    let footer = base + nblocks - 1;
    let (data, bloom, index) = (base, base + 1, base + 2);

    // 1. Restart count 0x7FFF (bit 15 clear, so no compression flag): the
    //    restart tail computation underflows -> CorruptBlock, not a panic.
    let dev = device_with_block(&pristine, data, |b| {
        b[BLOCK - 6..BLOCK - 4].copy_from_slice(&0x7FFFu16.to_le_bytes());
        fix_block_crc(b);
    });
    assert!(
        matches!(
            try_get(&dev, footer, b"skey00"),
            Err(Error::CorruptBlock { .. })
        ),
        "huge restart count must be CorruptBlock"
    );

    // 2. First restart offset out of bounds: binary search must fail
    //    cleanly, not index out of bounds.
    let dev = device_with_block(&pristine, data, |b| {
        let rcount = usize::from(u16::from_le_bytes([b[BLOCK - 6], b[BLOCK - 5]]) & 0x7FFF);
        let rstart = (BLOCK - 4) - 2 - rcount * 2;
        b[rstart..rstart + 2].copy_from_slice(&0x7FFEu16.to_le_bytes());
        fix_block_crc(b);
    });
    assert!(
        matches!(
            try_get(&dev, footer, b"skey00"),
            Err(Error::CorruptBlock { .. })
        ),
        "OOB restart offset must be CorruptBlock"
    );

    // 3. Bloom block zeroed with a BROKEN CRC: the gate is advisory, so
    //    every key must still read back exactly.
    let dev = device_with_block(&pristine, bloom, |b| {
        b.fill(0); // CRC left broken on purpose.
    });
    for i in 0..40u8 {
        let key = format!("skey{i:02}");
        let got = try_get(&dev, footer, key.as_bytes()).unwrap();
        if i % 13 == 12 {
            assert_eq!(got, None, "tombstone {i}");
        } else {
            assert_eq!(got.unwrap(), vec![i; usize::from(i % 13)]);
        }
    }

    // 4. Bloom block zeroed with a REPAIRED CRC: the filter now false-
    //    negatives every key. That is trusted-block semantics (a CRC-valid
    //    block is authoritative), but it must never panic or hard-error.
    let dev = device_with_block(&pristine, bloom, |b| {
        b.fill(0);
        fix_block_crc(b);
    });
    for i in 0..40u8 {
        let key = format!("skey{i:02}");
        assert!(
            try_get(&dev, footer, key.as_bytes()).is_ok(),
            "corrupt-but-CRC-valid bloom must not hard-error"
        );
    }

    // 5. Index entry with a 0xFFFF key length: the index walk must fail
    //    cleanly, not slice out of bounds.
    let dev = device_with_block(&pristine, index, |b| {
        b[0..2].copy_from_slice(&0xFFFFu16.to_le_bytes());
        fix_block_crc(b);
    });
    assert!(
        matches!(
            try_get(&dev, footer, b"skey00"),
            Err(Error::CorruptBlock { .. })
        ),
        "corrupt index entry must be CorruptBlock"
    );

    // 6. Footer with k = 0: open must reject the table.
    let dev = device_with_block(&pristine, footer, |b| {
        b[32] = 0;
        fix_block_crc(b);
    });
    assert!(
        matches!(
            try_get(&dev, footer, b"skey00"),
            Err(Error::CorruptBlock { .. })
        ),
        "zeroed footer k must be CorruptBlock"
    );

    // 7. Footer lying about rdel_blocks: the table must fail fast with
    //    `CorruptBlock` — at open (the section would start before block 0:
    //    it sits right before the bloom block) or at the first rdel CRC
    //    mismatch — never spin over u32::MAX blocks.
    let dev = device_with_block(&pristine, footer, |b| {
        b[33..37].copy_from_slice(&500u32.to_le_bytes());
        fix_block_crc(b);
    });
    let mut scratch = [0u8; BLOCK];
    let outcome = block_on(TableReader::<MemDevice<BLOCK>, BLOCK, 1024>::open(
        &dev,
        &mut scratch,
        footer,
    ))
    .and_then(|reader| block_on(reader.covering_rdel_seq(&mut scratch, b"skey00", u64::MAX)));
    assert!(
        matches!(outcome, Err(Error::CorruptBlock { .. })),
        "lying rdel_blocks must fail fast, not hang: {outcome:?}"
    );
    // The same lie with a count that fits below the bloom block: open
    // succeeds, and the probe reads data blocks as rdel blocks — their
    // CRCs pass but the count field (a restart count) must still never
    // yield a wrong answer or a hang.
    let dev = device_with_block(&pristine, footer, |b| {
        b[33..37].copy_from_slice(&1u32.to_le_bytes());
        fix_block_crc(b);
    });
    let mut scratch = [0u8; BLOCK];
    let reader = block_on(TableReader::<MemDevice<BLOCK>, BLOCK, 1024>::open(
        &dev,
        &mut scratch,
        footer,
    ))
    .unwrap();
    let probe = block_on(reader.covering_rdel_seq(&mut scratch, b"skey00", u64::MAX));
    assert!(
        matches!(probe, Ok(None) | Err(Error::CorruptBlock { .. })),
        "a misread rdel section must be absent-or-corrupt: {probe:?}"
    );

    // Compressed corpus: a data block carrying the compression flag.
    let cbase = 200u64;
    let (_, cpristine, cnblocks) = sstable_corpus_compressed(cbase);
    let cfooter = cbase + cnblocks - 1;
    let flagged = (0..cnblocks - 3)
        .map(|b| cbase + b)
        .find(|id| {
            #[allow(clippy::cast_possible_truncation)]
            let blk = &cpristine[*id as usize];
            u16::from_le_bytes([blk[BLOCK - 6], blk[BLOCK - 5]]) & 0x8000 != 0
        })
        .expect("compressed corpus has no flagged block");

    // 8. Compression flag with an impossible length (0x7FFF): inflate
    //    rejects it and the read reports the corrupt block — never a
    //    panic, and never "absent" (which would let an older version
    //    elsewhere win).
    let dev = device_with_block(&cpristine, flagged, |b| {
        b[BLOCK - 6..BLOCK - 4].copy_from_slice(&0xFFFFu16.to_le_bytes());
        fix_block_crc(b);
    });
    assert!(
        matches!(
            try_get(&dev, cfooter, b"ckey000"),
            Err(Error::CorruptBlock { id }) if id == flagged
        ),
        "impossible compressed length must be CorruptBlock"
    );

    // 9. Compression flag with a plausible length over garbage bytes: the
    //    LZ77 decoder rejects the stream, or it decodes to bytes that no
    //    longer hold the key — never a panic, never a wrong value.
    let dev = device_with_block(&cpristine, flagged, |b| {
        let clen = usize::from(u16::from_le_bytes([b[BLOCK - 6], b[BLOCK - 5]]) & 0x7FFF);
        for x in b.iter_mut().take(clen) {
            *x = 0xFF;
        }
        fix_block_crc(b);
    });
    assert!(
        matches!(
            try_get(&dev, cfooter, b"ckey000"),
            Ok(None) | Err(Error::CorruptBlock { .. })
        ),
        "garbage compressed payload must be absent-or-corrupt, never a wrong value"
    );
}

// ---------------------------------------------------------------------------
// Bloom filter pure functions on arbitrary bytes.
// ---------------------------------------------------------------------------

/// `bloom_maybe_contains` on arbitrary filter bytes, lengths (including 0),
/// and `k` values must never panic. A saturated filter must never report
/// a false negative.
#[test]
#[allow(clippy::cast_possible_truncation)]
fn bloom_filter_never_panics() {
    let mut rng = Lcg::new(SEED.wrapping_add(0x9000));
    let iters = if cfg!(miri) { 50 } else { 5000 };
    for i in 0..iters {
        let len = (rng.next() % 65) as usize;
        let mut filter = vec![0u8; len];
        for b in &mut filter {
            *b = rng.next() as u8;
        }
        let k = (rng.next() % 41) as u8;
        let mut key = [0u8; 32];
        for b in &mut key {
            *b = rng.next() as u8;
        }
        let klen = (rng.next() % 33) as usize;
        // No panic is the assertion.
        let _ = bloom_maybe_contains(&filter, &key[..klen], k);
        // Saturated filter: every probe bit set -> no false negatives.
        filter.fill(0xFF);
        assert!(
            bloom_maybe_contains(&filter, &key[..klen], k),
            "iter {i}: saturated filter false-negative"
        );
        // Empty filter degrades to "no information", never a panic.
        assert!(bloom_maybe_contains(&[], &key[..klen], k));
    }
}

// ---------------------------------------------------------------------------
// Memtable insert path: adversarial lengths, duplicate storms, tombstone
// interleavings.
// ---------------------------------------------------------------------------

/// Adversarial memtable inserts: empty keys, `KEY_MAX` boundary keys
/// (255/256/257), huge keys, empty/`VAL_MAX`-boundary/oversize values,
/// duplicate-key storms, and tombstone interleavings. Every rejection must
/// be a typed error (`EmptyKey`/`KeyTooLarge`/`ValueTooLarge`/`TableFull`/
/// `ArenaFull`) — never a panic — and every accepted version chain must
/// read back exactly, newest first.
/// Accepted version chain per key, newest last: (seq, tombstone, value).
type VersionChain = Vec<(u64, bool, Vec<u8>)>;

#[test]
#[allow(clippy::cast_possible_truncation)]
#[allow(clippy::too_many_lines)]
fn memtable_adversarial_inserts() {
    let mut rng = Lcg::new(SEED.wrapping_add(0xA000));
    let mut table = MemTable::<32, 1024, 256, 1024>::new();
    // Oracle: accepted version chains per key, newest last.
    let mut oracle: BTreeMap<Vec<u8>, VersionChain> = BTreeMap::new();
    let mut seq = 0u64;
    let mut last_key: Vec<u8> = Vec::new();
    let mut rejected = 0u32;
    let iters = if cfg!(miri) { 100 } else { 3000 };
    for _ in 0..iters {
        // Key length classes: empty, tiny, boundary-1, boundary,
        // boundary+1, huge.
        let klen = match rng.next() % 8 {
            0 => 0,
            1 => 1,
            2 => 255,
            3 => 256,
            4 => 257,
            5 => 3000,
            _ => 1 + (rng.next() % 16) as usize,
        };
        // Duplicate-key storm: often reuse the previous key.
        let key: Vec<u8> = if !last_key.is_empty() && rng.next().is_multiple_of(3) {
            last_key.clone()
        } else {
            let mut k = vec![0u8; klen];
            for b in &mut k {
                *b = rng.next() as u8;
            }
            k
        };
        let vlen = match rng.next() % 6 {
            0 => 0,
            1 => 1,
            2 => 1023,
            3 => 1024,
            4 => 1025,
            _ => (rng.next() % 32) as usize,
        };
        let mut val = vec![0u8; vlen];
        for b in &mut val {
            *b = rng.next() as u8;
        }
        let tombstone = rng.next().is_multiple_of(4);
        seq += 1;
        match table.insert::<DevError>(&key, &val, seq, tombstone) {
            Ok(()) => {
                oracle
                    .entry(key.clone())
                    .or_default()
                    .push((seq, tombstone, val.clone()));
                last_key = key;
            }
            Err(
                Error::EmptyKey
                | Error::KeyTooLarge { .. }
                | Error::ValueTooLarge { .. }
                | Error::TableFull
                | Error::ArenaFull,
            ) => {
                rejected += 1;
            }
            Err(e) => panic!("unexpected insert error: {e:?}"),
        }
    }
    assert!(
        rejected > 0,
        "adversarial classes never hit a rejection path"
    );
    // Every accepted chain reads back exactly, newest version first.
    // (Tombstones deliberately store a zero-length value: the memtable
    // forces `vlen = 0` for them in `plan`.)
    for (key, chain) in &oracle {
        let (eseq, etomb, eval) = chain.last().unwrap();
        let got = table
            .get(key)
            .unwrap_or_else(|| panic!("accepted key {key:?} lost"));
        assert_eq!(got.seq, *eseq, "key {key:?}: seq");
        assert_eq!(got.tombstone, *etomb, "key {key:?}: tombstone");
        let expect_val: &[u8] = if *etomb { &[] } else { eval.as_slice() };
        assert_eq!(got.val, expect_val, "key {key:?}: value");
    }
}

/// Adversarial range-tombstone bounds at the memtable level: empty and
/// inverted bounds are the `Db`'s contract to reject, but the memtable
/// itself must never panic on them — only typed errors or acceptance.
#[test]
#[allow(clippy::cast_possible_truncation)]
fn memtable_adversarial_range_dels() {
    let mut rng = Lcg::new(SEED.wrapping_add(0xB000));
    let mut table = MemTable::<32, 1024, 256, 1024>::new();
    let mut seq = 0u64;
    let iters = if cfg!(miri) { 30 } else { 500 };
    for _ in 0..iters {
        let alen = [0usize, 1, 256, 257][(rng.next() % 4) as usize];
        let blen = [0usize, 1, 256, 257][(rng.next() % 4) as usize];
        let mut a = vec![0u8; alen];
        for b in &mut a {
            *b = rng.next() as u8;
        }
        let mut bnd = vec![0u8; blen];
        for b in &mut bnd {
            *b = rng.next() as u8;
        }
        seq += 1;
        match table.insert_range_del::<DevError>(&a, &bnd, seq) {
            Ok(())
            | Err(
                Error::EmptyKey | Error::KeyTooLarge { .. } | Error::TableFull | Error::ArenaFull,
            ) => {}
            Err(e) => panic!("unexpected range-del error: {e:?}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Block cache (v0.16): a poisoned cache entry must not bypass the read
// path's checks.
// ---------------------------------------------------------------------------

/// Comparable outcome of one point lookup: value bytes + metadata, a
/// tombstone, a miss, or a stringified error.
#[derive(Debug, PartialEq, Eq)]
enum LookupOut {
    Value(Vec<u8>, u64, u64),
    Tombstone(u64),
    Missing,
    Err(String),
}

/// Looks `key` up through `reader`, mapping the outcome to [`LookupOut`].
fn lookup_outcome(reader: &TableReader<MemDevice<BLOCK>, BLOCK, 1024>, key: &[u8]) -> LookupOut {
    let mut scratch = [0u8; BLOCK];
    let mut decomp = [0u8; BLOCK];
    let mut val_buf = [0u8; 1024];
    match block_on(reader.lookup(&mut scratch, &mut decomp, key, &mut val_buf)) {
        Ok(SstLookup::Value {
            len,
            seq,
            expire_at,
        }) => LookupOut::Value(val_buf[..len].to_vec(), seq, expire_at),
        Ok(SstLookup::Tombstone { seq }) => LookupOut::Tombstone(seq),
        Ok(SstLookup::Missing) => LookupOut::Missing,
        Err(e) => LookupOut::Err(format!("{e:?}")),
    }
}

/// A poisoned cache entry must not bypass the read path's checks: the same
/// poisoned bytes served from the cache and read straight from the device
/// must produce identical outcomes — CRC, magic, decompression, and parse
/// checks all run *after* the cache, on identical bytes.
#[test]
#[allow(clippy::cast_possible_truncation)]
fn cache_poisoned_block_matches_device() {
    use core::cell::RefCell;
    let base = 200u64;
    let (dev, pristine, nblocks) = sstable_corpus_compressed(base);
    let footer = base + nblocks - 1;
    let data_block = base; // first data block; ckey000.. live here
    let keys: Vec<Vec<u8>> = (0..12u64)
        .map(|j| format!("ckey{j:03}").into_bytes())
        .collect();
    let iters: usize = if cfg!(miri) { 5 } else { 150 };
    for i in 0..iters {
        let mut rng = Lcg::new(SEED.wrapping_add(0xC000 + i as u64));
        // Poisoned image: mutate the true block, sometimes repairing the
        // CRC so the parsers run on garbage.
        let mut poisoned = pristine[data_block as usize];
        let repair = rng.next().is_multiple_of(2);
        mutate(&mut rng, core::slice::from_mut(&mut poisoned), repair);

        // Path A: the poisoned image served from the cache.
        let cache = RefCell::new(BlockCache::<BLOCK, 8>::new());
        let port: &dyn CachePort<BLOCK> = &cache;
        port.put(7, data_block, &poisoned, true);
        let mut scratch = [0u8; BLOCK];
        let reader_a = block_on(TableReader::<MemDevice<BLOCK>, BLOCK, 1024>::open_cached(
            &dev,
            Some(port),
            7,
            &mut scratch,
            footer,
        ))
        .unwrap();

        // Path B: the same poisoned bytes read straight from the device.
        let mut dev_b = MemDevice::<BLOCK>::new();
        dev_b.blocks_mut().extend_from_slice(&pristine);
        dev_b.blocks_mut()[data_block as usize] = poisoned;
        let mut scratch_b = [0u8; BLOCK];
        let reader_b = block_on(TableReader::<MemDevice<BLOCK>, BLOCK, 1024>::open(
            &dev_b,
            &mut scratch_b,
            footer,
        ))
        .unwrap();

        for k in &keys {
            assert_eq!(
                lookup_outcome(&reader_a, k),
                lookup_outcome(&reader_b, k),
                "iter {i} key {k:?}: cache bypassed a read-path check"
            );
        }
    }
}
