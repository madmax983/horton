//! v0.13: hand-rolled LZ77 block compression.
//!
//! Codec proofs (round-trips, edge cases, deterministic decoder fuzz with
//! a no-panic contract, ratio measurement on realistic data) plus the
//! integration proofs: flushed tables compress, every read path
//! decompresses transparently, and mixed compressed/raw tables read
//! exactly.

mod common;

use common::{Lcg, MemDevice, TestDb, block_on, test_config};
use core::task::{Context, Poll};
use horton::compress::{COMPRESS_MIN_SAVING, CompressScratch, decompress};
use horton::sstable::{SstEntry, bloom_k, plan_table, write_table};
use horton::{BlockDevice, Compaction, Progress, SealedTable};

const BLOCK: usize = 4096;
const BODY: usize = BLOCK - 4; // decompressed size is always the full logical block

type TestScan<'d> = horton::Scan<'d, MemDevice<BLOCK>, BLOCK, 256, 1024, 64, 4096, 7, 4, 1024, 8>;

const fn scratch() -> CompressScratch<BLOCK> {
    CompressScratch::new()
}

/// Compresses `src` and round-trips it, asserting byte-identity.
fn roundtrip(src: &[u8; BODY]) {
    let mut cs = scratch();
    let clen = cs.compress(src).expect("compressible input must encode");
    let enc = cs.compressed().to_vec();
    assert_eq!(enc.len(), clen);
    let mut out = [0xAAu8; BODY];
    let n = decompress(&enc, &mut out).expect("valid stream must decode");
    assert_eq!(n, BODY);
    assert_eq!(&out, src);
}

#[test]
fn roundtrip_all_zeros() {
    roundtrip(&[0u8; BODY]);
}

#[test]
fn roundtrip_all_same_byte() {
    roundtrip(&[0x5Au8; BODY]);
}

#[test]
#[allow(clippy::cast_possible_truncation)]
fn roundtrip_incompressible_random() {
    // Random data: the codec may decline (None) or emit a stream that
    // still round-trips. Either is correct; a panic or mismatch is not.
    let mut rng = Lcg::new(0x1234_5678);
    let mut src = [0u8; BODY];
    for b in &mut src {
        *b = rng.next() as u8;
    }
    let mut cs = scratch();
    if let Some(clen) = cs.compress(&src) {
        let enc = cs.compressed().to_vec();
        assert_eq!(enc.len(), clen);
        let mut out = [0u8; BODY];
        let n = decompress(&enc, &mut out).expect("emitted stream must decode");
        assert_eq!(n, BODY);
        assert_eq!(&out, &src);
    }
}

#[test]
#[allow(clippy::cast_precision_loss)] // 4 KiB values: f64 represents them exactly
fn roundtrip_structured_kv() {
    // Realistic KV-ish payload: common key prefixes, JSON-ish values.
    let mut src = [0u8; BODY];
    let mut off = 0usize;
    let mut i = 0u32;
    while off + 64 <= BODY {
        let chunk = format!(
            "user:{i:06}:profile{{\"name\":\"user {i}\",\"email\":\"user{i}@example.com\"}}\n"
        );
        let bytes = chunk.as_bytes();
        let n = bytes.len().min(BODY - off);
        src[off..off + n].copy_from_slice(&bytes[..n]);
        off += n;
        i += 1;
    }
    let mut cs = scratch();
    let clen = cs.compress(&src).expect("structured data must compress");
    // Realistic data compresses well past the worth-it threshold.
    assert!(
        BODY - clen >= COMPRESS_MIN_SAVING,
        "saving = {} bytes",
        BODY - clen
    );
    let enc = cs.compressed().to_vec();
    let mut out = [0u8; BODY];
    assert_eq!(decompress(&enc, &mut out).unwrap(), BODY);
    assert_eq!(&out, &src);
    eprintln!(
        "structured_kv ratio: {clen}/{BODY} = {:.3}",
        clen as f64 / BODY as f64
    );
}

#[test]
fn roundtrip_single_byte_repeats() {
    // Pathological for greedy matchers: single-byte input.
    let mut src = [0u8; BODY];
    src[0] = 0xFF;
    roundtrip(&src);
}

#[test]
#[allow(clippy::cast_possible_truncation)]
fn roundtrip_alternating_pattern() {
    let mut src = [0u8; BODY];
    for (i, b) in src.iter_mut().enumerate() {
        *b = (i % 3) as u8;
    }
    roundtrip(&src);
}

/// The decoder never panics: mutated and truncated streams either decode
/// to a fully-formed block or return `Err`. (A test panic IS the failure
/// signal — Miri additionally turns UB into a failure.)
#[test]
#[allow(clippy::cast_possible_truncation)]
fn decoder_fuzz_no_panic() {
    let mut rng = Lcg::new(0xDEAD_BEEF);
    // Seed corpus: valid streams from varied inputs.
    let mut corpus: Vec<Vec<u8>> = Vec::new();
    let mut seeds: Vec<[u8; BODY]> = Vec::new();
    let zeros = [0u8; BODY];
    let mut patterned = [0u8; BODY];
    let mut text = [0u8; BODY];
    for (i, b) in patterned.iter_mut().enumerate() {
        *b = (i % 251) as u8;
    }
    for (i, b) in text.iter_mut().enumerate() {
        *b = b"the quick brown fox jumps over the lazy dog "[i % 44];
    }
    let mut random = [0u8; BODY];
    for b in &mut random {
        *b = rng.next() as u8;
    }
    seeds.append(&mut vec![zeros, patterned, text, random]);
    for src in &seeds {
        let mut cs = scratch();
        if cs.compress(src).is_some() {
            corpus.push(cs.compressed().to_vec());
        }
    }
    assert_ne!(corpus.len(), 0, "seed corpus must not be empty");

    let mut out = [0xCCu8; BODY];
    for round in 0..2000 {
        let base = &corpus[(rng.next() as usize) % corpus.len()];
        let mut mutated = base.clone();
        // Mutation: byte flips, truncation, splicing, length tweaks.
        match rng.next() % 5 {
            0 => {
                // Flip up to 4 bytes.
                for _ in 0..=rng.next() % 4 {
                    if !mutated.is_empty() {
                        let i = (rng.next() as usize) % mutated.len();
                        mutated[i] ^= 1 << (rng.next() % 8);
                    }
                }
            }
            1 => {
                // Truncate.
                let n = (rng.next() as usize) % (mutated.len() + 1);
                mutated.truncate(n);
            }
            2 => {
                // Append garbage.
                let n = (rng.next() % 16) as usize;
                for _ in 0..n {
                    mutated.push(rng.next() as u8);
                }
            }
            3 => {
                // Splice two streams.
                let other = &corpus[(rng.next() as usize) % corpus.len()];
                let at = (rng.next() as usize) % (mutated.len() + 1);
                let take = (rng.next() as usize) % (other.len() + 1);
                mutated.splice(
                    at..at.min(mutated.len()),
                    other[..take.min(other.len())].iter().copied(),
                );
            }
            _ => {
                // Pure garbage.
                mutated.clear();
                let n = (rng.next() % 64) as usize;
                for _ in 0..n {
                    mutated.push(rng.next() as u8);
                }
            }
        }
        out.fill(0xCC);
        if let Ok(n) = decompress(&mutated, &mut out) {
            assert_eq!(n, BODY, "round {round}: short decode");
        }
    }
}

/// Puts `k`/`v`, flushing first when the memtable arena is full. The
/// test memtable holds ~4 KiB; large values need several flushes.
fn put_flushing(db: &mut TestDb<MemDevice<BLOCK>>, k: &[u8], v: &[u8]) {
    match block_on(db.put(k, v)) {
        Ok(_) => {}
        Err(horton::Error::ArenaFull | horton::Error::NoSpace) => {
            block_on(db.flush()).unwrap();
            block_on(db.put(k, v)).unwrap();
        }
        Err(e) => panic!("put failed: {e:?}"),
    }
}

/// Counts data blocks across all levels carrying the compression flag.
fn flagged_blocks(db: &TestDb<MemDevice<BLOCK>>) -> (u32, u32) {
    let mut flagged = 0u32;
    let mut total = 0u32;
    let mut li = 0usize;
    while let Some(tables) = db.level_tables(li) {
        for t in tables {
            let data_blocks = t.block_count - 3;
            total += data_blocks;
            for b in 0..data_blocks {
                let blk = read_block(db.device(), t.first_block + u64::from(b));
                let trailer = u16::from_le_bytes(blk[BODY - 2..BODY].try_into().unwrap());
                if trailer & 0x8000 != 0 {
                    flagged += 1;
                    let clen = usize::from(trailer & 0x7FFF);
                    assert!(clen < BODY - COMPRESS_MIN_SAVING, "clen={clen}");
                }
            }
        }
        li += 1;
    }
    (flagged, total)
}

/// Realistic end-to-end ratio: compressible KV data through the real
/// flush path. Data blocks must actually carry the compression flag,
/// and every key must read back exactly.
#[test]
#[allow(clippy::cast_possible_truncation)]
fn flush_compresses_realistic_data() {
    let mut db = TestDb::new(MemDevice::<BLOCK>::new(), test_config());
    block_on(db.open()).unwrap();
    // Values share long prefixes; flush whenever the arena fills.
    for i in 0..64u32 {
        let k = format!("user:{i:06}:profile");
        let v = format!(
            "{{\"id\":{i},\"name\":\"Test User {i}\",\"email\":\"test.user.{i}@example.com\",\"role\":\"member\"}}"
        );
        put_flushing(&mut db, k.as_bytes(), v.as_bytes());
    }
    block_on(db.flush()).unwrap();
    // Reads are exact through decompression.
    for i in 0..64u32 {
        let k = format!("user:{i:06}:profile");
        let want = format!(
            "{{\"id\":{i},\"name\":\"Test User {i}\",\"email\":\"test.user.{i}@example.com\",\"role\":\"member\"}}"
        );
        let mut buf = [0u8; 1024];
        let n = block_on(db.get(k.as_bytes(), &mut buf))
            .unwrap()
            .expect("present");
        assert_eq!(&buf[..n], want.as_bytes());
    }
    // At least one data block is flagged compressed.
    let (flagged, total) = flagged_blocks(&db);
    assert!(flagged > 0, "no data block was compressed");
    eprintln!("compressed {flagged}/{total} data blocks");
}

/// Incompressible data stays raw: data blocks packed nearly full of
/// large random values must NOT carry the compression flag — with less
/// than [`COMPRESS_MIN_SAVING`] bytes of zero fill and negligible entry
/// framing, there is nothing worth saving. (Small random values would
/// still flag: their entry headers and sequence numbers compress, and
/// the zero fill after a few entries compresses too. Both are correct —
/// flagging those blocks genuinely saves flash.)
#[test]
#[allow(clippy::cast_possible_truncation)]
fn flush_leaves_random_data_raw() {
    const BLOOM: usize = 1024;
    let mut dev = MemDevice::<BLOCK>::new();
    // 6 entries × 2018 bytes ≈ 12 KiB: every data block holds 2 entries
    // (4036 payload bytes, ~48 bytes of zero fill) — incompressible.
    let mut rng = Lcg::new(0x7777);
    let mut keys: Vec<Vec<u8>> = Vec::new();
    let mut vals: Vec<Vec<u8>> = Vec::new();
    for i in 0..6u32 {
        keys.push(format!("r{i:03}").into_bytes());
        let mut v = vec![0u8; 2000];
        for b in &mut v {
            *b = rng.next() as u8;
        }
        vals.push(v);
    }
    let entries: Vec<SstEntry<'_>> = keys
        .iter()
        .zip(vals.iter())
        .enumerate()
        .map(|(i, (k, v))| SstEntry {
            key: k,
            val: v,
            seq: i as u64 + 1,
            tombstone: false,
            expire_at: 0,
        })
        .collect();
    let plan =
        plan_table::<core::convert::Infallible, BLOCK, 256>(entries.iter().copied()).unwrap();
    let k = bloom_k(BLOOM * 8, plan.entry_count);
    let mut cs = CompressScratch::<BLOCK>::new();
    let base = 136u64;
    let nblocks = block_on(write_table::<MemDevice<BLOCK>, BLOCK, BLOOM, 256>(
        &mut dev,
        base,
        k,
        entries.iter().copied(),
        Some(&mut cs),
    ))
    .unwrap();
    assert_eq!(nblocks, plan.data_blocks + 3);
    assert!(plan.data_blocks >= 2, "want multiple full blocks");
    // Every data block is packed full of random bytes: all stay raw —
    // even the last, which is just as full as the rest.
    for b in 0..plan.data_blocks {
        let blk = read_block(&dev, base + b);
        let trailer = u16::from_le_bytes(blk[BODY - 2..BODY].try_into().unwrap());
        assert_eq!(trailer & 0x8000, 0, "full random block {b} should stay raw");
    }
    // And the table reads back exactly through a reader.
    let mut scratch = [0u8; BLOCK];
    let mut decomp = [0u8; BLOCK];
    let reader = block_on(
        horton::sstable::TableReader::<MemDevice<BLOCK>, BLOCK, BLOOM>::open(
            &dev,
            &mut scratch,
            base + nblocks - 1,
        ),
    )
    .unwrap();
    let mut vbuf = [0u8; 2048];
    for (i, key) in keys.iter().enumerate() {
        let n = block_on(reader.get(&mut scratch, &mut decomp, key, &mut vbuf))
            .unwrap()
            .expect("present");
        assert_eq!(&vbuf[..n], &vals[i]);
    }
}

/// Mixed table: compressible and incompressible keys in one flush read
/// back exactly, exercising both paths in a single table.
#[test]
#[allow(clippy::cast_possible_truncation)]
fn mixed_table_reads_exactly() {
    let mut db = TestDb::new(MemDevice::<BLOCK>::new(), test_config());
    block_on(db.open()).unwrap();
    let mut rng = Lcg::new(0x9999);
    // 32 small puts fit the arena in one flush (L0 holds 4 tables; the
    // point here is one mixed table, not flush batching).
    for i in 0..32u32 {
        let k = format!("m{i:04}");
        if i % 2 == 0 {
            let v = format!("repeated-payload-{i}-{}", "x".repeat(80));
            block_on(db.put(k.as_bytes(), v.as_bytes())).unwrap();
        } else {
            let mut v = [0u8; 64];
            for b in &mut v {
                *b = rng.next() as u8;
            }
            block_on(db.put(k.as_bytes(), &v)).unwrap();
        }
    }
    block_on(db.flush()).unwrap();
    // Every key reads back exactly; the RNG stream is replayed in the
    // same order as the write loop (odd iterations consume 64 draws).
    let mut rng = Lcg::new(0x9999);
    for i in 0..32u32 {
        let k = format!("m{i:04}");
        let mut buf = [0u8; 1024];
        let n = block_on(db.get(k.as_bytes(), &mut buf))
            .unwrap()
            .expect("present");
        if i % 2 == 0 {
            let want = format!("repeated-payload-{i}-{}", "x".repeat(80));
            assert_eq!(&buf[..n], want.as_bytes());
        } else {
            let mut want = [0u8; 64];
            for b in &mut want {
                *b = rng.next() as u8;
            }
            assert_eq!(&buf[..n], &want);
        }
    }
    // And a full scan sees all 32.
    let mut scan = TestScan::new(&db);
    block_on(scan.seek(b"", None, u64::MAX)).unwrap();
    let mut kbuf = [0u8; 256];
    let mut vbuf = [0u8; 1024];
    let mut count = 0u32;
    while block_on(scan.next(&mut kbuf, &mut vbuf)).unwrap().is_some() {
        count += 1;
    }
    assert_eq!(count, 32);
}

/// Reads one block through the poll interface (test devices are `Ready`).
fn read_block<D: horton::BlockDevice>(dev: &D, id: u64) -> [u8; BLOCK] {
    use core::task::{Context, Poll};
    let mut buf = [0u8; BLOCK];
    let waker = common::noop_waker();
    let mut cx = Context::from_waker(&waker);
    match dev.poll_read_block(&mut cx, id, &mut buf) {
        Poll::Ready(Ok(())) => buf,
        _ => panic!("read failed"),
    }
}

// ---------------------------------------------------------------------------
// Integration: compaction and archive/ingest over compressed tables
// ---------------------------------------------------------------------------

/// Compaction merges compressed tables: four L0 tables of compressible
/// data compact down, every key reads back exactly through
/// decompression, and the merged output's own data blocks carry the
/// compression flag (the output path trial-compresses too).
#[test]
fn compaction_merges_compressed_tables() {
    type TestCompaction = Compaction<4096, 256, 1024, 1024>;
    let mut db = TestDb::new(MemDevice::<BLOCK>::new(), test_config());
    block_on(db.open()).unwrap();
    // Four tables of compressible data (values share long prefixes).
    // 16 puts × ~210 B ≈ 3.4 KiB per table: one solid data block each;
    // 64 puts total stay under the 128-block WAL region (one block per
    // put), and each `t` fits the 4 KiB arena in exactly one flush.
    for t in 0..4u32 {
        for i in 0..16u32 {
            let k = format!("t{t}:user:{i:04}");
            let v = format!(
                "{{\"t\":{t},\"i\":{i},\"name\":\"Test User {i}\",\"pad\":\"{}\"}}",
                "x".repeat(140)
            );
            block_on(db.put(k.as_bytes(), v.as_bytes())).unwrap();
        }
        block_on(db.flush()).unwrap();
    }
    // L0 is full (4 tables); drain every compaction job to idle.
    let mut c = TestCompaction::new();
    while db.compaction_pending() {
        loop {
            match block_on(db.compact_step(&mut c)) {
                Ok(Progress::More) => {}
                Ok(Progress::Done) => break,
                Err(e) => panic!("compaction failed: {e:?}"),
            }
        }
    }
    // Every key reads back exactly through decompression.
    for t in 0..4u32 {
        for i in 0..16u32 {
            let k = format!("t{t}:user:{i:04}");
            let want = format!(
                "{{\"t\":{t},\"i\":{i},\"name\":\"Test User {i}\",\"pad\":\"{}\"}}",
                "x".repeat(140)
            );
            let mut buf = [0u8; 1024];
            let n = block_on(db.get(k.as_bytes(), &mut buf))
                .unwrap()
                .expect("present");
            assert_eq!(&buf[..n], want.as_bytes());
        }
    }
    // The merged output itself is compressed.
    let (flagged, total) = flagged_blocks(&db);
    assert!(flagged > 0, "compaction output should compress");
    eprintln!("compaction output: {flagged}/{total} data blocks compressed");
}

/// Archive/ingest round-trip over compressed blocks: the uploaded bytes
/// keep their compression flags, the ingest grafts the table back, and
/// every key reads back exactly. Ingest relocates block pointers only —
/// the compressed payloads are never inflated and re-compressed.
#[test]
fn archive_ingest_preserves_compressed_blocks() {
    let mut db = TestDb::new(MemDevice::<BLOCK>::new(), test_config());
    block_on(db.open()).unwrap();
    // 28 puts x ~140 B ~= 3.9 KiB: one flush, one table.
    for i in 0..20u32 {
        let k = format!("arc:{i:04}");
        let v = [
            "{\"i\":",
            &i.to_string(),
            ",\"payload\":\"",
            &"z".repeat(120),
            "\"}",
        ]
        .concat();
        block_on(db.put(k.as_bytes(), v.as_bytes())).unwrap();
    }
    block_on(db.flush()).unwrap();
    let l0 = db.level_tables(0).expect("level 0 exists");
    assert_eq!(l0.len(), 1);
    let table_id = l0[0].id;
    let (flagged_before, _) = flagged_blocks(&db);
    assert!(flagged_before > 0, "test needs a compressed table");

    // Upload: stream the table's blocks into a remote device at base 0.
    let plan = db.archive_plan(0, table_id).expect("table must be present");
    let sealed: SealedTable<256> = plan.sealed();
    let mut remote = MemDevice::<BLOCK>::new();
    for i in 0..plan.table.block_count {
        let buf = read_block(db.device(), plan.table.first_block + u64::from(i));
        let waker = common::noop_waker();
        let mut cx = Context::from_waker(&waker);
        match remote.poll_write_block(&mut cx, u64::from(i), &buf) {
            Poll::Ready(Ok(())) => {}
            _ => panic!("remote write failed"),
        }
    }
    assert!(block_on(db.archive_commit(0, table_id)).unwrap());
    assert!(db.archive_plan(0, table_id).is_none());

    // Ingest: graft the table back from the remote bytes.
    assert!(block_on(db.ingest_table(&sealed, &remote, 0)).unwrap());
    for i in 0..20u32 {
        let k = format!("arc:{i:04}");
        let want = [
            "{\"i\":",
            &i.to_string(),
            ",\"payload\":\"",
            &"z".repeat(120),
            "\"}",
        ]
        .concat();
        let mut buf = [0u8; 1024];
        let n = block_on(db.get(k.as_bytes(), &mut buf))
            .unwrap()
            .expect("present");
        assert_eq!(&buf[..n], want.as_bytes());
    }
    // The flags survived the relocation untouched.
    let (flagged_after, _) = flagged_blocks(&db);
    assert_eq!(
        flagged_after, flagged_before,
        "ingest must preserve compression flags"
    );
}
