# ADR-0008: Hand-rolled per-block LZ77 compression with a trailer flag

Status: Accepted (v0.13, 2026-09-23)

## Context

SSTable data blocks were stored raw. The crate has no dependencies and
may not allocate or panic (ADR-0002), so compression has to be written
in-tree. Point reads go through the block index to a single data block
(§4.7), so blocks are accessed at random, one at a time.

## Decision

- LZ77 written from scratch in `src/compress.rs`: `compress` and
  `decompress`, no dependency, no `std`, no allocation, no panics, with
  a caller-owned `CompressScratch<BLOCK>` (hash table plus output
  staging).
- The writer trial-compresses every sealed data block and keeps the
  compressed form only if it saves at least `COMPRESS_MIN_SAVING` (128)
  bytes; otherwise the block is stored raw. Bloom, index and footer
  blocks are never compressed.
- The flag lives in the existing trailer. The last `u16` of a data
  block is the restart count, capped at 128 by the writer, so bit 15 is
  always free. Bit 15 set means compressed: the low 15 bits are the
  compressed length `clen`, and bytes `[0..clen]` hold the stream. Bit
  15 clear is the unchanged raw layout.
- The stream encodes the whole logical block `[0..BLOCK-4]`, so it
  always decompresses to exactly `BLOCK - 4` bytes and the existing
  parsers run on the output untouched. The stream is token/literal/match
  triples: a 4-bit literal length and a 4-bit match length minus 4,
  LZ4-style extension bytes, a `u16` little-endian match offset, and a
  minimum match of 4.
- The CRC still covers the physical block. The decoder bounds-checks
  every read and write and returns `CorruptBlock` on a malformed
  stream.
- Callers pass `Option<&mut CompressScratch<BLOCK>>` to
  `TableWriter::push`/`finish` and `write_table` (`None` stores raw).
  `Db::flush` keeps one scratch on its stack per flush, compaction one
  per job, and `Db` gains a one-block `decomp_scratch` for reads.

## Consequences

- Tables mix compressed and raw blocks freely, and pre-v0.13 all-raw
  tables read unchanged.
- The crash model is unchanged: compression is a pure function applied
  at seal time, and a torn compressed block fails its CRC like a raw
  one.
- Measured at `BLOCK = 4096`: structured key-value data compresses to
  905/4092 (0.221); random data stays raw. Archive and ingest preserve
  the flags bit for bit.
- Every read of a compressed block pays a decompression pass, and every
  sealed data block pays one trial compression plus 8 KiB of transient
  scratch (`CompressScratch<4096>` is 8,200 bytes).
- Limits: best effort per block, so random, already-compressed and
  marginally compressible blocks stay raw. No dictionary, no training,
  and no cross-block matches: each block is independent, so random
  access never decompresses a neighbor.
- `clen` has 15 bits, so a block whose compressed form exceeds 32,767
  bytes is stored raw (irrelevant at `BLOCK = 4096`).

## References

- [Milestone log](../history/milestones-v0.1-v0.16.md) v0.13 (Scope, Format, Caller scratch, Crash model, Proof,
  Honest limits, Measured)
- SPEC §1, §4.4
