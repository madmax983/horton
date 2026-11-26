# ADR-0026: The hot path, measured again: CRC, codec, lent cache images, the memtable's sorted view

Status: Accepted (unreleased, 2026-11-23)

## Context

v0.20 made reads cheap by keeping CRC-32 off cache hits (ADR-0016), by
reading only what a probe uses (ADR-0017) and by taking reads out of
the host store's lock (ADR-0022, ADR-0023). A profile of the host
store after those passes (`examples/kvstore`, callgrind, 200 K keys,
one thread) put the remaining instructions here:

| | fill (`fillrandom`) | reads (`readrandom`, `seekrandom`, `readseq`) |
|---|---:|---:|
| LZ77 compression, in `seal_block` | 38% | 5% |
| CRC-32: sealing blocks, checking device reads, WAL records | 18% | 4% |
| LZ77 decompression, and its `memcpy` calls | 3% | 22% |
| copying whole 16 KiB images out of the cache | | 17% |
| zeroing a 4 KiB value buffer per table a read probes | | 14% |
| shifting the memtable's sorted slots on insert | 4% | |

Four ideas were on the table. Two were already done: a cache hit
runs no CRC-32 (ADR-0016), and the memtable counts its range
tombstones, so a point read does not scan for them when there are
none. The other two:

- **A carry-less-multiply CRC** (PCLMULQDQ, ARMv8 PMULL). The library
  is `#![forbid(unsafe_code)]`. Since Rust 1.87 most `core::arch`
  intrinsics are safe to call from a function with the right
  `#[target_feature]`, but calling such a function from one without
  the attribute still needs `unsafe`, even when the build enables the
  feature globally (`-C target-feature`). rustc says so in as many
  words. There is no safe way in.
- **Group commit across WAL blocks.** One write group is one WAL block
  and one `fdatasync`, so a sync covers at most about 120 puts of 100
  bytes. With the host store's four writer threads a synced group
  averages 2.6 client writes (`docs/THROUGHPUT.md`): the block is never
  what limits a group there. Making one sync cover several blocks would
  also mean splitting `Db::write` into "append to the WAL" and "apply
  to the memtable", so that no reader sees a write before it is
  durable.

## Decision

Seven changes, none to the on-disk format. Each one gives the same bytes,
or the same answers, as before.

1. **CRC-32 runs as three interleaved chains** on 64-bit targets, for
   inputs of 768 bytes or more (`crc::STREAMS_MIN`). One slicing-by-8
   step waits about ten cycles for the step before it; three
   independent chains fill that wait. The three registers are joined
   with the CRC's own algebra: the register a segment leaves is what it
   would be from zero, `XOR` the first register multiplied by
   `x^(8·len)` modulo the polynomial. Four chains measured slower than
   three (register pressure). 32-bit targets, the ESP32-S3 among them,
   keep the one chain: an in-order core gains little from more.
2. **CRC-32 carries the register across runs of zero bytes** instead of
   computing through them, by the same multiplication. A compressed
   block is about half zero fill; so is the gap in a raw data block and
   the unused end of an index. Only runs the function has seen to be
   zero (in 256-byte steps) are carried. Damage where zeros belong is
   run through the tables and changes the result as it must. This
   covers every CRC the library computes, on write and on device read.
   The operator table has an entry per bit of `usize`, so any length
   has one (review found a 32-entry table panicking at 4 GiB).
3. **The LZ77 encoder** counts a match eight bytes at a time, loads its
   4-byte words with one bounds check, indexes four positions of a
   match where it indexed every one, and, like LZ4, lengthens its step
   after 64 positions with no match. The stream format and the decoder's
   rules are unchanged: old images decode as before, and images written
   now decode on older versions. The output is about 2% smaller on the
   benchmark's blocks: the table is updated at the probed position
   itself, where it was skipped.
4. **The LZ77 decoder** copies a short literal run or match as one fixed
   16-byte chunk (where a copy of `n` bytes is a call), fills a run of
   one byte (every block's zero fill) instead of looping, and expands
   other overlapping matches by doubling. The bytes a fixed chunk writes
   past the run lie ahead of the output position and are written again
   before anything reads them.
5. **A cache can lend an image in place** (`CachePort::with_range`, a
   provided method). The cache checks the chunks a range touches
   against their RAM checksums, as `get_range_into` does, then hands
   the bytes where they lie to a closure. The index search runs that
   way, and a compressed data block is inflated straight from the
   cache, where 16 KiB were copied out first. Each block read is one
   lookup, so `CacheStats` counts a block once: a miss still means the
   device did the work. (A first version probed the compression flag
   and then lent only the compressed prefix, half the summing, but that
   was two lookups and counted every block twice; review caught it.)
   The default answers `None` ("I do not lend"), and the reader copies
   as before, so a `CachePort` written before this keeps working. The
   library's `RefCell<BlockCache>` and the host store's `SharedCache`
   both lend. ADR-0016's rule is untouched: a damaged chunk is never
   handed out, lent or copied.
6. **Point reads no longer zero a `VAL_MAX` buffer per table.** While
   no table has produced a value yet, the lookup writes straight into
   the accumulator's stage, which has nothing to protect. A spare
   buffer is used only when a staged value could be beaten. Version
   pruning makes that rare.
7. **The memtable keeps a sorted view of 4-byte positions** (`order`)
   over slots kept in arrival order. A random insert into a full host
   memtable (4,096 entries) shifted 32-byte slots, 32 KB on average; it
   now shifts positions, an eighth as many bytes.

## Consequences

- **Speed.** On the benchmark blocks, CRC-32 of a 16 KiB block runs at
  3.6 GB/s where it ran at 1.4, and at 5.2 GB/s for a block that is
  half zeros. The encoder is 1.45 times faster and the decoder 1.4. A
  memtable insert at 4,096 entries takes 0.34 µs where it took 0.77.
  The host store's end-to-end numbers are in `docs/THROUGHPUT.md`.
- **The memtable trades walking for inserting.** An in-order walk now
  goes through the sorted view, about 1.5 ns an entry against 1.0. A
  flush walks the table once (a few microseconds), and a scan page of
  1,024 memtable keys takes about 3 µs longer. Each insert saves more
  than that.
- **RAM.** The sorted view is 4 bytes per memtable entry: 64 bytes on
  the ESP32-S3 profile (16 entries), 16 KiB on the host store. The
  profile's `get()` future grew 72 bytes. The peak is 114,144 of
  114,688 bytes (BUDGET.md).
- **The ESP32-S3 keeps the one-chain CRC** and gains only from zero
  runs, the codec and the memtable. Nothing here was measured on the
  device.
- **Not done:** a hardware CRC (needs `unsafe`, see Context), and
  group commit across WAL blocks (does not limit any measured workload,
  and needs the write path split first).
- **Not done here:** a point read still inflated a whole 16 KiB data
  block to find one key: the restart offsets that let it binary-search
  are at the end of the block. (Done since: ADR-0027.) Inflating only up to the key would need a
  resumable decoder and a linear search. The index search still parses
  every entry of the index, which the walk bound and the check for
  structural corruption both use. (Done since: ADR-0034, which counts
  and locates them with a trailer.)

## References

- [ADR-0016](0016-cache-hits-reverify-with-a-ram-checksum.md) (the
  rule for cached images, which lending keeps),
  [ADR-0017](0017-bucketed-bloom-and-ranged-reads.md) (ranged reads),
  [ADR-0022](0022-host-cache-hits-write-nothing-shared.md) (the host
  store's cache, whose immutable entries make lending safe),
  [ADR-0008](0008-hand-rolled-lz77-block-compression.md) (the codec)
- `src/crc.rs` (`update_streams`, `zeros_op`, `crc32`),
  `src/compress.rs`, `src/cache.rs` (`with_range`),
  `src/sstable.rs` (`read_data_block`, `inflate_lent`, `parse_verified`),
  `src/db/read.rs` (`consider_table`), `src/memtable.rs` (`order`)
- [`docs/THROUGHPUT.md`](../THROUGHPUT.md), [BUDGET.md](../../BUDGET.md)
