# ADR-0017: Bucketed bloom filter; point reads fetch ranges of metadata blocks

Status: Accepted (v0.20, 2026-11-20)

## Context

A point read probes every table whose key range covers the key: every
level-0 table and one table per deeper level, up to ten in the host
store's shape. For each it read two whole blocks to learn almost
nothing: the footer, of which 45 bytes carry fields, and the bloom
block, of which `k` bits (at most 30) are looked at. With ADR-0016
those are cache hits at 2 µs each instead of 8, but twenty of them per
read still outweigh the one index and one data block that hold the
answer.

The footer could be read as a range at once: its fields are the first
45 bytes. The bloom filter could not. Its probes were spread over the
whole filter by double hashing, so reading `k` bits meant touching `k`
places in the block.

## Decision

**The bloom filter is bucketed.** The filter's `BLOOM_BYTES` are split
into 64-byte buckets (`BLOOM_BUCKET_BYTES`). A key hashes to one
bucket, and its `k` probes are double-hashed inside that bucket's 512
bits, with an odd stride so they never repeat. A filter smaller than
one bucket is a single bucket; bytes past the last whole bucket are
unused. `sstable::BloomProbe` computes the bucket and tests it.

**Point reads ask for ranges.** `TableReader` reads the footer as
bytes `0..45` and the bloom as the key's bucket, through
`read_range_verified`. On a cache hit that copies and checks only the
chunk the range lies in (ADR-0016). On a miss the whole block is read
and CRC-checked as before, then cached.

**The format changes.** A filter written by the old rule and read by
the new one would report keys it holds as absent. The table magic
becomes `hrtsst03`, and the manifest magic `hrtman07` (same layout as
`hrtman06`), so a v0.19 image is refused when it is opened and not at
its first read.

## Consequences

- **A probe that the filter rejects costs well under a microsecond**
  once the two blocks are cached: two tag lookups and two chunk sums.
  In the host store a read of an absent key went from 26 µs to 18 µs
  on this change alone, most of the rest being thread hand-off (see
  `docs/THROUGHPUT.md`).
- **Slightly more false positives.** Keys are not spread evenly over
  buckets, and a fuller bucket filters worse. At 10 bits per key the
  rate goes from 0.84% to 0.98%, at 16 from 0.05% to 0.09% (Poisson
  bucket loads, `k` as `bloom_k` picks it). Below 4 bits per key the
  two are the same, and equally poor.
- **The ESP32-S3 profile is unchanged in effect.** Its 64-byte filter
  is exactly one bucket. A filter size that is not a multiple of 64
  loses the remainder (at most 63 bytes).
- **Old images are rejected, as the format policy requires.** Archives
  made by v0.19 (`flight_recorder`'s export) cannot be restored by
  v0.20.
- **Not done:** the index block is still read and checked whole on
  every lookup that passes the filter, and it is the larger half of
  what such a lookup now reads besides its data block. Verifying only
  the index entries a search touches needs per-entry or per-group
  checksums in the block, which costs index capacity (a table's index
  is one block). That trade is left for when it is needed. (Done
  since: ADR-0032, with the cache's RAM checksums and restart offsets
  in the index block instead of checksums in it.)

## References

- [ADR-0016](0016-cache-hits-reverify-with-a-ram-checksum.md) (ranged
  cache reads), [ADR-0007](0007-clock-block-cache.md)
- SPEC.md §4.4 (table format), §4.8 (read path)
- `src/sstable.rs` (`BloomProbe`, `read_range_verified`,
  `TableReader::open_cached`, `lookup_at`), `tests/sstable.rs`
- [`docs/THROUGHPUT.md`](../THROUGHPUT.md)
