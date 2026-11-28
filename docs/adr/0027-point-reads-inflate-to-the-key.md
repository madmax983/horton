# ADR-0027: A point read inflates a data block only as far as its key

Status: Accepted (unreleased, 2026-11-23; over a block with entry points, from its key's chunk: ADR-0040)

## Context

A data block is 16 KiB on the host store, LZ77-compressed to about half
(ADR-0008). Its entries come first and its restart offsets (one per
16 entries) last, and a point lookup binary-searches the restarts to
find where to start a short linear scan. So a point read inflated all
16 KiB of a block to read one entry, about 3 µs, even when the entry
was the block's first. ADR-0026 listed this as the largest per-read
cost it left.

The restarts cannot move to the front without a format change, and an
LZ77 stream cannot be entered in the middle.

## Decision

**A point lookup inflates as it scans.** The decoder became resumable
(`compress::Inflate`: `run(src, dst, until)` decodes whole tokens until
`until` bytes are final, and can be called again). `decompress` is that
with `until` the whole block, unchanged. For a compressed block, the
lookup (`sstable::data_lookup_inflating`) scans the entries from the
first, keeping the decode one step (1 KiB) ahead of the entry it
parses, and stops where the answer is decided:

- at the newest version of the key visible at the snapshot: its value,
  or a tombstone;
- at a key larger than the one sought: absent.

**Everything else is the whole-block search, unchanged.** When the scan
reaches the end of the entries (zero padding, or the restart offsets,
whose first is always 0 and so parses as an empty key), or meets
anything that does not parse, it inflates the rest of the block and
runs `data_lookup` on it. That covers a version run that may go on in
the next block, and it keeps every corruption check that the
whole-block search makes there.

**The answers are the same.** The whole-block search's binary search
only picks where its linear scan starts; for a block the writer
produced, a scan from the first entry passes over smaller keys to the
same entry and decides the same way. A unit test compares the two on
every data block of compressed and raw tables, for every key, several
snapshots and a buffer too small for the value, and checks that a hit
on a block's first key leaves the second half of the output untouched.

The block is searched where it lies in the cache when the cache lends
images (`CachePort::with_range`, ADR-0026): one lookup, counted once.
A raw block is searched in place, with no copy.

## Consequences

- **Speed.** A point read inflates on average about half of the one
  data block it reads, and the binary search over the restarts becomes
  a linear scan over the entries before the key (a key compare each).
  One reader thread on the host store, 300 K keys: `readrandom` 9%
  faster. Scans, seeks and compaction need whole blocks and are
  unchanged.
- **Less is checked on the way to an early answer.** A whole-block
  search validated the restart trailer before it read an entry, and
  inflating the whole block validated the rest of the compressed
  stream. A lookup that stops early does neither for the part it did
  not reach. That part has passed its block CRC (or its RAM checksum,
  ADR-0016), so only a block that was written wrong and checksummed
  could tell the difference; a misread of such a block was possible
  before too, wherever the damage did not stop the search.
- **Not done:** a block whose key is past its last entry still inflates
  whole (the run may continue in the next block, and the whole-block
  rules decide that). A read of a key absent from a table usually
  stops at the bloom filter and never reaches a data block.

A seek does the same since ADR-0039, and checks the stream as it
decodes rather than whole first; its later steps go on with the decode.

## References

- [ADR-0026](0026-hot-path-pass-crc-codec-cache-memtable.md) (where
  this was listed as not done),
  [ADR-0008](0008-hand-rolled-lz77-block-compression.md) (the codec),
  [ADR-0016](0016-cache-hits-reverify-with-a-ram-checksum.md)
- `src/compress.rs` (`Inflate`), `src/sstable.rs`
  (`data_lookup_inflating`, `lookup_image`, `lookup_data_block`, and
  the `partial_inflate_tests` module)
- [`docs/THROUGHPUT.md`](../THROUGHPUT.md)
