# ADR-0031: A point read checks only the cached bytes it reads

Status: Accepted (unreleased, 2026-11-25)

## Context

Since ADR-0016 every block-cache hit is checked against RAM checksums
before the reader sees the image: a slot keeps one sum per chunk of
its image, and a hit recomputes them over the bytes it hands out. The
lent read that replaced the copy (`CachePort::with_range`) kept that
rule, so a point read's data-block hit summed the whole block, every
chunk, before the search began: 16 chunks in the library's
`BlockCache`, 64 in the host store's cache (one per 256 bytes of a
16 KiB block).

A point read does not read the whole block. In a raw block (levels 0
and 1 since ADR-0030) it reads the restart trailer, a binary search's
few restart entries and the short run of entries up to its key: a few
hundred bytes of 16 KiB. Step 21 of `docs/THROUGHPUT.md` profiled the
summing at about 16.8 K instructions a hit, and a prototype that
checked only what the search reads ran 8.5% fewer instructions per
read at 300 K keys and 1.3% at 1 M.

The bloom probe already pays for only the chunks it reads
(`read_range_verified`, ADR-0017). The index search cannot: it parses
every entry of the index block to count them (`index_partition`), so
it reads the whole block anyway, and it keeps the whole-image check.

## Decision

**A cache can lend an image with a check instead of checked.**
`CachePort::with_checked` hands `read` the whole cached image where it
lies, unchecked, and a `check(from, to)` that sums the chunks
overlapping `from..to` (each at most once per lookup) and says whether
they still match. The lookup is a hit only if every check held; a
check that fails drops the entry, counts it rejected, and the lookup is
a miss, as a failed whole-image check was. An empty range holds; a
range past the image fails. `BlockCache` (and so `RefCell<BlockCache>`)
and the host store's `SharedCache` implement it. The provided method,
for a cache that only lends whole images, checks the whole image first
and hands `read` a `check` that always holds: what such a cache did
before.

**The point read's search checks every range before it acts on it.**
`data_lookup` takes the `check` and calls it on the restart trailer and
restart array before reading an offset from them, on each entry's whole
extent (`off..next`, which holds every byte its parse read) before
comparing its key or copying its value, and on a run of padding before
calling it padding. `false` ends the search with `CorruptBlock`. A
compressed block's stream is checked whole before it is inflated
(ADR-0027's partial inflate reads it from the front). The search of a
block read from the device, or copied out of a cache that does not
lend, passes a check that always holds.

**An error is checked against the whole image.** A parse can fail on
bytes it read before it could check them: a flipped length that runs
past the block, a flipped op byte. When the search ends in any error,
`lookup_data_block` checks the whole image; if any chunk fails, the
lookup is a miss and the block is read from the device. So a fault in
RAM is never reported as `CorruptBlock`; only a block that is malformed
on the device (and passed its CRC) is.

What a point read guarantees is unchanged: a fault in a cached image's
RAM never changes its answer. Either the search never acts on the
faulty byte, or a check over it fails before it does and the block is
read again.

## Consequences

- **Cheaper hits** (`docs/THROUGHPUT.md`, step 22, on identical
  tables against the build before). A raw data block's hit sums 8.4
  of its 64 chunks on average: the trailer, one per restart the binary
  search parses, and the run it scans. A compressed block checks its
  whole stream, about half the block. Instructions per point read fall
  19.7% at 300 K keys (every table raw) and 8.1% at 1 M when the keys
  read are cached; point reads served from the cache are 10% to 13%
  faster on one reader and 12% to 15% on four. Where most reads go to
  the device (the bench's full key range at 1 M keys) the change is
  inside run-to-run spread. Seeks and scans do not use it and did not
  move.
- **The index block is now the larger check.** It is still summed whole
  on every point read, 25% to 31% of a read served from the cache.
  Searching it a range at a time would need its entries counted and
  located without parsing them all: a format change, left as a lever.
- **A fault can stay longer.** A flip in a chunk no lookup reads is no
  longer found and dropped by the next hit on the block; it is found by
  the first read that touches that chunk (a point read whose search
  crosses it, or a scan or compaction, which still check whole images)
  or goes with the slot's eviction. It never reaches a
  caller.
- **Stats.** A lookup is still counted once: a hit, or a miss and a
  rejection. A lookup whose search stops short of a damaged chunk is
  now a hit, where it was a rejection.
- **API.** `CachePort::with_checked` and the `cache::CheckedRead` type
  are new; the trait method is provided, so other implementations build
  and behave as before. As with `with_range`, `read` must not call back
  into the cache, and `check` is valid only while `read` runs.
- **Tests.** `sstable::partial_inflate_tests` checks that a lookup
  checked range by range answers as the whole-block search does, that a
  raw block's search touches a few chunks, and that a check failing on
  any of them ends the search. It also flips one bit of every byte of
  every data block of a raw and a compressed table, one at a time, in
  the cache's RAM, and checks that every lookup of the block answers as
  if the image were intact, and that one lookup notices fewer than half
  of the flips in a raw block. Removing the scan's entry check or the
  whole-image check on an error fails it. `cache::tests` and the host
  store's tests check the checked lend itself: a check clear of a flip
  holds, one over it fails and drops the entry, and the provided
  method checks the whole image before `read` runs.
- **Review.** Two adversarial reviews found no case where a flip
  changes an answer. One drove `TableReader` through the cache over
  every bit of every block at block sizes 512, 1,000 (not a multiple of
  16, so the last chunk is longer), 4,096 and 16,384, raw and
  compressed, with values too large for the caller's buffer: about 160
  million lookups, every answer as from the intact device. The other
  did the same through the host store's `Store::get`, with three reader
  threads running while the damage was replanted. Some of the search's
  checks turned out to be covered by others (the binary search's,
  because the scan re-reads the restart it starts from; the padding's,
  because a flip in zeros fails the parse and the whole image is
  checked); they stay, since the cover depends on the block's layout.
  Fixed from the reviews: the chunk counts the docs gave, the rule
  that `read` must not call back into the cache, a provided `check`
  that held for ranges past the image, and a compile-time bound on
  the chunk mask's width.

## References

- [ADR-0016](0016-cache-hits-reverify-with-a-ram-checksum.md) (the
  RAM checksums), [ADR-0017](0017-bucketed-bloom-and-ranged-reads.md)
  (ranged reads), [ADR-0027](0027-point-reads-inflate-to-the-key.md),
  [ADR-0030](0030-no-compression-in-shallow-levels.md)
- `src/cache.rs` (`CachePort::with_checked`, `BlockCache::with_checked`,
  `CheckedRead`), `src/sstable.rs` (`data_lookup`,
  `lookup_image_checked`, `lookup_data_block`),
  `examples/kvstore/cache.rs` (`SharedCache::with_checked`)
- [`docs/THROUGHPUT.md`](../THROUGHPUT.md)
