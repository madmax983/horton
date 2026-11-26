# ADR-0016: Block-cache hits re-verify with a RAM checksum

Status: Accepted (v0.20, 2026-11-20). Supersedes ADR-0014.

## Context

ADR-0014 kept a full CRC-32 on every block-cache hit. Its reason stands:
a cached image sits in RAM for the life of the process, firmware RAM has
no ECC, and a flipped bit in an index, bloom or data block gives a wrong
answer, not an error. It also named the price, and said what a change
would have to do: "say what replaces the protection".

The price turned out larger than a firmware profile shows. On the host
key-value store (`examples/kvstore`: 16 KiB blocks, four client threads)
a profile of db_bench's workloads put **53% of all CPU** in
`check_block_crc`. A point read cost 134 µs, nearly all of it
re-checking blocks that had already passed: up to ten tables probed,
two blocks each, 8 µs of CRC per block. RocksDB on the same machine does
the same read in 2 to 13 µs. `docs/THROUGHPUT.md` has the measurements.

Two facts shape the answer:

- The cache needs to know one thing on a hit: are these the bytes I was
  given? It does not need to know whether they are a valid block. The
  CRC answered the second question in order to answer the first.
- Table-driven CRC-32 is about the slowest way to answer either. It
  runs at 2.0 GB/s here in safe Rust (slicing-by-8), is bound by table
  lookups, and cannot use the CPU's instructions for it: the library
  forbids `unsafe`.

## Decision

**Only verified images enter the cache.** The read funnel
(`sstable::read_block_verified`, `read_range_verified`) reads a missed
block from the device, checks its CRC-32, and inserts it only if the
CRC matched. A block that fails is returned as `CorruptBlock` and is
not kept.

**Every hit is checked against a RAM checksum.** A slot stores
`cache::image_sum` of the image it was given, computed from the
caller's verified buffer. A hit recomputes the sum over the bytes it
hands out and compares. If they differ, the slot is dropped and the
lookup is a miss, so the caller reads the device again. The CRC-32 is
not run on a hit.

**The sum is per chunk.** An image is summed in 16 chunks
(`SUM_CHUNKS`). `get_range_into` copies a byte range of a cached block
and checks only the chunks the range touches. A reader that needs 45
bytes of a footer or 64 bytes of a bloom block pays for one chunk, not
the block (ADR-0017 uses this). Since ADR-0031 a point read's data
block is lent with a check instead, and the search checks each range
just before it acts on it: what it never reads, it never checks. Since
ADR-0032 the index block of a point read or a seek is lent the same
way.

**The sum is four multiply-rotate lanes** (xxHash's round): 64-bit
lanes on 64-bit targets, 32-bit lanes elsewhere, because an ESP32's
multiplier is 32 bits wide. It lives only in RAM. It is not a format,
it may differ between targets, and it may change between versions.

What it guarantees, for two images of equal length:

- A change confined to one lane word is always detected: every
  single-bit flip, and every burst inside one 8-byte word (4-byte on
  32-bit targets). Each lane step is a bijection of its word, and the
  final combination keeps a difference in exactly one lane.
  `cache::tests` checks every bit of several blocks exhaustively.
- Any other change is missed with probability about 2⁻⁶⁴. The lanes are
  not linear, so equal and opposite changes in two words do not cancel
  as they would in a sum or an XOR.

## Consequences

- **Speed.** On x86-64 the sum runs at 11 to 14 GB/s against CRC-32's
  2.0: a hit on a 16 KiB block costs 1.4 µs of checking, not 8.3 µs.
  The kvstore's point reads went from 134 µs to 39 µs on this change
  alone, and sequential scans doubled.
- **The protection changes shape; it is not removed.** CRC-32
  guarantees every burst up to 32 bits and every error of up to three
  bits in a 4 KiB block. The sum guarantees every error inside one
  word and is probabilistic beyond that, at 2⁻⁶⁴ where a CRC-32's
  misses are at 2⁻³². For the fault this guards against (a few
  independent flips in SRAM) both detect everything that can be
  expected to happen. Nothing here weakens the check of what comes off
  the device: that is still CRC-32, on every device read.
- **A flipped bit heals.** Under ADR-0014 a damaged cached image
  reported `CorruptBlock` on every hit until it was evicted. Now it is
  one extra device read, counted in `CacheStats::rejected`, and the
  read succeeds.
- **A corrupt device block is never cached.** Every lookup that needs
  it goes to the device and reports it. This replaces ADR-0007's
  byte-identity rule ("a hit is indistinguishable from a re-read,
  corruption included"): a hit is now indistinguishable from a re-read
  of a good block. `tests/cache.rs` and `tests/fuzz.rs` are rewritten
  to that rule.
- **RAM.** A slot carries 16 sums: 144 bytes of tag and sums against 16
  bytes before. The ESP32-S3 profile's two slots add 264 bytes to
  `Db`; the 112 KiB budget holds (BUDGET.md).
- **`BlockCache::put` trusts its caller.** The cache hands back exactly
  what it was given. Code outside horton that inserts unverified bytes
  gets them back unverified.
- **Not covered, as before:** the caller's own buffers. Between the
  check and the parse, a verified image sits in the `Db`'s scratch
  block, where a flip is as undetected as it always was.

## References

- [ADR-0014](0014-cache-hits-reverify-crc.md) (superseded),
  [ADR-0007](0007-clock-block-cache.md) (the cache; its byte-identity
  rule is narrowed here), [ADR-0017](0017-bucketed-bloom-and-ranged-reads.md)
- `src/cache.rs` (`image_sum`, `get_into`, `get_range_into`),
  `src/sstable.rs` (`read_block_verified`, `read_range_verified`)
- [`docs/THROUGHPUT.md`](../THROUGHPUT.md)
