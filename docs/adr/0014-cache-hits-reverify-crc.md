# ADR-0014: Block-cache hits re-verify the CRC

Status: Accepted (v0.17, 2026-09-29)

## Context

ADR-0007's cache stores the physical block image exactly as the device
returned it, and every reader runs its CRC check after the cache. So a
hit pays a block copy plus a full-block CRC-32 even though the same
bytes passed the check when they were read.

A Bolt profile (callgrind, `benches/write_path.rs`) showed the cost:
`check_block_crc` is 70.4% of 232.9M Ir. The bench does 13,936 block
reads, of which the cache serves 10,479 (75%) and the device 3,457
(`Db::cache_stats`). Roughly 123M Ir, about 53% of the bench, is
re-checking bytes already verified. The CRC loop is already
slicing-by-8 at about 2.9 Ir per byte, so the only large lever left is
to skip it on hits. Bolt runs kept rediscovering this (issues #3, #13,
#15).

Skipping would need two changes: cache only CRC-verified blocks (today
`read_block_cached` inserts before any caller checks), and treat a hit
as "verified".

## Decision

A cache hit is treated exactly like a device read: the caller still
checks the CRC (and magic, bloom, decompression) on the copied image.
We do not skip verification on hits, and we do not add a "verified" bit
to cache entries.

Correctness over speed. The re-check is the only thing that catches a
bit flip in the cached copy. The ESP32-S3's SRAM has no ECC, the cache
holds up to `CACHE` block images for the life of the process, and a
silently wrong index, bloom or data block gives a wrong answer (for
example a stale read from a deeper table), never an error. The cost of
the check is bounded and paid only in CPU; the cost of missing a flip
is unbounded.

## Consequences

- Hits keep the byte-identity rule of ADR-0007: a hit is
  indistinguishable from a re-read, corruption included. Corrupt
  images may sit in the cache; every hit on one reports
  `Error::CorruptBlock`.
- Read-heavy workloads stay CRC-bound. On `write_path`, `check_block_crc`
  remains about 70% of instructions. A cache saves device reads, not
  CRC work; on real flash the device read is the expensive part.
- Performance work should not propose skipping CRC on hits. A change
  here needs a new ADR superseding this one, and would have to say what
  replaces the protection (ECC RAM, an opt-in const-generic flag, or a
  periodic scrub) and rewrite the corruption-through-cache tests in
  `tests/cache.rs`.
- Other levers on CRC cost (caching verified footer/index/bloom state
  per table, narrowing CRC coverage to payload length) are separate
  format or ownership changes; see issue #15. This ADR does not decide
  them.

## References

- [ADR-0007](0007-clock-block-cache.md) (byte-identity rule)
- `src/sstable.rs` `read_block_cached`, `read_data_block`,
  `check_block_crc`; `src/cache.rs`
- Bolt findings: issues #3, #13, #15
