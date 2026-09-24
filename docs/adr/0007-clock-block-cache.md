# ADR-0007: CLOCK block cache keyed by (table id, block id), caller-owned

Status: Accepted (v0.16, 2026-09-23)

## Context

Before v0.16 every point read and scan read its SSTable blocks from the
device, however often the same block was read. Since v0.15, reads also
scan range-tombstone blocks per key. A cache has to fit horton's memory
rules (ADR-0002): fixed size, never global, never allocated, and counted
in the RAM budget.

## Decision

- `cache::BlockCache<const BLOCK: usize, const SLOTS: usize>` lives
  inside `Db` as a const-generic field with `CACHE` slots, built by
  `Db::new`. `CACHE = 0` disables it.
- It serves point reads (`TableReader`), forward scans and reverse
  scans, for data, index, bloom, footer and range-tombstone blocks. WAL,
  manifest and compaction-merge reads bypass it.
- The key is `(table_id: u32, device_block_id: u64)`. Table ids only
  increase and are never reused within a manifest lineage, and a table's
  blocks are immutable once its id is visible. A stale entry can never
  describe live data, even after the blocks are reclaimed, so
  correctness rests on id monotonicity, not on timely invalidation.
- Invalidation is hygiene only: when compaction drops input tables, `Db`
  calls `invalidate_table(id)` to free their slots.
- Eviction is CLOCK (second chance): one reference bit per slot and one
  hand, O(1) amortized, no linked lists. It was chosen over
  direct-mapped (a sequential scan would keep evicting hot index blocks
  it collides with) and over true LRU (a doubly-linked list is more
  mutable state and proof surface, for a marginal win, since scan
  streams defeat LRU and CLOCK equally).
- Scan-loaded data blocks insert cold, so a full scan sweeps its own
  blocks out instead of displacing the point-read hot set. Index, bloom,
  footer and range-tombstone blocks insert hot.
- The cache stores the physical block image exactly as the device
  returned it. CRC checks, decompression and TTL/range shadowing run
  after it, so a hit behaves exactly like a re-read, corruption
  included.
- It follows `get_scratch`'s `RefCell` discipline. If two interleaved
  `get`s contend, one silently bypasses the cache via `try_borrow_mut`.

## Consequences

- The cache holds no durable state and adds no commit points. A crash
  empties it, and recovery never consults it.
- On the standard test profile (`CACHE = 8`), repeated point reads do
  no table-region device reads. SPEC names the v0.15 per-key
  range-tombstone scan as the biggest winner.
- RAM cost: `BlockCache<4096, 8>` is 32,920 bytes and
  `BlockCache<4096, 2>` is 8,248. The ESP32-S3 profile uses 2 slots and
  totals 92,520 bytes against a 98,304-byte budget.
- Limits: the first read of a block is never faster, and a scan larger
  than the cache still streams from the device. Hit rate depends on the
  workload; `Db::cache_stats` exposes it.
- The cache is inline in `Db`, so it grows `Db` wherever `Db` lives.
  v0.16 notes that, with the ~352 KiB `Manifest::recover` stack probe,
  it pushes stack-heavy debug tests near the 2 MiB test-thread limit.

## References

- [Milestone log](../history/milestones-v0.1-v0.16.md) v0.16 (Scope, Cache key, Invalidation protocol, Eviction
  policy, Byte-identity rule, Concurrency, Crash model, Measured,
  Honest limits)
- [Milestone log](../history/milestones-v0.1-v0.16.md) v0.15 (range-tombstone scan cost), §1
