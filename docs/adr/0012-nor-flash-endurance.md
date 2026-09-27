# ADR-0012: NOR flash endurance — group commit and a manifest ring, no page-program path yet

Status: Accepted (v0.17, 2026-09-24)

## Context

On `FlashBlockDevice` every block write is one sector erase plus
program. The architecture review (F9) worked through what that means
for W25Q-class NOR (100k P/E cycles, 45–400 ms per 4 KiB erase):

- Each durable single-op write commits one whole WAL block, so a
  worst-case ESP32-profile record (119 bytes) costs a 4,096-byte erase:
  about 34× write amplification, and roughly 20 durable writes per
  second.
- A WAL sector is erased once per `wal_blocks` commits.
- Every flush, compaction output, archive and ingest rewrites the
  manifest. With two copies (ADR-0003), those two sectors wear out
  first: on the order of a month at one write per second with the
  ESP32 profile's 16-entry memtable.
- The old allocator was first-fit from the lowest free block, which
  concentrated table wear at the start of the region.

## Decision

- **Group commit is `WriteBatch`.** A batch commits every op that fits
  one WAL block with one block write (one erase). `Db::write` and
  `flash.rs` document it as the endurance lever. No `put_nosync` +
  `commit()` API is added: it would weaken the "durable when it
  returns" contract of every single-op write for a gain `WriteBatch`
  already offers.
- **Manifest ring.** `Config::with_manifest_ring(n)` keeps `n` manifest
  copies back to back; commit `seq` goes to copy `seq % n`, and
  recovery takes the newest intact copy (ADR-0011). Manifest erases
  spread over `n` copies instead of two.
- **Next-fit table slots.** The slot allocator (ADR-0009) hands out
  slots next-fit from a rotating hint, so successive tables land in
  successive slots across the whole region.
- **No NOR page-program append path yet.** Programming WAL records into
  an already-erased sector (as LittleFS and SPIFFS do) would remove the
  per-write erase, but it needs a new optional `BlockDevice` capability
  (program without erase, plus a partial-block read contract), a WAL
  format that tolerates partially programmed blocks, and its own crash
  model. It is deferred until there is hardware to measure it on.

## Consequences

- A caller that batches writes pays one erase per batch; a caller that
  does not still pays one per write. The docs say so.
- With a ring of `n` copies, the manifest region takes `n ×
  Manifest::max_blocks` blocks; `Db::open` checks the layout
  (`Error::BadConfig`).
- WAL wear and single-write latency are unchanged. The figures above
  are analysis, not silicon measurements.

## References

- `docs/ARCHITECTURE_REVIEW.md` F9
- `src/flash.rs` (Endurance), `Db::write`, `Config::with_manifest_ring`
- ADR-0003, ADR-0009, ADR-0011
