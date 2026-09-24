# ADR-0003: Double-buffered manifest as the single commit point; reserve-then-claim allocation

Status: Accepted (v0.2 and v0.3, 2026-09-13; reclamation added in v0.4.1).
Allocation superseded by ADR-0009; one-block slots superseded by ADR-0011
(v0.17). The single-commit-point rule stands.

## Context

Flush and compaction write many blocks, and a crash can stop them at any
block. SPEC requires that a crash leave either the old state or the new
one, never a mix (§4.5, §4.6). The device only offers whole-block reads,
writes and `flush` (ADR-0001).

## Decision

- The manifest (levels of `TableRef`s, `wal_head`, `next_table_id`, and
  a manifest sequence) is the crash-safe root pointer. It is stored
  double-buffered in two fixed block slots: write slot `seq % 2` with a
  CRC, then `flush()`. Recovery picks the valid slot with the higher
  sequence.
- That write is the only commit point. The flush protocol is: write all
  SSTable blocks, `flush()`, write the manifest slot, `flush()`; WAL
  blocks at or before `wal_head` are then free. Compaction (v0.4), WAL
  wrap (v0.3), `archive_commit` (v0.10) and `ingest_table` (v0.12)
  each become visible in one manifest write too.
- Allocation (§7): fixed WAL, table and manifest regions from `Config`.
  Each region gets a bump pointer plus a sorted `FreeList<CAP>`.
  Allocation is free-list first (first-fit contiguous run), then the
  bump. The run is only *reserved* until the manifest commit lands, and
  is claimed after it, so a failed flush changes nothing.
- The open-time sweep treats any block not referenced by the manifest or
  the WAL range as free, and reclaims such blocks below the bump's
  resume point into the free list.
- Reclamation (v0.4.1): input runs return to the free list strictly
  after the commit. It is best-effort: a full free list must not fail a
  job that already committed. Unreclaimed blocks stay orphans until the
  next open's sweep.
- Pre-1.0 format policy (v0.4.1): no compatibility across minor
  versions. The manifest magic changes with the layout (`hrtman01` to
  `hrtman02`), and a foreign magic is `CorruptManifest`, never a
  misparse.

## Consequences

- A crash leaves exactly the old or the new state. Crash-injection tests
  enumerate every block write over flush, compaction, archive and ingest
  and assert this (v0.4, v0.9, v0.10, v0.12).
- Partial output needs no cleanup protocol: it is orphaned and swept on
  open. This is also why compaction scratch can be dropped mid-job.
- The flush protocol costs two device flushes per commit: one after the
  table blocks and one after the manifest slot.
- Each slot is one block, so the encoded manifest must fit in `BLOCK`
  bytes. v0.4's tests hit this: fixed 256-byte key bounds (544 bytes
  per table) overflowed a 4 KiB block at 8 tables, and were changed to
  length-prefixed bounds.
- A free list that is too small leaks blocks until the next open.
- After the WAL wraps, recovery must skip stale pre-wrap blocks with a
  sequence floor (`seq <= manifest.max_seq`) (§7).
- On-disk images do not survive a minor-version upgrade before 1.0.

## References

- SPEC §4.5 (Manifest), §4.6 (crash and reclamation bullets), §7
- [Milestone log](../history/milestones-v0.1-v0.16.md): v0.2, v0.3, v0.4, v0.4.1, v0.9, v0.10, v0.12
