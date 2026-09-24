# ADR-0004: Caller-driven, bounded compaction with caller-owned scratch

Status: Accepted (v0.4, 2026-09-14; extended to every level in v0.8)

## Context

Leveled compaction merges whole tables into new ones. On firmware that
work has to be interleaved with real-time work, horton ships no executor
(ADR-0001), and there is no heap for merge state (ADR-0002). §1 states
that the compaction scratch is caller-owned, not `Db` RAM.

## Decision

- The API is `db.compact_step(&mut scratch) -> Result<Progress, Error>`.
  `scratch` is a typed `Compaction<BLOCK, KEY_MAX, VAL_MAX, BLOOM_BYTES>`
  from `Compaction::new()`, typically a static or a stack local the
  caller keeps across calls. It is never stored inside `Db`.
- Each call does bounded work: it seals at most one output block and
  returns `Progress::More`. The call that exhausts the merge seals the
  table and commits the manifest atomically, returning `Progress::Done`.
- The merge is k-way over at most `KMAX = 8` table cursors. The winner
  is picked by linear scan (minimum key, highest sequence on ties), with
  no heap.
- Selection (v0.8): take the deepest level, from `LEVELS - 2` up to L0,
  that holds at least `TABLES` tables. A full L0 compacts all of L0. A
  full deeper level compacts its oldest table (index 0) plus every
  overlapping table in the next level, closing over the key range so
  levels 1 and deeper keep their non-overlapping invariant.
- Retention (v0.5): per key, keep the live view's newest version plus
  the newest version at or below each live snapshot's watermark (the
  keep-set). A bottommost newest tombstone is dropped only when it
  predates every live snapshot.
- Driving to idle (v0.8): `compact_step` also returns `Done` when no job
  was selected, and `compaction_pending()` reports whether one is
  selectable, so callers loop on it.

## Consequences

- horton never compacts on its own. Firmware decides when, and can
  interleave one block of compaction with other work.
- The scratch can be dropped mid-job: partial output is invisible until
  the manifest commit and is swept as orphans on the next open.
- Compaction RAM is visible in the budget: `Compaction` is 56,408 bytes
  in the v0.16 ESP32-S3 profile.
- Work only happens when the caller drives it. A caller that does not
  compact lets levels fill up (`ingest_table` returns `NoSpace` when L0
  is full, v0.12).
- Deepest-first selection guarantees the target level has room, except
  for a full bottommost level. That case fails with `NoSpace` at select
  time, before any merge I/O, when the merge does not absorb a target
  table.
- At most 1 + 8 versions of a key survive a merge (one per live view).
- v0.15 added a caller-set TTL cutoff on `Compaction::purge_before`.
  Changing it mid-job is safe but incoherent (ADR-0006).
- Partially overlapping range tombstones with different sequences are
  kept, not merged: correct, merely uncompacted (v0.15).

## References

- SPEC §1, §4.6 (Compaction), §5
- SPEC §9: v0.4, v0.4.1, v0.5, v0.8, v0.12, v0.15, v0.16
