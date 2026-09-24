# ADR-0010: Split-output compaction with per-output commits

Status: Accepted (v0.17, 2026-09-24). Supersedes the selection and
output rules of ADR-0004; the caller-driven, bounded-step API stays.

## Context

Compaction wrote one output table per job, capped level sizes by table
count, and never merged disjoint tables. The architecture review (F6)
measured the result on `TestDb`: writes stopped for good with the table
region about 4% used (340 sequential or 644 random 1,000-byte writes).
With fixed slots (ADR-0009) an unsplit output also could not exceed one
slot.

## Decision

- **Split outputs.** A job writes as many outputs as it needs. An output
  ends at a key boundary once its data budget (the slot minus the
  range-tombstone budget and meta blocks) is within one key's worst-case
  run of blocks (`key_run_blocks`), so every version of a key lands in
  one output.
- **Commit per output.** Each sealed output commits on its own. The
  commit retires every input whose last key is at or below the output's
  last key `k`, and **narrows** every input that straddles `k` to start
  at `succ(k)`. Readers honour a table's `first_key` as its live lower
  bound, so narrowed prefixes are dead to every reader. A job
  interrupted between outputs leaves a consistent tree, and the next job
  merges on from the narrowed inputs.
- **Range tombstones** are clipped to each output's `[lo, hi)`, where
  `hi = succ(last key)`, so outputs stay disjoint.
- **Trivial moves.** A non-small source table that overlaps nothing in
  the next level moves down with one manifest edit and no rewrite.
- **Consolidation.** A small table (under three quarters of a slot) is
  rewritten with its small neighbours instead of moved.
- **Selection.** Full levels first, deepest first; then, when flush is
  down to the compaction reserve, pressure pushes down intermediate
  levels and L0 (two or more tables). A job is admitted only when the
  free slots cover its need (`1 + ceil(source data / (slot − 5))`); L0
  jobs shrink to their oldest tables to fit. The bottom level has no
  table cap.
- **Tombstone drops** (F15): a bottommost tombstone is dropped only when
  no table outside the job — shallower ones included — may hold an
  older version.

## Consequences

- Writes continue until most of the region holds live data: about 58%
  (sequential) and 63% (random) on 28 × 30-block slots, 63% and 73% on
  `test_config`. The remainder is the two-slot reserve, partly filled
  L0 flushes, and the consolidation threshold.
- A job can span many commits, so a crash mid-job loses at most the
  uncommitted output; crash tests enumerate every write of a
  multi-output job.
- Selection is more complex than "compact the full level", and the
  lifecycle fuzzer (`tests/lifecycle.rs`, three geometries) is what
  keeps it honest.

## References

- `src/db/compaction.rs`, `src/compact.rs`
- `docs/ARCHITECTURE_REVIEW.md` F6, F15
- `tests/compact.rs`, `tests/crash_compact.rs`,
  `tests/review_findings.rs` (`f6_*`, `f15_*`), `tests/lifecycle.rs`
