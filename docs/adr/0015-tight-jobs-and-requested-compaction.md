# ADR-0015: Tight jobs and requested compaction

Status: Accepted (unreleased, 2026-10-10)

## Context

Issue #32. A store at the kvstore shape (16 KiB blocks, 4 levels of 7
tables) filled with TTL data stops at 26 used and 2 free slots. Every
write then fails, also after the clock passes every TTL and the file is
reopened. Two causes:

1. **Admission.** A job needs `1 + ceil(data / (slot - 5))` free slots.
   A full table holds `slot - 3` data blocks, so a job for one deep table
   needs 3 slots. The compaction reserve (ADR-0009) is 2. No job can
   start, so no slot is ever freed.
2. **No compaction below the triggers.** Jobs run only for a full level
   or under region pressure. Data at the bottom level is rewritten only
   when a push-down from above overlaps it. Tombstones and expired values
   below the triggers keep their slots.

## Decision

**Tight jobs.** When no job fits its estimate, a wanted level gets a
*tight* job. A tight job needs one free slot. It commits an output only
when, after the commit, its committed outputs do not exceed its retired
inputs (`model_tight_commit_ok`). After each commit, the free slots are
at least their start value. As a result, the next output always has a
slot (`model_tight_job`, checked for every short sequence in
`src/model.rs`). If the next commit would break the rule, the job drops
that output (it releases the slot; nothing points at it) and ends. If a
job for a wanted level committed nothing, the step returns `RegionFull`:
the job cannot free a slot. A requested job skips its tables. A job
whose outputs retire inputs as it writes (data that a purge or tombstones
remove) can run at the reserve. The rule applies after each commit, not
only at the end: a job whose first output covers less than one input
gives up, also when later inputs would shrink.

**Requested compaction.** `Db::request_compaction(level)` compacts each
table from `level` down to the bottom once:

- Above the bottom, a table moves down a level (merge or move).
- At the bottom, a table is rewritten in place, with the small same-node
  tables on both sides of it. A rewrite is always tight: a rewrite that
  does not shrink costs I/O but never a slot.
- Each level takes a mark (the next table id) when the request reaches
  it. Tables below the mark are due. The request moves on only between
  jobs, so every output of a job is due at the next level. The bottom
  level takes the mark from when the request opened: the request does
  not rewrite the bottom tables its own jobs wrote there.
- A table whose job gives up, or that a foreign table vetoes, is skipped
  (one bit per slot). A veto skips only the tables of the vetoed job. The request ends, unless no slot is free. Then
  `compact_step` returns `RegionFull`.
- A requested job runs after the jobs that fit for full levels and region
  pressure, and before their tight jobs. A requested job that is not
  tight must leave the reserve free.
- When a tight job for a wanted level gives up while the request has
  work, the step returns `Done`, not `RegionFull`: the next step runs
  the request.
- The state is about 20 bytes in `Db`, in memory only. `open()` clears it.

The reserve stays at 2 slots.

## Consequences

- A full region of purgeable or deleted data recovers: set
  `purge_before` (or delete), call `request_compaction(0)`, and run
  `compact_step` while `compaction_pending()` is true.
- A request reads and writes every table it reaches once. Set
  `purge_before` before the request: bottom tables that the request's
  own jobs wrote are not rewritten. A bottom table
  that does not shrink costs one read and one write. When its output does
  not fit one slot, the job drops the output and the table stays.
- A tight job for a wanted level over live data drops its first output
  and returns `RegionFull` each time it runs. The region is full: delete,
  archive, or grow it.
- A region full of live data still refuses deletes when the memtable is
  full (a delete is a write). Hosts that must always delete keep slots
  free above the reserve, as `autumn-plugin-horton` does.
- No on-disk format change. The reserve stays at 2, so small shapes lose
  no space.

## Alternatives rejected

- **Reserve of 3 or more.** Small shapes (4 slots of 8 blocks) would lose
  a quarter of their space, and the estimate is still not exact.
- **Exact admission by a dry run.** It reads every input twice and must
  copy the writer's packing and compression rules.
- **Flush into the reserve when the memtable holds only deletes.** The
  next job may then find no slot.
- **Per-table tombstone and TTL statistics.** An on-disk format change.

## References

- `src/db/compaction.rs`: `request_compaction`, `try_select_group`,
  `pick_sources`, `tight_commit_ok`, `compact_give_up`
- `src/model.rs`: `model_tight_commit_ok`, `model_tight_job`
- `tests/reclaim.rs`; requests in `tests/fuzz_differential.rs` and
  `tests/lifecycle.rs`
- SPEC §4.6, §4.7, §5, §6, §8; ADR-0009, ADR-0010
