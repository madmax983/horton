# ADR-0009: Fixed-slot table allocator

Status: Accepted (v0.17, 2026-09-24). Supersedes the allocation part of
ADR-0003.

## Context

ADR-0003 allocated table runs from a bump pointer plus a sorted free
list, reserved until the manifest commit and claimed after it. The
architecture review found three defects in that scheme:

- **F1.** Compaction reserved `sum(input data blocks) + rdel + 3` blocks
  on the premise that a merge never needs more data blocks than its
  inputs. Next-fit block packing is not sub-additive under interleaving,
  so an output could run past its reservation onto the next live table.
- **F3.** The open-time sweep inserted every unreferenced block below
  the bump into a fixed-capacity free list; a list that filled up made
  `open()` fail after an ordinary compaction.
- **F14.** A compaction job only peeked at its output run, so a flush
  in the middle of the job could take the same blocks.

## Decision

- The table region is `LEVELS × TABLES` fixed **slots** of equal size,
  `slot_blocks = region / slots` (`SlotMap`). A slot holds at most one
  table, and every table lives wholly inside one slot. `LEVELS × TABLES
  ≤ 64` (const-asserted), so slot state is two `u64` bitmaps: `used` and
  `reserved`.
- `open()` rebuilds the map from the manifest: a live table outside its
  slot is `CorruptManifest`; every other slot is free. There is no sweep
  and no free list, so nothing can overflow.
- `open()` refuses a region whose slots cannot hold a full memtable's
  table (`Error::BadConfig`), so a flush always fits a slot.
- Flush and ingest find a free slot and **claim** it only after their
  manifest commit lands; they hold `&mut self` throughout, so nothing
  can take it in between. Compaction **reserves** its output slot for
  the life of the job (a job spans many `compact_step` calls), and
  releases it on abort.
- Every table writer is capped at its slot (`with_block_limit`): a
  table that would outgrow its slot fails with `TableTooLarge` instead
  of writing into a neighbour. F1 becomes impossible by construction.
- Compaction keeps a reserve of two slots (free or already reserved by
  the running job) that flush and ingest never take, so a merge can
  always start.
- Allocation is next-fit from a rotating hint, which spreads flash wear
  (ADR-0012).

## Consequences

- Table size is capped at one slot. Compaction splits its output at
  slot size (ADR-0010), so this caps tables, not data.
- A table smaller than its slot wastes the rest of the slot. Compaction
  consolidates small tables to keep that bounded, and the capacity test
  (`f6_writes_continue_until_the_region_is_mostly_full`) pins at least
  half the region holding live data when writes stop.
- The region size is now a runtime property checked in `open()`, not a
  hidden limit found at the first large flush.

## References

- `src/slots.rs`, `Db::open`, `Db::free_slot_for`
- `docs/ARCHITECTURE_REVIEW.md` F1, F3, F14
- `tests/review_findings.rs`: `f1_*`, `f3_*`, `f14_*`; `tests/slots.rs`
