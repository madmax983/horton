# ADR-0013: Capacity errors name their remedy

Status: Accepted (v0.17, 2026-09-24)

## Context

`Error::NoSpace` covered about ten conditions: WAL exhausted, L0 full,
table region full, snapshot limit, manifest overflow, index overflow,
counter overflow, out-of-range levels, and more. Each needs a different
response, and callers (the review's own tests included) had to guess
with `compaction_pending()` (architecture review F10).

## Decision

`NoSpace` is replaced by variants that each name one remedy:

| Variant | Remedy |
|---|---|
| `WalFull` | `flush()` (it wraps the WAL), retry |
| `NeedsCompaction` | `compact_step` until `Done`, retry |
| `RegionFull` | delete and compact, archive, or grow the region |
| `SnapshotLimit` | release a snapshot |
| `TableTooLarge` | smaller entries, fewer range tombstones per table, larger blocks or slots |
| `ManifestFull` | none through `Db` (an invariant violation) |
| `CounterExhausted` | none (2^32 tables or 2^64 writes) |
| `BadLevel { level }` | pass a level below `LEVELS` |
| `BadConfig` | fix the `Config` (from `open`) |
| `Busy` | finish the other `get`, retry |

`NeedsCompaction` is returned only while `compaction_pending()` is
true, and `compaction_pending()` also reports an in-flight job. So a
compact-then-retry loop always makes progress, and a region that
compaction cannot help reports `RegionFull` (with `LEVELS = 1`, a full
L0 is `RegionFull`).

## Consequences

- Callers can act on the error alone.
- The enum grew; code that matched `NoSpace` must be updated (pre-1.0,
  no compatibility promise).

## References

- `src/error.rs`, `Db::no_room`, `Db::compaction_pending`
- `docs/ARCHITECTURE_REVIEW.md` F10
- `tests/review_findings.rs`: `f10_capacity_errors_name_their_remedy`
