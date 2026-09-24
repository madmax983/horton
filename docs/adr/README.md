# Architecture Decision Records

Each record captures one load-bearing decision: its context, what was
decided, and what it costs. The first eight record decisions already
made and described in [`SPEC.md`](../../SPEC.md). 0009–0013 record the
v0.17 redesign that fixed the architecture review's findings
([`ARCHITECTURE_REVIEW.md`](../ARCHITECTURE_REVIEW.md)).

ADRs 0001–0008 cite section numbers of the v0.16 `SPEC.md` (git
history keeps it); its §9 milestone log now lives in
[`docs/history/milestones-v0.1-v0.16.md`](../history/milestones-v0.1-v0.16.md).

| ADR | Decision |
|---|---|
| [0001](0001-poll-based-block-device.md) | Poll-based async `BlockDevice` trait; horton ships no executor |
| [0002](0002-no-std-no-alloc-caller-owned-memory.md) | `no_std`, no `alloc`, all memory caller-owned and sized by const generics |
| [0003](0003-double-buffered-manifest-commit.md) | Double-buffered manifest as the single atomic commit point; reserve-then-claim allocation (partly superseded by 0009, 0011) |
| [0004](0004-caller-driven-bounded-compaction.md) | Compaction is caller-driven, one output block per step, with caller-owned scratch (selection and outputs superseded by 0010) |
| [0005](0005-ingest-copies-and-grafts-at-l0.md) | Re-attached archive tables are copied in, relocated, and grafted at L0 |
| [0006](0006-caller-supplied-clock-for-ttl.md) | horton owns no clock; the caller supplies `now` for TTL reads and purges |
| [0007](0007-clock-block-cache.md) | CLOCK block cache inside `Db`, keyed by (table id, block id) |
| [0008](0008-hand-rolled-lz77-block-compression.md) | Hand-rolled per-block LZ77 compression flagged in the block trailer |
| [0009](0009-fixed-slot-table-allocator.md) | The table region is fixed slots, one table each; claim after commit, reserve for compaction |
| [0010](0010-split-output-compaction.md) | Compaction splits outputs at slot size and commits each, narrowing straddling inputs |
| [0011](0011-multi-block-manifest-and-edits.md) | Multi-block manifest copies, an optional copy ring, and commits staged as small edits |
| [0012](0012-nor-flash-endurance.md) | NOR endurance: `WriteBatch` group commit and a manifest ring; no page-program path yet |
| [0013](0013-capacity-errors-name-their-remedy.md) | `NoSpace` split into errors that each name their remedy |

## Adding a record

A new decision gets a new ADR with the next number. Existing records
are not rewritten when a decision changes: write a new ADR that
supersedes the old one, and change the old one's status to
`Superseded by ADR-NNNN`.

Template:

```markdown
# ADR-NNNN: Title

Status: Proposed | Accepted (vX.Y, YYYY-MM-DD) | Superseded by ADR-NNNN

## Context
## Decision
## Consequences
## References
```

State consequences honestly, including the limits, and point
References at the SPEC sections the decision lives in.
