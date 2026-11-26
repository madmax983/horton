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
| [0014](0014-cache-hits-reverify-crc.md) | Block-cache hits re-verify the CRC; no skip-on-hit, correctness over speed (superseded by 0016) |
| [0015](0015-tight-jobs-and-requested-compaction.md) | Tight jobs run at the reserve and never grow the region; `request_compaction` compacts below the triggers |
| [0016](0016-cache-hits-reverify-with-a-ram-checksum.md) | Only CRC-verified blocks are cached; hits re-verify against a per-chunk RAM checksum, not CRC-32 |
| [0017](0017-bucketed-bloom-and-ranged-reads.md) | Bloom probes stay in one 64-byte bucket; point reads fetch ranges of the footer and bloom blocks (`hrtsst03`; extended by 0020) |
| [0018](0018-detached-point-reads.md) | A point read is a memtable probe plus a table phase that runs without the `Db`; the caller keeps table writes away from detached readers |
| [0019](0019-detached-scans-block-pool-lazy-seek.md) | Scans read from parts and can run without the `Db` (the memtable copied a page at a time); a scan can be lent block buffers; a forward seek reads only the tables that hold its key |
| [0020](0020-bloom-filters-span-blocks-and-fold.md) | A bloom filter may span blocks; the writer builds it at full length and folds it to the table's entry count (`hrtsst04`) |
| [0021](0021-the-manifest-record-describes-a-table.md) | The manifest record names every section of a table and its filter's shape; readers, scans and compaction do not read footers (`hrtman09`) |
| [0022](0022-host-cache-hits-write-nothing-shared.md) | The host store's block cache: immutable entries and a per-thread view, so a hit takes no lock and writes nothing shared |
| [0023](0023-host-point-reads-take-no-lock.md) | The host store's point reads leave the request queue, and take no lock when a filter over the memtable says it cannot hold the key; flush and compaction shut a gate and wait for per-thread pins |
| [0024](0024-detached-compaction-merges.md) | A compaction job's merge runs without the `Db` (`Compaction::merge_detached`); the host store merges on its compaction thread outside the engine lock, and a write that waits for room gives the lock up |
| [0025](0025-two-compaction-jobs-at-a-time.md) | Two compaction jobs at a time (`Db::compact_step_in`, one job a lane); flush keeps a reserve for each lane that is stepped, and with two lanes pressure merges level 0 first; the host store has a compaction thread for each lane |
| [0026](0026-hot-path-pass-crc-codec-cache-memtable.md) | The hot path measured again: CRC-32 in three chains and across zero runs, a faster LZ77 codec, cache images lent in place (`CachePort::with_range`), no value buffer zeroed per probed table, and a sorted view of positions over the memtable; why no hardware CRC and no group commit across WAL blocks |
| [0027](0027-point-reads-inflate-to-the-key.md) | A point read inflates a compressed data block only as far as its key (a resumable decoder and a scan from the first entry), and leaves what the scan cannot decide to the whole-block search |
| [0028](0028-group-commit-across-wal-blocks.md) | Group commit across WAL blocks: `Db::write_group` commits the longest prefix of atomic units that fits, each inside one block, under one flush, visible only after it; open zeroes blocks a reordering device stranded past the recovered log, so a dropped write never comes back |
| [0029](0029-atomic-batches-across-wal-blocks.md) | An atomic batch may span up to 16 WAL blocks: `SPAN` and `CONT` op-byte flags and consecutive sequence numbers tie its blocks together, recovery replays it only when all are intact, open's scrub reaches 16 blocks past where recovery stopped; manifest `hrtman10` |
| [0030](0030-no-compression-in-shallow-levels.md) | Tables written into levels 0 and 1 store their data blocks raw (`Config::compression_from_level`, default 2): a table's blocks are packed by raw size, so compression saved no blocks there and cost a pass to write and to read |
| [0031](0031-point-reads-check-what-they-read.md) | A point read searches a cached data block where it lies and checks against the RAM checksums only the ranges it acts on (`CachePort::with_checked`); a search that fails on unchecked bytes checks the whole image, so a RAM fault is a miss, never `CorruptBlock` |
| [0032](0032-one-opener-per-host-store.md) | The host store locks `<store>.lock` before it opens the store: a second opener gets `StoreError::Locked`, not a WAL that two processes write over |
| [0033](0033-host-store-refuses-bad-input-first.md) | The host store checks its input before it writes: a size too small for the shape, a usage error and an inverted range fail, and no file is sized or created for them |

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
