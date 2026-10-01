# Tiered storage sweeper + table-shipping replication

**Status:** design spec (2026-10-01). Blessed as a direction by Mark;
details open where marked.

## 1. Goal

Run horton on small fixed cloud volumes (1 GiB Fly/Hetzner) with two
properties volumes alone can't give:

1. **Tiered storage** — hot data on the volume, cold data in cheap storage,
   via two archive sweeps: by *age* (steady state) and by *pressure*
   (safety valve).
2. **Replication** — spin up another Fly machine / EC2 instance and get an
   eventually-consistent copy, without a second database system.

## 2. Layering: policy lives outside horton

Everything in this spec is a **host-side policy loop** — a new crate next
to `horton-pg-sink`, not code inside horton. Rationale:

- Horton's core is `no_alloc`, clock-free, and deliberately does **not**
  track read access (write-on-read would burn flash endurance — see
  `docs/adr/0012-nor-flash-endurance.md`). Policy needs clocks, heuristics,
  and network I/O; none of that belongs in the core.
- The mechanism the policy needs **already exists** in horton:
  - `Db::archive_plan(level, table_id)` → immutable sealed table bytes,
    streamed out through `Db::device()`.
  - `Db::archive_commit(level, table_id)` → one atomic manifest write
    drops the table and reclaims its slot. Refuses with
    `Error::WouldResurrect` when the table's tombstones still shadow live
    data (so: prefer insert-only tables, or compact first).
  - `Db::ingest_table(&SealedTable, remote: &R, src_base)` → idempotent
    re-attach of a sealed table from *any* `BlockDevice` — including a
    network-backed one. Same descriptor attaches exactly once; a
    conflicting descriptor under a live id is refused
    (`Error::IngestConflict`). Needs L0 room (`NeedsCompaction` when full).
- `horton-pg-sink` is the reference cold path to Postgres (watermarked
  drain); the sweeper reuses its upload-then-commit discipline.
- **Sink contract (decided 2026-10-01, Mark): the cold target is
  configurable.** `horton-sweeper` defines the sink as a trait —
  `seal(descriptor)`, stream blocks, `commit() -> Receipt`, plus read-back
  for the cold path and replica bootstrap. Postgres and object storage are
  first-party impls; a secret third sink implements the trait. We are
  keepers of the contract and the shape (the `SealedTable` descriptor,
  the upload-then-verify-then-commit discipline), agnostic to where the
  bytes land.
- **New crate `horton-sweeper`** (decided 2026-10-01, Mark): for exactly
  that reason — the contract lives in the crate, sinks plug into it. Not
  grown inside `horton-pg-sink` (that crate is the Postgres sink impl, and
  stays one).

## 3. Gap: table inventory API

The sweeper must enumerate tables — level, id, key bounds, min/max seq,
entry count, block count — to choose victims. `Manifest::tables()` is
public but unreachable through `Db`; there is no public inventory
accessor today. **Required small addition** (in horton, behind no new
feature): e.g. `Db::tables(level: usize) -> Option<&[TableRef<KEY_MAX>]>`.
`TableRef` is already public (it rides in `ArchivePlan`). Everything else
the sweeper needs is public.

## 4. Sweep 1 — by age (steady state)

- The sweeper keeps a **seq→wall-clock index**: it observes every seal
  (level, table id, max_seq) and stamps it. Tiny, host-side, persisted
  however the host likes (sqlite, a file, Postgres).
- Policy: tables with `max_seq` older than `age_threshold` (per key-prefix
  if wanted) are archived: `archive_plan` → upload bytes to cold storage →
  verify → `archive_commit`.
- Cadence is periodic and predictable — this is the steady state for
  time-series/telemetry workloads (the `flight-recorder` sizing preset).
- Insert-only workloads never trip the tombstone rule; with deletes, the
  sweeper compacts the candidate table down first (or skips it for the
  pressure sweep's colder candidates).

## 5. Sweep 2 — by pressure (safety valve)

- Trigger: free slots below `min_free_slots`, or table bytes above a
  high-water mark. This is the valve for when the age sweep falls behind
  or a burst fills the volume.
- **Victim selection without access tracking in the DB.** Horton's reads
  stay side-effect-free; coldness is inferred:
  1. Primary signal: **key-range / seq recency** — low `max_seq` and key
     ranges far from the write head are cold. For time-series this is
     exact (newest tables are hottest, by construction).
  2. Optional **host hot-range hints**: the application samples its own
     hot keys (1-in-N, in the cache layer — §6) and the sweeper spares
     tables whose key bounds intersect. The DB never sees this.
  3. Tombstone-rule-aware: skip (or compact-first) tables whose commit
     would hit `WouldResurrect`.
  4. Never target L0's head or an in-flight compaction's inputs
     (`archive_commit` aborts the in-flight job; the policy shouldn't
     cause that routinely).
- Victims go through the same upload-then-commit path as the age sweep.

## 6. Host-side access tracking (write-behind cache note)

Mark's wrinkle, recorded: read tracking **must not** live in horton
(flash wear), but it works fine **above** horton — in a write-behind
cache on a cloud volume, where the backing store is network block and
there is no wear budget to protect. That layer owns the LRU/LFU, feeds
hot-range hints to the pressure sweep (§5.2), and horton stays dumb.
On raw flash the cache still works; it just doesn't persist its heat
map across restarts (or persists it lazily — host's choice).

## 7. Replication: sealed tables are the unit

The key insight: **a sealed SSTable is immutable**, so it is the natural
replication unit. No WAL shipping, no row-level oplog, no conflict
resolution on the write path.

- The primary exposes sealed tables (inventory from §3 + byte stream,
  same shape as the archive path). The replica fetches
  `(SealedTable, bytes)` and calls `ingest_table` — **idempotent**, so
  at-least-once delivery is safe and a crashed mid-copy transfer retries
  cleanly.
- The `remote: &R: BlockDevice` parameter means the replica can read the
  primary's table bytes through a network-backed device impl, or through
  the cold store (S3) — the primary doesn't even need to stay up for a
  new replica to bootstrap from cold storage.
- What does **not** replicate: the memtable and WAL tail. A replica lags
  the primary by up to one flush, by design.
- **Topology: primaries are multi-writer-capable** (revised 2026-10-01 —
  Mark vetoed single-writer-per-shard as a concession). Conflict analysis:
  append-mostly workloads (timestamps, UUIDs — horton's home turf) are
  **conflict-free by construction**: keys are unique per writer, so
  table-shipping replication needs no resolution at all. True key
  collisions across primaries resolve by deterministic LWW on
  (source wall-clock at seal, node_id), applied identically by every
  replica → deterministic convergence. This is *not* linearizability and
  is documented as such. v1: ship and test the conflict-free path first;
  the collision path gets its own convergence test (two primaries, same
  key, all replicas converge identically).
- Consistency: **eventual**, stated plainly. A replica converges as
  tables seal; reads may be stale by the memtable tail. Documented, not
  hidden.
- Open verification: `ingest_table` preserves the source table's
  seqnums — the replica's `next_seq` must stay ≥ max ingested seq across
  ingest + local writes. Verify with a dedicated test before calling
  replication done.

## 8. Tiered reads

- Hot path: local `Db::get` / scans, unchanged.
- Cold path: the cold store (Postgres via pg-sink, or object storage)
  answers for archived key ranges. The sweeper's inventory (§3) tells the
  router which ranges are cold.
- Heat re-attach: if a cold range gets hot, `ingest_table` grafts it back
  (needs L0 room — the sweeper compacts or ages something else out first).
  No data movement the DB didn't already know how to do.

## 9. The seq→wall-clock index: tradeoff analysis

The age sweep needs `max_seq → wall-clock` per sealed table. Key facts
that shape the tradeoff:

- Write volume is tiny: one entry per table seal (flushes/compactions —
  a few per minute at most). Read pattern is trivial: periodic full scan,
  filter by time. Nothing here needs a query planner.
- **The index is a hint, not source of truth.** If it's empty, stale, or
  wrong, the pressure sweep (§5) still bounds capacity; the only effect
  is archival timing shifting by the staleness window. This decision
  cannot cause data loss.
- Observation race: the sweeper polls inventory (§3), so a table can seal
  *and* be compacted away between polls — its data survives in the merged
  table, which gets stamped on the next poll. Age attribution drifts by
  at most the poll interval. At day-scale thresholds, noise.

Options:

1. **In-memory + periodic snapshot** (JSON/bincode every N minutes, plus
   on clean shutdown). Simplest possible, zero deps. Crash loses ≤ N
   minutes of stamps → affected tables look younger → archived slightly
   late. For day-scale thresholds the error is noise. Best when you don't
   care about precision.
2. **Append-only log** (one line per seal: `seq, timestamp`; replay on
   boot; rotate occasionally). Crash-safe, ~50 lines, no deps. fsync
   policy is yours (per-seal for the paranoid, periodic for the sane —
   staleness only shifts timing). Best durability-per-complexity.
3. **sqlite.** Full durability and SQL — but the heaviest dependency in
   the list (C library or bundled build) for a workload that is one
   append and one scan. Only wins if the sweeper later grows genuinely
   relational needs (audit log, multi-tenant bookkeeping). Today it has
   none.
4. **Reuse the cold store (Postgres).** Zero new infra *if* PG is the
   sink — but couples the sweeper to PG when the sink is S3 or the secret
   third one, and adds a network round-trip per seal. Fine as an option,
   bad as the default.

**Decided 2026-10-01 (Mark): append-only log.** One line per seal,
replay on boot, rotate occasionally. In-memory+snapshot stays documented
as the coarse-threshold "don't care" mode. sqlite only if the sweeper's
state ever outgrows a log.

## 10. Open questions (Mark's calls)

1. Single-writer primary per shard — ~~blessed as the model~~ **revised
   2026-10-01**: multi-writer primaries allowed (Mark vetoed the
   concession); append-mostly workloads are conflict-free by
   construction; true key collisions resolve by deterministic LWW on
   (seal wall-clock, node_id) — convergent, not linearizable.
2. ~~Reference cold target~~ **Decided 2026-10-01:** pluggable sink trait;
   PG + object storage first-party; third sinks to the contract.
3. ~~New crate or grow pg-sink~~ **Decided 2026-10-01:** new
   `horton-sweeper` crate holds the contract.
4. ~~Seq→time index backing~~ **Decided 2026-10-01 (Mark):**
   append-only log.
