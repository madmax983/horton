# horton — SPEC v0.17

A log-structured merge-tree key-value store for places where
`malloc` doesn't exist: firmware, kernels, bootloaders.

This file is the normative spec: constraints, data model, on-device
formats, protocols, and the public API. What changed when is in
[`CHANGELOG.md`](CHANGELOG.md); why the load-bearing decisions were made
is in [`docs/adr/`](docs/adr/README.md); the v0.1–v0.16 milestone log
(with its test-run reports) is kept verbatim in
[`docs/history/milestones-v0.1-v0.16.md`](docs/history/milestones-v0.1-v0.16.md).

Status: prose spec, executable models in `src/model.rs`, and a runtime
invariant checker (`Db::check_invariants`). Verus proofs of the slot
allocator's state machine and the durable monotone sequence counter are
queued as the PROOF step (SPEC-PROOF-RED-GREEN-REFACTOR).

## 1. Hard constraints (CI-enforced)

- `#![no_std]`, no `extern crate alloc`, `#![forbid(unsafe_code)]`. The
  `[dependencies]` table is empty: only `core` may be used. `std` is
  test-only.
- All memory is caller-provided and compile-time sized via const generics.
  Constructors are `const fn` where possible, so a `Db` can live in a
  `static`.
- No recursion, no panics in library code, no `unwrap`/`expect`/`assert`
  in non-test code. Every failure is a `Result`.
- **RAM is measured, not estimated.** `tests/profile.rs` asserts that the
  ESP32-S3 profile's structs (`Db`, `Scan`, `Compaction`) plus the
  largest set of futures that can be live at once stay within
  `ESP32S3_RAM_BUDGET` (112 KiB). An executor stores the futures, so they
  count; see [`BUDGET.md`](BUDGET.md).
- CI (`.github/workflows/ci.yml`, toolchain pinned in
  `rust-toolchain.toml`): `cargo fmt --check`, `clippy -D warnings` with
  pedantic and nursery, rustdoc with `-D warnings`, the test suite in
  debug and release, a miri subset, the quickstart example, the bench
  build, and a gate that fails if any architecture-review regression test
  is `#[ignore]`d.

### Targets

- Host (x86_64, macOS, Linux) for development and tests.
- **ESP32-S3** (xtensa) under Tallow: `FlashBlockDevice` over SPI NOR
  flash with 4 KiB sectors, the `Esp32S3*` profile in `src/profile.rs`,
  and a QEMU smoke test (`xtensa-smoke/`, `xtensa-check.sh`).

## 2. Data model

- Key: byte string, `0 < len <= KEY_MAX`, ordered by unsigned
  lexicographic byte order. Empty keys are rejected.
- Value: byte string, `len <= VAL_MAX`. Empty values are allowed (distinct
  from deletion).
- Every mutation carries a `u64` sequence number from a monotonic counter
  that never repeats, across crashes, WAL wraps, and compactions (the
  manifest persists a high-water mark). Sequence 0 is reserved and
  invisible.
- Mutations: put, put with an absolute expiry tick (TTL; horton owns no
  clock, the caller passes `now` to reads), point delete (a tombstone),
  and range delete of `[start, end)` (a range tombstone). A `WriteBatch`
  commits several ops atomically.
- Visibility: the newest version wins; a range tombstone hides every
  older version of the keys it covers; an expired value reads as missing
  and never falls through to an older version. A snapshot (a sequence
  watermark, at most 8 live) sees exactly the mutations at or below it.
- Single-threaded `&mut self` writes; `get` takes `&self`, and a `Scan`
  borrows the `Db`, so the compiler forbids writes during a scan.

## 3. Architecture

```
put / delete / write ─► WAL (1 block per commit) ─► MemTable
                                                        │ flush()
                                                        ▼
         manifest copies ◄── commit ── table in a free slot (L0)
                ▲                             │ compact_step()
                │                             ▼
                └────── commit ─── split outputs in L1..Ln
get / Scan / RevScan ◄── memtable + tables (bloom, key-range, seq prunes; block cache)
```

Write path is WAL-first: a mutation is durable once its WAL block is
written and flushed; the memtable is a volatile index over the WAL. Every
structural change (flush, WAL wrap, compaction output, archive, ingest)
becomes visible in exactly one manifest commit.

## 4. Components

### 4.1 MemTable — sorted slots over a bump arena

`MemTable<CAP, ARENA, KEY_MAX, VAL_MAX>`: `CAP` slots sorted by
`(key, seq desc)`, key and value bytes bump-allocated in an `ARENA`-byte
array. Every mutation takes a fresh slot (the memtable keeps each key's
whole version chain, newest first, because snapshots can observe older
versions). Range tombstones take a slot too (`start` as the key, `end` as
the value, flagged). A full memtable rejects the write with `TableFull`
(slots) or `ArenaFull` (bytes); the caller flushes. O(n) insert by
`copy_within`, O(log n) lookup.

### 4.2 WAL — append-only framed log

Record (little-endian):

```
magic: u16 = 0x6C73 | len: u32 | seq: u64 | op: u8 | key_len: u16
| val_len: u16 | key | val | [expire_at: u64 for op 4] | crc32: u32
```

Ops: 1 put, 2 delete, 3 range delete (`key` = start, `val` = end),
4 put with TTL. `crc32` covers everything after `magic`. Records never
span blocks.

- Each durable write commits one whole block (zero-padded) and moves on;
  a block holding acknowledged records is never rewritten. A
  `WriteBatch` stages all its ops into one block (group commit); a batch
  larger than a block is `BatchTooLarge`, never split.
- Recovery replays `[wal_head, …)` in order and stops at the first
  corrupt or truncated record: the torn tail is the crash boundary, not an
  error. Records at or below the manifest's `flushed_seq` are stale
  pre-wrap blocks and are skipped.
- When the region is exhausted the next flush wraps it: `wal_head`
  returns to `wal_start` in the same manifest commit.

### 4.3 BlockDevice — the only I/O boundary

```rust
pub trait BlockDevice {
    type Error;
    const BLOCK: usize;
    fn poll_read_block(&self, cx: &mut Context<'_>, id: u64, buf: &mut [u8])
        -> Poll<Result<(), Self::Error>>;
    fn poll_write_block(&mut self, cx: &mut Context<'_>, id: u64, buf: &[u8])
        -> Poll<Result<(), Self::Error>>;
    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>>;
}
```

Poll-based, so it needs no executor and no unstable features (ADR-0001).
All I/O is whole blocks (`buf.len() == BLOCK`, else `BadBufferLen`). The
`Db` API is `async fn`s; the host drives them with any executor.

### 4.4 SSTable — immutable sorted run

```
[data block]* [rdel block]* [bloom block] [index block] [footer block]
```

Every block is `BLOCK` bytes ending in a CRC32 of its payload.

- **Data block:** entries `key_len u16 | val_len u16 | seq u64 | op u8 |
  key | val [| expire_at u64]`, sorted by key then sequence descending,
  then `u16` restart offsets (one per 16 entries) and a `u16` count. Bit
  15 of the count flags an LZ77-compressed block (ADR-0008); readers
  inflate transparently.
- **Rdel block:** range tombstones `start_len u16 | end_len u16 | seq u64
  | start | end`, sorted by start, then a `u16` count. Never compressed.
- **Bloom block:** `BLOOM_BYTES × 8` bits; `k` probes by double hashing.
  Advisory: a bad CRC disables the filter.
- **Index block:** per data block `first_key_len u16 | first_key |
  block_id u64 | max_seq u64`.
- **Footer:** `magic u64 = "hrtsst02" | index_block u64 | bloom_block u64
  | entry_count u64 | k u8 | rdel_blocks u32 | min_seq u64`.

A CRC failure on a footer, index, data, or rdel block is `CorruptBlock`
everywhere, never "absent" (which would let an older version win). All
data-block reads go through `sstable::read_data_block`.

A table's `TableRef` carries `first_key` (its **live lower bound**),
`last_key` (covering both sections, the greatest rdel end inclusively),
`max_seq`/`min_seq` over both sections, `entry_count`, and `rdel_blocks`.
Entries and range-tombstone pieces below `first_key` are dead to every
reader (a compaction narrowed the table past them).

### 4.5 Manifest — the crash-safe root pointer

The manifest (`Manifest<LEVELS, TABLES, KEY_MAX>`) holds `wal_head`,
`next_table_id`, `flushed_seq` (the WAL replay floor), `seq_high` (the
sequence high-water mark), and a pool of `LEVELS × TABLES` table refs
shared by the levels. L0 holds at most `TABLES` refs, oldest first;
deeper levels hold disjoint runs sorted by first key, in any share of
the pool.

Storage (format `hrtman05`, ADR-0011): two or more **copies**
(`ManifestLayout::pair` or `ManifestLayout::ring`), each
`Manifest::max_blocks::<BLOCK>()` blocks long, the compile-time worst
case. Block layout:

```
magic: u64 = "hrtman05" | seq: u64 | index: u16 | count: u16 | len: u32
| body chunk[len] | crc32: u32
```

The body (concatenated chunks) is `wal_head u64 | next_table_id u32 |
flushed_seq u64 | seq_high u64 | nlevels u32`, then per level `count u32`
and its table refs.

- **Commit:** write copy `(seq + 1) % copies` block by block (only the
  blocks the body fills), then `flush()`. It is the single atomic commit
  point.
- **Recovery:** a copy is intact only when every block's CRC holds and all
  agree on `seq` and `count`; the newest intact copy wins. A commit torn
  anywhere falls back to the previous copy. No intact copy but a blank
  one is a fresh device.
- **Staged edits:** a commit is described by a `ManifestEdit` (scalars,
  removals and narrowings as pool-position masks, at most one added ref).
  `commit_edit` encodes the manifest as the edit would leave it and
  applies the edit in memory only after the commit lands, so a failed
  commit changes nothing.
- **Format policy (pre-1.0):** no compatibility across minor versions.
  The WAL, SSTable, and manifest each carry a magic that changes with the
  layout; a foreign magic is rejected, never misread.

### 4.6 Table region — fixed slots

The table region `[tbl_start, tbl_end)` is `LEVELS × TABLES` (at most 64)
equal slots of `(tbl_end − tbl_start) / (LEVELS × TABLES)` blocks; one
table per slot, wholly inside it (ADR-0009). `open()` rebuilds the slot
map from the manifest and refuses a slot smaller than a full memtable's
table (`BadConfig`). Flush and ingest claim a free slot after their
manifest commit; a compaction job reserves its output slot for the job's
life. Two slots are kept free for compaction. Every table writer is
capped at its slot (`TableTooLarge`). Allocation is next-fit, which
spreads flash wear.

### 4.7 Compaction — leveled, bounded, caller-driven

`db.compact_step(&mut scratch) -> Result<Progress, Error>`, with a
caller-owned `Compaction<BLOCK, KEY_MAX, VAL_MAX, BLOOM_BYTES>` scratch
(ADR-0004). Each call does bounded work (at most one output block, or one
output's seal and commit) and returns `More` or `Done`;
`compaction_pending()` says whether a job is in flight or selectable.

- **Selection** (ADR-0010): full levels first, deepest first; then, when
  flush is down to the compaction reserve, pressure pushes intermediate
  levels and L0 down. A job is admitted only when free slots cover it; an
  L0 job shrinks to its oldest tables. A non-small table that overlaps
  nothing below moves down with one manifest edit; small tables are
  rewritten with their small neighbours.
- **Merge:** k-way over at most 8 cursors (sources, plus one
  concatenating cursor over the target level's tables), minimum key,
  highest sequence first. Per key it keeps the **keep-set**: the newest
  version plus the newest at or below each live snapshot.
- **Outputs split** at key boundaries when a slot's data budget is nearly
  full, and **each output commits on its own**: inputs wholly behind it
  retire, inputs straddling its last key `k` are narrowed to start at
  `succ(k)`. Range tombstones are clipped to each output's range, and to
  each input's live lower bound.
- **Drops:** a bottommost point tombstone older than every snapshot, with
  nothing older outside the job, drops its key. A range tombstone older
  than every snapshot drops every older version it covers, at any level;
  at the bottom it drops itself once nothing it hides is left (ADR-0010,
  F16). Expired values (`Compaction::purge_before`) become tombstones at
  their sequence.
- **Crash safety:** outputs are invisible until their commit; a crash
  leaves the tree as of the last landed commit, and the next job merges
  on from the narrowed inputs. Crash tests enumerate every write of
  multi-output jobs.

### 4.8 Read path

- **Point reads** (`get`, `get_at`, `get_with_time`, `get_at_with_time`):
  memtable, then L0 newest first, then deeper levels; key-range, bloom,
  and `max_seq` prunes; the highest sequence at or below the snapshot
  wins; a strictly newer covering range tombstone hides it; an expired
  winner reads as missing. Blocks are read through the `Db`'s shared
  buffers, so a second concurrent `get` on one handle is `Busy`.
- **Scans** (`Scan`, `RevScan`): a merge over the memtable and one
  cursor per table in caller-owned state, same winner rule, skipping
  hidden keys; `BufferTooSmall` never consumes an entry.
- **Block cache:** `CACHE` slots of CLOCK-evicted physical block images
  keyed by `(table id, block id)`; scans insert cold (ADR-0007).

### 4.9 Archive and ingest

`archive_plan` / `archive_commit` hand a sealed table to caller storage
and forget it locally; `archive_commit` refuses (`WouldResurrect`) a
table whose tombstones still hide a value anywhere. `ingest_table`
copies a sealed table back in, verifies and relocates it, and grafts it
at L0 (ADR-0005); the sequence counter resumes above it.

## 5. Public API

```rust
horton::db_types! {                      // named parameters (F11)
    block: 4096, key_max: 32, val_max: 64, memtable_entries: 16,
    memtable_arena: 2048, levels: 4, tables_per_level: 4, bloom_bytes: 64,
    cache_blocks: 2;
    type Db = MyDb; type Scan = MyScan; type RevScan = MyRevScan;
    type Compaction = MyCompaction;
}

impl Db<…> {
    pub const fn new(device: D, config: Config) -> Self;
    pub async fn open(&mut self) -> Result<OpenReport, Error<D::Error>>;
    pub async fn put(&mut self, key: &[u8], val: &[u8]) -> Result<u64, Error<D::Error>>;
    pub async fn put_with_ttl(&mut self, key: &[u8], val: &[u8], expire_at: u64) -> Result<u64, …>;
    pub async fn delete(&mut self, key: &[u8]) -> Result<u64, …>;
    pub async fn delete_range(&mut self, start: &[u8], end: &[u8]) -> Result<u64, …>;
    pub async fn write<const OPS: usize>(&mut self, batch: &WriteBatch<KEY_MAX, VAL_MAX, OPS>) -> Result<u64, …>;
    pub async fn get(&self, key: &[u8], val_buf: &mut [u8]) -> Result<Option<usize>, …>;
    pub async fn get_at(&self, key: &[u8], val_buf: &mut [u8], max_seq: u64) -> Result<Option<usize>, …>;
    pub async fn get_with_time(&self, key: &[u8], val_buf: &mut [u8], now: u64) -> Result<Option<usize>, …>;
    pub async fn flush(&mut self) -> Result<(), …>;
    pub async fn compact_step(&mut self, scratch: &mut Compaction<…>) -> Result<Progress, …>;
    pub fn compaction_pending(&self) -> bool;
    pub const fn snapshot(&mut self) -> Result<u64, …>;
    pub const fn release_snapshot(&mut self, snap: u64);
    pub fn archive_plan(&self, level: usize, table_id: u32) -> Option<ArchivePlan<KEY_MAX>>;
    pub async fn archive_commit(&mut self, level: usize, table_id: u32) -> Result<bool, …>;
    pub async fn ingest_table<R: BlockDevice>(&mut self, sealed: &SealedTable<KEY_MAX>, remote: &R, src_base: u64) -> Result<bool, …>;
    pub const fn slot_stats(&self) -> SlotStats;
    pub fn check_invariants(&self) -> Result<(), &'static str>;
}
// Scan::new(&db) → seek(start, end, max_seq) → next(key_buf, val_buf)
// RevScan::new(&db) → seek_prev(from, lower, max_seq) → prev(key_buf, val_buf)
```

Rustdoc on each item is the reference; this is the shape.

## 6. Error model

`Error<E>` has no strings and no allocation; every variant is returned,
never panicked. Capacity errors name their remedy (ADR-0013):

| Variant | Meaning | Remedy |
|---|---|---|
| `TableFull`, `ArenaFull` | memtable full | `flush()`, retry |
| `WalFull` | WAL region exhausted | `flush()` (it wraps the WAL), retry |
| `NeedsCompaction` | L0 full, or no slot until tables merge | `compact_step` until `Done`, retry |
| `RegionFull` | no slot, and compaction cannot free one | delete and compact, archive, or grow the region |
| `SnapshotLimit` | 8 snapshots live | release one |
| `TableTooLarge` | an entry, index, or rdel section does not fit | smaller entries, bigger blocks or slots |
| `BatchTooLarge`, `BatchFull` | batch exceeds a WAL block / `OPS` | split it |
| `Busy` | another `get` holds the shared read buffers | finish it, retry |
| `BadConfig` | regions overlap or are too small | fix the `Config` |
| `NotOpen` | used before `open()` | call `open()` |
| `WouldResurrect`, `IngestConflict` | archive / ingest refused | see the API docs |
| `KeyTooLarge`, `ValueTooLarge`, `EmptyKey`, `BufferTooSmall`, `BadBufferLen`, `BadLevel` | caller input | fix the call |
| `CorruptBlock`, `CorruptWal`, `CorruptManifest` | integrity failure | — |
| `ManifestFull`, `CounterExhausted` | invariant / counter overflow | — |
| `Device(E)` | the device's own error | — |

`NeedsCompaction` is returned only while `compaction_pending()` is true,
so a compact-then-retry loop always makes progress.

## 7. Region layout (`Config`)

`Config::new(wal_start, wal_end, tbl_start, tbl_end, manifest_a,
manifest_b)` places three disjoint regions in block ids;
`with_manifest_ring(n)` replaces the pair by `n` copies back to back from
`manifest_a`. Each manifest copy spans `Manifest::max_blocks` blocks.
`open()` checks, before any I/O, that no region is empty, that the WAL,
the table region, and every manifest copy are disjoint, and that a table
slot can hold a full memtable's table (`BadConfig` otherwise).

Sizing: the table region holds `LEVELS × TABLES` tables of at most
`slot_blocks` blocks each; a WAL block holds one durable commit, so the
WAL region bounds the commits between flushes.

## 8. Testing strategy

- Unit and integration tests per component, including decoder fuzzing
  (WAL, SSTable, manifest, compression) with arbitrary bytes.
- Differential oracles against a `BTreeMap` model: scripted and random
  op sequences, both scan directions, snapshots.
- **Lifecycle fuzzer** (`tests/lifecycle.rs`): long random sequences of
  every operation — including reopen, WAL wrap, archive and re-ingest,
  and TTL purges — on small regions across three geometries, checking
  `Db::check_invariants` after every op and the oracle throughout.
  `LIFECYCLE_SEEDS=n` sweeps wider.
- **Crash injectors:** every block write of flush, compaction
  (multi-output jobs included), archive, and ingest is a crash point, and
  torn manifest writes are injected; the recovered state is always one of
  the committed states.
- Regression tests for every architecture-review finding
  (`tests/review_findings.rs`, never `#[ignore]`d).
- `cargo miri` over a subset; the RAM gate (`tests/profile.rs`); benches
  (`benches/`).

## 9. Milestones

| Version | Theme |
|---|---|
| v0.1–v0.3 | MemTable, WAL, recovery, SSTables, manifest, flush, free-list allocation |
| v0.4–v0.8 | Caller-driven compaction, snapshots, scans, every-level compaction |
| v0.9–v0.12 | Crash hardening, archive, reverse scans, ingest |
| v0.13–v0.16 | Block compression, range deletes and TTL, block cache |
| v0.17 | Architecture review fixes: fixed table slots, split-output compaction, multi-block manifest with ring and staged edits, remedy-named errors, measured futures, range-delete space reclamation |

Details: [`CHANGELOG.md`](CHANGELOG.md) and
[`docs/history/milestones-v0.1-v0.16.md`](docs/history/milestones-v0.1-v0.16.md).

## 10. Open questions for Mark
1. ~~First target~~ — decided 2026-09-12: x86_64 + macOS first, ESP32-S3 on
   the v0.6 roadmap.
2. ~~`no_alloc` hard line?~~ — decided 2026-09-12, refined later the same
   day: the default build is hard no_alloc; a tiny dependency-free bump
   allocator over caller-provided memory is acceptable behind a default-off
   `scratch-bump` feature, for internal scratch only. Not needed before v0.4
   (compaction).
3. ~~Sync vs async `BlockDevice`?~~ — decided 2026-09-12: async from the
   start, poll-based trait (§4.3), `core`-only via `poll_fn`.
4. ~~Name~~ — decided 2026-09-12: **horton**. (Former working title:
   `zero-lsm`.)
