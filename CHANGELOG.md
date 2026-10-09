# Changelog

All notable changes to horton are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Entries up to
0.16.0 are extracted from the milestone log in `SPEC.md` §9. Dates are
given where SPEC records one.

Before 1.0 the on-disk format is not compatible across minor versions:
old images are rejected (for example as `CorruptManifest`), never
misread (SPEC §4.5).

## [Unreleased]

## [0.17.0] - 2026-10-09

The first release on crates.io.

Fixes every finding of the v0.16 architecture review
([`docs/ARCHITECTURE_REVIEW.md`](docs/ARCHITECTURE_REVIEW.md)). Each
confirmed defect has a regression test in `tests/review_findings.rs`; CI
fails if one is ever `#[ignore]`d again. The on-disk format changes
(manifest `hrtman05`, SSTable `hrtsst02`): v0.16 images are rejected, not
misread.

### Added

- **Key-value store example** (`examples/kvstore`): horton on a host the
  way LevelDB is used. A file-backed `BlockDevice` (`pread`/`pwrite`,
  `fdatasync`), a store thread that owns the `Db` behind a cloneable
  handle, group commit of concurrent writers into one WAL block, capacity
  errors handled and compaction run in the background, snapshots and
  paging iterators. `demo` tours the API, `put`/`get`/`scan`/… make it a
  command-line store, `bench` runs db_bench's workloads, and `crash` kills
  a multi-threaded writer process mid-write and checks the reopened store
  against every acknowledged write. CI runs all of it on Linux, macOS and
  Windows. The README lists where horton stops at host scale.
- **Ground station example** (`examples/ground_station`): the flight
  recorder's horton compiled to `wasm32-unknown-unknown` as a `no_std`,
  import-free `cdylib`, and a web page that opens a recorder's flash dump
  in the browser. It runs the device's recovery, checks every entry
  against the recorder's key hash, and charts the sensors. CI records a
  flight, reads it (and one killed three times with SIGKILL), and drives
  the page in headless Chromium.
- **Live telemetry** (`examples/ground_station/live`, `web/live.html`):
  the recorder's horton logging a live stream in a browser Web Worker.
  Its flash is an OPFS file behind the sync access handle, and cold
  tables go to IndexedDB through the archive API (stage, a `strict`
  IndexedDB transaction, then `archive_commit`). The simulated device
  resends every frame not yet acknowledged. A Node test cuts the power
  300 times (torn in-place writes, and cuts on either side of an archive
  store) and proves every durable tick whole across flash and archive; a
  Chromium test kills the Worker, then crashes the tab, and proves nothing
  acknowledged was lost. The logger's flash opens in the dump viewer, and
  the native recorder's `restore` reads its archive.
- The flight recorder's formats (`db_types!` shape, layout, key schema,
  archive object) live in `examples/flight_recorder/format.rs`, shared
  with the ground station and the live logger so none can drift apart.

### Fixed

- **F1** Compaction output could outgrow its reservation and overwrite
  a live table. Tables now live one per fixed slot, and every writer is
  capped at its slot (ADR-0009).
- **F2** A deleted key could come back after a WAL wrap and reopen. The
  manifest persists the WAL replay floor and a sequence high-water mark.
- **F3** `open()` failed after ordinary compaction when the free list
  filled. There is no free list any more: the slot map is rebuilt from
  the manifest.
- **F4** Writes before `open()` could destroy acknowledged data. Every
  device-touching call before `open()` returns `Error::NotOpen`.
- **F5** `RevScan` could yield an older memtable version of a key.
- **F6** Writes stopped with about 4% of the table region used.
  Compaction splits outputs, commits each one, moves and consolidates
  tables, and pushes down under pressure; writes now continue until most
  of the region holds live data (ADR-0010).
- **F7** The manifest had to fit one block (`TestDb` stopped at 7 of 28
  tables). Manifest copies span `Manifest::max_blocks` blocks (ADR-0011).
- **F12** A sequence-0 entry wedged `Scan::next`.
- **F13** A data block failing its CRC read as "absent" on point reads,
  surfacing stale values. It is `CorruptBlock` now.
- **F14** A flush during an in-flight compaction could take the job's
  output blocks. Compaction reserves its output slot.
- **F15** Compaction dropped a bottommost tombstone that an older
  re-ingested table still needed.
- **F16** (found while fixing F6) `delete_range` never gave space back:
  compaction kept every version a range tombstone hid, and the tombstone,
  forever. A merge now drops versions hidden from every reader by a range
  tombstone in the job, and a bottommost merge drops the tombstone once
  nothing it hides is left. Merges also clip each input's range
  tombstones to its live lower bound, so a narrowed table's dead
  tombstones are never re-emitted.
- **F18** A torn WAL block could replay part of a `WriteBatch`: the
  batch's first records passed their CRCs. Batch records are now grouped
  (the op byte's `MORE` bit) and recovery replays a group only when its
  closing record is intact (found by the flight recorder's power-cut test).
- **F19** When a torn WAL block still held valid records, writes made
  after the reopen could be lost at the next reopen: recovery stopped at
  the torn block. A torn batch block is now overwritten, and recovery reads
  past a torn block when the next block holds newer records.
- **F17** WAL writes after a reopen could be silently lost (found by the
  lifecycle fuzzer).
- A newer range tombstone could be dropped in favour of an older,
  identical one when merging.
- **F20** `Scan` and `RevScan` panicked when a level below 0 held more
  than `TABLES` tables, which the manifest allows once region pressure
  pushes tables down (found by the live logger). Their cursors are one
  pool now, level-major.
- **F21** On a device that overwrites in place (a file, an SD card, a
  disk), a batch torn over an older batch of the same shape recovered in
  part: the older batch's closing record, left behind the tear, closed
  the torn group (found by the live logger's power-cut test). Recovery
  ends a block at the first record no newer than the one before it.
- **F22** Overlapping range deletes wedged compaction for good (found by
  the kvstore example's crash test). Past four tombstones overlapping at
  a key the merge kept every range tombstone of the job, so a range
  deleted again and again piled them up; and every job reserved room for
  all of them at the `12 + 2 * KEY_MAX`-byte worst case. Once that
  outgrew a slot, every L0 job failed with `TableTooLarge` before it
  began, level 0 stayed full, and no flush could land. The merge now
  tracks only tombstones no newer, wider one dominates (eight at once,
  +632 bytes of `Compaction`), and where it still overflows it keeps only
  the tombstones reaching there. The reservation counts the tombstones'
  real bytes, plus what clipping at the output bounds can add.
- The flight recorder's `restore` failed on archives of more than a few
  dozen tables (`IngestConflict`, then `RegionFull`): it ingested the
  whole history into one database, whose shape has 16 table slots. It
  reads each table back through its own database now, as `verify` does.
- The ground station counted frames split with the archive as torn
  unless they were the oldest on flash; a table boundary can split any
  frame. A prefix or a suffix of a frame's sensors is now a split, and
  anything else is torn.
- The `multiwriter` feature did not build once v0.17 landed: the drainer
  and writer still used `Error::NoSpace` and `Db`'s removed `FREELIST`
  parameter. The `loom` ring model did not compile either.

### Added

- `Config::with_manifest_ring(n)`: rotate manifest commits over `n`
  copies for NOR endurance (ADR-0012).
- `ManifestLayout`, `ManifestEdit`, `Manifest::commit_edit`,
  `stage_remove`, `stage_narrow`, `apply_edit`, `narrow_table`,
  `KeyBound::successor`.
- `db_types!`: declare `Db`/`Scan`/`RevScan`/`Compaction` aliases with
  named parameters.
- `Db::slot_stats`, `Db::check_invariants` (the lifecycle fuzzer checks
  it after every operation).
- `Error::widen`: converts a `WriteBatch` or `MemTable` error
  (`Error<Infallible>`) to any device's error type.
- Errors `WalFull`, `NeedsCompaction`, `RegionFull`, `SnapshotLimit`,
  `ManifestFull`, `TableTooLarge`, `CounterExhausted`, `BadLevel`,
  `BadConfig`, `NotOpen`, `Busy`.
- `tests/lifecycle.rs`: a differential fuzzer over long operation
  sequences with reopen, across three geometries.
- The RAM gate measures every public future (`tests/profile.rs`).
- CI (fmt, clippy pedantic + nursery, rustdoc, debug and release tests,
  miri subset, bench build), a pinned toolchain, license texts, ADRs.
- A way in for newcomers: `db_types!` needs only `block`, `key_max`
  and `val_max`; the tuning parameters are optional and take the
  defaults in `horton::defaults`. `Config::whole_device(n)` lays the
  manifest, WAL and tables out over blocks `0..n`,
  `Db::MIN_DEVICE_BLOCKS` is the smallest `n` a shape fits, and
  `Db::config` shows the layout in use. The README's "Sizing" section
  explains every parameter and region in plain terms, and the quick start
  prints its RAM use and layout.
- CI builds, lints and tests the `multiwriter` and `loom` features.
- `examples/flight_recorder`: a sensor logger on simulated NOR flash that
  exercises the whole API, survives `kill -9` and simulated power cuts
  (`torture N`, run in CI), and streams cold tables to a directory or to
  S3 (`--s3`, signed by `curl --aws-sigv4`), with a `restore` ground
  station that re-ingests the archive.
- `FlashBlockDevice::flash` and `into_flash`: reach the chip behind the
  device.
- `Error` implements `Display` and `core::error::Error`.
- `Db::tables(level)`: the sealed tables of a level, for host-side
  archive and replication loops.
- Replica-side last-writer-wins: `TableRef` records `node_id` and
  `seal_wall`, `Config::with_node_id`, `Db::stamp_table`, and
  `Error::StampConflict`. Reads merge versions across nodes by
  `(seal_wall, node_id)`; the manifest magic is now `hrtman06`.
- The `multiwriter` ring carries range deletes and TTL puts
  (`WriteBatch::range_delete`, `WriteBatch::put_ttl`).
- Package metadata for crates.io (`repository`, `rust-version = "1.88"`,
  keywords, categories), a docs.rs build with `multiwriter`, and CI jobs
  for the MSRV and `cargo publish --dry-run`.

### Changed

- **Breaking:** `Error::NoSpace` is gone; each capacity condition has
  its own variant naming the remedy (ADR-0013).
- Multiwriter errors (feature `multiwriter`): a full ring at claim time
  is `Error::RingFull` (let the drainer sweep, then retry), and a ring
  payload the drainer cannot decode is `Error::BadPayload` (the drainer
  poisons). A batch error during a drainer sweep keeps its own variant.
- **Breaking:** `Config::new`'s second manifest position must leave room
  for a whole copy (`Manifest::max_blocks` blocks); `open()` rejects
  overlapping regions with `BadConfig`.
- **Breaking:** two `get` futures polled concurrently on one `Db`: the
  second returns `Error::Busy` instead of using fallback buffers.
- **Breaking:** a shape whose `memtable_arena` is smaller than
  `key_max + val_max` no longer compiles: a maximal write could never
  fit, not even right after a flush.
- **Breaking:** `Config` has a new public field, `device_blocks` (0 for
  a hand-placed layout); code that builds a `Config` literal must set it.
- **Breaking:** `horton::alloc` is renamed `horton::slots` (it shadowed
  the `alloc` crate). `model` and the low-level table builders
  (`plan_table`, `write_table`, …) are `#[doc(hidden)]`.
- `compaction_pending()` also reports an in-flight job.
- The table region is `LEVELS × TABLES` fixed slots (at most 64).
- Futures shrank: `flush` 29.8 → 17.6 KiB, `get` 9.1 → 1.1 KiB,
  `compact_step` 6.6 → 2.1 KiB, `ingest_table` 6.3 → 0.6 KiB,
  `Scan::next` 4.6 → 0.7 KiB, `RevScan::prev` 4.6 → 1.1 KiB. Manifest
  commits stage a small edit instead of a manifest copy.
- Scans' range-tombstone check skips tables whose `max_seq` cannot beat
  the winning version and stops at the first hit.
- The ESP32-S3 budget is 112 KiB and now covers structs plus peak
  futures (112,256 bytes measured).
- `db.rs` is split into `db/{mod,read,flush,archive,compaction,invariants}.rs`;
  point reads and the archive resurrection review share one read rule,
  the single-op writes share one path, every data-block read goes
  through `sstable::read_data_block`, and both scan directions share one
  range-tombstone lookup.
- `SPEC.md` is now the normative spec only (rewritten for v0.17); its
  milestone log moved to `docs/history/milestones-v0.1-v0.16.md`.
- **Breaking:** `Error` is `#[non_exhaustive]`. A `match` on it needs a
  `_` arm.
- **Breaking:** the `loom` feature is gone. Loom models run with
  `RUSTFLAGS="--cfg horton_loom"` and `--features multiwriter`, so Loom
  is never part of a normal build.
- **Breaking:** the unused `scratch-bump` feature is gone.

## [0.16.0] - 2026-09-23

### Added

- Caller-owned SSTable block cache, `cache::BlockCache<BLOCK, SLOTS>`,
  held inside `Db` with `CACHE` slots. `CACHE = 0` disables it.
- The cache serves point reads, forward scans and reverse scans for
  data, index, bloom, footer and range-tombstone blocks. WAL, manifest
  and compaction-merge reads bypass it.
- CLOCK (second-chance) eviction. Data blocks loaded by a scan insert
  cold, so a scan does not push out the point-read hot set.
- Entries are keyed by (table id, device block id); compaction
  invalidates the entries of the tables it drops.
- `Db::cache_stats` reports hits and misses.

### Changed

- The ESP32-S3 profile uses a 2-slot cache. It totals 92,520 bytes
  (`Db` 25,576 + `Scan` 10,536 + `Compaction` 56,408), within the
  98,304-byte budget.

## [0.15.0]

### Added

- `Db::delete_range(start, end)` writes a range tombstone that shadows
  every key in `[start, end)` with one sequence number.
- `Db::put_with_ttl(key, val, expire_at)`. Reads take a caller-supplied
  `now` and suppress values with `expire_at <= now`; horton owns no
  clock, and the existing read APIs use `now = 0`.
- TTL purge in compaction through `Compaction::purge_before`: an expired
  value becomes a point tombstone at the same sequence number.
- Range tombstones flow through the WAL, memtable, SSTables, point
  reads, both scan directions, flush, archive/ingest and compaction.
- `model_visible`, an executable model of the winner rule with range
  tombstones and TTL.

### Changed

- WAL records gain `Op::RangeDelete = 3` and `Op::PutTtl = 4`. Existing
  record kinds are byte-identical to 0.14.
- SSTables gain a range-tombstone section before the data blocks, and
  the footer and `TableRef` gain `rdel_blocks`. Data entries use op `4`
  for TTL values.
- `archive_commit` refuses a table carrying range tombstones
  (`WouldResurrect`) unless they provably shadow nothing outside it.

## [0.14.0] - 2026-09-23

### Added

- `RevScan`, the descending mirror of `Scan`: `seek_prev(from, lower,
  max_seq)` positions at the last entry `<= from`, and `prev` yields
  entries in descending key order.
- The same merge rules as `Scan`: highest sequence wins, tombstones are
  skipped, snapshot watermarks are respected, and `BufferTooSmall` is
  returned before any cursor advances.
- Version runs that straddle data blocks are followed backward, so the
  newest visible version of a key wins.
- `sstable::index_last_le_block` finds the last block whose first key
  is `<= from`.

## [0.13.0] - 2026-09-23

### Added

- Hand-rolled LZ77 block compression in `src/compress.rs`, with a
  caller-owned `CompressScratch<BLOCK>`. No dependency, no allocation.
- Each sealed data block is trial-compressed and kept compressed only if
  that saves at least 128 bytes (`COMPRESS_MIN_SAVING`).
- Compressed blocks are flagged by bit 15 of the restart-count trailer.
  The raw layout is unchanged, so pre-0.13 tables read unchanged.

### Changed

- `TableWriter::push`/`finish` and `write_table` take
  `Option<&mut CompressScratch<BLOCK>>`; `None` stores raw.
- `Db` gains a one-block `decomp_scratch` buffer for reads.
- The ESP32-S3 RAM budget is raised from 64 KiB to 96 KiB.

## [0.12.0]

### Added

- `Db::ingest_table` re-attaches an archived table. It copies the blocks
  from a caller `BlockDevice` source, relocates their block pointers,
  verifies CRCs, footer and entry count, and grafts the table into L0
  in one atomic manifest commit.
- Ingest is idempotent: `Ok(false)` when the same table is already
  attached, and `Error::IngestConflict` when the id is attached with a
  different shape.
- `ArchivePlan::sealed` gives the placement-free `SealedTable`
  descriptor that ingest takes.
- Combined remote/local reads: a re-attached table is an ordinary L0
  table, so highest-sequence-wins applies across both.

### Changed

- `archive_commit` enforces the tombstone rule itself. It refuses, with
  `Error::WouldResurrect { table }`, any removal that would resurrect a
  deleted key in the live view or a live snapshot. This replaces 0.10's
  documented caller discipline, which missed same-level and shallower
  tables.

## [0.11.0]

### Added

- `WriteBatch<KEY_MAX, VAL_MAX, OPS>`: a caller-owned, fixed-capacity
  batch of puts and deletes.
- `Db::write(batch)` applies a batch atomically with a single WAL block
  write and returns the base sequence number. An empty batch is a no-op.
- `Error::BatchTooLarge` for a batch larger than one WAL block. All
  validation happens up front, so a rejected batch leaves no trace.

### Fixed

- A failed `put` left its WAL record staged and reused its sequence
  number, so the mutation could resurrect through a later commit.
  `put`, `delete` and `write` now roll the stage back when the block
  write fails.

## [0.10.0]

### Added

- Archive API: `Db::level_tables`, `Db::archive_plan` (returns an
  `ArchivePlan`), and `Db::archive_commit`, which drops the table from
  the manifest in one atomic write and then reclaims its blocks.
- `Db::device()` is public so the caller can stream a table's blocks to
  its own remote sink. horton performs no networking.
- `archive_commit` is idempotent: `Ok(false)` when the table is already
  gone.
- Archiving tables that carry tombstones relies on a documented caller
  discipline (delete-bearing workloads archive only from the bottommost
  level). 0.12 replaces it with an enforced check.

### Changed

- Rust edition 2021 to 2024.

## [0.9.0]

### Added

- miri over the test suite. The crate forbids `unsafe`, so this is a
  pure UB check. Two exclusions (the `esp32s3` test binary and one fuzz
  test) are sandbox hangs, not test failures.
- In-tree, dependency-free fuzzers for the WAL, SSTable and manifest
  decoders. Bit flips, truncations and splices must yield clean errors,
  never a panic.
- Crash injection over compaction merges and manifest commits, not only
  flush: recovery must see exactly the pre- or post-commit state.

## [0.8.0]

### Added

- `Db::compaction_pending()` reports whether a compaction job is
  selectable.

### Changed

- Compaction works on every level. It takes the deepest full level; a
  full level deeper than L0 compacts its oldest table plus the
  overlapping tables in the next level.
- `NoSpace` is decided at select time, before any merge I/O, and only
  for a full bottommost level whose tables the merge does not absorb.
  A full L1 now drains into L2.
- `compact_step` returns `Progress::Done` when no job is selected.

### Fixed

- `SpiFlash::erase_sector` restores `CTRL` on every error path. A
  sector-erase timeout used to leave it cleared, destroying the
  boot-configured read mode.

## [0.7.0]

### Added

- ESP32-S3 SPI flash driver `SpiFlash<B: RegBus>` (`src/esp32s3.rs`),
  generic over a register-bus trait so all of its logic is safe code.
  The volatile MMIO adapter lives in the board crate.
- Sector erase, page program and user-mode `0x03` reads follow ESP-IDF
  v5.2's LL-layer register sequences.
- Write-enable is checked after every WREN (`Error::WriteEnableFailed`)
  and every busy poll is bounded (`Error::Timeout`).
- Host tests against a mock NOR chip, including a full `Db` on
  `FlashBlockDevice<SpiFlash<MockBus>>`. The driver is not yet verified
  on silicon: QEMU does not emulate the SPI user-command path.

## [0.6.0]

### Added

- Build gate for `xtensa-esp32s3-none-elf` with the ESP Rust fork
  (`xtensa-check.sh`).
- Bare-metal ESP32-S3 smoke binary (`xtensa-smoke/`) that boots under
  QEMU and drives a `Db` through put, get, delete, flush, compact, scan
  and snapshot.
- `Flash` trait and `FlashBlockDevice<F>`, an erase-aware `BlockDevice`
  wrapper: each whole-block write is erase-sector then program.
- `ESP32S3` const profile with compile-time size assertions, and
  `BUDGET.md`.

## [0.5.0]

### Added

- `Scan`, a merge iterator that borrows `&Db`, with `seek(start, end,
  max_seq)` and `next`. Prefix scans use an exclusive end bound.
- Snapshot reads: `snapshot()` (up to 8 live), `release_snapshot()`,
  `get_at()`, and `TableReader::get_at`/`lookup_at`.
- Executable models `model_winner`, `model_visible` and
  `model_keep_set` in `src/model.rs`, with property and differential
  tests.

### Changed

- The memtable and SSTables keep full version chains. Compaction keeps
  each key's keep-set and drops a bottommost newest tombstone only when
  it predates every live snapshot.

## [0.4.1]

### Added

- Compaction returns its input tables' block runs to the free list
  strictly after the manifest commit. This is best-effort; leftovers
  are swept by the next open.

### Changed

- On-disk format policy before 1.0: no compatibility across minor
  versions. A foreign manifest magic is `CorruptManifest`.
- Manifest magic `hrtman01` becomes `hrtman02` for the length-prefixed
  key-bound encoding.

## [0.4.0] - 2026-09-14

### Added

- `Db::compact_step(&mut Compaction)` returning `Progress::{More,
  Done}`, with caller-owned typed scratch. L0 to L1 only.
- Linear-scan k-way merge over up to 8 inputs; highest sequence wins.
- One output block per call; the final call commits the manifest
  atomically. A crash leaves exactly the pre- or post-compaction state.
- Tombstones are dropped only when no level numbered 2 or higher
  overlaps the output span.
- Slicing-by-8 CRC, a reused `get` scratch buffer, word-chunked zero
  scans, and callgrind bench harnesses.

### Fixed

- Manifest key bounds were written as full 256-byte arrays (544 bytes
  per table), overflowing a 4 KiB manifest block at 8 tables. They are
  now length-prefixed.

## [0.3.0] - 2026-09-13

### Added

- Full read path in `get()`: memtable, then L0 newest-first, then deeper
  levels, with key-range, bloom and sequence pruning. The highest
  sequence wins across levels, and a newer tombstone hides older values.
- `FreeList` with first-fit run allocation: free list first, claimed
  only after the manifest commit.
- The open-time sweep reclaims orphaned table blocks.
- The WAL wraps atomically with the flush that fills it; recovery skips
  stale pre-wrap blocks with a sequence floor.

## [0.2.0] - 2026-09-13

### Added

- SSTable writer and reader: immutable sorted runs of fixed-size blocks
  (format in SPEC §4.4).
- `Db::flush()` streams the memtable into an L0 SSTable.
- Double-buffered manifest and its atomic commit protocol.
- Bump-pointer block allocation.
- `get()` reads L0 tables newest-first.

## [0.1.0]

### Added

- MemTable: sorted slots over a bump arena.
- WAL: append-only, CRC-framed records written as whole blocks.
- Recovery replays the WAL up to the first corrupt or truncated record,
  the torn tail.
- The poll-based `BlockDevice` trait, with an in-memory device in
  tests.
- Crash injection over 3-op scripts.
