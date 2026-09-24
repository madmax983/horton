# Changelog

All notable changes to horton are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Entries up to
0.16.0 are extracted from the milestone log in `SPEC.md` §9. Dates are
given where SPEC records one.

Before 1.0 the on-disk format is not compatible across minor versions:
old images are rejected (for example as `CorruptManifest`), never
misread (SPEC §4.5).

## [Unreleased]

Fixes every finding of the v0.16 architecture review
([`docs/ARCHITECTURE_REVIEW.md`](docs/ARCHITECTURE_REVIEW.md)). Each
confirmed defect has a regression test in `tests/review_findings.rs`; CI
fails if one is ever `#[ignore]`d again. The on-disk format changes
(manifest `hrtman05`, SSTable `hrtsst02`): v0.16 images are rejected, not
misread.

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
- **F17** WAL writes after a reopen could be silently lost (found by the
  lifecycle fuzzer).
- A newer range tombstone could be dropped in favour of an older,
  identical one when merging.

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

### Changed

- **Breaking:** `Error::NoSpace` is gone; each capacity condition has
  its own variant naming the remedy (ADR-0013).
- **Breaking:** `Config::new`'s second manifest position must leave room
  for a whole copy (`Manifest::max_blocks` blocks); `open()` rejects
  overlapping regions with `BadConfig`.
- **Breaking:** two `get` futures polled concurrently on one `Db`: the
  second returns `Error::Busy` instead of using fallback buffers.
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
  futures (112,200 bytes measured).
- `db.rs` is split into `db/{mod,read,flush,archive,compaction,invariants}.rs`;
  point reads and the archive resurrection review share one read rule,
  the single-op writes share one path, every data-block read goes
  through `sstable::read_data_block`, and both scan directions share one
  range-tombstone lookup.
- `SPEC.md` is now the normative spec only (rewritten for v0.17); its
  milestone log moved to `docs/history/milestones-v0.1-v0.16.md`.

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
