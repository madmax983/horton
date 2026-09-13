# horton — SPEC v0.1 (draft)

A log-structured merge-tree key-value store for places where
`malloc` doesn't exist: firmware, kernels, bootloaders.

Status: prose spec (this file). Verus state-machine models for the memtable
sortedness invariant and the WAL prefix property are queued as the PROOF step,
per SPEC-PROOF-RED-GREEN-REFACTOR.

## 1. Hard constraints (CI-enforced, no exceptions)

- `#![no_std]`, no `extern crate alloc`. The `[dependencies]` table is empty:
  only `core` may be used.
- `no_alloc` is the default posture (Mark's call, 2026-09-12, refined same
  day): the default build allocates nothing, period. One exception is
  allowed — a tiny, dependency-free bump allocator over caller-provided
  memory, behind a default-off cargo feature (`scratch-bump`), for internal
  scratch only (e.g. compaction workspace). It is never required, never
  global, and never pulls in `alloc`. If it needs the heap, it doesn't belong
  in this codebase.
- All memory is caller-provided and compile-time sized via const generics.
  Constructors are `const fn` where possible.
- No recursion (firmware stack budgets), no panics in library code, no
  `unwrap`/`expect`/`assert` in non-test code. Every failure is a `Result`.
- Unsafe is disallowed except where explicitly justified in this spec
  (target: zero `unsafe` blocks; `copy_within` and friends are safe).
- Default budget profile (tunable via consts): ≤ 64 KiB RAM for
  memtable + scratch, ≤ 4 KiB stack per public call. Code size tracked in CI.

### Targets

- v0.1–v0.5: **x86_64** (VesperOS, bare metal) and **macOS** (dev/test host).
  The library is `#![no_std]` + `#![no_alloc]` on every target — on macOS the
  `BlockDevice` is implemented over `std::fs` in the harness, and the oracle
  and crash-injector tests run under `std`. `std` is test-only, never a
  library dependency.
- Roadmap (v0.6+): **ESP32-S3** under Tallow — SPI-flash `BlockDevice`,
  budget re-tune for ~320 KiB RAM reality.

## 2. Data model

- Key: byte string, `0 < len <= KEY_MAX` (default 256). Ordered by unsigned
  lexicographic byte order. Empty keys rejected.
- Value: byte string, `len <= VALUE_MAX` (default 1024). Empty values allowed
  (distinct from deletion).
- Every mutation carries a `u64` sequence number from a monotonic counter.
- Deletes are tombstones: a delete inserts a tombstone entry, not a hole.
- Read-your-writes within a `Db` handle; single-key atomicity. v0.1 is
  single-threaded (`&mut self` API); `Send` where possible, not `Sync`.

## 3. Architecture

```
put/delete ──► WAL append ──► MemTable insert ──► (full?) ──► flush ──► L0 SSTable
                                                                              │
get/scan ◄── L0..Ln ◄── bloom + key-range pruning ◄── leveled compaction ◄─────┘
```

Write path is WAL-first: a mutation is durable once the WAL record is
flushed; the memtable is a volatile index over it.

## 4. Components

### 4.1 MemTable — sorted slots over a bump arena

```rust
pub struct MemTable<const CAP: usize, const ARENA: usize, const KEY_MAX: usize, const VAL_MAX: usize> {
    slots: [Slot; CAP],   // sorted by key; len <= CAP tracked separately
    len: usize,
    arena: [u8; ARENA],   // bump-allocated key/value bytes
    arena_len: usize,
    seq: u64,             // last assigned sequence number
}
struct Slot { hash: u32, key_off: u32, key_len: u16, val_off: u32, val_len: u16, seq: u64, tombstone: bool }
```

- Insert: binary search `slots[..len]` by `(key, seq)`; on duplicate key,
  supersede in place if the arena has room, else append new bytes and mark the
  old slot dead (dead slots are skipped, reclaimed on flush — no compaction
  inside the memtable, bounded waste: at most one dead slot per key).
- New key bytes are bump-appended to `arena`; the slot is `copy_within`-shifted
  into sorted position. O(n) insert, O(log n) lookup; `CAP = 4096` keeps the
  shift cost trivial against flash write costs.
- Full conditions are explicit errors: `Error::TableFull` (slots) and
  `Error::ArenaFull` (bytes). The caller flushes on either.

Why not a skiplist: pointer chasing without allocation means fixed node pools
and worse cache behavior for zero benefit at these capacities.

### 4.2 WAL — append-only framed log

Record layout (all little-endian):

```
magic: u16 = 0x6C73 ("ls") | len: u32 | seq: u64 | op: u8 (1=put, 2=del)
| key_len: u16 | val_len: u16 | key: [u8; key_len] | val: [u8; val_len] | crc32: u32
```

- `crc32` covers everything after `magic`, computed with a hand-rolled
  IEEE CRC32 (bitwise, no table — 256-entry table is also fine, it's `const`).
- The WAL writer owns a caller-provided `[u8; BLOCK]` staging buffer and
  appends whole blocks through the `BlockDevice` trait. `async commit()` pads
  and flushes the partial block.
- Recovery: scan records in order, verify CRC, replay into a fresh memtable.
  Stop at the first corrupt/truncated record — that is the torn tail, and
  everything before it is a prefix (the WAL prefix property to be modeled in
  Verus). A torn tail is not an error; it is the expected crash boundary.

### 4.3 BlockDevice trait — the only I/O boundary (async from the start)

```rust
use core::task::{Context, Poll};

pub trait BlockDevice {
    type Error;
    const BLOCK: usize; // e.g. 4096; must be >= 512
    fn poll_read_block(&self, cx: &mut Context<'_>, id: u64, buf: &mut [u8])
        -> Poll<Result<(), Self::Error>>;
    fn poll_write_block(&mut self, cx: &mut Context<'_>, id: u64, buf: &[u8])
        -> Poll<Result<(), Self::Error>>;
    fn poll_flush(&mut self, cx: &mut Context<'_>)
        -> Poll<Result<(), Self::Error>>;
}
```

- Poll-based, not `async fn` in the trait: keeps the trait usable without an
  executor and without unstable features, and every host (VesperOS driver,
  macOS `std::fs` shim, ESP32-S3 SPI driver) can implement it by hand.
  Callers bridge with `core::future::poll_fn` — still `core`-only.
- All I/O is whole blocks; `buf.len() == Self::BLOCK` is a precondition
  (debug-checked, release returns `Error::BadBufferLen`).
- The `Db` API (§5) exposes `async fn`s; an `async fn` is a compiler-generated
  stack state machine and allocates nothing. The host drives the future with
  whatever executor it has (or manual `poll`); this crate ships no executor.

### 4.4 SSTable — immutable sorted run of fixed blocks

```
[data block]* [bloom block] [index block] [footer block]
```

- Data block (one `BLOCK`): entries sorted, each
  `key_len u16 | val_len u16 | seq u64 | op u8 | key | val`, with restart
  points every 16 entries (array of `u16` offsets at the block tail) so a
  block is binary-searchable without scanning.
- Bloom block: fixed `BLOOM_BITS` bit array + `k` stored in the footer.
  Hash is hand-rolled: `splitmix64` over an Fx-style fold of the key bytes;
  double hashing gives the k probes. No dependency, ~20 lines.
- Index block: one entry per data block —
  `first_key_len u16 | first_key | block_id u64 | max_seq u64`.
- Footer block: `magic u64 = 0x6C736D7461626C65 ("lsmtable")`
  `| index_block u64 | bloom_block u64 | entry_count u64 | k u8 | crc32 u32`.
- Every block carries a trailing CRC32 of its payload; a failed CRC means
  "treat table as absent past this point," never silent corruption.

### 4.5 Manifest — the crash-safe root pointer

```rust
pub struct Manifest<const LEVELS: usize, const TABLES: usize> {
    seq: u64,                        // manifest sequence, odd/even slots
    levels: [Level; LEVELS],          // LEVELS=7 default
    wal_head: u64,                   // first WAL block still needed
    next_table_id: u32,
}
struct Level { tables: [TableRef; TABLES], len: usize }
pub struct TableRef {
    id: u32, first_block: u64, block_count: u32,
    first_key: KeyBound<KEY_MAX>, last_key: KeyBound<KEY_MAX>,
    max_seq: u64, entry_count: u32,
}
```

- Stored double-buffered in two fixed block slots: write slot `seq % 2` with
  CRC, then `flush()`. Recovery picks the higher valid CRC slot. This is the
  atomic commit point for flush and compaction.
- Update protocol (flush): write all SSTable blocks → `flush()` → write
  manifest slot → `flush()` → WAL blocks at/before `wal_head` are now free.
  Crash anywhere before the manifest write = old state intact; crash after =
  new state intact. No in-between.

### 4.6 Compaction — leveled, bounded, caller-driven

- L0 holds ≤ 4 overlapping tables (flush outputs). When full, compact L0 → L1.
- L1+ are non-overlapping sorted runs; level `n+1` target size is 10x level
  `n`. Pick the table overlapping the smallest L0 key span (standard).
- Merge is k-way (`KMAX = 8`) over per-table block iterators, with the merge
  heap as a fixed `[HeapItem; KMAX]` array in a caller-provided scratch
  buffer — no allocation, no recursion.
- Output goes to new SSTable blocks streamed through a second scratch block.
  One `compact_step(&mut scratch) -> Result<Progress, Error>` call does
  bounded work (one input batch); the caller drives it to completion, so
  firmware can interleave compaction with real-time work. `Progress::{Done,
  More}` tells the caller.
- Tombstone drop rule: a tombstone may be dropped only when the merge output
  goes to the bottommost level containing that key's range. Enforced by
  checking the manifest's level key ranges before dropping.
- Crash during compaction is harmless: inputs are untouched until the new
  manifest slot commits; partial outputs are orphaned blocks, reclaimed by
  the next open's garbage sweep (any block not referenced by the manifest or
  the WAL range is free).

### 4.7 Read path

`get(key, val_buf) -> Result<Option<usize>, Error>`:

1. MemTable lookup (binary search). Newest seq wins; tombstone = `Ok(None)`.
2. L0, newest table first, bloom-gated, then block index → data block.
3. L1..Ln in order, key-range pruned, bloom-gated.
4. First hit at the highest seq wins; older versions and tombstones below it
   are invisible. Copies value bytes into the caller buffer; `Error::BufferTooSmall`
   if it doesn't fit (returns required length — no hidden truncation, ever).

`scan` (v0.5): a borrowed merge iterator over one block-iterator per level,
all state in `Scan<const K: usize>` owned by the caller. Prefix scans via
seek.

## 5. Public API (v0.1 surface; v0.2+ additive)

```rust
pub struct Db<D: BlockDevice, const KEY_MAX: usize, const VAL_MAX: usize,
              const CAP: usize, const ARENA: usize>;

impl<D: BlockDevice, ...> Db<D, ...> {
    pub const fn new(device: D, config: Config) -> Self;
    pub async fn open(&mut self) -> Result<OpenReport, Error<D::Error>>;
    pub async fn put(&mut self, key: &[u8], val: &[u8]) -> Result<u64, Error<D::Error>>; // -> seq
    pub async fn delete(&mut self, key: &[u8]) -> Result<u64, Error<D::Error>>;
    pub async fn get(&self, key: &[u8], val_buf: &mut [u8]) -> Result<Option<usize>, Error<D::Error>>;
    pub async fn flush(&mut self) -> Result<(), Error<D::Error>>;
    pub async fn compact_step(&mut self, scratch: &mut [u8]) -> Result<Progress, Error<D::Error>>;
}
```

`Config` is a plain struct of const-compatible tunables (block id ranges for
WAL/table/manifest regions, or a `BlockAllocator` policy — see open questions).

## 6. Error model

```rust
pub enum Error<E> {
    KeyTooLarge { len: usize, max: usize },
    ValueTooLarge { len: usize, max: usize },
    EmptyKey,
    TableFull,          // memtable slots exhausted — caller should flush()
    ArenaFull,          // memtable bytes exhausted — caller should flush()
    BufferTooSmall { need: usize },
    BadBufferLen,
    CorruptBlock { id: u64 },
    CorruptWal { offset: u64 },
    CorruptManifest,
    NoSpace,            // block allocator exhausted
    Device(E),          // underlying BlockDevice error, passed through
}
```

No strings, no allocation, `core::fmt::Debug` derivable. Every variant is
returned, never panicked.

## 7. Block allocation (open design point)

v0.1: simplest thing that works — three fixed regions from `Config`
(wal: `[wal_start, wal_end)`, tables: `[tbl_start, tbl_end)`,
manifest: two fixed block ids). A bump pointer per region; the open-time
garbage sweep rebuilds "free" as "not referenced."

v0.3: true free-list reuse. Each region gets a bump pointer plus a
`FreeList<CAP>` (sorted, `no_std`, no allocation). Allocation is
free-list-first (first-fit contiguous run), then the bump; the run is only
*reserved* until the manifest commit lands, then claimed — a failed flush
changes nothing. The open-time sweep reclaims unreferenced blocks below the
bump's resume point into the free list. Compaction (v0.4) will free whole
input tables into it. The WAL wraps instead of exhausting: the flush that
fills the region restarts `wal_head` at `wal_start` in the same atomic
manifest commit, and recovery skips stale pre-wrap blocks via a sequence
floor (`seq <= manifest.max_seq`).

## 8. Testing strategy

- Unit: memtable ordering/supersede, binary search edges, CRC32 vectors,
  bloom false-positive rate vs theory, SSTable encode/decode round-trip,
  manifest slot failover.
- Oracle: scripted op sequences against a `std::collections::BTreeMap`
  model (`std` allowed in tests, never in the library).
- Crash injector (Waymaker-style): enumerate every WAL/block write as a
  crash point over small op scripts; assert the recovered DB equals the
  prefix-oracle state. Exhaustive over writes, not sampled.
- `cargo miri` over the test suite; `unsafe` must stay at zero.
- Fuzz the WAL/SSTable decoders with arbitrary bytes (structure-aware,
  in-tree, no dependency).

## 9. Milestones

- v0.1 — MemTable + WAL + recovery. In-memory `BlockDevice` in tests.
  Gate: crash-injector green over 3-op scripts.
- ~~v0.2 — SSTable writer/reader + `flush()` + manifest commit protocol.~~
  DONE 2026-09-13 (commit 963f5f1): plus bump-pointer block allocation and
  L0-newest-first point reads in `get()`. 61 tests green, clippy
  pedantic+nursery clean, fmt clean.
- ~~v0.3 — Full read path (levels, bloom, ranges) + block allocator sweep.~~
  DONE 2026-09-13: `get()` scans memtable → L0 newest-first → deeper levels
  with key-range pruning, bloom gating, and sequence pruning; entry seqs are
  threaded through the SSTable lookup so highest-seq wins across levels and
  the highest-seq tombstone hides older values. `FreeList` with first-fit
  run allocation (free-list-first, claim-after-commit); open-time sweep
  reclaims orphaned table blocks. WAL wraps atomically with the filling
  flush; recovery skips stale pre-wrap blocks via a sequence floor. 74 tests
  green (13 new: 6 read-path, 5 free-list, WAL wrap, orphan reclaim),
  clippy pedantic+nursery clean, fmt clean. v0.4+ exclusions held: no
  compaction, no scan/snapshots.
- v0.4 — Leveled `compact_step` with bounded work + tombstone rule.
- v0.5 — `scan` iterator + snapshot reads + Verus models for the two
  core invariants.
- v0.6 (roadmap) — ESP32-S3 / Tallow port: SPI-flash `BlockDevice`,
  budget re-tune.

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
