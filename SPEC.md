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
  Permanent per-`Db` RAM beyond the memtable: one `BLOCK`-byte `get` scratch
  buffer (reused across calls instead of a per-call stack buffer) — count it
  against this budget. The v0.4 `Compaction` scratch is caller-owned, not
  `Db` RAM.

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

- Insert: binary search `slots[..len]` by `(key, seq)`; every mutation —
  including a same-key update — allocates one fresh slot and bumps the
  arena. There are no superseded or dead slots: the memtable keeps the
  key's complete version chain, newest-first within the key, because
  snapshots (v0.5) can observe any older version. A full table rejects
  same-key updates — `Error::TableFull` (slots) or `Error::ArenaFull`
  (bytes); the caller flushes on either. Full-version retention costs
  nothing extra at flush: the writer already walks `slots[..len]`.
- New key bytes are bump-appended to `arena`; the slot is `copy_within`-shifted
  into sorted position. O(n) insert, O(log n) lookup; `CAP = 4096` keeps the
  shift cost trivial against flash write costs.

Why not a skiplist: pointer chasing without allocation means fixed node pools
and worse cache behavior for zero benefit at these capacities.

### 4.2 WAL — append-only framed log

Record layout (all little-endian):

```
magic: u16 = 0x6C73 ("ls") | len: u32 | seq: u64 | op: u8 (1=put, 2=del)
| key_len: u16 | val_len: u16 | key: [u8; key_len] | val: [u8; val_len] | crc32: u32
```

- `crc32` covers everything after `magic`, computed with a hand-rolled
  IEEE CRC32, slicing-by-8 over eight 256-entry `const` tables (8 KiB
  `.rodata`; the tables are shared, not per-`Db` RAM). `#[inline(always)]`
  is measured with callgrind, not decorative — see `src/crc.rs`.
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
- **On-disk format policy (pre-1.0): no compatibility across minor
  versions.** The manifest magic changes whenever the layout changes
  (`hrtman01` → `hrtman02` in v0.4.1, when key bounds went from fixed
  `KEY_MAX` arrays to length-prefixed bytes). A foreign magic decodes as
  `CorruptManifest` — old bytes are rejected, never misparsed. WAL and
  SSTable formats carry their own magics under the same rule.

### 4.6 Compaction — leveled, bounded, caller-driven

```rust
db.compact_step(&mut scratch) -> Result<Progress, Error>
```

- `scratch` is a typed caller-owned `Compaction<BLOCK, KEY_MAX, VAL_MAX,
  BLOOM_BYTES>` (a `Compaction::new()` value, typically a static or a
  stack local the caller keeps across calls) — not a raw byte buffer, and
  never stored inside `Db`. It persists across `Progress::More` calls; it
  is droppable mid-job, because partial output is invisible until the
  manifest commit.
- **v0.4 scope: L0→L1 only.** When L0 is full (4 tables), one job compacts
  all of L0 plus every L1 table overlapping the L0 key span into one new
  L1 run. A full L1 (or more than `KMAX = 8` inputs) fails the job with
  `Error::NoSpace` and leaves the manifest untouched; cascading L1→L2 and
  deeper is deferred to a later version.
- Merge is k-way (`KMAX = 8`) over per-table block cursors. Winner selection
  is a linear scan over the ≤ 8 live cursors (minimum key; highest sequence
  wins ties) — no heap, hence no stale entries for exhausted cursors. The
  winner's entry is copied into the output *before* any cursor advances.
- Cursors treat the writer's zero padding (between the last entry and the
  restart trailer) as end-of-entries in the block, the same convention as
  the v0.3 point-lookup scan; anything else unparseable is `CorruptBlock`.
- Output streams through an incremental `TableWriter`: one
  `compact_step` call seals at most one output block (`Progress::More`),
  so firmware can interleave compaction with real-time work. The call that
  exhausts the merge seals the table and commits the manifest atomically
  (`Progress::Done`).
- Tombstone drop rule (v0.5): a bottommost tombstone may be dropped only
  when it predates every live snapshot. The merge keeps, per key, the
  **keep-set**: the live view's newest version plus the newest version at
  or below each live snapshot's watermark (threshold `u64::MAX` for the
  live view, then the snapshot watermarks sorted descending; each
  threshold is served by the first version at or below it, so at most
  1 + 8 versions survive per key). A bottommost newest tombstone with
  `seq < oldest_live_snapshot` (no live snapshots → `u64::MAX`, i.e. the
  v0.4 behavior) drops the whole key: in that case every reader — live
  and snapshot — would observe the tombstone, so deletion is
  observationally identical to absence. Dropping a tombstone a live
  snapshot could still observe would resurrect the shadowed value. The
  watermarks are copied at `compact_select`; a snapshot taken
  mid-compaction always has `seq >=` every version being compacted (seqs
  are monotonic), so it can never need older history the merge already
  discarded. Executable model: `model_keep_set` in `src/model.rs`,
  differentially tested against the real merge in `tests/compact.rs`.
- Crash during compaction is harmless: inputs are untouched until the new
  manifest slot commits; partial outputs are orphaned blocks, reclaimed by
  the next open's garbage sweep (any block not referenced by the manifest or
  the WAL range is free). Crash-injector tested: the post-crash state is
  always exactly pre- or post-compaction, never mixed.
- **Reclamation (v0.4.1):** strictly *after* the manifest commit — the
  visibility point — the input tables' block runs are returned to the free
  list, so the live session reuses them immediately (the all-tombstone case
  with no output table reclaims its inputs too). Reclamation is best-effort:
  the commit already happened, so a full free list must not fail the
  compaction; un-reclaimed blocks stay orphans and the next open's sweep
  reclaims them.

### 4.7 Read path

`get(key, val_buf) -> Result<Option<usize>, Error>` is `get_at(key, val_buf,
u64::MAX)`:

1. MemTable lookup (binary search over version chains). The newest version
   with `seq <= max_seq` wins; a tombstone there is `Ok(None)`.
2. L0, newest table first, bloom-gated, then block index → data block;
   the key's version run is scanned newest-first for the first version at
   or below `max_seq`. A run can straddle data blocks (many versions of
   one key): the index resolves to the run's first block and the lookup
   walks forward across contiguous data blocks while the run continues,
   bounded by the table's data-block count so it never strays into the
   bloom/index/footer blocks.
3. L1..Ln in order, key-range pruned, bloom-gated.
4. First hit wins; older versions and tombstones below the visible version
   are invisible. Copies value bytes into the caller buffer;
   `Error::BufferTooSmall` if it doesn't fit (returns required length —
   no hidden truncation, ever).

`scan` (v0.5): a borrowed merge iterator over one cursor per table plus the
memtable, all state in caller-owned `Scan<'d, ...>` (it borrows `&'d Db`,
so the compiler — not documentation — forbids `put`/`flush`/`compact`
mid-scan). One shared `[u8; BLOCK]` buffer; each cursor caches only its
head key (`[u8; KEY_MAX]`), seq, tombstone flag, and value length. Winner
selection mirrors the compaction merge: minimum key across live cursors,
highest seq wins ties, tombstone winners are skipped silently, and each
source is advanced past every version of the yielded key so an older
version is never yielded as a duplicate. Key/value bytes are copied into
caller buffers (`BufferTooSmall { need }` is returned *before* any cursor
advances, so a retry with a larger buffer sees the same entry — never
silent truncation, never a skipped entry). Prefix scans via
`seek(prefix, Some(prefix_end))`.

Snapshot reads (v0.5): `snapshot() -> Result<u64, Error>` registers the
current `next_seq` as a live snapshot (bounded: 8 live snapshots,
`NoSpace` when exhausted) and `release_snapshot(u64)` unregisters it.
`get_at(key, val_buf, max_seq)` and `Scan::seek(..., max_seq)` expose only
mutations with `seq <= max_seq`. The memtable and every SSTable keep full
version chains so older snapshots keep seeing their values; compaction's
per-key keep-set (§4.6) is what bounds the retention. Executable models:
`model_winner` / `model_visible` / `model_keep_set` in `src/model.rs`,
property- and differentially-tested against the read path and the merge.

## 5. Public API (v0.1 surface; v0.2+ additive)

```rust
pub struct Db<D: BlockDevice, const BLOCK: usize, const KEY_MAX: usize,
              const VAL_MAX: usize, const CAP: usize, const ARENA: usize,
              const LEVELS: usize, const TABLES: usize,
              const BLOOM_BYTES: usize, const FREELIST: usize>;

impl<D: BlockDevice, ...> Db<D, ...> {
    pub const fn new(device: D, config: Config) -> Self;
    pub async fn open(&mut self) -> Result<OpenReport, Error<D::Error>>;
    pub async fn put(&mut self, key: &[u8], val: &[u8]) -> Result<u64, Error<D::Error>>; // -> seq
    pub async fn delete(&mut self, key: &[u8]) -> Result<u64, Error<D::Error>>;
    pub async fn get(&self, key: &[u8], val_buf: &mut [u8]) -> Result<Option<usize>, Error<D::Error>>;
    pub async fn flush(&mut self) -> Result<(), Error<D::Error>>;
    pub async fn compact_step(&mut self, scratch: &mut Compaction<BLOCK, KEY_MAX, VAL_MAX, BLOOM_BYTES>)
        -> Result<Progress, Error<D::Error>>;
    // v0.5 additions:
    pub async fn get_at(&self, key: &[u8], val_buf: &mut [u8], max_seq: u64)
        -> Result<Option<usize>, Error<D::Error>>; // get() is get_at(.., u64::MAX)
    pub const fn snapshot(&mut self) -> Result<u64, Error<D::Error>>; // -> watermark (max 8 live)
    pub const fn release_snapshot(&mut self, snap: u64); // unknown watermark is a no-op
}

pub struct Scan<'d, D: BlockDevice, ...>; // borrows &'d Db; caller-owned state
impl Scan<'_, ...> {
    pub const fn new(db: &Db<...>) -> Self; // unpositioned; seek() before next()
    pub async fn seek(&mut self, start: &[u8], end: Option<&[u8]>, max_seq: u64)
        -> Result<(), Error<D::Error>>; // empty start scans from first key; end exclusive
    pub async fn next(&mut self, key_buf: &mut [u8], val_buf: &mut [u8])
        -> Result<Option<(usize, usize)>, Error<D::Error>>; // BufferTooSmall never consumes an entry
}

impl<D: BlockDevice, ...> TableReader<D, ...> {
    pub async fn get(&self, scratch: &mut [u8; BLOCK], key: &[u8], val_buf: &mut [u8])
        -> Result<Option<usize>, Error<D::Error>>;
    pub async fn get_at(&self, scratch: &mut [u8; BLOCK], key: &[u8], val_buf: &mut [u8],
                        max_seq: u64) -> Result<Option<usize>, Error<D::Error>>;
    pub async fn lookup(&self, scratch: &mut [u8; BLOCK], key: &[u8], val_buf: &mut [u8])
        -> Result<Lookup, Error<D::Error>>; // distinguishes tombstones
    pub async fn lookup_at(&self, scratch: &mut [u8; BLOCK], key: &[u8], val_buf: &mut [u8],
                           max_seq: u64) -> Result<Lookup, Error<D::Error>>;
}
```

`get` is `get_at(.., u64::MAX)` and `lookup` is `lookup_at(.., u64::MAX)` —
v0.5 added the `_at` snapshot variants without changing the existing
signatures. `snapshot()` registers the current `next_seq` as a live
watermark (at most 8; `NoSpace` beyond that); `release_snapshot` frees it.
`get_at`/`seek(.., max_seq)` observe only versions with `seq <= max_seq` —
pass a live snapshot's watermark for a pinned read. Unregistered watermarks
are best-effort: compaction retains exactly the live view plus the newest
version at or below each *live* snapshot's watermark (§4.6), so a watermark
below the oldest live snapshot may observe dropped history as absent.

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
- ~~v0.4 — Leveled `compact_step` with bounded work + tombstone rule.~~
  DONE 2026-09-14: `Db::compact_step(&mut Compaction) -> Result<Progress,
  Error>` with caller-owned typed scratch; L0→L1 only (cascade deferred);
  linear-scan k-way merge (KMAX = 8), highest-seq-wins dedup, tombstone drop
  only when no level ≥ 2 overlaps the output span; one output block per call
  (`Progress::{Done, More}`); crash-injector green (state exactly pre- or
  post-compaction, never mixed). Also fixed a v0.3 manifest-encoding bug the
  new tests exposed: key bounds were written as full 256-byte arrays
  (544 bytes/table, overflowing the 4 KiB manifest block at 8 tables) and are
  now length-prefixed variable-length. 87 tests green (9 new compaction
  tests + merged perf work: slicing-by-8 CRC, `get` scratch reuse, release
  free-list claim fix, word-chunked zero scans, callgrind bench harnesses),
  clippy pedantic+nursery clean, fmt clean.
- v0.4.1 — Compaction input-run reclamation + on-disk format policy.
  Reclaim input block runs into the free list strictly after the manifest
  commit (best-effort: a full free list can't fail the already-committed
  job; orphans are swept by the next open). Manifest magic `hrtman01` →
  `hrtman02` for the v0.4 key-bound encoding change; pre-1.0 policy is no
  format compat across minor versions — a foreign magic is
  `CorruptManifest`, never a misparse.
- v0.5 — `scan` iterator + snapshot reads + executable models of the core
  invariants (seq-ordered visibility, per-key keep-set) with property and
  differential tests. Machine-checked proofs are deferred: the Verus
  toolchain is not installed on the dev host (confirmed 2026-09-14), so
  the models are pure total `no_alloc` functions written to lift into
  Verus `spec` fns later. Full version chains are retained in the memtable
  and SSTables; compaction emits the per-key keep-set (live view plus the
  newest version at or below each live snapshot watermark) and drops a
  bottommost newest tombstone only when it predates every live snapshot.
- v0.6 — ESP32-S3 / Tallow port.
  - **Target build gate**: the library builds for
    `xtensa-esp32s3-none-elf` with the ESP Rust fork (`-Z build-std=core`);
    `xtensa-check.sh` reproduces it. The crate is `#![no_std]` +
    `#![forbid(unsafe_code)]` + core-only, so the port needs no `unsafe`
    and no new dependencies.
  - **On-target smoke proof**: `xtensa-smoke/` is a bare-metal xtensa
    binary (own reset vector, linker script, UART0 console, panic
    handler) that boots under `qemu-system-xtensa -machine esp32s3`,
    drives a `Db` through put/get/delete/flush/compact/scan/snapshot
    against a RAM-backed `BlockDevice`, and prints `SMOKE PASS`/`FAIL`
    over UART0. `run-smoke.sh` builds the flash image and runs QEMU.
    This is the "it runs on the chip" proof — not just "it compiles".
  - **SPI-flash `BlockDevice`**: `src/flash.rs` defines the `Flash`
    trait (sector erase + program + read — the only `unsafe`-needing
    half, implemented per-board) and `FlashBlockDevice<F>`, the safe
    erase-aware `BlockDevice` wrapper. Horton block writes are
    whole-sector (BLOCK = 4096 = flash sector size), so a write is
    erase-sector then program — no read-modify-write, no hidden RAM.
    The ESP32-S3 SPI register implementation is v0.7's
    `src/esp32s3.rs` (below); on-silicon verification of the command
    sequences still needs real hardware.
    The wrapper's erase discipline is property-tested on host against a
    strict mock flash (erase sets 0xFF, program only clears bits,
    programming an unerased sector is an error).
  - **Budget re-tune**: the `ESP32S3` const profile
    (`BLOCK=4096, KEY_MAX=32, VAL_MAX=64, CAP=16, ARENA=2048, ...`)
    sizes a database at well under 64 KiB of RAM; `BUDGET.md` shows the
    accounting and the profile carries compile-time size assertions.
  - Honest limits: QEMU proves logic on the target ISA, not flash
    programming or timing; the SPI MMIO primitive and power-loss
    behavior need hardware. Throughput numbers, if any, are measured —
    never estimated.
- v0.7 — Real ESP32-S3 SPI flash driver (`src/esp32s3.rs`).
  - **Scope**: the `Flash` trait's ESP32-S3 implementation — the half
    v0.6 explicitly deferred as "Tallow driver work". It lives in this
    crate (not Tallow) because it is Horton's board seam, and it stays
    `#![no_std]` + `#![no_alloc]` + core-only like everything else.
  - **Design**: `SpiFlash<B: RegBus>` is generic over a tiny register
    bus trait (`read`/`write`/`read_word` at SPI1 offsets). All of the
    driver's logic is safe (`#![forbid(unsafe_code)]` holds for the
    whole crate): the volatile-MMIO adapter — the half-dozen lines that
    actually touch `0x6000_2000` — lives in the board/Tallow crate,
    which owns the `unsafe`. Host tests plug in a mock bus backed by
    an emulated NOR chip.
  - **Register sequences** follow ESP-IDF v5.2's LL layer verbatim
    (`components/hal/esp32s3/include/hal/spimem_flash_ll.h`,
    `spi_flash_hal_iram.c`), not the TRM prose:
    - `erase_sector`: WREN → ADDR → save CTRL / CTRL=0 → dedicated SE
      bit → spin on CMD-bit clear → restore CTRL → RDSR, poll WIP with
      a bounded spin cap (`Error::Timeout`).
    - `program`: WREN, then per chunk (≤ 64 bytes — the W0–W15 buffer —
      and never crossing a 256-byte page): ADDR = `addr | (len << 24)`,
      words into W0.., `usr_dummy = 0`, dedicated PP bit → spin →
      WIP poll. Chunking matches ESP-IDF's `set_buffer_data`.
    - `read`: user-mode `0x03` transactions (CMD 8b, ADDR 24b, MISO),
      ≤ 64 bytes each, straight from the chip. This deliberately
      bypasses the DROM flash cache: a cached read path would need a
      cache invalidate after every program/erase (ROM
      `Cache_Invalidate_Addr`, unverifiable in this environment and
      fragile to link by hand), while user-mode reads are
      correct-by-construction and fully provable in the host mock.
      Cost is bounded and measurable on silicon later; the cached-read
      optimization is future work, not a v0.7 claim.
    - Every WREN is followed by a WEL check via RDSR
      (`Error::WriteEnableFailed`); every poll is bounded.
  - **Proof** (this is the leg that changed vs the plan): a bare-metal
    probe run 2026-09-15 showed QEMU's `esp32s3` machine does **not**
    emulate the SPI_MEM user-command path — DROM CPU reads return
    `0xDEADBEEF` regardless of flash contents, CMD bits clear
    instantly with no observable effect, and MISO reads return `0xFF`
    for regions known to hold other bytes. So the proof is:
    (1) host tests against the mock NOR — exact register-write
    sequences asserted, WEL/WIP semantics emulated, plus a full `Db`
    on `FlashBlockDevice<SpiFlash<MockBus>>` doing puts/gets/flush/
    overwrite/reopen; (2) the Xtensa build gate — the driver builds
    for `xtensa-esp32s3-none-elf`; (3) QEMU runs the probe without bus
    faults (register addresses are at least mapped), with the
    non-emulation recorded here, not hidden.
  - Honest limits: on-silicon verification is still pending — actual
    command timing, WIP behavior, and power-loss during program/erase
    cannot be proven without hardware. No timing or power-loss claims
    are made.

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
