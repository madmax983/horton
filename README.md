# horton

A log-structured merge-tree key-value store for places where `malloc`
doesn't exist: firmware, kernels, bootloaders.

- **`#![no_std]`, no `alloc`, zero dependencies.** The `[dependencies]` table
  is empty; the crate uses nothing but `core`.
- **`#![forbid(unsafe_code)]`.** The ESP32-S3 flash driver's register logic
  is safe too. The volatile MMIO half lives in the board crate.
- **Every byte is caller-owned and sized at compile time.** The database, a
  scan, and the compaction scratch are fixed-size values sized by const
  generics. Put them in `static`s and you know your RAM bill before you
  flash the board.
- **No panics in library code.** Every failure is an [`Error`](src/error.rs)
  variant, including `BufferTooSmall { need }` instead of silent truncation.
- **Async from the bottom.** The only I/O boundary is a poll-based
  [`BlockDevice`](src/device.rs) trait. The API is `async fn`s that allocate
  nothing, and horton ships no executor: use embassy, RTIC, or a poll loop.

> **Status: pre-1.0 (v0.17), experimental.** The on-disk format changes
> between minor versions, and old images are rejected rather than misread.
> v0.17 fixes every finding of [the architecture review](docs/ARCHITECTURE_REVIEW.md)
> (each has a regression test in `tests/review_findings.rs`). It is not yet
> verified on ESP32-S3 silicon.

## Features

| Area | What you get |
|---|---|
| Writes | `put`, `delete`, `delete_range` (range tombstones), `put_with_ttl` (caller-clock expiry), atomic multi-op `WriteBatch` (up to one WAL block; also the group-commit path) |
| Reads | `get`, snapshot reads (`snapshot` / `get_at`), forward `Scan` and reverse `RevScan` merge iterators with prefix/range bounds |
| Durability | WAL-first. Every mutation's CRC-framed record is on the device before the call returns. The manifest (two copies, or a ring of `n` for flash endurance) is the single atomic commit point for flushes, compactions and archive operations |
| Tables | Immutable SSTables with restart-point data blocks, a per-table bloom filter, one index block, a range-tombstone section, and optional hand-rolled LZ77 block compression |
| Compaction | Leveled and caller-driven: `compact_step` does bounded work per call, so firmware can interleave it with real-time work. Outputs split at slot size and commit one by one; trivial moves, small-table consolidation, and region-pressure push-down keep the region usable until it is mostly full. Snapshot-aware version retention; point and range deletes give their space back |
| Caching | Optional fixed-slot CLOCK block cache inside `Db` (`CACHE = 0` turns it off) |
| Cold storage | Archive API: seal a table, stream its blocks to your own remote sink, then forget it locally (`archive_plan` / `archive_commit`). `ingest_table` re-attaches it later. The archive commit refuses any removal that would bring deleted data back |
| Hardware | `FlashBlockDevice` (erase-aware NOR adapter) and an ESP32-S3 SPI1 flash driver over a pluggable register bus |

## Quick start

horton has no dependencies and is not on crates.io yet. Use a path or git
dependency:

```toml
[dependencies]
horton = { git = "https://github.com/madmax983/horton" }
```

Implement `BlockDevice` for your storage, choose the sizes, and drive the
futures with your executor. Condensed from
[`examples/quickstart.rs`](examples/quickstart.rs), which is complete and
runs with `cargo run --example quickstart`:

```rust
use horton::{BlockDevice, Config, Progress, Scan};

// The sizes horton compiles in. It allocates nothing, so every buffer is
// a fixed size chosen here. Only these three are required; the tuning
// knobs have defaults (see "Sizing" below for what each one means).
horton::db_types! {
    block: 4096,  // bytes per device block (your flash sector size)
    key_max: 64,  // longest key you will store, in bytes
    val_max: 256; // longest value you will store, in bytes
    type Db = MyDb;
    type Compaction = MyCompaction;
}

// Tell horton how many blocks the device has; it decides where the
// manifest, the WAL and the tables go.
let config = Config::whole_device(512);
let mut db = MyDb::new(RamDisk::new(512), config); // `const fn`: fine in a `static`
db.open().await?; // recover manifest, rebuild table slots, replay WAL

db.put(b"sensor/001", b"21.5C").await?; // durable when this returns
db.delete(b"sensor/002").await?;

let mut val = [0u8; 256];
if let Some(n) = db.get(b"sensor/001", &mut val).await? {
    // &val[..n] == b"21.5C"
}

db.flush().await?; // memtable -> immutable SSTable (atomic manifest commit)

let mut scan = Scan::new(&db); // borrows `db`: no writes while it lives
scan.seek(b"sensor/", Some(b"sensor0"), u64::MAX).await?;
let mut key = [0u8; 64];
while let Some((kl, vl)) = scan.next(&mut key, &mut val).await? {
    // (&key[..kl], &val[..vl])
}

let mut scratch = MyCompaction::new(); // caller-owned
while db.compaction_pending() {
    while db.compact_step(&mut scratch).await? == Progress::More {}
}
```

Capacity errors name their remedy:

| Error | Meaning | Do this |
|---|---|---|
| `TableFull`, `ArenaFull` | the memtable is full | `flush()`, retry |
| `WalFull` | the WAL region is exhausted | `flush()` (it wraps the WAL), retry |
| `NeedsCompaction` | level 0 is full, or no slot is free until tables merge | `compact_step` until `Done`, retry |
| `RegionFull` | no slot is free and compaction cannot free one | delete and compact, archive, or grow the region |
| `SnapshotLimit` | eight snapshots are live | release one |
| `TableTooLarge` | an entry, index, or range-tombstone section does not fit | smaller entries, bigger blocks or slots |
| `BatchTooLarge` | a batch does not fit one WAL block | split it |
| `Busy` | another `get` on this handle holds the read buffers | finish it, retry |
| `BadConfig` | regions overlap or are too small, or the device is below `MIN_DEVICE_BLOCKS` (from `open`) | fix the `Config`, or use a bigger device |

`NeedsCompaction` is only returned while `compaction_pending()` is true, so
a compact-then-retry loop always makes progress.

## Demo: a flight recorder that survives power cuts

[`examples/flight_recorder`](examples/flight_recorder) is a sensor logger
built the way firmware would use horton, and it exercises the whole API.
It runs on a simulated NOR flash chip that counts erases and can lose power
in the middle of any erase or program. Each tick it commits a four-sensor
frame as one `WriteBatch`, writes a debug trace that expires, and runs one
bounded compaction step. Every 1000 ticks it purges a glitch window with a
range delete, checking that a snapshot taken before the purge still sees
it. Tables older than the hot window stream to object storage and are
committed away locally.

```sh
cargo run --release --example flight_recorder                  # record; Ctrl-C or kill -9 any time, run again
cargo run --release --example flight_recorder -- torture 1000  # 1000 power cuts, each tearing a write
cargo run --release --example flight_recorder -- restore       # rebuild the history from the archive
```

`record` keeps its flash image and archive under `target/flight_recorder/`.
Kill it whenever you like; the next run recovers, proves every
acknowledged frame is still there (on flash or in the archive), and
resumes. `torture` does the same after every simulated power cut:

```text
power cuts             1000, each tearing an erase or program partway
acknowledged frames    144494 ticks, every one present (on flash or in the archive)
frames cut mid-write   716 landed whole, the rest not at all; never in part
lost or corrupt        0
snapshots, TTLs        139 snapshot and 2864 TTL checks passed
archive                212 tables, 21340 KiB, each re-ingested and checked
```

To archive to S3 or any S3-compatible store (MinIO, Cloudflare R2,
LocalStack), add `--s3 s3://bucket/prefix`. The example signs requests
with `curl --aws-sigv4`, so it needs no extra crates; it reads
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` and, for a
non-AWS endpoint, `AWS_ENDPOINT_URL`. `restore --s3 …` rebuilds the
history from the bucket on a "ground station".

This demo found two WAL bugs, F18 and F19 in the
[review status](docs/ARCHITECTURE_REVIEW.md): a torn block could replay
part of a batch, and writes after a reopen could be lost behind a torn
block. Both are fixed.

## Architecture

```mermaid
flowchart LR
    subgraph RAM["RAM (caller-owned, fixed size)"]
        MT["MemTable<br/>sorted slots + bump arena"]
        WS["WAL stage<br/>1 block"]
        MF["Manifest<br/>levels → TableRefs"]
        BC["BlockCache<br/>CLOCK, CACHE slots"]
    end
    subgraph DEV["BlockDevice"]
        MS["Manifest copies<br/>pair or ring"]
        WAL["WAL region<br/>CRC-framed records"]
        TBL["Table region<br/>SSTables"]
    end
    W(["put / delete / write"]) --> WS -->|"commit: 1 block write + flush"| WAL
    W --> MT
    MT -->|"flush()"| TBL
    TBL -->|"compact_step()"| TBL
    MF -->|"commit = visibility point"| MS
    R(["get / Scan / RevScan"]) --> MT
    R --> BC --> TBL
```

**Write path.** A mutation is encoded into the WAL stage block, written and
flushed to the device, and only then inserted into the memtable. A torn WAL
tail is the expected crash boundary, and recovery replays up to it.

**Flush.** The memtable is streamed into a new SSTable in a free *table
slot*. The table region is split into `LEVELS × TABLES` equal slots, one
table per slot, so the manifest's table capacity and the region's space
are one budget and a table can never grow into its neighbour. The slot is
claimed only when the manifest commit lands, so a failed or interrupted
flush changes nothing.

**Read path.** Reads check the memtable, then level 0 newest-first, then
deeper levels. Tables are pruned by key range and max sequence and gated by
the bloom filter. The highest sequence number wins, and range tombstones
and TTL expiry are applied afterwards.

**Compaction.** Full levels compact first, deepest first; when the region
runs short, pressure pushes tables down too. A job merges L0 (or its oldest
tables), or one table of a deeper level, with the overlapping tables below
in a linear-scan k-way merge (at most 8 cursors) that keeps the newest
version of each key plus the newest version visible to each live snapshot.
Outputs split at slot size and commit one at a time: inputs the output has
passed retire, the one it is inside is narrowed past it. Tombstones — point
and range — are dropped once no reader can see what they hide
([ADR-0010](docs/adr/0010-split-output-compaction.md)).

**Crash model.** The manifest is stored as whole copies (a pair, or a ring
of `n`), each spanning as many blocks as the worst case needs; every block
carries the commit's sequence number and CRC, and recovery takes the
newest copy whose blocks all agree. Every structural change (flush,
compaction output, archive, ingest, WAL wrap) becomes visible in exactly
one manifest commit. Blocks written before that point sit in a slot no
manifest table references, which is simply free again after `open()`
rebuilds the slot map from the manifest.

### On-device layout

```text
block ids:  [manifest copy]×2..n ... [ WAL region ) ... [ table region: LEVELS×TABLES slots )
             └ each Manifest::max_blocks  └ wal_start..wal_end   └ tbl_start..tbl_end
               blocks, magic "hrtman05"

SSTable:    [data]* [rdel]* [bloom] [index] [footer]
             │       │        │       │       └ magic "hrtsst02", ids, entry count, k, rdel count, min seq
             │       │        │       └ per data block: first key, block id, max seq
             │       │        └ BLOOM_BYTES bit array
             │       └ range tombstones sorted by start
             └ entries + restart points; optionally LZ77-compressed (flag bit in trailer)
every block ends in a CRC32 of its payload
```

The full format and protocol spec is [`SPEC.md`](SPEC.md) §4.

## Sizing

horton allocates nothing, so you choose every buffer size when you
compile, and you tell it how much of the device it may use. Both have
defaults; this section explains what they mean when you want to change
them.

### How horton stores data

New writes go to two places: a log on the device (the **WAL**, so they
survive a crash) and a sorted buffer in RAM (the **memtable**, so they can
be read back fast). When the memtable fills up, `flush()` writes it to the
device as one sorted, read-only **table**. Tables are grouped into
**levels**: new tables land in level 0, and **compaction** merges them
into deeper levels, which keeps reads fast and frees the space of
overwritten and deleted data. The **manifest** is the record of which
tables exist and where.

### The sizes in `db_types!`

| Parameter | What it is | How to choose | Default |
|---|---|---|---|
| `block` | Bytes the device reads or writes at once | Your device's block size: the erase sector on NOR flash (usually 4096), 512 on SD cards. At least 512, and `key_max + val_max + 23` must fit | required |
| `key_max` | Longest key, in bytes | Longer keys fail with `KeyTooLarge`. RAM buffers are sized for it, so do not round up much | required |
| `val_max` | Longest value, in bytes | Longer values fail with `ValueTooLarge` | required |
| `memtable_entries` | Writes held in RAM before they must be flushed. Every `put` or `delete` takes one until the next flush | More means fewer flushes and less flash wear, but more RAM. When full, writes fail with `TableFull`: call `flush()` | 64 |
| `memtable_arena` | RAM bytes for those writes' keys and values | About `memtable_entries` × your typical key + value size. When full, writes fail with `ArenaFull`: call `flush()` | 128 per entry, at least `key_max + val_max` |
| `levels` | Levels of tables on the device | 4 suits most uses | 4 |
| `tables_per_level` | Tables each level holds | Below 8. `levels × tables_per_level` (at most 64) is how many tables exist. More tables mean more data between compactions, but more tables to check per read and smaller table slots | 4 |
| `bloom_bytes` | Per-table filter that lets a read skip tables that cannot hold the key | About 1.25 bytes per key in a table gives about 1% wasted reads | 256 |
| `cache_blocks` | Recently read blocks kept in RAM, `block` bytes each | 0 turns it off. A few blocks speed up repeated reads | 0 |

Give the parameters in this order and leave out any tuning knob to keep
its default. A misspelled, repeated or misordered name does not compile,
and neither does a shape that breaks a rule above (for example an arena
smaller than one largest entry).

**What it costs in RAM:** the structs are the whole bill. Print
`size_of::<MyDb<YourDevice>>()` and the same for your `Scan` and
`Compaction` types; `cargo run --example quickstart` does. With the quick
start's sizes that is about 25 KiB for `Db`, 11 KiB for a `Scan` and
61 KiB for `Compaction` (you only need one while compacting). The
`async` calls' futures add at most about 18 KiB more (`flush()`,
`archive_commit()`).

`horton::profile` has a measured ESP32-S3 instantiation (4 KiB blocks,
32-byte keys, 64-byte values, 16-entry memtable, 4 × 4 levels, 2-slot
cache): `Db` 25,256 + `Scan` 10,536 + `Compaction` 58,840 = 94,632 bytes
of structs, plus at most 17.6 KiB of live futures (`archive_commit()`),
for a measured peak of 112,256 bytes under a 112 KiB budget asserted by
`tests/profile.rs`. See [`BUDGET.md`](BUDGET.md).

### Mapping it onto your device

horton addresses the device in blocks, numbered from 0. It needs three
regions that do not overlap:

| Region | What it holds | Size |
|---|---|---|
| Manifest | The record of which tables exist, in two copies so a crash mid-update always leaves one whole | `Manifest::max_blocks` blocks per copy (one for small shapes) |
| WAL | Every write, until the next flush | One block per write or `WriteBatch` between flushes; when it runs out, writes fail with `WalFull` (call `flush()`) |
| Tables | The flushed and compacted tables | `levels × tables_per_level` equal slots, each big enough for a full memtable's table |

**The easy way:** `Config::whole_device(n)` gives horton blocks `0..n` and
lets it place the regions: manifest first, then the WAL (about an eighth
of the rest), then the tables to the end of the device. `n` is your
device's (or flash partition's) size divided by `block`: a 1 MiB
partition of 4 KiB sectors is 256 blocks. `MyDb::<D>::MIN_DEVICE_BLOCKS`
is the smallest `n` the shape fits (166 for the quick start); below it,
`open()` returns `BadConfig`. `db.config()` shows where everything went.
The layout depends only on `n` and the manifest's size, so reopening finds
the data where it left it.

**By hand:** `Config::new(wal_start, wal_end, tbl_start, tbl_end,
manifest_a, manifest_b)` places each region yourself (ranges include the
start and exclude the end), for example to leave part of the device to
other data or to give the WAL more room for wear. `open()` checks it,
returning `BadConfig` otherwise:

- **Manifest:** each copy is `Manifest::max_blocks::<BLOCK>()` blocks, the
  worst case for `LEVELS × TABLES` tables with `KEY_MAX` bounds (1 block
  for the ESP32 profile, 4 for 256-byte keys × 28 tables).
- **Tables:** the region splits into `LEVELS × TABLES` equal slots, and a
  slot must hold a full memtable's table. A table never outgrows its slot,
  so the region holds `LEVELS × TABLES × slot_blocks` blocks of tables.
- **WAL:** one block per durable commit between flushes.

The const generics behind `db_types!` (`BLOCK`, `KEY_MAX`, `VAL_MAX`,
`CAP`, `ARENA`, `LEVELS`, `TABLES`, `BLOOM_BYTES`, `CACHE`) and the rules
each must meet are listed on `Db` in the API docs.

### Flash endurance

On NOR flash every block write is a sector erase. Batch writes that arrive
together into one `WriteBatch` (one erase instead of one per op), and use
`Config::with_manifest_ring(n)` to spread manifest erases over `n` copies.
Table slots are allocated next-fit, so table erases spread across the
region. See [ADR-0012](docs/adr/0012-nor-flash-endurance.md).

## Platforms

- **Host (x86_64 / macOS / Linux):** where the test suite runs. `std`
  appears only in tests, benches, and examples.
- **ESP32-S3 (`xtensa-esp32s3-none-elf`):** the library builds with the ESP
  Rust fork (`./xtensa-check.sh`). A bare-metal smoke binary
  (`xtensa-smoke/`) boots under QEMU and exercises
  put/get/delete/flush/compact/scan/snapshot. `src/esp32s3.rs` follows
  ESP-IDF v5.2's register sequences and is verified against a mock NOR chip.
  **It has not been verified on silicon yet** (timing, power loss), because
  QEMU does not emulate the SPI user-command path.

## Testing

```sh
cargo test                        # ~350 tests: unit, integration, crash injection, fuzz
LIFECYCLE_SEEDS=300 cargo test --release --test lifecycle   # wider fuzz sweep
cargo test --release              # same suite, optimized
cargo run --example quickstart
cargo +nightly miri test --test <name>          # UB check (the crate has no unsafe)
cargo build --release --benches                 # callgrind instruction-count harnesses
./xtensa-check.sh                               # ESP32-S3 build gate (needs the esp toolchain)
```

What the suite covers:

- **Crash injection.** Every block write is enumerated as a crash point over
  flush, compaction (multi-output jobs included), TTL purge, batches,
  archive and ingest. The recovered state must be one of the committed
  states. A torn-write variant models partially written blocks, including
  torn multi-block manifest commits.
- **Lifecycle fuzzer** (`tests/lifecycle.rs`): long random operation
  sequences with reopen, WAL wrap, archive and re-ingest on small regions,
  checking `Db::check_invariants` after every op against a `BTreeMap`
  oracle.
- **Review regressions** (`tests/review_findings.rs`): one test per
  architecture-review finding; CI fails if one is ignored.
- **Differential and property tests** against a `BTreeMap` oracle and the
  executable models in `src/model.rs` (visibility, the per-key keep-set,
  TTL and range-delete winners).
- **In-tree, dependency-free fuzzers** for the WAL, SSTable, manifest and
  LZ77 decoders. Bit flips, truncations and splices must produce clean
  `Error`s and never a panic.
- **Mutation testing** with `cargo-mutants` on the core modules. See
  [`MUTATION_TRIAGE.md`](MUTATION_TRIAGE.md).

## Repository layout

| Path | Contents |
|---|---|
| `src/db/` | `Db`: `mod.rs` (config, open/recovery, write path), `read.rs`, `flush.rs`, `compaction.rs` (job policy and commits), `archive.rs` (archive and ingest), `invariants.rs` |
| `src/wal.rs` | WAL record codec, writer, recovery |
| `src/memtable.rs` | Sorted-slot memtable over a bump arena |
| `src/sstable.rs` | SSTable writer and reader, bloom filter, index, range-tombstone blocks, relocation |
| `src/compact.rs` | Merge engine, cursors, range-tombstone merger |
| `src/scan.rs` | `Scan` / `RevScan` merge iterators |
| `src/manifest.rs` | Manifest: multi-block copies, pair or ring layout, staged edits, commit/recover |
| `src/slots.rs` | Table-slot allocator: one table per fixed slot, next-fit, reservations |
| `src/cache.rs` | CLOCK block cache |
| `src/compress.rs` | LZ77 block codec |
| `src/batch.rs` | `WriteBatch` |
| `src/crc.rs` | Slicing-by-8 CRC-32 |
| `src/model.rs` | Executable models used as test oracles |
| `src/device.rs`, `src/flash.rs`, `src/esp32s3.rs` | `BlockDevice` trait, NOR flash adapter, ESP32-S3 SPI flash driver |
| `src/profile.rs`, `src/macros.rs`, `src/defaults.rs` | Measured ESP32-S3 profile; the `db_types!` macro and its defaults |
| `examples/quickstart.rs` | The quick start, complete |
| `examples/flight_recorder/` | The flight recorder demo: simulated NOR flash with power cuts, the recorder, a checker, and archiving to a directory or S3 |
| `tests/` | Integration, crash, fuzz, differential, and mutation-killing tests |
| `benches/` | `harness = false` callgrind/cachegrind harnesses |
| `xtensa-smoke/` | Bare-metal ESP32-S3 QEMU smoke test |

## Documents

- [`SPEC.md`](SPEC.md): normative spec: constraints, formats, protocols, API.
- [`CHANGELOG.md`](CHANGELOG.md): what changed in each version.
- [`docs/adr/`](docs/adr/README.md): architecture decision records.
- [`docs/ARCHITECTURE_REVIEW.md`](docs/ARCHITECTURE_REVIEW.md): the v0.16
  architecture review and the status of each finding.
- [`docs/history/`](docs/history/milestones-v0.1-v0.16.md): the v0.1–v0.16
  milestone log with its test-run reports.
- [`BUDGET.md`](BUDGET.md): ESP32-S3 RAM accounting.
- [`MUTATION_TRIAGE.md`](MUTATION_TRIAGE.md): mutation-testing results and
  equivalent-mutant reasoning.

## License

Dual-licensed under MIT OR Apache-2.0, as declared in `Cargo.toml`.
