# Horton ESP32-S3 RAM budget (v0.20)

All numbers are **measured** with `core::mem::size_of` /
`core::mem::size_of_val` on the host (`tests/profile.rs` prints and
asserts them). The layout is identical on xtensa: the structs are inline
arrays with no heap pointers, and nothing is allocated. The device's own
storage (a RAM disk in tests, a flash region on hardware) is extra and
not counted here.

## Profile

`src/profile.rs` declares `Esp32S3Db`, `Esp32S3Scan`, `Esp32S3RevScan` and
`Esp32S3Compaction` with `db_types!`.

| Param | Value | Why |
|---|---|---|
| `BLOCK` | 4096 | SPI flash sector size, required by `FlashBlockDevice` |
| `KEY_MAX` | 32 | small keys |
| `VAL_MAX` | 64 | small values |
| `CAP` / `ARENA` | 16 / 2048 | memtable: 16 entries, 2 KiB arena |
| `LEVELS` / `TABLES` | 4 / 4 | 16 table slots |
| `BLOOM_BYTES` | 64 | 512-bit bloom per table: one bucket, one block (a filter that fits a block is never folded: ADR-0020) |
| `CACHE` | 2 | two cached block images |

## Measured static RAM

| Struct | Bytes | Notes |
|---|---|---|
| `Db` | 25,976 | memtable (with its sorted view: ADR-0026), WAL stage block, manifest, slot map, two shared read buffers (point reads and the block scratch of every `&mut self` call), 2-slot block cache (each slot a 4 KiB image, a tag and 16 chunk sums: ADR-0016), the region layout, the requested-compaction state |
| `Scan` | 10,904 | one decoded block, the physical read buffer, cursors; block buffers lent with `use_block_pool` are the caller's and not counted (the profile lends none) |
| `Compaction` | 59,640 | 8 merge cursors (one logical block each) sharing one physical read buffer, the output table writer, trial-compression scratch, the range-tombstone coverage stream (2,728 bytes: F16, F22) |
| **Structs** | **96,520** | |

## Measured futures

An `async fn` keeps every local that lives across an `.await` in its
future, and the executor stores the future: in a static task arena
(embassy) or on a poll loop's stack. So futures are RAM too.

| Future | Bytes | Notes |
|---|---|---|
| `archive_commit()` | 17,688 | the resurrection review's entry stream over the candidate |
| `flush()` | 17,640 | table writer (data + index blocks) and trial-compression scratch |
| `open()` | 7,216 | WAL recovery |
| `compact_step()` | 3,032 | merge state lives in the caller's `Compaction` |
| `get()` | 1,432 | blocks are read through the `Db`'s shared buffers; a table's reader is built from its manifest record, with no footer read to hold state for (ADR-0021) |
| `ingest_table()` | 608 | copies through the `Db`'s block scratch |
| `put()`, `delete()`, `write()`, … | ≤ 528 | |
| `Scan::next()`, `RevScan::prev()` | ≤ 1,280 | |
| `Scan::seek()`, `RevScan::seek_prev()` | ≤ 1,288 | |

At most one `&mut self` call runs at a time, and it excludes scans
(a `Scan` borrows the `Db`). A `get` can run alongside a scan step. The
peak is therefore

```
structs + max(largest &mut self future, get + largest scan future)
= 96,520 + max(17,688, 1,432 + 1,288) = 114,208 bytes
```

Budget: **114,688 bytes (112 KiB)**, `ESP32S3_RAM_BUDGET` in
`src/profile.rs`, asserted by `tests/profile.rs` against that peak. Growth
past it fails the test suite loudly, so re-tuning is a conscious
decision, not silent bloat.

History:

- 64 KiB until v0.13.
- 96 KiB from v0.13: block compression costs about 20 KiB of
  caller-owned scratch.
- 112 KiB from v0.17: the futures are counted (architecture review F8).
  Before the fix the `flush()` future alone was 29.8 KiB. It now borrows
  the `Db`'s block scratch, stages the range-tombstone section in its
  table writer's idle data block, and commits the manifest as a small
  `ManifestEdit` instead of a full copy. `get()` returns `Error::Busy`
  instead of carrying two fallback block buffers in every future.
- Still 112 KiB at v0.20, with 480 bytes to spare: the block cache
  keeps 16 checksums per slot (ADR-0016), 264 bytes for the two slots,
  and the read futures carry a bloom probe and a byte range. `get()`
  grew by another 152 bytes when its table phase became shared with
  detached reads (ADR-0018); it is not the peak. A `Scan` grew by 80
  bytes (the bookkeeping for lent block buffers and for tables it has
  not reached yet: ADR-0019) and the memtable by 8 (a count of its
  range tombstones). A table reference grew from 120 to 128 bytes (its
  bloom section: its block count, probe count and length, ADR-0020 and
  ADR-0021), which is 128 bytes in the `Db`'s 16-table manifest, 72 in
  `Compaction`, and most of the 208 bytes `open()` gained. `get()`
  gave 56 back when it stopped reading footers. The `Db` grew by 40
  bytes for the bookkeeping of a second compaction job, and
  `compact_step()` by 160 for the lane it steps (ADR-0025); firmware
  that runs one job at a time pays those and nothing else. Atomic
  batches across WAL blocks (ADR-0029) added 56 bytes to the `Db` (the
  WAL writer's open group and the blocks a refused batch needed) and up
  to 32 to the write futures (`delete_range()` is 528). The
  compression threshold in `Config` (ADR-0030) added 8.

## Context

The ESP32-S3 has 512 KiB of HP SRAM, so 112 KiB is 22% of it, and the
kernel, task stacks and drivers still have comfortable room. The smoke
test (`xtensa-smoke/`) also keeps a 34-block RAM disk (136 KiB) in
`.bss`; that is test-only scaffolding, not Horton RAM. On hardware the
device is a flash region owned by `FlashBlockDevice`, which adds **zero**
RAM (no read-modify-write buffering: every block write is one sector
erase plus program).

`TestDb` (`KEY_MAX = 256`, 7 levels × 4 tables) has a 15.5 KiB manifest.
Before `ManifestEdit`, every commit future held a copy of it.
