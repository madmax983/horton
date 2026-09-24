# Horton ESP32-S3 RAM budget (v0.17)

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
| `BLOOM_BYTES` | 64 | 512-bit bloom per table |
| `CACHE` | 2 | two cached block images |

## Measured static RAM

| Struct | Bytes | Notes |
|---|---|---|
| `Db` | 25,248 | memtable, WAL stage block, manifest, slot map, two shared read buffers (point reads and the block scratch of every `&mut self` call), 2-slot block cache |
| `Scan` | 10,536 | logical block buffer, physical read buffer, cursors |
| `Compaction` | 56,744 | 8 merge cursors (one logical block each) sharing one physical read buffer, the output table writer, trial-compression scratch |
| **Structs** | **92,528** | |

## Measured futures

An `async fn` keeps every local that lives across an `.await` in its
future, and the executor stores the future: in a static task arena
(embassy) or on a poll loop's stack. So futures are RAM too.

| Future | Bytes | Notes |
|---|---|---|
| `flush()` | 17,576 | table writer (data + index blocks) and trial-compression scratch |
| `archive_commit()` | 17,496 | the resurrection review's entry stream over the candidate |
| `open()` | 6,824 | WAL recovery |
| `compact_step()` | 2,056 | merge state lives in the caller's `Compaction` |
| `get()` | 1,056 | blocks are read through the `Db`'s shared buffers |
| `ingest_table()` | 560 | copies through the `Db`'s block scratch |
| `put()`, `delete()`, `write()`, … | ≤ 384 | |
| `Scan::next()`, `RevScan::prev()` | 4,752 | |
| `Scan::seek()`, `RevScan::seek_prev()` | ≤ 1,104 | |

At most one `&mut self` call runs at a time, and it excludes scans
(a `Scan` borrows the `Db`). A `get` can run alongside a scan step. The
peak is therefore

```
structs + max(largest &mut self future, get + largest scan future)
= 92,528 + max(17,576, 1,056 + 4,752) = 110,104 bytes
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
