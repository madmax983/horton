# Horton ESP32-S3 RAM budget (v0.13)

All numbers are **measured** with `core::mem::size_of` on the host
(`tests/profile.rs` prints and asserts them); the layout is identical on
xtensa — the structs are inline arrays, no heap pointers, nothing allocated.
The device's own storage (RAM disk in tests, flash region on hardware) is
extra and not counted here.

## Profile

`src/profile.rs`: `Esp32S3Db`, `Esp32S3Scan`, `Esp32S3Compaction`.

| Param | Value | Why |
|---|---|---|
| `BLOCK` | 4096 | SPI flash sector size — required by `FlashBlockDevice` |
| `KEY_MAX` | 32 | small keys |
| `VAL_MAX` | 64 | small values |
| `CAP` / `ARENA` | 16 / 2048 | memtable: 16 entries, 2 KiB arena |
| `LEVELS` / `TABLES` | 4 / 4 | up to 4 levels × 4 tables |
| `BLOOM_BYTES` | 64 | 512-bit bloom per table |
| `FREELIST` | 64 | reclaimed-block free list |

## Measured static RAM

| Struct | Bytes | Notes |
|---|---|---|
| `Db` | 17,064 | v0.6 baseline 12,960 + 4,096 decompression buffer (point reads inflate here) |
| `Scan` | 10,392 | v0.6 baseline 6,296 + 4,096 logical block buffer (physical reads land in the shared `raw` buffer, inflate here) |
| `Compaction` | 56,192 | v0.6 baseline 43,896 + 8,192 trial-compression scratch + 4,096 shared physical-read buffer (8 merge cursors share one `raw`; each keeps only its logical block) |
| **Total** | **83,648** | |

Budget: **98,304 bytes (96 KiB)** — `ESP32S3_RAM_BUDGET` in `src/profile.rs`,
asserted by `tests/profile.rs`. Raised from 64 KiB for v0.13: block
compression costs ~20 KiB of caller-owned scratch, every byte load-bearing
(the read path cannot inflate without a target; the writer cannot
trial-compress without staging). 96 KiB is 19% of the S3's 512 KiB SRAM —
still comfortable room for the kernel, stacks, and drivers. Growth past it
fails the test suite loudly; re-tuning is then a conscious SPEC decision,
not silent bloat.

## Context

The ESP32-S3 has 512 KiB of HP SRAM. Tallow's kernel, task stacks, and
drivers take their share; 64 KiB for the database leaves comfortable room.
The smoke test (`xtensa-smoke/`) additionally keeps a 34-block RAM disk
(136 KiB) in `.bss` — that is test-only scaffolding, not Horton RAM.
On hardware the device is a flash region owned by `FlashBlockDevice`,
which adds **zero** RAM (no read-modify-write buffering: every block write
is one sector erase + program).
