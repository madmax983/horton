# Horton ESP32-S3 RAM budget (v0.6)

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
| `Db` | 12,960 | WAL stage 4,096 + read buffer 4,096 + memtable 2,456 + manifest/snapshots/misc |
| `Scan` | 6,296 | per-level/per-table scan cursors (indices, no block buffers) |
| `Compaction` | 43,896 | 8 merge cursors × ~4.3 KiB (one 4 KiB block buffer each) + inputs + snapshot watermarks |
| **Total** | **63,152** | |

Budget: **65,536 bytes (64 KiB)** — `ESP32S3_RAM_BUDGET` in `src/profile.rs`,
asserted by `tests/profile.rs`. Growth past it fails the test suite loudly;
re-tuning is then a conscious SPEC decision, not silent bloat.

## Context

The ESP32-S3 has 512 KiB of HP SRAM. Tallow's kernel, task stacks, and
drivers take their share; 64 KiB for the database leaves comfortable room.
The smoke test (`xtensa-smoke/`) additionally keeps a 34-block RAM disk
(136 KiB) in `.bss` — that is test-only scaffolding, not Horton RAM.
On hardware the device is a flash region owned by `FlashBlockDevice`,
which adds **zero** RAM (no read-modify-write buffering: every block write
is one sector erase + program).
