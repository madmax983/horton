# ADR-0001: Poll-based async `BlockDevice` trait, no executor

Status: Accepted (v0.1, 2026-09-12)

## Context

horton runs where there is no `malloc`: firmware, kernels, bootloaders.
The storage device is the only I/O boundary, and it has to be
implementable by hand on every host SPEC names: a VesperOS driver on
x86_64 bare metal, a `std::fs` shim on macOS for tests, and an ESP32-S3
SPI flash driver. The library may use only `core` (ADR-0002), so it
cannot box futures or depend on a runtime. SPEC §10 question 3 asked
whether the device should be sync or async.

## Decision

- Async from the start, through a poll-based trait rather than
  `async fn` in the trait: `poll_read_block`, `poll_write_block` and
  `poll_flush`, each returning `Poll<Result<(), Self::Error>>`, plus an
  associated `Error` type and `const BLOCK` (at least 512).
- Polling keeps the trait usable without an executor and without
  unstable features. Callers bridge to futures with
  `core::future::poll_fn`, which is still `core`-only.
- All I/O is whole blocks. `buf.len() == Self::BLOCK` is a precondition:
  debug-checked, and `Error::BadBufferLen` in release.
- The `Db` API is `async fn`s. The compiler turns each into a state
  machine that allocates nothing. The host drives it with whatever
  executor it has, or by polling manually. horton ships no executor.
- Device errors pass through unchanged as `Error::Device(E)`.

## Consequences

- One trait covers the test RAM disks, `FlashBlockDevice<F>` (v0.6) and
  a full `Db` on `FlashBlockDevice<SpiFlash<MockBus>>` (v0.7).
- Other features reuse the boundary instead of adding I/O paths: the
  archive API streams table blocks through `Db::device()` (v0.10), and
  `ingest_table` reads from a second `R: BlockDevice` source (v0.12).
- Durability rests on the device. The commit protocol is "write blocks,
  `flush()`, write manifest slot, `flush()`" (§4.5), so `poll_flush`
  must be a real barrier. A block that landed with a failed flush is
  treated like a crash at that instant (v0.11).
- Flash erase is not part of the trait. `FlashBlockDevice` maps each
  whole-block write to erase-sector then program, which ties `BLOCK` to
  the flash sector size (4096 on the ESP32-S3) (v0.6).
- Whole-block granularity: a WAL `commit()` pads and writes the partial
  block (§4.2).
- `ingest_table` requires the source's `BLOCK` to equal the database's
  (`BadBufferLen` otherwise) and `R::Error: Into<D::Error>` (v0.12).
- Callers must bring an executor or a poll loop.

## References

- SPEC §4.3 (BlockDevice trait), §4.2, §4.5, §6, §10 question 3
- [Milestone log](../history/milestones-v0.1-v0.16.md): v0.6, v0.7, v0.10, v0.11, v0.12
