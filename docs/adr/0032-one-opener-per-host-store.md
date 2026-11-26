# ADR-0032: The host store has one opener at a time

Status: Accepted (unreleased, 2026-11-26)

## Context

This record is about `examples/kvstore`, the host store, not the
library. The library is `no_std` and the caller owns the device, so the
host must stop two openers of one device.

The kvstore did not stop them. Two processes could open one store file
at the same time. Each process appended to the WAL at the same
position, and the last write replaced the other. A `put` gave exit
status 0 and its value was gone at the next open. Two CLI `put`s at the
same time lost one acknowledged write in nearly every try (issue #44).
LevelDB and RocksDB refuse a second opener with a `LOCK` file.

Facts that limit the design:

- One `Store` opens its file four times: the engine, one device for
  each of the two compaction lanes, and one read-only device for
  detached reads. A lock in `FileDevice::open` locks out the store's
  own handles.
- On Unix, `File::try_lock` is `flock`: advisory, and held by one open
  file. Other handles read and write as before.
- On Windows, `File::try_lock` is `LockFileEx` on all bytes: mandatory.
  A lock on the store file stops the reads and writes of the store's
  other handles.

## Decision

**`Store::open` locks `<store>.lock`, a file beside the store, before
it opens the store.** It uses `std::fs::File::try_lock` (exclusive,
does not wait). If another opener holds the lock, the open fails with
`StoreError::Open` ("the store is open already"). The `Store` keeps the
lock until its `Drop` has closed the store.

- **Lock first.** The lock comes before `FileDevice::open` sets the
  size of a new file. Thus two openers cannot give one store two sizes.
- **The store, not a handle, keeps the lock.** A `Handle` can live
  after its `Store`. It holds no lock, so the file is free when the
  `Store` is closed.
- **The path is canonical.** `StoreLock::acquire` creates the store
  file if necessary, then resolves its path. A symbolic link to a store
  gives the same lock file.
- **The lock file stays.** If the lock file is deleted, a new opener
  can lock a new file while an old opener holds the deleted one. The
  operating system releases the lock when the process stops, also on
  `kill -9`.
- **A failure to lock is a failure to open.** If the file system does
  not lock, the open fails. It does not continue without the lock.

Not done:

- A byte-range lock on the store file (SQLite's method). It needs
  `libc` and `windows-sys`; the examples use only `std`.
- A PID file. It is not released when a process is killed.
- An open that waits for the lock. The CLI must fail fast.

## Consequences

- A second opener gets an error, and no acknowledged write is lost.
  Two CLI calls at the same time can now fail. The caller tries again.
- Each store has a second file, `<store>.lock`, of zero bytes.
  `Store::destroy` deletes both files.
- A hard link to a store has a different name, and so a different lock
  file. The lock does not stop that.
- `File::try_lock` is in Rust 1.89. The library's MSRV is 1.88, but the
  examples build on the pinned toolchain (1.97). The lint
  `clippy::incompatible_msrv` is allowed on that one function.

## References

- Issue #44: the race and its repro.
- `examples/kvstore/device.rs` (`StoreLock`), `store.rs` (`Store::open`,
  `Store::destroy`).
- ADR-0018 (detached reads), ADR-0024 and ADR-0025 (compaction lanes):
  why the store opens its file more than one time.
