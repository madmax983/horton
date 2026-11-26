# ADR-0033: The host store refuses bad input before it writes

Status: Accepted (unreleased, 2026-11-26)

## Context

This record is about `examples/kvstore`, not the library. A test session
(issue #46) found three problems:

1. **A refused open left a sized file.** `--size-mb 8` gave 512 blocks.
   `FileDevice::open` sized the new file, and then `Engine::open` found
   it too small. The next open used the file's size, not `--size-mb`,
   and failed again. The error did not give the remedy (ADR-0013).
2. **A usage error created a store.** `cli()` opened the store before it
   read its arguments. `kvstore get` with no key made a 256 MiB file.
3. **An inverted range did nothing.** `delrange z a` and `scan b a`
   gave exit status 0. RocksDB's `DeleteRange` gives `InvalidArgument`
   when start > end.

## Decision

**The host store checks its input before it writes to the disk.**

- **Size first.** `Store::open` checks the size after the lock and
  before `FileDevice::open`. A file of zero bytes is new, and gets
  `Options::create_blocks`. If that is fewer than
  `MIN_DEVICE_BLOCKS`, the open fails with `StoreError::TooSmall`, and
  the file is not sized. The next open makes the store. An existing
  file that is too small also gives `TooSmall`. Its message tells you
  to delete the file or to use another path. The CLI adds the
  `--size-mb` value to use.
- **Parse first.** `cli()` parses all of its arguments into a `Cmd`,
  then opens the store. A usage error opens nothing. Too many
  arguments is also a usage error.
- **Order first.** `delete_range`, `scan` and `range` refuse
  `start > end` with `StoreError::InvertedRange`. An empty range
  (`start == end`) is not an error.
- **No overflow.** `--size-mb` saturates, and `FileDevice::open`
  refuses a size that does not fit in a `u64` of bytes.

Not done:

- **The library does not change.** `Db::delete_range` keeps its
  documented contract: an inverted range is a no-op. Firmware callers
  and their tests use it.
- **No empty file is deleted.** `StoreLock::acquire` creates the store
  file, because a canonical path needs a file that exists (ADR-0032).
  A refused open can leave a file of zero bytes. The next open treats
  it as new. A lock path that does not resolve symbolic links can give
  two openers two locks (issue #44).
- **No file is resized.** A file that is too small can hold a store of
  an older shape. The store does not change it.

## Consequences

- A refused open does not block the next open with the correct size.
- A swapped pair of range bounds is an error, not a silent no-op.
  Callers that sent inverted ranges as a no-op must check the order.
- `StoreError` has two more variants.

## References

- Issue #46: the session report.
- `examples/kvstore/store.rs` (`check_size`, `check_order`,
  `StoreError::TooSmall`, `StoreError::InvertedRange`), `main.rs`
  (`parse`, `Cmd`, `Common::open`), `device.rs` (`FileDevice::open`).
- ADR-0013 (capacity errors name their remedy), ADR-0032 (one opener).
