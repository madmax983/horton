# ADR-0028: Group commit across WAL blocks

Status: Accepted (unreleased, 2026-11-24)

## Context

`Db::write` makes one batch durable with one WAL block write and one
device flush, and a batch must fit a block: that is what makes it
atomic (a torn block replays none of an unfinished group). The host
store (`examples/kvstore`) commits its writer queue that way, so one
`fdatasync` covered at most one block of client writes: about 120 puts
of 100 bytes in a 16 KiB block, two batches of 50.

With four writers that never mattered: a synced group averaged 2.6
client writes, far from a block. With more writers, or larger batches,
the block was the limit, and every block paid a flush.

Covering several blocks with one flush changes what a crash can leave.
Before, a block was flushed before the next was written. Now several
are written and then flushed, and a device may put them on the medium
in any order before the flush: a power cut can leave a later block of
the group without an earlier one.

## Decision

**`Db::write_group(units)`** commits many atomic units under one flush.
Each unit is a client's batch of borrowed ops (`GroupOp`), checked by
the same code as `WriteBatch` (`batch::check_op`).

- Each unit is placed inside one WAL block. When the next unit does
  not fit what is left of the stage, the stage is written out as a block
  without a flush (`WalWriter::make_room`).
- The longest prefix that fits is committed: each unit fits a block,
  the prefix fits the memtable's slots and arena and the WAL blocks
  left, and it spans at most `GROUP_BLOCKS_MAX` (16) blocks. The result
  (`GroupCommit`) says how many units, and their base sequence number.
  When the first unit does not fit, the error is the one `Db::write`
  would give it alone; the caller flushes and passes the rest again.
- One device flush, then every unit is applied to the memtable, in
  order. A reader never sees a unit before it is durable: the memtable
  is the only way a write becomes visible.
- A failure part way (a block write or the flush) fails the group.
  Records still in the stage are dropped, so no later commit writes
  them; if a block had reached the device, every sequence number the
  group used is consumed, as `Db::write` consumes its own.

**Open scrubs stranded blocks.** After WAL recovery, open reads the
`GROUP_BLOCKS_MAX - 1` blocks after the one recovery stopped at, and
zeroes (then flushes) any that holds a record newer than every record
recovered and than the flush floor. Only an unflushed group can have put
such records there, and only within that window. Without the scrub,
the next write fills the gap recovery stopped at, and the following
recovery reads straight on into the stranded block: writes this
recovery dropped would come back. Blocks are only read unless one needs
zeroing, so an open costs flash nothing in the common case.

**Open numbers new writes above whatever it finds there.** The scrub
also reads the block recovery stopped at, and looks for intact records
at every offset of each block it reads, not only from the start. New
writes are numbered above the highest sequence number found. Recovery
used to number them from the newest record it took, reusing the numbers
of records it dropped while their bytes were still on the medium; a
block written again in place can tear and keep its old bytes after the
new ones, and recovery takes records in a block while their numbers
rise. With new numbers above the old, the old bytes read as older and
are rejected. (Each of the two, the numbering and searching every
offset before zeroing, fixes the case review found; both are kept.)

**A failed commit leaves its records accounted for.** A commit that
fails after a block of it reached the device is reported as failed and
not applied, but the device may keep the records and the next open
replays them. Two rules make that safe (`Unapplied` in `src/db/mod.rs`):

- Every capacity check, in `write`, the single-op writes and
  `write_group`, counts those records' memtable slots and arena bytes
  as taken (the whole failed commit's, an upper bound), so a writer gets
  `TableFull` / `ArenaFull` in time instead of a database that replays
  more than its memtable holds and will not open (`CorruptWal`). A flush
  moves `wal_head` past them and clears the count; a flush of an empty
  memtable does it too, so the error always has its remedy. This was
  latent before: a single put whose flush failed left one record so.
- The next write flushes the device before adding blocks of its own, so
  an unflushed run is never longer than one commit and stays inside the
  scrub's window.

A contiguity rule (stop recovery where a block's first sequence number
does not follow the last) would also catch that, but sequence numbers
legitimately jump: ingest and archive move `next_seq` with no WAL
record. The scrub needs no such assumption.

**What a crash keeps.** Acknowledged writes survive, as before. Units
of a group whose flush had not returned survive whole or not at all, in
any combination (a later unit may survive an earlier one; neither was
acknowledged, and their clients were waiting at once). Whatever one
recovery keeps, later crashes keep: nothing it dropped comes back.

**The host store** packs queued client batches into up to
`GROUP_BLOCKS_MAX` blocks and commits them with `write_group`,
acknowledging each committed prefix. Its 2.9 MiB inline `WriteBatch`
for the group is gone: units borrow the clients' own batches.

## Consequences

- **Fewer syncs where blocks were the limit.** Synced `fillrandom`
  with 8 writers in batches of 50: 2,001 syncs became about 935 (4.3
  client batches each, from 2.0) and throughput rose 22 to 26%. With 16
  writers in batches of 20: 5.0 batches a sync became 9.0, 16 to 20%
  faster. Four writers of single puts: unchanged, as expected.
- **Opening reads up to 15 more WAL blocks.** A few milliseconds on SPI
  flash; writes only after a crash that stranded a block.
- **`Db::write` is unchanged**, and so is every image it writes:
  `write_group` uses the same record format and grouping, so images
  written by either read back by either. No format change.
- **The `multiwriter` drainer** still commits through `Db::write`.
  `WalWriter::append_batch`, the lower-level primitive, also writes
  several blocks before one flush and had the same exposure; the scrub
  covers its batches of up to `GROUP_BLOCKS_MAX` blocks, and its docs
  now say so. It has no callers in the library.
- **Review found four bugs in the first version**, each reproduced: a
  database that would not open after a failed group, a dropped unit
  coming back past the scrub window after a failed flush, a partial unit
  out of bytes left behind a torn rewrite, and a panic at the top of the
  sequence counter. Each has a test; `tests/group_commit_crash.rs` runs
  the reviewers' randomized crash model (dropped, torn and torn-in-place
  writes at every point, crashes during open) over 1,500 seeds in
  release builds, 150 in debug.
- **Not done:** a group's units are still at most one block each. A
  single batch larger than a block stays `BatchTooLarge`: making one
  batch span blocks atomically needs a record that closes a group
  across blocks, a format change.

## References

- [ADR-0026](0026-hot-path-pass-crc-codec-cache-memtable.md) (where
  this was listed as not done), [ADR-0011](0011-multi-block-manifest-and-edits.md)
- `src/db/group.rs` (`write_group`, `GroupOp`, `GroupCommit`,
  `GROUP_BLOCKS_MAX`), `src/wal.rs` (`make_room`, `scrub_stranded`),
  `src/batch.rs` (`check_op`), `tests/group_commit.rs`,
  `examples/kvstore/store.rs` (`take_group`, `write_group`)
- SPEC §4.2, [`docs/THROUGHPUT.md`](../THROUGHPUT.md)
