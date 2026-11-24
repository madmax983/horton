# ADR-0029: Atomic batches across WAL blocks

Status: Accepted (unreleased, 2026-11-24)

## Context

A `WriteBatch`, and each unit of a `write_group` (ADR-0028), had to fit
one WAL block. That is what made it atomic: recovery replays a group of
records only when its closing record is intact, and a group never left
its block. A batch larger than a block was `BatchTooLarge`, and a caller
with more to write atomically had nowhere to go.

Group commit already writes up to `GROUP_BLOCKS_MAX` (16) blocks before
one flush, and open already deals with what a crash leaves of them: it
zeroes blocks stranded past the recovered log and numbers new writes
above every record it finds there. What was missing is a group that
closes in a later block than it began, and a recovery that can tell
when every block of it is there.

## Decision

**A group may span consecutive WAL blocks.** Two more flags in the
record's op byte (the op kind keeps bits 0 to 4):

| Flag | Bit | On |
|---|---|---|
| `MORE` | `0x80` | every record of a group but the last (as before) |
| `SPAN` | `0x40` | a block's last record, when its group goes on in the next block; only with `MORE`; clean zero padding must follow it |
| `CONT` | `0x20` | a block's first record, when it continues the group the previous block's `SPAN` left open |

A group's records take consecutive sequence numbers, so the first record
of each later block must be numbered one past the `SPAN` record before
it. That, with `CONT`, ties the blocks of a group together: an older
block, a block of another group, or bytes left behind a torn rewrite
cannot join one.

**The writer.** When the next record of an open group does not fit what
is left of the stage, `WalWriter` marks the stage's last record `SPAN`,
re-seals that record's CRC, writes the block without a flush, and gives
the record `CONT` as the first of the next block. A group may span
blocks only when it began at the start of a block, and over at most
`GROUP_BLOCKS_MAX` blocks; otherwise the writer refuses it with
`BatchTooLarge` and drops it from the stage. A failed block write inside
a group drops it too. So no later commit ever writes any of an
abandoned group, whoever calls `WalWriter`.

**`Db::write` and `write_group`** plan a batch before writing anything,
packing record by record as the writer does (`Placement`): a unit that
does not fit what is left of a block starts a fresh one, and a record
that does not fit starts the next block. A byte sum is not enough: three
records of 60% of a block take three blocks. The checks, in order:

- a record larger than a block, a batch that needs more than
  `GROUP_BLOCKS_MAX` blocks (or more than the whole WAL region), or one
  that even an empty memtable cannot hold, is `BatchTooLarge` (a flush
  would not help);
- the memtable as it is: `TableFull` / `ArenaFull`;
- the WAL blocks left: `WalFull`, before any block is written. The
  number of blocks it needed is remembered, and the next flush wraps the
  region when fewer are left than that (before, a flush wrapped only an
  exhausted region, which a commit of one block was enough for).

A device error after a block of the batch landed fails it, unapplied:
its sequence numbers are consumed, the rest of it is dropped from the
stage, and its memtable cost counts as taken until a flush moves past
it (`Unapplied`, ADR-0028).

**Recovery** reads each block once and carries an open span: the block
and sequence number where a spanning group began, and the last sequence
number so far. The rules:

- A block joins the open span only if its first record has `CONT` and
  the next sequence number. When the closing record arrives, the group
  is replayed by reading its blocks again from the first, so no record
  of it reaches the memtable before the whole group is known to be
  there (no allocation: the stage buffer is reused), then the rest of
  the closing block is read as usual.
- Anything else breaks the span and drops all of it: the next block
  blank, stale, torn or not continuing it, a tear inside a continuing
  block, a chain longer than `GROUP_BLOCKS_MAX` blocks.
- After a break, the block where it broke (never the one after it) goes
  on with the log only if it begins with a record that is not `CONT` and
  is newer than everything replayed and than the flush floor. A commit
  that failed part way leaves a dead span with acknowledged writes after
  it, and those must be read; anything older is the end of the log.
- Writing resumes at the dead span's first block, since nothing of it
  is live.
- Torn bytes, never a group: `CONT` while no span is open, `CONT`
  anywhere but first, `SPAN` not followed by clean padding, `SPAN`
  without `MORE`, a span begun anywhere but a block's start.

**Open's scrub reaches past where recovery stopped reading.** ADR-0028's
scrub read `GROUP_BLOCKS_MAX` blocks from where writing resumes. With a
span, writing can resume up to 15 blocks before the block recovery
stopped at. A commit that failed mid-span, then a group commit of 16
blocks after it whose flush never came, put that group's last blocks
beyond such a window: recovery dropped them, writes filled the gap, and
the next recovery replayed them. The scrub now reads from where writing
resumes to `GROUP_BLOCKS_MAX` blocks past where recovery stopped
(`RecoverState::blocks_read`). Every unflushed block lies inside that.

**The manifest magic is `hrtman10`.** An older build reads a flagged
record as an unknown op, a tear: it would drop a spanning group and
every write after it, and write over them. The magic keeps the two
apart. Images written by older builds are rejected. A manifest copy that starts with `hrtman` and another version
now rejects the image (`CorruptManifest`). Before, such a copy beside a
blank one read as a fresh device: the old WAL was replayed over tables
this build could not see. The WAL record magic is unchanged: a
never-flushed old image (no manifest) is a WAL of records that are a
subset of the new format, and reads correctly.

## Consequences

- **A batch can be up to 16 WAL blocks**, about 64 KiB with 4 KiB
  blocks and 256 KiB in the host store. The memtable usually binds
  first on firmware: a batch must fit `CAP` slots and `ARENA` bytes even
  empty. A `WriteBatch<OPS>` holds `OPS * (KEY_MAX + VAL_MAX)` bytes
  inline, so a firmware caller with large batches is better served by
  `write_group` with borrowed `GroupOp`s.
- **Latency and wear grow with the batch.** Each block is a sector erase
  on NOR flash (tens of milliseconds, hundreds in the worst case), and a
  batch is acknowledged only after all of them and the flush. A batch of
  many blocks also pays an erase for the padding of its last block, and
  a span that does not fit what is left of a block starts a fresh one.
  Small batches cost what they did.
- **Images and the API.** Old images are rejected (above).
  `RecoverState` gains `blocks_read`. `BatchTooLarge` now names what was
  exceeded (a record and the block, a batch and 16 blocks, or a batch
  and the empty memtable's slots or arena). The host store's `Batch`
  and its writer queue plan with the same packing.
- **An older build cannot be made to reject a new image** that has no
  manifest yet (never flushed) or one manifest copy beside a blank one:
  it reads the new manifest as damaged, and the flagged records as a
  torn tail. Downgrades are not supported.
- **Review.** A design critique before the code found the scrub window
  above, the break rules (which block after a break is tested, and that
  an orphan `CONT` block ends the log), `Db::write`'s rollback keeping a
  half-written group staged and reusing its sequence numbers, the
  manifest copy beside a blank one, mid-block span heads, the planner's
  byte sum, and the `WalFull` that no flush answered. Each is fixed with
  a test: `tests/wal_span.rs` (format and recovery rules, every subset of
  a group's blocks, tears at every offset), `tests/group_commit.rs` and
  `tests/write_batch.rs` (failures part way, the scrub window, `WalFull`,
  crashes at every point), `tests/manifest.rs`, and the randomized crash
  model in `tests/group_commit_crash.rs`, now with spanning units and
  failed spanning batches followed by writes.

## References

- [ADR-0028](0028-group-commit-across-wal-blocks.md) (group commit and
  the scrub), [ADR-0011](0011-multi-block-manifest-and-edits.md)
- `src/wal.rs` (`SPAN`, `CONT`, `seal_span`, `recover_from`,
  `scan_block`, `replay_span`, `scrub_stranded`), `src/db/group.rs`
  (`Placement`, `plan_group`), `src/db/mod.rs` (`Db::write`, open's
  scrub), `src/db/flush.rs` (`wal_spent`), `src/manifest.rs`
  (`Probe::Foreign`)
- SPEC §4.2, §4.5
