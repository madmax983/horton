# ADR-0030: No compression in levels 0 and 1

Status: Accepted (unreleased, 2026-11-25)

## Context

Since ADR-0008 the table writer trial-compresses every data block it
seals with LZ77 and keeps the compressed form when it saves at least
128 bytes. A table's blocks are a fixed size, though, and the writer
packs entries into a block by their raw size: a block is sealed when
the next entry would not fit it uncompressed. So a compressed table has
exactly as many blocks as a raw one, and occupies the same slot, writes
the same blocks (on NOR flash, the same erases) and is read with the
same device reads. What compression bought was a smaller payload inside
a block that is written and read whole anyway.

It was not free. Every data block written costs a trial compression and
every data block read an inflate, on a hit in the block cache too (the
cache keeps the compressed image). In a profile of the host store
(docs/THROUGHPUT.md, step 19) LZ77 was about 30% of all instructions in
a batched fill, and with compression switched off in a copy random
fills ran 70% faster.

The cost falls hardest on the shallow levels. Every write is flushed
into level 0, and level 0 drains into level 1, so those tables are
written most often and live the shortest: in a random fill of 1 M keys
an entry is written to a table about eleven times, and in a store with
slots to spare nearly all compaction is level 0 draining into level 1,
which rewrites level 1's tables again and again. They are also the tables a point read for a
recent key reaches first.

## Decision

**`Config::compression_from_level`** (default 2,
`DEFAULT_COMPRESSION_FROM_LEVEL`; set with
`Config::with_compression_from_level`) is the shallowest level a table
is written into compressed. A flush compresses only if it is 0; a
compaction job compresses its outputs only if its target level is at
or past it. The job decides when it is selected (`output_blocks` in
the caller's `Compaction`), so its detached merge quanta and every
output of a job that splits follow it. Bloom, index, footer and
range-tombstone blocks were never compressed and are not now.

- **0** compresses every table, as before this ADR.
- **`LEVELS` or more** compresses none.
- **It applies when a table is written.** A table compaction moves down
  whole (nothing below overlaps it) keeps its blocks as they are, so a
  raw level-1 table can sit at level 2 or deeper until a merge rewrites
  it; an ingested table keeps the blocks it was archived with. Forcing a
  rewrite only to compress would cost block writes and save none.
- **Not part of the format.** Each data block says whether it is
  compressed (bit 15 of its restart count, ADR-0008), and every reader
  branches on it per block. A store reopened under another threshold,
  or by an older or newer build, reads every table; the setting changes
  only what is written next. No format version changes.

The default keeps compression below level 1 rather than removing it:
the deep levels hold most of the data and are rewritten least, so their
share of the cost is smallest, and a writer that packed blocks by their
compressed size (fewer blocks per table, a format change) would make
compression save space there first.

## Consequences

Measured on the host store (docs/THROUGHPUT.md, step 21), against the
build that compressed every level, side by side:

- **Writes are faster.** At 1 M keys without `fdatasync`, two runs:
  `fillseq` 16% and 25%, `fillrandom` 36% and 40%, `overwrite` 43% and
  42%. Batched writes 13% to 50%, synced or not. One synced random fill
  into an empty store wrote the same blocks (68,740 before, 68,737
  after) at 1.28 to 1.31 times the rate.
- **Seeks are faster** (by 39% and 43% at one and four reader threads,
  on identical tables): a seek positions a cursor in every table that
  could hold its key, and the shallow ones no longer inflate a block.
- **Point reads and scans are slower** on identical tables: point reads
  by 4% to 12%, scans by 7% to 20%. Why a raw block costs a reader more
  than inflating a compressed one is being measured (step 21).

- **No change in space or flash wear.** Tables keep their block counts,
  so slots, the region's capacity and the erases a flush or compaction
  costs are as before.
- **RAM.** `Config` gains one byte, which pads the `Db` by 8 (BUDGET.md).
  The compression scratch in `Compaction` and in a flush's future is
  still there: a job whose target is compressed uses it.
- **API.** `Config` is a struct with public fields, so code that builds
  it as a struct literal must name the new field; the constructors set
  it.
- **Tests.** `tests/compression_levels.rs` checks raw shallow levels and
  compressed deep ones, the threshold at 0 and past the last level, a
  moved table that stays raw, and reads across the mix and after a
  reopen. Tests whose subject is the compressed path
  (`tests/compress.rs`, the cache test's compressed case) set the
  threshold to 0.

## References

- [ADR-0008](0008-hand-rolled-lz77-block-compression.md) (the codec and
  the block flag)
- `src/db/mod.rs` (`Config::compression_from_level`, `compresses`),
  `src/db/flush.rs`, `src/db/compaction.rs` (`compact_start`,
  `compact_seal_output`), `src/compact.rs` (`merge_step`)
- SPEC §4.4, [`docs/THROUGHPUT.md`](../THROUGHPUT.md)
