# ADR-0034: Index blocks end in restart offsets, and a search checks what it reads

Status: Accepted (unreleased, 2026-11-26)

## Context

A table's index is one block of entries, `first_key_len u16 |
first_key | block_id u64 | max_seq u64` per data block, followed by zero
padding. The entries are variable-length and the block held nothing
else: no count, no offsets. To find a key, `index_partition` parsed
every entry, to count them (the count bounds a point read's walk across
blocks) and to tell the padding from a damaged entry, and compared a
sample of their keys.

So an index search read the whole block, and a cached one was checked
whole. Since ADR-0031 a point read checks only the bytes of a data
block it reads, but its index block was still summed whole against the
cache's RAM checksums: all 64 chunks of a 16 KiB block, 25% to 31% of a
point read served from the cache (`docs/THROUGHPUT.md`, step 22). A
seek copied every index block it searched out of the cache, summed
whole, about 7 of them per seek in the host store.

ADR-0017 and ADR-0031 left this as a lever: a search that reads only
part of the block has to count and locate the entries without parsing
them all, which the format did not allow.

## Decision

**An index block ends in a trailer.** After the entries and the zero
padding, at the end of the block's body (before its CRC), come a `u16`
offset for every 16th entry (`INDEX_STRIDE`), the first of them 0, then
the entry count as a `u16`. A block of `n` entries has
`index_tail(n) = 2 + 2⌈n/16⌉` bytes of trailer. This is the restart
layout of a data block (SPEC §4.4), with the count where a data block
keeps its flag word. Every writer seals its index through
`TableWriter::finish_meta`, which writes the trailer. `append_index`
refuses an entry when the entries and the trailer for one more would
not fit the block (`TableTooLarge`), and `TableWriter::index_len`
counts the trailer, so compaction's `should_split` reserves two more
bytes per future entry.

**A search reads the trailer, a binary search's restarts and one run
of at most 16 entries.** `index_partition` reads the count and the
restarts, binary-searches the restarts' first keys, then scans the 16
entries the boundary falls in. Point reads (`index_lookup`) and both
seek directions (`index_lookup` forward, `index_last_le_block` reverse)
use it. Relocation and compaction read the trailer's count and parse
the entries in front of it.

**A cached index block is lent with a check, as a data block is.**
`search_verified` searches the image where it lies through
`CachePort::with_checked` (ADR-0031). The search checks the count's
two bytes before it reads the count, the whole restart array before it
reads an offset, and each entry's extent (`off..next`) before it
compares its key or returns its block. A failed check ends the search
and the lookup is a miss. A search that fails on any error has the
whole image checked, so a fault in RAM is a miss and a device read,
never `CorruptBlock`. A miss, or a cache that does not lend, reads the
block from the device and searches it with a check that always holds.
Scans no longer copy an index block out of the cache.

**What is corruption.** A count whose trailer does not fit the block, a
restart at or past the trailer, a first restart other than 0, or an
entry that does not parse inside the entries' area is `CorruptBlock`.
The padding is not read, so a non-zero byte in it is no longer found.
It cannot change an answer: every byte the search acts on is in an
entry, a restart or the count, and checked.

**The magics change.** SSTables are `hrtsst05` (from `hrtsst04`) and
the manifest `hrtman11` (from `hrtman10`). A `Db` reads no table
footers (ADR-0021), so it is the manifest magic that turns away an
image written before this change; a table footer's magic turns away an
archive (`flight_recorder` restore, `relocate_table`). Both are
rejected, never misread.

**Shapes whose index cannot hold one key's run are refused at compile
time.** Compaction never splits a key's versions across outputs, so one
output's one-block index must hold the data blocks of one key under
`MAX_SNAPSHOTS` snapshots (`Compaction::key_run_blocks`), trailer
included. `Db`'s `ASSERT_INDEX` checks it (`sstable::index_holds`).
Before this change nothing did. A shape whose largest entry fills a
small block (512-byte blocks, 236-byte keys and 200-byte values, for
one) could already wedge compaction on a key with a few versions. The
trailer moved that line by a few bytes, so a compaction that
completed before could fail with `TableTooLarge` on every retry. Such
shapes no longer build. Every shape in the crate, its tests and its
examples passes.

## Consequences

- **Cheaper reads and seeks** (`docs/THROUGHPUT.md`, step 23, each
  build on its own store of the same shape). An index search sums about
  8.5 of the index block's 64 chunks instead of all of them.
  - Instructions per point read fall 58% at 300 K keys (every key read
    cached) and 21% to 34% at 1 M keys. Per seek they fall 32% to 36%.
  - Point reads at 1 M keys are 32% to 41% faster when the keys read
    are cached. On the full key range they are 19% faster on one
    reader and 14% on four. A read's index block is a cache hit even
    when its data block is not, so unlike ADR-0031's change this one
    reaches reads that go to the device.
  - Cached seeks are 58% to 75% faster, and cold seeks over fresh
    stores about 19%.
  - Scans and fills did not move.
- **The index holds a little less.** A table's index of `n` entries of
  `k`-byte keys needs `n × (18 + k) + 2 + 2⌈n/16⌉` bytes of the block's
  `BLOCK − 4`. The host store's index of 16-byte keys holds 479 data
  blocks instead of 481; the default `TestDb` shape (4 KiB blocks,
  256-byte keys) holds 14, as before.
- **Images and archives from before are rejected.** That includes
  every image and archive v0.19 wrote, and those of builds of this
  release from before the change (`hrtman10`, `hrtsst04`).
- **Garbage in the padding goes unseen.** A table that passed its CRC
  with stray bytes between its entries and its trailer used to be
  `CorruptBlock` on every lookup. Now it reads correctly. Only bytes
  that can change an answer are corruption.
- **A flush can still outgrow its index.** A full memtable of the
  largest entries can need more data blocks than one index block
  names, for example at 1 KiB blocks with a 16 KiB arena of 32-byte
  keys. A flush then fails with `TableTooLarge`, and goes on failing,
  as it did before; the trailer moves that line by a few bytes too. A
  compile-time check over the worst flush would refuse shapes the
  crate's own tests use, which in practice never fill a memtable with
  the largest entries. Splitting a flush across tables would fix it;
  that is separate work.
- **API.** `sstable::index_tail` is new and public, so a host can size
  an index. `TableWriter::index_len` now counts the trailer, so an
  empty writer's is 2.
- **Tests.**
  - The index search answers as a model over a slice and checks at
    most its binary search's restarts and one run of entries
    (`sstable::tests`).
  - A trailer that does not fit the block is `CorruptBlock`, and a
    stray byte in the padding changes no answer.
  - With every byte the search did not check randomized, it gives the
    same answers and checks no new range. Key lengths run from 1 to
    120 bytes, with runs of duplicates, up to a full block.
  - Each bit of every byte of index blocks of 0 to 3,000 entries is
    flipped in turn, at 512 and 4,096 bytes, under a check that fails
    exactly on ranges over the flip. Every search answers as over the
    intact block or fails, and none answers after a failed check.
  - A bit of every byte of a cached index block is flipped in the
    cache's RAM, through `TableReader` and through forward and reverse
    scans (`tests/index_flip_scan.rs`). Every answer is the intact
    one.
  - Each guard is shown to matter. Removing the entry check or the
    restart check fails the flip test. Removing either scan's check
    fails the scan test. Removing the whole-image check on an error
    fails `TableReader`'s.
- **Review.** Three adversarial reviews found no case where a flip
  changes an answer.
  - One ran the new search against the old one, ported from `main`,
    on every entry count up to 80 and on 300 full blocks at 512, 4,096
    and 16,384 bytes. It also flipped every bit of every byte under a
    byte-precise check, and drove `TableReader` and `Db` scans through
    a lending cache. Every answer matched.
  - Another checked the writers, relocation and old-image rejection,
    and found the compaction wedge above. The fix is `ASSERT_INDEX`.
  - Their test gaps (scan seeks under a RAM flip, variable key lengths
    and block sizes) are filled by the tests above.

## References

- SPEC.md §4.4 (the index block and the footer), §4.9 (archive and ingest)
- [ADR-0016](0016-cache-hits-reverify-with-a-ram-checksum.md) (RAM
  checksums), [ADR-0017](0017-bucketed-bloom-and-ranged-reads.md),
  [ADR-0021](0021-the-manifest-record-describes-a-table.md),
  [ADR-0031](0031-point-reads-check-what-they-read.md) (the checked lend)
- `src/sstable.rs` (`index_tail`, `write_index_tail`, `IndexTrailer`,
  `index_partition`, `search_verified`, `index_holds`), `src/scan.rs`
  (`position_cursor`, `add_cursor_rev`), `src/db/mod.rs`
  (`ASSERT_INDEX`)
- [`docs/THROUGHPUT.md`](../THROUGHPUT.md), step 23
