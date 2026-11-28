# ADR-0040: Compressed data blocks carry entry points

Status: Proposed (unreleased, 2026-11-28; numbers and review pending)

## Context

A compressed data block (levels 2 and down, ADR-0030) is one LZ77
stream (ADR-0008), and the restart offsets that a search binary-searches
sit at the end of the block it decodes to. A point read (ADR-0027) and
a seek (ADR-0039) therefore decode from the block's first byte to the
entry they want: half of a 16 KiB block on average. After ADR-0039 that
decode was still a sixth of a cached seek at 1 M keys and a third of a
cached point read.

A stream can be entered part way only if no match crosses the point a
decode begins at, and the reader knows where that point is in the stream
and in the block.

## Decision

**The writer cuts a compressed block's stream at some of its restarts,
and lists the cuts in the block** (SPEC §4.4; `src/points.rs`):

```
stream | zero fill | n × (s u16 | t u16 | b u16) | n u16 | flag u16 | crc
```

- At a cut the encoder forgets its window (`CompressScratch::
  compress_chunked`): no match reaches below it or crosses the next
  cut. The cut is recorded as an entry point: `s` is the stream offset
  of the token a decode may begin at; `t` the logical offset that token
  writes from (its literals may begin a few bytes before the cut); `b`
  the chunk's head, the restart at the cut.
- Cuts are at least 2 KiB, or a sixteenth of the block, apart, and at
  most 15: about seven in a 16 KiB block.
- The stream stays one stream. A reader that knows nothing of the table
  decodes it as before and ignores the fill. Every block an older writer
  sealed has a zero count, and reads as before. **No magic bump**: old
  binaries read new images and new binaries old ones.
- Only blocks of 8 to 64 KiB carry a table. Over 64 KiB the offsets do
  not fit 16 bits. Under 8 KiB a block holds one cut at most: measured
  at 4 KiB, a point read with one was 1.9% slower, not faster, and the
  stream 1% to 10% longer. To a reader, the fill of a block of another
  size is fill on every path.

**A table is checked before it is used, and verified by every whole
decode.**

- Its shape (`point_table`): at most 15 points, wholly past the stream,
  ascending, each `t` at or below its `b`, each `s` inside the stream.
  A lend checks the count word and the table's bytes against the cache's
  RAM sums first (`table_of`).
- Whether each point is where a token starts, with no later match
  reaching below it, and whether each head is a restart, only a decode
  can tell. Every whole decode checks all three (`inflate_image`,
  `decompress_points`; `Inflate` carries a floor below which no match
  may reach). So a table that would mislead a decode begun at one of its
  points is `CorruptBlock` wherever the block is read whole: device
  reads, compaction, reverse scans, the fallbacks of partial reads.
- The writer checks the table it is about to write against its own
  stream, by token headers alone, decoding nothing
  (`compress::points_hold`; no buffer is free to decode into). A table
  that does not hold is not written. A block that clears the saving
  threshold only without a table is compressed plainly.

**A point read decodes only the chunk that holds its key**
(`data_lookup_points`, `chunk_for_key`). It decodes chunk 0's first
entry; a key at or before it is in chunk 0. Otherwise a binary search
over the other heads, each decoded by a probe of its own chunk, finds
the last head below the key, and the scan of ADR-0027 runs from there.
It checks stream bytes just before it decodes them, rather than the
whole stream first. A probe that writes over what chunk 0's decode made
makes that decode start again. What the scan does not decide goes to
the whole block, as before.

**A forward seek searches the heads the same way, inside its lend**
(`lend_prefix`, `search_heads`). The search state lives in the Prefix
gate and survives the parse's reruns. The gate admits only bytes from
where the decode began (`lo`) to where it has reached. The walk of
ADR-0039 then starts at the chosen head. A miss below `lo`, a parse
error with `lo` above 0, or a head that does not parse restart the
decode from the block's start, whole. Later steps go on with the same
decode (`Blocks::extend`), the stream being one stream; `Resume` keeps
`lo`, and the lend count moves into bit 15 of the stream length, so
`Resume` stays 8 bytes. A buffer that holds a decode begun at `lo`
claims no grain below it. Its first grain counts only once the grain
after it is whole (`Gated::holds`).

**A block with no table costs what it did.** Its seek takes a build of
the lend with the table's logic compiled out (`lend_prefix_in::<_, _,
_, false>`), and decodes with no floor check (`Inflate::decode::<_,
false>`).

## Alternatives

The design panel built three prototypes on the same store:

- **Raw restarts**: every restart an entry point, the restart array
  raw. As fast, but a row per restart cost the flight recorder's shape
  +8% in compression, the logical trailer had to be rebuilt in the
  buffer from rows, and ADR-0027's untouched-bytes test was relaxed.
- **The earlier panel's chunk table, ported**: slower at every page
  size (−15%, −10%, −1% on seeks), `Resume` 2 bytes longer, a 176-byte
  frame per lend.
- **Cuts at a sixteenth of the block, at least 1 KiB apart**: telemetry
  values compressed 21% worse. At 2 KiB the bench store's cost is
  unchanged (+1.5%) and telemetry's halves.

## Consequences

- **Speed** (`docs/THROUGHPUT.md`, step 27). [numbers]
- **Compression.** The bench store's streams are 1.5% longer (the
  cuts) and its tables add about 44 bytes a block. Blocks stay `BLOCK`
  bytes on the device and in the cache, so this is not space: it is
  RAM-sum work, and blocks near the saving threshold. No bench block
  fell back to raw.
- **No-table blocks** (old images, and every block under 8 KiB, the
  firmware's): point reads +0.4% and seeks +0.7% in instructions, from
  the gate's lower bound and the lend's larger state. A firmware seek
  over a two-block cache mostly reads the device, which inflates whole
  and does not take this path.
- **RAM.** None on the ESP32-S3 profile: its 4 KiB blocks carry no
  table, `Resume` stays 8 bytes, and the writer's arrays live in a
  synchronous helper. The peak is 114,176 bytes.
- **Trust.** A table is a new thing the CRC vouches for. Whole decodes
  verify it, so a wrong one is `CorruptBlock` there. A partial read over
  a wrong table answered, in every test, as the intact block does, or
  failed; and the writer does not write one. A table that slips past
  both is the class ADR-0027 accepts for unread parts of a block.
- **Tests.**
  - `compress::tests`:
    - a chunked stream decompresses to the block, from every point too;
    - a match below the floor is `Invalid` exactly when one exists, at
      every token of plain and chunked streams;
    - a point moved off a token is `Invalid`;
    - the writer's check agrees with the decoder on corrupted streams.
  - `points::tests`: one test per shape rule, the restart rule, the
    writer's bounds, and the size rule.
  - `sstable` partial-inflate tests at 4 KiB and 16 KiB: answers equal
    the whole block's; a wrong table is `CorruptBlock` whole and a point
    read over it answers truly or fails (0 wrong of 50,236 answers); a
    flip in a cached block with points, table and count included, never
    changes a lookup.
  - `scan::tests::prefix` at 8, 16 and 32 KiB: a seek and 17 parks
    equal the whole block, through `complete` and `extend`; flips on the
    1st, 2nd or 9th lend are misses.
  - `tests/entry_points.rs` over a 16 KiB store: a wrong table is
    `CorruptBlock` to scans both ways and to compaction; stripped tables
    read the same; flips stuck, first-lend-only and later-lend-only
    never change a scan, and reach decodes begun at an entry point.
- **Review.** [rounds]

## References

- [ADR-0008](0008-hand-rolled-lz77-block-compression.md) (the codec),
  [ADR-0027](0027-point-reads-inflate-to-the-key.md),
  [ADR-0035](0035-scans-copy-only-what-they-check.md),
  [ADR-0039](0039-seeks-inflate-only-what-they-read.md)
- SPEC §4.4 (the compressed data block)
- `src/points.rs`, `src/compress.rs` (`compress_chunked`,
  `decompress_points`, `points_hold`, `Inflate::at`), `src/sstable.rs`
  (`compress_data_block`, `data_lookup_points`, `chunk_for_key`),
  `src/scan.rs` (`lend_prefix`, `search_heads`, `Stream`)
- [`docs/THROUGHPUT.md`](../THROUGHPUT.md), step 27
