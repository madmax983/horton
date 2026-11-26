# ADR-0035: A scan copies from a cached data block only what it checks

Status: Accepted (unreleased, 2026-11-26)

## Context

Since ADR-0031 a point read searches a cached data block where it lies
and checks only the ranges it acts on, and since ADR-0034 so does every
index search. A scan still read its data blocks whole. Each block a
scan reached was summed against all of its RAM checksum chunks
(`CachePort::with_range(0, BLOCK)`), copied out of the cache into a
scratch buffer, and copied again into the scan's block buffer. The
cursor was then parked by parsing the block's entries one by one from
offset 0 up to the seek key, without the restart points. A seek that
yields one entry reads a few hundred bytes of each block it positions
in, and paid for 16 KiB of it twice: 321 to 450 chunk sums a seek, 20%
to 25% of its instructions (`docs/THROUGHPUT.md`, step 23). The two
copies cost more still.

A scan cannot simply search the lent image in place, as a point read
does: the scan keeps its block and goes on parsing it after the lend
has ended, on later `next` calls, when the merge returns to its cursor.
And it cannot copy the whole image and check the rest later. A later
check sums the cache's image as it is then, not the scan's copy. After
the slot is refilled with a clean image, or once a transient fault has
gone, the check passes while the copy still holds the flipped byte.

## Decision

**A scan buffer knows which of its bytes were checked.** Each
`ScanBlock` carries `valid`, a 64-bit map over the block in grains of
`BLOCK / 64` bytes rounded up (256 bytes at 16 KiB, the host store's
checksum chunk; 64 bytes at 4 KiB). A set bit means one of two things.
Either the grain was copied from a cache image right after a check of
it held, in a lend that held. Or the whole buffer came from a
CRC-checked device read, or from a whole lend, and every bit is set.
`valid` is reset whenever the buffer's tag is (`victim`, `forget`,
`lend`). Debug builds fill a buffer with `0xA5` whenever it is picked
for a block, so a parser that read a byte no gate let it read fails
ordinary tests, not only the bit-flip ones.

**Every block parser asks a gate before it acts on bytes.** The scan's
parsers are free functions over a block and a `Gate`: `seek_block`,
`park_in`, `value_at`, `search_block_rev` and the reverse walk. Each
calls `check(from, to)` on a range before it acts on it: the count
word, the restart array, each probed restart's entry, each entry walked
(`off..next`), a run of padding, the entry whose value it copies. There
are three gates:

- `Open`, over a block read whole: every range inside the block holds.
- `Gated`, over a scan buffer: a range holds when its grains are valid.
  A range that is not is a miss.
- `CopyIn`, over a lent image: a range not yet taken is rounded out to
  grains, checked against the cache's RAM sums, and copied into the
  buffer right after the check holds. Only those grains are marked.

A parser may also name the entry after the one it parks on (`ahead`),
or the zero padding there when the head is the block's last entry. A
lend takes it then, so that the advance past the head finds it in the
buffer. The buffer gate ignores it. A lookahead entry that parses as
neither is left for the step that reaches it to report.

**`Blocks::load_with` drives every block read of both scan
directions.**

- A buffer that holds the block is parsed through `Gated`. If the parse
  misses, or fails while the buffer lacks any grain, `complete` takes
  the rest of the block: one lend that checks and copies each missing
  grain, kept only if the lend held, else a device read that replaces
  the buffer whole. The parse then runs again over the full buffer.
- Otherwise a cache that lends has the block parsed in place through
  `CopyIn` (`lend_into`). A parse that fails has the whole image
  checked before its error is believed (the ADR-0031 rule). The grains
  copied are kept only if the lend returned `Some(true)`.
- Otherwise, and on a miss or a failed check, the block is read from
  the device whole (`sstable::read_data_block`), and parsed through
  `Open`.

So an error is believed only from bytes that all passed a check. A
change in the cache's RAM is a miss and a device read; only damage the
CRC let through is `CorruptBlock`.

**Why a refill cannot launder a byte.** A check vouches only for the
image it has just summed, and only for the bytes copied from it right
then. It never vouches for bytes already in the buffer. A flip in a
grain the first lend did not read is not copied and not marked. A later
lend either finds a clean image and copies clean bytes, or sees the
flip and fails its check (a miss, then the device), or misses. Grains
gathered across lends compose because a block's content never changes
under its tag. Table ids are never reused, and a scan holds the `Db`
borrowed, or, in the host store, its tables pinned.

**What each read takes.**

- A forward seek positions with a binary search over the block's
  restarts, where it walked every entry from offset 0. It takes the
  trailer, the probed restarts, the run it walks and the entry after
  its head: a few grains of a raw block, in one lend.
- A cursor that crosses into its next block will read it through, so
  that block is taken whole in one lend.
- So is each block a reverse cursor steps back into, and each block
  `resolve_run` reads.
- The reverse cursor's own block, and each re-park on it, is searched
  for what the search reads.
- A compressed block is inflated whole, from a lend that checks its
  flag and its stream (`0..clen`) rather than the whole image.

**`CachePort::checks_ranges` is new.** It says whether `with_checked`
checks only the ranges it is asked about. The provided method answers
`false`, which matches the provided `with_checked` (whole image checked
up front). `BlockCache` answers `true` when it has slots, and so does
the host store's cache. When it is `false`, a scan takes each block
whole in its first lend. Otherwise every extra lend would pay another
whole-image check. A wrong answer costs time, never a byte.
`with_checked`'s contract gains one rule: a reader may keep a copy of a
range it checked once the lend has returned `Some(true)`, and nothing
copied from a lend that failed.

**One copy, and `read_data_block` is the device funnel.** Every block
the scan takes arrives in its buffer in one copy: from the cache, a
grain at a time or whole, or from the inflate. The lent-image path of
`sstable::read_data_block` (`inflate_lent`, `inflate_image`) is gone.
`read_data_block` reads a block from the device (or copies it out of a
cache that does not lend), checks its CRC, and leaves the logical block
in the caller's buffer. Both scan directions use it on a miss, and so
does compaction, which reads past the cache.

## Consequences

- **Cheaper seeks** (`docs/THROUGHPUT.md`, step 24, both builds on the
  same stores).
  - A seek takes 58% to 70% fewer instructions: two 16 KiB copies and
    a whole-block sum per data block were 63% to 72% of it.
  - At 1 M keys seeks are 80% to 86% faster on the hot range and about
    50% on the full range.
  - Point reads are level. `readseq` is level, and `readreverse` within
    3%, once the gate became an enum and a step over a held block a
    synchronous call.
- **Stats.** One lend is one lookup, as ADR-0031 has it.
  - A seek that yields one entry asks the cache once for the data block
    it lands in (`tests/data_flip_scan.rs` asserts it). It asks twice
    when its head is the block's last entry: the lookahead takes the
    padding after the head, so the step past it finds the block's end
    in the buffer and asks only for the next block. It also asks twice
    when its key is a block's first, as before: the index names the
    block before it.
  - A scan that reads past what its first lend took asks once more, to
    complete the block.
  - A cursor whose buffer the merge gave to another table asks again
    when the merge comes back, as before. That lend takes only the
    entry it needs and the one after it.
  - A miss is counted once, then read from the device.
- **RAM.** Each `ScanBlock` grows by 8 bytes, and so do `Scan` and
  `RevScan` (one `ScanBlock` each). The host store's 33 pool buffers
  per reader grow by 264 bytes. No new block buffer. A scan step's
  future holds the parse closure and the head it finds across the
  device read of a miss: on the ESP32-S3 profile `Scan::seek` and
  `Scan::next` futures grew by 312 bytes (to 1,224 and 1,368), and
  `RevScan`'s by 176 and 192 (to 1,464 and 1,472), in the first build.
  The synchronous step over a held block brought them back down (1,208
  and 1,352, 1,392 and 1,400, as measured when ADR-0036 landed). The
  profile's peak stayed at 114,208 bytes, set by `archive_commit`
  (`BUDGET.md`).
- **Less corruption found by a seek.** A seek used to parse every entry
  of its block before its key. Now it parses only the restarts it
  probes and the run it walks. A block whose CRC passed with a damaged
  entry earlier in the block is therefore no longer `CorruptBlock` at
  that seek. A scan that reads the entry still reports it. Entries the
  seek skips cannot change its answer.
- **Format.** Unchanged.
- **API.** `CachePort::checks_ranges` is new, with a provided method;
  existing implementations compile and behave as before.
- **Tests.**
  - `scan::tests`, over random raw blocks of 512, 1,000 and 4,096 bytes
    with version runs, tombstones, TTL puts and restarts every 1 to 20
    entries:
    - A seek through a lend's gate answers as a walk from offset 0
      does. Run again over the buffer, with every grain the lend did
      not take randomized, it needs nothing more. The value copy and
      the next 17 parks answer as over the intact block, or miss.
    - The reverse search holds the same over random bounds.
    - Padding is believed only where it was checked.
    - The grain arithmetic covers each block size.
  - `tests/data_flip_scan.rs` builds a store of several overlapping
    tables, raw and compressed.
    - A bit of every 97th byte (every 7th in release) is flipped in
      every cached data block. The flip is either stuck or present only
      on a block's first lend.
    - Under the flip run forward and reverse scans from six starts,
      taking 1, 2 and 17 entries, with and without a block pool, plus
      full scans both ways.
    - Every result equals the uncached scan's. None is `CorruptBlock`.
      Flips were lent, and some were rejected.
    - A limit-1 seek asks the cache for its index block and one data
      block, with no miss.
  - `tests/index_flip_scan.rs`'s cache now checks ranges too.
  - Each guard is shown to matter. Each of these mutants fails a test:
    - removing the buffer gate;
    - believing a buffer's error when the buffer is not full;
    - dropping the whole-image check on a lend's error;
    - copying the whole image at the first lend (the laundering
      variant above);
    - dropping the entry check, the restart-array check, the reverse
      walk's check, the padding check or the lookahead.

    Two more were found by the third review (below), and their tests
    added:
    - `complete` keeping a lend that returned `Some(false)` after every
      check it asked held, which the `with_checked` contract allows;
    - `load_with` lending a partial buffer's block again instead of
      completing it.

    Four mutants survive as equivalent:
    - copying before the check: a failed check fails the whole lend,
      and nothing is kept;
    - `victim` keeping `valid`: `valid` is meaningful only under a tag,
      and every tag sets it;
    - a lookahead taken by every gate: only a lend's does anything;
    - `held` parsing a full buffer through the buffer gate: the same
      answer, with one extra lookup on an error.

- **Review.** An adversarial review checked the gates byte by byte, the
  rule for believing an error, the `id` and `valid` bookkeeping, the
  compressed branch, `resolve_run`, the panics and caches that lie. It
  found one defect and reproduced it.
  - The rewritten reverse park moved its cursor to the earlier block
    before reading it. A device read of that block that failed left the
    cursor's head in one block and its block id in another, so a
    retried `prev` copied another entry's value.
  - The forward park had the same flaw, from before this change, when
    one park crossed two blocks: its first had no entry visible at the
    snapshot.
  - Both parks now keep the block they walk in locals, and write the
    cursor only when they have found its entry or its end.
  - `tests/scan_retry.rs` fails each device read of a clean scan in
    turn, forward and reverse, with and without a cache and a pool, at
    every entry visible and at a snapshot that hides whole blocks. Every
    retried scan must yield exactly the clean one. It fails on the code
    before the fix, both cases, and the forward case fails on `main`
    too.
  - A second round, of three reviews with a skeptic verifying each
    finding, found no wrong answer.
    - One compared both scan directions against a model of the store
      over random workloads. These had overlapping tables, version runs
      spanning several blocks, snapshots, TTL, range deletes, bounds,
      limits, re-seeks, pools of 0 to 5, compressed levels, six cache
      kinds (one lying about `checks_ranges`) and `BufferTooSmall`
      retries. Everything matched, on `main` too.
    - One drove `load_with`, `value_into` and `complete` directly at
      512, 1,000 and 4,096 bytes. It flipped one and two bits of every
      byte, stuck or on chosen lends only, and nothing changed an
      answer.
    - One fuzzed CRC-valid malformed blocks through 720,000 scans,
      which gave no panic, and checked the stats, the host store's pool
      reuse and the RAM numbers.
    - It found one cost regression. A seek onto a block's last entry
      did not take the padding after it, so the step past the head
      asked for the rest of the block only to find zeros: three
      data-block lookups where `main` took two. The lookahead now
      names the padding.
      `a_seek_onto_a_blocks_last_entry_takes_its_padding` fails without
      it.
  - Measuring then showed full scans 5% to 9% slower in wall clock, with
    flat instructions and fewer simulated cache misses. Two changes
    brought forward scans level and reverse scans to within 3%:
    - The gate became an enum, so a check over a whole buffer is a
      branch rather than a call.
    - A step over a block the scan holds runs synchronously
      (`Blocks::held`), so it builds no future carrying the device
      read.
  - A third round reviewed those changes and re-ran the mutants against
    them. It found no wrong answer, and found the two test gaps above.
    `loaded` now asserts, in debug builds, that no two buffers hold one
    block. `held` and `lend_into` fail closed if a parser ever swaps
    its gate.

## References

- [ADR-0016](0016-cache-hits-reverify-with-a-ram-checksum.md) (RAM
  checksums), [ADR-0019](0019-detached-scans-block-pool-lazy-seek.md)
  (block pool), [ADR-0027](0027-point-reads-inflate-to-the-key.md),
  [ADR-0031](0031-point-reads-check-what-they-read.md) (the checked
  lend), [ADR-0034](0034-index-blocks-end-in-restart-offsets.md)
- `src/scan.rs` (`ScanBlock::valid`, `Gate`, `Open`, `Gated`,
  `CopyIn`, `Blocks::load_with`, `lend_into`, `complete`,
  `seek_block`, `park_in`, `search_block_rev`), `src/cache.rs`
  (`CachePort::checks_ranges`), `src/sstable.rs` (`read_data_block`)
- [`docs/THROUGHPUT.md`](../THROUGHPUT.md), step 24
