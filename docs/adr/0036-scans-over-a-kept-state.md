# ADR-0036: A scan can be built in a state its caller keeps

Status: Accepted (unreleased, 2026-11-26)

## Context

A `Scan` holds its block buffers inline: a read scratch and a block
buffer of `BLOCK` bytes each, the end bound's and the memtable head's
key and value buffers, and a cursor for every table the manifest can
hold. Building one zeroes all of it. At the host store's shape (16 KiB
blocks, 128-byte keys, 4 KiB values, 4 × 7 tables) that is 44,000
bytes.

The host store serves a scan a page at a time. Each page copies part of
the memtable, takes the current manifest, and builds a detached scan
over them (ADR-0019) with `Box::new(KvScan::detached(..))`. That
zeroing was 37,300 instructions of every page: a fifth of a cached seek
at 1 M keys and over a third at 300 K, once ADR-0035 had taken the
rest of a seek's cost out (`docs/THROUGHPUT.md`, step 24).

A scan cannot simply be kept between pages. It borrows its memtable
page and manifest for its life, and the next page borrows others. Its
storage, though, borrows nothing.

## Decision

**A scan's storage can live apart from it.** `ScanState` holds a forward
scan's bulk storage: the read scratch and own block buffer, the key
buffers and the cursor pool. `RevScanState` holds a reverse scan's.
Both are public, lifetime-free, `const fn new()` and `Default`.
`Scan::detached_in(&mut state, memtable, manifest, device, cache,
node_id)` builds a detached scan in a kept state, and
`RevScan::detached_in` builds a reverse one. Inside, the scan reaches
its storage through a private `Held<'d, T>`, either `Own(T)` or
`Lent(&'d mut T)`. `Scan::new` and `Scan::detached` build `Own` and
behave as before. `db_types!` names the two states.

**What decides whether a byte of the state is read lives in the scan.**
That covers the key lengths, `has_end` and `has_lower`, the memtable
head's lengths and `mem_live`, `cursor_counts` and `level_base`, and
which block a buffer holds and which of its grains were checked
(ADR-0035). All of it starts fresh with each scan, so a scan in a kept
state starts as a new one does: whatever an earlier scan left in the
state is there only as bytes that nothing yet admits. Four guards keep
the block buffers honest:

- `Blocks::over` forgets the kept own buffer's block.
- `use_block_pool` forgets the pool's.
- A forward seek forgets every decoded block, as it did.
- A reverse seek now does too. Before this change it kept them, which
  was harmless while a reverse scan's buffers lived and died with it.

**A cursor is counted only once it is empty.** A forward seek counted a
table's cursor slot before reading the cursor into place. A seek that
failed there (a device error on the index block) left the slot counted
with whatever it held before. On a fresh scan that was an empty cursor.
On a kept state it was an earlier scan's head, which a `next` after the
failed seek would yield. The same was true of a re-seek of one scan,
from before this change. The slot is now emptied before it is counted.

**The host store keeps one forward and one reverse state per reader
thread.** They are thread-locals, built the first time a thread scans
each way, next to the pool of lent block buffers it already kept. Each
page builds its scan in them, in place, with no `Box`. A page that
fails, or panics, leaves the state as fit as any other.

## Alternatives

- **A public generic storage parameter** (`Scan<.., S: DerefMut<Target
  = ScanState>>`). It removes the branch on the owned path. But it adds
  a type parameter and a public `Inline` type, puts every scalar in the
  kept state (so correctness rests on a hand-kept list of fields to
  reset), lets a caller's `DerefMut` hand out a different state between
  calls, and grew the ESP32-S3 scan futures by 24 to 48 bytes.
- **An `OWN` const parameter**, where a scan without its own buffer
  takes its scratch and block from the lent pool. It still zeroed
  11 KiB a page, added a runtime error for a pool too small, and put
  `Option`s on the firmware's hot path.

## Consequences

- **Cheaper pages** (`docs/THROUGHPUT.md`, step 25). A host-store seek
  takes 39,400 fewer instructions at every size: −38% at 300 K keys on
  the hot range, −22% at 1 M. One-thread seeks are 8% to 12% faster in
  wall clock, four-thread seeks about 6%. Scans and point reads do not
  move.
- **RAM.**
  - The host store keeps 44,000 + 43,552 bytes per reader thread,
    beside its 512 KiB pool of lent block buffers.
  - A scan built in a kept state is still `size_of::<Scan>()`
    (`Held` is as large as `Own`), so it is built where it is used.
    Building it writes a few words.
  - On the ESP32-S3 profile `Scan` is 40 bytes smaller (10,872). The
    start key's copy, which nothing read, is gone. The futures are
    unchanged and the peak is 114,168 bytes.
- **The owned path pays a branch per access to its storage.** The
  firmware's scans (`Scan::new`) reach their storage through `Held`
  too. The hot loops take the reference once per call. The library's
  scan bench runs 1.0% more instructions than before (552.1 M against
  546.6 M; 2.25% before the loops were changed).
- **API.** `ScanState`, `RevScanState`, `Scan::detached_in`,
  `RevScan::detached_in` and two `db_types!` kinds are new. Nothing
  existing changes. There is no kept-state counterpart of `Scan::new`.
  A firmware scan over the `Db` keeps building its own: the case that
  pays is a host scanning a page at a time.
- **Tests.**
  - `tests/detached_scan.rs`:
    - One pair of states and one pool serve every page of every range,
      in both directions, with and without a cache, over two
      generations of the store.
    - Twin stores share block ids but hold other bytes, each with its
      own cache. 400 short scans alternate between them on one state.
    - A seek that fails part-way over a kept state, then a step, does
      what it does on a fresh scan, in both directions.
  - `tests/data_flip_scan.rs`: the bit-flip runs, stuck and
    first-lend-only, with every scan built in one kept state after a
    scan of a twin store through it.
  - The host store:
    - pages of every size between writes, flushes and compactions,
      checked against a model;
    - 60 rounds that rewrite every key into blocks earlier rounds' tables
      held;
    - states built once per thread.
  - `scan::tests`: `Blocks::over` forgets a kept buffer's block.
  - Each guard alone is backed by another. Removing both of a reverse
    scan's, or every guard, fails the tests above, and so does counting
    a slot before emptying it.
- **Review.** An adversarial review (two lenses, a skeptic for each
  finding) found and reproduced the cursor-slot defect above, and two
  tests that could not see a carried-over block. Both tests were made to
  see it. It also measured the owned-path branch cost, which the loops
  now pay once per call.

## References

- [ADR-0019](0019-detached-scans-block-pool-lazy-seek.md) (detached
  scans, the page and the pool),
  [ADR-0035](0035-scans-copy-only-what-they-check.md) (grains a buffer
  has checked)
- `src/scan.rs` (`Held`, `BlockBufs`, `ScanKeys`, `RevScanKeys`,
  `ScanState`, `RevScanState`, `Scan::detached_in`,
  `RevScan::detached_in`, `Blocks::over`, `add_cursor`),
  `src/macros.rs` (`db_types!`), `examples/kvstore/store.rs`
  (`SCAN_STATE`, `REV_SCAN_STATE`, `merge_fwd`, `merge_rev`)
- [`docs/THROUGHPUT.md`](../THROUGHPUT.md), step 25
