# Changelog

All notable changes to horton are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Entries up to
0.16.0 are extracted from the milestone log in `SPEC.md` §9. Dates are
given where SPEC records one.

Before 1.0 the on-disk format is not compatible across minor versions:
old images are rejected (for example as `CorruptManifest`), never
misread (SPEC §4.5).

## [Unreleased]

The next release is 0.20.0. **The on-disk format changes** (manifest
`hrtman10`, SSTable `hrtsst04`, WAL records with `SPAN` and `CONT`
flags): v0.19 images, and archives exported by v0.19, are rejected, not
misread. So are images written by a build from before this release
(`hrtman07` to `hrtman09`, `hrtsst03`); archives exported by those
builds (`hrtsst04`) still ingest.

Read throughput is the subject:
[`docs/THROUGHPUT.md`](docs/THROUGHPUT.md) measures horton's host
key-value store against RocksDB on db_bench's workloads, before and
after. Against 0.19, one point read is 5 times cheaper (10 for an
absent key), one thread's scan 9 times and its seek 7 times, and
unsynced writes 2.5 to 2.9 times; synced writes are unchanged. With
detached reads the host store's point reads, scans and seeks also run
on every reader thread at once, and with bloom filters sized to their
tables a read of an absent key stops at the filter, and takes no lock
on the way: at 1 M keys on four threads, point reads are 26 times
0.19's rate, reads of absent keys 620 times, seeks 60 times and scans
39 times. Compaction's merge runs outside the host store's lock, so
reads go on while it is written to, and two compaction jobs run at a
time: random writes at 1 M keys are at 0.7 of RocksDB's rate given
the same memory, from 0.37.

### Added

- **Atomic batches across WAL blocks** (ADR-0029). A `WriteBatch`, and
  a `write_group` unit, may span up to `GROUP_BLOCKS_MAX` (16) WAL
  blocks; recovery replays it only when every block of it is intact,
  whatever subset a crash leaves. Two op-byte flags, `SPAN` and `CONT`,
  mark where a group goes on and where it continues. A batch that does
  not fit 16 blocks, or an empty memtable, is `BatchTooLarge`; one the
  WAL region cannot take before its end is `WalFull` before any block is
  written, and the next flush wraps the region. `RecoverState` gains
  `blocks_read`. The kvstore's `Batch` may span blocks too.
- **Group commit across WAL blocks** (ADR-0028). `Db::write_group`
  commits many atomic units (`GroupOp` slices: each a client's batch,
  borrowed) under one device flush, each unit inside one WAL block, at
  most `GROUP_BLOCKS_MAX` blocks; nothing is visible before the flush
  returns. It commits the longest prefix that fits the memtable and the
  WAL and says how many (`GroupCommit`). The kvstore's writer queue
  commits through it: synced writes from 8 threads in batches of 50 take
  half the `fdatasync`s and run 22 to 26% faster.
- **Bloom filters that span blocks** (ADR-0020). `bloom_bytes` may now
  exceed a block. It is the most filter a table can have: the table
  writer builds the filter at that length and, when the table ends,
  folds it down to the fewest blocks that give each entry 10 bits
  (`BLOOM_BITS_PER_KEY`), so a small table still writes one bloom block
  and a large one is no longer starved. `BloomShape` is the size rule;
  `TableWriter::probes_for` picks the probe count for an expected entry
  count; `TableWriter::restart` and `set_block_limit` start a writer
  over in place. A configuration that fits one block behaves exactly as
  before.
- `TableRef::bloom_blocks`, `FinishedTable::bloom_blocks`,
  `TableReader::bloom_blocks` and `bloom_len`.
- **`TableReader::from_ref`** builds a reader from a table's manifest
  record, with no I/O (ADR-0021). `TableRef` gains `bloom_k` and
  `bloom_len`, and `bloom_first()` and `index_block()`;
  `FinishedTable` gains `bloom_k` and `bloom_len`; `TableReader` gains
  `bloom_k()`.
- **Detached point reads** (ADR-0018). `Db::probe_memtable` runs the
  memtable half of a point read and returns a `MemProbe`;
  `TableView::resolve` finishes it over a copy of `Db::manifest`, any
  handle to the device and a caller-owned `ReadScratch`, without the
  `Db`. A host can run the table half of many reads at once on other
  threads while one thread owns the `Db`; an async firmware can wait
  for flash without holding a borrow of the `Db`. The caller must keep
  `flush`, `compact_step`, `ingest_table` and `archive_commit` from
  running while a detached read is in its tables. `db_types!` takes
  `TableView`, `MemProbe` and `ReadScratch` aliases.
- `MemProbe::new`, `Db::probe_memtable_into` and `MemProbe::set_absent`
  (ADR-0023). A host keeps one probe per reader thread and has the
  memtable's part of a read written into it. And a host that can tell
  without the `Db` that the memtable holds nothing for a key (say,
  from a filter of its own over the keys written since the last flush)
  builds that probe itself with `set_absent`, so the read does not
  wait for whoever has the `Db`. The claim is the caller's: a false
  one answers from the tables alone.
- **Detached compaction merges** (ADR-0024).
  `Compaction::merge_detached` runs a job's merge quanta without the
  `Db`, over any handle to the device and a copy of `Db::manifest`;
  `Db::compact_step` selects the job, and seals and commits each
  output the merge fills. `Compaction::wants_merge` says when a
  quantum is next, `Detached` is what one returns, and
  `Compaction::abandon` gives a job up. A host runs the merge outside
  the lock that guards its `Db`. The caller must not run
  `request_compaction`, `ingest_table`, `archive_commit`, `open` or
  another scratch's `compact_step` while a merge is detached. The
  result is the tree `compact_step` alone leaves, block for block;
  firmware's `compact_step` loop is unchanged and the scratch is the
  size it was.
- **Two compaction jobs at a time** (ADR-0025).
  `Db::compact_step_in(lane, scratch)` steps the job of one of two
  lanes (`Lane`, `COMPACTION_LANES`), each with a scratch of its own;
  `Db::lane_target` says what level a lane's job writes to. Two jobs
  run side by side when they share no table and write to different
  levels; tight and requested jobs run alone. For a caller that steps
  both lanes, flush keeps a compaction reserve for each (four slots)
  while the region has seven slots or more above its bottom level,
  and under region pressure level 0 is merged before a deeper table is
  pushed down. `compact_step` is unchanged: one job, one reserve,
  deepest first, and when it selects a job it abandons every lane's.
  The ESP32-S3 profile's `Db` is 40 bytes larger and its
  `compact_step` future 160.
- **Detached scans** (ADR-0019). `Scan::detached` and
  `RevScan::detached` scan without the `Db`, over a copy of part of the
  memtable made by `Db::copy_memtable_range` / `copy_memtable_range_rev`
  (`MemTable::copy_range` / `copy_range_rev`), which return the horizon
  a page must stop at.
- **A block pool for scans** (ADR-0019). `Scan::use_block_pool` and
  `RevScan::use_block_pool` lend a scan caller-owned `ScanBlock`
  buffers, so a table's block stays decoded while the merge is at
  another table. Without a pool a scan reloads a block each time it
  comes back to a table, which was most of a scan's time over
  overlapping tables.

- `BlockCache::get_range_into` (and `CachePort::get_range_into`): copy a
  byte range of a cached block, checking only the chunks of the image
  the range touches. `cache::image_sum` is the RAM checksum behind it;
  `CacheStats::rejected` counts hits whose image had changed in RAM
  (ADR-0016).
- `sstable::BloomProbe` and `BLOOM_BUCKET_BYTES`: the 64-byte bucket of
  a bloom filter that one key's probes fall in, for a reader that holds
  only part of the filter (ADR-0017).
- `examples/kvstore/vs-rocksdb.sh` runs db_bench's workloads on RocksDB
  and on the kvstore and prints the rates side by side.

- `horton::fingerprint`: `Fingerprint` (a count and a mergeable
  multiset hash of key-value pairs), `fingerprint` and
  `fingerprint_buckets` over a positioned `Scan`. Equal visible contents
  give equal fingerprints whatever the write history, flushes or
  compactions; per-range fingerprints merge, for bucketed anti-entropy
  between a device and its replica.
- `horton::diff`: `Diff` merge-joins two positioned ascending sources
  and yields a `Delta` (`OnlyLeft`, `OnlyRight`, `Differs`) for every key
  whose visible value differs, in one pass over four caller-owned
  buffers. `SortedEntries` is the source trait; `Scan` implements it.
  After `fingerprint_buckets` finds the bucket that disagrees, a diff of
  that bucket is the exact repair list.
- `Db::request_compaction(level)` compacts every table from `level` down
  to the bottom once, also below the level triggers. Bottom tables are
  rewritten in place, so `purge_before` and range tombstones free space
  there (issue #32, ADR-0015).
- `model_tight_commit_ok`, `model_tight_job` and `TightJob` in
  `horton::model`.
- The kvstore `compact` command uses `request_compaction(0)`.
- **`CachePort::with_range`** and `BlockCache::with_range` lend a
  checked range of a cached image to a closure in place, with no copy
  (ADR-0026). The index search and the inflate of a compressed data
  block run that way. It is a provided method whose default (`None`:
  "I do not lend") keeps the copying path, so existing `CachePort`
  implementations work unchanged. The kvstore's `SharedCache` lends.

### Changed

- **A point read inflates a data block only as far as its key**
  (ADR-0027). The LZ77 decoder can stop and resume; a point lookup
  scans a compressed block's entries as it inflates them and stops at
  the answer, leaving what the scan cannot decide (the end of the
  entries) to the whole-block search. Same answers, about half the
  inflating; `readrandom` on one thread 9% faster. A raw block is
  searched where it lies in the cache.
- **The hot path, measured again** (ADR-0026). No format change: every
  CRC is the same value, and the LZ77 stream format is unchanged (images
  written now decode on older versions and the other way round).
  - CRC-32 runs as three interleaved chains on 64-bit targets, and
    carries its register across runs of zero bytes with one
    multiplication instead of computing through them: 2.6 times faster
    on a 16 KiB block, 3.7 on a block that is half zeros (most
    compressed blocks). A hardware CRC is not possible under
    `#![forbid(unsafe_code)]`.
  - The LZ77 encoder is 1.45 times faster (a match is counted 8 bytes
    at a time, a long match is no longer indexed byte by byte, and runs
    with no match are stepped over faster) and its output about 2%
    smaller. The decoder is 1.4 times faster (short runs copied as one
    16-byte chunk, one-byte runs filled, overlapping matches doubled).
  - A cache hit on a compressed data block is inflated where it lies in
    the cache, and the index is searched there: no 16 KiB copy per hit.
    One block read is still one cache lookup, counted once.
  - A point read no longer zeroes a `VAL_MAX` buffer for every table it
    looks into.
  - The memtable keeps slots in arrival order behind a sorted view of
    4-byte positions: a random insert into a 4,096-entry memtable takes
    0.34 µs, where shifting the 32-byte slots took 0.77. The view costs
    4 bytes an entry (64 bytes on the ESP32-S3 profile; BUDGET.md).
  - kvstore at 1 M keys, four threads, against the build before (mean
    of two pairs): `fillseq` 1.28×, `fillrandom` 1.34×, `overwrite`
    1.41×, `readrandom` 1.63×, `readmissing` 1.41×, `readseq` 1.24×,
    `readreverse` 1.32×, `readwhilewriting` 1.56×, `deleterandom`
    1.28× (`docs/THROUGHPUT.md`, step 16).

- **Block-cache hits no longer re-run CRC-32** (ADR-0016, superseding
  ADR-0014). Only blocks whose CRC matched are cached, and a hit is
  checked against a RAM checksum of the cached image instead: about 6
  times cheaper on a 64-bit host, with no lookup tables. A bit that
  flips in a cached image is still never served; it now costs one
  re-read from the device where it used to return `CorruptBlock` on
  every hit. A block that fails its CRC is never cached, so each read
  of it reaches the device and reports it.
- **The bloom filter is bucketed** (ADR-0017): a key's probes all fall
  in one 64-byte bucket, and a point read fetches that bucket and the
  footer's 45 bytes from the cache instead of two whole blocks. A probe
  of a table that does not hold the key costs well under a microsecond
  once cached.
  False positives rise slightly (0.84% to 0.98% at 10 bits per key).
- **A block cache above 8 slots is hashed**: a block's slot is one of 8
  chosen from its key, so lookups and inserts no longer scan every
  slot. Caches of up to 8 slots (every firmware profile) keep one CLOCK
  hand over all slots, as before.
- An index-block search parses the index once. It used to re-parse it
  from the start for every step of a binary search, about 2,000 entry
  parses for a full 16 KiB index.
- `CachePort` has a new required method, `get_range_into`, and
  `CacheStats` a new field, `rejected`.
- The ESP32-S3 profile's `Db` grows by 264 bytes (the cache's per-chunk
  sums); the RAM budget is unchanged.
- **Reads, scans and compaction no longer read table footers**
  (ADR-0021). The manifest record names every section of a table and
  its filter's shape, so a point read that the filter turns away is
  one read of one bloom bucket, and a seek reads one block fewer per
  table. A damaged footer no longer fails a read; it fails a
  re-attach (`ingest_table`), which is where a footer is still read.
  A record whose filter length disagrees with its bloom blocks is
  `CorruptManifest`. `sstable::footer_index_block` is gone.
- **The table footer records the filter's length** (`bloom_len`), and
  the manifest each table's bloom-block count. A reader takes the
  filter's size from the table, not from its own `BLOOM_BYTES`, and
  refuses a footer whose length and block ids disagree.
  `TableRef::data_blocks` is `block_count - rdel_blocks - bloom_blocks
  - 2`. A table reference is 7 bytes longer on the device and 8 in RAM.
- `BLOOM_BYTES` is no longer limited to `BLOCK - 4`.
- A bloom probe's bit positions in a whole bucket are reduced with a
  mask where they took a 64-bit division each. The positions are the
  same.
- The kvstore's `stats` prints the bloom blocks its tables hold, and
  its `bench` takes `--read-threads N`, so reads can be measured over
  tables that fills on one thread always leave the same.
- A compaction job reuses its table writer in place for each output.
- **The kvstore's block cache is its own, and a hit in it takes no
  lock and writes nothing shared** (`examples/kvstore/cache.rs`,
  ADR-0022). Entries are immutable; each reader thread keeps a view of
  the slots it uses and checks one atomic epoch per slot; the reference
  bit is tested before it is set; counters are per thread. Hits are
  still checked against a RAM checksum (ADR-0016), now kept per 256
  bytes. It replaces the library's `BlockCache` in eight locked
  shards.
- The kvstore's `bench` has `readoutside`: reads of keys past every
  table, which touch no table and no cache, to tell the store's own
  cost per read from the tables'.
- **kvstore point reads do not queue, and most take no lock**
  (`examples/kvstore/gate.rs`, ADR-0023). A read takes the engine lock
  itself for its memtable probe; the request queue carries only
  writes. A read of a key the memtable cannot hold takes no lock at
  all: a filter over the keys written since the last flush, the
  manifest's published sequence number and a pin in the reader
  thread's own slot tell it so, and it goes straight to the tables.
  Flush and each compaction step shut that gate and wait for the pins.
  A key written since the last flush, and every key after a
  `delete_range` or a recovery until the next flush, still asks the
  memtable under the lock. `stats` counts both kinds of read.
- **kvstore compaction merges outside the engine lock**
  (`examples/kvstore/handoff.rs`, ADR-0024). The compaction thread
  has a handle of its own to the file. A step under the engine lock
  selects a job, or seals and commits an output; the scratch is then
  lent to the compaction thread, which merges with no lock and without
  shutting the readers' gate, and gives it back. `stats` counts the
  steps that ran outside the lock.
- **A kvstore write that waits for compaction gives the engine lock
  up.** When level 0 or the table region is full, the group at the
  head of the queue is kept there and its thread lets go of the lock
  until compaction has made room. Reads that need the memtable are no
  longer held for the length of a compaction job.
- **The kvstore runs two compaction jobs at a time** (ADR-0025): a
  compaction thread, a scratch and a file handle for each of horton's
  two lanes. The steps with the database are taken for both lanes by
  whichever thread has the engine lock, and a lane that found no job
  is not asked again until the tables or the other lane's job change.
  `stats` counts the merges that began while another was running, and
  `bench` reports them with each fill.
- The kvstore's `bench` has db_bench's `readwhilewriting`: reader
  threads read random keys while one more thread overwrites.
- The kvstore's `bench` builds `readmissing`'s keys on the stack. The
  allocation per key had become a third of the loop, and a lock that
  four reader threads waited on.
- **kvstore tables carry up to sixteen bloom blocks** (`bloom_bytes` is
  255 KiB), and the store boxes its flush future, which holds the
  writer and so the filter.
- **A forward seek reads the tables that hold its key** (ADR-0019). A
  table that starts after the seek key is read when the scan reaches
  it. A seek followed by a few entries no longer reads a footer, an
  index and a data block of every table in range. An error in a table
  the scan has not reached is now reported by the `next` that gets
  there, not by `seek`; it is still reported before anything of that
  table is yielded.
- `Db::node_id` is public.
- `Db::get` and its variants run the same table phase as a detached
  read; the `get()` future is 152 bytes larger (the ESP32-S3 peak is
  unchanged).
- A memtable counts its range tombstones, so a point read no longer
  walks every slot to learn that none covers the key.
- **kvstore point reads, scans and seeks run in parallel.** The
  memtable's part (a probe, or a page's slice) is taken under the
  engine lock; the caller then reads the tables on its own thread,
  through a read-only handle to the file and a 16 MiB cache shared by
  all readers. Scans
  lend each page 32 block buffers. Flush and compaction wait for
  readers in their table phase to leave. The `Db`'s own cache is off:
  no read goes through the `Db` any more. `bench`'s `readseq` and
  `readreverse` run one scan per client thread.
- **kvstore** runs horton under one lock instead of on a store thread.
  Writes are queued and committed in order by whichever thread holds
  the lock (flat combining, which gives group commit), and waiters
  spin for 100 µs before they park. Dropping the `Store` closes it: surviving handles
  return `Closed`.
- `compaction_pending()` is also true while a request has work.
- `compact_step` can return `RegionFull` from a tight job that cannot
  free a slot. It changes nothing then.

### Fixed

- **An image of another format version is rejected even when one of its
  manifest copies is blank.** Such a copy read as damaged, and beside a
  blank one the device read as fresh: the WAL replayed over tables the
  build could not see. An intact copy of another version (`hrtman`, other
  digits, a valid CRC under that version's layout) now rejects the
  image; a copy torn inside its magic still loses to the other copy
  (ADR-0029).
- **A WAL block write that reports an error consumes its sequence
  numbers.** Such a write may still reach the medium; reusing its
  numbers could let a later batch continue a failed batch's span
  (ADR-0029).
- **A crash can no longer bring back a write that recovery dropped.**
  A group of WAL blocks written before one flush (`write_group`, and
  `WalWriter::append_batch`) can be left with a later block on the
  medium and an earlier one not. Recovery stopped at the earlier one,
  but once the next write filled it, a following recovery would read
  on into the later block. Open now zeroes such stranded blocks
  (ADR-0028).
- **A failed write can no longer make the database unopenable.** A
  commit that failed after its block reached the device (a put whose
  flush failed) left a record the memtable did not count; enough writes
  after it, and the next open replayed more than the memtable holds and
  failed with `CorruptWal` every time. Capacity checks now count such
  records until a flush moves past them, and the next write flushes the
  device before adding blocks (ADR-0028).
- New writes after a recovery are numbered above every record left past
  the recovered log, so bytes a torn rewrite leaves behind can never
  replay as a newer write (ADR-0028).
- The kvstore's deadlock guard in its tests allows five minutes in an
  unoptimized build. One test took 52 s of its one-minute limit alone,
  and timed out beside the rest of `cargo test`.
- A full table region no longer stops all writes. When no job fits its slot
  estimate, a *tight* job runs with one free slot and never grows the
  region. A region full of expired or deleted data now gives its space
  back; before, every write failed with `RegionFull` (issue #32).
- The last output of a compaction job no longer claims a key range past
  its inputs. Its range-tombstone end could be the next table's first
  key, so two tables of one level overlapped.
- The profiling profile is `valgrind` (it was `valgriind`). The README's
  callgrind line uses it, so the benches keep their debug symbols.

## [0.18.0] - 2026-10-09

### Changed

- Release builds use fat LTO, `opt-level = 3` and one codegen unit. The
  `valgrind` profile keeps release code with debug symbols for profiling.
  Profiles do not reach dependents: the library and its on-disk format
  are as in 0.17.0.

## [0.17.0] - 2026-10-09

The first release on crates.io.

Fixes every finding of the v0.16 architecture review
([`docs/ARCHITECTURE_REVIEW.md`](docs/ARCHITECTURE_REVIEW.md)). Each
confirmed defect has a regression test in `tests/review_findings.rs`; CI
fails if one is ever `#[ignore]`d again. The on-disk format changes
(manifest `hrtman05`, SSTable `hrtsst02`): v0.16 images are rejected, not
misread.

### Added

- **Key-value store example** (`examples/kvstore`): horton on a host the
  way LevelDB is used. A file-backed `BlockDevice` (`pread`/`pwrite`,
  `fdatasync`), a store thread that owns the `Db` behind a cloneable
  handle, group commit of concurrent writers into one WAL block, capacity
  errors handled and compaction run in the background, snapshots and
  paging iterators. `demo` tours the API, `put`/`get`/`scan`/… make it a
  command-line store, `bench` runs db_bench's workloads, and `crash` kills
  a multi-threaded writer process mid-write and checks the reopened store
  against every acknowledged write. CI runs all of it on Linux, macOS and
  Windows. The README lists where horton stops at host scale.
- **Ground station example** (`examples/ground_station`): the flight
  recorder's horton compiled to `wasm32-unknown-unknown` as a `no_std`,
  import-free `cdylib`, and a web page that opens a recorder's flash dump
  in the browser. It runs the device's recovery, checks every entry
  against the recorder's key hash, and charts the sensors. CI records a
  flight, reads it (and one killed three times with SIGKILL), and drives
  the page in headless Chromium.
- **Live telemetry** (`examples/ground_station/live`, `web/live.html`):
  the recorder's horton logging a live stream in a browser Web Worker.
  Its flash is an OPFS file behind the sync access handle, and cold
  tables go to IndexedDB through the archive API (stage, a `strict`
  IndexedDB transaction, then `archive_commit`). The simulated device
  resends every frame not yet acknowledged. A Node test cuts the power
  300 times (torn in-place writes, and cuts on either side of an archive
  store) and proves every durable tick whole across flash and archive; a
  Chromium test kills the Worker, then crashes the tab, and proves nothing
  acknowledged was lost. The logger's flash opens in the dump viewer, and
  the native recorder's `restore` reads its archive.
- The flight recorder's formats (`db_types!` shape, layout, key schema,
  archive object) live in `examples/flight_recorder/format.rs`, shared
  with the ground station and the live logger so none can drift apart.

### Fixed

- **F1** Compaction output could outgrow its reservation and overwrite
  a live table. Tables now live one per fixed slot, and every writer is
  capped at its slot (ADR-0009).
- **F2** A deleted key could come back after a WAL wrap and reopen. The
  manifest persists the WAL replay floor and a sequence high-water mark.
- **F3** `open()` failed after ordinary compaction when the free list
  filled. There is no free list any more: the slot map is rebuilt from
  the manifest.
- **F4** Writes before `open()` could destroy acknowledged data. Every
  device-touching call before `open()` returns `Error::NotOpen`.
- **F5** `RevScan` could yield an older memtable version of a key.
- **F6** Writes stopped with about 4% of the table region used.
  Compaction splits outputs, commits each one, moves and consolidates
  tables, and pushes down under pressure; writes now continue until most
  of the region holds live data (ADR-0010).
- **F7** The manifest had to fit one block (`TestDb` stopped at 7 of 28
  tables). Manifest copies span `Manifest::max_blocks` blocks (ADR-0011).
- **F12** A sequence-0 entry wedged `Scan::next`.
- **F13** A data block failing its CRC read as "absent" on point reads,
  surfacing stale values. It is `CorruptBlock` now.
- **F14** A flush during an in-flight compaction could take the job's
  output blocks. Compaction reserves its output slot.
- **F15** Compaction dropped a bottommost tombstone that an older
  re-ingested table still needed.
- **F16** (found while fixing F6) `delete_range` never gave space back:
  compaction kept every version a range tombstone hid, and the tombstone,
  forever. A merge now drops versions hidden from every reader by a range
  tombstone in the job, and a bottommost merge drops the tombstone once
  nothing it hides is left. Merges also clip each input's range
  tombstones to its live lower bound, so a narrowed table's dead
  tombstones are never re-emitted.
- **F18** A torn WAL block could replay part of a `WriteBatch`: the
  batch's first records passed their CRCs. Batch records are now grouped
  (the op byte's `MORE` bit) and recovery replays a group only when its
  closing record is intact (found by the flight recorder's power-cut test).
- **F19** When a torn WAL block still held valid records, writes made
  after the reopen could be lost at the next reopen: recovery stopped at
  the torn block. A torn batch block is now overwritten, and recovery reads
  past a torn block when the next block holds newer records.
- **F17** WAL writes after a reopen could be silently lost (found by the
  lifecycle fuzzer).
- A newer range tombstone could be dropped in favour of an older,
  identical one when merging.
- **F20** `Scan` and `RevScan` panicked when a level below 0 held more
  than `TABLES` tables, which the manifest allows once region pressure
  pushes tables down (found by the live logger). Their cursors are one
  pool now, level-major.
- **F21** On a device that overwrites in place (a file, an SD card, a
  disk), a batch torn over an older batch of the same shape recovered in
  part: the older batch's closing record, left behind the tear, closed
  the torn group (found by the live logger's power-cut test). Recovery
  ends a block at the first record no newer than the one before it.
- **F22** Overlapping range deletes wedged compaction for good (found by
  the kvstore example's crash test). Past four tombstones overlapping at
  a key the merge kept every range tombstone of the job, so a range
  deleted again and again piled them up; and every job reserved room for
  all of them at the `12 + 2 * KEY_MAX`-byte worst case. Once that
  outgrew a slot, every L0 job failed with `TableTooLarge` before it
  began, level 0 stayed full, and no flush could land. The merge now
  tracks only tombstones no newer, wider one dominates (eight at once,
  +632 bytes of `Compaction`), and where it still overflows it keeps only
  the tombstones reaching there. The reservation counts the tombstones'
  real bytes, plus what clipping at the output bounds can add.
- The flight recorder's `restore` failed on archives of more than a few
  dozen tables (`IngestConflict`, then `RegionFull`): it ingested the
  whole history into one database, whose shape has 16 table slots. It
  reads each table back through its own database now, as `verify` does.
- The ground station counted frames split with the archive as torn
  unless they were the oldest on flash; a table boundary can split any
  frame. A prefix or a suffix of a frame's sensors is now a split, and
  anything else is torn.
- The `multiwriter` feature did not build once v0.17 landed: the drainer
  and writer still used `Error::NoSpace` and `Db`'s removed `FREELIST`
  parameter. The `loom` ring model did not compile either.

### Added

- `Config::with_manifest_ring(n)`: rotate manifest commits over `n`
  copies for NOR endurance (ADR-0012).
- `ManifestLayout`, `ManifestEdit`, `Manifest::commit_edit`,
  `stage_remove`, `stage_narrow`, `apply_edit`, `narrow_table`,
  `KeyBound::successor`.
- `db_types!`: declare `Db`/`Scan`/`RevScan`/`Compaction` aliases with
  named parameters.
- `Db::slot_stats`, `Db::check_invariants` (the lifecycle fuzzer checks
  it after every operation).
- `Error::widen`: converts a `WriteBatch` or `MemTable` error
  (`Error<Infallible>`) to any device's error type.
- Errors `WalFull`, `NeedsCompaction`, `RegionFull`, `SnapshotLimit`,
  `ManifestFull`, `TableTooLarge`, `CounterExhausted`, `BadLevel`,
  `BadConfig`, `NotOpen`, `Busy`.
- `tests/lifecycle.rs`: a differential fuzzer over long operation
  sequences with reopen, across three geometries.
- The RAM gate measures every public future (`tests/profile.rs`).
- CI (fmt, clippy pedantic + nursery, rustdoc, debug and release tests,
  miri subset, bench build), a pinned toolchain, license texts, ADRs.
- A way in for newcomers: `db_types!` needs only `block`, `key_max`
  and `val_max`; the tuning parameters are optional and take the
  defaults in `horton::defaults`. `Config::whole_device(n)` lays the
  manifest, WAL and tables out over blocks `0..n`,
  `Db::MIN_DEVICE_BLOCKS` is the smallest `n` a shape fits, and
  `Db::config` shows the layout in use. The README's "Sizing" section
  explains every parameter and region in plain terms, and the quick start
  prints its RAM use and layout.
- CI builds, lints and tests the `multiwriter` and `loom` features.
- `examples/flight_recorder`: a sensor logger on simulated NOR flash that
  exercises the whole API, survives `kill -9` and simulated power cuts
  (`torture N`, run in CI), and streams cold tables to a directory or to
  S3 (`--s3`, signed by `curl --aws-sigv4`), with a `restore` ground
  station that re-ingests the archive.
- `FlashBlockDevice::flash` and `into_flash`: reach the chip behind the
  device.
- `Error` implements `Display` and `core::error::Error`.
- `Db::tables(level)`: the sealed tables of a level, for host-side
  archive and replication loops.
- Replica-side last-writer-wins: `TableRef` records `node_id` and
  `seal_wall`, `Config::with_node_id`, `Db::stamp_table`, and
  `Error::StampConflict`. Reads merge versions across nodes by
  `(seal_wall, node_id)`; the manifest magic is now `hrtman06`.
- The `multiwriter` ring carries range deletes and TTL puts
  (`WriteBatch::range_delete`, `WriteBatch::put_ttl`).
- Package metadata for crates.io (`repository`, `rust-version = "1.88"`,
  keywords, categories), a docs.rs build with `multiwriter`, and CI jobs
  for the MSRV and `cargo publish --dry-run`.

### Changed

- **Breaking:** `Error::NoSpace` is gone; each capacity condition has
  its own variant naming the remedy (ADR-0013).
- Multiwriter errors (feature `multiwriter`): a full ring at claim time
  is `Error::RingFull` (let the drainer sweep, then retry), and a ring
  payload the drainer cannot decode is `Error::BadPayload` (the drainer
  poisons). A batch error during a drainer sweep keeps its own variant.
- **Breaking:** `Config::new`'s second manifest position must leave room
  for a whole copy (`Manifest::max_blocks` blocks); `open()` rejects
  overlapping regions with `BadConfig`.
- **Breaking:** two `get` futures polled concurrently on one `Db`: the
  second returns `Error::Busy` instead of using fallback buffers.
- **Breaking:** a shape whose `memtable_arena` is smaller than
  `key_max + val_max` no longer compiles: a maximal write could never
  fit, not even right after a flush.
- **Breaking:** `Config` has a new public field, `device_blocks` (0 for
  a hand-placed layout); code that builds a `Config` literal must set it.
- **Breaking:** `horton::alloc` is renamed `horton::slots` (it shadowed
  the `alloc` crate). `model` and the low-level table builders
  (`plan_table`, `write_table`, …) are `#[doc(hidden)]`.
- `compaction_pending()` also reports an in-flight job.
- The table region is `LEVELS × TABLES` fixed slots (at most 64).
- Futures shrank: `flush` 29.8 → 17.6 KiB, `get` 9.1 → 1.1 KiB,
  `compact_step` 6.6 → 2.1 KiB, `ingest_table` 6.3 → 0.6 KiB,
  `Scan::next` 4.6 → 0.7 KiB, `RevScan::prev` 4.6 → 1.1 KiB. Manifest
  commits stage a small edit instead of a manifest copy.
- Scans' range-tombstone check skips tables whose `max_seq` cannot beat
  the winning version and stops at the first hit.
- The ESP32-S3 budget is 112 KiB and now covers structs plus peak
  futures (112,256 bytes measured).
- `db.rs` is split into `db/{mod,read,flush,archive,compaction,invariants}.rs`;
  point reads and the archive resurrection review share one read rule,
  the single-op writes share one path, every data-block read goes
  through `sstable::read_data_block`, and both scan directions share one
  range-tombstone lookup.
- `SPEC.md` is now the normative spec only (rewritten for v0.17); its
  milestone log moved to `docs/history/milestones-v0.1-v0.16.md`.
- **Breaking:** `Error` is `#[non_exhaustive]`. A `match` on it needs a
  `_` arm.
- **Breaking:** the `loom` feature is gone. Loom models run with
  `RUSTFLAGS="--cfg horton_loom"` and `--features multiwriter`, so Loom
  is never part of a normal build.
- **Breaking:** the unused `scratch-bump` feature is gone.

## [0.16.0] - 2026-09-23

### Added

- Caller-owned SSTable block cache, `cache::BlockCache<BLOCK, SLOTS>`,
  held inside `Db` with `CACHE` slots. `CACHE = 0` disables it.
- The cache serves point reads, forward scans and reverse scans for
  data, index, bloom, footer and range-tombstone blocks. WAL, manifest
  and compaction-merge reads bypass it.
- CLOCK (second-chance) eviction. Data blocks loaded by a scan insert
  cold, so a scan does not push out the point-read hot set.
- Entries are keyed by (table id, device block id); compaction
  invalidates the entries of the tables it drops.
- `Db::cache_stats` reports hits and misses.

### Changed

- The ESP32-S3 profile uses a 2-slot cache. It totals 92,520 bytes
  (`Db` 25,576 + `Scan` 10,536 + `Compaction` 56,408), within the
  98,304-byte budget.

## [0.15.0]

### Added

- `Db::delete_range(start, end)` writes a range tombstone that shadows
  every key in `[start, end)` with one sequence number.
- `Db::put_with_ttl(key, val, expire_at)`. Reads take a caller-supplied
  `now` and suppress values with `expire_at <= now`; horton owns no
  clock, and the existing read APIs use `now = 0`.
- TTL purge in compaction through `Compaction::purge_before`: an expired
  value becomes a point tombstone at the same sequence number.
- Range tombstones flow through the WAL, memtable, SSTables, point
  reads, both scan directions, flush, archive/ingest and compaction.
- `model_visible`, an executable model of the winner rule with range
  tombstones and TTL.

### Changed

- WAL records gain `Op::RangeDelete = 3` and `Op::PutTtl = 4`. Existing
  record kinds are byte-identical to 0.14.
- SSTables gain a range-tombstone section before the data blocks, and
  the footer and `TableRef` gain `rdel_blocks`. Data entries use op `4`
  for TTL values.
- `archive_commit` refuses a table carrying range tombstones
  (`WouldResurrect`) unless they provably shadow nothing outside it.

## [0.14.0] - 2026-09-23

### Added

- `RevScan`, the descending mirror of `Scan`: `seek_prev(from, lower,
  max_seq)` positions at the last entry `<= from`, and `prev` yields
  entries in descending key order.
- The same merge rules as `Scan`: highest sequence wins, tombstones are
  skipped, snapshot watermarks are respected, and `BufferTooSmall` is
  returned before any cursor advances.
- Version runs that straddle data blocks are followed backward, so the
  newest visible version of a key wins.
- `sstable::index_last_le_block` finds the last block whose first key
  is `<= from`.

## [0.13.0] - 2026-09-23

### Added

- Hand-rolled LZ77 block compression in `src/compress.rs`, with a
  caller-owned `CompressScratch<BLOCK>`. No dependency, no allocation.
- Each sealed data block is trial-compressed and kept compressed only if
  that saves at least 128 bytes (`COMPRESS_MIN_SAVING`).
- Compressed blocks are flagged by bit 15 of the restart-count trailer.
  The raw layout is unchanged, so pre-0.13 tables read unchanged.

### Changed

- `TableWriter::push`/`finish` and `write_table` take
  `Option<&mut CompressScratch<BLOCK>>`; `None` stores raw.
- `Db` gains a one-block `decomp_scratch` buffer for reads.
- The ESP32-S3 RAM budget is raised from 64 KiB to 96 KiB.

## [0.12.0]

### Added

- `Db::ingest_table` re-attaches an archived table. It copies the blocks
  from a caller `BlockDevice` source, relocates their block pointers,
  verifies CRCs, footer and entry count, and grafts the table into L0
  in one atomic manifest commit.
- Ingest is idempotent: `Ok(false)` when the same table is already
  attached, and `Error::IngestConflict` when the id is attached with a
  different shape.
- `ArchivePlan::sealed` gives the placement-free `SealedTable`
  descriptor that ingest takes.
- Combined remote/local reads: a re-attached table is an ordinary L0
  table, so highest-sequence-wins applies across both.

### Changed

- `archive_commit` enforces the tombstone rule itself. It refuses, with
  `Error::WouldResurrect { table }`, any removal that would resurrect a
  deleted key in the live view or a live snapshot. This replaces 0.10's
  documented caller discipline, which missed same-level and shallower
  tables.

## [0.11.0]

### Added

- `WriteBatch<KEY_MAX, VAL_MAX, OPS>`: a caller-owned, fixed-capacity
  batch of puts and deletes.
- `Db::write(batch)` applies a batch atomically with a single WAL block
  write and returns the base sequence number. An empty batch is a no-op.
- `Error::BatchTooLarge` for a batch larger than one WAL block. All
  validation happens up front, so a rejected batch leaves no trace.

### Fixed

- A failed `put` left its WAL record staged and reused its sequence
  number, so the mutation could resurrect through a later commit.
  `put`, `delete` and `write` now roll the stage back when the block
  write fails.

## [0.10.0]

### Added

- Archive API: `Db::level_tables`, `Db::archive_plan` (returns an
  `ArchivePlan`), and `Db::archive_commit`, which drops the table from
  the manifest in one atomic write and then reclaims its blocks.
- `Db::device()` is public so the caller can stream a table's blocks to
  its own remote sink. horton performs no networking.
- `archive_commit` is idempotent: `Ok(false)` when the table is already
  gone.
- Archiving tables that carry tombstones relies on a documented caller
  discipline (delete-bearing workloads archive only from the bottommost
  level). 0.12 replaces it with an enforced check.

### Changed

- Rust edition 2021 to 2024.

## [0.9.0]

### Added

- miri over the test suite. The crate forbids `unsafe`, so this is a
  pure UB check. Two exclusions (the `esp32s3` test binary and one fuzz
  test) are sandbox hangs, not test failures.
- In-tree, dependency-free fuzzers for the WAL, SSTable and manifest
  decoders. Bit flips, truncations and splices must yield clean errors,
  never a panic.
- Crash injection over compaction merges and manifest commits, not only
  flush: recovery must see exactly the pre- or post-commit state.

## [0.8.0]

### Added

- `Db::compaction_pending()` reports whether a compaction job is
  selectable.

### Changed

- Compaction works on every level. It takes the deepest full level; a
  full level deeper than L0 compacts its oldest table plus the
  overlapping tables in the next level.
- `NoSpace` is decided at select time, before any merge I/O, and only
  for a full bottommost level whose tables the merge does not absorb.
  A full L1 now drains into L2.
- `compact_step` returns `Progress::Done` when no job is selected.

### Fixed

- `SpiFlash::erase_sector` restores `CTRL` on every error path. A
  sector-erase timeout used to leave it cleared, destroying the
  boot-configured read mode.

## [0.7.0]

### Added

- ESP32-S3 SPI flash driver `SpiFlash<B: RegBus>` (`src/esp32s3.rs`),
  generic over a register-bus trait so all of its logic is safe code.
  The volatile MMIO adapter lives in the board crate.
- Sector erase, page program and user-mode `0x03` reads follow ESP-IDF
  v5.2's LL-layer register sequences.
- Write-enable is checked after every WREN (`Error::WriteEnableFailed`)
  and every busy poll is bounded (`Error::Timeout`).
- Host tests against a mock NOR chip, including a full `Db` on
  `FlashBlockDevice<SpiFlash<MockBus>>`. The driver is not yet verified
  on silicon: QEMU does not emulate the SPI user-command path.

## [0.6.0]

### Added

- Build gate for `xtensa-esp32s3-none-elf` with the ESP Rust fork
  (`xtensa-check.sh`).
- Bare-metal ESP32-S3 smoke binary (`xtensa-smoke/`) that boots under
  QEMU and drives a `Db` through put, get, delete, flush, compact, scan
  and snapshot.
- `Flash` trait and `FlashBlockDevice<F>`, an erase-aware `BlockDevice`
  wrapper: each whole-block write is erase-sector then program.
- `ESP32S3` const profile with compile-time size assertions, and
  `BUDGET.md`.

## [0.5.0]

### Added

- `Scan`, a merge iterator that borrows `&Db`, with `seek(start, end,
  max_seq)` and `next`. Prefix scans use an exclusive end bound.
- Snapshot reads: `snapshot()` (up to 8 live), `release_snapshot()`,
  `get_at()`, and `TableReader::get_at`/`lookup_at`.
- Executable models `model_winner`, `model_visible` and
  `model_keep_set` in `src/model.rs`, with property and differential
  tests.

### Changed

- The memtable and SSTables keep full version chains. Compaction keeps
  each key's keep-set and drops a bottommost newest tombstone only when
  it predates every live snapshot.

## [0.4.1]

### Added

- Compaction returns its input tables' block runs to the free list
  strictly after the manifest commit. This is best-effort; leftovers
  are swept by the next open.

### Changed

- On-disk format policy before 1.0: no compatibility across minor
  versions. A foreign manifest magic is `CorruptManifest`.
- Manifest magic `hrtman01` becomes `hrtman02` for the length-prefixed
  key-bound encoding.

## [0.4.0] - 2026-09-14

### Added

- `Db::compact_step(&mut Compaction)` returning `Progress::{More,
  Done}`, with caller-owned typed scratch. L0 to L1 only.
- Linear-scan k-way merge over up to 8 inputs; highest sequence wins.
- One output block per call; the final call commits the manifest
  atomically. A crash leaves exactly the pre- or post-compaction state.
- Tombstones are dropped only when no level numbered 2 or higher
  overlaps the output span.
- Slicing-by-8 CRC, a reused `get` scratch buffer, word-chunked zero
  scans, and callgrind bench harnesses.

### Fixed

- Manifest key bounds were written as full 256-byte arrays (544 bytes
  per table), overflowing a 4 KiB manifest block at 8 tables. They are
  now length-prefixed.

## [0.3.0] - 2026-09-13

### Added

- Full read path in `get()`: memtable, then L0 newest-first, then deeper
  levels, with key-range, bloom and sequence pruning. The highest
  sequence wins across levels, and a newer tombstone hides older values.
- `FreeList` with first-fit run allocation: free list first, claimed
  only after the manifest commit.
- The open-time sweep reclaims orphaned table blocks.
- The WAL wraps atomically with the flush that fills it; recovery skips
  stale pre-wrap blocks with a sequence floor.

## [0.2.0] - 2026-09-13

### Added

- SSTable writer and reader: immutable sorted runs of fixed-size blocks
  (format in SPEC §4.4).
- `Db::flush()` streams the memtable into an L0 SSTable.
- Double-buffered manifest and its atomic commit protocol.
- Bump-pointer block allocation.
- `get()` reads L0 tables newest-first.

## [0.1.0]

### Added

- MemTable: sorted slots over a bump arena.
- WAL: append-only, CRC-framed records written as whole blocks.
- Recovery replays the WAL up to the first corrupt or truncated record,
  the torn tail.
- The poll-based `BlockDevice` trait, with an in-memory device in
  tests.
- Crash injection over 3-op scripts.
