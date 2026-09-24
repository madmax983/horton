//! Database: WAL + memtable + `SSTables` (v0.3).
//!
//! Write path is WAL-first: every mutation is appended to the WAL and
//! committed before it lands in the memtable, so a crash can only lose the
//! un-acknowledged tail. [`Db::flush`] drains the memtable into an immutable
//! `SSTable` in the table region and commits the manifest; the manifest commit
//! is the single atomic visibility point — a crash before it leaves the old
//! manifest plus a replayable WAL, a crash after it leaves the new state.
//! Reads are served from the memtable first, then level 0 (newest table
//! first), then deeper levels in order; every table is key-range pruned and
//! bloom-gated, and the hit with the highest sequence number wins.

use core::cell::RefCell;
use core::future::poll_fn;

use crate::alloc::{Bump, FreeList};
use crate::batch::WriteBatch;
use crate::cache::{BlockCache, CachePort, CacheStats};
use crate::compact::{
    COMPACTION_KMAX, Compaction, EntryStream, Input, MergeOutcome, Progress, State, init_cursor,
    ranges_overlap,
};
use crate::device::BlockDevice;
use crate::error::Error;
use crate::manifest::{KeyBound, Manifest, TableRef};
use crate::memtable::MemTable;
use crate::sstable;
use crate::wal::{Op, RecoverState, WAL_RECORD_OVERHEAD, WalWriter};

/// Placement of the database regions on the device.
///
/// Three disjoint regions plus two fixed single-block manifest slots.
/// Region overlap is a caller bug.
#[derive(Debug, Clone, Copy)]
pub struct Config {
    /// First block id of the WAL region.
    pub wal_start: u64,
    /// One past the last block id of the WAL region.
    pub wal_end: u64,
    /// First block id of the `SSTable` region.
    pub tbl_start: u64,
    /// One past the last block id of the `SSTable` region.
    pub tbl_end: u64,
    /// First manifest slot (one block).
    pub manifest_a: u64,
    /// Second manifest slot (one block).
    pub manifest_b: u64,
}

impl Config {
    /// Describes the WAL region, the `SSTable` region, and the manifest slots.
    #[must_use]
    pub const fn new(
        wal_start: u64,
        wal_end: u64,
        tbl_start: u64,
        tbl_end: u64,
        manifest_a: u64,
        manifest_b: u64,
    ) -> Self {
        Self {
            wal_start,
            wal_end,
            tbl_start,
            tbl_end,
            manifest_a,
            manifest_b,
        }
    }
}

/// Summary of a [`Db::open`] call.
#[derive(Debug, Clone, Copy)]
pub struct OpenReport {
    /// WAL records replayed into the memtable.
    pub recovered_records: u64,
    /// Highest sequence number found; the next mutation uses `max_seq + 1`.
    pub max_seq: u64,
    /// `SSTables` referenced by level 0 of the recovered manifest.
    pub l0_tables: usize,
}

/// A plan to archive (upload, then forget) one sealed `SSTable`.
///
/// Returned by [`Db::archive_plan`]: the table's level and placement
/// record. The caller streams every block in
/// `[table.first_block, table.end_block())` — read through
/// [`Db::device`] — to durable remote storage, confirms the upload, then
/// calls [`Db::archive_commit`] to drop the table from the manifest and
/// reclaim its blocks. This is the flush-to-object-storage primitive: the
/// table's bytes are immutable once sealed, so the upload needs no
/// coordination with horton beyond "all bytes, then commit".
#[derive(Debug, Clone, Copy)]
pub struct ArchivePlan<const KEY_MAX: usize> {
    /// The level holding the table.
    pub level: usize,
    /// The table's placement record: id and block range.
    pub table: TableRef<KEY_MAX>,
}

/// A placement-free table descriptor for re-attach (`ingest_table`).
///
/// Produced by [`ArchivePlan::sealed`] from a table that was archived away.
/// It carries everything needed to validate and graft a copy of the table
/// back into a database — possibly a different one — without any reference
/// to where the table's blocks lived on the source device.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SealedTable<const KEY_MAX: usize> {
    /// The table's original id; ingest preserves it so the table is
    /// idempotent across retries and database copies.
    pub id: u32,
    /// Total blocks: rdel blocks + data blocks + bloom + index + footer.
    pub block_count: u32,
    /// Smallest key in the table.
    pub first_key: KeyBound<KEY_MAX>,
    /// Largest key in the table.
    pub last_key: KeyBound<KEY_MAX>,
    /// Highest sequence number in the table.
    pub max_seq: u64,
    /// Key/value entries (including tombstones); cross-checked against
    /// the copied table's footer on ingest.
    pub entry_count: u32,
    /// Range-tombstone blocks at the table's start; cross-checked against
    /// the copied table's footer on ingest, and needed to locate the data
    /// section when relocating the table.
    pub rdel_blocks: u32,
}

impl<const KEY_MAX: usize> ArchivePlan<KEY_MAX> {
    /// Strips this plan down to its placement-free sealed descriptor.
    ///
    /// The descriptor is a pure value: it can cross a device boundary
    /// (e.g. to cold storage) and later re-enter through
    /// [`Db::ingest_table`].
    #[must_use]
    pub const fn sealed(&self) -> SealedTable<KEY_MAX> {
        SealedTable {
            id: self.table.id,
            block_count: self.table.block_count,
            first_key: self.table.first_key,
            last_key: self.table.last_key,
            max_seq: self.table.max_seq,
            entry_count: self.table.entry_count,
            rdel_blocks: self.table.rdel_blocks,
        }
    }
}

/// Best hit seen so far by [`Db::get`]: the highest-sequence lookup result.
/// When `Value`, the winning bytes are staged in `get`'s staging buffer.
enum Best {
    /// No hit yet.
    Missing,
    /// A tombstone beat every value so far.
    Tombstone,
    /// A value won; holds its byte length.
    Value(usize),
}

/// Accumulator for [`Db::get_at`]'s multi-table read: the winning staged
/// value bytes plus the sequence that won them. Bundled into one struct so
/// `consider_table` stays under the argument-count lint; table lookups
/// copy into a per-table buffer first, and only a winning hit is promoted
/// into `stage`, so a losing hit can never clobber the winner.
struct ReadAcc<const VAL_MAX: usize> {
    stage: [u8; VAL_MAX],
    best: Best,
    best_seq: u64,
    /// Expiry tick of the winning value; 0 = no expiry. Checked against
    /// the caller's `now` before the value is returned.
    best_expire_at: u64,
    /// Highest range-tombstone sequence covering the key at/below the
    /// snapshot, across the memtable and every considered table. Beats
    /// the point winner when strictly newer (sequences are unique per
    /// mutation, so equality cannot happen).
    cover_seq: u64,
}

impl<const VAL_MAX: usize> ReadAcc<VAL_MAX> {
    const fn new() -> Self {
        Self {
            stage: [0u8; VAL_MAX],
            best: Best::Missing,
            best_seq: 0,
            best_expire_at: 0,
            cover_seq: 0,
        }
    }
}

/// The tables one compaction job merges, chosen by `compact_select`.
struct JobInputs<const KEY_MAX: usize> {
    /// Total input tables pushed into the scratch.
    n_inputs: usize,
    /// Of those, how many came from the target level.
    n_tgt_inputs: usize,
    /// Merged key-range closure over all inputs.
    first: KeyBound<KEY_MAX>,
    last: KeyBound<KEY_MAX>,
    /// Summed entry counts (for the bloom filter sizing).
    total_entries: u64,
    /// Summed data blocks, excluding each table's rdel blocks and the 3
    /// framing blocks (for the output run reservation). The merged rdel
    /// section gets its own exact budget from a dry-run pass; see
    /// `count_rdel_merge`.
    total_data: u64,
}

/// Maximum live snapshots. Snapshot slots are plain `u64`s in the `Db`;
/// the bound keeps that state tiny and the exhaustion error explicit.
pub(crate) const MAX_SNAPSHOTS: usize = 8;

/// The database handle. Owns the WAL writer (and through it, the device),
/// the memtable, the manifest, and the table-region allocator (bump pointer
/// plus free list of reclaimed blocks).
pub struct Db<
    D: BlockDevice,
    const BLOCK: usize,
    const KEY_MAX: usize,
    const VAL_MAX: usize,
    const CAP: usize,
    const ARENA: usize,
    const LEVELS: usize,
    const TABLES: usize,
    const BLOOM_BYTES: usize,
    const FREELIST: usize,
    // Block-cache slots (cache::BlockCache). 0 disables the cache.
    const CACHE: usize,
> {
    wal: WalWriter<D, BLOCK>,
    table: MemTable<CAP, ARENA, KEY_MAX, VAL_MAX>,
    manifest: Manifest<LEVELS, TABLES, KEY_MAX>,
    tbl_bump: Bump,
    tbl_free: FreeList<FREELIST>,
    cfg: Config,
    next_seq: u64,
    /// Live snapshot watermarks (sequence numbers). A snapshot pins reads
    /// to mutations with `seq <= watermark`; while any snapshot is live,
    /// compaction must not drop a tombstone at or above the oldest
    /// watermark. Bounded: [`MAX_SNAPSHOTS`] live snapshots, then
    /// [`Error::NoSpace`]. Snapshots are in-memory only — they do not
    /// survive `open()`.
    snapshots: [u64; MAX_SNAPSHOTS],
    n_snapshots: usize,
    /// Block-read buffer for [`Db::get`]. A read fills every byte before
    /// `get` reads it. A zeroed buffer on each call would waste work.
    /// Calls reuse this buffer instead.
    ///
    /// The buffer sits in a `RefCell`. This lets `get` write to it
    /// through `&self`. Two `get` calls can run at once (interleaved
    /// awaits on one executor). The second call then uses its own local
    /// buffer, not this one.
    ///
    /// This buffer used to live on `get`'s stack, for one call only. It
    /// now lives here, for the life of the `Db`. Count `BLOCK` bytes of
    /// permanent RAM for this field against SPEC.md's RAM budget.
    get_scratch: RefCell<[u8; BLOCK]>,
    /// Decompression buffer for point reads: data blocks flagged
    /// compressed inflate into here before parsing (see
    /// [`sstable::TableReader::lookup_at`](crate::sstable::TableReader::lookup_at)).
    /// Same borrow discipline as `get_scratch`: `get_at` tries the shared
    /// buffer and falls back to a stack buffer when a concurrent `get`
    /// already holds it. Count another `BLOCK` bytes of permanent RAM.
    decomp_scratch: RefCell<[u8; BLOCK]>,
    /// `SSTable` block cache (`CACHE` slots of `BLOCK` bytes plus one tag
    /// per slot). Served on the read path — point reads, forward and
    /// reverse scans — keyed by `(table_id, device_block_id)`. Same
    /// `RefCell` discipline as `get_scratch`: contention degrades to a
    /// silent bypass, never a panic. Count `CACHE * (BLOCK + tag)` bytes
    /// of permanent RAM against SPEC.md's RAM budget.
    cache: RefCell<BlockCache<BLOCK, CACHE>>,
}

/// WAL staging snapshot, taken before a mutation stages its records.
/// Lets [`Db::rollback_commit`] undo a failed commit: staged-but-undurable
/// records are truncated away, and sequence numbers are consumed only when
/// a block actually landed.
#[derive(Debug, Clone, Copy)]
struct StageMark {
    stage_len: usize,
    next_block: u64,
}

impl<
    D: BlockDevice,
    const BLOCK: usize,
    const KEY_MAX: usize,
    const VAL_MAX: usize,
    const CAP: usize,
    const ARENA: usize,
    const LEVELS: usize,
    const TABLES: usize,
    const BLOOM_BYTES: usize,
    const FREELIST: usize,
    const CACHE: usize,
> Db<D, BLOCK, KEY_MAX, VAL_MAX, CAP, ARENA, LEVELS, TABLES, BLOOM_BYTES, FREELIST, CACHE>
{
    const ASSERT_BLOCK: () = assert!(BLOCK == D::BLOCK, "BLOCK must equal D::BLOCK");
    const ASSERT_KEY: () = assert!(
        KEY_MAX >= 1 && KEY_MAX <= 0xFFFF,
        "KEY_MAX must be within 1..=0xFFFF"
    );
    const ASSERT_VAL: () = assert!(VAL_MAX <= 0xFFFF, "VAL_MAX must fit in u16");
    const ASSERT_REC: () = assert!(
        BLOCK >= 23 + KEY_MAX + VAL_MAX,
        "BLOCK must fit the largest WAL record"
    );
    const ASSERT_BLOOM: () = assert!(
        BLOOM_BYTES >= 1 && BLOOM_BYTES + 4 <= BLOCK,
        "BLOOM_BYTES must be within 1..=BLOCK-4"
    );

    /// Creates a closed database handle over `device`.
    #[must_use]
    pub const fn new(device: D, config: Config) -> Self {
        // Associated consts are lazy: referencing them here forces the
        // parameter checks to be evaluated for every instantiation.
        let () = Self::ASSERT_BLOCK;
        let () = Self::ASSERT_KEY;
        let () = Self::ASSERT_VAL;
        let () = Self::ASSERT_REC;
        let () = Self::ASSERT_BLOOM;
        Self {
            wal: WalWriter::new(device, config.wal_start, config.wal_end),
            table: MemTable::new(),
            manifest: Manifest::new(),
            tbl_bump: Bump::new(config.tbl_start, config.tbl_end),
            tbl_free: FreeList::new(),
            cfg: config,
            next_seq: 0,
            snapshots: [0u64; MAX_SNAPSHOTS],
            n_snapshots: 0,
            get_scratch: RefCell::new([0u8; BLOCK]),
            decomp_scratch: RefCell::new([0u8; BLOCK]),
            cache: RefCell::new(BlockCache::new()),
        }
    }

    /// Consumes the handle and returns the underlying device.
    #[must_use]
    pub fn into_device(self) -> D {
        self.wal.into_device()
    }

    /// Takes a snapshot: registers the current sequence watermark and
    /// returns it. Reads pinned to the watermark (`get_at`, `Scan::seek`
    /// with `max_seq`) see exactly the mutations with `seq <= watermark`,
    /// however much is written afterwards. While the snapshot is live,
    /// compaction retains any tombstone a read at the watermark could
    /// observe. Release with [`release_snapshot`](Db::release_snapshot);
    /// snapshots are in-memory only and do not survive `open()`.
    ///
    /// # Errors
    ///
    /// [`Error::NoSpace`] when [`MAX_SNAPSHOTS`] snapshots are already live.
    pub const fn snapshot(&mut self) -> Result<u64, Error<D::Error>> {
        if self.n_snapshots >= MAX_SNAPSHOTS {
            return Err(Error::NoSpace);
        }
        let snap = self.next_seq;
        self.snapshots[self.n_snapshots] = snap;
        self.n_snapshots += 1;
        Ok(snap)
    }

    /// Releases a snapshot taken by [`snapshot`](Db::snapshot), freeing its
    /// slot so compaction may drop tombstones again. Releasing an unknown
    /// watermark is a no-op.
    pub const fn release_snapshot(&mut self, snap: u64) {
        let mut i = 0;
        while i < self.n_snapshots {
            if self.snapshots[i] == snap {
                self.n_snapshots -= 1;
                self.snapshots[i] = self.snapshots[self.n_snapshots];
                return;
            }
            i += 1;
        }
    }

    /// Oldest live snapshot watermark, or `u64::MAX` when none is live.
    /// Compaction captures this at select time as the tombstone-drop floor.
    pub(crate) const fn oldest_snapshot_seq(&self) -> u64 {
        let mut min = u64::MAX;
        let mut i = 0;
        while i < self.n_snapshots {
            if self.snapshots[i] < min {
                min = self.snapshots[i];
            }
            i += 1;
        }
        min
    }

    /// The device, for the scan iterator's block reads — and for the
    /// archive upload loop, which streams a sealed table's blocks through
    /// it ([`Db::archive_plan`]).
    #[must_use]
    pub const fn device(&self) -> &D {
        self.wal.device()
    }

    /// The memtable, for the scan iterator's memtable cursor.
    pub(crate) const fn memtable(&self) -> &MemTable<CAP, ARENA, KEY_MAX, VAL_MAX> {
        &self.table
    }

    /// The manifest, for the scan iterator's table cursors.
    pub(crate) const fn manifest_ref(&self) -> &Manifest<LEVELS, TABLES, KEY_MAX> {
        &self.manifest
    }

    /// The block cache, for the scan iterators' block reads. Scans borrow
    /// it through the same [`CachePort`] view the point-read path uses.
    pub(crate) fn cache_port(&self) -> &dyn CachePort<BLOCK> {
        &self.cache
    }

    /// Test-visible block-cache counters: hits, misses, occupancy. Lets
    /// callers prove the cache is earning its RAM instead of trusting us.
    #[must_use]
    pub fn cache_stats(&self) -> CacheStats {
        self.cache.stats()
    }

    /// Opens the database: recovers the manifest, rebuilds the table-region
    /// allocator with the open-time sweep, replays the WAL from the
    /// manifest's `wal_head` into a fresh memtable, and resumes the sequence
    /// counter and the WAL append position. Idempotent.
    ///
    /// The sweep re-derives "free" as "not referenced": the bump resumes
    /// past the highest manifest-referenced table block, and unreferenced
    /// blocks below that point (orphans of torn flushes) go on the free
    /// list for true reuse. In v0.3 the write path cannot strand such
    /// blocks — a torn flush's run always starts at or above the resume
    /// point, where the bump simply overwrites it — so the list only holds
    /// what the sweep finds here; v0.4 compaction will free whole tables
    /// into it. Size `FREELIST` to hold the table region's block count: an
    /// undersized list fails the open loudly instead of leaking silently.
    ///
    /// # Errors
    ///
    /// [`Error::CorruptManifest`], [`Error::CorruptWal`], [`Error::NoSpace`]
    /// (undersized `FREELIST`), or [`Error::Device`].
    pub async fn open(&mut self) -> Result<OpenReport, Error<D::Error>> {
        self.table.clear();
        let mut scratch = [0u8; BLOCK];
        let (manifest, fresh) = Manifest::recover(
            self.wal.device_mut(),
            &mut scratch,
            self.cfg.manifest_a,
            self.cfg.manifest_b,
        )
        .await?;
        self.manifest = manifest;
        if fresh {
            // Nothing was ever committed: the whole WAL region is live.
            self.manifest.set_wal_head(self.cfg.wal_start);
        }
        // Sweep: the bump resumes past the highest referenced table block;
        // unreferenced blocks below it are reclaimed into the free list.
        self.tbl_bump.set_next(self.cfg.tbl_start);
        self.tbl_free = FreeList::new();
        if let Some(end) = self.manifest.table_region_end() {
            self.tbl_bump.set_next(end);
            for id in self.cfg.tbl_start..end {
                if !self.manifest.is_table_block_referenced(id) {
                    self.tbl_free.insert::<D::Error>(id)?;
                }
            }
        }
        // The persisted flush floor skips stale pre-wrap WAL records (see
        // `WalWriter::recover_from`). It must be the persisted value, not
        // one derived from live tables: compaction and archival can remove
        // the tables holding the newest sequences, and a derived floor
        // would then fall below stale records and replay them.
        let state: RecoverState = self
            .wal
            .recover_from(
                &mut self.table,
                self.manifest.wal_head(),
                self.manifest.flushed_seq(),
            )
            .await?;
        // Resume above every sequence ever issued: the WAL's newest record,
        // the persisted high-water mark, and any live table (an ingested
        // table can carry sequences from another history).
        self.next_seq = state
            .max_seq
            .max(self.manifest.seq_high())
            .max(self.manifest.max_seq());
        Ok(OpenReport {
            recovered_records: state.records,
            max_seq: self.next_seq,
            l0_tables: self.manifest.l0().len(),
        })
    }

    /// Captures the WAL staging state before a mutation, for rollback.
    const fn stage_mark(&self) -> StageMark {
        StageMark {
            stage_len: self.wal.staged_bytes(),
            next_block: self.wal.next_block(),
        }
    }

    /// Undoes a failed commit. If no block landed (`next_block`
    /// unchanged), the staged records never became durable and are
    /// truncated away. If a block landed but the device flush failed —
    /// the device lied, indistinguishable from a crash at that instant —
    /// the records may replay on recovery, so `seqs` sequence numbers are
    /// consumed now and never reused.
    fn rollback_commit(&mut self, mark: StageMark, seqs: u64) -> Result<(), Error<D::Error>> {
        if self.wal.next_block() == mark.next_block {
            self.wal.truncate_stage(mark.stage_len);
        } else {
            self.next_seq = self.next_seq.checked_add(seqs).ok_or(Error::NoSpace)?;
        }
        Ok(())
    }

    /// Stores `key` → `val`, durable before it returns. Returns the sequence
    /// number assigned to the mutation.
    ///
    /// # Errors
    ///
    /// [`Error::EmptyKey`], [`Error::KeyTooLarge`], [`Error::ValueTooLarge`],
    /// [`Error::TableFull`], [`Error::ArenaFull`], [`Error::NoSpace`], or
    /// [`Error::Device`].
    pub async fn put(&mut self, key: &[u8], val: &[u8]) -> Result<u64, Error<D::Error>> {
        self.table.check_insert::<D::Error>(key, val, false)?;
        let seq = self.next_seq.checked_add(1).ok_or(Error::NoSpace)?;
        let mark = self.stage_mark();
        if let Err(e) = self.wal.append(seq, Op::Put, key, val).await {
            self.rollback_commit(mark, 0)?;
            return Err(e);
        }
        if let Err(e) = self.wal.commit().await {
            let landed = self.wal.next_block() != mark.next_block;
            self.rollback_commit(mark, u64::from(landed))?;
            return Err(e);
        }
        self.next_seq = seq;
        // The table is unchanged since check_insert, so this cannot fail.
        self.table.insert(key, val, seq, false)?;
        Ok(seq)
    }

    /// Deletes `key` via a tombstone, durable before it returns. Returns the
    /// sequence number assigned to the mutation.
    ///
    /// # Errors
    ///
    /// Same as [`put`](Db::put).
    pub async fn delete(&mut self, key: &[u8]) -> Result<u64, Error<D::Error>> {
        self.table.check_insert::<D::Error>(key, &[], true)?;
        let seq = self.next_seq.checked_add(1).ok_or(Error::NoSpace)?;
        let mark = self.stage_mark();
        if let Err(e) = self.wal.append(seq, Op::Delete, key, &[]).await {
            self.rollback_commit(mark, 0)?;
            return Err(e);
        }
        if let Err(e) = self.wal.commit().await {
            let landed = self.wal.next_block() != mark.next_block;
            self.rollback_commit(mark, u64::from(landed))?;
            return Err(e);
        }
        self.next_seq = seq;
        // The table is unchanged since check_insert, so this cannot fail.
        self.table.insert(key, &[], seq, true)?;
        Ok(seq)
    }

    /// Deletes every key in `[start, end)` via one range tombstone, durable
    /// before it returns. An empty or inverted range (`start >= end`) is a
    /// no-op: it consumes no sequence number, writes no WAL record, and
    /// returns the current sequence number. Returns the sequence number
    /// assigned to the tombstone.
    ///
    /// # Errors
    ///
    /// Same as [`put`](Db::put).
    pub async fn delete_range(&mut self, start: &[u8], end: &[u8]) -> Result<u64, Error<D::Error>> {
        if start >= end {
            return Ok(self.next_seq);
        }
        self.table.check_insert_range_del::<D::Error>(start, end)?;
        let seq = self.next_seq.checked_add(1).ok_or(Error::NoSpace)?;
        let mark = self.stage_mark();
        if let Err(e) = self.wal.append(seq, Op::RangeDelete, start, end).await {
            self.rollback_commit(mark, 0)?;
            return Err(e);
        }
        if let Err(e) = self.wal.commit().await {
            let landed = self.wal.next_block() != mark.next_block;
            self.rollback_commit(mark, u64::from(landed))?;
            return Err(e);
        }
        self.next_seq = seq;
        // The table is unchanged since check_insert_range_del, so this
        // cannot fail.
        self.table.insert_range_del::<D::Error>(start, end, seq)?;
        Ok(seq)
    }

    /// Puts `key`/`val` with an absolute expiry tick, durable before it
    /// returns. Horton owns no clock: `expire_at` is caller-defined, and
    /// timed reads ([`get_at_with_time`](Db::get_at_with_time)) suppress
    /// the value once `expire_at <= now`. `expire_at == 0` means no
    /// expiry and behaves exactly like [`put`](Db::put). Returns the
    /// sequence number assigned to the mutation.
    ///
    /// # Errors
    ///
    /// Same as [`put`](Db::put).
    pub async fn put_with_ttl(
        &mut self,
        key: &[u8],
        val: &[u8],
        expire_at: u64,
    ) -> Result<u64, Error<D::Error>> {
        self.table.check_insert::<D::Error>(key, val, false)?;
        let seq = self.next_seq.checked_add(1).ok_or(Error::NoSpace)?;
        let mark = self.stage_mark();
        if let Err(e) = self.wal.append_ttl(seq, key, val, expire_at).await {
            self.rollback_commit(mark, 0)?;
            return Err(e);
        }
        if let Err(e) = self.wal.commit().await {
            let landed = self.wal.next_block() != mark.next_block;
            self.rollback_commit(mark, u64::from(landed))?;
            return Err(e);
        }
        self.next_seq = seq;
        // The table is unchanged since check_insert, so this cannot fail.
        self.table
            .insert_ttl::<D::Error>(key, val, seq, expire_at)?;
        Ok(seq)
    }

    /// Applies every op in `batch` atomically: all become durable and
    /// visible together, or none does. Returns the base sequence number;
    /// op `i` (in queue order) takes `base + i`.
    ///
    /// Atomicity rides on a single WAL commit: the batch is staged into
    /// the WAL's RAM buffer and made durable by one block write, so the
    /// torn-tail rule yields all-or-nothing for free. A batch whose
    /// encoded size exceeds one block cannot commit atomically and is
    /// rejected with [`Error::BatchTooLarge`] — never silently split.
    /// An empty batch is a no-op returning the current sequence number.
    ///
    /// # Errors
    ///
    /// [`Error::BatchTooLarge`], [`Error::TableFull`],
    /// [`Error::ArenaFull`], [`Error::NoSpace`], or [`Error::Device`].
    /// A rejected batch leaves no trace: no WAL records, no staged bytes,
    /// no consumed sequence numbers.
    pub async fn write<const OPS: usize>(
        &mut self,
        batch: &WriteBatch<KEY_MAX, VAL_MAX, OPS>,
    ) -> Result<u64, Error<D::Error>> {
        let ops = batch.ops();
        let n = ops.len();
        if n == 0 {
            return Ok(self.next_seq);
        }
        // Total capacity for the whole batch, up front: sizes were
        // validated when the batch was built.
        let mut need_arena = 0usize;
        let mut need_wal = 0usize;
        for op in ops {
            need_arena = need_arena
                .checked_add(op.key().len() + op.val().len())
                .ok_or(Error::ArenaFull)?;
            need_wal = need_wal
                .checked_add(WAL_RECORD_OVERHEAD + op.key().len() + op.val().len())
                .ok_or(Error::NoSpace)?;
        }
        if need_wal > BLOCK {
            return Err(Error::BatchTooLarge {
                bytes: need_wal,
                max: BLOCK,
            });
        }
        if self.table.len() + n > CAP {
            return Err(Error::TableFull);
        }
        if self.table.arena_len() + need_arena > ARENA {
            return Err(Error::ArenaFull);
        }
        // Drain any previously staged data so the batch still commits as
        // one block write. (In practice the stage is always empty between
        // Db calls — every mutation commits immediately.)
        if self.wal.staged_bytes() > 0 {
            self.wal.commit().await?;
        }
        debug_assert_eq!(self.wal.staged_bytes(), 0);

        let base = self.next_seq.checked_add(1).ok_or(Error::NoSpace)?;
        let nu64 = u64::try_from(n).map_err(|_| Error::NoSpace)?;
        let last = base.checked_add(nu64 - 1).ok_or(Error::NoSpace)?;

        let mark = self.stage_mark();
        for (i, op) in ops.iter().enumerate() {
            let seq = base
                .checked_add(u64::try_from(i).map_err(|_| Error::NoSpace)?)
                .ok_or(Error::NoSpace)?;
            // Unreachable in practice: sizes were validated when the batch
            // was built and the block fit was checked above. Roll back
            // anyway — atomicity is never best-effort here.
            if let Err(e) = self.wal.append(seq, op.kind(), op.key(), op.val()).await {
                self.rollback_commit(mark, 0)?;
                return Err(e);
            }
        }
        if let Err(e) = self.wal.commit().await {
            let landed = self.wal.next_block() != mark.next_block;
            self.rollback_commit(mark, if landed { nu64 } else { 0 })?;
            return Err(e);
        }
        self.next_seq = last;
        // The table is unchanged since the capacity check, so this cannot fail.
        for (i, op) in ops.iter().enumerate() {
            let seq = base
                .checked_add(u64::try_from(i).map_err(|_| Error::NoSpace)?)
                .ok_or(Error::NoSpace)?;
            self.table
                .insert::<D::Error>(op.key(), op.val(), seq, op.kind() == Op::Delete)?;
        }
        Ok(base)
    }

    /// Reads `key` into `val_buf`: memtable first, then level 0 newest table
    /// first, then deeper levels in order. This is
    /// [`get_at`](Db::get_at) with `max_seq = u64::MAX`: the latest view.
    ///
    /// Returns `Ok(None)` for missing keys and tombstones. Never truncates:
    /// an undersized buffer yields [`Error::BufferTooSmall`] with the
    /// required length — the length of the *winning* value, even when an
    /// older shadowed version would have fit.
    ///
    /// # Errors
    ///
    /// [`Error::BufferTooSmall`] when `val_buf` is smaller than the winning
    /// value, [`Error::CorruptManifest`] when a table's block range is
    /// malformed, [`Error::CorruptBlock`] when a table's index or footer
    /// fails verification, or [`Error::Device`] on I/O failure.
    // This call holds `get_scratch`'s borrow across its own awaits.
    // `try_borrow_mut` stops two calls from holding this borrow at once.
    // So the lint does not apply here.
    #[allow(clippy::await_holding_refcell_ref)]
    pub async fn get(
        &self,
        key: &[u8],
        val_buf: &mut [u8],
    ) -> Result<Option<usize>, Error<D::Error>> {
        self.get_at_with_time(key, val_buf, u64::MAX, 0).await
    }

    /// Reads `key` into `val_buf` as of a snapshot: like [`get`](Db::get),
    /// but only mutations with `seq <= max_seq` are visible. Newer versions
    /// (including newer tombstones) are invisible, so an older value — or
    /// a missing key — can correctly win. Pass a watermark from
    /// [`snapshot`](Db::snapshot) for a pinned read, or `u64::MAX` for the
    /// latest view.
    ///
    /// Returns `Ok(None)` for missing keys and tombstones. Never truncates:
    /// an undersized buffer yields [`Error::BufferTooSmall`] with the
    /// required length — the length of the *winning* value, even when an
    /// older shadowed version would have fit.
    ///
    /// # Errors
    ///
    /// [`Error::BufferTooSmall`] when `val_buf` is smaller than the winning
    /// value, [`Error::CorruptManifest`] when a table's block range is
    /// malformed, [`Error::CorruptBlock`] when a table's index or footer
    /// fails verification, or [`Error::Device`] on I/O failure.
    // This call holds `get_scratch`'s borrow across its own awaits.
    // `try_borrow_mut` stops two calls from holding this borrow at once.
    // So the lint does not apply here.
    #[allow(clippy::await_holding_refcell_ref)]
    pub async fn get_at(
        &self,
        key: &[u8],
        val_buf: &mut [u8],
        max_seq: u64,
    ) -> Result<Option<usize>, Error<D::Error>> {
        self.get_at_with_time(key, val_buf, max_seq, 0).await
    }

    /// Timed read: like [`get`](Db::get), but values whose `expire_at` is
    /// nonzero and `<= now` are suppressed — they read as missing (a
    /// newer live version still wins; an expired winner never falls
    /// through to an older version). Horton owns no clock: `now` is the
    /// caller's tick, compared against the absolute `expire_at` stored by
    /// [`put_with_ttl`](Db::put_with_ttl).
    ///
    /// # Errors
    ///
    /// Same as [`get`](Db::get).
    #[allow(clippy::await_holding_refcell_ref)]
    pub async fn get_with_time(
        &self,
        key: &[u8],
        val_buf: &mut [u8],
        now: u64,
    ) -> Result<Option<usize>, Error<D::Error>> {
        self.get_at_with_time(key, val_buf, u64::MAX, now).await
    }

    /// Timed read: like [`get_at`](Db::get_at), but values whose
    /// `expire_at` is nonzero and `<= now` are suppressed — they read as
    /// missing (a newer live version still wins; an expired winner never
    /// falls through to an older version). Callers without a clock pass
    /// `now = 0`, which is what [`get`](Db::get) and
    /// [`get_at`](Db::get_at) do.
    ///
    /// # Errors
    ///
    /// Same as [`get_at`](Db::get_at).
    #[allow(clippy::await_holding_refcell_ref)]
    pub async fn get_at_with_time(
        &self,
        key: &[u8],
        val_buf: &mut [u8],
        max_seq: u64,
        now: u64,
    ) -> Result<Option<usize>, Error<D::Error>> {
        // The winning value's bytes are staged here; table lookups copy
        // into a per-table buffer first so a losing hit can never clobber
        // the winner. Values are at most VAL_MAX bytes (enforced on the
        // write path), so the staging always fits.
        let mut acc = ReadAcc::<VAL_MAX>::new();

        if let Some(entry) = self.table.get_at(key, max_seq) {
            // The memtable holds the newest mutations; `get_at` already
            // selected the newest version at or below the snapshot, and
            // skips range-tombstone slots (they are not versions of `key`).
            acc.best_seq = entry.seq;
            if entry.tombstone {
                acc.best = Best::Tombstone;
            } else {
                acc.stage[..entry.val.len()].copy_from_slice(entry.val);
                acc.best = Best::Value(entry.val.len());
                acc.best_expire_at = entry.expire_at;
            }
        }
        // A memtable range tombstone covering `key` beats any older point
        // version; the per-table probes happen inside `consider_table`.
        if let Some(q) = self.table.max_covering_rdel(key, max_seq) {
            acc.cover_seq = q;
        }

        // Use the shared buffer when it is free (see `get_scratch`). Fall
        // back to a local buffer when another `get` call already holds it.
        let mut shared_scratch = self.get_scratch.try_borrow_mut().ok();
        let mut owned_scratch;
        let scratch: &mut [u8; BLOCK] = if let Some(guard) = shared_scratch.as_mut() {
            guard
        } else {
            owned_scratch = [0u8; BLOCK];
            &mut owned_scratch
        };
        // The decompression buffer follows the same discipline: shared
        // when free, stack-local when a concurrent `get` holds it.
        let mut shared_decomp = self.decomp_scratch.try_borrow_mut().ok();
        let mut owned_decomp;
        let decomp: &mut [u8; BLOCK] = if let Some(guard) = shared_decomp.as_mut() {
            guard
        } else {
            owned_decomp = [0u8; BLOCK];
            &mut owned_decomp
        };
        // Level 0, newest table first: its tables overlap, and newer tables
        // hold higher sequence numbers.
        for tref in self.manifest.l0().iter().rev() {
            self.consider_table(tref, key, max_seq, scratch, decomp, &mut acc)
                .await?;
        }
        // Deeper levels in order. Highest-seq-wins keeps the result exact
        // regardless of how tables are placed; v0.4 compaction will keep
        // each level's ranges disjoint and sorted.
        for li in 1..LEVELS {
            // `li < LEVELS` by construction; the fallback is unreachable.
            let tables = self.manifest.level(li).unwrap_or(&[]);
            for tref in tables {
                self.consider_table(tref, key, max_seq, scratch, decomp, &mut acc)
                    .await?;
            }
        }

        match acc.best {
            Best::Missing | Best::Tombstone => Ok(None),
            Best::Value(len) => {
                // A covering range tombstone newer than the point winner
                // hides the key; an expired winner reads as missing and
                // never falls through to an older version.
                if acc.cover_seq > acc.best_seq {
                    return Ok(None);
                }
                if acc.best_expire_at != 0 && acc.best_expire_at <= now {
                    return Ok(None);
                }
                if len > val_buf.len() {
                    return Err(Error::BufferTooSmall { need: len });
                }
                val_buf[..len].copy_from_slice(&acc.stage[..len]);
                Ok(Some(len))
            }
        }
    }

    /// Re-attaches an archived table from a source device (v0.12).
    ///
    /// `sealed` is the placement-free descriptor from
    /// [`ArchivePlan::sealed`](ArchivePlan::sealed); `remote` holds the
    /// table's blocks laid out contiguously starting at `src_base`. The
    /// table is copied into a locally reserved run, its block CRCs are
    /// verified as they land, its index/footer block pointers are
    /// relocated to the destination layout, and the copy is validated
    /// (footer magic/CRC plus the descriptor's entry count) before it is
    /// grafted into L0 through one atomic manifest commit — the same
    /// visibility point flush uses. Data/index/bloom block CRCs are
    /// re-verified on the read paths, exactly as for flushed tables.
    ///
    /// Returns `Ok(true)` when the table was attached, `Ok(false)` when a
    /// table with the same id and an identical descriptor is already
    /// attached (idempotent retry). The re-attached table joins L0, not
    /// its former level: L0 tolerates overlap and highest-sequence-wins
    /// stays exact.
    ///
    /// Crash safety: the run is reserved, not claimed, until the manifest
    /// commit lands, so a crash before the commit leaves only orphaned,
    /// invisible blocks; a crash after it leaves the table fully
    /// attached. Retrying after any crash converges to exactly one copy.
    ///
    /// # Errors
    ///
    /// [`Error::IngestConflict`] when the id is already attached with a
    /// *different* descriptor, [`Error::NoSpace`] when L0 is full or the
    /// table region has no room, [`Error::CorruptBlock`] when the source
    /// bytes fail validation (the manifest is untouched), or
    /// [`Error::Device`] on I/O failure from either device.
    pub async fn ingest_table<R>(
        &mut self,
        sealed: &SealedTable<KEY_MAX>,
        remote: &R,
        src_base: u64,
    ) -> Result<bool, Error<D::Error>>
    where
        R: BlockDevice,
        R::Error: Into<D::Error>,
    {
        // The source device must speak the same block size: the copy
        // buffer is `BLOCK` bytes and the trait contract requires
        // `buf.len() == R::BLOCK` on every call.
        if R::BLOCK != BLOCK {
            return Err(Error::BadBufferLen);
        }
        // Idempotency: the same descriptor attaches exactly once; a
        // conflicting descriptor under a live id is refused.
        if let Some(existing) = self.manifest.find_table(sealed.id) {
            let same = existing.block_count == sealed.block_count
                && existing.first_key == sealed.first_key
                && existing.last_key == sealed.last_key
                && existing.max_seq == sealed.max_seq
                && existing.entry_count == sealed.entry_count
                && existing.rdel_blocks == sealed.rdel_blocks;
            return if same {
                Ok(false)
            } else {
                Err(Error::IngestConflict { id: sealed.id })
            };
        }
        // L0 must have room: like flush, a full L0 is the caller's signal
        // to compact first.
        if self.manifest.l0_is_full() {
            return Err(Error::NoSpace);
        }
        let blocks = u64::from(sealed.block_count);
        let total = usize::try_from(blocks).map_err(|_| Error::CorruptManifest)?;
        // Reserve the run without claiming it (mirrors flush): free list
        // first, then the bump. Nothing moves until the manifest commit
        // below lands, so a crash mid-copy leaves only orphans.
        let free_base = self.tbl_free.find_run(total);
        let base = match free_base {
            Some(b) => b,
            None => self.tbl_bump.peek_run::<D::Error>(blocks)?,
        };
        // Stream the blocks from the source device, verifying each
        // block's CRC as it lands so remote corruption fails fast,
        // before the manifest commit.
        let mut buf = [0u8; BLOCK];
        for k in 0..sealed.block_count {
            let src = src_base
                .checked_add(u64::from(k))
                .ok_or(Error::CorruptManifest)?;
            poll_fn(|cx| remote.poll_read_block(cx, src, &mut buf))
                .await
                .map_err(|e| Error::Device(e.into()))?;
            sstable::check_block_crc::<D::Error, BLOCK>(&buf, src)?;
            let dst = base
                .checked_add(u64::from(k))
                .ok_or(Error::CorruptManifest)?;
            poll_fn(|cx| self.wal.device_mut().poll_write_block(cx, dst, &buf))
                .await
                .map_err(Error::Device)?;
        }
        // Relocate the copy: index entries and the footer carry the
        // absolute block ids of the table's original placement, which are
        // rewritten to the destination layout and re-sealed.
        sstable::relocate_table(
            self.wal.device_mut(),
            base,
            sealed.block_count,
            sealed.rdel_blocks,
            &mut buf,
        )
        .await?;
        // Validate the relocated copy: footer magic/CRC plus the
        // descriptor's entry count.
        let footer = base
            .checked_add(blocks)
            .and_then(|end| end.checked_sub(1))
            .ok_or(Error::CorruptManifest)?;
        let reader = sstable::TableReader::<D, BLOCK, BLOOM_BYTES>::open(
            self.wal.device(),
            &mut buf,
            footer,
            base,
        )
        .await?;
        if reader.entry_count() != u64::from(sealed.entry_count)
            || reader.rdel_blocks() != sealed.rdel_blocks
        {
            return Err(Error::CorruptBlock { id: footer });
        }
        // Graft into L0 through the atomic manifest commit. Future local
        // tables must never collide with the ingested id, so the id floor
        // advances past it (monotone; never lowers the counter).
        let mut staged = self.manifest;
        staged.advance_next_table_id(sealed.id.saturating_add(1));
        // The table may carry sequences from another history: the counter
        // must resume above them, or a later local write could lose to an
        // older ingested version under highest-sequence-wins.
        staged.raise_seq_high(self.next_seq.max(sealed.max_seq));
        staged.add_l0_table::<D::Error>(TableRef {
            id: sealed.id,
            first_block: base,
            block_count: sealed.block_count,
            first_key: sealed.first_key,
            last_key: sealed.last_key,
            max_seq: sealed.max_seq,
            entry_count: sealed.entry_count,
            rdel_blocks: sealed.rdel_blocks,
        })?;
        let (slot_a, slot_b) = (self.cfg.manifest_a, self.cfg.manifest_b);
        staged
            .commit(self.wal.device_mut(), &mut buf, slot_a, slot_b)
            .await?;
        // Commit point passed: publish the staged state, then claim the
        // reserved run — strictly after the visibility point.
        self.manifest = staged;
        self.next_seq = self.next_seq.max(sealed.max_seq);
        match free_base {
            Some(b) => {
                debug_assert_eq!(b, base);
                // Must run unconditionally: `claim_run` removes the run
                // from the free list, and `debug_assert!` does not
                // evaluate its argument in release builds.
                let claimed = self.tbl_free.claim_run(b, total);
                debug_assert!(claimed, "find_run's own result must still claim");
            }
            None => {
                self.tbl_bump
                    .set_next(base.checked_add(blocks).ok_or(Error::NoSpace)?);
            }
        }
        Ok(true)
    }

    /// Considers one table for [`Db::get_at`]: key-range prune, sequence prune,
    /// then a bloom-gated lookup. A hit with a higher sequence number than
    /// the best so far — and visible at `max_seq` — is promoted into `acc`.
    async fn consider_table(
        &self,
        tref: &TableRef<KEY_MAX>,
        key: &[u8],
        max_seq: u64,
        scratch: &mut [u8; BLOCK],
        decomp: &mut [u8; BLOCK],
        acc: &mut ReadAcc<VAL_MAX>,
    ) -> Result<(), Error<D::Error>> {
        // Both prunes are exact: the table's keys all lie within its bounds,
        // and no entry here can carry a seq above the table's max.
        if !tref.covers(key) || tref.max_seq <= acc.best_seq {
            return Ok(());
        }
        let footer = tref
            .first_block
            .checked_add(u64::from(tref.block_count))
            .and_then(|end| end.checked_sub(1))
            .ok_or(Error::CorruptManifest)?;
        let reader = sstable::TableReader::<D, BLOCK, BLOOM_BYTES>::open_cached(
            self.wal.device(),
            Some(&self.cache as &dyn CachePort<BLOCK>),
            tref.id,
            scratch,
            footer,
            tref.first_block,
        )
        .await?;
        // `tmp` (not `acc.stage`) receives the value: only a winning hit is
        // promoted, so a losing hit cannot clobber the staged winner.
        let mut tmp = [0u8; VAL_MAX];
        match reader
            .lookup_at(scratch, decomp, key, &mut tmp, max_seq)
            .await?
        {
            sstable::Lookup::Value {
                len,
                seq,
                expire_at,
            } if seq > acc.best_seq && seq <= max_seq => {
                acc.best_seq = seq;
                acc.stage[..len].copy_from_slice(&tmp[..len]);
                acc.best = Best::Value(len);
                acc.best_expire_at = expire_at;
            }
            sstable::Lookup::Tombstone { seq } if seq > acc.best_seq && seq <= max_seq => {
                acc.best_seq = seq;
                acc.best = Best::Tombstone;
            }
            _ => {}
        }
        // A range tombstone in this table covering `key` shadows older
        // point versions; the accumulator keeps the highest covering
        // sequence and compares it against the point winner at the end.
        if reader.rdel_blocks() > 0
            && let Some(q) = reader.covering_rdel_seq(scratch, key, max_seq).await?
            && q > acc.cover_seq
        {
            acc.cover_seq = q;
        }
        Ok(())
    }

    /// Wraps the WAL when the region is exhausted and nothing is unflushed.
    ///
    /// The `wal_head` move rides a manifest commit, so it stays atomic with
    /// the flush; stale pre-wrap blocks are skipped at recovery by the
    /// sequence floor (see `WalWriter::recover_from`).
    async fn wrap_wal_if_full(&mut self, scratch: &mut [u8; BLOCK]) -> Result<(), Error<D::Error>> {
        if self.wal.next_block() < self.cfg.wal_end {
            return Ok(());
        }
        // Safe: the memtable is empty, and every WAL record not yet flushed
        // into a table is replayed into the memtable at open — so no live
        // records exist. (Stale pre-wrap blocks may still sit between
        // `wal_head` and the append position; the sequence floor skips them
        // at recovery.)
        let mut staged = self.manifest;
        staged.set_wal_head(self.cfg.wal_start);
        // Every issued mutation has left the WAL: the memtable is empty.
        staged.note_flushed(self.next_seq);
        let (slot_a, slot_b) = (self.cfg.manifest_a, self.cfg.manifest_b);
        staged
            .commit(self.wal.device_mut(), scratch, slot_a, slot_b)
            .await?;
        self.manifest = staged;
        self.wal.reset_to(self.cfg.wal_start);
        Ok(())
    }

    /// Writes the memtable's range-tombstone section at `base`, returning
    /// the blocks written.
    async fn write_flush_rdel(
        device: &mut D,
        table: &MemTable<CAP, ARENA, KEY_MAX, VAL_MAX>,
        base: u64,
    ) -> Result<u32, Error<D::Error>> {
        sstable::write_rdel_blocks::<D, BLOCK>(
            device,
            base,
            table.iter().filter_map(|e| {
                if e.range_del {
                    Some(sstable::RdelEntry {
                        start: e.key,
                        end: e.val,
                        seq: e.seq,
                    })
                } else {
                    None
                }
            }),
        )
        .await
    }

    /// Builds the flushed table's [`TableRef`]. Key bounds and `max_seq`
    /// cover both sections: range tombstones participate in table pruning
    /// and winner selection.
    fn flush_tref(
        id: u32,
        base: u64,
        total: u64,
        plan: &sstable::TablePlan<KEY_MAX>,
        rdel_plan: &sstable::RdelPlan<'_>,
    ) -> Result<TableRef<KEY_MAX>, Error<D::Error>> {
        let rdel_first = rdel_plan
            .first
            .and_then(KeyBound::from_slice)
            .unwrap_or(KeyBound::EMPTY);
        let rdel_last = rdel_plan
            .max_end
            .and_then(KeyBound::from_slice)
            .unwrap_or(KeyBound::EMPTY);
        Ok(TableRef {
            id,
            first_block: base,
            block_count: u32::try_from(total).map_err(|_| Error::NoSpace)?,
            first_key: plan.first_key.min(rdel_first),
            last_key: plan.last_key.max(rdel_last),
            max_seq: plan.max_seq.max(rdel_plan.max_seq),
            entry_count: u32::try_from(plan.entry_count).map_err(|_| Error::NoSpace)?,
            rdel_blocks: rdel_plan.blocks,
        })
    }
    /// Flushes the memtable into a new `SSTable` and commits the manifest.
    ///
    /// Protocol: commit the WAL (every acked mutation is durable) → plan the
    /// table → stream its blocks → commit the manifest (the single atomic
    /// visibility point) → clear the memtable. A crash before the manifest
    /// commit leaves the old manifest plus a replayable WAL; a crash after
    /// leaves the new state. Never a mix.
    ///
    /// Table blocks come from the free list first (reclaimed orphans), then
    /// the bump pointer. The run is only *reserved* until the manifest commit
    /// lands — claimed from the free list or advanced on the bump afterwards —
    /// so a returned I/O error leaves the in-memory state exactly as it was
    /// and the flush can simply be retried.
    ///
    /// When the WAL region is exhausted, the flush wraps it: every record is
    /// flushed at that point, so `wal_head` restarts at `wal_start` in the
    /// same atomic manifest commit, giving the WAL an unbounded lifetime.
    /// Stale pre-wrap blocks are skipped at recovery by the sequence floor.
    ///
    /// A flush with an empty memtable still commits the WAL and wraps it if
    /// full, and is otherwise a no-op.
    ///
    /// # Errors
    ///
    /// [`Error::NoSpace`] when the table region is exhausted, the table's
    /// index would overflow one block, or level 0 is full (compaction is a
    /// v0.4 item), or [`Error::Device`] on I/O failure.
    pub async fn flush(&mut self) -> Result<(), Error<D::Error>> {
        // Every acked mutation must be durable in the WAL or the new table.
        self.wal.commit().await?;
        let mut scratch = [0u8; BLOCK];
        if self.table.is_empty() {
            // Still a no-op for the table region, but the WAL may need
            // wrapping after earlier flushes filled it.
            self.wrap_wal_if_full(&mut scratch).await?;
            return Ok(());
        }
        // Fail before doing I/O when level 0 cannot take another table;
        // `add_l0_table` re-checks authoritatively below.
        if self.manifest.l0_is_full() {
            return Err(Error::NoSpace);
        }
        // Pass 1: pure computation of the table shape. Point entries and
        // range tombstones are planned separately: the rdel section sits
        // *before* the data section on device.
        let plan = sstable::plan_table::<D::Error, BLOCK, KEY_MAX>(
            self.table
                .iter()
                .filter(|e| !e.range_del)
                .map(sstable::SstEntry::from),
        )?;
        let rdel_plan =
            sstable::plan_rdel_blocks::<D::Error, BLOCK>(self.table.iter().filter_map(|e| {
                if e.range_del {
                    Some(sstable::RdelEntry {
                        start: e.key,
                        end: e.val,
                        seq: e.seq,
                    })
                } else {
                    None
                }
            }))?;
        let k = sstable::bloom_k(BLOOM_BYTES * 8, plan.entry_count);
        let total = u64::from(rdel_plan.blocks)
            .checked_add(plan.data_blocks)
            .and_then(|n| n.checked_add(3))
            .ok_or(Error::NoSpace)?;
        let total_usize = usize::try_from(total).map_err(|_| Error::NoSpace)?;
        // Reserve the run without claiming it: free list first, then the
        // bump. Neither moves until the manifest commit below has landed.
        let free_base = self.tbl_free.find_run(total_usize);
        let base = match free_base {
            Some(b) => b,
            None => self.tbl_bump.peek_run::<D::Error>(total)?,
        };
        // Pass 2: stream the blocks — the rdel section first, then the data
        // section at `base + rdel_blocks`. `data` doubles as the manifest
        // scratch below; it is a plain stack local. `cs` is this flush's
        // compression scratch: every data block is trial-compressed and
        // the compressed form kept when it saves enough.
        let mut data = [0u8; BLOCK];
        let mut cs = crate::compress::CompressScratch::<BLOCK>::new();
        let rdel_written = Self::write_flush_rdel(self.wal.device_mut(), &self.table, base).await?;
        debug_assert_eq!(rdel_written, rdel_plan.blocks);
        let data_base = base
            .checked_add(u64::from(rdel_plan.blocks))
            .ok_or(Error::NoSpace)?;
        let written = sstable::write_table::<D, BLOCK, BLOOM_BYTES, KEY_MAX>(
            self.wal.device_mut(),
            data_base,
            k,
            self.table
                .iter()
                .filter(|e| !e.range_del)
                .map(sstable::SstEntry::from),
            Some(&mut cs),
            rdel_plan.blocks,
        )
        .await?;
        debug_assert_eq!(
            written.checked_add(u64::from(rdel_plan.blocks)),
            Some(total)
        );
        // The manifest commit is the atomic visibility point. Stage the new
        // manifest in a copy and publish it only after the commit lands, so
        // a returned I/O error leaves the in-memory state exactly as it was
        // and the flush can simply be retried.
        let mut staged = self.manifest;
        let id = staged.alloc_table_id::<D::Error>()?;
        let tref = Self::flush_tref(id, base, total, &plan, &rdel_plan)?;
        staged.add_l0_table::<D::Error>(tref)?;
        // Advance the WAL head past the flushed records; wrap the region
        // when it is exhausted. Folded into this same atomic commit, so no
        // extra crash window opens between the wrap and its durability.
        let wrap = self.wal.next_block() >= self.cfg.wal_end;
        staged.set_wal_head(if wrap {
            self.cfg.wal_start
        } else {
            self.wal.next_block()
        });
        // Every issued mutation is now in a table or behind `wal_head`:
        // raise the persisted replay floor with this same commit.
        staged.note_flushed(self.next_seq);
        let (slot_a, slot_b) = (self.cfg.manifest_a, self.cfg.manifest_b);
        staged
            .commit(self.wal.device_mut(), &mut data, slot_a, slot_b)
            .await?;
        // Commit point passed: publish the staged state.
        self.manifest = staged;
        match free_base {
            Some(b) => {
                debug_assert_eq!(b, base);
                // Must run unconditionally: `claim_run` removes the run from
                // the free list, and `debug_assert!` does not evaluate its
                // argument in release builds. Wrapping the call itself in
                // `debug_assert!` would silently skip that removal in
                // release, leaving claimed blocks marked free forever.
                let claimed = self.tbl_free.claim_run(b, total_usize);
                debug_assert!(claimed, "find_run's own result must still claim");
            }
            None => {
                self.tbl_bump
                    .set_next(base.checked_add(total).ok_or(Error::NoSpace)?);
            }
        }
        // The flushed contents now live in the table; drop the memtable.
        self.table.clear();
        if wrap {
            self.wal.reset_to(self.cfg.wal_start);
        }
        Ok(())
    }
}

impl<
    D: BlockDevice,
    const BLOCK: usize,
    const KEY_MAX: usize,
    const VAL_MAX: usize,
    const CAP: usize,
    const ARENA: usize,
    const LEVELS: usize,
    const TABLES: usize,
    const BLOOM_BYTES: usize,
    const FREELIST: usize,
    const CACHE: usize,
> Db<D, BLOCK, KEY_MAX, VAL_MAX, CAP, ARENA, LEVELS, TABLES, BLOOM_BYTES, FREELIST, CACHE>
{
    /// Reports whether [`compact_step`](Db::compact_step) would select a
    /// compaction job right now: some level below the top holds `>= TABLES`
    /// tables. Unlike [`Progress::Done`], which a finished job also
    /// returns, this distinguishes "a job just finished, more may be
    /// pending" from "nothing to do" — firmware idle loops and test
    /// drivers use it to decide whether another `compact_step` is
    /// worthwhile.
    #[must_use]
    pub fn compaction_pending(&self) -> bool {
        if LEVELS < 2 || TABLES == 0 {
            return false;
        }
        for lvl in (0..LEVELS - 1).rev() {
            if let Some(tables) = self.manifest.level(lvl)
                && tables.len() >= TABLES
            {
                return true;
            }
        }
        false
    }

    /// The live table refs at `level`, oldest first — the archive
    /// candidate list. Returns `None` for an out-of-range level.
    #[must_use]
    pub fn level_tables(&self, level: usize) -> Option<&[TableRef<KEY_MAX>]> {
        self.manifest.level(level)
    }

    /// Plans the archival of one sealed table: returns its level and block
    /// range for upload, or `None` when `table_id` is not at `level`
    /// (already archived, compacted away, or never existed).
    ///
    /// The caller protocol:
    ///
    /// 1. Stream every block in `[plan.table.first_block,
    ///    plan.table.end_block())` — read through [`Db::device`] — to the
    ///    remote sink.
    /// 2. Confirm the bytes are durably stored remotely (the sink is the
    ///    caller's network code; horton never sees it).
    /// 3. Call [`Db::archive_commit`].
    ///
    /// Crash order is what makes this safe: a crash anywhere before the
    /// commit leaves the table in the manifest, so the upload simply
    /// repeats — the sink must therefore be idempotent per table id
    /// (re-uploading a fully uploaded table is always allowed). A crash
    /// during the commit is decided by the atomic manifest write: old
    /// slot (table still local, re-upload) or new slot (table gone, bytes
    /// already remote). The commit never runs before the upload is
    /// confirmed, so no crash can lose acknowledged data.
    ///
    /// Tombstone rule: the commit drops the table's tombstones from local
    /// view. If a deeper level holds an older version of a key the
    /// archived table deleted, that older version becomes visible locally
    /// again. With deletes in the workload, archive only from the
    /// bottommost level (nothing is deeper, so nothing can resurrect);
    /// insert-only workloads — sensor logs with timestamp keys — are safe
    /// from any level.
    #[must_use]
    pub fn archive_plan(&self, level: usize, table_id: u32) -> Option<ArchivePlan<KEY_MAX>> {
        let tables = self.manifest.level(level)?;
        let table = tables.iter().find(|t| t.id == table_id)?;
        Some(ArchivePlan {
            level,
            table: *table,
        })
    }

    /// Commits an archival: drops the table from the manifest in one
    /// atomic manifest write and reclaims its blocks into the free list.
    ///
    /// Call only after the table's bytes are durably stored remotely —
    /// once this returns `Ok(true)` the data is gone locally by design.
    /// Returns `Ok(false)` when the table is no longer at `level`
    /// (idempotent: safe to retry after a crash that may or may not have
    /// committed, or when a concurrent compaction already merged it away —
    /// the uploaded bytes are still a valid copy of that data).
    ///
    /// Reclamation is best-effort after the visibility point, exactly like
    /// compaction: a full free list cannot fail the commit, and
    /// un-reclaimed blocks become orphans the next [`Db::open`] sweep
    /// reclaims.
    ///
    /// # Errors
    ///
    /// [`Error::Device`] on I/O failure. The manifest write is atomic:
    /// either the table is gone (all of it) or it is still fully present.
    pub async fn archive_commit(
        &mut self,
        level: usize,
        table_id: u32,
    ) -> Result<bool, Error<D::Error>> {
        let Some(plan) = self.archive_plan(level, table_id) else {
            return Ok(false);
        };
        // Tombstone-rule enforcement (v0.12): refuse to remove a table
        // whose deletion would resurrect a shadowed value in any live
        // view. v0.10's "only deeper tables are hazardous" prose was
        // incomplete — a same-level, shallower, or re-ingested older copy
        // can resurrect a value too — so the check reasons over every
        // other table, not just deeper levels. Pure reads: the database
        // is untouched on refusal.
        self.check_no_resurrection(&plan.table).await?;
        let mut scratch = [0u8; BLOCK];
        let mut staged = self.manifest;
        if !staged.remove_table_from_level::<D::Error>(level, table_id)? {
            return Ok(false);
        }
        staged.raise_seq_high(self.next_seq);
        let (slot_a, slot_b) = (self.cfg.manifest_a, self.cfg.manifest_b);
        staged
            .commit(self.wal.device_mut(), &mut scratch, slot_a, slot_b)
            .await?;
        // Commit point passed: publish the staged state, then reclaim the
        // table's run strictly after the visibility point.
        self.manifest = staged;
        // The table's blocks are unreachable now; drop its cache entries
        // so their slots serve the hot set (hygiene — ids never repeat,
        // so stale entries could never be read).
        self.cache.invalidate_table(table_id);
        let mut k = 0u64;
        let blocks = u64::from(plan.table.block_count);
        while k < blocks {
            if let Some(id) = plan.table.first_block.checked_add(k)
                && self.tbl_free.insert::<D::Error>(id).is_err()
            {
                break;
            }
            k += 1;
        }
        Ok(true)
    }

    /// Tombstone-rule enforcement for [`Db::archive_commit`].
    ///
    /// Streams every tombstone in `candidate` and, for the live view and
    /// every registered snapshot watermark, compares the current read
    /// against a read excluding the candidate table. If the current view
    /// is deleted but the exclusion reveals a value, removing the
    /// candidate would resurrect that value and the archival is refused
    /// with [`Error::WouldResurrect`]. Pure reads; the database is
    /// untouched.
    async fn check_no_resurrection(
        &self,
        candidate: &TableRef<KEY_MAX>,
    ) -> Result<(), Error<D::Error>> {
        let mut key = [0u8; KEY_MAX];
        let mut val = [0u8; VAL_MAX];
        let mut stream =
            EntryStream::<D, BLOCK, KEY_MAX, VAL_MAX>::open(self.wal.device(), candidate).await?;
        // Copy the head key out so the stream borrow ends before the
        // read probes below.
        while let Some((k, seq, tombstone)) = stream.head() {
            let klen = k.len();
            key[..klen].copy_from_slice(k);
            if tombstone {
                // The live view plus every registered snapshot watermark.
                // Both probes use the same view watermark: a view whose
                // watermark predates the tombstone cannot see it and is
                // skipped, and for every other view the two reads differ
                // only by the candidate. (Reading the alternate at
                // `view.min(seq)` would hide later protective tombstones
                // and falsely report a resurrection.)
                let views = core::iter::once(u64::MAX)
                    .chain(self.snapshots[..self.n_snapshots].iter().copied());
                for view in views {
                    if seq > view {
                        continue;
                    }
                    let cur_hit = self
                        .get_at_excluding(&key[..klen], &mut val, view, None)
                        .await?;
                    if cur_hit.is_none() {
                        let alt_hit = self
                            .get_at_excluding(&key[..klen], &mut val, view, Some(candidate.id))
                            .await?;
                        if alt_hit.is_some() {
                            return Err(Error::WouldResurrect {
                                table: candidate.id,
                            });
                        }
                    }
                }
            }
            if !stream.advance().await? {
                break;
            }
        }
        // Range tombstones: detaching the candidate must not resurrect a
        // key the tombstone currently hides. For each tombstone, the live
        // view plus every snapshot that can see it must observe no
        // difference with the candidate excluded. Two conservative,
        // bounded checks (full key enumeration would be O(database)):
        //
        // 1. No other table's key range may overlap the tombstone range —
        //    a deeper table could hold a key this tombstone hides.
        // 2. No memtable point key inside the range may flip from
        //    hidden to visible when the candidate is excluded.
        //
        // Refusal is the safe answer; the documented remedy is to
        // compact first (merging the tombstone into the overlapping
        // table), then archive.
        let mut raw = [0u8; BLOCK];
        let mut b = 0u32;
        while b < candidate.rdel_blocks {
            let id =
                candidate
                    .first_block
                    .checked_add(u64::from(b))
                    .ok_or(Error::CorruptBlock {
                        id: candidate.first_block,
                    })?;
            poll_fn(|cx| self.wal.device().poll_read_block(cx, id, &mut raw))
                .await
                .map_err(Error::Device)?;
            let count = sstable::rdel_block_count::<D::Error, BLOCK>(&raw, id)?;
            let mut off = 0usize;
            let mut i = 0usize;
            while i < count {
                // Bounded by the stored count: the count/CRC trailer is
                // never parsed as entries.
                let (e, next) = sstable::rdel_parse_at(&raw[..], off)
                    .map_err(|()| Error::CorruptBlock { id })?;
                self.check_rdel_no_resurrection(
                    candidate, e.start, e.end, e.seq, &mut key, &mut val,
                )
                .await?;
                off = next;
                i += 1;
            }
            b += 1;
        }
        Ok(())
    }

    /// One range tombstone's share of the archival resurrection review.
    /// Refuses with [`Error::WouldResurrect`] unless the tombstone
    /// provably shadows nothing outside the candidate.
    async fn check_rdel_no_resurrection(
        &self,
        candidate: &TableRef<KEY_MAX>,
        start: &[u8],
        end: &[u8],
        seq: u64,
        key: &mut [u8; KEY_MAX],
        val: &mut [u8; VAL_MAX],
    ) -> Result<(), Error<D::Error>> {
        let refuse = || Error::WouldResurrect {
            table: candidate.id,
        };
        // Table bounds are inclusive; the tombstone end is exclusive, so
        // treating it as inclusive is conservative (a table starting
        // exactly at `end` holds no covered key, but flagging it only
        // skips an archive, never a result).
        let start_b = KeyBound::from_slice(start).ok_or(Error::CorruptBlock {
            id: candidate.first_block,
        })?;
        let end_b = KeyBound::from_slice(end).ok_or(Error::CorruptBlock {
            id: candidate.first_block,
        })?;
        for lvl in 0..LEVELS {
            let tables = self.manifest.level(lvl).ok_or(Error::NoSpace)?;
            for t in tables {
                if t.id != candidate.id && ranges_overlap(t.first_key, t.last_key, start_b, end_b) {
                    return Err(refuse());
                }
            }
        }
        // The live view plus every registered snapshot watermark. A view
        // whose watermark predates the tombstone cannot see it and is
        // skipped, mirroring the point-tombstone review.
        let views =
            core::iter::once(u64::MAX).chain(self.snapshots[..self.n_snapshots].iter().copied());
        for view in views {
            if seq > view {
                continue;
            }
            // The memtable iterator is key-ascending, so the range walk
            // stops at `end`. Range-tombstone slots are skipped (they
            // affect both reads equally); memtable point tombstones are
            // skipped (they win in both reads, so no flip is possible).
            for entry in &self.table {
                let k = entry.key;
                if k < start {
                    continue;
                }
                if k >= end {
                    break;
                }
                if entry.range_del || entry.tombstone {
                    continue;
                }
                let klen = k.len();
                key[..klen].copy_from_slice(k);
                let cur_hit = self.get_at_excluding(&key[..klen], val, view, None).await?;
                if cur_hit.is_none() {
                    let alt_hit = self
                        .get_at_excluding(&key[..klen], val, view, Some(candidate.id))
                        .await?;
                    if alt_hit.is_some() {
                        return Err(refuse());
                    }
                }
            }
        }
        Ok(())
    }

    /// [`Db::get_at`] with one table excluded from the read.
    ///
    /// `exclude` holds a table id to skip (the archival candidate under
    /// resurrection review, or `None` for a normal read). The memtable is
    /// always included.
    async fn get_at_excluding(
        &self,
        key: &[u8],
        val_buf: &mut [u8],
        max_seq: u64,
        exclude: Option<u32>,
    ) -> Result<Option<usize>, Error<D::Error>> {
        let mut acc = ReadAcc::<VAL_MAX>::new();

        if let Some(entry) = self.table.get_at(key, max_seq) {
            // The memtable holds the newest mutations; `get_at` already
            // selected the newest version at or below the snapshot.
            acc.best_seq = entry.seq;
            if entry.tombstone {
                acc.best = Best::Tombstone;
            } else {
                acc.stage[..entry.val.len()].copy_from_slice(entry.val);
                acc.best = Best::Value(entry.val.len());
                acc.best_expire_at = entry.expire_at;
            }
        }
        // Memtable range tombstones hide the key exactly like table ones;
        // expiry is read-time (`now = 0` here), so TTL values still count
        // as live for the resurrection review.
        if let Some(q) = self.table.max_covering_rdel(key, max_seq)
            && q > acc.cover_seq
        {
            acc.cover_seq = q;
        }

        let mut scratch = [0u8; BLOCK];
        let mut decomp = [0u8; BLOCK];
        // Level 0, newest table first: its tables overlap, and newer
        // tables hold higher sequence numbers.
        for tref in self.manifest.l0().iter().rev() {
            if Some(tref.id) != exclude {
                self.consider_table(tref, key, max_seq, &mut scratch, &mut decomp, &mut acc)
                    .await?;
            }
        }
        // Deeper levels in order. Highest-seq-wins keeps the result exact
        // regardless of how tables are placed.
        for li in 1..LEVELS {
            let tables = self.manifest.level(li).unwrap_or(&[]);
            for tref in tables {
                if Some(tref.id) != exclude {
                    self.consider_table(tref, key, max_seq, &mut scratch, &mut decomp, &mut acc)
                        .await?;
                }
            }
        }

        match acc.best {
            Best::Missing | Best::Tombstone => Ok(None),
            Best::Value(len) => {
                // A covering range tombstone newer than the point winner
                // hides the key. (`now = 0`: TTL values read as live, the
                // conservative choice for a resurrection review.)
                if acc.cover_seq > acc.best_seq {
                    return Ok(None);
                }
                if len > val_buf.len() {
                    return Err(Error::BufferTooSmall { need: len });
                }
                val_buf[..len].copy_from_slice(&acc.stage[..len]);
                Ok(Some(len))
            }
        }
    }

    /// Runs one bounded compaction step using the caller's `scratch`.
    ///
    /// When some level is full, the first call selects a job for the
    /// deepest full level — all of L0, or the oldest table of a deeper
    /// level — plus the overlapping tables of the level below, and merges
    /// them one output block per call ([`Progress::More`]); the call that
    /// exhausts the merge seals the output table and commits the manifest
    /// atomically, returning [`Progress::Done`]. With no full level this
    /// is a no-op returning [`Progress::Done`]. Note `Done` is returned in
    /// both cases, so `while db.compact_step(&mut scratch).await? ==
    /// Progress::More {}` drives exactly one job; loop on
    /// [`compaction_pending`](Db::compaction_pending) to drain every
    /// pending job.
    ///
    /// The scratch is reusable across jobs and droppable mid-job: partial
    /// output is invisible until the manifest commit, so abandoning it only
    /// orphans blocks the next `open()` sweep reclaims.
    ///
    /// # Errors
    ///
    /// [`Error::NoSpace`] when the job would exceed [`COMPACTION_KMAX`]
    /// inputs, no output run can be reserved, or the target level cannot
    /// absorb the output table (a full bottommost level the merge does not
    /// drain into — raised at select time, before any merge I/O);
    /// [`Error::CorruptBlock`] on a torn input table (compaction never
    /// silently drops entries); [`Error::Device`] on I/O failure. A failed
    /// step resets the scratch; the device manifest is untouched, so the
    /// job can be reselected later.
    pub async fn compact_step(
        &mut self,
        scratch: &mut Compaction<BLOCK, KEY_MAX, VAL_MAX, BLOOM_BYTES>,
    ) -> Result<Progress, Error<D::Error>> {
        if scratch.state == State::Idle && !self.compact_select(scratch).await? {
            return Ok(Progress::Done);
        }
        let outcome = match scratch.merge_step(self.wal.device_mut()).await {
            Ok(o) => o,
            Err(e) => {
                scratch.reset();
                return Err(e);
            }
        };
        match outcome {
            MergeOutcome::More => Ok(Progress::More),
            MergeOutcome::Exhausted => {
                if let Err(e) = self.compact_commit(scratch).await {
                    scratch.reset();
                    return Err(e);
                }
                Ok(Progress::Done)
            }
        }
    }

    /// Live snapshot watermarks, sorted descending. At most
    /// [`MAX_SNAPSHOTS`] items, so insertion sort is trivially bounded.
    const fn sorted_snapshot_watermarks(&self) -> ([u64; MAX_SNAPSHOTS], usize) {
        let mut n = 0usize;
        let mut sorted = [0u64; MAX_SNAPSHOTS];
        let mut j = 0usize;
        while j < self.n_snapshots {
            let s = self.snapshots[j];
            let mut i = n;
            while i > 0 && sorted[i - 1] < s {
                sorted[i] = sorted[i - 1];
                i -= 1;
            }
            sorted[i] = s;
            n += 1;
            j += 1;
        }
        (sorted, n)
    }

    /// Selects the next compaction job into `scratch`: the deepest full
    /// level's tables (all of L0, or the oldest table of a deeper level)
    /// plus the target level's overlapping tables. Returns `false` when no
    /// level is full and there is no work.
    /// Counts the output table's range-tombstone section with a dry-run of
    /// the rdel merge. A sorted cross-input merge is not a subsequence of
    /// the concatenation, so greedy repacking can use a different block
    /// count than the inputs' rdel blocks (more or fewer); only an exact
    /// count is reservation-safe. The write pass replays the same
    /// deterministic merge over the immutable input sections, so the
    /// budget always matches. The select-time oldest snapshot pins both
    /// passes: a snapshot taken mid-compaction always has a seq above
    /// every version being merged.
    async fn count_output_rdel(
        &mut self,
        c: &mut Compaction<BLOCK, KEY_MAX, VAL_MAX, BLOOM_BYTES>,
        job: &JobInputs<KEY_MAX>,
        bottommost: bool,
        oldest_snapshot: u64,
    ) -> Result<u32, Error<D::Error>> {
        let device = self.wal.device_mut();
        crate::compact::count_rdel_merge(
            &*device,
            &c.inputs[..job.n_inputs],
            &mut c.raw,
            bottommost,
            oldest_snapshot,
        )
        .await
    }

    /// Reports whether a compaction output at `tgt` covering
    /// `[first, last]` is bottommost: tombstones drop only when the output
    /// reaches the bottommost level holding the merged range, because
    /// nothing below can hide an older version of a dropped key.
    fn is_bottommost_output(
        &self,
        tgt: usize,
        first: KeyBound<KEY_MAX>,
        last: KeyBound<KEY_MAX>,
    ) -> bool {
        for lvl in tgt + 1..LEVELS {
            let Some(tables) = self.manifest.level(lvl) else {
                break;
            };
            for t in tables {
                if ranges_overlap(first, last, t.first_key, t.last_key) {
                    return false;
                }
            }
        }
        true
    }

    async fn compact_select(
        &mut self,
        c: &mut Compaction<BLOCK, KEY_MAX, VAL_MAX, BLOOM_BYTES>,
    ) -> Result<bool, Error<D::Error>> {
        // v0.8: every level compacts. The deepest full level is selected
        // first, so a job's target always has room: a full non-bottom
        // target would have been selected itself.
        if LEVELS < 2 || TABLES == 0 {
            return Ok(false);
        }
        let mut src = LEVELS;
        for lvl in (0..LEVELS - 1).rev() {
            let tables = self.manifest.level(lvl).ok_or(Error::NoSpace)?;
            if tables.len() >= TABLES {
                src = lvl;
                break;
            }
        }
        if src >= LEVELS - 1 {
            return Ok(false);
        }
        let tgt = src + 1;
        // Copy the refs so the manifest borrow ends before the scratch and
        // the device are touched.
        let mut src_refs = [TableRef::EMPTY; TABLES];
        let mut tgt_refs = [TableRef::EMPTY; TABLES];
        let (src_take, tgt_len, tgt_take) = {
            let manifest = &self.manifest;
            let s = manifest.level(src).ok_or(Error::NoSpace)?;
            let t = manifest.level(tgt).ok_or(Error::NoSpace)?;
            let st = s.len().min(TABLES);
            let tt = t.len().min(TABLES);
            src_refs[..st].copy_from_slice(&s[..st]);
            tgt_refs[..tt].copy_from_slice(&t[..tt]);
            (st, t.len(), tt)
        };

        let job =
            Self::select_job_inputs(c, src, &src_refs[..src_take], tgt, &tgt_refs[..tgt_take])?;
        // The target absorbs the output table: it must fit once the
        // overlapping inputs leave. Checked here — before any merge I/O —
        // instead of failing at commit.
        if tgt_len - job.n_tgt_inputs + 1 > TABLES {
            return Err(Error::NoSpace);
        }
        let bottommost = self.is_bottommost_output(tgt, job.first, job.last);
        // The output's range-tombstone section is fully determined by the
        // inputs, so its exact block budget is counted first with a
        // dry-run of the merge. A sorted cross-input merge is not a
        // subsequence of the concatenation, so greedy repacking can use a
        // different block count than the inputs' rdel blocks (more or
        // fewer); only an exact count is reservation-safe. The write pass
        // replays the same deterministic merge over the immutable input
        // sections, so the budget always matches (debug-asserted below).
        // The select-time oldest snapshot pins both passes: a snapshot
        // taken mid-compaction always has a seq above every version being
        // merged.
        let oldest_snapshot = self.oldest_snapshot_seq();
        let rdel_budget = self
            .count_output_rdel(c, &job, bottommost, oldest_snapshot)
            .await?;
        // Reserve the output run: the data merge only shrinks the inputs
        // (dedup plus tombstone drops), so their data blocks always
        // suffice for the data section; the rdel section gets its exact
        // counted budget; plus the 3 framing blocks. Free list first,
        // then the bump — claimed only after the manifest commit, exactly
        // like flush.
        let out_blocks = job
            .total_data
            .checked_add(u64::from(rdel_budget))
            .ok_or(Error::NoSpace)?
            .checked_add(3)
            .ok_or(Error::NoSpace)?;
        let out_len = usize::try_from(out_blocks).map_err(|_| Error::NoSpace)?;
        let (out_base, from_free) = match self.tbl_free.find_run(out_len) {
            Some(b) => (b, true),
            None => (self.tbl_bump.peek_run::<D::Error>(out_blocks)?, false),
        };
        c.out_base = out_base;
        c.out_blocks = out_blocks;
        c.out_len = out_len;
        c.from_free = from_free;
        c.target_level = tgt;
        c.bottommost = bottommost;
        // The version-retention set: snapshots live at select time pin this
        // compaction's keep-set. Watermarks are stored descending so the
        // merge can walk its thresholds (live view, then each snapshot) in
        // order. A snapshot taken mid-compaction always has a seq above
        // every version being merged, so the select-time set is exactly the
        // history that needs protection.
        let (sorted, n) = self.sorted_snapshot_watermarks();
        c.snapshots = sorted;
        c.n_snapshots = n;
        c.oldest_snapshot = oldest_snapshot;
        c.n_inputs = job.n_inputs;
        // The output's range-tombstone section streams out now, before the
        // data merge starts: a bounded k-way merge over the inputs'
        // sorted rdel sections (see `RdelMerger`). Crash story matches the
        // data blocks: invisible until the manifest commit, orphans swept
        // on open.
        let mut rdel_out = sstable::RdelWriter::<BLOCK>::new(out_base);
        let mut rdel_stats = crate::compact::RdelStats::<KEY_MAX>::new();
        {
            let device = self.wal.device_mut();
            let mut merger = crate::compact::RdelMerger::<KEY_MAX>::new(
                &c.inputs[..job.n_inputs],
                bottommost,
                c.oldest_snapshot,
            );
            while merger.next_merged(&*device, &mut c.raw).await? {
                let e = merger.current_entry();
                rdel_out.push(&mut *device, e).await?;
                rdel_stats.observe::<D::Error>(&e, out_base)?;
            }
            c.rdel_blocks = rdel_out.finish(&mut *device).await?;
        }
        debug_assert_eq!(
            c.rdel_blocks, rdel_budget,
            "rdel merge replay diverged from its counted budget"
        );
        c.rdel_first = rdel_stats.first;
        c.rdel_last = rdel_stats.last;
        c.rdel_max_seq = rdel_stats.max_seq;
        // The data section starts after the rdel section.
        let data_base = out_base
            .checked_add(u64::from(c.rdel_blocks))
            .ok_or(Error::NoSpace)?;
        c.writer = sstable::TableWriter::new(
            data_base,
            sstable::bloom_k(BLOOM_BYTES * 8, job.total_entries),
        );
        // Position one cursor per input on its first entry.
        let device = self.wal.device_mut();
        for i in 0..job.n_inputs {
            let tref = c.inputs[i].tref;
            init_cursor(&*device, &mut c.raw, &tref, &mut c.cursors[i]).await?;
        }
        c.state = State::Merging;
        Ok(true)
    }

    /// Pushes one compaction job's input tables into the scratch: all of a
    /// full L0, or the oldest table of a full deeper level, plus the target
    /// level's tables overlapping the merged range. The merged range starts
    /// as the source span and widens as overlapping target tables join, so
    /// the output span can never swallow a surviving target table.
    ///
    /// `src_tables` is never empty: the caller only selects levels holding
    /// `>= TABLES >= 1` tables.
    fn select_job_inputs(
        c: &mut Compaction<BLOCK, KEY_MAX, VAL_MAX, BLOOM_BYTES>,
        src: usize,
        src_tables: &[TableRef<KEY_MAX>],
        tgt: usize,
        tgt_tables: &[TableRef<KEY_MAX>],
    ) -> Result<JobInputs<KEY_MAX>, Error<D::Error>> {
        let mut job = JobInputs {
            n_inputs: 0,
            n_tgt_inputs: 0,
            first: src_tables[0].first_key,
            last: src_tables[0].last_key,
            total_entries: 0,
            total_data: 0,
        };
        // Pushes one input table, widening the merged range and totals.
        let mut push = |level: usize, t: &TableRef<KEY_MAX>| -> Result<(), Error<D::Error>> {
            if job.n_inputs >= COMPACTION_KMAX {
                return Err(Error::NoSpace);
            }
            c.inputs[job.n_inputs] = Input { level, tref: *t };
            job.n_inputs += 1;
            job.total_entries = job
                .total_entries
                .checked_add(u64::from(t.entry_count))
                .ok_or(Error::NoSpace)?;
            let rdel = u64::from(t.rdel_blocks);
            let data = u64::from(t.block_count)
                .checked_sub(rdel)
                .ok_or(Error::CorruptBlock { id: t.first_block })?
                .checked_sub(3)
                .ok_or(Error::CorruptBlock { id: t.first_block })?;
            job.total_data = job.total_data.checked_add(data).ok_or(Error::NoSpace)?;
            Ok(())
        };
        if src == 0 {
            // L0 tables may overlap each other, so the whole level joins.
            for t in src_tables {
                push(0, t)?;
                if t.first_key.as_slice() < job.first.as_slice() {
                    job.first = t.first_key;
                }
                if t.last_key.as_slice() > job.last.as_slice() {
                    job.last = t.last_key;
                }
            }
        } else {
            // Deeper levels never overlap within themselves: compact the
            // oldest table. Index 0 is FIFO with no extra state, because
            // the picked table leaves the level.
            push(src, &src_tables[0])?;
        }
        // Overlapping target tables join the merge (target runs never
        // overlap each other, so range overlap is the exact join
        // condition).
        for t in tgt_tables {
            if ranges_overlap(job.first, job.last, t.first_key, t.last_key) {
                push(tgt, t)?;
                job.n_tgt_inputs += 1;
                if t.first_key.as_slice() < job.first.as_slice() {
                    job.first = t.first_key;
                }
                if t.last_key.as_slice() > job.last.as_slice() {
                    job.last = t.last_key;
                }
            }
        }
        Ok(job)
    }

    /// Commits the finished merge: seals the output table (unless the merge
    /// produced no entries), swaps the input tables for it in a staged
    /// manifest, and claims the output run after the commit lands.
    async fn compact_commit(
        &mut self,
        c: &mut Compaction<BLOCK, KEY_MAX, VAL_MAX, BLOOM_BYTES>,
    ) -> Result<(), Error<D::Error>> {
        let mut scratch = [0u8; BLOCK];
        let mut staged = self.manifest;
        // Seal the output table first: finish flushes, so its blocks are
        // durable before the manifest makes them visible. A merge that
        // kept only range tombstones (no point entries) still seals a
        // table — range-only tables are first-class (zero data blocks).
        let out_ref = if c.writer.entry_count() > 0 || c.rdel_blocks > 0 {
            let done: sstable::FinishedTable<KEY_MAX> = c
                .writer
                .finish(self.wal.device_mut(), Some(&mut c.compress), c.rdel_blocks)
                .await?;
            let total = u64::from(c.rdel_blocks)
                .checked_add(done.data_blocks)
                .and_then(|n| n.checked_add(3))
                .ok_or(Error::NoSpace)?;
            let id = staged.alloc_table_id::<D::Error>()?;
            Some(TableRef {
                id,
                first_block: c.out_base,
                block_count: u32::try_from(total).map_err(|_| Error::NoSpace)?,
                // `KeyBound::min/max` let `EMPTY` lose, so a missing
                // section never corrupts the bounds.
                first_key: done.first_key.min(c.rdel_first),
                last_key: done.last_key.max(c.rdel_last),
                max_seq: done.max_seq.max(c.rdel_max_seq),
                entry_count: u32::try_from(done.entry_count).map_err(|_| Error::NoSpace)?,
                rdel_blocks: c.rdel_blocks,
            })
        } else {
            None
        };
        for input in c.inputs.iter().take(c.n_inputs) {
            staged.remove_table_from_level::<D::Error>(input.level, input.tref.id)?;
        }
        if let Some(tref) = out_ref {
            staged.add_table_to_level::<D::Error>(c.target_level, tref)?;
        }
        // Compaction may drop the tables holding the newest sequences
        // (bottommost tombstones): persist the counter so it never regresses.
        staged.raise_seq_high(self.next_seq);
        let (slot_a, slot_b) = (self.cfg.manifest_a, self.cfg.manifest_b);
        staged
            .commit(self.wal.device_mut(), &mut scratch, slot_a, slot_b)
            .await?;
        // Commit point passed: publish the staged state, then claim the
        // output run (free list or bump) exactly like flush does.
        self.manifest = staged;
        if out_ref.is_some() {
            if c.from_free {
                // Must run unconditionally: `claim_run` removes the run from
                // the free list, and `debug_assert!` does not evaluate its
                // argument in release builds. (Same fix as flush's claim.)
                let claimed = self.tbl_free.claim_run(c.out_base, c.out_len);
                debug_assert!(claimed, "merge's own reservation must still claim");
            } else {
                self.tbl_bump
                    .set_next(c.out_base.checked_add(c.out_blocks).ok_or(Error::NoSpace)?);
            }
        }
        // Reclaim the input runs strictly after the visibility point: the
        // manifest no longer references them, so they are orphans.
        // Best-effort — the commit already happened, so a full free list
        // must not fail the compaction; un-reclaimed blocks stay orphans
        // and the next open() sweep reclaims them.
        for input in c.inputs.iter().take(c.n_inputs) {
            // Drop the retired table's cache entries so their slots serve
            // the hot set (hygiene — ids never repeat, so stale entries
            // could never be read).
            self.cache.invalidate_table(input.tref.id);
            let mut k = 0u64;
            let blocks = u64::from(input.tref.block_count);
            while k < blocks {
                if let Some(id) = input.tref.first_block.checked_add(k)
                    && self.tbl_free.insert::<D::Error>(id).is_err()
                {
                    break;
                }
                k += 1;
            }
        }
        c.reset();
        Ok(())
    }
}
