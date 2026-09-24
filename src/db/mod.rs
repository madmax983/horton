//! The database: WAL + memtable + `SSTables` in fixed table slots.
//!
//! Write path is WAL-first: every mutation is appended to the WAL and
//! committed before it lands in the memtable, so a crash can only lose the
//! un-acknowledged tail. [`Db::flush`] drains the memtable into an immutable
//! `SSTable` in a free table slot and commits the manifest; the manifest
//! commit is the single atomic visibility point — a crash before it leaves
//! the old manifest plus a replayable WAL, a crash after it leaves the new
//! state. Reads are served from the memtable first, then level 0 (newest
//! table first), then deeper levels in order; every table is key-range
//! pruned and bloom-gated, and the hit with the highest sequence number
//! wins.
//!
//! This module holds the handle, `open()`, and the write path; the rest of
//! [`Db`]'s methods live by concern in `read`, `flush`, `compaction`,
//! `archive` (archive and ingest), and `invariants`.

use core::cell::RefCell;

use crate::alloc::{MAX_SLOTS, SlotMap};
use crate::batch::WriteBatch;
use crate::cache::{BlockCache, CachePort, CacheStats};
use crate::device::BlockDevice;
use crate::error::Error;
use crate::manifest::{Manifest, TableRef};
use crate::memtable::MemTable;
use crate::sstable;
use crate::wal::{Op, RecoverState, WAL_RECORD_OVERHEAD, WalWriter};

mod archive;
mod compaction;
mod flush;
mod invariants;
mod read;

pub use archive::{ArchivePlan, SealedTable};

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

/// Table-region occupancy, from [`Db::slot_stats`].
///
/// The table region is `slots` fixed slots of `slot_blocks` blocks, one
/// table per slot (see [`SlotMap`]). `used + reserved + free == slots`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotStats {
    /// Total table slots (`LEVELS * TABLES`).
    pub slots: u32,
    /// Blocks per slot: the largest table the region holds.
    pub slot_blocks: u64,
    /// Slots holding a live table.
    pub used: u32,
    /// Slots reserved by the in-flight compaction job.
    pub reserved: u32,
    /// Slots available to the next flush, ingest, or compaction output.
    pub free: u32,
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

/// Maximum live snapshots. Snapshot slots are plain `u64`s in the `Db`;
/// the bound keeps that state tiny and the exhaustion error explicit.
pub(crate) const MAX_SNAPSHOTS: usize = 8;

/// The database handle. Owns the WAL writer (and through it, the device),
/// the memtable, the manifest, and the table-region slot allocator.
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
    // Block-cache slots (cache::BlockCache). 0 disables the cache.
    const CACHE: usize,
> {
    wal: WalWriter<D, BLOCK>,
    table: MemTable<CAP, ARENA, KEY_MAX, VAL_MAX>,
    manifest: Manifest<LEVELS, TABLES, KEY_MAX>,
    /// The table region as `LEVELS * TABLES` fixed slots, one table each
    /// (see [`SlotMap`]). Rebuilt from the manifest by `open()`.
    slots: SlotMap,
    /// Generation of the compaction job in flight, stamped into its
    /// [`Compaction`] scratch at select time. A scratch whose stamp no
    /// longer matches (another scratch started a job, or the job was
    /// aborted) is stale and is reset before it can touch the device.
    job_gen: u32,
    /// True while a compaction job holds slot reservations.
    job_active: bool,
    /// Slots of the active job's input tables, one bit per slot. Archiving
    /// one of them aborts the job (its merge still reads the table).
    job_inputs: u64,
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
    /// True once [`Db::open`] has recovered the database. Every operation
    /// that reads or writes the device checks it and returns
    /// [`Error::NotOpen`] otherwise: before recovery the WAL append
    /// position is `wal_start`, so a write would clobber live WAL blocks.
    opened: bool,
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
    const CACHE: usize,
> Db<D, BLOCK, KEY_MAX, VAL_MAX, CAP, ARENA, LEVELS, TABLES, BLOOM_BYTES, CACHE>
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
    const ASSERT_SLOTS: () = assert!(
        LEVELS >= 1 && TABLES >= 1 && LEVELS * TABLES <= MAX_SLOTS,
        "LEVELS * TABLES (one table slot per manifest entry) must be within 1..=64"
    );

    /// Table slots: one per manifest table ref.
    const SLOTS: usize = LEVELS * TABLES;

    /// Smallest usable slot: a full memtable's table must fit one, or the
    /// write path could wedge on a flush that never fits.
    const MIN_SLOT_BLOCKS: u64 = sstable::max_flush_blocks::<BLOCK>(CAP, ARENA, KEY_MAX, VAL_MAX);

    /// Free slots flush and ingest leave untouched, so a compaction job can
    /// always reserve its output slot: compaction is what frees slots
    /// (several inputs become one output), so taking the last free slot
    /// for a flush would wedge the database.
    const COMPACTION_RESERVE: u32 = 1;

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
        let () = Self::ASSERT_SLOTS;
        Self {
            wal: WalWriter::new(device, config.wal_start, config.wal_end),
            table: MemTable::new(),
            manifest: Manifest::new(),
            slots: SlotMap::new(),
            job_gen: 0,
            job_active: false,
            job_inputs: 0,
            cfg: config,
            next_seq: 0,
            snapshots: [0u64; MAX_SNAPSHOTS],
            n_snapshots: 0,
            opened: false,
            get_scratch: RefCell::new([0u8; BLOCK]),
            decomp_scratch: RefCell::new([0u8; BLOCK]),
            cache: RefCell::new(BlockCache::new()),
        }
    }

    /// Whether [`open`](Db::open) has completed successfully on this handle.
    #[must_use]
    pub const fn is_open(&self) -> bool {
        self.opened
    }

    /// `Err(NotOpen)` unless [`open`](Db::open) has succeeded.
    pub(crate) const fn ensure_open(&self) -> Result<(), Error<D::Error>> {
        if self.opened {
            Ok(())
        } else {
            Err(Error::NotOpen)
        }
    }

    /// Picks the slot for a flushed or ingested table of `blocks` blocks.
    /// Nothing is reserved: the caller holds `&mut self` from here to its
    /// manifest commit and then [`claim`](SlotMap::claim)s the slot, so no
    /// other operation can take it in between, and a future dropped
    /// mid-write leaks nothing. The last [`COMPACTION_RESERVE`] free slots
    /// are left for compaction.
    ///
    /// [`COMPACTION_RESERVE`]: Self::COMPACTION_RESERVE
    fn free_slot_for(&self, blocks: u64) -> Result<u32, Error<D::Error>> {
        if blocks > self.slots.slot_blocks() || self.slots.free_slots() <= Self::COMPACTION_RESERVE
        {
            return Err(Error::NoSpace);
        }
        self.slots.find_free().ok_or(Error::NoSpace)
    }

    /// Abandons the in-flight compaction job, if any: its reserved output
    /// slot returns to the free set, its staged output refs are dropped,
    /// and its scratch goes stale (reset on its next `compact_step`).
    /// Committed state is untouched — the job's inputs are all still live,
    /// so a later select simply redoes it.
    fn abort_job(&mut self) {
        if self.job_active {
            self.slots.release_all();
            self.manifest.clear_pending();
            self.job_active = false;
            self.job_inputs = 0;
        }
    }

    /// The slot of live table `t` (`None` only for a ref the slot map
    /// never accepted, which `open()` rules out).
    fn slot_of(&self, t: &TableRef<KEY_MAX>) -> Option<u32> {
        self.slots.slot_of(t.first_block, u64::from(t.block_count))
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
        if !self.opened {
            return Err(Error::NotOpen);
        }
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

    /// Table-region occupancy: how many table slots are used, reserved by
    /// an in-flight compaction job, or free. Zeroed before
    /// [`open`](Db::open).
    #[must_use]
    pub const fn slot_stats(&self) -> SlotStats {
        SlotStats {
            slots: self.slots.slots(),
            slot_blocks: self.slots.slot_blocks(),
            used: self.slots.used_slots(),
            reserved: self.slots.reserved_slots(),
            free: self.slots.free_slots(),
        }
    }

    /// Test-visible block-cache counters: hits, misses, occupancy. Lets
    /// callers prove the cache is earning its RAM instead of trusting us.
    #[must_use]
    pub fn cache_stats(&self) -> CacheStats {
        self.cache.stats()
    }

    /// Opens the database: recovers the manifest, rebuilds the table-slot
    /// map from it, replays the WAL from the manifest's `wal_head` into a
    /// fresh memtable, and resumes the sequence counter and the WAL append
    /// position. Idempotent.
    ///
    /// The table region is laid out as `LEVELS * TABLES` equal slots (see
    /// [`SlotMap`]); a slot is used exactly when a manifest table lives in
    /// it. Blocks left behind by a torn flush or an abandoned compaction
    /// need no sweep: they sit in a slot no table references, which is
    /// simply free. Any in-flight compaction job is forgotten (its scratch
    /// resets on its next step).
    ///
    /// # Errors
    ///
    /// [`Error::NoSpace`] when the table region is too small for the slot
    /// layout (each slot must hold a full memtable's table),
    /// [`Error::CorruptManifest`] (also when a live table does not sit
    /// wholly inside its own slot), [`Error::CorruptWal`], or
    /// [`Error::Device`].
    pub async fn open(&mut self) -> Result<OpenReport, Error<D::Error>> {
        // A failed or interrupted open leaves the handle closed.
        self.opened = false;
        self.table.clear();
        self.job_active = false;
        self.job_inputs = 0;
        self.job_gen = self.job_gen.wrapping_add(1);
        let mut slots = SlotMap::layout(
            self.cfg.tbl_start,
            self.cfg.tbl_end,
            Self::SLOTS,
            Self::MIN_SLOT_BLOCKS,
        )
        .ok_or(Error::NoSpace)?;
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
        // Rebuild the used set: every live table must sit wholly inside its
        // own slot. A table straddling a slot boundary, or two tables in
        // one slot, cannot have been written under this layout — the
        // manifest or the configuration is wrong, and guessing would hand
        // out live blocks. Next-fit resumes past the newest table, so
        // allocation keeps rotating across reboots.
        let mut newest: Option<(u32, u32)> = None;
        for t in self.manifest.tables() {
            let slot = slots
                .slot_of(t.first_block, u64::from(t.block_count))
                .ok_or(Error::CorruptManifest)?;
            if !slots.mark_used(slot) {
                return Err(Error::CorruptManifest);
            }
            if newest.is_none_or(|(id, _)| t.id > id) {
                newest = Some((t.id, slot));
            }
        }
        if let Some((_, slot)) = newest {
            slots.set_hint_after(slot);
        }
        self.slots = slots;
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
        self.opened = true;
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
        self.ensure_open()?;
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
        self.ensure_open()?;
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
        self.ensure_open()?;
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
        self.ensure_open()?;
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
        self.ensure_open()?;
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

    /// The live table refs at `level`, oldest first — the archive
    /// candidate list. Returns `None` for an out-of-range level.
    #[must_use]
    pub fn level_tables(&self, level: usize) -> Option<&[TableRef<KEY_MAX>]> {
        self.manifest.level(level)
    }
}
