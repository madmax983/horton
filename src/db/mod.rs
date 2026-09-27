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

use crate::batch::WriteBatch;
use crate::cache::{BlockCache, CachePort, CacheStats};
use crate::compact::COMPACTION_KMAX;
use crate::device::BlockDevice;
use crate::error::Error;
use crate::manifest::{Manifest, ManifestLayout, TableRef};
use crate::memtable::MemTable;
use crate::slots::{MAX_SLOTS, SlotMap};
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
/// Three disjoint regions: the WAL, the table region, and the manifest's
/// copies. Each manifest copy spans `Manifest::max_blocks` blocks (a
/// compile-time function of the `Db` shape — one block for small shapes):
/// by default two copies, starting at `manifest_a` and `manifest_b`; with
/// [`with_manifest_ring`](Config::with_manifest_ring), `n` copies back to
/// back from `manifest_a`. [`Db::open`] refuses overlapping regions with
/// [`Error::BadConfig`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Config {
    /// First block id of the WAL region.
    pub wal_start: u64,
    /// One past the last block id of the WAL region.
    pub wal_end: u64,
    /// First block id of the `SSTable` region.
    pub tbl_start: u64,
    /// One past the last block id of the `SSTable` region.
    pub tbl_end: u64,
    /// First block of the first manifest copy.
    pub manifest_a: u64,
    /// First block of the second manifest copy (unused by a ring).
    pub manifest_b: u64,
    /// Manifest copies in a ring from `manifest_a`; 0 for the default pair.
    pub manifest_ring: u32,
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
            manifest_ring: 0,
        }
    }

    /// Keeps `copies` manifest copies back to back from `manifest_a`
    /// instead of the pair at `manifest_a` and `manifest_b`. Commits
    /// rotate through the copies, so each copy's blocks are erased once
    /// per `copies` commits: on NOR flash, where the manifest is the most
    /// frequently erased region, this multiplies its life. Fewer than two
    /// copies are treated as two.
    #[must_use]
    pub const fn with_manifest_ring(mut self, copies: u32) -> Self {
        self.manifest_ring = if copies < 2 { 2 } else { copies };
        self
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
///
/// # Type parameters
///
/// Prefer [`db_types!`](crate::db_types), which takes these by name.
///
/// | Parameter | Meaning | Checked at compile time |
/// |---|---|---|
/// | `D` | the [`BlockDevice`] | `D::BLOCK == BLOCK` |
/// | `BLOCK` | device block size in bytes | fits the largest WAL record, range tombstone, and bloom filter; exceeds the 28-byte manifest block frame |
/// | `KEY_MAX` | longest key | `1..=65535` |
/// | `VAL_MAX` | longest value | `..=65535` |
/// | `CAP` | memtable entry slots | |
/// | `ARENA` | memtable key/value bytes | |
/// | `LEVELS` | LSM levels. `1` disables compaction: level 0 fills, then flush reports [`Error::RegionFull`] | `LEVELS * TABLES` within `1..=64` |
/// | `TABLES` | tables per level (L0's limit; deeper levels share the pool) | below [`COMPACTION_KMAX`] |
/// | `BLOOM_BYTES` | bloom filter bytes per table | `1..=BLOCK-4` |
/// | `CACHE` | block-cache slots; `0` disables the cache | |
///
/// The region layout in [`Config`] is checked by [`open`](Db::open)
/// ([`Error::BadConfig`]).
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
    /// [`Error::SnapshotLimit`]. Snapshots are in-memory only — they do not
    /// survive `open()`.
    snapshots: [u64; MAX_SNAPSHOTS],
    n_snapshots: usize,
    /// True once [`Db::open`] has recovered the database. Every operation
    /// that reads or writes the device checks it and returns
    /// [`Error::NotOpen`] otherwise: before recovery the WAL append
    /// position is `wal_start`, so a write would clobber live WAL blocks.
    opened: bool,
    /// Block-read buffer for point reads, and the block scratch of every
    /// `&mut self` operation (manifest commits, ingest's copy): their
    /// futures hold no block buffer of their own (F8).
    ///
    /// The buffer sits in a `RefCell` so `get` can use it through
    /// `&self`. Two `get` calls polled concurrently on one handle cannot
    /// both have it: the second returns [`Error::Busy`] rather than carry
    /// a fallback buffer in every `get` future. `&mut self` operations
    /// reach it with `get_mut`, which no read can contend with.
    get_scratch: RefCell<[u8; BLOCK]>,
    /// Decompression buffer for point reads: data blocks flagged
    /// compressed inflate into here before parsing (see
    /// [`sstable::TableReader::lookup_at`](crate::sstable::TableReader::lookup_at)).
    /// Same borrow discipline as `get_scratch`.
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

/// One single-op write, for [`Db::commit_mutation`].
#[derive(Debug, Clone, Copy)]
enum Mutation<'a> {
    Put {
        key: &'a [u8],
        val: &'a [u8],
    },
    PutTtl {
        key: &'a [u8],
        val: &'a [u8],
        expire_at: u64,
    },
    Delete {
        key: &'a [u8],
    },
    RangeDelete {
        start: &'a [u8],
        end: &'a [u8],
    },
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
    const ASSERT_JOB: () = assert!(
        TABLES < COMPACTION_KMAX,
        "TABLES must be below COMPACTION_KMAX: an L0 job reads every L0 table \
         through its own cursor, plus one cursor for the level below"
    );
    const ASSERT_RDEL: () = assert!(
        12 + 2 * KEY_MAX + 6 <= BLOCK,
        "BLOCK must fit one maximal range tombstone (12 + 2 * KEY_MAX bytes) plus its trailer"
    );
    // `&&` short-circuits in const evaluation: `max_blocks` (which divides
    // by the body bytes per block) is only evaluated when a block has any.
    const ASSERT_MANIFEST: () = assert!(
        BLOCK > crate::manifest::BLOCK_FRAME
            && Manifest::<LEVELS, TABLES, KEY_MAX>::max_blocks::<BLOCK>() <= 0xFFFF,
        "BLOCK must exceed the 28-byte manifest block frame, and one manifest copy \
         (Manifest::max_blocks) must span at most 65535 blocks"
    );

    /// Table slots: one per manifest table ref.
    const SLOTS: usize = LEVELS * TABLES;

    /// Smallest usable slot: a full memtable's table must fit one, or the
    /// write path could wedge on a flush that never fits.
    const MIN_SLOT_BLOCKS: u64 = sstable::max_flush_blocks::<BLOCK>(CAP, ARENA, KEY_MAX, VAL_MAX);

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
        let () = Self::ASSERT_JOB;
        let () = Self::ASSERT_RDEL;
        let () = Self::ASSERT_MANIFEST;
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
    /// mid-write leaks nothing. Compaction keeps [`COMPACTION_RESERVE`]
    /// slots of headroom — free, or already reserved by the running job —
    /// that flush and ingest never take.
    ///
    /// # Errors
    ///
    /// [`Error::TableTooLarge`] when the table is larger than a slot;
    /// [`Error::NeedsCompaction`] when no slot is free beyond the reserve
    /// but compaction has work that frees slots (a pending or in-flight
    /// job); [`Error::RegionFull`] when it has none.
    ///
    /// [`COMPACTION_RESERVE`]: Self::COMPACTION_RESERVE
    fn free_slot_for(&self, blocks: u64) -> Result<u32, Error<D::Error>> {
        if blocks > self.slots.slot_blocks() {
            return Err(Error::TableTooLarge);
        }
        let headroom = self.slots.free_slots() + self.slots.reserved_slots();
        let slot = if headroom > Self::COMPACTION_RESERVE {
            self.slots.find_free()
        } else {
            None
        };
        slot.ok_or_else(|| self.no_room())
    }

    /// The error for "no room for another table": [`Error::NeedsCompaction`]
    /// while compaction has work that frees room, else
    /// [`Error::RegionFull`]. Callers that retry after compacting therefore
    /// never spin: `NeedsCompaction` implies
    /// [`compaction_pending`](Self::compaction_pending).
    fn no_room(&self) -> Error<D::Error> {
        if self.compaction_pending() {
            Error::NeedsCompaction
        } else {
            Error::RegionFull
        }
    }

    /// Abandons the in-flight compaction job, if any: its reserved output
    /// slot returns to the free set and its scratch goes stale (reset on
    /// its next `compact_step`). What the job already committed stays — a
    /// consistent tree whose inputs were retired or narrowed past each
    /// committed output — and a later select merges on from there.
    const fn abort_job(&mut self) {
        if self.job_active {
            self.slots.release_all();
            self.job_active = false;
            self.job_inputs = 0;
        }
    }

    /// Slots of headroom compaction keeps for itself — free, or already
    /// reserved by the running job — which flush and ingest never take:
    /// the output slot a job writes into, plus one slot of room for the
    /// sources' data, which drains into outputs before the sources retire.
    /// Jobs are admitted against the actual free slots (see
    /// `compact_select`), shrinking an L0 job to its oldest tables when
    /// slots are short. A single-level tree never compacts.
    const COMPACTION_RESERVE: u32 = if LEVELS >= 2 { 2 } else { 0 };

    /// Where the manifest's copies live.
    const fn manifest_layout(&self) -> ManifestLayout {
        if self.cfg.manifest_ring == 0 {
            ManifestLayout::pair(self.cfg.manifest_a, self.cfg.manifest_b)
        } else {
            ManifestLayout::ring(self.cfg.manifest_a, self.cfg.manifest_ring)
        }
    }

    /// Refuses a configuration whose regions are empty or overlap: the
    /// WAL, the table region, and every manifest copy
    /// (`Manifest::max_blocks` blocks each) must be pairwise disjoint.
    fn check_regions(&self) -> Result<(), Error<D::Error>> {
        let c = &self.cfg;
        let stride = Manifest::<LEVELS, TABLES, KEY_MAX>::max_blocks::<BLOCK>();
        let layout = self.manifest_layout();
        let disjoint = |a: (u64, u64), b: (u64, u64)| a.1 <= b.0 || b.1 <= a.0;
        let wal = (c.wal_start, c.wal_end);
        let tbl = (c.tbl_start, c.tbl_end);
        if wal.0 >= wal.1 || tbl.0 >= tbl.1 || !disjoint(wal, tbl) {
            return Err(Error::BadConfig);
        }
        for k in 0..layout.copies() {
            let start = layout.copy_start(k, stride);
            let copy = (start, start.checked_add(stride).ok_or(Error::BadConfig)?);
            if !disjoint(copy, wal) || !disjoint(copy, tbl) {
                return Err(Error::BadConfig);
            }
            for j in 0..k {
                let other = layout.copy_start(j, stride);
                if !disjoint(copy, (other, other + stride)) {
                    return Err(Error::BadConfig);
                }
            }
        }
        Ok(())
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
    /// [`Error::SnapshotLimit`] when eight snapshots (the fixed limit) are
    /// already live.
    pub const fn snapshot(&mut self) -> Result<u64, Error<D::Error>> {
        if !self.opened {
            return Err(Error::NotOpen);
        }
        if self.n_snapshots >= MAX_SNAPSHOTS {
            return Err(Error::SnapshotLimit);
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
    /// [`Error::BadConfig`] when the regions overlap or are empty, or the
    /// table region is too small for the slot layout (each slot must hold
    /// a full memtable's table),
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
        self.check_regions()?;
        let mut slots = SlotMap::layout(
            self.cfg.tbl_start,
            self.cfg.tbl_end,
            Self::SLOTS,
            Self::MIN_SLOT_BLOCKS,
        )
        .ok_or(Error::BadConfig)?;
        let mut scratch = [0u8; BLOCK];
        let layout = self.manifest_layout();
        let (manifest, fresh) =
            Manifest::recover_from(self.wal.device_mut(), &mut scratch, layout).await?;
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
            self.next_seq = self
                .next_seq
                .checked_add(seqs)
                .ok_or(Error::CounterExhausted)?;
        }
        Ok(())
    }

    /// The single-op write path shared by [`put`](Db::put),
    /// [`delete`](Db::delete), [`delete_range`](Db::delete_range) and
    /// [`put_with_ttl`](Db::put_with_ttl): validate against the memtable,
    /// append one WAL record, commit it, then apply it to the memtable.
    /// A failed append or commit rolls the WAL stage back
    /// ([`rollback_commit`](Self::rollback_commit)), so a rejected write
    /// leaves no trace and consumes no sequence number unless its block
    /// may already be durable.
    async fn commit_mutation(&mut self, m: Mutation<'_>) -> Result<u64, Error<D::Error>> {
        self.ensure_open()?;
        match m {
            Mutation::Put { key, val } | Mutation::PutTtl { key, val, .. } => {
                self.table.check_insert::<D::Error>(key, val, false)?;
            }
            Mutation::Delete { key } => self.table.check_insert::<D::Error>(key, &[], true)?,
            Mutation::RangeDelete { start, end } => {
                self.table.check_insert_range_del::<D::Error>(start, end)?;
            }
        }
        let seq = self
            .next_seq
            .checked_add(1)
            .ok_or(Error::CounterExhausted)?;
        let mark = self.stage_mark();
        let appended = match m {
            Mutation::Put { key, val } => self.wal.append(seq, Op::Put, key, val).await,
            Mutation::PutTtl {
                key,
                val,
                expire_at,
            } => self.wal.append_ttl(seq, key, val, expire_at).await,
            Mutation::Delete { key } => self.wal.append(seq, Op::Delete, key, &[]).await,
            Mutation::RangeDelete { start, end } => {
                self.wal.append(seq, Op::RangeDelete, start, end).await
            }
        };
        if let Err(e) = appended {
            self.rollback_commit(mark, 0)?;
            return Err(e);
        }
        if let Err(e) = self.wal.commit().await {
            let landed = self.wal.next_block() != mark.next_block;
            self.rollback_commit(mark, u64::from(landed))?;
            return Err(e);
        }
        self.next_seq = seq;
        // The memtable is unchanged since the check above, so the insert
        // cannot fail.
        match m {
            Mutation::Put { key, val } => self.table.insert(key, val, seq, false)?,
            Mutation::PutTtl {
                key,
                val,
                expire_at,
            } => self
                .table
                .insert_ttl::<D::Error>(key, val, seq, expire_at)?,
            Mutation::Delete { key } => self.table.insert(key, &[], seq, true)?,
            Mutation::RangeDelete { start, end } => {
                self.table.insert_range_del::<D::Error>(start, end, seq)?;
            }
        }
        Ok(seq)
    }

    /// Stores `key` → `val`, durable before it returns. Returns the sequence
    /// number assigned to the mutation.
    ///
    /// # Errors
    ///
    /// [`Error::EmptyKey`], [`Error::KeyTooLarge`], [`Error::ValueTooLarge`],
    /// [`Error::TableFull`] or [`Error::ArenaFull`] (flush, then retry),
    /// [`Error::WalFull`] (flush, then retry), [`Error::CounterExhausted`],
    /// or [`Error::Device`].
    pub async fn put(&mut self, key: &[u8], val: &[u8]) -> Result<u64, Error<D::Error>> {
        self.commit_mutation(Mutation::Put { key, val }).await
    }

    /// Deletes `key` via a tombstone, durable before it returns. Returns the
    /// sequence number assigned to the mutation.
    ///
    /// # Errors
    ///
    /// Same as [`put`](Db::put).
    pub async fn delete(&mut self, key: &[u8]) -> Result<u64, Error<D::Error>> {
        self.commit_mutation(Mutation::Delete { key }).await
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
        self.commit_mutation(Mutation::RangeDelete { start, end })
            .await
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
        self.commit_mutation(Mutation::PutTtl {
            key,
            val,
            expire_at,
        })
        .await
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
    /// This is also horton's **group commit**: every single-op write
    /// (`put`, `delete`, …) costs one WAL block write — one sector erase on
    /// NOR flash — while a batch pays that once for all its ops. Batch
    /// writes that arrive together to save both time and flash wear.
    ///
    /// # Errors
    ///
    /// [`Error::BatchTooLarge`], [`Error::TableFull`],
    /// [`Error::ArenaFull`], [`Error::WalFull`], [`Error::CounterExhausted`],
    /// or [`Error::Device`]. A rejected batch leaves no trace: no WAL records, no staged bytes,
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
                .ok_or(Error::BatchTooLarge {
                    bytes: usize::MAX,
                    max: BLOCK,
                })?;
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

        let base = self
            .next_seq
            .checked_add(1)
            .ok_or(Error::CounterExhausted)?;
        let nu64 = u64::try_from(n).map_err(|_| Error::CounterExhausted)?;
        let last = base.checked_add(nu64 - 1).ok_or(Error::CounterExhausted)?;

        let mark = self.stage_mark();
        for (i, op) in ops.iter().enumerate() {
            let seq = base
                .checked_add(u64::try_from(i).map_err(|_| Error::CounterExhausted)?)
                .ok_or(Error::CounterExhausted)?;
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
                .checked_add(u64::try_from(i).map_err(|_| Error::CounterExhausted)?)
                .ok_or(Error::CounterExhausted)?;
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
