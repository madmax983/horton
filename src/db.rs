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

use crate::alloc::{Bump, FreeList};
use crate::compact::{
    init_cursor, ranges_overlap, Compaction, Input, MergeOutcome, Progress, State, COMPACTION_KMAX,
};
use crate::device::BlockDevice;
use crate::error::Error;
use crate::manifest::{Manifest, TableRef};
use crate::memtable::MemTable;
use crate::sstable;
use crate::wal::{Op, RecoverState, WalWriter};

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
> {
    wal: WalWriter<D, BLOCK>,
    table: MemTable<CAP, ARENA, KEY_MAX, VAL_MAX>,
    manifest: Manifest<LEVELS, TABLES, KEY_MAX>,
    tbl_bump: Bump,
    tbl_free: FreeList<FREELIST>,
    cfg: Config,
    next_seq: u64,
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
    > Db<D, BLOCK, KEY_MAX, VAL_MAX, CAP, ARENA, LEVELS, TABLES, BLOOM_BYTES, FREELIST>
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
            get_scratch: RefCell::new([0u8; BLOCK]),
        }
    }

    /// Consumes the handle and returns the underlying device.
    #[must_use]
    pub fn into_device(self) -> D {
        self.wal.into_device()
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
        // The sequence floor skips stale pre-wrap WAL records (see
        // `WalWriter::recover_from`); when the WAL never wrapped it is a
        // no-op, because every live record is newer than every table.
        let state: RecoverState = self
            .wal
            .recover_from(
                &mut self.table,
                self.manifest.wal_head(),
                self.manifest.max_seq(),
            )
            .await?;
        self.next_seq = state.max_seq.max(self.manifest.max_seq());
        Ok(OpenReport {
            recovered_records: state.records,
            max_seq: self.next_seq,
            l0_tables: self.manifest.l0().len(),
        })
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
        self.wal.append(seq, Op::Put, key, val).await?;
        self.wal.commit().await?;
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
        self.wal.append(seq, Op::Delete, key, &[]).await?;
        self.wal.commit().await?;
        self.next_seq = seq;
        // The table is unchanged since check_insert, so this cannot fail.
        self.table.insert(key, &[], seq, true)?;
        Ok(seq)
    }

    /// Reads `key` into `val_buf`: memtable first, then level 0 newest table
    /// first, then deeper levels in order. Every table is key-range pruned
    /// (no I/O when the key falls outside its bounds), bloom-gated, and
    /// sequence-pruned (no I/O when its `max_seq` cannot beat the best hit
    /// so far). The hit with the highest sequence number wins; a tombstone
    /// there hides every older version.
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
        // The winning value's bytes are staged here; table lookups copy
        // into a per-table buffer first so a losing hit can never clobber
        // the winner. Values are at most VAL_MAX bytes (enforced on the
        // write path), so the staging always fits.
        let mut stage = [0u8; VAL_MAX];
        let mut best = Best::Missing;
        let mut best_seq = 0u64;

        if let Some(entry) = self.table.get(key) {
            best_seq = entry.seq;
            if entry.tombstone {
                best = Best::Tombstone;
            } else {
                stage[..entry.val.len()].copy_from_slice(entry.val);
                best = Best::Value(entry.val.len());
            }
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
        // Level 0, newest table first: its tables overlap, and newer tables
        // hold higher sequence numbers.
        for tref in self.manifest.l0().iter().rev() {
            self.consider_table(tref, key, scratch, &mut stage, &mut best, &mut best_seq)
                .await?;
        }
        // Deeper levels in order. Highest-seq-wins keeps the result exact
        // regardless of how tables are placed; v0.4 compaction will keep
        // each level's ranges disjoint and sorted.
        for li in 1..LEVELS {
            // `li < LEVELS` by construction; the fallback is unreachable.
            let tables = self.manifest.level(li).unwrap_or(&[]);
            for tref in tables {
                self.consider_table(tref, key, scratch, &mut stage, &mut best, &mut best_seq)
                    .await?;
            }
        }

        match best {
            Best::Missing | Best::Tombstone => Ok(None),
            Best::Value(len) => {
                if len > val_buf.len() {
                    return Err(Error::BufferTooSmall { need: len });
                }
                val_buf[..len].copy_from_slice(&stage[..len]);
                Ok(Some(len))
            }
        }
    }

    /// Considers one table for [`Db::get`]: key-range prune, sequence prune,
    /// then a bloom-gated lookup. A hit with a higher sequence number than
    /// the best so far is promoted into `stage`/`best`/`best_seq`.
    async fn consider_table(
        &self,
        tref: &TableRef<KEY_MAX>,
        key: &[u8],
        scratch: &mut [u8; BLOCK],
        stage: &mut [u8; VAL_MAX],
        best: &mut Best,
        best_seq: &mut u64,
    ) -> Result<(), Error<D::Error>> {
        // Both prunes are exact: the table's keys all lie within its bounds,
        // and no entry here can carry a seq above the table's max.
        if !tref.covers(key) || tref.max_seq <= *best_seq {
            return Ok(());
        }
        let footer = tref
            .first_block
            .checked_add(u64::from(tref.block_count))
            .and_then(|end| end.checked_sub(1))
            .ok_or(Error::CorruptManifest)?;
        let reader =
            sstable::TableReader::<D, BLOCK, BLOOM_BYTES>::open(self.wal.device(), scratch, footer)
                .await?;
        // `tmp` (not `stage`) receives the value: only a winning hit is
        // promoted, so a losing hit cannot clobber the staged winner.
        let mut tmp = [0u8; VAL_MAX];
        match reader.lookup(scratch, key, &mut tmp).await? {
            sstable::Lookup::Value { len, seq } if seq > *best_seq => {
                *best_seq = seq;
                stage[..len].copy_from_slice(&tmp[..len]);
                *best = Best::Value(len);
            }
            sstable::Lookup::Tombstone { seq } if seq > *best_seq => {
                *best_seq = seq;
                *best = Best::Tombstone;
            }
            _ => {}
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
        let (slot_a, slot_b) = (self.cfg.manifest_a, self.cfg.manifest_b);
        staged
            .commit(self.wal.device_mut(), scratch, slot_a, slot_b)
            .await?;
        self.manifest = staged;
        self.wal.reset_to(self.cfg.wal_start);
        Ok(())
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
        // Pass 1: pure computation of the table shape.
        let plan = sstable::plan_table::<D::Error, BLOCK, KEY_MAX>(
            self.table.iter().map(sstable::SstEntry::from),
        )?;
        let k = sstable::bloom_k(BLOOM_BYTES * 8, plan.entry_count);
        let total = plan.data_blocks.checked_add(3).ok_or(Error::NoSpace)?;
        let total_usize = usize::try_from(total).map_err(|_| Error::NoSpace)?;
        // Reserve the run without claiming it: free list first, then the
        // bump. Neither moves until the manifest commit below has landed.
        let free_base = self.tbl_free.find_run(total_usize);
        let base = match free_base {
            Some(b) => b,
            None => self.tbl_bump.peek_run::<D::Error>(total)?,
        };
        // Pass 2: stream the blocks. `data` doubles as the manifest scratch
        // below; it is a plain stack local.
        let mut data = [0u8; BLOCK];
        let written = sstable::write_table::<D, BLOCK, BLOOM_BYTES, KEY_MAX>(
            self.wal.device_mut(),
            base,
            k,
            self.table.iter().map(sstable::SstEntry::from),
        )
        .await?;
        debug_assert_eq!(written, total);
        // The manifest commit is the atomic visibility point. Stage the new
        // manifest in a copy and publish it only after the commit lands, so
        // a returned I/O error leaves the in-memory state exactly as it was
        // and the flush can simply be retried.
        let mut staged = self.manifest;
        let id = staged.alloc_table_id::<D::Error>()?;
        let tref = TableRef {
            id,
            first_block: base,
            block_count: u32::try_from(total).map_err(|_| Error::NoSpace)?,
            first_key: plan.first_key,
            last_key: plan.last_key,
            max_seq: plan.max_seq,
            entry_count: u32::try_from(plan.entry_count).map_err(|_| Error::NoSpace)?,
        };
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
    > Db<D, BLOCK, KEY_MAX, VAL_MAX, CAP, ARENA, LEVELS, TABLES, BLOOM_BYTES, FREELIST>
{
    /// Runs one bounded compaction step using the caller's `scratch`.
    ///
    /// When L0 is full, the first call selects all of L0 plus the
    /// overlapping L1 tables and merges them one output block per call
    /// ([`Progress::More`]); the call that exhausts the merge seals the
    /// output table and commits the manifest atomically, returning
    /// [`Progress::Done`]. With L0 not full this is a no-op returning
    /// [`Progress::Done`]. Drive it with `while db.compact_step(&mut
    /// scratch).await? == Progress::More {}`.
    ///
    /// The scratch is reusable across jobs and droppable mid-job: partial
    /// output is invisible until the manifest commit, so abandoning it only
    /// orphans blocks the next `open()` sweep reclaims.
    ///
    /// # Errors
    ///
    /// [`Error::NoSpace`] when the job would exceed [`COMPACTION_KMAX`]
    /// inputs, no output run can be reserved, or the target level is full;
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

    /// Selects the next compaction job into `scratch`: all of L0 plus the
    /// L1 tables overlapping the merged key range. Returns `false` when L0
    /// is not full and there is no work.
    async fn compact_select(
        &mut self,
        c: &mut Compaction<BLOCK, KEY_MAX, VAL_MAX, BLOOM_BYTES>,
    ) -> Result<bool, Error<D::Error>> {
        // v0.4 compacts L0 -> L1 only, triggered by a full L0.
        if LEVELS < 2 {
            return Ok(false);
        }
        let l0 = self.manifest.level(0).ok_or(Error::NoSpace)?;
        if l0.len() < TABLES {
            return Ok(false);
        }
        // Copy the refs so the manifest borrow ends before the scratch and
        // the device are touched.
        let mut l0_refs = [TableRef::EMPTY; TABLES];
        let l0_take = l0.len().min(TABLES);
        l0_refs[..l0_take].copy_from_slice(&l0[..l0_take]);
        let l1 = self.manifest.level(1).ok_or(Error::NoSpace)?;
        let mut l1_refs = [TableRef::EMPTY; TABLES];
        let l1_take = l1.len().min(TABLES);
        l1_refs[..l1_take].copy_from_slice(&l1[..l1_take]);

        let mut n_inputs = 0usize;
        let mut first = l0_refs[0].first_key;
        let mut last = l0_refs[0].last_key;
        let mut total_entries = 0u64;
        let mut total_data = 0u64;
        // Pushes one input table, widening the merged range and totals.
        let mut push = |level: usize, t: &TableRef<KEY_MAX>| -> Result<(), Error<D::Error>> {
            if n_inputs >= COMPACTION_KMAX {
                return Err(Error::NoSpace);
            }
            c.inputs[n_inputs] = Input { level, tref: *t };
            n_inputs += 1;
            total_entries = total_entries
                .checked_add(u64::from(t.entry_count))
                .ok_or(Error::NoSpace)?;
            let data = u64::from(t.block_count)
                .checked_sub(3)
                .ok_or(Error::CorruptBlock { id: t.first_block })?;
            total_data = total_data.checked_add(data).ok_or(Error::NoSpace)?;
            Ok(())
        };
        for t in l0_refs.iter().take(l0_take) {
            push(0, t)?;
            if t.first_key.as_slice() < first.as_slice() {
                first = t.first_key;
            }
            if t.last_key.as_slice() > last.as_slice() {
                last = t.last_key;
            }
        }
        // Overlapping L1 tables join the merge (L1 runs never overlap each
        // other, so range overlap is the exact join condition).
        for t in l1_refs.iter().take(l1_take) {
            if ranges_overlap(first, last, t.first_key, t.last_key) {
                push(1, t)?;
                if t.first_key.as_slice() < first.as_slice() {
                    first = t.first_key;
                }
                if t.last_key.as_slice() > last.as_slice() {
                    last = t.last_key;
                }
            }
        }
        // Tombstones drop only when the output reaches the bottommost level
        // holding the merged range: nothing below can hide an older version
        // of a dropped key.
        let mut bottommost = true;
        'levels: for lvl in 2..LEVELS {
            let Some(tables) = self.manifest.level(lvl) else {
                break 'levels;
            };
            for t in tables {
                if ranges_overlap(first, last, t.first_key, t.last_key) {
                    bottommost = false;
                    break 'levels;
                }
            }
        }
        // Reserve the output run: the merge only shrinks the inputs (dedup
        // plus tombstone drops), so their data blocks plus the 3 framing
        // blocks always suffice. Free list first, then the bump — claimed
        // only after the manifest commit, exactly like flush.
        let out_blocks = total_data.checked_add(3).ok_or(Error::NoSpace)?;
        let out_len = usize::try_from(out_blocks).map_err(|_| Error::NoSpace)?;
        let (out_base, from_free) = match self.tbl_free.find_run(out_len) {
            Some(b) => (b, true),
            None => (self.tbl_bump.peek_run::<D::Error>(out_blocks)?, false),
        };
        c.out_base = out_base;
        c.out_blocks = out_blocks;
        c.out_len = out_len;
        c.from_free = from_free;
        c.target_level = 1;
        c.bottommost = bottommost;
        c.n_inputs = n_inputs;
        c.writer =
            sstable::TableWriter::new(out_base, sstable::bloom_k(BLOOM_BYTES * 8, total_entries));
        // Position one cursor per input on its first entry.
        let device = self.wal.device_mut();
        for i in 0..n_inputs {
            let tref = c.inputs[i].tref;
            init_cursor(&*device, &tref, &mut c.cursors[i]).await?;
        }
        c.state = State::Merging;
        Ok(true)
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
        // durable before the manifest makes them visible.
        let out_ref = if c.writer.entry_count() > 0 {
            let done: sstable::FinishedTable<KEY_MAX> =
                c.writer.finish(self.wal.device_mut()).await?;
            let total = done.data_blocks.checked_add(3).ok_or(Error::NoSpace)?;
            let id = staged.alloc_table_id::<D::Error>()?;
            Some(TableRef {
                id,
                first_block: c.out_base,
                block_count: u32::try_from(total).map_err(|_| Error::NoSpace)?,
                first_key: done.first_key,
                last_key: done.last_key,
                max_seq: done.max_seq,
                entry_count: u32::try_from(done.entry_count).map_err(|_| Error::NoSpace)?,
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
        c.reset();
        Ok(())
    }
}
