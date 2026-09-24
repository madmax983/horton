//! Compaction: job selection, the bounded merge step, and its commit.

use super::{Db, MAX_SNAPSHOTS};
use crate::cache::CachePort;
use crate::compact::{
    COMPACTION_KMAX, Compaction, Input, MergeOutcome, Progress, State, init_cursor, ranges_overlap,
};
use crate::device::BlockDevice;
use crate::error::Error;
use crate::manifest::{KeyBound, TableRef};
use crate::sstable;
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
    /// output is invisible until the manifest commit, and its reserved
    /// slot is released when the next `compact_step` (with any scratch)
    /// abandons the stale job. One job runs at a time; a scratch whose job
    /// was abandoned (another scratch started one, or an archive or ingest
    /// aborted it) resets itself on its next step.
    ///
    /// # Errors
    ///
    /// [`Error::NoSpace`] when the job would exceed [`COMPACTION_KMAX`]
    /// inputs, no output slot is free (or the slot cannot fit the merge), or the target level cannot
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
        self.ensure_open()?;
        // A scratch whose job is no longer the active one (another scratch
        // selected a job, or the job was aborted) must not touch the
        // device: its reservation is gone.
        if scratch.state != State::Idle && (!self.job_active || scratch.job_gen != self.job_gen) {
            scratch.reset();
        }
        if scratch.state == State::Idle {
            // One job at a time: a job some other scratch left behind
            // is abandoned.
            self.abort_job();
            match self.compact_select(scratch).await {
                Ok(true) => {}
                Ok(false) => return Ok(Progress::Done),
                Err(e) => {
                    self.abort_job();
                    scratch.reset();
                    return Err(e);
                }
            }
        }
        let outcome = match scratch.merge_step(self.wal.device_mut()).await {
            Ok(o) => o,
            Err(e) => {
                self.abort_job();
                scratch.reset();
                return Err(e);
            }
        };
        match outcome {
            MergeOutcome::More => Ok(Progress::More),
            MergeOutcome::Exhausted => {
                if let Err(e) = self.compact_commit(scratch).await {
                    self.abort_job();
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

    /// Lowest sequence that a table outside the job's first `n_inputs`
    /// inputs, or the memtable, could hold for a key in `[first, last]`:
    /// the floor below which the job may drop a tombstone. Tables at every
    /// level count — a re-ingested table at L0 can be older than
    /// tombstones below it.
    fn outside_min_seq(
        &self,
        c: &Compaction<BLOCK, KEY_MAX, VAL_MAX, BLOOM_BYTES>,
        n_inputs: usize,
        first: KeyBound<KEY_MAX>,
        last: KeyBound<KEY_MAX>,
    ) -> u64 {
        let mut floor = self.table.min_seq();
        for lvl in 0..LEVELS {
            for t in self.manifest.level(lvl).unwrap_or(&[]) {
                let in_job = c.inputs[..n_inputs].iter().any(|i| i.tref.id == t.id);
                if !in_job && ranges_overlap(first, last, t.first_key, t.last_key) {
                    floor = floor.min(t.min_seq);
                }
            }
        }
        floor
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
        let outside_min_seq = self.outside_min_seq(c, job.n_inputs, job.first, job.last);
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
        // Reserve the output slot. The job spans many `compact_step`
        // calls; the reservation keeps flushes in between from taking the
        // slot. The rdel section gets its exact counted budget and the 3
        // framing blocks theirs; the data section gets the rest of the
        // slot, and the writer's block limit enforces it.
        let data_budget = self
            .slots
            .slot_blocks()
            .checked_sub(u64::from(rdel_budget))
            .and_then(|n| n.checked_sub(3))
            .filter(|&n| n > 0)
            .ok_or(Error::NoSpace)?;
        let out_slot = self.slots.reserve().ok_or(Error::NoSpace)?;
        self.job_active = true;
        self.job_gen = self.job_gen.wrapping_add(1);
        self.job_inputs = 0;
        for input in &c.inputs[..job.n_inputs] {
            if let Some(slot) = self.slot_of(&input.tref) {
                self.job_inputs |= 1u64 << slot;
            }
        }
        c.job_gen = self.job_gen;
        c.out_slot = out_slot;
        c.out_base = self.slots.slot_base(out_slot);
        c.target_level = tgt;
        c.bottommost = bottommost;
        c.outside_min_seq = outside_min_seq;
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
        c.rdel_blocks = rdel_budget;
        // The data section leads the output slot; the range-tombstone
        // section follows it and is written at commit. The writer's block
        // limit is the slot's data budget: a merge that would need more
        // blocks fails with `NoSpace` instead of writing past the slot.
        c.writer = sstable::TableWriter::new(
            c.out_base,
            sstable::bloom_k(BLOOM_BYTES * 8, job.total_entries),
        )
        .with_block_limit(data_budget);
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
        // Seal the output table first: data, then the range-tombstone
        // section (a bounded k-way merge over the inputs' sorted rdel
        // sections, see `RdelMerger`), then bloom/index/footer. The meta
        // write flushes, so the blocks are durable before the manifest makes
        // them visible. A merge that kept only range tombstones (no point
        // entries) still seals a table — range-only tables are first-class
        // (zero data blocks).
        let device = self.wal.device_mut();
        let data_blocks = c
            .writer
            .seal_data(&mut *device, Some(&mut c.compress))
            .await?;
        let rdel_base = c.out_base.checked_add(data_blocks).ok_or(Error::NoSpace)?;
        let mut rdel_out = sstable::RdelWriter::<BLOCK>::new(rdel_base);
        let mut rdel_stats = crate::compact::RdelStats::<KEY_MAX>::new();
        {
            let mut merger = crate::compact::RdelMerger::<KEY_MAX>::new(
                &c.inputs[..c.n_inputs],
                c.bottommost,
                c.oldest_snapshot,
            );
            while merger.next_merged(&*device, &mut c.raw).await? {
                let e = merger.current_entry();
                rdel_out.push(&mut *device, e).await?;
                rdel_stats.observe::<D::Error>(&e, rdel_base)?;
            }
        }
        let rdel_blocks = rdel_out.finish(&mut *device).await?;
        debug_assert_eq!(
            rdel_blocks, c.rdel_blocks,
            "rdel merge replay diverged from its counted budget"
        );
        let out_ref = if c.writer.entry_count() > 0 || rdel_blocks > 0 {
            let done: sstable::FinishedTable<KEY_MAX> = c
                .writer
                .finish_meta(&mut *device, rdel_blocks, rdel_stats.min_seq)
                .await?;
            let total = u64::from(rdel_blocks)
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
                first_key: done.first_key.min(rdel_stats.first),
                last_key: done.last_key.max(rdel_stats.last),
                max_seq: done.max_seq.max(rdel_stats.max_seq),
                min_seq: done.min_seq,
                entry_count: u32::try_from(done.entry_count).map_err(|_| Error::NoSpace)?,
                rdel_blocks,
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
        // Commit point passed: publish the staged state, then settle the
        // slots: the output's reservation becomes a used slot (or is
        // released when the merge emitted nothing), and the retired
        // inputs' slots are free — strictly after the visibility point.
        self.manifest = staged;
        if out_ref.is_some() {
            self.slots.commit(c.out_slot);
        } else {
            self.slots.release(c.out_slot);
        }
        for input in c.inputs.iter().take(c.n_inputs) {
            // Drop the retired table's cache entries so their slots serve
            // the hot set (hygiene — ids never repeat, so stale entries
            // could never be read).
            self.cache.invalidate_table(input.tref.id);
            if let Some(slot) = self.slot_of(&input.tref) {
                self.slots.free(slot);
            }
        }
        self.job_active = false;
        self.job_inputs = 0;
        c.reset();
        Ok(())
    }
}
