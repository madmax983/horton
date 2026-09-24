//! Compaction: job selection, the bounded merge step, and its commits.
//!
//! A job moves data from a *source* level down to the *target* level below
//! it: all of L0, or one table of a deeper level, merged with every target
//! table its key range overlaps. The target tables are disjoint and sorted,
//! so one concatenating cursor reads any number of them; the sources get a
//! cursor each (L0 holds at most `TABLES < COMPACTION_KMAX` tables).
//!
//! The merge writes a *sequence* of output tables, each in its own table
//! slot, ending each at a key boundary when the next key might not fit
//! (see `Compaction::should_split`). Each output commits as soon as it is
//! sealed, in one manifest write that also settles every input against the
//! output's last key `k`: an input whose whole range lies at or below `k`
//! retires (its slot is freed), and an input straddling `k` has its live
//! lower bound raised past `k` ([`Manifest::narrow_table`]), so the prefix
//! it shares with the committed outputs is read only from them. Every
//! commit therefore leaves a consistent, disjoint, readable tree; a job
//! needs only the slot of the output it is writing plus room for the
//! sources' data, and a job cut short (crash, abort, no slot left) keeps
//! what it committed — the next select merges on from there.
//!
//! A source table that overlaps nothing below and fills at least three
//! quarters of a slot is *moved* down by a manifest edit instead of being
//! rewritten. A smaller table merges instead, absorbing a small neighbour at either edge
//! of its range, so appends consolidate into full tables rather than
//! filling the tree with one tiny table per flush.
//!
//! [`Manifest::narrow_table`]: crate::manifest::Manifest::narrow_table

use super::{Db, MAX_SNAPSHOTS};
use crate::cache::CachePort;
use crate::compact::{
    Compaction, Input, MergeOutcome, Progress, RdelMerger, RdelStats, State, TargetView,
    count_rdel_merge, init_cursor, ranges_overlap, rdel_blocks_bound,
};
use crate::device::BlockDevice;
use crate::error::Error;
use crate::manifest::{KeyBound, ManifestEdit, TableRef};
use crate::sstable::{self, RdelEntry};

/// What `compact_select` decided.
enum Selected {
    /// No level is full: nothing to do.
    Idle,
    /// A table moved down a level by a manifest edit alone.
    Moved,
    /// A merge job is set up in the scratch.
    Merging,
}

/// Clips a merged range tombstone to an output's key range `[lo, hi)`
/// (either side open when `None`); `None` when nothing is left.
fn clip<'a, const KEY_MAX: usize>(
    e: RdelEntry<'a>,
    lo: Option<&'a KeyBound<KEY_MAX>>,
    hi: Option<&'a KeyBound<KEY_MAX>>,
) -> Option<RdelEntry<'a>> {
    let start = match lo {
        Some(lo) if lo.as_slice() > e.start => lo.as_slice(),
        _ => e.start,
    };
    let end = match hi {
        Some(hi) if hi.as_slice() < e.end => hi.as_slice(),
        _ => e.end,
    };
    (start < end).then_some(RdelEntry {
        start,
        end,
        seq: e.seq,
    })
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
    /// Reports whether [`compact_step`](Db::compact_step) has work: a job
    /// is in flight, or it would select one right now — some level above
    /// the bottom holds `>= TABLES` tables, or the table region is down to
    /// its compaction reserve while L0 holds two or more tables (merging
    /// them frees slots). Unlike [`Progress::Done`], which a finished job
    /// also returns, this distinguishes "a job just finished, more may be
    /// pending" from "nothing to do" — firmware idle loops and test
    /// drivers use it to decide whether another `compact_step` is
    /// worthwhile. [`Error::NeedsCompaction`] is returned only while this
    /// is true.
    #[must_use]
    pub fn compaction_pending(&self) -> bool {
        self.job_active || self.full_level().is_some()
    }

    /// True when flush and ingest are down to the compaction reserve: the
    /// next table would eat into the headroom compaction keeps.
    const fn under_pressure(&self) -> bool {
        self.slots.free_slots() + self.slots.reserved_slots() <= Self::COMPACTION_RESERVE
    }

    /// Whether level `l` (above the bottom) wants a job: it is full, or the
    /// region is under pressure and the level has something to push down —
    /// any table of an intermediate level (merging it into the level below
    /// collapses overwritten versions and frees slots), or two L0 tables.
    fn wants_job(&self, l: usize, pressure: bool) -> bool {
        let n = self.manifest.level(l).map_or(0, <[_]>::len);
        n >= TABLES || (pressure && n >= if l == 0 { 2 } else { 1 })
    }

    /// The level the next job drains: the deepest level above the bottom
    /// holding `>= TABLES` tables. The bottom level has no table cap: it
    /// grows into whatever slots the levels above leave free.
    ///
    /// Region pressure also selects a level: once flush is down to the
    /// compaction reserve, levels below their trigger would otherwise hold
    /// stale versions (and L0 its partial fill) forever while every flush
    /// is refused. Under pressure the deepest intermediate level with a
    /// table is pushed down, then L0 once it holds two tables. Every such
    /// job moves data strictly deeper, so the chain ends.
    fn full_level(&self) -> Option<usize> {
        if LEVELS < 2 {
            return None;
        }
        (0..LEVELS - 1)
            .rev()
            .find(|&l| self.wants_job(l, false))
            .or_else(|| {
                let pressure = self.under_pressure();
                (0..LEVELS - 1).rev().find(|&l| self.wants_job(l, pressure))
            })
    }

    /// Runs one bounded compaction step using the caller's `scratch`.
    ///
    /// When some level is full, the first call selects a job for the
    /// deepest full level — all of L0, or one table of a deeper level (the
    /// one overlapping the fewest tables below) — plus the overlapping
    /// tables of the level below. A table that overlaps nothing below and
    /// fills at least three quarters of a slot moves down without being
    /// rewritten, and the call returns [`Progress::Done`]. Otherwise each call merges
    /// until one output block seals ([`Progress::More`]); a full output
    /// ends at a key boundary and the next one begins in a fresh slot,
    /// with progress committed whenever the outputs so far cover a whole
    /// target table. The call that exhausts the merge commits the rest
    /// atomically and returns [`Progress::Done`]. With no full level this
    /// is a no-op returning [`Progress::Done`]. Note `Done` is returned in
    /// every case, so `while db.compact_step(&mut scratch).await? ==
    /// Progress::More {}` drives exactly one job; loop on
    /// [`compaction_pending`](Db::compaction_pending) to drain every
    /// pending job.
    ///
    /// The scratch is reusable across jobs and droppable mid-job: output
    /// not yet committed is invisible, and its reserved slots are released
    /// when the next `compact_step` (with any scratch) abandons the stale
    /// job. One job runs at a time; a scratch whose job was abandoned
    /// (another scratch started one, or an archive or ingest aborted it)
    /// resets itself on its next step.
    ///
    /// # Errors
    ///
    /// [`Error::RegionFull`] when a level wants a job but none fits the
    /// free slots (delete data, archive, or grow the region);
    /// [`Error::TableTooLarge`] when a slot cannot hold the job's
    /// range-tombstone budget plus one key's versions; [`Error::CorruptBlock`] on a torn input table
    /// (compaction never silently drops entries); [`Error::Device`] on I/O
    /// failure. A failed step abandons the job: progress already committed
    /// stays, and the next step selects afresh.
    pub async fn compact_step(
        &mut self,
        scratch: &mut Compaction<BLOCK, KEY_MAX, VAL_MAX, BLOOM_BYTES>,
    ) -> Result<Progress, Error<D::Error>> {
        self.ensure_open()?;
        // A scratch whose job is no longer the active one (another scratch
        // selected a job, or the job was aborted) must not touch the
        // device: its reservations are gone.
        if scratch.state != State::Idle && (!self.job_active || scratch.job_gen != self.job_gen) {
            scratch.reset();
        }
        let step = if scratch.state == State::Idle {
            // One job at a time: a job some other scratch left behind
            // is abandoned.
            self.abort_job();
            match self.compact_select(scratch).await {
                Ok(Selected::Idle | Selected::Moved) => return Ok(Progress::Done),
                Ok(Selected::Merging) => self.compact_advance(scratch).await,
                Err(e) => Err(e),
            }
        } else {
            self.compact_advance(scratch).await
        };
        if step.is_err() {
            self.abort_job();
            scratch.reset();
        }
        step
    }

    /// One merge quantum of the active job, plus whatever it triggers: a
    /// full output is sealed, progress committed, and the next output
    /// opened; an exhausted merge is sealed and committed in full.
    async fn compact_advance(
        &mut self,
        c: &mut Compaction<BLOCK, KEY_MAX, VAL_MAX, BLOOM_BYTES>,
    ) -> Result<Progress, Error<D::Error>> {
        let tgt_level = self.manifest.level(c.target_level).unwrap_or(&[]);
        let outcome = c.merge_step(self.wal.device_mut(), tgt_level).await?;
        match outcome {
            MergeOutcome::More => Ok(Progress::More),
            MergeOutcome::Split => {
                let out = self.compact_seal_output(c, false).await?;
                self.compact_commit(c, out, false).await?;
                if self.compact_open_output(c).is_err() {
                    // No slot for the next output: the job ends here, with
                    // everything up to the last output committed. The next
                    // select merges on from the narrowed inputs.
                    self.job_active = false;
                    self.job_inputs = 0;
                    c.reset();
                    return Ok(Progress::Done);
                }
                Ok(Progress::More)
            }
            MergeOutcome::Exhausted => {
                let out = self.compact_seal_output(c, true).await?;
                self.compact_commit(c, out, true).await?;
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

    /// Lowest sequence that a table outside the job, or the memtable, could
    /// hold for a key in `[first, last]`: the floor below which the job may
    /// drop a tombstone. Tables at every level count — a re-ingested table
    /// at L0 can be older than tombstones below it.
    fn outside_min_seq(
        &self,
        c: &Compaction<BLOCK, KEY_MAX, VAL_MAX, BLOOM_BYTES>,
        first: KeyBound<KEY_MAX>,
        last: KeyBound<KEY_MAX>,
    ) -> u64 {
        let mut floor = self.table.min_seq();
        for t in self.manifest.tables() {
            let in_job = c.inputs[..c.n_src].iter().any(|i| i.tref.id == t.id)
                || c.tgt[..c.n_tgt].contains(&t.id);
            if !in_job && ranges_overlap(first, last, t.first_key, t.last_key) {
                floor = floor.min(t.min_seq);
            }
        }
        floor
    }

    /// Whether table `t` fills less than three quarters of a slot: worth
    /// consolidating with a neighbour rather than moving down as is. The
    /// threshold trades rewriting for density: appends grow their newest
    /// table until it crosses it, so slots end up at least three quarters
    /// full, at the cost of rewriting that table once per L0 job.
    fn is_small(&self, t: &TableRef<KEY_MAX>) -> bool {
        u64::from(t.block_count).saturating_mul(4) < self.slots.slot_blocks().saturating_mul(3)
    }

    /// The target-level run `[a, b)` of tables overlapping `[first, last]`.
    /// Target tables are disjoint and sorted, so the overlapping ones are
    /// contiguous, and widening the range to cover them cannot reach any
    /// further table.
    fn overlap_run(
        tables: &[TableRef<KEY_MAX>],
        first: KeyBound<KEY_MAX>,
        last: KeyBound<KEY_MAX>,
    ) -> (usize, usize) {
        let a = tables.partition_point(|t| t.last_key.as_slice() < first.as_slice());
        let mut b = a;
        while b < tables.len()
            && ranges_overlap(first, last, tables[b].first_key, tables[b].last_key)
        {
            b += 1;
        }
        (a, b)
    }

    /// Data an output table holds, in blocks: its slot minus bloom, index,
    /// and footer, minus the split margin (a key may need a block more).
    fn output_capacity(&self) -> u64 {
        self.slots.slot_blocks().saturating_sub(5).max(1)
    }

    /// Free slots a merge over sources holding `src_data` blocks needs: the
    /// output it writes, plus room for the sources' data, which drains
    /// into committed outputs before the sources retire (targets retire as
    /// the merge passes them, freeing a slot per slot they fill).
    fn job_need(&self, src_data: u64) -> u64 {
        1 + src_data.div_ceil(self.output_capacity())
    }

    /// Selects the next compaction job into `c`, or moves a table down
    /// outright. See [`compact_step`](Db::compact_step) for the policy.
    ///
    /// Candidates are the full levels, deepest first, then the levels
    /// region pressure wants (see `full_level`). A merge is admitted only
    /// when the free slots cover its need (`job_need`); an L0 job shrinks
    /// to its oldest tables to fit, and a level whose job does not fit
    /// yields to the next candidate.
    ///
    /// # Errors
    ///
    /// [`Error::RegionFull`] when some level wants compaction but no job
    /// fits the free slots.
    async fn compact_select(
        &mut self,
        c: &mut Compaction<BLOCK, KEY_MAX, VAL_MAX, BLOOM_BYTES>,
    ) -> Result<Selected, Error<D::Error>> {
        if LEVELS < 2 {
            return Ok(Selected::Idle);
        }
        let pressure = self.under_pressure();
        let mut wanted = false;
        // Full levels first, deepest first; then, under pressure, the
        // levels pressure wants.
        for pass in [false, true] {
            if pass && !pressure {
                break;
            }
            for src in (0..LEVELS - 1).rev() {
                if !self.wants_job(src, pass) || (pass && self.wants_job(src, false)) {
                    continue;
                }
                wanted = true;
                if let Some(sel) = self.try_select(c, src).await? {
                    return Ok(sel);
                }
            }
        }
        if wanted {
            Err(Error::RegionFull)
        } else {
            Ok(Selected::Idle)
        }
    }

    /// Selects a job draining level `src` into `c`, or moves a table down
    /// outright; `None` when the merge would not fit the free slots.
    async fn try_select(
        &mut self,
        c: &mut Compaction<BLOCK, KEY_MAX, VAL_MAX, BLOOM_BYTES>,
        src: usize,
    ) -> Result<Option<Selected>, Error<D::Error>> {
        let tgt = src + 1;
        let free = u64::from(self.slots.free_slots());
        let (src_tables, tgt_tables) = (
            self.manifest.level(src).unwrap_or(&[]),
            self.manifest.level(tgt).unwrap_or(&[]),
        );
        let data = |t: &TableRef<KEY_MAX>| u64::from(t.block_count).saturating_sub(3);
        // Sources: L0's oldest tables — all of them when the slots allow —
        // or the one table of a deeper level whose range overlaps the
        // fewest target tables (ties to the lowest key), the cheapest to
        // push down.
        c.reset();
        let mut src_data = 0u64;
        if src == 0 {
            for t in src_tables {
                let d = src_data + data(t);
                if c.n_src > 0 && self.job_need(d) > free {
                    break;
                }
                c.inputs[c.n_src] = Input { level: 0, tref: *t };
                c.n_src += 1;
                src_data = d;
            }
        } else {
            let pick = src_tables
                .iter()
                .min_by_key(|t| {
                    let (a, b) = Self::overlap_run(tgt_tables, t.first_key, t.last_key);
                    b - a
                })
                .copied()
                .ok_or(Error::CorruptManifest)?;
            c.inputs[0] = Input {
                level: src,
                tref: pick,
            };
            c.n_src = 1;
            src_data = data(&pick);
        }
        let mut first = c.inputs[0].tref.first_key;
        let mut last = c.inputs[0].tref.last_key;
        for input in &c.inputs[1..c.n_src] {
            first = first.min(input.tref.first_key);
            last = last.max(input.tref.last_key);
        }
        let (mut a, mut b) = Self::overlap_run(tgt_tables, first, last);
        if src > 0 && a == b && !self.is_small(&c.inputs[0].tref) {
            // Nothing below overlaps and the table is worth keeping whole:
            // re-parent it with one manifest edit, no data rewritten.
            let tref = c.inputs[0].tref;
            c.reset();
            let mut edit = ManifestEdit::new();
            self.manifest
                .stage_remove::<D::Error>(&mut edit, src, tref.id)?;
            edit.add::<D::Error>(tgt, tref)?;
            edit.raise_seq_high(self.next_seq);
            let layout = self.manifest_layout();
            self.manifest
                .commit_edit(
                    &edit,
                    self.wal.device_mut(),
                    self.get_scratch.get_mut(),
                    layout,
                )
                .await?;
            return Ok(Some(Selected::Moved));
        }
        if self.job_need(src_data) > free {
            c.reset();
            return Ok(None);
        }
        // Consolidation: a small neighbour just outside either edge of
        // the run joins the job, so small tables merge into full ones
        // instead of each holding a slot forever. The neighbour is
        // adjacent in key order, so the widened range swallows no other
        // table.
        if a > 0 && self.is_small(&tgt_tables[a - 1]) {
            a -= 1;
        }
        if b < tgt_tables.len() && self.is_small(&tgt_tables[b]) {
            b += 1;
        }
        for (k, t) in tgt_tables[a..b].iter().enumerate() {
            c.tgt[k] = t.id;
            first = first.min(t.first_key);
            last = last.max(t.last_key);
        }
        c.n_tgt = b - a;
        self.compact_start(c, tgt, first, last).await?;
        Ok(Some(Selected::Merging))
    }

    /// Sets up the merge for the inputs `compact_select` chose: the
    /// tombstone-drop gates, the snapshot keep-set, the per-output budgets,
    /// the first output's slot, and the input cursors.
    async fn compact_start(
        &mut self,
        c: &mut Compaction<BLOCK, KEY_MAX, VAL_MAX, BLOOM_BYTES>,
        tgt: usize,
        first: KeyBound<KEY_MAX>,
        last: KeyBound<KEY_MAX>,
    ) -> Result<(), Error<D::Error>> {
        c.target_level = tgt;
        // The first output's range tombstones are clipped to the job's
        // range: an input's range-tombstone section can reach below its
        // live lower bound (the part a past job already merged).
        c.out_lo = first;
        c.bottommost = self.is_bottommost_output(tgt, first, last);
        c.outside_min_seq = self.outside_min_seq(c, first, last);
        // The version-retention set: snapshots live at select time pin this
        // job's keep-set. Watermarks are stored descending so the merge
        // can walk its thresholds (live view, then each snapshot) in
        // order. A snapshot taken mid-compaction always has a seq above
        // every version being merged, so the select-time set is exactly
        // the history that needs protection.
        let (sorted, n) = self.sorted_snapshot_watermarks();
        c.snapshots = sorted;
        c.n_snapshots = n;
        c.oldest_snapshot = self.oldest_snapshot_seq();
        // Every output reserves room for its share of the merged
        // range-tombstone section. A dry run counts the merged entries;
        // an output's clipped share is at most that many, which bounds its
        // blocks (`rdel_blocks_bound`).
        let rdel_entries = {
            let targets = TargetView {
                level: self.manifest.level(tgt).unwrap_or(&[]),
                ids: &c.tgt[..c.n_tgt],
            };
            count_rdel_merge(
                self.wal.device(),
                &c.inputs[..c.n_src],
                &targets,
                &mut c.raw,
                c.bottommost,
                c.oldest_snapshot,
            )
            .await?
        };
        let rdel_budget = rdel_blocks_bound::<BLOCK, KEY_MAX>(rdel_entries);
        // The data section gets the rest of the slot. It must hold at
        // least one key's worst case or no output could end.
        let margin = Compaction::<BLOCK, KEY_MAX, VAL_MAX, BLOOM_BYTES>::key_run_blocks(n);
        let data_budget = self
            .slots
            .slot_blocks()
            .checked_sub(rdel_budget)
            .and_then(|d| d.checked_sub(3))
            .filter(|&d| d >= margin)
            .ok_or(Error::TableTooLarge)?;
        c.split_margin = margin;
        c.rdel_budget = u32::try_from(rdel_budget).map_err(|_| Error::TableTooLarge)?;
        c.data_budget = data_budget;
        // Bloom probes sized for one output's expected share of the
        // entries.
        let (mut entries, mut data) = (0u64, 0u64);
        for t in c.inputs[..c.n_src].iter().map(|i| &i.tref).chain(
            self.manifest
                .level(tgt)
                .unwrap_or(&[])
                .iter()
                .filter(|t| c.tgt[..c.n_tgt].contains(&t.id)),
        ) {
            entries = entries.saturating_add(u64::from(t.entry_count));
            data = data.saturating_add(t.data_blocks().unwrap_or(0));
        }
        let per_output = entries.saturating_mul(data_budget) / data.max(1) + 1;
        c.bloom_k = sstable::bloom_k(BLOOM_BYTES * 8, per_output.min(entries.max(1)));
        // The job is live from here: its first output slot is reserved,
        // and archiving any of its inputs aborts it.
        self.job_active = true;
        self.job_gen = self.job_gen.wrapping_add(1);
        c.job_gen = self.job_gen;
        self.job_inputs = 0;
        for i in 0..c.n_src {
            if let Some(slot) = self.slot_of(&c.inputs[i].tref) {
                self.job_inputs |= 1u64 << slot;
            }
        }
        for t in self.manifest.level(tgt).unwrap_or(&[]) {
            if c.tgt[..c.n_tgt].contains(&t.id)
                && let Some(slot) = self.slot_of(t)
            {
                self.job_inputs |= 1u64 << slot;
            }
        }
        self.compact_open_output(c)?;
        // Position one cursor per source, and the concatenating cursor on
        // the first target with entries.
        let device = self.wal.device();
        for i in 0..c.n_src {
            let tref = c.inputs[i].tref;
            init_cursor(device, &mut c.raw, &tref, &mut c.cursors[i]).await?;
        }
        let tgt_level = self.manifest.level(tgt).unwrap_or(&[]);
        c.open_next_target(device, tgt_level).await?;
        c.state = State::Merging;
        Ok(())
    }

    /// Reserves a slot for the next output and points a fresh writer at it.
    fn compact_open_output(
        &mut self,
        c: &mut Compaction<BLOCK, KEY_MAX, VAL_MAX, BLOOM_BYTES>,
    ) -> Result<(), Error<D::Error>> {
        let slot = self.slots.reserve().ok_or(Error::RegionFull)?;
        c.out_slot = slot;
        c.out_base = self.slots.slot_base(slot);
        // The writer's block limit is the data budget: a merge that would
        // need more blocks fails with `TableTooLarge` instead of writing past
        // the slot.
        c.writer = sstable::TableWriter::new(c.out_base, c.bloom_k).with_block_limit(c.data_budget);
        Ok(())
    }

    /// Seals the current output: its data section, then its share of the
    /// merged range-tombstone section, then bloom, index, and footer (the
    /// meta write flushes, so the blocks are durable before the commit
    /// makes them visible). Returns the output's ref, or `None` — with its
    /// slot released — when it ended up with neither entries nor range
    /// tombstones.
    ///
    /// Range tombstones are clipped to the output's key range: from the
    /// successor of the previous output's last key (the job's lower bound
    /// for the first output) to the successor of this one's (unbounded for
    /// the last output), so the outputs stay disjoint and every covered key
    /// is covered by exactly the output holding it. A non-final output's
    /// range therefore ends exactly at its last key.
    async fn compact_seal_output(
        &mut self,
        c: &mut Compaction<BLOCK, KEY_MAX, VAL_MAX, BLOOM_BYTES>,
        last_output: bool,
    ) -> Result<Option<TableRef<KEY_MAX>>, Error<D::Error>> {
        let device = self.wal.device_mut();
        let data_blocks = c
            .writer
            .seal_data(&mut *device, Some(&mut c.compress))
            .await?;
        let last_key = c.writer.last_key();
        let hi = if last_output {
            None
        } else {
            last_key.successor()
        };
        let lo = c.out_lo;
        let rdel_base = c
            .out_base
            .checked_add(data_blocks)
            .ok_or(Error::TableTooLarge)?;
        // The writer's data buffer is idle until `finish_meta`: the rdel
        // section stages there.
        let mut rdel_out = sstable::RdelWriter::<BLOCK>::new(rdel_base, c.writer.spare_block())
            .with_block_limit(c.rdel_budget);
        let mut rdel_stats = RdelStats::<KEY_MAX>::new();
        {
            // Retired inputs lie behind this output's range (and their
            // blocks may already hold other tables): only the live ones
            // can reach it.
            let targets = TargetView {
                level: self.manifest.level(c.target_level).unwrap_or(&[]),
                ids: &c.tgt[c.tgt_retired..c.n_tgt],
            };
            let mut merger = RdelMerger::<KEY_MAX>::new(
                &c.inputs[..c.n_src],
                c.src_retired,
                !targets.ids.is_empty(),
                c.bottommost,
                c.oldest_snapshot,
            );
            while merger.next_merged(&*device, &mut c.raw, &targets).await? {
                if let Some(piece) = clip(merger.current_entry(), Some(&lo), hi.as_ref()) {
                    rdel_out.push(&mut *device, piece).await?;
                    rdel_stats.observe::<D::Error>(&piece, rdel_base)?;
                }
            }
        }
        let rdel_blocks = rdel_out.finish(&mut *device).await?;
        if let Some(hi) = hi {
            c.out_lo = hi;
        }
        if c.writer.entry_count() == 0 && rdel_blocks == 0 {
            self.slots.release(c.out_slot);
            return Ok(None);
        }
        let done = c
            .writer
            .finish_meta(&mut *device, rdel_blocks, rdel_stats.min_seq)
            .await?;
        let total = u64::from(rdel_blocks)
            .checked_add(done.data_blocks)
            .and_then(|n| n.checked_add(3))
            .ok_or(Error::TableTooLarge)?;
        let id = self.manifest.alloc_table_id::<D::Error>()?;
        Ok(Some(TableRef {
            id,
            first_block: c.out_base,
            block_count: u32::try_from(total).map_err(|_| Error::TableTooLarge)?,
            // `KeyBound::min/max` let `EMPTY` lose, so a missing section
            // never corrupts the bounds. A non-final output's pieces end
            // at the successor of its last key, so its last key bounds
            // them exactly; the final output keeps the conservative
            // (exclusive) piece end.
            first_key: done.first_key.min(rdel_stats.first),
            last_key: if hi.is_some() {
                done.last_key
            } else {
                done.last_key.max(rdel_stats.last)
            },
            max_seq: done.max_seq.max(rdel_stats.max_seq),
            min_seq: done.min_seq,
            entry_count: u32::try_from(done.entry_count).map_err(|_| Error::TableTooLarge)?,
            rdel_blocks,
        }))
    }

    /// Commits one output (if any) in one manifest write, settling every
    /// live input against the output's last key `k`: an input whose range
    /// ends at or below `k` retires, and one straddling `k` is narrowed to
    /// start just past it. Every key at or below `k` then reads from the
    /// committed outputs alone, so the target level stays disjoint and a
    /// tombstone the merge dropped can expose nothing. The final commit
    /// retires every remaining input and ends the job.
    ///
    /// Slots settle strictly after the visibility point: the output's
    /// reservation becomes a used slot, and retired inputs' slots are free.
    async fn compact_commit(
        &mut self,
        c: &mut Compaction<BLOCK, KEY_MAX, VAL_MAX, BLOOM_BYTES>,
        out: Option<TableRef<KEY_MAX>>,
        last_commit: bool,
    ) -> Result<(), Error<D::Error>> {
        // The frontier: everything at or below `k` is in the outputs.
        let k = match out {
            Some(t) if !last_commit => t.last_key,
            _ => KeyBound::EMPTY,
        };
        let past_k = k.successor();
        let mut edit = ManifestEdit::new();
        let mut freed = 0u64;
        let mut retired_src = 0u8;
        let mut retired_tgt = c.tgt_retired;
        // Settles input `id` at `level`: retire it, or narrow it past `k`.
        // Returns whether it retired.
        let mut settle = |edit: &mut ManifestEdit<KEY_MAX>,
                          level: usize,
                          id: u32|
         -> Result<bool, Error<D::Error>> {
            let Some(t) = self.manifest.find_table(id).copied() else {
                return Err(Error::CorruptManifest);
            };
            let retire = last_commit || t.last_key.as_slice() <= k.as_slice();
            if retire {
                if let Some(slot) = self.slots.slot_of(t.first_block, u64::from(t.block_count)) {
                    freed |= 1u64 << slot;
                }
                self.manifest.stage_remove::<D::Error>(edit, level, id)?;
            } else if t.first_key.as_slice() <= k.as_slice()
                && let Some(first) = past_k
            {
                self.manifest
                    .stage_narrow::<D::Error>(edit, level, id, first)?;
            }
            Ok(retire)
        };
        for i in 0..c.n_src {
            if c.src_retired & (1u8 << i) == 0
                && settle(&mut edit, c.inputs[i].level, c.inputs[i].tref.id)?
            {
                retired_src |= 1u8 << i;
            }
        }
        for j in c.tgt_retired..c.n_tgt {
            // Targets are sorted and disjoint: the retired ones are a prefix.
            if settle(&mut edit, c.target_level, c.tgt[j])? {
                retired_tgt = j + 1;
            }
        }
        // Retire and narrow first, then insert the output: its first key
        // must sort against the inputs' raised bounds.
        if let Some(t) = out {
            edit.add::<D::Error>(c.target_level, t)?;
        }
        // Compaction may drop the tables holding the newest sequences
        // (bottommost tombstones): persist the counter so it never regresses.
        edit.raise_seq_high(self.next_seq);
        let layout = self.manifest_layout();
        self.manifest
            .commit_edit(
                &edit,
                self.wal.device_mut(),
                self.get_scratch.get_mut(),
                layout,
            )
            .await?;
        // Commit point passed: the edit is applied. Settle the slots and
        // drop the retired tables' cache entries (hygiene — ids never
        // repeat, so stale entries could never be read).
        if out.is_some() {
            self.slots.commit(c.out_slot);
        }
        for slot in 0..64u32 {
            if freed & (1u64 << slot) != 0 {
                self.slots.free(slot);
            }
        }
        for i in 0..c.n_src {
            if retired_src & (1u8 << i) != 0 {
                self.cache.invalidate_table(c.inputs[i].tref.id);
            }
        }
        for &id in &c.tgt[c.tgt_retired..retired_tgt] {
            self.cache.invalidate_table(id);
        }
        self.job_inputs &= !freed;
        c.src_retired |= retired_src;
        c.tgt_retired = retired_tgt;
        if last_commit {
            self.job_active = false;
            self.job_inputs = 0;
            c.reset();
        }
        Ok(())
    }
}
