//! Flush: the memtable becomes one `SSTable` in a free table slot.

use super::Db;
use crate::device::BlockDevice;
use crate::error::Error;
use crate::manifest::{KeyBound, ManifestEdit, TableRef};
use crate::memtable::MemTable;
use crate::sstable;
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
    /// Wraps the WAL when the region is exhausted and nothing is unflushed.
    ///
    /// The `wal_head` move rides a manifest commit, so it stays atomic with
    /// the flush; stale pre-wrap blocks are skipped at recovery by the
    /// sequence floor (see `WalWriter::recover_from`).
    async fn wrap_wal_if_full(&mut self) -> Result<(), Error<D::Error>> {
        if self.wal.next_block() < self.cfg.wal_end {
            return Ok(());
        }
        // Safe: the memtable is empty, and every WAL record not yet flushed
        // into a table is replayed into the memtable at open — so no live
        // records exist. (Stale pre-wrap blocks may still sit between
        // `wal_head` and the append position; the sequence floor skips them
        // at recovery.)
        let mut edit = ManifestEdit::new();
        edit.set_wal_head(self.cfg.wal_start);
        // Every issued mutation has left the WAL: the memtable is empty.
        edit.note_flushed(self.next_seq);
        let layout = self.manifest_layout();
        self.manifest
            .commit_edit(
                &edit,
                self.wal.device_mut(),
                self.get_scratch.get_mut(),
                layout,
            )
            .await?;
        self.wal.reset_to(self.cfg.wal_start);
        Ok(())
    }

    /// Writes the memtable's range-tombstone section at `base` through
    /// the block buffer `buf`, returning the blocks written.
    async fn write_flush_rdel(
        device: &mut D,
        table: &MemTable<CAP, ARENA, KEY_MAX, VAL_MAX>,
        base: u64,
        buf: &mut [u8; BLOCK],
    ) -> Result<u32, Error<D::Error>> {
        sstable::write_rdel_blocks::<D, BLOCK>(
            device,
            base,
            buf,
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
            block_count: u32::try_from(total).map_err(|_| Error::TableTooLarge)?,
            first_key: plan.first_key.min(rdel_first),
            last_key: plan.last_key.max(rdel_last),
            max_seq: plan.max_seq.max(rdel_plan.max_seq),
            min_seq: plan.min_seq.min(rdel_plan.min_seq),
            entry_count: u32::try_from(plan.entry_count).map_err(|_| Error::TableTooLarge)?,
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
    /// The table goes into a free slot of the table region (see
    /// [`SlotMap`](crate::alloc::SlotMap)), claimed only once the manifest commit lands, so a
    /// returned I/O error leaves the in-memory state exactly as it was and
    /// the flush can simply be retried.
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
    /// [`Error::NeedsCompaction`] when level 0 is full, or no table slot
    /// is free beyond the compaction reserve and compaction can free one:
    /// run [`compact_step`](Db::compact_step) until
    /// [`Progress::Done`](crate::Progress::Done), then retry.
    /// [`Error::RegionFull`] when no slot is free and compaction has
    /// nothing to do. [`Error::TableTooLarge`] when the table's index or
    /// range-tombstone section would overflow its budget.
    /// [`Error::Device`] on I/O failure.
    pub async fn flush(&mut self) -> Result<(), Error<D::Error>> {
        self.ensure_open()?;
        // Every acked mutation must be durable in the WAL or the new table.
        self.wal.commit().await?;
        if self.table.is_empty() {
            // Still a no-op for the table region, but the WAL may need
            // wrapping after earlier flushes filled it.
            self.wrap_wal_if_full().await?;
            return Ok(());
        }
        // Fail before doing I/O when level 0 cannot take another table;
        // `add_l0_table` re-checks authoritatively below.
        if self.manifest.l0_is_full() {
            return Err(self.no_room());
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
            .ok_or(Error::TableTooLarge)?;
        // Pick a free slot; it is claimed only once the manifest commit
        // below has landed.
        let slot = self.free_slot_for(total)?;
        let base = self.slots.slot_base(slot);
        // Pass 2: stream the blocks — the data section at `base`, then the
        // rdel section right after it, then bloom/index/footer. `cs` is
        // this flush's compression scratch: every data block is
        // trial-compressed and the compressed form kept when it saves
        // enough. The writer's block limit is the planned data-block
        // count: plan and writer share one packing rule, so it is never
        // reached — but a mismatch fails loudly instead of overrunning
        // the slot. The rdel section is written through the writer's data
        // buffer, idle between `seal_data` and `finish_meta`.
        let mut cs = crate::compress::CompressScratch::<BLOCK>::new();
        {
            let device = self.wal.device_mut();
            let table = &self.table;
            let mut w = sstable::TableWriter::<BLOCK, BLOOM_BYTES, KEY_MAX>::new(base, k)
                .with_block_limit(plan.data_blocks);
            for e in table
                .iter()
                .filter(|e| !e.range_del)
                .map(sstable::SstEntry::from)
            {
                w.push(&mut *device, e, Some(&mut cs)).await?;
            }
            let data_blocks = w.seal_data(&mut *device, Some(&mut cs)).await?;
            debug_assert_eq!(data_blocks, plan.data_blocks);
            let rdel_base = base.checked_add(data_blocks).ok_or(Error::TableTooLarge)?;
            let rdel_written =
                Self::write_flush_rdel(&mut *device, table, rdel_base, w.spare_block()).await?;
            debug_assert_eq!(rdel_written, rdel_plan.blocks);
            w.finish_meta(&mut *device, rdel_written, rdel_plan.min_seq)
                .await?;
        }
        // The manifest commit is the atomic visibility point. Stage the
        // change as an edit, applied in memory only once the commit lands,
        // so a returned I/O error leaves the in-memory state exactly as it
        // was and the flush can simply be retried.
        let mut edit = ManifestEdit::new();
        let id = self.manifest.next_table_id();
        edit.advance_next_table_id(id.checked_add(1).ok_or(Error::CounterExhausted)?);
        let tref = Self::flush_tref(id, base, total, &plan, &rdel_plan)?;
        edit.add::<D::Error>(0, tref)?;
        // Advance the WAL head past the flushed records; wrap the region
        // when it is exhausted. Folded into this same atomic commit, so no
        // extra crash window opens between the wrap and its durability.
        let wrap = self.wal.next_block() >= self.cfg.wal_end;
        edit.set_wal_head(if wrap {
            self.cfg.wal_start
        } else {
            self.wal.next_block()
        });
        // Every issued mutation is now in a table or behind `wal_head`:
        // raise the persisted replay floor with this same commit.
        edit.note_flushed(self.next_seq);
        let layout = self.manifest_layout();
        self.manifest
            .commit_edit(
                &edit,
                self.wal.device_mut(),
                self.get_scratch.get_mut(),
                layout,
            )
            .await?;
        // Commit point passed: the edit is applied; claim the slot.
        self.slots.claim(slot);
        // The flushed contents now live in the table; drop the memtable.
        self.table.clear();
        if wrap {
            self.wal.reset_to(self.cfg.wal_start);
        }
        Ok(())
    }
}
