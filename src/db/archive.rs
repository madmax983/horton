//! Archive and ingest: moving sealed tables off and back onto the device.

use core::future::poll_fn;

use super::Db;
use crate::cache::CachePort;
use crate::compact::{EntryStream, ranges_overlap};
use crate::device::BlockDevice;
use crate::error::Error;
use crate::manifest::{KeyBound, ManifestEdit, TableRef};
use crate::sstable;
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
    /// Total blocks: data blocks + rdel blocks + bloom + index + footer.
    pub block_count: u32,
    /// Smallest key in the table.
    pub first_key: KeyBound<KEY_MAX>,
    /// Largest key in the table.
    pub last_key: KeyBound<KEY_MAX>,
    /// Highest sequence number in the table.
    pub max_seq: u64,
    /// Lowest sequence number in the table; cross-checked against the
    /// copied table's footer on ingest (compaction's tombstone-drop rule
    /// relies on it).
    pub min_seq: u64,
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
            min_seq: self.table.min_seq,
            entry_count: self.table.entry_count,
            rdel_blocks: self.table.rdel_blocks,
        }
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
    const CACHE: usize,
> Db<D, BLOCK, KEY_MAX, VAL_MAX, CAP, ARENA, LEVELS, TABLES, BLOOM_BYTES, CACHE>
{
    /// Re-attaches an archived table from a source device (v0.12).
    ///
    /// `sealed` is the placement-free descriptor from
    /// [`ArchivePlan::sealed`](ArchivePlan::sealed); `remote` holds the
    /// table's blocks laid out contiguously starting at `src_base`. The
    /// table is copied into a free local table slot, its block CRCs are
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
    /// Crash safety: the slot is claimed only once the manifest commit
    /// lands, so a crash before the commit leaves only unreferenced blocks
    /// in a free slot; a crash after it leaves the table fully attached.
    /// Retrying after any crash converges to exactly one copy. A
    /// successful ingest aborts any in-flight compaction job (the job's
    /// tombstone-drop floor predates the ingested table).
    ///
    /// # Errors
    ///
    /// [`Error::IngestConflict`] when the id is already attached with a
    /// *different* descriptor, [`Error::NeedsCompaction`] when L0 is full
    /// (or no slot is free until compaction frees one),
    /// [`Error::RegionFull`] when no slot is free at all,
    /// [`Error::TableTooLarge`] when the table is larger than a slot,
    /// [`Error::CorruptBlock`] when the source
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
        self.ensure_open()?;
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
                && existing.min_seq == sealed.min_seq
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
            return Err(self.no_room());
        }
        let blocks = u64::from(sealed.block_count);
        // Pick a free slot (mirrors flush). It is claimed only after the
        // manifest commit below lands, so a crash mid-copy leaves nothing
        // but unreferenced blocks in a free slot.
        let slot = self.free_slot_for(blocks)?;
        let base = self.slots.slot_base(slot);
        // Stream the blocks from the source device, verifying each
        // block's CRC as it lands so remote corruption fails fast,
        // before the manifest commit.
        // The `Db`'s block scratch carries the copy: `&mut self` rules out
        // a concurrent read.
        Self::copy_verified_blocks(
            self.wal.device_mut(),
            remote,
            src_base,
            base,
            sealed.block_count,
            self.get_scratch.get_mut(),
        )
        .await?;
        // Relocate the copy: index entries and the footer carry the
        // absolute block ids of the table's original placement, which are
        // rewritten to the destination layout and re-sealed.
        sstable::relocate_table(
            self.wal.device_mut(),
            base,
            sealed.block_count,
            sealed.rdel_blocks,
            self.get_scratch.get_mut(),
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
            self.get_scratch.get_mut(),
            footer,
        )
        .await?;
        if reader.entry_count() != u64::from(sealed.entry_count)
            || reader.rdel_blocks() != sealed.rdel_blocks
            || reader.min_seq() != sealed.min_seq
        {
            return Err(Error::CorruptBlock { id: footer });
        }
        // The ingested table may hold versions older than tombstones an
        // in-flight compaction job is about to drop (the job's drop floor
        // predates it), so the job restarts from scratch.
        self.abort_job();
        // Graft into L0 through the atomic manifest commit. Future local
        // tables must never collide with the ingested id, so the id floor
        // advances past it (monotone; never lowers the counter).
        let mut edit = ManifestEdit::new();
        edit.advance_next_table_id(sealed.id.saturating_add(1));
        // The table may carry sequences from another history: the counter
        // must resume above them, or a later local write could lose to an
        // older ingested version under highest-sequence-wins.
        edit.raise_seq_high(self.next_seq.max(sealed.max_seq));
        edit.add::<D::Error>(
            0,
            TableRef {
                id: sealed.id,
                first_block: base,
                block_count: sealed.block_count,
                first_key: sealed.first_key,
                last_key: sealed.last_key,
                max_seq: sealed.max_seq,
                min_seq: sealed.min_seq,
                entry_count: sealed.entry_count,
                rdel_blocks: sealed.rdel_blocks,
            },
        )?;
        let layout = self.manifest_layout();
        self.manifest
            .commit_edit(
                &edit,
                self.wal.device_mut(),
                self.get_scratch.get_mut(),
                layout,
            )
            .await?;
        // Commit point passed: the edit is applied; claim the slot —
        // strictly after the visibility point.
        self.next_seq = self.next_seq.max(sealed.max_seq);
        self.slots.claim(slot);
        Ok(true)
    }

    /// Copies `count` blocks from `remote` at `src_base` to the local
    /// device at `dst_base`, verifying each block's CRC as it lands.
    async fn copy_verified_blocks<R>(
        device: &mut D,
        remote: &R,
        src_base: u64,
        dst_base: u64,
        count: u32,
        buf: &mut [u8; BLOCK],
    ) -> Result<(), Error<D::Error>>
    where
        R: BlockDevice,
        R::Error: Into<D::Error>,
    {
        for k in 0..count {
            let src = src_base
                .checked_add(u64::from(k))
                .ok_or(Error::CorruptManifest)?;
            poll_fn(|cx| remote.poll_read_block(cx, src, buf))
                .await
                .map_err(|e| Error::Device(e.into()))?;
            sstable::check_block_crc::<D::Error, BLOCK>(buf, src)?;
            let dst = dst_base
                .checked_add(u64::from(k))
                .ok_or(Error::CorruptManifest)?;
            poll_fn(|cx| device.poll_write_block(cx, dst, buf))
                .await
                .map_err(Error::Device)?;
        }
        Ok(())
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
    /// atomic manifest write and frees its table slot.
    ///
    /// Call only after the table's bytes are durably stored remotely —
    /// once this returns `Ok(true)` the data is gone locally by design.
    /// Returns `Ok(false)` when the table is no longer at `level`
    /// (idempotent: safe to retry after a crash that may or may not have
    /// committed, or when a concurrent compaction already merged it away —
    /// the uploaded bytes are still a valid copy of that data).
    ///
    /// Archiving an input of the in-flight compaction job aborts the job
    /// (its merge still reads the table); a later `compact_step` selects
    /// afresh.
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
        self.ensure_open()?;
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
        let slot = self.slot_of(&plan.table);
        if slot.is_some_and(|s| self.job_inputs & (1u64 << s) != 0) {
            self.abort_job();
        }
        let mut edit = ManifestEdit::new();
        if !self
            .manifest
            .stage_remove::<D::Error>(&mut edit, level, table_id)?
        {
            return Ok(false);
        }
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
        // Commit point passed: the edit is applied; free the table's slot
        // strictly after the visibility point.
        // The table's blocks are unreachable now; drop its cache entries
        // so their slots serve the hot set (hygiene — ids never repeat,
        // so stale entries could never be read).
        self.cache.invalidate_table(table_id);
        if let Some(slot) = slot {
            self.slots.free(slot);
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
        self.check_points_no_resurrection(candidate, &mut key, &mut val)
            .await?;
        self.check_rdels_no_resurrection(candidate, &mut key, &mut val)
            .await
    }

    /// The point-tombstone half of [`check_no_resurrection`]: its own
    /// future, so the entry stream's block buffers and the rdel half's
    /// block buffer never coexist in the caller's future.
    ///
    /// [`check_no_resurrection`]: Self::check_no_resurrection
    async fn check_points_no_resurrection(
        &self,
        candidate: &TableRef<KEY_MAX>,
        key: &mut [u8; KEY_MAX],
        val: &mut [u8; VAL_MAX],
    ) -> Result<(), Error<D::Error>> {
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
                    let cur_hit = self.read_point(&key[..klen], val, view, 0, None).await?;
                    if cur_hit.is_none() {
                        let alt_hit = self
                            .read_point(&key[..klen], val, view, 0, Some(candidate.id))
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
        Ok(())
    }

    /// The range-tombstone half of [`check_no_resurrection`].
    ///
    /// [`check_no_resurrection`]: Self::check_no_resurrection
    async fn check_rdels_no_resurrection(
        &self,
        candidate: &TableRef<KEY_MAX>,
        key: &mut [u8; KEY_MAX],
        val: &mut [u8; VAL_MAX],
    ) -> Result<(), Error<D::Error>> {
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
        let rdel_first = candidate.rdel_first().ok_or(Error::CorruptBlock {
            id: candidate.first_block,
        })?;
        let mut b = 0u32;
        while b < candidate.rdel_blocks {
            let id = rdel_first
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
                self.check_rdel_no_resurrection(candidate, e.start, e.end, e.seq, key, val)
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
            let tables = self
                .manifest
                .level(lvl)
                .ok_or(Error::BadLevel { level: lvl })?;
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
                let cur_hit = self.read_point(&key[..klen], val, view, 0, None).await?;
                if cur_hit.is_none() {
                    let alt_hit = self
                        .read_point(&key[..klen], val, view, 0, Some(candidate.id))
                        .await?;
                    if alt_hit.is_some() {
                        return Err(refuse());
                    }
                }
            }
        }
        Ok(())
    }
}
