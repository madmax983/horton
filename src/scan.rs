//! Merge scan iterator over the memtable and every `SSTable`.
//!
//! A `Scan` is caller-owned state that borrows the database: it holds one
//! shared block buffer plus one lightweight cursor per table (cached head
//! key, seq, tombstone flag, value length) and a memtable position. The
//! borrow — not documentation — is what forbids `put`/`flush`/`compact`
//! while a scan is in flight, so cursor positions stay valid and the
//! snapshot view stays stable.
//!
//! Winner selection mirrors the compaction merge: the minimum key across
//! live heads, highest sequence number winning ties, tombstone winners
//! skipped silently. Entries with `seq > max_seq` are invisible, which is
//! what makes scans at a [`Db::snapshot`] watermark repeatable.
//!
//! Key/value bytes go into caller buffers. [`Error::BufferTooSmall`] is
//! reported *before* any cursor advances, so retrying with a larger buffer
//! yields the same entry — never silent truncation, never a skipped entry.

use core::future::poll_fn;

use crate::db::Db;
use crate::device::BlockDevice;
use crate::error::Error;
use crate::manifest::TableRef;
use crate::sstable;

/// One table's position in a scan. The head entry is cached so the merge
/// compares keys without reloading blocks; values are re-parsed from the
/// shared buffer only for the winning entry.
#[derive(Clone, Copy)]
struct ScanCursor<const KEY_MAX: usize> {
    first_block: u64,
    data_blocks: u64,
    block_idx: u64,
    block_id: u64,
    /// Offset of the head entry in the block.
    off: usize,
    /// Offset just past the head entry (where parking resumes).
    next: usize,
    /// Entries end in the current block (start of the restart trailer).
    end: usize,
    live: bool,
    key: [u8; KEY_MAX],
    key_len: usize,
    seq: u64,
    tombstone: bool,
    val_len: usize,
}

impl<const KEY_MAX: usize> ScanCursor<KEY_MAX> {
    /// A `const` item can't use generic params in const operations on
    /// stable, hence a `const fn` instead of an associated constant.
    const fn empty() -> Self {
        Self {
            first_block: 0,
            data_blocks: 0,
            block_idx: 0,
            block_id: 0,
            off: 0,
            next: 0,
            end: 0,
            live: false,
            key: [0u8; KEY_MAX],
            key_len: 0,
            seq: 0,
            tombstone: false,
            val_len: 0,
        }
    }
}

/// Caller-owned merge scan over one database.
///
/// Created by [`Scan::new`], positioned by [`seek`](Scan::seek), advanced
/// by [`next`](Scan::next). Borrows the database for its whole lifetime:
/// while a `Scan` is alive, `put`, `delete`, `flush`, and `compact_step`
/// cannot run (they need `&mut`), so cursor positions and the snapshot
/// view cannot shift under the scan.
///
/// All memory is caller-owned: one `[u8; BLOCK]` buffer plus one small
/// cursor per table. No allocation, no hidden buffering.
pub struct Scan<
    'd,
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
    db: &'d Db<D, BLOCK, KEY_MAX, VAL_MAX, CAP, ARENA, LEVELS, TABLES, BLOOM_BYTES, FREELIST>,
    start: [u8; KEY_MAX],
    start_len: usize,
    end: [u8; KEY_MAX],
    end_len: usize,
    has_end: bool,
    max_seq: u64,
    /// Physical block as last read (CRC-verified); `block` below is the
    /// logical block inflated from it. Shared block buffer; `block_id`
    /// says what it currently holds.
    raw: [u8; BLOCK],
    /// Logical block: `raw` copied here when uncompressed, decompressed
    /// here when the compression flag is set. Every parser reads from
    /// here, so decompression is transparent to the scan.
    block: [u8; BLOCK],
    block_id: Option<u64>,
    /// Memtable head: slot index plus cached entry bytes.
    mem_idx: usize,
    mem_key: [u8; KEY_MAX],
    mem_key_len: usize,
    mem_val: [u8; VAL_MAX],
    mem_val_len: usize,
    mem_seq: u64,
    mem_tombstone: bool,
    mem_live: bool,
    /// One cursor row per level; `cursor_counts[li]` says how many of row
    /// `li` are in use. (Nested rather than flat: stable Rust forbids
    /// const-generic products like `LEVELS * TABLES` in array lengths.)
    cursors: [[ScanCursor<KEY_MAX>; TABLES]; LEVELS],
    cursor_counts: [usize; LEVELS],
}

impl<
    'd,
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
> Scan<'d, D, BLOCK, KEY_MAX, VAL_MAX, CAP, ARENA, LEVELS, TABLES, BLOOM_BYTES, FREELIST>
{
    /// Creates an unpositioned scan over `db`. Call [`seek`](Scan::seek)
    /// before [`next`](Scan::next).
    #[must_use]
    pub const fn new(
        db: &'d Db<D, BLOCK, KEY_MAX, VAL_MAX, CAP, ARENA, LEVELS, TABLES, BLOOM_BYTES, FREELIST>,
    ) -> Self {
        Self {
            db,
            start: [0u8; KEY_MAX],
            start_len: 0,
            end: [0u8; KEY_MAX],
            end_len: 0,
            has_end: false,
            max_seq: u64::MAX,
            raw: [0u8; BLOCK],
            block: [0u8; BLOCK],
            block_id: None,
            mem_idx: 0,
            mem_key: [0u8; KEY_MAX],
            mem_key_len: 0,
            mem_val: [0u8; VAL_MAX],
            mem_val_len: 0,
            mem_seq: 0,
            mem_tombstone: false,
            mem_live: false,
            cursors: [[ScanCursor::empty(); TABLES]; LEVELS],
            cursor_counts: [0; LEVELS],
        }
    }

    /// Positions the scan at the first entry `>= start` and below `end`
    /// (exclusive; `None` scans to the last key), showing only mutations
    /// with `seq <= max_seq`. Pass a [`Db::snapshot`] watermark for a
    /// pinned read, or `u64::MAX` for the latest view. Prefix scans are
    /// `seek(prefix, Some(prefix_end))`.
    ///
    /// An empty `start` scans from the first key. A `start` at or above
    /// `end` simply yields nothing. Re-seeking a scan is allowed and
    /// repositions every cursor from scratch.
    ///
    /// # Errors
    ///
    /// [`Error::KeyTooLarge`] when a bound exceeds `KEY_MAX`,
    /// [`Error::EmptyKey`] for an empty `end` bound,
    /// [`Error::CorruptBlock`] on a torn index/data block or bad footer,
    /// [`Error::CorruptManifest`] on a malformed table range, or
    /// [`Error::Device`] on I/O failure.
    pub async fn seek(
        &mut self,
        start: &[u8],
        end: Option<&[u8]>,
        max_seq: u64,
    ) -> Result<(), Error<D::Error>> {
        if start.len() > KEY_MAX {
            return Err(Error::KeyTooLarge {
                len: start.len(),
                max: KEY_MAX,
            });
        }
        match end {
            Some([]) => return Err(Error::EmptyKey),
            Some(e) if e.len() > KEY_MAX => {
                return Err(Error::KeyTooLarge {
                    len: e.len(),
                    max: KEY_MAX,
                });
            }
            Some(e) => {
                self.end[..e.len()].copy_from_slice(e);
                self.end_len = e.len();
                self.has_end = true;
            }
            None => {
                self.has_end = false;
                self.end_len = 0;
            }
        }
        self.start[..start.len()].copy_from_slice(start);
        self.start_len = start.len();
        self.max_seq = max_seq;
        self.block_id = None;

        // Copy the shared ref: everything derived from it lives
        // independently of this `&mut self` borrow.
        let db = self.db;
        // Memtable cursor: first slot at/after `start`.
        self.mem_idx = db.memtable().lower_bound(start);
        self.advance_mem();

        // One cursor per table overlapping [start, end). The merge resolves
        // L0 overlaps and cross-level duplication; order here is irrelevant.
        self.cursor_counts = [0; LEVELS];
        for li in 0..LEVELS {
            let tables = db.manifest_ref().level(li).unwrap_or(&[]);
            for tref in tables {
                // Key-range prune: no I/O for tables outside the scan.
                if tref.last_key.as_slice() < start {
                    continue;
                }
                if self.has_end && tref.first_key.as_slice() >= &self.end[..self.end_len] {
                    continue;
                }
                // `level()` slices a `[TableRef; TABLES]`, so the row below
                // cannot overflow.
                self.add_cursor(tref, li, start).await?;
            }
        }
        Ok(())
    }

    /// The source of the minimum live head key, and that key itself, or
    /// `None` when every source is exhausted. Source 0 is the memtable;
    /// sources 1..=total are table cursors in level-major order. Carries
    /// the leader's key alongside its source so callers (and this loop's
    /// own comparisons) never re-derive it through another `cursor_pos`
    /// lookup.
    fn min_src(&self, total: usize) -> Option<(usize, &[u8])> {
        let mut best: Option<(usize, &[u8])> = None;
        for src in 0..=total {
            let Some(k) = self.head_key(src) else {
                continue;
            };
            if best.is_none_or(|(_, m)| k < m) {
                best = Some((src, k));
            }
        }
        best
    }

    /// Yields the next entry: copies the key into `key_buf` and the value
    /// into `val_buf`, returning their lengths. Returns `Ok(None)` at the
    /// end of the range. Deleted keys are skipped silently.
    ///
    /// [`Error::BufferTooSmall`] fires *before* any cursor advances, so a
    /// retry with a larger buffer yields the same entry.
    ///
    /// # Errors
    ///
    /// [`Error::BufferTooSmall`] with the required length, [`Error::CorruptBlock`]
    /// on a torn data block, or [`Error::Device`] on I/O failure.
    pub async fn next(
        &mut self,
        key_buf: &mut [u8],
        val_buf: &mut [u8],
    ) -> Result<Option<(usize, usize)>, Error<D::Error>> {
        let total = self.total_cursors();
        loop {
            // Pass 1: the source of the minimum live head key, or None
            // when every source is exhausted. Source 0 is the memtable;
            // sources 1..=total are table cursors in level-major order.
            let Some((min_src, min_key)) = self.min_src(total) else {
                return Ok(None);
            };
            // The end bound is exclusive: a head at/above it ends the scan.
            if self.has_end && min_key >= &self.end[..self.end_len] {
                return Ok(None);
            }
            // Pass 2: among heads on the minimum key, the highest seq wins.
            // `min_key` is invariant for this whole pass (computed once by
            // `min_src` above), so it is compared directly instead of
            // re-fetched through `cursor_pos` on every iteration.
            let mut winner_src = min_src;
            let mut winner_seq = 0u64;
            let mut winner_tombstone = false;
            let mut winner_key_len = 0usize;
            let mut winner_val_len = 0usize;
            for src in 0..=total {
                let Some((k, seq, tomb, vlen)) = self.head(src) else {
                    continue;
                };
                if k != min_key {
                    continue;
                }
                if seq > winner_seq {
                    winner_seq = seq;
                    winner_src = src;
                    winner_tombstone = tomb;
                    winner_key_len = k.len();
                    winner_val_len = vlen;
                }
            }
            let (winner_bid, winner_off, winner_end) = if winner_src == 0 {
                (0u64, 0usize, 0usize)
            } else {
                let (wli, wti) = self.cursor_pos(winner_src);
                let c = &self.cursors[wli][wti];
                (c.block_id, c.off, c.end)
            };
            // Buffer checks and the key copy happen *before* any cursor
            // advances, so BufferTooSmall never consumes an entry.
            if winner_key_len > key_buf.len() {
                return Err(Error::BufferTooSmall {
                    need: winner_key_len,
                });
            }
            {
                let wk: &[u8] = if winner_src == 0 {
                    &self.mem_key[..winner_key_len]
                } else {
                    let (wli, wti) = self.cursor_pos(winner_src);
                    &self.cursors[wli][wti].key[..winner_key_len]
                };
                key_buf[..winner_key_len].copy_from_slice(wk);
            }
            if !winner_tombstone && winner_val_len > val_buf.len() {
                return Err(Error::BufferTooSmall {
                    need: winner_val_len,
                });
            }
            // Copy the winner's value while its cursor is still parked.
            if !winner_tombstone {
                if winner_src == 0 {
                    val_buf[..winner_val_len].copy_from_slice(&self.mem_val[..winner_val_len]);
                } else {
                    self.ensure_block(winner_bid).await?;
                    let entry = sstable::parse_data_entry::<D::Error, BLOCK>(
                        &self.block,
                        winner_off,
                        winner_end,
                        winner_bid,
                    )
                    .map_err(|_| Error::CorruptBlock { id: winner_bid })?;
                    debug_assert_eq!(entry.val.len(), winner_val_len);
                    val_buf[..entry.val.len()].copy_from_slice(entry.val);
                }
            }
            // Advance every head sitting on the minimum key — the winner
            // included. Compare against the copy in `key_buf`: the heads
            // move as cursors advance. A source may hold several versions
            // of the yielded key (newest first); each advance parks at the
            // next visible version, so loop until the head leaves the key —
            // otherwise an older version would be yielded as a duplicate.
            self.advance_past_key(&key_buf[..winner_key_len], total)
                .await?;
            if winner_tombstone {
                continue;
            }
            return Ok(Some((winner_key_len, winner_val_len)));
        }
    }

    /// Advances every head currently sitting on `minkey` — the memtable
    /// position and each live table cursor — so the next `next()` call sees
    /// a fresh minimum key. A cursor holding several versions of the key
    /// re-parks at the next visible version until its head leaves the key.
    async fn advance_past_key(
        &mut self,
        minkey: &[u8],
        total: usize,
    ) -> Result<(), Error<D::Error>> {
        while self.mem_live && &self.mem_key[..self.mem_key_len] == minkey {
            self.mem_idx += 1;
            self.advance_mem();
        }
        for src in 1..=total {
            let (li, ti) = self.cursor_pos(src);
            loop {
                if !self.cursors[li][ti].live {
                    break;
                }
                let tied = &self.cursors[li][ti].key[..self.cursors[li][ti].key_len] == minkey;
                if !tied {
                    break;
                }
                self.park_cursor(li, ti, None).await?;
            }
        }
        Ok(())
    }

    /// Reads and CRC-verifies block `id` into the shared buffer, inflating
    /// it when the compression flag is set. Afterwards `self.block` holds
    /// the logical block; every parser reads from there. For data blocks.
    async fn read_verify(&mut self, id: u64) -> Result<(), Error<D::Error>> {
        let db = self.db;
        poll_fn(|cx| db.device().poll_read_block(cx, id, &mut self.raw))
            .await
            .map_err(Error::Device)?;
        sstable::check_block_crc::<D::Error, BLOCK>(&self.raw, id)?;
        if !sstable::inflate_data_block::<D::Error, BLOCK>(&self.raw, &mut self.block, id)? {
            self.block.copy_from_slice(&self.raw);
        }
        self.block_id = Some(id);
        Ok(())
    }

    /// Reads and CRC-verifies block `id` into the shared buffer with no
    /// inflation. For index blocks, which are never compressed (their
    /// tail bytes are index payload, not a restart count — running them
    /// through the flag check would misread a coincidental bit).
    async fn read_verify_index(&mut self, id: u64) -> Result<(), Error<D::Error>> {
        let db = self.db;
        poll_fn(|cx| db.device().poll_read_block(cx, id, &mut self.block))
            .await
            .map_err(Error::Device)?;
        sstable::check_block_crc::<D::Error, BLOCK>(&self.block, id)?;
        self.block_id = Some(id);
        Ok(())
    }

    /// Ensures the shared buffer holds block `id` (verified on load).
    async fn ensure_block(&mut self, id: u64) -> Result<(), Error<D::Error>> {
        if self.block_id == Some(id) {
            return Ok(());
        }
        self.read_verify(id).await
    }

    /// Adds a cursor over `tref`, parked at the first entry `>= start` with
    /// `seq <= max_seq`. The caller key-range prunes; this resolves the
    /// table's block index to the starting data block.
    async fn add_cursor(
        &mut self,
        tref: &TableRef<KEY_MAX>,
        li: usize,
        start: &[u8],
    ) -> Result<(), Error<D::Error>> {
        let data_blocks =
            u64::from(tref.block_count)
                .checked_sub(3)
                .ok_or(Error::CorruptBlock {
                    id: tref.first_block,
                })?;
        if data_blocks == 0 {
            return Err(Error::CorruptBlock {
                id: tref.first_block,
            });
        }
        let footer = tref
            .first_block
            .checked_add(u64::from(tref.block_count))
            .and_then(|end| end.checked_sub(1))
            .ok_or(Error::CorruptManifest)?;
        let db = self.db;
        let index_id = sstable::footer_index_block(db.device(), &mut self.block, footer).await?;
        self.block_id = Some(footer);
        self.read_verify_index(index_id).await?;
        let payload_end = BLOCK - sstable::CRC_LEN;
        let data_id =
            sstable::index_lookup::<D::Error>(&self.block[..payload_end], start, index_id)?;
        // `None`: `start` sorts before every block — begin at block 0.
        // (The walk budget in the tuple's second half is for point
        // lookups; the scan cursor walks blocks on its own.)
        let block_idx = match data_id {
            Some((id, _)) => id
                .checked_sub(tref.first_block)
                .ok_or(Error::CorruptBlock { id })?,
            None => 0,
        };
        if block_idx >= data_blocks {
            return Err(Error::CorruptBlock {
                id: tref.first_block,
            });
        }
        let bid = tref
            .first_block
            .checked_add(block_idx)
            .ok_or(Error::CorruptBlock {
                id: tref.first_block,
            })?;
        self.read_verify(bid).await?;
        let end = sstable::data_entries_end::<D::Error, BLOCK>(&self.block, bid)?;
        if end == 0 {
            // A data block always carries at least one entry.
            return Err(Error::CorruptBlock { id: bid });
        }
        let ti = self.cursor_counts[li];
        self.cursors[li][ti] = ScanCursor {
            first_block: tref.first_block,
            data_blocks,
            block_idx,
            block_id: bid,
            off: 0,
            next: 0,
            end,
            live: true,
            key: [0u8; KEY_MAX],
            key_len: 0,
            seq: 0,
            tombstone: false,
            val_len: 0,
        };
        self.cursor_counts[li] += 1;
        self.park_cursor(li, ti, Some(start)).await
    }

    /// Parks cursor `(li, ti)` at the next entry at/after its current `next`
    /// offset with `seq <= max_seq` and (when `floor` is `Some`)
    /// `key >= floor`. Crosses block boundaries; sets `live = false` at
    /// exhaustion. A torn block is [`Error::CorruptBlock`]: unlike point
    /// lookups, a scan must never silently skip entries.
    async fn park_cursor(
        &mut self,
        li: usize,
        ti: usize,
        floor: Option<&[u8]>,
    ) -> Result<(), Error<D::Error>> {
        let max_seq = self.max_seq;
        loop {
            let (bid, off, end, idx, first, nblocks) = {
                let c = &self.cursors[li][ti];
                if !c.live {
                    return Ok(());
                }
                (
                    c.block_id,
                    c.next,
                    c.end,
                    c.block_idx,
                    c.first_block,
                    c.data_blocks,
                )
            };
            self.ensure_block(bid).await?;
            let mut off = off;
            // Scan this block's entries for the next visible one.
            let parked = loop {
                let Ok(entry) =
                    sstable::parse_data_entry::<D::Error, BLOCK>(&self.block, off, end, bid)
                else {
                    // Genuine zero padding marks end-of-entries; any
                    // other unparseable structure is corruption.
                    if sstable::all_zero(&self.block[off..end]) {
                        break None;
                    }
                    return Err(Error::CorruptBlock { id: bid });
                };
                let next = entry.next;
                let visible = entry.seq <= max_seq && floor.is_none_or(|f| entry.key >= f);
                if visible {
                    // Copy the head out: the borrow of `self.block` must end
                    // before the cursor is mutated.
                    let mut key = [0u8; KEY_MAX];
                    key[..entry.key.len()].copy_from_slice(entry.key);
                    break Some((
                        key,
                        entry.key.len(),
                        entry.seq,
                        entry.tombstone,
                        entry.val.len(),
                        off,
                        next,
                    ));
                }
                off = next;
            };
            if let Some((key, klen, seq, tomb, vlen, eoff, next)) = parked {
                let c = &mut self.cursors[li][ti];
                c.key = key;
                c.key_len = klen;
                c.seq = seq;
                c.tombstone = tomb;
                c.val_len = vlen;
                c.off = eoff;
                c.next = next;
                c.live = true;
                return Ok(());
            }
            // Block exhausted: move to the next data block, if any.
            if idx + 1 >= nblocks {
                self.cursors[li][ti].live = false;
                return Ok(());
            }
            let nid = first
                .checked_add(idx + 1)
                .ok_or(Error::CorruptBlock { id: first })?;
            self.read_verify(nid).await?;
            let nend = sstable::data_entries_end::<D::Error, BLOCK>(&self.block, nid)?;
            if nend == 0 {
                return Err(Error::CorruptBlock { id: nid });
            }
            let c = &mut self.cursors[li][ti];
            c.block_id = nid;
            c.block_idx = idx + 1;
            c.end = nend;
            c.next = 0;
        }
    }

    /// Parks the memtable head at the next live slot at/after `mem_idx`
    /// with `seq <= max_seq`. The scan borrows the database, so no
    /// `put`/`flush` can shift slots under this index walk.
    fn advance_mem(&mut self) {
        let max_seq = self.max_seq;
        let db = self.db;
        self.mem_live = false;
        while self.mem_idx < db.memtable().slot_len() {
            if let Some(v) = db.memtable().slot_view(self.mem_idx)
                && v.seq <= max_seq
            {
                self.mem_key[..v.key.len()].copy_from_slice(v.key);
                self.mem_key_len = v.key.len();
                self.mem_val[..v.val.len()].copy_from_slice(v.val);
                self.mem_val_len = v.val.len();
                self.mem_seq = v.seq;
                self.mem_tombstone = v.tombstone;
                self.mem_live = true;
                return;
            }
            self.mem_idx += 1;
        }
    }

    /// Total live-or-dead table cursors across all levels.
    const fn total_cursors(&self) -> usize {
        let mut total = 0usize;
        let mut li = 0usize;
        while li < LEVELS {
            total += self.cursor_counts[li];
            li += 1;
        }
        total
    }

    /// Maps a 1-based table source to its `(level, row)` position.
    /// Callers only pass `src <= total_cursors()`.
    const fn cursor_pos(&self, src: usize) -> (usize, usize) {
        let mut s = src - 1;
        let mut li = 0;
        while li < LEVELS && s >= self.cursor_counts[li] {
            s -= self.cursor_counts[li];
            li += 1;
        }
        (li, s)
    }

    /// Head key of a source: source 0 is the memtable, sources 1..=total
    /// are table cursors in level-major order. `None` when the source is
    /// exhausted.
    fn head_key(&self, src: usize) -> Option<&[u8]> {
        if src == 0 {
            if self.mem_live {
                Some(&self.mem_key[..self.mem_key_len])
            } else {
                None
            }
        } else {
            let (li, ti) = self.cursor_pos(src);
            let c = &self.cursors[li][ti];
            if c.live {
                Some(&c.key[..c.key_len])
            } else {
                None
            }
        }
    }

    /// Head `(key, seq, tombstone, val_len)` of a source, or `None` when
    /// exhausted. Source 0 is the memtable; sources 1..=total are table
    /// cursors in level-major order. One `cursor_pos` lookup instead of
    /// the two `head_key`/`head_meta` used to cost a caller needing both.
    fn head(&self, src: usize) -> Option<(&[u8], u64, bool, usize)> {
        if src == 0 {
            self.mem_live.then_some((
                &self.mem_key[..self.mem_key_len],
                self.mem_seq,
                self.mem_tombstone,
                self.mem_val_len,
            ))
        } else {
            let (li, ti) = self.cursor_pos(src);
            let c = &self.cursors[li][ti];
            c.live
                .then_some((&c.key[..c.key_len], c.seq, c.tombstone, c.val_len))
        }
    }
}
