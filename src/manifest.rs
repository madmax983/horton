//! Crash-safe root pointer: the manifest.
//!
//! Double-buffered in two fixed block slots. Each commit bumps `seq`,
//! serializes into one block with a CRC32, writes slot `seq % 2`, and flushes
//! the device. Recovery picks the highest-seq slot with a valid CRC.
//!
//! Layout (all little-endian):
//!
//! ```text
//! magic: u64 = "hrtman04" | payload_len: u32 | payload | crc32: u32
//! ```
//!
//! `payload` is variable-length (only populated levels are stored):
//!
//! ```text
//! seq: u64 | wal_head: u64 | next_table_id: u32
//!     | flushed_seq: u64 | seq_high: u64 | nlevels: u32
//! per level: count: u32 | count * tableref
//! tableref: id: u32 | first_block: u64 | block_count: u32
//!           | fk_len: u16 | fk_bytes[fk_len]
//!           | lk_len: u16 | lk_bytes[lk_len]
//!           | max_seq: u64 | min_seq: u64 | entry_count: u32 | rdel_blocks: u32
//! ```
//!
//! `crc32` covers `magic` through the end of `payload`. The rest of the block
//! is zero padding.

use core::future::poll_fn;
use core::marker::PhantomData;

use crate::crc::crc32;
use crate::device::BlockDevice;
use crate::error::Error;

/// Manifest block magic: ASCII "hrtman04".
///
/// The magic changes whenever the layout changes (pre-1.0 format policy:
/// no compatibility across minor versions). "hrtman01" was the v0.4.0
/// layout with fixed `KEY_MAX` key bounds; "hrtman02" was the v0.4.1
/// layout with length-prefixed key bounds; "hrtman03" is the v0.15 layout
/// with the per-table `rdel_blocks` count; "hrtman04" (v0.17) adds the
/// persisted sequence floors `flushed_seq` and `seq_high`. A foreign magic
/// decodes as
/// [`Error::CorruptManifest`] — old bytes are rejected, never misparsed.
pub const MANIFEST_MAGIC: u64 = u64::from_le_bytes(*b"hrtman04");

/// Fixed-size key bound: `len` significant bytes of `bytes`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyBound<const KEY_MAX: usize> {
    /// Significant bytes.
    pub len: u16,
    /// Key bytes, `len` of them significant.
    pub bytes: [u8; KEY_MAX],
}

impl<const KEY_MAX: usize> KeyBound<KEY_MAX> {
    /// An empty bound.
    pub const EMPTY: Self = Self {
        len: 0,
        bytes: [0u8; KEY_MAX],
    };

    /// Copies `key` into a bound; `None` when `key` exceeds `KEY_MAX`.
    #[must_use]
    pub fn from_slice(key: &[u8]) -> Option<Self> {
        if key.len() > KEY_MAX {
            return None;
        }
        let mut bytes = [0u8; KEY_MAX];
        bytes[..key.len()].copy_from_slice(key);
        // `Db` upholds `KEY_MAX <= 0xFFFF` with const asserts, so this only
        // fails for direct misuse with a larger `KEY_MAX`.
        let len = u16::try_from(key.len()).ok()?;
        Some(Self { len, bytes })
    }

    /// The significant bytes.
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        &self.bytes[..usize::from(self.len)]
    }

    /// The lesser bound; [`KeyBound::EMPTY`] (len 0) loses to any real key.
    /// Used to fold range-tombstone bounds into a table's key range.
    #[must_use]
    pub fn min(self, other: Self) -> Self {
        if self.len == 0 {
            return other;
        }
        if other.len == 0 {
            return self;
        }
        if other.as_slice() < self.as_slice() {
            other
        } else {
            self
        }
    }

    /// The greater bound; [`KeyBound::EMPTY`] loses to any real key.
    #[must_use]
    pub fn max(self, other: Self) -> Self {
        if self.len == 0 {
            return other;
        }
        if other.len == 0 {
            return self;
        }
        if other.as_slice() > self.as_slice() {
            other
        } else {
            self
        }
    }
}

/// One `SSTable` placement record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableRef<const KEY_MAX: usize> {
    /// Table id, unique per database lifetime.
    pub id: u32,
    /// First block id of the table. On-device layout (v0.17):
    /// `[data]* [rdel]* [bloom] [index] [footer]` — see
    /// [`data_first`](Self::data_first), [`rdel_first`](Self::rdel_first),
    /// and [`footer_block`](Self::footer_block).
    pub first_block: u64,
    /// Total blocks: rdel blocks + data blocks + bloom + index + footer.
    pub block_count: u32,
    /// Smallest key in the table (includes range-tombstone bounds).
    pub first_key: KeyBound<KEY_MAX>,
    /// Largest key in the table (includes range-tombstone bounds; a
    /// tombstone's exclusive end is stored as-is, so `last_key` may name
    /// one past the last covered key — conservative for overlap checks,
    /// never unsound).
    pub last_key: KeyBound<KEY_MAX>,
    /// Highest sequence number in the table (includes range tombstones).
    pub max_seq: u64,
    /// Lowest sequence number in the table (includes range tombstones).
    /// Compaction drops a tombstone only when no table outside the job
    /// that overlaps it has `min_seq` at or below the tombstone's sequence
    /// — such a table could hold an older version the tombstone hides
    /// (an ingested table re-attached at L0, for instance).
    pub min_seq: u64,
    /// Key/value entries (including tombstones).
    pub entry_count: u32,
    /// Range-tombstone blocks, stored after the data blocks (see
    /// [`rdel_first`](Self::rdel_first)).
    pub rdel_blocks: u32,
}

impl<const KEY_MAX: usize> TableRef<KEY_MAX> {
    /// An empty (zero) reference, used for array initialization.
    pub const EMPTY: Self = Self {
        id: 0,
        first_block: 0,
        block_count: 0,
        first_key: KeyBound::EMPTY,
        last_key: KeyBound::EMPTY,
        max_seq: 0,
        min_seq: 0,
        entry_count: 0,
        rdel_blocks: 0,
    };

    /// Data blocks: `block_count - rdel_blocks - 3` (bloom, index,
    /// footer), or `None` for a malformed ref.
    #[must_use]
    pub const fn data_blocks(&self) -> Option<u64> {
        let total = self.block_count as u64;
        let rdel = self.rdel_blocks as u64;
        match total.checked_sub(rdel) {
            Some(n) => n.checked_sub(3),
            None => None,
        }
    }

    /// First data block: the table starts with its data section.
    #[must_use]
    pub const fn data_first(&self) -> u64 {
        self.first_block
    }

    /// First range-tombstone block: right after the data section, or
    /// `None` for a malformed ref.
    #[must_use]
    pub const fn rdel_first(&self) -> Option<u64> {
        match self.data_blocks() {
            Some(d) => self.first_block.checked_add(d),
            None => None,
        }
    }

    /// The footer: the table's last block, or `None` for a malformed ref.
    #[must_use]
    pub const fn footer_block(&self) -> Option<u64> {
        match self.first_block.checked_add(self.block_count as u64) {
            Some(end) => end.checked_sub(1),
            None => None,
        }
    }

    /// One past the last block id of the table.
    #[must_use]
    pub const fn end_block(&self) -> u64 {
        self.first_block + self.block_count as u64
    }

    /// True when `key` lies within the table's key bounds (inclusive).
    ///
    /// Exact: every key stored in the table is within
    /// `[first_key, last_key]`, so a key outside the bounds is definitely
    /// absent and the table can be skipped without any I/O.
    #[must_use]
    pub fn covers(&self, key: &[u8]) -> bool {
        self.first_key.as_slice() <= key && key <= self.last_key.as_slice()
    }
}

/// The manifest: sequence, WAL head, table-id counter, the persisted
/// sequence floors, and every level's tables.
///
/// Levels share one pool of `LEVELS * TABLES` table refs
/// ([`CAPACITY`](Self::CAPACITY)): level 0 holds at most `TABLES` (flush
/// output, oldest first), and deeper levels take any share of the rest,
/// kept sorted by first key (disjoint runs). A full pool — not a full
/// level — is what limits the tree. Each level's refs are contiguous in
/// the pool, so [`level`](Self::level) is a plain slice.
///
/// Past the live refs the pool may hold *pending* refs: the output tables
/// of the in-flight compaction job, staged until its manifest commit
/// adopts them. Pending refs count against the capacity (their tables
/// occupy real slots) but are invisible to every read, and never encoded.
#[derive(Debug, Clone, Copy)]
pub struct Manifest<const LEVELS: usize, const TABLES: usize, const KEY_MAX: usize> {
    seq: u64,
    wal_head: u64,
    next_table_id: u32,
    /// Highest sequence number that has left the WAL: every record with
    /// `seq <= flushed_seq` is in a table or behind `wal_head`. Recovery
    /// skips WAL records at or below it (stale pre-wrap blocks). Raised
    /// only by flush — never derived from live tables, which can shrink.
    flushed_seq: u64,
    /// Highest sequence number ever issued as of the last commit.
    /// Recovery resumes the counter at or above it, so sequence numbers
    /// are never reused even after compaction or archival removes the
    /// tables that held them.
    seq_high: u64,
    /// The table-ref pool, viewed flat: level 0's refs, then level 1's,
    /// …, then the pending refs.
    pool: [[TableRef<KEY_MAX>; TABLES]; LEVELS],
    /// Live refs per level.
    counts: [usize; LEVELS],
    /// Pending refs after the live ones.
    pending: usize,
}

impl<const LEVELS: usize, const TABLES: usize, const KEY_MAX: usize>
    Manifest<LEVELS, TABLES, KEY_MAX>
{
    /// A fresh (empty) manifest. `wal_head` is set by the caller on first open.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            seq: 0,
            wal_head: 0,
            next_table_id: 0,
            flushed_seq: 0,
            seq_high: 0,
            pool: [[TableRef::EMPTY; TABLES]; LEVELS],
            counts: [0; LEVELS],
            pending: 0,
        }
    }

    /// Table refs the pool holds across all levels (live plus pending).
    pub const CAPACITY: usize = LEVELS * TABLES;

    const fn flat(&self) -> &[TableRef<KEY_MAX>] {
        self.pool.as_flattened()
    }

    const fn flat_mut(&mut self) -> &mut [TableRef<KEY_MAX>] {
        self.pool.as_flattened_mut()
    }

    /// Pool index where level `level`'s refs begin.
    fn start(&self, level: usize) -> usize {
        self.counts[..level].iter().sum()
    }

    /// Live refs across all levels.
    fn live(&self) -> usize {
        self.counts.iter().sum()
    }

    /// Every live table ref, level by level.
    #[must_use]
    pub fn tables(&self) -> &[TableRef<KEY_MAX>] {
        &self.flat()[..self.live()]
    }

    /// The staged output refs of the in-flight compaction job.
    #[must_use]
    pub fn pending(&self) -> &[TableRef<KEY_MAX>] {
        let live = self.live();
        &self.flat()[live..live + self.pending]
    }

    /// Pool entries still free (neither live nor pending).
    #[must_use]
    pub fn free_refs(&self) -> usize {
        Self::CAPACITY - self.live() - self.pending
    }

    /// Inserts `tref` into `level` at pool index `pos`, shifting every
    /// later ref (later levels and the pending tail) up by one.
    fn insert_at<E>(
        &mut self,
        pos: usize,
        level: usize,
        tref: TableRef<KEY_MAX>,
    ) -> Result<(), Error<E>> {
        let used = self.live() + self.pending;
        if used >= Self::CAPACITY {
            return Err(Error::NoSpace);
        }
        let flat = self.flat_mut();
        flat.copy_within(pos..used, pos + 1);
        flat[pos] = tref;
        self.counts[level] += 1;
        Ok(())
    }

    /// Manifest sequence number (bumped per commit).
    #[must_use]
    pub const fn seq(&self) -> u64 {
        self.seq
    }

    /// First WAL block still needed for recovery.
    #[must_use]
    pub const fn wal_head(&self) -> u64 {
        self.wal_head
    }

    /// Sets the WAL head (advanced past flushed WAL blocks on flush).
    pub const fn set_wal_head(&mut self, head: u64) {
        self.wal_head = head;
    }

    /// Highest sequence number that has left the WAL (see the field docs):
    /// the WAL replay floor.
    #[must_use]
    pub const fn flushed_seq(&self) -> u64 {
        self.flushed_seq
    }

    /// Highest sequence number issued as of the last commit: the floor the
    /// sequence counter resumes from.
    #[must_use]
    pub const fn seq_high(&self) -> u64 {
        self.seq_high
    }

    /// Records a flush: every mutation with `seq <= seq` has left the WAL.
    /// Monotone — a lower value never lowers either floor.
    pub const fn note_flushed(&mut self, seq: u64) {
        if seq > self.flushed_seq {
            self.flushed_seq = seq;
        }
        self.raise_seq_high(seq);
    }

    /// Raises the persisted sequence high-water mark. Monotone.
    pub const fn raise_seq_high(&mut self, seq: u64) {
        if seq > self.seq_high {
            self.seq_high = seq;
        }
    }

    /// Next table id to assign.
    #[must_use]
    pub const fn next_table_id(&self) -> u32 {
        self.next_table_id
    }

    /// Advances the table-id counter.
    ///
    /// # Errors
    ///
    /// [`Error::NoSpace`] on counter overflow (unreachable in practice).
    pub fn bump_table_id<E>(&mut self) -> Result<(), Error<E>> {
        self.next_table_id = self.next_table_id.checked_add(1).ok_or(Error::NoSpace)?;
        Ok(())
    }

    /// Allocates the next table id, returning it.
    ///
    /// # Errors
    ///
    /// [`Error::NoSpace`] on counter overflow (unreachable in practice).
    pub fn alloc_table_id<E>(&mut self) -> Result<u32, Error<E>> {
        let id = self.next_table_id;
        self.bump_table_id()?;
        Ok(id)
    }

    /// Raises the next-table-id floor to at least `floor`.
    ///
    /// Used when ingesting an externally-sealed table whose id may be
    /// above the local counter: future local tables must never collide
    /// with an ingested id. Idempotent and monotone — never lowers the
    /// counter.
    pub const fn advance_next_table_id(&mut self, floor: u32) {
        if floor > self.next_table_id {
            self.next_table_id = floor;
        }
    }

    /// Finds a live table by id, searching every level.
    #[must_use]
    pub fn find_table(&self, id: u32) -> Option<&TableRef<KEY_MAX>> {
        self.tables().iter().find(|t| t.id == id)
    }

    /// True when level 0 already holds `TABLES` tables: flush must wait
    /// for compaction to drain it.
    #[must_use]
    pub const fn l0_is_full(&self) -> bool {
        self.counts[0] >= TABLES
    }

    /// Appends a table ref to level 0 (flush output; oldest first).
    ///
    /// # Errors
    ///
    /// [`Error::NoSpace`] when level 0 already holds `TABLES` tables or the
    /// pool is full.
    pub fn add_l0_table<E>(&mut self, tref: TableRef<KEY_MAX>) -> Result<(), Error<E>> {
        if self.counts[0] >= TABLES {
            return Err(Error::NoSpace);
        }
        self.insert_at(self.counts[0], 0, tref)
    }

    /// Live table refs of level 0, oldest first.
    #[must_use]
    pub fn l0(&self) -> &[TableRef<KEY_MAX>] {
        &self.flat()[..self.counts[0]]
    }

    /// Live table refs of level `idx`, or `None` when `idx` is out of
    /// range. Level 0 holds overlapping flush output, oldest first; deeper
    /// levels hold disjoint runs sorted by first key.
    #[must_use]
    pub fn level(&self, idx: usize) -> Option<&[TableRef<KEY_MAX>]> {
        if idx >= LEVELS {
            return None;
        }
        let s = self.start(idx);
        Some(&self.flat()[s..s + self.counts[idx]])
    }

    /// Adds a table ref to `level`: appended to level 0, inserted in
    /// first-key order into deeper levels. The caller keeps deeper levels'
    /// key ranges disjoint (compaction's job selection does).
    ///
    /// # Errors
    ///
    /// [`Error::NoSpace`] when `level` is out of range, level 0 is full,
    /// or the pool is full.
    pub fn add_table_to_level<E>(
        &mut self,
        level: usize,
        tref: TableRef<KEY_MAX>,
    ) -> Result<(), Error<E>> {
        if level >= LEVELS {
            return Err(Error::NoSpace);
        }
        if level == 0 {
            return self.add_l0_table(tref);
        }
        let s = self.start(level);
        let within = self.flat()[s..s + self.counts[level]]
            .partition_point(|t| t.first_key.as_slice() < tref.first_key.as_slice());
        self.insert_at(s + within, level, tref)
    }

    /// Removes the table with `id` from `level`, keeping the pool packed.
    /// Returns `true` when a table was removed.
    ///
    /// Removing a table that is not there is not an error — compaction
    /// replays its input set against the live manifest, and a missing input
    /// simply means there is nothing to remove.
    ///
    /// # Errors
    ///
    /// [`Error::NoSpace`] when `level` is out of range.
    pub fn remove_table_from_level<E>(&mut self, level: usize, id: u32) -> Result<bool, Error<E>> {
        if level >= LEVELS {
            return Err(Error::NoSpace);
        }
        let s = self.start(level);
        let Some(pos) = self.flat()[s..s + self.counts[level]]
            .iter()
            .position(|t| t.id == id)
        else {
            return Ok(false);
        };
        let used = self.live() + self.pending;
        let flat = self.flat_mut();
        flat.copy_within(s + pos + 1..used, s + pos);
        flat[used - 1] = TableRef::EMPTY;
        self.counts[level] -= 1;
        Ok(true)
    }

    /// Stages a compaction output ref as pending: it occupies a pool entry
    /// but stays invisible until [`adopt_pending`](Self::adopt_pending).
    ///
    /// # Errors
    ///
    /// [`Error::NoSpace`] when the pool is full.
    pub fn push_pending<E>(&mut self, tref: TableRef<KEY_MAX>) -> Result<(), Error<E>> {
        let used = self.live() + self.pending;
        if used >= Self::CAPACITY {
            return Err(Error::NoSpace);
        }
        self.flat_mut()[used] = tref;
        self.pending += 1;
        Ok(())
    }

    /// Discards every pending ref (an abandoned compaction job).
    pub fn clear_pending(&mut self) {
        let live = self.live();
        let used = live + self.pending;
        self.flat_mut()[live..used].fill(TableRef::EMPTY);
        self.pending = 0;
    }

    /// Moves every pending ref into `level` (in first-key order): the
    /// compaction commit's staging step.
    ///
    /// # Errors
    ///
    /// [`Error::NoSpace`] when `level` is out of range, or is level 0 and
    /// cannot take them.
    pub fn adopt_pending<E>(&mut self, level: usize) -> Result<(), Error<E>> {
        while self.pending > 0 {
            let used = self.live() + self.pending;
            let tref = self.flat()[used - 1];
            self.flat_mut()[used - 1] = TableRef::EMPTY;
            self.pending -= 1;
            self.add_table_to_level(level, tref)?;
        }
        Ok(())
    }

    /// Highest sequence number across all live tables (0 when empty).
    ///
    /// Derived, so it can *fall* when compaction or archival removes the
    /// tables holding the newest sequences. Never use it as a durable
    /// floor: that is what [`flushed_seq`](Self::flushed_seq) and
    /// [`seq_high`](Self::seq_high) are for.
    #[must_use]
    pub fn max_seq(&self) -> u64 {
        self.tables().iter().map(|t| t.max_seq).max().unwrap_or(0)
    }

    /// Serializes into `out` (zero-padded to a full block, CRC-terminated).
    ///
    /// # Errors
    ///
    /// [`Error::NoSpace`] when the populated manifest does not fit in one
    /// block — size `LEVELS`/`TABLES`/`KEY_MAX`/`BLOCK` so that it does.
    /// (v0.2 only populates L0, so this is generous in practice.)
    pub fn encode<E, const BLOCK: usize>(&self, out: &mut [u8; BLOCK]) -> Result<(), Error<E>> {
        out.fill(0);
        let mut enc = Encoder::<E> {
            buf: &mut out[..],
            off: 0,
            marker: PhantomData,
        };
        enc.u64(MANIFEST_MAGIC)?;
        enc.u32(0)?; // payload_len, patched below
        let payload_start = enc.off;
        enc.u64(self.seq)?;
        enc.u64(self.wal_head)?;
        enc.u32(self.next_table_id)?;
        enc.u64(self.flushed_seq)?;
        enc.u64(self.seq_high)?;
        enc.u32(u32::try_from(LEVELS).map_err(|_| Error::NoSpace)?)?;
        for li in 0..LEVELS {
            let level = self.level(li).unwrap_or(&[]);
            enc.u32(u32::try_from(level.len()).map_err(|_| Error::NoSpace)?)?;
            for tref in level {
                enc.u32(tref.id)?;
                enc.u64(tref.first_block)?;
                enc.u32(tref.block_count)?;
                enc.u16(tref.first_key.len)?;
                enc.bytes(&tref.first_key.bytes[..usize::from(tref.first_key.len)])?;
                enc.u16(tref.last_key.len)?;
                enc.bytes(&tref.last_key.bytes[..usize::from(tref.last_key.len)])?;
                enc.u64(tref.max_seq)?;
                enc.u64(tref.min_seq)?;
                enc.u32(tref.entry_count)?;
                enc.u32(tref.rdel_blocks)?;
            }
        }
        let payload_len = u32::try_from(enc.off - payload_start).map_err(|_| Error::NoSpace)?;
        out[8..12].copy_from_slice(&payload_len.to_le_bytes());
        let crc_end = payload_start + usize::try_from(payload_len).map_err(|_| Error::NoSpace)?;
        let total = crc_end.checked_add(4).ok_or(Error::NoSpace)?;
        if total > BLOCK {
            return Err(Error::NoSpace);
        }
        let crc = crc32(&out[..crc_end]);
        out[crc_end..crc_end + 4].copy_from_slice(&crc.to_le_bytes());
        Ok(())
    }

    /// Decodes one block. Any structural problem yields
    /// [`Error::CorruptManifest`] — never a partial manifest.
    ///
    /// # Errors
    ///
    /// [`Error::CorruptManifest`] on any inconsistency.
    pub fn decode<E, const BLOCK: usize>(buf: &[u8; BLOCK]) -> Result<Self, Error<E>> {
        let mut dec = Decoder {
            buf: &buf[..],
            off: 0,
        };
        let corrupt = || Error::CorruptManifest;
        if dec.u64().map_err(|()| corrupt())? != MANIFEST_MAGIC {
            return Err(corrupt());
        }
        let payload_len =
            usize::try_from(dec.u32().map_err(|()| corrupt())?).map_err(|_| corrupt())?;
        let crc_end = 12usize.checked_add(payload_len).ok_or_else(corrupt)?;
        let total = crc_end.checked_add(4).ok_or_else(corrupt)?;
        if total > BLOCK {
            return Err(corrupt());
        }
        let stored = u32::from_le_bytes(
            buf[crc_end..crc_end + 4]
                .try_into()
                .map_err(|_| corrupt())?,
        );
        if crc32(&buf[..crc_end]) != stored {
            return Err(corrupt());
        }
        let seq = dec.u64().map_err(|()| corrupt())?;
        let wal_head = dec.u64().map_err(|()| corrupt())?;
        let next_table_id = dec.u32().map_err(|()| corrupt())?;
        let flushed_seq = dec.u64().map_err(|()| corrupt())?;
        let seq_high = dec.u64().map_err(|()| corrupt())?;
        let nlevels = dec.u32().map_err(|()| corrupt())?;
        if usize::try_from(nlevels).map_err(|_| corrupt())? != LEVELS {
            return Err(corrupt());
        }
        let mut out = Self::new();
        out.seq = seq;
        out.wal_head = wal_head;
        out.next_table_id = next_table_id;
        out.flushed_seq = flushed_seq;
        out.seq_high = seq_high;
        let mut total = 0usize;
        for li in 0..LEVELS {
            let count =
                usize::try_from(dec.u32().map_err(|()| corrupt())?).map_err(|_| corrupt())?;
            // Level 0 is capped at TABLES; the whole tree at the pool.
            if (li == 0 && count > TABLES) || count > Self::CAPACITY - total {
                return Err(corrupt());
            }
            for i in 0..count {
                out.flat_mut()[total + i] = Self::decode_tableref(&mut dec)?;
            }
            out.counts[li] = count;
            total += count;
        }
        if dec.off != crc_end {
            return Err(corrupt());
        }
        Ok(out)
    }

    /// Decodes one table ref; any problem is [`Error::CorruptManifest`].
    fn decode_tableref<E>(dec: &mut Decoder<'_>) -> Result<TableRef<KEY_MAX>, Error<E>> {
        let corrupt = || Error::CorruptManifest;
        let id = dec.u32().map_err(|()| corrupt())?;
        let first_block = dec.u64().map_err(|()| corrupt())?;
        let block_count = dec.u32().map_err(|()| corrupt())?;
        let first_key = Self::decode_bound(dec)?;
        let last_key = Self::decode_bound(dec)?;
        let max_seq = dec.u64().map_err(|()| corrupt())?;
        let min_seq = dec.u64().map_err(|()| corrupt())?;
        let entry_count = dec.u32().map_err(|()| corrupt())?;
        let rdel_blocks = dec.u32().map_err(|()| corrupt())?;
        Ok(TableRef {
            id,
            first_block,
            block_count,
            first_key,
            last_key,
            max_seq,
            min_seq,
            entry_count,
            rdel_blocks,
        })
    }

    /// Decodes one key bound.
    fn decode_bound<E>(dec: &mut Decoder<'_>) -> Result<KeyBound<KEY_MAX>, Error<E>> {
        let corrupt = || Error::CorruptManifest;
        let len = dec.u16().map_err(|()| corrupt())?;
        let n = usize::from(len);
        if n > KEY_MAX {
            return Err(corrupt());
        }
        let bytes_in = dec.bytes(n).map_err(|()| corrupt())?;
        let mut bytes = [0u8; KEY_MAX];
        bytes[..n].copy_from_slice(bytes_in);
        Ok(KeyBound { len, bytes })
    }

    /// Reads both slots and returns the winning manifest.
    ///
    /// Picks the highest-seq slot with a valid CRC. Two blank slots mean a
    /// fresh device (`true`) — blank is all zeros on a zeroed device, all
    /// `0xFF` on erased NOR flash. A blank slot paired with a corrupt one is
    /// treated as fresh: that is a torn *first* commit, and the WAL (whose
    /// head is still `wal_start`) replays everything — no data loss, the
    /// orphaned blocks are simply never referenced.
    ///
    /// # Errors
    ///
    /// [`Error::CorruptManifest`] when slots are non-blank but no slot
    /// carries a valid CRC, or [`Error::Device`] on I/O failure.
    pub async fn recover<D: BlockDevice, const BLOCK: usize>(
        device: &mut D,
        scratch: &mut [u8; BLOCK],
        slot_a: u64,
        slot_b: u64,
    ) -> Result<(Self, bool), Error<D::Error>> {
        let a = Self::read_slot(device, scratch, slot_a).await?;
        let b = Self::read_slot(device, scratch, slot_b).await?;
        match (a, b) {
            (Slot::Valid(sa, ma), Slot::Valid(sb, mb)) => {
                Ok((if sa >= sb { ma } else { mb }, false))
            }
            (Slot::Valid(_, m), _) | (_, Slot::Valid(_, m)) => Ok((m, false)),
            (Slot::Blank, Slot::Blank | Slot::Corrupt) | (Slot::Corrupt, Slot::Blank) => {
                Ok((Self::new(), true))
            }
            (Slot::Corrupt, Slot::Corrupt) => Err(Error::CorruptManifest),
        }
    }

    /// Reads one slot: blank (all zeros on a zeroed device, all `0xFF` on
    /// erased NOR flash), corrupt, or a valid manifest.
    async fn read_slot<D: BlockDevice, const BLOCK: usize>(
        device: &D,
        scratch: &mut [u8; BLOCK],
        slot: u64,
    ) -> Result<Slot<LEVELS, TABLES, KEY_MAX>, Error<D::Error>> {
        poll_fn(|cx| device.poll_read_block(cx, slot, scratch))
            .await
            .map_err(Error::Device)?;
        if scratch.iter().all(|&b| b == 0) || scratch.iter().all(|&b| b == 0xFF) {
            return Ok(Slot::Blank);
        }
        Ok(Self::decode::<D::Error, BLOCK>(scratch)
            .map_or_else(|_| Slot::Corrupt, |m| Slot::Valid(m.seq, m)))
    }

    /// Commits: bumps `seq`, writes slot `seq % 2` with CRC, flushes.
    ///
    /// # Errors
    ///
    /// [`Error::NoSpace`] when the manifest does not fit one block or the
    /// sequence counter overflows, or [`Error::Device`] on I/O failure.
    pub async fn commit<D: BlockDevice, const BLOCK: usize>(
        &mut self,
        device: &mut D,
        scratch: &mut [u8; BLOCK],
        slot_a: u64,
        slot_b: u64,
    ) -> Result<(), Error<D::Error>> {
        self.seq = self.seq.checked_add(1).ok_or(Error::NoSpace)?;
        let slot = if self.seq.is_multiple_of(2) {
            slot_a
        } else {
            slot_b
        };
        self.encode::<D::Error, BLOCK>(scratch)?;
        poll_fn(|cx| device.poll_write_block(cx, slot, scratch))
            .await
            .map_err(Error::Device)?;
        poll_fn(|cx| device.poll_flush(cx))
            .await
            .map_err(Error::Device)?;
        Ok(())
    }
}

impl<const LEVELS: usize, const TABLES: usize, const KEY_MAX: usize> Default
    for Manifest<LEVELS, TABLES, KEY_MAX>
{
    /// A fresh (empty) manifest; identical to [`Manifest::new`].
    fn default() -> Self {
        Self::new()
    }
}

/// What [`Manifest::read_slot`] found.
enum Slot<const LEVELS: usize, const TABLES: usize, const KEY_MAX: usize> {
    /// A CRC-valid manifest with its sequence number.
    Valid(u64, Manifest<LEVELS, TABLES, KEY_MAX>),
    /// All zeros: never written.
    Blank,
    /// Non-zero but undecodable.
    Corrupt,
}

/// Bounds-checked little-endian reader over a byte slice.
struct Decoder<'a> {
    buf: &'a [u8],
    off: usize,
}

impl Decoder<'_> {
    fn bytes(&mut self, n: usize) -> Result<&[u8], ()> {
        let end = self.off.checked_add(n).ok_or(())?;
        let b = self.buf.get(self.off..end).ok_or(())?;
        self.off = end;
        Ok(b)
    }

    fn u16(&mut self) -> Result<u16, ()> {
        let b = self.bytes(2)?;
        Ok(u16::from_le_bytes([b[0], b[1]]))
    }

    fn u32(&mut self) -> Result<u32, ()> {
        let b = self.bytes(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn u64(&mut self) -> Result<u64, ()> {
        let b = self.bytes(8)?;
        Ok(u64::from_le_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }
}

/// Bounds-checked little-endian writer over a byte slice.
struct Encoder<'a, E> {
    buf: &'a mut [u8],
    off: usize,
    marker: PhantomData<E>,
}

impl<E> Encoder<'_, E> {
    fn bytes(&mut self, b: &[u8]) -> Result<(), Error<E>> {
        let end = self.off.checked_add(b.len()).ok_or(Error::NoSpace)?;
        if end > self.buf.len() {
            return Err(Error::NoSpace);
        }
        self.buf[self.off..end].copy_from_slice(b);
        self.off = end;
        Ok(())
    }

    fn u16(&mut self, v: u16) -> Result<(), Error<E>> {
        self.bytes(&v.to_le_bytes())
    }

    fn u32(&mut self, v: u32) -> Result<(), Error<E>> {
        self.bytes(&v.to_le_bytes())
    }

    fn u64(&mut self, v: u64) -> Result<(), Error<E>> {
        self.bytes(&v.to_le_bytes())
    }
}
