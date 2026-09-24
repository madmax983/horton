//! Crash-safe root pointer: the manifest.
//!
//! The manifest is stored as whole *copies*, two or more (see
//! [`ManifestLayout`]); each commit bumps `seq` and writes copy
//! `seq % copies`, then flushes the device. A copy is a run of blocks —
//! as many as the worst-case manifest needs ([`Manifest::max_blocks`]),
//! of which a commit writes only those its current contents fill. Every
//! block carries the commit's `seq` and its own CRC, so recovery accepts a
//! copy only when all of its blocks are intact *and* agree on `seq`: a
//! commit torn anywhere across its blocks leaves its copy invalid, and the
//! previous commit's copy wins.
//!
//! Block layout (all little-endian):
//!
//! ```text
//! magic: u64 = "hrtman05" | seq: u64 | index: u16 | count: u16
//!     | len: u32 | body[len] | crc32: u32
//! ```
//!
//! `crc32` covers `magic` through the end of the block's body chunk; the
//! rest of the block is zero padding. Every block but the last of a copy
//! carries a full chunk of `BLOCK - 28` body bytes. The body, concatenated
//! across the copy's `count` blocks, is:
//!
//! ```text
//! wal_head: u64 | next_table_id: u32 | flushed_seq: u64 | seq_high: u64
//!     | nlevels: u32
//! per level: count: u32 | count * tableref
//! tableref: id: u32 | first_block: u64 | block_count: u32
//!           | fk_len: u16 | fk_bytes[fk_len]
//!           | lk_len: u16 | lk_bytes[lk_len]
//!           | max_seq: u64 | min_seq: u64 | entry_count: u32 | rdel_blocks: u32
//! ```

use core::future::{Future, poll_fn};

use crate::crc::crc32;
use crate::device::BlockDevice;
use crate::error::Error;

/// Manifest block magic: ASCII "hrtman05".
///
/// The magic changes whenever the layout changes (pre-1.0 format policy:
/// no compatibility across minor versions). "hrtman01" was the v0.4.0
/// layout with fixed `KEY_MAX` key bounds; "hrtman02" was the v0.4.1
/// layout with length-prefixed key bounds; "hrtman03" is the v0.15 layout
/// with the per-table `rdel_blocks` count; "hrtman04" added the persisted
/// sequence floors `flushed_seq` and `seq_high`; "hrtman05" (v0.17) spreads
/// a copy over as many blocks as it needs. A foreign magic decodes as
/// [`Error::CorruptManifest`] — old bytes are rejected, never misparsed.
pub const MANIFEST_MAGIC: u64 = u64::from_le_bytes(*b"hrtman05");

/// Bytes of block header before a body chunk: magic, seq, index, count,
/// len.
const BLOCK_HEADER: usize = 24;

/// Bytes of a block's trailing CRC32.
const BLOCK_CRC: usize = 4;

/// Where the manifest's copies live. Each copy is a run of
/// [`Manifest::max_blocks`] blocks; commits rotate through the copies, so
/// every copy's blocks are erased once per `copies` commits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManifestLayout {
    first: u64,
    second: u64,
    /// Ring copies; 0 for the two-copy pair at `first` and `second`.
    ring: u32,
}

impl ManifestLayout {
    /// Two copies, starting at blocks `a` and `b` (the classic
    /// double-buffered pair; copy `seq % 2` is written).
    #[must_use]
    pub const fn pair(a: u64, b: u64) -> Self {
        Self {
            first: a,
            second: b,
            ring: 0,
        }
    }

    /// `copies` copies back to back from block `start`: copy `k` starts at
    /// `start + k * max_blocks`. More copies spread the manifest's erase
    /// wear — on NOR flash the manifest is otherwise the first region to
    /// wear out. Fewer than two copies are treated as two.
    #[must_use]
    pub const fn ring(start: u64, copies: u32) -> Self {
        Self {
            first: start,
            second: 0,
            ring: if copies < 2 { 2 } else { copies },
        }
    }

    /// Number of copies.
    #[must_use]
    pub const fn copies(&self) -> u32 {
        if self.ring == 0 { 2 } else { self.ring }
    }

    /// First block of copy `k`, for copies of `stride` blocks.
    #[must_use]
    pub const fn copy_start(&self, k: u32, stride: u64) -> u64 {
        if self.ring == 0 {
            if k.is_multiple_of(2) { self.first } else { self.second }
        } else {
            self.first + (k % self.ring) as u64 * stride
        }
    }
}

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

    /// The smallest key bound strictly after this one with no key of at
    /// most `KEY_MAX` bytes in between: every key `k` satisfies
    /// `k <= self` exactly when `k < successor`. `None` when this is the
    /// greatest possible key (`KEY_MAX` bytes of `0xFF`), or empty.
    ///
    /// Compaction splits its output at key boundaries and clips range
    /// tombstones to `[.., successor(last key))`, so each output's range
    /// ends exactly at its last key and the next output starts strictly
    /// after it — the outputs stay disjoint.
    #[must_use]
    pub fn successor(&self) -> Option<Self> {
        let len = usize::from(self.len);
        if len == 0 {
            return None;
        }
        let mut next = *self;
        if len < KEY_MAX {
            // `k || 0x00` is the immediate successor: nothing sorts between
            // a key and itself extended by a zero byte.
            next.bytes[len] = 0;
            next.len += 1;
            return Some(next);
        }
        // A full-length key has no longer extension: increment the last
        // byte that is not 0xFF and drop everything after it.
        let mut i = len;
        while i > 0 {
            i -= 1;
            if next.bytes[i] != 0xFF {
                next.bytes[i] += 1;
                next.bytes[i + 1..].fill(0);
                // `i < len <= 0xFFFF`: the narrowing is exact.
                next.len = u16::try_from(i + 1).ok()?;
                return Some(next);
            }
        }
        None
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
    /// Live lower bound: the smallest key the table answers for (includes
    /// range-tombstone bounds). Normally the table's smallest key; a
    /// compaction that has already moved the table's prefix into a newer
    /// output raises it past that prefix (see
    /// [`Manifest::narrow_table`]). Entries below it are dead: every
    /// reader skips them.
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
    /// Exact: every *live* key in the table is within
    /// `[first_key, last_key]`, so a key outside the bounds is definitely
    /// absent (or dead) and the table can be skipped without any I/O.
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
    /// and so on.
    pool: [[TableRef<KEY_MAX>; TABLES]; LEVELS],
    /// Live refs per level.
    counts: [usize; LEVELS],
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
        }
    }

    /// Table refs the pool holds across all levels.
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

    /// Pool entries still free.
    #[must_use]
    pub fn free_refs(&self) -> usize {
        Self::CAPACITY - self.live()
    }

    /// Inserts `tref` into `level` at pool index `pos`, shifting every
    /// later level's refs up by one.
    fn insert_at<E>(
        &mut self,
        pos: usize,
        level: usize,
        tref: TableRef<KEY_MAX>,
    ) -> Result<(), Error<E>> {
        let used = self.live();
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
        let used = self.live();
        let flat = self.flat_mut();
        flat.copy_within(s + pos + 1..used, s + pos);
        flat[used - 1] = TableRef::EMPTY;
        self.counts[level] -= 1;
        Ok(true)
    }

    /// Raises the live lower bound of table `id` at `level` to `first`:
    /// every entry and range-tombstone piece below `first` becomes dead to
    /// all readers. Compaction commits its outputs a table at a time; once
    /// an output holding every input key up to some frontier is live, each
    /// input still straddling the frontier is narrowed past it, so the
    /// merged prefix is read only from the output. Returns `false` when
    /// the table is not at `level`.
    ///
    /// `first` must not exceed the table's `last_key` (the caller narrows
    /// only tables straddling the frontier), so a deeper level stays sorted
    /// and disjoint.
    ///
    /// # Errors
    ///
    /// [`Error::NoSpace`] when `level` is out of range.
    pub fn narrow_table<E>(
        &mut self,
        level: usize,
        id: u32,
        first: KeyBound<KEY_MAX>,
    ) -> Result<bool, Error<E>> {
        if level >= LEVELS {
            return Err(Error::NoSpace);
        }
        let s = self.start(level);
        let n = self.counts[level];
        let Some(t) = self.flat_mut()[s..s + n].iter_mut().find(|t| t.id == id) else {
            return Ok(false);
        };
        debug_assert!(first.as_slice() <= t.last_key.as_slice());
        if first.as_slice() > t.first_key.as_slice() {
            t.first_key = first;
        }
        Ok(true)
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

    /// Body bytes a block carries: the block minus its header and CRC.
    const fn chunk<const BLOCK: usize>() -> usize {
        BLOCK - BLOCK_HEADER - BLOCK_CRC
    }

    /// The largest body any manifest of this shape can encode to: every
    /// pool entry in use, every key bound at `KEY_MAX` bytes.
    const MAX_BODY: usize = 8 + 4 + 8 + 8 + 4 + 4 * LEVELS + Self::CAPACITY * (44 + 2 * KEY_MAX);

    /// Blocks one copy of this manifest shape can need, at most: the size
    /// of every copy in a [`ManifestLayout`]. A compile-time function of
    /// `LEVELS`, `TABLES`, `KEY_MAX`, and `BLOCK`.
    #[must_use]
    pub const fn max_blocks<const BLOCK: usize>() -> u64 {
        Self::MAX_BODY.div_ceil(Self::chunk::<BLOCK>()) as u64
    }

    /// Body bytes this manifest encodes to.
    #[must_use]
    pub fn body_len(&self) -> usize {
        let refs: usize = self
            .tables()
            .iter()
            .map(|t| 44 + usize::from(t.first_key.len) + usize::from(t.last_key.len))
            .sum();
        8 + 4 + 8 + 8 + 4 + 4 * LEVELS + refs
    }

    /// Blocks this manifest's copy occupies now.
    #[must_use]
    pub fn encoded_blocks<const BLOCK: usize>(&self) -> usize {
        self.body_len().div_ceil(Self::chunk::<BLOCK>()).max(1)
    }

    /// Writes the body to `w`, which keeps only the bytes of one block's
    /// chunk.
    fn write_body(&self, w: &mut Window<'_>) {
        w.u64(self.wal_head);
        w.u32(self.next_table_id);
        w.u64(self.flushed_seq);
        w.u64(self.seq_high);
        // `LEVELS` and each level's count are bounded by the pool, far
        // below `u32::MAX`.
        #[allow(clippy::cast_possible_truncation)]
        w.u32(LEVELS as u32);
        for li in 0..LEVELS {
            let level = self.level(li).unwrap_or(&[]);
            #[allow(clippy::cast_possible_truncation)]
            w.u32(level.len() as u32);
            for tref in level {
                w.u32(tref.id);
                w.u64(tref.first_block);
                w.u32(tref.block_count);
                w.u16(tref.first_key.len);
                w.bytes(tref.first_key.as_slice());
                w.u16(tref.last_key.len);
                w.bytes(tref.last_key.as_slice());
                w.u64(tref.max_seq);
                w.u64(tref.min_seq);
                w.u32(tref.entry_count);
                w.u32(tref.rdel_blocks);
            }
        }
    }

    /// Encodes block `index` of this manifest's copy into `out`: header,
    /// the body's `index`-th chunk, CRC, zero padding.
    ///
    /// # Errors
    ///
    /// [`Error::NoSpace`] when `index` is past the copy's last block.
    pub fn encode_block<E, const BLOCK: usize>(
        &self,
        index: usize,
        out: &mut [u8; BLOCK],
    ) -> Result<(), Error<E>> {
        let chunk = Self::chunk::<BLOCK>();
        let count = self.encoded_blocks::<BLOCK>();
        if index >= count {
            return Err(Error::NoSpace);
        }
        let lo = index * chunk;
        let len = self.body_len().saturating_sub(lo).min(chunk);
        out.fill(0);
        {
            let mut w = Window {
                out: &mut out[BLOCK_HEADER..BLOCK_HEADER + len],
                pos: 0,
                lo,
            };
            self.write_body(&mut w);
        }
        out[0..8].copy_from_slice(&MANIFEST_MAGIC.to_le_bytes());
        out[8..16].copy_from_slice(&self.seq.to_le_bytes());
        let (i16, c16, l32) = (
            u16::try_from(index).map_err(|_| Error::NoSpace)?,
            u16::try_from(count).map_err(|_| Error::NoSpace)?,
            u32::try_from(len).map_err(|_| Error::NoSpace)?,
        );
        out[16..18].copy_from_slice(&i16.to_le_bytes());
        out[18..20].copy_from_slice(&c16.to_le_bytes());
        out[20..24].copy_from_slice(&l32.to_le_bytes());
        let crc_end = BLOCK_HEADER + len;
        let crc = crc32(&out[..crc_end]);
        out[crc_end..crc_end + BLOCK_CRC].copy_from_slice(&crc.to_le_bytes());
        Ok(())
    }

    /// Serializes a manifest that fits one block into `out` (a one-block
    /// copy). Tools and tests use it; commits go through
    /// [`commit_to`](Self::commit_to), which spreads larger manifests over
    /// several blocks.
    ///
    /// # Errors
    ///
    /// [`Error::NoSpace`] when the manifest needs more than one block.
    pub fn encode<E, const BLOCK: usize>(&self, out: &mut [u8; BLOCK]) -> Result<(), Error<E>> {
        if self.encoded_blocks::<BLOCK>() > 1 {
            return Err(Error::NoSpace);
        }
        self.encode_block(0, out)
    }

    /// Decodes a one-block copy (the inverse of [`encode`](Self::encode)).
    /// Any structural problem yields [`Error::CorruptManifest`] — never a
    /// partial manifest.
    ///
    /// # Errors
    ///
    /// [`Error::CorruptManifest`] on any inconsistency, including a copy
    /// that spans more than one block.
    pub fn decode<E, const BLOCK: usize>(buf: &[u8; BLOCK]) -> Result<Self, Error<E>> {
        let head = check_block::<BLOCK>(buf).ok_or(Error::CorruptManifest)?;
        if head.index != 0 || head.count != 1 {
            return Err(Error::CorruptManifest);
        }
        let mut src = SliceSource {
            body: &buf[BLOCK_HEADER..BLOCK_HEADER + head.len],
        };
        // A slice source never waits: one poll runs the decode to the end.
        let decoded = {
            let mut fut = core::pin::pin!(Self::decode_body::<E, _>(&mut src));
            let mut cx = core::task::Context::from_waker(core::task::Waker::noop());
            match fut.as_mut().poll(&mut cx) {
                core::task::Poll::Ready(d) => d,
                core::task::Poll::Pending => Err(Error::CorruptManifest),
            }
        };
        let mut out = decoded?;
        if !src.body.is_empty() {
            return Err(Error::CorruptManifest);
        }
        out.seq = head.seq;
        Ok(out)
    }

    /// Parses a body from `src`: [`Error::CorruptManifest`] on any
    /// inconsistency.
    async fn decode_body<E, S: BodySource<E>>(src: &mut S) -> Result<Self, Error<E>> {
        let corrupt = || Error::CorruptManifest;
        let mut out = Self::new();
        out.wal_head = src.u64().await?;
        out.next_table_id = src.u32().await?;
        out.flushed_seq = src.u64().await?;
        out.seq_high = src.u64().await?;
        if usize::try_from(src.u32().await?).map_err(|_| corrupt())? != LEVELS {
            return Err(corrupt());
        }
        let mut total = 0usize;
        for li in 0..LEVELS {
            let count = usize::try_from(src.u32().await?).map_err(|_| corrupt())?;
            // Level 0 is capped at TABLES; the whole tree at the pool.
            if (li == 0 && count > TABLES) || count > Self::CAPACITY - total {
                return Err(corrupt());
            }
            for i in 0..count {
                out.flat_mut()[total + i] = Self::decode_tableref(src).await?;
            }
            out.counts[li] = count;
            total += count;
        }
        Ok(out)
    }

    /// Decodes one table ref.
    async fn decode_tableref<E, S: BodySource<E>>(
        src: &mut S,
    ) -> Result<TableRef<KEY_MAX>, Error<E>> {
        Ok(TableRef {
            id: src.u32().await?,
            first_block: src.u64().await?,
            block_count: src.u32().await?,
            first_key: Self::decode_bound(src).await?,
            last_key: Self::decode_bound(src).await?,
            max_seq: src.u64().await?,
            min_seq: src.u64().await?,
            entry_count: src.u32().await?,
            rdel_blocks: src.u32().await?,
        })
    }

    /// Decodes one key bound.
    async fn decode_bound<E, S: BodySource<E>>(src: &mut S) -> Result<KeyBound<KEY_MAX>, Error<E>> {
        let len = src.u16().await?;
        let n = usize::from(len);
        if n > KEY_MAX {
            return Err(Error::CorruptManifest);
        }
        let mut bytes = [0u8; KEY_MAX];
        src.take(&mut bytes[..n]).await?;
        Ok(KeyBound { len, bytes })
    }

    /// Recovers the two-copy manifest at blocks `a` and `b`: shorthand for
    /// [`recover_from`](Self::recover_from) with [`ManifestLayout::pair`].
    ///
    /// # Errors
    ///
    /// As [`recover_from`](Self::recover_from).
    pub async fn recover<D: BlockDevice, const BLOCK: usize>(
        device: &mut D,
        scratch: &mut [u8; BLOCK],
        a: u64,
        b: u64,
    ) -> Result<(Self, bool), Error<D::Error>> {
        Self::recover_from(device, scratch, ManifestLayout::pair(a, b)).await
    }

    /// Reads every copy and returns the newest intact one.
    ///
    /// A copy is intact when all of its blocks carry valid CRCs, the same
    /// `seq`, and a consistent index and count. Copies that were never
    /// written are blank — all zeros on a zeroed device, all `0xFF` on
    /// erased NOR flash. No intact copy but a blank one means a fresh
    /// device (`true`): either nothing was ever committed, or the *first*
    /// commit was torn, and then the WAL (whose head is still `wal_start`)
    /// replays everything — no data loss, the orphaned blocks are simply
    /// never referenced.
    ///
    /// # Errors
    ///
    /// [`Error::CorruptManifest`] when no copy is intact or blank, or
    /// [`Error::Device`] on I/O failure.
    pub async fn recover_from<D: BlockDevice, const BLOCK: usize>(
        device: &mut D,
        scratch: &mut [u8; BLOCK],
        layout: ManifestLayout,
    ) -> Result<(Self, bool), Error<D::Error>> {
        let stride = Self::max_blocks::<BLOCK>();
        let mut best: Option<(u64, u64, usize)> = None;
        let mut blank = false;
        for k in 0..layout.copies() {
            let start = layout.copy_start(k, stride);
            match Self::probe_copy(&*device, scratch, start, stride).await? {
                Probe::Intact { seq, count } => {
                    if best.is_none_or(|(s, _, _)| seq > s) {
                        best = Some((seq, start, count));
                    }
                }
                Probe::Blank => blank = true,
                Probe::Corrupt => {}
            }
        }
        let Some((seq, start, count)) = best else {
            return if blank {
                Ok((Self::new(), true))
            } else {
                Err(Error::CorruptManifest)
            };
        };
        let mut src = BlockSource::<D, BLOCK> {
            device: &*device,
            scratch,
            start,
            count,
            next: 0,
            off: 0,
            len: 0,
        };
        let mut out = Self::decode_body(&mut src).await?;
        if src.next != count || src.off != src.len {
            return Err(Error::CorruptManifest);
        }
        out.seq = seq;
        Ok((out, false))
    }

    /// Checks one copy: blank, intact (with its `seq` and block count), or
    /// corrupt.
    async fn probe_copy<D: BlockDevice, const BLOCK: usize>(
        device: &D,
        scratch: &mut [u8; BLOCK],
        start: u64,
        stride: u64,
    ) -> Result<Probe, Error<D::Error>> {
        read_block(device, start, scratch).await?;
        if scratch.iter().all(|&b| b == 0) || scratch.iter().all(|&b| b == 0xFF) {
            return Ok(Probe::Blank);
        }
        let Some(head) = check_block::<BLOCK>(scratch) else {
            return Ok(Probe::Corrupt);
        };
        if head.index != 0 || head.count == 0 || head.count as u64 > stride {
            return Ok(Probe::Corrupt);
        }
        let chunk = Self::chunk::<BLOCK>();
        if head.count > 1 && head.len != chunk {
            return Ok(Probe::Corrupt);
        }
        for i in 1..head.count {
            read_block(device, start + i as u64, scratch).await?;
            let Some(h) = check_block::<BLOCK>(scratch) else {
                return Ok(Probe::Corrupt);
            };
            let last = i + 1 == head.count;
            if h.seq != head.seq
                || h.index != i
                || h.count != head.count
                || (!last && h.len != chunk)
            {
                return Ok(Probe::Corrupt);
            }
        }
        Ok(Probe::Intact {
            seq: head.seq,
            count: head.count,
        })
    }

    /// Commits to the two-copy manifest at blocks `a` and `b`: shorthand
    /// for [`commit_to`](Self::commit_to) with [`ManifestLayout::pair`].
    ///
    /// # Errors
    ///
    /// As [`commit_to`](Self::commit_to).
    pub async fn commit<D: BlockDevice, const BLOCK: usize>(
        &mut self,
        device: &mut D,
        scratch: &mut [u8; BLOCK],
        a: u64,
        b: u64,
    ) -> Result<(), Error<D::Error>> {
        self.commit_to(device, scratch, ManifestLayout::pair(a, b))
            .await
    }

    /// Commits: bumps `seq`, writes copy `seq % copies` block by block,
    /// then flushes the device. Only the blocks the manifest currently
    /// fills are written.
    ///
    /// # Errors
    ///
    /// [`Error::NoSpace`] when the sequence counter overflows, or
    /// [`Error::Device`] on I/O failure.
    pub async fn commit_to<D: BlockDevice, const BLOCK: usize>(
        &mut self,
        device: &mut D,
        scratch: &mut [u8; BLOCK],
        layout: ManifestLayout,
    ) -> Result<(), Error<D::Error>> {
        self.seq = self.seq.checked_add(1).ok_or(Error::NoSpace)?;
        let copies = u64::from(layout.copies());
        // `seq % copies < copies <= u32::MAX`: the narrowing is exact.
        #[allow(clippy::cast_possible_truncation)]
        let k = (self.seq % copies) as u32;
        let start = layout.copy_start(k, Self::max_blocks::<BLOCK>());
        for i in 0..self.encoded_blocks::<BLOCK>() {
            self.encode_block::<D::Error, BLOCK>(i, scratch)?;
            let id = start + i as u64;
            poll_fn(|cx| device.poll_write_block(cx, id, scratch))
                .await
                .map_err(Error::Device)?;
        }
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

/// What [`Manifest::probe_copy`] found.
enum Probe {
    /// Every block intact and agreeing.
    Intact { seq: u64, count: usize },
    /// Never written.
    Blank,
    /// Written but not intact (torn or damaged).
    Corrupt,
}

/// A manifest block's parsed header.
struct BlockHead {
    seq: u64,
    index: usize,
    count: usize,
    len: usize,
}

/// Validates one manifest block — magic, chunk length, CRC — and returns
/// its header.
fn check_block<const BLOCK: usize>(buf: &[u8; BLOCK]) -> Option<BlockHead> {
    let u64_at = |o: usize| {
        buf.get(o..o + 8)
            .and_then(|b| b.try_into().ok())
            .map(u64::from_le_bytes)
    };
    if u64_at(0)? != MANIFEST_MAGIC {
        return None;
    }
    let seq = u64_at(8)?;
    let index = usize::from(u16::from_le_bytes(buf[16..18].try_into().ok()?));
    let count = usize::from(u16::from_le_bytes(buf[18..20].try_into().ok()?));
    let len = usize::try_from(u32::from_le_bytes(buf[20..24].try_into().ok()?)).ok()?;
    let crc_end = BLOCK_HEADER.checked_add(len)?;
    if crc_end.checked_add(BLOCK_CRC)? > BLOCK {
        return None;
    }
    let stored = u32::from_le_bytes(buf[crc_end..crc_end + BLOCK_CRC].try_into().ok()?);
    (crc32(&buf[..crc_end]) == stored).then_some(BlockHead {
        seq,
        index,
        count,
        len,
    })
}

/// Reads one block.
async fn read_block<D: BlockDevice, const BLOCK: usize>(
    device: &D,
    id: u64,
    buf: &mut [u8; BLOCK],
) -> Result<(), Error<D::Error>> {
    poll_fn(|cx| device.poll_read_block(cx, id, buf))
        .await
        .map_err(Error::Device)
}

/// A body writer that keeps only one block's chunk: bytes at body offsets
/// `[lo, lo + out.len())` land in `out`, the rest are counted and dropped.
struct Window<'a> {
    out: &'a mut [u8],
    /// Body offset of the next byte.
    pos: usize,
    lo: usize,
}

impl Window<'_> {
    fn bytes(&mut self, b: &[u8]) {
        let (start, end) = (self.pos, self.pos + b.len());
        let hi = self.lo + self.out.len();
        let (from, to) = (start.max(self.lo), end.min(hi));
        if from < to {
            self.out[from - self.lo..to - self.lo].copy_from_slice(&b[from - start..to - start]);
        }
        self.pos = end;
    }

    fn u16(&mut self, v: u16) {
        self.bytes(&v.to_le_bytes());
    }

    fn u32(&mut self, v: u32) {
        self.bytes(&v.to_le_bytes());
    }

    fn u64(&mut self, v: u64) {
        self.bytes(&v.to_le_bytes());
    }
}

/// Where a body is parsed from. Private, statically dispatched: the async
/// methods let one parser serve both a slice and a stream of device
/// blocks.
trait BodySource<E> {
    /// Fills `dst` from the body; [`Error::CorruptManifest`] when it runs
    /// out.
    async fn take(&mut self, dst: &mut [u8]) -> Result<(), Error<E>>;

    async fn u16(&mut self) -> Result<u16, Error<E>> {
        let mut b = [0u8; 2];
        self.take(&mut b).await?;
        Ok(u16::from_le_bytes(b))
    }

    async fn u32(&mut self) -> Result<u32, Error<E>> {
        let mut b = [0u8; 4];
        self.take(&mut b).await?;
        Ok(u32::from_le_bytes(b))
    }

    async fn u64(&mut self) -> Result<u64, Error<E>> {
        let mut b = [0u8; 8];
        self.take(&mut b).await?;
        Ok(u64::from_le_bytes(b))
    }
}

/// A body held in one slice.
struct SliceSource<'a> {
    body: &'a [u8],
}

impl<E> BodySource<E> for SliceSource<'_> {
    async fn take(&mut self, dst: &mut [u8]) -> Result<(), Error<E>> {
        let (head, rest) = self
            .body
            .split_at_checked(dst.len())
            .ok_or(Error::CorruptManifest)?;
        dst.copy_from_slice(head);
        self.body = rest;
        Ok(())
    }
}

/// A body streamed from a copy's blocks, one block at a time through the
/// caller's scratch buffer.
struct BlockSource<'a, D, const BLOCK: usize> {
    device: &'a D,
    scratch: &'a mut [u8; BLOCK],
    start: u64,
    count: usize,
    /// Next block of the copy to load.
    next: usize,
    /// Consumed bytes of the loaded block's chunk, and its length.
    off: usize,
    len: usize,
}

impl<D: BlockDevice, const BLOCK: usize> BodySource<D::Error> for BlockSource<'_, D, BLOCK> {
    async fn take(&mut self, dst: &mut [u8]) -> Result<(), Error<D::Error>> {
        let mut done = 0;
        while done < dst.len() {
            if self.off == self.len {
                if self.next == self.count {
                    return Err(Error::CorruptManifest);
                }
                let id = self.start + self.next as u64;
                read_block(self.device, id, self.scratch).await?;
                // Re-validated: the probe read these blocks, but a read
                // is a read.
                let head = check_block::<BLOCK>(self.scratch).ok_or(Error::CorruptManifest)?;
                if head.index != self.next {
                    return Err(Error::CorruptManifest);
                }
                self.next += 1;
                self.off = 0;
                self.len = head.len;
                continue;
            }
            let n = (self.len - self.off).min(dst.len() - done);
            let from = BLOCK_HEADER + self.off;
            dst[done..done + n].copy_from_slice(&self.scratch[from..from + n]);
            self.off += n;
            done += n;
        }
        Ok(())
    }
}
