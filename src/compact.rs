//! Bounded leveled compaction: the caller-driven merge engine.
//!
//! When a level fills (`TABLES` tables), [`Db::compact_step`] selects the
//! deepest full level's job: all of L0, or the oldest table of a deeper
//! level, plus the tables of the level below whose key ranges overlap, as
//! one compaction job. The merge is incremental: one call pushes merged
//! entries into the output table until an output block seals (or the merge
//! exhausts), reporting [`Progress::More`] while work remains. A final
//! manifest commit swaps the input tables for the output table atomically.
//!
//! The caller owns the [`Compaction`] scratch — the output table's staging
//! buffers plus one read cursor per input table, at most
//! [`COMPACTION_KMAX`] tables per job. Nothing is allocated. Dropping the
//! scratch mid-job is crash-safe: partial output tables are invisible until
//! the manifest commit, so they become orphans reclaimed by the next
//! `open()` sweep, while the input tables stay referenced; a fresh scratch
//! simply selects the job again.
//!
//! Compaction preserves the read path's visibility rule: each key keeps
//! the newest version (the live view) plus the newest version at or below
//! each live snapshot's watermark — older versions are dead to every reader
//! and are not emitted. A bottommost tombstone older than every live
//! snapshot drops the whole key: nothing below can hide an older version,
//! and deletion is observationally identical to absence there.

use core::future::poll_fn;

use crate::db::MAX_SNAPSHOTS;
use crate::device::BlockDevice;
use crate::error::Error;
use crate::manifest::{KeyBound, TableRef};
use crate::sstable::{self, PushOutcome, SstEntry, TableWriter};

/// Maximum tables merged in one compaction job (spec `KMAX = 8`).
pub const COMPACTION_KMAX: usize = 8;

/// What [`Db::compact_step`](crate::db::Db::compact_step) accomplished.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Progress {
    /// No compaction was needed, or the selected job just committed.
    Done,
    /// The job made bounded progress (one output block sealed); call again.
    More,
}

/// Merge state of the key currently being compacted. A sealed output block
/// may interrupt a key mid-versions; [`KeyState`] plus the `served` flags
/// resume it exactly. `Dropping` implies the key is active: the whole key
/// is being discarded (bottommost tombstone older than every live
/// snapshot), so the two booleans it replaces could never disagree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeyState {
    /// No key is being merged (between keys, or the job just started).
    Idle,
    /// Merging versions of `key`; thresholds in `served` are being filled.
    Merging,
    /// The active key's versions are all dropped.
    Dropping,
}

/// Caller-owned scratch driving one compaction job at a time.
///
/// Create it once and hand it to every
/// [`compact_step`](crate::db::Db::compact_step) call; it is reusable
/// across jobs — a finished job leaves it idle, and the next call selects a
/// fresh job when L0 is full again. The type parameters must match the
/// [`Db`](crate::db::Db) it drives.
pub struct Compaction<
    const BLOCK: usize,
    const KEY_MAX: usize,
    const VAL_MAX: usize,
    const BLOOM_BYTES: usize,
> {
    pub(crate) state: State,
    pub(crate) inputs: [Input<KEY_MAX>; COMPACTION_KMAX],
    pub(crate) n_inputs: usize,
    pub(crate) target_level: usize,
    /// True when the output level is the bottommost one holding the merged
    /// key range: a tombstone there shadows nothing below and may be
    /// dropped — unless a live snapshot could still observe it (see
    /// `oldest_snapshot`).
    pub(crate) bottommost: bool,
    /// Live snapshot watermarks at select time, sorted descending. The
    /// merge's per-key keep-set is the newest version (the live view) plus
    /// the newest version at or below each watermark; older versions are
    /// dead to every reader and are not emitted.
    pub(crate) snapshots: [u64; MAX_SNAPSHOTS],
    /// How many entries of [`snapshots`](Self::snapshots) are live.
    pub(crate) n_snapshots: usize,
    /// Oldest live snapshot sequence at select time (`u64::MAX` when no
    /// snapshot is live). A bottommost tombstone is dropped only when its
    /// seq is below this floor: then every snapshot's visible version of
    /// the key is the tombstone itself, so the keep-set is just it, and
    /// deletion is observationally identical to absence. Captured at
    /// `compact_select`; a snapshot taken mid-compaction always has
    /// `seq >=` every version being merged, so it can never observe the
    /// difference.
    pub(crate) oldest_snapshot: u64,
    pub(crate) out_base: u64,
    pub(crate) out_blocks: u64,
    pub(crate) out_len: usize,
    pub(crate) from_free: bool,
    pub(crate) writer: TableWriter<BLOCK, BLOOM_BYTES, KEY_MAX>,
    pub(crate) cursors: [Cursor<BLOCK, KEY_MAX, VAL_MAX>; COMPACTION_KMAX],
    /// The key currently being merged: a sealed output block may interrupt
    /// a key mid-versions, and matching on these bytes resumes it exactly
    /// (thresholds already served are not served twice).
    key: [u8; KEY_MAX],
    key_len: usize,
    key_state: KeyState,
    /// Thresholds served for [`key`](Self::key): index 0 is the live view
    /// (`u64::MAX`), indices `1..=n_snapshots` are the snapshots.
    served: [bool; MAX_SNAPSHOTS + 1],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum State {
    Idle,
    Merging,
}

/// One input table: its level and manifest reference.
#[derive(Clone, Copy)]
pub(crate) struct Input<const KEY_MAX: usize> {
    pub(crate) level: usize,
    pub(crate) tref: TableRef<KEY_MAX>,
}

impl<const KEY_MAX: usize> Input<KEY_MAX> {
    pub(crate) const EMPTY: Self = Self {
        level: 0,
        tref: TableRef::EMPTY,
    };
}

/// Read cursor over one input table's data blocks: the current block plus
/// the parsed head entry. Owns its buffers so the k-way merge never
/// allocates.
#[derive(Clone, Copy)]
pub(crate) struct Cursor<const BLOCK: usize, const KEY_MAX: usize, const VAL_MAX: usize> {
    first_block: u64,
    data_blocks: u64,
    block_idx: u64,
    block_id: u64,
    block: [u8; BLOCK],
    /// Start of the restart trailer: entries end at the first zero padding
    /// before this offset (the writer zero-fills `[payload..rstart)`).
    rstart: usize,
    next: usize,
    pub(crate) live: bool,
    key: [u8; KEY_MAX],
    key_len: usize,
    val: [u8; VAL_MAX],
    val_len: usize,
    seq: u64,
    tombstone: bool,
}

impl<const BLOCK: usize, const KEY_MAX: usize, const VAL_MAX: usize>
    Cursor<BLOCK, KEY_MAX, VAL_MAX>
{
    pub(crate) const EMPTY: Self = Self {
        first_block: 0,
        data_blocks: 0,
        block_idx: 0,
        block_id: 0,
        block: [0u8; BLOCK],
        rstart: 0,
        next: 0,
        live: false,
        key: [0u8; KEY_MAX],
        key_len: 0,
        val: [0u8; VAL_MAX],
        val_len: 0,
        seq: 0,
        tombstone: false,
    };
}

/// What one [`Compaction::merge_step`] did.
pub(crate) enum MergeOutcome {
    /// One output block sealed; the merge has more entries.
    More,
    /// Every input is exhausted; the caller should commit.
    Exhausted,
}

impl<const BLOCK: usize, const KEY_MAX: usize, const VAL_MAX: usize, const BLOOM_BYTES: usize>
    Default for Compaction<BLOCK, KEY_MAX, VAL_MAX, BLOOM_BYTES>
{
    fn default() -> Self {
        Self::new()
    }
}

impl<const BLOCK: usize, const KEY_MAX: usize, const VAL_MAX: usize, const BLOOM_BYTES: usize>
    Compaction<BLOCK, KEY_MAX, VAL_MAX, BLOOM_BYTES>
{
    /// Fresh idle scratch.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            state: State::Idle,
            inputs: [Input::EMPTY; COMPACTION_KMAX],
            n_inputs: 0,
            target_level: 0,
            bottommost: false,
            snapshots: [0u64; MAX_SNAPSHOTS],
            n_snapshots: 0,
            oldest_snapshot: u64::MAX,
            out_base: 0,
            out_blocks: 0,
            out_len: 0,
            from_free: false,
            writer: TableWriter::new(0, 0),
            cursors: [Cursor::EMPTY; COMPACTION_KMAX],
            key: [0u8; KEY_MAX],
            key_len: 0,
            key_state: KeyState::Idle,
            served: [false; MAX_SNAPSHOTS + 1],
        }
    }

    /// Abandons any in-progress job. Partial output blocks were never
    /// claimed or referenced, so they are simply free; the inputs are still
    /// in the manifest and a later select will redo the job.
    pub(crate) const fn reset(&mut self) {
        self.state = State::Idle;
        self.n_inputs = 0;
        self.key_state = KeyState::Idle;
    }

    /// One bounded merge quantum: pushes merged entries into the output
    /// table until one output block seals, or every input is exhausted.
    ///
    /// Each key's versions surface newest-first, one per loop iteration:
    /// the iteration's head (highest sequence among the cursors tied on
    /// the minimum key) is emitted exactly when an unserved threshold —
    /// the live view, then each snapshot, descending — covers it. That
    /// emission then serves every threshold the version satisfies, which
    /// is precisely the keep-set: the newest version plus the newest
    /// version at or below each snapshot. A sealed block may interrupt a
    /// key mid-versions; the per-key state resumes it exactly.
    ///
    /// # Errors
    ///
    /// [`Error::CorruptBlock`] on a torn input block (compaction must never
    /// silently drop entries), [`Error::NoSpace`] when an entry cannot fit
    /// in an empty output block, or [`Error::Device`] on I/O failure.
    pub(crate) async fn merge_step<D: BlockDevice>(
        &mut self,
        device: &mut D,
    ) -> Result<MergeOutcome, Error<D::Error>> {
        loop {
            // The live cursor on the minimum key. KMAX = 8, so a linear
            // scan is trivially bounded — and unlike a heap it cannot hold
            // stale entries for exhausted cursors.
            let mut best = COMPACTION_KMAX;
            for ci in 0..self.n_inputs {
                if !self.cursors[ci].live {
                    continue;
                }
                if best == COMPACTION_KMAX {
                    best = ci;
                    continue;
                }
                if self.cursors[ci].key[..self.cursors[ci].key_len]
                    < self.cursors[best].key[..self.cursors[best].key_len]
                {
                    best = ci;
                }
            }
            if best == COMPACTION_KMAX {
                self.key_state = KeyState::Idle;
                return Ok(MergeOutcome::Exhausted);
            }
            // This iteration's head: the highest sequence among the cursors
            // tied on the minimum key. Each cursor sits at its table's
            // version-run start and runs are newest-first, so the head is
            // the newest version not yet processed for this key.
            let mut head = best;
            for ci in 0..self.n_inputs {
                if !self.cursors[ci].live || ci == best {
                    continue;
                }
                let tied = self.cursors[ci].key[..self.cursors[ci].key_len]
                    == self.cursors[best].key[..self.cursors[best].key_len];
                if tied && self.cursors[ci].seq > self.cursors[head].seq {
                    head = ci;
                }
            }
            // New key: (re)start the per-key threshold state. After a
            // mid-key seal the bytes match and the served flags resume the
            // key exactly where it stopped.
            let hkey_len = self.cursors[best].key_len;
            if self.key_state == KeyState::Idle
                || self.key_len != hkey_len
                || self.key[..hkey_len] != self.cursors[best].key[..hkey_len]
            {
                self.key[..hkey_len].copy_from_slice(&self.cursors[best].key[..hkey_len]);
                self.key_len = hkey_len;
                self.key_state = KeyState::Merging;
                self.served = [false; MAX_SNAPSHOTS + 1];
                // Bottommost tombstone drop: the head is the key's newest
                // version. Dropping the whole key is safe exactly when the
                // tombstone predates every live snapshot — then each
                // snapshot's visible version is the tombstone itself, the
                // keep-set is just it, and deletion is observationally
                // identical to absence.
                let c = &self.cursors[head];
                if self.bottommost && c.tombstone && c.seq < self.oldest_snapshot {
                    self.key_state = KeyState::Dropping;
                }
            }
            let (seq, tombstone, val_len) = {
                let c = &self.cursors[head];
                (c.seq, c.tombstone, c.val_len)
            };
            // The head is emitted when some unserved threshold covers it:
            // threshold 0 is the live view, the rest are the snapshots.
            let mut emit = false;
            if self.key_state != KeyState::Dropping {
                let mut ti = 0;
                while ti < 1 + self.n_snapshots {
                    let th = if ti == 0 {
                        u64::MAX
                    } else {
                        self.snapshots[ti - 1]
                    };
                    if !self.served[ti] && seq <= th {
                        emit = true;
                        break;
                    }
                    ti += 1;
                }
            }
            if emit {
                let sealed = {
                    let c = &self.cursors[head];
                    let e = SstEntry {
                        key: &c.key[..c.key_len],
                        val: &c.val[..val_len],
                        seq,
                        tombstone,
                    };
                    self.writer.push(device, e).await?
                };
                // The emitted version is the newest at or below every
                // threshold it satisfies (versions surface newest-first),
                // so it serves all of them at once.
                let mut ti = 0;
                while ti < 1 + self.n_snapshots {
                    let th = if ti == 0 {
                        u64::MAX
                    } else {
                        self.snapshots[ti - 1]
                    };
                    if seq <= th {
                        self.served[ti] = true;
                    }
                    ti += 1;
                }
                advance_cursor(&*device, &mut self.cursors[head]).await?;
                if sealed == PushOutcome::BlockSealed {
                    return Ok(MergeOutcome::More);
                }
            } else {
                // Skipped (or the key is dropped): this version serves no
                // threshold. Only the head advances — a tied cursor parked
                // on an older version may still serve a smaller threshold
                // on a later iteration.
                advance_cursor(&*device, &mut self.cursors[head]).await?;
            }
        }
    }
}

/// True when the two key ranges share at least one key.
pub(crate) fn ranges_overlap<const KEY_MAX: usize>(
    a_first: KeyBound<KEY_MAX>,
    a_last: KeyBound<KEY_MAX>,
    b_first: KeyBound<KEY_MAX>,
    b_last: KeyBound<KEY_MAX>,
) -> bool {
    a_first.as_slice() <= b_last.as_slice() && b_first.as_slice() <= a_last.as_slice()
}

/// Reads one block; the closure-free form keeps the borrow checker happy
/// across the `.await`.
async fn read_block_into<D: BlockDevice, const BLOCK: usize>(
    device: &D,
    id: u64,
    block: &mut [u8; BLOCK],
) -> Result<(), Error<D::Error>> {
    poll_fn(|cx| device.poll_read_block(cx, id, block))
        .await
        .map_err(Error::Device)
}

/// Loads data block `idx` of the cursor's table: reads, CRC-checks, and
/// locates the entries end.
async fn read_data_block<
    D: BlockDevice,
    const BLOCK: usize,
    const KEY_MAX: usize,
    const VAL_MAX: usize,
>(
    device: &D,
    cur: &mut Cursor<BLOCK, KEY_MAX, VAL_MAX>,
    idx: u64,
) -> Result<(), Error<D::Error>> {
    let id = cur
        .first_block
        .checked_add(idx)
        .ok_or(Error::CorruptBlock {
            id: cur.first_block,
        })?;
    read_block_into(device, id, &mut cur.block).await?;
    sstable::check_block_crc(&cur.block, id)?;
    let rstart = sstable::data_entries_end(&cur.block, id)?;
    if rstart == 0 {
        // A data block always carries at least one entry.
        return Err(Error::CorruptBlock { id });
    }
    cur.block_id = id;
    cur.block_idx = idx;
    cur.rstart = rstart;
    Ok(())
}

/// Parses the entry at `off` into the cursor's head buffers. Returns `true`
/// when parked on an entry, `false` at the writer's zero padding (end of
/// entries in this block — the same convention as the v0.3 lookup scan:
/// `data_entry_parse` rejects a zero `key_len`, which only padding has).
/// Anything else unparseable is [`Error::CorruptBlock`].
fn parse_head_at<E, const BLOCK: usize, const KEY_MAX: usize, const VAL_MAX: usize>(
    cur: &mut Cursor<BLOCK, KEY_MAX, VAL_MAX>,
    off: usize,
) -> Result<bool, Error<E>> {
    let Ok(entry) =
        sstable::parse_data_entry::<E, BLOCK>(&cur.block, off, cur.rstart, cur.block_id)
    else {
        // Genuine zero padding (all zeros up to the restart tail) marks
        // end-of-entries; any other unparseable structure is corruption.
        if sstable::all_zero(&cur.block[off..cur.rstart]) {
            return Ok(false);
        }
        return Err(Error::CorruptBlock { id: cur.block_id });
    };
    let key = entry.key;
    let val = entry.val;
    if key.len() > KEY_MAX {
        return Err(Error::KeyTooLarge {
            len: key.len(),
            max: KEY_MAX,
        });
    }
    if val.len() > VAL_MAX {
        return Err(Error::ValueTooLarge {
            len: val.len(),
            max: VAL_MAX,
        });
    }
    cur.key[..key.len()].copy_from_slice(key);
    cur.key_len = key.len();
    cur.val[..val.len()].copy_from_slice(val);
    cur.val_len = val.len();
    cur.seq = entry.seq;
    cur.tombstone = entry.tombstone;
    cur.next = entry.next;
    cur.live = true;
    Ok(true)
}

/// Positions a cursor on its table's first entry. A table with no data
/// blocks parks exhausted.
pub(crate) async fn init_cursor<
    D: BlockDevice,
    const BLOCK: usize,
    const KEY_MAX: usize,
    const VAL_MAX: usize,
>(
    device: &D,
    tref: &TableRef<KEY_MAX>,
    cur: &mut Cursor<BLOCK, KEY_MAX, VAL_MAX>,
) -> Result<(), Error<D::Error>> {
    *cur = Cursor::EMPTY;
    cur.first_block = tref.first_block;
    let data_blocks = u64::from(tref.block_count)
        .checked_sub(3)
        .ok_or(Error::CorruptBlock {
            id: tref.first_block,
        })?;
    cur.data_blocks = data_blocks;
    if data_blocks == 0 {
        return Ok(());
    }
    read_data_block(device, cur, 0).await?;
    if !parse_head_at(cur, 0)? {
        // The writer never emits an empty data block.
        return Err(Error::CorruptBlock {
            id: cur.first_block,
        });
    }
    Ok(())
}

/// Advances the cursor past its head entry. Returns `true` when parked on a
/// new head, `false` when the table is exhausted.
async fn advance_cursor<
    D: BlockDevice,
    const BLOCK: usize,
    const KEY_MAX: usize,
    const VAL_MAX: usize,
>(
    device: &D,
    cur: &mut Cursor<BLOCK, KEY_MAX, VAL_MAX>,
) -> Result<bool, Error<D::Error>> {
    // Another entry in this block? Zero padding means the block's entries
    // are done — move on to the next data block.
    if cur.next < cur.rstart && parse_head_at(cur, cur.next)? {
        return Ok(true);
    }
    let idx = cur
        .block_idx
        .checked_add(1)
        .ok_or(Error::CorruptBlock { id: cur.block_id })?;
    if idx >= cur.data_blocks {
        cur.live = false;
        return Ok(false);
    }
    read_data_block(device, cur, idx).await?;
    if !parse_head_at(cur, 0)? {
        // The writer never emits an empty data block.
        return Err(Error::CorruptBlock { id: cur.block_id });
    }
    Ok(true)
}

/// Streams one table's entries for the tombstone-rule check.
///
/// A thin [`Cursor`] driver: it reuses compaction's entry parsing (and
/// CRC verification), so the resurrection check sees exactly the entries
/// compaction would merge. Parked on the first entry after
/// [`open`](Self::open); [`head`](Self::head) returns `None` once the
/// table is exhausted.
pub(crate) struct EntryStream<
    'd,
    D: BlockDevice,
    const BLOCK: usize,
    const KEY_MAX: usize,
    const VAL_MAX: usize,
> {
    device: &'d D,
    cur: Cursor<BLOCK, KEY_MAX, VAL_MAX>,
}

impl<'d, D: BlockDevice, const BLOCK: usize, const KEY_MAX: usize, const VAL_MAX: usize>
    EntryStream<'d, D, BLOCK, KEY_MAX, VAL_MAX>
{
    /// Opens the stream on `tref`'s first entry. A table with no data
    /// blocks parks exhausted (its head is `None`).
    ///
    /// # Errors
    ///
    /// [`Error::CorruptBlock`] when a data block fails its CRC or an
    /// entry fails to parse, or [`Error::Device`] on I/O failure.
    pub(crate) async fn open(
        device: &'d D,
        tref: &TableRef<KEY_MAX>,
    ) -> Result<Self, Error<D::Error>> {
        let mut cur = Cursor::EMPTY;
        init_cursor(device, tref, &mut cur).await?;
        Ok(Self { device, cur })
    }

    /// The parked entry's key, sequence, and tombstone flag, or `None`
    /// when the table is exhausted.
    #[must_use]
    pub(crate) fn head(&self) -> Option<(&[u8], u64, bool)> {
        if !self.cur.live {
            return None;
        }
        Some((
            &self.cur.key[..self.cur.key_len],
            self.cur.seq,
            self.cur.tombstone,
        ))
    }

    /// Advances past the head entry. Returns `true` when parked on a new
    /// head, `false` when the table is exhausted.
    ///
    /// # Errors
    ///
    /// [`Error::CorruptBlock`] when the next block fails its CRC or an
    /// entry fails to parse, or [`Error::Device`] on I/O failure.
    pub(crate) async fn advance(&mut self) -> Result<bool, Error<D::Error>> {
        advance_cursor(self.device, &mut self.cur).await
    }
}
