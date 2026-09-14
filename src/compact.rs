//! Bounded leveled compaction: the caller-driven merge engine.
//!
//! When L0 fills (`TABLES` tables), [`Db::compact_step`] selects all of L0
//! plus the L1 tables whose key ranges overlap as one compaction job. The
//! merge is incremental: one call pushes merged entries into the output
//! table until an output block seals (or the merge exhausts), reporting
//! [`Progress::More`] while work remains. A final manifest commit swaps the
//! input tables for the output table atomically.
//!
//! The caller owns the [`Compaction`] scratch — the output table's staging
//! buffers plus one read cursor per input table, at most
//! [`COMPACTION_KMAX`] tables per job. Nothing is allocated. Dropping the
//! scratch mid-job is crash-safe: partial output tables are invisible until
//! the manifest commit, so they become orphans reclaimed by the next
//! `open()` sweep, while the input tables stay referenced; a fresh scratch
//! simply selects the job again.
//!
//! Compaction preserves the read path's highest-sequence-wins rule: when
//! several inputs hold the same key, the entry with the highest sequence
//! number survives. Tombstones drop the key, and a tombstone itself is
//! dropped only when the output reaches the bottommost level holding the
//! merged key range — nothing below can hide an older version there.

use core::future::poll_fn;

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
    pub(crate) bottommost: bool,
    pub(crate) out_base: u64,
    pub(crate) out_blocks: u64,
    pub(crate) out_len: usize,
    pub(crate) from_free: bool,
    pub(crate) writer: TableWriter<BLOCK, BLOOM_BYTES, KEY_MAX>,
    pub(crate) cursors: [Cursor<BLOCK, KEY_MAX, VAL_MAX>; COMPACTION_KMAX],
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
            out_base: 0,
            out_blocks: 0,
            out_len: 0,
            from_free: false,
            writer: TableWriter::new(0, 0),
            cursors: [Cursor::EMPTY; COMPACTION_KMAX],
        }
    }

    /// Abandons any in-progress job. Partial output blocks were never
    /// claimed or referenced, so they are simply free; the inputs are still
    /// in the manifest and a later select will redo the job.
    pub(crate) const fn reset(&mut self) {
        self.state = State::Idle;
        self.n_inputs = 0;
    }

    /// One bounded merge quantum: pushes merged entries into the output
    /// table until one output block seals, or every input is exhausted.
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
            // The live cursor on the minimum key; among ties the highest
            // sequence wins. KMAX = 8, so a linear scan is trivially
            // bounded — and unlike a heap it cannot hold stale entries for
            // exhausted cursors.
            let mut best = COMPACTION_KMAX;
            let mut tied = [false; COMPACTION_KMAX];
            for ci in 0..self.n_inputs {
                if !self.cursors[ci].live {
                    continue;
                }
                if best == COMPACTION_KMAX {
                    best = ci;
                    tied[ci] = true;
                    continue;
                }
                let ord = self.cursors[ci].key[..self.cursors[ci].key_len]
                    .cmp(&self.cursors[best].key[..self.cursors[best].key_len]);
                match ord {
                    core::cmp::Ordering::Less => {
                        best = ci;
                        tied = [false; COMPACTION_KMAX];
                        tied[ci] = true;
                    }
                    core::cmp::Ordering::Equal => {
                        tied[ci] = true;
                        if self.cursors[ci].seq > self.cursors[best].seq {
                            best = ci;
                        }
                    }
                    core::cmp::Ordering::Greater => {}
                }
            }
            if best == COMPACTION_KMAX {
                return Ok(MergeOutcome::Exhausted);
            }
            // Advance every tied loser past the consumed key. The winner
            // stays parked until its entry is safely in the output.
            for ci in 0..self.n_inputs {
                if tied[ci] && ci != best {
                    advance_cursor(&*device, &mut self.cursors[ci]).await?;
                }
            }
            // The winner survives — unless it is a tombstone whose key
            // range reached the bottommost level, where nothing below can
            // hold an older version of the key.
            let (tombstone, seq, key_len, val_len) = {
                let c = &self.cursors[best];
                (c.tombstone, c.seq, c.key_len, c.val_len)
            };
            if tombstone && self.bottommost {
                advance_cursor(&*device, &mut self.cursors[best]).await?;
                continue;
            }
            let sealed = {
                let c = &self.cursors[best];
                let e = SstEntry {
                    key: &c.key[..key_len],
                    val: &c.val[..val_len],
                    seq,
                    tombstone,
                };
                self.writer.push(device, e).await?
            };
            advance_cursor(&*device, &mut self.cursors[best]).await?;
            if sealed == PushOutcome::BlockSealed {
                return Ok(MergeOutcome::More);
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
