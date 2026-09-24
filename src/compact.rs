//! Bounded leveled compaction: the caller-driven merge engine.
//!
//! When a level fills (`TABLES` tables), [`Db::compact_step`] selects a job
//! draining the deepest full level into the one below: L0's tables (its
//! oldest ones, when free slots are short), or one table of a deeper
//! level, merged with every table of the level below whose key range
//! overlaps. The job policy — selection, admission, trivial moves,
//! consolidation, region pressure, and the commit protocol — lives in
//! `db::compaction`; this module is the merge engine it drives.
//!
//! The merge is incremental: one call pushes merged entries into the
//! current output table until an output block seals, the output fills at a
//! key boundary (`MergeOutcome::Split`: the `Db` seals and commits it and
//! opens the next), or the merge exhausts. Sources are read through one
//! cursor each; the target level's tables are disjoint and sorted, so one
//! concatenating cursor reads any number of them.
//!
//! The caller owns the [`Compaction`] scratch — the output table's staging
//! buffers plus the read cursors, at most [`COMPACTION_KMAX`] of them.
//! Nothing is allocated. Dropping the scratch mid-job is crash-safe: the
//! output being written is invisible until its commit, and its table slot
//! stays reserved only until the next `compact_step` (with any scratch)
//! abandons the stale job; outputs already committed stay, and a fresh
//! scratch merges on from there.
//!
//! Compaction preserves the read path's visibility rule: each key keeps
//! the newest version (the live view) plus the newest version at or below
//! each live snapshot's watermark — older versions are dead to every reader
//! and are not emitted. A bottommost tombstone older than every live
//! snapshot drops the whole key: nothing below can hide an older version,
//! and deletion is observationally identical to absence there.
//!
//! [`Db::compact_step`]: crate::db::Db::compact_step

use core::future::poll_fn;

use crate::alloc::MAX_SLOTS;
use crate::compress::CompressScratch;
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
/// fresh job when some level is full again. The type parameters must match
/// the [`Db`](crate::db::Db) it drives.
pub struct Compaction<
    const BLOCK: usize,
    const KEY_MAX: usize,
    const VAL_MAX: usize,
    const BLOOM_BYTES: usize,
> {
    pub(crate) state: State,
    /// The job's source tables — all of L0, or one table of a deeper
    /// level — each read through its own cursor (`cursors[..n_src]`).
    pub(crate) inputs: [Input<KEY_MAX>; COMPACTION_KMAX],
    pub(crate) n_src: usize,
    /// Sources already retired by a progress commit (bit `i` for
    /// `inputs[i]`): their whole range lies behind the committed outputs,
    /// and their slots may already hold other tables.
    pub(crate) src_retired: u8,
    pub(crate) target_level: usize,
    /// Ids of the job's target-level tables, in key order. Target tables
    /// are disjoint and sorted, so one concatenating cursor
    /// (`cursors[n_src]`) reads them all in sequence: a job may span any
    /// number of them.
    pub(crate) tgt: [u32; MAX_SLOTS],
    pub(crate) n_tgt: usize,
    /// Targets opened by the concatenating cursor so far (the one it is
    /// reading is `tgt[tgt_open - 1]`).
    pub(crate) tgt_open: usize,
    /// Targets whose whole key range lies behind the committed outputs
    /// (`tgt[..tgt_retired]`): removed from the manifest, their data lives
    /// in those outputs now.
    pub(crate) tgt_retired: usize,
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
    /// Lowest sequence any table *outside* this job — at any level,
    /// shallower ones included — or the memtable could hold for a key in
    /// the job's range (`u64::MAX` when nothing outside overlaps).
    /// Captured at `compact_select`. A tombstone at or above it may be
    /// hiding an older version outside the job (an ingested table
    /// re-attached at L0, say), so it is never dropped: dropping it would
    /// resurrect that version.
    pub(crate) outside_min_seq: u64,
    /// TTL purge cutoff for this job: an emitted value with
    /// `expire_at != 0 && expire_at <= purge_before` is converted to a
    /// point tombstone at the same sequence — never silently dropped
    /// (dropping it would resurrect the older version beneath it at
    /// snapshot reads). `0` disables the purge. Set before driving the
    /// job; the caller promises every future read uses
    /// `now >= purge_before`, so the converted tombstone is
    /// observationally identical to the expired value for all of them.
    /// Sticky across jobs until changed. Public so callers driving
    /// `compact_step` directly (and tests) can set it; `Db` leaves it
    /// `0` unless told otherwise.
    pub purge_before: u64,
    /// Blocks reserved in every output for its range-tombstone section: an
    /// upper bound on the blocks any output's clipped share of the merged
    /// section can need (see `rdel_blocks_bound`).
    pub(crate) rdel_budget: u32,
    /// Data blocks every output may use: its slot minus the rdel budget
    /// and the 3 framing blocks. The writer's block limit.
    pub(crate) data_budget: u64,
    /// Blocks one key's retained versions can still need, closing seal
    /// included (see [`key_run_blocks`](Self::key_run_blocks)): an output
    /// ends before a key when fewer than this many are left.
    pub(crate) split_margin: u64,
    /// Bloom probes per key for every output.
    pub(crate) bloom_k: u8,
    /// The current output's reserved slot and its first block.
    pub(crate) out_slot: u32,
    pub(crate) out_base: u64,
    /// Left clip bound of the current output: the job range's lower bound
    /// for the first output, then the successor of the previous output's
    /// last key. Range tombstones are cut at output boundaries so the
    /// outputs stay disjoint, and inside the job's range.
    pub(crate) out_lo: KeyBound<KEY_MAX>,
    /// The [`Db`](crate::db::Db)'s job generation when this scratch
    /// selected its job. A mismatch means another scratch started a job,
    /// or the job was aborted (archive, ingest, reopen): the scratch is
    /// stale and resets instead of touching the device.
    pub(crate) job_gen: u32,
    pub(crate) writer: TableWriter<BLOCK, BLOOM_BYTES, KEY_MAX>,
    /// Caller-owned compression scratch for the output tables: every
    /// sealed data block is trial-compressed (see
    /// [`TableWriter::push`](crate::sstable::TableWriter::push)). Lives
    /// for the job's duration; `const`-constructible so [`new`](Self::new)
    /// stays `const`.
    pub(crate) compress: CompressScratch<BLOCK>,
    /// Shared physical-read scratch: one block buffer lent to
    /// [`read_data_block`] on every cursor fill. The physical bytes are
    /// dead once inflated into the cursor's `block`, so the merge cursors
    /// share this instead of each owning one.
    pub(crate) raw: [u8; BLOCK],
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
/// the parsed head entry. Owns its logical block buffer so the k-way
/// merge never allocates; the physical read scratch is shared — the
/// [`Compaction`] (or [`EntryStream`]) hands its one `raw` buffer to
/// [`read_data_block`] on each fill, since the physical bytes are dead
/// once inflated.
#[derive(Clone, Copy)]
pub(crate) struct Cursor<const BLOCK: usize, const KEY_MAX: usize, const VAL_MAX: usize> {
    first_block: u64,
    data_blocks: u64,
    block_idx: u64,
    block_id: u64,
    /// Logical block: inflated here when the compression flag is set,
    /// copied here from the physical read when clear. Every parser
    /// below reads from here, so decompression is transparent.
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
    /// TTL expiry of the head entry (`0` = no expiry). Carried so the
    /// merge's TTL purge can convert expired values to tombstones.
    expire_at: u64,
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
        expire_at: 0,
    };
}

/// What one [`Compaction::merge_step`] did.
pub(crate) enum MergeOutcome {
    /// One output block sealed; the merge has more entries.
    More,
    /// The current output is full at a key boundary: seal it, commit
    /// progress, and open the next output before merging on.
    Split,
    /// Every input is exhausted; the caller should seal and commit.
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
            n_src: 0,
            src_retired: 0,
            target_level: 0,
            tgt: [0u32; MAX_SLOTS],
            n_tgt: 0,
            tgt_open: 0,
            tgt_retired: 0,
            bottommost: false,
            snapshots: [0u64; MAX_SNAPSHOTS],
            n_snapshots: 0,
            oldest_snapshot: u64::MAX,
            outside_min_seq: u64::MAX,
            purge_before: 0,
            rdel_budget: 0,
            data_budget: 0,
            split_margin: 0,
            bloom_k: 1,
            out_slot: 0,
            out_base: 0,
            out_lo: KeyBound::EMPTY,
            job_gen: 0,
            writer: TableWriter::new(0, 0),
            compress: CompressScratch::new(),
            raw: [0u8; BLOCK],
            cursors: [Cursor::EMPTY; COMPACTION_KMAX],
            key: [0u8; KEY_MAX],
            key_len: 0,
            key_state: KeyState::Idle,
            served: [false; MAX_SNAPSHOTS + 1],
        }
    }

    /// Abandons any in-progress job. Uncommitted output blocks were never
    /// referenced, so their slots are simply free once the `Db` releases
    /// the reservations; progress already committed stays, and a later
    /// select redoes the rest.
    ///
    /// `purge_before` is sticky across jobs (it is a caller-owned clock
    /// cutoff, not per-job state); change it explicitly when the cutoff
    /// moves.
    pub(crate) const fn reset(&mut self) {
        self.state = State::Idle;
        self.n_src = 0;
        self.src_retired = 0;
        self.n_tgt = 0;
        self.tgt_open = 0;
        self.tgt_retired = 0;
        self.key_state = KeyState::Idle;
        self.rdel_budget = 0;
        self.out_lo = KeyBound::EMPTY;
    }

    /// Cursors the merge reads: one per source, plus the concatenating
    /// target cursor when the job has targets.
    const fn n_cursors(&self) -> usize {
        self.n_src + if self.n_tgt > 0 { 1 } else { 0 }
    }

    /// Loads the next target that has point entries into the concatenating
    /// cursor, or parks it exhausted after the last target. Range-only
    /// targets have no data blocks and are skipped (their range tombstones
    /// reach the outputs through the rdel merge).
    ///
    /// `tgt_level` is the target level's live refs; the job's targets are
    /// found in it by id.
    pub(crate) async fn open_next_target<D: BlockDevice>(
        &mut self,
        device: &D,
        tgt_level: &[TableRef<KEY_MAX>],
    ) -> Result<(), Error<D::Error>> {
        let ci = self.n_src;
        loop {
            if self.tgt_open >= self.n_tgt {
                self.cursors[ci].live = false;
                return Ok(());
            }
            let id = self.tgt[self.tgt_open];
            let tref = tgt_level
                .iter()
                .find(|t| t.id == id)
                .copied()
                .ok_or(Error::CorruptManifest)?;
            self.tgt_open += 1;
            init_cursor(device, &mut self.raw, &tref, &mut self.cursors[ci]).await?;
            if self.cursors[ci].live {
                return Ok(());
            }
        }
    }

    /// Advances input cursor `ci` past its head; the concatenating target
    /// cursor rolls over to the next target when its table runs out.
    async fn advance_input<D: BlockDevice>(
        &mut self,
        device: &D,
        ci: usize,
        tgt_level: &[TableRef<KEY_MAX>],
    ) -> Result<(), Error<D::Error>> {
        if !advance_cursor(device, &mut self.raw, &mut self.cursors[ci]).await? && ci == self.n_src
        {
            self.open_next_target(device, tgt_level).await?;
        }
        Ok(())
    }

    /// Whether the current output should end before the next key starts.
    /// Outputs end only at key boundaries, so every key's versions land in
    /// one table and the outputs stay disjoint. A key emits at most one
    /// version per threshold (live view plus each snapshot), each push
    /// seals at most one block, and closing the output seals one more — so
    /// the output ends while that worst case still fits both its data
    /// budget and its one-block index.
    const fn should_split(&self) -> bool {
        if self.writer.entry_count() == 0 {
            return false;
        }
        if self.writer.data_blocks() + self.split_margin > self.data_budget {
            return true;
        }
        // Every seal adds one index entry of `18 + key` bytes.
        let index_room = BLOCK.saturating_sub(sstable::CRC_LEN);
        // `split_margin` is a handful of blocks: the narrowing is exact.
        #[allow(clippy::cast_possible_truncation)]
        let index_need = self.split_margin as usize * (18 + KEY_MAX);
        self.writer.index_len() + index_need > index_room
    }

    /// Blocks one key can still need in the output, its closing seal
    /// included. A key emits at most one version per threshold (the live
    /// view plus each of `n_snapshots` snapshots), each at most
    /// `ENTRY_HEADER + KEY_MAX + VAL_MAX + 8` bytes (the 8: a TTL expiry).
    /// The first push may seal the block already in progress; after that a
    /// block seals only once it holds more than a block's room minus one
    /// entry, so `v` versions seal at most `1 + v * entry / min_fill`
    /// blocks — then the output's closing seal.
    pub(crate) const fn key_run_blocks(n_snapshots: usize) -> u64 {
        let versions = 1 + n_snapshots;
        let entry = sstable::ENTRY_HEADER + KEY_MAX + VAL_MAX + 8;
        let room = BLOCK.saturating_sub(sstable::MAX_DATA_TAIL);
        let seals = if room > entry {
            1 + versions * entry / (room - entry + 1)
        } else {
            versions
        };
        // A handful of blocks: the widening is exact.
        (seals + 1) as u64
    }

    /// Index of the live cursor on the minimum key, or `COMPACTION_KMAX`
    /// when every cursor is exhausted. At most `COMPACTION_KMAX` cursors,
    /// so a linear scan is trivially bounded — and unlike a heap it cannot
    /// hold stale entries for exhausted cursors.
    fn min_key_cursor(cursors: &[Cursor<BLOCK, KEY_MAX, VAL_MAX>]) -> usize {
        let mut best = COMPACTION_KMAX;
        for (ci, c) in cursors.iter().enumerate() {
            if !c.live {
                continue;
            }
            if best == COMPACTION_KMAX
                || c.key[..c.key_len] < cursors[best].key[..cursors[best].key_len]
            {
                best = ci;
            }
        }
        best
    }

    /// Index of the highest-sequence cursor tied on `best`'s key. Each
    /// cursor sits at its table's version-run start and runs are
    /// newest-first, so the head is the newest version not yet processed
    /// for this key.
    fn head_cursor(cursors: &[Cursor<BLOCK, KEY_MAX, VAL_MAX>], best: usize) -> usize {
        let mut head = best;
        let bkey = &cursors[best].key[..cursors[best].key_len];
        for (ci, c) in cursors.iter().enumerate() {
            if c.live && ci != best && &c.key[..c.key_len] == bkey && c.seq > cursors[head].seq {
                head = ci;
            }
        }
        head
    }

    /// One bounded merge quantum: pushes merged entries into the current
    /// output table until one output block seals ([`MergeOutcome::More`]),
    /// the output is full at a key boundary ([`MergeOutcome::Split`]: the
    /// caller seals it, commits progress, and opens the next output; the
    /// next key is not consumed), or every input is exhausted
    /// ([`MergeOutcome::Exhausted`]).
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
    /// `tgt_level` is the target level's live refs, for the concatenating
    /// target cursor.
    ///
    /// # Errors
    ///
    /// [`Error::CorruptBlock`] on a torn input block (compaction must never
    /// silently drop entries), [`Error::NoSpace`] when an entry cannot fit
    /// in an empty output block, or [`Error::Device`] on I/O failure.
    pub(crate) async fn merge_step<D: BlockDevice>(
        &mut self,
        device: &mut D,
        tgt_level: &[TableRef<KEY_MAX>],
    ) -> Result<MergeOutcome, Error<D::Error>> {
        let n = self.n_cursors();
        loop {
            let best = Self::min_key_cursor(&self.cursors[..n]);
            if best == COMPACTION_KMAX {
                self.key_state = KeyState::Idle;
                return Ok(MergeOutcome::Exhausted);
            }
            let head = Self::head_cursor(&self.cursors[..n], best);
            // New key: (re)start the per-key threshold state. After a
            // mid-key seal the bytes match and the served flags resume the
            // key exactly where it stopped.
            let hkey_len = self.cursors[best].key_len;
            if self.key_state == KeyState::Idle
                || self.key_len != hkey_len
                || self.key[..hkey_len] != self.cursors[best].key[..hkey_len]
            {
                if self.should_split() {
                    // Nothing of the new key was consumed; the next call
                    // starts it afresh in the next output.
                    self.key_state = KeyState::Idle;
                    return Ok(MergeOutcome::Split);
                }
                self.key[..hkey_len].copy_from_slice(&self.cursors[best].key[..hkey_len]);
                self.key_len = hkey_len;
                self.key_state = KeyState::Merging;
                self.served = [false; MAX_SNAPSHOTS + 1];
                if self.may_drop_key(head) {
                    self.key_state = KeyState::Dropping;
                }
            }
            let (seq, tombstone, val_len, expire_at) = {
                let c = &self.cursors[head];
                (c.seq, c.tombstone, c.val_len, c.expire_at)
            };
            // TTL purge: an emitted value already expired as of
            // `purge_before` becomes a point tombstone at the same
            // sequence — never silently dropped. Dropping it would
            // resurrect the older version beneath it at snapshot reads
            // (the expired version can be some snapshot's newest visible
            // version, and only its presence — or a tombstone at its
            // sequence — keeps that read at absent). The drop check above
            // already treated it as a tombstone, so the existing
            // threshold/bottommost machinery keeps or drops it exactly as
            // if the caller had deleted the key. `purge_before == 0`
            // disables the purge. Surviving values carry their `expire_at`
            // through to the output table.
            let purged = !tombstone && expire_at != 0 && expire_at <= self.purge_before;
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
                        val: if purged { &[] } else { &c.val[..val_len] },
                        seq,
                        tombstone: tombstone || purged,
                        expire_at: if tombstone || purged { 0 } else { expire_at },
                    };
                    self.writer
                        .push(device, e, Some(&mut self.compress))
                        .await?
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
                self.advance_input(&*device, head, tgt_level).await?;
                if sealed == PushOutcome::BlockSealed {
                    return Ok(MergeOutcome::More);
                }
            } else {
                // Skipped (or the key is dropped): this version serves no
                // threshold. Only the head advances — a tied cursor parked
                // on an older version may still serve a smaller threshold
                // on a later iteration.
                self.advance_input(&*device, head, tgt_level).await?;
            }
        }
    }

    /// The bottommost tombstone drop, decided on a key's newest version
    /// (the `head` cursor). Dropping the whole key is safe exactly when:
    ///
    /// - the output is bottommost for the job's range and the head is a
    ///   tombstone (or a value the TTL purge will turn into one, so the
    ///   drop matches a caller-issued delete exactly);
    /// - the tombstone predates every live snapshot — then each
    ///   snapshot's visible version is the tombstone itself, the keep-set
    ///   is just it, and deletion is observationally identical to absence;
    /// - nothing outside the job can hold an older version it hides
    ///   (`outside_min_seq`).
    ///
    /// Inputs holding older versions of the key are no hazard: the
    /// progress commit that makes this output live also retires, or
    /// narrows past the key, every input that reaches it.
    const fn may_drop_key(&self, head: usize) -> bool {
        let c = &self.cursors[head];
        let effective_tombstone =
            c.tombstone || (c.expire_at != 0 && c.expire_at <= self.purge_before);
        self.bottommost
            && effective_tombstone
            && c.seq < self.oldest_snapshot
            && c.seq < self.outside_min_seq
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

/// The job's remaining target tables for a range-tombstone merge pass: the
/// target level's live refs plus the ids to read, in key order. Targets
/// are disjoint and sorted, so their rdel sections concatenate into one
/// sorted stream (every start in one table precedes every start in the
/// next) and one merge head reads them all.
pub(crate) struct TargetView<'a, const KEY_MAX: usize> {
    pub(crate) level: &'a [TableRef<KEY_MAX>],
    pub(crate) ids: &'a [u32],
}

impl<const KEY_MAX: usize> TargetView<'_, KEY_MAX> {
    /// The rdel section `(first block, blocks)` of target `i`, `None` past
    /// the last target. A target missing from the level, or too short for
    /// its sections, is a corrupt manifest.
    fn section<E>(&self, i: usize) -> Option<Result<(u64, u32), Error<E>>> {
        let id = *self.ids.get(i)?;
        let Some(t) = self.level.iter().find(|t| t.id == id) else {
            return Some(Err(Error::CorruptManifest));
        };
        Some(
            t.rdel_first()
                .map(|first| (first, t.rdel_blocks))
                .ok_or(Error::CorruptManifest),
        )
    }
}

/// Folded bounds of one compaction's output range-tombstone section, for
/// the output [`TableRef`](crate::manifest::TableRef).
pub(crate) struct RdelStats<const KEY_MAX: usize> {
    pub(crate) first: KeyBound<KEY_MAX>,
    pub(crate) last: KeyBound<KEY_MAX>,
    pub(crate) max_seq: u64,
    pub(crate) min_seq: u64,
}

impl<const KEY_MAX: usize> RdelStats<KEY_MAX> {
    pub(crate) const fn new() -> Self {
        Self {
            first: KeyBound::EMPTY,
            last: KeyBound::EMPTY,
            max_seq: 0,
            min_seq: u64::MAX,
        }
    }

    pub(crate) fn observe<E>(
        &mut self,
        e: &sstable::RdelEntry<'_>,
        id: u64,
    ) -> Result<(), Error<E>> {
        let corrupt = || Error::CorruptBlock { id };
        let start = KeyBound::from_slice(e.start).ok_or_else(corrupt)?;
        let end = KeyBound::from_slice(e.end).ok_or_else(corrupt)?;
        self.first = self.first.min(start);
        // The rdel end is exclusive; storing it as the table's inclusive
        // `last_key` is conservative for pruning (a table starting exactly
        // at `end` never holds a covered key, but treating it as
        // overlapping only skips a prune, never a result).
        self.last = self.last.max(end);
        if e.seq > self.max_seq {
            self.max_seq = e.seq;
        }
        if e.seq < self.min_seq {
            self.min_seq = e.seq;
        }
        Ok(())
    }
}

/// Streams the compaction inputs' range-tombstone sections into one sorted
/// output section: a bounded k-way merge over the inputs' stored rdel
/// sections, each sorted by `start` ascending. Ties on `start` may come in
/// any sequence order: a flush writes them newest first, but compaction
/// clips pieces to its outputs' ranges, and raising several starts to the
/// same lower bound keeps their original, start-ordered sequence. Nothing
/// here relies on the order of ties.
///
/// Per input the merge keeps a read position plus one copied head entry
/// (the shared `raw` block buffer is reused across inputs, so borrowed
/// heads would dangle). Each step takes the minimum head, coalesces it
/// into the pending entry when it shares the pending sequence and
/// overlaps or abuts it (same `seq` means the same logical delete, so the
/// union is exact — this also folds exact duplicates), and otherwise
/// emits the pending entry and starts a new one. Differently-sequenced
/// overlaps are kept as-is: correct, merely uncompacted. A tombstone is
/// dropped only when the output is bottommost and its `seq` is below the
/// oldest snapshot.
///
/// The merge is a pull iterator: [`next_merged`](RdelMerger::next_merged)
/// advances past one emitted entry and reports whether the merger's
/// [`current_entry`](RdelMerger::current_entry) is valid. Driving it twice
/// over the immutable input sections yields the identical entry sequence,
/// which is how compaction counts the section's exact block budget before
/// reserving output space (see [`count_rdel_merge`]).
///
/// Entry parsing is bounded by each block's stored count: the count/CRC
/// trailer is never parsed as entries.
///
/// # Errors
///
/// [`Error::CorruptBlock`] on CRC failure or malformed entries, or
/// [`Error::Device`] on I/O failure.
pub(crate) struct RdelMerger<const KEY_MAX: usize> {
    heads: [RdelHead<KEY_MAX>; COMPACTION_KMAX],
    n: usize,
    pending: OwnedRdel<KEY_MAX>,
    has_pending: bool,
    current: OwnedRdel<KEY_MAX>,
    /// The last emitted tombstone. An older identical-range tombstone may
    /// be dropped only when this shadows it with `seq <= oldest_snapshot`
    /// — otherwise a live snapshot between the two sequences would lose
    /// its covering tombstone and resurrect covered keys.
    last_emitted: Option<OwnedRdel<KEY_MAX>>,
    bottommost: bool,
    oldest_snapshot: u64,
}

/// One merge input: its section location, read position, and copied head
/// entry. The concatenating head reads the targets' sections one after
/// another (`next_target` is the next one to open).
struct RdelHead<const KEY_MAX: usize> {
    first_block: u64,
    rdel_blocks: u32,
    concat: bool,
    next_target: usize,
    /// Index of the block holding the next entry to parse.
    block: u32,
    /// Byte offset of that entry within its block.
    off: usize,
    /// Entries left in `block` from `off` onward. Zero exactly when `block`
    /// is fresh (just advanced to) or the section is exhausted.
    remaining: usize,
    has_head: bool,
    entry: OwnedRdel<KEY_MAX>,
}

/// An owned range-tombstone entry: `[u8; KEY_MAX]` is `Copy`, so heads and
/// the pending entry move by value without allocation.
#[derive(Clone, Copy)]
struct OwnedRdel<const KEY_MAX: usize> {
    start: [u8; KEY_MAX],
    start_len: usize,
    end: [u8; KEY_MAX],
    end_len: usize,
    seq: u64,
}

impl<const KEY_MAX: usize> OwnedRdel<KEY_MAX> {
    const fn empty() -> Self {
        Self {
            start: [0u8; KEY_MAX],
            start_len: 0,
            end: [0u8; KEY_MAX],
            end_len: 0,
            seq: 0,
        }
    }

    fn copy_from_entry(&mut self, e: &sstable::RdelEntry<'_>) {
        self.start = [0u8; KEY_MAX];
        self.start[..e.start.len()].copy_from_slice(e.start);
        self.start_len = e.start.len();
        self.end = [0u8; KEY_MAX];
        self.end[..e.end.len()].copy_from_slice(e.end);
        self.end_len = e.end.len();
        self.seq = e.seq;
    }

    /// True when `other` names the identical `(start, end)` range.
    fn same_range(&self, other: &Self) -> bool {
        if self.start_len != other.start_len || self.end_len != other.end_len {
            return false;
        }
        // Lengths are equal: compare with the shared length.
        let sl = self.start_len;
        let el = self.end_len;
        self.start[..sl] == other.start[..sl] && self.end[..el] == other.end[..el]
    }
}

impl<const KEY_MAX: usize> RdelHead<KEY_MAX> {
    const fn new(first_block: u64, rdel_blocks: u32) -> Self {
        Self {
            first_block,
            rdel_blocks,
            concat: false,
            next_target: 0,
            block: 0,
            off: 0,
            remaining: 0,
            has_head: false,
            entry: OwnedRdel::empty(),
        }
    }
}

/// Merge order: `start` ascending, `seq` descending.
fn rdel_entry_less<const KEY_MAX: usize>(a: &OwnedRdel<KEY_MAX>, b: &OwnedRdel<KEY_MAX>) -> bool {
    match a.start[..a.start_len].cmp(&b.start[..b.start_len]) {
        core::cmp::Ordering::Less => true,
        core::cmp::Ordering::Greater => false,
        core::cmp::Ordering::Equal => a.seq > b.seq,
    }
}

/// Parses the next entry of `head`'s section into its copied head slot.
/// Idempotent: a no-op when the head is already filled or the section is
/// exhausted. The block is re-read on every call — `raw` is shared across
/// inputs — which is sound because sealed `SSTable` blocks are immutable.
async fn fill_rdel_head<D: BlockDevice, const BLOCK: usize, const KEY_MAX: usize>(
    device: &D,
    head: &mut RdelHead<KEY_MAX>,
    raw: &mut [u8; BLOCK],
    targets: &TargetView<'_, KEY_MAX>,
) -> Result<(), Error<D::Error>> {
    if head.has_head {
        return Ok(());
    }
    loop {
        if head.block >= head.rdel_blocks {
            // Section exhausted: the concatenating head moves on to the
            // next target's section; a single-table head is done.
            if !head.concat {
                return Ok(());
            }
            let Some(section) = targets.section(head.next_target) else {
                return Ok(());
            };
            let (first, blocks) = section?;
            head.next_target += 1;
            head.first_block = first;
            head.rdel_blocks = blocks;
            head.block = 0;
            head.off = 0;
            head.remaining = 0;
            continue;
        }
        let id =
            head.first_block
                .checked_add(u64::from(head.block))
                .ok_or(Error::CorruptBlock {
                    id: head.first_block,
                })?;
        read_block_into(device, id, raw).await?;
        let count = sstable::rdel_block_count::<D::Error, BLOCK>(raw, id)?;
        if count == 0 {
            // Defensive: the writer never seals an empty rdel block, but a
            // zero-count block must not stall the merge.
            head.block += 1;
            head.off = 0;
            head.remaining = 0;
            continue;
        }
        if head.remaining == 0 {
            // Fresh block (invariant: `remaining == 0` only here or when
            // exhausted, which returned above).
            head.remaining = count;
        }
        let (e, next) =
            sstable::rdel_parse_at(&raw[..], head.off).map_err(|()| Error::CorruptBlock { id })?;
        head.entry.copy_from_entry(&e);
        head.has_head = true;
        head.off = next;
        head.remaining -= 1;
        if head.remaining == 0 {
            head.block += 1;
            head.off = 0;
        }
        return Ok(());
    }
}

impl<const KEY_MAX: usize> RdelMerger<KEY_MAX> {
    /// Merges the range-tombstone sections of `sources` (one head each,
    /// skipping those whose bit is set in `retired`) and, when
    /// `with_targets`, of the targets passed to every
    /// [`next_merged`](Self::next_merged) call (one concatenating head).
    /// `bottommost` and `oldest_snapshot` gate the tombstone drop rule.
    pub(crate) fn new(
        sources: &[Input<KEY_MAX>],
        retired: u8,
        with_targets: bool,
        bottommost: bool,
        oldest_snapshot: u64,
    ) -> Self {
        let n_src = sources.len().min(COMPACTION_KMAX - 1);
        let heads = core::array::from_fn(|i| {
            if i < n_src && retired & (1u8 << i) == 0 {
                let t = &sources[i].tref;
                // A malformed ref (no room for its sections) reads from an
                // impossible base, which surfaces as `CorruptBlock`.
                RdelHead::new(t.rdel_first().unwrap_or(u64::MAX), t.rdel_blocks)
            } else {
                // A retired source's blocks may already hold another
                // table: its head reads nothing.
                let mut h = RdelHead::new(0, 0);
                h.concat = i == n_src && with_targets;
                h
            }
        });
        Self {
            heads,
            n: n_src + usize::from(with_targets),
            pending: OwnedRdel::empty(),
            has_pending: false,
            current: OwnedRdel::empty(),
            last_emitted: None,
            bottommost,
            oldest_snapshot,
        }
    }

    /// Index of the minimum valid head under the merge order, if any.
    fn min_head(&self) -> Option<usize> {
        let mut m: Option<usize> = None;
        for i in 0..self.n {
            if !self.heads[i].has_head {
                continue;
            }
            let is_min =
                m.is_none_or(|j| rdel_entry_less(&self.heads[i].entry, &self.heads[j].entry));
            if is_min {
                m = Some(i);
            }
        }
        m
    }

    /// Moves head `mi` into the pending slot, applying the bottommost drop
    /// rule (a dropped head simply never becomes pending). A tombstone is
    /// droppable only when it is shadowed by the last emitted tombstone:
    /// identical `(start, end)`, a *newer* sequence, and
    /// `seq <= oldest_snapshot`. Then every live snapshot sees the
    /// shadowing tombstone (or a newer one), so the dropped one was never
    /// decisive. Without the shadow gate, dropping a tombstone below the
    /// oldest snapshot would resurrect covered keys at any live snapshot
    /// sitting between the two sequences. The *newer* check matters
    /// because ties on `start` can arrive oldest first (see
    /// [`RdelMerger`]): an identical range emitted earlier is not
    /// necessarily the newer one, and dropping the newer would expose
    /// every version between the two sequences.
    fn set_pending_from_head(&mut self, mi: usize) {
        let e = &self.heads[mi].entry;
        // The shadow gate: the last emitted tombstone must name the
        // identical range, be newer, and be visible to every live
        // snapshot.
        let shadowed = match &self.last_emitted {
            Some(p) => {
                let newer = p.seq > e.seq;
                let visible_to_all = p.seq <= self.oldest_snapshot;
                newer && visible_to_all && p.same_range(e)
            }
            None => false,
        };
        if self.bottommost && e.seq < self.oldest_snapshot && shadowed {
            self.has_pending = false;
        } else {
            self.pending = *e;
            self.has_pending = true;
        }
    }

    /// Advances past one merged entry. Returns `true` when
    /// [`current_entry`](RdelMerger::current_entry) now holds the next
    /// entry of the merged section, `false` when the merge is exhausted.
    ///
    /// # Errors
    ///
    /// [`Error::CorruptBlock`] on CRC failure or malformed entries, or
    /// [`Error::Device`] on I/O failure.
    pub(crate) async fn next_merged<D: BlockDevice, const BLOCK: usize>(
        &mut self,
        device: &D,
        raw: &mut [u8; BLOCK],
        targets: &TargetView<'_, KEY_MAX>,
    ) -> Result<bool, Error<D::Error>> {
        loop {
            for i in 0..self.n {
                if !self.heads[i].has_head {
                    fill_rdel_head(device, &mut self.heads[i], raw, targets).await?;
                }
            }
            let Some(mi) = self.min_head() else {
                if self.has_pending {
                    self.current = self.pending;
                    self.last_emitted = Some(self.pending);
                    self.has_pending = false;
                    return Ok(true);
                }
                return Ok(false);
            };
            let mut emit = false;
            let h = &self.heads[mi].entry;
            let p = &self.pending;
            if self.has_pending && h.seq == p.seq && h.start[..h.start_len] <= p.end[..p.end_len] {
                // Same-sequence overlap or adjacency: coalesce to the
                // union. Merge order is `start` ascending within a `seq`,
                // so `pending.start <= head.start` and only the end can
                // grow. Exact duplicates fold here too.
                if h.end[..h.end_len] > p.end[..p.end_len] {
                    self.pending.end = h.end;
                    self.pending.end_len = h.end_len;
                }
            } else {
                if self.has_pending {
                    self.current = self.pending;
                    self.last_emitted = Some(self.pending);
                    emit = true;
                }
                self.set_pending_from_head(mi);
            }
            self.heads[mi].has_head = false;
            if emit {
                return Ok(true);
            }
        }
    }

    /// The entry produced by the last
    /// [`next_merged`](RdelMerger::next_merged) returning `true`.
    pub(crate) fn current_entry(&self) -> sstable::RdelEntry<'_> {
        sstable::RdelEntry {
            start: &self.current.start[..self.current.start_len],
            end: &self.current.end[..self.current.end_len],
            seq: self.current.seq,
        }
    }
}

/// Dry-run pass of the range-tombstone merge over a whole job: returns how
/// many merged entries it yields. Every output's clipped share of the
/// merged section is at most that many entries (clipping only cuts
/// entries to the output's range), each at most `12 + 2 * KEY_MAX` bytes,
/// so [`rdel_blocks_bound`] of it bounds every output's rdel section.
///
/// # Errors
///
/// [`Error::CorruptBlock`] on CRC failure or malformed entries,
/// [`Error::CorruptManifest`] when a target is missing, or
/// [`Error::Device`] on I/O failure.
pub(crate) async fn count_rdel_merge<D, const BLOCK: usize, const KEY_MAX: usize>(
    device: &D,
    sources: &[Input<KEY_MAX>],
    targets: &TargetView<'_, KEY_MAX>,
    raw: &mut [u8; BLOCK],
    bottommost: bool,
    oldest_snapshot: u64,
) -> Result<u64, Error<D::Error>>
where
    D: BlockDevice,
{
    let mut merger = RdelMerger::new(
        sources,
        0,
        !targets.ids.is_empty(),
        bottommost,
        oldest_snapshot,
    );
    let mut n = 0u64;
    while merger.next_merged(device, raw, targets).await? {
        n += 1;
    }
    Ok(n)
}

/// Upper bound on the blocks `entries` range tombstones occupy, each at
/// most `12 + 2 * KEY_MAX` bytes. The rdel writer seals a block only when
/// the next entry no longer fits, so every block but the last holds at
/// least `(BLOCK - trailer) / max_entry` entries.
pub(crate) const fn rdel_blocks_bound<const BLOCK: usize, const KEY_MAX: usize>(
    entries: u64,
) -> u64 {
    if entries == 0 {
        return 0;
    }
    let per_block = sstable::rdel_entries_per_block::<BLOCK, KEY_MAX>();
    if per_block == 0 {
        return u64::MAX;
    }
    entries.div_ceil(per_block)
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

/// Loads data block `idx` of the cursor's table: reads, CRC-checks,
/// inflates when the compression flag is set, and locates the entries end.
///
/// `cur.block` always ends up holding the logical block: a flagged block
/// is decompressed from `raw` into `cur.block`; a raw block is copied
/// `raw -> block` (one `BLOCK`-sized memcpy) so every parser below keeps
/// reading from a single buffer. `raw` is the caller's shared physical
/// scratch — its contents are dead on return.
async fn read_data_block<
    D: BlockDevice,
    const BLOCK: usize,
    const KEY_MAX: usize,
    const VAL_MAX: usize,
>(
    device: &D,
    raw: &mut [u8; BLOCK],
    cur: &mut Cursor<BLOCK, KEY_MAX, VAL_MAX>,
    idx: u64,
) -> Result<(), Error<D::Error>> {
    let id = cur
        .first_block
        .checked_add(idx)
        .ok_or(Error::CorruptBlock {
            id: cur.first_block,
        })?;
    read_block_into(device, id, raw).await?;
    sstable::check_block_crc(raw, id)?;
    if sstable::inflate_data_block::<D::Error, BLOCK>(raw, &mut cur.block, id)? {
        // Flagged: `cur.block` now holds the inflated logical block.
    } else {
        cur.block.copy_from_slice(raw);
    }
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
    cur.expire_at = entry.expire_at;
    cur.next = entry.next;
    cur.live = true;
    Ok(true)
}

/// Positions a cursor on its table's first *live* entry: the first entry
/// at or above the table's `first_key`, which a compaction commit may have
/// raised past a prefix it already merged (see
/// [`Manifest::narrow_table`](crate::manifest::Manifest::narrow_table)). A
/// table with no data blocks, or none at or above the bound, parks
/// exhausted.
///
/// The data section leads the table (`[data]* [rdel]* [bloom] [index]
/// [footer]`), so the cursor's block window is `[first_block, first_block +
/// data_blocks)`. The starting block comes from the table's index.
///
/// `raw` is the caller's shared physical-read scratch, lent to
/// [`read_data_block`] for the fill.
pub(crate) async fn init_cursor<
    D: BlockDevice,
    const BLOCK: usize,
    const KEY_MAX: usize,
    const VAL_MAX: usize,
>(
    device: &D,
    raw: &mut [u8; BLOCK],
    tref: &TableRef<KEY_MAX>,
    cur: &mut Cursor<BLOCK, KEY_MAX, VAL_MAX>,
) -> Result<(), Error<D::Error>> {
    *cur = Cursor::EMPTY;
    cur.first_block = tref.data_first();
    let data_blocks = tref.data_blocks().ok_or(Error::CorruptBlock {
        id: tref.first_block,
    })?;
    cur.data_blocks = data_blocks;
    if data_blocks == 0 {
        return Ok(());
    }
    // The first data block whose key range can reach the bound.
    let floor = tref.first_key.as_slice();
    let footer = tref.footer_block().ok_or(Error::CorruptManifest)?;
    let index_id = sstable::footer_index_block(device, None, tref.id, raw, footer).await?;
    read_block_into(device, index_id, raw).await?;
    sstable::check_block_crc(raw, index_id)?;
    let payload_end = BLOCK - sstable::CRC_LEN;
    let start = match sstable::index_lookup::<D::Error>(&raw[..payload_end], floor, index_id)? {
        Some((id, _)) => id
            .checked_sub(cur.first_block)
            .filter(|&i| i < data_blocks)
            .ok_or(Error::CorruptBlock { id: index_id })?,
        None => 0,
    };
    read_data_block(device, raw, cur, start).await?;
    if !parse_head_at(cur, 0)? {
        // The writer never emits an empty data block.
        return Err(Error::CorruptBlock {
            id: cur.first_block,
        });
    }
    // Skip the dead prefix below the bound (at most the rest of the
    // starting block plus whatever the index could not rule out).
    while cur.live && &cur.key[..cur.key_len] < floor {
        advance_cursor(device, raw, cur).await?;
    }
    Ok(())
}

/// Advances the cursor past its head entry. Returns `true` when parked on a
/// new head, `false` when the table is exhausted.
///
/// `raw` is the caller's shared physical-read scratch, lent to
/// [`read_data_block`] when the next block loads.
async fn advance_cursor<
    D: BlockDevice,
    const BLOCK: usize,
    const KEY_MAX: usize,
    const VAL_MAX: usize,
>(
    device: &D,
    raw: &mut [u8; BLOCK],
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
    read_data_block(device, raw, cur, idx).await?;
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
    /// Physical-read scratch lent to the cursor fills; dead once a block
    /// is inflated into `cur.block`.
    raw: [u8; BLOCK],
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
        let mut raw = [0u8; BLOCK];
        init_cursor(device, &mut raw, tref, &mut cur).await?;
        Ok(Self { device, raw, cur })
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
        advance_cursor(self.device, &mut self.raw, &mut self.cur).await
    }
}

#[cfg(test)]
mod tests {
    extern crate std;

    use super::*;
    use std::task::{Context, Poll, Waker};
    use std::vec;
    use std::vec::Vec;

    fn block_on<F: core::future::Future>(f: F) -> F::Output {
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let mut pinned = std::boxed::Box::pin(f);
        loop {
            match pinned.as_mut().poll(&mut cx) {
                Poll::Ready(v) => return v,
                Poll::Pending => std::thread::yield_now(),
            }
        }
    }

    /// In-memory block device for merge tests. Always `Ready`.
    struct TestDevice<const BLOCK: usize> {
        blocks: Vec<[u8; BLOCK]>,
    }

    impl<const BLOCK: usize> TestDevice<BLOCK> {
        fn new() -> Self {
            Self { blocks: Vec::new() }
        }
    }

    impl<const BLOCK: usize> BlockDevice for TestDevice<BLOCK> {
        type Error = core::convert::Infallible;
        const BLOCK: usize = BLOCK;

        fn poll_read_block(
            &self,
            _cx: &mut Context<'_>,
            id: u64,
            buf: &mut [u8],
        ) -> Poll<Result<(), Self::Error>> {
            let mut tmp = [0u8; BLOCK];
            if let Some(b) = usize::try_from(id).ok().and_then(|i| self.blocks.get(i)) {
                tmp.copy_from_slice(b);
            }
            buf.copy_from_slice(&tmp[..buf.len()]);
            Poll::Ready(Ok(()))
        }

        fn poll_write_block(
            &mut self,
            _cx: &mut Context<'_>,
            id: u64,
            buf: &[u8],
        ) -> Poll<Result<(), Self::Error>> {
            if let Ok(i) = usize::try_from(id) {
                while self.blocks.len() <= i {
                    self.blocks.push([0u8; BLOCK]);
                }
                self.blocks[i].copy_from_slice(&buf[..BLOCK.min(buf.len())]);
            }
            Poll::Ready(Ok(()))
        }

        fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    /// Writes one input's rdel section at `base` from `(start, end, seq)`
    /// triples (already in merge order) and returns its `TableRef`.
    fn write_section<const BLOCK: usize>(
        dev: &mut TestDevice<BLOCK>,
        base: u64,
        entries: &[(&[u8], &[u8], u64)],
    ) -> TableRef<256> {
        let rdel_blocks = block_on(sstable::write_rdel_blocks::<_, BLOCK>(
            dev,
            base,
            entries.iter().map(|(s, e, seq)| sstable::RdelEntry {
                start: s,
                end: e,
                seq: *seq,
            }),
        ))
        .unwrap();
        // A range-only table: no data blocks, so the rdel section starts at
        // `base` (bloom, index, and footer would follow it).
        let mut t = TableRef::<256>::EMPTY;
        t.first_block = base;
        t.rdel_blocks = rdel_blocks;
        t.block_count = rdel_blocks + 3;
        t
    }

    /// Drives a merge to exhaustion, collecting `(start, end, seq)`.
    fn run_merge<const BLOCK: usize>(
        dev: &TestDevice<BLOCK>,
        inputs: &[Input<256>],
        bottommost: bool,
        oldest_snapshot: u64,
    ) -> Vec<(Vec<u8>, Vec<u8>, u64)> {
        let mut merger = RdelMerger::<256>::new(inputs, 0, false, bottommost, oldest_snapshot);
        let mut raw = [0u8; BLOCK];
        let mut out = Vec::new();
        let none = TargetView {
            level: &[],
            ids: &[],
        };
        while block_on(merger.next_merged(dev, &mut raw, &none)).unwrap() {
            let e = merger.current_entry();
            out.push((e.start.to_vec(), e.end.to_vec(), e.seq));
        }
        out
    }

    fn input(t: &TableRef<256>) -> Input<256> {
        Input { level: 0, tref: *t }
    }

    #[test]
    fn rdel_merge_orders_coalesces_and_dedups() {
        let mut dev = TestDevice::<4096>::new();
        // Input 0: [a,c)@9 and [m,s)@5.
        let t0 = write_section(&mut dev, 10, &[(b"a", b"c", 9), (b"m", b"s", 5)]);
        // Input 1: [b,d)@7, an exact duplicate of [m,s)@5, and [n,t)@5
        // (same seq, overlapping -> coalesces to [m,t)@5).
        let t1 = write_section(
            &mut dev,
            20,
            &[(b"b", b"d", 7), (b"m", b"s", 5), (b"n", b"t", 5)],
        );
        let inputs = [input(&t0), input(&t1)];
        let got = run_merge(&dev, &inputs, false, 0);
        assert_eq!(
            got,
            vec![
                (b"a".to_vec(), b"c".to_vec(), 9),
                (b"b".to_vec(), b"d".to_vec(), 7),
                (b"m".to_vec(), b"t".to_vec(), 5),
            ]
        );
    }

    #[test]
    fn rdel_merge_never_drops_the_newer_of_identical_ranges_arriving_oldest_first() {
        // A compaction output whose pieces were clipped up to its lower
        // bound holds ties on `start` in the order of their original
        // starts, which can be oldest first: here [k,p)@285 then [k,p)@355.
        // Bottommost with every snapshot newer, identical ranges may fold —
        // but only the *older* may go. Dropping @355 as "shadowed" by the
        // earlier-emitted @285 resurrected every version between the two
        // sequences (found by the tight-slot lifecycle fuzzer).
        let mut dev = TestDevice::<4096>::new();
        let t = write_section(&mut dev, 10, &[(b"k", b"p", 285), (b"k", b"p", 355)]);
        let got = run_merge(&dev, &[input(&t)], true, 474);
        assert!(
            got.iter()
                .any(|(s, e, q)| s == b"k" && e == b"p" && *q == 355),
            "the newer tombstone was dropped: {got:?}"
        );
        // In the usual order (newest first) the older one does fold away.
        let t = write_section(&mut dev, 30, &[(b"k", b"p", 355), (b"k", b"p", 285)]);
        let got = run_merge(&dev, &[input(&t)], true, 474);
        assert_eq!(got, vec![(b"k".to_vec(), b"p".to_vec(), 355)]);
    }

    #[test]
    fn rdel_merge_same_start_orders_seq_desc_and_keeps_both() {
        let mut dev = TestDevice::<4096>::new();
        let t0 = write_section(&mut dev, 10, &[(b"k", b"p", 3)]);
        let t1 = write_section(&mut dev, 20, &[(b"k", b"p", 8)]);
        let inputs = [input(&t0), input(&t1)];
        // Same (start, end), different seq: both survive, newest first —
        // a snapshot between the seqs still needs the older one.
        let got = run_merge(&dev, &inputs, false, 0);
        assert_eq!(
            got,
            vec![
                (b"k".to_vec(), b"p".to_vec(), 8),
                (b"k".to_vec(), b"p".to_vec(), 3),
            ]
        );
    }

    #[test]
    fn rdel_merge_never_drops_newest_range() {
        // A range tombstone that is the newest covering its range is never
        // dropped — not even bottommost below the oldest snapshot. Unlike a
        // point tombstone (whose whole key goes), dropping it would
        // resurrect covered values for live snapshots and the live view.
        let mut dev = TestDevice::<4096>::new();
        let t0 = write_section(&mut dev, 10, &[(b"a", b"b", 5), (b"c", b"d", 12)]);
        let inputs = [input(&t0)];
        // Bottommost with oldest snapshot 10: nothing drops.
        let got = run_merge(&dev, &inputs, true, 10);
        assert_eq!(
            got,
            vec![
                (b"a".to_vec(), b"b".to_vec(), 5),
                (b"c".to_vec(), b"d".to_vec(), 12),
            ]
        );
        // Not bottommost: nothing drops either.
        let got = run_merge(&dev, &inputs, false, 10);
        assert_eq!(
            got,
            vec![
                (b"a".to_vec(), b"b".to_vec(), 5),
                (b"c".to_vec(), b"d".to_vec(), 12),
            ]
        );
    }

    #[test]
    fn rdel_merge_drops_shadowed_identical_below_snapshot() {
        // An older identical-range tombstone drops only when the newer
        // shadowing tombstone is also at or below the oldest snapshot:
        // then no live reader falls between their sequences.
        let mut dev = TestDevice::<4096>::new();
        // Identical [a,z) at seq 5 and seq 8, plus an unrelated range.
        let t0 = write_section(&mut dev, 10, &[(b"a", b"z", 5)]);
        let t1 = write_section(&mut dev, 20, &[(b"a", b"z", 8), (b"m", b"n", 3)]);
        let inputs = [input(&t0), input(&t1)];
        // Oldest snapshot 8: the @8 shadow is visible to every live reader,
        // so @5 drops; the unrelated @3 is newest for its range and stays.
        let got = run_merge(&dev, &inputs, true, 8);
        assert_eq!(
            got,
            vec![
                (b"a".to_vec(), b"z".to_vec(), 8),
                (b"m".to_vec(), b"n".to_vec(), 3),
            ]
        );
        // Oldest snapshot 7: a live snapshot sits between the sequences,
        // so @5 must stay.
        let got = run_merge(&dev, &inputs, true, 7);
        assert_eq!(
            got,
            vec![
                (b"a".to_vec(), b"z".to_vec(), 8),
                (b"a".to_vec(), b"z".to_vec(), 5),
                (b"m".to_vec(), b"n".to_vec(), 3),
            ]
        );
        // Not bottommost: the shadow gate never opens.
        let got = run_merge(&dev, &inputs, false, 8);
        assert_eq!(
            got,
            vec![
                (b"a".to_vec(), b"z".to_vec(), 8),
                (b"a".to_vec(), b"z".to_vec(), 5),
                (b"m".to_vec(), b"n".to_vec(), 3),
            ]
        );
    }

    #[test]
    fn rdel_merge_spans_multiple_blocks_per_input() {
        // BLOCK=64: a 16-byte entry leaves ~3 per block, so 10 disjoint
        // entries span 4 blocks per input and stress head refill.
        let mut dev = TestDevice::<64>::new();
        let mk = |i: u8| [b'k', b'0' + i / 10, b'0' + i % 10];
        let e0: Vec<([u8; 3], [u8; 3], u64)> = (0..10)
            .map(|i| (mk(i * 2), mk(i * 2 + 1), u64::from(i)))
            .collect();
        let e1: Vec<([u8; 3], [u8; 3], u64)> = (0..10)
            .map(|i| (mk(i * 2 + 1), mk(i * 2 + 2), 100 + u64::from(i)))
            .collect();
        // Interleaved starts: input 1's entries sort between input 0's.
        let t0 = write_section(
            &mut dev,
            10,
            &e0.iter()
                .map(|(s, e, q)| (&s[..], &e[..], *q))
                .collect::<Vec<_>>(),
        );
        assert!(t0.rdel_blocks > 1);
        let t1 = write_section(
            &mut dev,
            50,
            &e1.iter()
                .map(|(s, e, q)| (&s[..], &e[..], *q))
                .collect::<Vec<_>>(),
        );
        assert!(t1.rdel_blocks > 1);
        let inputs = [input(&t0), input(&t1)];
        let got = run_merge(&dev, &inputs, false, 0);
        assert_eq!(got.len(), 20);
        // Merged output is sorted by (start asc); seqs are unique here.
        for w in got.windows(2) {
            assert!(w[0].0 < w[1].0, "out of order: {w:?}");
        }
    }

    #[test]
    fn rdel_blocks_bound_covers_the_writer() {
        // `rdel_blocks_bound` must never undercount what the writer packs,
        // for any mix of entry sizes up to the maximum (12 + 2 * KEY_MAX).
        // 64-byte blocks and KEY_MAX = 8: at most 28-byte entries, two per
        // block.
        assert_eq!(sstable::rdel_entries_per_block::<64, 8>(), 2);
        let mut rng = 7u64;
        for n in 0u8..30 {
            let mut dev = TestDevice::<64>::new();
            let mut w = sstable::RdelWriter::<64>::new(200);
            for i in 0..n {
                rng = rng.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                let sl = 1 + usize::try_from(rng >> 61).unwrap(); // 1..=8
                let el = 1 + usize::try_from((rng >> 58) & 7).unwrap();
                let start = vec![b'a'.wrapping_add(i); sl];
                let end = vec![b'z'; el];
                block_on(w.push(
                    &mut dev,
                    sstable::RdelEntry {
                        start: &start,
                        end: &end,
                        seq: u64::from(i),
                    },
                ))
                .unwrap();
            }
            let written = block_on(w.finish(&mut dev)).unwrap();
            let bound = rdel_blocks_bound::<64, 8>(u64::from(n));
            assert!(
                u64::from(written) <= bound,
                "{n} entries: {written} > {bound}"
            );
        }
        // Maximum-size entries meet the bound exactly.
        let mut dev = TestDevice::<64>::new();
        let mut w = sstable::RdelWriter::<64>::new(200);
        for i in 0..5u8 {
            block_on(w.push(
                &mut dev,
                sstable::RdelEntry {
                    start: &[i; 8],
                    end: &[0xFF; 8],
                    seq: u64::from(i),
                },
            ))
            .unwrap();
        }
        assert_eq!(u64::from(block_on(w.finish(&mut dev)).unwrap()), 3);
        assert_eq!(rdel_blocks_bound::<64, 8>(5), 3);
        assert_eq!(rdel_blocks_bound::<64, 8>(0), 0);
    }

    #[test]
    fn rdel_writer_refuses_to_pass_its_block_limit() {
        let mut dev = TestDevice::<64>::new();
        let mut w = sstable::RdelWriter::<64>::new(200).with_block_limit(1);
        let e = |i: u8| sstable::RdelEntry {
            start: &[b'a'; 8],
            end: &[b'z'; 8],
            seq: u64::from(i),
        };
        block_on(w.push(&mut dev, e(0))).unwrap();
        block_on(w.push(&mut dev, e(1))).unwrap();
        // The third entry would seal block 0 and open block 1: allowed.
        block_on(w.push(&mut dev, e(2))).unwrap();
        // Sealing block 1 would pass the limit.
        assert!(matches!(block_on(w.finish(&mut dev)), Err(Error::NoSpace)));
    }

    #[test]
    fn rdel_merge_empty_inputs_yield_nothing() {
        let mut dev = TestDevice::<4096>::new();
        let t0 = write_section(&mut dev, 10, &[]);
        assert_eq!(t0.rdel_blocks, 0);
        let inputs = [input(&t0)];
        assert_eq!(run_merge(&dev, &inputs, false, 0), Vec::new());
        assert_eq!(run_merge(&dev, &[], false, 0), Vec::new());
    }
}
