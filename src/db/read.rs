//! Point reads: the memtable, then every table newest-first, highest
//! *version* wins (sequence number within one node, `(seal_wall,
//! node_id)` across nodes — see [`crate::version`]).
//!
//! A read has two phases, and they need different things:
//!
//! 1. **The memtable probe** ([`Db::probe_memtable`]) needs the `Db`. It
//!    is a search of RAM, with no I/O, and yields a [`MemProbe`].
//! 2. **The table phase** ([`TableView::resolve`]) needs a manifest, a
//!    device to read blocks from, and two block buffers. It does all the
//!    I/O, and it does not need the `Db`.
//!
//! [`Db::get`] and its variants run both over the `Db`'s own manifest,
//! device, buffers and cache. A caller can also run them apart: take the
//! probe and a copy of [`Db::manifest`] from the `Db`, then resolve the
//! probe anywhere, over any handle to the same device, with buffers of
//! its own. That is a *detached* read (ADR-0018). On a host it lets many
//! threads read tables at once while one thread owns the `Db`; on an
//! async executor it lets a read wait for flash without holding a borrow
//! of the `Db`.

use super::Db;
use crate::cache::CachePort;
use crate::device::BlockDevice;
use crate::error::Error;
use crate::manifest::{Manifest, TableRef};
use crate::sstable;
use crate::version::{Version, version_gt};

/// Best hit seen so far by [`Db::get`]: the highest-version lookup
/// result. When `Value`, the winning bytes are staged in `get`'s staging
/// buffer.
#[derive(Debug, Clone, Copy)]
enum Best {
    /// No hit yet.
    Missing,
    /// A tombstone beat every value so far.
    Tombstone,
    /// A value won; holds its byte length.
    Value(usize),
}

/// Accumulator for [`Db::get_at`]'s multi-table read: the winning staged
/// value bytes plus the version that won them. Bundled into one struct so
/// `consider_table` stays under the argument-count lint. While a value is
/// staged, a table lookup copies into a spare buffer and only a winning
/// hit is promoted into `stage`, so a losing hit can never clobber the
/// winner. With nothing staged, a lookup writes `stage` directly: its
/// bytes are read only once a hit has won (`best` says how many).
#[derive(Debug, Clone, Copy)]
struct ReadAcc<const VAL_MAX: usize> {
    stage: [u8; VAL_MAX],
    best: Best,
    best_ver: Version,
    /// Expiry tick of the winning value; 0 = no expiry. Checked against
    /// the caller's `now` before the value is returned.
    best_expire_at: u64,
    /// Highest range-tombstone version covering the key at/below the
    /// snapshot, across the memtable and every considered table. Beats
    /// the point winner when strictly newer under the merge rule
    /// (versions are unique per mutation, so equality cannot happen).
    cover_ver: Version,
}

impl<const VAL_MAX: usize> ReadAcc<VAL_MAX> {
    /// Back to "nothing found yet". The staging buffer keeps its bytes:
    /// they are read only up to the length a later winner records.
    const fn clear(&mut self) {
        self.best = Best::Missing;
        self.best_ver = Version::ZERO;
        self.best_expire_at = 0;
        self.cover_ver = Version::ZERO;
    }

    const fn new() -> Self {
        Self {
            stage: [0u8; VAL_MAX],
            best: Best::Missing,
            best_ver: Version::ZERO,
            best_expire_at: 0,
            cover_ver: Version::ZERO,
        }
    }
}

/// A point read that has consulted the memtable and not yet the tables:
/// the first half of a detached read. Made by [`Db::probe_memtable`],
/// finished by [`TableView::resolve`].
///
/// It is plain data: the key, the snapshot it reads at, and what the
/// memtable held for the key (the newest visible version and its value,
/// and the newest range tombstone covering it). It borrows nothing, so
/// it can outlive the borrow of the `Db` that made it and cross to
/// another thread or task.
///
/// A probe is the memtable *as of the call*. Resolved against the tables
/// as of the same instant, it gives the answer [`Db::get_at`] would have
/// given then. See [`TableView`] for what must hold in between.
#[derive(Debug, Clone, Copy)]
pub struct MemProbe<const KEY_MAX: usize, const VAL_MAX: usize> {
    key: [u8; KEY_MAX],
    key_len: usize,
    max_seq: u64,
    acc: ReadAcc<VAL_MAX>,
}

impl<const KEY_MAX: usize, const VAL_MAX: usize> Default for MemProbe<KEY_MAX, VAL_MAX> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const KEY_MAX: usize, const VAL_MAX: usize> MemProbe<KEY_MAX, VAL_MAX> {
    /// An empty probe, for [`Db::probe_memtable_into`] to fill. A probe
    /// is `KEY_MAX + VAL_MAX` bytes and more: a reader that makes many
    /// reads keeps one and fills it each time, where
    /// [`Db::probe_memtable`] builds (and zeroes) a new one per read.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            key: [0u8; KEY_MAX],
            key_len: 0,
            max_seq: 0,
            acc: ReadAcc::new(),
        }
    }

    /// Makes this the probe of a key the memtable holds nothing for: no
    /// version of it at any sequence number, and no range tombstone
    /// covering it. That is exactly what [`Db::probe_memtable_into`]
    /// leaves when the memtable has neither, so
    /// [`TableView::resolve`] then answers from the tables alone.
    ///
    /// For a host that can tell, without the `Db`, that the memtable
    /// cannot hold a key (say, from a filter over the keys written since
    /// the last flush), and wants such a read not to wait for whoever
    /// has the `Db`. **The claim is the caller's.** If the memtable does
    /// hold a version of the key, or a range tombstone over it, a read
    /// resolved from this probe misses it and returns what the tables
    /// say: an older value, or a deleted one.
    ///
    /// Returns `false`, leaving the probe empty, when `key` is longer
    /// than `KEY_MAX`.
    pub fn set_absent(&mut self, key: &[u8], max_seq: u64) -> bool {
        self.key_len = 0;
        self.max_seq = max_seq;
        self.acc.clear();
        let Some(dst) = self.key.get_mut(..key.len()) else {
            return false;
        };
        dst.copy_from_slice(key);
        self.key_len = key.len();
        true
    }

    /// The key this probe reads.
    #[must_use]
    pub fn key(&self) -> &[u8] {
        self.key.get(..self.key_len).unwrap_or(&[])
    }

    /// The snapshot this probe reads at (`u64::MAX`: the latest view).
    #[must_use]
    pub const fn max_seq(&self) -> u64 {
        self.max_seq
    }

    /// True when the memtable held a version of the key (a value or a
    /// tombstone) visible at the snapshot. A memtable version is newer
    /// than everything in the tables, so the answer is already decided:
    /// [`TableView::resolve`] returns it without reading any block.
    #[must_use]
    pub const fn is_settled(&self) -> bool {
        !matches!(self.acc.best, Best::Missing)
    }
}

/// Caller-owned block buffers for the table phase of a detached read:
/// one for a block as stored and one to inflate it into. `2 * BLOCK`
/// bytes; keep one per reader (per thread, or per task).
pub struct ReadScratch<const BLOCK: usize> {
    raw: [u8; BLOCK],
    decomp: [u8; BLOCK],
}

impl<const BLOCK: usize> ReadScratch<BLOCK> {
    /// Zeroed buffers. `const`, so they can sit in a `static` or a
    /// `const`-built struct.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            raw: [0u8; BLOCK],
            decomp: [0u8; BLOCK],
        }
    }
}

impl<const BLOCK: usize> Default for ReadScratch<BLOCK> {
    fn default() -> Self {
        Self::new()
    }
}

/// The tables of a database, for reading without the `Db`: a manifest,
/// a device to read its tables' blocks from, and optionally a block
/// cache. The second half of a detached read (ADR-0018).
///
/// The manifest is usually a copy of [`Db::manifest`] (it is `Copy`),
/// and the device any handle that reads the same blocks: the `Db`'s own
/// device, or a second handle to the same file or flash region. Nothing
/// here borrows the `Db`, so the `Db` stays free for its owner.
///
/// # What the caller must hold to
///
/// A table's blocks must not be rewritten while a view whose manifest
/// names the table is reading them. Tables are immutable, but a table
/// that compaction retires gives its slot back, and a later table is
/// written there. The `Db` cannot see a detached reader, so the caller
/// keeps these apart: **from taking the probe and the manifest copy
/// until [`resolve`](Self::resolve) returns, do not run
/// [`flush`](Db::flush), [`compact_step`](Db::compact_step),
/// [`ingest_table`](Db::ingest_table) or
/// [`archive_commit`](Db::archive_commit) on the `Db`.** Those are the
/// calls that write the table region or change which tables exist.
/// `put`, `delete`, `write`, `delete_range` and everything else that
/// only reaches the WAL and the memtable are safe to run meanwhile, and
/// do not change the read's answer.
///
/// Take the probe and the manifest copy with nothing in between that
/// changes the tables (compare [`Manifest::seq`] to tell whether a copy
/// is still current). A reader that breaks the rule can read a block of
/// a different table: every block still passes its CRC, so the result
/// is a wrong answer or a `CorruptBlock`, not memory unsafety.
///
/// The cache, if one is given, must be one that other threads are not
/// using through a `RefCell` (the `Db`'s own cache is not reachable
/// from here for that reason). Give each reader its own
/// [`BlockCache`](crate::BlockCache), or implement [`CachePort`] over a
/// lock.
pub struct TableView<
    'a,
    R: BlockDevice,
    const BLOCK: usize,
    const KEY_MAX: usize,
    const VAL_MAX: usize,
    const LEVELS: usize,
    const TABLES: usize,
    const BLOOM_BYTES: usize,
> {
    device: &'a R,
    manifest: &'a Manifest<LEVELS, TABLES, KEY_MAX>,
    cache: Option<&'a dyn CachePort<BLOCK>>,
}

impl<
    'a,
    R: BlockDevice,
    const BLOCK: usize,
    const KEY_MAX: usize,
    const VAL_MAX: usize,
    const LEVELS: usize,
    const TABLES: usize,
    const BLOOM_BYTES: usize,
> TableView<'a, R, BLOCK, KEY_MAX, VAL_MAX, LEVELS, TABLES, BLOOM_BYTES>
{
    const ASSERT_BLOCK: () = assert!(BLOCK == R::BLOCK, "BLOCK must equal R::BLOCK");

    /// A view of `manifest`'s tables, read from `device` through `cache`
    /// (`None`: every block comes from the device).
    #[must_use]
    pub const fn new(
        device: &'a R,
        manifest: &'a Manifest<LEVELS, TABLES, KEY_MAX>,
        cache: Option<&'a dyn CachePort<BLOCK>>,
    ) -> Self {
        // Associated consts are lazy: referencing it here forces the
        // check for every instantiation.
        let () = Self::ASSERT_BLOCK;
        Self {
            device,
            manifest,
            cache,
        }
    }

    /// Finishes the read `probe` began: consults every table that can
    /// hold a newer version of the probe's key than the memtable did,
    /// and writes the winning value into `val_buf`. `now` is the
    /// caller's clock for TTL (`0`: nothing expires), as in
    /// [`Db::get_with_time`].
    ///
    /// Returns what [`Db::get_at_with_time`] would have returned when
    /// the probe was taken, provided this view's manifest is the `Db`'s
    /// manifest of that moment (see the type's docs).
    ///
    /// # Errors
    ///
    /// [`Error::BufferTooSmall`] when `val_buf` is smaller than the
    /// winning value, [`Error::CorruptManifest`] when a table's block
    /// range is malformed, [`Error::CorruptBlock`] when a block fails
    /// verification, or [`Error::Device`] on I/O failure.
    pub async fn resolve(
        &self,
        probe: &MemProbe<KEY_MAX, VAL_MAX>,
        scratch: &mut ReadScratch<BLOCK>,
        val_buf: &mut [u8],
        now: u64,
    ) -> Result<Option<usize>, Error<R::Error>> {
        let mut acc = probe.acc;
        self.finish(
            probe.key(),
            probe.max_seq,
            &mut acc,
            &mut scratch.raw,
            &mut scratch.decomp,
            val_buf,
            now,
            None,
        )
        .await
    }

    /// The table phase of the point-read rule, shared by [`Db::get`]'s
    /// variants, the archive resurrection review and
    /// [`resolve`](Self::resolve): level 0 newest table first, then
    /// deeper levels; the highest *version* at or below `max_seq` wins
    /// (see [`crate::version`]), a strictly newer covering range
    /// tombstone hides it, and a winner expired at `now` reads as
    /// missing (`now = 0`: nothing expires). `acc` carries what the
    /// memtable held. `exclude` names one table to leave out (the
    /// archival candidate), or `None`.
    #[allow(clippy::too_many_arguments)] // one call site per caller; a struct would only rename them.
    async fn finish(
        &self,
        key: &[u8],
        max_seq: u64,
        acc: &mut ReadAcc<VAL_MAX>,
        scratch: &mut [u8; BLOCK],
        decomp: &mut [u8; BLOCK],
        val_buf: &mut [u8],
        now: u64,
        exclude: Option<u32>,
    ) -> Result<Option<usize>, Error<R::Error>> {
        // Level 0, newest table first: its tables overlap, and newer tables
        // hold higher sequence numbers. Deeper levels in order:
        // highest-version-wins keeps the result exact however tables are
        // placed.
        for li in 0..LEVELS {
            let tables = self.manifest.level(li).unwrap_or(&[]);
            let n = tables.len();
            for i in 0..n {
                let tref = &tables[if li == 0 { n - 1 - i } else { i }];
                if Some(tref.id) != exclude {
                    self.consider_table(tref, key, max_seq, scratch, decomp, acc)
                        .await?;
                }
            }
        }
        match acc.best {
            Best::Missing | Best::Tombstone => Ok(None),
            Best::Value(len) => {
                // A covering range tombstone newer than the point winner
                // hides the key; an expired winner reads as missing and
                // never falls through to an older version.
                if version_gt(acc.cover_ver, acc.best_ver) {
                    return Ok(None);
                }
                if acc.best_expire_at != 0 && acc.best_expire_at <= now {
                    return Ok(None);
                }
                if len > val_buf.len() {
                    return Err(Error::BufferTooSmall { need: len });
                }
                val_buf[..len].copy_from_slice(&acc.stage[..len]);
                Ok(Some(len))
            }
        }
    }

    /// Considers one table: key-range prune, version prune, then a
    /// bloom-gated lookup. A hit with a higher version than the best so
    /// far — and visible at `max_seq` — is promoted into `acc`.
    async fn consider_table(
        &self,
        tref: &TableRef<KEY_MAX>,
        key: &[u8],
        max_seq: u64,
        scratch: &mut [u8; BLOCK],
        decomp: &mut [u8; BLOCK],
        acc: &mut ReadAcc<VAL_MAX>,
    ) -> Result<(), Error<R::Error>> {
        // Both prunes are exact: the table's keys all lie within its bounds,
        // and no entry here can carry a version above the table's
        // `(node, wall, max_seq)` bound.
        let bound = Version::table(tref.node_id, tref.seal_wall, tref.max_seq);
        if !tref.covers(key) || !version_gt(bound, acc.best_ver) {
            return Ok(());
        }
        // From the manifest record, with no I/O: the footer says nothing
        // the record does not.
        let reader =
            sstable::TableReader::<R, BLOCK, BLOOM_BYTES>::from_ref(self.device, self.cache, tref)?;
        // A hit wins if it is visible at `max_seq` and newer than the
        // best so far.
        let wins = |seq: u64, best: Version| {
            seq <= max_seq && version_gt(Version::table(tref.node_id, tref.seal_wall, seq), best)
        };
        // Only a winning hit may replace a staged winner, so while one is
        // staged the value goes to a spare buffer and is promoted if it
        // wins. With nothing staged (no table has given a value yet: the
        // common case, and nearly every table a read looks into),
        // `acc.stage` holds nothing to protect and the value goes straight
        // there. That spares a `VAL_MAX` buffer zeroed per table, and the
        // second copy.
        let found = if matches!(acc.best, Best::Value(_)) {
            let mut spare = [0u8; VAL_MAX];
            let found = reader
                .lookup_at(scratch, decomp, key, &mut spare, max_seq)
                .await?;
            if let sstable::Lookup::Value { len, seq, .. } = found
                && wins(seq, acc.best_ver)
            {
                acc.stage[..len].copy_from_slice(&spare[..len]);
            }
            found
        } else {
            reader
                .lookup_at(scratch, decomp, key, &mut acc.stage, max_seq)
                .await?
        };
        match found {
            sstable::Lookup::Value {
                len,
                seq,
                expire_at,
            } if wins(seq, acc.best_ver) => {
                acc.best_ver = Version::table(tref.node_id, tref.seal_wall, seq);
                acc.best = Best::Value(len);
                acc.best_expire_at = expire_at;
            }
            sstable::Lookup::Tombstone { seq } if wins(seq, acc.best_ver) => {
                acc.best_ver = Version::table(tref.node_id, tref.seal_wall, seq);
                acc.best = Best::Tombstone;
            }
            _ => {}
        }
        // A range tombstone in this table covering `key` shadows older
        // point versions; the accumulator keeps the highest covering
        // version and compares it against the point winner at the end.
        if reader.rdel_blocks() > 0
            && let Some(q) = reader.covering_rdel_seq(scratch, key, max_seq).await?
            && version_gt(
                Version::table(tref.node_id, tref.seal_wall, q),
                acc.cover_ver,
            )
        {
            acc.cover_ver = Version::table(tref.node_id, tref.seal_wall, q);
        }
        Ok(())
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
    /// Reads `key` into `val_buf`: memtable first, then level 0 newest table
    /// first, then deeper levels in order. This is
    /// [`get_at`](Db::get_at) with `max_seq = u64::MAX`: the latest view.
    ///
    /// Returns `Ok(None)` for missing keys and tombstones. Never truncates:
    /// an undersized buffer yields [`Error::BufferTooSmall`] with the
    /// required length — the length of the *winning* value, even when an
    /// older shadowed version would have fit.
    ///
    /// # Errors
    ///
    /// [`Error::BufferTooSmall`] when `val_buf` is smaller than the winning
    /// value, [`Error::CorruptManifest`] when a table's block range is
    /// malformed or its record misdescribes its filter,
    /// [`Error::CorruptBlock`] when a table's index or data block fails
    /// verification, [`Error::Busy`] when another `get` on this
    /// handle is in flight (reads share the `Db`'s block buffers), or
    /// [`Error::Device`] on I/O failure.
    pub async fn get(
        &self,
        key: &[u8],
        val_buf: &mut [u8],
    ) -> Result<Option<usize>, Error<D::Error>> {
        self.get_at_with_time(key, val_buf, u64::MAX, 0).await
    }

    /// Reads `key` into `val_buf` as of a snapshot: like [`get`](Db::get),
    /// but only mutations with `seq <= max_seq` are visible. Newer versions
    /// (including newer tombstones) are invisible, so an older value — or
    /// a missing key — can correctly win. Pass a watermark from
    /// [`snapshot`](Db::snapshot) for a pinned read, or `u64::MAX` for the
    /// latest view.
    ///
    /// Returns `Ok(None)` for missing keys and tombstones. Never truncates:
    /// an undersized buffer yields [`Error::BufferTooSmall`] with the
    /// required length — the length of the *winning* value, even when an
    /// older shadowed version would have fit.
    ///
    /// # Errors
    ///
    /// Same as [`get`](Db::get).
    pub async fn get_at(
        &self,
        key: &[u8],
        val_buf: &mut [u8],
        max_seq: u64,
    ) -> Result<Option<usize>, Error<D::Error>> {
        self.get_at_with_time(key, val_buf, max_seq, 0).await
    }

    /// Timed read: like [`get`](Db::get), but values whose `expire_at` is
    /// nonzero and `<= now` are suppressed — they read as missing (a
    /// newer live version still wins; an expired winner never falls
    /// through to an older version). Horton owns no clock: `now` is the
    /// caller's tick, compared against the absolute `expire_at` stored by
    /// [`put_with_ttl`](Db::put_with_ttl).
    ///
    /// # Errors
    ///
    /// Same as [`get`](Db::get).
    pub async fn get_with_time(
        &self,
        key: &[u8],
        val_buf: &mut [u8],
        now: u64,
    ) -> Result<Option<usize>, Error<D::Error>> {
        self.get_at_with_time(key, val_buf, u64::MAX, now).await
    }

    /// Timed read: like [`get_at`](Db::get_at), but values whose
    /// `expire_at` is nonzero and `<= now` are suppressed — they read as
    /// missing (a newer live version still wins; an expired winner never
    /// falls through to an older version). Callers without a clock pass
    /// `now = 0`, which is what [`get`](Db::get) and
    /// [`get_at`](Db::get_at) do.
    ///
    /// # Errors
    ///
    /// Same as [`get_at`](Db::get_at).
    pub async fn get_at_with_time(
        &self,
        key: &[u8],
        val_buf: &mut [u8],
        max_seq: u64,
        now: u64,
    ) -> Result<Option<usize>, Error<D::Error>> {
        self.ensure_open()?;
        self.read_point(key, val_buf, max_seq, now, None).await
    }

    /// The first phase of a point read, by itself: what the memtable
    /// holds for `key` as of `max_seq` (`u64::MAX`: the latest view). No
    /// I/O. Finish the read with [`TableView::resolve`], over a copy of
    /// [`manifest`](Db::manifest) taken at the same moment; see
    /// [`TableView`] for the rule a detached read must keep.
    ///
    /// # Errors
    ///
    /// [`Error::NotOpen`] before [`open`](Db::open), or
    /// [`Error::KeyTooLarge`] when `key` is longer than `KEY_MAX` (no
    /// stored key is, and the probe carries its key).
    pub fn probe_memtable(
        &self,
        key: &[u8],
        max_seq: u64,
    ) -> Result<MemProbe<KEY_MAX, VAL_MAX>, Error<D::Error>> {
        let mut probe = MemProbe::new();
        self.probe_memtable_into(key, max_seq, &mut probe)?;
        Ok(probe)
    }

    /// [`probe_memtable`](Db::probe_memtable) into a probe the caller
    /// already has: the same result, without building a new probe. What
    /// `probe` held before is gone, whatever it was.
    ///
    /// This is the part of a detached read that needs the `Db`, so a
    /// host that shares a `Db` between threads runs it under its lock;
    /// it is a search of RAM and a copy of at most one value.
    ///
    /// # Errors
    ///
    /// As [`probe_memtable`](Db::probe_memtable). On an error `probe` is
    /// left empty: it reads no key.
    pub fn probe_memtable_into(
        &self,
        key: &[u8],
        max_seq: u64,
        probe: &mut MemProbe<KEY_MAX, VAL_MAX>,
    ) -> Result<(), Error<D::Error>> {
        // Emptied first: a probe that failed to fill must not go on
        // answering for the key it held before.
        probe.key_len = 0;
        probe.max_seq = max_seq;
        probe.acc.clear();
        self.ensure_open()?;
        probe
            .key
            .get_mut(..key.len())
            .ok_or(Error::KeyTooLarge {
                len: key.len(),
                max: KEY_MAX,
            })?
            .copy_from_slice(key);
        probe.key_len = key.len();
        self.fill_memtable_acc(key, max_seq, &mut probe.acc);
        Ok(())
    }

    /// The committed manifest: the tables at each level, as the last
    /// flush, compaction, ingest or archive left them. It is `Copy`; a
    /// detached read ([`TableView`]) runs over a copy, and
    /// [`Manifest::seq`] tells whether a copy is still current.
    #[must_use]
    pub const fn manifest(&self) -> &Manifest<LEVELS, TABLES, KEY_MAX> {
        &self.manifest
    }

    /// The memtable's share of a forward scan page, for a detached scan
    /// ([`Scan::detached`](crate::Scan::detached)): copies into `page`
    /// at most `max_keys` keys of the memtable in `[start, end)`, in
    /// order from `start`, and returns the horizon, the first key not
    /// copied. See [`MemTable::copy_range`](crate::MemTable::copy_range).
    /// No I/O.
    ///
    /// # Errors
    ///
    /// [`Error::NotOpen`] before [`open`](Db::open), or
    /// [`Error::TableFull`] / [`Error::ArenaFull`] when `page` cannot
    /// hold even one key.
    pub fn copy_memtable_range<const C2: usize, const A2: usize>(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        max_keys: usize,
        page: &mut crate::MemTable<C2, A2, KEY_MAX, VAL_MAX>,
    ) -> Result<Option<crate::KeyBound<KEY_MAX>>, Error<D::Error>> {
        self.ensure_open()?;
        self.table.copy_range(start, end, max_keys, page)
    }

    /// The memtable's share of a reverse scan page: the at most
    /// `max_keys` greatest keys at or below `from` and above `lower`.
    /// See [`MemTable::copy_range_rev`](crate::MemTable::copy_range_rev).
    /// No I/O.
    ///
    /// # Errors
    ///
    /// [`Error::NotOpen`] before [`open`](Db::open), or
    /// [`Error::TableFull`] / [`Error::ArenaFull`] when `page` cannot
    /// hold `max_keys` keys.
    pub fn copy_memtable_range_rev<const C2: usize, const A2: usize>(
        &self,
        from: &[u8],
        lower: Option<&[u8]>,
        max_keys: usize,
        page: &mut crate::MemTable<C2, A2, KEY_MAX, VAL_MAX>,
    ) -> Result<Option<crate::KeyBound<KEY_MAX>>, Error<D::Error>> {
        self.ensure_open()?;
        self.table.copy_range_rev(from, lower, max_keys, page)
    }

    /// What the memtable holds for `key` at `max_seq`: the newest visible
    /// version, staged, and the newest covering range tombstone.
    fn memtable_acc(&self, key: &[u8], max_seq: u64) -> ReadAcc<VAL_MAX> {
        let mut acc = ReadAcc::<VAL_MAX>::new();
        self.fill_memtable_acc(key, max_seq, &mut acc);
        acc
    }

    /// [`memtable_acc`](Self::memtable_acc) into an accumulator the
    /// caller has: every field is set, and the staging buffer is written
    /// only as far as the winning value goes (nothing past that length
    /// is ever read).
    fn fill_memtable_acc(&self, key: &[u8], max_seq: u64, acc: &mut ReadAcc<VAL_MAX>) {
        // The winning value's bytes are staged here; a table lookup made
        // while a value is staged copies into a spare buffer first, so a
        // losing hit can never clobber the winner (see `consider_table`).
        // Values are at most VAL_MAX bytes (enforced on the write path),
        // so the staging always fits.
        acc.clear();
        if let Some(entry) = self.table.get_at(key, max_seq) {
            // The memtable holds the newest mutations; `get_at` already
            // selected the newest version at or below the snapshot, and
            // skips range-tombstone slots (they are not versions of `key`).
            // Its version is `(own_node, u64::MAX, seq)`: newer than any
            // sealed table, so read-your-writes holds under the merge rule.
            acc.best_ver = Version::memtable(self.node_id(), entry.seq);
            if entry.tombstone {
                acc.best = Best::Tombstone;
            } else {
                acc.stage[..entry.val.len()].copy_from_slice(entry.val);
                acc.best = Best::Value(entry.val.len());
                acc.best_expire_at = entry.expire_at;
            }
        }
        // A memtable range tombstone covering `key` beats any older point
        // version; the per-table probes happen in the table phase.
        if let Some(q) = self.table.max_covering_rdel(key, max_seq) {
            acc.cover_ver = Version::memtable(self.node_id(), q);
        }
    }

    /// The point-read rule, shared by every `get` variant and the archive
    /// resurrection review: the memtable, then the tables
    /// ([`TableView`]'s table phase, over this `Db`'s manifest, device
    /// and cache). `exclude` names one table to leave out (the archival
    /// candidate), or `None`.
    ///
    /// Blocks are read through the `Db`'s two shared buffers, so the
    /// future holds no block buffer of its own.
    ///
    /// # Errors
    ///
    /// [`Error::Busy`] when another read holds the shared buffers (two
    /// `get` futures polled concurrently); otherwise as [`get`](Db::get).
    // The borrows are held across this call's own awaits on purpose;
    // `try_borrow_mut` turns a concurrent second read into `Busy`, never a
    // panic.
    #[allow(clippy::await_holding_refcell_ref)]
    pub(super) async fn read_point(
        &self,
        key: &[u8],
        val_buf: &mut [u8],
        max_seq: u64,
        now: u64,
        exclude: Option<u32>,
    ) -> Result<Option<usize>, Error<D::Error>> {
        let mut acc = self.memtable_acc(key, max_seq);
        let (Ok(mut scratch), Ok(mut decomp)) = (
            self.get_scratch.try_borrow_mut(),
            self.decomp_scratch.try_borrow_mut(),
        ) else {
            return Err(Error::Busy);
        };
        TableView::<D, BLOCK, KEY_MAX, VAL_MAX, LEVELS, TABLES, BLOOM_BYTES>::new(
            self.wal.device(),
            &self.manifest,
            Some(&self.cache as &dyn CachePort<BLOCK>),
        )
        .finish(
            key,
            max_seq,
            &mut acc,
            &mut scratch,
            &mut decomp,
            val_buf,
            now,
            exclude,
        )
        .await
    }
}
