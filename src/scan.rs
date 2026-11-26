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

use crate::cache::CachePort;
use crate::db::Db;
use crate::device::BlockDevice;
use crate::error::Error;
use crate::manifest::{Manifest, TableRef};
use crate::memtable::MemTable;
use crate::sstable;
use crate::version::{Version, version_gt};

/// What a scan reads: a memtable, the tables a manifest names, a device
/// to read their blocks from and a cache for them. A scan over a `Db`
/// ([`Scan::new`]) takes all four from the `Db`. A detached scan
/// ([`Scan::detached`]) is handed them one by one, and none of them need
/// be the `Db`'s own.
struct Source<
    'd,
    D: BlockDevice,
    const BLOCK: usize,
    const KEY_MAX: usize,
    const VAL_MAX: usize,
    const CAP: usize,
    const ARENA: usize,
    const LEVELS: usize,
    const TABLES: usize,
> {
    memtable: &'d MemTable<CAP, ARENA, KEY_MAX, VAL_MAX>,
    manifest: &'d Manifest<LEVELS, TABLES, KEY_MAX>,
    device: &'d D,
    cache: Option<&'d dyn CachePort<BLOCK>>,
    /// The node the memtable's versions carry in the replica merge.
    node_id: u32,
    /// False for a `Db` that has not been opened: its memtable and
    /// manifest are not the database's yet.
    open: bool,
}

// Hand-written: a derive would demand `D: Copy`, and the device is only
// ever held by reference.
impl<
    D: BlockDevice,
    const BLOCK: usize,
    const KEY_MAX: usize,
    const VAL_MAX: usize,
    const CAP: usize,
    const ARENA: usize,
    const LEVELS: usize,
    const TABLES: usize,
> Clone for Source<'_, D, BLOCK, KEY_MAX, VAL_MAX, CAP, ARENA, LEVELS, TABLES>
{
    fn clone(&self) -> Self {
        *self
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
> Copy for Source<'_, D, BLOCK, KEY_MAX, VAL_MAX, CAP, ARENA, LEVELS, TABLES>
{
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
> Source<'d, D, BLOCK, KEY_MAX, VAL_MAX, CAP, ARENA, LEVELS, TABLES>
{
    /// Everything a scan reads, taken from `db`.
    const fn of_db<const BLOOM_BYTES: usize, const CACHE: usize>(
        db: &'d Db<D, BLOCK, KEY_MAX, VAL_MAX, CAP, ARENA, LEVELS, TABLES, BLOOM_BYTES, CACHE>,
    ) -> Self {
        Self {
            memtable: db.memtable(),
            manifest: db.manifest_ref(),
            device: db.device(),
            cache: Some(db.cache_port()),
            node_id: db.node_id(),
            open: db.is_open(),
        }
    }

    const fn ensure_open(&self) -> Result<(), Error<D::Error>> {
        if self.open {
            Ok(())
        } else {
            Err(Error::NotOpen)
        }
    }

    /// Whether a range tombstone at/below `max_seq` and newer than `above`
    /// covers `key`, across the memtable and every table with a
    /// range-tombstone section — the scans' "is the winning version hidden?"
    /// test. `above` is the winner's version; "newer" is the merge rule
    /// (see [`crate::version`]). Tables that cannot hold such a tombstone
    /// are skipped without I/O: no rdel section, no version above the
    /// winner's (exact: every version in the table is at or below its
    /// `(node, wall, max_seq)` bound), or bounds that cannot contain `key`
    /// (`last_key` carries the greatest exclusive rdel end inclusively, so
    /// that prune is a conservative superset).
    /// The search stops at the first hit. `scratch` is the scan's
    /// physical-read buffer, dead between block reads, so the scan
    /// futures hold no block buffer for it.
    async fn rdel_hides(
        &self,
        key: &[u8],
        max_seq: u64,
        above: Version,
        scratch: &mut [u8; BLOCK],
    ) -> Result<bool, Error<D::Error>> {
        if self
            .memtable
            .max_covering_rdel(key, max_seq)
            .is_some_and(|q| version_gt(Version::memtable(self.node_id, q), above))
        {
            return Ok(true);
        }
        for li in 0..LEVELS {
            let tables = self.manifest.level(li).unwrap_or(&[]);
            for tref in tables {
                // The table's best possible version: nothing in it can
                // beat `above` when even this bound does not.
                let bound = Version::table(tref.node_id, tref.seal_wall, tref.max_seq);
                if tref.rdel_blocks == 0
                    || !version_gt(bound, above)
                    || tref.first_key.as_slice() > key
                    || tref.last_key.as_slice() < key
                {
                    continue;
                }
                if sstable::covering_rdel_seq_in(
                    self.device,
                    self.cache,
                    tref.id,
                    scratch,
                    tref.rdel_first().ok_or(Error::CorruptManifest)?,
                    tref.rdel_blocks,
                    key,
                    max_seq,
                )
                .await?
                .is_some_and(|q| version_gt(Version::table(tref.node_id, tref.seal_wall, q), above))
                {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }
}

/// Part of a scan's state: its own, or kept by the caller in a
/// [`ScanState`] and lent for the scan's life ([`Scan::detached_in`]).
enum Held<'d, T> {
    Own(T),
    Lent(&'d mut T),
}

impl<T> core::ops::Deref for Held<'_, T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &T {
        match self {
            Self::Own(t) => t,
            Self::Lent(t) => t,
        }
    }
}

impl<T> core::ops::DerefMut for Held<'_, T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut T {
        match self {
            Self::Own(t) => t,
            Self::Lent(t) => t,
        }
    }
}

/// A scan's read scratch and its own block buffer.
struct BlockBufs<const BLOCK: usize> {
    /// A block as stored, before it is inflated into a buffer; also the
    /// scratch for range-tombstone probes. Dead between block reads.
    raw: [u8; BLOCK],
    /// The scan's own buffer.
    own: ScanBlock<BLOCK>,
}

impl<const BLOCK: usize> BlockBufs<BLOCK> {
    const fn new() -> Self {
        Self {
            raw: [0u8; BLOCK],
            own: ScanBlock::new(),
        }
    }
}

/// A forward scan's key buffers and cursors. Nothing in them is read
/// before a seek has written it, so a scan over a kept set starts as a
/// new one does.
struct ScanKeys<
    const KEY_MAX: usize,
    const VAL_MAX: usize,
    const LEVELS: usize,
    const TABLES: usize,
> {
    end: [u8; KEY_MAX],
    mem_key: [u8; KEY_MAX],
    mem_val: [u8; VAL_MAX],
    cursors: [[ScanCursor<KEY_MAX>; TABLES]; LEVELS],
}

impl<const KEY_MAX: usize, const VAL_MAX: usize, const LEVELS: usize, const TABLES: usize>
    ScanKeys<KEY_MAX, VAL_MAX, LEVELS, TABLES>
{
    const fn new() -> Self {
        Self {
            end: [0u8; KEY_MAX],
            mem_key: [0u8; KEY_MAX],
            mem_val: [0u8; VAL_MAX],
            cursors: [[ScanCursor::empty(); TABLES]; LEVELS],
        }
    }
}

/// The buffers and cursors of a [`Scan`], kept by the caller between
/// scans ([`Scan::detached_in`]).
///
/// A caller that scans often, over a source that changes each time,
/// builds and zeroes them once instead of for every scan: a block
/// buffer and a read scratch of `BLOCK` bytes each, three key buffers
/// and the cursors (44,000 bytes at the host store's shape of 16 KiB
/// blocks, 128-byte keys and 4 KiB values). A state holds nothing a scan reads from. What decides
/// whether a scan reads a byte of it (key lengths, cursor counts, the
/// memtable head, which block a buffer holds and which of its grains
/// were checked) lives in the scan and starts fresh with it, so a scan
/// over a kept state starts as a new scan does (ADR-0037).
pub struct ScanState<
    const BLOCK: usize,
    const KEY_MAX: usize,
    const VAL_MAX: usize,
    const LEVELS: usize,
    const TABLES: usize,
> {
    blocks: BlockBufs<BLOCK>,
    keys: ScanKeys<KEY_MAX, VAL_MAX, LEVELS, TABLES>,
}

impl<
    const BLOCK: usize,
    const KEY_MAX: usize,
    const VAL_MAX: usize,
    const LEVELS: usize,
    const TABLES: usize,
> ScanState<BLOCK, KEY_MAX, VAL_MAX, LEVELS, TABLES>
{
    /// An empty set. `const`, so it can be built in place.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            blocks: BlockBufs::new(),
            keys: ScanKeys::new(),
        }
    }
}

impl<
    const BLOCK: usize,
    const KEY_MAX: usize,
    const VAL_MAX: usize,
    const LEVELS: usize,
    const TABLES: usize,
> Default for ScanState<BLOCK, KEY_MAX, VAL_MAX, LEVELS, TABLES>
{
    fn default() -> Self {
        Self::new()
    }
}

/// A reverse scan's key buffers and cursors: as [`ScanKeys`].
struct RevScanKeys<
    const KEY_MAX: usize,
    const VAL_MAX: usize,
    const LEVELS: usize,
    const TABLES: usize,
> {
    ceil: [u8; KEY_MAX],
    lower: [u8; KEY_MAX],
    mem_key: [u8; KEY_MAX],
    mem_val: [u8; VAL_MAX],
    cursors: [[RevCursor<KEY_MAX>; TABLES]; LEVELS],
}

impl<const KEY_MAX: usize, const VAL_MAX: usize, const LEVELS: usize, const TABLES: usize>
    RevScanKeys<KEY_MAX, VAL_MAX, LEVELS, TABLES>
{
    const fn new() -> Self {
        Self {
            ceil: [0u8; KEY_MAX],
            lower: [0u8; KEY_MAX],
            mem_key: [0u8; KEY_MAX],
            mem_val: [0u8; VAL_MAX],
            cursors: [[RevCursor::empty(); TABLES]; LEVELS],
        }
    }
}

/// The buffers and cursors of a [`RevScan`], kept apart from it, as
/// [`ScanState`] is for a forward scan ([`RevScan::detached_in`]).
pub struct RevScanState<
    const BLOCK: usize,
    const KEY_MAX: usize,
    const VAL_MAX: usize,
    const LEVELS: usize,
    const TABLES: usize,
> {
    blocks: BlockBufs<BLOCK>,
    keys: RevScanKeys<KEY_MAX, VAL_MAX, LEVELS, TABLES>,
}

impl<
    const BLOCK: usize,
    const KEY_MAX: usize,
    const VAL_MAX: usize,
    const LEVELS: usize,
    const TABLES: usize,
> RevScanState<BLOCK, KEY_MAX, VAL_MAX, LEVELS, TABLES>
{
    /// An empty set. `const`, so it can be built in place.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            blocks: BlockBufs::new(),
            keys: RevScanKeys::new(),
        }
    }
}

impl<
    const BLOCK: usize,
    const KEY_MAX: usize,
    const VAL_MAX: usize,
    const LEVELS: usize,
    const TABLES: usize,
> Default for RevScanState<BLOCK, KEY_MAX, VAL_MAX, LEVELS, TABLES>
{
    fn default() -> Self {
        Self::new()
    }
}

/// A block buffer lent to a scan: see [`Scan::use_block_pool`]. `BLOCK`
/// bytes and a tag.
pub struct ScanBlock<const BLOCK: usize> {
    /// The device block decoded here, if any.
    id: Option<u64>,
    /// When it was last used, on the scan's own counter.
    used: u64,
    /// Which grains of `data` hold checked bytes of block `id`, one bit
    /// per [`grain`] (ADR-0035). [`full`] once the whole block is in.
    /// Meaningful only while `id` is `Some`.
    valid: u64,
    data: [u8; BLOCK],
}

impl<const BLOCK: usize> ScanBlock<BLOCK> {
    /// An empty buffer. `const`, so a pool can sit in a `static` or be
    /// built in place.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            id: None,
            used: 0,
            valid: 0,
            data: [0u8; BLOCK],
        }
    }
}

impl<const BLOCK: usize> ScanBlock<BLOCK> {
    /// Drops its tag: it holds no block.
    const fn forget(&mut self) {
        self.id = None;
        self.valid = 0;
    }
}

impl<const BLOCK: usize> Default for ScanBlock<BLOCK> {
    fn default() -> Self {
        Self::new()
    }
}

/// The bytes one bit of [`ScanBlock::valid`] stands for: a 64th of the
/// block, rounded up (256 bytes at 16 KiB, 64 at 4 KiB).
const fn grain<const BLOCK: usize>() -> usize {
    let g = BLOCK.div_ceil(64);
    if g == 0 { 1 } else { g }
}

/// The grains a block has.
const fn grains<const BLOCK: usize>() -> usize {
    BLOCK.div_ceil(grain::<BLOCK>())
}

/// The bitmap of a block whose every grain is in.
const fn full<const BLOCK: usize>() -> u64 {
    let n = grains::<BLOCK>();
    if n >= 64 { u64::MAX } else { (1u64 << n) - 1 }
}

/// The grains overlapping bytes `from..to` (`from < to <= BLOCK`).
const fn mask<const BLOCK: usize>(from: usize, to: usize) -> u64 {
    let g = grain::<BLOCK>();
    let first = from / g;
    let last = (to - 1) / g;
    let upto = if last >= 63 {
        u64::MAX
    } else {
        (1u64 << (last + 1)) - 1
    };
    upto & !((1u64 << first) - 1)
}

/// What a block parser asks before it acts on bytes of the block it is
/// handed: their key, their lengths, their sequence number, a restart
/// offset, a run of padding. `check` says whether bytes `from..to` may
/// be acted on; a parser that gets `false` stops with
/// [`Error::CorruptBlock`], which its driver then judges (see
/// [`Blocks::load_with`]). `ahead` names bytes the next step will
/// likely read, for a lend to take while it can (ADR-0035).
///
/// An enum rather than a trait object: a scan checks every entry it
/// steps over, and over a whole buffer (`Open`, the common case) a check
/// is then one predictable branch, not a call.
enum Gate<'a, 'c, const BLOCK: usize> {
    /// A whole verified block: every range inside it holds.
    Open,
    /// A scan buffer holding part of its block.
    Held(Gated<BLOCK>),
    /// A cache's lent image, taken into a buffer as it is checked.
    Lent(CopyIn<'a, 'c, BLOCK>),
}

impl<const BLOCK: usize> Gate<'_, '_, BLOCK> {
    /// Whether bytes `from..to` may be acted on.
    #[inline]
    fn check(&mut self, from: usize, to: usize) -> bool {
        match self {
            Self::Open => to <= from || to <= BLOCK,
            Self::Held(g) => g.check(from, to),
            Self::Lent(c) => c.check(from, to),
        }
    }

    /// Whether `ahead` does anything: only a lend takes bytes ahead.
    const fn takes_ahead(&self) -> bool {
        matches!(self, Self::Lent(_))
    }

    /// Bytes `from..to` the scan's next step will likely read.
    fn ahead(&mut self, from: usize, to: usize) {
        if let Self::Lent(c) = self {
            // A failed check is sticky: the lend becomes a miss.
            let _ = c.check(from, to);
        }
    }

    /// Whether a check over a buffer found a grain it lacks.
    #[cfg(test)]
    const fn missed(&self) -> bool {
        matches!(self, Self::Held(Gated { missed: true, .. }))
    }
}

/// A gate over a scan buffer: a range holds when every grain it touches
/// holds checked bytes. `missed` records that one did not, so that the
/// driver takes the rest of the block and parses again.
struct Gated<const BLOCK: usize> {
    valid: u64,
    missed: bool,
}

impl<const BLOCK: usize> Gated<BLOCK> {
    const fn check(&mut self, from: usize, to: usize) -> bool {
        if to <= from {
            return true;
        }
        let m = if to > BLOCK {
            0
        } else {
            mask::<BLOCK>(from, to)
        };
        let held = to <= BLOCK && self.valid & m == m;
        self.missed |= !held;
        held
    }
}

/// A gate over a cache's lent image: a range not yet taken is checked
/// against the cache's RAM checksums, a grain at a time, and copied into
/// the scan's buffer right after its check held. Only bytes copied that
/// way are ever marked valid: a check vouches for the image it just
/// summed, never for what the buffer already holds (ADR-0035).
struct CopyIn<'a, 'c, const BLOCK: usize> {
    image: &'a [u8; BLOCK],
    data: &'a mut [u8; BLOCK],
    valid: u64,
    check: &'a mut (dyn FnMut(usize, usize) -> bool + 'c),
}

impl<const BLOCK: usize> CopyIn<'_, '_, BLOCK> {
    fn check(&mut self, from: usize, to: usize) -> bool {
        if to <= from {
            return true;
        }
        if to > BLOCK {
            return false;
        }
        let m = mask::<BLOCK>(from, to);
        if self.valid & m == m {
            return true;
        }
        let g = grain::<BLOCK>();
        let lo = from / g * g;
        let hi = to.div_ceil(g).saturating_mul(g).min(BLOCK);
        if !(self.check)(lo, hi) {
            return false;
        }
        self.data[lo..hi].copy_from_slice(&self.image[lo..hi]);
        self.valid |= m;
        true
    }
}

/// A scan's block buffers. A scan keeps one cursor per table but, by
/// default, one decoded block for all of them: whenever the merge turns
/// to another table's cursor, that table's block is fetched and inflated
/// again. That keeps a scan small (ADR-0002's point), and it is most of
/// a scan's time when tables overlap. A caller with memory to spare
/// lends the scan more buffers, and each cursor's block then stays
/// decoded while the merge is elsewhere.
///
/// A buffer may hold only part of its block: the grains a lend from the
/// cache checked and copied in ([`ScanBlock::valid`], ADR-0035). Every
/// parser reads a buffer through a [`Gate`], so it never acts on a byte
/// that was not checked.
struct Blocks<'d, const BLOCK: usize> {
    /// The read scratch and the scan's own buffer: the scan's, or kept
    /// by the caller in a [`ScanState`].
    bufs: Held<'d, BlockBufs<BLOCK>>,
    /// Buffers the caller lent.
    pool: &'d mut [ScanBlock<BLOCK>],
    /// The buffer the parsers read: 0 is `own`, `i + 1` is `pool[i]`.
    cur: usize,
    tick: u64,
}

/// What one lend of a block from the cache came to.
enum Lend<R> {
    /// A hit: the parse ran over checked bytes, with this answer.
    Done(R),
    /// Not cached, or a check failed: read the device.
    Miss,
    /// The cache does not lend its images.
    Unlent,
}

impl<'d, const BLOCK: usize> Blocks<'d, BLOCK> {
    /// Blocks over `bufs`, which may hold a block of an earlier scan:
    /// it is forgotten.
    const fn over(mut bufs: Held<'d, BlockBufs<BLOCK>>) -> Self {
        match &mut bufs {
            Held::Own(b) => b.own.forget(),
            Held::Lent(b) => b.own.forget(),
        }
        Self {
            bufs,
            pool: &mut [],
            cur: 0,
            tick: 0,
        }
    }

    /// The decoded block the last [`find`](Self::find) or
    /// [`loaded`](Self::loaded) settled on.
    fn current(&self) -> &ScanBlock<BLOCK> {
        self.cur
            .checked_sub(1)
            .and_then(|i| self.pool.get(i))
            .unwrap_or(&self.bufs.own)
    }

    fn slot_mut(&mut self, at: usize) -> &mut ScanBlock<BLOCK> {
        match at.checked_sub(1).and_then(|i| self.pool.get_mut(i)) {
            Some(lent) => lent,
            None => &mut self.bufs.own,
        }
    }

    /// The read scratch and the current buffer, borrowed apart.
    fn split(&mut self) -> (&mut [u8; BLOCK], &mut ScanBlock<BLOCK>) {
        let bufs = &mut *self.bufs;
        let block = match self.cur.checked_sub(1).and_then(|i| self.pool.get_mut(i)) {
            Some(lent) => lent,
            None => &mut bufs.own,
        };
        (&mut bufs.raw, block)
    }

    /// Makes the buffer holding block `id` current, if one does.
    fn find(&mut self, id: u64) -> bool {
        let held = if self.bufs.own.id == Some(id) {
            Some(0)
        } else {
            self.pool
                .iter()
                .position(|b| b.id == Some(id))
                .map(|i| i + 1)
        };
        let Some(at) = held else {
            return false;
        };
        self.tick += 1;
        let tick = self.tick;
        self.slot_mut(at).used = tick;
        self.cur = at;
        true
    }

    /// Picks the buffer the next block is decoded into (an empty one,
    /// else the least recently used), makes it current and empties it.
    /// Returns the read buffer and that buffer. Call
    /// [`loaded`](Self::loaded) once the block is in it.
    fn victim(&mut self) -> (&mut [u8; BLOCK], &mut ScanBlock<BLOCK>) {
        let age = |b: &ScanBlock<BLOCK>| b.id.map_or(0, |_| b.used + 1);
        let mut at = 0;
        let mut oldest = age(&self.bufs.own);
        for (i, lent) in self.pool.iter().enumerate() {
            if age(lent) < oldest {
                oldest = age(lent);
                at = i + 1;
            }
        }
        self.cur = at;
        let (raw, block) = self.split();
        // Until `loaded`, the buffer holds no block: a failed read must
        // not leave the old tag on new bytes, nor its grains.
        block.id = None;
        block.valid = 0;
        // Debug builds fill it, so that a parser reading a byte no gate
        // let it read fails ordinary tests, not only the bit-flip ones.
        if cfg!(debug_assertions) {
            block.data.fill(0xA5);
        }
        (raw, block)
    }

    /// Records that the current buffer now holds the grains `valid` of
    /// block `id`.
    fn loaded(&mut self, id: u64, valid: u64) {
        self.tick += 1;
        let tick = self.tick;
        // At most one buffer holds a block: a buffer is filled only when
        // `find` found none for it, or refilled in place (`complete`).
        debug_assert!(
            core::iter::once(&self.bufs.own)
                .chain(self.pool.iter())
                .enumerate()
                .all(|(at, b)| at == self.cur || b.id != Some(id)),
            "block {id} in two buffers"
        );
        let block = self.slot_mut(self.cur);
        block.id = Some(id);
        block.used = tick;
        block.valid = valid;
    }

    /// Forgets every decoded block (a re-seek starts from scratch).
    fn forget(&mut self) {
        self.bufs.own.forget();
        for lent in self.pool.iter_mut() {
            lent.id = None;
            lent.valid = 0;
        }
    }

    /// Takes `pool` as extra buffers, in place of any lent before.
    fn lend(&mut self, pool: &'d mut [ScanBlock<BLOCK>]) {
        self.cur = 0;
        self.pool = pool;
        self.forget();
    }

    /// Runs the parse `f` over data block `bid` of table `table_id` and
    /// returns its answer, having let it act only on checked bytes.
    ///
    /// - A buffer that holds the block is parsed through a [`Gated`]
    ///   gate. If the parse reaches a grain the buffer lacks, or fails
    ///   while the buffer lacks any, the rest of the block is taken
    ///   ([`complete`](Self::complete)) and the parse runs again over
    ///   the whole block.
    /// - Otherwise a cache that lends has the block parsed where it lies
    ///   ([`lend_into`](Self::lend_into)); the grains the parse checked
    ///   are copied into a buffer as it goes. `whole` takes the whole
    ///   block instead, for a cursor about to read it through.
    /// - Otherwise, and on a miss or a failed check, the block is read
    ///   from the device ([`sstable::read_data_block`]) whole, and parsed
    ///   there.
    ///
    /// An error is believed only from bytes that all passed a check, so
    /// that a change in the cache's RAM is a miss read from the device,
    /// and only damage the CRC let through is [`Error::CorruptBlock`].
    #[allow(clippy::too_many_arguments)] // the scan's source, split for the borrow
    async fn load_with<D: BlockDevice, R>(
        &mut self,
        device: &D,
        cache: Option<&dyn CachePort<BLOCK>>,
        table_id: u32,
        bid: u64,
        whole: bool,
        f: &mut impl FnMut(&[u8; BLOCK], &mut Gate<'_, '_, BLOCK>) -> Result<R, Error<D::Error>>,
    ) -> Result<R, Error<D::Error>> {
        if let Some(answer) = self.held(bid, f) {
            return answer;
        }
        if self.current().id == Some(bid) {
            // `held` found the buffer, and it lacks what the parse reads.
            self.complete(device, cache, table_id, bid).await?;
            return f(&self.current().data, &mut Gate::Open);
        }
        let mut missed = false;
        if let Some(c) = cache {
            match self.lend_into(c, table_id, bid, whole, f) {
                Lend::Done(answer) => return answer,
                Lend::Miss => missed = true,
                Lend::Unlent => {}
            }
        }
        let (raw, block) = self.victim();
        sstable::read_data_block(device, cache, table_id, bid, raw, &mut block.data, missed)
            .await?;
        self.loaded(bid, full::<BLOCK>());
        f(&self.current().data, &mut Gate::Open)
    }

    /// The first step of [`load_with`](Self::load_with), synchronous so
    /// that a scan's step over a block it holds builds no future: the
    /// parse `f` over the buffer holding block `bid`, when one does and
    /// the answer stands. `None` when no buffer holds the block (the
    /// current buffer is then not `bid`'s), or when the one that does
    /// lacks what the parse reads (it is then current).
    fn held<E, R>(
        &mut self,
        bid: u64,
        f: &mut impl FnMut(&[u8; BLOCK], &mut Gate<'_, '_, BLOCK>) -> Result<R, Error<E>>,
    ) -> Option<Result<R, Error<E>>> {
        if !self.find(bid) {
            return None;
        }
        let block = self.current();
        if block.valid == full::<BLOCK>() {
            // Every byte is in, checked: nothing to gate, and an error
            // stands.
            return Some(f(&block.data, &mut Gate::Open));
        }
        let mut gate = Gate::Held(Gated::<BLOCK> {
            valid: block.valid,
            missed: false,
        });
        let answer = f(&block.data, &mut gate);
        // Fails closed: only a buffer gate that saw no miss vouches.
        let held = matches!(gate, Gate::Held(Gated { missed: false, .. }));
        (held && answer.is_ok()).then_some(answer)
    }

    /// [`value_into`](Self::value_into) from a buffer that holds the
    /// block, synchronously (see [`held`](Self::held)); `None` when none
    /// holds what the copy reads.
    fn value_held<E>(
        &mut self,
        (_, bid, off, end): (u32, u64, usize, usize),
        out: &mut [u8],
    ) -> Option<Result<usize, Error<E>>> {
        self.held(bid, &mut |b, g| value_at(b, off, end, bid, g, out))
    }

    /// Copies the value of the entry at `(table_id, bid, off, end)` (a
    /// cursor's head: its table, block, offset and the block's entries
    /// end) into `out`, and returns its length.
    async fn value_into<D: BlockDevice>(
        &mut self,
        device: &D,
        cache: Option<&dyn CachePort<BLOCK>>,
        (table_id, bid, off, end): (u32, u64, usize, usize),
        out: &mut [u8],
    ) -> Result<usize, Error<D::Error>> {
        self.load_with(device, cache, table_id, bid, false, &mut |b, g| {
            value_at(b, off, end, bid, g, out)
        })
        .await
    }

    /// One lend of block `bid` from `cache`, parsed by `f` into a fresh
    /// buffer (see [`load_with`](Self::load_with)). A raw block is parsed
    /// in the image through a [`CopyIn`] gate when the cache checks
    /// ranges and `whole` is not asked for; otherwise it is checked and
    /// copied whole. A compressed block has its flag and stream checked
    /// and is inflated whole; a parse of what it inflates to rests on
    /// checked bytes, and its error stands. A parse of a raw image, or an
    /// inflate, that fails has the whole image checked before its error
    /// is believed (ADR-0031). The buffer keeps what was copied only when
    /// the lend held.
    fn lend_into<E, R>(
        &mut self,
        cache: &dyn CachePort<BLOCK>,
        table_id: u32,
        bid: u64,
        whole: bool,
        f: &mut impl FnMut(&[u8; BLOCK], &mut Gate<'_, '_, BLOCK>) -> Result<R, Error<E>>,
    ) -> Lend<Result<R, Error<E>>> {
        let ranged = !whole && cache.checks_ranges();
        let body_end = BLOCK - sstable::CRC_LEN;
        let (_, block) = self.victim();
        let data = &mut block.data;
        let mut valid = 0u64;
        let mut answer = None;
        let lent = cache.with_checked(table_id, bid, &mut |image, check| {
            let (Ok(image), Some(at)) = (<&[u8; BLOCK]>::try_from(image), body_end.checked_sub(2))
            else {
                return;
            };
            if !check(at, body_end) {
                return;
            }
            let flag = u16::from_le_bytes([image[at], image[at + 1]]);
            if flag & 0x8000 != 0 {
                let clen = usize::from(flag & 0x7FFF);
                let inflated = clen < body_end
                    && check(0, clen)
                    && crate::compress::decompress(&image[..clen], &mut data[..body_end]).is_ok();
                if inflated {
                    valid = full::<BLOCK>();
                    answer = Some(f(data, &mut Gate::Open));
                } else if check(0, BLOCK) {
                    answer = Some(Err(Error::CorruptBlock { id: bid }));
                }
                return;
            }
            if !ranged {
                if check(0, BLOCK) {
                    data.copy_from_slice(image);
                    valid = full::<BLOCK>();
                    answer = Some(f(data, &mut Gate::Open));
                }
                return;
            }
            let mut gate = Gate::Lent(CopyIn {
                image,
                data,
                valid: 0,
                check,
            });
            let parsed = f(image, &mut gate);
            // Fails closed: a gate that is no longer the lend's answers
            // nothing, and the lend is a miss.
            let Gate::Lent(c) = gate else {
                return;
            };
            if parsed.is_err() {
                let _ = (c.check)(0, BLOCK);
            }
            valid = c.valid;
            answer = Some(parsed);
        });
        match (lent, answer) {
            (None, _) => Lend::Unlent,
            (Some(true), Some(answer)) => {
                if valid != 0 {
                    self.loaded(bid, valid);
                }
                Lend::Done(answer)
            }
            (Some(_), _) => Lend::Miss,
        }
    }

    /// Takes the grains of block `bid` the current buffer (which holds
    /// part of it) lacks: one lend that checks and copies each, kept only
    /// if the lend held; otherwise the block is read from the device and
    /// replaces the buffer whole. Nothing already in the buffer is
    /// re-vouched by a check of the cache's image.
    async fn complete<D: BlockDevice>(
        &mut self,
        device: &D,
        cache: Option<&dyn CachePort<BLOCK>>,
        table_id: u32,
        bid: u64,
    ) -> Result<(), Error<D::Error>> {
        let missed = if let Some(c) = cache {
            let (_, block) = self.split();
            let mut valid = block.valid;
            let data = &mut block.data;
            let g = grain::<BLOCK>();
            let lent = c.with_checked(table_id, bid, &mut |image, check| {
                if image.len() != BLOCK {
                    return;
                }
                for i in 0..grains::<BLOCK>() {
                    if valid & (1u64 << i) == 0 {
                        let (lo, hi) = (i * g, ((i + 1) * g).min(BLOCK));
                        if !check(lo, hi) {
                            return;
                        }
                        data[lo..hi].copy_from_slice(&image[lo..hi]);
                        valid |= 1u64 << i;
                    }
                }
            });
            if lent == Some(true) && valid == full::<BLOCK>() {
                block.valid = valid;
                return Ok(());
            }
            lent.is_some()
        } else {
            false
        };
        let (raw, block) = self.split();
        block.id = None;
        block.valid = 0;
        sstable::read_data_block(device, cache, table_id, bid, raw, &mut block.data, missed)
            .await?;
        self.loaded(bid, full::<BLOCK>());
        Ok(())
    }
}

/// A table cursor's head entry, as a block parse found it.
#[derive(Clone, Copy)]
struct Head<const KEY_MAX: usize> {
    key: [u8; KEY_MAX],
    key_len: usize,
    seq: u64,
    tombstone: bool,
    val_len: usize,
    expire_at: u64,
    /// The entry's offset in its block.
    off: usize,
    /// The offset just past it.
    next: usize,
}

impl<const KEY_MAX: usize> Head<KEY_MAX> {
    /// The head for entry `e` at `off`; `None` when its key is longer
    /// than `KEY_MAX` (a block no `Db` of this shape wrote).
    fn of(e: &sstable::ParsedDataEntry<'_>, off: usize) -> Option<Self> {
        let mut key = [0u8; KEY_MAX];
        key.get_mut(..e.key.len())?.copy_from_slice(e.key);
        Some(Self {
            key,
            key_len: e.key.len(),
            seq: e.seq,
            tombstone: e.tombstone,
            val_len: e.val.len(),
            expire_at: e.expire_at,
            off,
            next: e.next,
        })
    }
}

/// The entries end of data block `b` (see [`sstable::data_entries_end`]),
/// with its count word checked first. A block always holds an entry.
fn entries_end<E, const BLOCK: usize>(
    b: &[u8; BLOCK],
    bid: u64,
    g: &mut Gate<'_, '_, BLOCK>,
) -> Result<usize, Error<E>> {
    let body_end = BLOCK - sstable::CRC_LEN;
    let at = body_end.checked_sub(2).filter(|&at| g.check(at, body_end));
    match at.map(|_| sstable::data_entries_end::<E, BLOCK>(b, bid)) {
        Some(Ok(end)) if end > 0 => Ok(end),
        _ => Err(Error::CorruptBlock { id: bid }),
    }
}

/// Restart `i` of data block `b` whose entries end at `end`: the entry
/// offset it points at, or `None` when there is no such restart or it
/// points into the trailer. The caller has checked the restart array.
fn restart_at<const BLOCK: usize>(b: &[u8; BLOCK], end: usize, i: usize) -> Option<usize> {
    let at = end.checked_add(i.checked_mul(2)?)?;
    if at.checked_add(2)? > BLOCK - sstable::CRC_LEN - 2 {
        return None;
    }
    let roff = usize::from(u16::from_le_bytes([*b.get(at)?, *b.get(at + 1)?]));
    (roff < end).then_some(roff)
}

/// The entry at `off`, checked whole before anything of it is used.
fn entry_checked<'b, E, const BLOCK: usize>(
    b: &'b [u8; BLOCK],
    off: usize,
    end: usize,
    bid: u64,
    g: &mut Gate<'_, '_, BLOCK>,
) -> Result<sstable::ParsedDataEntry<'b>, Error<E>> {
    let e = sstable::parse_data_entry::<E, BLOCK>(b, off, end, bid)?;
    if g.check(off, e.next) {
        Ok(e)
    } else {
        Err(Error::CorruptBlock { id: bid })
    }
}

/// Names the entry at `off`, if one parses there, as the next to be
/// read (see [`Gate::ahead`]); where the entries have ended in zero
/// padding, names the padding, which the next step checks before it
/// believes the block is done. A parse that fails on anything else is
/// left for the step that reaches it to report.
fn look_ahead<const BLOCK: usize>(
    b: &[u8; BLOCK],
    off: usize,
    end: usize,
    g: &mut Gate<'_, '_, BLOCK>,
) {
    if off >= end || !g.takes_ahead() {
        return;
    }
    match sstable::parse_data_entry::<(), BLOCK>(b, off, end, 0) {
        Ok(n) => g.ahead(off, n.next),
        Err(_) if b.get(off..end).is_some_and(sstable::all_zero) => g.ahead(off, end),
        Err(_) => {}
    }
}

/// The first entry at or after offset `off` in data block `b` (entries
/// ending at `end`) with `seq <= max_seq` and, with a `floor`, a key at
/// or above it; `None` when the block has no more. The entry after it is
/// named as the next to read. Genuine zero padding ends the entries; any
/// other entry that does not parse is [`Error::CorruptBlock`]: a scan
/// must never silently skip entries.
fn park_in<E, const BLOCK: usize, const KEY_MAX: usize>(
    b: &[u8; BLOCK],
    mut off: usize,
    end: usize,
    floor: Option<&[u8]>,
    max_seq: u64,
    bid: u64,
    g: &mut Gate<'_, '_, BLOCK>,
) -> Result<Option<Head<KEY_MAX>>, Error<E>> {
    let corrupt = || Error::CorruptBlock { id: bid };
    loop {
        if off >= end {
            return Ok(None);
        }
        let Ok(e) = sstable::parse_data_entry::<E, BLOCK>(b, off, end, bid) else {
            let padding = g.check(off, end) && b.get(off..end).is_some_and(sstable::all_zero);
            return if padding { Ok(None) } else { Err(corrupt()) };
        };
        if !g.check(off, e.next) {
            return Err(corrupt());
        }
        if e.seq <= max_seq && floor.is_none_or(|f| e.key >= f) {
            let head = Head::of(&e, off).ok_or_else(corrupt)?;
            look_ahead(b, e.next, end, g);
            return Ok(Some(head));
        }
        off = e.next;
    }
}

/// Positions in data block `b`: its entries end, and the first entry
/// with a key at or above `start` and `seq <= max_seq` (`None`: the
/// block has none). A binary search over the block's restarts finds the
/// region to walk from, so only the trailer, the probed restarts, the
/// run walked and the entry after the head are read, each checked first.
fn seek_block<E, const BLOCK: usize, const KEY_MAX: usize>(
    b: &[u8; BLOCK],
    start: &[u8],
    max_seq: u64,
    bid: u64,
    g: &mut Gate<'_, '_, BLOCK>,
) -> Result<(usize, Option<Head<KEY_MAX>>), Error<E>> {
    let corrupt = || Error::CorruptBlock { id: bid };
    let end = entries_end::<E, BLOCK>(b, bid, g)?;
    let body_end = BLOCK - sstable::CRC_LEN;
    if !g.check(end, body_end) {
        return Err(corrupt());
    }
    let rcount = (body_end - 2 - end) / 2;
    // The first restart whose key sorts at or above `start`; entries of
    // the region before it may too, and nothing earlier can.
    let (mut lo, mut hi) = (0usize, if start.is_empty() { 0 } else { rcount });
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let roff = restart_at(b, end, mid).ok_or_else(corrupt)?;
        if entry_checked::<E, BLOCK>(b, roff, end, bid, g)?.key < start {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    let from = match lo.checked_sub(1) {
        Some(r) => restart_at(b, end, r).ok_or_else(corrupt)?,
        None => 0,
    };
    Ok((end, park_in(b, from, end, Some(start), max_seq, bid, g)?))
}

/// Copies the value of the entry at `off` in data block `b` (entries
/// ending at `end`) into `out`, after checking the entry, and names the
/// entry after it as the next to read.
fn value_at<E, const BLOCK: usize>(
    b: &[u8; BLOCK],
    off: usize,
    end: usize,
    bid: u64,
    g: &mut Gate<'_, '_, BLOCK>,
    out: &mut [u8],
) -> Result<usize, Error<E>> {
    let e = entry_checked::<E, BLOCK>(b, off, end, bid, g)
        .map_err(|_| Error::CorruptBlock { id: bid })?;
    out.get_mut(..e.val.len())
        .ok_or(Error::CorruptBlock { id: bid })?
        .copy_from_slice(e.val);
    look_ahead(b, e.next, end, g);
    Ok(e.val.len())
}

/// One table's position in a scan. The head entry is cached so the merge
/// compares keys without reloading blocks; values are re-parsed from the
/// shared buffer only for the winning entry.
#[derive(Clone, Copy)]
struct ScanCursor<const KEY_MAX: usize> {
    /// Owning table's manifest id: first half of the block-cache key.
    table_id: u32,
    /// Replica-merge origin of the owning table (see [`crate::version`]).
    node_id: u32,
    /// Seal wall-clock (seconds) of the owning table.
    seal_wall: u64,
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
    /// Not positioned yet: the table starts after the scan's start key,
    /// so nothing of it has been read. `key` is then the table's lower
    /// bound, not an entry: no entry of the table sorts below it, which
    /// is all the merge needs until the scan gets there. The cursor is
    /// positioned for real (and this cleared) before its head can win.
    pending: bool,
    key: [u8; KEY_MAX],
    key_len: usize,
    seq: u64,
    tombstone: bool,
    val_len: usize,
    /// Absolute expiry tick of the head entry; 0 = no expiry.
    expire_at: u64,
}

impl<const KEY_MAX: usize> ScanCursor<KEY_MAX> {
    /// Parks the cursor on `h`, in its current block.
    const fn set_head(&mut self, h: &Head<KEY_MAX>) {
        self.key = h.key;
        self.key_len = h.key_len;
        self.seq = h.seq;
        self.tombstone = h.tombstone;
        self.val_len = h.val_len;
        self.expire_at = h.expire_at;
        self.off = h.off;
        self.next = h.next;
        self.live = true;
    }

    /// A `const` item can't use generic params in const operations on
    /// stable, hence a `const fn` instead of an associated constant.
    const fn empty() -> Self {
        Self {
            table_id: 0,
            node_id: 0,
            seal_wall: 0,
            first_block: 0,
            data_blocks: 0,
            block_idx: 0,
            block_id: 0,
            off: 0,
            next: 0,
            end: 0,
            live: false,
            pending: false,
            key: [0u8; KEY_MAX],
            key_len: 0,
            seq: 0,
            tombstone: false,
            val_len: 0,
            expire_at: 0,
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
    const CACHE: usize,
> {
    src: Source<'d, D, BLOCK, KEY_MAX, VAL_MAX, CAP, ARENA, LEVELS, TABLES>,
    end_len: usize,
    has_end: bool,
    max_seq: u64,
    /// Caller-supplied clock for TTL reads: values with nonzero
    /// `expire_at <= now` are suppressed.
    now: u64,
    /// The scan's block buffers: one of its own, any the caller lent it
    /// ([`use_block_pool`](Self::use_block_pool)), and the buffer a
    /// block is read into before it is inflated. Every parser reads the
    /// current one.
    blocks: Blocks<'d, BLOCK>,
    /// Memtable head: slot index plus cached entry bytes.
    mem_idx: usize,
    mem_key_len: usize,
    mem_val_len: usize,
    mem_seq: u64,
    mem_tombstone: bool,
    mem_live: bool,
    /// Absolute expiry tick of the memtable head; 0 = no expiry.
    mem_expire_at: u64,
    /// The end bound's bytes, the memtable head's key and value, and one
    /// pool of `LEVELS × TABLES` cursors, the manifest's whole-tree
    /// capacity, used level-major: level `li`'s cursors are the
    /// `cursor_counts[li]` slots from `level_base[li]` of the flattened
    /// pool. A level below 0 may hold more than `TABLES` tables (they
    /// share the pool; F20), so rows per level would overflow. (Declared
    /// nested because stable Rust forbids const-generic products like
    /// `LEVELS * TABLES` in array lengths.) The scan's own, or kept by
    /// the caller ([`Scan::detached_in`]).
    k: Held<'d, ScanKeys<KEY_MAX, VAL_MAX, LEVELS, TABLES>>,
    cursor_counts: [usize; LEVELS],
    /// Where each level's cursors start in the flattened pool.
    level_base: [usize; LEVELS],
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
    const CACHE: usize,
> Scan<'d, D, BLOCK, KEY_MAX, VAL_MAX, CAP, ARENA, LEVELS, TABLES, BLOOM_BYTES, CACHE>
{
    /// Creates an unpositioned scan over `db`. Call [`seek`](Scan::seek)
    /// before [`next`](Scan::next).
    #[must_use]
    pub const fn new(
        db: &'d Db<D, BLOCK, KEY_MAX, VAL_MAX, CAP, ARENA, LEVELS, TABLES, BLOOM_BYTES, CACHE>,
    ) -> Self {
        Self::over(Source::of_db(db))
    }

    /// An unpositioned scan over `src`.
    const fn over(src: Source<'d, D, BLOCK, KEY_MAX, VAL_MAX, CAP, ARENA, LEVELS, TABLES>) -> Self {
        Self::over_in(src, Held::Own(BlockBufs::new()), Held::Own(ScanKeys::new()))
    }

    /// An unpositioned scan over `src`, in `bufs` and `k`.
    const fn over_in(
        src: Source<'d, D, BLOCK, KEY_MAX, VAL_MAX, CAP, ARENA, LEVELS, TABLES>,
        bufs: Held<'d, BlockBufs<BLOCK>>,
        k: Held<'d, ScanKeys<KEY_MAX, VAL_MAX, LEVELS, TABLES>>,
    ) -> Self {
        Self {
            src,
            end_len: 0,
            has_end: false,
            max_seq: u64::MAX,
            now: 0,
            blocks: Blocks::over(bufs),
            mem_idx: 0,
            mem_key_len: 0,
            mem_val_len: 0,
            mem_seq: 0,
            mem_tombstone: false,
            mem_live: false,
            mem_expire_at: 0,
            k,
            cursor_counts: [0; LEVELS],
            level_base: [0; LEVELS],
        }
    }

    /// [`detached`](Self::detached), in buffers the caller keeps:
    /// `state` serves this scan for its life, and the next one after
    /// it. A scan in a kept `state` starts as a new scan does and
    /// yields the same entries, whatever an earlier scan in it saw or
    /// how it ended; what it saves is building and zeroing its buffers
    /// for every scan (see [`ScanState`]).
    ///
    /// The scan itself is still as large as one that owns its buffers
    /// (`size_of::<Scan>()`): build it where it is used, not to move it.
    /// Building one in a kept state writes a few words and zeroes
    /// nothing. One state serves one scan at a time.
    #[must_use]
    pub const fn detached_in(
        state: &'d mut ScanState<BLOCK, KEY_MAX, VAL_MAX, LEVELS, TABLES>,
        memtable: &'d MemTable<CAP, ARENA, KEY_MAX, VAL_MAX>,
        manifest: &'d Manifest<LEVELS, TABLES, KEY_MAX>,
        device: &'d D,
        cache: Option<&'d dyn CachePort<BLOCK>>,
        node_id: u32,
    ) -> Self {
        let ScanState { blocks, keys } = state;
        Self::over_in(
            Source {
                memtable,
                manifest,
                device,
                cache,
                node_id,
                open: true,
            },
            Held::Lent(blocks),
            Held::Lent(keys),
        )
    }

    /// Creates an unpositioned scan that does not borrow a `Db`: a
    /// *detached* scan (ADR-0019), the scan counterpart of a detached
    /// point read ([`TableView`](crate::TableView)). It merges
    /// `memtable` with the tables `manifest` names, read from `device`
    /// through `cache` (`None`: every block comes from the device).
    /// `node_id` is the `Db`'s [`node_id`](Db::node_id).
    ///
    /// `memtable` is usually a copy of part of the `Db`'s memtable, made
    /// by [`Db::copy_memtable_range`], and `manifest` a copy of
    /// [`Db::manifest`] taken at the same moment; the scan then yields
    /// what a scan over the `Db` would have yielded at that moment, up
    /// to the copy's horizon. Nothing here borrows the `Db`, so its
    /// owner can go on writing, and any number of detached scans can run
    /// at once, on other threads or tasks.
    ///
    /// The caller keeps the same rule as for a detached point read:
    /// while the scan is alive, do not run `flush`, `compact_step`,
    /// `ingest_table` or `archive_commit` on the `Db`. They can rewrite
    /// a table the scan is in the middle of.
    #[must_use]
    pub const fn detached(
        memtable: &'d MemTable<CAP, ARENA, KEY_MAX, VAL_MAX>,
        manifest: &'d Manifest<LEVELS, TABLES, KEY_MAX>,
        device: &'d D,
        cache: Option<&'d dyn CachePort<BLOCK>>,
        node_id: u32,
    ) -> Self {
        Self::over(Source {
            memtable,
            manifest,
            device,
            cache,
            node_id,
            open: true,
        })
    }

    /// Lends the scan extra block buffers, for as long as it lives.
    ///
    /// A scan has one cursor per table and, of its own, one decoded
    /// block: each time the merge moves to another table's cursor, that
    /// table's block is read and inflated again. With a pool, each
    /// cursor's block stays decoded while the merge is elsewhere, up to
    /// the pool's size (least recently used goes first). One buffer per
    /// table that overlaps the range is the most that helps; firmware
    /// that scans little passes none and keeps the scan small.
    ///
    /// Call it before [`seek`](Self::seek); it forgets what the scan had
    /// decoded. The scan's results do not depend on the pool.
    pub fn use_block_pool(&mut self, pool: &'d mut [ScanBlock<BLOCK>]) {
        self.blocks.lend(pool);
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
    /// The seek reads the tables whose key range holds `start`. A table
    /// that starts after `start` is read when the scan reaches it, by
    /// [`next`](Scan::next), so a seek followed by a few entries costs
    /// the tables around `start` and not every table of the range. An
    /// error in such a table is reported by the `next` that gets there,
    /// before that `next` yields anything.
    ///
    /// # Errors
    ///
    /// [`Error::KeyTooLarge`] when a bound exceeds `KEY_MAX`,
    /// [`Error::EmptyKey`] for an empty `end` bound,
    /// [`Error::CorruptBlock`] on a torn index or data block,
    /// [`Error::CorruptManifest`] on a malformed table range, or
    /// [`Error::Device`] on I/O failure.
    pub async fn seek(
        &mut self,
        start: &[u8],
        end: Option<&[u8]>,
        max_seq: u64,
    ) -> Result<(), Error<D::Error>> {
        self.seek_with_time(start, end, max_seq, 0).await
    }

    /// Timed seek: like [`seek`](Scan::seek), but values whose `expire_at`
    /// is nonzero and `<= now` are suppressed, and range tombstones hide
    /// the keys they cover. Callers without a clock pass `now = 0`.
    ///
    /// # Errors
    ///
    /// Same as [`seek`](Scan::seek).
    pub async fn seek_with_time(
        &mut self,
        start: &[u8],
        end: Option<&[u8]>,
        max_seq: u64,
        now: u64,
    ) -> Result<(), Error<D::Error>> {
        self.src.ensure_open()?;
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
                self.k.end[..e.len()].copy_from_slice(e);
                self.end_len = e.len();
                self.has_end = true;
            }
            None => {
                self.has_end = false;
                self.end_len = 0;
            }
        }
        self.max_seq = max_seq;
        self.now = now;
        self.blocks.forget();

        // Copy the shared ref: everything derived from it lives
        // independently of this `&mut self` borrow.
        let db = self.src;
        // Memtable cursor: first slot at/after `start`.
        self.mem_idx = db.memtable.lower_bound(start);
        self.advance_mem();

        // One cursor per table overlapping [start, end). The merge resolves
        // L0 overlaps and cross-level duplication; order here is irrelevant.
        self.cursor_counts = [0; LEVELS];
        let mut base = 0;
        for li in 0..LEVELS {
            self.level_base[li] = base;
            let tables = db.manifest.level(li).unwrap_or(&[]);
            for tref in tables {
                // Key-range prune: no I/O for tables outside the scan.
                if tref.last_key.as_slice() < start {
                    continue;
                }
                if self.has_end && tref.first_key.as_slice() >= &self.k.end[..self.end_len] {
                    continue;
                }
                // Entries below the table's live lower bound are dead (a
                // compaction already merged them into a newer table): the
                // cursor starts at the bound at the earliest.
                let from = start.max(tref.first_key.as_slice());
                if tref.first_key.as_slice() > start {
                    // The table starts after `start`: nothing of it can
                    // be the scan's first entry. Its cursor waits at the
                    // table's lower bound and is positioned, with I/O,
                    // only if the scan gets that far. A seek that reads
                    // a few entries then touches the tables around
                    // `start`, not every table to the end of the range.
                    self.add_pending(tref, li)?;
                } else {
                    self.add_cursor(tref, li, from).await?;
                }
            }
            base += self.cursor_counts[li];
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
        // Through the keys once, not once a source.
        let keys = &*self.k;
        let mut best: Option<(usize, &[u8])> = None;
        for src in 0..=total {
            let Some(k) = self.head_key(keys, src) else {
                continue;
            };
            if best.is_none_or(|(_, m)| k < m) {
                best = Some((src, k));
            }
        }
        best
    }

    /// The first table source still waiting at its lower bound with that
    /// bound on `key`, if any (see [`ScanCursor::pending`]).
    fn pending_on(&self, total: usize, key: &[u8]) -> Option<usize> {
        let cursors = self.k.cursors.as_flattened();
        (1..=total).find(|&src| {
            let (li, ti) = self.cursor_pos(src);
            let c = &cursors[self.level_base[li] + ti];
            c.live && c.pending && &c.key[..c.key_len] == key
        })
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
    /// on a torn data block (or a torn index block of a table the scan
    /// reaches for the first time),
    /// [`Error::CorruptManifest`] on a malformed table range, or
    /// [`Error::Device`] on I/O failure.
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
            if self.has_end && min_key >= &self.k.end[..self.end_len] {
                return Ok(None);
            }
            // A cursor still waiting at its table's lower bound has
            // reached the front: position it for real, then look again.
            // Its first entry sorts at or after the bound, so nothing
            // already compared is disturbed.
            if let Some(src) = self.pending_on(total, min_key) {
                self.position_pending(src).await?;
                continue;
            }
            // Pass 2: among heads on the minimum key, the highest version
            // wins (see [`crate::version`]). `min_key` is invariant for
            // this whole pass (computed once by `min_src` above), so it is
            // compared directly instead of re-fetched through `cursor_pos`
            // on every iteration.
            let mut winner_src = min_src;
            let mut winner_ver = Version::ZERO;
            let mut winner_tombstone = false;
            // Seeded from the leader's key, so a key whose only versions
            // carry the reserved sequence 0 still has its bytes (and is
            // skipped below) instead of yielding an empty key forever.
            let mut winner_key_len = min_key.len();
            let mut winner_val_len = 0usize;
            let mut winner_expire_at = 0u64;
            let keys = &*self.k;
            for src in 0..=total {
                let Some((k, _seq, tomb, vlen, exp)) = self.head(keys, src) else {
                    continue;
                };
                if k != min_key {
                    continue;
                }
                let ver = self.head_version(keys, src);
                if version_gt(ver, winner_ver) {
                    winner_ver = ver;
                    winner_src = src;
                    winner_tombstone = tomb;
                    winner_key_len = k.len();
                    winner_val_len = vlen;
                    winner_expire_at = exp;
                }
            }
            let (winner_table, winner_bid, winner_off, winner_end) = if winner_src == 0 {
                (0u32, 0u64, 0usize, 0usize)
            } else {
                let (wli, wti) = self.cursor_pos(winner_src);
                let c = &self.k.cursors.as_flattened()[self.level_base[wli] + wti];
                (c.table_id, c.block_id, c.off, c.end)
            };
            // The winner key is materialized into a local buffer first, so a
            // hidden winner (tombstone, range-covered, or expired) never
            // touches the caller's buffers: a hidden key must not fail
            // the scan with `BufferTooSmall`, and its bytes are still
            // needed to advance past it below.
            let mut wkey = [0u8; KEY_MAX];
            {
                let wk: &[u8] = if winner_src == 0 {
                    &self.k.mem_key[..winner_key_len]
                } else {
                    let (wli, wti) = self.cursor_pos(winner_src);
                    &self.k.cursors.as_flattened()[self.level_base[wli] + wti].key[..winner_key_len]
                };
                wkey[..winner_key_len].copy_from_slice(wk);
            }
            let wkey = &wkey[..winner_key_len];
            // Range tombstones and TTL expiry shadow the point winner: a
            // covering range tombstone newer than the winning version, or a
            // winning value with `expire_at <= now`, hides the key. An
            // expired winner never falls through to an older version.
            let covered = self
                .src
                .rdel_hides(wkey, self.max_seq, winner_ver, &mut self.blocks.bufs.raw)
                .await?;
            let expired =
                !winner_tombstone && winner_expire_at != 0 && winner_expire_at <= self.now;
            // Sequence 0 is reserved (the counter issues 1 and up): a
            // version carrying it — only possible in an externally built,
            // ingested table — is invisible, exactly as on the point-read
            // path.
            if winner_ver.seq == 0 || winner_tombstone || covered || expired {
                // Hidden: advance past the key without yielding it and
                // without demanding caller buffer space for it.
                self.advance_past_key(wkey, total).await?;
                continue;
            }
            // Buffer checks and the key copy happen *before* any cursor
            // advances, so BufferTooSmall never consumes an entry.
            if winner_key_len > key_buf.len() {
                return Err(Error::BufferTooSmall {
                    need: winner_key_len,
                });
            }
            key_buf[..winner_key_len].copy_from_slice(wkey);
            if winner_val_len > val_buf.len() {
                return Err(Error::BufferTooSmall {
                    need: winner_val_len,
                });
            }
            // Copy the winner's value while its cursor is still parked.
            // (A tombstone winner continued above, so this always copies.)
            if winner_src == 0 {
                val_buf[..winner_val_len].copy_from_slice(&self.k.mem_val[..winner_val_len]);
            } else {
                let db = self.src;
                let at = (winner_table, winner_bid, winner_off, winner_end);
                let copied = match self.blocks.value_held(at, val_buf) {
                    Some(copied) => copied?,
                    None => {
                        self.blocks
                            .value_into(db.device, db.cache, at, val_buf)
                            .await?
                    }
                };
                debug_assert_eq!(copied, winner_val_len);
            }
            // Advance every head sitting on the minimum key — the winner
            // included. Compare against the copy in `key_buf`: the heads
            // move as cursors advance. A source may hold several versions
            // of the yielded key (newest first); each advance parks at the
            // next visible version, so loop until the head leaves the key —
            // otherwise an older version would be yielded as a duplicate.
            self.advance_past_key(&key_buf[..winner_key_len], total)
                .await?;
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
        while self.mem_live && &self.k.mem_key[..self.mem_key_len] == minkey {
            self.mem_idx += 1;
            self.advance_mem();
        }
        for src in 1..=total {
            let (li, ti) = self.cursor_pos(src);
            loop {
                let c = &self.k.cursors.as_flattened()[self.level_base[li] + ti];
                if !c.live || &c.key[..c.key_len] != minkey {
                    break;
                }
                self.park_cursor(li, ti, None).await?;
            }
        }
        Ok(())
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
        if Self::data_blocks_of(tref)? == 0 {
            // Range-only table (flush or compaction carrying nothing but
            // range tombstones): no point cursor to add. Its rdel section
            // still shadows keys via the separate covering probes, which
            // walk the manifest, not the cursors.
            return Ok(());
        }
        let ti = self.cursor_counts[li];
        // Emptied before it is counted: until `position_cursor` has read
        // it into place, the slot holds no cursor, whatever an earlier
        // seek, or an earlier scan in a kept state, left in it.
        *self
            .k
            .cursors
            .as_flattened_mut()
            .get_mut(self.level_base[li] + ti)
            .ok_or(Error::CorruptManifest)? = ScanCursor::empty();
        self.cursor_counts[li] += 1;
        self.position_cursor(tref, li, ti, start).await
    }

    /// Adds a cursor over `tref` that is not positioned yet: it waits at
    /// the table's lower bound and reads nothing (see
    /// [`ScanCursor::pending`]).
    fn add_pending(&mut self, tref: &TableRef<KEY_MAX>, li: usize) -> Result<(), Error<D::Error>> {
        let data_blocks = Self::data_blocks_of(tref)?;
        if data_blocks == 0 {
            return Ok(());
        }
        let ti = self.cursor_counts[li];
        let bound = tref.first_key.as_slice();
        // The manifest holds at most the pool's worth of tables, so this
        // never fails; if it did, the manifest is what is wrong.
        let slot = self
            .k
            .cursors
            .as_flattened_mut()
            .get_mut(self.level_base[li] + ti)
            .ok_or(Error::CorruptManifest)?;
        *slot = ScanCursor::empty();
        slot.table_id = tref.id;
        slot.live = true;
        slot.pending = true;
        slot.key
            .get_mut(..bound.len())
            .ok_or(Error::CorruptManifest)?
            .copy_from_slice(bound);
        slot.key_len = bound.len();
        self.cursor_counts[li] += 1;
        Ok(())
    }

    /// Positions the pending cursor at source `src` for real: at its
    /// table's first entry at or after the scan's start.
    async fn position_pending(&mut self, src: usize) -> Result<(), Error<D::Error>> {
        let (li, ti) = self.cursor_pos(src);
        let table_id = self.k.cursors.as_flattened()[self.level_base[li] + ti].table_id;
        // The scan's source does not change under it, so the table is
        // still in the manifest it was added from.
        let tref = *self
            .src
            .manifest
            .find_table(table_id)
            .ok_or(Error::CorruptManifest)?;
        // Entries below the table's live lower bound are dead; the
        // bound is above `start`, or the cursor would not be pending.
        let from = tref.first_key;
        self.position_cursor(&tref, li, ti, from.as_slice()).await
    }

    /// A table's data-block count, checked.
    fn data_blocks_of(tref: &TableRef<KEY_MAX>) -> Result<u64, Error<D::Error>> {
        tref.data_blocks().ok_or(Error::CorruptBlock {
            id: tref.first_block,
        })
    }

    /// Reads cursor `(li, ti)` into place over `tref`: resolves the
    /// table's block index to the data block that can hold `start`, and
    /// parks at the first entry `>= start` with `seq <= max_seq`.
    async fn position_cursor(
        &mut self,
        tref: &TableRef<KEY_MAX>,
        li: usize,
        ti: usize,
        start: &[u8],
    ) -> Result<(), Error<D::Error>> {
        // The data section leads the table; the scan cursor walks data
        // blocks only.
        let data_first = tref.data_first();
        let data_blocks = Self::data_blocks_of(tref)?;
        // The manifest names the index block: no footer is read.
        let index_id = tref.index_block().ok_or(Error::CorruptManifest)?;
        let db = self.src;
        let data_id = sstable::search_verified(
            db.device,
            db.cache,
            tref.id,
            index_id,
            &mut self.blocks.bufs.raw,
            |payload, check| sstable::index_lookup::<D::Error>(payload, start, index_id, check),
        )
        .await?;
        // `None`: `start` sorts before every block — begin at block 0.
        // (The walk budget in the tuple's second half is for point
        // lookups; the scan cursor walks blocks on its own.)
        let block_idx = match data_id {
            Some((id, _)) => id
                .checked_sub(data_first)
                .ok_or(Error::CorruptBlock { id })?,
            None => 0,
        };
        if block_idx >= data_blocks {
            return Err(Error::CorruptBlock {
                id: tref.first_block,
            });
        }
        let bid = data_first
            .checked_add(block_idx)
            .ok_or(Error::CorruptBlock {
                id: tref.first_block,
            })?;
        // Only what the seek reads of the block is taken: its trailer,
        // the restarts it probes, the run it walks and the entry after
        // the head (ADR-0035).
        let max_seq = self.max_seq;
        let (end, head) = self
            .blocks
            .load_with(db.device, db.cache, tref.id, bid, false, &mut |b, g| {
                seek_block::<D::Error, BLOCK, KEY_MAX>(b, start, max_seq, bid, g)
            })
            .await?;
        // The manifest holds at most the pool's worth of tables, so this
        // never fails; if it did, the manifest is what is wrong.
        let slot = self
            .k
            .cursors
            .as_flattened_mut()
            .get_mut(self.level_base[li] + ti)
            .ok_or(Error::CorruptManifest)?;
        *slot = ScanCursor {
            table_id: tref.id,
            node_id: tref.node_id,
            seal_wall: tref.seal_wall,
            first_block: data_first,
            data_blocks,
            block_idx,
            block_id: bid,
            expire_at: 0,
            off: 0,
            next: end,
            end,
            live: true,
            pending: false,
            key: [0u8; KEY_MAX],
            key_len: 0,
            seq: 0,
            tombstone: false,
            val_len: 0,
        };
        let Some(h) = head else {
            // Nothing in this block: the park crosses to the next.
            return self.park_cursor(li, ti, Some(start)).await;
        };
        slot.set_head(&h);
        Ok(())
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
        let (tid, mut bid, mut off, mut end, mut idx, first, nblocks) = {
            let c = &self.k.cursors.as_flattened()[self.level_base[li] + ti];
            if !c.live {
                return Ok(());
            }
            (
                c.table_id,
                c.block_id,
                c.next,
                c.end,
                c.block_idx,
                c.first_block,
                c.data_blocks,
            )
        };
        // The cursor keeps its head, and the block it lies in, until the
        // park has found the next one: a read that fails on the way
        // leaves the cursor where it was, so a retried `next` sees it
        // unchanged.
        loop {
            let db = self.src;
            let mut park = |b: &[u8; BLOCK], g: &mut Gate<'_, '_, BLOCK>| {
                park_in::<D::Error, BLOCK, KEY_MAX>(b, off, end, floor, max_seq, bid, g)
            };
            let parked = match self.blocks.held(bid, &mut park) {
                Some(parked) => parked?,
                None => {
                    self.blocks
                        .load_with(db.device, db.cache, tid, bid, false, &mut park)
                        .await?
                }
            };
            if let Some(h) = parked {
                let c = &mut self.k.cursors.as_flattened_mut()[self.level_base[li] + ti];
                c.block_id = bid;
                c.block_idx = idx;
                c.end = end;
                c.set_head(&h);
                return Ok(());
            }
            // Block exhausted: move to the next data block, if any.
            if idx + 1 >= nblocks {
                self.k.cursors.as_flattened_mut()[self.level_base[li] + ti].live = false;
                return Ok(());
            }
            let nid = first
                .checked_add(idx + 1)
                .ok_or(Error::CorruptBlock { id: first })?;
            // A cursor that crosses into a block reads it through: it is
            // taken whole, in one lookup.
            end = self
                .blocks
                .load_with(db.device, db.cache, tid, nid, true, &mut |b, g| {
                    entries_end::<D::Error, BLOCK>(b, nid, g)
                })
                .await?;
            bid = nid;
            idx += 1;
            off = 0;
        }
    }

    /// Parks the memtable head at the next live slot at/after `mem_idx`
    /// with `seq <= max_seq`. The scan borrows the database, so no
    /// `put`/`flush` can shift slots under this index walk.
    fn advance_mem(&mut self) {
        let max_seq = self.max_seq;
        let db = self.src;
        self.mem_live = false;
        while self.mem_idx < db.memtable.slot_len() {
            // Range-tombstone slots sort by their start key but are not
            // point versions: the merge never yields them as entries.
            if let Some(v) = db.memtable.slot_view(self.mem_idx)
                && v.seq <= max_seq
                && !v.range_del
            {
                self.k.mem_key[..v.key.len()].copy_from_slice(v.key);
                self.mem_key_len = v.key.len();
                self.k.mem_val[..v.val.len()].copy_from_slice(v.val);
                self.mem_val_len = v.val.len();
                self.mem_seq = v.seq;
                self.mem_tombstone = v.tombstone;
                self.mem_expire_at = v.expire_at;
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
    fn head_key<'a>(
        &self,
        keys: &'a ScanKeys<KEY_MAX, VAL_MAX, LEVELS, TABLES>,
        src: usize,
    ) -> Option<&'a [u8]> {
        if src == 0 {
            if self.mem_live {
                Some(&keys.mem_key[..self.mem_key_len])
            } else {
                None
            }
        } else {
            let (li, ti) = self.cursor_pos(src);
            let c = &keys.cursors.as_flattened()[self.level_base[li] + ti];
            if c.live {
                Some(&c.key[..c.key_len])
            } else {
                None
            }
        }
    }

    /// Head `(key, seq, tombstone, val_len, expire_at)` of a source, or
    /// `None` when exhausted. Source 0 is the memtable; sources 1..=total
    /// are table cursors in level-major order. One `cursor_pos` lookup
    /// instead of the two `head_key`/`head_meta` used to cost a caller
    /// needing both.
    fn head<'a>(
        &self,
        keys: &'a ScanKeys<KEY_MAX, VAL_MAX, LEVELS, TABLES>,
        src: usize,
    ) -> Option<(&'a [u8], u64, bool, usize, u64)> {
        if src == 0 {
            self.mem_live.then_some((
                &keys.mem_key[..self.mem_key_len],
                self.mem_seq,
                self.mem_tombstone,
                self.mem_val_len,
                self.mem_expire_at,
            ))
        } else {
            let (li, ti) = self.cursor_pos(src);
            let c = &keys.cursors.as_flattened()[self.level_base[li] + ti];
            c.live.then_some((
                &c.key[..c.key_len],
                c.seq,
                c.tombstone,
                c.val_len,
                c.expire_at,
            ))
        }
    }

    /// The head entry's version under the replica LWW merge rule: the
    /// memtable's versions are the node's own newest; table heads carry
    /// their table's origin.
    fn head_version(
        &self,
        keys: &ScanKeys<KEY_MAX, VAL_MAX, LEVELS, TABLES>,
        src: usize,
    ) -> Version {
        if src == 0 {
            Version::memtable(self.src.node_id, self.mem_seq)
        } else {
            let (li, ti) = self.cursor_pos(src);
            let c = &keys.cursors.as_flattened()[self.level_base[li] + ti];
            Version::table(c.node_id, c.seal_wall, c.seq)
        }
    }
}

// ---------------------------------------------------------------------------
// Reverse scan
// ---------------------------------------------------------------------------

/// A reverse table cursor: parked at the greatest entry at/below the seek
/// bound seen so far, walking to earlier data blocks as needed. Unlike the
/// forward cursor there is no resume offset — every park re-searches the
/// block's restart points, because a backward walk cannot resume
/// mid-block without re-deriving the region structure anyway.
#[derive(Clone, Copy)]
struct RevCursor<const KEY_MAX: usize> {
    /// Owning table's manifest id: first half of the block-cache key.
    table_id: u32,
    /// Replica-merge origin of the owning table (see [`crate::version`]).
    node_id: u32,
    /// Seal wall-clock (seconds) of the owning table.
    seal_wall: u64,
    first_block: u64,
    block_idx: u64,
    block_id: u64,
    /// Offset of the parked head entry in the loaded block: re-parsed on
    /// each `prev` to copy the value bytes.
    off: usize,
    /// Entries end (restart array start) of the loaded block.
    end: usize,
    live: bool,
    key: [u8; KEY_MAX],
    key_len: usize,
    seq: u64,
    tombstone: bool,
    val_len: usize,
    /// Absolute expiry tick of the head entry; 0 = no expiry.
    expire_at: u64,
}

impl<const KEY_MAX: usize> RevCursor<KEY_MAX> {
    const fn empty() -> Self {
        Self {
            table_id: 0,
            node_id: 0,
            seal_wall: 0,
            first_block: 0,
            block_idx: 0,
            block_id: 0,
            off: 0,
            end: 0,
            live: false,
            key: [0u8; KEY_MAX],
            key_len: 0,
            seq: 0,
            tombstone: false,
            val_len: 0,
            expire_at: 0,
        }
    }
}

/// Caller-owned reverse merge scan over one database.
///
/// Created by [`RevScan::new`], positioned by
/// [`seek_prev`](RevScan::seek_prev), advanced by [`prev`](RevScan::prev).
/// Borrows the database for its whole lifetime: while a `RevScan` is
/// alive, `put`, `delete`, `flush`, and `compact_step` cannot run (they
/// need `&mut`), so cursor positions and the snapshot view stay valid.
///
/// `seek_prev(from, lower, max_seq)` parks every source at the greatest
/// key `<= from` — an empty `from` starts at the last key — and `prev`
/// yields keys in descending order down to (but excluding) `lower`.
/// Winner selection mirrors the forward [`Scan`]: the maximum key across
/// live heads, highest visible sequence winning ties, tombstone winners
/// skipped silently. Entries with `seq > max_seq` are invisible, which is
/// what makes reverse scans at a [`Db::snapshot`] watermark repeatable.
///
/// Positioning inside a data block binary-searches the restart points
/// for the last restart that can lead to a qualifying entry, then walks
/// regions backward. A key's versions are stored newest-first, so a
/// backward walk meets a straddling run's *older* versions first; ties
/// across regions keep the earlier region's (newer) version, and a run
/// that straddles data blocks is followed back into earlier blocks
/// (`resolve_run`), so the newest visible version always wins.
///
/// All memory is caller-owned: one `[u8; BLOCK]` buffer plus one small
/// cursor per table. No allocation, no hidden buffering.
///
/// A reverse block head: key bytes, key length, sequence number,
/// tombstone flag, value length, expiry tick, and entry offset.
type RevHead<const KEY_MAX: usize> = ([u8; KEY_MAX], usize, u64, bool, usize, u64, usize);
/// A reverse block head plus its block location: head, block id, entries
/// end, and block index within the table.
type RevHeadAt<const KEY_MAX: usize> = (
    [u8; KEY_MAX],
    usize,
    u64,
    bool,
    usize,
    u64,
    usize,
    u64,
    usize,
    u64,
);

/// Descending ordered scan over the memtable and all `SSTable`s.
///
/// The reverse mirror of [`Scan`]: `seek_prev(from, lower, max_seq)`
/// positions at the greatest visible entry `<= from` (an empty `from`
/// starts at the last key; `lower` is an exclusive lower bound),
/// and `prev(key_buf, val_buf)` yields entries in descending key order.
/// Highest visible sequence wins across all sources; tombstones are
/// skipped silently. Snapshot semantics mirror the forward scan: entries
/// with `seq > max_seq` are invisible. `BufferTooSmall` fires before any
/// cursor advances, so a retry yields the same entry.
pub struct RevScan<
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
    const CACHE: usize,
> {
    src: Source<'d, D, BLOCK, KEY_MAX, VAL_MAX, CAP, ARENA, LEVELS, TABLES>,
    /// Inclusive ceiling, `k.ceil[..ceil_len]`: entries qualify when
    /// `key <= ceil`. Set when `seek_prev`'s `from` is non-empty; an
    /// empty `from` means no ceiling — the scan starts at the last key.
    ceil_len: usize,
    /// Exclusive lower bound, `k.lower[..lower_len]`: a head at/below it
    /// ends the scan.
    lower_len: usize,
    has_lower: bool,
    max_seq: u64,
    /// Caller-supplied clock for TTL reads: values with nonzero
    /// `expire_at <= now` are suppressed.
    now: u64,
    /// The scan's block buffers: one of its own, any the caller lent it
    /// ([`use_block_pool`](Self::use_block_pool)), and the buffer a
    /// block is read into before it is inflated. Every parser reads the
    /// current one.
    blocks: Blocks<'d, BLOCK>,
    /// Memtable head: slot index plus cached entry bytes. The walk moves
    /// downward from the last slot at/below the ceiling.
    mem_idx: usize,
    mem_key_len: usize,
    mem_val_len: usize,
    mem_seq: u64,
    mem_tombstone: bool,
    mem_live: bool,
    /// Absolute expiry tick of the memtable head; 0 = no expiry.
    mem_expire_at: u64,
    /// The key buffers (`ceil`, `lower`, the memtable head's key and
    /// value) and one pool of `LEVELS × TABLES` cursors, used
    /// level-major: level `li`'s cursors are the `cursor_counts[li]`
    /// slots from `level_base[li]` of the flattened pool. Its own, or
    /// kept by the caller ([`RevScan::detached_in`]).
    k: Held<'d, RevScanKeys<KEY_MAX, VAL_MAX, LEVELS, TABLES>>,
    cursor_counts: [usize; LEVELS],
    /// Where each level's cursors start in the flattened pool.
    level_base: [usize; LEVELS],
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
    const CACHE: usize,
> RevScan<'d, D, BLOCK, KEY_MAX, VAL_MAX, CAP, ARENA, LEVELS, TABLES, BLOOM_BYTES, CACHE>
{
    /// Creates an unpositioned reverse scan that does not borrow a `Db`:
    /// the reverse of [`Scan::detached`], over a memtable copy made by
    /// [`Db::copy_memtable_range_rev`]. The same rule applies.
    #[must_use]
    pub const fn detached(
        memtable: &'d MemTable<CAP, ARENA, KEY_MAX, VAL_MAX>,
        manifest: &'d Manifest<LEVELS, TABLES, KEY_MAX>,
        device: &'d D,
        cache: Option<&'d dyn CachePort<BLOCK>>,
        node_id: u32,
    ) -> Self {
        Self::over(Source {
            memtable,
            manifest,
            device,
            cache,
            node_id,
            open: true,
        })
    }

    /// Creates an unpositioned reverse scan over `db`. Call
    /// [`seek_prev`](RevScan::seek_prev) before [`prev`](RevScan::prev).
    #[must_use]
    pub const fn new(
        db: &'d Db<D, BLOCK, KEY_MAX, VAL_MAX, CAP, ARENA, LEVELS, TABLES, BLOOM_BYTES, CACHE>,
    ) -> Self {
        Self::over(Source::of_db(db))
    }

    /// An unpositioned scan over `src`.
    const fn over(src: Source<'d, D, BLOCK, KEY_MAX, VAL_MAX, CAP, ARENA, LEVELS, TABLES>) -> Self {
        Self::over_in(
            src,
            Held::Own(BlockBufs::new()),
            Held::Own(RevScanKeys::new()),
        )
    }

    /// An unpositioned scan over `src`, in `bufs` and `k`.
    const fn over_in(
        src: Source<'d, D, BLOCK, KEY_MAX, VAL_MAX, CAP, ARENA, LEVELS, TABLES>,
        bufs: Held<'d, BlockBufs<BLOCK>>,
        k: Held<'d, RevScanKeys<KEY_MAX, VAL_MAX, LEVELS, TABLES>>,
    ) -> Self {
        Self {
            src,
            ceil_len: 0,
            lower_len: 0,
            has_lower: false,
            max_seq: u64::MAX,
            now: 0,
            blocks: Blocks::over(bufs),
            mem_idx: 0,
            mem_key_len: 0,
            mem_val_len: 0,
            mem_seq: 0,
            mem_tombstone: false,
            mem_live: false,
            mem_expire_at: 0,
            k,
            cursor_counts: [0; LEVELS],
            level_base: [0; LEVELS],
        }
    }

    /// [`detached`](Self::detached), in buffers the caller keeps; see
    /// [`Scan::detached_in`].
    #[must_use]
    pub const fn detached_in(
        state: &'d mut RevScanState<BLOCK, KEY_MAX, VAL_MAX, LEVELS, TABLES>,
        memtable: &'d MemTable<CAP, ARENA, KEY_MAX, VAL_MAX>,
        manifest: &'d Manifest<LEVELS, TABLES, KEY_MAX>,
        device: &'d D,
        cache: Option<&'d dyn CachePort<BLOCK>>,
        node_id: u32,
    ) -> Self {
        let RevScanState { blocks, keys } = state;
        Self::over_in(
            Source {
                memtable,
                manifest,
                device,
                cache,
                node_id,
                open: true,
            },
            Held::Lent(blocks),
            Held::Lent(keys),
        )
    }

    /// Lends the scan extra block buffers, for as long as it lives; see
    /// [`Scan::use_block_pool`]. Call it before
    /// [`seek_prev`](Self::seek_prev).
    pub fn use_block_pool(&mut self, pool: &'d mut [ScanBlock<BLOCK>]) {
        self.blocks.lend(pool);
    }

    /// Positions the scan at the greatest entry `<= from` and above
    /// `lower` (exclusive; `None` scans through the first key), showing
    /// only mutations with `seq <= max_seq`. Pass a [`Db::snapshot`]
    /// watermark for a pinned read, or `u64::MAX` for the latest view.
    ///
    /// An empty `from` starts at the last key. A `from` below every key
    /// yields nothing. Re-seeking a scan is allowed and repositions every
    /// cursor from scratch.
    ///
    /// # Errors
    ///
    /// [`Error::KeyTooLarge`] when a bound exceeds `KEY_MAX`,
    /// [`Error::CorruptBlock`] on a torn index or data block,
    /// [`Error::CorruptManifest`] on a malformed table range, or
    /// [`Error::Device`] on I/O failure.
    pub async fn seek_prev(
        &mut self,
        from: &[u8],
        lower: Option<&[u8]>,
        max_seq: u64,
    ) -> Result<(), Error<D::Error>> {
        self.seek_prev_with_time(from, lower, max_seq, 0).await
    }

    /// Timed reverse seek: like [`seek_prev`](RevScan::seek_prev), but
    /// values whose `expire_at` is nonzero and `<= now` are suppressed,
    /// and range tombstones hide the keys they cover. Callers without a
    /// clock pass `now = 0`.
    ///
    /// # Errors
    ///
    /// Same as [`seek_prev`](RevScan::seek_prev).
    pub async fn seek_prev_with_time(
        &mut self,
        from: &[u8],
        lower: Option<&[u8]>,
        max_seq: u64,
        now: u64,
    ) -> Result<(), Error<D::Error>> {
        self.src.ensure_open()?;
        if from.len() > KEY_MAX {
            return Err(Error::KeyTooLarge {
                len: from.len(),
                max: KEY_MAX,
            });
        }
        if let Some(l) = lower {
            if l.len() > KEY_MAX {
                return Err(Error::KeyTooLarge {
                    len: l.len(),
                    max: KEY_MAX,
                });
            }
            self.k.lower[..l.len()].copy_from_slice(l);
            self.lower_len = l.len();
            self.has_lower = true;
        } else {
            self.has_lower = false;
            self.lower_len = 0;
        }
        // The ceiling is inclusive: `from` itself is a candidate. Empty
        // `from` means no ceiling — the scan starts at the last key.
        if from.is_empty() {
            self.ceil_len = 0;
        } else {
            self.k.ceil[..from.len()].copy_from_slice(from);
            self.ceil_len = from.len();
        }
        self.max_seq = max_seq;
        self.now = now;
        // A seek starts with no decoded block, as a forward one does.
        self.blocks.forget();
        let db = self.src;
        // Memtable cursor: the last slot at/below `from` (or the last slot
        // when `from` is empty). When `from` is present that is the *end*
        // of its version run: `advance_mem_rev` resolves the whole run, so
        // starting at the run start would skip `from`'s older versions when
        // the newest one sits above `max_seq`.
        let n = db.memtable.slot_len();
        self.mem_idx = if n == 0 {
            0
        } else if from.is_empty() {
            n - 1
        } else {
            let mut ub = db.memtable.lower_bound(from);
            while ub < n && db.memtable.slot_view(ub).is_some_and(|v| v.key == from) {
                ub += 1;
            }
            if ub > 0 {
                ub - 1
            } else {
                n // every slot sorts above `from`
            }
        };
        self.advance_mem_rev();

        // One cursor per table overlapping (lower, from]. The merge
        // resolves L0 overlaps and cross-level duplication; order here is
        // irrelevant.
        self.cursor_counts = [0; LEVELS];
        let mut base = 0;
        for li in 0..LEVELS {
            self.level_base[li] = base;
            let tables = db.manifest.level(li).unwrap_or(&[]);
            for tref in tables {
                // Key-range prune: no I/O for tables outside the scan.
                if !from.is_empty() && tref.first_key.as_slice() > &self.k.ceil[..self.ceil_len] {
                    continue;
                }
                if self.has_lower && tref.last_key.as_slice() <= &self.k.lower[..self.lower_len] {
                    continue;
                }
                self.add_cursor_rev(tref, li, from).await?;
            }
            base += self.cursor_counts[li];
        }
        Ok(())
    }

    /// Yields the previous entry: copies the key into `key_buf` and the
    /// value into `val_buf`, returning their lengths. Returns `Ok(None)`
    /// at the end of the range (at/below the lower bound). Deleted keys
    /// are skipped silently.
    ///
    /// [`Error::BufferTooSmall`] fires *before* any cursor advances, so a
    /// retry with a larger buffer yields the same entry.
    ///
    /// # Errors
    ///
    /// [`Error::BufferTooSmall`] with the required length,
    /// [`Error::CorruptBlock`] on a torn data block, or [`Error::Device`]
    /// on I/O failure.
    pub async fn prev(
        &mut self,
        key_buf: &mut [u8],
        val_buf: &mut [u8],
    ) -> Result<Option<(usize, usize)>, Error<D::Error>> {
        let total = self.total_cursors();
        loop {
            // Pass 1: the source of the maximum live head key, or None
            // when every source is exhausted. Source 0 is the memtable;
            // sources 1..=total are table cursors in level-major order.
            let Some((max_src, max_key)) = self.max_src(total) else {
                return Ok(None);
            };
            // The lower bound is exclusive: a head at/below it ends the
            // scan.
            if self.has_lower && max_key <= &self.k.lower[..self.lower_len] {
                return Ok(None);
            }
            // Pass 2: among heads on the maximum key, the highest
            // version wins (see [`crate::version`]). `max_key` is invariant
            // for this whole pass (computed once by `max_src` above), so it
            // is compared directly instead of re-fetched through
            // `cursor_pos` on every iteration.
            let mut winner_src = max_src;
            let mut winner_ver = Version::ZERO;
            let mut winner_tombstone = false;
            // Seeded from the leader's key, so a key whose only versions
            // carry the reserved sequence 0 still has its bytes (and is
            // skipped below) instead of yielding an empty key forever.
            let mut winner_key_len = max_key.len();
            let mut winner_val_len = 0usize;
            let mut winner_expire_at = 0u64;
            let keys = &*self.k;
            for src in 0..=total {
                let Some((k, _seq, tomb, vlen, exp)) = self.head(keys, src) else {
                    continue;
                };
                if k != max_key {
                    continue;
                }
                let ver = self.head_version(keys, src);
                if version_gt(ver, winner_ver) {
                    winner_ver = ver;
                    winner_src = src;
                    winner_tombstone = tomb;
                    winner_key_len = k.len();
                    winner_val_len = vlen;
                    winner_expire_at = exp;
                }
            }
            let (winner_table, winner_bid, winner_off, winner_end) = if winner_src == 0 {
                (0u32, 0u64, 0usize, 0usize)
            } else {
                let (wli, wti) = self.cursor_pos(winner_src);
                let c = &self.k.cursors.as_flattened()[self.level_base[wli] + wti];
                (c.table_id, c.block_id, c.off, c.end)
            };
            // The winner key is materialized into a local buffer first, so a
            // hidden winner (tombstone, range-covered, or expired) never
            // touches the caller's buffers: a hidden key must not fail
            // the scan with `BufferTooSmall`, and its bytes are still
            // needed to advance past it below.
            let mut wkey = [0u8; KEY_MAX];
            {
                let wk: &[u8] = if winner_src == 0 {
                    &self.k.mem_key[..winner_key_len]
                } else {
                    let (wli, wti) = self.cursor_pos(winner_src);
                    &self.k.cursors.as_flattened()[self.level_base[wli] + wti].key[..winner_key_len]
                };
                wkey[..winner_key_len].copy_from_slice(wk);
            }
            let wkey = &wkey[..winner_key_len];
            // Range tombstones and TTL expiry shadow the point winner: a
            // covering range tombstone newer than the winning version, or a
            // winning value with `expire_at <= now`, hides the key. An
            // expired winner never falls through to an older version.
            let covered = self
                .src
                .rdel_hides(wkey, self.max_seq, winner_ver, &mut self.blocks.bufs.raw)
                .await?;
            let expired =
                !winner_tombstone && winner_expire_at != 0 && winner_expire_at <= self.now;
            // Sequence 0 is reserved (the counter issues 1 and up): a
            // version carrying it — only possible in an externally built,
            // ingested table — is invisible, exactly as on the point-read
            // path.
            if winner_ver.seq == 0 || winner_tombstone || covered || expired {
                // Hidden: advance past the key without yielding it and
                // without demanding caller buffer space for it.
                self.advance_past_key_rev(wkey, total).await?;
                continue;
            }
            // Buffer checks and the key copy happen *before* any cursor
            // advances, so BufferTooSmall never consumes an entry.
            if winner_key_len > key_buf.len() {
                return Err(Error::BufferTooSmall {
                    need: winner_key_len,
                });
            }
            key_buf[..winner_key_len].copy_from_slice(wkey);
            if winner_val_len > val_buf.len() {
                return Err(Error::BufferTooSmall {
                    need: winner_val_len,
                });
            }
            // Copy the winner's value while its cursor is still parked.
            // (A tombstone winner continued above, so this always copies.)
            if winner_src == 0 {
                val_buf[..winner_val_len].copy_from_slice(&self.k.mem_val[..winner_val_len]);
            } else {
                let db = self.src;
                let at = (winner_table, winner_bid, winner_off, winner_end);
                let copied = match self.blocks.value_held(at, val_buf) {
                    Some(copied) => copied?,
                    None => {
                        self.blocks
                            .value_into(db.device, db.cache, at, val_buf)
                            .await?
                    }
                };
                debug_assert_eq!(copied, winner_val_len);
            }
            // Advance every head sitting on the maximum key — the winner
            // included. Compare against the copy in `key_buf`: the heads
            // move as cursors advance. A source may hold several versions
            // of the yielded key (newest first); each advance parks
            // strictly below the key, so loop until the head leaves the
            // key — otherwise an older version would be yielded as a
            // duplicate.
            self.advance_past_key_rev(&key_buf[..winner_key_len], total)
                .await?;
            return Ok(Some((winner_key_len, winner_val_len)));
        }
    }

    /// The source of the maximum live head key, and that key itself, or
    /// `None` when every source is exhausted. Source 0 is the memtable;
    /// sources 1..=total are table cursors in level-major order. Carries
    /// the leader's key alongside its source so callers (and this loop's
    /// own comparisons) never re-derive it through another `cursor_pos`
    /// lookup.
    fn max_src(&self, total: usize) -> Option<(usize, &[u8])> {
        // Through the keys once, not once a source.
        let keys = &*self.k;
        let mut best: Option<(usize, &[u8])> = None;
        for src in 0..=total {
            let Some(k) = self.head_key(keys, src) else {
                continue;
            };
            if best.is_none_or(|(_, m)| k > m) {
                best = Some((src, k));
            }
        }
        best
    }

    /// Advances every head currently sitting on `maxkey` — the memtable
    /// position and each live table cursor — so the next `prev()` call
    /// sees a fresh maximum key. A table cursor holding several versions
    /// of the key re-parks strictly below the key until its head leaves
    /// it.
    async fn advance_past_key_rev(
        &mut self,
        maxkey: &[u8],
        total: usize,
    ) -> Result<(), Error<D::Error>> {
        while self.mem_live && &self.k.mem_key[..self.mem_key_len] == maxkey {
            if self.mem_idx == 0 {
                self.mem_live = false;
                break;
            }
            self.mem_idx -= 1;
            self.advance_mem_rev();
        }
        // Copy the bound: the re-park borrows it while `self` is mutably
        // borrowed for the park.
        let mut bk = [0u8; KEY_MAX];
        bk[..maxkey.len()].copy_from_slice(maxkey);
        let bound = &bk[..maxkey.len()];
        for src in 1..=total {
            let (li, ti) = self.cursor_pos(src);
            loop {
                let tied = {
                    let c = &self.k.cursors.as_flattened()[self.level_base[li] + ti];
                    c.live && &c.key[..c.key_len] == maxkey
                };
                if !tied {
                    break;
                }
                self.park_cursor_rev(li, ti, Some((bound, false))).await?;
            }
        }
        Ok(())
    }

    /// Adds a cursor over `tref`, parked at the greatest entry `<= from`
    /// with `seq <= max_seq` (`from` empty: the table's last entry). The
    /// caller key-range prunes; this resolves the table's block index to
    /// the starting data block. When `from` sorts before the table's
    /// first block, no cursor is added — the table cannot contribute.
    async fn add_cursor_rev(
        &mut self,
        tref: &TableRef<KEY_MAX>,
        li: usize,
        from: &[u8],
    ) -> Result<(), Error<D::Error>> {
        // The data section leads the table; the scan cursor walks data
        // blocks only.
        let data_first = tref.data_first();
        let data_blocks = tref.data_blocks().ok_or(Error::CorruptBlock {
            id: tref.first_block,
        })?;
        if data_blocks == 0 {
            // Range-only table (flush or compaction carrying nothing but
            // range tombstones): no point cursor to add. Its rdel section
            // still shadows keys via the separate covering probes, which
            // walk the manifest, not the cursors.
            return Ok(());
        }
        // The manifest names the index block: no footer is read.
        let index_id = tref.index_block().ok_or(Error::CorruptManifest)?;
        // Last data block whose first key sorts at/before `from`; empty
        // `from` starts at the table's last data block.
        let block_idx = if from.is_empty() {
            data_blocks - 1
        } else {
            let db = self.src;
            match sstable::search_verified(
                db.device,
                db.cache,
                tref.id,
                index_id,
                &mut self.blocks.bufs.raw,
                |payload, check| {
                    sstable::index_last_le_block::<D::Error>(payload, from, index_id, check)
                },
            )
            .await?
            {
                Some(id) => id
                    .checked_sub(data_first)
                    .ok_or(Error::CorruptBlock { id })?,
                None => return Ok(()),
            }
        };
        if block_idx >= data_blocks {
            return Err(Error::CorruptBlock {
                id: tref.first_block,
            });
        }
        let bid = data_first
            .checked_add(block_idx)
            .ok_or(Error::CorruptBlock {
                id: tref.first_block,
            })?;
        let ti = self.cursor_counts[li];
        // The manifest holds at most the pool's worth of tables, so this
        // never fails; if it did, the manifest is what is wrong.
        let slot = self
            .k
            .cursors
            .as_flattened_mut()
            .get_mut(self.level_base[li] + ti)
            .ok_or(Error::CorruptManifest)?;
        *slot = RevCursor {
            table_id: tref.id,
            node_id: tref.node_id,
            seal_wall: tref.seal_wall,
            first_block: data_first,
            block_idx,
            block_id: bid,
            off: 0,
            // The park reads the block, and with it the entries end.
            end: 0,
            live: true,
            key: [0u8; KEY_MAX],
            key_len: 0,
            seq: 0,
            tombstone: false,
            val_len: 0,
            expire_at: 0,
        };
        self.cursor_counts[li] += 1;
        // Park at the inclusive ceiling, or unconditionally at the last
        // entry when `from` is empty.
        if from.is_empty() {
            self.park_cursor_rev(li, ti, None).await?;
        } else {
            // Copy the ceiling: the park borrows the bound while `self`
            // is mutably borrowed for the park.
            let mut ck = [0u8; KEY_MAX];
            let clen = self.ceil_len;
            ck[..clen].copy_from_slice(&self.k.ceil[..clen]);
            self.park_cursor_rev(li, ti, Some((&ck[..clen], true)))
                .await?;
        }
        Ok(())
    }

    /// Parks cursor `(li, ti)` at the greatest entry satisfying `bound`
    /// with `seq <= max_seq`, searching the cursor's current block and
    /// walking to earlier data blocks as needed. `bound` is
    /// `(key, inclusive)`: entries qualify when sorting below `key`, or
    /// equal when `inclusive`; `None` qualifies every entry. Sets
    /// `live = false` at exhaustion. A torn block is
    /// [`Error::CorruptBlock`]: unlike point lookups, a scan must never
    /// silently skip entries.
    async fn park_cursor_rev(
        &mut self,
        li: usize,
        ti: usize,
        bound: Option<(&[u8], bool)>,
    ) -> Result<(), Error<D::Error>> {
        // Copy the bound key: the search borrows it while `self` is
        // mutably borrowed for block loads.
        let mut bkey = [0u8; KEY_MAX];
        let blen = match bound {
            Some((k, _)) => {
                bkey[..k.len()].copy_from_slice(k);
                k.len()
            }
            None => 0,
        };
        let bincl = bound.is_some_and(|(_, incl)| incl);
        let bound_copy: Option<(&[u8], bool)> = bound.map(|_| (&bkey[..blen], bincl));
        let max_seq = self.max_seq;
        // The cursor's own block is searched for what the search reads;
        // a block it steps back into is taken whole (ADR-0035).
        let mut whole = false;
        let (tid, first, mut bid, mut idx) = {
            let c = &self.k.cursors.as_flattened()[self.level_base[li] + ti];
            if !c.live {
                return Ok(());
            }
            (c.table_id, c.first_block, c.block_id, c.block_idx)
        };
        // The cursor keeps its head, and the block it lies in, until the
        // park has found the next one: a block it steps back into is
        // read before the cursor moves, so a read that fails leaves the
        // cursor where it was, and a retried `prev` sees it unchanged.
        loop {
            let db = self.src;
            // Below the table's live lower bound every entry is dead (a
            // compaction already merged them into a newer table), and a
            // backward walk only goes lower.
            let lower = db.manifest.find_table(tid).map(|t| t.first_key);
            let mut search = |b: &[u8; BLOCK], g: &mut Gate<'_, '_, BLOCK>| {
                let (end, hit) =
                    search_block_rev::<D::Error, BLOCK, KEY_MAX>(b, bound_copy, max_seq, bid, g)?;
                let Some(h) = hit else {
                    return Ok((end, RevFound::Nothing));
                };
                if lower.is_some_and(|l| h.0[..h.1] < *l.as_slice()) {
                    return Ok((end, RevFound::Dead));
                }
                // A version run can straddle data blocks: when the
                // candidate is the block's first key, newer versions
                // may live in earlier blocks.
                let at_first =
                    idx > 0 && entry_key_at::<D::Error, BLOCK>(b, 0, end, bid, g)? == &h.0[..h.1];
                Ok((end, RevFound::Head(h, at_first)))
            };
            let (end, found) = match self.blocks.held(bid, &mut search) {
                Some(found) => found?,
                None => {
                    self.blocks
                        .load_with(db.device, db.cache, tid, bid, whole, &mut search)
                        .await?
                }
            };
            match found {
                RevFound::Head(h, at_first) => {
                    // A block can seal mid-run, so the run's first block
                    // need not share the first key. Following the run is
                    // what keeps a backward walker from settling on a
                    // stale version it met first.
                    let (key, klen, seq, tomb, vlen, exp, off) = h;
                    let (key, klen, seq, tomb, vlen, exp, off, bid, end, idx) = if at_first {
                        self.resolve_run(tid, first, idx, &key[..klen], max_seq)
                            .await?
                            .unwrap_or((key, klen, seq, tomb, vlen, exp, off, bid, end, idx))
                    } else {
                        (key, klen, seq, tomb, vlen, exp, off, bid, end, idx)
                    };
                    let c = &mut self.k.cursors.as_flattened_mut()[self.level_base[li] + ti];
                    c.key = key;
                    c.key_len = klen;
                    c.seq = seq;
                    c.tombstone = tomb;
                    c.val_len = vlen;
                    c.expire_at = exp;
                    c.off = off;
                    c.block_id = bid;
                    c.end = end;
                    c.block_idx = idx;
                    c.live = true;
                    return Ok(());
                }
                RevFound::Dead => {
                    self.k.cursors.as_flattened_mut()[self.level_base[li] + ti].live = false;
                    return Ok(());
                }
                RevFound::Nothing => {}
            }
            // Block yielded nothing: step to the previous data block, if
            // any.
            if idx == 0 {
                self.k.cursors.as_flattened_mut()[self.level_base[li] + ti].live = false;
                return Ok(());
            }
            idx -= 1;
            bid = first
                .checked_add(idx)
                .ok_or(Error::CorruptBlock { id: first })?;
            whole = true;
        }
    }

    /// Follows a version run into earlier data blocks. `key` was found in
    /// block `idx` at that block's first key, so newer versions may live
    /// in previous blocks (a block can seal mid-run, leaving the run's
    /// head in a block whose first key sorts below `key`). Walks backward
    /// adopting each earlier block's newest visible `key` until a block's
    /// first key differs from `key` — the run cannot extend further — or
    /// a block yields no visible `key`. Returns the newest visible
    /// version's head info plus its block, or `None` when no earlier
    /// block holds a visible `key` (the caller's candidate stands).
    async fn resolve_run(
        &mut self,
        table_id: u32,
        first: u64,
        idx: u64,
        key: &[u8],
        max_seq: u64,
    ) -> Result<Option<RevHeadAt<KEY_MAX>>, Error<D::Error>> {
        // Copy the key: the searches borrow the bound while `self` is
        // mutably borrowed for block loads.
        let mut kk = [0u8; KEY_MAX];
        kk[..key.len()].copy_from_slice(key);
        let klen = key.len();
        let kk = &kk[..klen];
        let mut best: Option<RevHeadAt<KEY_MAX>> = None;
        let mut pidx = idx;
        while pidx > 0 {
            pidx -= 1;
            let pbid = first
                .checked_add(pidx)
                .ok_or(Error::CorruptBlock { id: first })?;
            let db = self.src;
            // Each earlier block of the run is read through: whole.
            let found = self
                .blocks
                .load_with(db.device, db.cache, table_id, pbid, true, &mut |b, g| {
                    let (pend, hit) = search_block_rev::<D::Error, BLOCK, KEY_MAX>(
                        b,
                        Some((kk, true)),
                        max_seq,
                        pbid,
                        g,
                    )?;
                    match hit {
                        Some(h) if h.0[..h.1] == *kk => {
                            let at_first =
                                entry_key_at::<D::Error, BLOCK>(b, 0, pend, pbid, g)? == kk;
                            Ok(Some((h, pend, at_first)))
                        }
                        _ => Ok(None),
                    }
                })
                .await?;
            let Some((hit, pend, at_first)) = found else {
                break;
            };
            best = Some((
                hit.0, hit.1, hit.2, hit.3, hit.4, hit.5, hit.6, pbid, pend, pidx,
            ));
            // The run can only straddle further while it starts at this
            // block's first key.
            if !at_first {
                break;
            }
        }
        Ok(best)
    }

    /// Parks the memtable head on the greatest key at/below `mem_idx` that
    /// has a point version visible at `max_seq`, choosing that key's
    /// *newest* visible version.
    ///
    /// A key's versions sit in one run of slots ordered newest-first, so a
    /// downward walk meets the oldest version first. The walk therefore
    /// resolves the whole run — from the landing slot down to the run
    /// start — and picks its lowest-index (newest) point slot with
    /// `seq <= max_seq`, leaving `mem_idx` at the run start so the next
    /// step moves to the previous key. Range-tombstone slots sort by their
    /// start key but are not point versions: they bound the run and are
    /// never yielded. The scan borrows the database, so no `put`/`flush`
    /// can shift slots under this index walk.
    fn advance_mem_rev(&mut self) {
        let max_seq = self.max_seq;
        let db = self.src;
        let mem = db.memtable;
        self.mem_live = false;
        while self.mem_idx < mem.slot_len() {
            let Some(landing) = mem.slot_view(self.mem_idx) else {
                return;
            };
            let key = landing.key;
            let mut run_start = self.mem_idx;
            let mut newest_visible: Option<usize> = None;
            let mut j = self.mem_idx;
            while let Some(w) = mem.slot_view(j) {
                if w.key != key {
                    break;
                }
                run_start = j;
                // Lower index = newer: the last hit on the way down wins.
                if !w.range_del && w.seq <= max_seq {
                    newest_visible = Some(j);
                }
                if j == 0 {
                    break;
                }
                j -= 1;
            }
            self.mem_idx = run_start;
            if let Some(v) = newest_visible.and_then(|b| mem.slot_view(b)) {
                self.k.mem_key[..v.key.len()].copy_from_slice(v.key);
                self.mem_key_len = v.key.len();
                self.k.mem_val[..v.val.len()].copy_from_slice(v.val);
                self.mem_val_len = v.val.len();
                self.mem_seq = v.seq;
                self.mem_tombstone = v.tombstone;
                self.mem_expire_at = v.expire_at;
                self.mem_live = true;
                return;
            }
            // No visible point version of this key: the previous key.
            if run_start == 0 {
                return;
            }
            self.mem_idx = run_start - 1;
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
    fn head_key<'a>(
        &self,
        keys: &'a RevScanKeys<KEY_MAX, VAL_MAX, LEVELS, TABLES>,
        src: usize,
    ) -> Option<&'a [u8]> {
        if src == 0 {
            if self.mem_live {
                Some(&keys.mem_key[..self.mem_key_len])
            } else {
                None
            }
        } else {
            let (li, ti) = self.cursor_pos(src);
            let c = &keys.cursors.as_flattened()[self.level_base[li] + ti];
            if c.live {
                Some(&c.key[..c.key_len])
            } else {
                None
            }
        }
    }

    /// Head `(key, seq, tombstone, val_len, expire_at)` of a source, or
    /// `None` when exhausted. Source 0 is the memtable; sources 1..=total
    /// are table cursors in level-major order. One `cursor_pos` lookup
    /// instead of the two `head_key`/`head_meta` used to cost a caller
    /// needing both.
    fn head<'a>(
        &self,
        keys: &'a RevScanKeys<KEY_MAX, VAL_MAX, LEVELS, TABLES>,
        src: usize,
    ) -> Option<(&'a [u8], u64, bool, usize, u64)> {
        if src == 0 {
            self.mem_live.then_some((
                &keys.mem_key[..self.mem_key_len],
                self.mem_seq,
                self.mem_tombstone,
                self.mem_val_len,
                self.mem_expire_at,
            ))
        } else {
            let (li, ti) = self.cursor_pos(src);
            let c = &keys.cursors.as_flattened()[self.level_base[li] + ti];
            c.live.then_some((
                &c.key[..c.key_len],
                c.seq,
                c.tombstone,
                c.val_len,
                c.expire_at,
            ))
        }
    }

    /// The head entry's version under the replica LWW merge rule: the
    /// memtable's versions are the node's own newest; table heads carry
    /// their table's origin.
    fn head_version(
        &self,
        keys: &RevScanKeys<KEY_MAX, VAL_MAX, LEVELS, TABLES>,
        src: usize,
    ) -> Version {
        if src == 0 {
            Version::memtable(self.src.node_id, self.mem_seq)
        } else {
            let (li, ti) = self.cursor_pos(src);
            let c = &keys.cursors.as_flattened()[self.level_base[li] + ti];
            Version::table(c.node_id, c.seal_wall, c.seq)
        }
    }
}

/// What a reverse park found in one block.
enum RevFound<const KEY_MAX: usize> {
    /// No entry satisfies the bound: step to the previous block.
    Nothing,
    /// The best entry sorts below the table's live lower bound.
    Dead,
    /// The best entry, and whether it is the block's first key.
    Head(RevHead<KEY_MAX>, bool),
}

/// The key of the entry at `off`, checked whole before it is used.
fn entry_key_at<'b, E, const BLOCK: usize>(
    b: &'b [u8; BLOCK],
    off: usize,
    end: usize,
    bid: u64,
    g: &mut Gate<'_, '_, BLOCK>,
) -> Result<&'b [u8], Error<E>> {
    entry_checked::<E, BLOCK>(b, off, end, bid, g).map(|e| e.key)
}

/// Searches data block `b` for the greatest entry satisfying `bound`
/// with `seq <= max_seq`. Returns the block's entries end, and the
/// entry's key bytes, key length, seq, tombstone flag, value length,
/// expiry tick, and offset — or `None` when the block holds no
/// qualifying entry. Every byte it acts on is checked first.
///
/// Positioning binary-searches the restart points for the last restart
/// that can lead to a qualifying entry, then walks regions backward. A
/// key's versions are stored newest-first, so the walk meets a
/// straddling run's older versions first: within a region the
/// strictly-greatest key wins, and across regions a tie keeps the
/// earlier region's (newer) version — a version run straddling restarts
/// still resolves to the newest visible version. The walk breaks out
/// early once the next region's first key drops below the best key
/// found: everything further back is smaller and cannot tie or beat it.
fn search_block_rev<E, const BLOCK: usize, const KEY_MAX: usize>(
    b: &[u8; BLOCK],
    bound: Option<(&[u8], bool)>,
    max_seq: u64,
    bid: u64,
    g: &mut Gate<'_, '_, BLOCK>,
) -> Result<(usize, Option<RevHead<KEY_MAX>>), Error<E>> {
    let end = entries_end::<E, BLOCK>(b, bid, g)?;
    let body_end = BLOCK - sstable::CRC_LEN;
    let rcount = (body_end - 2 - end) / 2;
    // A data block always carries at least one entry, hence at least
    // one restart point.
    if rcount == 0 || !g.check(end, body_end) {
        return Err(Error::CorruptBlock { id: bid });
    }
    let Some(rstart) = last_qualifying_restart::<E, BLOCK>(b, end, rcount, bound, bid, g)? else {
        // Every restart sorts above the bound: no entry in this block
        // can qualify.
        return Ok((end, None));
    };
    let best = walk_regions_rev::<E, BLOCK, KEY_MAX>(b, end, rstart, bound, max_seq, bid, g)?;
    Ok((end, best))
}

/// Binary search: the last restart whose key can lead to a qualifying
/// entry — `bound_ok` over the restart keys, taking the upper bound.
/// `None` when every restart sorts above the bound.
fn last_qualifying_restart<E, const BLOCK: usize>(
    b: &[u8; BLOCK],
    end: usize,
    rcount: usize,
    bound: Option<(&[u8], bool)>,
    bid: u64,
    g: &mut Gate<'_, '_, BLOCK>,
) -> Result<Option<usize>, Error<E>> {
    let bound_ok = |key: &[u8]| -> bool {
        match bound {
            None => true,
            Some((bk, inclusive)) => key < bk || (inclusive && key == bk),
        }
    };
    let mut lo = 0usize;
    let mut hi = rcount;
    let mut r: Option<usize> = None;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let roff = restart_at(b, end, mid).ok_or(Error::CorruptBlock { id: bid })?;
        if bound_ok(entry_key_at::<E, BLOCK>(b, roff, end, bid, g)?) {
            r = Some(mid);
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Ok(r)
}

/// Region walk, backward from region `rstart`: returns the greatest
/// entry satisfying `bound` with `seq <= max_seq`, newest visible
/// version winning ties — or `None` when the block holds no qualifying
/// entry.
fn walk_regions_rev<E, const BLOCK: usize, const KEY_MAX: usize>(
    b: &[u8; BLOCK],
    end: usize,
    rstart: usize,
    bound: Option<(&[u8], bool)>,
    max_seq: u64,
    bid: u64,
    g: &mut Gate<'_, '_, BLOCK>,
) -> Result<Option<RevHead<KEY_MAX>>, Error<E>> {
    let corrupt = || Error::CorruptBlock { id: bid };
    let mut best: Option<RevHead<KEY_MAX>> = None;
    let mut j = rstart;
    loop {
        // Early break: region `j`'s keys are all at/below the next
        // region's first key; once that drops below the best key found,
        // nothing further back can tie or beat it.
        if j < rstart
            && let Some(bk) = best.as_ref()
        {
            let nroff = restart_at(b, end, j + 1).ok_or_else(corrupt)?;
            if entry_key_at::<E, BLOCK>(b, nroff, end, bid, g)? < &bk.0[..bk.1] {
                break;
            }
        }
        let rs = restart_at(b, end, j).ok_or_else(corrupt)?;
        let limit = if j == rstart {
            end
        } else {
            restart_at(b, end, j + 1).ok_or_else(corrupt)?
        };
        // Region-best: the strictly-greatest qualifying key in this
        // region (newest visible version on ties, since the region runs
        // newest-first).
        let mut rbest: Option<RevHead<KEY_MAX>> = None;
        let mut off = rs;
        while off < limit {
            let Ok(entry) = sstable::parse_data_entry::<E, BLOCK>(b, off, end, bid) else {
                // Genuine zero padding marks end-of-entries, but only in
                // the top region (padding lives after the last entry);
                // anywhere else an unparseable entry is corruption.
                if j == rstart
                    && g.check(off, end)
                    && b.get(off..end).is_some_and(sstable::all_zero)
                {
                    break;
                }
                return Err(corrupt());
            };
            if !g.check(off, entry.next) {
                return Err(corrupt());
            }
            let next = entry.next;
            // Entries ascend: past the bound, nothing later in the region
            // can qualify.
            let dominated = match bound {
                None => false,
                Some((bk, inclusive)) => {
                    if inclusive {
                        entry.key > bk
                    } else {
                        entry.key >= bk
                    }
                }
            };
            if dominated {
                break;
            }
            if entry.seq <= max_seq {
                let better = rbest.as_ref().is_none_or(|rb| entry.key > &rb.0[..rb.1]);
                if better {
                    let mut kb = [0u8; KEY_MAX];
                    kb.get_mut(..entry.key.len())
                        .ok_or_else(corrupt)?
                        .copy_from_slice(entry.key);
                    rbest = Some((
                        kb,
                        entry.key.len(),
                        entry.seq,
                        entry.tombstone,
                        entry.val.len(),
                        entry.expire_at,
                        off,
                    ));
                }
            }
            off = next;
        }
        // Merge: a tie keeps the earlier region's (newer) version.
        if let Some(rb) = rbest {
            let replace = best.as_ref().is_none_or(|bb| rb.0[..rb.1] >= bb.0[..bb.1]);
            if replace {
                best = Some(rb);
            }
        }
        if j == 0 {
            break;
        }
        j -= 1;
    }
    Ok(best)
}

#[cfg(test)]
#[allow(
    clippy::cast_possible_truncation,
    clippy::range_plus_one,
    reason = "random test blocks: truncated randomness is still random, and `0..1 + n` reads as one or more"
)]
mod tests {
    //! The block parsers act only on bytes their gate let through: what a
    //! parse took from a lent image is all a later parse of the buffer
    //! needs, or the later parse says it missed (ADR-0035).
    extern crate std;
    use super::{
        CopyIn, Gate, Gated, Head, RevHead, entries_end, full, grain, grains, mask, park_in,
        search_block_rev, seek_block, value_at,
    };
    use crate::error::Error;
    use crate::sstable::CRC_LEN;
    use std::vec::Vec;

    type R<T> = Result<T, Error<()>>;

    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        fn below(&mut self, n: usize) -> usize {
            (self.next() % n as u64) as usize
        }
    }

    /// One entry as a block stores it: key, seq, op byte, value, expiry.
    type Entry = (Vec<u8>, u64, u8, Vec<u8>, u64);

    /// A raw data block of `entries` (sorted by key, newest version
    /// first), a restart every `every` entries, zero padding after them.
    fn block<const B: usize>(entries: &[Entry], every: usize) -> [u8; B] {
        let mut b = [0u8; B];
        let mut off = 0;
        let mut restarts = Vec::new();
        for (i, (k, seq, op, v, exp)) in entries.iter().enumerate() {
            if i % every == 0 {
                restarts.push(u16::try_from(off).unwrap());
            }
            let vl = if *op == 2 { 0 } else { v.len() };
            b[off..off + 2].copy_from_slice(&u16::try_from(k.len()).unwrap().to_le_bytes());
            b[off + 2..off + 4].copy_from_slice(&u16::try_from(vl).unwrap().to_le_bytes());
            b[off + 4..off + 12].copy_from_slice(&seq.to_le_bytes());
            b[off + 12] = *op;
            off += 13;
            b[off..off + k.len()].copy_from_slice(k);
            off += k.len();
            b[off..off + vl].copy_from_slice(&v[..vl]);
            off += vl;
            if *op == 4 {
                b[off..off + 8].copy_from_slice(&exp.to_le_bytes());
                off += 8;
            }
        }
        let body_end = B - CRC_LEN;
        let end = body_end - 2 - 2 * restarts.len();
        assert!(off <= end, "entries overflow the block");
        for (i, r) in restarts.iter().enumerate() {
            b[end + 2 * i..end + 2 * i + 2].copy_from_slice(&r.to_le_bytes());
        }
        b[body_end - 2..body_end]
            .copy_from_slice(&u16::try_from(restarts.len()).unwrap().to_le_bytes());
        b
    }

    /// Random entries that fit a `B`-byte block: keys from a small
    /// alphabet so that runs of versions occur, some tombstones and TTL
    /// puts.
    fn entries<const B: usize>(rng: &mut Rng, every: usize) -> Vec<Entry> {
        let room = B - CRC_LEN - 2;
        let mut keys: Vec<Vec<u8>> = (0..1 + rng.below(60))
            .map(|_| {
                let n = 1 + rng.below(12);
                (0..n).map(|_| b'a' + rng.below(6) as u8).collect()
            })
            .collect();
        keys.sort();
        keys.dedup();
        let mut out = Vec::new();
        let mut used = 0;
        'keys: for k in keys {
            let mut seq = 1 + rng.below(40) as u64 * 3;
            for _ in 0..1 + rng.below(4) {
                let op = [1u8, 1, 1, 2, 4][rng.below(5)];
                let v: Vec<u8> = (0..rng.below(40)).map(|_| rng.next() as u8).collect();
                let len =
                    13 + k.len() + if op == 2 { 0 } else { v.len() } + if op == 4 { 8 } else { 0 };
                let restarts = (out.len() + 1).div_ceil(every);
                if used + len + 2 * restarts > room {
                    break 'keys;
                }
                used += len;
                out.push((k.clone(), seq, op, v, 9));
                seq = seq.saturating_sub(1 + rng.below(3) as u64).max(1);
            }
        }
        if out.is_empty() {
            out.push((b"k".to_vec(), 1, 1, b"v".to_vec(), 0));
        }
        out
    }

    /// Runs `f` over `image` as a lend would: through a [`CopyIn`] gate
    /// whose cache check always holds and is counted. Returns the answer,
    /// the buffer it filled (the rest poisoned with `poison`) and its
    /// grains.
    fn lend<const B: usize, T>(
        image: &[u8; B],
        poison: &mut Rng,
        f: &mut dyn FnMut(&[u8; B], &mut Gate<'_, '_, B>) -> T,
    ) -> (T, [u8; B], u64) {
        let mut data = [0u8; B];
        let mut check = |_: usize, _: usize| true;
        let mut gate = Gate::Lent(CopyIn {
            image,
            data: &mut data,
            valid: 0,
            check: &mut check,
        });
        let answer = f(image, &mut gate);
        let Gate::Lent(CopyIn { valid, .. }) = gate else {
            unreachable!("the gate is a lend's")
        };
        let g = grain::<B>();
        for i in 0..grains::<B>() {
            if valid & (1 << i) == 0 {
                for byte in &mut data[i * g..((i + 1) * g).min(B)] {
                    *byte = poison.next() as u8;
                }
            }
        }
        (answer, data, valid)
    }

    fn key_of<const K: usize>(h: &Head<K>) -> &[u8] {
        &h.key[..h.key_len]
    }

    /// The first entry at or after `start` with `seq <= max_seq`, the
    /// slow way: every entry from the first.
    fn oracle_seek<const B: usize>(
        b: &[u8; B],
        start: &[u8],
        max_seq: u64,
    ) -> (usize, Option<Head<64>>) {
        let end = entries_end::<(), B>(b, 1, &mut Gate::Open).unwrap();
        let head =
            park_in::<(), B, 64>(b, 0, end, Some(start), max_seq, 1, &mut Gate::Open).unwrap();
        (end, head)
    }

    fn seek_parse_holds<const B: usize>(seed: u64) {
        let mut rng = Rng(seed);
        for _ in 0..300 {
            let every = 1 + rng.below(20);
            let es = entries::<B>(&mut rng, every);
            let image = block::<B>(&es, every);
            for _ in 0..8 {
                let start: Vec<u8> = (0..rng.below(13))
                    .map(|_| b'a' + rng.below(7) as u8)
                    .collect();
                let max_seq = if rng.below(3) == 0 {
                    1 + rng.below(120) as u64
                } else {
                    u64::MAX
                };
                let want = oracle_seek(&image, &start, max_seq);
                let mut poison = Rng(rng.next() | 1);
                let (got, data, valid): (R<(usize, Option<Head<64>>)>, _, _) =
                    lend(&image, &mut poison, &mut |b, g| {
                        seek_block::<(), B, 64>(b, &start, max_seq, 1, g)
                    });
                let (end, head) = got.unwrap();
                assert_eq!(end, want.0);
                assert_eq!(
                    head.map(|h| (h.off, h.next)),
                    want.1.map(|h| (h.off, h.next))
                );
                assert_eq!(head.as_ref().map(key_of), want.1.as_ref().map(key_of));
                // The same seek over the buffer finds all it reads there.
                let mut gate = Gate::Held(Gated::<B> {
                    valid,
                    missed: false,
                });
                let again = seek_block::<(), B, 64>(&data, &start, max_seq, 1, &mut gate).unwrap();
                assert!(!gate.missed());
                assert_eq!(
                    again.1.map(|h| (h.off, h.next)),
                    want.1.map(|h| (h.off, h.next))
                );
                let Some(h) = head else { continue };
                // The head's value and the step past it come from the
                // buffer, through its grains: no miss, the same answers.
                let mut gate = Gate::Held(Gated::<B> {
                    valid,
                    missed: false,
                });
                let mut out = [0u8; 64];
                let n = value_at::<(), B>(&data, h.off, end, 1, &mut gate, &mut out).unwrap();
                assert!(!gate.missed());
                assert_eq!(n, h.val_len);
                let next = park_in::<(), B, 64>(&data, h.next, end, None, max_seq, 1, &mut gate);
                let truth =
                    park_in::<(), B, 64>(&image, h.next, end, None, max_seq, 1, &mut Gate::Open);
                if !gate.missed()
                    && let Ok(n) = next
                {
                    let t = truth.unwrap();
                    assert_eq!(n.map(|h| (h.off, h.next)), t.map(|h| (h.off, h.next)));
                }
                // Further on, the buffer either answers truly or misses.
                let mut gate = Gate::Held(Gated::<B> {
                    valid,
                    missed: false,
                });
                let mut off = h.next;
                for _ in 0..17 {
                    let n = park_in::<(), B, 64>(&data, off, end, None, max_seq, 1, &mut gate);
                    let t =
                        park_in::<(), B, 64>(&image, off, end, None, max_seq, 1, &mut Gate::Open);
                    let (false, Ok(n)) = (gate.missed(), n) else {
                        break;
                    };
                    let t = t.unwrap();
                    assert_eq!(
                        n.map(|h| (h.off, h.next, h.seq)),
                        t.map(|h| (h.off, h.next, h.seq))
                    );
                    let Some(n) = n else { break };
                    off = n.next;
                }
            }
        }
    }

    #[test]
    fn a_seek_answers_from_the_bytes_it_checked() {
        seek_parse_holds::<512>(0x5EE1);
        seek_parse_holds::<1000>(0x5EE2);
        seek_parse_holds::<4096>(0x5EE3);
    }

    fn rev_key<const K: usize>(h: &RevHead<K>) -> (&[u8], u64, usize) {
        (&h.0[..h.1], h.2, h.6)
    }

    fn rev_parse_holds<const B: usize>(seed: u64) {
        let mut rng = Rng(seed);
        for _ in 0..300 {
            let every = 1 + rng.below(20);
            let es = entries::<B>(&mut rng, every);
            let image = block::<B>(&es, every);
            for _ in 0..8 {
                let bk: Vec<u8> = (0..1 + rng.below(12))
                    .map(|_| b'a' + rng.below(7) as u8)
                    .collect();
                let bound = match rng.below(3) {
                    0 => None,
                    1 => Some((bk.as_slice(), true)),
                    _ => Some((bk.as_slice(), false)),
                };
                let max_seq = if rng.below(3) == 0 {
                    1 + rng.below(120) as u64
                } else {
                    u64::MAX
                };
                let want =
                    search_block_rev::<(), B, 64>(&image, bound, max_seq, 1, &mut Gate::Open)
                        .unwrap();
                let mut poison = Rng(rng.next() | 1);
                let (got, data, valid): (R<(usize, Option<RevHead<64>>)>, _, _) =
                    lend(&image, &mut poison, &mut |b, g| {
                        search_block_rev::<(), B, 64>(b, bound, max_seq, 1, g)
                    });
                let got = got.unwrap();
                assert_eq!(got.0, want.0);
                assert_eq!(got.1.as_ref().map(rev_key), want.1.as_ref().map(rev_key));
                // The same search over the buffer needs nothing more.
                let mut gate = Gate::Held(Gated::<B> {
                    valid,
                    missed: false,
                });
                let again =
                    search_block_rev::<(), B, 64>(&data, bound, max_seq, 1, &mut gate).unwrap();
                assert!(!gate.missed());
                assert_eq!(again.1.as_ref().map(rev_key), want.1.as_ref().map(rev_key));
                // The step below the head answers truly or misses.
                let Some(h) = got.1 else { continue };
                let below = Some((&h.0[..h.1], false));
                let mut gate = Gate::Held(Gated::<B> {
                    valid,
                    missed: false,
                });
                let n = search_block_rev::<(), B, 64>(&data, below, max_seq, 1, &mut gate);
                let t = search_block_rev::<(), B, 64>(&image, below, max_seq, 1, &mut Gate::Open);
                // An error from a buffer that lacks grains is not
                // believed (`Blocks::load_with` takes the rest): a miss.
                if !gate.missed()
                    && let Ok(n) = n
                {
                    assert_eq!(
                        n.1.as_ref().map(rev_key),
                        t.unwrap().1.as_ref().map(rev_key)
                    );
                }
            }
        }
    }

    #[test]
    fn a_reverse_search_answers_from_the_bytes_it_checked() {
        rev_parse_holds::<512>(0x4E51);
        rev_parse_holds::<1000>(0x4E52);
        rev_parse_holds::<4096>(0x4E53);
    }

    /// A block whose padding holds a stray byte is corrupt. A buffer that
    /// lacks that grain must not call it padding: its own bytes there are
    /// not the block's.
    #[test]
    fn padding_is_believed_only_where_it_was_checked() {
        let es: Vec<Entry> = (0..3u8)
            .map(|i| (std::vec![b'a' + i], 5, 1, std::vec![7; 3], 0))
            .collect();
        let mut image = block::<4096>(&es, 16);
        let end = entries_end::<(), 4096>(&image, 1, &mut Gate::Open).unwrap();
        image[end - 1] = 1;
        let tail = 3 * (13 + 1 + 3);
        let whole = park_in::<(), 4096, 64>(&image, tail, end, None, u64::MAX, 1, &mut Gate::Open);
        assert!(whole.is_err(), "a stray byte in the padding is corruption");
        // A buffer holding the entries' grains, zeros elsewhere: the
        // padding check misses rather than finding zeros.
        let mut data = image;
        data[end - 1] = 0;
        let mut gate = Gate::Held(Gated::<4096> {
            valid: mask::<4096>(0, tail),
            missed: false,
        });
        let r = park_in::<(), 4096, 64>(&data, tail, end, None, u64::MAX, 1, &mut gate);
        assert!(gate.missed() && r.is_err());
    }

    #[test]
    fn grains_cover_the_block() {
        fn holds<const B: usize>() {
            let g = grain::<B>();
            assert!(grains::<B>() <= 64 && g * grains::<B>() >= B);
            assert_eq!(mask::<B>(0, B), full::<B>());
            assert_eq!(mask::<B>(0, 1), 1);
            assert_eq!(mask::<B>(B - 1, B), 1 << (grains::<B>() - 1));
            assert_eq!(mask::<B>(g - 1, g + 1), 0b11);
            let mut gate = Gate::Held(Gated::<B> {
                valid: mask::<B>(g, 2 * g),
                missed: false,
            });
            assert!(gate.check(g, 2 * g) && gate.check(5, 5) && !gate.missed());
            assert!(!gate.check(g - 1, g + 1) && gate.missed());
            assert!(
                !Gated::<B> {
                    valid: full::<B>(),
                    missed: false
                }
                .check(0, B + 1)
            );
        }
        holds::<512>();
        holds::<1000>();
        holds::<4096>();
        holds::<16384>();
        holds::<64>();
    }

    /// Blocks over a kept set of buffers forget the block an earlier
    /// scan left in them: a scan over a kept `ScanState` never answers
    /// from it, whatever its tag and grains say (ADR-0037).
    #[test]
    fn blocks_over_kept_buffers_forget_what_they_held() {
        use super::{BlockBufs, Blocks, Held};
        const B: usize = 4096;
        let mut bufs = BlockBufs::<B>::new();
        bufs.own.id = Some(9);
        bufs.own.valid = full::<B>();
        let mut blocks = Blocks::<B>::over(Held::Lent(&mut bufs));
        let mut any = |_: &[u8; B], _: &mut Gate<'_, '_, B>| -> R<()> { Ok(()) };
        assert!(blocks.held(9, &mut any).is_none());
        assert_eq!(blocks.current().id, None);
        assert_eq!((bufs.own.id, bufs.own.valid), (None, 0));
    }

    /// `held` answers from a buffer only what it can stand behind: a
    /// whole buffer's answer, error or not; a partial buffer's answer
    /// when its parse stayed inside the grains it holds and succeeded.
    /// Otherwise it leaves the buffer current, for `load_with` to take
    /// the rest of the block.
    #[test]
    fn held_answers_only_from_what_it_holds() {
        use super::{Blocks, ScanBlock};
        const B: usize = 4096;
        let mut pool = [ScanBlock::<B>::new(), ScanBlock::<B>::new()];
        let mut blocks = Blocks::<B>::over(super::Held::Own(super::BlockBufs::new()));
        blocks.lend(&mut pool);
        // own: block 9, whole. pool[0]: block 7, grain 0 only.
        blocks.bufs.own.id = Some(9);
        blocks.bufs.own.valid = full::<B>();
        blocks.pool[0].id = Some(7);
        blocks.pool[0].valid = 1;
        let mut calls = 0;
        let mut err = |_: &[u8; B], g: &mut Gate<'_, '_, B>| -> R<u8> {
            calls += 1;
            assert!(!g.takes_ahead());
            Err(Error::CorruptBlock { id: 0 })
        };
        // Whole: the error stands.
        assert!(matches!(blocks.held(9, &mut err), Some(Err(_))));
        assert_eq!(blocks.current().id, Some(9));
        // Partial, an error without a miss: not believed, buffer current.
        assert!(blocks.held(7, &mut err).is_none());
        assert_eq!(blocks.current().id, Some(7));
        // Not held: the current buffer is not its.
        assert!(blocks.held(5, &mut err).is_none());
        assert_ne!(blocks.current().id, Some(5));
        assert_eq!(calls, 2);
        // Partial, a parse inside what it holds: believed.
        let mut ok = |_: &[u8; B], g: &mut Gate<'_, '_, B>| -> R<u8> {
            Ok(u8::from(g.check(0, grain::<B>())))
        };
        assert!(matches!(blocks.held(7, &mut ok), Some(Ok(1))));
        // Partial, a parse that reaches past it: a miss, even if Ok.
        let mut past = |_: &[u8; B], g: &mut Gate<'_, '_, B>| -> R<u8> {
            Ok(u8::from(g.check(0, grain::<B>() + 1)))
        };
        assert!(blocks.held(7, &mut past).is_none());
        assert_eq!(blocks.current().id, Some(7));
    }
}
