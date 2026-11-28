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
use crate::compress::Inflate;
use crate::db::Db;
use crate::device::BlockDevice;
use crate::error::Error;
use crate::manifest::{Manifest, TableRef};
use crate::memtable::MemTable;
use crate::points::{POINT_LEN, point_at, table_of};
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
/// buffer and a read scratch of `BLOCK` bytes each, two key buffers, a
/// value buffer and the cursors (43,872 bytes at the host store's shape
/// of 16 KiB blocks, 128-byte keys and 4 KiB values).
///
/// A scan reads nothing of a state before it has written it. What
/// decides whether it reads a byte (key lengths, cursor counts, the
/// memtable head, which block a buffer holds and which of its grains
/// were checked) lives in the scan and starts fresh with it, so a scan
/// over a kept state starts as a new scan does (ADR-0037). A state, like
/// a lent block pool, may serve scans of any store. A block cache may
/// not: it keys a block by its table and block id, which another store
/// reuses.
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
    /// For a compressed block inflated only part way, the decode to go
    /// on with ([`Resume`], ADR-0039). Meaningful only while `id` is
    /// `Some`, and cleared with `valid`.
    resume: Resume,
    data: [u8; BLOCK],
}

/// A partial inflate a buffer holds (ADR-0039): where the decode stood
/// once the prefix it made final was kept, and the length of the
/// compressed stream, which names the stream. `clen` 0: none. The
/// buffer's grains are then exactly those below `dp`
/// ([`grains_below`]); the output below `dp` is the window the decode
/// goes on from.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Resume {
    /// Stream bytes consumed: the next token starts here.
    sp: u16,
    /// Bytes of the logical block that are final.
    dp: u16,
    /// The stream's length; bit 15 set once two lends or more have gone
    /// on with the decode (each decodes further past what its parse
    /// reads than the one before, [`prefix_step`]).
    clen: u16,
    /// Where the decode began (ADR-0040): 0, the block's start; else the
    /// token of an entry point, below which the buffer holds nothing of
    /// the block. Bytes from here to the end of its grain are final too.
    lo: u16,
}

impl Resume {
    const NONE: Self = Self {
        sp: 0,
        dp: 0,
        clen: 0,
        lo: 0,
    };

    /// The decode `inflate` of a stream of `clen` bytes, begun at `lo`
    /// and kept by its `lends`th lend (1 or more); `None` when its
    /// positions do not fit (a block over 64 KiB is inflated whole).
    fn of(inflate: &Inflate, clen: usize, lends: u8, lo: usize) -> Option<Self> {
        let len = u16::try_from(clen).ok().filter(|&c| c != 0 && c < 0x8000)?;
        Some(Self {
            sp: u16::try_from(inflate.consumed()).ok()?,
            dp: u16::try_from(inflate.done()).ok()?,
            clen: if lends >= 2 { len | 0x8000 } else { len },
            lo: u16::try_from(lo).ok()?,
        })
    }

    /// The compressed stream's length.
    const fn stream_len(self) -> u16 {
        self.clen & 0x7FFF
    }

    /// Lends that have gone on with the decode: 1, or 2 for more.
    const fn lends(self) -> u8 {
        if self.clen & 0x8000 != 0 { 2 } else { 1 }
    }
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
            resume: Resume::NONE,
            data: [0u8; BLOCK],
        }
    }
}

impl<const BLOCK: usize> ScanBlock<BLOCK> {
    /// Drops its tag: it holds no block.
    const fn forget(&mut self) {
        self.id = None;
        self.valid = 0;
        self.resume = Resume::NONE;
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
    /// A compressed block being inflated from its lent image, final as
    /// far as [`Prefix::upto`]: a parse that reads further is run again
    /// over more (ADR-0039).
    Prefix(Prefix<'a>),
}

/// A gate over the part of a compressed block inflated so far, from
/// stream bytes the lend checked ([`lend_prefix`], ADR-0039). A range
/// holds when it lies below `upto`; one that does not is a miss, and the
/// furthest byte a miss asked for is recorded, for the parse to be run
/// again once the decode has gone past it.
#[derive(Clone, Copy)]
struct Prefix<'a> {
    /// Bytes `lo..upto` of the logical block are final: the decode
    /// began at `lo`, the block's start or an entry point (ADR-0040).
    lo: usize,
    upto: usize,
    /// Where a seek's walk had got to: every entry before it was passed
    /// over, from final bytes, in an earlier run of the parse.
    walked: usize,
    /// The furthest byte a miss asked for, and the lowest.
    want: usize,
    low: usize,
    missed: bool,
    /// A miss asked for bytes below `lo`, which the decode cannot make.
    below: bool,
    /// The block's entry points, checked (ADR-0040); empty: none.
    table: &'a [u8],
    /// A seek's binary search over the chunk heads `1..=n`: the first
    /// whose key sorts at or above its start lies in `sl..sh`.
    sl: u8,
    sh: u8,
    /// The chunk the decode began at, [`NO_CHUNK`] while there is none.
    main: u8,
    /// The chunk a probe decoded, and the bytes of it final.
    probed: u8,
    plo: usize,
    phi: usize,
    /// What the parse asks the lend to decode before it runs again.
    ask: Ask,
}

/// No chunk: see [`Prefix::main`].
const NO_CHUNK: u8 = u8::MAX;

/// What a parse over a [`Prefix`] asks for, besides bytes past the
/// decode's end (ADR-0040).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Ask {
    Nothing,
    /// The head of chunk `.0`, final up to `.1`: a probe of the search.
    Probe(u8, usize),
    /// The decode, begun at chunk `.0`.
    Main(u8),
    /// The whole block, from its start.
    Whole,
}

impl Prefix<'_> {
    /// Whether bytes `from..to` are final; a miss is recorded.
    const fn check(&mut self, from: usize, to: usize) -> bool {
        if to <= from || (from >= self.lo && to <= self.upto) {
            return true;
        }
        self.missed = true;
        if from < self.lo {
            self.below = true;
        }
        if to > self.want {
            self.want = to;
        }
        if from < self.low {
            self.low = from;
        }
        false
    }

    /// Whether a probe has made bytes `from..to` of chunk `c` final.
    const fn probed(&self, c: u8, from: usize, to: usize) -> bool {
        self.probed == c && from >= self.plo && to <= self.phi
    }

    /// Asks for `ask`, a miss.
    const fn ask(&mut self, ask: Ask) {
        self.ask = ask;
        self.missed = true;
    }
}

impl<const BLOCK: usize> Gate<'_, '_, BLOCK> {
    /// Whether bytes `from..to` may be acted on.
    #[allow(clippy::inline_always)] // a fourth variant stopped LLVM inlining it: measured
    #[inline(always)]
    fn check(&mut self, from: usize, to: usize) -> bool {
        match self {
            Self::Open => to <= from || to <= BLOCK,
            Self::Held(g) => g.check(from, to),
            Self::Lent(c) => c.check(from, to),
            Self::Prefix(p) => p.check(from, to),
        }
    }

    /// For a block being inflated part way, where a seek walks from:
    /// the block's restarts lie at its end, not yet inflated.
    const fn linear_from(&self) -> Option<usize> {
        match self {
            Self::Prefix(p) => Some(p.walked),
            _ => None,
        }
    }

    /// A seek's walk has reached `off`, every entry before it passed
    /// over.
    const fn at(&mut self, off: usize) {
        if let Self::Prefix(p) = self {
            p.walked = off;
        }
    }

    /// Whether bytes `from..to` are already let through, asking for
    /// nothing: no check is made and no miss recorded. A lend's gate
    /// answers `false`, its bytes being checked only when asked.
    const fn holds(&self, from: usize, to: usize) -> bool {
        if to <= from {
            return true;
        }
        match self {
            Self::Open => to <= BLOCK,
            Self::Held(g) => g.holds(from, to),
            Self::Lent(_) => false,
            Self::Prefix(p) => from >= p.lo && to <= p.upto,
        }
    }

    /// Whether `ahead` does anything: only a lend takes bytes ahead.
    const fn takes_ahead(&self) -> bool {
        matches!(self, Self::Lent(_) | Self::Prefix(_))
    }

    /// Bytes `from..to` the scan's next step will likely read.
    #[inline]
    fn ahead(&mut self, from: usize, to: usize) {
        match self {
            // A failed check is sticky: the lend becomes a miss.
            Self::Lent(c) => {
                let _ = c.check(from, to);
            }
            // A prefix short of it is inflated further, and the parse
            // run again.
            Self::Prefix(p) => {
                let _ = p.check(from, to);
            }
            _ => {}
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
    /// Where a kept decode began ([`Resume::lo`]): bytes from it to the
    /// end of its grain hold too, when the grain after it does.
    lo: usize,
    missed: bool,
}

impl<const BLOCK: usize> Gated<BLOCK> {
    #[inline]
    const fn holds(&self, from: usize, to: usize) -> bool {
        if to <= from {
            return true;
        }
        if to > BLOCK {
            return false;
        }
        let m = mask::<BLOCK>(from, to);
        self.valid & m == m || (self.lo != 0 && self.holds_past_lo(from, m))
    }

    /// The rest of [`holds`](Self::holds), for a buffer whose decode
    /// began at an entry point (ADR-0040): the grain `lo` lies in holds
    /// from `lo` on once the grain after it does.
    #[cold]
    #[inline(never)]
    const fn holds_past_lo(&self, from: usize, m: u64) -> bool {
        let g = grain::<BLOCK>();
        let first = self.lo / g;
        if from < self.lo || first + 1 >= 64 {
            return false;
        }
        // The decode began inside grain `first` and made the rest of it:
        // the grain after it is whole.
        let partial = 1u64 << first;
        self.valid & (partial << 1) != 0 && (self.valid | partial) & m == m
    }

    const fn check(&mut self, from: usize, to: usize) -> bool {
        let held = self.holds(from, to);
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

/// How much of a block that no buffer holds a load takes (see
/// [`Blocks::load_with`]).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Take {
    /// What the parse reads, each range checked first, when the cache
    /// checks ranges; a compressed block is inflated whole.
    Ranged,
    /// As [`Ranged`](Self::Ranged), but a compressed block is inflated
    /// only as far as the parse reads, from as much of its stream as
    /// that takes (ADR-0039). A forward scan's: a walk from the first
    /// entry finds its head, where a reverse scan starts from the
    /// restarts at the block's end.
    Prefix,
    /// The whole block, for a cursor about to read it through.
    Whole,
}

impl Take {
    /// [`Whole`](Self::Whole) when `whole`, else [`Ranged`](Self::Ranged).
    const fn ranged_unless(whole: bool) -> Self {
        if whole { Self::Whole } else { Self::Ranged }
    }
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
        block.resume = Resume::NONE;
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
        block.resume = Resume::NONE;
    }

    /// Forgets every decoded block (a re-seek starts from scratch).
    fn forget(&mut self) {
        self.bufs.own.forget();
        for lent in self.pool.iter_mut() {
            lent.forget();
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
        take: Take,
        f: &mut impl FnMut(&[u8; BLOCK], &mut Gate<'_, '_, BLOCK>) -> Result<R, Error<D::Error>>,
    ) -> Result<R, Error<D::Error>> {
        if let Some(answer) = self.held(bid, f) {
            return answer;
        }
        // `held` found the buffer, and it lacks what the parse reads: the
        // rest of it, unless it is a partial inflate to go on with.
        if self.current().id == Some(bid) && !(cache.is_some() && self.extends(take)) {
            self.complete(device, cache, table_id, bid).await?;
            return f(&self.current().data, &mut Gate::Open);
        }
        let mut missed = false;
        if let Some(c) = cache {
            match self.lend_or_extend(c, table_id, bid, take, f) {
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
            lo: usize::from(block.resume.lo),
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
        take: Take,
        out: &mut [u8],
    ) -> Result<usize, Error<D::Error>> {
        self.load_with(device, cache, table_id, bid, take, &mut |b, g| {
            value_at(b, off, end, bid, g, out)
        })
        .await
    }

    /// Whether a load of the block the current buffer holds goes on with
    /// its partial inflate ([`extend`](Self::extend)) rather than taking
    /// the rest of it whole ([`complete`](Self::complete)).
    fn extends(&self, take: Take) -> bool {
        matches!(take, Take::Prefix) && self.current().resume.clen != 0
    }

    /// [`load_with`](Self::load_with)'s lend, synchronous so that it adds
    /// nothing to the future every scan step holds: [`extend`] when the
    /// current buffer holds a partial inflate of block `bid` (which it
    /// forgets unless the lend answered: the block is then read whole),
    /// else [`lend_into`] a fresh buffer.
    ///
    /// [`extend`]: Self::extend
    /// [`lend_into`]: Self::lend_into
    fn lend_or_extend<E, R>(
        &mut self,
        cache: &dyn CachePort<BLOCK>,
        table_id: u32,
        bid: u64,
        take: Take,
        f: &mut impl FnMut(&[u8; BLOCK], &mut Gate<'_, '_, BLOCK>) -> Result<R, Error<E>>,
    ) -> Lend<Result<R, Error<E>>> {
        if self.current().id != Some(bid) {
            return self.lend_into(cache, table_id, bid, take, f);
        }
        debug_assert!(self.extends(take));
        let lent = self.extend(cache, table_id, bid, f);
        if !matches!(lent, Lend::Done(_)) {
            self.split().1.forget();
        }
        lent
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
        take: Take,
        f: &mut impl FnMut(&[u8; BLOCK], &mut Gate<'_, '_, BLOCK>) -> Result<R, Error<E>>,
    ) -> Lend<Result<R, Error<E>>> {
        let ranged = take != Take::Whole && cache.checks_ranges();
        let body_end = BLOCK - sstable::CRC_LEN;
        let (_, block) = self.victim();
        let data = &mut block.data;
        let mut valid = 0u64;
        let mut resume = Resume::NONE;
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
            if flag & 0x8000 != 0 && ranged && take == Take::Prefix && keeps_decode::<BLOCK>() {
                let clen = usize::from(flag & 0x7FFF);
                let table = match table_of(image, clen, check) {
                    Ok(table) => table,
                    Err(true) => return,
                    Err(false) => {
                        if check(0, BLOCK) {
                            answer = Some(Err(Error::CorruptBlock { id: bid }));
                        }
                        return;
                    }
                };
                let kept = lend_prefix(image, data, None, clen, 0, table, bid, check, f);
                if let Some((parsed, grains, partial)) = kept {
                    answer = Some(parsed);
                    valid = grains;
                    resume = partial;
                }
                return;
            }
            if flag & 0x8000 != 0 {
                let clen = usize::from(flag & 0x7FFF);
                let inflated = clen < body_end
                    && check(0, clen)
                    && table_of(image, clen, check).is_ok()
                    && crate::points::inflate_image(
                        &image[..body_end],
                        clen,
                        &mut data[..body_end],
                    );
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
                    let (_, block) = self.split();
                    block.resume = resume;
                }
                Lend::Done(answer)
            }
            (Some(_), _) => Lend::Miss,
        }
    }

    /// Takes the rest of block `bid`, of which the current buffer holds
    /// part, in one lend: the grains it lacks, each checked and copied,
    /// or, for a compressed block inflated part way, the rest of its
    /// stream inflated ([`inflate_rest`](Self::inflate_rest)). What
    /// that brings in is kept only if the lend held; otherwise the block
    /// is read from the device and replaces the buffer whole. Nothing
    /// already in the buffer is re-vouched by a check of the cache's
    /// image.
    async fn complete<D: BlockDevice>(
        &mut self,
        device: &D,
        cache: Option<&dyn CachePort<BLOCK>>,
        table_id: u32,
        bid: u64,
    ) -> Result<(), Error<D::Error>> {
        let lent = cache.and_then(|c| self.take_rest(c, table_id, bid));
        if lent == Some(true) {
            return Ok(());
        }
        let (raw, block) = self.split();
        block.forget();
        sstable::read_data_block(
            device,
            cache,
            table_id,
            bid,
            raw,
            &mut block.data,
            lent.is_some(),
        )
        .await?;
        self.loaded(bid, full::<BLOCK>());
        Ok(())
    }

    /// The step of [`load_with`](Self::load_with) for a parse that reads
    /// past the partial inflate the current buffer holds (ADR-0039): one
    /// lend goes on with the kept decode ([`Resume`]) only as far as the
    /// parse reads, as [`lend_prefix`] does, from the stream bytes ahead
    /// of it, which this lend checks; the output it copies from is the
    /// buffer's own. The image's flag must name the same stream. The
    /// buffer keeps what the lend adds only if the lend held.
    fn extend<E, R>(
        &mut self,
        cache: &dyn CachePort<BLOCK>,
        table_id: u32,
        bid: u64,
        f: &mut impl FnMut(&[u8; BLOCK], &mut Gate<'_, '_, BLOCK>) -> Result<R, Error<E>>,
    ) -> Lend<Result<R, Error<E>>> {
        let body_end = BLOCK - sstable::CRC_LEN;
        let (_, block) = self.split();
        let r = block.resume;
        let held = block.valid;
        let data = &mut block.data;
        let mut kept = None;
        let lent = cache.with_checked(table_id, bid, &mut |image, check| {
            let Ok(image) = <&[u8; BLOCK]>::try_from(image) else {
                return;
            };
            let at = body_end - 2;
            if !check(at, body_end)
                || u16::from_le_bytes([image[at], image[at + 1]]) != 0x8000 | r.stream_len()
            {
                return;
            }
            let clen = usize::from(r.stream_len());
            let Ok(table) = table_of(image, clen, check) else {
                return;
            };
            let st = Stream::resumed(r, table);
            let Some(entry) = chunk_at(table, r.lo) else {
                return;
            };
            debug_assert_eq!(
                held,
                grains_in::<BLOCK>(usize::from(r.lo), usize::from(r.dp)),
                "a kept decode names the grains it made"
            );
            kept = lend_prefix(
                image,
                data,
                Some((st, entry)),
                clen,
                r.lends(),
                table,
                bid,
                check,
                f,
            );
        });
        match (lent, kept) {
            (None, _) => Lend::Unlent,
            (Some(true), Some((answer, valid, resume))) => {
                if valid == 0 {
                    block.forget();
                } else {
                    block.valid = valid;
                    block.resume = resume;
                }
                Lend::Done(answer)
            }
            (Some(_), _) => Lend::Miss,
        }
    }

    /// [`complete`](Self::complete)'s one lend: [`inflate_rest`] for a
    /// partial inflate, else [`copy_rest`]. Its own function, so that the
    /// choice adds nothing to `complete`'s future, which every scan
    /// step's holds.
    ///
    /// [`inflate_rest`]: Self::inflate_rest
    /// [`copy_rest`]: Self::copy_rest
    fn take_rest(&mut self, cache: &dyn CachePort<BLOCK>, table_id: u32, bid: u64) -> Option<bool> {
        if self.current().resume.clen == 0 {
            self.copy_rest(cache, table_id, bid)
        } else {
            self.inflate_rest(cache, table_id, bid)
        }
    }

    /// [`complete`](Self::complete)'s lend for a raw block: copies in
    /// each grain the current buffer lacks, checked first. `Some(true)`
    /// when the buffer now holds the whole block; `Some(false)` when a
    /// check failed, or the image is a compressed block's (whose grains
    /// are not its logical block's); `None` when the cache lent nothing.
    fn copy_rest(&mut self, cache: &dyn CachePort<BLOCK>, table_id: u32, bid: u64) -> Option<bool> {
        let (_, block) = self.split();
        let mut valid = block.valid;
        let data = &mut block.data;
        let g = grain::<BLOCK>();
        let at = BLOCK - sstable::CRC_LEN - 2;
        let lent = cache.with_checked(table_id, bid, &mut |image, check| {
            if image.len() != BLOCK {
                return;
            }
            // The flag, from the buffer when it holds the trailer's
            // grain, else from the image, checked.
            let flag = if valid & mask::<BLOCK>(at, at + 2) == mask::<BLOCK>(at, at + 2) {
                &data[at..at + 2]
            } else if check(at, at + 2) {
                &image[at..at + 2]
            } else {
                return;
            };
            if flag[1] & 0x80 != 0 {
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
        let done = lent == Some(true) && valid == full::<BLOCK>();
        if done {
            block.valid = valid;
        }
        lent.map(|_| done)
    }

    /// [`complete`](Self::complete)'s lend for a compressed block the
    /// current buffer holds inflated part way (ADR-0039): the kept
    /// decode ([`Resume`]) goes on to the end of the block, from the
    /// stream bytes ahead of it, which this lend checks; the output it
    /// copies from is the buffer's own. The image's flag must name the
    /// same stream. Answers as [`copy_rest`](Self::copy_rest).
    ///
    /// A backstop: every load that can find a partial inflate in its
    /// buffer is a forward scan's, which goes on with it through
    /// [`extend`](Self::extend) instead. This one completes it for any
    /// other load, from the cache rather than the device.
    fn inflate_rest(
        &mut self,
        cache: &dyn CachePort<BLOCK>,
        table_id: u32,
        bid: u64,
    ) -> Option<bool> {
        let body_end = BLOCK - sstable::CRC_LEN;
        let (_, block) = self.split();
        let r = block.resume;
        let data = &mut block.data;
        let mut done = false;
        let lent = cache.with_checked(table_id, bid, &mut |image, check| {
            let Ok(image) = <&[u8; BLOCK]>::try_from(image) else {
                return;
            };
            let at = body_end - 2;
            if !check(at, body_end)
                || u16::from_le_bytes([image[at], image[at + 1]]) != 0x8000 | r.stream_len()
            {
                return;
            }
            let clen = usize::from(r.stream_len());
            let Ok(table) = table_of(image, clen, check) else {
                return;
            };
            // A decode begun at an entry point made nothing below it: the
            // block is inflated from its start instead (ADR-0040).
            let mut st = if r.lo == 0 {
                Stream::resumed(r, table)
            } else {
                Stream::fresh(clen)
            };
            done = st.grow(image, data, body_end, table, check) == Ok(true)
                && (table.is_empty()
                    || crate::points::points_are_restarts(&data[..body_end], table));
        });
        let done = lent == Some(true) && done;
        if done {
            block.valid = full::<BLOCK>();
            block.resume = Resume::NONE;
        }
        lent.map(|_| done)
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
#[inline]
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
    if let Gate::Prefix(p) = g {
        // Bytes past the prefix are not final: take the header first,
        // then the entry it gives; where the entries end, the whole
        // block, so that the step past the head knows it is done.
        let hdr = off + sstable::ENTRY_HEADER;
        if hdr > p.upto {
            let _ = p.check(off, hdr.min(BLOCK - sstable::CRC_LEN));
            return;
        }
        match sstable::parse_data_entry::<(), BLOCK>(b, off, end, 0) {
            Ok(n) => {
                let _ = p.check(off, n.next);
            }
            Err(_) => {
                // The rest of the block: a decode begun at an entry point
                // has all a park past here reads (ADR-0040).
                let _ = p.check(off, BLOCK - sstable::CRC_LEN);
            }
        }
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
    // A cursor a partial inflate positioned parses against the
    // provisional end; once the block's count word is in, against the
    // real one, so that no byte of the trailer is read as an entry
    // (review of ADR-0039).
    let body_end = BLOCK - sstable::CRC_LEN;
    let end = if end == provisional_end::<BLOCK>() && g.holds(body_end - 2, body_end) {
        entries_end::<E, BLOCK>(b, bid, g)?
    } else {
        end
    };
    loop {
        if off >= end {
            return Ok(None);
        }
        let Ok(e) = sstable::parse_data_entry::<E, BLOCK>(b, off, end, bid) else {
            // Past a partial inflate the entries end is not known yet:
            // the block's trailer says it.
            let end = if end == provisional_end::<BLOCK>() {
                entries_end::<E, BLOCK>(b, bid, g)?
            } else {
                end
            };
            let padding =
                off <= end && g.check(off, end) && b.get(off..end).is_some_and(sstable::all_zero);
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
    if let Some(from) = g.linear_from() {
        return seek_linear(b, from, start, max_seq, bid, g);
    }
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

/// The bound a cursor parses against in a block inflated only part way,
/// whose entries end is not known: no block's entries reach past it (its
/// trailer holds at least the count and restart 0).
const fn provisional_end<const BLOCK: usize>() -> usize {
    BLOCK - sstable::CRC_LEN - 4
}

/// [`seek_block`] over a compressed block being inflated (ADR-0039): a
/// walk from the first entry, or from where an earlier run of it
/// stopped short of the prefix, to the head. The entries end is
/// [`provisional_end`]. What the walk cannot decide (the end of the
/// entries, an entry that does not parse) it leaves to the whole block,
/// by asking for all of it: the driver then runs [`seek_block`] over the
/// whole block.
///
/// The block's restarts, which [`seek_block`] searches, lie at its end,
/// not yet inflated; this walk does what they are for. It steps from
/// entry to entry by their headers alone, and compares a key only every
/// [`SKIP_STRIDE`]th entry. Keys are sorted, so the head lies at or past
/// the last compared entry whose key sorts below `start` (the floor);
/// from there a full walk parses and compares each entry. Every header
/// the walk steps by is checked final first, and every entry it parses
/// is checked whole.
fn seek_linear<E, const BLOCK: usize, const KEY_MAX: usize>(
    b: &[u8; BLOCK],
    from: usize,
    start: &[u8],
    max_seq: u64,
    bid: u64,
    g: &mut Gate<'_, '_, BLOCK>,
) -> Result<(usize, Option<Head<KEY_MAX>>), Error<E>> {
    let corrupt = || Error::CorruptBlock { id: bid };
    let body_end = BLOCK - sstable::CRC_LEN;
    let end = provisional_end::<BLOCK>();
    let Some(from) = chunk_head(b, from, start, end, g) else {
        return Err(corrupt());
    };
    let mut off = skip_below(b, from, start, end, g);
    loop {
        g.at(off);
        let hdr = off + sstable::ENTRY_HEADER;
        if hdr > end {
            let _ = g.check(0, body_end);
            return Err(corrupt());
        }
        // A header past the prefix may parse as anything; its entry's
        // check then fails, since it ends past the header.
        let Ok(e) = sstable::parse_data_entry::<E, BLOCK>(b, off, end, bid) else {
            // Not an entry: a header not yet final is a miss; a final one
            // ends the entries, which the whole block decides.
            if g.check(off, hdr) {
                let _ = g.check(0, body_end);
            }
            return Err(corrupt());
        };
        if !g.check(off, e.next) {
            return Err(corrupt());
        }
        if e.seq <= max_seq && e.key >= start {
            let head = Head::of(&e, off).ok_or_else(corrupt)?;
            look_ahead(b, e.next, end, g);
            return Ok((end, Some(head)));
        }
        off = e.next;
    }
}

/// Output a probe of a chunk head asks for past the head: its entry,
/// most often (ADR-0040).
const PROBE_STEP: usize = 160;

/// The first part of [`seek_linear`] over a block with entry points
/// (ADR-0040): where its walk may begin. A binary search over the chunk
/// heads finds the last whose key sorts below `start`, each head decoded
/// by a probe of its own chunk, and the decode then begins at that
/// chunk; the walk begins at its head, or at `from` past it. The search
/// is kept in the gate across runs of the parse, so each run goes on
/// where the last stopped. `None`: the parse asked for a probe, the
/// decode, or the whole block (a head that does not parse), and runs
/// again. Over a gate with no table, `from`.
///
/// A decode kept from an earlier lend began at its chunk because that
/// head sorted below an earlier seek's start; a seek that sorts at or
/// below the head asks for the whole block.
#[inline]
fn chunk_head<const BLOCK: usize>(
    b: &[u8; BLOCK],
    from: usize,
    start: &[u8],
    end: usize,
    g: &mut Gate<'_, '_, BLOCK>,
) -> Option<usize> {
    match g {
        Gate::Prefix(p) if !p.table.is_empty() => search_heads(b, from, start, end, p),
        _ => Some(from),
    }
}

/// [`chunk_head`] over a block that has a table.
#[inline(never)]
fn search_heads<const BLOCK: usize>(
    b: &[u8; BLOCK],
    from: usize,
    start: &[u8],
    end: usize,
    p: &mut Prefix<'_>,
) -> Option<usize> {
    if start.is_empty() {
        p.sh = p.sl;
    }
    // The key of the entry at `head`, by the bytes `final_` lets through.
    while p.sl < p.sh {
        let mid = p.sl + (p.sh - p.sl) / 2;
        let head = point_at(p.table, usize::from(mid) - 1).2;
        let hdr = head + sstable::ENTRY_HEADER;
        if !p.probed(mid, head, hdr) {
            p.ask(Ask::Probe(mid, hdr + PROBE_STEP));
            return None;
        }
        let (kl, next) = header_span(b, head);
        if kl == 0 || next > end {
            p.ask(Ask::Whole);
            return None;
        }
        if !p.probed(mid, head, next) {
            p.ask(Ask::Probe(mid, next));
            return None;
        }
        if &b[hdr..hdr + kl] < start {
            p.sl = mid + 1;
        } else {
            p.sh = mid;
        }
    }
    let chunk = p.sl - 1;
    if p.main != chunk {
        p.ask(Ask::Main(chunk));
        return None;
    }
    if chunk == 0 {
        return Some(from);
    }
    let head = point_at(p.table, usize::from(chunk) - 1).2;
    let hdr = head + sstable::ENTRY_HEADER;
    if !p.check(head, hdr) {
        return None;
    }
    let (kl, next) = header_span(b, head);
    if kl == 0 || next > end {
        p.ask(Ask::Whole);
        return None;
    }
    if !p.check(head, next) {
        return None;
    }
    if &b[hdr..hdr + kl] >= start {
        p.ask(Ask::Whole);
        return None;
    }
    Some(from.max(head))
}

/// The key length and the end of the entry whose header lies at `off`
/// in `b`, read from the header alone (the caller has checked it is
/// final). The end counts a TTL entry's expiry.
fn header_span<const BLOCK: usize>(b: &[u8; BLOCK], off: usize) -> (usize, usize) {
    let h = &b[off..off + sstable::ENTRY_HEADER];
    let kl = usize::from(u16::from_le_bytes([h[0], h[1]]));
    let vl = usize::from(u16::from_le_bytes([h[2], h[3]]));
    let ttl = if h[12] == crate::wal::Op::PutTtl.to_u8() {
        8
    } else {
        0
    };
    (kl, off + sstable::ENTRY_HEADER + kl + vl + ttl)
}

/// How many entries [`seek_linear`] steps over by their headers between
/// the keys it compares: the restart interval, as [`seek_block`]'s
/// binary search over restarts would.
const SKIP_STRIDE: usize = 16;

/// The first part of [`seek_linear`]: from `from` (an entry every entry
/// before which was passed over), the last entry it can show sorts below
/// `start`, by comparing every [`SKIP_STRIDE`]th key and stepping between
/// them by headers. It stops at the first compared key at or above
/// `start`, and at anything it cannot step over: a header or a compared
/// entry not yet final, a header that ends the entries or does not fit.
/// The full walk from the floor then decides, and asks for what it
/// needs; this part asks for nothing, so that it never makes the decode
/// go past a head the full walk finds sooner.
fn skip_below<const BLOCK: usize>(
    b: &[u8; BLOCK],
    from: usize,
    start: &[u8],
    end: usize,
    g: &mut Gate<'_, '_, BLOCK>,
) -> usize {
    let Gate::Prefix(p) = g else {
        return from;
    };
    // Bytes the decode has made final; the walk reads no others.
    let upto = p.upto.min(end);
    let ttl = crate::wal::Op::PutTtl.to_u8();
    let (mut floor, mut off, mut stepped) = (from, from, 0usize);
    // The last entry stepped over since the floor that lies wholly in
    // the prefix: its header and key offset and its key's length.
    let mut last = None;
    loop {
        let hdr = off + sstable::ENTRY_HEADER;
        if hdr > upto {
            break;
        }
        let header = &b[off..hdr];
        let kl = usize::from(u16::from_le_bytes([header[0], header[1]]));
        let vl = usize::from(u16::from_le_bytes([header[2], header[3]]));
        if kl == 0 {
            // The end of the entries: the full walk from the floor meets
            // it, and leaves it to the whole block.
            return floor;
        }
        let next = hdr + kl + vl + if header[12] == ttl { 8 } else { 0 };
        if next > upto {
            break;
        }
        if stepped.is_multiple_of(SKIP_STRIDE) {
            if &b[hdr..hdr + kl] >= start {
                return floor;
            }
            floor = off;
            last = None;
        } else {
            last = Some((off, hdr, kl));
        }
        off = next;
        stepped += 1;
    }
    // Stopped at the prefix's end: the last entry wholly in it is the
    // floor if its key sorts below `start`, so that the full walk starts
    // there, and a parse run again over more of the block goes on from
    // about where this one stopped.
    if let Some((at, hdr, kl)) = last
        && &b[hdr..hdr + kl] < start
    {
        floor = at;
    }
    p.walked = floor;
    floor
}

/// Output a seek's partial inflate decodes past what its parse asked
/// for, so that the parse is run again once per few entries rather than
/// once per entry, and the steps after the seek's head find their
/// entries already in.
const PREFIX_STEP: usize = 1024;

/// How far past what its parse asks for the `lends`th lend of a partial
/// inflate decodes: [`PREFIX_STEP`] for the seek's, twice that for the
/// first step past it, and the rest of the block for the next. A short
/// scan stops soon after its last entry; one that has gone on twice is
/// reading through the block, and takes the rest in one lend (measured:
/// more steps cost a lend and a re-run parse each, ADR-0039).
const fn prefix_step(lends: u8) -> usize {
    match lends {
        0 | 1 => PREFIX_STEP << lends,
        _ => usize::MAX / 4,
    }
}

/// What [`lend_prefix`] keeps: the parse's answer, the grains of the
/// buffer it leaves valid, and the decode to go on with.
type Kept<R, E> = (Result<R, Error<E>>, u64, Resume);

/// The compressed branch of [`Blocks::lend_into`] for a forward scan
/// over a cache that checks ranges, and of [`Blocks::extend`]
/// (ADR-0039): block `bid`'s stream, at the front of `image`, is checked
/// and inflated into `data` only as far as the parse `f` reads, and `f`
/// runs over a [`Gate::Prefix`] that lets it act only on output already
/// final. The decode `st` starts at the stream's start, or where an
/// earlier lend (the `lends`th) kept it; this lend checks every stream
/// byte it hands the decoder, from where the decode stands.
///
/// - A parse that reaches past the prefix is run again over a longer
///   one. The decoder reads only stream bytes this lend has checked.
/// - A parse error is not believed from a prefix: the block is inflated
///   whole and `f` runs over all of it, as the whole-block path does.
///   A stream that does not decode is believed only once the whole
///   image passes its checks.
/// - The buffer keeps the whole grains below what the decode made
///   final, and the decode is kept with them ([`Resume`]), for
///   [`Blocks::extend`] or [`Blocks::complete`] to go on from.
///
/// `None`: a check failed, and the lend is a miss.
#[allow(clippy::too_many_arguments)]
fn lend_prefix<E, R, const BLOCK: usize>(
    image: &[u8; BLOCK],
    data: &mut [u8; BLOCK],
    start: Option<(Stream, u8)>,
    clen: usize,
    lends: u8,
    table: &[u8],
    bid: u64,
    check: &mut (dyn FnMut(usize, usize) -> bool + '_),
    f: &mut impl FnMut(&[u8; BLOCK], &mut Gate<'_, '_, BLOCK>) -> Result<R, Error<E>>,
) -> Option<Kept<R, E>> {
    // A block with no table, every block under 8 KiB among them, takes
    // a build of this with the table's logic folded away: it costs what
    // the decode cost before entry points (ADR-0040).
    if table.is_empty() {
        lend_prefix_in::<E, R, BLOCK, false>(image, data, start, clen, lends, table, bid, check, f)
    } else {
        lend_prefix_in::<E, R, BLOCK, true>(image, data, start, clen, lends, table, bid, check, f)
    }
}

/// [`lend_prefix`], for a block with a table of entry points (`POINTS`)
/// or with none.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn lend_prefix_in<E, R, const BLOCK: usize, const POINTS: bool>(
    image: &[u8; BLOCK],
    data: &mut [u8; BLOCK],
    start: Option<(Stream, u8)>,
    clen: usize,
    lends: u8,
    table: &[u8],
    bid: u64,
    check: &mut (dyn FnMut(usize, usize) -> bool + '_),
    f: &mut impl FnMut(&[u8; BLOCK], &mut Gate<'_, '_, BLOCK>) -> Result<R, Error<E>>,
) -> Option<Kept<R, E>> {
    let table: &[u8] = if POINTS { table } else { &[] };
    let body_end = BLOCK - sstable::CRC_LEN;
    let step = prefix_step(lends);
    // Damage the image's checks let through, as the whole path says.
    let corrupt = |check: &mut (dyn FnMut(usize, usize) -> bool + '_)| {
        check(0, BLOCK).then_some((Err(Error::CorruptBlock { id: bid }), 0, Resume::NONE))
    };
    if clen >= body_end {
        return corrupt(check);
    }
    let n = table.len() / POINT_LEN;
    // The decode the parse reads, and the chunk it began at. A fresh lend
    // over a block with entry points has none until a seek's search has
    // chosen its chunk (ADR-0040).
    let searching = start.is_none() && n > 0;
    let (mut main, entry) = match start {
        Some((st, e)) => (Some(st), e),
        None if n == 0 => (Some(Stream::fresh(clen)), 0),
        None => (None, 0),
    };
    let mut lo = entry_floor(table, entry);
    let mut target = main.as_ref().map_or(0, |d| d.inflate.done() + step);
    let mut probe: Option<(u8, Stream)> = None;
    #[allow(clippy::cast_possible_truncation)] // at most MAX_POINTS + 1
    let top = (n + 1) as u8;
    let mut p = Prefix {
        lo,
        upto: lo,
        walked: entry_head(table, entry),
        want: 0,
        low: usize::MAX,
        missed: false,
        below: false,
        table,
        sl: if searching {
            1
        } else {
            entry.saturating_add(1)
        },
        sh: if searching {
            top
        } else {
            entry.saturating_add(1)
        },
        main: if main.is_some() { entry } else { NO_CHUNK },
        probed: NO_CHUNK,
        plo: 0,
        phi: 0,
        ask: Ask::Nothing,
    };
    #[cfg(test)]
    let mut spins = 0u32;
    let parsed = loop {
        #[cfg(test)]
        {
            spins += 1;
            assert!(
                spins < 10_000,
                "lend_prefix spins: ask {:?} missed {} below {} lo {} upto {} want {} low {} main {} sl {} sh {} target {target} n {n}",
                p.ask,
                p.missed,
                p.below,
                p.lo,
                p.upto,
                p.want,
                p.low,
                p.main,
                p.sl,
                p.sh
            );
        }
        if let Some(decode) = main.as_mut() {
            match decode.grow(image, data, target, table, check) {
                Ok(true) => {}
                Ok(false) => return None,
                Err(()) => return corrupt(check),
            }
        }
        let dp = main.as_ref().map_or(lo, |d| d.inflate.done());
        if lo == 0 && dp >= body_end {
            // The whole block, its entry points checked: its parse, and
            // its errors, stand.
            if n > 0 && !crate::points::points_are_restarts(&data[..body_end], table) {
                return corrupt(check);
            }
            return Some((f(data, &mut Gate::Open), full::<BLOCK>(), Resume::NONE));
        }
        p.upto = dp;
        p.want = 0;
        p.low = usize::MAX;
        p.missed = false;
        p.below = false;
        p.ask = Ask::Nothing;
        if POINTS && let Some((c, ps)) = &probe {
            p.probed = *c;
            p.plo = entry_floor(table, *c);
            p.phi = ps.inflate.done();
        }
        let mut gate = Gate::Prefix(p);
        let parsed = f(data, &mut gate);
        // Fails closed: a gate that is no longer the prefix's answers
        // nothing, and the lend is a miss.
        let Gate::Prefix(ran) = gate else {
            return None;
        };
        p = ran;
        if !POINTS {
            // No table: ADR-0039's loop, the decode from the block's
            // start, run on past each miss.
            if p.missed {
                target = p.want.max(dp + 1) + step;
            } else if parsed.is_err() {
                target = body_end;
            } else {
                break parsed;
            }
            continue;
        }
        let mut whole = false;
        let mut begin = None;
        match p.ask {
            Ask::Probe(c, want) => {
                if probe.as_ref().is_none_or(|(pc, _)| *pc != c) {
                    probe = Some((c, Stream::at(table, c, clen)?));
                }
                if let Some((_, ps)) = probe.as_mut() {
                    match ps.grow(image, data, want, table, check) {
                        Ok(true) => {}
                        Ok(false) => return None,
                        Err(()) => return corrupt(check),
                    }
                    // A probe the block cannot satisfy (it asked past the
                    // block's end) is the whole block's to decide, never
                    // asked again (review of ADR-0040: it looped).
                    whole = ps.inflate.done() < want;
                }
                if !whole {
                    continue;
                }
            }
            Ask::Main(c) => begin = Some(c),
            Ask::Whole => whole = true,
            Ask::Nothing if !p.missed => {}
            Ask::Nothing if p.below => whole = true,
            Ask::Nothing if main.is_none() => {
                // A parse other than a seek's, over a fresh lend: the
                // decode begins at the chunk that holds the lowest byte
                // it asked for. A parse of the buffer's stale bytes fails
                // and asks for the trailer first; that names no chunk,
                // and the decode begins at the block's start (review of
                // ADR-0040: it began at the last chunk, then whole).
                let mut c = 0usize;
                if p.low < provisional_end::<BLOCK>() {
                    while c < n && point_at(table, c).1 <= p.low {
                        c += 1;
                    }
                } else {
                    // Nor is the trailer a target: the decode runs a step
                    // from the chunk's head, and the parse, over real
                    // bytes, asks again.
                    p.want = 0;
                }
                #[allow(clippy::cast_possible_truncation)] // at most MAX_POINTS
                let c = c as u8;
                begin = Some(c);
            }
            Ask::Nothing => {
                target = p.want.max(dp + 1) + step;
                continue;
            }
        }
        if let Some(c) = begin {
            main = match probe.take() {
                Some((pc, ps)) if pc == c => Some(ps),
                _ if c == 0 => Some(Stream::fresh(clen)),
                _ => Some(Stream::at(table, c, clen)?),
            };
            lo = entry_floor(table, c);
            p.lo = lo;
            p.main = c;
            p.walked = entry_head(table, c);
            target = p.walked.max(p.want) + step;
            continue;
        }
        if whole || (parsed.is_err() && (lo > 0 || main.is_none())) {
            // Not decided over the chunk: the whole block, from its start.
            main = Some(Stream::fresh(clen));
            probe = None;
            lo = 0;
            p.lo = 0;
            p.main = 0;
            p.sl = 1;
            p.sh = 1;
            p.walked = 0;
            target = body_end;
            continue;
        }
        if parsed.is_err() {
            target = body_end;
            continue;
        }
        break parsed;
    };
    let Some(mut kept) = main else {
        // The parse read nothing: nothing to keep.
        return Some((parsed, 0, Resume::NONE));
    };
    // The buffer keeps whole grains: finish the last one begun, or, when
    // the decode cannot be kept, the block.
    let dp = kept.inflate.done();
    let next = lends.saturating_add(1);
    match kept.grow(
        image,
        data,
        dp.next_multiple_of(grain::<BLOCK>()),
        table,
        check,
    ) {
        Ok(true) => {}
        Ok(false) => return None,
        Err(()) => return corrupt(check),
    }
    let dp = kept.inflate.done();
    if (dp < body_end || lo > 0)
        && let Some(resume) = Resume::of(&kept.inflate, clen, next, lo)
    {
        return Some((parsed, grains_in::<BLOCK>(lo, dp), resume));
    }
    // The decode cannot be kept: the buffer is marked whole only once it
    // is (review of ADR-0039: a block over 64 KiB was marked whole with
    // its end not inflated).
    if lo > 0 {
        return None;
    }
    match kept.grow(image, data, body_end, table, check) {
        Ok(true) if n == 0 || crate::points::points_are_restarts(&data[..body_end], table) => {
            Some((parsed, full::<BLOCK>(), Resume::NONE))
        }
        Ok(true) | Err(()) => corrupt(check),
        Ok(false) => None,
    }
}

/// The token offset of chunk `c`'s entry point: where a decode begun
/// there begins (0 for chunk 0).
fn entry_floor(table: &[u8], c: u8) -> usize {
    usize::from(c)
        .checked_sub(1)
        .map_or(0, |i| point_at(table, i).1)
}

/// The first chunk whose entry point's token is `lo` (chunk 0 for 0):
/// where a kept decode began. `None`: no such chunk.
fn chunk_at(table: &[u8], lo: u16) -> Option<u8> {
    if lo == 0 {
        return Some(0);
    }
    let i = (0..table.len() / POINT_LEN).find(|&i| point_at(table, i).1 == usize::from(lo))?;
    u8::try_from(i + 1).ok()
}

/// The head of chunk `c`: its first entry (0 for chunk 0).
fn entry_head(table: &[u8], c: u8) -> usize {
    usize::from(c)
        .checked_sub(1)
        .map_or(0, |i| point_at(table, i).2)
}

/// Whether a buffer can keep a partial inflate of a `BLOCK`-byte block:
/// [`Resume`] holds its positions in 16 bits. A larger block is inflated
/// whole, as before ADR-0039.
const fn keeps_decode<const BLOCK: usize>() -> bool {
    BLOCK - sstable::CRC_LEN <= u16::MAX as usize
}

/// A partial inflate over a lent image whose stream is checked only as
/// far as the decoder reads it (ADR-0039), from the block's start or an
/// entry point (ADR-0040).
struct Stream {
    inflate: crate::compress::Inflate,
    clen: usize,
    /// Stream bytes checked in this lend: `[from_check..checked)`, where
    /// `from_check` is where this lend's checks began.
    checked: usize,
    /// The next entry point the decode must reach at a token's start,
    /// as an index into the table.
    next: usize,
}

impl Stream {
    /// A decode from the stream's start.
    const fn fresh(clen: usize) -> Self {
        Self {
            inflate: Inflate::new(),
            clen,
            checked: 0,
            next: 0,
        }
    }

    /// A decode begun at the entry point of chunk `c` (`1..=n`).
    fn at(table: &[u8], c: u8, clen: usize) -> Option<Self> {
        let i = usize::from(c).checked_sub(1)?;
        if i >= table.len() / POINT_LEN {
            return None;
        }
        let (s, t, _) = point_at(table, i);
        Some(Self {
            inflate: Inflate::at(s, t),
            clen,
            checked: s,
            next: i,
        })
    }

    /// The decode a buffer kept, to go on with: its floor is the last
    /// entry point it has passed, each of which it checked.
    fn resumed(r: Resume, table: &[u8]) -> Self {
        let n = table.len() / POINT_LEN;
        let dp = usize::from(r.dp);
        let (mut next, mut floor) = (0usize, 0usize);
        while next < n && point_at(table, next).1 <= dp {
            floor = point_at(table, next).1;
            next += 1;
        }
        Self {
            inflate: Inflate::resumed_at(usize::from(r.sp), dp, floor),
            clen: usize::from(r.stream_len()),
            checked: usize::from(r.sp),
            next,
        }
    }

    /// Decodes until `until` bytes of the logical block are final,
    /// checking stream bytes a few grains at a time just before the
    /// decoder may read them, and each entry point of `table` as the
    /// decode reaches it: a token must start there (ADR-0040).
    /// `Ok(false)`: a check failed (the lend is a miss). `Err`: the
    /// stream does not decode, or a point does not hold.
    #[allow(clippy::inline_always)] // measured: not inlined, it cost every seek
    #[inline(always)]
    fn grow<const BLOCK: usize>(
        &mut self,
        image: &[u8; BLOCK],
        data: &mut [u8; BLOCK],
        until: usize,
        table: &[u8],
        check: &mut (dyn FnMut(usize, usize) -> bool + '_),
    ) -> Result<bool, ()> {
        let body_end = BLOCK - sstable::CRC_LEN;
        let until = until.min(body_end);
        let g = grain::<BLOCK>();
        let n = table.len() / POINT_LEN;
        loop {
            while self.next < n {
                let (s, t, _) = point_at(table, self.next);
                if self.inflate.done() < t {
                    break;
                }
                if self.inflate.done() != t || self.inflate.consumed() != s {
                    return Err(());
                }
                self.inflate.enter();
                self.next += 1;
            }
            let done = self.inflate.done();
            if done >= until {
                return Ok(true);
            }
            let stop = if self.next < n {
                until.min(point_at(table, self.next).1)
            } else {
                until
            };
            if self.checked < self.clen {
                let guess = ((until - done) / 2).max(2 * g);
                let to = (self.checked + guess).next_multiple_of(g).min(self.clen);
                if !check(self.checked, to) {
                    return Ok(false);
                }
                self.checked = to;
            }
            let whole = self.checked >= self.clen;
            let prefix = image.get(..self.checked).ok_or(())?;
            let out = data.get_mut(..body_end).ok_or(())?;
            self.inflate
                .run_prefix(prefix, whole, out, stop)
                .map_err(|_| ())?;
            if whole && self.inflate.done() < stop {
                return Err(());
            }
        }
    }
}

/// The grains wholly inside bytes `lo..dp` (`dp` at the block's end
/// counts as `BLOCK`: the trailer's grain is in).
const fn grains_in<const BLOCK: usize>(lo: usize, dp: usize) -> u64 {
    let first = lo.div_ceil(grain::<BLOCK>());
    let below = if dp >= BLOCK - sstable::CRC_LEN {
        full::<BLOCK>()
    } else {
        grains_below::<BLOCK>(dp)
    };
    if first >= 64 {
        0
    } else {
        below & !((1u64 << first) - 1)
    }
}

/// The grains wholly below byte `dp`.
const fn grains_below<const BLOCK: usize>(dp: usize) -> u64 {
    let n = dp / grain::<BLOCK>();
    if n >= grains::<BLOCK>() {
        full::<BLOCK>()
    } else {
        (1u64 << n) - 1
    }
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
                            .value_into(db.device, db.cache, at, Take::Prefix, val_buf)
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
            .load_with(
                db.device,
                db.cache,
                tref.id,
                bid,
                Take::Prefix,
                &mut |b, g| seek_block::<D::Error, BLOCK, KEY_MAX>(b, start, max_seq, bid, g),
            )
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
                        .load_with(db.device, db.cache, tid, bid, Take::Prefix, &mut park)
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
                .load_with(db.device, db.cache, tid, nid, Take::Whole, &mut |b, g| {
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
                            .value_into(db.device, db.cache, at, Take::Ranged, val_buf)
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
                        .load_with(
                            db.device,
                            db.cache,
                            tid,
                            bid,
                            Take::ranged_unless(whole),
                            &mut search,
                        )
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
                .load_with(
                    db.device,
                    db.cache,
                    table_id,
                    pbid,
                    Take::Whole,
                    &mut |b, g| {
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
                    },
                )
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
                    lo: 0,
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
                    lo: 0,
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
                    lo: 0,
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
                    lo: 0,
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
                    lo: 0,
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
            lo: 0,
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
                lo: 0,
                valid: mask::<B>(g, 2 * g),
                missed: false,
            });
            assert!(gate.check(g, 2 * g) && gate.check(5, 5) && !gate.missed());
            assert!(!gate.check(g - 1, g + 1) && gate.missed());
            assert!(
                !Gated::<B> {
                    lo: 0,
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

    mod prefix {
        //! A compressed block inflated only as far as a seek reads
        //! (ADR-0039) answers as the whole block does.
        use super::super::{BlockBufs, Blocks, Held, Lend, park_in, seek_block};
        use super::{Entry, Gate, R, Rng, block, entries, key_of};
        use crate::cache::{CachePort, CacheStats, CheckedRead};
        use crate::compress::CompressScratch;
        use crate::device::BlockDevice;
        use crate::sstable::CRC_LEN;
        use core::cell::Cell;
        use core::future::Future;
        use core::pin::pin;
        use core::task::{Context, Poll, Waker};
        extern crate std;
        use std::boxed::Box;

        /// Lends one image. Chunks of 1/64 block are "summed": a check
        /// fails when it covers `bad` while `flips` lends remain flipped.
        struct One<const B: usize> {
            image: Box<[u8; B]>,
            bad: usize,
            flips: Cell<usize>,
            lends: Cell<usize>,
            summed: Cell<usize>,
        }

        impl<const B: usize> CachePort<B> for One<B> {
            fn get_into(&self, _: u32, _: u64, _: &mut [u8; B]) -> bool {
                false
            }
            fn get_range_into(&self, _: u32, _: u64, _: usize, _: &mut [u8]) -> bool {
                false
            }
            fn with_checked(&self, _: u32, _: u64, read: &mut CheckedRead<'_>) -> Option<bool> {
                self.lends.set(self.lends.get() + 1);
                let flipped = self.flips.get() > 0;
                if flipped {
                    self.flips.set(self.flips.get() - 1);
                }
                let mut image = *self.image;
                let g = super::grain::<B>();
                if flipped {
                    image[self.bad] ^= 0x10;
                }
                let mut ok = true;
                read(&image, &mut |from, to| {
                    if to <= from {
                        return ok;
                    }
                    if to > B
                        || (flipped && from / g <= self.bad / g && self.bad < to.div_ceil(g) * g)
                    {
                        ok = false;
                    }
                    self.summed
                        .set(self.summed.get() + to.div_ceil(g) - from / g);
                    ok
                });
                Some(ok)
            }
            fn checks_ranges(&self) -> bool {
                true
            }
            fn put(&self, _: u32, _: u64, _: &[u8; B], _: bool) {}
            fn invalidate_table(&self, _: u32) {}
            fn stats(&self) -> CacheStats {
                CacheStats::default()
            }
        }

        /// A device every read of which fails: a test that reaches it
        /// was a miss.
        struct NoDevice;
        impl BlockDevice for NoDevice {
            type Error = ();
            const BLOCK: usize = 0;
            fn poll_read_block(
                &self,
                _: &mut Context<'_>,
                _: u64,
                _: &mut [u8],
            ) -> Poll<Result<(), ()>> {
                Poll::Ready(Err(()))
            }
            fn poll_write_block(
                &mut self,
                _: &mut Context<'_>,
                _: u64,
                _: &[u8],
            ) -> Poll<Result<(), ()>> {
                Poll::Ready(Err(()))
            }
            fn poll_flush(&mut self, _: &mut Context<'_>) -> Poll<Result<(), ()>> {
                Poll::Ready(Ok(()))
            }
        }

        fn block_on<F: Future>(f: F) -> F::Output {
            let mut f = pin!(f);
            let mut cx = Context::from_waker(Waker::noop());
            loop {
                if let Poll::Ready(v) = f.as_mut().poll(&mut cx) {
                    return v;
                }
            }
        }

        std::thread_local! {
            /// Whether [`compressed`] writes entry points (ADR-0040).
            static CHUNKED: Cell<bool> = const { Cell::new(false) };
            /// Seeks whose decode began past the block's start.
            static ENTERED: Cell<usize> = const { Cell::new(0) };
        }

        /// Runs `f` with [`compressed`] writing entry points.
        fn chunked<T>(f: impl FnOnce() -> T) -> T {
            CHUNKED.with(|c| c.set(true));
            let r = f();
            CHUNKED.with(|c| c.set(false));
            r
        }

        fn compressed<const B: usize>(logical: &[u8; B]) -> Option<Box<[u8; B]>> {
            let body_end = B - CRC_LEN;
            let mut cs = Box::new(CompressScratch::<B>::new());
            // Entry points at restarts a ninth of the block apart, as
            // the writer chooses them (ADR-0040).
            let mut bounds = std::vec::Vec::new();
            if CHUNKED.with(Cell::get) && u16::try_from(body_end).is_ok() {
                let rc = usize::from(u16::from_le_bytes([
                    logical[body_end - 2],
                    logical[body_end - 1],
                ]));
                let rstart = body_end - 2 - 2 * rc;
                let mut last = 0;
                for i in 1..rc {
                    let r = usize::from(u16::from_le_bytes([
                        logical[rstart + 2 * i],
                        logical[rstart + 2 * i + 1],
                    ]));
                    if bounds.len() < crate::points::MAX_POINTS && r >= last + (B / 9).max(64) {
                        bounds.push(r);
                        last = r;
                    }
                }
            }
            let mut points = std::vec![(0usize, 0usize); bounds.len()];
            let reserve = if bounds.is_empty() {
                0
            } else {
                2 + 6 * bounds.len()
            };
            let clen = cs.compress_chunked(&logical[..body_end], &bounds, &mut points, reserve)?;
            let mut img = Box::new([0u8; B]);
            img[..clen].copy_from_slice(&cs.compressed()[..clen]);
            if !bounds.is_empty() {
                let mut at = body_end - 4 - 6 * bounds.len();
                for (&(sp, tp), &b) in points.iter().zip(&bounds) {
                    for v in [sp, tp, b] {
                        img[at..at + 2].copy_from_slice(&u16::try_from(v).unwrap().to_le_bytes());
                        at += 2;
                    }
                }
                img[at..at + 2]
                    .copy_from_slice(&u16::try_from(bounds.len()).unwrap().to_le_bytes());
            }
            let flag = 0x8000u16 | u16::try_from(clen).unwrap();
            img[body_end - 2..body_end].copy_from_slice(&flag.to_le_bytes());
            Some(img)
        }

        #[allow(clippy::too_many_lines)] // one model run: seek, value, parks
        fn holds<const B: usize>(seed: u64, flip_lends: usize) -> usize {
            let mut rng = Rng(seed);
            let every = 1 + rng.below(20);
            let es: std::vec::Vec<Entry> = entries::<B>(&mut rng, every);
            let logical: [u8; B] = block::<B>(&es, every);
            let Some(image) = compressed(&logical) else {
                return 0;
            };
            let body_end = B - CRC_LEN;
            let bad = rng.below(body_end);
            let mut starts: std::vec::Vec<std::vec::Vec<u8>> =
                es.iter().map(|e| e.0.clone()).collect();
            starts.push(std::vec::Vec::new());
            starts.push(b"zzzzzzzzzzzzzz".to_vec());
            for k in es.iter().take(8) {
                let mut k2 = k.0.clone();
                k2.push(b'0');
                starts.push(k2);
            }
            let mut partial = 0;
            for start in &starts {
                for max_seq in [u64::MAX, 60, 20, 2] {
                    let cache = One {
                        image: image.clone(),
                        bad,
                        flips: Cell::new(flip_lends),
                        lends: Cell::new(0),
                        summed: Cell::new(0),
                    };
                    let want: R<(usize, Option<super::Head<64>>)> =
                        seek_block::<(), B, 64>(&logical, start, max_seq, 7, &mut Gate::Open);
                    let (want_end, want_head) = want.unwrap();
                    let mut blocks = Blocks::<B>::over(Held::Own(BlockBufs::new()));
                    let got =
                        blocks.lend_into(&cache, 1, 7, super::super::Take::Prefix, &mut |b, g| {
                            seek_block::<(), B, 64>(b, start, max_seq, 7, g)
                        });
                    let (end, head) = match got {
                        Lend::Done(r) => r.unwrap(),
                        Lend::Miss => {
                            assert!(flip_lends > 0, "a clean lend missed");
                            continue;
                        }
                        Lend::Unlent => unreachable!(),
                    };
                    grains_hold(blocks.current(), &logical);
                    if blocks.current().resume.lo > 0 {
                        ENTERED.with(|e| e.set(e.get() + 1));
                    }
                    assert_eq!(
                        head.as_ref()
                            .map(|h| (key_of(h).to_vec(), h.seq, h.off, h.next)),
                        want_head
                            .as_ref()
                            .map(|h| (key_of(h).to_vec(), h.seq, h.off, h.next)),
                        "seed {seed} start {start:?} seq {max_seq}"
                    );
                    // A walk from the first entry does not know the
                    // entries end: it parks against the provisional one,
                    // even where its decode ran to the block's end.
                    if blocks.current().valid == super::full::<B>() {
                        assert!(end == want_end || end == super::super::provisional_end::<B>());
                    } else {
                        partial += 1;
                        assert_eq!(end, super::super::provisional_end::<B>());
                    }
                    // The head's value, from a lend of its own (a cursor
                    // whose buffer went to another table).
                    if let Some(h) = head.as_ref().filter(|h| !h.tombstone) {
                        let cache2 = One {
                            image: image.clone(),
                            bad,
                            flips: Cell::new(flip_lends),
                            lends: Cell::new(0),
                            summed: Cell::new(0),
                        };
                        let mut other = Blocks::<B>::over(Held::Own(BlockBufs::new()));
                        let mut out = [0u8; B];
                        let mut want_out = [0u8; B];
                        let wl = super::value_at::<(), B>(
                            &logical,
                            h.off,
                            want_end,
                            7,
                            &mut Gate::Open,
                            &mut want_out,
                        )
                        .unwrap();
                        match other.lend_into(
                            &cache2,
                            1,
                            7,
                            super::super::Take::Prefix,
                            &mut |b, g| super::value_at::<(), B>(b, h.off, end, 7, g, &mut out),
                        ) {
                            Lend::Done(r) => {
                                let n = r.unwrap();
                                assert_eq!(out[..n], want_out[..wl]);
                            }
                            Lend::Miss => assert!(flip_lends > 0),
                            Lend::Unlent => unreachable!(),
                        }
                    }
                    // The next 17 parks: from the buffer, or after a
                    // resumed inflate in one more lend.
                    let (mut off, mut woff) = match &head {
                        Some(h) => (h.next, h.next),
                        None => continue,
                    };
                    for step in 0..17 {
                        let want: Option<super::Head<64>> = park_in::<(), B, 64>(
                            &logical,
                            woff,
                            want_end,
                            None,
                            max_seq,
                            7,
                            &mut Gate::Open,
                        )
                        .unwrap();
                        let mut park = |b: &[u8; B], g: &mut Gate<'_, '_, B>| {
                            park_in::<(), B, 64>(b, off, end, None, max_seq, 7, g)
                        };
                        let in_buffer = blocks.held(7, &mut park);
                        // The seek took the entry after its head, in
                        // whole grains: the first step needs no lend.
                        assert!(
                            in_buffer.is_some()
                                || step > 0
                                || max_seq != u64::MAX
                                || flip_lends > 0,
                            "seed {seed}: the step past the head lent the block again: resume {:?} valid {:x} off {off} start {start:?} want {:?}",
                            blocks.current().resume,
                            blocks.current().valid,
                            want.as_ref().map(|h| (h.off, h.next))
                        );
                        let got = if let Some(r) = in_buffer {
                            r.unwrap()
                        } else if seed.is_multiple_of(2) && blocks.current().resume.clen != 0 {
                            // Going on with the inflate only as far as the
                            // park reads (`load_with`'s way).
                            assert_eq!(blocks.current().id, Some(7));
                            match blocks.extend(&cache, 1, 7, &mut park) {
                                Lend::Done(r) => {
                                    grains_hold(blocks.current(), &logical);
                                    r.unwrap()
                                }
                                Lend::Miss => {
                                    assert!(flip_lends > 0, "a clean extension missed");
                                    break;
                                }
                                Lend::Unlent => unreachable!(),
                            }
                        } else {
                            assert_eq!(blocks.current().id, Some(7));
                            let done = block_on(blocks.complete(&NoDevice, Some(&cache), 1, 7));
                            if done.is_err() {
                                assert!(flip_lends > 0, "a clean completion missed");
                                break;
                            }
                            assert_eq!(blocks.current().valid, super::full::<B>());
                            assert_eq!(blocks.current().data[..body_end], logical[..body_end]);
                            blocks.held(7, &mut park).unwrap().unwrap()
                        };
                        assert_eq!(
                            got.as_ref()
                                .map(|h| (key_of(h).to_vec(), h.seq, h.off, h.next)),
                            want.as_ref()
                                .map(|h| (key_of(h).to_vec(), h.seq, h.off, h.next)),
                        );
                        match got {
                            Some(h) => {
                                off = h.next;
                                woff = h.next;
                            }
                            None => break,
                        }
                    }
                }
            }
            partial
        }

        /// Every grain `block` holds is the logical block's, and a kept
        /// decode names exactly those grains.
        fn grains_hold<const B: usize>(block: &super::super::ScanBlock<B>, logical: &[u8; B]) {
            let g = super::grain::<B>();
            let body_end = B - CRC_LEN;
            for i in 0..super::grains::<B>() {
                if block.valid & (1u64 << i) != 0 {
                    let (lo, hi) = (i * g, ((i + 1) * g).min(body_end));
                    assert_eq!(block.data[lo..hi], logical[lo..hi], "grain {i}");
                }
            }
            if block.resume.clen != 0 {
                // The decode began at its entry chunk's token (ADR-0040).
                let lo = usize::from(block.resume.lo);
                let dp = usize::from(block.resume.dp);
                assert_eq!(block.valid, super::super::grains_in::<B>(lo, dp));
                assert!(dp < body_end || lo > 0, "a decode kept at the block's end");
                assert_eq!(block.data[lo..dp], logical[lo..dp], "the decode's window");
            } else if block.id.is_some() {
                assert_eq!(
                    block.valid,
                    super::full::<B>(),
                    "a partial buffer keeps its decode"
                );
            }
            assert!(block.resume == super::super::Resume::NONE || block.resume.clen != 0);
        }

        #[test]
        fn a_partial_inflate_seeks_and_parks_as_the_whole_block() {
            let mut partial = 0;
            for seed in 1..200 {
                partial += holds::<4096>(seed, 0);
                partial += holds::<16384>(seed, 0);
                partial += holds::<1000>(seed, 0);
            }
            assert!(partial > 1000, "{partial} partial seeks");
        }

        /// The same over blocks with entry points (ADR-0040): a seek
        /// searches the chunk heads and decodes from one, and its parks
        /// go on through the chunks after it.
        #[test]
        fn a_chunked_inflate_seeks_and_parks_as_the_whole_block() {
            // Block sizes that carry entry points (ADR-0040).
            let partial = chunked(|| {
                let mut partial = 0;
                for seed in 1..200 {
                    partial += holds::<8192>(seed, 0);
                    partial += holds::<16384>(seed, 0);
                    partial += holds::<32768>(seed, 0);
                }
                partial
            });
            assert!(partial > 1000, "{partial} partial seeks");
            let entered = ENTERED.with(Cell::get);
            std::println!("{partial} partial seeks, {entered} begun at an entry point");
            // The entries a test block holds fill about 7 KB, so the
            // larger blocks have few cuts among them.
            assert!(
                entered > partial / 3,
                "{entered} of {partial} seeks began at an entry point"
            );
        }

        /// A flip on the first, second or ninth lend over blocks with
        /// entry points is a miss, never a wrong answer.
        #[test]
        fn a_chunked_inflate_misses_on_a_flip() {
            chunked(|| {
                for seed in 1..80 {
                    for flips in [1, 2, 9] {
                        holds::<8192>(seed, flips);
                        holds::<16384>(seed, flips);
                    }
                }
            });
        }

        /// The largest blocks that keep a decode: their positions fill
        /// [`Resume`]'s 16 bits and their grains the 64-bit map (review
        /// of ADR-0039). One past them inflates whole.
        #[test]
        fn a_partial_inflate_holds_at_the_16_bit_limit() {
            let mut partial = 0;
            for seed in 1..40 {
                partial += holds::<65_536>(seed, 0);
                partial += holds::<65_539>(seed, 0);
                assert_eq!(
                    holds::<65_540>(seed, 0),
                    0,
                    "a block over the limit kept a decode"
                );
            }
            for seed in 1..10 {
                holds::<65_536>(seed, 2);
            }
            assert!(partial > 100, "{partial} partial seeks");
        }

        /// A compressible block of `B` bytes with no long match.
        #[allow(clippy::unnecessary_box_returns)] // over 64 KiB: kept off the stack
        fn big_logical<const B: usize>() -> Box<[u8; B]> {
            let mut logical: Box<[u8; B]> =
                std::vec![0u8; B].into_boxed_slice().try_into().unwrap();
            let mut x = 0x1234_5678u32;
            for (i, b) in logical.iter_mut().enumerate().take(B - CRC_LEN) {
                *b = if i % 64 == 0 {
                    x = x.wrapping_mul(1_103_515_245).wrapping_add(12345);
                    (x >> 16).to_le_bytes()[0] | 1
                } else {
                    b'a'
                };
            }
            logical
        }

        /// A block over 64 KiB, whose decode a buffer cannot keep, is
        /// inflated whole before the buffer is marked whole: through a
        /// lend, and through [`lend_prefix`] itself. Found in review of
        /// ADR-0039: the grain-rounding step took the decode past 16
        /// bits and the buffer was marked whole with its end never
        /// inflated, so a later parse read stale bytes as checked ones.
        #[test]
        fn a_block_over_64_kib_is_marked_whole_only_once_inflated() {
            const B: usize = 131_072;
            let logical = big_logical::<B>();
            let image = compressed::<B>(&logical).expect("compresses");
            let cache = One {
                image: image.clone(),
                bad: 0,
                flips: Cell::new(0),
                lends: Cell::new(0),
                summed: Cell::new(0),
            };
            let mut bufs = Box::new(BlockBufs::<B>::new());
            bufs.own.data.fill(0xA5);
            let mut blocks = Blocks::<B>::over(Held::Lent(&mut bufs));
            let mut early = |b: &[u8; B], g: &mut Gate<'_, '_, B>| {
                if g.check(0, 62_600) {
                    Ok::<u8, crate::Error<()>>(b[62_599])
                } else {
                    Err(crate::Error::CorruptBlock { id: 7 })
                }
            };
            match blocks.lend_into(&cache, 1, 7, super::super::Take::Prefix, &mut early) {
                Lend::Done(r) => assert_eq!(r.unwrap(), logical[62_599]),
                _ => panic!("a clean lend missed"),
            }
            assert_eq!(blocks.current().valid, super::full::<B>());
            assert_eq!(blocks.current().data[..B - CRC_LEN], logical[..B - CRC_LEN]);
            // The prefix path itself, should it be handed such a block.
            let mut data: Box<[u8; B]> =
                std::vec![0xA5u8; B].into_boxed_slice().try_into().unwrap();
            let flag = u16::from_le_bytes([image[B - CRC_LEN - 2], image[B - CRC_LEN - 1]]);
            let kept = super::super::lend_prefix(
                &image,
                &mut data,
                None,
                usize::from(flag & 0x7FFF),
                0,
                &[],
                7,
                &mut |_, _| true,
                &mut early,
            );
            let (answer, valid, resume) = kept.expect("the checks held");
            assert_eq!(answer.unwrap(), logical[62_599]);
            assert_eq!(
                (valid, resume),
                (super::full::<B>(), super::super::Resume::NONE)
            );
            assert_eq!(data[..B - CRC_LEN], logical[..B - CRC_LEN]);
        }

        /// Every entry a scan from the block's first yields, through the
        /// whole block or a partial inflate of `image`.
        fn keys_scanned<const B: usize>(
            b: &[u8; B],
            image: Option<&[u8; B]>,
        ) -> std::vec::Vec<std::vec::Vec<u8>> {
            let mut out = std::vec::Vec::new();
            let cache = One {
                image: Box::new(*image.unwrap_or(b)),
                bad: 0,
                flips: Cell::new(0),
                lends: Cell::new(0),
                summed: Cell::new(0),
            };
            let mut blocks = Blocks::<B>::over(Held::Own(BlockBufs::new()));
            let (end, head) = if image.is_some() {
                let take = super::super::Take::Prefix;
                let Lend::Done(r) = blocks.lend_into(&cache, 1, 7, take, &mut |bb, g| {
                    seek_block::<(), B, 64>(bb, b"", u64::MAX, 7, g)
                }) else {
                    panic!("a clean lend missed");
                };
                r.unwrap()
            } else {
                seek_block::<(), B, 64>(b, b"", u64::MAX, 7, &mut Gate::Open).unwrap()
            };
            let Some(mut h) = head else { return out };
            loop {
                out.push(key_of(&h).to_vec());
                let off = h.next;
                let mut park = |bb: &[u8; B], g: &mut Gate<'_, '_, B>| {
                    park_in::<(), B, 64>(bb, off, end, None, u64::MAX, 7, g)
                };
                let next = if image.is_some() {
                    blocks.held(7, &mut park).unwrap_or_else(|| {
                        assert_eq!(blocks.inflate_rest(&cache, 1, 7), Some(true));
                        blocks.held(7, &mut park).unwrap()
                    })
                } else {
                    park(b, &mut Gate::Open)
                };
                match next.unwrap() {
                    Some(n) => h = n,
                    None => return out,
                }
            }
        }

        /// A CRC-valid block the writer never makes, whose restart array
        /// does not begin with 0 and whose trailer reads as an entry: a
        /// cursor a partial inflate positioned parses it against the real
        /// entries end once the block is in, as the whole block does, and
        /// finds no entry in the trailer. Found in review of ADR-0039: it
        /// parsed against the provisional end and yielded a phantom.
        #[test]
        fn a_trailer_is_never_parsed_as_an_entry() {
            const B: usize = 4096;
            let body_end = B - CRC_LEN;
            let mut b = Box::new([0u8; B]);
            // Eight restarts and the count: the entries end 18 bytes
            // before the body does.
            let x = body_end - 18;
            let (mut off, mut i) = (0usize, 0u32);
            while off < x {
                let k = std::format!("a{i:04}").into_bytes();
                let left = x - off;
                let vl = if left < 120 {
                    left - 13 - k.len()
                } else {
                    60 - 13 - k.len()
                };
                b[off..off + 2].copy_from_slice(&u16::try_from(k.len()).unwrap().to_le_bytes());
                b[off + 2..off + 4].copy_from_slice(&u16::try_from(vl).unwrap().to_le_bytes());
                b[off + 4..off + 12].copy_from_slice(&5u64.to_le_bytes());
                b[off + 12] = 1;
                b[off + 13..off + 13 + k.len()].copy_from_slice(&k);
                b[off + 13 + k.len()..off + 13 + k.len() + vl].fill(b'v');
                off += 13 + k.len() + vl;
                i += 1;
            }
            // Restarts that read as an entry with key "z", then count 8.
            let mut t = [0u8; 18];
            t[0..2].copy_from_slice(&1u16.to_le_bytes());
            t[4..12].copy_from_slice(&1u64.to_le_bytes());
            t[12] = 1;
            t[13] = b'z';
            t[16..18].copy_from_slice(&8u16.to_le_bytes());
            b[x..body_end].copy_from_slice(&t);
            let image = compressed::<B>(&b).expect("compresses");
            let whole = keys_scanned::<B>(&b, None);
            assert!(whole.len() > 10 && whole.iter().all(|k| k[0] == b'a'));
            assert_eq!(keys_scanned::<B>(&b, Some(&image)), whole);
        }

        /// A clean lend of `image`, compressed from `logical`, through a
        /// [`One`] whose checks are counted.
        fn one<const B: usize>(image: &[u8; B]) -> One<B> {
            One {
                image: Box::new(*image),
                bad: 0,
                flips: Cell::new(0),
                lends: Cell::new(0),
                summed: Cell::new(0),
            }
        }

        /// A parse lets through only bytes the decode has made final:
        /// for every length `n`, a parse that checks `0..n` and reads byte
        /// `n - 1` reads the logical block's. A gate that let one byte
        /// too many through would hand it a byte not yet written (found
        /// by mutation in review of ADR-0039: `to <= upto + 1` passed
        /// every other test).
        #[test]
        fn a_prefix_gate_lets_through_only_final_bytes() {
            const B: usize = 4096;
            let logical = big_logical::<B>();
            let image = compressed::<B>(&logical).expect("compresses");
            for n in 1..B - CRC_LEN {
                let cache = one(&image);
                let mut blocks = Blocks::<B>::over(Held::Own(BlockBufs::new()));
                let got =
                    blocks.lend_into(&cache, 1, 7, super::super::Take::Prefix, &mut |b, g| {
                        if g.check(0, n) {
                            Ok::<u8, crate::Error<()>>(b[n - 1])
                        } else {
                            Err(crate::Error::CorruptBlock { id: 7 })
                        }
                    });
                let Lend::Done(r) = got else {
                    panic!("a clean lend missed");
                };
                assert_eq!(r.unwrap(), logical[n - 1], "byte {}", n - 1);
            }
        }

        /// A seek to a block's first key checks and inflates only the
        /// start of its stream: on 4 KiB blocks well under 60% of it, in
        /// the median. (Review of ADR-0039: a step of 3,000 bytes, most
        /// of the saving gone, passed every other test.)
        #[test]
        fn a_seek_to_the_first_key_checks_a_short_prefix() {
            const B: usize = 4096;
            let g = super::grain::<B>();
            let mut shares = std::vec::Vec::new();
            for seed in 1..200 {
                let mut rng = Rng(seed);
                let every = 1 + rng.below(20);
                let es: std::vec::Vec<Entry> = entries::<B>(&mut rng, every);
                let logical: [u8; B] = block::<B>(&es, every);
                let Some(image) = compressed(&logical) else {
                    continue;
                };
                let flag = u16::from_le_bytes([image[B - CRC_LEN - 2], image[B - CRC_LEN - 1]]);
                let clen = usize::from(flag & 0x7FFF);
                if clen < 8 * g {
                    continue;
                }
                let cache = one(&image);
                let mut blocks = Blocks::<B>::over(Held::Own(BlockBufs::new()));
                let first = es[0].0.clone();
                let got =
                    blocks.lend_into(&cache, 1, 7, super::super::Take::Prefix, &mut |b, gt| {
                        seek_block::<(), B, 64>(b, &first, u64::MAX, 7, gt)
                    });
                assert!(matches!(got, Lend::Done(Ok(_))));
                // Grains summed below the flag's, as a share of the stream's.
                let flag_grains = 1;
                shares.push((cache.summed.get() - flag_grains) * 100 / clen.div_ceil(g));
            }
            shares.sort_unstable();
            assert!(shares.len() > 50, "{} blocks", shares.len());
            let median = shares[shares.len() / 2];
            assert!(
                median < 60,
                "a first-key seek checked {median}% of the stream"
            );
        }

        /// A seek's parse is run again over a longer prefix from where
        /// its walk stopped, not from the first entry, and a few times
        /// a block at most. (Review of ADR-0039: dropping the walk's
        /// bookkeeping, or a step of 0 or 64 bytes, passed every other
        /// test: the answers were right, the walk quadratic.)
        #[test]
        fn a_seek_reruns_its_walk_from_where_it_stopped() {
            const B: usize = 16_384;
            let body_end = B - CRC_LEN;
            for seed in 1..60 {
                let mut rng = Rng(seed);
                let every = 1 + rng.below(20);
                let es: std::vec::Vec<Entry> = entries::<B>(&mut rng, every);
                let logical: [u8; B] = block::<B>(&es, every);
                let Some(image) = compressed(&logical) else {
                    continue;
                };
                let cache = one(&image);
                let last = es[es.len() - 1].0.clone();
                let mut froms = std::vec::Vec::new();
                let mut blocks = Blocks::<B>::over(Held::Own(BlockBufs::new()));
                let got =
                    blocks.lend_into(&cache, 1, 7, super::super::Take::Prefix, &mut |b, g| {
                        froms.push(g.linear_from());
                        seek_block::<(), B, 64>(b, &last, u64::MAX, 7, g)
                    });
                assert!(matches!(got, Lend::Done(Ok(_))));
                let walked: std::vec::Vec<usize> = froms.iter().flatten().copied().collect();
                assert!(walked.windows(2).all(|w| w[0] <= w[1]), "{walked:?}");
                assert!(
                    froms.len() <= 2 + body_end / super::super::PREFIX_STEP,
                    "{} runs",
                    froms.len()
                );
                if walked.len() > 2 {
                    assert!(
                        walked[walked.len() - 1] > 0,
                        "every run walked from the start"
                    );
                }
            }
        }

        /// The lend that goes on with a kept decode checks the stream
        /// from where the decode stood, not from its start (review of
        /// ADR-0039: checking from 0 passed every other test).
        #[test]
        fn going_on_with_a_decode_checks_only_the_stream_ahead() {
            const B: usize = 16_384;
            let g = super::grain::<B>();
            let mut seen = 0;
            for seed in 1..80 {
                let mut rng = Rng(seed);
                let every = 1 + rng.below(20);
                let es: std::vec::Vec<Entry> = entries::<B>(&mut rng, every);
                let logical: [u8; B] = block::<B>(&es, every);
                let Some(image) = compressed(&logical) else {
                    continue;
                };
                let flag = u16::from_le_bytes([image[B - CRC_LEN - 2], image[B - CRC_LEN - 1]]);
                let clen = usize::from(flag & 0x7FFF);
                let cache = one(&image);
                let mid = es[es.len() / 2].0.clone();
                let mut blocks = Blocks::<B>::over(Held::Own(BlockBufs::new()));
                let got =
                    blocks.lend_into(&cache, 1, 7, super::super::Take::Prefix, &mut |b, gt| {
                        seek_block::<(), B, 64>(b, &mid, u64::MAX, 7, gt)
                    });
                assert!(matches!(got, Lend::Done(Ok(_))));
                let sp = usize::from(blocks.current().resume.sp);
                if blocks.current().resume.clen == 0 || sp < 4 * g {
                    continue;
                }
                let before = cache.summed.get();
                assert_eq!(blocks.inflate_rest(&cache, 1, 7), Some(true));
                let summed = cache.summed.get() - before;
                // The stream from `sp`'s grain, and the flag's.
                assert!(
                    summed <= (clen - sp / g * g).div_ceil(g) + 2,
                    "{summed} grains"
                );
                assert_eq!(blocks.current().data[..B - CRC_LEN], logical[..B - CRC_LEN]);
                seen += 1;
            }
            assert!(seen > 10, "{seen} blocks");
        }

        /// A flag naming a stream as long as the body is corrupt through
        /// a partial inflate as through a whole one (review of ADR-0039:
        /// without the length check the partial path decoded it).
        #[test]
        fn a_stream_as_long_as_the_body_is_corrupt() {
            const B: usize = 4096;
            let body_end = B - CRC_LEN;
            // A block of entries, so that a decode of its real stream
            // answers a seek.
            let mut rng = Rng(7);
            let es: std::vec::Vec<Entry> = entries::<B>(&mut rng, 4);
            let logical: [u8; B] = block::<B>(&es, 4);
            let mut image = *compressed::<B>(&logical).expect("compresses");
            let flag = 0x8000u16 | u16::try_from(body_end).unwrap();
            image[body_end - 2..body_end].copy_from_slice(&flag.to_le_bytes());
            for take in [super::super::Take::Ranged, super::super::Take::Prefix] {
                let cache = one(&image);
                let mut blocks = Blocks::<B>::over(Held::Own(BlockBufs::new()));
                let got = blocks.lend_into(&cache, 1, 7, take, &mut |b, g| {
                    seek_block::<(), B, 64>(b, b"", u64::MAX, 7, g)
                });
                assert!(
                    matches!(got, Lend::Done(Err(crate::Error::CorruptBlock { id: 7 }))),
                    "{take:?}"
                );
            }
        }

        /// A buffer holding part of a raw block is never completed from a
        /// compressed image under the same tag: the lend refuses it, and
        /// the block is read from the device (a backstop; tags are not
        /// reused).
        #[test]
        fn a_raw_buffer_is_not_completed_from_a_compressed_image() {
            const B: usize = 4096;
            let logical = big_logical::<B>();
            let mut raw = *logical;
            raw[B - CRC_LEN - 2..B - CRC_LEN].copy_from_slice(&1u16.to_le_bytes());
            let mut blocks = Blocks::<B>::over(Held::Own(BlockBufs::new()));
            let got =
                blocks.lend_into(&one(&raw), 1, 7, super::super::Take::Ranged, &mut |b, g| {
                    if g.check(0, 100) {
                        Ok::<u8, crate::Error<()>>(b[99])
                    } else {
                        Err(crate::Error::CorruptBlock { id: 7 })
                    }
                });
            assert!(matches!(got, Lend::Done(Ok(_))));
            assert_ne!(blocks.current().valid, super::full::<B>());
            let image = compressed::<B>(&logical).expect("compresses");
            assert_eq!(blocks.copy_rest(&one(&image), 1, 7), Some(false));
            assert!(block_on(blocks.complete(&NoDevice, Some(&one(&image)), 1, 7)).is_err());
            assert_eq!(blocks.current().id, None);
        }

        /// A chunked block of random entries at `B` bytes, and its image.
        fn chunked_block<const B: usize>(seed: u64) -> Option<([u8; B], Box<[u8; B]>)> {
            let mut rng = Rng(seed);
            let every = 1 + rng.below(4);
            let es: std::vec::Vec<Entry> = entries::<B>(&mut rng, every);
            let logical: [u8; B] = block::<B>(&es, every);
            let image = chunked(|| compressed::<B>(&logical))?;
            let n = usize::from(u16::from_le_bytes([
                image[B - CRC_LEN - 4],
                image[B - CRC_LEN - 3],
            ]));
            (n > 0).then_some((logical, image))
        }

        /// A table whose last head lies within an entry header of the
        /// trailer, under a valid CRC: a seek past every head is
        /// `CorruptBlock`, as the whole block is, and ends. (Review of
        /// ADR-0040: the seek asked for a probe of that head's header,
        /// which the block cannot hold, on every run of its parse.)
        #[test]
        fn a_seek_over_a_head_against_the_trailer_ends() {
            const B: usize = 16_384;
            let mut tried = 0;
            for seed in 1..60 {
                let Some((_, mut image)) = chunked_block::<B>(seed) else {
                    continue;
                };
                let count = B - CRC_LEN - 4;
                let n = usize::from(u16::from_le_bytes([image[count], image[count + 1]]));
                let last = count - 2;
                let near = u16::try_from(count - 10).unwrap();
                image[last..last + 2].copy_from_slice(&near.to_le_bytes());
                let cache = one(&image);
                let mut blocks = Blocks::<B>::over(Held::Own(BlockBufs::new()));
                let got =
                    blocks.lend_into(&cache, 1, 7, super::super::Take::Prefix, &mut |b, g| {
                        seek_block::<(), B, 64>(b, b"zzzzzzzzzzzz", u64::MAX, 7, g)
                    });
                assert!(
                    matches!(got, Lend::Done(Err(crate::Error::CorruptBlock { id: 7 }))),
                    "seed {seed}, {n} points"
                );
                tried += 1;
            }
            assert!(tried > 10, "{tried}");
        }

        /// A park over a fresh lend of a block with a table, from an
        /// entry near the block's start, decodes from the start and stops
        /// soon after it, not the whole block. (Review of ADR-0040: the
        /// parse of the buffer's stale bytes asked for the trailer first,
        /// and the decode began at the last chunk, then went whole.)
        #[test]
        fn a_park_over_a_fresh_lend_decodes_from_its_chunk() {
            const B: usize = 16_384;
            let mut partial = 0;
            for seed in 1..60 {
                let Some((logical, image)) = chunked_block::<B>(seed) else {
                    continue;
                };
                let end = super::super::entries_end::<(), B>(&logical, 7, &mut Gate::Open).unwrap();
                let first =
                    park_in::<(), B, 64>(&logical, 0, end, None, u64::MAX, 7, &mut Gate::Open)
                        .unwrap()
                        .unwrap();
                let want = park_in::<(), B, 64>(
                    &logical,
                    first.next,
                    end,
                    None,
                    u64::MAX,
                    7,
                    &mut Gate::Open,
                )
                .unwrap();
                let cache = one(&image);
                let mut bufs = Box::new(BlockBufs::<B>::new());
                bufs.own.data.fill(0x5A);
                let mut blocks = Blocks::<B>::over(Held::Lent(&mut bufs));
                // A cursor a partial inflate positioned parks against the
                // provisional end, as the seek left it.
                let pend = super::super::provisional_end::<B>();
                let got =
                    blocks.lend_into(&cache, 1, 7, super::super::Take::Prefix, &mut |b, g| {
                        park_in::<(), B, 64>(b, first.next, pend, None, u64::MAX, 7, g)
                    });
                let Lend::Done(Ok(got)) = got else {
                    panic!("seed {seed}: a clean lend missed");
                };
                assert_eq!(got.map(|h| (h.off, h.next)), want.map(|h| (h.off, h.next)));
                grains_hold(blocks.current(), &logical);
                if blocks.current().valid != super::full::<B>() {
                    partial += 1;
                    assert_eq!(blocks.current().resume.lo, 0, "seed {seed}");
                }
            }
            assert!(
                partial > 10,
                "{partial} parks stopped short of the whole block"
            );
        }

        #[test]
        fn a_flipped_stream_byte_is_a_miss_never_an_answer() {
            for seed in 1..200 {
                for lends in [1, 2, 9] {
                    holds::<4096>(seed, lends);
                    holds::<16384>(seed, lends);
                }
            }
        }
    }
}
