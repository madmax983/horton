//! Append-only write-ahead log with CRC-framed records.
//!
//! Record layout (all little-endian):
//!
//! ```text
//! magic: u16 = 0x6C73 | len: u32 | seq: u64 | op: u8 | key_len: u16
//! | val_len: u16 | key | val | crc32: u32
//! ```
//!
//! `len` covers everything from `seq` through `crc32` inclusive, and `crc32`
//! covers everything after `magic` except itself. Records never span
//! blocks. Recovery replays records in order and stops at the first
//! corrupt/truncated record — the torn tail is the expected crash boundary,
//! not an error.
//!
//! The `op` byte's high bit ([`MORE`]) groups records: a record with it set
//! is followed by more records of the same atomic group, and the group ends
//! at the first record without it. Recovery replays a group only once its
//! closing record is intact, so a torn block write can never replay part of
//! a [`WriteBatch`](crate::WriteBatch).
//!
//! A group may span blocks (ADR-0029): the last record of a block whose
//! group goes on carries [`SPAN`], and the next block's first record
//! [`CONT`]. A group spans blocks only from the start of a block, over at
//! most [`GROUP_BLOCKS_MAX`] blocks; recovery
//! replays it only when every one of them is intact, and drops all of it
//! otherwise. The op kind is the byte's low five bits.

use core::future::poll_fn;

use crate::GROUP_BLOCKS_MAX;
use crate::crc::crc32;
use crate::device::BlockDevice;
use crate::error::Error;
use crate::memtable::MemTable;

/// Record magic: ASCII "ls".
pub const WAL_MAGIC: u16 = 0x6C73;

/// The `op` byte's group flag: more records of this record's atomic group
/// follow it. See the module docs.
pub const MORE: u8 = 0x80;
/// The `op` byte's span flag, on the last record of a block (with
/// [`MORE`]): the record's group continues in the next block, which
/// begins with a [`CONT`] record (ADR-0029).
pub const SPAN: u8 = 0x40;
/// The `op` byte's continuation flag, on the first record of a block: it
/// continues the group the previous block's [`SPAN`] record left open.
pub const CONT: u8 = 0x20;
/// The op byte's flag bits; the low bits are the [`Op`].
const FLAGS: u8 = MORE | SPAN | CONT;
/// Fixed header bytes before the key: magic + len + seq + op + `key_len` + `val_len`.
pub const WAL_HEADER_LEN: usize = 19;
/// Trailing CRC32 bytes.
pub const WAL_TRAILER_LEN: usize = 4;
/// Total record overhead: header + trailer.
pub const WAL_RECORD_OVERHEAD: usize = WAL_HEADER_LEN + WAL_TRAILER_LEN;

/// Mutation kind stored in a WAL record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Op {
    /// Insert or overwrite a key.
    Put = 1,
    /// Delete a key (tombstone).
    Delete = 2,
    /// Delete a key range `[key, val)`: `key` is the inclusive start,
    /// `val` the exclusive end.
    RangeDelete = 3,
    /// Insert with an absolute expiry tick: like [`Op::Put`] with an 8-byte
    /// little-endian `expire_at` appended after the value.
    PutTtl = 4,
}

impl Op {
    /// Encodes the op as its wire byte.
    #[must_use]
    pub const fn to_u8(self) -> u8 {
        self as u8
    }

    /// Decodes a wire byte; unknown values return `None`.
    #[must_use]
    pub const fn from_u8(byte: u8) -> Option<Self> {
        match byte {
            1 => Some(Self::Put),
            2 => Some(Self::Delete),
            3 => Some(Self::RangeDelete),
            4 => Some(Self::PutTtl),
            _ => None,
        }
    }
}

/// Total encoded length of a record for the given key/value lengths.
const fn record_len(key_len: usize, val_len: usize) -> usize {
    WAL_RECORD_OVERHEAD + key_len + val_len
}

/// Total encoded length of a [`Op::PutTtl`] record: like [`record_len`]
/// plus the 8-byte expiry.
const fn record_len_ttl(key_len: usize, val_len: usize) -> usize {
    WAL_RECORD_OVERHEAD + key_len + val_len + 8
}

/// Serializes one record into `out`.
///
/// For [`Op::PutTtl`], `extra` carries the 8-byte little-endian `expire_at`
/// appended after the value; for every other op it is empty (enforced by
/// [`WalWriter::append`] vs [`WalWriter::append_ttl`]).
///
/// Precondition: `out.len() >= record_len(key.len(), val.len()) + extra.len()`,
/// `key.len() <= 0xFFFF`, and `val.len() <= 0xFFFF` (upheld by
/// [`WalWriter::append`]). Returns the bytes written.
#[allow(clippy::too_many_arguments)]
fn encode_record(
    out: &mut [u8],
    seq: u64,
    op: Op,
    flags: u8,
    key: &[u8],
    key_len: u16,
    val: &[u8],
    val_len: u16,
    extra: &[u8],
) -> usize {
    let kl = usize::from(key_len);
    let vl = usize::from(val_len);
    let el = extra.len();
    // `extra` is either empty or the 8-byte TTL expiry (precondition,
    // upheld by the caller). The length field must cover exactly the
    // bytes written below; the debug_assert ties the two together — a
    // defensive clamp on one side only would desync them.
    debug_assert!(el == 0 || el == 8);
    let el32: u32 = if el == 8 { 8 } else { 0 };
    debug_assert_eq!(el, el32 as usize);
    // len covers seq..=crc32: 8 + 1 + 2 + 2 + kl + vl + el + 4. No
    // overflow: key_len/val_len are u16 and `extra` is at most 8 bytes.
    let len = 17u32 + u32::from(key_len) + u32::from(val_len) + el32;
    out[0..2].copy_from_slice(&WAL_MAGIC.to_le_bytes());
    out[2..6].copy_from_slice(&len.to_le_bytes());
    out[6..14].copy_from_slice(&seq.to_le_bytes());
    out[14] = op.to_u8() | flags;
    out[15..17].copy_from_slice(&key_len.to_le_bytes());
    out[17..19].copy_from_slice(&val_len.to_le_bytes());
    out[19..19 + kl].copy_from_slice(&key[..kl]);
    out[19 + kl..19 + kl + vl].copy_from_slice(&val[..vl]);
    out[19 + kl + vl..19 + kl + vl + el].copy_from_slice(extra);
    let crc_end = 19 + kl + vl + el;
    let crc = crc32(&out[2..crc_end]);
    out[crc_end..crc_end + 4].copy_from_slice(&crc.to_le_bytes());
    record_len(kl, vl) + el
}

/// A successfully decoded record, borrowing the input block.
struct Decoded<'a> {
    seq: u64,
    op: Op,
    /// More records of this record's atomic group follow ([`MORE`]).
    more: bool,
    /// The group continues in the next block ([`SPAN`]).
    span: bool,
    /// The record continues a group from the previous block ([`CONT`]).
    cont: bool,
    key: &'a [u8],
    val: &'a [u8],
    /// For [`Op::PutTtl`]: the absolute expiry tick; 0 otherwise.
    expire_at: u64,
    total_len: usize,
}

/// What [`scan_record`] found at the current offset.
enum Scan<'a> {
    /// A fully intact, CRC-valid record.
    Record(Decoded<'a>),
    /// The rest of the block is zero padding: the block is cleanly consumed.
    CleanEnd,
    /// Non-zero data that is not a valid record: the torn tail starts here.
    Corrupt,
}

/// Classifies the data at the current block offset: a valid record, clean
/// zero padding, or the torn tail.
fn scan_record(buf: &[u8]) -> Scan<'_> {
    if all_zero(buf) {
        return Scan::CleanEnd;
    }
    decode_record(buf).map_or(Scan::Corrupt, Scan::Record)
}

/// The highest sequence number of any intact record in `block`, at any
/// offset: a record left behind a torn write need not start where a scan
/// from the block's start would look. `None` when there is none.
fn highest_seq_anywhere(block: &[u8]) -> Option<u64> {
    let magic = WAL_MAGIC.to_le_bytes();
    let mut highest = None;
    for off in 0..block.len().saturating_sub(WAL_HEADER_LEN) {
        if block[off..off + 2] == magic
            && let Some(rec) = decode_record(&block[off..])
        {
            highest = Some(highest.map_or(rec.seq, |h: u64| h.max(rec.seq)));
        }
    }
    highest
}

/// `true` if every byte in `bytes` is zero.
///
/// Word-chunked instead of a byte-at-a-time `iter().all()`: a run of live
/// records still hits the first (non-zero) byte immediately, but the
/// clean-padding tail — the common case this exists for, since every record
/// scan ends by confirming the rest of the block is zero — is checked 8
/// bytes at a time instead of one.
fn all_zero(bytes: &[u8]) -> bool {
    let (chunks, remainder) = bytes.as_chunks::<8>();
    chunks.iter().all(|c| u64::from_ne_bytes(*c) == 0) && remainder.iter().all(|&b| b == 0)
}

/// Decodes one record at the start of `buf`.
///
/// Returns `None` for anything that is not a fully intact, CRC-valid record:
/// a torn write, a bad magic, a bad length, or a CRC mismatch.
fn decode_record(buf: &[u8]) -> Option<Decoded<'_>> {
    if buf.len() < WAL_HEADER_LEN {
        return None;
    }
    if u16::from_le_bytes([buf[0], buf[1]]) != WAL_MAGIC {
        return None;
    }
    let len = u32::from_le_bytes([buf[2], buf[3], buf[4], buf[5]]) as usize;
    let total = len.checked_add(6)?;
    if total > buf.len() {
        return None;
    }
    let seq = u64::from_le_bytes([
        buf[6], buf[7], buf[8], buf[9], buf[10], buf[11], buf[12], buf[13],
    ]);
    let op = Op::from_u8(buf[14] & !FLAGS)?;
    let more = buf[14] & MORE != 0;
    let span = buf[14] & SPAN != 0;
    let cont = buf[14] & CONT != 0;
    // A span continues an open group, and a continuation follows one:
    // anything else is not a record this writer wrote.
    if span && !more {
        return None;
    }
    let kl = usize::from(u16::from_le_bytes([buf[15], buf[16]]));
    let vl = usize::from(u16::from_le_bytes([buf[17], buf[18]]));
    // A PutTtl record carries 8 expiry bytes after the value.
    let el = if op == Op::PutTtl { 8 } else { 0 };
    if WAL_RECORD_OVERHEAD - 6 + kl + vl + el != len {
        return None;
    }
    let key = buf.get(19..19 + kl)?;
    let val = buf.get(19 + kl..19 + kl + vl)?;
    let expiry_off = 19 + kl + vl;
    let expire_at = if op == Op::PutTtl {
        u64::from_le_bytes(buf.get(expiry_off..expiry_off + 8)?.try_into().ok()?)
    } else {
        0
    };
    let crc_at = expiry_off + el;
    let stored = u32::from_le_bytes(buf.get(crc_at..crc_at + 4)?.try_into().ok()?);
    if crc32(buf.get(2..crc_at)?) != stored {
        return None;
    }
    Some(Decoded {
        seq,
        op,
        more,
        span,
        cont,
        key,
        val,
        expire_at,
        total_len: total,
    })
}

/// Summary of a [`WalWriter::recover`] run.
#[derive(Debug, Clone, Copy)]
pub struct RecoverState {
    /// Records successfully replayed.
    pub records: u64,
    /// Highest sequence number seen.
    pub max_seq: u64,
    /// WAL blocks consumed (first unwritten block is `wal_start + blocks_used`).
    pub blocks_used: u64,
    /// WAL blocks read up to the one recovery stopped at, from where it
    /// began. At least `blocks_used`; more when a group that spanned
    /// blocks broke off, since writing resumes at its first block
    /// (ADR-0029).
    pub blocks_read: u64,
}

/// One record in a WAL batch: the drainer sweep's unit of work.
///
/// `expire_at` is used only for [`Op::PutTtl`] (0 = never, stored as
/// [`Op::Put`], matching [`WalWriter::append_ttl`]); every other op
/// ignores it.
#[derive(Debug, Clone, Copy)]
pub struct BatchRecord<'a> {
    /// Sequence number of the record.
    pub seq: u64,
    /// Mutation kind.
    pub op: Op,
    /// Key bytes; for [`Op::RangeDelete`], the inclusive start.
    pub key: &'a [u8],
    /// Value bytes; for [`Op::RangeDelete`], the exclusive end; for
    /// [`Op::Delete`], ignored.
    pub val: &'a [u8],
    /// Absolute expiry tick for [`Op::PutTtl`].
    pub expire_at: u64,
}

/// Report from [`WalWriter::append_batch`]: the batch is prefix-atomic —
/// a prefix of the input is durable, and the records after it never
/// became durable.
#[derive(Debug)]
#[must_use]
pub struct BatchReport<E> {
    /// Records flush-acknowledged: `records[..durable]` are durable. The
    /// drainer advances its durable watermark over exactly these tickets.
    pub durable: usize,
    /// Records whose sequence numbers are consumed: `records[..consumed]`
    /// are either durable or may replay after a crash (blocks landed but
    /// the flush failed — the "device lied" case, same rule as the
    /// single-put rollback). Never reuse their seqnums.
    /// `records[consumed..]` never touched the device and may be retried
    /// with fresh seqnums. Always `consumed >= durable`.
    pub consumed: usize,
    /// `None` iff the whole batch is durable. Otherwise the error that
    /// determined the outcome: a commit failure subsumes an earlier append
    /// failure, because the commit is the durability step.
    pub error: Option<Error<E>>,
}

/// WAL writer: owns the device, a `[u8; BLOCK]` staging buffer, and the
/// append position. Records are staged in RAM and become durable on
/// [`commit`](WalWriter::commit).
pub struct WalWriter<D: BlockDevice, const BLOCK: usize> {
    device: D,
    wal_start: u64,
    wal_end: u64,
    stage: [u8; BLOCK],
    stage_len: usize,
    /// High-water mark: `stage[dirty_to..]` is guaranteed already zero (from
    /// the last block written, or the initial `[0u8; BLOCK]`). Lets
    /// [`write_stage`](WalWriter::write_stage) skip re-zeroing bytes that
    /// are already zero; see its doc comment.
    dirty_to: usize,
    next_block: u64,
    max_seq: u64,
    /// The last staged record opened a group (its [`MORE`] flag is set), so
    /// the stage must not be written until the group closes, or is sealed
    /// with [`SPAN`] to go on in the next block.
    group_open: bool,
    /// Offset in the stage of the last record staged: the one a seal marks
    /// with [`SPAN`].
    last_record: usize,
    /// The stage was sealed inside a group: the next record continues it
    /// and carries [`CONT`].
    continues: bool,
    /// Where the open group began in the stage: only a group that began
    /// at offset 0 may go on in the next block (ADR-0029).
    group_start: usize,
    /// Blocks of the open group already written: a group spans at most
    /// [`GROUP_BLOCKS_MAX`] blocks.
    group_blocks: usize,
}

impl<D: BlockDevice, const BLOCK: usize> WalWriter<D, BLOCK> {
    const ASSERT_BLOCK: () = assert!(BLOCK == D::BLOCK, "BLOCK must equal D::BLOCK");
    // The spec requires device blocks to be >= 512 bytes.
    const ASSERT_MIN: () = assert!(BLOCK >= 512, "BLOCK must be at least 512 bytes");

    /// Creates a writer over `device`, appending WAL blocks in
    /// `[wal_start, wal_end)`.
    #[must_use]
    pub const fn new(device: D, wal_start: u64, wal_end: u64) -> Self {
        // Associated consts are lazy: referencing them here forces the
        // parameter checks to be evaluated for every instantiation.
        let () = Self::ASSERT_BLOCK;
        let () = Self::ASSERT_MIN;
        Self {
            device,
            wal_start,
            wal_end,
            stage: [0u8; BLOCK],
            stage_len: 0,
            dirty_to: 0,
            next_block: wal_start,
            max_seq: 0,
            group_open: false,
            last_record: 0,
            continues: false,
            group_start: 0,
            group_blocks: 0,
        }
    }

    /// Consumes the writer and returns the underlying device.
    #[must_use]
    pub fn into_device(self) -> D {
        self.device
    }

    /// The underlying device (shared: block reads only need `&`).
    #[must_use]
    pub const fn device(&self) -> &D {
        &self.device
    }

    /// The underlying device, mutably.
    pub const fn device_mut(&mut self) -> &mut D {
        &mut self.device
    }

    /// Next block id that will be written: the WAL append position, and the
    /// value flush stores as the manifest's `wal_head`.
    #[must_use]
    pub const fn next_block(&self) -> u64 {
        self.next_block
    }

    /// Bytes currently staged in RAM: durable only after
    /// [`commit`](WalWriter::commit).
    #[must_use]
    pub const fn staged_bytes(&self) -> usize {
        self.stage_len
    }

    /// Truncates the staging buffer back to `len` bytes, discarding staged
    /// records that never became durable. Used to roll back a failed
    /// commit so a failed mutation can never resurrect through a later
    /// commit. Only valid when no block landed since the mark (`len` at
    /// most `stage_len`); shrinking is exactly the case
    /// `write_stage` already re-zeroes.
    pub fn truncate_stage(&mut self, len: usize) {
        debug_assert!(len <= self.stage_len, "truncate past staged data");
        // The discarded bytes stay in the buffer: keep them inside the
        // dirty mark so the next `write_stage` zeroes them. Otherwise a
        // shorter record staged next would be written with the discarded
        // record's tail after it — a torn tail that ends recovery early.
        if self.stage_len > self.dirty_to {
            self.dirty_to = self.stage_len;
        }
        self.stage_len = len;
        // Whatever group was open is discarded with its records.
        self.group_open = false;
        self.continues = false;
    }

    /// Repositions the append pointer to `block` (the WAL wrap).
    ///
    /// The caller must guarantee no live records exist: every record below
    /// the old position is flushed into `SSTables` (see [`crate::Db::flush`]),
    /// and the staging buffer is empty. Stale blocks left behind are skipped
    /// at recovery by the sequence floor; the manifest's `wal_head` move is
    /// committed atomically with the flush that wraps.
    pub fn reset_to(&mut self, block: u64) {
        debug_assert_eq!(self.stage_len, 0, "reset with staged records");
        self.next_block = block;
    }

    /// WAL blocks left before the end of the region: how many more
    /// commits (or sealed stages) fit.
    #[must_use]
    pub(crate) const fn blocks_left(&self) -> u64 {
        self.wal_end.saturating_sub(self.next_block)
    }

    /// Writes the stage out as a block of its own, without a flush, when
    /// `bytes` more would not fit it: the atomic group about to be
    /// appended then starts a fresh block, and stays inside it or spans
    /// blocks from its start (ADR-0029). The
    /// group commit ([`Db::write_group`](crate::Db::write_group)) seals
    /// blocks this way and flushes once at the end.
    ///
    /// # Errors
    ///
    /// [`Error::WalFull`] when the region is exhausted, or
    /// [`Error::Device`] on I/O failure.
    pub(crate) async fn make_room(&mut self, bytes: usize) -> Result<(), Error<D::Error>> {
        debug_assert!(!self.group_open, "make_room inside an open group");
        if self.stage_len > 0 && self.stage_len + bytes > BLOCK {
            self.write_stage().await?;
        }
        Ok(())
    }

    /// After recovery: makes what lies just past the recovered log
    /// harmless (ADR-0028). Reads the block writing resumes at and every
    /// one after it before block `until`; zeroes (then flushes) each of
    /// the later ones that holds a record newer than `newer_than` (the
    /// newest sequence number recovery saw, or the flush floor if
    /// higher); and returns the highest sequence number of any record
    /// found in them, at any offset, so the caller can resume numbering
    /// above it. `Db` passes `until` = where recovery stopped reading plus
    /// [`GROUP_BLOCKS_MAX`].
    ///
    /// A group commit writes up to `span` blocks before its one flush,
    /// and a device may reach the medium in any order before a flush, so
    /// a crash can leave records of the group that recovery did not take.
    /// Two things could bring them back later:
    ///
    /// - **A stranded block.** Recovery stopped at a gap, and writing
    ///   resumes there; once the gap is filled the next recovery reads on
    ///   into a later block of the group. Zeroing it prevents that. Only
    ///   blocks before `until` can hold such records, and only records
    ///   newer than everything recovered: older ones are stale, and every
    ///   recovery stops at them as before. The blocks of a group that
    ///   spanned blocks and broke off, between where writing resumes and
    ///   where recovery stopped, hold such records too (ADR-0029).
    /// - **Bytes left behind a torn write.** A block written again in
    ///   place can tear, keeping its old bytes past the tear. Recovery
    ///   takes records in a block only while their sequence numbers rise,
    ///   so old records after new ones are rejected, provided the new
    ///   ones are numbered above them. Recovery would otherwise number
    ///   new writes from the newest record it took, reusing the numbers
    ///   of the ones it dropped. The returned maximum is what the caller
    ///   numbers above. It is searched for at every offset, because a
    ///   tear can expose a record that a scan from the block's start would
    ///   not reach.
    ///
    /// Blocks are only read unless one needs zeroing, so an open costs
    /// nothing on flash in the common case.
    ///
    /// # Errors
    ///
    /// [`Error::Device`] on I/O failure.
    pub(crate) async fn scrub_stranded(
        &mut self,
        newer_than: u64,
        until: u64,
    ) -> Result<(u64, u64), Error<D::Error>> {
        debug_assert_eq!(self.stage_len, 0, "scrub with records staged");
        let first = self.next_block;
        let end = until.min(self.wal_end);
        let mut scrubbed = 0;
        let mut highest = 0u64;
        for id in first..end {
            let device = &mut self.device;
            let block = &mut self.stage;
            poll_fn(|cx| device.poll_read_block(cx, id, block))
                .await
                .map_err(Error::Device)?;
            let seen = highest_seq_anywhere(&self.stage);
            highest = highest.max(seen.unwrap_or(0));
            if id > first && seen.is_some_and(|seq| seq > newer_than) {
                self.stage.fill(0);
                let device = &mut self.device;
                let block = &self.stage;
                poll_fn(|cx| device.poll_write_block(cx, id, block))
                    .await
                    .map_err(Error::Device)?;
                scrubbed += 1;
            }
        }
        // The stage held the blocks read: none of it is known to be zero.
        self.dirty_to = BLOCK;
        if scrubbed > 0 {
            let device = &mut self.device;
            poll_fn(|cx| device.poll_flush(cx))
                .await
                .map_err(Error::Device)?;
        }
        Ok((scrubbed, highest))
    }

    /// Highest sequence number appended or recovered so far.
    #[must_use]
    pub const fn max_seq(&self) -> u64 {
        self.max_seq
    }

    /// Appends a record to the staging buffer. Not durable until
    /// [`commit`](WalWriter::commit). For [`Op::Delete`], `val` is ignored;
    /// for [`Op::RangeDelete`], `key` is the inclusive start and `val` the
    /// exclusive end. [`Op::PutTtl`] cannot be appended here — use
    /// [`append_ttl`](WalWriter::append_ttl), which carries the expiry.
    ///
    /// # Errors
    ///
    /// [`Error::KeyTooLarge`] / [`Error::ValueTooLarge`] when the key/value
    /// do not fit the u16 wire fields, [`Error::BatchTooLarge`] when a
    /// single record exceeds `BLOCK`, [`Error::WalFull`] when the WAL
    /// region is exhausted, or
    /// [`Error::Device`] on I/O failure.
    pub async fn append(
        &mut self,
        seq: u64,
        op: Op,
        key: &[u8],
        val: &[u8],
    ) -> Result<(), Error<D::Error>> {
        debug_assert_ne!(op, Op::PutTtl, "PutTtl needs append_ttl");
        self.append_inner(seq, op, key, val, 0, false).await
    }

    /// Appends one record of an atomic group: pass `more = true` for every
    /// record but the last. Recovery replays the group only if its last
    /// record is intact, so a torn write never replays part of it.
    ///
    /// A group that does not fit what is left of the stage goes on into
    /// the next block (ADR-0029): the stage is written out (no flush) with
    /// its last record marked [`SPAN`], and this record starts the next
    /// block marked [`CONT`]. Only a group that began at the start of a
    /// block may do that, over at most
    /// [`GROUP_BLOCKS_MAX`] blocks; start a large
    /// group on an empty stage ([`commit`](WalWriter::commit) first, as
    /// [`Db::write`](crate::Db::write) does). Until the closing record is
    /// committed, a crash drops the whole group.
    ///
    /// # Errors
    ///
    /// Same as [`append`](WalWriter::append), plus [`Error::BatchTooLarge`]
    /// when the group would have to go on into the next block but began
    /// part way into this one, or would span more than
    /// [`GROUP_BLOCKS_MAX`] blocks. That error,
    /// and a failed block write inside the group, drop the group's
    /// records from the stage (none of it is replayed); appending goes on
    /// with a new group.
    pub async fn append_grouped(
        &mut self,
        seq: u64,
        op: Op,
        key: &[u8],
        val: &[u8],
        more: bool,
    ) -> Result<(), Error<D::Error>> {
        debug_assert_ne!(op, Op::PutTtl, "PutTtl needs append_ttl");
        self.append_inner(seq, op, key, val, 0, more).await
    }

    /// Appends a [`Op::PutTtl`] record as part of a group: like
    /// [`append_grouped`](WalWriter::append_grouped), but carrying the
    /// absolute expiry tick `expire_at` (see
    /// [`append_ttl`](WalWriter::append_ttl)); `more` marks a record that
    /// is not the last of its group. `expire_at == 0` degrades to a plain
    /// [`Op::Put`], mirroring [`append_ttl`](WalWriter::append_ttl).
    ///
    /// # Errors
    ///
    /// Same as [`append_grouped`](WalWriter::append_grouped).
    pub async fn append_grouped_ttl(
        &mut self,
        seq: u64,
        key: &[u8],
        val: &[u8],
        expire_at: u64,
        more: bool,
    ) -> Result<(), Error<D::Error>> {
        let op = if expire_at == 0 { Op::Put } else { Op::PutTtl };
        self.append_inner(seq, op, key, val, expire_at, more).await
    }

    /// Appends a [`Op::PutTtl`] record: like [`append`](WalWriter::append)
    /// with [`Op::Put`], plus the absolute expiry tick `expire_at` (a value
    /// with `expire_at <= now` reads as absent; 0 means "never expires" and
    /// is stored as a plain [`Op::Put`]).
    ///
    /// # Errors
    ///
    /// Same as [`append`](WalWriter::append).
    pub async fn append_ttl(
        &mut self,
        seq: u64,
        key: &[u8],
        val: &[u8],
        expire_at: u64,
    ) -> Result<(), Error<D::Error>> {
        let op = if expire_at == 0 { Op::Put } else { Op::PutTtl };
        self.append_inner(seq, op, key, val, expire_at, false).await
    }

    /// Shared append path; `expire_at` is used only for [`Op::PutTtl`], and
    /// `more` marks a record that is not the last of its group.
    async fn append_inner(
        &mut self,
        seq: u64,
        op: Op,
        key: &[u8],
        val: &[u8],
        expire_at: u64,
        more: bool,
    ) -> Result<(), Error<D::Error>> {
        let key_len = u16::try_from(key.len()).map_err(|_| Error::KeyTooLarge {
            len: key.len(),
            max: usize::from(u16::MAX),
        })?;
        let vlen = if op == Op::Delete { 0 } else { val.len() };
        let val_len = u16::try_from(vlen).map_err(|_| Error::ValueTooLarge {
            len: vlen,
            max: usize::from(u16::MAX),
        })?;
        let rlen = if op == Op::PutTtl {
            record_len_ttl(key.len(), vlen)
        } else {
            record_len(key.len(), vlen)
        };
        if rlen > BLOCK {
            return Err(Error::BatchTooLarge {
                bytes: rlen,
                max: BLOCK,
            });
        }
        if self.stage_len + rlen > BLOCK {
            if self.group_open {
                self.seal_span().await?;
            } else {
                self.write_stage().await?;
            }
        }
        if !self.group_open {
            // This record begins a group (or is one by itself).
            self.group_start = self.stage_len;
            self.group_blocks = 0;
        }
        let expiry = expire_at.to_le_bytes();
        let extra = if op == Op::PutTtl { &expiry[..] } else { &[] };
        let flags = if more { MORE } else { 0 } | if self.continues { CONT } else { 0 };
        let n = encode_record(
            &mut self.stage[self.stage_len..],
            seq,
            op,
            flags,
            key,
            key_len,
            &val[..vlen],
            val_len,
            extra,
        );
        debug_assert_eq!(n, rlen);
        self.last_record = self.stage_len;
        self.stage_len += n;
        self.group_open = more;
        self.continues = false;
        if seq > self.max_seq {
            self.max_seq = seq;
        }
        Ok(())
    }

    /// The open group does not fit what is left of the stage: writes the
    /// stage out with the group's last record here marked [`SPAN`], and
    /// the next record continues the group, carrying [`CONT`] (ADR-0029).
    ///
    /// Only a group that began at the start of a block may span blocks,
    /// and over at most [`GROUP_BLOCKS_MAX`] of them: recovery needs the
    /// first, and open's scrub the second. A group that cannot go on is
    /// refused with [`Error::BatchTooLarge`], and so is one whose block
    /// write fails; either way the group is dropped from the stage, so no
    /// later commit writes any of it. A block of it already written is
    /// never closed, and recovery drops the whole group.
    ///
    /// # Errors
    ///
    /// [`Error::BatchTooLarge`], [`Error::WalFull`] or [`Error::Device`].
    async fn seal_span(&mut self) -> Result<(), Error<D::Error>> {
        if self.group_start != 0 || self.group_blocks + 2 > GROUP_BLOCKS_MAX {
            let bytes = self.stage_len - self.group_start;
            let start = self.group_start;
            self.truncate_stage(start);
            return Err(Error::BatchTooLarge {
                bytes,
                max: if start == 0 {
                    GROUP_BLOCKS_MAX * BLOCK
                } else {
                    BLOCK - start
                },
            });
        }
        self.mark_span();
        if let Err(e) = self.write_stage().await {
            self.truncate_stage(0);
            return Err(e);
        }
        self.group_blocks += 1;
        self.continues = true;
        Ok(())
    }

    /// Marks the last staged record with [`SPAN`] and re-seals its CRC.
    /// It belongs to the open group, so it carries [`MORE`].
    fn mark_span(&mut self) {
        let at = self.last_record;
        debug_assert!(self.stage[at + 14] & MORE != 0, "span without MORE");
        self.stage[at + 14] |= SPAN;
        let len = u32::from_le_bytes([
            self.stage[at + 2],
            self.stage[at + 3],
            self.stage[at + 4],
            self.stage[at + 5],
        ]) as usize;
        // The CRC covers `len` and everything after it but itself.
        let crc_at = at + 6 + len - WAL_TRAILER_LEN;
        let crc = crc32(&self.stage[at + 2..crc_at]);
        self.stage[crc_at..crc_at + WAL_TRAILER_LEN].copy_from_slice(&crc.to_le_bytes());
    }

    /// Writes the staging buffer (zero-padded to a full block) and flushes
    /// the device. Idempotent: a no-op when nothing is staged.
    ///
    /// # Errors
    ///
    /// [`Error::WalFull`] when the WAL region is exhausted, or
    /// [`Error::Device`] on I/O failure.
    pub async fn commit(&mut self) -> Result<(), Error<D::Error>> {
        if self.stage_len > 0 {
            self.write_stage().await?;
        }
        let device = &mut self.device;
        poll_fn(|cx| device.poll_flush(cx))
            .await
            .map_err(Error::Device)?;
        Ok(())
    }

    /// Appends `records` as one batch and flushes once.
    ///
    /// Staging, full-block writes, and record encoding are exactly the
    /// single-record path's; the batch only changes the flush granularity
    /// and the error semantics. An empty batch is a no-op (no flush).
    ///
    /// The batch is prefix-atomic (SPEC §11): once a full staging block
    /// lands on the device it cannot be unwritten, so a failed batch
    /// resolves to a durable prefix, never to a hole.
    ///
    /// Several blocks are written before the one flush, and a device may
    /// persist them in any order, so a crash can keep a later block
    /// without an earlier one. [`Db::open`](crate::Db::open) zeroes such
    /// stranded blocks within [`GROUP_BLOCKS_MAX`]
    /// blocks of where recovery stopped (ADR-0028); keep a batch to that
    /// many blocks, as [`Db::write_group`](crate::Db::write_group) does,
    /// for the guarantee that a later recovery never brings back a record
    /// an earlier one dropped. See [`BatchReport`]
    /// for the exact `durable` / `consumed` / `error` contract. This is the
    /// primitive the multiwriter drainer sweep is built on: drain up to
    /// `max_writes` tickets, `append_batch`, flush once.
    ///
    /// # Errors
    ///
    /// The returned [`BatchReport::error`] carries [`Error::KeyTooLarge`]
    /// / [`Error::ValueTooLarge`] for a bad record, [`Error::BatchTooLarge`]
    /// when a record exceeds `BLOCK`, [`Error::WalFull`] when the WAL
    /// region is exhausted, or [`Error::Device`] on I/O failure — the same
    /// variants as
    /// [`append`](WalWriter::append).
    pub async fn append_batch(&mut self, records: &[BatchRecord<'_>]) -> BatchReport<D::Error> {
        if records.is_empty() {
            return BatchReport {
                durable: 0,
                consumed: 0,
                error: None,
            };
        }
        let mark_len = self.stage_len;
        let mark_block = self.next_block;
        let mut appended = 0;
        let mut append_err = None;
        for rec in records {
            // `append` debug-asserts against `PutTtl`; TTL records go
            // through `append_ttl`, which stores `expire_at == 0` as a
            // plain `Put`.
            let r = if rec.op == Op::PutTtl {
                self.append_ttl(rec.seq, rec.key, rec.val, rec.expire_at)
                    .await
            } else {
                self.append(rec.seq, rec.op, rec.key, rec.val).await
            };
            if let Err(e) = r {
                append_err = Some(e);
                break;
            }
            appended += 1;
        }
        match self.commit().await {
            Ok(()) => BatchReport {
                durable: appended,
                consumed: appended,
                error: append_err,
            },
            Err(commit_err) => {
                // The commit is the durability step: its error determines
                // the outcome, subsuming any earlier append error.
                if self.next_block == mark_block {
                    // Nothing reached the device: truncate the staged
                    // prefix; every seqnum is reusable.
                    self.truncate_stage(mark_len);
                    BatchReport {
                        durable: 0,
                        consumed: 0,
                        error: Some(commit_err),
                    }
                } else {
                    // Blocks landed but the flush failed — the device lied,
                    // indistinguishable from a crash at that instant: the
                    // appended prefix may replay, so its seqnums are
                    // consumed, never reused.
                    BatchReport {
                        durable: 0,
                        consumed: appended,
                        error: Some(commit_err),
                    }
                }
            }
        }
    }

    /// Writes the current staging buffer as one zero-padded block.
    ///
    /// `stage` is a persistent buffer reused across every commit, not a
    /// fresh one per call. `dirty_to` (the previous commit's `stage_len`)
    /// marks how far its old content could still be nonzero: everything
    /// from there to `BLOCK` was zeroed by that commit's own fill and
    /// nothing has written past it since. So there is live garbage to clear
    /// only where this record set is shorter than the last one
    /// (`stage_len < dirty_to`) — zeroing the shrunk gap is enough to
    /// restore "everything past `stage_len` is zero" for the write below;
    /// re-zeroing out to `BLOCK` every time (the previous approach) redid
    /// that work even when this batch is the same size or grew.
    async fn write_stage(&mut self) -> Result<(), Error<D::Error>> {
        if self.next_block >= self.wal_end {
            return Err(Error::WalFull);
        }
        if self.stage_len < self.dirty_to {
            self.stage[self.stage_len..self.dirty_to].fill(0);
        }
        self.dirty_to = self.stage_len;
        let id = self.next_block;
        let device = &mut self.device;
        let stage = &self.stage;
        poll_fn(|cx| device.poll_write_block(cx, id, stage))
            .await
            .map_err(Error::Device)?;
        self.next_block += 1;
        self.stage_len = 0;
        Ok(())
    }

    /// Replays the WAL from `wal_start` into `table`. See
    /// [`recover_from`](WalWriter::recover_from).
    ///
    /// # Errors
    ///
    /// Same as [`recover_from`](WalWriter::recover_from).
    pub async fn recover<
        const CAP: usize,
        const ARENA: usize,
        const KEY_MAX: usize,
        const VAL_MAX: usize,
    >(
        &mut self,
        table: &mut MemTable<CAP, ARENA, KEY_MAX, VAL_MAX>,
    ) -> Result<RecoverState, Error<D::Error>> {
        self.recover_from(table, self.wal_start, 0).await
    }

    /// Replays the WAL from block `from` into `table`, stopping at the first
    /// corrupt/truncated record. Positions the writer to continue appending
    /// after the last consumed block and resets the staging buffer.
    ///
    /// `Db` passes the manifest's `wal_head`: blocks before it were flushed
    /// into `SSTables` and are no longer needed for recovery.
    ///
    /// Records with `seq <= seq_floor` are skipped, not replayed: after a WAL
    /// wrap (`wal_head` moved back to `wal_start` by a flush) the scan passes
    /// over stale pre-wrap blocks, and every mutation they hold is already
    /// in a table — replaying them would resurrect superseded versions that
    /// shadow newer table data via the memtable. `Db` passes the manifest's
    /// `max_seq`; it is always a no-op when the WAL never wrapped, because
    /// live records all have `seq > max_seq`.
    ///
    /// A group of records ([`append_grouped`](WalWriter::append_grouped))
    /// replays only when its closing record is intact; a torn write never
    /// replays part of one. A block torn before any group completed ends the
    /// log, and the writer resumes on that block. A block torn after
    /// complete groups ends the log too, unless the next block starts with
    /// a newer record: an earlier recovery resumed past it, and the writes
    /// that followed are live.
    ///
    /// A torn tail is the expected crash boundary, not an error. So is the
    /// first block holding only stale records: appends run sequentially
    /// from `wal_head`, and a wrap moves `wal_head` back to `wal_start` in
    /// the same commit that flushes everything, so stale blocks can only
    /// follow the live tail. The writer resumes at that block, overwriting
    /// dead records instead of treating the region as full.
    ///
    /// # Errors
    ///
    /// [`Error::CorruptWal`] if a structurally valid record does not fit the
    /// table. Through [`crate::Db`] that would take a WAL holding more than
    /// one memtable's worth of records past `wal_head`: every write checks
    /// capacity counting the records a failed commit may have left there
    /// (ADR-0028), so it does not happen short of corruption. Or
    /// [`Error::Device`] on I/O failure.
    pub async fn recover_from<
        const CAP: usize,
        const ARENA: usize,
        const KEY_MAX: usize,
        const VAL_MAX: usize,
    >(
        &mut self,
        table: &mut MemTable<CAP, ARENA, KEY_MAX, VAL_MAX>,
        from: u64,
        seq_floor: u64,
    ) -> Result<RecoverState, Error<D::Error>> {
        let mut state = RecoverState {
            records: 0,
            max_seq: 0,
            blocks_used: 0,
            blocks_read: 0,
        };
        // Set after a torn block that still held complete groups, and after
        // a group that spanned blocks broke off: the log goes on only if the
        // next block holds newer records (writes made after an earlier
        // recovery resumed past it).
        let mut newer_than: Option<u64> = None;
        // A group that began in an earlier block and goes on (ADR-0029).
        let mut span: Option<OpenSpan> = None;
        // Blocks read so far. `state.blocks_used` is where writing resumes:
        // past the last block that held a complete group, which the blocks
        // of a group that never closed do not.
        let mut cursor = 0u64;
        loop {
            let id = from + cursor;
            if id >= self.wal_end {
                break; // Region exhausted: nothing beyond wal_end was written.
            }
            let device = &mut self.device;
            let block = &mut self.stage;
            poll_fn(|cx| device.poll_read_block(cx, id, block))
                .await
                .map_err(Error::Device)?;
            let seen = scan_block(&self.stage, span.map(|s| s.last_seq), seq_floor);
            if span.is_some() && !seen.joined {
                // The next block does not continue the open group: it
                // never finished. Drop all of it. This block (never the
                // one after it) goes on with the log only if it is newer
                // than everything taken, the flush floor included: a
                // failed commit can leave a dead span behind live writes.
                span = None;
                newer_than = Some(state.max_seq.max(seq_floor));
            }
            if let Some(floor) = newer_than.take()
                && seen.first_seq.is_none_or(|seq| seq <= floor)
            {
                break; // The tear, or the broken group, was the tail after all.
            }
            if seen.joined {
                let Some(open) = span else {
                    return Err(Error::CorruptWal { offset: id });
                };
                if id - open.start_block >= GROUP_BLOCKS_MAX as u64 {
                    // Longer than any group a writer makes: not one.
                    break;
                }
                match seen.closes_span {
                    Some(end) => {
                        // The group is whole: replay it from where it began.
                        self.replay_span(table, open, id, end, seq_floor, &mut state)
                            .await?;
                        span = None;
                        state.blocks_used = cursor + 1;
                    }
                    None if seen.opens_span.is_some() => {
                        // The whole block goes on with the group.
                        span = Some(OpenSpan {
                            last_seq: seen.last_seq.unwrap_or(open.last_seq),
                            ..open
                        });
                        cursor += 1;
                        continue;
                    }
                    None => {
                        // Torn inside the group, or the group's records
                        // end without closing it: it never finished, and
                        // nothing was written after it.
                        break;
                    }
                }
            }
            if !seen.joined && !seen.live && !seen.torn && seen.first_seq.is_some() {
                // Only stale (already flushed) records: the live log ended
                // at the previous block. Resume appending here.
                break;
            }
            if !seen.joined && seen.complete == 0 && seen.opens_span.is_none() {
                // Unwritten, or torn before any group completed: the log
                // ends here, and the writer resumes on this block, so no
                // write ever lands behind a torn block without a group.
                break;
            }
            // Replay the complete groups after whatever the span took.
            let replayed = self.replay_range(
                table,
                id,
                seen.closes_span.unwrap_or(0),
                seen.complete,
                seq_floor,
                &mut state,
            )?;
            if replayed || seen.complete > seen.closes_span.unwrap_or(0) {
                state.blocks_used = cursor + 1;
            }
            if let Some(start) = seen.opens_span {
                // A group begins here and goes on in the next block.
                span = Some(OpenSpan {
                    start_block: id,
                    start_off: start,
                    last_seq: seen.last_seq.unwrap_or(0),
                });
            } else if seen.torn {
                // A torn tail after complete groups: the crash boundary,
                // unless newer writes follow (see `newer_than`).
                newer_than = Some(state.max_seq);
            }
            cursor += 1;
        }
        state.blocks_read = cursor;
        self.next_block = from + state.blocks_used;
        self.stage_len = 0;
        // Recovery read blocks into `stage`: none of it is known to be zero
        // any more, so the next `write_stage` must zero the whole tail.
        self.dirty_to = BLOCK;
        self.max_seq = state.max_seq;
        self.group_open = false;
        self.continues = false;
        Ok(state)
    }

    /// Replays the records of the block in the stage (block `id`) from
    /// offset `from` up to `to`: complete groups, as [`scan_block`] found
    /// them. Records at or below `seq_floor` are stale and skipped.
    /// Returns whether any record was replayed.
    fn replay_range<
        const CAP: usize,
        const ARENA: usize,
        const KEY_MAX: usize,
        const VAL_MAX: usize,
    >(
        &self,
        table: &mut MemTable<CAP, ARENA, KEY_MAX, VAL_MAX>,
        id: u64,
        from: usize,
        to: usize,
        seq_floor: u64,
        state: &mut RecoverState,
    ) -> Result<bool, Error<D::Error>> {
        let mut at = from;
        let mut live = false;
        while at < to {
            let Scan::Record(rec) = scan_record(&self.stage[at..]) else {
                return Err(Error::CorruptWal { offset: id });
            };
            live |= replay_record(table, &rec, id, seq_floor, state)?;
            at += rec.total_len;
        }
        Ok(live)
    }

    /// Replays a group that spanned blocks, now that its closing record
    /// (ending at `end` in block `close`) has been found intact: re-reads
    /// each of its blocks from where it began, so no record of it is held
    /// in RAM before the group is known to be whole. Leaves block `close`
    /// in the stage.
    async fn replay_span<
        const CAP: usize,
        const ARENA: usize,
        const KEY_MAX: usize,
        const VAL_MAX: usize,
    >(
        &mut self,
        table: &mut MemTable<CAP, ARENA, KEY_MAX, VAL_MAX>,
        open: OpenSpan,
        close: u64,
        end: usize,
        seq_floor: u64,
        state: &mut RecoverState,
    ) -> Result<(), Error<D::Error>> {
        let mut id = open.start_block;
        let mut from = open.start_off;
        loop {
            let device = &mut self.device;
            let block = &mut self.stage;
            poll_fn(|cx| device.poll_read_block(cx, id, block))
                .await
                .map_err(Error::Device)?;
            let to = if id == close {
                end
            } else {
                // Up to and including the block's `SPAN` record.
                span_end(&self.stage, from).ok_or(Error::CorruptWal { offset: id })?
            };
            self.replay_range(table, id, from, to, seq_floor, state)?;
            if id == close {
                return Ok(());
            }
            id += 1;
            from = 0;
        }
    }
}

/// A group open across blocks during recovery (ADR-0029).
#[derive(Debug, Clone, Copy)]
struct OpenSpan {
    /// The block and offset of the group's first record.
    start_block: u64,
    start_off: usize,
    /// The sequence number of its last record so far: the next block must
    /// continue with the one after.
    last_seq: u64,
}

/// What one block holds, as recovery reads it.
#[derive(Debug, Clone, Copy, Default)]
struct BlockScan {
    /// The block begins by continuing the open group (its first record
    /// carries [`CONT`] and the next sequence number).
    joined: bool,
    /// End of the record that closes the open group, when the block does.
    closes_span: Option<usize>,
    /// End of the block's last complete group (0: none).
    complete: usize,
    /// Start of a group that goes on in the next block ([`SPAN`]).
    opens_span: Option<usize>,
    /// Anything that is not a record, or a group cut short, before the
    /// block's clean zero padding.
    torn: bool,
    /// Some record is newer than the flush floor.
    live: bool,
    first_seq: Option<u64>,
    last_seq: Option<u64>,
}

/// Reads the records of `block`. `continuing` is the last sequence number
/// of the group open from the block before, if any; `seq_floor` marks
/// records already flushed.
///
/// A block's records carry strictly increasing sequence numbers, and
/// sequence numbers are never reused, so a record no newer than the one
/// before it is a previous write's bytes left behind a torn in-place
/// write (a file or disk keeps them; NOR flash erases first). Taking it
/// would let an old group's closing record close a new, torn group (F21).
/// A [`CONT`] record anywhere but first, or first without the group it
/// continues, and a [`SPAN`] record not followed by clean padding, are
/// such bytes as well.
fn scan_block(block: &[u8], continuing: Option<u64>, seq_floor: u64) -> BlockScan {
    let mut scan = BlockScan::default();
    let mut off = 0usize;
    // Start of the group being read; `None` while still inside the group
    // continued from the block before.
    let mut group_start = None;
    while off < block.len() {
        match scan_record(&block[off..]) {
            Scan::Record(rec) => {
                if scan.last_seq.is_some_and(|last| rec.seq <= last) {
                    scan.torn = true;
                    break;
                }
                if off == 0 {
                    scan.joined = rec.cont && continuing.is_some_and(|last| rec.seq == last + 1);
                    if rec.cont != scan.joined {
                        scan.torn = true;
                        break;
                    }
                    if !scan.joined {
                        group_start = Some(0);
                    }
                } else if rec.cont {
                    scan.torn = true;
                    break;
                }
                scan.first_seq.get_or_insert(rec.seq);
                scan.last_seq = Some(rec.seq);
                scan.live |= rec.seq > seq_floor;
                off += rec.total_len;
                if rec.span {
                    // A group goes on in the next block only from the start
                    // of this one: it continues one (no group start of its
                    // own here), or began at offset 0 (ADR-0029).
                    if group_start.is_none_or(|g| g == 0) && all_zero(&block[off..]) {
                        scan.opens_span = Some(0);
                    } else {
                        scan.torn = true;
                    }
                    break;
                }
                if !rec.more {
                    if group_start.is_none() {
                        scan.closes_span = Some(off);
                    }
                    scan.complete = off;
                    group_start = Some(off);
                }
            }
            // Clean zero padding: this block is done.
            Scan::CleanEnd => break,
            // Non-zero data that is not a valid record: torn.
            Scan::Corrupt => {
                scan.torn = true;
                break;
            }
        }
    }
    // A group whose closing record never landed is torn, even when what
    // did land reads as clean padding; one that goes on in the next block
    // is not.
    if scan.opens_span.is_none() && scan.complete < off {
        scan.torn = true;
    }
    scan
}

/// The end of the [`SPAN`] record that ends a group's part of `block`,
/// reading from `from`.
fn span_end(block: &[u8], from: usize) -> Option<usize> {
    let mut at = from;
    while at < block.len() {
        let Scan::Record(rec) = scan_record(&block[at..]) else {
            return None;
        };
        at += rec.total_len;
        if rec.span {
            return Some(at);
        }
    }
    None
}

/// Applies one recovered record to `table` unless it is stale (at or
/// below `seq_floor`), and counts it. Returns whether it was applied.
fn replay_record<
    const CAP: usize,
    const ARENA: usize,
    const KEY_MAX: usize,
    const VAL_MAX: usize,
    E,
>(
    table: &mut MemTable<CAP, ARENA, KEY_MAX, VAL_MAX>,
    rec: &Decoded<'_>,
    id: u64,
    seq_floor: u64,
    state: &mut RecoverState,
) -> Result<bool, Error<E>> {
    if rec.seq > state.max_seq {
        state.max_seq = rec.seq;
    }
    // Stale pre-wrap records: already in a table, never replayed (see the
    // `seq_floor` docs on `recover_from`).
    if rec.seq <= seq_floor {
        return Ok(false);
    }
    let applied = match rec.op {
        Op::RangeDelete => table.insert_range_del::<E>(rec.key, rec.val, rec.seq),
        Op::PutTtl => table.insert_ttl::<E>(rec.key, rec.val, rec.seq, rec.expire_at),
        _ => table.insert::<E>(rec.key, rec.val, rec.seq, rec.op == Op::Delete),
    };
    applied.map_err(|_| Error::CorruptWal { offset: id })?;
    state.records += 1;
    Ok(true)
}
