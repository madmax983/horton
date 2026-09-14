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
//! covers everything after `magic` except itself. Records never span blocks
//! in v0.1. Recovery replays records in order and stops at the first
//! corrupt/truncated record — the torn tail is the expected crash boundary,
//! not an error.

use core::future::poll_fn;

use crate::crc::crc32;
use crate::device::BlockDevice;
use crate::error::Error;
use crate::memtable::MemTable;

/// Record magic: ASCII "ls".
pub const WAL_MAGIC: u16 = 0x6C73;
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
            _ => None,
        }
    }
}

/// Total encoded length of a record for the given key/value lengths.
const fn record_len(key_len: usize, val_len: usize) -> usize {
    WAL_RECORD_OVERHEAD + key_len + val_len
}

/// Serializes one record into `out`.
///
/// Precondition: `out.len() >= record_len(key.len(), val.len())`,
/// `key.len() <= 0xFFFF`, and `val.len() <= 0xFFFF` (upheld by
/// [`WalWriter::append`]). Returns the bytes written.
fn encode_record(
    out: &mut [u8],
    seq: u64,
    op: Op,
    key: &[u8],
    key_len: u16,
    val: &[u8],
    val_len: u16,
) -> usize {
    let kl = usize::from(key_len);
    let vl = usize::from(val_len);
    // len covers seq..=crc32: 8 + 1 + 2 + 2 + kl + vl + 4 = 17 + kl + vl.
    let len = 17u32 + u32::from(key_len) + u32::from(val_len);
    out[0..2].copy_from_slice(&WAL_MAGIC.to_le_bytes());
    out[2..6].copy_from_slice(&len.to_le_bytes());
    out[6..14].copy_from_slice(&seq.to_le_bytes());
    out[14] = op.to_u8();
    out[15..17].copy_from_slice(&key_len.to_le_bytes());
    out[17..19].copy_from_slice(&val_len.to_le_bytes());
    out[19..19 + kl].copy_from_slice(&key[..kl]);
    out[19 + kl..19 + kl + vl].copy_from_slice(&val[..vl]);
    let crc_end = 19 + kl + vl;
    let crc = crc32(&out[2..crc_end]);
    out[crc_end..crc_end + 4].copy_from_slice(&crc.to_le_bytes());
    record_len(kl, vl)
}

/// A successfully decoded record, borrowing the input block.
struct Decoded<'a> {
    seq: u64,
    op: Op,
    key: &'a [u8],
    val: &'a [u8],
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

/// `true` if every byte in `bytes` is zero.
///
/// Word-chunked instead of a byte-at-a-time `iter().all()`: a run of live
/// records still hits the first (non-zero) byte immediately, but the
/// clean-padding tail — the common case this exists for, since every record
/// scan ends by confirming the rest of the block is zero — is checked 8
/// bytes at a time instead of one.
fn all_zero(bytes: &[u8]) -> bool {
    let mut chunks = bytes.chunks_exact(8);
    chunks.all(|c| u64::from_ne_bytes(c.try_into().unwrap_or([0; 8])) == 0)
        && chunks.remainder().iter().all(|&b| b == 0)
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
    let op = Op::from_u8(buf[14])?;
    let kl = usize::from(u16::from_le_bytes([buf[15], buf[16]]));
    let vl = usize::from(u16::from_le_bytes([buf[17], buf[18]]));
    if WAL_RECORD_OVERHEAD - 6 + kl + vl != len {
        return None;
    }
    let key = buf.get(19..19 + kl)?;
    let val = buf.get(19 + kl..19 + kl + vl)?;
    let crc_at = 19 + kl + vl;
    let stored = u32::from_le_bytes(buf.get(crc_at..crc_at + 4)?.try_into().ok()?);
    if crc32(buf.get(2..crc_at)?) != stored {
        return None;
    }
    Some(Decoded {
        seq,
        op,
        key,
        val,
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

    /// Highest sequence number appended or recovered so far.
    #[must_use]
    pub const fn max_seq(&self) -> u64 {
        self.max_seq
    }

    /// Appends a record to the staging buffer. Not durable until
    /// [`commit`](WalWriter::commit). For [`Op::Delete`], `val` is ignored.
    ///
    /// # Errors
    ///
    /// [`Error::KeyTooLarge`] / [`Error::ValueTooLarge`] when the key/value
    /// do not fit the u16 wire fields, [`Error::NoSpace`] when a single
    /// record exceeds `BLOCK` or the WAL region is exhausted, or
    /// [`Error::Device`] on I/O failure.
    pub async fn append(
        &mut self,
        seq: u64,
        op: Op,
        key: &[u8],
        val: &[u8],
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
        let rlen = record_len(key.len(), vlen);
        if rlen > BLOCK {
            return Err(Error::NoSpace);
        }
        if self.stage_len + rlen > BLOCK {
            self.write_stage().await?;
        }
        let n = encode_record(
            &mut self.stage[self.stage_len..],
            seq,
            op,
            key,
            key_len,
            &val[..vlen],
            val_len,
        );
        debug_assert_eq!(n, rlen);
        self.stage_len += n;
        if seq > self.max_seq {
            self.max_seq = seq;
        }
        Ok(())
    }

    /// Writes the staging buffer (zero-padded to a full block) and flushes
    /// the device. Idempotent: a no-op when nothing is staged.
    ///
    /// # Errors
    ///
    /// [`Error::NoSpace`] when the WAL region is exhausted, or
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
            return Err(Error::NoSpace);
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
    /// A torn tail is the expected crash boundary, not an error.
    ///
    /// # Errors
    ///
    /// [`Error::CorruptWal`] if a structurally valid record does not fit the
    /// table (practically unreachable through [`crate::Db`]), or
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
        };
        loop {
            let id = from + state.blocks_used;
            if id >= self.wal_end {
                break; // Region exhausted: nothing beyond wal_end was written.
            }
            let device = &mut self.device;
            let block = &mut self.stage;
            poll_fn(|cx| device.poll_read_block(cx, id, block))
                .await
                .map_err(Error::Device)?;
            let mut off = 0usize;
            let mut corrupt = false;
            while off < BLOCK {
                match scan_record(&block[off..]) {
                    Scan::Record(rec) => {
                        let tombstone = rec.op == Op::Delete;
                        if rec.seq > state.max_seq {
                            state.max_seq = rec.seq;
                        }
                        // Stale pre-wrap records: already in a table, never
                        // replayed (see the `seq_floor` docs above).
                        if rec.seq > seq_floor {
                            table
                                .insert::<D::Error>(rec.key, rec.val, rec.seq, tombstone)
                                .map_err(|_| Error::CorruptWal { offset: id })?;
                            state.records += 1;
                        }
                        off += rec.total_len;
                    }
                    // Clean zero padding: this block is done; the log may
                    // continue in the next block.
                    Scan::CleanEnd => break,
                    // Non-zero data that is not a valid record: the torn
                    // tail. Stop recovery here.
                    Scan::Corrupt => {
                        corrupt = true;
                        break;
                    }
                }
            }
            if off == 0 {
                break; // Unwritten block: end of log.
            }
            state.blocks_used += 1;
            if corrupt {
                break; // Torn tail: expected crash boundary, not an error.
            }
        }
        self.next_block = from + state.blocks_used;
        self.stage_len = 0;
        self.max_seq = state.max_seq;
        Ok(state)
    }
}
