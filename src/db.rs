//! Minimal v0.1 database: WAL + memtable.
//!
//! Write path is WAL-first: every mutation is appended to the WAL and
//! committed before it lands in the memtable, so a crash can only lose the
//! un-acknowledged tail. Reads are served from the memtable (`SSTables` arrive
//! in v0.2).

use crate::device::BlockDevice;
use crate::error::Error;
use crate::memtable::MemTable;
use crate::wal::{Op, RecoverState, WalWriter};

/// Placement of the WAL on the device: blocks `[wal_start, wal_end)`.
///
/// v0.1 uses a bump pointer with no reuse; the open-time position is derived
/// from replay. (True block allocation is a v0.3 item per the spec.)
#[derive(Debug, Clone, Copy)]
pub struct Config {
    /// First block id of the WAL region.
    pub wal_start: u64,
    /// One past the last block id of the WAL region.
    pub wal_end: u64,
}

impl Config {
    /// Creates a WAL region descriptor.
    #[must_use]
    pub const fn new(wal_start: u64, wal_end: u64) -> Self {
        Self { wal_start, wal_end }
    }
}

/// Summary of a [`Db::open`] call.
#[derive(Debug, Clone, Copy)]
pub struct OpenReport {
    /// WAL records replayed into the memtable.
    pub recovered_records: u64,
    /// Highest sequence number found; the next mutation uses `max_seq + 1`.
    pub max_seq: u64,
}

/// The database handle. Owns the WAL writer (and through it, the device)
/// plus the memtable and the sequence counter.
pub struct Db<
    D: BlockDevice,
    const BLOCK: usize,
    const KEY_MAX: usize,
    const VAL_MAX: usize,
    const CAP: usize,
    const ARENA: usize,
> {
    wal: WalWriter<D, BLOCK>,
    table: MemTable<CAP, ARENA, KEY_MAX, VAL_MAX>,
    next_seq: u64,
}

impl<
        D: BlockDevice,
        const BLOCK: usize,
        const KEY_MAX: usize,
        const VAL_MAX: usize,
        const CAP: usize,
        const ARENA: usize,
    > Db<D, BLOCK, KEY_MAX, VAL_MAX, CAP, ARENA>
{
    const ASSERT_BLOCK: () = assert!(BLOCK == D::BLOCK, "BLOCK must equal D::BLOCK");
    const ASSERT_KEY: () = assert!(
        KEY_MAX >= 1 && KEY_MAX <= 0xFFFF,
        "KEY_MAX must be within 1..=0xFFFF"
    );
    const ASSERT_VAL: () = assert!(VAL_MAX <= 0xFFFF, "VAL_MAX must fit in u16");
    const ASSERT_REC: () = assert!(
        BLOCK >= 23 + KEY_MAX + VAL_MAX,
        "BLOCK must fit the largest WAL record"
    );

    /// Creates a closed database handle over `device`.
    #[must_use]
    pub const fn new(device: D, config: Config) -> Self {
        // Associated consts are lazy: referencing them here forces the
        // parameter checks to be evaluated for every instantiation.
        let () = Self::ASSERT_BLOCK;
        let () = Self::ASSERT_KEY;
        let () = Self::ASSERT_VAL;
        let () = Self::ASSERT_REC;
        Self {
            wal: WalWriter::new(device, config.wal_start, config.wal_end),
            table: MemTable::new(),
            next_seq: 0,
        }
    }

    /// Consumes the handle and returns the underlying device.
    #[must_use]
    pub fn into_device(self) -> D {
        self.wal.into_device()
    }

    /// Opens the database: replays the WAL into a fresh memtable and resumes
    /// the sequence counter and the WAL append position. Idempotent.
    ///
    /// A torn WAL tail stops recovery silently; it is the expected crash
    /// boundary, not an error.
    ///
    /// # Errors
    ///
    /// [`Error::CorruptWal`] or [`Error::Device`].
    pub async fn open(&mut self) -> Result<OpenReport, Error<D::Error>> {
        self.table.clear();
        let state: RecoverState = self.wal.recover(&mut self.table).await?;
        self.next_seq = state.max_seq;
        Ok(OpenReport {
            recovered_records: state.records,
            max_seq: state.max_seq,
        })
    }

    /// Stores `key` → `val`, durable before it returns. Returns the sequence
    /// number assigned to the mutation.
    ///
    /// # Errors
    ///
    /// [`Error::EmptyKey`], [`Error::KeyTooLarge`], [`Error::ValueTooLarge`],
    /// [`Error::TableFull`], [`Error::ArenaFull`], [`Error::NoSpace`], or
    /// [`Error::Device`].
    pub async fn put(&mut self, key: &[u8], val: &[u8]) -> Result<u64, Error<D::Error>> {
        self.table.check_insert::<D::Error>(key, val, false)?;
        let seq = self.next_seq.checked_add(1).ok_or(Error::NoSpace)?;
        self.wal.append(seq, Op::Put, key, val).await?;
        self.wal.commit().await?;
        self.next_seq = seq;
        // The table is unchanged since check_insert, so this cannot fail.
        self.table.insert(key, val, seq, false)?;
        Ok(seq)
    }

    /// Deletes `key` via a tombstone, durable before it returns. Returns the
    /// sequence number assigned to the mutation.
    ///
    /// # Errors
    ///
    /// Same as [`put`](Db::put).
    pub async fn delete(&mut self, key: &[u8]) -> Result<u64, Error<D::Error>> {
        self.table.check_insert::<D::Error>(key, &[], true)?;
        let seq = self.next_seq.checked_add(1).ok_or(Error::NoSpace)?;
        self.wal.append(seq, Op::Delete, key, &[]).await?;
        self.wal.commit().await?;
        self.next_seq = seq;
        // The table is unchanged since check_insert, so this cannot fail.
        self.table.insert(key, &[], seq, true)?;
        Ok(seq)
    }

    /// Reads `key` from the memtable into `val_buf`.
    ///
    /// Returns `Ok(None)` for missing keys and tombstones. Never truncates:
    /// an undersized buffer yields [`Error::BufferTooSmall`] with the
    /// required length.
    ///
    /// # Errors
    ///
    /// [`Error::BufferTooSmall`] when `val_buf` is smaller than the value.
    // `get` performs no I/O today, but the spec mandates a uniform async API
    // across all `Db` methods; a future version may await (e.g. SSTable reads).
    #[allow(clippy::unused_async, clippy::unused_async_trait_impl)]
    pub async fn get(
        &self,
        key: &[u8],
        val_buf: &mut [u8],
    ) -> Result<Option<usize>, Error<D::Error>> {
        let Some(entry) = self.table.get(key) else {
            return Ok(None);
        };
        if entry.tombstone {
            return Ok(None);
        }
        if entry.val.len() > val_buf.len() {
            return Err(Error::BufferTooSmall {
                need: entry.val.len(),
            });
        }
        val_buf[..entry.val.len()].copy_from_slice(entry.val);
        Ok(Some(entry.val.len()))
    }

    /// Makes all staged WAL data durable. In v0.1 every mutation already
    /// commits, so this is idempotent; it exists for the v0.2 flush protocol.
    ///
    /// # Errors
    ///
    /// [`Error::NoSpace`] or [`Error::Device`].
    pub async fn flush(&mut self) -> Result<(), Error<D::Error>> {
        self.wal.commit().await
    }
}
