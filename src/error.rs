//! The crate-wide error type. No strings, no allocation.

/// Every failure the crate can report.
///
/// `E` is the error type of the caller's [`BlockDevice`](crate::BlockDevice)
/// implementation and is passed through untouched in [`Error::Device`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error<E> {
    /// Key longer than the table's `KEY_MAX`.
    KeyTooLarge {
        /// Length of the rejected key in bytes.
        len: usize,
        /// The configured maximum.
        max: usize,
    },
    /// Value longer than the table's `VAL_MAX`.
    ValueTooLarge {
        /// Length of the rejected value in bytes.
        len: usize,
        /// The configured maximum.
        max: usize,
    },
    /// Keys must be non-empty.
    EmptyKey,
    /// All memtable slots are in use; the caller should flush (v0.2+).
    TableFull,
    /// The memtable arena is out of bytes; the caller should flush (v0.2+).
    ArenaFull,
    /// The caller's output buffer is too small; `need` bytes are required.
    /// Nothing was truncated.
    BufferTooSmall {
        /// Required buffer length in bytes.
        need: usize,
    },
    /// A block buffer did not match the device's `BLOCK` size.
    BadBufferLen,
    /// A stored block failed its integrity check.
    CorruptBlock {
        /// Block id that failed verification.
        id: u64,
    },
    /// The WAL is inconsistent in a way that is not a clean torn tail.
    CorruptWal {
        /// Block offset where the inconsistency was found.
        offset: u64,
    },
    /// No manifest slot carried a valid CRC.
    CorruptManifest,
    /// Out of addressable blocks (WAL region exhausted, oversize record, …).
    NoSpace,
    /// A [`WriteBatch`](crate::WriteBatch) already holds `OPS` operations.
    BatchFull,
    /// A [`WriteBatch`](crate::WriteBatch) does not fit one WAL block, so
    /// it cannot commit atomically. Split it into smaller batches.
    BatchTooLarge {
        /// Total encoded size of the batch in bytes.
        bytes: usize,
        /// The WAL block size: the atomicity ceiling.
        max: usize,
    },
    /// Archiving a table was refused: removing it would resurrect a value
    /// that is currently shadowed by one of the table's tombstones in some
    /// live view (live read or a registered snapshot). The database is
    /// unchanged; remove or outlive the shadowing value first.
    WouldResurrect {
        /// Id of the table whose archival was refused.
        table: u32,
    },
    /// An ingest was refused: a table with the same id is already
    /// attached but its descriptor differs from the one being ingested.
    IngestConflict {
        /// The conflicting table id.
        id: u32,
    },
    /// The database handle was used before a successful
    /// [`Db::open`](crate::Db::open). Nothing was read or written: call
    /// `open()` first. (Writing before recovery would append over live WAL
    /// blocks and destroy acknowledged mutations.)
    NotOpen,
    /// The underlying block device reported an error.
    Device(E),
}
