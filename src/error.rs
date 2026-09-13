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
    /// The underlying block device reported an error.
    Device(E),
}
