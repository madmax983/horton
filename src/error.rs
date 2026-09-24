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
    /// The WAL region has no block left for the next commit. Nothing was
    /// written. **Remedy:** [`Db::flush`](crate::Db::flush), which moves
    /// the memtable into a table and wraps the WAL, then retry.
    WalFull,
    /// Level 0 is full, or no table slot is free until compaction merges
    /// tables. Nothing was written. **Remedy:** run
    /// [`Db::compact_step`](crate::Db::compact_step) until it returns
    /// [`Progress::Done`](crate::Progress::Done), then retry.
    NeedsCompaction,
    /// The table region is full: no slot is free and compaction cannot
    /// free one (`compact_step` reports this when a level wants a job but
    /// no job fits the free slots). Nothing was written. **Remedy:** delete
    /// data and compact, archive tables, or configure a larger table
    /// region. Reads keep working.
    RegionFull,
    /// All eight snapshot slots are in use. **Remedy:** release a snapshot
    /// with [`Db::release_snapshot`](crate::Db::release_snapshot).
    SnapshotLimit,
    /// A [`Manifest`](crate::Manifest) has no room for the change: its
    /// level or table pool is full, or a one-block encode was asked of a
    /// manifest that spans several blocks. [`Db`](crate::Db) checks
    /// capacity before it changes the manifest, so through `Db` this is an
    /// internal invariant violation.
    ManifestFull,
    /// A table does not fit where it must go: one entry is larger than a
    /// data block, the index or range-tombstone section outgrows its
    /// budget, the table (for example an ingested one) is larger than a
    /// table slot, or a writer reached its block limit. Nothing became
    /// visible. **Remedy:** smaller entries, fewer range tombstones per
    /// table, or larger blocks or slots.
    TableTooLarge,
    /// A counter overflowed: the write sequence, a table id, or the
    /// manifest sequence. Unreachable in practice (it takes 2^32 tables
    /// or 2^64 writes).
    CounterExhausted,
    /// A level index is out of range: it must be below `LEVELS`.
    BadLevel {
        /// The rejected level.
        level: usize,
    },
    /// A [`WriteBatch`](crate::WriteBatch) already holds `OPS` operations.
    BatchFull,
    /// A [`WriteBatch`](crate::WriteBatch), or a single WAL record, does
    /// not fit one WAL block, so it cannot commit atomically. Split it into
    /// smaller batches.
    BatchTooLarge {
        /// Total encoded size of the batch or record in bytes.
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
    /// The [`Config`](crate::Config) cannot work: a region is empty, or the
    /// WAL, the table region, and the manifest copies overlap (each copy
    /// spans [`Manifest::max_blocks`](crate::Manifest::max_blocks) blocks).
    /// Nothing was read or written.
    BadConfig,
    /// Another point read on this [`Db`](crate::Db) holds the shared read
    /// buffers: two `get` futures were polled concurrently on one handle.
    /// Nothing was read. **Remedy:** finish the other read, then retry —
    /// or read through a [`Scan`](crate::Scan), which owns its buffers.
    Busy,
    /// The database handle was used before a successful
    /// [`Db::open`](crate::Db::open). Nothing was read or written: call
    /// `open()` first. (Writing before recovery would append over live WAL
    /// blocks and destroy acknowledged mutations.)
    NotOpen,
    /// The underlying block device reported an error.
    Device(E),
}

impl Error<core::convert::Infallible> {
    /// Widens a device-free error (from [`WriteBatch`](crate::WriteBatch)
    /// or [`MemTable`](crate::MemTable), which never touch a device) to
    /// any device's error type, so it can flow into a `Db` result:
    /// `batch.put(k, v).map_err(Error::widen)?`.
    #[must_use]
    pub const fn widen<E>(self) -> Error<E> {
        match self {
            Self::KeyTooLarge { len, max } => Error::KeyTooLarge { len, max },
            Self::ValueTooLarge { len, max } => Error::ValueTooLarge { len, max },
            Self::EmptyKey => Error::EmptyKey,
            Self::TableFull => Error::TableFull,
            Self::ArenaFull => Error::ArenaFull,
            Self::BufferTooSmall { need } => Error::BufferTooSmall { need },
            Self::BadBufferLen => Error::BadBufferLen,
            Self::CorruptBlock { id } => Error::CorruptBlock { id },
            Self::CorruptWal { offset } => Error::CorruptWal { offset },
            Self::CorruptManifest => Error::CorruptManifest,
            Self::WalFull => Error::WalFull,
            Self::NeedsCompaction => Error::NeedsCompaction,
            Self::RegionFull => Error::RegionFull,
            Self::SnapshotLimit => Error::SnapshotLimit,
            Self::ManifestFull => Error::ManifestFull,
            Self::TableTooLarge => Error::TableTooLarge,
            Self::CounterExhausted => Error::CounterExhausted,
            Self::BadLevel { level } => Error::BadLevel { level },
            Self::BatchFull => Error::BatchFull,
            Self::BatchTooLarge { bytes, max } => Error::BatchTooLarge { bytes, max },
            Self::WouldResurrect { table } => Error::WouldResurrect { table },
            Self::IngestConflict { id } => Error::IngestConflict { id },
            Self::BadConfig => Error::BadConfig,
            Self::Busy => Error::Busy,
            Self::NotOpen => Error::NotOpen,
            Self::Device(e) => match e {},
        }
    }
}
