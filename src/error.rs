//! The crate-wide error type. No strings, no allocation.

/// Every failure the crate can report.
///
/// `E` is the error type of the caller's [`BlockDevice`](crate::BlockDevice)
/// implementation and is passed through untouched in [`Error::Device`].
///
/// New variants can come in a minor release, so a `match` needs a `_` arm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
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
    /// no job can free a slot). Nothing was written. **Remedy:** delete
    /// data or set [`purge_before`](crate::Compaction::purge_before), then
    /// call [`request_compaction`](crate::Db::request_compaction) and
    /// compact; archive tables; or configure a larger table region. Reads
    /// keep working.
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
    /// A stamp was refused: the table is already stamped with a different
    /// origin. Table ids are never reused, so a conflicting re-stamp is a
    /// caller bug — the stamper disagrees with itself about when (or as
    /// whom) the table sealed. Nothing was read or written.
    StampConflict {
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
    /// The multiwriter admission ring has no free slot. No ticket and no
    /// sequence number were consumed. **Remedy:** let the drainer sweep
    /// (it frees slots as it drains), then retry.
    RingFull,
    /// A multiwriter ring payload did not decode: it was not built by one
    /// of the `drainer::payload::encode_*` constructors. The drainer is
    /// poisoned and nothing was written.
    BadPayload,
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
            Self::StampConflict { id } => Error::StampConflict { id },
            Self::BadConfig => Error::BadConfig,
            Self::Busy => Error::Busy,
            Self::NotOpen => Error::NotOpen,
            Self::RingFull => Error::RingFull,
            Self::BadPayload => Error::BadPayload,
            Self::Device(e) => match e {},
        }
    }
}

impl<E: core::fmt::Display> core::fmt::Display for Error<E> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::KeyTooLarge { len, max } => write!(f, "key is {len} bytes; the limit is {max}"),
            Self::ValueTooLarge { len, max } => {
                write!(f, "value is {len} bytes; the limit is {max}")
            }
            Self::EmptyKey => f.write_str("key is empty"),
            Self::TableFull => f.write_str("memtable is full: flush, then retry"),
            Self::ArenaFull => f.write_str("memtable arena is full: flush, then retry"),
            Self::BufferTooSmall { need } => write!(f, "buffer is too small: {need} bytes needed"),
            Self::BadBufferLen => f.write_str("block buffer length is not the device block size"),
            Self::CorruptBlock { id } => write!(f, "block {id} failed its integrity check"),
            Self::CorruptWal { offset } => write!(f, "WAL is corrupt at block offset {offset}"),
            Self::CorruptManifest => f.write_str("no manifest copy is valid"),
            Self::WalFull => f.write_str("WAL is full: flush, then retry"),
            Self::NeedsCompaction => f.write_str("compaction is needed: compact, then retry"),
            Self::RegionFull => {
                f.write_str("table region is full: delete and compact, archive, or grow it")
            }
            Self::SnapshotLimit => f.write_str("eight snapshots are live: release one"),
            Self::ManifestFull => f.write_str("manifest has no room for the change"),
            Self::TableTooLarge => f.write_str("table does not fit its block or slot budget"),
            Self::CounterExhausted => f.write_str("a sequence or id counter overflowed"),
            Self::BadLevel { level } => write!(f, "level {level} is out of range"),
            Self::BatchFull => f.write_str("write batch is full"),
            Self::BatchTooLarge { bytes, max } => {
                write!(f, "batch is {bytes} bytes; one WAL block holds {max}")
            }
            Self::WouldResurrect { table } => {
                write!(f, "archiving table {table} would bring deleted data back")
            }
            Self::IngestConflict { id } => {
                write!(f, "table {id} is attached with a different descriptor")
            }
            Self::StampConflict { id } => {
                write!(f, "table {id} is stamped with a different origin")
            }
            Self::BadConfig => f.write_str("config regions are empty, too small or overlap"),
            Self::Busy => f.write_str("another read holds the read buffers: retry"),
            Self::NotOpen => f.write_str("database is not open"),
            Self::RingFull => f.write_str("admission ring is full: let the drainer sweep"),
            Self::BadPayload => f.write_str("ring payload did not decode"),
            Self::Device(e) => write!(f, "device error: {e}"),
        }
    }
}

impl<E: core::fmt::Debug + core::fmt::Display> core::error::Error for Error<E> {}
