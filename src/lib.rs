//! horton: a zero-dependency, zero-allocation LSM-tree key-value store.
//!
//! The whole crate is `#![no_std]` + `#![forbid(unsafe_code)]` and depends on
//! nothing but `core`. All memory is caller-provided and compile-time sized
//! via const generics; every fallible operation returns [`Error`].
//!
//! v0.16 surface: [`MemTable`], the [`wal`] write-ahead log, the async
//! [`BlockDevice`] trait, [`sstable`] immutable sorted runs, the
//! [`manifest`] crash-safe root pointer, the [`alloc`] table-slot
//! allocator (one table per fixed slot), the [`Db`] database (WAL + memtable +
//! flush into `SSTables`, multi-level bloom-gated reads with key-range
//! pruning and highest-sequence-wins), the [`Scan`] merge iterator with
//! snapshot reads and reverse iteration, atomic multi-op [`WriteBatch`]
//! writes via [`Db::write`](Db::write), the archive API
//! ([`Db::archive_plan`], [`Db::archive_commit`], [`ArchivePlan`]) — seal
//! a table, stream its blocks to caller-owned remote storage through
//! [`Db::device`], then forget it locally — hand-rolled LZ77 block
//! compression ([`compress`]): the writer trial-compresses every data
//! block and keeps the compressed form when it saves at least
//! [`compress::COMPRESS_MIN_SAVING`] bytes; every read path decompresses
//! transparently — range deletes ([`Db::delete_range`](Db::delete_range))
//! with per-range tombstone sections in every table, and absolute-tick
//! TTLs ([`Db::put_with_ttl`](Db::put_with_ttl)) with read-time expiry
//! and compaction-time purge — and the caller-owned block cache
//! ([`cache`]): a fixed-capacity CLOCK cache over inline storage, sized
//! by the `CACHE` const generic on [`Db`] (`0` disables it), caching
//! physical device-block images before CRC, bloom, decompression, TTL,
//! and range-delete interpretation, with explicit invalidation when
//! compaction or archive removal retires tables. See SPEC §9 for the
//! tombstone rule: delete-bearing workloads archive only from the
//! bottommost level.

#![no_std]
#![forbid(unsafe_code)]
#![warn(missing_docs)]
// horton targets single-threaded executors on `#![no_std]` systems: its
// async functions intentionally hold `&`/`&mut` across `.await` without
// requiring the device (or the futures) to be `Send`.
#![allow(clippy::future_not_send)]

pub mod alloc;
pub mod batch;
pub mod cache;
pub mod compact;
pub mod compress;
pub mod crc;
pub mod db;
pub mod device;
pub mod error;
pub mod esp32s3;
pub mod flash;
pub mod manifest;
pub mod memtable;
pub mod model;
pub mod profile;
pub mod scan;
pub mod sstable;
pub mod wal;

pub use alloc::{MAX_SLOTS, SlotMap};
pub use batch::WriteBatch;
pub use cache::{BlockCache, CachePort, CacheStats};
pub use compact::{COMPACTION_KMAX, Compaction, Progress};
pub use crc::crc32;
pub use db::{ArchivePlan, Config, Db, OpenReport, SealedTable, SlotStats};
pub use device::BlockDevice;
pub use error::Error;
pub use manifest::{KeyBound, MANIFEST_MAGIC, Manifest, ManifestLayout, TableRef};
pub use memtable::{Entry as MemTableEntry, MemTable};
pub use scan::{RevScan, Scan};
pub use sstable::{
    Lookup as SstLookup, SSTABLE_MAGIC, SstEntry, TablePlan, TableReader, bloom_k,
    bloom_maybe_contains, plan_table, write_table,
};
pub use wal::{Op, RecoverState, WalWriter};
