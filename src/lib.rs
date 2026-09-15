//! horton: a zero-dependency, zero-allocation LSM-tree key-value store.
//!
//! The whole crate is `#![no_std]` + `#![forbid(unsafe_code)]` and depends on
//! nothing but `core`. All memory is caller-provided and compile-time sized
//! via const generics; every fallible operation returns [`Error`].
//!
//! v0.3 surface: [`MemTable`], the [`wal`] write-ahead log, the async
//! [`BlockDevice`] trait, [`sstable`] immutable sorted runs, the
//! [`manifest`] crash-safe root pointer, the [`alloc`] block allocator
//! (bump pointer plus free list), and the [`Db`] database (WAL + memtable +
//! flush into `SSTables`, multi-level bloom-gated reads with key-range
//! pruning and highest-sequence-wins).

#![no_std]
#![forbid(unsafe_code)]
#![warn(missing_docs)]
// horton targets single-threaded executors on `#![no_std]` systems: its
// async functions intentionally hold `&`/`&mut` across `.await` without
// requiring the device (or the futures) to be `Send`.
#![allow(clippy::future_not_send)]

pub mod alloc;
pub mod compact;
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

pub use alloc::{Bump, FreeList};
pub use compact::{Compaction, Progress, COMPACTION_KMAX};
pub use crc::crc32;
pub use db::{Config, Db, OpenReport};
pub use device::BlockDevice;
pub use error::Error;
pub use manifest::{KeyBound, Level, Manifest, TableRef, MANIFEST_MAGIC};
pub use memtable::{Entry as MemTableEntry, MemTable};
pub use scan::Scan;
pub use sstable::{
    bloom_k, bloom_maybe_contains, plan_table, write_table, Lookup as SstLookup, SstEntry,
    TablePlan, TableReader, SSTABLE_MAGIC,
};
pub use wal::{Op, RecoverState, WalWriter};
