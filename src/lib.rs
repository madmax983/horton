//! horton: a zero-dependency, zero-allocation LSM-tree key-value store.
//!
//! The whole crate is `#![no_std]` + `#![forbid(unsafe_code)]` and depends on
//! nothing but `core`. All memory is caller-provided and compile-time sized
//! via const generics; every fallible operation returns [`Error`].
//!
//! v0.1 surface: [`MemTable`], the [`wal`] write-ahead log, the async
//! [`BlockDevice`] trait, and the minimal [`Db`] (WAL + memtable).

#![no_std]
#![forbid(unsafe_code)]
#![warn(missing_docs)]
// horton targets single-threaded executors on `#![no_std]` systems: its
// async functions intentionally hold `&`/`&mut` across `.await` without
// requiring the device (or the futures) to be `Send`.
#![allow(clippy::future_not_send)]

pub mod crc;
pub mod db;
pub mod device;
pub mod error;
pub mod memtable;
pub mod wal;

pub use crc::crc32;
pub use db::{Config, Db, OpenReport};
pub use device::BlockDevice;
pub use error::Error;
pub use memtable::MemTable;
pub use wal::{Op, RecoverState, WalWriter};
