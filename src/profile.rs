//! Budget profiles: named const-generic instantiations with measured RAM.
//!
//! Every profile below is caller-owned static memory — nothing is allocated,
//! so "RAM" here is `size_of` of the structs, measured on the host (same
//! layout on xtensa; see `BUDGET.md` for the accounting). The device's own
//! storage (e.g. a RAM disk or flash region) is extra.

use crate::compact::Compaction;
use crate::{Db, Scan};

/// ESP32-S3 profile: 4 KiB blocks (the SPI flash sector size — required by
/// [`FlashBlockDevice`](crate::flash::FlashBlockDevice)), keys to 32 bytes,
/// values to 64, a 16-entry memtable over a 2 KiB arena, 4 levels of up to
/// 4 tables each.
///
/// Measured static RAM: [`Db`] 12,960 + [`Scan`] 6,296 + [`Compaction`]
/// 43,896 = 63,152 bytes, under [`ESP32S3_RAM_BUDGET`]. The compaction
/// scratch dominates (8 merge cursors × one 4 KiB block buffer each) —
/// that is the price of the bounded merge, and it is all caller-owned.
pub type Esp32S3Db<D> = Db<D, 4096, 32, 64, 16, 2048, 4, 4, 64, 64>;

/// [`Scan`] instantiated for the ESP32-S3 profile.
pub type Esp32S3Scan<'d, D> = Scan<'d, D, 4096, 32, 64, 16, 2048, 4, 4, 64, 64>;

/// [`Compaction`] scratch instantiated for the ESP32-S3 profile.
pub type Esp32S3Compaction = Compaction<4096, 32, 64, 64>;

/// Static RAM budget for the ESP32-S3 profile.
///
/// Covers one [`Db`], one [`Scan`], and one [`Compaction`] scratch.
/// `tests/profile.rs` asserts the measured total stays under this; growth
/// past it fails loudly so the re-tune is a conscious decision, not
/// silent bloat.
pub const ESP32S3_RAM_BUDGET: usize = 65_536;
