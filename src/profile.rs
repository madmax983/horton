//! Budget profiles: named const-generic instantiations with measured RAM.
//!
//! Every profile below is caller-owned static memory — nothing is allocated,
//! so "RAM" here is `size_of` of the structs, measured on the host (same
//! layout on xtensa; see `BUDGET.md` for the accounting). The device's own
//! storage (e.g. a RAM disk or flash region) is extra.

use crate::compact::Compaction;
use crate::{Db, Scan};

/// ESP32-S3 profile: the [`Db`] instantiation for 4 KiB SPI-flash sectors.
///
/// Keys to 32 bytes, values to 64, a 16-entry memtable over a 2 KiB arena,
/// 4 levels of up to 4 tables each, a 2-slot block cache. `BLOCK = 4096`
/// is the SPI flash sector size, required by
/// [`FlashBlockDevice`](crate::flash::FlashBlockDevice).
///
/// Static RAM is measured, not estimated: `tests/profile.rs` prints the
/// `size_of` of [`Db`], [`Scan`], and [`Compaction`] for this profile and
/// asserts their sum stays under [`ESP32S3_RAM_BUDGET`]. `BUDGET.md`
/// records the current numbers and what each buffer is for.
pub type Esp32S3Db<D> = Db<D, 4096, 32, 64, 16, 2048, 4, 4, 64, 64, 2>;

/// [`Scan`] instantiated for the ESP32-S3 profile.
pub type Esp32S3Scan<'d, D> = Scan<'d, D, 4096, 32, 64, 16, 2048, 4, 4, 64, 64, 2>;

/// [`Compaction`] scratch instantiated for the ESP32-S3 profile.
pub type Esp32S3Compaction = Compaction<4096, 32, 64, 64>;

/// Static RAM budget for the ESP32-S3 profile.
///
/// Covers one [`Db`], one [`Scan`], and one [`Compaction`] scratch.
/// `tests/profile.rs` asserts the measured total stays under this; growth
/// past it fails loudly so the re-tune is a conscious decision, not
/// silent bloat.
///
/// Raised from 64 KiB to 96 KiB for v0.13: block compression needs ~20
/// KiB of caller-owned scratch (see `BUDGET.md`), and the
/// S3's 512 KiB SRAM still leaves ample room for the kernel, stacks,
/// and drivers.
pub const ESP32S3_RAM_BUDGET: usize = 98_304;
