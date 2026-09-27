//! Budget profiles: named const-generic instantiations with measured RAM.
//!
//! Every profile below is caller-owned static memory — nothing is allocated,
//! so "RAM" here is `size_of` of the structs, measured on the host (same
//! layout on xtensa; see `BUDGET.md` for the accounting). The device's own
//! storage (e.g. a RAM disk or flash region) is extra.

crate::db_types! {
    block: 4096,
    key_max: 32,
    val_max: 64,
    memtable_entries: 16,
    memtable_arena: 2048,
    levels: 4,
    tables_per_level: 4,
    bloom_bytes: 64,
    cache_blocks: 2;

    /// ESP32-S3 profile: the [`Db`](crate::Db) instantiation for 4 KiB SPI-flash sectors.
    ///
    /// Keys to 32 bytes, values to 64, a 16-entry memtable over a 2 KiB arena,
    /// 4 levels of up to 4 tables each, a 2-slot block cache. `BLOCK = 4096`
    /// is the SPI flash sector size, required by
    /// [`FlashBlockDevice`](crate::flash::FlashBlockDevice).
    ///
    /// Static RAM is measured, not estimated: `tests/profile.rs` prints the
    /// `size_of` of [`Db`](crate::Db), [`Scan`](crate::Scan), and
    /// [`Compaction`](crate::Compaction) for this profile and
    /// asserts their sum stays under [`ESP32S3_RAM_BUDGET`]. `BUDGET.md`
    /// records the current numbers and what each buffer is for.
    pub type Db = Esp32S3Db;

    /// [`Scan`](crate::Scan) instantiated for the ESP32-S3 profile.
    pub type Scan = Esp32S3Scan;

    /// [`RevScan`](crate::RevScan) instantiated for the ESP32-S3 profile.
    pub type RevScan = Esp32S3RevScan;

    /// [`Compaction`](crate::Compaction) scratch instantiated for the
    /// ESP32-S3 profile.
    pub type Compaction = Esp32S3Compaction;
}

/// Static RAM budget for the ESP32-S3 profile.
///
/// Covers one [`Db`](crate::Db), one [`Scan`](crate::Scan), and one
/// [`Compaction`](crate::Compaction) scratch, **plus the largest set of
/// futures that can be live at once** (an executor stores them: a static
/// task arena, or a poll loop's stack) — the largest `&mut self` call's
/// future, or a `get` alongside a scan step. `tests/profile.rs` measures
/// every public future and asserts the total stays under this; growth
/// past it fails loudly so the re-tune is a conscious decision, not
/// silent bloat.
///
/// History: 64 KiB until v0.13; 96 KiB for block compression's ~20 KiB of
/// caller-owned scratch; 112 KiB once the futures were counted (F8 in
/// `docs/ARCHITECTURE_REVIEW.md` — `flush` alone is ~17 KiB, most of it
/// its table writer and compression scratch). The S3's 512 KiB SRAM still
/// leaves ample room for the kernel, stacks, and drivers.
pub const ESP32S3_RAM_BUDGET: usize = 114_688;
