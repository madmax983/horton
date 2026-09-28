//! The recorder's on-flash format: horton's compiled-in shape, the
//! partition layout, and the key schema. Everything a reader must agree
//! on to open a recorder's flash, and nothing else.
//!
//! `main.rs` re-exports it, and the browser ground station
//! (`examples/ground_station`) compiles this same file, so the two cannot
//! drift apart. It uses only `core`, since the ground station is `no_std`.

use horton::Config;

/// Flash sector size: horton's block.
pub const BLOCK: usize = 4096;
/// Sectors in the recorder's flash partition (2 MiB).
pub const SECTORS: usize = 512;
/// Key: an 8-byte big-endian tick, a kind byte, and a sensor or code byte.
pub const KEY_MAX: usize = 10;
/// Value: 8 bytes.
pub const VAL_MAX: usize = 8;
/// Levels of tables: the default.
pub const LEVELS: usize = horton::defaults::LEVELS;
/// Tables per level: the default.
pub const TABLES: usize = horton::defaults::TABLES_PER_LEVEL;

horton::db_types! {
    block: BLOCK,
    key_max: KEY_MAX,
    val_max: VAL_MAX,
    memtable_entries: 128,  // about 25 ticks between flushes
    memtable_arena: 4096,   // 128 entries of 10 + 8 bytes, with room to spare
    cache_blocks: 4;        // keeps the hot index blocks in RAM

    /// The recorder's database, over any block device.
    pub type Db = RecorderDb;
    /// A forward range scan.
    pub type Scan = RecorderScan;
    /// A reverse range scan.
    pub type RevScan = RecorderRevScan;
    /// Compaction scratch.
    pub type Compaction = RecorderCompaction;
}

/// Sensors per frame.
pub const SENSORS: u8 = 4;
/// Debug traces expire this many ticks after they are written.
pub const TTL_TICKS: u64 = 64;

/// The key of `kind` (`r` reading, `e` event, `d` debug) at tick `t`.
#[must_use]
pub fn key(t: u64, kind: u8, id: u8) -> [u8; KEY_MAX] {
    let mut k = [0u8; KEY_MAX];
    k[..8].copy_from_slice(&t.to_be_bytes());
    k[8] = kind;
    k[9] = id;
    k
}

/// The tick a key belongs to.
#[must_use]
pub fn tick_of(key: &[u8]) -> u64 {
    let mut t = [0u8; 8];
    let n = key.len().min(8);
    t[..n].copy_from_slice(&key[..n]);
    u64::from_be_bytes(t)
}

/// The value stored under `key`: a hash of the key, so a checker needs no
/// copy of the data to know what every value must be.
#[must_use]
pub fn value(key: &[u8]) -> [u8; VAL_MAX] {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in key {
        h = (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3);
    }
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h.to_le_bytes()
}

/// A reading's temperature, for display: 15.00 to 34.99 °C.
#[must_use]
pub fn celsius(v: &[u8]) -> f64 {
    let raw = u64::from_le_bytes(v.try_into().unwrap_or([0; 8]));
    15.0 + f64::from(u32::try_from(raw % 2000).unwrap_or(0)) / 100.0
}

/// The boot counter's key: `0xFF` sorts it after every tick.
pub const BOOT_KEY: &[u8] = b"\xFFboot";

/// The recorder's layout: the whole partition, with the manifest in a ring
/// of four copies to spread its erases.
#[must_use]
pub const fn config() -> Config {
    Config::whole_device(SECTORS as u64).with_manifest_ring(4)
}
