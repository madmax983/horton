//! The recorder's formats: horton's compiled-in shape, the partition
//! layout, the key schema, and the archive object a cold table is stored
//! as. Everything another reader or writer must agree on to share a
//! recorder's flash and archive, and nothing else.
//!
//! `main.rs` re-exports it, and the browser ground station and live logger
//! (`examples/ground_station`) compile this same file, so none of them can
//! drift apart. It uses only `core` and does not panic, since those are
//! `no_std` WebAssembly modules.

use horton::{Config, KeyBound, SealedTable, crc32};

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

/// An alarm event joins the frame every this many ticks.
pub const EVENT_EVERY: u64 = 250;
/// Tables holding only ticks older than this go to the archive.
pub const HOT_TICKS: u64 = 3000;

/// The first bytes of an archive object.
pub const ARCHIVE_MAGIC: &[u8; 8] = b"HRTARCH1";

/// The name a table is archived under. Table ids never repeat, so storing
/// a table again after a crash overwrites the object with the same bytes.
pub struct ObjectName([u8; 21]);

impl ObjectName {
    /// `tables/NNNNNNNNNN.hrt` for `table_id`.
    #[must_use]
    pub fn new(table_id: u32) -> Self {
        let mut name = *b"tables/0000000000.hrt";
        let mut id = table_id;
        for digit in name[7..17].iter_mut().rev() {
            *digit = b'0' + (id % 10) as u8;
            id /= 10;
        }
        Self(name)
    }

    /// The name as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        // Built from ASCII above.
        core::str::from_utf8(&self.0).unwrap_or("tables/?.hrt")
    }
}

/// Encodes an archive object's header block: the table's
/// [`SealedTable`] descriptor and a CRC. The table's blocks follow it,
/// exactly as they sat on flash.
#[must_use]
pub fn encode_archive_header(sealed: &SealedTable<KEY_MAX>) -> [u8; BLOCK] {
    let mut h = [0u8; BLOCK];
    let mut at = 0;
    let mut put = |bytes: &[u8]| {
        h[at..at + bytes.len()].copy_from_slice(bytes);
        at += bytes.len();
    };
    put(ARCHIVE_MAGIC);
    put(&sealed.id.to_le_bytes());
    put(&sealed.block_count.to_le_bytes());
    put(&sealed.max_seq.to_le_bytes());
    put(&sealed.min_seq.to_le_bytes());
    put(&sealed.entry_count.to_le_bytes());
    put(&sealed.rdel_blocks.to_le_bytes());
    for bound in [&sealed.first_key, &sealed.last_key] {
        put(&bound.len.to_le_bytes());
        put(&bound.bytes);
    }
    let crc = crc32(&h[..at]);
    h[at..at + 4].copy_from_slice(&crc.to_le_bytes());
    h
}

/// Decodes an archive object's header block, or says why it is not one.
///
/// # Errors
///
/// The block is too short, has the wrong magic, or fails its CRC.
pub fn decode_archive_header(h: &[u8]) -> Result<SealedTable<KEY_MAX>, &'static str> {
    if h.len() < BLOCK || !h.starts_with(ARCHIVE_MAGIC) {
        return Err("not an archive object");
    }
    let mut at = ARCHIVE_MAGIC.len();
    let mut take = |n: usize| {
        let s = &h[at..at + n];
        at += n;
        s
    };
    let id = u32::from_le_bytes(le(take(4)));
    let block_count = u32::from_le_bytes(le(take(4)));
    let max_seq = u64::from_le_bytes(le(take(8)));
    let min_seq = u64::from_le_bytes(le(take(8)));
    let entry_count = u32::from_le_bytes(le(take(4)));
    let rdel_blocks = u32::from_le_bytes(le(take(4)));
    let mut bound = || {
        let len = u16::from_le_bytes(le(take(2)));
        KeyBound {
            len,
            bytes: le(take(KEY_MAX)),
        }
    };
    let first_key = bound();
    let last_key = bound();
    let end = at;
    if u32::from_le_bytes(le(&h[end..end + 4])) != crc32(&h[..end]) {
        return Err("archive header CRC mismatch");
    }
    Ok(SealedTable {
        id,
        block_count,
        first_key,
        last_key,
        max_seq,
        min_seq,
        entry_count,
        rdel_blocks,
    })
}

/// Copies exactly `N` bytes out of `s`, which the callers above size to `N`.
fn le<const N: usize>(s: &[u8]) -> [u8; N] {
    let mut a = [0u8; N];
    a.copy_from_slice(&s[..N]);
    a
}
