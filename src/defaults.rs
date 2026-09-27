//! The values [`db_types!`](crate::db_types) uses for tuning parameters
//! you leave out. The macro's documentation explains each parameter and
//! how to choose it.

/// [`db_types!`](crate::db_types)'s default `memtable_entries`.
pub const MEMTABLE_ENTRIES: usize = 64;
/// [`db_types!`](crate::db_types)'s default `levels`.
pub const LEVELS: usize = 4;
/// [`db_types!`](crate::db_types)'s default `tables_per_level`.
pub const TABLES_PER_LEVEL: usize = 4;
/// [`db_types!`](crate::db_types)'s default `bloom_bytes`.
pub const BLOOM_BYTES: usize = 256;
/// [`db_types!`](crate::db_types)'s default `cache_blocks` (off).
pub const CACHE_BLOCKS: usize = 0;

/// [`db_types!`](crate::db_types)'s default `memtable_arena`: 128 bytes
/// per memtable entry, and never less than one largest entry
/// (`key_max + val_max`), so a maximal write into an empty memtable fits.
#[must_use]
pub const fn memtable_arena(entries: usize, key_max: usize, val_max: usize) -> usize {
    let per_entry = entries.saturating_mul(128);
    let largest = key_max.saturating_add(val_max);
    if per_entry > largest {
        per_entry
    } else {
        largest
    }
}
