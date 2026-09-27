//! [`db_types!`](crate::db_types): name a database shape once, with named
//! parameters, instead of repeating ten positional const generics.

/// Declares the types for one database shape: [`Db`](crate::Db),
/// [`Scan`](crate::Scan), [`RevScan`](crate::RevScan) and
/// [`Compaction`](crate::Compaction) with every size filled in.
///
/// horton allocates nothing, so every buffer size is fixed when you
/// compile. This macro is where you choose those sizes. Only three are
/// required; the rest are tuning knobs with defaults that suit most uses:
///
/// ```
/// horton::db_types! {
///     block: 4096, // bytes per device block (your flash sector size)
///     key_max: 32, // longest key you will store, in bytes
///     val_max: 64; // longest value you will store, in bytes
///
///     /// The application's database.
///     pub type Db = SensorDb;
///     pub type Compaction = SensorCompaction;
/// }
/// ```
///
/// # The parameters
///
/// horton keeps new writes in RAM (the *memtable*) and in a log on the
/// device (the *WAL*, for crash safety). [`Db::flush`](crate::Db::flush)
/// writes the memtable to the device as one sorted, read-only *table*.
/// Tables are grouped into *levels*, and compaction merges them so reads
/// stay fast and deleted data frees its space.
///
/// | Parameter | What it is | How to choose | Default |
/// |---|---|---|---|
/// | `block` | Bytes the device reads or writes at once. | Your [`BlockDevice::BLOCK`](crate::BlockDevice::BLOCK): the erase sector on NOR flash (usually 4096), 512 on SD cards. At least 512, and `key_max + val_max + 23` must fit. | required |
/// | `key_max` | Longest key, in bytes. | Longer keys fail with `KeyTooLarge`. RAM buffers are sized for it, so do not round up much. | required |
/// | `val_max` | Longest value, in bytes. | Longer values fail with `ValueTooLarge`. | required |
/// | `memtable_entries` | Writes held in RAM before they must be flushed. Every `put` or `delete` takes one until the next flush. | More means fewer flushes and less flash wear, but more RAM. When full, writes fail with `TableFull`: call `flush()`. | 64 |
/// | `memtable_arena` | RAM bytes for those writes' keys and values. | About `memtable_entries` × your typical key + value size. When full, writes fail with `ArenaFull`: call `flush()`. | 128 per entry, and at least `key_max + val_max` |
/// | `levels` | Levels of tables on the device. | 4 suits most uses. | 4 |
/// | `tables_per_level` | Tables each level holds. | Below 8. `levels × tables_per_level` (at most 64) is how many tables exist; the table region is split into that many equal slots. More tables mean more data between compactions, more tables to check per read, and smaller slots. | 4 |
/// | `bloom_bytes` | Per-table filter that lets a read skip tables that cannot hold the key. | About 1.25 bytes per key in a table gives about 1% wasted reads. More costs device space and a little compaction RAM. | 256 |
/// | `cache_blocks` | Recently read blocks kept in RAM, `block` bytes each. | 0 turns it off. A few blocks speed up repeated reads. | 0 |
///
/// Where the data goes on the device is a separate choice:
/// [`Config::whole_device`](crate::Config::whole_device) lays the regions
/// out for you. To see what a shape costs in RAM, print
/// `core::mem::size_of::<SensorDb<YourDevice>>()` (and the same for
/// `Scan` and `Compaction`), or see `BUDGET.md` for a worked profile.
///
/// # Rules
///
/// Parameters are named and come in the order of the table above; an
/// omitted one takes its default. A misspelled, repeated or misordered
/// name is a compile error, so two sizes can never be swapped by accident:
///
/// ```compile_fail
/// horton::db_types! {
///     block: 4096,
///     key_max: 32,
///     val_max: 64,
///     levels: 4,
///     memtable_entries: 16; // must come before `levels`
///     type Db = Misordered;
/// }
/// ```
///
/// ```compile_fail
/// horton::db_types! {
///     block: 4096,
///     key_max: 32; // `val_max` is required
///     type Db = Missing;
/// }
/// ```
///
/// Each alias line is `type Kind = Name;` with an optional visibility and
/// doc comments, which are kept. The aliases are generic where the
/// underlying type is: `Db = Name` gives `Name<D>` for any device `D`,
/// `Scan`/`RevScan = Name` give `Name<'d, D>`, and `Compaction = Name`
/// gives a plain `Name`. Every alias is the same type as the positional
/// spelling:
///
/// ```
/// horton::db_types! {
///     block: 4096,
///     key_max: 32,
///     val_max: 64,
///     memtable_entries: 16,
///     memtable_arena: 2048,
///     levels: 4,
///     tables_per_level: 4,
///     bloom_bytes: 64,
///     cache_blocks: 2;
///
///     pub type Db = SensorDb;
///     pub type Scan = SensorScan;
///     pub type RevScan = SensorRevScan;
///     pub type Compaction = SensorCompaction;
/// }
///
/// fn same<D: horton::BlockDevice>(
///     db: horton::Db<D, 4096, 32, 64, 16, 2048, 4, 4, 64, 2>,
/// ) -> SensorDb<D> {
///     db
/// }
/// let _: fn() -> SensorCompaction = horton::Compaction::<4096, 32, 64, 64>::new;
/// ```
///
/// The remaining rules (for example that `block` fits the largest WAL
/// record) are checked at compile time by [`Db::new`](crate::Db::new).
#[macro_export]
macro_rules! db_types {
    (
        block: $block:expr,
        key_max: $key:expr,
        val_max: $val:expr
        $(, memtable_entries: $cap:expr)?
        $(, memtable_arena: $arena:expr)?
        $(, levels: $levels:expr)?
        $(, tables_per_level: $tables:expr)?
        $(, bloom_bytes: $bloom:expr)?
        $(, cache_blocks: $cache:expr)?
        $(,)?;
        $($aliases:tt)*
    ) => {
        $crate::__db_types_resolved! {
            (
                $block,
                $key,
                $val,
                $crate::__db_or!([$($cap)?] $crate::defaults::MEMTABLE_ENTRIES),
                $crate::__db_or!(
                    [$($arena)?]
                    $crate::defaults::memtable_arena(
                        $crate::__db_or!([$($cap)?] $crate::defaults::MEMTABLE_ENTRIES),
                        $key,
                        $val,
                    )
                ),
                $crate::__db_or!([$($levels)?] $crate::defaults::LEVELS),
                $crate::__db_or!([$($tables)?] $crate::defaults::TABLES_PER_LEVEL),
                $crate::__db_or!([$($bloom)?] $crate::defaults::BLOOM_BYTES),
                $crate::__db_or!([$($cache)?] $crate::defaults::CACHE_BLOCKS)
            )
            $($aliases)*
        }
    };
}

/// Emits [`db_types!`](crate::db_types)'s aliases once every parameter
/// has a value. Not public API.
#[doc(hidden)]
#[macro_export]
macro_rules! __db_types_resolved {
    (
        ($b:expr, $k:expr, $v:expr, $cap:expr, $a:expr, $l:expr, $t:expr, $bl:expr, $c:expr)
        $(
            $(#[$meta:meta])*
            $vis:vis type $kind:ident = $name:ident;
        )*
    ) => {
        $(
            $crate::__db_type_alias! {
                $kind [$(#[$meta])*] $vis $name ($b, $k, $v, $cap, $a, $l, $t, $bl, $c)
            }
        )*
    };
}

/// The value given to [`db_types!`](crate::db_types), or the default.
/// Not public API.
#[doc(hidden)]
#[macro_export]
macro_rules! __db_or {
    ([] $default:expr) => {
        $default
    };
    ([$value:expr] $default:expr) => {
        $value
    };
}

/// Emits one alias for [`db_types!`](crate::db_types). Not public API.
#[doc(hidden)]
#[macro_export]
macro_rules! __db_type_alias {
    (Db [$(#[$meta:meta])*] $vis:vis $name:ident
        ($b:expr, $k:expr, $v:expr, $cap:expr, $a:expr, $l:expr, $t:expr, $bl:expr, $c:expr)) => {
        $(#[$meta])*
        $vis type $name<D> =
            $crate::Db<D, { $b }, { $k }, { $v }, { $cap }, { $a }, { $l }, { $t }, { $bl }, { $c }>;
    };
    (Scan [$(#[$meta:meta])*] $vis:vis $name:ident
        ($b:expr, $k:expr, $v:expr, $cap:expr, $a:expr, $l:expr, $t:expr, $bl:expr, $c:expr)) => {
        $(#[$meta])*
        $vis type $name<'d, D> = $crate::Scan<
            'd, D, { $b }, { $k }, { $v }, { $cap }, { $a }, { $l }, { $t }, { $bl }, { $c },
        >;
    };
    (RevScan [$(#[$meta:meta])*] $vis:vis $name:ident
        ($b:expr, $k:expr, $v:expr, $cap:expr, $a:expr, $l:expr, $t:expr, $bl:expr, $c:expr)) => {
        $(#[$meta])*
        $vis type $name<'d, D> = $crate::RevScan<
            'd, D, { $b }, { $k }, { $v }, { $cap }, { $a }, { $l }, { $t }, { $bl }, { $c },
        >;
    };
    (Compaction [$(#[$meta:meta])*] $vis:vis $name:ident
        ($b:expr, $k:expr, $v:expr, $cap:expr, $a:expr, $l:expr, $t:expr, $bl:expr, $c:expr)) => {
        $(#[$meta])*
        $vis type $name = $crate::Compaction<{ $b }, { $k }, { $v }, { $bl }>;
    };
}
