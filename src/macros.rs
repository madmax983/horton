//! [`db_types!`](crate::db_types): name a database shape once, with named
//! parameters, instead of repeating ten positional const generics.

/// Declares type aliases for one database shape, with every const generic
/// named.
///
/// [`Db`](crate::Db), [`Scan`](crate::Scan), [`RevScan`](crate::RevScan)
/// and [`Compaction`](crate::Compaction) take up to ten positional const
/// generics, several of them plausibly `4096`, and a swapped pair still
/// compiles. This macro takes them by name, in a fixed order — a
/// misspelled or misordered name is a compile error — and emits any of
/// the four aliases, all sharing one shape:
///
/// ```
/// horton::db_types! {
///     block: 4096,           // BLOCK: device block size in bytes
///     key_max: 32,           // KEY_MAX: longest key
///     val_max: 64,           // VAL_MAX: longest value
///     memtable_entries: 16,  // CAP: memtable slots
///     memtable_arena: 2048,  // ARENA: memtable key/value bytes
///     levels: 4,             // LEVELS
///     tables_per_level: 4,   // TABLES
///     bloom_bytes: 64,       // BLOOM_BYTES: per-table bloom filter
///     cache_blocks: 2;       // CACHE: block-cache slots (0 = off)
///
///     /// The application's database.
///     pub type Db = SensorDb;
///     pub type Scan = SensorScan;
///     pub type RevScan = SensorRevScan;
///     pub type Compaction = SensorCompaction;
/// }
///
/// // Same types as the positional spelling.
/// fn same<D: horton::BlockDevice>(
///     db: horton::Db<D, 4096, 32, 64, 16, 2048, 4, 4, 64, 2>,
/// ) -> SensorDb<D> {
///     db
/// }
/// let _: fn() -> SensorCompaction = horton::Compaction::<4096, 32, 64, 64>::new;
/// ```
///
/// The aliases are generic where the underlying type is:
/// `Db = Name` gives `Name<D>`, `Scan`/`RevScan = Name` give
/// `Name<'d, D>`, and `Compaction = Name` gives a plain `Name`. Doc
/// comments and other attributes on an alias line are kept. Parameter
/// validity (for example `BLOCK` fitting the largest WAL record) is still
/// checked at compile time by [`Db::new`](crate::Db::new).
#[macro_export]
macro_rules! db_types {
    (
        block: $block:expr,
        key_max: $key:expr,
        val_max: $val:expr,
        memtable_entries: $cap:expr,
        memtable_arena: $arena:expr,
        levels: $levels:expr,
        tables_per_level: $tables:expr,
        bloom_bytes: $bloom:expr,
        cache_blocks: $cache:expr $(,)?;
        $(
            $(#[$meta:meta])*
            $vis:vis type $kind:ident = $name:ident;
        )*
    ) => {
        $(
            $crate::__db_type_alias! {
                $kind [$(#[$meta])*] $vis $name
                ($block, $key, $val, $cap, $arena, $levels, $tables, $bloom, $cache)
            }
        )*
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
