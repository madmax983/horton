//! kvstore: horton as a host key-value store, the way `LevelDB` or
//! `RocksDB` is used: a file on disk, a handle shared by many threads, and
//! compaction that runs by itself.
//!
//! ```text
//! cargo run --release --example kvstore -- demo              # a tour of the API
//! cargo run --release --example kvstore -- put hello world   # a command-line store, like `ldb`
//! cargo run --release --example kvstore -- get hello
//! cargo run --release --example kvstore -- scan --limit 10
//! cargo run --release --example kvstore -- bench             # db_bench's workloads
//! cargo run --release --example kvstore -- crash 20          # kill -9 the writer 20 times; nothing acknowledged is lost
//! ```
//!
//! The store is one file (default `target/kvstore/kv.db`, `--db PATH`),
//! created sparse at `--size-mb` (default 256) and horton's device from
//! then on. One process at a time opens it: a second opener gets an error
//! (ADR-0032). `store.rs` turns horton's single-owner `Db` into a shared
//! handle with group commit and background compaction; `device.rs` is the
//! file-backed block device; `bench.rs` and `crash.rs` are the benchmark
//! and the crash test. `vs-rocksdb.sh` runs the benchmark against
//! `RocksDB`'s `db_bench`; `docs/THROUGHPUT.md` reads the result.

mod bench;
mod cache;
mod crash;
mod device;
mod gate;
mod handoff;
mod store;

use core::future::Future;
use core::pin::pin;
use core::task::{Context, Poll, Waker};
use std::path::PathBuf;
use std::time::Instant;

use store::{Batch, Handle, Options, Store, StoreError};

/// Bytes per block: horton's unit of I/O.
///
/// Bigger than a flash sector because a host wants bigger tables: a
/// table's index is one block, so the block size bounds how much one
/// table can hold (about 7 MiB of 16-byte keys here), and there are at
/// most `LEVELS × TABLES` tables.
pub const BLOCK: usize = 16 * 1024;
/// Longest key.
pub const KEY_MAX: usize = 128;
/// Longest value.
pub const VAL_MAX: usize = 4096;
/// Levels of tables.
pub const LEVELS: usize = 4;
/// Tables per level.
pub const TABLES_PER_LEVEL: usize = 7;
/// The most bloom filter a table carries: sixteen blocks of 64-byte
/// buckets, about 2 million bits. A table gets as many of those blocks
/// as its entry count calls for (`horton::BloomShape`).
pub const BLOOM_BYTES: usize = 16 * ((BLOCK - 4) / 64) * 64;
/// Writes the memtable holds before it is flushed to a table.
///
/// As many `db_bench` writes (16-byte keys, 100-byte values) as the
/// arena holds, about 950 KB of it. At 4,096 the slots filled at half
/// the arena, and the store flushed twice as often as a 1 MiB memtable
/// does elsewhere (docs/THROUGHPUT.md, step 20).
pub const MEMTABLE_ENTRIES: usize = 8192;
/// Bytes of keys and values the memtable holds.
pub const MEMTABLE_ARENA: usize = 1 << 20;

// The shape of the store: 4 levels of up to 7 tables, so at most 28
// tables of about 7 MiB, some 200 MiB of tables (a 256 MiB
// file gives each table an 8 MiB slot). The memtable (8192 writes, 1 MiB)
// is what one flush turns into a table. A point read probes up to 7
// tables in level 0 and one per level below.
//
// A table's bloom filter is sized to it, 10 bits a key: one 16 KiB block
// for the 8192 entries of a flush, up to sixteen for the 100,000 and
// more of a full table at the bottom. The writer keeps the largest
// filter in RAM while it writes (`BLOOM_BYTES`, 255 KiB), which is what
// lets a filter span blocks at all.
//
// The block cache is the store's own (`cache::SharedCache`, 16 MiB;
// LevelDB's default is 8, RocksDB's 32): point reads and scans both run
// outside the `Db`, on their callers' threads, and share it. The cache
// inside the `Db` would serve only reads made through the `Db` itself,
// and the store makes none, so it is switched off.
horton::db_types! {
    block: BLOCK,
    key_max: KEY_MAX,
    val_max: VAL_MAX,
    memtable_entries: MEMTABLE_ENTRIES,
    memtable_arena: MEMTABLE_ARENA,
    levels: LEVELS,
    tables_per_level: TABLES_PER_LEVEL,
    bloom_bytes: BLOOM_BYTES,
    cache_blocks: 0;

    /// The store's database, over any device.
    pub type Db = KvDb;
    pub type Scan = KvScan;
    pub type RevScan = KvRevScan;
    pub type Compaction = KvCompaction;
    /// The tables of the store, read without the `Db` (a detached read).
    pub type TableView = KvTables;
    /// A point read between its memtable probe and its table phase.
    pub type MemProbe = KvProbe;
    /// A reader thread's block buffers.
    pub type ReadScratch = KvReadScratch;
}

/// A copy of part of the memtable, for one page of a detached scan: a
/// memtable of the database's own shape, so any slice of it fits.
pub type KvMemPage = horton::MemTable<MEMTABLE_ENTRIES, MEMTABLE_ARENA, KEY_MAX, VAL_MAX>;

/// The manifest: which tables exist, level by level.
pub type KvManifest = horton::Manifest<LEVELS, TABLES_PER_LEVEL, KEY_MAX>;

/// horton ships no executor. The file device completes every call before
/// returning, so every future is ready the first time it is polled.
pub fn block_on<F: Future>(fut: F) -> F::Output {
    let mut fut = pin!(fut);
    let mut cx = Context::from_waker(Waker::noop());
    loop {
        if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
            return v;
        }
    }
}

/// Options every command takes.
pub struct Common {
    pub db: PathBuf,
    pub size_mb: u64,
    pub sync: bool,
}

impl Common {
    #[must_use]
    pub const fn options(&self) -> Options {
        Options {
            // Saturates: a size too large to make fails at the open.
            create_blocks: self.size_mb.saturating_mul((1 << 20) / BLOCK as u64),
            sync: self.sync,
        }
    }

    /// Opens the store, creating its directory if needed.
    ///
    /// # Errors
    ///
    /// The directory or the store cannot be opened. A file too small for
    /// the shape gives the `--size-mb` to use.
    pub fn open(&self) -> Result<Store, String> {
        if let Some(dir) = self.db.parent() {
            std::fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
        }
        Store::open(&self.db, self.options())
            .map(|(s, _)| s)
            .map_err(|e| match e {
                StoreError::TooSmall { min, .. } => format!(
                    "{e}. For a new store, use --size-mb {} or more",
                    store::mib_for(min)
                ),
                e => e.to_string(),
            })
    }
}

const USAGE: &str = "usage: kvstore [--db PATH] [--size-mb N] [--no-sync] COMMAND

commands:
  demo                          a tour: batches, snapshots, iterators, reopen
  put KEY VALUE                 store a value
  get KEY                       print a value
  del KEY                       delete a key
  delrange START END            delete every key in [START, END)
  scan [START [END]] [--reverse] [--limit N]
                                print the keys in [START, END)
  compact                       flush, then compact every table once
  stats                         the layout, the levels and the counters
  bench [--num N] [--value-size N] [--threads N] [--read-threads N]
        [--batch N] [--benchmarks a,b,...]
                                db_bench's workloads on a fresh store
  crash [N] [--threads N] [--seed N] [--sync]
                                kill -9 a writing process N times (default
                                20), reopen, and check every acknowledged
                                write is there and nothing else is

--no-sync skips fdatasync, like LevelDB's default WriteOptions: a write
survives the process dying but not the machine losing power.";

fn main() {
    let mut common = Common {
        db: PathBuf::from("target/kvstore/kv.db"),
        size_mb: 256,
        sync: true,
    };
    let mut rest = Vec::new();
    let mut args = std::env::args().skip(1);
    let bad = |e: String| -> ! {
        if !e.is_empty() {
            eprintln!("{e}\n");
        }
        eprintln!("{USAGE}");
        std::process::exit(2);
    };
    while let Some(a) = args.next() {
        match a.as_str() {
            "--db" => {
                common.db = args
                    .next()
                    .map_or_else(|| bad("--db needs a path".into()), PathBuf::from);
            }
            "--size-mb" => {
                common.size_mb = args
                    .next()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or_else(|| bad("--size-mb needs a number".into()));
            }
            "--no-sync" => common.sync = false,
            "-h" | "--help" => bad(String::new()),
            _ => rest.push(a),
        }
    }
    let Some(cmd) = rest.first().cloned() else {
        bad(String::new())
    };
    let args = &rest[1..];
    let result = match cmd.as_str() {
        "demo" => demo(&common),
        "bench" => bench::run(&common, args),
        "crash" => crash::run(&common, args),
        crash::CHILD => crash::child(&common, args),
        _ => cli(&common, &cmd, args),
    };
    if let Err(e) = result {
        if e.starts_with("usage") {
            bad(e.trim_start_matches("usage").trim().to_string());
        }
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

/// One `ldb`-style command, parsed before the store opens.
#[derive(Debug, PartialEq, Eq)]
enum Cmd<'a> {
    Put(&'a [u8], &'a [u8]),
    Get(&'a [u8]),
    Del(&'a [u8]),
    DelRange(&'a [u8], &'a [u8]),
    Scan {
        start: &'a [u8],
        end: Option<&'a [u8]>,
        reverse: bool,
        limit: usize,
    },
    Compact,
    Stats,
}

/// Parses a command and its arguments. It does not open a file.
///
/// # Errors
///
/// A usage error (an unknown command, or the wrong arguments), or an
/// inverted range.
fn parse<'a>(cmd: &str, args: &'a [String]) -> Result<Cmd<'a>, String> {
    let b = String::as_bytes;
    let cmd = match (cmd, args) {
        ("put", [k, v]) => Cmd::Put(b(k), b(v)),
        ("get", [k]) => Cmd::Get(b(k)),
        ("del", [k]) => Cmd::Del(b(k)),
        ("delrange", [start, end]) => Cmd::DelRange(b(start), b(end)),
        ("compact", []) => Cmd::Compact,
        ("stats", []) => Cmd::Stats,
        ("scan", _) => parse_scan(args)?,
        ("put" | "get" | "del" | "delrange" | "compact" | "stats", _) => {
            return Err(format!("usage {cmd} has the wrong number of arguments"));
        }
        (other, _) => return Err(format!("usage unknown command {other}")),
    };
    match cmd {
        Cmd::DelRange(start, end)
        | Cmd::Scan {
            start,
            end: Some(end),
            ..
        } => {
            store::check_order(start, Some(end)).map_err(|e| e.to_string())?;
        }
        _ => {}
    }
    Ok(cmd)
}

/// Parses `scan [START [END]] [--reverse] [--limit N]`.
fn parse_scan(args: &[String]) -> Result<Cmd<'_>, String> {
    let mut bounds = Vec::new();
    let mut reverse = false;
    let mut limit = usize::MAX;
    let mut it = args.iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--reverse" => reverse = true,
            "--limit" => {
                limit = it
                    .next()
                    .and_then(|s| s.parse().ok())
                    .ok_or("usage --limit needs a number")?;
            }
            _ => bounds.push(a.as_bytes()),
        }
    }
    let (start, end) = match bounds[..] {
        [] => (&b""[..], None),
        [start] => (start, None),
        [start, end] => (start, Some(end)),
        _ => return Err("usage scan has more than two bounds".into()),
    };
    Ok(Cmd::Scan {
        start,
        end,
        reverse,
        limit,
    })
}

/// The `ldb`-style commands: one operation on the store at `--db`. The
/// arguments are parsed first: a usage error opens no store.
fn cli(common: &Common, cmd: &str, args: &[String]) -> Result<(), String> {
    let cmd = parse(cmd, args)?;
    let store = common.open()?;
    let err = |e: StoreError| e.to_string();
    match cmd {
        Cmd::Put(k, v) => store.put(k, v).map_err(err),
        Cmd::Get(k) => {
            let v = store.get(k).map_err(err)?.ok_or("not found")?;
            println!("{}", String::from_utf8_lossy(&v));
            Ok(())
        }
        Cmd::Del(k) => store.delete(k).map_err(err),
        Cmd::DelRange(start, end) => store.delete_range(start, end).map_err(err),
        Cmd::Scan {
            start,
            end,
            reverse,
            limit,
        } => {
            for kv in store.range(start, end, reverse).map_err(err)?.take(limit) {
                let (k, v) = kv.map_err(err)?;
                println!(
                    "{} = {}",
                    String::from_utf8_lossy(&k),
                    String::from_utf8_lossy(&v)
                );
            }
            Ok(())
        }
        Cmd::Compact => {
            let t = Instant::now();
            store.compact().map_err(err)?;
            println!("compacted in {:.2?}", t.elapsed());
            print_stats(&store)
        }
        Cmd::Stats => print_stats(&store),
    }
}

/// A counter as a float, for printing ratios.
#[allow(clippy::cast_precision_loss, reason = "printed, not computed with")]
#[must_use]
pub const fn float(n: u64) -> f64 {
    n as f64
}

/// Prints the store's layout, levels and counters.
///
/// # Errors
///
/// The store is closed.
pub fn print_stats(store: &Handle) -> Result<(), String> {
    let s = store.stats().map_err(|e| e.to_string())?;
    let mib = |blocks: u64| float(blocks * BLOCK as u64) / float(1 << 20);
    println!(
        "device    {} blocks of {} KiB ({:.0} MiB); {} table slots of {} blocks ({:.1} MiB)",
        s.device_blocks,
        BLOCK / 1024,
        mib(s.device_blocks),
        s.slots.slots,
        s.slots.slot_blocks,
        mib(s.slots.slot_blocks)
    );
    println!(
        "tables    {} used, {} free; per level {:?}; {} blocks of bloom filter, {} of range \
         tombstones",
        s.slots.used, s.slots.free, s.level_tables, s.bloom_blocks, s.rdel_blocks
    );
    println!(
        "writes    {} client writes in {} commits since open ({:.1} per commit)",
        s.grouped_writes,
        s.groups,
        float(s.grouped_writes) / float(s.groups.max(1))
    );
    println!(
        "work      {} flushes, {} compaction jobs in {} steps, {} of them outside the engine lock",
        s.flushes, s.compaction_jobs, s.compaction_steps, s.detached_steps
    );
    println!(
        "          {} merges began while another was running",
        s.merges_side_by_side
    );
    println!(
        "reads     {} without the engine lock, {} under it, since open",
        s.reads_unlocked, s.reads_locked
    );
    println!(
        "i/o       {} block reads, {} block writes, {} syncs; cache {} hits, {} misses",
        s.io.reads, s.io.writes, s.io.syncs, s.cache_hits, s.cache_misses
    );
    Ok(())
}

/// Prints a passed check, or fails the demo.
fn check(ok: bool, what: &str) -> Result<(), String> {
    if ok {
        println!("  ok  {what}");
        Ok(())
    } else {
        Err(format!("demo check failed: {what}"))
    }
}

/// How many keys `[start, end)` holds; the iterator's first error fails.
fn count(store: &Handle, start: &[u8], end: &[u8]) -> Result<usize, String> {
    let mut n = 0;
    for kv in store
        .range(start, Some(end), false)
        .map_err(|e| e.to_string())?
    {
        kv.map_err(|e| e.to_string())?;
        n += 1;
    }
    Ok(n)
}

/// A tour of the store's API on a fresh file, each step checked.
fn demo(common: &Common) -> Result<(), String> {
    let path = common.db.with_file_name("demo.db");
    Store::destroy(&path).map_err(|e| e.to_string())?;
    let common = Common {
        db: path,
        size_mb: 128,
        sync: common.sync,
    };
    println!("open {}", common.db.display());
    let store = common.open()?;
    demo_writes(&store)?;
    demo_reads(&store)?;

    println!("\ncompact, close, and reopen:");
    store.compact().map_err(|e| e.to_string())?;
    print_stats(&store)?;
    drop(store);
    let (store, report) = Store::open(&common.db, common.options()).map_err(|e| e.to_string())?;
    println!(
        "  reopened: {} WAL records replayed, {} tables in level 0",
        report.recovered_records, report.l0_tables
    );
    check(
        store.get(b"acct/alice").map_err(|e| e.to_string())? == Some(b"70".to_vec()),
        "acct/alice = 70 after reopen",
    )?;
    check(
        count(&store, b"user/", b"user0")? == 3499,
        "3499 users after reopen",
    )?;
    drop(store);
    Store::destroy(&common.db).map_err(|e| e.to_string())?;
    println!("\ndemo passed");
    Ok(())
}

/// Concurrent writers, then an atomic batch under a snapshot.
fn demo_writes(store: &Store) -> Result<(), String> {
    let err = |e: StoreError| e.to_string();
    println!("\nwrites from 8 threads at once, committed in groups:");
    let t = Instant::now();
    std::thread::scope(|s| {
        let mut writers = Vec::new();
        for w in 0..8 {
            let h = store.handle();
            writers.push(s.spawn(move || -> Result<(), StoreError> {
                for i in 0..500 {
                    let key = format!("user/{w}/{i:04}");
                    h.put(key.as_bytes(), format!("name-{w}-{i}").as_bytes())?;
                }
                Ok(())
            }));
        }
        writers.into_iter().try_for_each(|w| {
            w.join()
                .map_err(|_| "a writer panicked".to_string())?
                .map_err(err)
        })
    })?;
    let s = store.stats().map_err(err)?;
    println!(
        "  4000 puts in {:.2?}: {} commits, {:.1} puts per commit",
        t.elapsed(),
        s.groups,
        float(s.grouped_writes) / float(s.groups.max(1))
    );
    check(
        store.get(b"user/3/0042").map_err(err)? == Some(b"name-3-42".to_vec()),
        "get user/3/0042",
    )?;

    println!("\nan atomic batch: move a balance between two accounts");
    store.put(b"acct/alice", b"100").map_err(err)?;
    store.put(b"acct/bob", b"0").map_err(err)?;
    let before = store.snapshot().map_err(err)?;
    let mut b = Batch::new();
    b.put(b"acct/alice", b"70").put(b"acct/bob", b"30");
    store.write(b).map_err(err)?;
    check(
        store.get(b"acct/bob").map_err(err)? == Some(b"30".to_vec()),
        "both halves applied",
    )?;
    check(
        store.get_at(b"acct/bob", &before).map_err(err)? == Some(b"0".to_vec()),
        "a snapshot taken before still reads bob = 0",
    )
}

/// Iterators pinned to snapshots, reverse scans, and a range delete.
fn demo_reads(store: &Store) -> Result<(), String> {
    let err = |e: StoreError| e.to_string();
    println!("\nan iterator over a prefix, pinned to a snapshot:");
    let iter = store
        .range(b"user/5/", Some(b"user/5/\xff"), false)
        .map_err(err)?;
    store.delete(b"user/5/0000").map_err(err)?;
    let mut n = 0;
    for kv in iter {
        kv.map_err(err)?;
        n += 1;
    }
    check(
        n == 500,
        "it still sees all 500 of user/5, deleted one included",
    )?;
    check(
        count(store, b"user/5/", b"user/5/\xff")? == 499,
        "a new iterator sees 499",
    )?;
    let last = store
        .scan(b"user/", Some(b"user0"), true, 1, None)
        .map_err(err)?;
    check(
        last.first().map(|kv| kv.0.as_slice()) == Some(b"user/7/0499"),
        "a reverse scan starts at user/7/0499",
    )?;

    println!("\na range delete removes a whole prefix with one record:");
    store.delete_range(b"user/2/", b"user/3/").map_err(err)?;
    check(
        count(store, b"user/", b"user0")? == 3499,
        "user/2 is gone: 3499 users left",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Options for a store path in the temporary directory that does not
    /// exist. Drop deletes the store, also after a panic.
    struct TempCommon(Common);

    impl core::ops::Deref for TempCommon {
        type Target = Common;
        fn deref(&self) -> &Common {
            &self.0
        }
    }

    impl Drop for TempCommon {
        fn drop(&mut self) {
            remove(&self.0);
        }
    }

    fn temp_common(name: &str, size_mb: u64) -> TempCommon {
        let db = std::env::temp_dir().join(format!(
            "horton-kvstore-cli-{}-{name}.db",
            std::process::id()
        ));
        let common = Common {
            db,
            size_mb,
            sync: false,
        };
        remove(&common);
        TempCommon(common)
    }

    /// Deletes a test store and its lock file.
    fn remove(common: &Common) {
        let _ = Store::destroy(&common.db);
        if let Ok(lock) = device::lock_path(&common.db) {
            let _ = std::fs::remove_file(lock);
        }
    }

    fn strings(args: &[&str]) -> Vec<String> {
        args.iter().map(|&a| a.to_string()).collect()
    }

    /// Issue #46, item 2: a usage error opens nothing, so it creates no
    /// file.
    #[test]
    fn a_usage_error_opens_nothing() {
        let common = temp_common("usage", 256);
        let cases: &[(&str, &[&str])] = &[
            ("get", &[]),
            ("get", &["a", "b"]),
            ("put", &["a"]),
            ("put", &["a", "b", "c"]),
            ("del", &[]),
            ("delrange", &["a"]),
            ("delrange", &["a", "b", "c"]),
            ("scan", &["a", "b", "c"]),
            ("scan", &["--limit"]),
            ("scan", &["--limit", "-1"]),
            ("compact", &["a"]),
            ("stats", &["a"]),
            ("nope", &[]),
        ];
        for (cmd, args) in cases {
            let e = cli(&common, cmd, &strings(args)).err();
            let created = common.db.exists();
            remove(&common);
            assert!(
                e.as_deref().is_some_and(|e| e.starts_with("usage")),
                "{cmd} {args:?}: {e:?}"
            );
            assert!(!created, "{cmd} {args:?} created the store");
        }
    }

    /// Issue #46, item 1: a size too small for the shape names the
    /// `--size-mb` to use, and leaves no sized file.
    #[test]
    fn a_small_size_names_the_size_to_use() {
        let common = temp_common("small", 8);
        let e = cli(&common, "put", &strings(&["a", "b"])).err();
        let len = std::fs::metadata(&common.db).map_or(0, |m| m.len());
        remove(&common);
        let min = KvDb::<device::FileDevice>::MIN_DEVICE_BLOCKS;
        let want = format!("--size-mb {}", store::mib_for(min));
        assert!(e.as_deref().is_some_and(|e| e.contains(&want)), "{e:?}");
        assert_eq!(len, 0, "the refused open sized the file");
    }

    /// A size in MiB too large for a block count does not overflow.
    #[test]
    fn a_huge_size_does_not_overflow() {
        let common = temp_common("huge", u64::MAX);
        assert_eq!(common.options().create_blocks, u64::MAX);
    }

    /// Issue #46, item 3: the CLI refuses an inverted range before it
    /// opens the store.
    #[test]
    fn the_cli_refuses_an_inverted_range() {
        let common = temp_common("inverted", 128);
        let del = cli(&common, "delrange", &strings(&["z", "a"])).err();
        let scan = cli(&common, "scan", &strings(&["z", "a", "--reverse"])).err();
        let created = common.db.exists();
        remove(&common);
        let want = Some(StoreError::InvertedRange.to_string());
        assert_eq!(del, want);
        assert_eq!(scan, want);
        assert!(!created, "an inverted range created the store");
    }

    /// `parse` accepts each command line that it accepted before.
    #[test]
    fn parse_reads_each_command_line() {
        let scan = |start: &'static [u8], end, reverse, limit| Cmd::Scan {
            start,
            end,
            reverse,
            limit,
        };
        let cases: &[(&str, &[&str], Cmd<'_>)] = &[
            ("put", &["k", "v"], Cmd::Put(b"k", b"v")),
            ("get", &["k"], Cmd::Get(b"k")),
            ("del", &["k"], Cmd::Del(b"k")),
            ("delrange", &["a", "b"], Cmd::DelRange(b"a", b"b")),
            ("delrange", &["a", "a"], Cmd::DelRange(b"a", b"a")),
            ("compact", &[], Cmd::Compact),
            ("stats", &[], Cmd::Stats),
            ("scan", &[], scan(b"", None, false, usize::MAX)),
            ("scan", &["a"], scan(b"a", None, false, usize::MAX)),
            (
                "scan",
                &["a", "c"],
                scan(b"a", Some(&b"c"[..]), false, usize::MAX),
            ),
            (
                "scan",
                &["a", "--reverse", "c"],
                scan(b"a", Some(&b"c"[..]), true, usize::MAX),
            ),
            (
                "scan",
                &["--limit", "1", "a", "b"],
                scan(b"a", Some(&b"b"[..]), false, 1),
            ),
            (
                "scan",
                &["--reverse", "--limit", "0"],
                scan(b"", None, true, 0),
            ),
            (
                "scan",
                &["b", "b"],
                scan(b"b", Some(&b"b"[..]), false, usize::MAX),
            ),
        ];
        for (cmd, args, want) in cases {
            let args = strings(args);
            assert_eq!(parse(cmd, &args).as_ref(), Ok(want), "{cmd} {args:?}");
        }
    }
}
