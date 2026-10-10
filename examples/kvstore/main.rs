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
//! then on. `store.rs` turns horton's single-owner `Db` into a shared
//! handle with group commit and background compaction; `device.rs` is the
//! file-backed block device; `bench.rs` and `crash.rs` are the benchmark
//! and the crash test.

mod bench;
mod crash;
mod device;
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

// The shape of the store: 4 levels of up to 7 tables, so at most 28
// tables of about 7 MiB, some 200 MiB of compressed tables (a 256 MiB
// file gives each table an 8 MiB slot). The memtable (4096 writes, 1 MiB)
// is what one flush turns into a table; the bloom filter takes a whole
// block; the cache keeps 64 blocks (1 MiB) of recently read tables. A
// point read probes up to 7 tables in level 0 and one per level below.
horton::db_types! {
    block: BLOCK,
    key_max: KEY_MAX,
    val_max: VAL_MAX,
    memtable_entries: 4096,
    memtable_arena: 1 << 20,
    levels: LEVELS,
    tables_per_level: 7,
    bloom_bytes: BLOCK - 4,
    cache_blocks: 64;

    /// The store's database, over any device.
    pub type Db = KvDb;
    pub type Scan = KvScan;
    pub type RevScan = KvRevScan;
    pub type Compaction = KvCompaction;
}

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
            create_blocks: self.size_mb * 1024 * 1024 / BLOCK as u64,
            sync: self.sync,
        }
    }

    /// Opens the store, creating its directory if needed.
    ///
    /// # Errors
    ///
    /// The directory or the store cannot be opened.
    pub fn open(&self) -> Result<Store, String> {
        if let Some(dir) = self.db.parent() {
            std::fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
        }
        Store::open(&self.db, self.options())
            .map(|(s, _)| s)
            .map_err(|e| e.to_string())
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
  bench [--num N] [--value-size N] [--threads N] [--batch N]
        [--benchmarks a,b,...]  db_bench's workloads on a fresh store
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

/// The `ldb`-style commands: one operation on the store at `--db`.
fn cli(common: &Common, cmd: &str, args: &[String]) -> Result<(), String> {
    let arg = |i: usize| -> Result<&[u8], String> {
        args.get(i)
            .map(String::as_bytes)
            .ok_or_else(|| format!("usage {cmd} is missing an argument"))
    };
    let err = |e: StoreError| e.to_string();
    match cmd {
        "put" => {
            let store = common.open()?;
            store.put(arg(0)?, arg(1)?).map_err(err)
        }
        "get" => {
            let store = common.open()?;
            let v = store.get(arg(0)?).map_err(err)?.ok_or("not found")?;
            println!("{}", String::from_utf8_lossy(&v));
            Ok(())
        }
        "del" => {
            let store = common.open()?;
            store.delete(arg(0)?).map_err(err)
        }
        "delrange" => {
            let store = common.open()?;
            store.delete_range(arg(0)?, arg(1)?).map_err(err)
        }
        "scan" => {
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
            let store = common.open()?;
            let start = bounds.first().copied().unwrap_or_default();
            let end = bounds.get(1).copied();
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
        "compact" => {
            let store = common.open()?;
            let t = Instant::now();
            store.compact().map_err(err)?;
            println!("compacted in {:.2?}", t.elapsed());
            print_stats(&store)
        }
        "stats" => print_stats(&common.open()?.handle()),
        other => Err(format!("usage unknown command {other}")),
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
/// The store thread has stopped.
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
        "tables    {} used, {} free; per level {:?}; {} blocks of range tombstones",
        s.slots.used, s.slots.free, s.level_tables, s.rdel_blocks
    );
    println!(
        "writes    {} client writes in {} commits since open ({:.1} per commit)",
        s.grouped_writes,
        s.groups,
        float(s.grouped_writes) / float(s.groups.max(1))
    );
    println!(
        "work      {} flushes, {} compaction jobs in {} steps",
        s.flushes, s.compaction_jobs, s.compaction_steps
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
    let _ = std::fs::remove_file(&path);
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
    let _ = std::fs::remove_file(&common.db);
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
