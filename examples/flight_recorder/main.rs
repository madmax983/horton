//! Flight recorder: a sensor logger on NOR flash that survives power cuts
//! and streams cold data to object storage.
//!
//! ```text
//! cargo run --release --example flight_recorder                  # record; Ctrl-C any time, run again
//! cargo run --release --example flight_recorder -- torture 1000  # 1000 simulated power cuts
//! cargo run --release --example flight_recorder -- restore       # rebuild the history from the archive
//! ```
//!
//! Add `--s3 s3://bucket/prefix` to archive to S3 instead of a local
//! directory (see `cloud.rs` for the environment variables). The recorder
//! keeps its flash image and archive under `target/flight_recorder/`;
//! `--fresh` starts over.
//!
//! The database is horton, compiled for a 2 MiB NOR flash partition:
//! 512 sectors of 4 KiB, keys of 10 bytes, values of 8.
//!
//! If a check ever fails, `FR_DEBUG=1` makes `torture` print point reads of
//! the failing ticks and the table layout.

mod archive;
mod cloud;
mod flash;
mod format;
mod recorder;
mod verify;

use core::future::Future;
use core::pin::pin;
use core::task::{Context, Poll, Waker};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use horton::flash::FlashBlockDevice;
use horton::{BlockDevice, Config, Manifest};

use archive::{RamDevice, fetch, ingest};
use cloud::{DirBucket, MemBucket, ObjectStore, S3Bucket};
use flash::SimFlash;
use recorder::{Recorder, Stats, Stop, latest_tick};
use verify::{ArchiveIndex, Coverage, check, scan};

pub use format::*;

/// The recorder's flash as horton sees it.
type Chip = FlashBlockDevice<SimFlash, BLOCK, SECTORS>;

/// A glitch window is purged once per this many ticks.
pub const PURGE_EVERY: u64 = 1000;

/// The ticks `[a, b)` of epoch `k`'s glitch window, purged at tick
/// `k * PURGE_EVERY + 500`.
#[must_use]
pub const fn purge_window(k: u64) -> (u64, u64) {
    (k * PURGE_EVERY + 400, k * PURGE_EVERY + 410)
}

/// `record` paces itself to this many ticks a second, so there is time to
/// pull the plug; `--fast` drops the pacing.
const TICKS_PER_SECOND: u64 = 2000;

/// horton ships no executor; this one polls until done. Every device here
/// completes at once, so it never spins.
pub fn block_on<F: Future>(fut: F) -> F::Output {
    let mut fut = pin!(fut);
    let mut cx = Context::from_waker(Waker::noop());
    loop {
        if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
            return v;
        }
    }
}

struct Args {
    mode: String,
    count: Option<u64>,
    dir: PathBuf,
    s3: Option<String>,
    fresh: bool,
    fast: bool,
    seed: u64,
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args {
        mode: "record".into(),
        count: None,
        dir: PathBuf::from("target/flight_recorder"),
        s3: None,
        fresh: false,
        fast: false,
        seed: 0x5eed,
    };
    let number = |s: String, flag: &str| s.parse::<u64>().map_err(|e| format!("{flag}: {e}"));
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        let mut next = |what: &str| it.next().ok_or_else(|| format!("{a} needs {what}"));
        match a.as_str() {
            "record" | "torture" | "restore" => args.mode = a,
            "--s3" => args.s3 = Some(next("s3://bucket/prefix")?),
            "--dir" => args.dir = PathBuf::from(next("a directory")?),
            "--seed" => args.seed = number(next("a number")?, "--seed")?,
            "--ticks" => args.count = Some(number(next("a number")?, "--ticks")?),
            "--fresh" => args.fresh = true,
            "--fast" => args.fast = true,
            "-h" | "--help" => return Err(String::new()),
            n if n.parse::<u64>().is_ok() => args.count = n.parse().ok(),
            other => return Err(format!("unknown argument {other}")),
        }
    }
    Ok(args)
}

const USAGE: &str =
    "usage: flight_recorder [record|torture [N]|restore] [--ticks N] [--s3 s3://bucket/prefix]
                       [--dir PATH] [--fresh] [--fast] [--seed N]

  record   log sensor frames to simulated flash, archiving cold tables.
           Ctrl-C (or kill -9) whenever you like, then run it again.
  torture  cut the power N times (default 500) at random writes, reboot,
           and prove nothing acknowledged was lost.
  restore  rebuild the full history from the archive, as a ground station.";

fn main() {
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            if !e.is_empty() {
                eprintln!("{e}\n");
            }
            eprintln!("{USAGE}");
            std::process::exit(2);
        }
    };
    let result = match args.mode.as_str() {
        "torture" => torture(&args),
        "restore" => open_store(&args).and_then(|s| restore(&*s)),
        _ => record(&args),
    };
    if let Err(e) = result {
        eprintln!("\nerror: {e}");
        std::process::exit(1);
    }
}

fn open_store(args: &Args) -> Result<Box<dyn ObjectStore>, String> {
    match &args.s3 {
        Some(url) => Ok(Box::new(S3Bucket::from_url(url)?)),
        None => Ok(Box::new(
            DirBucket::new(args.dir.join("archive")).map_err(|e| e.to_string())?,
        )),
    }
}

/// `record`: the recorder on a flash image that outlives the process.
fn record(args: &Args) -> Result<(), String> {
    if args.fresh {
        let _ = std::fs::remove_dir_all(&args.dir);
    }
    std::fs::create_dir_all(&args.dir).map_err(|e| e.to_string())?;
    let mut store = open_store(args)?;
    let image = args.dir.join("flash.img");
    let reboot = image.exists();
    let chip = SimFlash::file(&image, SECTORS).map_err(|e| format!("{}: {e}", image.display()))?;

    header("flight recorder");
    println!(
        "flash    {} ({} sectors of {} KiB); this shape needs at least {}",
        image.display(),
        SECTORS,
        BLOCK / 1024,
        RecorderDb::<Chip>::MIN_DEVICE_BLOCKS
    );
    println!("archive  {}", store.location());
    ram_report();

    let mut db = Box::new(RecorderDb::new(Chip::new(chip, 0), config()));
    let opened = block_on(db.open()).map_err(|e| format!("open: {e:?}"))?;
    assert!(db.is_open(), "open() succeeded, so the handle is usable");
    let resume = latest_tick(&db).map_err(|s| s.0)?.map_or(0, |t| t + 1);
    let boots = boot(&mut db)?;
    layout_report(&db);
    if reboot {
        println!(
            "\nboot #{boots}: recovered {} WAL records, {} tables in level 0; resuming at tick {resume}",
            opened.recovered_records, opened.l0_tables
        );
        let mut index = ArchiveIndex::default();
        verify_report(&db, &*store, &mut index, None)?;
    } else {
        println!("\nboot #{boots}: a fresh chip");
    }

    let ticks = args.count.unwrap_or(20_000);
    let rate = if args.fast {
        None
    } else {
        Some(TICKS_PER_SECOND)
    };
    println!(
        "\nrecording ticks {resume}..{}{}; Ctrl-C any time and run again to watch it recover\n",
        resume + ticks,
        rate.map_or(String::new(), |r| format!(" at {r} ticks/s"))
    );
    let mut rec = Recorder::new(&mut *store).map_err(|s| s.0)?;
    let start = Instant::now();
    let mut last_line = Instant::now();
    rec.run(&mut db, resume, resume + ticks, &mut |t, rec, db| {
        if let Some(rate) = rate {
            let due = start + Duration::from_micros((t - resume + 1) * 1_000_000 / rate);
            std::thread::sleep(due.saturating_duration_since(Instant::now()));
        }
        if last_line.elapsed() >= Duration::from_millis(500) {
            last_line = Instant::now();
            progress(t, &rec.stats, db);
        }
    })
    .map_err(|s| s.0)?;
    progress(resume + ticks - 1, &rec.stats, &db);
    println!();
    tour(&mut db, &mut rec, resume + ticks - 1)?;
    let stats = rec.stats;
    let location = rec.location();
    drop(rec);

    wear_report(&db);
    run_report(&stats, &location);
    header("verifying");
    let mut index = ArchiveIndex::default();
    verify_report(&db, &*store, &mut index, None)?;
    println!("\nnext: run it again to resume, `torture` to cut the power 500 times, or `restore`");
    Ok(())
}

/// Bumps the boot counter: a plain `get` and `put` of a metadata key.
fn boot<D: BlockDevice>(db: &mut RecorderDb<D>) -> Result<u64, String>
where
    D::Error: core::fmt::Debug,
{
    let mut buf = [0u8; VAL_MAX];
    let n = block_on(db.get(BOOT_KEY, &mut buf))
        .map_err(|e| format!("get: {e:?}"))?
        .map_or(0, |_| u64::from_le_bytes(buf))
        + 1;
    block_on(db.put(BOOT_KEY, &n.to_le_bytes())).map_err(|e| format!("put: {e:?}"))?;
    Ok(n)
}

/// A small xorshift generator: a torture run is reproducible from its seed.
struct Rng(u64);

impl Rng {
    /// Seeds through splitmix64, so nearby seeds give unrelated streams.
    const fn new(seed: u64) -> Self {
        let mut z = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        Self((z ^ (z >> 31)) | 1)
    }

    const fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

/// The power-cut test's state across reboots.
struct Torture<'s> {
    rec: Recorder<'s>,
    index: ArchiveIndex,
    /// The newest tick whose frame the recorder saw acknowledged.
    acked: Option<u64>,
    /// Cuts during a frame's write after which the frame had landed.
    landed: u64,
    layout: Option<Config>,
}

impl Torture<'_> {
    /// Powers the chip on, recovers, and records until the armed cut.
    fn run_until_cut(&mut self, chip: SimFlash, cut: u64) -> Result<SimFlash, String> {
        let mut db = Box::new(RecorderDb::new(Chip::new(chip, 0), config()));
        self.rec.rebooted();
        // The cut can land in recovery itself; then there is nothing to run.
        if block_on(db.open()).is_ok() {
            let mut t = latest_tick(&db).ok().flatten().map_or(0, |t| t + 1);
            loop {
                if let Err(Stop(msg)) = self.rec.tick(&mut db, t) {
                    if db.device().flash().powered() {
                        return Err(format!(
                            "cut {cut}: the recorder stopped with the power on: {msg}"
                        ));
                    }
                    break;
                }
                self.acked = Some(t);
                t += 1;
            }
        }
        if db.device().flash().powered() {
            return Err(format!("cut {cut}: recovery failed with the power on"));
        }
        let mut chip = db.into_device().into_flash();
        chip.restore_power();
        Ok(chip)
    }

    /// Reboots with the power on and proves nothing acknowledged was lost.
    fn check(&mut self, chip: SimFlash, cut: u64) -> Result<SimFlash, String> {
        let mut db = Box::new(RecorderDb::new(Chip::new(chip, 0), config()));
        block_on(db.open()).map_err(|e| format!("cut {cut}: recovery failed: {e:?}"))?;
        self.layout = Some(db.config());
        let mut problems = Vec::new();
        if let Err(e) = db.check_invariants() {
            problems.push(format!("invariant: {e}"));
        }
        let mut local = Coverage::default();
        scan(&db, &mut local, &mut problems);
        self.index.refresh(self.rec.store(), &mut problems);
        // The newest frame is the last one acknowledged, or the one being
        // written when the power died, if it landed whole.
        match (self.acked, local.max_tick) {
            (Some(a), Some(n)) if n == a => {}
            (Some(a), Some(n)) if n == a + 1 => {
                self.landed += 1;
                self.acked = Some(n);
            }
            (None, None) => {}
            (None, Some(0)) => self.acked = Some(0),
            (a, n) => problems.push(format!(
                "acknowledged up to tick {a:?}, recovered up to {n:?}"
            )),
        }
        check(
            &local,
            &self.index.coverage,
            self.acked.map(|a| a + 1),
            &mut problems,
        );
        if !problems.is_empty() {
            if std::env::var_os("FR_DEBUG").is_some() {
                diagnose(&db, &problems);
            }
            return Err(format!("cut {cut}: {}", problems.join("\n  ")));
        }
        Ok(db.into_device().into_flash())
    }
}

/// `torture`: cut the power at random writes, reboot, and check.
fn torture(args: &Args) -> Result<(), String> {
    let cuts = args.count.unwrap_or(500);
    header("power-cut torture test");
    println!(
        "{cuts} power cuts at random erase or program operations, each tearing that operation"
    );
    println!(
        "partway; after every cut: reboot, recover, and prove nothing acknowledged was lost.\n"
    );
    let mut rng = Rng::new(args.seed);
    let mut bucket = MemBucket::default();
    let mut run = Torture {
        rec: Recorder::new(&mut bucket).map_err(|s| s.0)?,
        index: ArchiveIndex::default(),
        acked: None,
        landed: 0,
        layout: None,
    };
    let mut chip = SimFlash::ram(SECTORS);
    let started = Instant::now();
    for cut in 1..=cuts {
        chip.cut_power_after(
            1 + rng.below(1500),
            u8::try_from(rng.below(256)).expect("below 256"),
        );
        chip = run.run_until_cut(chip, cut)?;
        chip = run.check(chip, cut)?;
        if cut % 50 == 0 || cut == cuts {
            println!(
                "  {cut:>5} cuts  tick {:>7}  archived {:>4} tables  {:>5.1}s",
                run.acked.unwrap_or(0),
                run.index.objects(),
                started.elapsed().as_secs_f64()
            );
        }
    }
    header("result");
    println!("power cuts             {cuts}, each tearing an erase or program partway");
    println!(
        "acknowledged frames    {} ticks, every one present (on flash or in the archive)",
        run.acked.map_or(0, |a| a + 1)
    );
    println!(
        "frames cut mid-write   {} landed whole, the rest not at all; never in part",
        run.landed
    );
    println!("lost or corrupt        0");
    println!(
        "snapshots, TTLs        {} snapshot and {} TTL checks passed",
        run.rec.stats.snapshot_checks, run.rec.stats.ttl_checks
    );
    println!(
        "archive                {} tables, {} KiB, each re-ingested and checked",
        run.index.objects(),
        run.index.bytes / 1024
    );
    if let Some(layout) = run.layout {
        println!("flash wear             erases per sector, by region:");
        wear_lines(chip.erase_counts(), &layout);
    }
    Ok(())
}

/// `restore`: a ground station rebuilding the history from the archive.
///
/// Each table is read back through its own throwaway database of the
/// recorder's shape, exactly as the recorder would read it. One database
/// for the whole history would not do: the shape has `LEVELS × TABLES`
/// table slots whatever the device size, and archived tables hold
/// disjoint ticks, so compaction has nothing to merge and the slots run
/// out after a few dozen tables. Tables can overlap (a table merged while
/// it was being archived leaves a spare copy), so each reading counts once.
fn restore(store: &dyn ObjectStore) -> Result<(), String> {
    header("ground station");
    let keys = store.list("tables/").map_err(|e| e.to_string())?;
    println!("archive  {}: {} tables", store.location(), keys.len());
    if keys.is_empty() {
        println!(
            "nothing archived yet: run `record` long enough for tables to go cold ({HOT_TICKS} ticks)"
        );
        return Ok(());
    }
    // Which readings (bits 0-3) and alarm (bit 4) each tick has so far.
    let mut seen: Vec<u8> = Vec::new();
    let mut sensors = [SensorStats::default(); SENSORS as usize];
    let (mut events, mut first, mut last, mut blocks) = (0u64, u64::MAX, 0u64, 0u64);
    let mut problems = Vec::new();
    let mut cov = Coverage::default();
    let mut scratch = Box::new(RecorderCompaction::new());
    for key in &keys {
        let table = fetch(store, key)?;
        blocks += u64::from(table.sealed.block_count);
        let size = ((u64::from(table.sealed.block_count) + 1) * 64)
            .max(RecorderDb::<RamDevice>::MIN_DEVICE_BLOCKS);
        let mut db = Box::new(RecorderDb::new(
            RamDevice::new(size),
            Config::whole_device(size),
        ));
        block_on(db.open()).map_err(|e| format!("open: {e:?}"))?;
        ingest(&mut db, &mut scratch, &table)
            .map_err(|e| format!("ingest table {}: {e:?}", table.sealed.id))?;

        let mut scan = Box::new(RecorderScan::new(&db));
        block_on(scan.seek(&[], Some(&[0x01]), u64::MAX)).map_err(|e| format!("{e:?}"))?;
        let (mut key_buf, mut val_buf) = ([0u8; KEY_MAX], [0u8; VAL_MAX]);
        while let Some((kl, vl)) =
            block_on(scan.next(&mut key_buf, &mut val_buf)).map_err(|e| format!("{e:?}"))?
        {
            let tick = tick_of(&key_buf[..kl]);
            let bit = match key_buf[8] {
                b'r' => 1 << key_buf[9],
                b'e' => 1 << 4,
                _ => continue,
            };
            let i = usize::try_from(tick).map_err(|e| e.to_string())?;
            if seen.len() <= i {
                seen.resize(i + 1, 0);
            }
            if seen[i] & bit != 0 {
                continue; // already counted from an overlapping table
            }
            seen[i] |= bit;
            (first, last) = (first.min(tick), last.max(tick));
            if key_buf[8] == b'r' {
                sensors[usize::from(key_buf[9])].add(celsius(&val_buf[..vl]));
            } else {
                events += 1;
            }
        }
        drop(scan);
        verify::scan(&db, &mut cov, &mut problems);
        if let Err(e) = db.check_invariants() {
            problems.push(format!("{key}: {e}"));
        }
    }
    println!(
        "read back {} tables ({} KiB), each through its own database",
        keys.len(),
        blocks * 4
    );

    let gaps = purged_windows(&seen, first, last);
    println!("\nhistory  ticks {first}..={last}, {events} alarm events");
    println!(
        "purged   {} glitch windows the recorder deleted before archiving: {}",
        gaps.len(),
        gaps.join(", ")
    );
    for (i, stat) in sensors.iter().enumerate() {
        if stat.readings > 0 {
            println!(
                "sensor {i} {:>7} readings  min {:5.2} °C  max {:5.2} °C  mean {:5.2} °C",
                stat.readings,
                stat.min,
                stat.max,
                stat.sum / stat.weight
            );
        }
    }
    if problems.is_empty() {
        println!(
            "\nevery archived value checks out ({} entries)",
            cov.entries
        );
        Ok(())
    } else {
        Err(problems.join("\n"))
    }
}

/// Runs of ticks with no readings between `first` and `last`, as
/// `a..b` (`seen[t]` bits 0-3 are the readings found for tick `t`): the
/// glitch windows the recorder purged before archiving.
fn purged_windows(seen: &[u8], first: u64, last: u64) -> Vec<String> {
    let mut gaps = Vec::new();
    let mut run: Option<u64> = None;
    for t in first..=last.max(first) {
        let empty = usize::try_from(t)
            .ok()
            .and_then(|i| seen.get(i))
            .is_none_or(|b| b.trailing_zeros() >= 4);
        match (empty, run) {
            (true, None) => run = Some(t),
            (false, Some(a)) => {
                gaps.push(format!("{a}..{t}"));
                run = None;
            }
            _ => {}
        }
    }
    gaps
}

/// One sensor's readings across the history.
#[derive(Clone, Copy)]
struct SensorStats {
    readings: u64,
    weight: f64,
    min: f64,
    max: f64,
    sum: f64,
}

impl Default for SensorStats {
    fn default() -> Self {
        Self {
            readings: 0,
            weight: 0.0,
            min: f64::MAX,
            max: f64::MIN,
            sum: 0.0,
        }
    }
}

impl SensorStats {
    fn add(&mut self, celsius: f64) {
        self.readings += 1;
        self.weight += 1.0;
        self.min = self.min.min(celsius);
        self.max = self.max.max(celsius);
        self.sum += celsius;
    }
}

/// A short walk through the read API on the live database.
fn tour<D: BlockDevice>(
    db: &mut RecorderDb<D>,
    rec: &mut Recorder<'_>,
    now: u64,
) -> Result<(), String>
where
    D::Error: core::fmt::Debug,
{
    header("reading it back");
    let cache_before = db.cache_stats();
    let e = |e: horton::Error<D::Error>| format!("{e:?}");
    let (mut k, mut v) = ([0u8; KEY_MAX], [0u8; VAL_MAX]);

    // The newest frame, from a reverse scan.
    let mut rev = Box::new(RecorderRevScan::new(db));
    block_on(rev.seek_prev(&key(now, 0xFF, 0xFF), Some(&key(now, 0, 0)), u64::MAX)).map_err(e)?;
    let mut frame = Vec::new();
    while let Some((kl, vl)) = block_on(rev.prev(&mut k, &mut v)).map_err(e)? {
        if k[8] == b'r' {
            frame.push(format!("s{}={:.2}°C", k[9], celsius(&v[..vl])));
        }
        debug_assert_eq!(kl, KEY_MAX);
    }
    frame.reverse();
    println!("newest frame      tick {now}: {}", frame.join("  "));

    // The last alarms, walking backwards through recent history.
    // Clock-aware reverse scan: expired debug traces are skipped for us.
    block_on(rev.seek_prev_with_time(&key(now, 0xFF, 0xFF), None, u64::MAX, now)).map_err(e)?;
    let mut alarms = Vec::new();
    while alarms.len() < 3 {
        let Some(_) = block_on(rev.prev(&mut k, &mut v)).map_err(e)? else {
            break;
        };
        if k[8] == b'e' {
            alarms.push(format!("code {} at tick {}", k[9], tick_of(&k)));
        }
    }
    drop(rev);
    println!("latest alarms     {}", alarms.join(", "));

    // A window scan, with and without a clock: expired debug traces vanish.
    let from = now.saturating_sub(199);
    let mut scan = Box::new(RecorderScan::new(db));
    let mut count = |scan: &mut RecorderScan<'_, D>, clock: u64| -> Result<(u64, u64), String> {
        block_on(scan.seek_with_time(&key(from, 0, 0), Some(&key(now + 1, 0, 0)), u64::MAX, clock))
            .map_err(e)?;
        let (mut readings, mut traces) = (0, 0);
        while block_on(scan.next(&mut k, &mut v)).map_err(e)?.is_some() {
            match k[8] {
                b'r' => readings += 1,
                b'd' => traces += 1,
                _ => {}
            }
        }
        Ok((readings, traces))
    };
    let (readings, all_traces) = count(&mut scan, 0)?;
    let (_, live_traces) = count(&mut scan, now)?;
    drop(scan);
    println!(
        "last 200 ticks    {readings} readings; {live_traces} of {all_traces} debug traces still live (TTL {TTL_TICKS} ticks)"
    );

    // A point read with a clock, and a snapshot that outlives a delete.
    let trace = key(now, b'd', 0);
    let fresh = block_on(db.get_with_time(&trace, &mut v, now))
        .map_err(e)?
        .is_some();
    let aged = block_on(db.get_with_time(&trace, &mut v, now + TTL_TICKS))
        .map_err(e)?
        .is_some();
    println!("debug trace       live now: {fresh}; live {TTL_TICKS} ticks from now: {aged}");
    // A marker key (0xFF sorts it after every tick): written, pinned by a
    // snapshot, then deleted. The live view loses it; the snapshot keeps it.
    let marker = b"\xFFmarker";
    rec.retry(db, now, |db| block_on(db.put(marker, &now.to_le_bytes())))
        .map_err(|s| s.0)?;
    let snap = db.snapshot().map_err(e)?;
    rec.retry(db, now, |db| block_on(db.delete(marker)))
        .map_err(|s| s.0)?;
    let seen_live = block_on(db.get(marker, &mut v)).map_err(e)?.is_some();
    let seen_snap = block_on(db.get_at(marker, &mut v, snap))
        .map_err(e)?
        .is_some();
    db.release_snapshot(snap);
    println!(
        "snapshot          a key deleted after the snapshot: live view sees it {seen_live}, snapshot sees it {seen_snap}"
    );
    let cache = db.cache_stats();
    let (hits, misses) = (
        cache.hits - cache_before.hits,
        cache.misses - cache_before.misses,
    );
    println!(
        "block cache       {hits} of {} block reads above came from RAM ({} slots of {BLOCK} bytes)",
        hits + misses,
        cache.capacity
    );
    Ok(())
}

fn header(title: &str) {
    println!(
        "\n── {title} {}",
        "─".repeat(60usize.saturating_sub(title.len()))
    );
}

fn ram_report() {
    println!(
        "RAM      Db {} B, Scan {} B, RevScan {} B, Compaction {} B: fixed, no allocator",
        size_of::<RecorderDb<Chip>>(),
        size_of::<RecorderScan<'_, Chip>>(),
        size_of::<RecorderRevScan<'_, Chip>>(),
        size_of::<RecorderCompaction>()
    );
}

fn layout_report<D: BlockDevice>(db: &RecorderDb<D>) {
    let c = db.config();
    println!(
        "layout   manifest ring of {} × {} block(s) at 0, WAL {}..{}, tables {}..{} ({} slots of {} blocks)",
        c.manifest_ring,
        Manifest::<LEVELS, TABLES, KEY_MAX>::max_blocks::<BLOCK>(),
        c.wal_start,
        c.wal_end,
        c.tbl_start,
        c.tbl_end,
        db.slot_stats().slots,
        db.slot_stats().slot_blocks,
    );
}

fn progress<D: BlockDevice>(t: u64, s: &Stats, db: &RecorderDb<D>) {
    let slots = db.slot_stats();
    println!(
        "  tick {t:>8}  flushes {:>5}  compaction steps {:>5}  table slots in use {:>2}/{}  archived {:>4} tables ({:>6} KiB)",
        s.flushes,
        s.compaction_steps,
        slots.used + slots.reserved,
        slots.slots,
        s.archived_tables,
        s.archived_bytes / 1024,
    );
}

fn run_report(s: &Stats, location: &str) {
    header("this run");
    println!(
        "ticks {}, flushes {}, compaction steps {}",
        s.ticks, s.flushes, s.compaction_steps
    );
    println!(
        "glitch windows purged {} (range delete), snapshot checks {}, TTL checks {}",
        s.purges, s.snapshot_checks, s.ttl_checks
    );
    println!(
        "archived {} tables ({} KiB) to {location}; {} deferred by the resurrection check",
        s.archived_tables,
        s.archived_bytes / 1024,
        s.archive_refusals
    );
}

fn wear_report(db: &RecorderDb<Chip>) {
    header("flash wear this run");
    wear_lines(db.device().flash().erase_counts(), &db.config());
}

/// Erases per sector in each region. The WAL takes one erase per commit,
/// so it wears fastest; a `WriteBatch` (one erase for a whole frame) and
/// the manifest ring are how the recorder keeps that in check.
fn wear_lines(erases: &[u32], c: &Config) {
    let stride = Manifest::<LEVELS, TABLES, KEY_MAX>::max_blocks::<BLOCK>();
    let copies = if c.manifest_ring == 0 {
        2
    } else {
        u64::from(c.manifest_ring)
    };
    for (name, a, b) in [
        ("manifest", 0, copies * stride),
        ("WAL", c.wal_start, c.wal_end),
        ("tables", c.tbl_start, c.tbl_end),
    ] {
        let region = &erases[usize::try_from(a).unwrap_or(0)..usize::try_from(b).unwrap_or(0)];
        if let (Some(lo), Some(hi)) = (region.iter().min(), region.iter().max()) {
            let sum: u64 = region.iter().map(|&e| u64::from(e)).sum();
            let mean = sum / region.len() as u64;
            println!(
                "  {name:<9} sectors {a:>3}..{b:<3}  min {lo:>6}  mean {mean:>6}  max {hi:>6}"
            );
        }
    }
}

fn verify_report(
    db: &RecorderDb<Chip>,
    store: &dyn ObjectStore,
    index: &mut ArchiveIndex,
    in_flight: Option<u64>,
) -> Result<(), String> {
    let mut problems = Vec::new();
    if let Err(e) = db.check_invariants() {
        problems.push(format!("invariant: {e}"));
    }
    let mut local = Coverage::default();
    scan(db, &mut local, &mut problems);
    index.refresh(store, &mut problems);
    check(&local, &index.coverage, in_flight, &mut problems);
    if problems.is_empty() {
        println!(
            "verified: ticks 0..={} complete across flash ({} entries) and archive ({} tables); frames atomic, purges held, values intact",
            local.max_tick.unwrap_or(0),
            local.entries,
            index.objects()
        );
        Ok(())
    } else {
        Err(format!("verification failed:\n  {}", problems.join("\n  ")))
    }
}

/// Debug aid: point reads for the failing ticks, and the table layout.
fn diagnose(db: &RecorderDb<Chip>, problems: &[String]) {
    for p in problems {
        let Some(t) = p
            .strip_prefix("tick ")
            .and_then(|r| r.split(':').next())
            .and_then(|n| n.parse::<u64>().ok())
        else {
            continue;
        };
        let mut v = [0u8; VAL_MAX];
        for s in 0..SENSORS {
            let got = block_on(db.get(&key(t, b'r', s), &mut v));
            eprintln!("  get tick {t} sensor {s}: {got:?}");
        }
    }
    for level in 0..LEVELS {
        for tb in db.level_tables(level).unwrap_or(&[]) {
            eprintln!(
                "  L{level} table {:>4} blocks {:>4}+{:<3} ticks {}..={} ({:02x?}..{:02x?}) seq {}..={} entries {} rdel {}",
                tb.id,
                tb.first_block,
                tb.block_count,
                tick_of(tb.first_key.as_slice()),
                tick_of(tb.last_key.as_slice()),
                &tb.first_key.as_slice()[8.min(tb.first_key.as_slice().len())..],
                &tb.last_key.as_slice()[8.min(tb.last_key.as_slice().len())..],
                tb.min_seq,
                tb.max_seq,
                tb.entry_count,
                tb.rdel_blocks
            );
        }
    }
}
