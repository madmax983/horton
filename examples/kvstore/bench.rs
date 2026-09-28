//! `db_bench`'s workloads, run on a fresh store.
//!
//! The names and the report follow `LevelDB`'s `db_bench`: 16-byte keys,
//! 100-byte values that compress to about half, `micros/op` and `MB/s`.
//! Two differences matter when comparing numbers:
//!
//! - **Every write is synced by default.** `db_bench`'s `fillseq` and
//!   `fillrandom` do not sync (its `fillsync` does). Pass `--no-sync` for
//!   the like-for-like run.
//! - **Writers are threads, not one loop.** `--threads N` (default 4)
//!   clients share the store, so group commit has something to group;
//!   `--batch N` puts N writes in each client batch, like `fillbatch`.

use std::fmt::Write as _;
use std::time::{Duration, Instant};

use crate::store::{Batch, Handle, Store};
use crate::{Common, float, print_stats};

const DEFAULT: &str = "fillseq,fillrandom,overwrite,readrandom,readmissing,readseq,readreverse,seekrandom,deleterandom,compact,readrandom";

struct Params {
    num: u64,
    value_size: usize,
    threads: u64,
    batch: u64,
    benchmarks: Vec<String>,
}

fn parse(args: &[String]) -> Result<Params, String> {
    let mut p = Params {
        num: 100_000,
        value_size: 100,
        threads: 4,
        batch: 1,
        benchmarks: DEFAULT.split(',').map(String::from).collect(),
    };
    let mut it = args.iter();
    while let Some(a) = it.next() {
        let mut num = || -> Result<u64, String> {
            it.next()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| format!("usage {a} needs a number"))
        };
        match a.as_str() {
            "--num" => p.num = num()?,
            "--value-size" => p.value_size = usize::try_from(num()?).map_err(|e| e.to_string())?,
            "--threads" => p.threads = num()?.max(1),
            "--batch" => p.batch = num()?.max(1),
            "--benchmarks" => {
                p.benchmarks = it
                    .next()
                    .ok_or("usage --benchmarks needs a list")?
                    .split(',')
                    .map(String::from)
                    .collect();
            }
            other => return Err(format!("usage unknown bench argument {other}")),
        }
    }
    if p.value_size > crate::VAL_MAX {
        return Err(format!("--value-size is at most {}", crate::VAL_MAX));
    }
    Ok(p)
}

/// xorshift64*: a small deterministic generator, one per client thread.
pub struct Rng(u64);

impl Rng {
    pub const fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    pub const fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// Uniform in `0..n` (`n > 0`), near enough for a benchmark.
    pub const fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// `db_bench`'s key: the index as 16 decimal digits.
fn key(i: u64) -> [u8; 16] {
    let mut k = [b'0'; 16];
    let mut n = i;
    for b in k.iter_mut().rev() {
        *b = b'0' + (n % 10) as u8;
        n /= 10;
    }
    k
}

/// Values that compress to about half, like `db_bench`'s: random bytes,
/// each repeated once.
struct Values {
    data: Vec<u8>,
    pos: usize,
}

impl Values {
    fn new(size: usize) -> Self {
        let mut rng = Rng::new(301);
        let mut data = Vec::with_capacity(1 << 20);
        while data.len() < (1 << 20) {
            let chunk: Vec<u8> = (0..size.max(2) / 2)
                .map(|_| b' ' + u8::try_from(rng.below(95)).unwrap_or(0))
                .collect();
            data.extend_from_slice(&chunk);
            data.extend_from_slice(&chunk);
        }
        Self { data, pos: 0 }
    }

    fn next(&mut self, size: usize) -> &[u8] {
        if self.pos + size > self.data.len() {
            self.pos = 0;
        }
        self.pos += size;
        &self.data[self.pos - size..self.pos]
    }
}

/// What one benchmark did.
#[derive(Default)]
struct Done {
    ops: u64,
    bytes: u64,
    found: u64,
    note: String,
}

pub fn run(common: &Common, args: &[String]) -> Result<(), String> {
    let p = parse(args)?;
    let path = common.db.with_file_name("bench.db");
    let _ = std::fs::remove_file(&path);
    let common = Common {
        db: path,
        size_mb: common.size_mb.max(256),
        sync: common.sync,
    };
    let store = common.open()?;
    println!("horton:     {}", env!("CARGO_PKG_VERSION"));
    println!("Keys:       16 bytes each");
    println!(
        "Values:     {} bytes each ({} bytes after compression)",
        p.value_size,
        p.value_size / 2
    );
    println!("Entries:    {}", p.num);
    println!(
        "Writers:    {} threads{}; {}",
        p.threads,
        if p.batch > 1 {
            format!(", {} writes per batch", p.batch)
        } else {
            String::new()
        },
        if common.sync {
            "every commit fdatasync'd (--no-sync to skip)"
        } else {
            "no fdatasync (LevelDB's default)"
        }
    );
    println!("File:       {}", common.db.display());
    println!("------------------------------------------------");
    for name in &p.benchmarks {
        let t = Instant::now();
        let done = match name.as_str() {
            "fillseq" => fill(&store, &p, false),
            "fillrandom" | "overwrite" => fill(&store, &p, true),
            "readrandom" => read_random(&store, &p, false),
            "readmissing" => read_random(&store, &p, true),
            "readseq" => read_seq(&store, false),
            "readreverse" => read_seq(&store, true),
            "seekrandom" => seek_random(&store, &p),
            "deleterandom" => delete_random(&store, &p),
            "compact" => store
                .compact()
                .map(|()| Done::default())
                .map_err(|e| e.to_string()),
            "stats" => print_stats(&store).map(|()| Done::default()),
            other => Err(format!("unknown benchmark {other}")),
        }
        .map_err(|e| {
            // What the store looked like when it gave up.
            let _ = print_stats(&store);
            format!("{name}: {e}")
        })?;
        report(name, t.elapsed(), &done);
    }
    println!("------------------------------------------------");
    print_stats(&store)?;
    drop(store);
    // Leave no 256 MiB file behind (it is sparse, but still).
    let _ = std::fs::remove_file(&common.db);
    Ok(())
}

fn report(name: &str, elapsed: Duration, d: &Done) {
    let secs = elapsed.as_secs_f64();
    let mut line = format!("{name:<12} : ");
    if d.ops == 0 {
        let _ = write!(line, "{secs:>11.3} s");
    } else {
        let _ = write!(line, "{:>11.3} micros/op;", secs * 1e6 / float(d.ops));
        if d.bytes > 0 {
            let _ = write!(line, " {:>6.1} MB/s", float(d.bytes) / 1_048_576.0 / secs);
        }
        let _ = write!(line, " ({:.0} ops/s)", float(d.ops) / secs);
    }
    if !d.note.is_empty() {
        let _ = write!(line, " {}", d.note);
    }
    println!("{line}");
}

/// Runs `per_thread(handle, thread, ops)` on every client thread, each
/// doing its share of `num`, and sums what they did.
fn clients(
    store: &Store,
    p: &Params,
    per_thread: impl Fn(&Handle, u64, u64) -> Result<Done, String> + Sync,
) -> Result<Done, String> {
    let results: Vec<Result<Done, String>> = std::thread::scope(|s| {
        let threads: Vec<_> = (0..p.threads)
            .map(|t| {
                let h = store.handle();
                let share = p.num / p.threads + u64::from(t < p.num % p.threads);
                let f = &per_thread;
                s.spawn(move || f(&h, t, share))
            })
            .collect();
        threads
            .into_iter()
            .map(|t| t.join().unwrap_or_else(|_| Err("a client panicked".into())))
            .collect()
    });
    let mut total = Done::default();
    for r in results {
        let d = r?;
        total.ops += d.ops;
        total.bytes += d.bytes;
        total.found += d.found;
    }
    Ok(total)
}

fn fill(store: &Store, p: &Params, random: bool) -> Result<Done, String> {
    let before = store.stats().map_err(|e| e.to_string())?;
    let mut done = clients(store, p, |h, t, share| {
        let mut rng = Rng::new(t + 1);
        let mut values = Values::new(p.value_size);
        let mut d = Done::default();
        let mut i = 0;
        while i < share {
            let mut batch = Batch::new();
            for _ in 0..p.batch.min(share - i) {
                // Sequential keys interleave the threads' shares so the
                // whole key space is written in order.
                let k = if random {
                    rng.below(p.num)
                } else {
                    i * p.threads + t
                };
                batch.put(&key(k), values.next(p.value_size));
                d.bytes += 16 + p.value_size as u64;
                i += 1;
            }
            h.write(batch).map_err(|e| e.to_string())?;
        }
        d.ops = share;
        Ok(d)
    })?;
    let after = store.stats().map_err(|e| e.to_string())?;
    let commits = after.groups - before.groups;
    let writes = after.grouped_writes - before.grouped_writes;
    done.note = format!(
        "[{commits} commits, {:.1} client batches each]",
        float(writes) / float(commits.max(1))
    );
    Ok(done)
}

fn read_random(store: &Store, p: &Params, missing: bool) -> Result<Done, String> {
    let mut done = clients(store, p, |h, t, share| {
        let mut rng = Rng::new(1000 + t);
        let mut d = Done::default();
        for _ in 0..share {
            let k = key(rng.below(p.num));
            let v = if missing {
                // db_bench's readmissing: a key just past a real one.
                let mut m = k.to_vec();
                m.push(b'.');
                h.get(&m)
            } else {
                h.get(&k)
            }
            .map_err(|e| e.to_string())?;
            if let Some(v) = v {
                d.found += 1;
                d.bytes += 16 + v.len() as u64;
            }
        }
        d.ops = share;
        Ok(d)
    })?;
    done.note = format!("({} of {} found)", done.found, done.ops);
    if missing && done.found != 0 {
        return Err(format!("{} missing keys were found", done.found));
    }
    Ok(done)
}

fn read_seq(store: &Store, reverse: bool) -> Result<Done, String> {
    let mut d = Done::default();
    let mut last: Option<Vec<u8>> = None;
    for kv in store.range(b"", None, reverse).map_err(|e| e.to_string())? {
        let (k, v) = kv.map_err(|e| e.to_string())?;
        if let Some(prev) = &last {
            let ordered = if reverse { *prev > k } else { *prev < k };
            if !ordered {
                return Err("keys out of order".into());
            }
        }
        d.ops += 1;
        d.bytes += (k.len() + v.len()) as u64;
        last = Some(k);
    }
    Ok(d)
}

fn seek_random(store: &Store, p: &Params) -> Result<Done, String> {
    // A seek re-reads every table's index, so do a tenth as many.
    let p = Params {
        num: (p.num / 10).max(1),
        benchmarks: Vec::new(),
        ..*p
    };
    let mut done = clients(store, &p, |h, t, share| {
        let mut rng = Rng::new(2000 + t);
        let mut d = Done::default();
        for _ in 0..share {
            let k = key(rng.below(p.num * 10));
            let page = h
                .scan(&k, None, false, 1, None)
                .map_err(|e| e.to_string())?;
            d.found += u64::from(page.first().is_some_and(|(found, _)| found == &k));
        }
        d.ops = share;
        Ok(d)
    })?;
    done.note = format!("({} of {} found)", done.found, done.ops);
    Ok(done)
}

fn delete_random(store: &Store, p: &Params) -> Result<Done, String> {
    clients(store, p, |h, t, share| {
        let mut rng = Rng::new(3000 + t);
        for _ in 0..share {
            h.delete(&key(rng.below(p.num)))
                .map_err(|e| e.to_string())?;
        }
        Ok(Done {
            ops: share,
            ..Done::default()
        })
    })
}
