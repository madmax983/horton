//! The crash test: kill the writing process, reopen, and check.
//!
//! Each cycle starts a child process (this program, `crash-child`) whose
//! writer threads apply a deterministic stream of operations to the
//! store: puts, deletes, atomic batches and range deletes, each thread on
//! its own keys. A thread prints `t n` once operation `n` has returned
//! `Ok`, so every line the parent reads is an acknowledged write. A
//! reader thread checks, through snapshots, that no batch is ever seen
//! half applied. The parent kills the child (`SIGKILL` on Unix,
//! `TerminateProcess` on Windows) at a random moment, which lands in the
//! middle of WAL commits, flushes and compactions, then reopens the store
//! and compares all of it with a model built from the acknowledged
//! operations:
//!
//! - every acknowledged operation is there;
//! - nothing else is, except each thread's one operation in flight when
//!   the process died, which may have landed, and then landed whole;
//! - no value is corrupt.
//!
//! The next cycle's child resumes each thread after its last acknowledged
//! operation (replaying an in-flight one that landed is harmless: the
//! operations are deterministic).
//!
//! Killing a process tests crash safety, not power loss: the operating
//! system still writes back what the process wrote. The flight recorder's
//! `torture` covers torn writes on a simulated flash chip.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use crate::Common;
use crate::bench::Rng;
use crate::store::{Batch, BatchOp, Handle, Store};

/// The child's command name.
pub const CHILD: &str = "crash-child";

/// Keys each thread overwrites, deletes and range-deletes.
const KEYS: u64 = 1000;

/// A child that outlives its parent stops after this long.
const CHILD_LIFETIME: Duration = Duration::from_mins(2);

/// One step of a thread's operation stream.
#[derive(Debug, Clone)]
enum Op {
    Write(Batch),
    Range(Vec<u8>, Vec<u8>),
}

const fn mix(t: u64, n: u64) -> u64 {
    let mut r = Rng::new((t << 40) ^ n ^ 0x00C0_FFEE);
    r.next()
}

fn wkey(t: u64, i: u64) -> Vec<u8> {
    format!("w{t}/{i:03}").into_bytes()
}

/// The value thread `t` writes at step `n`: names both, then a filler
/// whose length and letter depend on `n`, so a stale or torn value
/// cannot pass for the right one.
fn value(t: u64, n: u64) -> Vec<u8> {
    let h = mix(t, n);
    let mut v = format!("{t}:{n}:").into_bytes();
    let fill = usize::try_from((h >> 8) % 400).unwrap_or(0);
    v.resize(v.len() + fill, b'a' + u8::try_from(n % 26).unwrap_or(0));
    v
}

/// Thread `t`'s operation `n`.
fn op(t: u64, n: u64) -> Op {
    let hash = mix(t, n);
    let key = wkey(t, hash % KEYS);
    let mut batch = Batch::new();
    match n % 8 {
        0..=4 => {
            batch.put(&key, &value(t, n));
        }
        5 => {
            batch.delete(&key);
        }
        6 => {
            // Atomic: both halves carry the same value, and a key goes.
            let val = value(t, n);
            batch
                .put(format!("p{t}/a").as_bytes(), &val)
                .put(format!("p{t}/b").as_bytes(), &val)
                .delete(&key);
        }
        _ => {
            // A range of 1 to 20 keys.
            let first = hash % (KEYS - 20);
            let len = 1 + (hash >> 32) % 20;
            return Op::Range(wkey(t, first), wkey(t, first + len));
        }
    }
    Op::Write(batch)
}

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

fn apply(model: &mut Model, op: &Op) {
    match op {
        Op::Write(b) => {
            for o in b.ops() {
                match o {
                    BatchOp::Put(k, v) => {
                        model.insert(k.clone(), v.clone());
                    }
                    BatchOp::Delete(k) => {
                        model.remove(k);
                    }
                }
            }
        }
        Op::Range(s, e) => model.retain(|k, _| k < s || k >= e),
    }
}

fn run_op(h: &Handle, op: Op) -> Result<(), String> {
    match op {
        Op::Write(b) => h.write(b),
        Op::Range(s, e) => h.delete_range(&s, &e),
    }
    .map_err(|e| e.to_string())
}

/// Thread `t`'s keys in `m`.
fn owned(m: &Model, t: u64) -> Model {
    let (w, p) = (format!("w{t}/"), format!("p{t}/"));
    m.iter()
        .filter(|(k, _)| k.starts_with(w.as_bytes()) || k.starts_with(p.as_bytes()))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// The child: write until killed.
pub fn child(common: &Common, args: &[String]) -> Result<(), String> {
    let starts: Vec<u64> = args
        .first()
        .ok_or("usage crash-child STARTS")?
        .split(',')
        .map(|s| s.parse().map_err(|e| format!("{s}: {e}")))
        .collect::<Result<_, _>>()?;
    let store = common.open()?;
    println!("ready");
    let deadline = Instant::now() + CHILD_LIFETIME;
    std::thread::scope(|s| {
        for (t, &start) in (0u64..).zip(&starts) {
            let h = store.handle();
            s.spawn(move || {
                let mut n = start;
                while Instant::now() < deadline {
                    if let Err(e) = run_op(&h, op(t, n)) {
                        println!("error {t} {n} {e}");
                        std::process::exit(3);
                    }
                    println!("{t} {n}");
                    n += 1;
                }
            });
        }
        // The reader: a batch is never visible in part.
        let h = store.handle();
        let threads = starts.len() as u64;
        s.spawn(move || {
            while Instant::now() < deadline {
                let Ok(snap) = h.snapshot() else { continue };
                for t in 0..threads {
                    let a = h.get_at(format!("p{t}/a").as_bytes(), &snap);
                    let b = h.get_at(format!("p{t}/b").as_bytes(), &snap);
                    if a != b {
                        println!("violation thread {t}: p/a and p/b differ in one snapshot");
                        std::process::exit(4);
                    }
                }
            }
        });
    });
    Ok(())
}

struct Params {
    cycles: u64,
    threads: u64,
    seed: u64,
    sync: bool,
}

fn parse(common: &Common, args: &[String]) -> Result<Params, String> {
    let mut p = Params {
        cycles: 20,
        threads: 4,
        seed: 0x5eed,
        sync: common.sync,
    };
    let mut it = args.iter();
    while let Some(a) = it.next() {
        let mut num = || -> Result<u64, String> {
            it.next()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| format!("usage {a} needs a number"))
        };
        match a.as_str() {
            "--threads" => p.threads = num()?.clamp(1, 16),
            "--seed" => p.seed = num()?,
            n if n.parse::<u64>().is_ok() => p.cycles = n.parse().unwrap_or(p.cycles),
            other => return Err(format!("usage unknown crash argument {other}")),
        }
    }
    Ok(p)
}

/// What one child left: the lines it printed.
fn run_child(common: &Common, starts: &[u64], kill_after: Duration) -> Result<Vec<String>, String> {
    let exe = std::env::current_exe().map_err(|e| e.to_string())?;
    let mut cmd = Command::new(exe);
    cmd.arg("--db")
        .arg(&common.db)
        .arg("--size-mb")
        .arg(common.size_mb.to_string());
    if !common.sync {
        cmd.arg("--no-sync");
    }
    let starts = starts
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let mut child = cmd
        .arg(CHILD)
        .arg(starts)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|e| format!("starting the child: {e}"))?;
    let stdout = child.stdout.take().ok_or("no child stdout")?;
    let (ready_tx, ready_rx) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        let mut lines = Vec::new();
        for line in BufReader::new(stdout).lines() {
            let Ok(line) = line else { break };
            if line == "ready" {
                let _ = ready_tx.send(());
            } else {
                lines.push(line);
            }
        }
        lines
    });
    let ready = ready_rx.recv_timeout(Duration::from_mins(1));
    if ready.is_ok() {
        std::thread::sleep(kill_after);
    }
    // Already exited (it failed) is fine: its output says why.
    let _ = child.kill();
    let status = child.wait().map_err(|e| e.to_string())?;
    let lines = reader.join().map_err(|_| "the reader panicked")?;
    if ready.is_err() {
        return Err(format!("the child never opened the store ({status})"));
    }
    Ok(lines)
}

/// The parent's view: what every thread has had acknowledged.
struct Acked {
    model: Model,
    /// Each thread's next operation: the one in flight at the kill.
    next: Vec<u64>,
}

impl Acked {
    /// Applies the acknowledgements a child printed; returns how many.
    fn absorb(&mut self, lines: &[String]) -> Result<u64, String> {
        let mut count = 0;
        for line in lines {
            let mut parts = line.split(' ');
            let (Some(t), Some(n), None) = (parts.next(), parts.next(), parts.next()) else {
                return Err(format!("the child said: {line}"));
            };
            let (Ok(t), Ok(n)) = (t.parse::<u64>(), n.parse::<u64>()) else {
                // A line torn by the kill: never acknowledged.
                continue;
            };
            let slot = usize::try_from(t).map_err(|e| e.to_string())?;
            if self.next.get(slot) != Some(&n) {
                return Err(format!("thread {t} acknowledged {n} out of order"));
            }
            apply(&mut self.model, &op(t, n));
            self.next[slot] = n + 1;
            count += 1;
        }
        Ok(count)
    }

    /// Checks everything `store` holds against the model: each thread's
    /// keys are as acknowledged, or as acknowledged plus the whole of its
    /// in-flight operation. Returns the contents and how many in-flight
    /// operations landed.
    fn verify(&self, store: &Handle) -> Result<(Model, u64), String> {
        let mut actual = Model::new();
        for kv in store.range(b"", None, false).map_err(|e| e.to_string())? {
            let (k, v) = kv.map_err(|e| format!("scan: {e}"))?;
            actual.insert(k, v);
        }
        let (mut seen, mut landed) = (0, 0);
        for (t, &next) in (0u64..).zip(&self.next) {
            let got = owned(&actual, t);
            seen += got.len();
            let want = owned(&self.model, t);
            if got == want {
                continue;
            }
            let mut with_inflight = self.model.clone();
            apply(&mut with_inflight, &op(t, next));
            if got == owned(&with_inflight, t) {
                landed += 1;
                continue;
            }
            return Err(format!(
                "thread {t} after op {}: {}",
                next.saturating_sub(1),
                diff(&want, &got)
            ));
        }
        if seen != actual.len() {
            return Err(format!("{} keys belong to no writer", actual.len() - seen));
        }
        // Point reads agree with the scan.
        for (k, v) in actual.iter().step_by(7) {
            if store.get(k).map_err(|e| e.to_string())?.as_ref() != Some(v) {
                return Err(format!(
                    "get disagrees with the scan on {}",
                    String::from_utf8_lossy(k)
                ));
            }
        }
        Ok((actual, landed))
    }
}

/// The first few keys where `want` and `got` differ.
fn diff(want: &Model, got: &Model) -> String {
    let show = |v: Option<&Vec<u8>>| {
        v.map_or_else(
            || "absent".into(),
            |v| String::from_utf8_lossy(&v[..v.len().min(12)]).into_owned(),
        )
    };
    let lines: Vec<String> = want
        .keys()
        .chain(got.keys())
        .filter(|k| want.get(*k) != got.get(*k))
        .take(5)
        .map(|k| {
            format!(
                "{}: want {}, got {}",
                String::from_utf8_lossy(k),
                show(want.get(k)),
                show(got.get(k))
            )
        })
        .collect();
    lines.join("; ")
}

pub fn run(common: &Common, args: &[String]) -> Result<(), String> {
    let p = parse(common, args)?;
    let path = common.db.with_file_name("crash.db");
    let _ = std::fs::remove_file(&path);
    let common = Common {
        db: path,
        size_mb: 128,
        sync: p.sync,
    };
    if let Some(dir) = common.db.parent() {
        std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
    }
    println!(
        "crash test: {} cycles, {} writer threads, {}; {}",
        p.cycles,
        p.threads,
        if p.sync {
            "fdatasync on"
        } else {
            "no fdatasync"
        },
        common.db.display()
    );
    let mut rng = Rng::new(p.seed);
    let mut acked = Acked {
        model: Model::new(),
        next: vec![0; usize::try_from(p.threads).map_err(|e| e.to_string())?],
    };
    let (mut writes, mut landed, mut replayed) = (0, 0, 0);
    let mut verified = Model::new();
    let started = Instant::now();
    for cycle in 1..=p.cycles {
        let kill_after = Duration::from_millis(20 + rng.below(400));
        let lines = run_child(&common, &acked.next, kill_after)?;
        let this_cycle = acked
            .absorb(&lines)
            .map_err(|e| format!("cycle {cycle}: {e}"))?;
        writes += this_cycle;

        let (store, report) =
            Store::open(&common.db, common.options()).map_err(|e| format!("cycle {cycle}: {e}"))?;
        replayed += report.recovered_records;
        let (actual, l) = acked
            .verify(&store)
            .map_err(|e| format!("cycle {cycle}: {e}"))?;
        landed += l;
        let s = store.stats().map_err(|e| e.to_string())?;
        println!(
            "cycle {cycle:>3}: killed after {:>3} ms, {this_cycle:>5} writes acknowledged; \
             reopened: {:>4} WAL records replayed, tables {:?} ({} range-tombstone blocks), \
             {} keys match",
            kill_after.as_millis(),
            report.recovered_records,
            s.level_tables,
            s.rdel_blocks,
            actual.len()
        );
        verified = actual;
    }

    // One last pass: compaction changes nothing a reader sees.
    let (store, _) = Store::open(&common.db, common.options()).map_err(|e| e.to_string())?;
    store.compact().map_err(|e| e.to_string())?;
    let mut actual = Model::new();
    for kv in store.range(b"", None, false).map_err(|e| e.to_string())? {
        let (k, v) = kv.map_err(|e| e.to_string())?;
        actual.insert(k, v);
    }
    if actual != verified {
        return Err(format!(
            "compaction changed what the store holds: {}",
            diff(&verified, &actual)
        ));
    }
    println!();
    println!("process kills          {}", p.cycles);
    println!("acknowledged writes    {writes}, every one present");
    println!("in flight at the kill  {landed} landed whole, the rest not at all; never in part");
    println!("WAL records replayed   {replayed}");
    println!("after compacting       {} keys, unchanged", actual.len());
    println!("time                   {:.1?}", started.elapsed());
    drop(store);
    let _ = std::fs::remove_file(&common.db);
    Ok(())
}
