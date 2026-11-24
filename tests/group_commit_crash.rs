//! Randomized crash test for group commit across WAL blocks (ADR-0028).
//!
//! A device with a volatile write cache: every write since the last flush
//! is pending, and a crash decides each pending write's fate (lands,
//! lost, torn with zeros after the tear, or torn in place with the old
//! bytes after it). Each seed runs four rounds of: an acknowledged put,
//! sometimes a flush, sometimes a committed group, sometimes a batch over
//! several blocks whose block write fails and a put after it, then an
//! in-flight group whose flush fails (and sometimes one of its block
//! writes), a crash, and a reopen that may itself crash part way. One unit
//! in four spans WAL blocks (ADR-0029). After every reopen:
//!
//! - every acknowledged write is there;
//! - every unit of every group is there whole or not at all;
//! - what an earlier recovery decided about a group is unchanged.
//!
//! Adversarial review of the first version found writes coming back
//! (a block stranded past a hole; bytes left behind a torn rewrite, whose
//! sequence numbers recovery had reused) and a database that would not
//! open (records of a failed group overflowing the memtable at replay).

mod common;

use std::cell::Cell;
use std::collections::BTreeMap;
use std::task::{Context, Poll};

use common::block_on;
use horton::{BlockDevice, Compaction, Config, Error, GroupOp, Progress};

const B: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Fault;

/// What a crash does to one pending write.
#[derive(Debug, Clone, Copy)]
enum Fate {
    Land,
    Lose,
    /// The first `n` bytes land; the rest read as zeros.
    Torn(usize),
    /// The first `n` bytes land; the rest keep the block's old bytes.
    TornInPlace(usize),
}

/// A device with a volatile write cache.
#[derive(Default)]
struct Cache {
    durable: BTreeMap<u64, Box<[u8; B]>>,
    pending: Vec<(u64, Box<[u8; B]>)>,
    writes: usize,
    fail_write: Cell<Option<usize>>,
    fail_flush: Cell<bool>,
}

impl Cache {
    fn view(&self, id: u64) -> [u8; B] {
        let mut b = [0u8; B];
        if let Some(d) = self.durable.get(&id) {
            b.copy_from_slice(&d[..]);
        }
        for (i, d) in &self.pending {
            if *i == id {
                b.copy_from_slice(&d[..]);
            }
        }
        b
    }

    /// Power fails: each pending write meets its fate, in order.
    fn crash(&mut self, rng: &mut Rng) {
        for (id, data) in std::mem::take(&mut self.pending) {
            let old = self.view(id);
            let new: [u8; B] = match rng.fate() {
                Fate::Land => *data,
                Fate::Lose => continue,
                Fate::Torn(n) => {
                    let mut b = [0u8; B];
                    b[..n].copy_from_slice(&data[..n]);
                    b
                }
                Fate::TornInPlace(n) => {
                    let mut b = old;
                    b[..n].copy_from_slice(&data[..n]);
                    b
                }
            };
            self.durable.insert(id, Box::new(new));
        }
        self.fail_write.set(None);
        self.fail_flush.set(false);
    }
}

impl BlockDevice for Cache {
    type Error = Fault;
    const BLOCK: usize = B;

    fn poll_read_block(
        &self,
        _: &mut Context<'_>,
        id: u64,
        buf: &mut [u8],
    ) -> Poll<Result<(), Fault>> {
        buf[..B].copy_from_slice(&self.view(id));
        Poll::Ready(Ok(()))
    }

    fn poll_write_block(
        &mut self,
        _: &mut Context<'_>,
        id: u64,
        buf: &[u8],
    ) -> Poll<Result<(), Fault>> {
        let n = self.writes;
        self.writes += 1;
        if self.fail_write.get() == Some(n) {
            self.fail_write.set(None);
            return Poll::Ready(Err(Fault));
        }
        let mut b = Box::new([0u8; B]);
        b.copy_from_slice(&buf[..B]);
        self.pending.push((id, b));
        Poll::Ready(Ok(()))
    }

    fn poll_flush(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Fault>> {
        if self.fail_flush.get() {
            return Poll::Ready(Err(Fault));
        }
        for (id, d) in std::mem::take(&mut self.pending) {
            self.durable.insert(id, d);
        }
        Poll::Ready(Ok(()))
    }
}

type GroupDb = horton::Db<Cache, 4096, 256, 1024, 256, 65536, 7, 4, 1024, 8>;

/// A small LCG: deterministic, no dependency.
struct Rng(u64);

impl Rng {
    const fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 33
    }

    fn below(&mut self, n: usize) -> usize {
        usize::try_from(self.next() % u64::try_from(n).expect("n fits u64")).expect("below n")
    }

    fn fate(&mut self) -> Fate {
        match self.below(6) {
            0 | 1 => Fate::Land,
            2 => Fate::Lose,
            3 => Fate::Torn(self.below(B)),
            _ => Fate::TornInPlace(self.below(B)),
        }
    }
}

type Unit = Vec<(Vec<u8>, Vec<u8>)>;

fn units(rng: &mut Rng, tag: &str, n: usize) -> Vec<Unit> {
    (0..n)
        .map(|u| {
            // One unit in four is large: up to 20 records, which spans
            // up to seven WAL blocks (ADR-0029).
            let ops = if rng.below(4) == 0 {
                4 + rng.below(17)
            } else {
                1 + rng.below(3)
            };
            (0..ops)
                .map(|j| {
                    let key = format!("{tag}u{u:02}k{j}").into_bytes();
                    let len = [10, 300, 900][rng.below(3)];
                    let mut val = format!("{tag}-{u}-{j}:").into_bytes();
                    val.resize(len, b'.');
                    (key, val)
                })
                .collect()
        })
        .collect()
}

fn write_group(db: &mut GroupDb, data: &[Unit]) -> Result<horton::GroupCommit, Error<Fault>> {
    let ops: Vec<Vec<GroupOp<'_>>> = data
        .iter()
        .map(|u| u.iter().map(|(k, v)| GroupOp::put(k, v)).collect())
        .collect();
    let refs: Vec<&[GroupOp<'_>]> = ops.iter().map(Vec::as_slice).collect();
    block_on(db.write_group(&refs))
}

fn get(db: &GroupDb, key: &[u8]) -> Option<Vec<u8>> {
    let mut buf = [0u8; 1024];
    block_on(db.get(key, &mut buf))
        .expect("get")
        .map(|n| buf[..n].to_vec())
}

/// Which units of `data` are present, or which one is partial.
fn present(db: &GroupDb, data: &[Unit]) -> Result<Vec<bool>, String> {
    data.iter()
        .enumerate()
        .map(|(u, unit)| {
            let got: Vec<bool> = unit
                .iter()
                .map(|(k, v)| {
                    let found = get(db, k);
                    assert!(
                        found.as_ref().is_none_or(|x| x == v),
                        "a wrong value for unit {u}"
                    );
                    found.is_some()
                })
                .collect();
            if got.iter().all(|&g| g) || got.iter().all(|&g| !g) {
                Ok(got[0])
            } else {
                Err(format!("unit {u} partial: {got:?}"))
            }
        })
        .collect()
}

fn open(dev: Cache, cfg: Config) -> GroupDb {
    let mut db = GroupDb::new(dev, cfg);
    block_on(db.open()).expect("open");
    db
}

/// Opens, sometimes crashing during open's own writes first.
fn reopen(rng: &mut Rng, dev: Cache, cfg: Config) -> GroupDb {
    if rng.below(3) == 0 {
        dev.fail_flush.set(true);
        let mut db = GroupDb::new(dev, cfg);
        let opened = block_on(db.open());
        let mut dev = db.into_device();
        if opened.is_err() {
            dev.crash(rng);
        }
        dev.fail_flush.set(false);
        return open(dev, cfg);
    }
    open(dev, cfg)
}

fn flush(db: &mut GroupDb) {
    loop {
        match block_on(db.flush()) {
            Ok(()) => return,
            Err(Error::NeedsCompaction) => {
                let mut c = Box::new(Compaction::<4096, 256, 1024, 1024>::new());
                while block_on(db.compact_step(&mut c)).expect("compact") != Progress::Done {}
            }
            Err(e) => panic!("flush: {e:?}"),
        }
    }
}

/// A `Db::write` of one spanning batch (6 to 20 records of 900 bytes,
/// two to seven WAL blocks) whose block write number `fail` fails: an
/// error, or acknowledged when `fail` lies past its blocks. `None` when
/// it was refused before any I/O.
fn failed_batch(db: &mut GroupDb, unit: &Unit, fail: usize) -> Option<bool> {
    let mut b = horton::WriteBatch::<256, 1024, 32>::new();
    for (k, v) in unit {
        b.put(k, v).expect("op");
    }
    db.device().fail_write.set(Some(db.device().writes + fail));
    let res = block_on(db.write(&b));
    db.device().fail_write.set(None);
    match res {
        Ok(_) => Some(true),
        Err(Error::Device(Fault)) => Some(false),
        Err(Error::WalFull | Error::TableFull | Error::ArenaFull) => None,
        Err(e) => panic!("write: {e:?}"),
    }
}

/// Sometimes a spanning batch fails part way (a block write fails, no
/// crash) and stays on the device unflushed until the next write syncs
/// it; sometimes an acknowledged put follows it, landing behind its dead
/// blocks (review findings, ADR-0029). Returns the failed batch's unit,
/// if one failed: its fate is decided at the next reopen.
fn failed_step(
    rng: &mut Rng,
    db: &mut GroupDb,
    round: usize,
    acked: &mut Vec<(Vec<u8>, Vec<u8>)>,
) -> Vec<Unit> {
    let mut undecided = Vec::new();
    if rng.below(3) == 0 {
        let n = 6 + rng.below(15);
        let unit: Unit = (0..n)
            .map(|j| {
                let mut val = format!("f{round}-{j}:").into_bytes();
                val.resize(900, b'.');
                (format!("f{round}k{j:02}").into_bytes(), val)
            })
            .collect();
        let fail = rng.below(8);
        let mut out = failed_batch(db, &unit, fail);
        if out.is_none() {
            flush(db);
            out = failed_batch(db, &unit, fail);
        }
        match out {
            Some(true) => acked.extend(unit.iter().cloned()),
            Some(false) => undecided.push(unit),
            None => {}
        }
        if rng.below(2) == 0 {
            let key = format!("after{round}").into_bytes();
            if block_on(db.put(&key, b"w")).is_err() {
                flush(db);
                block_on(db.put(&key, b"w")).expect("put after a flush");
            }
            acked.push((key, b"w".to_vec()));
        }
    }
    undecided
}

/// One seed: four rounds of writes, a group in flight, a crash, a reopen.
fn run(seed: u64, cfg: Config) -> Result<(), String> {
    let mut rng = Rng(seed * 7919 + 1);
    let mut db = open(Cache::default(), cfg);
    if rng.below(2) == 0 {
        // Some history, flushed, so the WAL has stale blocks to read past.
        for i in 0..100 + rng.below(28) {
            let key = format!("old{i}");
            match block_on(db.put(key.as_bytes(), &[b'o'; 300])) {
                Ok(_) => {}
                Err(Error::WalFull | Error::TableFull | Error::ArenaFull) => {
                    flush(&mut db);
                    block_on(db.put(key.as_bytes(), &[b'o'; 300])).expect("put");
                }
                Err(e) => panic!("put: {e:?}"),
            }
        }
        flush(&mut db);
    }
    let mut acked: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut decided: Vec<(Vec<Unit>, Vec<bool>)> = Vec::new();
    for round in 0..4 {
        let key = format!("ack{round}").into_bytes();
        if block_on(db.put(&key, b"v")).is_err() {
            flush(&mut db);
            block_on(db.put(&key, b"v")).expect("put after a flush");
        }
        acked.push((key, b"v".to_vec()));
        if rng.below(4) == 0 {
            flush(&mut db);
        }
        if rng.below(2) == 0 {
            let n = 1 + rng.below(10);
            let g = units(&mut rng, &format!("c{round}"), n);
            let done = write_group(&mut db, &g).or_else(|_| {
                flush(&mut db);
                write_group(&mut db, &g)
            });
            match done {
                Ok(c) => acked.extend(g[..c.units].iter().flatten().cloned()),
                Err(_) => flush(&mut db),
            }
        }
        let undecided = failed_step(&mut rng, &mut db, round, &mut acked);
        // The group in flight: its flush fails, and sometimes a write.
        let n = 2 + rng.below(30);
        let g = units(&mut rng, &format!("g{round}"), n);
        let fail_at = (rng.below(3) == 0).then(|| rng.below(8));
        let arm = |db: &GroupDb| {
            if let Some(k) = fail_at {
                db.device().fail_write.set(Some(db.device().writes + k));
            }
            db.device().fail_flush.set(true);
        };
        arm(&db);
        let mut result = write_group(&mut db, &g);
        if matches!(
            result,
            Err(Error::WalFull | Error::TableFull | Error::ArenaFull)
        ) && db.device().pending.is_empty()
        {
            db.device().fail_flush.set(false);
            db.device().fail_write.set(None);
            flush(&mut db);
            arm(&db);
            result = write_group(&mut db, &g);
        }
        let tried = result.map_or(g.len(), |c| c.units);
        let mut dev = db.into_device();
        dev.crash(&mut rng);
        db = reopen(&mut rng, dev, cfg);
        for (k, v) in &acked {
            if get(&db, k).as_deref() != Some(v.as_slice()) {
                return Err(format!(
                    "round {round}: lost {}",
                    String::from_utf8_lossy(k)
                ));
            }
        }
        for (gi, (data, was)) in decided.iter().enumerate() {
            let now = present(&db, data).map_err(|e| format!("round {round}: group {gi} {e}"))?;
            if &now != was {
                return Err(format!(
                    "round {round}: group {gi} changed {was:?} -> {now:?}"
                ));
            }
        }
        let data = g[..tried].to_vec();
        let now = present(&db, &data).map_err(|e| format!("round {round}: new group {e}"))?;
        decided.push((data, now));
        if !undecided.is_empty() {
            let now =
                present(&db, &undecided).map_err(|e| format!("round {round}: failed batch {e}"))?;
            decided.push((undecided, now));
        }
    }
    Ok(())
}

/// The seed review found a partial unit with (bytes left behind a torn
/// rewrite of a block whose sequence numbers recovery had reused), and a
/// spread of others, on a large WAL and on one small enough to wrap.
#[test]
fn crashes_mid_group_keep_the_contract() {
    let large = Config::new(8, 136, 136, 4224, 0, 4);
    let small = Config::new(8, 40, 40, 4224, 0, 4);
    let seeds = if cfg!(debug_assertions) { 150 } else { 1500 };
    let mut failed = Vec::new();
    for seed in std::iter::once(5042).chain(0..seeds) {
        for (name, cfg) in [("large WAL", large), ("small WAL", small)] {
            if let Err(e) = run(seed, cfg) {
                failed.push(format!("seed {seed}, {name}: {e}"));
            }
        }
    }
    assert!(
        failed.is_empty(),
        "{} failures:\n{}",
        failed.len(),
        failed.join("\n")
    );
}
