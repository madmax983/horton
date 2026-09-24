//! Lifecycle differential fuzzer.
//!
//! Long randomized operation sequences — puts, deletes, range deletes, TTL
//! puts, atomic batches, snapshots, flushes, single compaction steps
//! interleaved with writes, WAL wraps, archive + re-ingest round trips, and
//! reopens — checked against a `BTreeMap` oracle, with
//! [`Db::check_invariants`](horton::Db::check_invariants) after every
//! operation and both scan directions compared against the oracle.
//!
//! The geometry is deliberately tight: a 40-block WAL wraps every few dozen
//! mutations, the memtable flushes often, and values range up to the
//! maximum size, so allocator reuse, compaction interleaving, and the
//! recovery floors are exercised together — the combinations the
//! single-feature suites never reach.

mod common;

use std::collections::BTreeMap;

use common::{Lcg, MemDevice, block_on, noop_waker};
use core::task::{Context, Poll};
use horton::{BlockDevice, Compaction, Config, Error, Progress, RevScan, Scan, WriteBatch};

const BLOCK: usize = 4096;
const KEY_MAX: usize = 16;
const VAL_MAX: usize = 300;
const TABLES: usize = 4;

type LDb = horton::Db<MemDevice<BLOCK>, BLOCK, KEY_MAX, VAL_MAX, 32, 2048, 4, TABLES, 256, 4096, 4>;
type LComp = Compaction<BLOCK, KEY_MAX, VAL_MAX, 256>;

/// Manifest slots 0/1, WAL `[2, 42)`, tables `[42, 1242)`.
const fn cfg() -> Config {
    Config::new(2, 42, 42, 1242, 0, 1)
}

/// One version of a key in the oracle.
#[derive(Clone, Debug)]
struct Ver {
    seq: u64,
    /// `None` for a tombstone.
    val: Option<Vec<u8>>,
    /// 0 = no expiry.
    expire_at: u64,
}

/// The reference model: every version of every key plus every range
/// tombstone, with horton's visibility rule applied at read time.
#[derive(Default)]
struct Oracle {
    keys: BTreeMap<Vec<u8>, Vec<Ver>>,
    rdels: Vec<(Vec<u8>, Vec<u8>, u64)>,
}

impl Oracle {
    /// Newest version at or below `max_seq`; hidden by a newer covering
    /// range tombstone, a tombstone winner, or expiry (an expired winner
    /// never falls through to an older version).
    fn visible(&self, key: &[u8], max_seq: u64, now: u64) -> Option<Vec<u8>> {
        let v = self
            .keys
            .get(key)?
            .iter()
            .filter(|v| v.seq <= max_seq)
            .max_by_key(|v| v.seq)?;
        let cover = self
            .rdels
            .iter()
            .filter(|(s, e, q)| *q <= max_seq && s.as_slice() <= key && key < e.as_slice())
            .map(|r| r.2)
            .max();
        if cover.is_some_and(|c| c > v.seq) {
            return None;
        }
        if v.expire_at != 0 && v.expire_at <= now {
            return None;
        }
        v.val.clone()
    }

    fn all(&self, max_seq: u64, now: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.keys
            .keys()
            .filter_map(|k| self.visible(k, max_seq, now).map(|v| (k.clone(), v)))
            .collect()
    }
}

/// How keys are drawn.
#[derive(Clone, Copy, Debug)]
enum Keys {
    /// 48 hot keys: heavy overwriting, everything overlaps.
    Hot,
    /// An increasing counter with occasional rewrites of recent keys:
    /// tables at every level cover disjoint ranges, so deeper-level jobs,
    /// movement down the levels, and capacity all come into play.
    Append,
}

struct Harness {
    keys: Keys,
    counter: u64,
    db: Box<LDb>,
    c: Box<LComp>,
    oracle: Oracle,
    snaps: Vec<u64>,
    now: u64,
    rng: Lcg,
    /// Set when the database reports it is genuinely full; the run stops.
    full: bool,
    op: usize,
    name: &'static str,
    /// The last few operations, for failure context.
    trail: Vec<String>,
}

/// Runs a mutation, making room (flush, then compaction) on a capacity
/// error and retrying once. `None` when the database is full.
macro_rules! retrying {
    ($h:expr, $call:expr) => {{
        let mut attempt = 0;
        loop {
            match block_on($call) {
                Ok(v) => break Some(v),
                Err(Error::TableFull | Error::ArenaFull | Error::NoSpace) if attempt == 0 => {
                    attempt += 1;
                    if $h.flush_all().is_err() {
                        $h.full = true;
                        break None;
                    }
                }
                Err(e) => panic!("op {} ({}): unexpected error {e:?}", $h.op, $h.name),
            }
        }
    }};
}

impl Harness {
    fn new(seed: u64, keys: Keys) -> Self {
        let mut db = Box::new(LDb::new(MemDevice::new(), cfg()));
        block_on(db.open()).unwrap();
        Self {
            keys,
            counter: 0,
            db,
            c: Box::new(LComp::new()),
            oracle: Oracle::default(),
            snaps: Vec::new(),
            now: 1,
            rng: Lcg::new(seed),
            full: false,
            op: 0,
            name: "",
            trail: Vec::new(),
        }
    }

    fn key(&mut self) -> Vec<u8> {
        match self.keys {
            Keys::Hot => format!("k{:02}", self.rng.next_bounded(48)).into_bytes(),
            Keys::Append => {
                if self.counter > 0 && self.rng.next_bounded(5) == 0 {
                    let back = 1 + u64::try_from(self.rng.next_bounded(200)).unwrap();
                    format!("a{:07}", self.counter.saturating_sub(back)).into_bytes()
                } else {
                    self.counter += 1;
                    format!("a{:07}", self.counter).into_bytes()
                }
            }
        }
    }

    fn val(&mut self) -> Vec<u8> {
        let len = if self.rng.next_bounded(10) < 7 {
            self.rng.next_bounded(33)
        } else {
            self.rng.next_bounded(VAL_MAX + 1)
        };
        (0..len)
            .map(|_| u8::try_from(self.rng.next_bounded(256)).unwrap())
            .collect()
    }

    /// Drives compaction until no job is pending. `Err` when a job cannot
    /// proceed (the level structure is full).
    fn drain(&mut self) -> Result<(), ()> {
        let mut jobs = 0;
        while self.db.compaction_pending() {
            self.c.purge_before = self.now;
            loop {
                match block_on(self.db.compact_step(&mut self.c)) {
                    Ok(Progress::More) => {}
                    Ok(Progress::Done) => break,
                    Err(Error::NoSpace) => return Err(()),
                    Err(e) => panic!("op {} ({}): compaction error {e:?}", self.op, self.name),
                }
            }
            jobs += 1;
            assert!(jobs < 1000, "compaction never settles");
        }
        Ok(())
    }

    /// Flushes, compacting first whenever level 0 is full.
    fn flush_all(&mut self) -> Result<(), ()> {
        loop {
            match block_on(self.db.flush()) {
                Ok(()) => return Ok(()),
                Err(Error::NoSpace) if self.db.compaction_pending() => self.drain()?,
                Err(Error::NoSpace) => return Err(()),
                Err(e) => panic!("op {} ({}): flush error {e:?}", self.op, self.name),
            }
        }
    }

    fn check_key(&self, key: &[u8], max_seq: u64) {
        let mut buf = [0u8; VAL_MAX];
        let got = block_on(self.db.get_at_with_time(key, &mut buf, max_seq, self.now))
            .unwrap_or_else(|e| panic!("op {} ({}): get error {e:?}", self.op, self.name))
            .map(|n| buf[..n].to_vec());
        let want = self.oracle.visible(key, max_seq, self.now);
        assert_eq!(
            got,
            want,
            "op {} ({}): get({:?}) at {max_seq} now {}",
            self.op,
            self.name,
            String::from_utf8_lossy(key),
            self.now
        );
    }

    fn scan(&self, max_seq: u64, reverse: bool) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut out = Vec::new();
        let mut key = [0u8; KEY_MAX];
        let mut val = [0u8; VAL_MAX];
        if reverse {
            let mut s = Box::new(RevScan::new(&*self.db));
            block_on(s.seek_prev_with_time(b"", None, max_seq, self.now)).unwrap();
            while let Some((kl, vl)) = block_on(s.prev(&mut key, &mut val)).unwrap() {
                out.push((key[..kl].to_vec(), val[..vl].to_vec()));
            }
            out.reverse();
        } else {
            let mut s = Box::new(Scan::new(&*self.db));
            block_on(s.seek_with_time(b"", None, max_seq, self.now)).unwrap();
            while let Some((kl, vl)) = block_on(s.next(&mut key, &mut val)).unwrap() {
                out.push((key[..kl].to_vec(), val[..vl].to_vec()));
            }
        }
        out
    }

    fn check_scans(&self) {
        let views: Vec<u64> = core::iter::once(u64::MAX)
            .chain(self.snaps.iter().copied())
            .collect();
        for view in views {
            let want = self.oracle.all(view, self.now);
            for reverse in [false, true] {
                let got = self.scan(view, reverse);
                assert!(
                    got == want,
                    "op {} ({}): scan reverse={reverse} at {view} now {} diverged:\n{}recent: {:?}",
                    self.op,
                    self.name,
                    self.now,
                    self.diff(&got, &want),
                    self.trail
                );
            }
        }
    }

    /// Human-readable difference between a scan result and the oracle,
    /// with the oracle's version history for each differing key.
    fn diff(&self, got: &[(Vec<u8>, Vec<u8>)], want: &[(Vec<u8>, Vec<u8>)]) -> String {
        use std::fmt::Write as _;
        let g: BTreeMap<_, _> = got.iter().cloned().collect();
        let w: BTreeMap<_, _> = want.iter().cloned().collect();
        let mut out = String::new();
        let mut keys: Vec<&Vec<u8>> = g.keys().chain(w.keys()).collect();
        keys.sort();
        keys.dedup();
        for k in keys {
            let (gv, wv) = (g.get(k), w.get(k));
            if gv == wv {
                continue;
            }
            let _ = writeln!(
                out,
                "  key {}: got {:?} want {:?}",
                String::from_utf8_lossy(k),
                gv.map(Vec::len),
                wv.map(Vec::len)
            );
            for v in self.oracle.keys.get(k).into_iter().flatten() {
                let _ = writeln!(
                    out,
                    "    seq {} {} expire {}",
                    v.seq,
                    v.val
                        .as_ref()
                        .map_or_else(|| "DEL".to_string(), |x| format!("len {}", x.len())),
                    v.expire_at
                );
            }
            for (a, b, q) in &self.oracle.rdels {
                if a.as_slice() <= k.as_slice() && k.as_slice() < b.as_slice() {
                    let _ = writeln!(
                        out,
                        "    rdel [{}, {}) seq {q}",
                        String::from_utf8_lossy(a),
                        String::from_utf8_lossy(b)
                    );
                }
            }
        }
        out
    }

    fn check_all_keys(&self) {
        let keys: Vec<Vec<u8>> = self.oracle.keys.keys().cloned().collect();
        let views: Vec<u64> = core::iter::once(u64::MAX)
            .chain(self.snaps.iter().copied())
            .collect();
        for k in &keys {
            for &v in &views {
                self.check_key(k, v);
            }
        }
    }

    /// Archives a random table (upload to a remote device, commit), then
    /// re-ingests it at L0. Net effect on every read: none.
    fn archive_round_trip(&mut self) {
        let levels: Vec<usize> = (0..4)
            .filter(|&l| !self.db.level_tables(l).unwrap().is_empty())
            .collect();
        if levels.is_empty() {
            return;
        }
        // Re-ingest needs room in L0.
        if self.db.level_tables(0).unwrap().len() >= TABLES && self.drain().is_err() {
            return;
        }
        if self.db.level_tables(0).unwrap().len() >= TABLES {
            return;
        }
        let level = levels[self.rng.next_bounded(levels.len())];
        let tables = self.db.level_tables(level).unwrap();
        if tables.is_empty() {
            return;
        }
        let t = tables[self.rng.next_bounded(tables.len())];
        let plan = self.db.archive_plan(level, t.id).unwrap();
        let mut remote = MemDevice::<BLOCK>::new();
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut buf = [0u8; BLOCK];
        for (k, id) in (t.first_block..t.end_block()).enumerate() {
            assert!(matches!(
                self.db.device().poll_read_block(&mut cx, id, &mut buf),
                Poll::Ready(Ok(()))
            ));
            assert!(matches!(
                remote.poll_write_block(&mut cx, u64::try_from(k).unwrap(), &buf),
                Poll::Ready(Ok(()))
            ));
        }
        match block_on(self.db.archive_commit(level, t.id)) {
            Ok(true) => {}
            Ok(false) | Err(Error::WouldResurrect { .. }) => return,
            Err(e) => panic!("op {} ({}): archive error {e:?}", self.op, self.name),
        }
        let sealed = plan.sealed();
        let r = block_on(self.db.ingest_table(&sealed, &remote, 0));
        assert_eq!(r, Ok(true), "op {} ({}): re-ingest", self.op, self.name);
    }

    fn reopen(&mut self) {
        let db = core::mem::replace(&mut self.db, Box::new(LDb::new(MemDevice::new(), cfg())));
        let dev = (*db).into_device();
        let mut db = Box::new(LDb::new(dev, cfg()));
        block_on(db.open()).unwrap_or_else(|e| panic!("op {}: reopen failed {e:?}", self.op));
        self.db = db;
        // Snapshots are in-memory only; a mid-job scratch belongs to the
        // old handle.
        self.snaps.clear();
        *self.c = LComp::new();
    }

    fn step(&mut self) {
        let r = self.rng.next_bounded(100);
        if r < 52 {
            self.mutate(r);
        } else {
            self.maintain(r);
        }
    }

    /// One mutation: put, delete, range delete, TTL put, or batch.
    fn mutate(&mut self, r: usize) {
        match r {
            0..=29 => {
                self.name = "put";
                let (k, v) = (self.key(), self.val());
                if let Some(seq) = retrying!(self, self.db.put(&k, &v)) {
                    self.oracle.keys.entry(k).or_default().push(Ver {
                        seq,
                        val: Some(v),
                        expire_at: 0,
                    });
                }
            }
            30..=37 => {
                self.name = "delete";
                let k = self.key();
                if let Some(seq) = retrying!(self, self.db.delete(&k)) {
                    self.oracle.keys.entry(k).or_default().push(Ver {
                        seq,
                        val: None,
                        expire_at: 0,
                    });
                }
            }
            38..=40 => {
                self.name = "delete_range";
                let (a, b) = (self.key(), self.key());
                let (a, b) = if a <= b { (a, b) } else { (b, a) };
                if a < b
                    && let Some(seq) = retrying!(self, self.db.delete_range(&a, &b))
                {
                    self.oracle.rdels.push((a, b, seq));
                }
            }
            41..=46 => {
                self.name = "put_with_ttl";
                let (k, v) = (self.key(), self.val());
                let expire = if self.rng.next_bounded(4) == 0 {
                    0
                } else {
                    self.now + 1 + u64::try_from(self.rng.next_bounded(40)).unwrap()
                };
                if let Some(seq) = retrying!(self, self.db.put_with_ttl(&k, &v, expire)) {
                    self.oracle.keys.entry(k).or_default().push(Ver {
                        seq,
                        val: Some(v),
                        expire_at: expire,
                    });
                }
            }
            _ => self.write_batch(),
        }
    }

    fn write_batch(&mut self) {
        self.name = "write_batch";
        let mut batch = WriteBatch::<KEY_MAX, VAL_MAX, 6>::new();
        let mut ops = Vec::new();
        for _ in 0..=self.rng.next_bounded(6) {
            let k = self.key();
            if self.rng.next_bounded(4) == 0 {
                batch.delete(&k).unwrap();
                ops.push((k, None));
            } else {
                let v = self.val();
                batch.put(&k, &v).unwrap();
                ops.push((k, Some(v)));
            }
        }
        if let Some(base) = retrying!(self, self.db.write(&batch)) {
            for (i, (k, v)) in ops.into_iter().enumerate() {
                self.oracle.keys.entry(k).or_default().push(Ver {
                    seq: base + u64::try_from(i).unwrap(),
                    val: v,
                    expire_at: 0,
                });
            }
        }
    }

    /// Everything else: reads, flush, compaction, snapshots, the clock,
    /// archive round trips, reopens, and scans.
    fn maintain(&mut self, r: usize) {
        match r {
            52..=59 => {
                self.name = "get";
                let k = self.key();
                self.check_key(&k, u64::MAX);
                if !self.snaps.is_empty() {
                    let s = self.snaps[self.rng.next_bounded(self.snaps.len())];
                    self.check_key(&k, s);
                }
            }
            60..=65 => {
                self.name = "flush";
                if self.flush_all().is_err() {
                    self.full = true;
                }
            }
            66..=75 => {
                self.name = "compact_step";
                self.c.purge_before = self.now;
                match block_on(self.db.compact_step(&mut self.c)) {
                    Ok(_) => {}
                    Err(Error::NoSpace) => self.full = true,
                    Err(e) => panic!("op {}: compact_step error {e:?}", self.op),
                }
            }
            76..=78 => {
                self.name = "drain";
                if self.drain().is_err() {
                    self.full = true;
                }
            }
            79..=81 => {
                self.name = "snapshot";
                if self.snaps.len() < 8 {
                    self.snaps.push(self.db.snapshot().unwrap());
                }
            }
            82..=84 => {
                self.name = "release";
                if !self.snaps.is_empty() {
                    let i = self.rng.next_bounded(self.snaps.len());
                    let s = self.snaps.swap_remove(i);
                    self.db.release_snapshot(s);
                }
            }
            85..=88 => {
                self.name = "tick";
                self.now += 1 + u64::try_from(self.rng.next_bounded(15)).unwrap();
            }
            89..=91 => {
                self.name = "archive_round_trip";
                self.archive_round_trip();
            }
            92..=93 => {
                self.name = "reopen";
                self.reopen();
            }
            _ => {
                self.name = "scan";
                self.check_scans();
            }
        }
    }
}

fn run(seed: u64, keys: Keys, ops: usize) -> usize {
    let mut h = Harness::new(seed, keys);
    for op in 0..ops {
        h.op = op;
        h.step();
        h.trail.push(format!("{op}:{}", h.name));
        if h.trail.len() > 40 {
            h.trail.remove(0);
        }
        if let Err(e) = h.db.check_invariants() {
            panic!(
                "seed {seed:#x} {keys:?} op {op} ({}): invariant violated: {e}\nrecent: {:?}",
                h.name, h.trail
            );
        }
        if h.full {
            eprintln!("seed {seed:#x} {keys:?}: database full after {op} ops");
            return op;
        }
        if op % 50 == 49 {
            h.name = "verify-all";
            h.check_all_keys();
        }
    }
    h.name = "final";
    h.check_all_keys();
    h.check_scans();
    h.reopen();
    h.check_all_keys();
    h.check_scans();
    ops
}

#[test]
fn lifecycle_fuzz_hot_keys() {
    // Debug builds are ~20x slower; release (CI's second test job) runs the
    // full depth.
    let ops = if cfg!(miri) {
        60
    } else if cfg!(debug_assertions) {
        250
    } else {
        700
    };
    for seed in 0..8u64 {
        let done = run(0x5eed_0000 + seed, Keys::Hot, ops);
        assert_eq!(done, ops, "seed {seed}: stopped early (database full)");
    }
}

#[test]
fn lifecycle_fuzz_append_keys() {
    let ops = if cfg!(miri) {
        60
    } else if cfg!(debug_assertions) {
        400
    } else {
        1500
    };
    for seed in 0..8u64 {
        let done = run(0xa99e_0000 + seed, Keys::Append, ops);
        assert_eq!(done, ops, "seed {seed}: stopped early (database full)");
    }
}
