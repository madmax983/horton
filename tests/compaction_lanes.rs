//! Two compaction jobs at a time (ADR-0025): `Db::compact_step_in`
//! drives a job in each `Lane`, and the two run side by side when they
//! share no table and write to different levels.
//!
//! RED: `Db::compact_step_in`, `Lane` and `COMPACTION_LANES` did not
//! exist; a `Db` knew one job, and a second scratch's first step
//! abandoned it.
//!
//! A job is a reorganization: no read may change because of one, or of
//! two interleaved in any order. So the tests drive both lanes at random
//! between writes, flushes, range deletes and snapshots, and hold every
//! read (now, and as of every live snapshot) to a model; they check the
//! structure after every step; and they cut the power at points where
//! two jobs are in flight.

use core::task::{Context, Poll};
use std::collections::BTreeMap;

mod common;
use common::{Lcg, MemDevice, block_on};
use horton::{BlockDevice, COMPACTION_LANES, Compaction, Error, Lane, Progress};

/// A memory device that counts its writes and, from the `cut_at`-th on,
/// drops them: the image is then what a power cut at that write leaves.
#[derive(Clone)]
struct CutDevice {
    inner: MemDevice<4096>,
    writes: u64,
    cut_at: u64,
}

impl CutDevice {
    fn new(cut_at: u64) -> Self {
        Self {
            inner: MemDevice::new(),
            writes: 0,
            cut_at,
        }
    }

    const fn is_cut(&self) -> bool {
        self.writes >= self.cut_at
    }
}

impl BlockDevice for CutDevice {
    type Error = core::convert::Infallible;
    const BLOCK: usize = 4096;

    fn poll_read_block(
        &self,
        cx: &mut Context<'_>,
        id: u64,
        buf: &mut [u8],
    ) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_read_block(cx, id, buf)
    }

    fn poll_write_block(
        &mut self,
        cx: &mut Context<'_>,
        id: u64,
        buf: &[u8],
    ) -> Poll<Result<(), Self::Error>> {
        self.writes += 1;
        if self.writes > self.cut_at {
            return Poll::Ready(Ok(()));
        }
        self.inner.poll_write_block(cx, id, buf)
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_flush(cx)
    }
}

/// `common::test_config` with the table region cut to 28 slots of 48
/// blocks: small enough that levels fill and jobs split, and large
/// enough that an output's worst-case split margin (five blocks with
/// six snapshots live, in this shape) is a small part of it. In slots
/// much smaller, outputs come out a third full and any compaction, one
/// job or two, fills the region with them.
const fn config() -> horton::Config {
    horton::Config::new(8, 136, 136, 136 + 28 * 48, 0, 4)
}

type Db = common::TestDb<CutDevice>;
type Scratch = Compaction<4096, 256, 1024, 1024>;
type Model = BTreeMap<Vec<u8>, Vec<u8>>;

/// `common::TestDb`'s shape: seven levels of four tables.
const LEVELS: usize = 7;
const TABLES: u32 = 4;

/// Keys of the usual history: enough that several levels fill.
const KEYS: usize = 3000;

fn key(i: usize) -> Vec<u8> {
    format!("key-{i:04}").into_bytes()
}

enum Op {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
    DeleteRange(Vec<u8>, Vec<u8>),
}

/// The next write: overwrites, deletes and (with `ranges`) range
/// deletes, so that jobs have versions and tombstones to drop.
fn next_op(rng: &mut Lcg, keys: usize, round: usize, ranges: bool) -> Op {
    let k = rng.next_bounded(keys);
    match rng.next_bounded(20) {
        0 if ranges => Op::DeleteRange(key(k), key(k + 1 + rng.next_bounded(6))),
        0..=3 => Op::Delete(key(k)),
        _ => {
            let fill = b'a' + u8::try_from(round % 26).unwrap();
            Op::Put(key(k), vec![fill; 40 + rng.next_bounded(400)])
        }
    }
}

/// A store, a scratch for each lane, and what every key should read as:
/// now, and as of each live snapshot.
struct Store {
    db: Box<Db>,
    scratch: Vec<Box<Scratch>>,
    model: Model,
    snapshots: Vec<(u64, Model)>,
    /// Steps taken while both lanes had a job in flight.
    side_by_side: u64,
    /// The key space of its history.
    keys: usize,
    /// Compaction is stepped after one write in this many (and whenever
    /// a flush is refused for want of it).
    step_every: usize,
    /// The fewest slots a flush left compaction (free, or reserved by
    /// a job in flight) where the reserve was one job's for a caller of
    /// `compact_step`, or both lanes'.
    least_headroom: u32,
    /// The same, for flushes in a region too full for both lanes'
    /// reserves, and how many of those found compaction idle.
    least_headroom_full: u32,
    full_and_idle: u32,
    /// Jobs started under pressure with two tables or more in level 0
    /// and a table in a level between: those that took level 0, and
    /// those that took the deeper table.
    took_level_0: u32,
    took_deeper: u32,
    /// Compaction is stepped with `compact_step`, one job at a time,
    /// and not lane by lane.
    one_job: bool,
    /// The device's write count at some of those steps.
    side_by_side_at: Vec<u64>,
    /// The last point write: its key, and what the key read as before
    /// and after it.
    last_write: Option<LastWrite>,
    /// Times a step found the region full and the snapshots were let go.
    region_full: u32,
}

/// A point write: its key, and the key's value before and after.
type LastWrite = (Vec<u8>, Option<Vec<u8>>, Option<Vec<u8>>);

impl Store {
    fn on(device: CutDevice) -> Self {
        let mut db = Box::new(Db::new(device, config()));
        block_on(db.open()).unwrap();
        Self {
            db,
            scratch: (0..COMPACTION_LANES)
                .map(|_| Box::new(Scratch::new()))
                .collect(),
            model: Model::new(),
            snapshots: Vec::new(),
            side_by_side: 0,
            keys: KEYS,
            step_every: 1,
            least_headroom: u32::MAX,
            least_headroom_full: u32::MAX,
            full_and_idle: 0,
            took_level_0: 0,
            took_deeper: 0,
            one_job: false,
            side_by_side_at: Vec::new(),
            last_write: None,
            region_full: 0,
        }
    }

    fn new() -> Self {
        Self::on(CutDevice::new(u64::MAX))
    }

    /// Jobs in flight: each holds one reserved slot while it merges.
    fn in_flight(&self) -> u32 {
        self.db.slot_stats().reserved
    }

    /// Tables in level `l`.
    fn tables(&self, l: usize) -> u32 {
        let n = self.db.level_tables(l).map_or(0, <[_]>::len);
        u32::try_from(n).unwrap()
    }

    /// Jobs in flight, by lane.
    fn jobs(&self) -> u32 {
        let n = Lane::ALL
            .iter()
            .filter(|l| self.db.lane_target(**l).is_some())
            .count();
        u32::try_from(n).unwrap()
    }

    /// The reserve as the library should hold it, restated from its
    /// rule: one job's (two slots) for a caller of `compact_step`; for
    /// a caller that steps both lanes, two jobs' while the region has
    /// seven slots or more above its bottom level, and otherwise one
    /// job's more than are in flight.
    fn reserve(&self) -> u32 {
        if self.one_job {
            2
        } else if self.roomy() {
            4
        } else {
            2 * (self.jobs() + 1).min(2)
        }
    }

    /// Room above the bottom level for both lanes' reserves, two tables
    /// in level 0 and one slot more.
    fn roomy(&self) -> bool {
        self.db.slot_stats().slots - self.tables(LEVELS - 1) >= 7
    }

    /// Tables in the levels between level 0 and the bottom.
    fn between(&self) -> u32 {
        (1..LEVELS - 1).map(|l| self.tables(l)).sum()
    }

    /// Tables in each level below level 0.
    fn below_level_0(&self) -> Vec<u32> {
        (1..LEVELS).map(|l| self.tables(l)).collect()
    }

    /// True when the next job to start has a choice that only pressure
    /// gives it: no job is in flight, the region is down to the
    /// reserve, no level is full, and there are both a level 0 job to
    /// take (two tables) and a table between level 0 and the bottom to
    /// push down.
    fn has_a_choice(&self) -> bool {
        let slots = self.db.slot_stats();
        self.jobs() == 0
            && slots.free + slots.reserved <= self.reserve()
            && (0..LEVELS - 1).all(|l| self.tables(l) < TABLES)
            && self.tables(0) >= 2
            && self.between() > 0
    }

    /// One step of `lane`'s job, and the structure checked after it.
    fn tick(&mut self, lane: Lane) -> Progress {
        let lane = if self.one_job { Lane::First } else { lane };
        // A job that starts under pressure with no level full: which
        // level it takes is the order under test.
        let below = self.below_level_0();
        let watched = self.has_a_choice();
        let i = Lane::ALL.iter().position(|l| *l == lane).unwrap();
        let stepped = if self.one_job {
            block_on(self.db.compact_step(&mut self.scratch[i]))
        } else {
            block_on(self.db.compact_step_in(lane, &mut self.scratch[i]))
        };
        if stepped == Ok(Progress::Done) {
            // Finished, idle, moved, given up: whichever, the lane holds
            // no job after a step that says so.
            assert_eq!(self.db.lane_target(lane), None);
        }
        let p = match stepped {
            Ok(p) => p,
            // The region is full of versions the snapshots pin (each
            // can hold a whole copy of the data in a region this small).
            // The remedy the error names: let them go.
            Err(Error::RegionFull) if !self.snapshots.is_empty() => {
                while !self.snapshots.is_empty() {
                    self.release_oldest();
                }
                self.region_full += 1;
                Progress::Done
            }
            Err(e) => panic!("{e:?}"),
        };
        self.db.check_invariants().unwrap();
        if watched {
            match self.db.lane_target(lane) {
                Some(1) => self.took_level_0 += 1,
                Some(_) => self.took_deeper += 1,
                // A table moved down a level without a merge.
                None if p == Progress::Done && self.below_level_0() != below => {
                    self.took_deeper += 1;
                }
                None => {}
            }
        }
        if self.in_flight() == 2 {
            self.side_by_side += 1;
            if self.side_by_side % 3 == 1 {
                self.side_by_side_at.push(self.db.device().writes);
            }
        }
        p
    }

    /// A write, with room made the way the error asks.
    fn write(&mut self, op: &Op) {
        for _ in 0..64 {
            let r = match op {
                Op::Put(k, v) => block_on(self.db.put(k, v)).map(drop),
                Op::Delete(k) => block_on(self.db.delete(k)).map(drop),
                Op::DeleteRange(a, b) => block_on(self.db.delete_range(a, b)).map(drop),
            };
            match r {
                Ok(()) => {
                    match op {
                        Op::Put(k, v) => {
                            let old = self.model.insert(k.clone(), v.clone());
                            self.last_write = Some((k.clone(), old, Some(v.clone())));
                        }
                        Op::Delete(k) => {
                            let old = self.model.remove(k);
                            self.last_write = Some((k.clone(), old, None));
                        }
                        Op::DeleteRange(a, b) => self.model.retain(|k, _| k < a || k >= b),
                    }
                    return;
                }
                Err(Error::TableFull | Error::ArenaFull | Error::WalFull) => self.flush(),
                Err(e) => panic!("{e:?}"),
            }
        }
        panic!("a write never found room");
    }

    fn flush(&mut self) {
        loop {
            // Whether compaction has work: a job in flight, or one
            // wanted. With none, flush goes to one job's reserve.
            let working = self.db.compaction_pending();
            let reserve = if working { self.reserve() } else { 2 };
            let full = !self.one_job && !self.roomy();
            let idle = self.jobs() == 0;
            match block_on(self.db.flush()) {
                Ok(()) => {
                    // A flush leaves compaction its reserve.
                    let slots = self.db.slot_stats();
                    let headroom = slots.free + slots.reserved;
                    assert!(headroom >= reserve, "a flush took a job's room: {slots:?}");
                    if full {
                        self.least_headroom_full = self.least_headroom_full.min(headroom);
                        self.full_and_idle += u32::from(idle);
                    } else if self.one_job || working {
                        self.least_headroom = self.least_headroom.min(headroom);
                    }
                    return;
                }
                Err(Error::NeedsCompaction) => {
                    for lane in Lane::ALL {
                        self.tick(lane);
                    }
                }
                Err(e) => panic!("{e:?}"),
            }
        }
    }

    fn drain(&mut self) {
        while self.db.compaction_pending() {
            for lane in Lane::ALL {
                self.tick(lane);
            }
        }
    }

    fn snapshot(&mut self) {
        if self.snapshots.len() < 6 {
            let seq = self.db.snapshot().unwrap();
            self.snapshots.push((seq, self.model.clone()));
        }
    }

    fn release_oldest(&mut self) {
        if !self.snapshots.is_empty() {
            let (seq, _) = self.snapshots.remove(0);
            self.db.release_snapshot(seq);
        }
    }

    /// Every key reads as the model says, now and as of every live
    /// snapshot.
    fn check(&self) {
        self.db.check_invariants().unwrap();
        let mut buf = [0u8; 1024];
        let views = self
            .snapshots
            .iter()
            .map(|(seq, model)| (*seq, model))
            .chain([(u64::MAX, &self.model)]);
        for (seq, model) in views {
            for i in 0..self.keys {
                let k = key(i);
                let got = block_on(self.db.get_at_with_time(&k, &mut buf, seq, 0)).unwrap();
                assert_eq!(
                    got.map(|n| buf[..n].to_vec()),
                    model.get(&k).cloned(),
                    "key {i} as of {seq}"
                );
            }
        }
    }
}

/// Runs `ops` writes with snapshots taken and released on the way and
/// both lanes stepped at random between them. `on_op` sees the store
/// after every write and its steps; returning false ends the run.
fn run(store: &mut Store, ops: usize, ranges: bool, mut on_op: impl FnMut(&Store) -> bool) {
    let mut rng = Lcg::new(0x1A2E_5EED);
    for n in 0..ops {
        let op = next_op(&mut rng, store.keys, n / 97, ranges);
        store.write(&op);
        match rng.next_bounded(60) {
            0 => store.snapshot(),
            1 => store.release_oldest(),
            _ => {}
        }
        // Up to three steps, each of a lane picked at random: every
        // interleaving of the two jobs' steps with each other and with
        // the writes comes up.
        for _ in 0..rng.next_bounded(4) {
            if store.db.compaction_pending() && n.is_multiple_of(store.step_every) {
                let lane = Lane::ALL[rng.next_bounded(COMPACTION_LANES)];
                store.tick(lane);
            }
        }
        if !on_op(store) {
            return;
        }
    }
}

/// The spine: two jobs in flight at once, their steps interleaved at
/// random with each other and with writes, flushes, range deletes and
/// snapshots, change no read. The structure is checked after every step
/// (no table in two jobs, no level with overlapping tables, a slot
/// reserved for each job and no more).
#[test]
fn two_jobs_side_by_side_change_no_read() {
    // A debug build compacts slowly: it gets a shorter history. (Two
    // jobs first run together some 3,000 writes in.)
    let history = if cfg!(debug_assertions) { 4000 } else { 30_000 };
    let mut store = Store::new();
    let mut ops = 0usize;
    run(&mut store, history, true, |s| {
        ops += 1;
        if !cfg!(debug_assertions) && ops.is_multiple_of(history / 6) {
            s.check();
        }
        true
    });
    store.check();
    // Two jobs were in flight together, and for a good many steps.
    assert!(
        store.side_by_side > 30,
        "{} steps side by side",
        store.side_by_side
    );
    assert_eq!(store.region_full, 0, "the region filled");
    store.flush();
    store.drain();
    assert_eq!(store.in_flight(), 0);
    store.check();
    // And it reopens to the same.
    for (seq, _) in core::mem::take(&mut store.snapshots) {
        store.db.release_snapshot(seq);
    }
    let Store { db, model, .. } = store;
    let mut again = Store::on(db.into_device());
    again.model = model;
    again.check();
}

/// A power cut with two jobs in flight loses nothing: each job's
/// outputs are invisible until its own commit, and each commit leaves a
/// whole tree. The cuts are placed at writes made while both lanes had
/// a job, found by running the history once without a cut.
#[test]
fn a_cut_with_two_jobs_in_flight_loses_nothing() {
    // Two jobs first run together some 3,000 writes in. Each cut replays
    // the history up to it: a debug build, which compacts slowly, gets
    // fewer of them.
    const OPS: usize = 4500;
    let cuts = if cfg!(debug_assertions) { 2 } else { 12 };
    let mut dry = Store::new();
    run(&mut dry, OPS, false, |_| true);
    let at = &dry.side_by_side_at;
    assert!(at.len() >= cuts, "{} cut points", at.len());
    for cut_at in at.iter().step_by(at.len() / cuts).copied() {
        // The same history, on a device that stops writing at `cut_at`:
        // run until the cut has happened.
        let mut store = Store::on(CutDevice::new(cut_at));
        run(&mut store, OPS, false, |s| !s.db.device().is_cut());
        assert!(
            store.db.device().is_cut(),
            "the history ended before {cut_at}"
        );
        // The cut fell in the last write or in the steps after it: that
        // write is there or it is not. Every write before it is.
        let (cut_key, old, new) = store.last_write.clone().expect("a write");

        // Power back on.
        let device = CutDevice {
            cut_at: u64::MAX,
            ..store.db.into_device()
        };
        let mut on = Store::on(device);
        assert_eq!(on.in_flight(), 0);
        let mut buf = [0u8; 1024];
        for i in 0..KEYS {
            let k = key(i);
            let got = block_on(on.db.get(&k, &mut buf))
                .unwrap()
                .map(|n| buf[..n].to_vec());
            if k == cut_key {
                assert!(got == new || got == old, "key {i} at cut {cut_at}");
            } else {
                assert_eq!(got, store.model.get(&k).cloned(), "key {i} at cut {cut_at}");
            }
            if let Some(v) = got {
                on.model.insert(k, v);
            }
        }
        // And the store goes on from there, both lanes working.
        for i in 0..KEYS / 2 {
            on.write(&Op::Put(key(i * 2), vec![b'z'; 100]));
        }
        on.flush();
        on.drain();
        on.check();
    }
}

/// Brings a store to two jobs in flight.
fn two_in_flight() -> Store {
    let mut store = Store::new();
    let mut found = false;
    run(&mut store, 6000, true, |s| {
        found = s.in_flight() == 2;
        !found
    });
    assert!(found, "two jobs never ran side by side");
    store
}

/// `compact_step` is still one job at a time: when it selects a job it
/// abandons what every lane has in flight, and their scratches are
/// stale.
#[test]
fn compact_step_keeps_to_one_job() {
    let mut store = two_in_flight();
    let mut third = Box::new(Scratch::new());
    block_on(store.db.compact_step(&mut third)).unwrap();
    store.db.check_invariants().unwrap();
    assert!(store.in_flight() <= 1, "a lane's job outlived compact_step");
    assert_eq!(
        store.db.lane_target(Lane::Second),
        None,
        "the second lane's job outlived compact_step"
    );
    // The lanes' scratches hold jobs that are gone: each resets itself
    // and starts afresh, or finds the level it wanted taken.
    while store.db.compaction_pending() {
        if block_on(store.db.compact_step(&mut third)).unwrap() == Progress::Done {
            break;
        }
        assert!(store.in_flight() <= 1);
    }
    store.drain();
    store.check();
}

/// A requested compaction ends both jobs.
#[test]
fn a_request_ends_both_jobs() {
    let mut store = two_in_flight();
    store.db.request_compaction(0).unwrap();
    assert_eq!(store.in_flight(), 0, "the request left a job in flight");
    store.db.check_invariants().unwrap();
    store.drain();
    store.check();
    assert_eq!(store.db.level_tables(0).map_or(0, <[_]>::len), 0);
}

/// A request's jobs run alone: the other lane starts nothing beside
/// one. The store is settled first (no level wants a job), so every job
/// that then runs is the request's.
#[test]
fn a_requested_job_runs_alone() {
    let mut store = Store::new();
    run(&mut store, 2500, true, |_| true);
    store.flush();
    store.drain();
    assert!(!store.db.compaction_pending());
    store.db.request_compaction(0).unwrap();
    let mut alone = 0;
    while store.db.compaction_pending() {
        store.tick(Lane::First);
        if store.in_flight() == 1 {
            for _ in 0..2 {
                assert_eq!(store.tick(Lane::Second), Progress::Done);
                assert_eq!(store.in_flight(), 1, "a job started beside a requested one");
            }
            alone += 1;
        }
    }
    assert!(alone > 10, "{alone} steps of a requested job seen");
    store.check();
    // The request compacted every table down: what the second lane was
    // offered beside it, it left for the request.
    let levels: Vec<usize> = (0..7)
        .map(|l| store.db.level_tables(l).map_or(0, <[_]>::len))
        .collect();
    assert_eq!(
        levels.iter().filter(|&&n| n > 0).count(),
        1,
        "the request left tables above the bottom: {levels:?}"
    );
    // The second lane's turn: it serves a request as the first does.
    for i in 0..KEYS {
        store.write(&Op::Put(key(i), vec![b'r'; 120]));
    }
    store.flush();
    store.db.request_compaction(0).unwrap();
    while store.db.compaction_pending() {
        store.tick(Lane::Second);
        if store.in_flight() == 1 {
            assert_eq!(store.tick(Lane::First), Progress::Done);
            assert_eq!(store.in_flight(), 1);
        }
    }
    store.check();
}

/// A scratch belongs to the lane its job started in. Stepped in the
/// other lane it is stale: it resets, and that lane's own job (if any)
/// is abandoned for the one it then selects, like any job whose scratch
/// was replaced.
#[test]
fn a_scratch_in_the_wrong_lane_is_stale() {
    let mut store = two_in_flight();
    store.scratch.swap(0, 1);
    for lane in Lane::ALL {
        store.tick(lane);
    }
    store.drain();
    store.check();
}

/// Two lanes in a region that is nearly full. With twice the keys, the
/// tables take all but a few of the slots, and every job runs on the
/// reserve: flushes are refused until compaction frees a slot, jobs
/// are admitted by the slot, and tight jobs (which run alone) come up.
/// The rules that keep two jobs from starving each other are what is
/// under test: a flush leaves each lane the slot its job writes and
/// one more, the region never dead-ends, and no read changes.
///
/// Release builds only: it takes tens of thousands of writes to fill
/// the region, and a debug build compacts too slowly for that.
#[cfg(not(debug_assertions))]
#[test]
fn two_jobs_in_a_nearly_full_region() {
    let mut store = Store::new();
    store.keys = 2 * KEYS;
    let mut ops = 0usize;
    run(&mut store, 60_000, true, |s| {
        ops += 1;
        if ops.is_multiple_of(15_000) {
            s.check();
        }
        true
    });
    store.check();
    assert!(
        store.side_by_side > 1000,
        "{} steps side by side",
        store.side_by_side
    );
    // The reserve was all that was left, and it was left (`flush`
    // asserts so each time).
    assert_eq!(
        store.least_headroom, 4,
        "the region was never under pressure"
    );
    assert!(store.db.slot_stats().used >= 20);
    assert_eq!(store.region_full, 0, "the region filled");

    // The same with compaction far behind the writers, as it is on a
    // host whose merges run on other threads: a job in flight sees many
    // flushes, each wanting a slot, and gets a step only when a flush is
    // refused for want of one.
    store.step_every = 40;
    let side_by_side = store.side_by_side;
    run(&mut store, 40_000, true, |_| true);
    // With so few slots to spare, two jobs fit less often.
    assert!(store.side_by_side > side_by_side + 40);
    assert_eq!(store.region_full, 0, "the region filled");
    store.flush();
    store.drain();
    store.check();
}

/// A second lane changes two things, and a caller that steps one job
/// at a time with `compact_step` keeps what it always had.
///
/// The reserve: flush keeps it once for each lane that is stepped (two
/// slots a job). Without its own reserve the second lane would have
/// nothing to run on: flush fills the region to the reserve, and the
/// job in flight holds that.
///
/// The order under pressure: with one job at a time the deepest table
/// between level 0 and the bottom is pushed down first; with two lanes,
/// level 0 is merged first. Pressure comes two slots sooner with a
/// second reserve, and level 0's tables are a slot each for a flush's
/// worth of data: merging them frees the most slots for the least
/// rewriting, and is the job that can run beside a long one below.
///
/// Release builds only, for the reason above.
#[cfg(not(debug_assertions))]
#[test]
fn a_second_lane_changes_the_reserve_and_the_order_under_pressure() {
    let mut store = Store::new();
    store.keys = 2 * KEYS;
    store.one_job = true;
    run(&mut store, 60_000, true, |_| true);
    assert_eq!(store.side_by_side, 0);
    assert_eq!(store.least_headroom, 2, "one job's reserve, as ever");
    assert!(
        store.took_deeper > 20 && store.took_deeper > 4 * store.took_level_0,
        "one job at a time took level 0 {} times and deeper {} times",
        store.took_level_0,
        store.took_deeper
    );

    // Both lanes are stepped from here on.
    store.one_job = false;
    for lane in Lane::ALL {
        store.tick(lane);
    }
    store.least_headroom = u32::MAX;
    (store.took_level_0, store.took_deeper) = (0, 0);
    run(&mut store, 30_000, true, |_| true);
    assert_eq!(store.least_headroom, 4, "a reserve for each lane");
    // (With two lanes a job is nearly always in flight, so the choice
    // comes up seldom here: the next test makes it.)
    assert_eq!(store.took_deeper, 0, "a deeper table went before level 0");
    // And in a region flush has filled, the second lane finds jobs.
    assert!(
        store.side_by_side > 300,
        "{} steps side by side",
        store.side_by_side
    );
    assert_eq!(store.region_full, 0, "the region filled");

    // One job at a time again.
    store.one_job = true;
    store.tick(Lane::First);
    store.least_headroom = u32::MAX;
    run(&mut store, 30_000, true, |_| true);
    assert_eq!(store.least_headroom, 2);
    store.flush();
    store.drain();
    store.check();
}

/// The order under pressure, on one image: a store is run, one job at
/// a time, to a point where the next job has the choice (no job in
/// flight, the region down to its reserve, two tables in level 0, a
/// table between level 0 and the bottom), and opened twice from there.
/// `compact_step` pushes the deeper table down. A caller that steps
/// both lanes merges level 0.
///
/// Release builds only, for the reason above.
#[cfg(not(debug_assertions))]
#[test]
fn under_pressure_one_job_goes_deepest_first_and_two_lanes_take_level_0() {
    let mut store = Store::new();
    store.keys = 2 * KEYS;
    store.one_job = true;
    let mut image = None;
    run(&mut store, 60_000, true, |s| {
        if s.has_a_choice() {
            image = Some(s.db.device().clone());
        }
        image.is_none()
    });
    let image = image.expect("the choice never came up");

    let mut one = Store::on(image.clone());
    one.one_job = true;
    assert!(one.has_a_choice());
    one.tick(Lane::First);
    assert_eq!((one.took_level_0, one.took_deeper), (0, 1));

    let mut two = Store::on(image);
    // Stepping the second lane is what says there are two.
    assert!(two.has_a_choice());
    two.tick(Lane::Second);
    assert_eq!((two.took_level_0, two.took_deeper), (1, 0));
    assert_eq!(two.db.lane_target(Lane::Second), Some(1));
}

/// In a region too full for it, the second lane's reserve gives way.
/// With fewer than seven slots above the bottom level, two reserves
/// would leave flush a slot or two to batch in, and every flush would
/// push a table at the bottom level. The reserve is then kept for one
/// job more than is in flight: flush fills level 0 down to one job's
/// reserve while compaction is idle, as it does for a caller of
/// `compact_step`, and keeps off the second lane's once a job runs.
/// And a second lane costs no capacity: a region whose compaction has
/// no work fills to one job's reserve.
///
/// Release builds only, for the reason above.
#[cfg(not(debug_assertions))]
#[test]
fn a_region_too_full_for_two_reserves_keeps_one_while_compaction_is_idle() {
    let mut store = Store::new();
    // Fresh keys until the bottom level has all but six of the slots.
    let mut n = 0;
    while store.roomy() {
        store.write(&Op::Put(key(n), vec![b'v'; 420]));
        n += 1;
        store.keys = n;
        for lane in Lane::ALL {
            if store.db.compaction_pending() {
                store.tick(lane);
            }
        }
        assert!(n < 100_000, "the bottom level never filled");
    }
    assert_eq!(store.full_and_idle, 0);
    // Overwrites from here: the data no longer grows, and compaction
    // keeps the region where it is.
    let mut rng = Lcg::new(0x000F_0117);
    for round in 0..12_000usize {
        let fill = b'a' + u8::try_from(round % 26).unwrap();
        store.write(&Op::Put(key(rng.next_bounded(n)), vec![fill; 420]));
        for _ in 0..rng.next_bounded(4) {
            if store.db.compaction_pending() {
                store.tick(Lane::ALL[rng.next_bounded(COMPACTION_LANES)]);
            }
        }
    }
    assert!(store.full_and_idle > 20, "{}", store.full_and_idle);
    assert_eq!(
        store.least_headroom_full, 2,
        "one job's reserve, while idle"
    );
    assert_eq!(store.region_full, 0, "the region filled");
    store.flush();
    store.drain();
    store.check();
}

/// Fresh keys, in order, until the region is full: how many it took.
fn capacity(one_job: bool) -> usize {
    let mut store = Store::new();
    store.one_job = one_job;
    let mut n = 0;
    loop {
        let k = format!("cap-{n:06}").into_bytes();
        match block_on(store.db.put(&k, &[b'c'; 420])) {
            Ok(_) => n += 1,
            Err(Error::TableFull | Error::ArenaFull | Error::WalFull) => loop {
                match block_on(store.db.flush()) {
                    Ok(()) => break,
                    Err(Error::NeedsCompaction) => {
                        for lane in Lane::ALL {
                            store.tick(lane);
                        }
                    }
                    Err(Error::RegionFull) => return n,
                    Err(e) => panic!("{e:?}"),
                }
            },
            Err(e) => panic!("{e:?}"),
        }
        assert!(n < 200_000, "the region never filled");
    }
}

/// A second lane costs no capacity. Its reserve is compaction's while
/// compaction has work; a region whose tables are all where they belong
/// has none, and fills to one job's reserve whichever way it is
/// compacted.
#[test]
fn a_second_lane_costs_no_capacity() {
    let (one, two) = (capacity(true), capacity(false));
    // The same to within a table: the two orders of compaction pack
    // the last tables a little differently. A reserve held to the end
    // would be two slots, of the 26 that hold tables.
    assert!(
        one.abs_diff(two) * 26 < one,
        "{one} keys with one job at a time, {two} with two lanes"
    );
}
