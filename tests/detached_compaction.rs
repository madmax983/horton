//! Detached compaction merges (ADR-0024): `Compaction::merge_detached`
//! runs a job's merge quanta without the `Db`, over another handle to
//! the device, and `Db::compact_step` picks up where they stopped.
//!
//! RED: `Compaction::merge_detached`, `Compaction::wants_merge` and
//! `Detached` did not exist; the merge ran only inside `compact_step`,
//! through the `Db`'s own device handle.
//!
//! The spine is a twin test. Two stores take the same writes, flushes,
//! snapshots and compaction ticks. One compacts with `compact_step`
//! alone. The other runs every merge quantum detached, through a second
//! handle. Their device images must be equal byte for byte: a detached
//! quantum is the quantum `compact_step` would have run, and nothing
//! else.

use core::cell::{Cell, RefCell};
use core::task::{Context, Poll};
use std::collections::BTreeMap;
use std::rc::Rc;

mod common;
use common::{Lcg, MemDevice, block_on};
use horton::{BlockDevice, Compaction, Detached, Error, Manifest, Progress};

/// The device said no (a planted fault).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Fault;

/// A handle to a memory device that other handles share. Each handle
/// counts its own I/O, and can be told to fail its writes.
struct Handle {
    storage: Rc<RefCell<MemDevice<4096>>>,
    reads: Cell<u64>,
    writes: u64,
    /// Writes this handle still performs before it fails every one.
    writes_left: Option<u64>,
}

impl Handle {
    fn new() -> Self {
        Self::to(&Rc::new(RefCell::new(MemDevice::new())))
    }

    /// Another handle to the same storage.
    fn to(storage: &Rc<RefCell<MemDevice<4096>>>) -> Self {
        Self {
            storage: Rc::clone(storage),
            reads: Cell::new(0),
            writes: 0,
            writes_left: None,
        }
    }

    const fn io(&self) -> (u64, u64) {
        (self.reads.get(), self.writes)
    }
}

impl BlockDevice for Handle {
    type Error = Fault;
    const BLOCK: usize = 4096;

    fn poll_read_block(
        &self,
        cx: &mut Context<'_>,
        id: u64,
        buf: &mut [u8],
    ) -> Poll<Result<(), Fault>> {
        self.reads.set(self.reads.get() + 1);
        self.storage
            .borrow()
            .poll_read_block(cx, id, buf)
            .map(|r| r.map_err(|e| match e {}))
    }

    fn poll_write_block(
        &mut self,
        cx: &mut Context<'_>,
        id: u64,
        buf: &[u8],
    ) -> Poll<Result<(), Fault>> {
        if let Some(left) = &mut self.writes_left {
            if *left == 0 {
                return Poll::Ready(Err(Fault));
            }
            *left -= 1;
        }
        self.writes += 1;
        self.storage
            .borrow_mut()
            .poll_write_block(cx, id, buf)
            .map(|r| r.map_err(|e| match e {}))
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Fault>> {
        self.storage
            .borrow_mut()
            .poll_flush(cx)
            .map(|r| r.map_err(|e| match e {}))
    }
}

/// `common::test_config` with the table region cut to 28 slots of 16
/// blocks: small enough that outputs fill and jobs split, large enough
/// for the workloads here with their snapshots.
const fn config() -> horton::Config {
    horton::Config::new(8, 136, 136, 136 + 28 * 16, 0, 4)
}

type Db = common::TestDb<Handle>;
type Scratch = Compaction<4096, 256, 1024, 1024>;
type TestManifest = Manifest<7, 4, 256>;

/// A store on its own storage, a second handle to that storage for the
/// merge, a scratch, and what every key should read as.
struct Store {
    db: Box<Db>,
    storage: Rc<RefCell<MemDevice<4096>>>,
    merge: Handle,
    scratch: Box<Scratch>,
    model: BTreeMap<Vec<u8>, Vec<u8>>,
    /// Detached quanta that sealed a block, and those that parked.
    quanta: u64,
    parked: u64,
    /// Whether this store runs its merge quanta detached.
    detached: bool,
    /// Live snapshots, oldest first.
    snapshots: Vec<u64>,
}

impl Store {
    fn new(detached: bool) -> Self {
        let storage = Rc::new(RefCell::new(MemDevice::new()));
        let mut db = Box::new(Db::new(Handle::to(&storage), config()));
        block_on(db.open()).unwrap();
        Self {
            db,
            merge: Handle::to(&storage),
            storage,
            scratch: Box::new(Scratch::new()),
            model: BTreeMap::new(),
            quanta: 0,
            parked: 0,
            detached,
            snapshots: Vec::new(),
        }
    }

    /// One compaction tick. Plain: one `compact_step`. Detached: the
    /// merge quantum through the second handle, and, when that needs the
    /// `Db`, the `compact_step` that acts on it. Either way the `Db`
    /// ends the tick in the state one `compact_step` leaves it in.
    fn tick(&mut self) -> Progress {
        if self.detached && self.scratch.wants_merge() {
            let manifest: TestManifest = *self.db.manifest();
            let db_io = self.db.device().io();
            let merged = block_on(self.scratch.merge_detached(&mut self.merge, &manifest));
            assert_eq!(
                self.db.device().io(),
                db_io,
                "a detached quantum went through the Db's own handle"
            );
            match merged.unwrap() {
                Detached::More => {
                    self.quanta += 1;
                    assert!(self.scratch.wants_merge());
                    return Progress::More;
                }
                Detached::NeedsDb => {
                    self.parked += 1;
                    assert!(!self.scratch.wants_merge());
                }
            }
        }
        block_on(self.db.compact_step(&mut self.scratch)).unwrap()
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
                        Op::Put(k, v) => drop(self.model.insert(k.clone(), v.clone())),
                        Op::Delete(k) => drop(self.model.remove(k)),
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
            match block_on(self.db.flush()) {
                Ok(()) => return,
                Err(Error::NeedsCompaction) => {
                    self.tick();
                }
                Err(e) => panic!("{e:?}"),
            }
        }
    }

    fn drain(&mut self) {
        while self.db.compaction_pending() {
            self.tick();
        }
    }

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let mut buf = [0u8; 1024];
        block_on(self.db.get(key, &mut buf))
            .unwrap()
            .map(|n| buf[..n].to_vec())
    }

    /// Every key of the model reads back, and no other key of the space.
    fn check(&self) {
        self.db.check_invariants().unwrap();
        for i in 0..KEYS {
            let k = key(i);
            assert_eq!(self.get(&k), self.model.get(&k).cloned(), "key {i}");
        }
    }

    fn image(&self) -> Vec<[u8; 4096]> {
        self.storage.borrow_mut().blocks_mut().clone()
    }
}

enum Op {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
    DeleteRange(Vec<u8>, Vec<u8>),
}

const KEYS: usize = 400;

fn key(i: usize) -> Vec<u8> {
    format!("key-{i:04}").into_bytes()
}

/// The next write of a workload with overwrites, deletes and range
/// deletes, so that jobs have versions and tombstones to drop.
fn next_op(rng: &mut Lcg, round: usize) -> Op {
    let k = rng.next_bounded(KEYS);
    match rng.next_bounded(20) {
        0 => Op::DeleteRange(key(k), key(k + 1 + rng.next_bounded(6))),
        1..=3 => Op::Delete(key(k)),
        _ => {
            let fill = b'a' + u8::try_from(round % 26).unwrap();
            Op::Put(key(k), vec![fill; 40 + rng.next_bounded(400)])
        }
    }
}

/// Runs the same history on a store that compacts the plain way and on
/// one that merges detached, a compaction tick between every two
/// writes, and returns both.
fn twins(ops: usize) -> (Store, Store) {
    let mut stores = [Store::new(false), Store::new(true)];
    let mut rng = Lcg::new(0x00C0_FFEE);
    for n in 0..ops {
        let op = next_op(&mut rng, n / 97);
        let snapshot = rng.next_bounded(60);
        let mut ticks = [Progress::Done; 2];
        for (s, tick) in stores.iter_mut().zip(&mut ticks) {
            s.write(&op);
            // Snapshots change what a job may drop: taken and released
            // at the same points in both.
            if snapshot == 0 && s.snapshots.len() < 6 {
                s.snapshots.push(s.db.snapshot().unwrap());
            } else if snapshot == 1 && !s.snapshots.is_empty() {
                let oldest = s.snapshots.remove(0);
                s.db.release_snapshot(oldest);
            }
            if s.db.compaction_pending() {
                *tick = s.tick();
            }
        }
        assert_eq!(ticks[0], ticks[1], "the twins' ticks diverged at op {n}");
        if n % 500 == 499 {
            for s in &stores {
                s.check();
            }
        }
    }
    stores.into()
}

/// The spine: a store whose merges all ran detached is, block for
/// block, the store `compact_step` alone would have left. Writes,
/// flushes and snapshots ran between the quanta of both.
#[test]
fn a_detached_merge_leaves_the_image_compact_step_leaves() {
    let (mut plain, mut detached) = twins(1800);
    assert_eq!(plain.quanta + plain.parked, 0);
    // The detached store did merge detached, through outputs that split
    // and jobs that ended, and its Db's handle wrote no merged block.
    assert!(detached.quanta > 200, "{} quanta", detached.quanta);
    assert!(detached.parked > 30, "{} parked", detached.parked);
    assert!(detached.merge.io().1 >= detached.quanta);
    assert!(plain.image() == detached.image(), "the images differ");
    for s in [&mut plain, &mut detached] {
        s.flush();
        s.drain();
        s.check();
    }
    assert!(plain.image() == detached.image(), "the images differ");
    assert_eq!(plain.model, detached.model);
    // Both reopen to the same contents.
    for s in [plain, detached] {
        let mut again = Box::new(Db::new(Handle::to(&s.storage), config()));
        block_on(again.open()).unwrap();
        let mut buf = [0u8; 1024];
        for i in 0..KEYS {
            let got = block_on(again.get(&key(i), &mut buf)).unwrap();
            assert_eq!(
                got.map(|n| buf[..n].to_vec()),
                s.model.get(&key(i)).cloned()
            );
        }
    }
}

/// Brings the store to a job in its merge: the scratch wants a quantum.
/// Steps here go through the `Db` (the store's own ticks are switched
/// to plain ones), so the tests below place every detached call.
fn to_merge(s: &mut Store) {
    s.detached = false;
    for round in 0..10_000 {
        if s.scratch.wants_merge() {
            return;
        }
        if s.db.compaction_pending() {
            block_on(s.db.compact_step(&mut s.scratch)).unwrap();
        } else {
            for i in 0..40 {
                s.write(&Op::Put(key((round + i * 7) % KEYS), vec![b'z'; 300]));
            }
        }
    }
    panic!("no job ever reached its merge");
}

/// A store with several levels of tables and a job in its merge.
fn mid_job() -> Store {
    let mut s = Store::new(false);
    let mut rng = Lcg::new(0x0BAD_5EED);
    for n in 0..4000 {
        s.write(&next_op(&mut rng, n / 97));
        if s.db.compaction_pending() {
            s.tick();
        }
    }
    to_merge(&mut s);
    s
}

/// Runs detached quanta until the merge needs the `Db`.
fn park(s: &mut Store) {
    let manifest: TestManifest = *s.db.manifest();
    for _ in 0..10_000 {
        if block_on(s.scratch.merge_detached(&mut s.merge, &manifest)).unwrap() == Detached::NeedsDb
        {
            return;
        }
    }
    panic!("the merge never needed the Db");
}

/// A merge that needs the `Db` waits for it: further calls touch
/// nothing, and an idle scratch has nothing to merge at all. The
/// `compact_step` that follows acts on what the merge left.
#[test]
fn a_parked_merge_does_nothing_until_compact_step() {
    let mut idle = Scratch::new();
    let mut handle = Handle::new();
    assert!(!idle.wants_merge());
    let empty: TestManifest = Manifest::new();
    assert_eq!(
        block_on(idle.merge_detached(&mut handle, &empty)),
        Ok(Detached::NeedsDb)
    );
    assert_eq!(handle.io(), (0, 0));

    let mut s = mid_job();
    assert_eq!(s.db.slot_stats().reserved, 1, "the job's output slot");
    park(&mut s);
    assert!(!s.scratch.wants_merge());
    let manifest: TestManifest = *s.db.manifest();
    let io = s.merge.io();
    for _ in 0..3 {
        assert_eq!(
            block_on(s.scratch.merge_detached(&mut s.merge, &manifest)),
            Ok(Detached::NeedsDb)
        );
    }
    assert_eq!(s.merge.io(), io, "a parked merge read or wrote");
    // The Db seals and commits what the merge filled: a manifest commit.
    let seq = s.db.manifest().seq();
    let step = block_on(s.db.compact_step(&mut s.scratch)).unwrap();
    assert!(s.db.manifest().seq() > seq, "nothing was committed");
    assert_eq!(s.scratch.wants_merge(), step == Progress::More);
    s.drain();
    s.check();
}

/// A detached quantum that fails loses the job, not the store. The
/// error goes to the caller of the quantum; the next `compact_step`
/// abandons the job (its reserved slot is free again) and the one after
/// starts over.
#[test]
fn a_failed_detached_quantum_abandons_the_job() {
    let mut s = mid_job();
    // The merge handle fails its third write, in whichever job that is.
    s.merge.writes_left = Some(2);
    let mut failed = None;
    for _ in 0..10_000 {
        to_merge(&mut s);
        let manifest: TestManifest = *s.db.manifest();
        match block_on(s.scratch.merge_detached(&mut s.merge, &manifest)) {
            Ok(Detached::More) => {}
            Ok(Detached::NeedsDb) => {
                block_on(s.db.compact_step(&mut s.scratch)).unwrap();
            }
            Err(e) => {
                failed = Some(e);
                break;
            }
        }
    }
    assert_eq!(failed, Some(Error::Device(Fault)));
    assert_eq!(s.merge.io().1, 2);
    // The scratch does not merge on from a failed write.
    assert!(!s.scratch.wants_merge());
    let manifest: TestManifest = *s.db.manifest();
    let io = s.merge.io();
    assert_eq!(
        block_on(s.scratch.merge_detached(&mut s.merge, &manifest)),
        Ok(Detached::NeedsDb)
    );
    assert_eq!(s.merge.io(), io);
    // The Db ends the job: nothing committed, nothing reserved.
    assert_eq!(s.db.slot_stats().reserved, 1);
    let seq = s.db.manifest().seq();
    assert_eq!(
        block_on(s.db.compact_step(&mut s.scratch)),
        Ok(Progress::Done)
    );
    assert_eq!(s.db.manifest().seq(), seq);
    assert_eq!(s.db.slot_stats().reserved, 0);
    assert!(s.db.compaction_pending(), "the work is still to do");
    assert!(!s.scratch.wants_merge());
    s.check();
    // The job is done over through the Db, and done.
    s.drain();
    s.check();
}

/// Blocks a detached merge has written are in a slot nothing points at.
/// A power cut in the middle of one leaves the store as it was when the
/// job began: every write is there, and the half-written output is free
/// space.
#[test]
fn a_cut_in_the_middle_of_a_detached_merge_loses_nothing() {
    let mut s = mid_job();
    let mut cuts = 0;
    for _ in 0..10_000 {
        if cuts == 12 {
            break;
        }
        to_merge(&mut s);
        let manifest: TestManifest = *s.db.manifest();
        if block_on(s.scratch.merge_detached(&mut s.merge, &manifest)).unwrap() == Detached::NeedsDb
        {
            block_on(s.db.compact_step(&mut s.scratch)).unwrap();
            continue;
        }
        // A block of the output is on the device, and no commit has
        // been made for it. The image as a power cut would leave it:
        let cut = Rc::new(RefCell::new(s.storage.borrow().clone()));
        let mut db = Box::new(Db::new(Handle::to(&cut), config()));
        block_on(db.open()).unwrap();
        db.check_invariants().unwrap();
        assert_eq!(db.slot_stats().reserved, 0);
        let mut buf = [0u8; 1024];
        for i in 0..KEYS {
            let got = block_on(db.get(&key(i), &mut buf)).unwrap();
            assert_eq!(
                got.map(|n| buf[..n].to_vec()),
                s.model.get(&key(i)).cloned()
            );
        }
        cuts += 1;
    }
    assert_eq!(cuts, 12);
    s.drain();
    s.check();
}

/// A scratch whose job the `Db` gave up while the merge was parked is
/// stale, whatever the merge left in it: `compact_step` starts afresh
/// and does not seal an output into a slot the job no longer holds.
#[test]
fn a_parked_scratch_whose_job_was_dropped_starts_afresh() {
    let mut s = mid_job();
    park(&mut s);
    // A new request ends the running job; its reservation goes.
    assert_eq!(s.db.slot_stats().reserved, 1);
    s.db.request_compaction(0).unwrap();
    assert_eq!(s.db.slot_stats().reserved, 0);
    assert!(!s.scratch.wants_merge());
    block_on(s.db.compact_step(&mut s.scratch)).unwrap();
    // Whatever that step did, it did as a new job, with a reservation
    // of its own or none.
    s.db.check_invariants().unwrap();
    s.check();
    s.drain();
    s.check();
}

/// A caller whose own part in the merge failed (its handle would not
/// flush, say) gives the job up, and the `Db` ends it as it ends a job
/// whose quantum failed: nothing is sealed, nothing committed, the slot
/// is free.
#[test]
fn an_abandoned_job_is_ended_by_the_next_compact_step() {
    let mut idle = Scratch::new();
    idle.abandon();
    assert!(!idle.wants_merge());

    let mut s = mid_job();
    park(&mut s);
    s.scratch.abandon();
    assert!(!s.scratch.wants_merge());
    let seq = s.db.manifest().seq();
    assert_eq!(
        block_on(s.db.compact_step(&mut s.scratch)),
        Ok(Progress::Done)
    );
    assert_eq!(
        s.db.manifest().seq(),
        seq,
        "an abandoned output was committed"
    );
    assert_eq!(s.db.slot_stats().reserved, 0);
    s.check();
    // An idle scratch that was "abandoned" selects as usual.
    let mut store = Store::new(false);
    store.write(&Op::Put(key(1), vec![b'v'; 10]));
    assert_eq!(
        block_on(store.db.compact_step(&mut idle)),
        Ok(Progress::Done)
    );
    s.drain();
    s.check();
}
