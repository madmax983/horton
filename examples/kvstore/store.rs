//! A `LevelDB`-shaped store over horton: many threads, one database.
//!
//! horton's [`Db`](horton::Db) is a single-owner value with `&mut self`
//! writes and no locks, the right shape for firmware. A host program
//! wants what `LevelDB` gives it instead: a handle any thread can clone,
//! writes from many threads at once, and compaction that happens by
//! itself. This module provides that by putting the `Db` behind one lock,
//! the **engine lock**, which `LevelDB` calls its mutex:
//!
//! ```text
//!  client threads ──▶ engine lock ──▶ Db ──▶ FileDevice
//!  compaction thread ──┘   (one holder at a time; each runs horton itself)
//! ```
//!
//! - **Callers run horton themselves.** Nothing is handed to a store
//!   thread: the thread holding the lock does the work and returns.
//!   (Until v0.20 a store thread did everything and every call paid two
//!   thread switches, a quarter of a read's time and half of a write's.)
//! - **The lock holder serves everyone waiting.** A point read or a
//!   write is queued first; then its thread goes for the lock. Whoever
//!   gets it serves the queue in order, its own request and the others',
//!   and marks each done. The threads it served see their result and
//!   never take the lock. This is flat combining, and it matters because
//!   the database is megabytes of state that would otherwise move
//!   between CPU caches on every call: four threads taking turns at the
//!   lock read slower than one thread alone.
//! - **Group commit** falls out of it: consecutive writes in the queue
//!   that fit one WAL block are committed as one [`WriteBatch`], one
//!   block write and one `fdatasync` for all of them. This is `LevelDB`'s
//!   writer queue. Each client's batch stays atomic (the group is all or
//!   nothing).
//! - **Waiters spin before they sleep.** An operation takes microseconds,
//!   so a waiting thread polls for its result and for the lock for up to
//!   [`SPIN`] before parking, as `RocksDB`'s write thread does. Parking
//!   costs more than the wait it would save. A parked thread is woken by
//!   the thread that serves its request, or, if it is at the head of the
//!   queue, by whoever releases the lock.
//! - **Capacity errors are handled here.** horton reports a full memtable,
//!   WAL or level 0 as an error naming its remedy; the caller holding the
//!   lock applies the remedy (`flush`, `compact_step`) and retries, so
//!   clients never see them.
//! - **Background compaction.** While a job is pending, every operation
//!   ends with one bounded `compact_step` before it gives the lock up,
//!   and a compaction thread runs them back to back whenever no client
//!   is waiting. Writers are never stalled for a whole compaction, which
//!   is what `LevelDB`'s L0 slowdown trigger approximates with sleeps.
//! - **Snapshots and iterators.** A [`Snapshot`] is horton's sequence
//!   watermark, released when dropped. An [`Iter`] pins one for its
//!   lifetime, like a `LevelDB` iterator, and pages through the range.
//!
//! Every call returns after the operation completes, so a write that
//! returned `Ok` is on the disk (or, without `sync`, in the operating
//! system's hands).

use std::collections::VecDeque;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, PoisonError, TryLockError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use horton::wal::WAL_RECORD_OVERHEAD;
use horton::{Error, OpenReport, Progress, SlotStats, WriteBatch};

use crate::device::{FileDevice, IoError, IoStats};
use crate::{BLOCK, KEY_MAX, KvCompaction, KvDb, KvRevScan, KvScan, LEVELS, VAL_MAX, block_on};

/// horton's error, with this store's device error.
pub type DbError = Error<IoError>;

/// Most ops one WAL block can hold: every op costs at least its record
/// overhead plus a one-byte key.
const GROUP_OPS: usize = BLOCK / (WAL_RECORD_OVERHEAD + 1);

/// The group-commit batch: one WAL block's worth of ops.
type GroupBatch = WriteBatch<KEY_MAX, VAL_MAX, GROUP_OPS>;

/// How many times one write retries after making room. Each retry follows
/// a flush or a whole compaction job, so a write that still does not fit
/// after this many is stuck (the region is full).
const ROOM_TRIES: usize = 16;

/// Entries per page when an [`Iter`] refills.
const PAGE: usize = 1024;

/// How long a thread polls for the engine lock before it parks. Longer
/// than any read or unsynced commit, far shorter than an `fdatasync` or a
/// flush; `RocksDB` uses the same figure for its write thread.
const SPIN: Duration = Duration::from_micros(100);

/// Lock polls between two `yield`s while spinning.
const POLLS_PER_YIELD: usize = 64;

/// Most queued requests one lock holder serves before it gives the lock
/// up (a write group counts as one). Bounds how long a thread works for
/// others after its own request is done.
const SERVE_MAX: usize = 64;

/// The longest a queued thread stays parked before it looks again. It is
/// woken sooner whenever there is something for it to do; this bounds the
/// cost of a wake-up that was missed.
const PARK: Duration = Duration::from_millis(2);

/// How long the compaction thread stands back when clients are waiting
/// for the lock (they run the compaction steps themselves meanwhile).
const BACKGROUND_PAUSE: Duration = Duration::from_micros(200);

/// Why a store call failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreError {
    /// horton refused the operation or the device failed.
    Db(DbError),
    /// The store could not be opened.
    Open(String),
    /// The store was closed (its [`Store`] was dropped), or a thread
    /// panicked while it held the engine.
    Closed,
}

impl core::fmt::Display for StoreError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Db(e) => write!(f, "{e:?}"),
            Self::Open(e) => write!(f, "open: {e}"),
            Self::Closed => write!(f, "the store is closed"),
        }
    }
}

impl std::error::Error for StoreError {}

impl From<DbError> for StoreError {
    fn from(e: DbError) -> Self {
        Self::Db(e)
    }
}

/// How to open a store.
#[derive(Debug, Clone, Copy)]
pub struct Options {
    /// Size of a new store's file, in blocks. An existing store keeps the
    /// size it was created with.
    pub create_blocks: u64,
    /// `fdatasync` every commit (`LevelDB`'s `WriteOptions::sync = true`).
    pub sync: bool,
}

/// One operation in a [`Batch`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchOp {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

/// Writes applied together or not at all, like `leveldb::WriteBatch`.
/// Its encoded size must fit one WAL block ([`BLOCK`] bytes, each op
/// costing its key and value plus 23 bytes); a larger batch is refused
/// with `BatchTooLarge` rather than split.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Batch {
    ops: Vec<BatchOp>,
}

impl Batch {
    pub const fn new() -> Self {
        Self { ops: Vec::new() }
    }

    pub fn put(&mut self, key: &[u8], val: &[u8]) -> &mut Self {
        self.ops.push(BatchOp::Put(key.to_vec(), val.to_vec()));
        self
    }

    pub fn delete(&mut self, key: &[u8]) -> &mut Self {
        self.ops.push(BatchOp::Delete(key.to_vec()));
        self
    }

    pub fn ops(&self) -> &[BatchOp] {
        &self.ops
    }

    /// Bytes this batch takes in a WAL block.
    fn wal_bytes(&self) -> usize {
        self.ops
            .iter()
            .map(|op| {
                WAL_RECORD_OVERHEAD
                    + match op {
                        BatchOp::Put(k, v) => k.len() + v.len(),
                        BatchOp::Delete(k) => k.len(),
                    }
            })
            .sum()
    }

    /// The error horton would return for this batch's first bad op.
    fn check(&self) -> Result<(), DbError> {
        for op in &self.ops {
            let (key, val) = match op {
                BatchOp::Put(k, v) => (k, v.as_slice()),
                BatchOp::Delete(k) => (k, &[][..]),
            };
            check_key(key)?;
            if val.len() > VAL_MAX {
                return Err(Error::ValueTooLarge {
                    len: val.len(),
                    max: VAL_MAX,
                });
            }
        }
        let bytes = self.wal_bytes();
        if bytes > BLOCK {
            return Err(Error::BatchTooLarge { bytes, max: BLOCK });
        }
        Ok(())
    }
}

const fn check_key(key: &[u8]) -> Result<(), DbError> {
    if key.is_empty() {
        Err(Error::EmptyKey)
    } else if key.len() > KEY_MAX {
        Err(Error::KeyTooLarge {
            len: key.len(),
            max: KEY_MAX,
        })
    } else {
        Ok(())
    }
}

/// What the store has done, and the state of the database.
#[derive(Debug, Clone, Copy)]
pub struct Stats {
    pub open: OpenReport,
    pub device_blocks: u64,
    pub slots: SlotStats,
    pub level_tables: [usize; LEVELS],
    /// Blocks of range tombstones across every table.
    pub rdel_blocks: u64,
    pub io: IoStats,
    pub cache_hits: u64,
    pub cache_misses: u64,
    /// Write groups committed, and the client writes they carried.
    pub groups: u64,
    pub grouped_writes: u64,
    pub flushes: u64,
    pub compaction_jobs: u64,
    pub compaction_steps: u64,
}

/// Key-value pairs, in scan order.
pub type Entries = Vec<(Vec<u8>, Vec<u8>)>;

/// One page of a scan: the entries, and whether the range is exhausted.
type Page = (Entries, bool);

/// What a queued request asks for.
enum Work {
    Write(Batch),
    /// A point read of a key as of a sequence number.
    Get(Vec<u8>, u64),
}

/// A request in the queue. The thread that serves it sets `done` (a
/// write's value is `None`); its caller returns as soon as it sees that.
struct Pending {
    work: Work,
    done: OnceLock<Result<Option<Vec<u8>>, StoreError>>,
    /// The thread that queued it, to wake when it is done.
    waiter: std::thread::Thread,
}

impl Pending {
    fn finish(&self, result: Result<Option<Vec<u8>>, StoreError>) {
        let _ = self.done.set(result);
        // Free unless the thread has parked.
        self.waiter.unpark();
    }
}

/// What every [`Handle`] shares.
struct Shared {
    /// The database. `None` once the store is closed.
    engine: Mutex<Option<Engine>>,
    /// Point reads and writes waiting to be served, oldest first. Taken
    /// after the engine lock by the thread serving and alone by a thread
    /// queueing, never the other way round.
    queue: Mutex<VecDeque<Arc<Pending>>>,
    /// Client threads polling or parked for the engine lock. The
    /// compaction thread stands back while this is nonzero.
    waiting: AtomicUsize,
    /// The compaction thread has work (or must look for some).
    background_due: AtomicBool,
    /// The store is closing: the compaction thread exits.
    stop: AtomicBool,
    /// Where the compaction thread sleeps until `background_due` or
    /// `stop`.
    wake: (Mutex<()>, Condvar),
}

/// The lock guard of plain data that a panic cannot leave half changed.
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The engine, locked: derefs to the [`Engine`]. A client ends its turn
/// with [`finish`](Locked::finish), which keeps compaction moving and
/// wakes whoever is next.
struct Locked<'a> {
    shared: &'a Shared,
    guard: MutexGuard<'a, Option<Engine>>,
}

impl core::ops::Deref for Locked<'_> {
    type Target = Engine;
    fn deref(&self) -> &Engine {
        // `Shared::opened` builds a `Locked` only over `Some`, and nothing
        // takes the engine out while the guard is held.
        self.guard.as_ref().unwrap_or_else(|| unreachable!())
    }
}

impl core::ops::DerefMut for Locked<'_> {
    fn deref_mut(&mut self) -> &mut Engine {
        self.guard.as_mut().unwrap_or_else(|| unreachable!())
    }
}

impl Locked<'_> {
    /// Ends a client operation: one bounded compaction step if a job is
    /// pending, so the job keeps moving however busy the clients are,
    /// then the lock is released.
    fn finish(mut self) {
        if self.background_pending() {
            self.background_step();
        }
        let pending = self.background_pending();
        let shared = self.shared;
        drop(self);
        shared.wake_queue();
        if pending {
            shared.wake_background();
        }
    }
}

impl Shared {
    /// The locked engine, or [`StoreError::Closed`] once it is gone.
    fn opened<'a>(
        &'a self,
        guard: MutexGuard<'a, Option<Engine>>,
    ) -> Result<Locked<'a>, StoreError> {
        if guard.is_some() {
            Ok(Locked {
                shared: self,
                guard,
            })
        } else {
            Err(StoreError::Closed)
        }
    }

    /// One poll of the engine lock: `None` while another thread holds it.
    fn poll_engine(&self) -> Option<Result<Locked<'_>, StoreError>> {
        match self.engine.try_lock() {
            Ok(guard) => Some(self.opened(guard)),
            Err(TryLockError::Poisoned(_)) => Some(Err(StoreError::Closed)),
            Err(TryLockError::WouldBlock) => None,
        }
    }

    /// Takes the engine lock for a client: poll for [`SPIN`], then block.
    fn engine(&self) -> Result<Locked<'_>, StoreError> {
        if let Some(r) = self.poll_engine() {
            return r;
        }
        self.waiting.fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();
        let got = 'spin: loop {
            for _ in 0..POLLS_PER_YIELD {
                if let Some(r) = self.poll_engine() {
                    break 'spin r;
                }
                core::hint::spin_loop();
            }
            if started.elapsed() >= SPIN {
                break match self.engine.lock() {
                    Ok(guard) => self.opened(guard),
                    Err(_) => Err(StoreError::Closed),
                };
            }
            std::thread::yield_now();
        };
        self.waiting.fetch_sub(1, Ordering::Relaxed);
        got
    }

    /// Runs `op` on the engine and ends the operation (see
    /// [`Locked::finish`]).
    fn with<T>(&self, op: impl FnOnce(&mut Engine) -> Result<T, DbError>) -> Result<T, StoreError> {
        let mut engine = self.engine()?;
        let r = op(&mut engine);
        engine.finish();
        r.map_err(StoreError::Db)
    }

    /// Queues `work` and returns its result once some thread has served
    /// it: this one, if it gets the engine lock first, in which case it
    /// serves the rest of the queue too.
    fn submit(&self, work: Work) -> Result<Option<Vec<u8>>, StoreError> {
        let me = Arc::new(Pending {
            work,
            done: OnceLock::new(),
            waiter: std::thread::current(),
        });
        lock(&self.queue).push_back(Arc::clone(&me));
        let mut waiting_since: Option<Instant> = None;
        let mut polls = 0usize;
        let result = loop {
            if let Some(r) = me.done.get() {
                break r.clone();
            }
            match self.poll_engine() {
                Some(Ok(mut engine)) => {
                    // The queue is served from its head, which may not
                    // reach this request in one turn; then go again.
                    engine.serve(&self.queue, &me);
                    engine.finish();
                }
                Some(Err(e)) => {
                    // Closed: nobody will serve this request.
                    lock(&self.queue).retain(|p| !Arc::ptr_eq(p, &me));
                    break me.done.get().cloned().unwrap_or(Err(e));
                }
                None => {
                    // Another thread holds the engine, and may be serving
                    // this very request. Poll, then yield, then park.
                    let since = *waiting_since.get_or_insert_with(|| {
                        self.waiting.fetch_add(1, Ordering::Relaxed);
                        Instant::now()
                    });
                    polls += 1;
                    if polls % POLLS_PER_YIELD != 0 {
                        core::hint::spin_loop();
                    } else if since.elapsed() < SPIN {
                        std::thread::yield_now();
                    } else {
                        std::thread::park_timeout(PARK);
                    }
                }
            }
        };
        if waiting_since.is_some() {
            self.waiting.fetch_sub(1, Ordering::Relaxed);
        }
        result
    }

    /// Wakes the thread whose request heads the queue, if it has parked:
    /// the engine is free and its request is next. Called by whoever
    /// releases the engine lock.
    fn wake_queue(&self) {
        if let Some(head) = lock(&self.queue).front() {
            head.waiter.unpark();
        }
    }

    fn wake_background(&self) {
        if !self.background_due.swap(true, Ordering::AcqRel) {
            let _g = lock(&self.wake.0);
            self.wake.1.notify_one();
        }
    }

    /// The compaction thread: sleeps until a job is pending, then runs
    /// bounded steps back to back, standing back whenever a client is
    /// waiting for the lock.
    fn background(&self) {
        loop {
            {
                let mut g = lock(&self.wake.0);
                while !self.background_due.load(Ordering::Acquire)
                    && !self.stop.load(Ordering::Acquire)
                {
                    g = self.wake.1.wait(g).unwrap_or_else(PoisonError::into_inner);
                }
            }
            loop {
                if self.stop.load(Ordering::Acquire) {
                    return;
                }
                if self.waiting.load(Ordering::Relaxed) > 0 {
                    std::thread::sleep(BACKGROUND_PAUSE);
                    continue;
                }
                let Ok(mut guard) = self.engine.lock() else {
                    return;
                };
                let Some(engine) = guard.as_mut() else {
                    return;
                };
                if !engine.background_pending() {
                    // Cleared under the engine lock: a client that leaves
                    // a job pending after this sets it again.
                    self.background_due.store(false, Ordering::Release);
                    break;
                }
                engine.background_step();
                drop(guard);
                self.wake_queue();
            }
        }
    }
}

/// A cloneable, `Send` handle to an open store. Every call blocks the
/// calling thread until the operation is done.
#[derive(Clone)]
pub struct Handle {
    shared: Arc<Shared>,
}

/// An open store: a [`Handle`] plus the compaction thread. Dropping it
/// closes the store: the database and its file are released, and calls
/// through handles still alive return [`StoreError::Closed`].
pub struct Store {
    handle: Handle,
    thread: Option<JoinHandle<()>>,
}

impl core::ops::Deref for Store {
    type Target = Handle;
    fn deref(&self) -> &Handle {
        &self.handle
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        let shared = &self.handle.shared;
        shared.stop.store(true, Ordering::Release);
        {
            let _g = lock(&shared.wake.0);
            shared.wake.1.notify_one();
        }
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
        // Close: drop the database (and its file) now, whoever still
        // holds a handle.
        drop(lock(&shared.engine).take());
    }
}

impl Store {
    /// Opens (or creates) the store in the file at `path`, recovering it
    /// if the last process died mid-write.
    pub fn open(path: &Path, options: Options) -> Result<(Self, OpenReport), StoreError> {
        let device = FileDevice::open(path, options.create_blocks, options.sync)
            .map_err(|e| StoreError::Open(format!("{}: {e}", path.display())))?;
        // The database is built on the stack before it is boxed, and it
        // is megabytes: build it on a thread with room.
        let engine = std::thread::Builder::new()
            .name("horton-open".into())
            .stack_size(OPEN_STACK)
            .spawn(move || Engine::open(device, options.sync))
            .map_err(|e| StoreError::Open(e.to_string()))?
            .join()
            .map_err(|_| StoreError::Open("the opening thread panicked".into()))??;
        let report = engine.open;
        let shared = Arc::new(Shared {
            engine: Mutex::new(Some(engine)),
            queue: Mutex::new(VecDeque::new()),
            waiting: AtomicUsize::new(0),
            // Look once: recovery may have left a job pending.
            background_due: AtomicBool::new(true),
            stop: AtomicBool::new(false),
            wake: (Mutex::new(()), Condvar::new()),
        });
        let background = Arc::clone(&shared);
        let thread = std::thread::Builder::new()
            .name("horton-compact".into())
            .spawn(move || background.background())
            .map_err(|e| StoreError::Open(e.to_string()))?;
        Ok((
            Self {
                handle: Handle { shared },
                thread: Some(thread),
            },
            report,
        ))
    }

    /// A handle for another thread.
    pub fn handle(&self) -> Handle {
        self.handle.clone()
    }
}

impl Handle {
    /// Stores `key → val`. Returns once it is durable.
    pub fn put(&self, key: &[u8], val: &[u8]) -> Result<(), StoreError> {
        let mut b = Batch::new();
        b.put(key, val);
        self.write(b)
    }

    /// Deletes `key` (a no-op if it is absent). Returns once durable.
    pub fn delete(&self, key: &[u8]) -> Result<(), StoreError> {
        let mut b = Batch::new();
        b.delete(key);
        self.write(b)
    }

    /// Applies `batch` atomically. Returns once it is durable.
    pub fn write(&self, batch: Batch) -> Result<(), StoreError> {
        batch.check()?;
        self.shared.submit(Work::Write(batch)).map(drop)
    }

    /// Deletes every key in `[start, end)` with one range tombstone, like
    /// `RocksDB`'s `DeleteRange`.
    pub fn delete_range(&self, start: &[u8], end: &[u8]) -> Result<(), StoreError> {
        check_key(start)?;
        check_key(end)?;
        self.shared.with(|e| {
            e.room
                .with_room(&mut e.db, |db| block_on(db.delete_range(start, end)))
                .map(drop)
        })
    }

    /// The value of `key`, or `None`.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        self.shared.submit(Work::Get(key.to_vec(), u64::MAX))
    }

    /// The value of `key` as of `snapshot`.
    pub fn get_at(&self, key: &[u8], snapshot: &Snapshot) -> Result<Option<Vec<u8>>, StoreError> {
        self.shared.submit(Work::Get(key.to_vec(), snapshot.seq))
    }

    /// A consistent view of the store as it is now; released on drop.
    /// horton keeps at most eight live at once (`SnapshotLimit`).
    pub fn snapshot(&self) -> Result<Snapshot, StoreError> {
        let seq = self.shared.with(|e| e.db.snapshot())?;
        Ok(Snapshot {
            seq,
            shared: Arc::clone(&self.shared),
        })
    }

    /// One page of `[start, end)` as of sequence `seq`.
    fn page(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        reverse: bool,
        limit: usize,
        seq: u64,
    ) -> Result<Page, StoreError> {
        self.shared.with(|e| {
            if reverse {
                e.scan_rev(start, end, limit, seq)
            } else {
                e.scan_fwd(start, end, limit, seq)
            }
        })
    }

    /// Up to `limit` entries of `[start, end)` (an empty `start` is the
    /// first key, `end = None` the last), ascending or, with `reverse`,
    /// descending, as of `snapshot` (or now).
    pub fn scan(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        reverse: bool,
        limit: usize,
        snapshot: Option<&Snapshot>,
    ) -> Result<Entries, StoreError> {
        let seq = snapshot.map_or(u64::MAX, |s| s.seq);
        Ok(self.page(start, end, reverse, limit, seq)?.0)
    }

    /// Iterates `[start, end)` in order (or in reverse) over a snapshot
    /// it takes now, fetching [`PAGE`] entries at a time.
    pub fn range(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        reverse: bool,
    ) -> Result<Iter, StoreError> {
        Ok(Iter {
            handle: self.clone(),
            snapshot: self.snapshot()?,
            start: start.to_vec(),
            end: end.map(<[u8]>::to_vec),
            reverse,
            resume: None,
            page: Vec::new().into_iter(),
            done: false,
        })
    }

    /// Writes the memtable to a table now (horton does this by itself when
    /// the memtable fills).
    pub fn flush(&self) -> Result<(), StoreError> {
        self.shared.with(|e| e.room.flush(&mut e.db))
    }

    /// Flushes, then compacts every table once, down to the bottom level,
    /// like `LevelDB`'s `CompactRange(nullptr, nullptr)`. This frees the
    /// space of deleted data.
    pub fn compact(&self) -> Result<(), StoreError> {
        self.shared.with(|e| {
            e.room.flush(&mut e.db)?;
            e.db.request_compaction(0)?;
            while e.db.compaction_pending() {
                e.room.job(&mut e.db)?;
            }
            Ok(())
        })
    }

    pub fn stats(&self) -> Result<Stats, StoreError> {
        self.shared.with(|e| Ok(e.stats()))
    }
}

/// A pinned view: reads through it see the store as it was when it was
/// taken. Released when dropped.
pub struct Snapshot {
    seq: u64,
    shared: Arc<Shared>,
}

impl Snapshot {
    /// horton's sequence watermark for this view.
    pub const fn seq(&self) -> u64 {
        self.seq
    }
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        let seq = self.seq;
        // A closed store has no snapshots left to release.
        let _ = self.shared.with(|e| {
            e.db.release_snapshot(seq);
            Ok(())
        });
    }
}

/// A range iterator over a snapshot. Yields `Result`s: a failed page ends
/// the iteration after the error.
pub struct Iter {
    handle: Handle,
    snapshot: Snapshot,
    start: Vec<u8>,
    end: Option<Vec<u8>>,
    reverse: bool,
    /// The last key yielded: the next page starts there, exclusive.
    resume: Option<Vec<u8>>,
    page: std::vec::IntoIter<(Vec<u8>, Vec<u8>)>,
    done: bool,
}

impl Iterator for Iter {
    type Item = Result<(Vec<u8>, Vec<u8>), StoreError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(kv) = self.page.next() {
                self.resume = Some(kv.0.clone());
                return Some(Ok(kv));
            }
            if self.done {
                return None;
            }
            // The next page resumes at the last key yielded and skips it,
            // so a key of `KEY_MAX` bytes needs no successor computed.
            let (start, end) = match (&self.resume, self.reverse) {
                (Some(k), false) => (k.clone(), self.end.clone()),
                (Some(k), true) => (self.start.clone(), Some(k.clone())),
                (None, _) => (self.start.clone(), self.end.clone()),
            };
            let resumed = self.resume.is_some();
            let seq = self.snapshot.seq;
            let reverse = self.reverse;
            let page = self.handle.page(&start, end.as_deref(), reverse, PAGE, seq);
            match page {
                Ok((mut entries, done)) => {
                    // Forward pages restart at the resume key, inclusive.
                    if resumed && !reverse && entries.first().map(|e| &e.0) == self.resume.as_ref()
                    {
                        entries.remove(0);
                    }
                    self.done = done;
                    self.page = entries.into_iter();
                }
                Err(e) => {
                    self.done = true;
                    return Some(Err(e));
                }
            }
        }
    }
}

/// Stack for the thread that builds the database: `KvDb::new` returns the
/// whole database by value (memtable, cache, buffers) before it is boxed,
/// and a debug build keeps more than one copy on the way.
const OPEN_STACK: usize = 64 << 20;

/// The database and what serves it. One thread at a time holds it, under
/// [`Shared::engine`].
struct Engine {
    db: Box<KvDb<FileDevice>>,
    room: Room,
    batch: Box<GroupBatch>,
    /// The writes of the group being committed (kept to reuse its
    /// allocation).
    group: Vec<Arc<Pending>>,
    /// Where a point read lands before it is copied out.
    val: Box<[u8; VAL_MAX]>,
    /// Background compaction failed (say, `RegionFull`) when the store
    /// had flushed this many times: leave it to the write path, which
    /// reports the error to a caller, until a flush changes the tables. A
    /// failing job reads its inputs before it fails, so retrying it on
    /// every request would stall them all.
    compaction_stuck: Option<u64>,
    /// Commits are `fdatasync`ed, so a write group costs a disk flush.
    sync: bool,
    open: OpenReport,
    groups: u64,
    grouped_writes: u64,
}

impl Engine {
    #[allow(
        clippy::large_stack_frames,
        reason = "the database is built on the stack before it is boxed; the opening thread has 64 MiB"
    )]
    fn open(device: FileDevice, sync: bool) -> Result<Self, StoreError> {
        let blocks = device.blocks();
        let min = KvDb::<FileDevice>::MIN_DEVICE_BLOCKS;
        if blocks < min {
            return Err(StoreError::Open(format!(
                "the file holds {blocks} blocks; this shape needs at least {min}"
            )));
        }
        let mut db = Box::new(KvDb::new(device, horton::Config::whole_device(blocks)));
        let open = block_on(db.open())?;
        Ok(Self {
            db,
            room: Room {
                scratch: Box::new(KvCompaction::new()),
                flushes: 0,
                jobs: 0,
                steps: 0,
            },
            batch: Box::new(GroupBatch::new()),
            group: Vec::new(),
            val: Box::new([0u8; VAL_MAX]),
            compaction_stuck: None,
            sync,
            open,
            groups: 0,
            grouped_writes: 0,
        })
    }

    fn background_pending(&self) -> bool {
        self.db.compaction_pending() && self.compaction_stuck != Some(self.room.flushes)
    }

    fn background_step(&mut self) {
        if self.room.step(&mut self.db).is_err() {
            self.compaction_stuck = Some(self.room.flushes);
        }
    }

    fn get(&mut self, key: &[u8], seq: u64) -> Result<Option<Vec<u8>>, DbError> {
        let n = block_on(self.db.get_at(key, &mut self.val[..], seq))?;
        Ok(n.map(|n| self.val[..n].to_vec()))
    }

    /// Serves the queue from its head, in order: each point read, and
    /// each run of writes as a group. Stops when the queue is empty or
    /// after [`SERVE_MAX`] turns.
    ///
    /// `me` is the caller's own request. Once it is done the caller goes
    /// on serving others only while that is cheap: it stops before a
    /// write group that will be `fdatasync`ed. Its client's next write
    /// can then join that group instead of waiting a flush behind it,
    /// which is what makes synced groups large.
    fn serve(&mut self, queue: &Mutex<VecDeque<Arc<Pending>>>, me: &Pending) {
        for _ in 0..SERVE_MAX {
            let leave_writes = self.sync && me.done.get().is_some();
            let Some(next) = self.take_group(queue, leave_writes) else {
                return;
            };
            match &next.work {
                Work::Get(key, seq) => {
                    let r = self.get(key, *seq).map_err(StoreError::Db);
                    next.finish(r);
                }
                Work::Write(_) => self.commit_group(),
            }
        }
    }

    /// Takes the request at the head of `queue`. If it is a write, it and
    /// every write queued directly behind it that fits the same WAL
    /// block are moved into `self.group`; with `leave_writes`, a write at
    /// the head stays queued and nothing is taken.
    fn take_group(
        &mut self,
        queue: &Mutex<VecDeque<Arc<Pending>>>,
        leave_writes: bool,
    ) -> Option<Arc<Pending>> {
        let mut queue = lock(queue);
        if leave_writes && matches!(queue.front()?.work, Work::Write(_)) {
            return None;
        }
        let head = queue.pop_front()?;
        if let Work::Write(first) = &head.work {
            self.group.clear();
            // The head always goes: `Handle::write` checked that it fits
            // a block by itself.
            let (mut bytes, mut ops) = (first.wal_bytes(), first.ops.len());
            self.group.push(Arc::clone(&head));
            while let Some(Work::Write(b)) = queue.front().map(|p| &p.work) {
                bytes += b.wal_bytes();
                ops += b.ops.len();
                if bytes > BLOCK || ops > GROUP_OPS {
                    break;
                }
                self.group.extend(queue.pop_front());
            }
        }
        Some(head)
    }

    /// Commits the writes in `self.group` as one batch and tells each of
    /// them the result.
    fn commit_group(&mut self) {
        self.batch.clear();
        let mut result = Ok(());
        let writes = self.group.iter().filter_map(|p| match &p.work {
            Work::Write(b) => Some(b),
            Work::Get(..) => None,
        });
        for op in writes.flat_map(|b| &b.ops) {
            let r = match op {
                BatchOp::Put(k, v) => self.batch.put(k, v),
                BatchOp::Delete(k) => self.batch.delete(k),
            };
            // Unreachable: `Handle::write` checked every op, and the
            // group fits one block.
            if let Err(e) = r {
                result = Err(e.widen());
                break;
            }
        }
        let batch = &*self.batch;
        let result = result
            .and_then(|()| {
                self.room
                    .with_room(&mut self.db, |db| block_on(db.write(batch)))
            })
            .map(|_| None)
            .map_err(StoreError::Db);
        self.groups += 1;
        self.grouped_writes += self.group.len() as u64;
        for p in self.group.drain(..) {
            p.finish(result.clone());
        }
    }

    fn scan_fwd(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        limit: usize,
        seq: u64,
    ) -> Result<Page, DbError> {
        let mut scan = Box::new(KvScan::new(&self.db));
        block_on(scan.seek(start, end, seq))?;
        let mut key = [0u8; KEY_MAX];
        let mut val = vec![0u8; VAL_MAX];
        let mut out = Vec::new();
        while out.len() < limit {
            match block_on(scan.next(&mut key, &mut val))? {
                Some((kl, vl)) => out.push((key[..kl].to_vec(), val[..vl].to_vec())),
                None => return Ok((out, true)),
            }
        }
        Ok((out, false))
    }

    /// Descending over `[start, end)`. horton's reverse scan starts at
    /// the greatest key `<= from` and stops above an exclusive lower
    /// bound, so the exclusive `end` is skipped here and the inclusive
    /// `start` is checked here.
    fn scan_rev(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        limit: usize,
        seq: u64,
    ) -> Result<Page, DbError> {
        // horton reads an empty `from` as "from the last key"; an empty
        // exclusive end is an empty range, refused as the forward scan
        // refuses it.
        if end == Some(&[]) {
            return Err(Error::EmptyKey);
        }
        let mut scan = Box::new(KvRevScan::new(&self.db));
        block_on(scan.seek_prev(end.unwrap_or(&[]), None, seq))?;
        let mut key = [0u8; KEY_MAX];
        let mut val = vec![0u8; VAL_MAX];
        let mut out = Vec::new();
        while out.len() < limit {
            match block_on(scan.prev(&mut key, &mut val))? {
                Some((kl, _)) if Some(&key[..kl]) == end => {}
                Some((kl, _)) if &key[..kl] < start => return Ok((out, true)),
                Some((kl, vl)) => out.push((key[..kl].to_vec(), val[..vl].to_vec())),
                None => return Ok((out, true)),
            }
        }
        Ok((out, false))
    }

    fn stats(&self) -> Stats {
        let mut level_tables = [0; LEVELS];
        let mut rdel_blocks = 0;
        for (l, n) in level_tables.iter_mut().enumerate() {
            let tables = self.db.level_tables(l).unwrap_or(&[]);
            *n = tables.len();
            rdel_blocks += tables.iter().map(|t| u64::from(t.rdel_blocks)).sum::<u64>();
        }
        let cache = self.db.cache_stats();
        Stats {
            open: self.open,
            device_blocks: self.db.device().blocks(),
            slots: self.db.slot_stats(),
            level_tables,
            rdel_blocks,
            io: self.db.device().stats(),
            cache_hits: cache.hits,
            cache_misses: cache.misses,
            groups: self.groups,
            grouped_writes: self.grouped_writes,
            flushes: self.room.flushes,
            compaction_jobs: self.room.jobs,
            compaction_steps: self.room.steps,
        }
    }
}

/// Makes room when horton asks for it: the compaction scratch, and counts
/// of what it did.
struct Room {
    scratch: Box<KvCompaction>,
    flushes: u64,
    jobs: u64,
    steps: u64,
}

impl Room {
    /// Runs `op`; when horton says the database needs room, makes it the
    /// way the error says and tries again.
    fn with_room<T>(
        &mut self,
        db: &mut KvDb<FileDevice>,
        mut op: impl FnMut(&mut KvDb<FileDevice>) -> Result<T, DbError>,
    ) -> Result<T, DbError> {
        for _ in 0..ROOM_TRIES {
            match op(db) {
                Err(Error::TableFull | Error::ArenaFull | Error::WalFull) => self.flush(db)?,
                Err(Error::NeedsCompaction) => self.job(db)?,
                r => return r,
            }
        }
        op(db)
    }

    /// Flushes the memtable, compacting first if level 0 is full.
    fn flush(&mut self, db: &mut KvDb<FileDevice>) -> Result<(), DbError> {
        for _ in 0..ROOM_TRIES {
            match block_on(db.flush()) {
                Err(Error::NeedsCompaction) => self.job(db)?,
                r => {
                    self.flushes += u64::from(r.is_ok());
                    return r;
                }
            }
        }
        block_on(db.flush())
    }

    /// Runs one compaction job (or finishes the one in flight).
    fn job(&mut self, db: &mut KvDb<FileDevice>) -> Result<(), DbError> {
        while self.step(db)? == Progress::More {}
        Ok(())
    }

    /// One bounded compaction step.
    fn step(&mut self, db: &mut KvDb<FileDevice>) -> Result<Progress, DbError> {
        let p = block_on(db.compact_step(&mut self.scratch))?;
        self.steps += 1;
        self.jobs += u64::from(p == Progress::Done);
        Ok(p)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A store in a fresh file, deleted when dropped.
    struct TempStore {
        store: Option<Store>,
        path: std::path::PathBuf,
    }

    impl TempStore {
        fn new(name: &str) -> Self {
            let path = std::env::temp_dir()
                .join(format!("horton-kvstore-{}-{name}.db", std::process::id()));
            let _ = std::fs::remove_file(&path);
            let store = Some(Self::open_at(&path));
            Self { store, path }
        }

        fn open_at(path: &Path) -> Store {
            let options = Options {
                create_blocks: KvDb::<FileDevice>::MIN_DEVICE_BLOCKS + 64,
                sync: false,
            };
            Store::open(path, options).expect("open").0
        }

        fn reopen(&mut self) {
            drop(self.store.take());
            self.store = Some(Self::open_at(&self.path));
        }
    }

    impl core::ops::Deref for TempStore {
        type Target = Store;
        fn deref(&self) -> &Store {
            self.store.as_ref().expect("open")
        }
    }

    impl Drop for TempStore {
        fn drop(&mut self) {
            drop(self.store.take());
            let _ = std::fs::remove_file(&self.path);
        }
    }

    fn key(i: usize) -> Vec<u8> {
        format!("k{i:06}").into_bytes()
    }

    fn keys(store: &Handle, start: &[u8], end: Option<&[u8]>, reverse: bool) -> Vec<Vec<u8>> {
        store
            .range(start, end, reverse)
            .expect("range")
            .map(|kv| kv.expect("page").0)
            .collect()
    }

    #[test]
    fn ranges_page_exactly_in_both_directions() {
        let store = TempStore::new("pages");
        // Two and a bit pages, so ranges cross page boundaries.
        let n = 2 * PAGE + 3;
        for chunk in (0..n).collect::<Vec<_>>().chunks(100) {
            let mut b = Batch::new();
            for &i in chunk {
                b.put(&key(i), b"v");
            }
            store.write(b).expect("write");
        }
        let all: Vec<_> = (0..n).map(key).collect();
        assert_eq!(keys(&store, b"", None, false), all);
        let mut rev = all;
        rev.reverse();
        assert_eq!(keys(&store, b"", None, true), rev);

        // [start, end): start inclusive, end exclusive, both directions,
        // with the bounds on and between page edges.
        for (a, b) in [
            (0, n),
            (1, PAGE),
            (PAGE - 1, PAGE + 1),
            (5, 2 * PAGE + 1),
            (7, 7),
        ] {
            let want: Vec<_> = (a..b).map(key).collect();
            assert_eq!(
                keys(&store, &key(a), Some(&key(b)), false),
                want,
                "[{a}, {b})"
            );
            let mut want_rev = want.clone();
            want_rev.reverse();
            assert_eq!(
                keys(&store, &key(a), Some(&key(b)), true),
                want_rev,
                "rev [{a}, {b})"
            );
        }
        // Bounds that fall between keys.
        let mid = |i: usize| {
            let mut k = key(i);
            k.push(b'x');
            k
        };
        let want: Vec<_> = (11..=PAGE + 20).map(key).collect();
        assert_eq!(keys(&store, &mid(10), Some(&mid(PAGE + 20)), false), want);
        let mut want_rev = want;
        want_rev.reverse();
        assert_eq!(
            keys(&store, &mid(10), Some(&mid(PAGE + 20)), true),
            want_rev
        );
        // Past the last key, and an empty end.
        assert!(keys(&store, b"z", None, false).is_empty());
        assert!(keys(&store, b"z", None, true).is_empty());
        assert_eq!(
            store.scan(b"", Some(b""), true, 10, None),
            Err(StoreError::Db(Error::EmptyKey))
        );
    }

    #[test]
    fn an_iterator_keeps_its_snapshot_across_pages() {
        let store = TempStore::new("snapshot-pages");
        let n = PAGE + 50;
        for i in 0..n {
            store.put(&key(i), b"old").expect("put");
        }
        let mut it = store.range(b"", None, false).expect("range");
        let first = it.next().expect("one").expect("page");
        assert_eq!(first, (key(0), b"old".to_vec()));
        // Rewrite and delete behind the iterator's back.
        for i in 0..n {
            if i % 2 == 0 {
                store.delete(&key(i)).expect("delete");
            } else {
                store.put(&key(i), b"new").expect("put");
            }
        }
        let rest: Vec<_> = it.map(|kv| kv.expect("page")).collect();
        assert_eq!(rest.len(), n - 1);
        assert!(rest.iter().all(|(_, v)| v == b"old"));
        // A new iterator sees the new state.
        let now: Vec<_> = store
            .range(b"", None, false)
            .expect("range")
            .map(|kv| kv.expect("page"))
            .collect();
        assert_eq!(now.len(), n / 2);
        assert!(now.iter().all(|(_, v)| v == b"new"));
    }

    #[test]
    fn snapshots_are_released_when_dropped() {
        let store = TempStore::new("snapshots");
        let held: Vec<_> = (0..8)
            .map(|_| store.snapshot().expect("snapshot"))
            .collect();
        assert!(matches!(
            store.snapshot(),
            Err(StoreError::Db(Error::SnapshotLimit))
        ));
        assert!(matches!(
            store.range(b"", None, false),
            Err(StoreError::Db(Error::SnapshotLimit))
        ));
        drop(held);
        store.snapshot().expect("released slots are free again");
    }

    #[test]
    fn bad_writes_fail_alone() {
        let store = TempStore::new("bad-writes");
        assert_eq!(store.put(b"", b"v"), Err(StoreError::Db(Error::EmptyKey)));
        assert!(matches!(
            store.put(&[b'k'; KEY_MAX + 1], b"v"),
            Err(StoreError::Db(Error::KeyTooLarge { .. }))
        ));
        assert!(matches!(
            store.put(b"k", &[0; VAL_MAX + 1]),
            Err(StoreError::Db(Error::ValueTooLarge { .. }))
        ));
        let mut big = Batch::new();
        for i in 0..8 {
            big.put(&key(i), &[b'v'; VAL_MAX]);
        }
        assert!(matches!(
            store.write(big),
            Err(StoreError::Db(Error::BatchTooLarge { .. }))
        ));
        // Nothing of the refused batch landed, and the store still works.
        assert_eq!(store.get(&key(0)), Ok(None));
        store.put(b"k", &[b'v'; VAL_MAX]).expect("a maximal value");
        assert_eq!(store.get(b"k"), Ok(Some(vec![b'v'; VAL_MAX])));
    }

    #[test]
    fn concurrent_writers_fill_flush_compact_and_reopen() {
        let mut store = TempStore::new("load");
        let (threads, per) = (4, 1500);
        std::thread::scope(|s| {
            for t in 0..threads {
                let h = store.handle();
                s.spawn(move || {
                    for i in 0..per {
                        // Overwrites: every key is written twice.
                        let k = key(t * per + i % (per / 2));
                        h.put(&k, format!("{t}:{i}").as_bytes()).expect("put");
                    }
                });
            }
        });
        let stats = store.stats().expect("stats");
        assert_eq!(stats.grouped_writes, (threads * per) as u64);
        assert!(stats.groups <= stats.grouped_writes);
        assert!(
            stats.flushes > 0,
            "6000 writes overflow a 4096-entry memtable"
        );
        // How often the writes flushed depends on how they grouped (one
        // WAL block per commit): flush eight more tables by hand, so
        // level 0 (7 tables) fills and compaction must run.
        for round in 0..8 {
            store
                .put(format!("z{round}").as_bytes(), b"filler")
                .expect("put");
            store.flush().expect("flush");
        }
        store.compact().expect("compact");
        assert!(store.stats().expect("stats").compaction_jobs > 0);

        let check = |store: &Handle| {
            for t in 0..threads {
                // A sample: point reads are slow in debug builds (every
                // probed block's CRC is checked).
                for i in (0..per / 2).step_by(37) {
                    let want = format!("{t}:{}", i + per / 2);
                    assert_eq!(
                        store.get(&key(t * per + i)).expect("get"),
                        Some(want.into_bytes())
                    );
                }
            }
            let n = store.range(b"", None, false).expect("range").count();
            assert_eq!(n, threads * per / 2 + 8);
        };
        check(&store);
        store
            .delete_range(&key(0), &key(per))
            .expect("delete_range");
        store.reopen();
        assert_eq!(store.get(&key(0)), Ok(None));
        assert_eq!(
            store.range(b"", None, false).expect("range").count(),
            (threads - 1) * per / 2 + 8
        );
    }

    #[test]
    fn compact_gives_deleted_space_back() {
        let store = TempStore::new("reclaim");
        for i in 0..300 {
            store.put(&key(i), b"value").expect("put");
        }
        store.flush().expect("flush");
        assert_eq!(store.stats().expect("stats").slots.used, 1);
        store
            .delete_range(&key(0), &key(300))
            .expect("delete_range");
        store.compact().expect("compact");
        // The range tombstone and the data it hid are both gone.
        let stats = store.stats().expect("stats");
        assert_eq!(stats.slots.used, 0, "{:?}", stats.slots);
        assert_eq!(store.range(b"", None, false).expect("range").count(), 0);
    }

    /// Flat combining: a thread's request is often served by another
    /// thread, and requests are served in the order they were queued. So
    /// a thread always reads its own last write, whoever did the work,
    /// and never a later or an earlier one.
    #[test]
    fn every_thread_reads_its_own_writes_through_the_queue() {
        let store = TempStore::new("combine");
        std::thread::scope(|s| {
            for t in 0..6usize {
                let h = store.handle();
                s.spawn(move || {
                    let k = key(t);
                    for i in 0..400usize {
                        let v = format!("{t}:{i}").into_bytes();
                        h.put(&k, &v).expect("put");
                        assert_eq!(h.get(&k).expect("get"), Some(v), "thread {t} write {i}");
                        if i % 50 == 49 {
                            h.delete(&k).expect("delete");
                            assert_eq!(h.get(&k).expect("get"), None, "thread {t} delete {i}");
                        }
                    }
                    h.put(&k, b"last").expect("put");
                });
            }
        });
        for t in 0..6usize {
            assert_eq!(store.get(&key(t)).expect("get"), Some(b"last".to_vec()));
        }
        let stats = store.stats().expect("stats");
        // 400 puts, 8 deletes and the last put, from each of 6 threads.
        assert_eq!(stats.grouped_writes, 6 * 409);
        assert!(stats.groups <= stats.grouped_writes);
    }

    /// Dropping the `Store` closes it. Handles that outlive it fail
    /// cleanly, and the file is free to be opened again at once.
    #[test]
    fn a_closed_store_refuses_calls_and_frees_its_file() {
        let mut store = TempStore::new("closed");
        store.put(b"k", b"v").expect("put");
        let stale = store.handle();
        let snapshot = stale.snapshot().expect("snapshot");
        store.reopen();
        assert_eq!(stale.get(b"k"), Err(StoreError::Closed));
        assert_eq!(stale.put(b"k", b"w"), Err(StoreError::Closed));
        assert_eq!(stale.flush(), Err(StoreError::Closed));
        assert!(matches!(
            stale.range(b"", None, false),
            Err(StoreError::Closed)
        ));
        // Releasing a snapshot of a closed store is a no-op, not a panic.
        drop(snapshot);
        // The reopened store has the write, and none of the refused ones.
        assert_eq!(store.get(b"k").expect("get"), Some(b"v".to_vec()));
    }

    /// Reads, writes, scans and a forced compaction from many threads at
    /// once: every operation completes (no request is left unserved, no
    /// thread is left parked) and the store ends up exactly as written.
    #[test]
    fn mixed_load_leaves_nothing_waiting() {
        let store = TempStore::new("mixed");
        std::thread::scope(|s| {
            for t in 0..4usize {
                let h = store.handle();
                s.spawn(move || {
                    for i in 0..1500usize {
                        let k = key(t * 10_000 + i);
                        h.put(&k, &k).expect("put");
                        if i % 97 == 0 {
                            let page = h
                                .scan(&key(t * 10_000), None, false, 5, None)
                                .expect("scan");
                            assert_eq!(page.first().map(|e| e.0.clone()), Some(key(t * 10_000)));
                        }
                        if i % 500 == 499 {
                            h.flush().expect("flush");
                        }
                    }
                });
            }
            let h = store.handle();
            s.spawn(move || {
                for _ in 0..3 {
                    h.compact().expect("compact");
                }
            });
        });
        for t in 0..4usize {
            let got = keys(
                &store,
                &key(t * 10_000),
                Some(&key(t * 10_000 + 9999)),
                false,
            );
            assert_eq!(got.len(), 1500, "thread {t}");
        }
    }
}
