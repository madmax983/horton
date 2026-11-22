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
//! - **The lock holder commits for everyone waiting.** A write is queued
//!   first; then its thread goes for the lock. Whoever gets it commits
//!   the queue in order, its own write and the others', and marks each
//!   done. The threads it served see their result and never take the
//!   lock. This is flat combining, and it matters because the database
//!   is megabytes of state that would otherwise move between CPU caches
//!   on every call: four threads taking turns at the lock write slower
//!   than one thread alone.
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
//! - **Background compaction.** While a job is pending, every write
//!   ends with one bounded `compact_step` before it gives the lock up,
//!   and a compaction thread runs them back to back whenever no client
//!   is waiting. Writers are never stalled for a whole compaction, which
//!   is what `LevelDB`'s L0 slowdown trigger approximates with sleeps.
//! - **Point reads run in parallel, and most take no lock** (ADR-0018,
//!   ADR-0023). A read is two phases: what the memtable holds for the
//!   key, then the tables. The second runs on the caller's thread, with
//!   its own buffers, through a read-only handle to the file and a cache
//!   shared by all readers ([`SharedCache`]), in which a hit takes no
//!   lock and writes nothing another reader reads. The first needs the
//!   database only when the memtable could hold the key. A filter over
//!   the keys written since the last flush says when it cannot, and the
//!   store publishes which manifest is current; a reader whose own copy
//!   of the manifest is that one, reading a key the filter does not
//!   have, goes straight to the tables ([`Gate::enter`]). It takes no
//!   lock and writes one word, its own. Any other read takes the engine
//!   lock itself for the memtable probe, a fraction of a microsecond,
//!   and joins no queue.
//! - **So do scans** (ADR-0019). A scan runs a page at a time. For each
//!   page the caller takes the engine lock, copies the slice of the
//!   memtable the page can reach, and lets go; then it merges that copy
//!   with the tables on its own thread, with a pool of block buffers so
//!   that each table's block stays decoded while the merge is at another
//!   table. The copy says where it stops knowing (its horizon), and the
//!   page stops there.
//! - **Flush and compaction wait for readers.** They are the only calls
//!   that write tables, and a reader must not have a table rewritten
//!   under it. A read in its table phase holds a [`Pin`]. Flush and each
//!   compaction step, which run under the engine lock, first shut the
//!   readers' gate and wait for the pins to drain ([`Gate::shut`]): a
//!   reader without the lock sees the gate shut and waits for the lock
//!   like the rest. A table phase is tens of microseconds, so the wait
//!   is short.
//! - **Snapshots and iterators.** A [`Snapshot`] is horton's sequence
//!   watermark, released when dropped. An [`Iter`] pins one for its
//!   lifetime, like a `LevelDB` iterator, and pages through the range.
//!
//! Every call returns after the operation completes, so a write that
//! returned `Ok` is on the disk (or, without `sync`, in the operating
//! system's hands).

use std::cell::RefCell;
use std::collections::VecDeque;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, PoisonError, TryLockError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use horton::wal::WAL_RECORD_OVERHEAD;
use horton::{CachePort, Error, OpenReport, Progress, ScanBlock, SlotStats, WriteBatch};

use crate::cache::SharedCache;
use crate::device::{FileDevice, IoError, IoStats, ReadDevice};
use crate::gate::{Gate, Pin, Slot};
use crate::{
    BLOCK, KEY_MAX, KvCompaction, KvDb, KvManifest, KvMemPage, KvProbe, KvReadScratch, KvRevScan,
    KvScan, KvTables, LEVELS, VAL_MAX, block_on,
};

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

/// Looks a reader takes at a held engine lock before it counts itself
/// as waiting. Another reader holds the lock for well under a
/// microsecond, so the lock is usually free again within these.
const READ_POLLS: usize = 32;

/// Block buffers a scanning thread lends each scan: one for every table
/// the store can hold, and a few for index blocks. 512 KiB a thread.
const SCAN_BLOCKS: usize = 32;

/// Memtable-page buffers shared by all scanning threads (1.2 MiB each).
/// A page of a scan holds one while it runs; more scanners than this
/// wait their turn.
const SCAN_PAGES: usize = 8;

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
    /// Blocks of bloom filter across every table: one for a small
    /// table, up to sixteen for a full one.
    pub bloom_blocks: u64,
    pub io: IoStats,
    pub cache_hits: u64,
    pub cache_misses: u64,
    /// Write groups committed, and the client writes they carried.
    pub groups: u64,
    pub grouped_writes: u64,
    pub flushes: u64,
    pub compaction_jobs: u64,
    pub compaction_steps: u64,
    /// Point reads that reached the tables without the engine lock, and
    /// point reads that took it.
    pub reads_unlocked: u64,
    pub reads_locked: u64,
}

/// Key-value pairs, in scan order.
pub type Entries = Vec<(Vec<u8>, Vec<u8>)>;

/// A memtable-page buffer on loan from the store's pool; it goes back
/// when dropped, whichever thread drops it.
struct PageLease {
    page: Option<Box<KvMemPage>>,
    home: Arc<Mutex<Vec<Box<KvMemPage>>>>,
}

impl PageLease {
    /// Takes a buffer from `pool`, waiting while every one is lent out.
    fn take(pool: &Arc<Mutex<Vec<Box<KvMemPage>>>>) -> Self {
        loop {
            let page = lock(pool).pop();
            if page.is_some() {
                return Self {
                    page,
                    home: Arc::clone(pool),
                };
            }
            std::thread::yield_now();
        }
    }
}

/// The store's memtable-page buffers. Each is over a megabyte and is
/// built on the stack before it is boxed: call this on a thread with
/// room.
#[allow(
    clippy::large_stack_frames,
    reason = "a page is built on the stack before it is boxed; the opening thread has 64 MiB"
)]
fn new_scan_pages() -> Vec<Box<KvMemPage>> {
    (0..SCAN_PAGES)
        .map(|_| Box::new(KvMemPage::new()))
        .collect()
}

impl Drop for PageLease {
    fn drop(&mut self) {
        if let Some(page) = self.page.take() {
            lock(&self.home).push(page);
        }
    }
}

/// What a scan page takes from the database before it merges: where its
/// copy of the memtable stops knowing (the nearest key it does not hold,
/// where the page must stop; `None` when it holds the rest of the
/// range), the manifest of that moment, and the node id.
struct PageStart {
    horizon: Option<Vec<u8>>,
    tables: Arc<KvManifest>,
    node: u32,
}

/// A write in the queue. The thread that commits it sets `done`; its
/// caller returns as soon as it sees that.
struct Pending {
    batch: Batch,
    done: OnceLock<Result<(), StoreError>>,
    /// The thread that queued it, to wake when it is done.
    waiter: std::thread::Thread,
}

impl Pending {
    fn finish(&self, result: Result<(), StoreError>) {
        let _ = self.done.set(result);
        // Free unless the thread has parked.
        self.waiter.unpark();
    }

    /// The outcome, once committed.
    fn reply(&self) -> Result<(), StoreError> {
        self.done.get().cloned().unwrap_or(Err(StoreError::Closed))
    }
}

/// What a reader thread keeps for its point reads: the probe the
/// memtable's part is written into, the manifest it last read by, the
/// buffers of the table phase (two blocks and a value), and its slot in
/// the gate of the store it last read.
struct Reader {
    probe: KvProbe,
    /// The engine's manifest as this thread last saw it. Compared by
    /// address with the engine's under the lock, and replaced only when
    /// the engine's has: a read then takes no reference count. Without
    /// the lock, its sequence number is compared with the one the gate
    /// publishes.
    tables: Option<Arc<KvManifest>>,
    scratch: KvReadScratch,
    val: [u8; VAL_MAX],
    /// Where this thread's pins go; see [`Gate`].
    slot: Arc<Slot>,
    /// [`Gate::id`] of the store `tables` and `slot` belong to (0: none
    /// yet).
    store: u64,
}

impl Reader {
    /// Makes this thread's reader state `gate`'s store's, and returns
    /// the thread's slot there.
    ///
    /// A thread that last read another store forgets that store's
    /// manifest here. Two stores' manifests can carry the same sequence
    /// number, and the number is all a read without the lock compares.
    fn attach(&mut self, gate: &Gate) -> &Arc<Slot> {
        if self.store != gate.id() {
            self.tables = None;
            self.slot = gate.register();
            self.store = gate.id();
        }
        &self.slot
    }
}

thread_local! {
    /// Every thread that reads keeps its own (40 KiB).
    static READER: RefCell<Box<Reader>> = RefCell::new(Box::new(Reader {
        probe: KvProbe::new(),
        tables: None,
        scratch: KvReadScratch::new(),
        val: [0u8; VAL_MAX],
        slot: Arc::new(Slot::unregistered()),
        store: 0,
    }));

    /// Every thread that scans keeps its own block buffers to lend its
    /// scans (see [`SCAN_BLOCKS`]), built the first time it scans.
    static SCAN_POOL: RefCell<Vec<ScanBlock<BLOCK>>> = const { RefCell::new(Vec::new()) };
}

/// Where a scan goes on from, between pages.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Cursor {
    key: Vec<u8>,
    /// True when `key` was the last entry yielded (the next page starts
    /// past it); false when the last page stopped short of `key`, at its
    /// memtable copy's horizon (the next page starts on it).
    yielded: bool,
}

/// What every [`Handle`] shares.
struct Shared {
    /// The database. `None` once the store is closed.
    engine: Mutex<Option<Engine>>,
    /// Writes waiting to be committed, oldest first. Taken after the
    /// engine lock by the thread committing and alone by a thread
    /// queueing, never the other way round. Reads do not come here.
    queue: Mutex<VecDeque<Arc<Pending>>>,
    /// How many writes are in `queue`. A reader giving up the engine
    /// looks at this, not at the queue: while no write waits it reads a
    /// word nobody is writing.
    queued: AtomicUsize,
    /// A hint that the engine lock is held, kept by whoever holds it. A
    /// reader that finds the lock taken watches this instead of trying
    /// the lock again and again: a failed try still writes the lock's
    /// cache line, and four readers doing that keep it from the thread
    /// that needs it to let go. Nothing depends on the hint being right.
    engine_held: AtomicBool,
    /// Client threads polling or parked for the engine lock. The
    /// compaction thread stands back while this is nonzero.
    waiting: AtomicUsize,
    /// The compaction thread has work (or must look for some).
    background_due: AtomicBool,
    /// The store is closing: the compaction thread exits.
    stop: AtomicBool,
    /// What lets a read reach the tables without the engine lock, and
    /// counts the readers in them (live [`Pin`]s). The engine and its
    /// [`Room`] hold the other references.
    gate: Arc<Gate>,
    /// The file, for those readers.
    read_device: ReadDevice,
    /// Their block cache.
    cache: SharedCache,
    /// Memtable-page buffers for scans; see [`PageLease`].
    pages: Arc<Mutex<Vec<Box<KvMemPage>>>>,
    /// Test hook: the next read to reach its table phase reports in on
    /// the sender and stays there, pinned, until the receiver yields.
    #[cfg(test)]
    hold_next_reader: Mutex<Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>>,
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

impl Drop for Locked<'_> {
    fn drop(&mut self) {
        // Before the guard goes (fields drop after this): see
        // `Shared::engine_held`.
        self.shared.engine_held.store(false, Ordering::Relaxed);
    }
}

impl Locked<'_> {
    /// Ends a client operation: one bounded compaction step if a job is
    /// pending and no reader is in its table phase, then the lock is
    /// released.
    ///
    /// The step is skipped, not waited for, while readers hold pins: a
    /// write should not stand behind other threads' reads for the sake
    /// of compaction. The compaction thread, and any write that needs
    /// room, do wait.
    fn finish(mut self) {
        if self.background_pending() && self.gate.pinned() == 0 {
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

impl Locked<'_> {
    /// Ends a read's hold of the engine: the lock is released and
    /// nothing else is done under it. No compaction step: the reader
    /// holds a pin, and a step would wait for it. If a write is queued
    /// its thread is woken, in case it parked while readers had the
    /// lock.
    fn release(self) {
        let pending = self.background_pending();
        let shared = self.shared;
        drop(self);
        if shared.queued.load(Ordering::Relaxed) > 0 {
            shared.wake_queue();
        }
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
            self.engine_held.store(true, Ordering::Relaxed);
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
                break self
                    .engine
                    .lock()
                    .map_or_else(|_| Err(StoreError::Closed), |guard| self.opened(guard));
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
        let r = op(&mut engine).map_err(StoreError::Db);
        engine.finish();
        r
    }

    /// Takes the engine lock for a read. A reader holds it for a
    /// fraction of a microsecond, so another reader that finds it taken
    /// looks again a few times before it joins the threads counted as
    /// waiting ([`engine`](Self::engine)).
    fn engine_for_read(&self) -> Result<Locked<'_>, StoreError> {
        for round in 0..READ_POLLS {
            if !self.engine_held.load(Ordering::Relaxed)
                && let Some(r) = self.poll_engine()
            {
                return r;
            }
            // Back off a little longer each time, so that readers who
            // collided do not all come back at once.
            for _ in 0..(1usize << round.min(4)) {
                core::hint::spin_loop();
            }
        }
        self.engine()
    }

    /// The part of a read that needs the database, under the engine
    /// lock, which this thread takes for itself: `op` is the memtable
    /// probe or page copy, and `pins` says whether what it found needs
    /// the tables. `slot` is the calling thread's.
    ///
    /// The pin is made here, in the same hold of the lock: flush and
    /// compaction, which need that lock, cannot have started since `op`
    /// read the manifest, and will wait for the pin before they do.
    #[allow(
        clippy::significant_drop_tightening,
        reason = "`release` gives the lock up, right after the pin is made under it"
    )]
    fn begin<'s, T>(
        &self,
        slot: &'s Slot,
        op: impl FnOnce(&mut Engine) -> Result<T, DbError>,
        pins: impl FnOnce(&T) -> bool,
    ) -> Result<(T, Option<Pin<'s>>), StoreError> {
        let mut engine = self.engine_for_read()?;
        let begun = op(&mut engine);
        let pin = begun.as_ref().is_ok_and(pins).then(|| Pin::new(slot));
        engine.release();
        Ok((begun.map_err(StoreError::Db)?, pin))
    }

    /// Queues `batch` and returns the request once some thread has
    /// committed it: this one, if it gets the engine lock first, in
    /// which case it commits the rest of the queue too. The outcome is
    /// in the request ([`Pending::reply`]).
    fn submit(&self, batch: Batch) -> Result<Arc<Pending>, StoreError> {
        let me = Arc::new(Pending {
            batch,
            done: OnceLock::new(),
            waiter: std::thread::current(),
        });
        {
            let mut queue = lock(&self.queue);
            queue.push_back(Arc::clone(&me));
            self.queued.store(queue.len(), Ordering::Relaxed);
        }
        let mut waiting_since: Option<Instant> = None;
        let mut polls = 0usize;
        let closed = loop {
            if me.done.get().is_some() {
                break None;
            }
            match self.poll_engine() {
                Some(Ok(mut engine)) => {
                    // The queue is served from its head, which may not
                    // reach this request in one turn; then go again.
                    engine.serve(self, &me);
                    engine.finish();
                }
                Some(Err(e)) => {
                    // Closed: nobody will serve this request (unless
                    // somebody just did).
                    let mut queue = lock(&self.queue);
                    queue.retain(|p| !Arc::ptr_eq(p, &me));
                    self.queued.store(queue.len(), Ordering::Relaxed);
                    drop(queue);
                    break me.done.get().is_none().then_some(e);
                }
                None => {
                    // Another thread holds the engine, and may be serving
                    // this very request. Poll, then yield, then park.
                    let since = *waiting_since.get_or_insert_with(|| {
                        self.waiting.fetch_add(1, Ordering::Relaxed);
                        Instant::now()
                    });
                    polls += 1;
                    if !polls.is_multiple_of(POLLS_PER_YIELD) {
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
        closed.map_or(Ok(me), Err)
    }

    /// A point read of `key` as of sequence `seq`.
    ///
    /// **Without the engine lock, when the memtable cannot hold the
    /// key.** The thread raises its pin and looks at three words nobody
    /// is writing ([`Gate::enter`]): the gate is open, the manifest it
    /// read by last time is still the current one, and the filter over
    /// the memtable does not have the key. Then the memtable's part of
    /// the read is known without asking the database (nothing), and the
    /// table phase starts at once.
    ///
    /// **Otherwise under the engine lock**, which this thread takes
    /// itself: it probes the memtable into its own probe, looks at which
    /// manifest is current, pins the tables if the read will need them,
    /// and lets go. It joins no queue and waits for no other thread to
    /// serve it.
    ///
    /// Either way the table phase runs here, alongside every other
    /// reader's.
    fn read(&self, key: &[u8], seq: u64) -> Result<Option<Vec<u8>>, StoreError> {
        if key.len() > KEY_MAX {
            // No stored key is this long.
            return Ok(None);
        }
        READER.with(|reader| {
            let mut reader = reader.borrow_mut();
            reader.attach(&self.gate);
            let Reader {
                probe,
                tables,
                scratch,
                val,
                slot,
                ..
            } = &mut **reader;
            let seen = tables.as_deref().map(KvManifest::seq);
            // Let in, the read knows what the memtable holds for its
            // key without asking: nothing. (The key fits a probe: its
            // length was checked above.)
            let unlocked = self
                .gate
                .enter(slot, seen, key)
                .filter(|_| probe.set_absent(key, seq));
            slot.count(unlocked.is_some());
            let pin = if unlocked.is_some() {
                unlocked
            } else {
                // A read the memtable settled reads no table and needs
                // no pin.
                self.begin(
                    slot,
                    |engine| {
                        engine.begin_read(key, seq, probe, tables)?;
                        Ok(probe.is_settled())
                    },
                    |settled| !settled,
                )?
                .1
            };
            #[cfg(test)]
            if pin.is_some() {
                self.hold_if_asked();
            }
            let Some(tables) = tables.as_deref() else {
                return Err(StoreError::Closed);
            };
            let view = KvTables::new(&self.read_device, tables, Some(&self.cache));
            let n = block_on(view.resolve(probe, scratch, &mut val[..], 0))?;
            drop(pin);
            Ok(n.map(|n| val[..n].to_vec()))
        })
    }

    /// Test hook: holds the calling reader here, in its table phase and
    /// pinned, if a test asked for the next reader to be held.
    #[cfg(test)]
    fn hold_if_asked(&self) {
        let hold = lock(&self.hold_next_reader).take();
        if let Some((entered, release)) = hold {
            let _ = entered.send(());
            let _ = release.recv();
        }
    }

    /// One page of a scan of `[start, end)`, ascending or descending, as
    /// of sequence `seq`: up to `limit` entries from `cursor` on (`None`:
    /// from the range's own bound), and where the next page goes on
    /// (`None`: the range is exhausted).
    ///
    /// The memtable's share is copied under the engine lock, which this
    /// thread takes itself, as a point read does; the merge with the
    /// tables runs here too, alongside every other reader's. A page can come back
    /// short of `limit` with more to follow: it stops at the horizon of
    /// its memtable copy.
    fn page(
        &self,
        (start, end): (&[u8], Option<&[u8]>),
        reverse: bool,
        cursor: Option<&Cursor>,
        limit: usize,
        seq: u64,
    ) -> Result<(Entries, Option<Cursor>), StoreError> {
        // horton reads an empty reverse `from` as "from the last key"; an
        // empty exclusive end is an empty range, refused in both
        // directions as the forward scan refuses it.
        if end == Some(&[]) {
            return Err(StoreError::Db(Error::EmptyKey));
        }
        // Where this page begins, and the key to pass over if it comes
        // first: the one the last page ended on, or a reverse range's
        // exclusive end.
        let (from, skip) = match (cursor, reverse) {
            (Some(c), _) => (c.key.as_slice(), c.yielded.then_some(c.key.as_slice())),
            (None, false) => (start, None),
            (None, true) => (end.unwrap_or(&[]), end),
        };
        // The buffer first: waiting for one must not hold the engine.
        let mut lease = PageLease::take(&self.pages);
        let Some(page) = lease.page.as_deref_mut() else {
            return Err(StoreError::Closed);
        };
        let slot = READER.with(|reader| Arc::clone(reader.borrow_mut().attach(&self.gate)));
        // One more key than the page yields: the key passed over.
        let (begun, pin) = self.begin(
            &slot,
            |engine| {
                engine.begin_page(
                    reverse,
                    from,
                    end.filter(|_| !reverse),
                    limit.saturating_add(1),
                    page,
                )
            },
            |_| true,
        )?;
        let PageStart {
            horizon,
            tables,
            node,
        } = begun;
        let memtable: &KvMemPage = page;
        #[cfg(test)]
        self.hold_if_asked();
        let horizon = horizon.as_deref();
        let parts = (memtable, &*tables, node);
        let mut out = Vec::new();
        let more = SCAN_POOL.with(|pool| {
            let mut pool = pool.borrow_mut();
            // Built one buffer at a time: 16 KiB each on the way through
            // the stack, not half a megabyte at once.
            while pool.len() < SCAN_BLOCKS {
                pool.push(ScanBlock::new());
            }
            if reverse {
                self.merge_rev(
                    parts,
                    &mut pool,
                    (from, horizon, start),
                    skip,
                    limit,
                    seq,
                    &mut out,
                )
            } else {
                self.merge_fwd(
                    parts,
                    &mut pool,
                    (from, horizon.or(end)),
                    skip,
                    limit,
                    seq,
                    &mut out,
                )
            }
        })?;
        let next = match (more, out.last(), horizon) {
            // Cut at `limit`: go on past the last key yielded.
            (true, Some((last, _)), _) => Some(Cursor {
                key: last.clone(),
                yielded: true,
            }),
            // Ran to the horizon: go on from it, unless a reverse scan
            // has already left its range.
            (false, _, Some(h)) if !reverse || h >= start => Some(Cursor {
                key: h.to_vec(),
                yielded: false,
            }),
            _ => None,
        };
        drop(pin);
        Ok((out, next))
        // The page buffer goes back to the pool here.
    }

    /// The table phase of a forward page: merges `memtable` with the
    /// tables over `[from, stop)`, passing over `skip` if it comes
    /// first. Returns true when it stopped at `limit` with the range not
    /// known to be exhausted.
    #[allow(clippy::too_many_arguments)] // a page's inputs; `page` is the only caller.
    fn merge_fwd(
        &self,
        (memtable, tables, node): (&KvMemPage, &KvManifest, u32),
        pool: &mut [ScanBlock<BLOCK>],
        (from, stop): (&[u8], Option<&[u8]>),
        skip: Option<&[u8]>,
        limit: usize,
        seq: u64,
        out: &mut Entries,
    ) -> Result<bool, StoreError> {
        let mut scan = Box::new(KvScan::detached(
            memtable,
            tables,
            &self.read_device,
            Some(&self.cache),
            node,
        ));
        scan.use_block_pool(pool);
        block_on(scan.seek(from, stop, seq))?;
        let mut key = [0u8; KEY_MAX];
        READER.with(|reader| {
            let val = &mut reader.borrow_mut().val;
            while out.len() < limit {
                match block_on(scan.next(&mut key, &mut val[..]))? {
                    Some((kl, _)) if out.is_empty() && Some(&key[..kl]) == skip => {}
                    Some((kl, vl)) => out.push((key[..kl].to_vec(), val[..vl].to_vec())),
                    None => return Ok(false),
                }
            }
            Ok(true)
        })
    }

    /// The table phase of a reverse page: descends from `from` to above
    /// `lower` (the memtable copy's horizon, exclusive), passing over
    /// `skip` if it comes first and ending the scan below `start`.
    /// horton's reverse scan starts at the greatest key `<= from` and
    /// stops above an exclusive lower bound, so the range's inclusive
    /// `start` is checked here.
    #[allow(clippy::too_many_arguments)] // a page's inputs; `page` is the only caller.
    fn merge_rev(
        &self,
        (memtable, tables, node): (&KvMemPage, &KvManifest, u32),
        pool: &mut [ScanBlock<BLOCK>],
        (from, lower, start): (&[u8], Option<&[u8]>, &[u8]),
        skip: Option<&[u8]>,
        limit: usize,
        seq: u64,
        out: &mut Entries,
    ) -> Result<bool, StoreError> {
        let mut scan = Box::new(KvRevScan::detached(
            memtable,
            tables,
            &self.read_device,
            Some(&self.cache),
            node,
        ));
        scan.use_block_pool(pool);
        block_on(scan.seek_prev(from, lower, seq))?;
        let mut key = [0u8; KEY_MAX];
        READER.with(|reader| {
            let val = &mut reader.borrow_mut().val;
            while out.len() < limit {
                match block_on(scan.prev(&mut key, &mut val[..]))? {
                    Some((kl, _)) if out.is_empty() && Some(&key[..kl]) == skip => {}
                    Some((kl, _)) if &key[..kl] < start => return Ok(false),
                    Some((kl, vl)) => out.push((key[..kl].to_vec(), val[..vl].to_vec())),
                    None => return Ok(false),
                }
            }
            Ok(true)
        })
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
        // Looked at before it is set: readers call this on every read
        // while a job is pending, and a flag that is already up should
        // not be written again.
        if !self.background_due.load(Ordering::Acquire)
            && !self.background_due.swap(true, Ordering::AcqRel)
        {
            let _g = lock(&self.wake.0);
            self.wake.1.notify_one();
        }
    }

    /// The compaction thread: sleeps until a job is pending, then runs
    /// bounded steps back to back, standing back whenever a client is
    /// waiting for the lock.
    fn background(&self) {
        loop {
            let mut asleep = lock(&self.wake.0);
            while !self.background_due.load(Ordering::Acquire) && !self.stop.load(Ordering::Acquire)
            {
                asleep = self
                    .wake
                    .1
                    .wait(asleep)
                    .unwrap_or_else(PoisonError::into_inner);
            }
            drop(asleep);
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
                self.engine_held.store(true, Ordering::Relaxed);
                let pending = engine.background_pending();
                if pending {
                    engine.background_step();
                } else {
                    // Cleared under the engine lock: a client that leaves
                    // a job pending after this sets it again.
                    self.background_due.store(false, Ordering::Release);
                }
                self.engine_held.store(false, Ordering::Relaxed);
                drop(guard);
                if !pending {
                    break;
                }
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
        // holds a handle. The gate is shut for good under the engine
        // lock, where no flush or compaction step can be about to open
        // it: from here no read reaches the tables without the lock,
        // and the lock has nothing behind it.
        let engine = {
            let mut guard = lock(&shared.engine);
            shared.gate.close();
            guard.take()
        };
        drop(engine);
        // Reads already in their table phase finish first: once this
        // returns the file may be opened, and its tables rewritten, by
        // another store. No new pin can appear.
        while shared.gate.pinned() != 0 {
            std::thread::yield_now();
        }
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
        let read_device = ReadDevice::open(path)
            .map_err(|e| StoreError::Open(format!("{}: {e}", path.display())))?;
        let (engine, cache, pages): (_, _, Vec<_>) = std::thread::Builder::new()
            .name("horton-open".into())
            .stack_size(OPEN_STACK)
            .spawn(move || {
                Engine::open(device, options.sync).map(|engine| {
                    // Megabyte-sized values, built on the stack before
                    // they are boxed: this thread has the room.
                    let pages = new_scan_pages();
                    (engine, SharedCache::new(), pages)
                })
            })
            .map_err(|e| StoreError::Open(e.to_string()))?
            .join()
            .map_err(|_| StoreError::Open("the opening thread panicked".into()))??;
        let report = engine.open;
        let gate = Arc::clone(&engine.gate);
        let shared = Arc::new(Shared {
            engine: Mutex::new(Some(engine)),
            queue: Mutex::new(VecDeque::new()),
            queued: AtomicUsize::new(0),
            engine_held: AtomicBool::new(false),
            waiting: AtomicUsize::new(0),
            // Look once: recovery may have left a job pending.
            background_due: AtomicBool::new(true),
            stop: AtomicBool::new(false),
            gate,
            read_device,
            cache,
            pages: Arc::new(Mutex::new(pages)),
            #[cfg(test)]
            hold_next_reader: Mutex::new(None),
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
        self.shared.submit(batch)?.reply()
    }

    /// Deletes every key in `[start, end)` with one range tombstone, like
    /// `RocksDB`'s `DeleteRange`.
    pub fn delete_range(&self, start: &[u8], end: &[u8]) -> Result<(), StoreError> {
        check_key(start)?;
        check_key(end)?;
        self.shared.with(|e| {
            let gate = &e.gate;
            e.room
                .with_room(&mut e.db, |db| {
                    // Before each try: making room flushes, and a flush
                    // says the memtable holds no range tombstone.
                    gate.note_unfiltered();
                    block_on(db.delete_range(start, end))
                })
                .map(drop)
        })
    }

    /// The value of `key`, or `None`.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        self.shared.read(key, u64::MAX)
    }

    /// The value of `key` as of `snapshot`.
    pub fn get_at(&self, key: &[u8], snapshot: &Snapshot) -> Result<Option<Vec<u8>>, StoreError> {
        self.shared.read(key, snapshot.seq)
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
        let mut out = Entries::new();
        let mut cursor = None;
        // A page can stop short of `limit` at its memtable copy's
        // horizon: go on until the limit or the end of the range.
        loop {
            let want = limit - out.len();
            let (mut entries, next) =
                self.shared
                    .page((start, end), reverse, cursor.as_ref(), want.min(PAGE), seq)?;
            out.append(&mut entries);
            if next.is_none() || out.len() >= limit {
                return Ok(out);
            }
            cursor = next;
        }
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
            cursor: None,
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
        let mut stats = self.shared.with(|e| Ok(e.stats()))?;
        (stats.reads_unlocked, stats.reads_locked) = self.shared.gate.reads();
        // Point reads' table phases go through the shared handle and
        // cache, not the engine's.
        let cache = self.shared.cache.stats();
        stats.io.reads += self.shared.read_device.reads();
        stats.cache_hits += cache.hits;
        stats.cache_misses += cache.misses;
        Ok(stats)
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
    /// Where the next page goes on from.
    cursor: Option<Cursor>,
    page: std::vec::IntoIter<(Vec<u8>, Vec<u8>)>,
    done: bool,
}

impl Iterator for Iter {
    type Item = Result<(Vec<u8>, Vec<u8>), StoreError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(kv) = self.page.next() {
                return Some(Ok(kv));
            }
            if self.done {
                return None;
            }
            let page = self.handle.shared.page(
                (&self.start, self.end.as_deref()),
                self.reverse,
                self.cursor.as_ref(),
                PAGE,
                self.snapshot.seq,
            );
            match page {
                Ok((entries, next)) => {
                    self.done = next.is_none();
                    self.cursor = next;
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
    /// The readers' gate (the room has it too, and [`Shared`]). The
    /// engine's part is to tell it what the memtable is about to hold.
    gate: Arc<Gate>,
    batch: Box<GroupBatch>,
    /// The writes of the group being committed (kept to reuse its
    /// allocation).
    group: Vec<Arc<Pending>>,
    /// The manifest as readers were last given it; replaced when the
    /// database's has moved on.
    tables: Arc<KvManifest>,
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
        let tables = Arc::new(*db.manifest());
        // Records replayed from the WAL are in the memtable, and no
        // filter bit stands for them: reads ask the memtable until the
        // first flush.
        let gate = Arc::new(Gate::new(tables.seq(), open.recovered_records > 0));
        Ok(Self {
            db,
            room: Room {
                scratch: Box::new(KvCompaction::new()),
                gate: Arc::clone(&gate),
                flushes: 0,
                jobs: 0,
                steps: 0,
            },
            gate,
            batch: Box::new(GroupBatch::new()),
            group: Vec::new(),
            tables,
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

    /// The engine's manifest for readers, brought up to date. Every
    /// manifest commit moves `seq`, so an equal `seq` is the same set of
    /// tables.
    fn tables(&mut self) -> &Arc<KvManifest> {
        if self.tables.seq() != self.db.manifest().seq() {
            self.tables = Arc::new(*self.db.manifest());
            // A flush or a compaction step has published this already.
            // A commit that left the tables alone has not, and readers
            // would compare against the old number until the next one.
            self.gate.publish(self.tables.seq());
        }
        &self.tables
    }

    /// The part of a point read that needs the database: the memtable
    /// probe, written into the caller's `probe`, and the manifest of
    /// this moment in `tables`. The caller's `tables` is replaced only
    /// when it is not the engine's current one, so a reader whose
    /// manifest is still current touches no reference count.
    fn begin_read(
        &mut self,
        key: &[u8],
        seq: u64,
        probe: &mut KvProbe,
        tables: &mut Option<Arc<KvManifest>>,
    ) -> Result<(), DbError> {
        self.db.probe_memtable_into(key, seq, probe)?;
        let current = self.tables();
        if !tables.as_ref().is_some_and(|t| Arc::ptr_eq(t, current)) {
            *tables = Some(Arc::clone(current));
        }
        Ok(())
    }

    /// The part of a scan page that needs the database: copies into
    /// `page` the at most `max_keys` keys of the memtable the page
    /// reaches first, and returns the copy's horizon (the nearest key it
    /// does not hold, where the page must stop; `None` when it holds the
    /// rest of the range), the manifest of this moment, and the node id.
    fn begin_page(
        &mut self,
        reverse: bool,
        from: &[u8],
        end: Option<&[u8]>,
        max_keys: usize,
        page: &mut KvMemPage,
    ) -> Result<PageStart, DbError> {
        let horizon = if reverse {
            self.db
                .copy_memtable_range_rev(from, None, max_keys, page)?
        } else {
            self.db.copy_memtable_range(from, end, max_keys, page)?
        };
        Ok(PageStart {
            horizon: horizon.map(|h| h.as_slice().to_vec()),
            tables: Arc::clone(self.tables()),
            node: self.db.node_id(),
        })
    }

    /// Commits the queue from its head, in order, a group at a time.
    /// Stops when the queue is empty or after [`SERVE_MAX`] groups.
    ///
    /// `me` is the caller's own write. The queue is first in, first out,
    /// so by the time `me` is committed so is everything that was
    /// waiting when it queued. The caller goes on committing others
    /// while that is cheap, which keeps the database in one CPU's cache.
    /// It stops before a group that will be `fdatasync`ed, so that its
    /// client's next write can join that group instead of waiting a
    /// flush behind it, which is what makes synced groups large.
    fn serve(&mut self, shared: &Shared, me: &Pending) {
        for _ in 0..SERVE_MAX {
            let committed = me.done.get().is_some();
            if !self.take_group(shared, self.sync && committed) {
                return;
            }
            self.commit_group();
        }
    }

    /// Moves the write at the head of the queue, and every write queued
    /// directly behind it that fits the same WAL block, into
    /// `self.group`. False when there is nothing to take, or when
    /// `leave` asks for the queue to be left as it is.
    fn take_group(&mut self, shared: &Shared, leave: bool) -> bool {
        if leave {
            return false;
        }
        let mut queue = lock(&shared.queue);
        let Some(head) = queue.pop_front() else {
            return false;
        };
        self.group.clear();
        // The head always goes: `Handle::write` checked that it fits a
        // block by itself.
        let (mut bytes, mut ops) = (head.batch.wal_bytes(), head.batch.ops.len());
        self.group.push(head);
        while let Some(next) = queue.front() {
            bytes += next.batch.wal_bytes();
            ops += next.batch.ops.len();
            if bytes > BLOCK || ops > GROUP_OPS {
                break;
            }
            self.group.extend(queue.pop_front());
        }
        shared.queued.store(queue.len(), Ordering::Relaxed);
        drop(queue);
        true
    }

    /// Commits the writes in `self.group` as one batch and tells each of
    /// them the result.
    fn commit_group(&mut self) {
        self.batch.clear();
        let mut result = Ok(());
        for op in self.group.iter().flat_map(|p| &p.batch.ops) {
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
        let (batch, group, gate) = (&*self.batch, &self.group, &self.gate);
        let result = result
            .and_then(|()| {
                self.room.with_room(&mut self.db, |db| {
                    // The filter first, then the memtable: a read that
                    // starts after this write returns must find the
                    // bits. And before each try: making room flushes,
                    // and a flush empties the filter.
                    for op in group.iter().flat_map(|p| &p.batch.ops) {
                        let (BatchOp::Put(key, _) | BatchOp::Delete(key)) = op;
                        gate.note(key);
                    }
                    block_on(db.write(batch))
                })
            })
            .map(drop)
            .map_err(StoreError::Db);
        self.groups += 1;
        self.grouped_writes += self.group.len() as u64;
        for p in self.group.drain(..) {
            p.finish(result.clone());
        }
    }

    fn stats(&self) -> Stats {
        let mut level_tables = [0; LEVELS];
        let (mut rdel_blocks, mut bloom_blocks) = (0, 0);
        for (l, n) in level_tables.iter_mut().enumerate() {
            let tables = self.db.level_tables(l).unwrap_or(&[]);
            *n = tables.len();
            rdel_blocks += tables.iter().map(|t| u64::from(t.rdel_blocks)).sum::<u64>();
            bloom_blocks += tables
                .iter()
                .map(|t| u64::from(t.bloom_blocks))
                .sum::<u64>();
        }
        let cache = self.db.cache_stats();
        Stats {
            open: self.open,
            device_blocks: self.db.device().blocks(),
            slots: self.db.slot_stats(),
            level_tables,
            rdel_blocks,
            bloom_blocks,
            io: self.db.device().stats(),
            cache_hits: cache.hits,
            cache_misses: cache.misses,
            groups: self.groups,
            grouped_writes: self.grouped_writes,
            flushes: self.room.flushes,
            compaction_jobs: self.room.jobs,
            compaction_steps: self.room.steps,
            // The gate's; `Handle::stats` fills them in.
            reads_unlocked: 0,
            reads_locked: 0,
        }
    }
}

/// Makes room when horton asks for it: the compaction scratch, and counts
/// of what it did.
struct Room {
    scratch: Box<KvCompaction>,
    /// Shut around every call that changes the tables; see [`Gate`].
    gate: Arc<Gate>,
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
    ///
    /// A flush holds a table writer, and a writer holds the largest
    /// bloom filter a table can have (`BLOOM_BYTES`, 255 KiB), so the
    /// flush future goes on the heap: this runs on whichever client
    /// thread holds the engine, with whatever stack that thread has.
    ///
    /// Each try is made with the gate shut, and the gate is opened
    /// again before anything else: the compaction between two tries
    /// shuts it for itself, a step at a time.
    fn flush(&mut self, db: &mut KvDb<FileDevice>) -> Result<(), DbError> {
        for attempt in 0..=ROOM_TRIES {
            self.gate.shut();
            let flushed = block_on(Box::pin(db.flush()));
            // A flush that returned `Ok` has emptied the memtable.
            self.gate.open(db.manifest().seq(), flushed.is_ok());
            match flushed {
                Err(Error::NeedsCompaction) if attempt < ROOM_TRIES => self.job(db)?,
                r => {
                    self.flushes += u64::from(r.is_ok());
                    return r;
                }
            }
        }
        Err(Error::NeedsCompaction)
    }

    /// Runs one compaction job (or finishes the one in flight).
    fn job(&mut self, db: &mut KvDb<FileDevice>) -> Result<(), DbError> {
        while self.step(db)? == Progress::More {}
        Ok(())
    }

    /// One bounded compaction step, with the gate shut around it.
    fn step(&mut self, db: &mut KvDb<FileDevice>) -> Result<Progress, DbError> {
        self.gate.shut();
        let stepped = block_on(db.compact_step(&mut self.scratch));
        self.gate.open(db.manifest().seq(), false);
        let p = stepped?;
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

    /// Runs `work` on its own thread and fails, instead of hanging, if
    /// it has not finished in a minute: a deadlock in the store shows up
    /// as a failed test.
    fn within_a_minute(work: impl FnOnce() + Send + 'static) {
        let (done, finished) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            work();
            let _ = done.send(());
        });
        match finished.recv_timeout(Duration::from_secs(60)) {
            Ok(()) => worker.join().expect("worker"),
            // The worker panicked: join to show its message.
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                worker.join().expect("worker");
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                panic!("the store did not finish in a minute: a deadlock")
            }
        }
    }

    /// A value that says which key it belongs to and which write it was,
    /// padded so that a few hundred of them fill the memtable.
    fn stamped(k: usize, version: u64) -> Vec<u8> {
        let mut v = format!("{k}:{version}:").into_bytes();
        v.resize(700, b'.');
        v
    }

    fn unstamp(v: &[u8]) -> (usize, u64) {
        let text = core::str::from_utf8(v).expect("a value is text");
        let mut parts = text.split(':');
        let k = parts.next().expect("key").parse().expect("key");
        let version = parts.next().expect("version").parse().expect("version");
        assert_eq!(v.len(), 700, "a value was cut short");
        (k, version)
    }

    /// The gate itself, without luck: a read is held in its table phase,
    /// pinned, and a flush and full compaction are started. They must
    /// not finish while the reader is in there, because they retire the
    /// table it is reading and write new tables into the freed slots.
    /// Once the reader leaves, they finish, and the reader's answer is
    /// the value as of its probe. Without [`Gate::shut`] the
    /// compaction completes under the reader and this test fails.
    #[test]
    fn flush_and_compaction_wait_for_a_reader_in_its_table_phase() {
        within_a_minute(|| {
            let store = TempStore::new("gate");
            for k in 0..60 {
                store.put(&key(k), &stamped(k, 1)).expect("put");
            }
            store.flush().expect("flush");

            let (entered_tx, entered) = std::sync::mpsc::channel();
            let (release, release_rx) = std::sync::mpsc::channel();
            *lock(&store.handle.shared.hold_next_reader) = Some((entered_tx, release_rx));
            let compacted = Arc::new(AtomicBool::new(false));
            std::thread::scope(|s| {
                let h = store.handle();
                let reader = s.spawn(move || h.get(&key(7)).expect("get"));
                entered.recv().expect("the reader reached its table phase");
                assert_eq!(store.handle.shared.gate.pinned(), 1);

                // Writes do not wait for the reader: they reach only the
                // WAL and the memtable.
                for k in 0..60 {
                    store.put(&key(k), &stamped(k, 2)).expect("put");
                }
                // A flush and a compaction do.
                let (h, done) = (store.handle(), Arc::clone(&compacted));
                let mutator = s.spawn(move || {
                    h.compact().expect("compact");
                    done.store(true, Ordering::SeqCst);
                });
                std::thread::sleep(Duration::from_millis(300));
                assert!(
                    !compacted.load(Ordering::SeqCst),
                    "tables were rewritten while a reader was inside them"
                );

                release.send(()).expect("release");
                let v = reader.join().expect("reader").expect("present");
                assert_eq!(unstamp(&v), (7, 1), "the read is as of its probe");
                mutator.join().expect("mutator");
            });
            assert!(compacted.load(Ordering::SeqCst));
            assert_eq!(store.handle.shared.gate.pinned(), 0);
            let v = store.get(&key(7)).expect("get").expect("present");
            assert_eq!(unstamp(&v), (7, 2));
        });
    }

    /// Readers on four threads against a store that two writers keep
    /// flushing and compacting. Every read must return the right key's
    /// value, whole, and never an older one than that reader has already
    /// seen; nothing may deadlock; and every pin must come back. (The
    /// gate is tested deterministically above: the window in which a
    /// missing gate would show here is microseconds wide.)
    #[test]
    fn readers_and_writers_run_together_under_compaction() {
        within_a_minute(|| {
            const KEYS: usize = 96;
            let store = TempStore::new("parallel");
            for k in 0..KEYS {
                store.put(&key(k), &stamped(k, 0)).expect("put");
            }
            let stop = Arc::new(AtomicBool::new(false));
            let reads = Arc::new(AtomicUsize::new(0));
            std::thread::scope(|s| {
                let readers: Vec<_> = (0..4usize)
                    .map(|t| {
                        let (h, stop, reads) =
                            (store.handle(), Arc::clone(&stop), Arc::clone(&reads));
                        s.spawn(move || {
                            let mut seen = [0u64; KEYS];
                            let mut i = t;
                            // Until the writers are done, and at least
                            // 300 reads however fast this machine is.
                            let mut mine = 0usize;
                            while mine < 300 || !stop.load(Ordering::Relaxed) {
                                mine += 1;
                                i = (i * 31 + 7) % KEYS;
                                let v = h.get(&key(i)).expect("get").expect("every key exists");
                                let (k, version) = unstamp(&v);
                                assert_eq!(k, i, "another key's value");
                                assert!(version >= seen[i], "key {i} went back in time");
                                seen[i] = version;
                                reads.fetch_add(1, Ordering::Relaxed);
                            }
                        })
                    })
                    .collect();
                let writers: Vec<_> = (0..2usize)
                    .map(|w| {
                        let h = store.handle();
                        s.spawn(move || {
                            // Each writer owns half the keys, so a key's
                            // versions only ever rise.
                            for version in 1..=40u64 {
                                for k in (w..KEYS).step_by(2) {
                                    h.put(&key(k), &stamped(k, version)).expect("put");
                                }
                            }
                        })
                    })
                    .collect();
                for w in writers {
                    w.join().expect("writer");
                }
                // Let the readers run on against the compactions that
                // are still pending, then stop them.
                store.compact().expect("compact");
                stop.store(true, Ordering::Relaxed);
                for r in readers {
                    r.join().expect("reader");
                }
            });
            let stats = store.stats().expect("stats");
            assert!(stats.flushes >= 4, "only {} flushes", stats.flushes);
            assert!(
                stats.compaction_jobs >= 2,
                "only {} jobs",
                stats.compaction_jobs
            );
            assert!(reads.load(Ordering::Relaxed) >= 4 * 300);
            // Every read gave its pin back.
            assert_eq!(store.handle.shared.gate.pinned(), 0);
            for k in 0..KEYS {
                let v = store.get(&key(k)).expect("get").expect("present");
                assert_eq!(unstamp(&v), (k, 40));
            }
        });
    }

    /// A reader holds its pin until it has run the table phase. It must
    /// not, meanwhile, wait for readers to drain, in a compaction step
    /// or a flush, or it waits for itself: a read that takes the engine
    /// lock gives it up without a step ([`Locked::release`]), and a
    /// read that takes none never had it. One thread alone, reading
    /// while a compaction job is pending, is the case. The compaction
    /// thread is stopped first, so the job stays pending and only this
    /// thread can move it.
    #[test]
    fn a_reader_serving_its_own_request_does_not_wait_for_itself() {
        within_a_minute(|| {
            let store = TempStore::new("self-pin");
            let shared = &store.handle.shared;
            shared.stop.store(true, Ordering::Release);
            shared.wake_background();
            let pending = || {
                lock(&shared.engine)
                    .as_ref()
                    .is_some_and(Engine::background_pending)
            };
            // Flush tables until level 0 wants compacting.
            let mut round = 0u64;
            while !pending() {
                for k in 0..40 {
                    store.put(&key(k), &stamped(k, round)).expect("put");
                }
                store.flush().expect("flush");
                round += 1;
                assert!(round < 20, "no compaction ever became pending");
            }
            // Each read pins the tables (the memtable is empty) with a
            // job pending, and leaves the job where it was.
            let mut reads_with_a_job_pending = 0;
            for k in 0..40 {
                reads_with_a_job_pending += u32::from(pending());
                let v = store.get(&key(k)).expect("get").expect("present");
                assert_eq!(unstamp(&v), (k, round - 1));
            }
            assert!(reads_with_a_job_pending > 0);
            // And a write that needs room still gets it.
            for k in 0..600 {
                store.put(&key(k + 1000), &stamped(k, 0)).expect("put");
            }
            assert_eq!(shared.gate.pinned(), 0);
        });
    }

    /// The point of the gate: a read of a key the memtable cannot hold
    /// waits for no lock of the store's. The test holds the engine
    /// lock, the write queue and the scan pages, all at once, and a
    /// thread reads flushed keys and absent keys meanwhile. A key that
    /// is in the memtable still asks the memtable, under the engine
    /// lock, and so waits for the test to let go.
    #[test]
    fn a_read_the_memtable_cannot_settle_waits_for_no_lock() {
        within_a_minute(|| {
            let store = TempStore::new("no-lock");
            for k in 0..60 {
                store.put(&key(k), &stamped(k, 1)).expect("put");
            }
            store.flush().expect("flush");
            store.put(&key(100), &stamped(100, 1)).expect("put");
            let shared = &store.handle.shared;
            std::thread::scope(|s| {
                let (warm_tx, warm) = std::sync::mpsc::channel();
                let (go, go_rx) = std::sync::mpsc::channel();
                let (out_tx, out) = std::sync::mpsc::channel();
                let h = store.handle();
                s.spawn(move || {
                    // A thread's first read takes the lock: it has no
                    // copy of the manifest yet.
                    assert!(h.get(&key(0)).expect("get").is_some());
                    warm_tx.send(()).expect("warm");
                    go_rx.recv().expect("go");
                    for k in 0..60 {
                        let v = h.get(&key(k)).expect("get").expect("present");
                        assert_eq!(unstamp(&v), (k, 1));
                    }
                    for k in 60..90 {
                        assert_eq!(h.get(&key(k)).expect("get"), None);
                    }
                    out_tx.send(()).expect("out");
                    go_rx.recv().expect("go");
                    let v = h.get(&key(100)).expect("get").expect("present");
                    assert_eq!(unstamp(&v), (100, 1));
                    out_tx.send(()).expect("out");
                });
                warm.recv().expect("warm");
                let before = shared.gate.reads();
                assert_eq!(before, (0, 1));
                let engine = lock(&shared.engine);
                let queue = lock(&shared.queue);
                let pages = lock(&shared.pages);
                go.send(()).expect("go");
                out.recv_timeout(Duration::from_secs(20))
                    .expect("reads the memtable could not settle waited for a lock");
                assert_eq!(shared.gate.reads(), (90, 1));
                go.send(()).expect("go");
                assert!(
                    out.recv_timeout(Duration::from_millis(200)).is_err(),
                    "a read of a key in the memtable did not ask the memtable"
                );
                drop((pages, queue, engine));
                out.recv_timeout(Duration::from_secs(20))
                    .expect("the read of a memtable key never finished");
                assert_eq!(shared.gate.reads(), (90, 2));
            });
        });
    }

    /// A read that reached the tables without the engine lock is in
    /// them like any other: flush and compaction wait for it. The same
    /// test as the one above for a read that took the lock, with the
    /// reader's manifest already current, so that it takes none.
    #[test]
    fn flush_and_compaction_wait_for_a_reader_that_took_no_lock() {
        within_a_minute(|| {
            let store = TempStore::new("gate-unlocked");
            for k in 0..60 {
                store.put(&key(k), &stamped(k, 1)).expect("put");
            }
            store.flush().expect("flush");
            let shared = &store.handle.shared;
            let compacted = Arc::new(AtomicBool::new(false));
            std::thread::scope(|s| {
                let (warm_tx, warm) = std::sync::mpsc::channel();
                let (go, go_rx) = std::sync::mpsc::channel();
                let h = store.handle();
                let reader = s.spawn(move || {
                    assert!(h.get(&key(0)).expect("get").is_some());
                    warm_tx.send(()).expect("warm");
                    go_rx.recv().expect("go");
                    h.get(&key(7)).expect("get")
                });
                warm.recv().expect("warm");
                let (entered_tx, entered) = std::sync::mpsc::channel();
                let (release, release_rx) = std::sync::mpsc::channel();
                *lock(&shared.hold_next_reader) = Some((entered_tx, release_rx));
                go.send(()).expect("go");
                entered.recv().expect("the reader reached its table phase");
                assert_eq!(shared.gate.pinned(), 1);
                assert_eq!(shared.gate.reads(), (1, 1), "the held read took no lock");

                for k in 0..60 {
                    store.put(&key(k), &stamped(k, 2)).expect("put");
                }
                let (h, done) = (store.handle(), Arc::clone(&compacted));
                let mutator = s.spawn(move || {
                    h.compact().expect("compact");
                    done.store(true, Ordering::SeqCst);
                });
                std::thread::sleep(Duration::from_millis(300));
                assert!(
                    !compacted.load(Ordering::SeqCst),
                    "tables were rewritten while a reader was inside them"
                );
                // While the gate is shut and waiting, another read does
                // not slip into the tables; it waits for the engine.
                release.send(()).expect("release");
                let v = reader.join().expect("reader").expect("present");
                assert_eq!(unstamp(&v), (7, 1), "the read is as of when it entered");
                mutator.join().expect("mutator");
            });
            assert!(compacted.load(Ordering::SeqCst));
            assert_eq!(shared.gate.pinned(), 0);
            let v = store.get(&key(7)).expect("get").expect("present");
            assert_eq!(unstamp(&v), (7, 2));
        });
    }

    /// The two tests above run a flush and a compaction together, and
    /// either one waiting would pass them. Here each is alone: a
    /// compaction step with nothing to flush, then a flush with nothing
    /// to compact. Neither may finish while a reader is in the tables.
    #[test]
    fn a_compaction_step_and_a_flush_each_wait_for_a_reader() {
        within_a_minute(|| {
            let store = TempStore::new("each-waits");
            let shared = &store.handle.shared;
            // No compaction thread: the job stays pending until this
            // test steps it.
            shared.stop.store(true, Ordering::Release);
            shared.wake_background();
            let pending = || {
                lock(&shared.engine)
                    .as_ref()
                    .is_some_and(Engine::background_pending)
            };
            let mut round = 0u64;
            while !pending() {
                for k in 0..40 {
                    store.put(&key(k), &stamped(k, round)).expect("put");
                }
                store.flush().expect("flush");
                round += 1;
                assert!(round < 20, "no compaction ever became pending");
            }
            // Set when the step, and when the flush, has run.
            let (step_ran, flush_ran) = (AtomicBool::new(false), AtomicBool::new(false));
            std::thread::scope(|s| {
                let (go, go_rx) = std::sync::mpsc::channel();
                let (out_tx, out) = std::sync::mpsc::channel();
                let h = store.handle();
                s.spawn(move || {
                    while go_rx.recv().is_ok() {
                        let v = h.get(&key(7)).expect("get").expect("present");
                        out_tx.send(unstamp(&v)).expect("out");
                    }
                });
                // Holds the reader's next read in its table phase.
                let hold = || {
                    let (entered_tx, entered) = std::sync::mpsc::channel();
                    let (release, release_rx) = std::sync::mpsc::channel();
                    *lock(&shared.hold_next_reader) = Some((entered_tx, release_rx));
                    go.send(()).expect("go");
                    entered.recv().expect("the reader reached its table phase");
                    assert_eq!(shared.gate.pinned(), 1);
                    release
                };
                go.send(()).expect("go");
                assert_eq!(out.recv().expect("out"), (7, round - 1));

                // A compaction step, alone.
                let release = hold();
                assert!(pending());
                let stepper = s.spawn(|| {
                    lock(&shared.engine)
                        .as_mut()
                        .expect("open")
                        .background_step();
                    step_ran.store(true, Ordering::SeqCst);
                });
                std::thread::sleep(Duration::from_millis(300));
                assert!(
                    !step_ran.load(Ordering::SeqCst),
                    "a compaction step ran while a reader was inside the tables"
                );
                release.send(()).expect("release");
                assert_eq!(out.recv().expect("out"), (7, round - 1));
                stepper.join().expect("stepper");
                assert!(step_ran.load(Ordering::SeqCst));

                // A flush, alone: the reader holds its pin, so the put
                // and the flush end their turns without a compaction
                // step.
                let release = hold();
                store.put(&key(1000), &stamped(1000, 0)).expect("put");
                let flusher = s.spawn(|| {
                    store.flush().expect("flush");
                    flush_ran.store(true, Ordering::SeqCst);
                });
                std::thread::sleep(Duration::from_millis(300));
                assert!(
                    !flush_ran.load(Ordering::SeqCst),
                    "a flush ran while a reader was inside the tables"
                );
                release.send(()).expect("release");
                assert_eq!(out.recv().expect("out"), (7, round - 1));
                flusher.join().expect("flusher");
                assert!(flush_ran.load(Ordering::SeqCst));
                drop(go);
            });
            assert_eq!(shared.gate.pinned(), 0);
        });
    }

    /// Whichever way a read goes, it sees every write that returned
    /// before it began. Reads of what only the tables hold take no
    /// lock; a key written, deleted or created since the last flush
    /// asks the memtable; so does every key once a range was deleted;
    /// and a flush makes all of them table reads again.
    #[test]
    fn a_read_sees_every_write_before_it_whichever_way_it_goes() {
        let store = TempStore::new("see-writes");
        let reads = || store.handle.shared.gate.reads();
        let get = |k: usize| store.get(&key(k)).expect("get").map(|v| unstamp(&v));
        for k in 0..200 {
            store.put(&key(k), &stamped(k, 1)).expect("put");
        }
        store.flush().expect("flush");
        // This thread's first look at the manifest: under the lock.
        assert_eq!(get(0), Some((0, 1)));
        assert_eq!(reads(), (0, 1));
        for k in 0..260 {
            assert_eq!(get(k), (k < 200).then_some((k, 1)));
        }
        assert_eq!(reads(), (260, 1));
        // An overwrite, a delete and a new key: each seen by the very
        // next read, which asks the memtable.
        store.put(&key(3), &stamped(3, 2)).expect("put");
        assert_eq!(get(3), Some((3, 2)));
        store.delete(&key(4)).expect("delete");
        assert_eq!(get(4), None);
        store.put(&key(230), &stamped(230, 1)).expect("put");
        assert_eq!(get(230), Some((230, 1)));
        assert_eq!(reads(), (260, 4));
        // The keys around them go on taking no lock.
        assert_eq!(get(5), Some((5, 1)));
        assert_eq!(get(231), None);
        assert_eq!(reads(), (262, 4));
        // A range tombstone is in the memtable under no key a read
        // could look up: every read asks the memtable.
        store
            .delete_range(&key(10), &key(20))
            .expect("delete_range");
        for k in 5..25 {
            assert_eq!(get(k), (!(10..20).contains(&k)).then_some((k, 1)));
        }
        assert_eq!(reads(), (262, 24));
        // A flush empties the memtable. The first read after it finds
        // its manifest replaced and asks; then none do.
        store.flush().expect("flush");
        assert_eq!(get(3), Some((3, 2)));
        assert_eq!(reads(), (262, 25));
        assert_eq!(get(3), Some((3, 2)));
        assert_eq!(get(4), None);
        assert_eq!(get(15), None);
        assert_eq!(get(230), Some((230, 1)));
        assert_eq!(get(5), Some((5, 1)));
        assert_eq!(reads(), (267, 25));
        let stats = store.stats().expect("stats");
        assert_eq!((stats.reads_unlocked, stats.reads_locked), (267, 25));
    }

    /// Records replayed from the WAL at open are in the memtable, and
    /// no filter bit was ever set for them. Until a flush, every read
    /// asks the memtable.
    #[test]
    fn a_memtable_recovered_from_the_wal_is_asked_until_it_is_flushed() {
        let mut store = TempStore::new("recovered");
        for k in 0..50 {
            store.put(&key(k), &stamped(k, 1)).expect("put");
        }
        store.flush().expect("flush");
        for k in 0..10 {
            store.put(&key(k), &stamped(k, 2)).expect("put");
        }
        store.delete(&key(20)).expect("delete");
        store.reopen();
        assert_eq!(store.stats().expect("stats").open.recovered_records, 11);
        let check = |store: &TempStore| {
            for _ in 0..2 {
                for k in 0..60 {
                    let want = match k {
                        0..10 => Some((k, 2)),
                        20 | 50.. => None,
                        _ => Some((k, 1)),
                    };
                    let got = store.get(&key(k)).expect("get").map(|v| unstamp(&v));
                    assert_eq!(got, want, "key {k}");
                }
            }
        };
        check(&store);
        assert_eq!(store.handle.shared.gate.reads(), (0, 120));
        store.flush().expect("flush");
        check(&store);
        // All but the first, which found a new manifest.
        assert_eq!(store.handle.shared.gate.reads(), (119, 121));
    }

    /// A thread keeps one manifest copy for its reads. Two stores'
    /// manifests can carry the same sequence number, which is all a
    /// read without the lock compares: a thread that goes from one
    /// store to another must not read the second by the first's tables.
    #[test]
    fn a_thread_reading_two_stores_keeps_their_tables_apart() {
        let a = TempStore::new("apart-a");
        let b = TempStore::new("apart-b");
        // One flush each, so one sequence number; different tables.
        for k in 0..40 {
            a.put(&key(k), &stamped(k, 1)).expect("put");
        }
        a.flush().expect("flush");
        for k in 1000..1400 {
            b.put(&key(k), &stamped(k, 1)).expect("put");
        }
        b.flush().expect("flush");
        let (ga, gb) = (&a.handle.shared.gate, &b.handle.shared.gate);
        assert_eq!(ga.tables_seq(), gb.tables_seq());
        let get = |store: &TempStore, k: usize| store.get(&key(k)).expect("get");
        for round in 0..3 {
            for k in 0..40 {
                let far = 1000 + k * 10 + round;
                assert_eq!(get(&a, k).map(|v| unstamp(&v)), Some((k, 1)));
                assert_eq!(get(&b, far).map(|v| unstamp(&v)), Some((far, 1)));
                assert_eq!(get(&a, far), None);
                assert_eq!(get(&b, k), None);
            }
        }
        // Every read above followed one of the other store: its thread
        // had that store's manifest no longer, and asked for it.
        assert_eq!((ga.reads(), gb.reads()), ((0, 240), (0, 240)));
        // Reads of one store in a row take no lock, as ever.
        assert_eq!(get(&a, 1).map(|v| unstamp(&v)), Some((1, 1)));
        assert_eq!(get(&a, 2).map(|v| unstamp(&v)), Some((2, 1)));
        assert_eq!(ga.reads(), (1, 241));
        assert_eq!((ga.pinned(), gb.pinned()), (0, 0));
    }

    /// A write that finds no room (a full memtable, a full WAL) flushes
    /// and tries again. The flush empties the filter, so the key must
    /// be noted for the second try as it was for the first: otherwise
    /// the key is in the memtable, the filter says it is not, and reads
    /// answer from the tables. The same for a range delete and its
    /// flag.
    ///
    /// Every key is in a table with an old value first. Then each is
    /// overwritten or range-deleted in turn, and the test looks at the
    /// ones whose write flushed on the way.
    #[test]
    fn a_write_that_had_to_make_room_is_still_seen() {
        within_a_minute(|| {
            const KEYS: usize = 3 * crate::MEMTABLE_ENTRIES;
            let store = TempStore::new("made-room");
            let flushes = || store.stats().expect("stats").flushes;
            for k in 0..KEYS {
                store.put(&key(k), b"old").expect("put");
            }
            store.flush().expect("flush");
            let (mut puts_made_room, mut deletes_made_room) = (0, 0);
            for k in 0..KEYS {
                let before = flushes();
                let want = if k % 2 == 0 {
                    store.put(&key(k), b"new").expect("put");
                    Some(b"new".to_vec())
                } else {
                    store
                        .delete_range(&key(k), &key(k + 1))
                        .expect("delete_range");
                    None
                };
                let made_room = flushes() > before;
                puts_made_room += usize::from(made_room && k % 2 == 0);
                deletes_made_room += usize::from(made_room && k % 2 == 1);
                // After a flush the first read finds a new manifest and
                // asks the memtable; the next ones would take no lock,
                // if the filter let them.
                for _ in 0..if made_room { 6 } else { 1 } {
                    assert_eq!(store.get(&key(k)).expect("get"), want, "key {k}");
                }
            }
            assert!(
                puts_made_room >= 2 && deletes_made_room >= 2,
                "{puts_made_room} puts and {deletes_made_room} range deletes made room"
            );
        });
    }

    /// Closing shuts the gate for good: a thread whose manifest was
    /// current, and whose reads were taking no lock, is refused like
    /// any other caller. (It would otherwise go on reading the file by
    /// a manifest the next store to open it will replace.)
    #[test]
    fn a_closed_store_answers_no_read_from_its_tables() {
        let mut store = TempStore::new("closed-reads");
        store.put(b"k", b"v").expect("put");
        store.flush().expect("flush");
        let stale = store.handle();
        assert_eq!(stale.get(b"k").expect("get"), Some(b"v".to_vec()));
        assert_eq!(stale.get(b"k").expect("get"), Some(b"v".to_vec()));
        assert_eq!(stale.get(b"absent").expect("get"), None);
        assert_eq!(store.handle.shared.gate.reads(), (2, 1));
        store.reopen();
        assert_eq!(stale.get(b"k"), Err(StoreError::Closed));
        assert_eq!(stale.get(b"absent"), Err(StoreError::Closed));
        assert_eq!(store.get(b"k").expect("get"), Some(b"v".to_vec()));
    }

    /// Reads that end in the memtable, in tables, or nowhere all agree
    /// with what was written, across flushes, deletes and a reopen; and
    /// the table phases went through the shared cache and read handle.
    #[test]
    fn reads_agree_wherever_the_key_lives() {
        let mut store = TempStore::new("where");
        for k in 0..200 {
            store.put(&key(k), &stamped(k, 1)).expect("put");
        }
        store.flush().expect("flush");
        for k in (0..200).step_by(3) {
            store.delete(&key(k)).expect("delete");
        }
        for k in (1..200).step_by(3) {
            store.put(&key(k), &stamped(k, 2)).expect("put");
        }
        let check = |store: &Store| {
            for k in 0..200 {
                let got = store.get(&key(k)).expect("get");
                match k % 3 {
                    0 => assert_eq!(got, None, "key {k} was deleted"),
                    1 => assert_eq!(unstamp(&got.expect("present")), (k, 2)),
                    _ => assert_eq!(unstamp(&got.expect("present")), (k, 1)),
                }
            }
            assert_eq!(store.get(&key(5000)).expect("get"), None);
            // Longer than any key the store accepts: absent, not an error.
            assert_eq!(store.get(&[b'k'; KEY_MAX + 1]).expect("get"), None);
        };
        check(&store);
        store.flush().expect("flush");
        check(&store);
        store.reopen();
        check(&store);
        // A snapshot read goes the same way.
        let snapshot = store.snapshot().expect("snapshot");
        store.put(&key(2), b"later").expect("put");
        store.flush().expect("flush");
        let old = store.get_at(&key(2), &snapshot).expect("get_at");
        assert_eq!(unstamp(&old.expect("present")), (2, 1));
        assert_eq!(store.get(&key(2)).expect("get"), Some(b"later".to_vec()));
        let cache = store.handle.shared.cache.stats();
        assert!(cache.hits > 0 && cache.len > 0, "{cache:?}");
        assert!(store.handle.shared.read_device.reads() > 0);
    }

    /// The gate holds for scans as for point reads: a scan page held in
    /// its table phase keeps flush and compaction from finishing, writes
    /// go on meanwhile, and the page yields the store as of its start.
    #[test]
    fn flush_and_compaction_wait_for_a_scan_in_its_table_phase() {
        within_a_minute(|| {
            let store = TempStore::new("scan-gate");
            for k in 0..60 {
                store.put(&key(k), &stamped(k, 1)).expect("put");
            }
            store.flush().expect("flush");

            let (entered_tx, entered) = std::sync::mpsc::channel();
            let (release, release_rx) = std::sync::mpsc::channel();
            *lock(&store.handle.shared.hold_next_reader) = Some((entered_tx, release_rx));
            let compacted = Arc::new(AtomicBool::new(false));
            std::thread::scope(|s| {
                let h = store.handle();
                let scanner = s.spawn(move || h.scan(&key(10), Some(&key(20)), false, 100, None));
                entered.recv().expect("the scan reached its table phase");
                assert_eq!(store.handle.shared.gate.pinned(), 1);
                for k in 0..60 {
                    store.put(&key(k), &stamped(k, 2)).expect("put");
                }
                let (h, done) = (store.handle(), Arc::clone(&compacted));
                let mutator = s.spawn(move || {
                    h.compact().expect("compact");
                    done.store(true, Ordering::SeqCst);
                });
                std::thread::sleep(Duration::from_millis(300));
                assert!(
                    !compacted.load(Ordering::SeqCst),
                    "tables were rewritten while a scan was inside them"
                );
                release.send(()).expect("release");
                let page = scanner.join().expect("scanner").expect("scan");
                let got: Vec<_> = page.iter().map(|(_, v)| unstamp(v)).collect();
                let want: Vec<_> = (10..20).map(|k| (k, 1)).collect();
                assert_eq!(got, want, "the page is the store as of its start");
                mutator.join().expect("mutator");
            });
            let shared = &store.handle.shared;
            assert_eq!(shared.gate.pinned(), 0);
            assert_eq!(
                lock(&shared.pages).len(),
                SCAN_PAGES,
                "a page buffer was lost"
            );
        });
    }

    /// A scan takes the memtable a page at a time, and a page stops
    /// where its copy of the memtable stops. With more keys in the
    /// memtable than a page copies, over tables holding other keys and
    /// older versions, scans in both directions, bounded and unbounded,
    /// and `scan` with limits on either side of a page, all yield
    /// exactly the model.
    #[test]
    fn scans_page_across_the_memtable_and_the_tables() {
        let store = TempStore::new("horizon");
        let mut model = std::collections::BTreeMap::new();
        // Tables: every third key, and old versions of the rest.
        for k in 0..3000usize {
            store.put(&key(k), b"old").expect("put");
            model.insert(key(k), b"old".to_vec());
        }
        store.flush().expect("flush");
        // Memtable: more keys than one page (1024), some deleted.
        for k in (0..3000usize).filter(|k| k % 3 != 0) {
            if k % 7 == 0 {
                store.delete(&key(k)).expect("delete");
                model.remove(&key(k));
            } else {
                store.put(&key(k), b"new").expect("put");
                model.insert(key(k), b"new".to_vec());
            }
        }
        let all: Entries = model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        let collect = |start: &[u8], end: Option<&[u8]>, reverse: bool| -> Entries {
            store
                .range(start, end, reverse)
                .expect("range")
                .map(|kv| kv.expect("page"))
                .collect()
        };
        assert_eq!(collect(b"", None, false), all);
        let mut rev = all.clone();
        rev.reverse();
        assert_eq!(collect(b"", None, true), rev);

        let (lo, hi) = (key(500), key(2600));
        let slice: Entries = all
            .iter()
            .filter(|(k, _)| *k >= lo && *k < hi)
            .cloned()
            .collect();
        assert_eq!(collect(&lo, Some(&hi), false), slice);
        let mut slice_rev = slice.clone();
        slice_rev.reverse();
        assert_eq!(collect(&lo, Some(&hi), true), slice_rev);

        // `scan` returns up to its limit even when that takes pages.
        for limit in [1usize, 7, 1024, 1025, 2500, 10_000] {
            let got = store
                .scan(&lo, Some(&hi), false, limit, None)
                .expect("scan");
            assert_eq!(
                got,
                slice[..limit.min(slice.len())],
                "forward, limit {limit}"
            );
            let got = store.scan(&lo, Some(&hi), true, limit, None).expect("scan");
            assert_eq!(
                got,
                slice_rev[..limit.min(slice.len())],
                "reverse, limit {limit}"
            );
        }
        assert_eq!(lock(&store.handle.shared.pages).len(), SCAN_PAGES);
    }

    /// Scanners on three threads, forward and in reverse, against two
    /// writers that keep the store flushing and compacting. Every scan
    /// sees every key exactly once, in order, with a whole value that is
    /// never older than that scanner has seen for the key.
    #[test]
    fn scanners_and_writers_run_together_under_compaction() {
        within_a_minute(|| {
            const KEYS: usize = 96;
            let store = TempStore::new("scan-parallel");
            for k in 0..KEYS {
                store.put(&key(k), &stamped(k, 0)).expect("put");
            }
            let stop = Arc::new(AtomicBool::new(false));
            let scans = Arc::new(AtomicUsize::new(0));
            std::thread::scope(|s| {
                let scanners: Vec<_> = (0..3usize)
                    .map(|t| {
                        let (h, stop, scans) =
                            (store.handle(), Arc::clone(&stop), Arc::clone(&scans));
                        s.spawn(move || {
                            let mut seen = [0u64; KEYS];
                            // Until the writers are done, and at least
                            // eight scans however fast this machine is.
                            let mut mine = 0usize;
                            while mine < 8 || !stop.load(Ordering::Relaxed) {
                                mine += 1;
                                let reverse = t == 2;
                                let got: Vec<_> = h
                                    .range(b"", None, reverse)
                                    .expect("range")
                                    .map(|kv| unstamp(&kv.expect("page").1))
                                    .collect();
                                let mut keys: Vec<_> = got.iter().map(|&(k, _)| k).collect();
                                if reverse {
                                    keys.reverse();
                                }
                                assert_eq!(keys, (0..KEYS).collect::<Vec<_>>(), "thread {t}");
                                for (k, version) in got {
                                    assert!(version >= seen[k], "key {k} went back in time");
                                    seen[k] = version;
                                }
                                scans.fetch_add(1, Ordering::Relaxed);
                            }
                        })
                    })
                    .collect();
                let writers: Vec<_> = (0..2usize)
                    .map(|w| {
                        let h = store.handle();
                        s.spawn(move || {
                            for version in 1..=40u64 {
                                for k in (w..KEYS).step_by(2) {
                                    h.put(&key(k), &stamped(k, version)).expect("put");
                                }
                            }
                        })
                    })
                    .collect();
                for w in writers {
                    w.join().expect("writer");
                }
                store.compact().expect("compact");
                stop.store(true, Ordering::Relaxed);
                for sc in scanners {
                    sc.join().expect("scanner");
                }
            });
            assert!(scans.load(Ordering::Relaxed) >= 3 * 8);
            let shared = &store.handle.shared;
            assert_eq!(shared.gate.pinned(), 0);
            assert_eq!(lock(&shared.pages).len(), SCAN_PAGES);
        });
    }
}
