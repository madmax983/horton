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
//!  client threads ───▶ engine lock ──▶ Db ──▶ FileDevice
//!  compaction threads ──┘   (one holder at a time; each runs horton itself)
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
//! - **Group commit** falls out of it: consecutive writes in the queue,
//!   up to `GROUP_BLOCKS_MAX` WAL blocks of them, are committed with
//!   horton's `Db::write_group` (ADR-0028): one `fdatasync` for all of
//!   them, and visible only once it has returned. This is `LevelDB`'s
//!   writer queue. Each client's batch stays atomic, inside one WAL
//!   block; a write is acknowledged when the prefix of the group it is in
//!   is durable.
//! - **Waiters spin before they sleep.** An operation takes microseconds,
//!   so a waiting thread polls for its result and for the lock for up to
//!   [`SPIN`] before parking, as `RocksDB`'s write thread does. Parking
//!   costs more than the wait it would save. A parked thread is woken by
//!   the thread that serves its request, or, if it is at the head of the
//!   queue, by whoever releases the lock.
//! - **Capacity errors are handled here.** horton reports a full memtable,
//!   WAL or level 0 as an error naming its remedy. A full memtable or
//!   WAL is flushed by the write that finds it, under the lock. A full
//!   level 0 needs a compaction job, and the write waits for the
//!   compaction threads to make room, *without* the lock: its group
//!   stays at the head of the queue and its thread comes back to look
//!   ([`Engine::commit_group`]). Clients never see these errors.
//! - **Compaction merges outside the lock** (ADR-0024). A compaction
//!   job is mostly merge: reading its input tables and writing its
//!   output's blocks. horton runs that part without the database, so
//!   a compaction thread does it with no lock, through a file handle
//!   of its own. What needs the database (choosing a job, sealing and
//!   committing an output) is a short step under the engine lock, taken
//!   by whichever thread has the lock: a writer ending its turn, or a
//!   compaction thread. The job's scratch is handed back and forth
//!   ([`Handoff`]). Readers of the tables are not kept out while a merge
//!   runs: it writes only blocks nothing points at yet.
//! - **Two compaction jobs at a time** (ADR-0025). horton runs a job in
//!   each of two lanes when the two share no table and write to
//!   different levels: level 0 draining into level 1, say, beside a
//!   table merging into the bottom level. Each lane has a scratch, a
//!   hand-off and a compaction thread of its own, and the steps with
//!   the database are taken for every lane that is due one
//!   ([`Room::background`]).
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
//! - **Flush and compaction's commits wait for readers.** They are what
//!   changes which tables exist and gives a table's blocks up for
//!   reuse, and a reader must not have that happen under it. A read in
//!   its table phase holds a [`Pin`]. A flush, and each of compaction's
//!   steps with the database, run under the engine lock and first shut
//!   the readers' gate and wait for the pins to drain ([`Gate::shut`]):
//!   a reader without the lock sees the gate shut and waits for the
//!   lock like the rest. A table phase is tens of microseconds, so the
//!   wait is short.
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
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError, TryLockError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use horton::wal::WAL_RECORD_OVERHEAD;
use horton::{
    BlockDevice, COMPACTION_LANES, CachePort, Detached, Error, GROUP_BLOCKS_MAX, GroupOp, Lane,
    OpenReport, Progress, ScanBlock, SlotStats,
};

use crate::cache::SharedCache;
use crate::device::{FileDevice, IoError, IoStats, ReadDevice, StoreLock};
use crate::gate::{Gate, Pin, Slot};
use crate::handoff::{Handoff, Next};
use crate::{
    BLOCK, KEY_MAX, KvCompaction, KvDb, KvManifest, KvMemPage, KvProbe, KvReadScratch, KvRevScan,
    KvScan, KvTables, LEVELS, MEMTABLE_ARENA, MEMTABLE_ENTRIES, VAL_MAX, block_on,
};

/// horton's error, with this store's device error.
pub type DbError = Error<IoError>;

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

/// How long a compaction thread waits before it tries the engine lock
/// again, when a step with the database is due and a client has the
/// lock. It never blocks on that lock: the client may be a writer
/// waiting, lock in hand, for this thread to merge.
const BACKGROUND_PAUSE: Duration = Duration::from_micros(100);

/// How long a writer whose write waits for compaction sleeps before it
/// looks again. It holds no lock meanwhile; each look takes the engine
/// lock for a moment and gives compaction a step if one is due.
const STALL_PAUSE: Duration = Duration::from_micros(200);

/// What a call that finds no room does when making room takes a
/// compaction job.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Stall {
    /// Sees the job through, engine lock in hand.
    Wait,
    /// Returns [`Error::NeedsCompaction`], for a caller that can give
    /// the engine lock up while compaction runs.
    Report,
}

/// The compaction scratch's way between the engine and the compaction
/// thread, with the manifest copy a merge reads its targets from.
type KvHandoff = Handoff<Box<KvCompaction>, Arc<KvManifest>>;

/// Why a store call failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreError {
    /// horton refused the operation or the device failed.
    Db(DbError),
    /// The store could not be opened.
    Open(String),
    /// Another opener holds the store (ADR-0032). Try again when it
    /// closes.
    Locked(String),
    /// The file is too small for the store's shape (ADR-0033). `blocks`
    /// is the size of the file, or of a new file. `existing` is `true`
    /// if the file had a size before the open. The open does not size a
    /// new file. Remedy: open with more blocks. For an existing file,
    /// use another path or delete the file.
    TooSmall {
        path: std::path::PathBuf,
        blocks: u64,
        min: u64,
        existing: bool,
    },
    /// A range starts after it ends (`start > end`, ADR-0033). Swap the
    /// bounds. An empty range (`start == end`) is not an error.
    InvertedRange,
    /// The store was closed (its [`Store`] was dropped), or a thread
    /// panicked while it held the engine.
    Closed,
}

impl core::fmt::Display for StoreError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Db(e) => write!(f, "{e:?}"),
            Self::Open(e) | Self::Locked(e) => write!(f, "open: {e}"),
            Self::TooSmall {
                path,
                blocks,
                min,
                existing,
            } => {
                let path = path.display();
                let mib = mib_for(*min);
                if *existing {
                    write!(
                        f,
                        "open: {path}: the file holds {blocks} blocks; this shape needs at least \
                         {min} blocks ({mib} MiB). A store keeps its size: use another path, or \
                         delete the file"
                    )
                } else {
                    write!(
                        f,
                        "open: {path}: a new store of {blocks} blocks is too small; this shape \
                         needs at least {min} blocks ({mib} MiB). The file is not sized"
                    )
                }
            }
            Self::InvertedRange => write!(f, "the range starts after it ends: swap the bounds"),
            Self::Closed => write!(f, "the store is closed"),
        }
    }
}

impl std::error::Error for StoreError {}

/// The mebibytes that hold `blocks` blocks, rounded up.
#[must_use]
pub const fn mib_for(blocks: u64) -> u64 {
    blocks.saturating_mul(BLOCK as u64).div_ceil(1 << 20)
}

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
/// Each op costs its key and value plus 23 bytes of WAL; a batch may
/// span WAL blocks, up to `GROUP_BLOCKS_MAX` of them ([`BLOCK`] bytes
/// each, ADR-0029). A larger batch is refused with `BatchTooLarge`
/// rather than split.
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

    /// Bytes each op's WAL record takes.
    fn records(&self) -> impl Iterator<Item = usize> + '_ {
        self.ops.iter().map(|op| {
            WAL_RECORD_OVERHEAD
                + match op {
                    BatchOp::Put(k, v) => k.len() + v.len(),
                    BatchOp::Delete(k) => k.len(),
                }
        })
    }

    /// Bytes this batch takes in the WAL.
    fn wal_bytes(&self) -> usize {
        self.records().sum()
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
        let mut alone = Placement::default();
        alone.unit(self);
        if alone.blocks() > GROUP_BLOCKS_MAX {
            return Err(Error::BatchTooLarge {
                bytes: self.wal_bytes(),
                max: GROUP_BLOCKS_MAX * BLOCK,
            });
        }
        // Nor may it outgrow an empty memtable, which horton refuses
        // as well: refused here, it never joins other clients' writes.
        if self.ops.len() > MEMTABLE_ENTRIES {
            return Err(Error::BatchTooLarge {
                bytes: self.ops.len(),
                max: MEMTABLE_ENTRIES,
            });
        }
        let arena: usize = self
            .ops
            .iter()
            .map(|op| match op {
                BatchOp::Put(k, v) => k.len() + v.len(),
                BatchOp::Delete(k) => k.len(),
            })
            .sum();
        if arena > MEMTABLE_ARENA {
            return Err(Error::BatchTooLarge {
                bytes: arena,
                max: MEMTABLE_ARENA,
            });
        }
        Ok(())
    }
}

/// Where horton places a group's batches in WAL blocks (ADR-0028,
/// ADR-0029): a batch that does not fit what is left of a block starts a
/// fresh one; inside a batch, a record that does not fit starts the next
/// block.
#[derive(Debug, Clone, Copy, Default)]
struct Placement {
    /// Blocks written out so far.
    sealed: usize,
    /// Bytes in the block being filled.
    filling: usize,
}

impl Placement {
    fn unit(&mut self, batch: &Batch) {
        let wal = batch.wal_bytes();
        if wal == 0 {
            return;
        }
        if self.filling > 0 && self.filling + wal > BLOCK {
            self.sealed += 1;
            self.filling = 0;
        }
        for rlen in batch.records() {
            if self.filling + rlen > BLOCK {
                self.sealed += 1;
                self.filling = rlen;
            } else {
                self.filling += rlen;
            }
        }
    }

    const fn blocks(&self) -> usize {
        self.sealed + if self.filling > 0 { 1 } else { 0 }
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

/// Refuses a range that starts after it ends (ADR-0033). An open end
/// (`None`) is the last key.
pub fn check_order(start: &[u8], end: Option<&[u8]>) -> Result<(), StoreError> {
    match end {
        Some(end) if start > end => Err(StoreError::InvertedRange),
        _ => Ok(()),
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
    /// Write groups committed (each one `fdatasync` when syncing), and
    /// the client writes they carried.
    pub groups: u64,
    pub grouped_writes: u64,
    pub flushes: u64,
    pub compaction_jobs: u64,
    /// Steps compaction took: those with the database, under the engine
    /// lock, and the merge quanta the compaction thread ran outside it.
    pub compaction_steps: u64,
    /// The merge quanta among them that ran outside the engine lock.
    pub detached_steps: u64,
    /// Merges that began while another lane's was running.
    pub merges_side_by_side: u64,
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
    /// The compaction threads have work (or must look for some).
    background_due: AtomicBool,
    /// The store is closing: the compaction threads exit.
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
    /// Where each lane's compaction scratch changes hands, and where its
    /// compaction thread sleeps until the scratch is lent, or
    /// `background_due` or `stop` is raised. The engine's [`Room`] has
    /// the other references.
    handoff: [Arc<KvHandoff>; COMPACTION_LANES],
    /// Merge quanta the compaction threads ran outside the engine lock,
    /// and the I/O of each one's handle to the file (reads, writes,
    /// syncs).
    detached_steps: AtomicU64,
    merge_io: [[AtomicU64; 3]; COMPACTION_LANES],
    /// Compaction threads merging right now, and how many merges began
    /// while another was running.
    merging: AtomicUsize,
    merges_side_by_side: AtomicU64,
    /// Test hook: the next merge the compaction thread picks up reports
    /// in on the sender and waits, scratch in hand, for the receiver.
    #[cfg(test)]
    hold_next_merge: Mutex<Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>>,
    /// Test hook: the next merge fails before its first quantum.
    #[cfg(test)]
    fail_next_merge: AtomicBool,
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
    /// The step is one with the database, for each lane that is due
    /// one: choosing a job, or sealing and committing the output a
    /// merge has filled. The merge itself is a compaction thread's,
    /// outside this lock; the step ends by lending it the scratch.
    ///
    /// The step is skipped, not waited for, while a lane's scratch is
    /// out merging, and while readers hold pins: a write should not
    /// stand behind a merge or other threads' reads for the sake of
    /// compaction. The compaction threads, and any call that needs room
    /// and is not queued, do wait.
    fn finish(mut self) {
        if self.steppable() && self.gate.pinned() == 0 {
            self.background_step();
        }
        // Still something a compaction thread could do under this lock
        // (the step above was skipped): tell them.
        let wake = self.steppable();
        let shared = self.shared;
        drop(self);
        shared.wake_queue();
        if wake {
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
        let pending = self.steppable();
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
        let started = Instant::now();
        'spin: loop {
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
        }
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
                    let stalled = engine.serve(self, &me);
                    engine.finish();
                    if stalled && me.done.get().is_none() {
                        // The head of the queue waits for compaction to
                        // make room, and this write with it. Not under
                        // the engine lock: readers need it meanwhile,
                        // and so do compaction's own steps.
                        std::thread::park_timeout(STALL_PAUSE);
                    }
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
                    let since = *waiting_since.get_or_insert_with(Instant::now);
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
        // while a step is due, and a flag that is already up should not
        // be written again.
        if !self.background_due.load(Ordering::Acquire)
            && !self.background_due.swap(true, Ordering::AcqRel)
        {
            for handoff in &self.handoff {
                handoff.wake();
            }
        }
    }

    /// A compaction thread, `lane`'s. It merges: whenever a step with
    /// the database leaves its lane's job in its merge, the scratch is
    /// lent to this thread, which reads the job's tables and writes its
    /// output through `device`, a handle of its own to the file, under
    /// no lock (horton's detached merge, ADR-0024). When the merge needs
    /// the database again it gives the scratch back, and whichever
    /// thread has the engine lock first takes the next step: a writer
    /// ending its turn, or one of these threads.
    ///
    /// There is a thread for each lane, and so, when horton finds two
    /// jobs that can run side by side (ADR-0025), two merges at once.
    fn background(&self, lane: usize, mut device: FileDevice) {
        /// Tells the handoff the thread has gone, however it goes: a
        /// caller waiting for a merge must not wait for one that will
        /// never be run.
        struct Leaving<'a>(&'a KvHandoff);
        impl Drop for Leaving<'_> {
            fn drop(&mut self) {
                self.0.leave();
            }
        }
        let handoff = &self.handoff[lane];
        let _leaving = Leaving(handoff);
        loop {
            match handoff.next(&self.stop, &self.background_due) {
                Next::Stop => return,
                Next::Merge(mut scratch, tables) => {
                    if self.merging.fetch_add(1, Ordering::AcqRel) > 0 {
                        self.merges_side_by_side.fetch_add(1, Ordering::Relaxed);
                    }
                    self.merge(lane, &mut scratch, &tables, &mut device);
                    self.merging.fetch_sub(1, Ordering::AcqRel);
                    handoff.give_back(scratch);
                    if self.stop.load(Ordering::Acquire) {
                        return;
                    }
                }
                Next::Look => {}
            }
            // A step with the database is due, unless a writer has taken
            // it already and lent the scratch again. The engine lock is
            // tried, never waited for: its holder may be a caller that
            // is waiting, lock in hand, for this thread to merge.
            loop {
                if self.stop.load(Ordering::Acquire) {
                    return;
                }
                if handoff.is_lent() {
                    break;
                }
                let mut guard = match self.engine.try_lock() {
                    Ok(guard) => guard,
                    Err(TryLockError::Poisoned(_)) => return,
                    Err(TryLockError::WouldBlock) => {
                        std::thread::sleep(BACKGROUND_PAUSE);
                        continue;
                    }
                };
                let Some(engine) = guard.as_mut() else {
                    return;
                };
                self.engine_held.store(true, Ordering::Relaxed);
                if !(engine.steppable() && engine.background_step()) {
                    // Nothing for a compaction thread to do under this
                    // lock. Cleared while it is held: a client that
                    // leaves a step due after this raises it again.
                    self.background_due.store(false, Ordering::Release);
                }
                self.engine_held.store(false, Ordering::Relaxed);
                drop(guard);
                self.wake_queue();
                break;
            }
        }
    }

    /// Merges with a lent scratch until the merge needs the database
    /// (its output is full, or its inputs are exhausted), the engine
    /// recalls the scratch, or the store closes. No lock is held: the
    /// scratch is this thread's until it gives it back, `tables` is the
    /// manifest as the engine lent it, and `device` is its own.
    ///
    /// A merge that fails gives its job up ([`Compaction::abandon`]):
    /// the next step with the database ends the job, and the store
    /// leaves compaction to the write path, which reports the error to
    /// a caller.
    ///
    /// [`Compaction::abandon`]: horton::Compaction::abandon
    fn merge(
        &self,
        lane: usize,
        scratch: &mut KvCompaction,
        tables: &KvManifest,
        device: &mut FileDevice,
    ) {
        let handoff = &self.handoff[lane];
        #[cfg(test)]
        self.hold_merge_if_asked();
        // horton does not panic. If it did here, the scratch would be
        // lost with this thread, and a writer waiting for it would wait
        // for good: the scratch goes back whatever happens.
        let merged = std::panic::catch_unwind(core::panic::AssertUnwindSafe(|| {
            #[cfg(test)]
            if self.fail_next_merge.swap(false, Ordering::SeqCst) {
                return Err(Error::CorruptManifest);
            }
            let mut wrote = false;
            let merged = loop {
                if handoff.recalled() || self.stop.load(Ordering::Acquire) {
                    break Ok(());
                }
                match block_on(scratch.merge_detached(device, tables)) {
                    Ok(Detached::More) => {
                        wrote = true;
                        self.detached_steps.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(Detached::NeedsDb) => break Ok(()),
                    Err(e) => break Err(e),
                }
            };
            // The database syncs the file before it commits an output.
            // That covers these blocks wherever a sync is the file's;
            // this handle syncs what it wrote all the same.
            if wrote && merged.is_ok() {
                block_on(core::future::poll_fn(|cx| device.poll_flush(cx)))
                    .map_err(Error::Device)?;
            }
            merged
        }));
        if !matches!(merged, Ok(Ok(()))) {
            scratch.abandon();
            handoff.fail();
        }
        let io = device.stats();
        for (total, now) in self.merge_io[lane]
            .iter()
            .zip([io.reads, io.writes, io.syncs])
        {
            total.store(now, Ordering::Relaxed);
        }
    }

    /// Test hook: holds the compaction thread here, a lent scratch in
    /// hand and no quantum run yet, if a test asked for the next merge
    /// to be held.
    #[cfg(test)]
    fn hold_merge_if_asked(&self) {
        let hold = lock(&self.hold_next_merge).take();
        if let Some((entered, release)) = hold {
            let _ = entered.send(());
            let _ = release.recv();
        }
    }
}

/// A cloneable, `Send` handle to an open store. Every call blocks the
/// calling thread until the operation is done.
#[derive(Clone)]
pub struct Handle {
    shared: Arc<Shared>,
}

/// An open store: a [`Handle`] plus the compaction threads. Dropping it
/// closes the store: the database and its file are released, and calls
/// through handles still alive return [`StoreError::Closed`].
pub struct Store {
    handle: Handle,
    threads: Vec<JoinHandle<()>>,
    /// The lock. It drops after `Drop::drop` returns, thus after the
    /// store closes.
    _lock: StoreLock,
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
        // Each compaction thread stops after the quantum it is in and
        // gives its scratch back. A job cut short there has written only
        // blocks nothing points at.
        shared.stop.store(true, Ordering::Release);
        for handoff in &shared.handoff {
            handoff.wake();
        }
        for t in self.threads.drain(..) {
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
    /// if the last process died mid-write. If another opener holds the
    /// store, it returns [`StoreError::Locked`].
    pub fn open(path: &Path, options: Options) -> Result<(Self, OpenReport), StoreError> {
        // Lock first: two openers must not both set a new file's size.
        let lock = lock_store(path)?;
        // Check the size before `FileDevice::open` sizes a new file.
        check_size(path, options.create_blocks)?;
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
        let handoff = engine.room.handoff.clone();
        let shared = Arc::new(Shared {
            engine: Mutex::new(Some(engine)),
            queue: Mutex::new(VecDeque::new()),
            queued: AtomicUsize::new(0),
            engine_held: AtomicBool::new(false),
            // Look once: recovery may have left a job pending.
            background_due: AtomicBool::new(true),
            stop: AtomicBool::new(false),
            gate,
            read_device,
            cache,
            pages: Arc::new(Mutex::new(pages)),
            #[cfg(test)]
            hold_next_reader: Mutex::new(None),
            handoff,
            detached_steps: AtomicU64::new(0),
            merge_io: [const { [const { AtomicU64::new(0) }; 3] }; COMPACTION_LANES],
            merging: AtomicUsize::new(0),
            merges_side_by_side: AtomicU64::new(0),
            #[cfg(test)]
            hold_next_merge: Mutex::new(None),
            #[cfg(test)]
            fail_next_merge: AtomicBool::new(false),
        });
        let mut store = Self {
            handle: Handle { shared },
            threads: Vec::new(),
            _lock: lock,
        };
        for lane in 0..COMPACTION_LANES {
            // Each compaction thread's own handle to the file. Opened,
            // not duplicated: see `ReadDevice`. (If one fails to open or
            // start, dropping `store` stops those that did.)
            let device = FileDevice::open(path, options.create_blocks, options.sync)
                .map_err(|e| StoreError::Open(format!("{}: {e}", path.display())))?;
            let background = Arc::clone(&store.handle.shared);
            let thread = std::thread::Builder::new()
                .name(format!("horton-compact-{lane}"))
                .spawn(move || background.background(lane, device))
                .map_err(|e| StoreError::Open(e.to_string()))?;
            store.threads.push(thread);
        }
        Ok((store, report))
    }

    /// Deletes the store at `path`, under its lock. The lock file stays
    /// (ADR-0032). A store that is not there is not an error.
    ///
    /// # Errors
    ///
    /// [`StoreError::Locked`] if another opener holds the store, or
    /// [`StoreError::Open`] if the lock or the delete fails.
    pub fn destroy(path: &Path) -> Result<(), StoreError> {
        if !path.try_exists().unwrap_or(true) {
            return Ok(());
        }
        let _lock = lock_store(path)?;
        // Delete the store, not a link to it.
        std::fs::canonicalize(path)
            .and_then(std::fs::remove_file)
            .map_err(|e| StoreError::Open(format!("{}: delete: {e}", path.display())))
    }

    /// A handle for another thread.
    pub fn handle(&self) -> Handle {
        self.handle.clone()
    }
}

/// Refuses a file too small for the shape (ADR-0033). A file of zero
/// bytes is new: it gets `create_blocks` blocks. A file of part blocks
/// is not a store: [`FileDevice::open`] refuses it.
fn check_size(path: &Path, create_blocks: u64) -> Result<(), StoreError> {
    let len = std::fs::metadata(path)
        .map_err(|e| StoreError::Open(format!("{}: {e}", path.display())))?
        .len();
    if len % BLOCK as u64 != 0 {
        return Ok(());
    }
    let blocks = if len == 0 {
        create_blocks
    } else {
        len / BLOCK as u64
    };
    let min = KvDb::<FileDevice>::MIN_DEVICE_BLOCKS;
    if blocks < min {
        return Err(StoreError::TooSmall {
            path: path.to_path_buf(),
            blocks,
            min,
            existing: len != 0,
        });
    }
    Ok(())
}

/// Locks the store at `path` (ADR-0032).
fn lock_store(path: &Path) -> Result<StoreLock, StoreError> {
    StoreLock::acquire(path).map_err(|e| {
        let locked = e.kind() == std::io::ErrorKind::WouldBlock;
        let e = format!("{}: {e}", path.display());
        if locked {
            StoreError::Locked(e)
        } else {
            StoreError::Open(e)
        }
    })
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
    /// A range that starts after it ends gives [`StoreError::InvertedRange`].
    pub fn delete_range(&self, start: &[u8], end: &[u8]) -> Result<(), StoreError> {
        check_key(start)?;
        check_key(end)?;
        check_order(start, Some(end))?;
        self.shared.with(|e| {
            let gate = &e.gate;
            e.room
                .with_room(&mut e.db, Stall::Wait, |db| {
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
    /// descending, as of `snapshot` (or now). A range that starts after
    /// it ends gives [`StoreError::InvertedRange`].
    pub fn scan(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        reverse: bool,
        limit: usize,
        snapshot: Option<&Snapshot>,
    ) -> Result<Entries, StoreError> {
        check_order(start, end)?;
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
    /// it takes now, fetching [`PAGE`] entries at a time. A range that
    /// starts after it ends gives [`StoreError::InvertedRange`].
    pub fn range(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        reverse: bool,
    ) -> Result<Iter, StoreError> {
        check_order(start, end)?;
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
            // A new request ends the job in flight and frees the slot it
            // is writing: the scratches come home first, so that no merge
            // is still writing there.
            e.room.home_all()?;
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
        // The compaction threads' merge quanta, and their handles' I/O.
        stats.detached_steps = self.shared.detached_steps.load(Ordering::Relaxed);
        stats.compaction_steps += stats.detached_steps;
        stats.merges_side_by_side = self.shared.merges_side_by_side.load(Ordering::Relaxed);
        for [reads, writes, syncs] in &self.shared.merge_io {
            stats.io.reads += reads.load(Ordering::Relaxed);
            stats.io.writes += writes.load(Ordering::Relaxed);
            stats.io.syncs += syncs.load(Ordering::Relaxed);
        }
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
    /// `Some` while the writes in `group` wait for compaction to make
    /// room: the manifest's sequence number when they were refused. No
    /// table has changed, and so no room has been made, until that
    /// number has.
    stalled: Option<u64>,
}

impl Engine {
    #[allow(
        clippy::large_stack_frames,
        reason = "the database is built on the stack before it is boxed; the opening thread has 64 MiB"
    )]
    fn open(device: FileDevice, sync: bool) -> Result<Self, StoreError> {
        let blocks = device.blocks();
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
                scratch: core::array::from_fn(|_| Some(Box::new(KvCompaction::new()))),
                handoff: core::array::from_fn(|_| Arc::new(Handoff::new())),
                idle_at: [None; COMPACTION_LANES],
                gate: Arc::clone(&gate),
                flushes: 0,
                jobs: 0,
                steps: 0,
            },
            gate,
            group: Vec::new(),
            tables,
            compaction_stuck: None,
            sync,
            open,
            groups: 0,
            grouped_writes: 0,
            stalled: None,
        })
    }

    fn background_pending(&self) -> bool {
        self.db.compaction_pending() && self.compaction_stuck != Some(self.room.flushes)
    }

    /// True when compaction has a step to take under the engine lock
    /// right now: a job is pending, and some lane can take a step
    /// without waiting for its compaction thread.
    fn steppable(&self) -> bool {
        self.background_pending() && self.room.steppable(&self.db)
    }

    /// The steps with the database that are due, one for each lane that
    /// can take one, each ending with the lane's scratch lent to its
    /// compaction thread if a job is in its merge. Returns true if a
    /// step was taken.
    ///
    /// A lane whose merge failed takes the step that gives its job up,
    /// and no lane starts another: a failed merge, like a step that
    /// fails, leaves compaction to the write path until the next flush
    /// (see `compaction_stuck`).
    fn background_step(&mut self) -> bool {
        let mut failed = false;
        for lane in 0..COMPACTION_LANES {
            if self.room.handoff[lane].take_failed() {
                failed = true;
                // The scratch is back, and says the job is lost.
                let _ = self.room.step(&mut self.db, lane);
            }
        }
        let stepped = if failed {
            Ok(true)
        } else {
            self.room.background(&mut self.db)
        };
        if failed || stepped.is_err() {
            self.compaction_stuck = Some(self.room.flushes);
        }
        stepped.unwrap_or(true)
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
    /// Stops when the queue is empty or after [`SERVE_MAX`] groups, and
    /// returns false; or at a group that has to wait for compaction to
    /// make room, and returns true. That group stays at the head, in
    /// `self.group`, and is tried again first by whichever thread comes
    /// next.
    ///
    /// `me` is the caller's own write. The queue is first in, first out,
    /// so by the time `me` is committed so is everything that was
    /// waiting when it queued. The caller goes on committing others
    /// while that is cheap, which keeps the database in one CPU's cache.
    /// It stops before a group that will be `fdatasync`ed, so that its
    /// client's next write can join that group instead of waiting a
    /// flush behind it, which is what makes synced groups large.
    fn serve(&mut self, shared: &Shared, me: &Pending) -> bool {
        for _ in 0..SERVE_MAX {
            if self.stalled.is_none() {
                let committed = me.done.get().is_some();
                if !self.take_group(shared, self.sync && committed) {
                    return false;
                }
            }
            if !self.commit_group() {
                return true;
            }
        }
        false
    }

    /// Moves the write at the head of the queue, and every write queued
    /// directly behind it that fits the same `GROUP_BLOCKS_MAX` WAL
    /// blocks, into
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
        // The head always goes: `Handle::write` checked that it fits
        // `GROUP_BLOCKS_MAX` blocks by itself. Behind it, as many writes as
        // fill those blocks, placed as horton places them (ADR-0028,
        // ADR-0029): one `fdatasync` for all of them.
        let mut place = Placement::default();
        place.unit(&head.batch);
        self.group.push(head);
        while let Some(next) = queue.front() {
            let mut then = place;
            then.unit(&next.batch);
            if then.blocks() > GROUP_BLOCKS_MAX {
                break;
            }
            place = then;
            self.group.extend(queue.pop_front());
        }
        shared.queued.store(queue.len(), Ordering::Relaxed);
        drop(queue);
        true
    }

    /// Commits the writes in `self.group` as one group commit and tells
    /// each of them the result. Returns false, with the writes not yet
    /// committed kept, when they have to wait for compaction to make
    /// room.
    ///
    /// A write that finds the memtable full flushes it here, under the
    /// engine lock. When the flush is refused because level 0 or the
    /// table region is full, room takes a compaction job, and that is
    /// not waited for here: the compaction thread merges outside this
    /// lock, and a writer that sat on the lock until it was done would
    /// keep out every reader that needs the memtable, and the steps the
    /// job itself needs the database for. The caller gives the lock up
    /// and comes back ([`Shared::submit`]).
    fn commit_group(&mut self) -> bool {
        if self.stalled == Some(self.db.manifest().seq()) && self.background_pending() {
            // No table has changed since the group was refused.
            return false;
        }
        self.stalled = None;
        let mut result = self.write_group(Stall::Report);
        if result == Err(Error::NeedsCompaction) {
            if self.background_pending() {
                self.stalled = Some(self.db.manifest().seq());
                // A write is about to wait on compaction: every lane
                // looks for a job once more, whatever it found before.
                self.room.idle_at = [None; COMPACTION_LANES];
                return false;
            }
            // Compaction is not running in the background (it failed
            // there): the job is run here, and what stops it is this
            // write's answer.
            result = self.write_group(Stall::Wait);
        }
        let result = result.map_err(StoreError::Db);
        for p in self.group.drain(..) {
            p.finish(result.clone());
        }
        true
    }

    /// Commits the writes of `self.group` with horton's group commit
    /// (`Db::write_group`, ADR-0028): each client's batch one atomic
    /// unit, the group one `fdatasync`. Makes room as `stall` says, and
    /// finishes each write as soon as the prefix it is in is durable.
    /// Returns once all are committed, or with the error that stopped
    /// the rest, which stay in `self.group`.
    ///
    /// horton commits the longest prefix that fits the memtable and the
    /// WAL. When that is not all of them, the rest go round again, and
    /// the room logic flushes first if the memtable or the WAL was full.
    fn write_group(&mut self, stall: Stall) -> Result<(), DbError> {
        while !self.group.is_empty() {
            let (room, db, group, gate) = (&mut self.room, &mut self.db, &self.group, &self.gate);
            // Each client's batch is one unit, its ops borrowed.
            let mut ops = Vec::new();
            let mut ends = Vec::with_capacity(group.len());
            for p in group {
                ops.extend(p.batch.ops.iter().map(|op| match op {
                    BatchOp::Put(k, v) => GroupOp::put(k, v),
                    BatchOp::Delete(k) => GroupOp::delete(k),
                }));
                ends.push(ops.len());
            }
            let mut start = 0;
            let units: Vec<&[GroupOp<'_>]> = ends
                .iter()
                .map(|&end| {
                    let unit = &ops[start..end];
                    start = end;
                    unit
                })
                .collect();
            let done = match room.with_room(db, stall, |db| {
                // The filter first, then the memtable: a read that
                // starts after this write returns must find the bits.
                // And before each try: making room flushes, and a flush
                // empties the filter.
                for p in group {
                    for op in &p.batch.ops {
                        let (BatchOp::Put(key, _) | BatchOp::Delete(key)) = op;
                        gate.note(key);
                    }
                }
                block_on(db.write_group(&units))
            }) {
                // The head can never commit, alone or not (`Handle::write`
                // checks for that first, so this is a backstop): it fails
                // by itself, and the writes behind it go on.
                Err(
                    e @ (Error::BatchTooLarge { .. }
                    | Error::EmptyKey
                    | Error::KeyTooLarge { .. }
                    | Error::ValueTooLarge { .. }),
                ) => {
                    let head = self.group.remove(0);
                    head.finish(Err(StoreError::Db(e)));
                    continue;
                }
                other => other?,
            };
            if done.units == 0 {
                // Unreachable: the head fits the blocks by itself, and a
                // refusal for want of room is an error, not an empty
                // prefix.
                return Err(Error::BatchTooLarge {
                    bytes: self.group.first().map_or(0, |p| p.batch.wal_bytes()),
                    max: GROUP_BLOCKS_MAX * BLOCK,
                });
            }
            self.groups += 1;
            self.grouped_writes += done.units as u64;
            for p in self.group.drain(..done.units) {
                p.finish(Ok(()));
            }
        }
        Ok(())
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
            // The compaction threads'; `Handle::stats` fills them in.
            detached_steps: 0,
            merges_side_by_side: 0,
            // The gate's; `Handle::stats` fills them in.
            reads_unlocked: 0,
            reads_locked: 0,
        }
    }
}

/// Makes room when horton asks for it: the compaction scratch, and counts
/// of what it did.
struct Room {
    /// Each lane's compaction scratch; `None` while it is lent to the
    /// lane's compaction thread (see [`Handoff`]).
    scratch: [Option<Box<KvCompaction>>; COMPACTION_LANES],
    handoff: [Arc<KvHandoff>; COMPACTION_LANES],
    /// Where a lane, without a job, last found none to take: the
    /// manifest's sequence number and the level the other lane's job
    /// was writing. Until one of them changes, horton would find none
    /// again, and the lane is not asked (asking shuts the readers'
    /// gate).
    idle_at: [Option<(u64, Option<usize>)>; COMPACTION_LANES],
    /// Shut around every call that changes the tables; see [`Gate`].
    gate: Arc<Gate>,
    flushes: u64,
    jobs: u64,
    steps: u64,
}

/// Test hook: times [`Room::job`] has gone round its lanes, in every
/// store of the process.
#[cfg(test)]
static JOB_PASSES: AtomicU64 = AtomicU64::new(0);

impl Room {
    /// Runs `op`; when horton says the database needs room, makes it the
    /// way the error says and tries again. Room that takes a compaction
    /// job is made or reported, as `stall` says.
    fn with_room<T>(
        &mut self,
        db: &mut KvDb<FileDevice>,
        stall: Stall,
        mut op: impl FnMut(&mut KvDb<FileDevice>) -> Result<T, DbError>,
    ) -> Result<T, DbError> {
        for _ in 0..ROOM_TRIES {
            match op(db) {
                Err(Error::TableFull | Error::ArenaFull | Error::WalFull) => match stall {
                    Stall::Wait => self.flush(db)?,
                    Stall::Report => self.try_flush(db)?,
                },
                Err(Error::NeedsCompaction) if stall == Stall::Wait => self.job(db)?,
                r => return r,
            }
        }
        op(db)
    }

    /// Flushes the memtable, compacting first if level 0 is full.
    fn flush(&mut self, db: &mut KvDb<FileDevice>) -> Result<(), DbError> {
        for _ in 0..ROOM_TRIES {
            match self.try_flush(db) {
                Err(Error::NeedsCompaction) => self.job(db)?,
                r => return r,
            }
        }
        self.try_flush(db)
    }

    /// One try at flushing the memtable: [`Error::NeedsCompaction`] when
    /// level 0 or the table region has no room for the table.
    ///
    /// A flush holds a table writer, and a writer holds the largest
    /// bloom filter a table can have (`BLOOM_BYTES`, 255 KiB), so the
    /// flush future goes on the heap: this runs on whichever client
    /// thread holds the engine, with whatever stack that thread has.
    ///
    /// The try is made with the gate shut, and the gate is opened again
    /// before anything else: a compaction that follows shuts it for
    /// itself, a step at a time.
    fn try_flush(&mut self, db: &mut KvDb<FileDevice>) -> Result<(), DbError> {
        self.gate.shut();
        let flushed = block_on(Box::pin(db.flush()));
        // A flush that returned `Ok` has emptied the memtable.
        self.gate.open(db.manifest().seq(), flushed.is_ok());
        self.flushes += u64::from(flushed.is_ok());
        flushed
    }

    /// Sees a compaction job through for a caller that needs the room
    /// now and holds the engine lock until it has it: returns when a
    /// job has ended, or when no lane has one.
    ///
    /// The steps with the database are taken here. The merges between
    /// them stay with the compaction threads: this thread lends each
    /// scratch and waits. It could merge itself, and would be no slower;
    /// but a step taken here shuts the readers' gate, and a merge on a
    /// compaction thread does not, so reads of the tables go on while
    /// the caller waits for room.
    fn job(&mut self, db: &mut KvDb<FileDevice>) -> Result<(), DbError> {
        // The caller was refused for want of room: every lane looks for
        // a job once more, whatever it found before.
        self.idle_at = [None; COMPACTION_LANES];
        loop {
            #[cfg(test)]
            JOB_PASSES.fetch_add(1, Ordering::Relaxed);
            let mut merging = false;
            for lane in 0..COMPACTION_LANES {
                if self.scratch[lane].is_none() {
                    // A merge is under way: let it finish where it is.
                    self.scratch[lane] = self.handoff[lane].try_back();
                }
                if self.scratch[lane].is_none() {
                    merging = true;
                    continue;
                }
                if self.is_idle(db, lane) {
                    continue;
                }
                match self.background_step(db, lane)? {
                    // The lane found no job to take.
                    Progress::Done if self.idle_at[lane].is_some() => {}
                    // The lane's job ended: room may have been made.
                    Progress::Done => return Ok(()),
                    // Lent to its thread (or, if the thread has gone,
                    // back at once for the next quantum here).
                    Progress::More => merging = true,
                }
            }
            if !merging {
                return Ok(());
            }
            if !self.steppable(db) {
                // Every lane is merging, or has nothing to do beside
                // the one that is.
                std::thread::sleep(STALL_PAUSE);
            }
        }
    }

    /// True when `lane` can take a step without waiting for its
    /// compaction thread: its scratch is here, or back from its merge
    /// (or lent to a thread that has gone, and then the merge is run
    /// here).
    fn ready(&self, lane: usize) -> bool {
        self.scratch[lane].is_some() || self.handoff[lane].is_back()
    }

    /// What a lane without a job goes by when it looks for one: the
    /// manifest, and the level the other lane's job writes to.
    fn outlook(db: &KvDb<FileDevice>, lane: usize) -> (u64, Option<usize>) {
        let beside = (0..COMPACTION_LANES)
            .filter(|&other| other != lane)
            .find_map(|other| db.lane_target(Lane::ALL[other]));
        (db.manifest().seq(), beside)
    }

    /// True when `lane` has no job and found none to take the last time
    /// it looked, and nothing it goes by has changed since.
    fn is_idle(&self, db: &KvDb<FileDevice>, lane: usize) -> bool {
        db.lane_target(Lane::ALL[lane]).is_none()
            && self.idle_at[lane] == Some(Self::outlook(db, lane))
    }

    /// True when some lane has a step to take now: it is ready, and it
    /// has a job in flight or reason to look for one.
    fn steppable(&self, db: &KvDb<FileDevice>) -> bool {
        (0..COMPACTION_LANES).any(|lane| self.ready(lane) && !self.is_idle(db, lane))
    }

    /// Brings `lane`'s scratch home, waiting for its compaction thread
    /// to let go of it if it is merging.
    fn home(&mut self, lane: usize) -> Result<(), DbError> {
        if self.scratch[lane].is_none() {
            self.scratch[lane] = self.handoff[lane].reclaim();
        }
        // Never missing: the scratch is here or with the handoff.
        self.scratch[lane].as_ref().map(drop).ok_or(Error::NotOpen)
    }

    /// Brings every lane's scratch home, for a caller about to end the
    /// jobs in flight; and forgets where the lanes last found nothing,
    /// since such a caller changes what there is to find.
    fn home_all(&mut self) -> Result<(), DbError> {
        self.idle_at = [None; COMPACTION_LANES];
        (0..COMPACTION_LANES).try_for_each(|lane| self.home(lane))
    }

    /// One bounded compaction step of `lane`'s job with the database,
    /// with the gate shut around it. The scratch comes home for it
    /// first. A lane that had no job and found none is marked idle.
    fn step(&mut self, db: &mut KvDb<FileDevice>, lane: usize) -> Result<Progress, DbError> {
        // Before the gate is shut: waiting for a merge quantum to end
        // must not keep readers out.
        self.home(lane)?;
        let Some(scratch) = self.scratch[lane].as_deref_mut() else {
            return Err(Error::NotOpen);
        };
        let had_job = db.lane_target(Lane::ALL[lane]).is_some();
        let seq = db.manifest().seq();
        self.gate.shut();
        let stepped = block_on(db.compact_step_in(Lane::ALL[lane], scratch));
        self.gate.open(db.manifest().seq(), false);
        self.idle_at[lane] = None;
        let p = stepped?;
        self.steps += 1;
        // `Done` ends a job, or says there was none to take. A job that
        // began and ended within the step (a table moved down a level)
        // changed the manifest.
        if p == Progress::Done && !had_job && db.manifest().seq() == seq {
            self.idle_at[lane] = Some(Self::outlook(db, lane));
        } else {
            self.jobs += u64::from(p == Progress::Done);
        }
        Ok(p)
    }

    /// One step of `lane` with the database, and then its scratch is
    /// lent to the lane's compaction thread if the step left a job in
    /// its merge.
    fn background_step(
        &mut self,
        db: &mut KvDb<FileDevice>,
        lane: usize,
    ) -> Result<Progress, DbError> {
        let p = self.step(db, lane)?;
        if self.scratch[lane].as_ref().is_some_and(|s| s.wants_merge())
            && let Some(scratch) = self.scratch[lane].take()
        {
            // The merge reads its target tables by the manifest as this
            // step left it.
            self.handoff[lane].lend(scratch, Arc::new(*db.manifest()));
        }
        Ok(p)
    }

    /// One step with the database for every lane that has one to take
    /// now. Returns true if any was taken. One each and no more: this
    /// runs at the end of a writer's turn, and whatever a step leaves
    /// to do is the next turn's or a compaction thread's.
    fn background(&mut self, db: &mut KvDb<FileDevice>) -> Result<bool, DbError> {
        let mut stepped = false;
        for lane in 0..COMPACTION_LANES {
            if self.ready(lane) && !self.is_idle(db, lane) {
                self.background_step(db, lane)?;
                stepped = true;
            }
        }
        Ok(stepped)
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

        fn options() -> Options {
            Options {
                create_blocks: KvDb::<FileDevice>::MIN_DEVICE_BLOCKS + 64,
                sync: false,
            }
        }

        fn open_at(path: &Path) -> Store {
            Store::open(path, Self::options()).expect("open").0
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
            remove_store(&self.path);
        }
    }

    /// Deletes a test store and its lock file. Only for tests: in a
    /// test, no other opener can use the lock file (ADR-0032).
    fn remove_store(path: &Path) {
        let _ = Store::destroy(path);
        if let Ok(lock) = crate::device::lock_path(path) {
            let _ = std::fs::remove_file(lock);
        }
    }

    /// A path in the temporary directory, for a test that does not use
    /// [`TempStore`].
    fn temp_path(name: &str) -> std::path::PathBuf {
        let path =
            std::env::temp_dir().join(format!("horton-kvstore-{}-{name}.db", std::process::id()));
        remove_store(&path);
        path
    }

    /// The text of a [`StoreError::Locked`], or a panic.
    fn locked(e: Option<StoreError>) -> String {
        match e {
            Some(StoreError::Locked(e)) => {
                assert!(e.contains("open already"), "{e}");
                e
            }
            Some(e) => panic!("wrong error: {e}"),
            None => panic!("the call succeeded on a locked store"),
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
    fn an_oversized_batch_does_not_fail_the_writes_grouped_with_it() {
        let store = TempStore::new("oversized-neighbour");
        // Review finding: one delete of a one-byte key more than the
        // memtable has slots (8,193) fits 13 16 KiB WAL blocks, but not
        // the memtable, which horton
        // refuses (BatchTooLarge). `Batch::check` let it through on blocks
        // alone, and its refusal failed every write grouped with it. Now
        // it is refused before it queues.
        let mut big = Batch::new();
        for _ in 0..=crate::MEMTABLE_ENTRIES {
            big.delete(b"a");
        }
        let shared = Arc::clone(&store.handle.shared);
        // With the engine held, the batch is refused without queueing
        // (nothing could commit it otherwise), and a write queued after
        // it commits once the engine is free.
        let held = shared.engine().expect("engine");
        let refused = store.handle().write(big);
        assert!(
            matches!(refused, Err(StoreError::Db(Error::BatchTooLarge { .. }))),
            "{refused:?}"
        );
        assert_eq!(shared.queued.load(Ordering::Relaxed), 0, "it never queued");
        std::thread::scope(|s| {
            let h = store.handle();
            let b = s.spawn(move || h.put(b"innocent", b"v"));
            while shared.queued.load(Ordering::Relaxed) < 1 {
                std::thread::yield_now();
            }
            drop(held);
            assert_eq!(b.join().expect("put"), Ok(()));
        });
        assert_eq!(store.get(b"innocent"), Ok(Some(b"v".to_vec())));
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
        // Three maximal records to a 16 KiB WAL block: 64 of them need 22
        // blocks, more than one commit writes.
        let mut big = Batch::new();
        for i in 0..64 {
            big.put(&key(i), &[b'v'; VAL_MAX]);
        }
        assert!(matches!(
            store.write(big),
            Err(StoreError::Db(Error::BatchTooLarge { .. }))
        ));
        // Nothing of the refused batch landed, and the store still works.
        assert_eq!(store.get(&key(0)), Ok(None));
        // Twenty of them span seven blocks: one atomic batch (ADR-0029).
        let mut spanning = Batch::new();
        for i in 100..120 {
            spanning.put(&key(i), &[b'w'; VAL_MAX]);
        }
        store
            .write(spanning)
            .expect("a batch over several WAL blocks");
        assert_eq!(store.get(&key(119)), Ok(Some(vec![b'w'; VAL_MAX])));
        store.put(b"k", &[b'v'; VAL_MAX]).expect("a maximal value");
        assert_eq!(store.get(b"k"), Ok(Some(vec![b'v'; VAL_MAX])));
    }

    #[test]
    fn concurrent_writers_fill_flush_compact_and_reopen() {
        let mut store = TempStore::new("load");
        let (threads, per) = (4, 2500);
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
            "10,000 writes overflow an 8,192-entry memtable"
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
    /// cleanly, and the file is free to be opened again at once: a
    /// `Handle` holds no lock (ADR-0032).
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

    /// Issue #44: one opener at a time. A second open of an open store
    /// fails, and the first store keeps all its writes.
    #[test]
    fn a_second_open_of_an_open_store_is_refused() {
        let store = TempStore::new("open-twice");
        store.put(b"a", b"1").expect("put");
        locked(Store::open(&store.path, TempStore::options()).err());
        store.put(b"b", b"2").expect("put");
        assert_eq!(store.get(b"a").expect("get"), Some(b"1".to_vec()));
        assert_eq!(store.get(b"b").expect("get"), Some(b"2".to_vec()));
    }

    /// An open that is refused does not set the size of a new file: a
    /// second opener cannot change the shape of a store that the first
    /// opener creates.
    #[test]
    fn a_refused_open_does_not_size_a_new_file() {
        let path = temp_path("refused-new");
        let held = StoreLock::acquire(&path).expect("lock");
        let second = Store::open(&path, TempStore::options()).err();
        let len = std::fs::metadata(&path).map(|m| m.len());
        drop(held);
        remove_store(&path);
        locked(second);
        assert_eq!(len.expect("metadata"), 0);
    }

    /// A symbolic link to an open store is the same store: its open
    /// fails too. After the close, the link opens the store.
    #[cfg(unix)]
    #[test]
    fn a_link_to_an_open_store_is_refused() {
        let mut store = TempStore::new("open-via-link");
        store.put(b"k", b"v").expect("put");
        let link = store.path.with_extension("link");
        let _ = std::fs::remove_file(&link);
        std::os::unix::fs::symlink(&store.path, &link).expect("symlink");
        let second = Store::open(&link, TempStore::options()).err();
        drop(store.store.take());
        let reopened = Store::open(&link, TempStore::options())
            .map(|(s, _)| s.get(b"k"))
            .map_err(|e| e.to_string());
        let _ = std::fs::remove_file(&link);
        locked(second);
        assert_eq!(reopened, Ok(Ok(Some(b"v".to_vec()))));
    }

    /// An open that fails after it takes the lock releases the lock.
    #[test]
    fn a_failed_open_releases_the_lock() {
        let path = temp_path("too-small");
        std::fs::write(&path, vec![0; BLOCK]).expect("write");
        let opened = Store::open(&path, TempStore::options()).err();
        let relocked = StoreLock::acquire(&path).map(drop).map_err(|e| e.kind());
        remove_store(&path);
        assert!(
            matches!(opened, Some(StoreError::TooSmall { existing: true, .. })),
            "{opened:?}"
        );
        assert_eq!(relocked, Ok(()));
    }

    /// Issue #46, item 1: an open with too few blocks for a new file
    /// does not size the file. The next open with enough blocks makes
    /// the store.
    #[test]
    fn a_new_store_too_small_for_the_shape_is_not_sized() {
        let path = temp_path("new-too-small");
        let min = KvDb::<FileDevice>::MIN_DEVICE_BLOCKS;
        let small = Options {
            create_blocks: min - 1,
            sync: false,
        };
        let refused = Store::open(&path, small).err();
        let len = std::fs::metadata(&path).map_or(0, |m| m.len());
        let opened = Store::open(&path, TempStore::options()).map(drop);
        remove_store(&path);
        let Some(e @ StoreError::TooSmall { .. }) = refused else {
            panic!("wrong result: {refused:?}");
        };
        assert_eq!(
            e,
            StoreError::TooSmall {
                path,
                blocks: min - 1,
                min,
                existing: false,
            }
        );
        assert!(e.to_string().contains("not sized"), "{e}");
        assert_eq!(len, 0, "the refused open sized the file");
        assert_eq!(opened, Ok(()));
    }

    /// A new store too large to make is an error, not an overflow, and
    /// the file is not sized.
    #[test]
    fn a_new_store_too_large_to_make_is_an_error() {
        let path = temp_path("too-large");
        let huge = Options {
            create_blocks: u64::MAX,
            sync: false,
        };
        let refused = Store::open(&path, huge).err();
        let len = std::fs::metadata(&path).map_or(0, |m| m.len());
        remove_store(&path);
        let Some(StoreError::Open(e)) = refused else {
            panic!("wrong result: {refused:?}");
        };
        assert!(e.contains("is too large"), "{e}");
        assert_eq!(len, 0, "the refused open sized the file");
    }

    /// A file that is not a whole number of blocks is not a store. The
    /// error says so, and does not tell the user to delete the file.
    #[test]
    fn a_file_of_part_blocks_is_not_a_store() {
        let path = temp_path("part-block");
        std::fs::write(&path, [0; 100]).expect("write");
        let refused = Store::open(&path, TempStore::options()).err();
        remove_store(&path);
        let Some(StoreError::Open(e)) = refused else {
            panic!("wrong result: {refused:?}");
        };
        assert!(e.contains("not a store"), "{e}");
    }

    /// The minimum size opens, for a new file and for an existing one.
    #[test]
    fn a_store_of_exactly_the_minimum_size_opens() {
        let min = KvDb::<FileDevice>::MIN_DEVICE_BLOCKS;
        let exact = Options {
            create_blocks: min,
            sync: false,
        };
        let path = temp_path("exact-min");
        let new = Store::open(&path, exact).map(drop);
        let len = std::fs::metadata(&path).map(|m| m.len());
        remove_store(&path);
        std::fs::File::create(&path)
            .and_then(|f| f.set_len(min * BLOCK as u64))
            .expect("create");
        let existing = Store::open(&path, TempStore::options()).map(drop);
        remove_store(&path);
        assert_eq!(new, Ok(()));
        assert_eq!(len.ok(), Some(min * BLOCK as u64));
        assert_eq!(existing, Ok(()));
    }

    /// Issue #46, item 1: an existing file that is too small gives an
    /// error that names the remedy (ADR-0013), and the file stays.
    #[test]
    fn an_existing_file_too_small_names_the_remedy() {
        let path = temp_path("old-too-small");
        let blocks = 512;
        std::fs::File::create(&path)
            .and_then(|f| f.set_len(blocks * BLOCK as u64))
            .expect("create");
        let refused = Store::open(&path, TempStore::options()).err();
        let len = std::fs::metadata(&path).map(|m| m.len());
        remove_store(&path);
        let min = KvDb::<FileDevice>::MIN_DEVICE_BLOCKS;
        let Some(e) = refused else {
            panic!("the open succeeded");
        };
        assert_eq!(
            e,
            StoreError::TooSmall {
                path,
                blocks,
                min,
                existing: true,
            }
        );
        let text = e.to_string();
        assert!(text.contains("delete the file"), "{text}");
        assert!(text.contains(&format!("{} MiB", mib_for(min))), "{text}");
        assert_eq!(len.ok(), Some(blocks * BLOCK as u64), "the file changed");
    }

    /// Issue #46, item 3: a range that starts after it ends is an error,
    /// as in `RocksDB`'s `DeleteRange`. An empty range is not.
    #[test]
    fn inverted_ranges_are_refused() {
        let store = TempStore::new("inverted");
        for k in [b"a", b"m", b"z"] {
            store.put(k, b"v").expect("put");
        }
        assert_eq!(
            store.delete_range(b"z", b"a"),
            Err(StoreError::InvertedRange)
        );
        assert_eq!(
            store.scan(b"z", Some(b"a"), false, 10, None),
            Err(StoreError::InvertedRange)
        );
        assert_eq!(
            store.scan(b"z", Some(b"a"), true, 10, None),
            Err(StoreError::InvertedRange)
        );
        assert!(matches!(
            store.range(b"z", Some(b"a"), false),
            Err(StoreError::InvertedRange)
        ));
        // Empty ranges are not errors.
        assert_eq!(store.delete_range(b"m", b"m"), Ok(()));
        assert_eq!(
            store.scan(b"m", Some(b"m"), false, 10, None),
            Ok(Vec::new())
        );
        assert!(keys(&store, b"m", Some(b"m"), true).is_empty());
        // Nothing was deleted.
        assert_eq!(keys(&store, b"", None, false), [b"a", b"m", b"z"]);
    }

    /// `destroy` deletes the store and keeps the lock file. It refuses
    /// an open store, and a store that is not there is not an error.
    #[test]
    fn destroy_deletes_a_closed_store_only() {
        let mut store = TempStore::new("destroy");
        let path = store.path.clone();
        let lock = crate::device::lock_path(&path).expect("lock path");
        let refused = Store::destroy(&path).err();
        let kept = path.exists();
        drop(store.store.take());
        let destroyed = Store::destroy(&path);
        let (gone, lock_kept) = (!path.exists(), lock.exists());
        let again = Store::destroy(&path);
        locked(refused);
        assert!(kept, "destroy deleted an open store");
        assert_eq!(destroyed, Ok(()));
        assert!(gone, "destroy left the store");
        assert!(lock_kept, "destroy deleted the lock file");
        assert_eq!(again, Ok(()));
    }

    /// A path with no file name has no lock file.
    #[test]
    fn a_path_with_no_file_name_has_no_lock_path() {
        let e = crate::device::lock_path(Path::new("/")).map_err(|e| e.kind());
        assert_eq!(e, Err(std::io::ErrorKind::InvalidInput));
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
    ///
    /// Five minutes in an unoptimized build. There the heaviest of these
    /// tests takes most of a minute on its own (every CRC and every
    /// block's compression runs unoptimized), and beside the rest of
    /// `cargo test` it ran past one: a slow machine read as a deadlock.
    /// A deadlock never finishes, so the longer bound still catches it.
    fn within_a_minute(work: impl FnOnce() + Send + 'static) {
        let limit = if cfg!(debug_assertions) { 300 } else { 60 };
        let (done, finished) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            work();
            let _ = done.send(());
        });
        match finished.recv_timeout(Duration::from_secs(limit)) {
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
            stop_compaction_threads(shared);
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
            stop_compaction_threads(shared);
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

    /// A value like [`stamped`], padded with letters that do not
    /// compress: tables of these have blocks for a merge to write.
    fn noisy(k: usize, version: u64) -> Vec<u8> {
        let mut v = format!("{k}:{version}:").into_bytes();
        let mut x = (k as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ version;
        while v.len() < 700 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            v.push(b'a' + u8::try_from(x % 26).expect("a letter"));
        }
        v
    }

    /// Arms the hook that holds the compaction thread at its next merge,
    /// scratch in hand. Returns the channel it reports in on and the one
    /// that lets it go.
    fn hold_next_merge(
        shared: &Shared,
    ) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
        let (entered_tx, entered) = std::sync::mpsc::channel();
        let (release, release_rx) = std::sync::mpsc::channel();
        *lock(&shared.hold_next_merge) = Some((entered_tx, release_rx));
        (entered, release)
    }

    fn job_pending(shared: &Shared) -> bool {
        lock(&shared.engine)
            .as_ref()
            .is_some_and(Engine::background_pending)
    }

    /// Stops the compaction threads and waits until they have gone: from
    /// then on a scratch that is lent is the engine's again at once, and
    /// a job moves only when a test's own thread steps it. (A thread
    /// that is merely on its way out still counts as merging.)
    fn stop_compaction_threads(shared: &Shared) {
        shared.stop.store(true, Ordering::Release);
        for handoff in &shared.handoff {
            handoff.wake();
        }
        eventually("the compaction threads went", || {
            shared.handoff.iter().all(|h| h.has_left())
        });
    }

    /// Flushes tables of fresh keys until level 0 wants compacting: the
    /// flush that fills it ends its turn by starting the job and lending
    /// the scratch. Returns how many keys there are, `0..n`.
    fn fill_level_zero(store: &TempStore) -> usize {
        // Asked of the database, not of the engine: a job that has
        // already failed (a test's doing) is no longer the engine's
        // idea of pending, and level 0 is as full as it was.
        let wanted = || {
            lock(&store.handle.shared.engine)
                .as_ref()
                .is_some_and(|e| e.db.compaction_pending())
        };
        let mut n = 0;
        while !wanted() {
            for k in n..n + 40 {
                store.put(&key(k), &noisy(k, 1)).expect("put");
            }
            n += 40;
            store.flush().expect("flush");
            assert!(n < 800, "no compaction ever became pending");
        }
        n
    }

    /// Polls `what` until it holds, for up to half a minute.
    fn eventually(what: &str, holds: impl Fn() -> bool) {
        let since = Instant::now();
        while !holds() {
            assert!(since.elapsed() < Duration::from_secs(30), "never: {what}");
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    /// The point of the handoff: a compaction job's merge runs on the
    /// compaction thread with no lock of the store's. The test holds the
    /// engine lock from before the first quantum until the merge has
    /// run to where it needs the database.
    #[test]
    fn the_merge_runs_while_the_engine_lock_is_held() {
        within_a_minute(|| {
            let store = TempStore::new("merge-unlocked");
            let shared = &store.handle.shared;
            let (entered, release) = hold_next_merge(shared);
            let n = fill_level_zero(&store);
            entered
                .recv_timeout(Duration::from_secs(30))
                .expect("the compaction thread never picked the merge up");
            assert_eq!(shared.detached_steps.load(Ordering::Relaxed), 0);
            {
                let _engine = lock(&shared.engine);
                release.send(()).expect("release");
                eventually("the merge came back", || {
                    shared.handoff.iter().any(|h| h.is_back())
                });
                // It wrote the output's blocks, every one of them here.
                assert!(shared.detached_steps.load(Ordering::Relaxed) >= 3);
            }
            // The step that seals and commits is anybody's with the
            // lock; then nothing is pending and every key is there.
            eventually("the job was committed", || !job_pending(shared));
            for k in 0..n {
                let v = store.get(&key(k)).expect("get").expect("present");
                assert_eq!(v, noisy(k, 1));
            }
            let stats = store.stats().expect("stats");
            assert!(stats.detached_steps >= 3);
            assert!(stats.compaction_steps > stats.detached_steps);
            assert!(stats.compaction_jobs >= 1);
        });
    }

    /// A write that has to wait for compaction waits without the engine
    /// lock. The compaction thread is held at its merge, a writer writes
    /// until level 0 is full again and its write can go no further, and
    /// then: the engine lock is free, reads that need it are answered,
    /// and the write is still waiting. Let go, the merge runs and the
    /// writer writes on.
    #[test]
    fn a_write_waiting_for_compaction_holds_no_lock() {
        within_a_minute(|| {
            let store = TempStore::new("stall-unlocked");
            let shared = &store.handle.shared;
            let (entered, release) = hold_next_merge(shared);
            let written = AtomicUsize::new(0);
            let stop = AtomicBool::new(false);
            std::thread::scope(|s| {
                let h = store.handle();
                let (written, stop) = (&written, &stop);
                let writer = s.spawn(move || {
                    while !stop.load(Ordering::SeqCst) {
                        let k = written.load(Ordering::SeqCst);
                        h.put(&key(k), &noisy(k, 1)).expect("put");
                        written.store(k + 1, Ordering::SeqCst);
                    }
                });
                entered
                    .recv_timeout(Duration::from_secs(30))
                    .expect("the compaction thread never picked a merge up");
                eventually("a write is waiting for room", || {
                    lock(&shared.engine)
                        .as_ref()
                        .is_some_and(|e| e.stalled.is_some())
                });
                let stuck_at = written.load(Ordering::SeqCst);
                assert!(stuck_at > 0);
                // Reads that take the engine lock (a thread's first read
                // of a store does) are answered while the write waits.
                let before = shared.gate.reads();
                for k in 0..20 {
                    let h = store.handle();
                    let v = s.spawn(move || h.get(&key(k))).join().expect("reader");
                    assert_eq!(v.expect("get"), Some(noisy(k, 1)));
                }
                assert_eq!(shared.gate.reads().1, before.1 + 20);
                assert_eq!(
                    written.load(Ordering::SeqCst),
                    stuck_at,
                    "a write got through a full level 0"
                );
                assert!(
                    lock(&shared.engine)
                        .as_ref()
                        .is_some_and(|e| e.stalled.is_some())
                );
                // Compaction goes on, and so does the writer.
                release.send(()).expect("release");
                eventually("the writer wrote on", || {
                    written.load(Ordering::SeqCst) > stuck_at + 2000
                });
                stop.store(true, Ordering::SeqCst);
                writer.join().expect("writer");
            });
            for k in 0..written.load(Ordering::SeqCst) {
                let v = store.get(&key(k)).expect("get").expect("present");
                assert_eq!(v, noisy(k, 1), "key {k}");
            }
        });
    }

    /// The engine can have the scratch back in the middle of a merge:
    /// `compact` ends the job in flight, so it recalls the scratch
    /// first and waits for the compaction thread to let go of it.
    #[test]
    fn compact_recalls_a_merge_in_flight() {
        within_a_minute(|| {
            let store = TempStore::new("recall");
            let shared = &store.handle.shared;
            let (entered, release) = hold_next_merge(shared);
            let n = fill_level_zero(&store);
            entered
                .recv_timeout(Duration::from_secs(30))
                .expect("the compaction thread never picked the merge up");
            let compacted = AtomicBool::new(false);
            std::thread::scope(|s| {
                let compacting = s.spawn(|| {
                    store.compact().expect("compact");
                    compacted.store(true, Ordering::SeqCst);
                });
                eventually("the scratch was recalled", || {
                    shared.handoff.iter().any(|h| h.recalled())
                });
                std::thread::sleep(Duration::from_millis(100));
                assert!(
                    !compacted.load(Ordering::SeqCst),
                    "compact went ahead while a merge held the scratch"
                );
                release.send(()).expect("release");
                compacting.join().expect("compact");
            });
            assert!(compacted.load(Ordering::SeqCst));
            assert!(!job_pending(shared));
            for k in 0..n {
                let v = store.get(&key(k)).expect("get").expect("present");
                assert_eq!(v, noisy(k, 1));
            }
            let stats = store.stats().expect("stats");
            assert_eq!(stats.level_tables[0], 0, "compact left tables in level 0");
        });
    }

    /// A merge that fails loses its job and nothing else. The store
    /// stops compacting in the background until the next flush (the
    /// write path, which has a caller to tell, takes over), and the job
    /// is done over.
    #[test]
    fn a_failed_merge_gives_its_job_up_and_the_store_goes_on() {
        within_a_minute(|| {
            let store = TempStore::new("merge-failed");
            let shared = &store.handle.shared;
            shared.fail_next_merge.store(true, Ordering::SeqCst);
            let n = fill_level_zero(&store);
            // Stuck: until the store has flushed again.
            let stuck = || {
                lock(&shared.engine)
                    .as_ref()
                    .is_some_and(|e| e.compaction_stuck == Some(e.room.flushes))
            };
            eventually("the failure reached the engine", stuck);
            assert!(!shared.fail_next_merge.load(Ordering::SeqCst));
            assert_eq!(shared.detached_steps.load(Ordering::Relaxed), 0);
            // The job was given up: nothing of it is reserved, and the
            // tables are the ones the flushes left.
            let stats = store.stats().expect("stats");
            assert_eq!(stats.slots.reserved, 0);
            assert_eq!(stats.compaction_jobs, 1, "the step that ended the job");
            for k in 0..n {
                assert_eq!(store.get(&key(k)).expect("get"), Some(noisy(k, 1)));
            }
            // Writes go on, the job is done over, and nothing is lost.
            for k in n..n + 3000 {
                store.put(&key(k), &noisy(k, 1)).expect("put");
            }
            store.compact().expect("compact");
            assert!(!stuck(), "a flush did not clear the failure");
            for k in 0..n + 3000 {
                assert_eq!(store.get(&key(k)).expect("get"), Some(noisy(k, 1)));
            }
            assert!(shared.detached_steps.load(Ordering::Relaxed) > 0);
        });
    }

    /// Closing waits for the compaction thread, which stops after the
    /// quantum it is in and gives the scratch back. The job it left is
    /// blocks nothing points at: the store reopens with every write.
    #[test]
    fn closing_stops_a_merge_and_loses_nothing() {
        within_a_minute(|| {
            let mut store = TempStore::new("close-merging");
            let (entered, release) = hold_next_merge(&store.handle.shared);
            let n = fill_level_zero(&store);
            entered
                .recv_timeout(Duration::from_secs(30))
                .expect("the compaction thread never picked the merge up");
            let closed = AtomicBool::new(false);
            std::thread::scope(|s| {
                let (store, closed) = (&mut store, &closed);
                let closing = s.spawn(move || {
                    store.reopen();
                    closed.store(true, Ordering::SeqCst);
                });
                std::thread::sleep(Duration::from_millis(200));
                assert!(
                    !closed.load(Ordering::SeqCst),
                    "the store closed with its compaction thread still merging"
                );
                release.send(()).expect("release");
                closing.join().expect("close");
            });
            for k in 0..n {
                assert_eq!(store.get(&key(k)).expect("get"), Some(noisy(k, 1)));
            }
            store.compact().expect("compact");
            for k in 0..n {
                assert_eq!(store.get(&key(k)).expect("get"), Some(noisy(k, 1)));
            }
        });
    }

    /// A call that is not queued sees its compaction job through with
    /// the engine lock in hand, and while every lane is merging or has
    /// nothing to do it sleeps: it does not go round and round the
    /// lanes. Here a flush waits for a job whose merge is held, and the
    /// other lane has found nothing to run beside it.
    #[test]
    fn a_call_waiting_for_a_merge_sleeps() {
        within_a_minute(|| {
            let store = TempStore::new("job-sleeps");
            let shared = &store.handle.shared;
            let (entered, release) = hold_next_merge(shared);
            let n = fill_level_zero(&store);
            entered
                .recv_timeout(Duration::from_secs(30))
                .expect("the compaction thread never picked the merge up");
            // Level 0 is full: the flush needs the job to end first.
            store.put(&key(n), &noisy(n, 1)).expect("put");
            let flushed = AtomicBool::new(false);
            std::thread::scope(|s| {
                let flushing = s.spawn(|| {
                    store.flush().expect("flush");
                    flushed.store(true, Ordering::SeqCst);
                });
                eventually("the flush is waiting", || {
                    shared.engine_held.load(Ordering::Relaxed)
                });
                let before = JOB_PASSES.load(Ordering::Relaxed);
                std::thread::sleep(Duration::from_millis(300));
                let passes = JOB_PASSES.load(Ordering::Relaxed) - before;
                assert!(!flushed.load(Ordering::SeqCst), "the flush did not wait");
                // One pass each `STALL_PAUSE` at most (other tests'
                // stores count too); a loop that did not sleep would
                // make millions.
                assert!(passes < 20_000, "{passes} passes in 300 ms");
                release.send(()).expect("release");
                flushing.join().expect("flush");
            });
            for k in 0..=n {
                assert_eq!(store.get(&key(k)).expect("get"), Some(noisy(k, 1)));
            }
        });
    }

    /// `compact` ends the jobs in flight (a request does), so every
    /// lane's scratch comes home first: a merge left running would go
    /// on writing into a slot the database has taken back. And what the
    /// lanes last found is forgotten, since the request changes it.
    #[test]
    fn compact_brings_every_scratch_home() {
        within_a_minute(|| {
            let store = TempStore::new("home-all");
            let shared = &store.handle.shared;
            stop_compaction_threads(shared);
            let mut guard = lock(&shared.engine);
            let engine = guard.as_mut().expect("open");
            for lane in 0..COMPACTION_LANES {
                let scratch = engine.room.scratch[lane].take().expect("home");
                engine.room.handoff[lane].lend(scratch, Arc::new(*engine.db.manifest()));
            }
            engine.room.idle_at = [Some((0, None)); COMPACTION_LANES];
            engine.room.home_all().expect("home");
            let home = engine.room.scratch.iter().all(Option::is_some);
            let idle_at = engine.room.idle_at;
            drop(guard);
            assert!(home, "a scratch was left with its compaction thread");
            assert_eq!(idle_at, [None; COMPACTION_LANES]);
        });
    }

    /// A lane that looked for a job and found none is not asked again
    /// until what it goes by has changed (the tables, or the level the
    /// other lane's job writes to): asking shuts the readers' gate, and
    /// horton would only say the same. Here one lane's job is held in
    /// its merge, and writes that flush nothing end their turns without
    /// a step.
    #[test]
    fn an_idle_lane_is_not_stepped_until_something_changes() {
        within_a_minute(|| {
            let store = TempStore::new("idle-lane");
            let shared = &store.handle.shared;
            let (entered, release) = hold_next_merge(shared);
            let n = fill_level_zero(&store);
            entered
                .recv_timeout(Duration::from_secs(30))
                .expect("the compaction thread never picked the merge up");
            let steps = || {
                lock(&shared.engine)
                    .as_ref()
                    .map(|e| e.room.steps)
                    .expect("open")
            };
            // The step that lent the one lane's scratch asked the other
            // lane too, and it found nothing beside that job.
            let before = steps();
            for k in n..n + 30 {
                store.put(&key(k), &noisy(k, 1)).expect("put");
            }
            assert!(job_pending(shared));
            assert_eq!(steps(), before, "an idle lane was stepped again");
            release.send(()).expect("release");
            eventually("the job was committed", || !job_pending(shared));
            assert!(steps() > before);
            for k in 0..n + 30 {
                assert_eq!(store.get(&key(k)).expect("get"), Some(noisy(k, 1)));
            }
        });
    }

    /// Two compaction jobs at a time (ADR-0025): each lane has a scratch,
    /// a hand-off and a thread, and when horton finds two jobs that can
    /// run side by side, two merges run at once. The smallest store is
    /// filled to where its bottom level is most of it, and overwritten
    /// at random until merges have overlapped a number of times; then
    /// every key reads as it was last written, also after a full
    /// compaction, and after a reopen.
    ///
    /// Release builds only: the store has to be nearly full before a
    /// second job has anything to do, and a debug build takes too long
    /// to fill it.
    #[cfg(not(debug_assertions))]
    #[test]
    fn two_merges_run_at_once_and_change_no_read() {
        within_a_minute(|| {
            const KEYS: usize = 60_000;
            let mut store = TempStore::new("two-lanes");
            let mut version = vec![1u64; KEYS];
            for k in 0..KEYS {
                store.put(&key(k), &noisy(k, 1)).expect("put");
            }
            let overlaps = |store: &TempStore| {
                let shared = &store.handle.shared;
                shared.merges_side_by_side.load(Ordering::Relaxed)
            };
            let mut x = 0x2545_F491_4F6C_DD1Du64;
            let mut round = 1u64;
            while overlaps(&store) < 8 {
                round += 1;
                for _ in 0..400 {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    let k = usize::try_from(x % KEYS as u64).expect("a key");
                    store.put(&key(k), &noisy(k, round)).expect("put");
                    version[k] = round;
                }
                assert!(round < 600, "two merges never ran at once");
            }
            let check = |store: &TempStore| {
                for (k, v) in version.iter().enumerate() {
                    let got = store.get(&key(k)).expect("get");
                    assert_eq!(got, Some(noisy(k, *v)), "key {k}");
                }
            };
            check(&store);
            let stats = store.stats().expect("stats");
            assert!(stats.merges_side_by_side >= 8);
            assert!(stats.detached_steps > 0);
            store.compact().expect("compact");
            check(&store);
            store.reopen();
            check(&store);
        });
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
