//! A `LevelDB`-shaped store over horton: many threads, one database.
//!
//! horton's [`Db`](horton::Db) is a single-owner value with `&mut self`
//! writes and no locks, the right shape for firmware. A host program
//! wants what `LevelDB` gives it instead: a handle any thread can clone,
//! writes from many threads at once, and compaction that happens by
//! itself. This module provides that with one **store thread** that owns
//! the `Db`:
//!
//! ```text
//!  client threads ──Request──▶ channel ──▶ store thread ──▶ Db ──▶ FileDevice
//!        ▲                                     │
//!        └──────────── reply ◀─────────────────┘
//! ```
//!
//! - **Group commit.** The store thread takes a write off the channel,
//!   then every write queued behind it that still fits one WAL block, and
//!   commits them all as one [`WriteBatch`]: one block write and one
//!   `fdatasync` for the whole group. This is `LevelDB`'s writer queue.
//!   Each client's batch stays atomic (the group is all or nothing).
//! - **Capacity errors are handled here.** horton reports a full memtable,
//!   WAL or level 0 as an error naming its remedy; the store thread
//!   applies the remedy (`flush`, `compact_step`) and retries, so callers
//!   never see them.
//! - **Background compaction.** While a job is pending, the store thread
//!   runs one bounded `compact_step` between requests, and runs them back
//!   to back when the channel is idle. Writers are never stalled for a
//!   whole compaction, which is what `LevelDB`'s L0 slowdown trigger
//!   approximates with sleeps.
//! - **Snapshots and iterators.** A [`Snapshot`] is horton's sequence
//!   watermark, released when dropped. An [`Iter`] pins one for its
//!   lifetime, like a `LevelDB` iterator, and pages through the range.
//!
//! Every reply is sent after the operation completes, so a write that
//! returned `Ok` is on the disk (or, without `sync`, in the operating
//! system's hands).

use std::path::Path;
use std::sync::mpsc::{self, Receiver, Sender, SyncSender, TryRecvError};
use std::thread::JoinHandle;

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

/// Why a store call failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreError {
    /// horton refused the operation or the device failed.
    Db(DbError),
    /// The store could not be opened.
    Open(String),
    /// The store thread has stopped (it hit an error it cannot recover
    /// from, or the store was closed).
    Closed,
}

impl core::fmt::Display for StoreError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Db(e) => write!(f, "{e:?}"),
            Self::Open(e) => write!(f, "open: {e}"),
            Self::Closed => write!(f, "the store thread has stopped"),
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

/// What the store thread has done, and the state of the database.
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

type Reply<T> = SyncSender<Result<T, StoreError>>;

/// Key-value pairs, in scan order.
pub type Entries = Vec<(Vec<u8>, Vec<u8>)>;

/// One page of a scan: the entries, and whether the range is exhausted.
type Page = (Entries, bool);

enum Request {
    Write(Batch, Reply<u64>),
    DeleteRange(Vec<u8>, Vec<u8>, Reply<u64>),
    Get(Vec<u8>, u64, Reply<Option<Vec<u8>>>),
    Scan {
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        reverse: bool,
        limit: usize,
        seq: u64,
        reply: Reply<Page>,
    },
    Snapshot(Reply<u64>),
    Release(u64),
    Flush(Reply<()>),
    CompactAll(Reply<()>),
    Stats(Reply<Stats>),
}

/// A cloneable, `Send` handle to an open store. Every call blocks the
/// calling thread until the store thread has done it.
#[derive(Clone)]
pub struct Handle {
    tx: Sender<Request>,
}

/// An open store: a [`Handle`] plus the store thread, stopped and joined
/// on drop (after every clone of the handle is gone).
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
        // Swap in a dead sender so ours drops: once the last clone is
        // gone the store thread's `recv` fails and it exits.
        let (dead, _) = mpsc::channel();
        drop(core::mem::replace(&mut self.handle.tx, dead));
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

impl Store {
    /// Opens (or creates) the store in the file at `path`, recovering it
    /// if the last process died mid-write.
    pub fn open(path: &Path, options: Options) -> Result<(Self, OpenReport), StoreError> {
        let device = FileDevice::open(path, options.create_blocks, options.sync)
            .map_err(|e| StoreError::Open(format!("{}: {e}", path.display())))?;
        let (tx, rx) = mpsc::channel();
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let thread = std::thread::Builder::new()
            .name("horton-store".into())
            // The database and the futures that drive it live on this
            // thread's stack while they are built: give it room.
            .stack_size(64 << 20)
            .spawn(move || {
                let mut engine = match Engine::open(device) {
                    Ok((engine, report)) => {
                        let _ = ready_tx.send(Ok(report));
                        engine
                    }
                    Err(e) => {
                        let _ = ready_tx.send(Err(e));
                        return;
                    }
                };
                engine.run(&rx);
            })
            .map_err(|e| StoreError::Open(e.to_string()))?;
        let report = ready_rx.recv().map_err(|_| StoreError::Closed)??;
        Ok((
            Self {
                handle: Handle { tx },
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
    fn call<T>(&self, make: impl FnOnce(Reply<T>) -> Request) -> Result<T, StoreError> {
        let (tx, rx) = mpsc::sync_channel(1);
        self.tx.send(make(tx)).map_err(|_| StoreError::Closed)?;
        rx.recv().map_err(|_| StoreError::Closed)?
    }

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
        self.call(|r| Request::Write(batch, r)).map(drop)
    }

    /// Deletes every key in `[start, end)` with one range tombstone, like
    /// `RocksDB`'s `DeleteRange`.
    pub fn delete_range(&self, start: &[u8], end: &[u8]) -> Result<(), StoreError> {
        check_key(start)?;
        check_key(end)?;
        self.call(|r| Request::DeleteRange(start.to_vec(), end.to_vec(), r))
            .map(drop)
    }

    /// The value of `key`, or `None`.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        self.call(|r| Request::Get(key.to_vec(), u64::MAX, r))
    }

    /// The value of `key` as of `snapshot`.
    pub fn get_at(&self, key: &[u8], snapshot: &Snapshot) -> Result<Option<Vec<u8>>, StoreError> {
        self.call(|r| Request::Get(key.to_vec(), snapshot.seq, r))
    }

    /// A consistent view of the store as it is now; released on drop.
    /// horton keeps at most eight live at once (`SnapshotLimit`).
    pub fn snapshot(&self) -> Result<Snapshot, StoreError> {
        let seq = self.call(Request::Snapshot)?;
        Ok(Snapshot {
            seq,
            tx: self.tx.clone(),
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
        let (page, _) = self.call(|reply| Request::Scan {
            start: start.to_vec(),
            end: end.map(<[u8]>::to_vec),
            reverse,
            limit,
            seq: snapshot.map_or(u64::MAX, |s| s.seq),
            reply,
        })?;
        Ok(page)
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
        self.call(Request::Flush)
    }

    /// Flushes, then compacts every table once, down to the bottom level,
    /// like `LevelDB`'s `CompactRange(nullptr, nullptr)`. Deleted data
    /// gives its space back.
    pub fn compact(&self) -> Result<(), StoreError> {
        self.call(Request::CompactAll)
    }

    pub fn stats(&self) -> Result<Stats, StoreError> {
        self.call(Request::Stats)
    }
}

/// A pinned view: reads through it see the store as it was when it was
/// taken. Released when dropped.
pub struct Snapshot {
    seq: u64,
    tx: Sender<Request>,
}

impl Snapshot {
    /// horton's sequence watermark for this view.
    pub const fn seq(&self) -> u64 {
        self.seq
    }
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        let _ = self.tx.send(Request::Release(self.seq));
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
            let page = self.handle.call(|reply| Request::Scan {
                start,
                end,
                reverse,
                limit: PAGE,
                seq,
                reply,
            });
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

/// The store thread's state.
struct Engine {
    db: Box<KvDb<FileDevice>>,
    room: Room,
    batch: Box<GroupBatch>,
    /// A request taken off the channel that did not fit the last group.
    held: Option<Request>,
    /// Background compaction failed (say, `RegionFull`) when the store
    /// had flushed this many times: leave it to the write path, which
    /// reports the error to a caller, until a flush changes the tables. A
    /// failing job reads its inputs before it fails, so retrying it on
    /// every request would stall them all.
    compaction_stuck: Option<u64>,
    open: OpenReport,
    groups: u64,
    grouped_writes: u64,
}

impl Engine {
    #[allow(
        clippy::large_stack_frames,
        reason = "the database is built on the stack before it is boxed; the store thread has 64 MiB"
    )]
    fn open(device: FileDevice) -> Result<(Self, OpenReport), StoreError> {
        let blocks = device.blocks();
        let min = KvDb::<FileDevice>::MIN_DEVICE_BLOCKS;
        if blocks < min {
            return Err(StoreError::Open(format!(
                "the file holds {blocks} blocks; this shape needs at least {min}"
            )));
        }
        let mut db = Box::new(KvDb::new(device, horton::Config::whole_device(blocks)));
        let open = block_on(db.open())?;
        let engine = Self {
            db,
            room: Room {
                scratch: Box::new(KvCompaction::new()),
                flushes: 0,
                jobs: 0,
                steps: 0,
            },
            batch: Box::new(GroupBatch::new()),
            held: None,
            compaction_stuck: None,
            open,
            groups: 0,
            grouped_writes: 0,
        };
        Ok((engine, open))
    }

    fn run(&mut self, rx: &Receiver<Request>) {
        loop {
            let req = if let Some(r) = self.held.take() {
                r
            } else if self.background_pending() {
                match rx.try_recv() {
                    Ok(r) => r,
                    Err(TryRecvError::Empty) => {
                        // Idle: compact.
                        self.background_step();
                        continue;
                    }
                    Err(TryRecvError::Disconnected) => return,
                }
            } else {
                match rx.recv() {
                    Ok(r) => r,
                    Err(_) => return,
                }
            };
            self.serve(req, rx);
            // Busy: one bounded step between requests keeps a pending job
            // moving without stalling anyone for a whole job.
            if self.background_pending() {
                self.background_step();
            }
        }
    }

    fn background_pending(&self) -> bool {
        self.db.compaction_pending() && self.compaction_stuck != Some(self.room.flushes)
    }

    fn background_step(&mut self) {
        if self.room.step(&mut self.db).is_err() {
            self.compaction_stuck = Some(self.room.flushes);
        }
    }

    fn serve(&mut self, req: Request, rx: &Receiver<Request>) {
        match req {
            Request::Write(batch, reply) => self.write_group(batch, reply, rx),
            Request::DeleteRange(start, end, reply) => {
                let r = self
                    .room
                    .with_room(&mut self.db, |db| block_on(db.delete_range(&start, &end)));
                let _ = reply.send(r.map_err(StoreError::Db));
            }
            Request::Get(key, seq, reply) => {
                let mut buf = vec![0u8; VAL_MAX];
                let r = block_on(self.db.get_at(&key, &mut buf, seq)).map(|n| {
                    n.map(|n| {
                        buf.truncate(n);
                        buf
                    })
                });
                let _ = reply.send(r.map_err(StoreError::Db));
            }
            Request::Scan {
                start,
                end,
                reverse,
                limit,
                seq,
                reply,
            } => {
                let r = if reverse {
                    self.scan_rev(&start, end.as_deref(), limit, seq)
                } else {
                    self.scan_fwd(&start, end.as_deref(), limit, seq)
                };
                let _ = reply.send(r.map_err(StoreError::Db));
            }
            Request::Snapshot(reply) => {
                let _ = reply.send(self.db.snapshot().map_err(StoreError::Db));
            }
            Request::Release(seq) => self.db.release_snapshot(seq),
            Request::Flush(reply) => {
                let _ = reply.send(self.room.flush(&mut self.db).map_err(StoreError::Db));
            }
            Request::CompactAll(reply) => {
                let r = self.room.flush(&mut self.db).and_then(|()| {
                    self.db.request_compaction(0)?;
                    while self.db.compaction_pending() {
                        self.room.job(&mut self.db)?;
                    }
                    Ok(())
                });
                let _ = reply.send(r.map_err(StoreError::Db));
            }
            Request::Stats(reply) => {
                let _ = reply.send(Ok(self.stats()));
            }
        }
    }

    /// Commits `first` together with every write queued behind it that
    /// fits the same WAL block.
    fn write_group(&mut self, first: Batch, reply: Reply<u64>, rx: &Receiver<Request>) {
        let mut bytes = first.wal_bytes();
        let mut ops = first.ops.len();
        let mut group = vec![(first, reply)];
        while let Ok(next) = rx.try_recv() {
            match next {
                Request::Write(b, r)
                    if bytes + b.wal_bytes() <= BLOCK && ops + b.ops.len() <= GROUP_OPS =>
                {
                    bytes += b.wal_bytes();
                    ops += b.ops.len();
                    group.push((b, r));
                }
                other => {
                    self.held = Some(other);
                    break;
                }
            }
        }
        self.batch.clear();
        let mut result = Ok(());
        for op in group.iter().flat_map(|(b, _)| &b.ops) {
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
        let result = result.and_then(|()| {
            self.room
                .with_room(&mut self.db, |db| block_on(db.write(batch)))
        });
        self.groups += 1;
        self.grouped_writes += group.len() as u64;
        for (_, reply) in group {
            let _ = reply.send(result.map_err(StoreError::Db));
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
}
