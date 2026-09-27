//! The flight recorder's firmware loop: what a sensor node does every tick.
//!
//! Each tick (the recorder's clock) it:
//!
//! 1. commits a **frame**, the four sensor readings plus any alarm event,
//!    as one [`WriteBatch`], so a power cut never leaves half a frame;
//! 2. writes a **debug trace** with a time-to-live, which reads as gone
//!    once expired and which compaction then drops for good;
//! 3. every 1000 ticks, **purges** a window of readings a sensor glitch
//!    corrupted, with one range delete, and checks that a snapshot taken
//!    just before still sees them;
//! 4. does one bounded **compaction** step, and every 250 ticks
//!    **archives** tables older than the hot window to object storage.
//!
//! When the database needs room (a full memtable, WAL or level 0) the
//! error says what to do, and [`Recorder::make_room`] does it.

use horton::{BlockDevice, Error, TableRef, WriteBatch};

use crate::archive::{object_key, upload};
use crate::cloud::ObjectStore;
use crate::{
    EVENT_EVERY, HOT_TICKS, KEY_MAX, LEVELS, PURGE_EVERY, RecorderCompaction, RecorderDb,
    RecorderRevScan, SENSORS, TTL_TICKS, VAL_MAX, block_on, key, purge_window, tick_of, value,
};

/// Counters for the report.
#[derive(Debug, Default, Clone, Copy)]
pub struct Stats {
    pub ticks: u64,
    pub flushes: u64,
    pub compaction_steps: u64,
    pub purges: u64,
    pub snapshot_checks: u64,
    pub ttl_checks: u64,
    pub archived_tables: u64,
    pub archived_bytes: u64,
    pub archive_refusals: u64,
}

/// The recorder: its compaction scratch, its store, and what it has done.
pub struct Recorder<'s> {
    scratch: Box<RecorderCompaction>,
    store: &'s mut dyn ObjectStore,
    uploaded: std::collections::BTreeSet<String>,
    snapshot: Option<u64>,
    pub stats: Stats,
}

/// A failure that stops the recorder, with the operation that hit it.
#[derive(Debug)]
pub struct Stop(pub String);

fn stop<E: core::fmt::Debug>(what: &str) -> impl Fn(E) -> Stop + '_ {
    move |e| Stop(format!("{what}: {e:?}"))
}

impl<'s> Recorder<'s> {
    pub fn new(store: &'s mut dyn ObjectStore) -> Result<Self, Stop> {
        let uploaded = store
            .list("tables/")
            .map_err(stop("listing the archive"))?
            .into_iter()
            .collect();
        Ok(Self {
            scratch: Box::new(RecorderCompaction::new()),
            store,
            uploaded,
            snapshot: None,
            stats: Stats::default(),
        })
    }

    pub fn location(&self) -> String {
        self.store.location()
    }

    /// The store tables are archived to.
    pub fn store(&self) -> &dyn ObjectStore {
        &*self.store
    }

    /// Forgets what a reboot loses: snapshots live only in RAM.
    pub const fn rebooted(&mut self) {
        self.snapshot = None;
    }

    /// Runs ticks `from..to`, calling `each` after every tick.
    pub fn run<D: BlockDevice>(
        &mut self,
        db: &mut RecorderDb<D>,
        from: u64,
        to: u64,
        each: &mut dyn FnMut(u64, &Self, &RecorderDb<D>),
    ) -> Result<(), Stop>
    where
        D::Error: core::fmt::Debug,
    {
        for t in from..to {
            self.tick(db, t)?;
            each(t, self, db);
        }
        Ok(())
    }

    /// One tick of the firmware loop.
    pub fn tick<D: BlockDevice>(&mut self, db: &mut RecorderDb<D>, t: u64) -> Result<(), Stop>
    where
        D::Error: core::fmt::Debug,
    {
        let phase = t % PURGE_EVERY;
        // A snapshot pins the view just before the glitch purge.
        if phase == 495 && self.snapshot.is_none() {
            self.snapshot = Some(db.snapshot().map_err(stop("snapshot"))?);
        }
        if phase == 500 && t >= PURGE_EVERY / 2 {
            let (a, b) = purge_window(t / PURGE_EVERY);
            self.retry(db, t, |db| {
                block_on(db.delete_range(&key(a, 0, 0), &key(b, 0, 0)))
            })?;
            self.stats.purges += 1;
        }
        if phase == 505 {
            self.check_snapshot(db, t)?;
        }

        // The frame: every sensor and any alarm, atomically.
        let mut frame = WriteBatch::<KEY_MAX, VAL_MAX, 5>::new();
        for s in 0..SENSORS {
            let k = key(t, b'r', s);
            frame
                .put(&k, &value(&k))
                .map_err(Error::widen::<D::Error>)
                .map_err(stop("frame"))?;
        }
        if t.is_multiple_of(EVENT_EVERY) {
            let k = key(t, b'e', u8::try_from(t / EVENT_EVERY % 7).expect("below 7"));
            frame
                .put(&k, &value(&k))
                .map_err(Error::widen::<D::Error>)
                .map_err(stop("frame"))?;
        }
        self.retry(db, t, |db| block_on(db.write(&frame)))?;

        // A debug trace that expires TTL_TICKS later.
        let k = key(t, b'd', 0);
        self.retry(db, t, |db| {
            block_on(db.put_with_ttl(&k, &value(&k), t + TTL_TICKS))
        })?;
        if t.is_multiple_of(50) && t > TTL_TICKS + 1 {
            self.check_ttl(db, t)?;
        }

        // Bounded background work: one compaction step, and now and then
        // a sweep of cold tables to the archive.
        if db.compaction_pending() {
            self.scratch.purge_before = t;
            match block_on(db.compact_step(&mut self.scratch)) {
                Ok(_) => self.stats.compaction_steps += 1,
                Err(Error::RegionFull) => {
                    self.archive_cold(db, t)?;
                }
                Err(e) => return Err(Stop(format!("compact_step at tick {t}: {e:?}"))),
            }
        }
        if t % 250 == 125 {
            self.archive_cold(db, t)?;
        }
        self.stats.ticks += 1;
        Ok(())
    }

    /// Runs `op`, making room and retrying whenever the error asks for it.
    pub fn retry<D: BlockDevice, T>(
        &mut self,
        db: &mut RecorderDb<D>,
        t: u64,
        mut op: impl FnMut(&mut RecorderDb<D>) -> Result<T, Error<D::Error>>,
    ) -> Result<T, Stop>
    where
        D::Error: core::fmt::Debug,
    {
        for _ in 0..64 {
            match op(db) {
                Ok(v) => return Ok(v),
                Err(e) => self.make_room(db, t, e)?,
            }
        }
        Err(Stop(format!(
            "tick {t}: no progress after 64 attempts to make room"
        )))
    }

    /// Does what a capacity error asks: flush, compact, or archive.
    fn make_room<D: BlockDevice>(
        &mut self,
        db: &mut RecorderDb<D>,
        t: u64,
        e: Error<D::Error>,
    ) -> Result<(), Stop>
    where
        D::Error: core::fmt::Debug,
    {
        match e {
            // The memtable or the WAL is full: move the memtable to a table.
            Error::TableFull | Error::ArenaFull | Error::WalFull => match block_on(db.flush()) {
                Ok(()) => {
                    self.stats.flushes += 1;
                    Ok(())
                }
                Err(e) => self.make_room(db, t, e),
            },
            // Level 0 is full, or no slot is free until tables merge.
            Error::NeedsCompaction => self.compact_all(db, t),
            // Nothing merges into free space: move cold tables away.
            Error::RegionFull => {
                if self.archive_cold(db, t)? == 0 {
                    return Err(Stop(format!(
                        "tick {t}: region full and nothing is cold enough to archive"
                    )));
                }
                Ok(())
            }
            e => Err(Stop(format!("tick {t}: {e:?}"))),
        }
    }

    fn compact_all<D: BlockDevice>(&mut self, db: &mut RecorderDb<D>, t: u64) -> Result<(), Stop>
    where
        D::Error: core::fmt::Debug,
    {
        self.scratch.purge_before = t;
        while db.compaction_pending() {
            match block_on(db.compact_step(&mut self.scratch)) {
                Ok(_) => self.stats.compaction_steps += 1,
                Err(Error::RegionFull) => {
                    if self.archive_cold(db, t)? == 0 {
                        return Err(Stop(format!(
                            "tick {t}: region full and nothing is cold enough to archive"
                        )));
                    }
                }
                Err(e) => return Err(Stop(format!("compaction at tick {t}: {e:?}"))),
            }
        }
        Ok(())
    }

    /// Archives every table that holds only ticks older than the hot
    /// window: upload it, then commit it away. Returns how many went.
    pub fn archive_cold<D: BlockDevice>(
        &mut self,
        db: &mut RecorderDb<D>,
        t: u64,
    ) -> Result<u64, Stop>
    where
        D::Error: core::fmt::Debug,
    {
        let Some(horizon) = t.checked_sub(HOT_TICKS) else {
            return Ok(0);
        };
        let horizon = key(horizon, 0, 0);
        let mut moved = 0;
        for level in (0..LEVELS).rev() {
            let cold: Vec<TableRef<KEY_MAX>> = db
                .level_tables(level)
                .unwrap_or(&[])
                .iter()
                .filter(|t| t.last_key.as_slice() < horizon.as_slice())
                .copied()
                .collect();
            for table in cold {
                let Some(plan) = db.archive_plan(level, table.id) else {
                    continue;
                };
                // 1. Upload (skipped when a crash already did it: ids never
                //    repeat, so the object is this table).
                let name = object_key(table.id);
                if self.uploaded.insert(name.clone()) {
                    match upload(db, &plan, self.store) {
                        Ok(bytes) => self.stats.archived_bytes += bytes,
                        Err(e) => {
                            self.uploaded.remove(&name);
                            return Err(Stop(format!("upload {name}: {e}")));
                        }
                    }
                }
                // 2. Only then forget it locally.
                match block_on(db.archive_commit(level, table.id)) {
                    Ok(true) => {
                        moved += 1;
                        self.stats.archived_tables += 1;
                    }
                    Ok(false) => {}
                    // Its range tombstone still hides data here; compaction
                    // will merge it first.
                    Err(Error::WouldResurrect { .. }) => self.stats.archive_refusals += 1,
                    Err(e) => return Err(Stop(format!("archive_commit at tick {t}: {e:?}"))),
                }
            }
        }
        Ok(moved)
    }

    /// The snapshot taken before the purge still sees the purged readings;
    /// the live view does not.
    fn check_snapshot<D: BlockDevice>(&mut self, db: &mut RecorderDb<D>, t: u64) -> Result<(), Stop>
    where
        D::Error: core::fmt::Debug,
    {
        let Some(snap) = self.snapshot.take() else {
            return Ok(());
        };
        let (a, _) = purge_window(t / PURGE_EVERY);
        let k = key(a, b'r', 0);
        let mut buf = [0u8; VAL_MAX];
        // The snapshot read is clock-aware too: readings never expire, so
        // the tick changes nothing here, but debug traces would.
        let before = block_on(db.get_at_with_time(&k, &mut buf, snap, t))
            .map_err(stop("get_at_with_time"))?;
        let now = block_on(db.get(&k, &mut buf)).map_err(stop("get"))?;
        db.release_snapshot(snap);
        // A window already archived before the snapshot is not local; only
        // judge windows the snapshot saw.
        if before.is_some() && now.is_some() {
            return Err(Stop(format!(
                "tick {t}: purged reading {a} is still visible"
            )));
        }
        self.stats.snapshot_checks += u64::from(before.is_some());
        Ok(())
    }

    /// A recent debug trace reads back; an expired one reads as gone.
    fn check_ttl<D: BlockDevice>(&mut self, db: &RecorderDb<D>, t: u64) -> Result<(), Stop>
    where
        D::Error: core::fmt::Debug,
    {
        let mut buf = [0u8; VAL_MAX];
        let fresh = key(t - 10, b'd', 0);
        let stale = key(t - TTL_TICKS - 1, b'd', 0);
        let fresh_seen =
            block_on(db.get_with_time(&fresh, &mut buf, t)).map_err(stop("get_with_time"))?;
        let stale_seen =
            block_on(db.get_with_time(&stale, &mut buf, t)).map_err(stop("get_with_time"))?;
        if stale_seen.is_some() {
            return Err(Stop(format!(
                "tick {t}: debug trace {} outlived its TTL",
                t - TTL_TICKS - 1
            )));
        }
        // A fresh trace may be missing only if it sits in a purged window.
        if fresh_seen.is_some() {
            self.stats.ttl_checks += 1;
        }
        Ok(())
    }
}

/// The newest tick in the database, from a reverse scan.
pub fn latest_tick<D: BlockDevice>(db: &RecorderDb<D>) -> Result<Option<u64>, Stop>
where
    D::Error: core::fmt::Debug,
{
    let mut scan = Box::new(RecorderRevScan::new(db));
    // Every data key starts with a tick below 2^56: its first byte is 0.
    block_on(scan.seek_prev(&[0x01], None, u64::MAX)).map_err(stop("seek_prev"))?;
    let mut k = [0u8; KEY_MAX];
    let mut v = [0u8; VAL_MAX];
    Ok(block_on(scan.prev(&mut k, &mut v))
        .map_err(stop("prev"))?
        .map(|(kl, _)| tick_of(&k[..kl])))
}
