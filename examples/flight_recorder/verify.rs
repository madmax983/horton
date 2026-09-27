//! Proving the recorder lost nothing.
//!
//! Every value is a pure function of its key, so a checker needs no copy
//! of the data: it scans what survived and checks four rules.
//!
//! 1. **Integrity:** every value is the one its key implies.
//! 2. **Nothing acknowledged is lost:** every tick up to the newest has
//!    its whole frame (four readings, plus the alarm every 250 ticks) on
//!    the recorder's flash or in the archive, except purged windows.
//! 3. **Frames are atomic:** a tick has all of its frame or none of it.
//! 4. **Deletes hold:** a purged window is gone from the recorder's own
//!    view. (The archive keeps cold copies made before a purge: archived
//!    tables are immutable, like a backup.)
//!
//! The power-cut test adds a fifth: the newest tick is the last one the
//! recorder acknowledged, or the one it was writing when the power died.

use std::collections::BTreeMap;

use horton::{BlockDevice, Config};

use crate::archive::{RamDevice, fetch, ingest};
use crate::cloud::ObjectStore;
use crate::{
    EVENT_EVERY, KEY_MAX, PURGE_EVERY, RecorderCompaction, RecorderDb, RecorderScan, SENSORS,
    VAL_MAX, block_on, purge_window, tick_of, value,
};

/// Which parts of each tick's frame were found: bits 0-3 are the sensors,
/// bit 4 the alarm event.
#[derive(Default)]
pub struct Coverage {
    ticks: Vec<u8>,
    pub entries: u64,
    pub max_tick: Option<u64>,
}

const EVENT_BIT: u8 = 1 << 4;
const FRAME: u8 = (1 << SENSORS) - 1;

impl Coverage {
    fn mark(&mut self, t: u64, bit: u8) {
        let i = usize::try_from(t).expect("tick fits in memory");
        if self.ticks.len() <= i {
            self.ticks.resize(i + 1, 0);
        }
        self.ticks[i] |= bit;
        self.max_tick = self.max_tick.max(Some(t));
    }

    fn at(&self, t: u64) -> u8 {
        usize::try_from(t)
            .ok()
            .and_then(|i| self.ticks.get(i))
            .copied()
            .unwrap_or(0)
    }
}

/// Scans a whole database into `cov`, checking every value on the way.
pub fn scan<D: BlockDevice>(db: &RecorderDb<D>, cov: &mut Coverage, problems: &mut Vec<String>)
where
    D::Error: core::fmt::Debug,
{
    let mut scan = Box::new(RecorderScan::new(db));
    if let Err(e) = block_on(scan.seek(&[], None, u64::MAX)) {
        problems.push(format!("scan: {e:?}"));
        return;
    }
    let mut k = [0u8; KEY_MAX];
    let mut v = [0u8; VAL_MAX];
    loop {
        let (kl, vl) = match block_on(scan.next(&mut k, &mut v)) {
            Ok(Some(lens)) => lens,
            Ok(None) => break,
            Err(e) => {
                problems.push(format!("scan: {e:?}"));
                return;
            }
        };
        let (key, val) = (&k[..kl], &v[..vl]);
        cov.entries += 1;
        if key.first() == Some(&0xFF) {
            continue; // metadata (the boot counter)
        }
        if kl != KEY_MAX || val != value(key) {
            problems.push(format!("corrupt entry {key:02x?} = {val:02x?}"));
            continue;
        }
        let t = tick_of(key);
        match key[8] {
            b'r' if key[9] < SENSORS => cov.mark(t, 1 << key[9]),
            b'e' if t.is_multiple_of(EVENT_EVERY) => cov.mark(t, EVENT_BIT),
            b'd' => {}
            _ => problems.push(format!("unexpected key {key:02x?}")),
        }
    }
}

/// What the archive holds, built up one object at a time. Objects never
/// change, so each is fetched and scanned once.
#[derive(Default)]
pub struct ArchiveIndex {
    seen: BTreeMap<String, (u64, u64)>,
    pub coverage: Coverage,
    pub bytes: u64,
}

impl ArchiveIndex {
    pub fn objects(&self) -> usize {
        self.seen.len()
    }

    /// Fetches and scans every object not seen yet.
    pub fn refresh(&mut self, store: &dyn ObjectStore, problems: &mut Vec<String>) {
        let keys = match store.list("tables/") {
            Ok(keys) => keys,
            Err(e) => {
                problems.push(format!("listing the archive: {e}"));
                return;
            }
        };
        for key in keys {
            if self.seen.contains_key(&key) {
                continue;
            }
            match fetch(store, &key) {
                Ok(table) => {
                    // A throwaway database of the recorder's shape reads the
                    // table back exactly as the recorder would.
                    let blocks = (u64::from(table.sealed.block_count) + 1) * 64;
                    let mut db = Box::new(RecorderDb::new(
                        RamDevice::new(blocks.max(4096)),
                        Config::whole_device(blocks.max(4096)),
                    ));
                    let mut scratch = Box::new(RecorderCompaction::new());
                    if let Err(e) = block_on(db.open())
                        .map(drop)
                        .and_then(|()| ingest(&mut db, &mut scratch, &table).map(drop))
                    {
                        problems.push(format!("{key}: ingest failed: {e:?}"));
                        continue;
                    }
                    scan(&db, &mut self.coverage, problems);
                    self.bytes += (u64::from(table.sealed.block_count) + 1) * crate::BLOCK as u64;
                    self.seen.insert(key, table.ticks());
                }
                Err(e) => problems.push(e),
            }
        }
    }
}

/// Checks rules 2-4 over the recorder's coverage and the archive's.
/// `in_flight` is the tick being written when the power died, if known.
pub fn check(
    local: &Coverage,
    archive: &Coverage,
    in_flight: Option<u64>,
    problems: &mut Vec<String>,
) {
    let Some(max) = local.max_tick else {
        if archive.max_tick.is_some() {
            problems.push("the recorder is empty but the archive is not".into());
        }
        return;
    };
    for t in 0..=max {
        let (mine, both) = (local.at(t), local.at(t) | archive.at(t));
        let epoch = t / PURGE_EVERY;
        let (a, b) = purge_window(epoch);
        let purge_at = epoch * PURGE_EVERY + PURGE_EVERY / 2;
        if (a..b).contains(&t) {
            if purge_at <= max {
                if mine != 0 {
                    problems.push(format!("tick {t}: purged at {purge_at} but still visible"));
                }
                continue;
            }
            // Purged by the write in flight, or not yet: all or nothing.
            if Some(purge_at) == in_flight.or(Some(max + 1)) && both & FRAME == 0 {
                continue;
            }
        }
        if both & FRAME != FRAME {
            problems.push(format!(
                "tick {t}: frame has sensors {:04b} (flash {:04b}, archive {:04b}), expected 1111",
                both & FRAME,
                mine & FRAME,
                archive.at(t) & FRAME
            ));
        }
        if t.is_multiple_of(EVENT_EVERY) && both & EVENT_BIT == 0 {
            problems.push(format!("tick {t}: alarm event lost"));
        }
        if problems.len() > 20 {
            problems.push("…".into());
            return;
        }
    }
}
