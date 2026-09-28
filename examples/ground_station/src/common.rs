//! What the dump viewer (`src/lib.rs`) and the live logger
//! (`live/src/lib.rs`) share: a one-poll executor, an error buffer, and
//! every read of a recorder database, generic over its block device.
//!
//! Both crates compile this file as their `common` module, next to the
//! recorder's `format` module, so a frame reads the same in both.

use core::fmt::{self, Write as _};
use core::future::Future;
use core::pin::pin;
use core::task::{Context, Poll, Waker};

use horton::{BlockDevice, Error, OpenReport};

use crate::format::{
    BOOT_KEY, EVENT_EVERY, KEY_MAX, LEVELS, RecorderDb, RecorderRevScan, RecorderScan, SENSORS,
    VAL_MAX, celsius, key, tick_of, value,
};

/// Columns per row of [`frames`]: tick, four sensors, alarm code,
/// debug trace written, debug trace still live.
pub const FRAME_COLS: usize = 8;
/// Most rows one [`frames`] call returns.
pub const MAX_ROWS: usize = 4096;
/// The result buffer: big enough for [`MAX_ROWS`] frame rows.
pub const OUT_LEN: usize = FRAME_COLS * MAX_ROWS;

/// Bits of a tick in a coverage map: bits 0-3 are the sensors, bit 4 the
/// alarm event.
pub const EVENT_BIT: u8 = 1 << 4;
/// Most ticks one coverage map spans.
pub const COVER_LEN: usize = 1 << 16;

// The row layout names the sensors: a recorder with more would need more
// columns.
const _: () = assert!(SENSORS == 4, "frame rows have four sensor columns");

/// A failed read: what was being done, and horton's error.
pub type Failure<E> = (&'static str, Error<E>);

/// horton ships no executor. Every device these modules use completes at
/// once (a host call, or memory), so one poll finishes any future and this
/// never spins.
pub fn run<F: Future>(f: F) -> F::Output {
    let mut f = pin!(f);
    let mut cx = Context::from_waker(Waker::noop());
    loop {
        if let Poll::Ready(v) = f.as_mut().poll(&mut cx) {
            return v;
        }
    }
}

/// A fixed buffer for error messages; longer messages are cut short.
pub struct ErrorText {
    pub buf: [u8; 160],
    pub len: usize,
}

impl ErrorText {
    /// An empty message.
    pub const fn new() -> Self {
        Self {
            buf: [0; 160],
            len: 0,
        }
    }

    /// Replaces the message with `what: detail` and returns `-1`, the
    /// exports' failure code.
    pub fn set(&mut self, what: &str, detail: &dyn fmt::Debug) -> i32 {
        self.len = 0;
        let _ = write!(self, "{what}: {detail:?}");
        -1
    }
}

impl fmt::Write for ErrorText {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        for c in s.chars() {
            let mut utf8 = [0u8; 4];
            let bytes = c.encode_utf8(&mut utf8).as_bytes();
            if self.len + bytes.len() > self.buf.len() {
                break;
            }
            self.buf[self.len..self.len + bytes.len()].copy_from_slice(bytes);
            self.len += bytes.len();
        }
        Ok(())
    }
}

/// The newest tick in `db`, from a reverse scan: every data key starts
/// with a tick below 2^56, so its first byte is 0.
///
/// # Errors
///
/// A device or corruption error from the scan.
pub fn newest_tick<D: BlockDevice>(db: &RecorderDb<D>) -> Result<Option<u64>, Error<D::Error>> {
    let mut rev = RecorderRevScan::new(db);
    run(rev.seek_prev(&[0x01], None, u64::MAX))?;
    let (mut k, mut v) = ([0u8; KEY_MAX], [0u8; VAL_MAX]);
    Ok(run(rev.prev(&mut k, &mut v))?.map(|(kl, _)| tick_of(&k[..kl])))
}

/// The oldest tick in `db` (older ones are in the archive).
///
/// # Errors
///
/// A device or corruption error from the scan.
pub fn oldest_tick<D: BlockDevice>(db: &RecorderDb<D>) -> Result<Option<u64>, Error<D::Error>> {
    let mut scan = RecorderScan::new(db);
    run(scan.seek(b"", Some(&[0x01]), u64::MAX))?;
    let (mut k, mut v) = ([0u8; KEY_MAX], [0u8; VAL_MAX]);
    Ok(run(scan.next(&mut k, &mut v))?.map(|(kl, _)| tick_of(&k[..kl])))
}

/// Writes a summary of `db` to `o` and returns how many values it wrote.
///
/// | Index | Value |
/// |---|---|
/// | 0 | WAL records replayed by `open` |
/// | 1 | highest sequence number |
/// | 2 | boot count (the recorder's `\xFFboot` key), or NaN |
/// | 3, 4 | oldest and newest tick, or NaN when there are none |
/// | 5, 6, 7, 8 | table slots: total, blocks each, used, free |
/// | 9 | manifest copies (the ring) |
/// | 10, 11 | WAL region: first block, end block |
/// | 12, 13 | table region: first block, end block |
/// | 14.. | tables in each level, `LEVELS` values |
///
/// # Errors
///
/// A read failed.
pub fn summary<D: BlockDevice>(
    db: &RecorderDb<D>,
    report: &OpenReport,
    o: &mut [f64],
) -> Result<usize, Failure<D::Error>> {
    let mut boot = [0u8; VAL_MAX];
    let boots = match run(db.get(BOOT_KEY, &mut boot)).map_err(|e| ("get boot count", e))? {
        Some(_) => u64::from_le_bytes(boot) as f64,
        None => f64::NAN,
    };
    let oldest = oldest_tick(db).map_err(|e| ("scan", e))?;
    let newest = newest_tick(db).map_err(|e| ("scan", e))?;
    let tick = |t: Option<u64>| t.map_or(f64::NAN, |t| t as f64);
    let (slots, c) = (db.slot_stats(), db.config());
    o[0] = report.recovered_records as f64;
    o[1] = report.max_seq as f64;
    o[2] = boots;
    o[3] = tick(oldest);
    o[4] = tick(newest);
    o[5] = f64::from(slots.slots);
    o[6] = slots.slot_blocks as f64;
    o[7] = f64::from(slots.used);
    o[8] = f64::from(slots.free);
    o[9] = f64::from(c.manifest_ring);
    o[10] = c.wal_start as f64;
    o[11] = c.wal_end as f64;
    o[12] = c.tbl_start as f64;
    o[13] = c.tbl_end as f64;
    for level in 0..LEVELS {
        o[14 + level] = db.level_tables(level).map_or(0, <[_]>::len) as f64;
    }
    Ok(14 + LEVELS)
}

/// Reads `count` ticks starting at `from` into `o`, one row of
/// [`FRAME_COLS`] values per tick (at most [`MAX_ROWS`] rows):
///
/// `[tick, s0, s1, s2, s3, alarm, trace, live]`
///
/// Sensor columns are °C, NaN where the database holds no reading: a
/// glitch window the recorder purged with a range delete, or ticks that
/// went to the archive. `alarm` is the alarm code or NaN. `trace` is 1
/// when the tick's debug trace is stored, `live` is 1 when it has not
/// expired by `now` (the recorder's clock is its tick).
///
/// Returns the rows written.
///
/// # Errors
///
/// A read failed.
pub fn frames<D: BlockDevice>(
    db: &RecorderDb<D>,
    from: u64,
    count: u32,
    now: u64,
    o: &mut [f64],
) -> Result<usize, Failure<D::Error>> {
    let rows = usize::try_from(count).map_or(MAX_ROWS, |n| n.min(MAX_ROWS));
    for (i, row) in o.chunks_exact_mut(FRAME_COLS).take(rows).enumerate() {
        row.fill(f64::NAN);
        row[0] = from.saturating_add(i as u64) as f64;
        row[6] = 0.0;
        row[7] = 0.0;
    }
    let end = key(from.saturating_add(rows as u64), 0, 0);
    let (mut k, mut v) = ([0u8; KEY_MAX], [0u8; VAL_MAX]);
    let mut scan = RecorderScan::new(db);
    // Pass 1, no clock: every stored entry, expired traces included.
    // Pass 2, clock `now`: the traces a live reader still sees.
    for (pass, clock) in [(1, 0), (2, now)] {
        run(scan.seek_with_time(&key(from, 0, 0), Some(&end), u64::MAX, clock))
            .map_err(|e| ("seek", e))?;
        while let Some((kl, _)) = run(scan.next(&mut k, &mut v)).map_err(|e| ("scan", e))? {
            if kl != KEY_MAX {
                continue;
            }
            let Ok(i) = usize::try_from(tick_of(&k) - from) else {
                continue;
            };
            let row = &mut o[i * FRAME_COLS..(i + 1) * FRAME_COLS];
            match (pass, k[8]) {
                (1, b'r') if k[9] < SENSORS => row[1 + usize::from(k[9])] = celsius(&v),
                (1, b'e') => row[5] = f64::from(k[9]),
                (1, b'd') => row[6] = 1.0,
                (2, b'd') => row[7] = 1.0,
                _ => {}
            }
        }
    }
    Ok(rows)
}

/// Point read of one sensor at one tick. Returns whether it is there, and
/// writes `[°C, intact]` to `o` when it is (intact is 1 when the value is
/// the one the recorder wrote).
///
/// # Errors
///
/// The read failed.
pub fn reading<D: BlockDevice>(
    db: &RecorderDb<D>,
    tick: u64,
    sensor: u8,
    o: &mut [f64],
) -> Result<bool, Failure<D::Error>> {
    let k = key(tick, b'r', sensor);
    let mut v = [0u8; VAL_MAX];
    if run(db.get(&k, &mut v)).map_err(|e| ("get", e))?.is_none() {
        return Ok(false);
    }
    o[0] = celsius(&v);
    o[1] = if v == value(&k) { 1.0 } else { 0.0 };
    Ok(true)
}

/// Checks every entry in `db`, the way the recorder's verifier does, and
/// writes the counts to `o`. Every value is a hash of its key, so no copy
/// of the data is needed. Returns the number of values written, and the
/// invariant violation if `Db::check_invariants` found one.
///
/// | Index | Value |
/// |---|---|
/// | 0 | entries (all keys, metadata included) |
/// | 1, 2, 3 | readings, alarms, debug traces |
/// | 4 | values that do not match their key (must be 0) |
/// | 5 | keys out of order (must be 0) |
/// | 6 | whole frames: ticks with all four readings |
/// | 7 | torn frames: partial frames no table boundary explains (must be 0) |
/// | 8 | `Db::check_invariants` passed (1) or failed (0) |
/// | 9 | readings in frames split with the archive |
/// | 10 | frames split with the archive |
///
/// A frame may be partial on flash without being torn. Archiving moves
/// whole tables, and a table boundary can fall inside a frame, cutting its
/// keys (they sort `s0` to `s3`) into a prefix and a suffix; either table
/// can go to the archive first. So a frame holding a prefix or a suffix of
/// its sensors (`s0 s1`, `s2 s3`, `s3`, ...) is split with the archive;
/// any other partial frame (`s0 s2`) is torn. A `WriteBatch` commits all
/// of a frame or none of it, so a torn frame means corruption or a bug.
/// Proving a split frame is whole needs the archive too, which the live
/// logger's history check does.
///
/// # Errors
///
/// A read failed.
pub fn verify<D: BlockDevice>(
    db: &RecorderDb<D>,
    o: &mut [f64],
) -> Result<(usize, Option<&'static str>), Failure<D::Error>> {
    let mut counts = [0u64; 11];
    let (mut k, mut val) = ([0u8; KEY_MAX], [0u8; VAL_MAX]);
    let (mut prev, mut prev_len) = ([0u8; KEY_MAX], 0usize);
    // The frame being counted: its tick and which sensors it has.
    let mut frame: Option<(u64, u8)> = None;
    let whole = (1u8 << SENSORS) - 1;
    let close = |counts: &mut [u64; 11], frame: Option<(u64, u8)>| match frame {
        Some((_, mask)) if mask == whole => counts[6] += 1,
        // A prefix or a suffix of the sensors: a table boundary cut the
        // frame, and the other part left with its table.
        Some((_, mask)) if mask & (mask + 1) == 0 || (mask | (mask - 1)) == whole => {
            counts[9] += u64::from(mask.count_ones());
            counts[10] += 1;
        }
        Some(_) => counts[7] += 1,
        None => {}
    };
    let mut scan = RecorderScan::new(db);
    run(scan.seek(b"", None, u64::MAX)).map_err(|e| ("seek", e))?;
    while let Some((kl, vl)) = run(scan.next(&mut k, &mut val)).map_err(|e| ("scan", e))? {
        counts[0] += 1;
        if prev_len > 0 && k[..kl] <= prev[..prev_len] {
            counts[5] += 1;
        }
        prev[..kl].copy_from_slice(&k[..kl]);
        prev_len = kl;
        if kl != KEY_MAX || k[0] != 0 {
            continue; // metadata (boot counter, markers)
        }
        if val[..vl] != value(&k) {
            counts[4] += 1;
        }
        match k[8] {
            b'r' if k[9] < SENSORS => {
                counts[1] += 1;
                let tick = tick_of(&k);
                match &mut frame {
                    Some((ft, mask)) if *ft == tick => *mask |= 1 << k[9],
                    _ => {
                        close(&mut counts, frame);
                        frame = Some((tick, 1 << k[9]));
                    }
                }
            }
            b'e' => counts[2] += 1,
            b'd' => counts[3] += 1,
            _ => {}
        }
    }
    close(&mut counts, frame);
    for (slot, n) in o.iter_mut().zip(counts) {
        *slot = n as f64;
    }
    let broken = db.check_invariants().err();
    o[8] = if broken.is_none() { 1.0 } else { 0.0 };
    Ok((11, broken))
}

/// What a coverage scan found.
pub struct Coverage {
    /// The tick `bits[0]` stands for; `None` when there were no frames.
    pub first: Option<u64>,
    /// How many ticks of `bits` are in use.
    pub len: usize,
    /// Entries scanned (metadata included).
    pub entries: u64,
    /// Values that do not match their key.
    pub bad_values: u64,
}

/// Marks, for every tick in `db`, which parts of its frame are there:
/// bits 0-3 for the sensors, [`EVENT_BIT`] for an alarm on an alarm tick.
/// `bits[i]` stands for tick `first + i`. This is how the live logger
/// proves nothing it acknowledged is missing, across flash and archive.
///
/// # Errors
///
/// A read failed, or `db` spans more ticks than `bits` holds
/// (`("coverage", Error::BufferTooSmall { need })`).
pub fn cover<D: BlockDevice>(
    db: &RecorderDb<D>,
    bits: &mut [u8],
) -> Result<Coverage, Failure<D::Error>> {
    let mut c = Coverage {
        first: None,
        len: 0,
        entries: 0,
        bad_values: 0,
    };
    let (mut k, mut v) = ([0u8; KEY_MAX], [0u8; VAL_MAX]);
    let mut scan = RecorderScan::new(db);
    run(scan.seek(b"", Some(&[0x01]), u64::MAX)).map_err(|e| ("seek", e))?;
    while let Some((kl, vl)) = run(scan.next(&mut k, &mut v)).map_err(|e| ("scan", e))? {
        c.entries += 1;
        if kl != KEY_MAX {
            continue;
        }
        if v[..vl] != value(&k) {
            c.bad_values += 1;
            continue;
        }
        let tick = tick_of(&k);
        let bit = match k[8] {
            b'r' if k[9] < SENSORS => 1 << k[9],
            b'e' if tick.is_multiple_of(EVENT_EVERY) => EVENT_BIT,
            _ => continue,
        };
        let first = *c.first.get_or_insert(tick);
        let i = usize::try_from(tick - first).unwrap_or(usize::MAX);
        if i >= bits.len() {
            return Err(("coverage", Error::BufferTooSmall { need: i + 1 }));
        }
        if i >= c.len {
            bits[c.len..=i].fill(0);
            c.len = i + 1;
        }
        bits[i] |= bit;
    }
    Ok(c)
}
