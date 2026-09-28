//! Ground station: the flight recorder's flash, read in a web browser.
//!
//! horton's on-disk format changes between versions, so the only reader
//! that is sure to understand a recorder's flash is the horton that wrote
//! it. This crate compiles that same horton, with the recorder's own
//! [`format`] (its `db_types!` shape, layout and key schema), to
//! `wasm32-unknown-unknown`. A page drops a flash dump into
//! [`gs_image_ptr`], calls [`gs_open`], and reads frames, alarms and
//! integrity reports out of [`gs_out_ptr`]. There is no server, and no
//! second implementation of the format.
//!
//! # The boundary
//!
//! Plain `extern "C"` exports and no imports: the module needs no
//! `wasm-bindgen` and no JavaScript glue beyond `WebAssembly.instantiate`.
//! Ticks cross as `u64` (`BigInt` in JavaScript). Results go to a fixed
//! `f64` buffer ([`gs_out_ptr`]); failures return a negative number and
//! leave a message at [`gs_error_ptr`].
//!
//! # Memory
//!
//! Like firmware, the module allocates nothing: the image, the database
//! and the result buffer are `static`s, so the page's memory bill is
//! fixed when this crate compiles (about 2.4 MiB of statics, 2 MiB of it
//! the image).
//!
//! # Safety
//!
//! The statics are `static mut`. That is sound here because
//! `wasm32-unknown-unknown` without the `atomics` feature has one thread,
//! and the module imports no functions, so JavaScript cannot run while an
//! export is running: every export has exclusive access for its duration.
//! JavaScript writes the image through the raw pointer between calls.

#![no_std]
#![allow(
    clippy::cast_precision_loss,
    reason = "ticks and counts go to JavaScript as f64; they stay far below 2^53"
)]

#[path = "../../flight_recorder/format.rs"]
#[allow(
    dead_code,
    reason = "shared with the recorder, which uses the parts a reader does not"
)]
mod format;

use core::fmt::{self, Write as _};
use core::future::Future;
use core::pin::pin;
use core::task::{Context, Poll, Waker};

use horton::{BlockDevice, OpenReport};

use format::{
    BLOCK, BOOT_KEY, KEY_MAX, LEVELS, RecorderDb, RecorderRevScan, RecorderScan, SECTORS, SENSORS,
    TTL_TICKS, VAL_MAX, celsius, config, key, tick_of, value,
};

/// Bytes in a flash dump: the recorder's whole partition.
const IMAGE_LEN: usize = SECTORS * BLOCK;

/// Columns per row of [`gs_frames`]: tick, four sensors, alarm code,
/// debug trace written, debug trace still live.
const FRAME_COLS: usize = 8;
/// Most rows one [`gs_frames`] call returns.
const MAX_ROWS: usize = 4096;
/// The result buffer: big enough for [`MAX_ROWS`] frame rows.
const OUT_LEN: usize = FRAME_COLS * MAX_ROWS;

// The row layout names the sensors: a recorder with more would need more
// columns.
const _: () = assert!(SENSORS == 4, "gs_frames has four sensor columns");

/// The flash dump. JavaScript copies the file here; horton reads and
/// writes it. Zero-initialized so it lands in `.bss` and costs the
/// download nothing. Recovery may write (it replays the WAL and can rewrite a
/// torn block), and those writes land in this copy, never in the file.
static mut IMAGE: [u8; IMAGE_LEN] = [0; IMAGE_LEN];
/// The database over [`IMAGE`]. Rebuilt by every [`gs_open`].
static mut DB: RecorderDb<ImageDisk> = RecorderDb::new(ImageDisk, config());
/// What the last successful [`gs_open`] recovered; `None` until then.
static mut REPORT: Option<OpenReport> = None;
/// Results, read by JavaScript as a `Float64Array`.
static mut OUT: [f64; OUT_LEN] = [0.0; OUT_LEN];
/// The last failure's message, UTF-8.
static mut ERROR: ErrorText = ErrorText {
    buf: [0; 160],
    len: 0,
};

/// The flash dump as a block device: block `id` is bytes
/// `id * BLOCK .. (id + 1) * BLOCK`, exactly where the recorder's NOR
/// adapter put it.
struct ImageDisk;

/// A block id past the end of the image.
#[derive(Debug)]
struct OutOfRange(#[allow(dead_code, reason = "read through Debug")] u64);

impl ImageDisk {
    fn block(id: u64) -> Result<core::ops::Range<usize>, OutOfRange> {
        usize::try_from(id)
            .ok()
            .filter(|&i| i < SECTORS)
            .map(|i| i * BLOCK..(i + 1) * BLOCK)
            .ok_or(OutOfRange(id))
    }
}

impl BlockDevice for ImageDisk {
    type Error = OutOfRange;
    const BLOCK: usize = BLOCK;

    fn poll_read_block(
        &self,
        _cx: &mut Context<'_>,
        id: u64,
        buf: &mut [u8],
    ) -> Poll<Result<(), OutOfRange>> {
        Poll::Ready(Self::block(id).map(|r| buf.copy_from_slice(&image()[r])))
    }

    fn poll_write_block(
        &mut self,
        _cx: &mut Context<'_>,
        id: u64,
        buf: &[u8],
    ) -> Poll<Result<(), OutOfRange>> {
        Poll::Ready(Self::block(id).map(|r| image()[r].copy_from_slice(buf)))
    }

    fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), OutOfRange>> {
        Poll::Ready(Ok(()))
    }
}

// SAFETY (all four accessors): see the crate docs. One thread, no imports,
// so an export holds the only live reference to each static. `IMAGE` is
// borrowed only inside a single device call, never across one, so it
// never aliases the `DB` borrow that makes the call. (`&raw mut` then a
// reborrow is the form the `static_mut_refs` lint asks for.)
#[allow(clippy::deref_addrof, reason = "see static_mut_refs above")]
fn image() -> &'static mut [u8; IMAGE_LEN] {
    unsafe { &mut *(&raw mut IMAGE) }
}
#[allow(clippy::deref_addrof, reason = "see static_mut_refs above")]
fn db() -> &'static mut RecorderDb<ImageDisk> {
    unsafe { &mut *(&raw mut DB) }
}
#[allow(clippy::deref_addrof, reason = "see static_mut_refs above")]
fn out() -> &'static mut [f64; OUT_LEN] {
    unsafe { &mut *(&raw mut OUT) }
}
#[allow(clippy::deref_addrof, reason = "see static_mut_refs above")]
fn error() -> &'static mut ErrorText {
    unsafe { &mut *(&raw mut ERROR) }
}

/// horton ships no executor. Every [`ImageDisk`] call completes at once,
/// so one poll finishes any future and this never spins.
fn run<F: Future>(f: F) -> F::Output {
    let mut f = pin!(f);
    let mut cx = Context::from_waker(Waker::noop());
    loop {
        if let Poll::Ready(v) = f.as_mut().poll(&mut cx) {
            return v;
        }
    }
}

/// A fixed buffer for error messages; longer messages are cut short.
struct ErrorText {
    buf: [u8; 160],
    len: usize,
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

/// Records `what` and `detail` as the last error and returns `-1`.
fn fail(what: &str, detail: &dyn fmt::Debug) -> i32 {
    let e = error();
    e.len = 0;
    let _ = write!(e, "{what}: {detail:?}");
    -1
}

/// Fails unless [`gs_open`] has succeeded since the last image load.
fn opened() -> Result<OpenReport, i32> {
    // SAFETY: see the crate docs; a copy out of the static.
    unsafe { REPORT }.ok_or_else(|| fail("not open", &"call gs_open first"))
}

/// The newest tick on the flash, from a reverse scan: every data key
/// starts with a tick below 2^56, so its first byte is 0.
fn newest_tick() -> Result<Option<u64>, horton::Error<OutOfRange>> {
    let mut rev = RecorderRevScan::new(db());
    run(rev.seek_prev(&[0x01], None, u64::MAX))?;
    let (mut k, mut v) = ([0u8; KEY_MAX], [0u8; VAL_MAX]);
    Ok(run(rev.prev(&mut k, &mut v))?.map(|(kl, _)| tick_of(&k[..kl])))
}

/// The oldest tick on the flash (older ones are in the archive).
fn oldest_tick() -> Result<Option<u64>, horton::Error<OutOfRange>> {
    let mut scan = RecorderScan::new(db());
    run(scan.seek(b"", Some(&[0x01]), u64::MAX))?;
    let (mut k, mut v) = ([0u8; KEY_MAX], [0u8; VAL_MAX]);
    Ok(run(scan.next(&mut k, &mut v))?.map(|(kl, _)| tick_of(&k[..kl])))
}

/// Where JavaScript copies the flash dump: [`gs_image_len`] bytes.
#[unsafe(no_mangle)]
pub extern "C" fn gs_image_ptr() -> *mut u8 {
    (&raw mut IMAGE).cast()
}

/// The size a flash dump must have: the recorder's partition, 2 MiB.
#[unsafe(no_mangle)]
pub const extern "C" fn gs_image_len() -> usize {
    IMAGE_LEN
}

/// The result buffer, read as a `Float64Array` of [`gs_out_len`].
#[unsafe(no_mangle)]
pub extern "C" fn gs_out_ptr() -> *const f64 {
    (&raw const OUT).cast()
}

/// Length of the result buffer, in `f64`s.
#[unsafe(no_mangle)]
pub const extern "C" fn gs_out_len() -> usize {
    OUT_LEN
}

/// The last error's message: [`gs_error_len`] bytes of UTF-8.
#[unsafe(no_mangle)]
pub extern "C" fn gs_error_ptr() -> *const u8 {
    error().buf.as_ptr()
}

/// Length of the last error's message, in bytes.
#[unsafe(no_mangle)]
pub extern "C" fn gs_error_len() -> usize {
    error().len
}

/// Opens the image in [`gs_image_ptr`] as the recorder would after a
/// reboot: recover the newest manifest, rebuild the table slots, replay
/// the WAL up to any torn tail. Call it after every image load.
///
/// Returns the WAL records replayed (writes that were only in the log when
/// the dump was taken), or `-1` with a message: a dump from another
/// horton version fails as `CorruptManifest`, never misread.
#[unsafe(no_mangle)]
pub extern "C" fn gs_open() -> i32 {
    // SAFETY: see the crate docs.
    unsafe { REPORT = None };
    *db() = RecorderDb::new(ImageDisk, config());
    match run(db().open()) {
        Ok(report) => {
            // SAFETY: see the crate docs.
            unsafe { REPORT = Some(report) };
            i32::try_from(report.recovered_records).unwrap_or(i32::MAX)
        }
        Err(e) => fail("open", &e),
    }
}

/// Fills the result buffer with a summary of the open image. Returns the
/// number of values written, or `-1`.
///
/// | Index | Value |
/// |---|---|
/// | 0 | WAL records replayed by `gs_open` |
/// | 1 | highest sequence number |
/// | 2 | boot count (the recorder's `\xFFboot` key), or NaN |
/// | 3, 4 | oldest and newest tick on flash, or NaN when there are none |
/// | 5, 6, 7, 8 | table slots: total, blocks each, used, free |
/// | 9 | manifest copies (the ring) |
/// | 10, 11 | WAL region: first block, end block |
/// | 12, 13 | table region: first block, end block |
/// | 14.. | tables in each level, `LEVELS` values |
#[unsafe(no_mangle)]
pub extern "C" fn gs_summary() -> i32 {
    let report = match opened() {
        Ok(r) => r,
        Err(code) => return code,
    };
    let mut boot = [0u8; VAL_MAX];
    let boots = match run(db().get(BOOT_KEY, &mut boot)) {
        Ok(Some(_)) => u64::from_le_bytes(boot) as f64,
        Ok(None) => f64::NAN,
        Err(e) => return fail("get boot count", &e),
    };
    let (oldest, newest) = match (oldest_tick(), newest_tick()) {
        (Ok(a), Ok(b)) => (a, b),
        (Err(e), _) | (_, Err(e)) => return fail("scan", &e),
    };
    let tick = |t: Option<u64>| t.map_or(f64::NAN, |t| t as f64);
    let d = db();
    let (slots, c) = (d.slot_stats(), d.config());
    let o = out();
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
        o[14 + level] = d.level_tables(level).map_or(0, <[_]>::len) as f64;
    }
    i32::try_from(14 + LEVELS).unwrap_or(i32::MAX)
}

/// Reads `count` ticks starting at `from` into the result buffer, one row
/// of eight values per tick (at most 4096 rows):
///
/// `[tick, s0, s1, s2, s3, alarm, trace, live]`
///
/// Sensor columns are °C, NaN where the flash holds no reading: a glitch
/// window the recorder purged with a range delete, or ticks that went to
/// the archive. `alarm` is the alarm code or NaN. `trace` is 1 when the
/// tick's debug trace is stored, `live` is 1 when it has not expired by
/// `now` (the recorder's clock is its tick; pass the newest tick).
///
/// Returns the rows written, or `-1`.
#[unsafe(no_mangle)]
pub extern "C" fn gs_frames(from: u64, count: u32, now: u64) -> i32 {
    if let Err(code) = opened() {
        return code;
    }
    let rows = usize::try_from(count).map_or(MAX_ROWS, |n| n.min(MAX_ROWS));
    let o = out();
    for (i, row) in o.chunks_exact_mut(FRAME_COLS).take(rows).enumerate() {
        row.fill(f64::NAN);
        row[0] = from.saturating_add(i as u64) as f64;
        row[6] = 0.0;
        row[7] = 0.0;
    }
    let end = key(from.saturating_add(rows as u64), 0, 0);
    let (mut k, mut v) = ([0u8; KEY_MAX], [0u8; VAL_MAX]);
    let mut scan = RecorderScan::new(db());
    // Pass 1, no clock: every stored entry, expired traces included.
    // Pass 2, clock `now`: the traces a live reader still sees.
    for (pass, clock) in [(1, 0), (2, now)] {
        if let Err(e) = run(scan.seek_with_time(&key(from, 0, 0), Some(&end), u64::MAX, clock)) {
            return fail("seek", &e);
        }
        loop {
            let kl = match run(scan.next(&mut k, &mut v)) {
                Ok(Some((kl, _))) => kl,
                Ok(None) => break,
                Err(e) => return fail("scan", &e),
            };
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
    i32::try_from(rows).unwrap_or(i32::MAX)
}

/// Point read of one sensor at one tick. Returns 1 and writes
/// `[°C, intact]` (intact is 1 when the value is the one the recorder
/// wrote), 0 when the flash holds no such reading, or `-1`.
#[unsafe(no_mangle)]
pub extern "C" fn gs_reading(tick: u64, sensor: u32) -> i32 {
    if let Err(code) = opened() {
        return code;
    }
    let Ok(sensor) = u8::try_from(sensor) else {
        return 0;
    };
    let k = key(tick, b'r', sensor);
    let mut v = [0u8; VAL_MAX];
    match run(db().get(&k, &mut v)) {
        Ok(Some(_)) => {
            let o = out();
            o[0] = celsius(&v);
            o[1] = if v == value(&k) { 1.0 } else { 0.0 };
            1
        }
        Ok(None) => 0,
        Err(e) => fail("get", &e),
    }
}

/// Checks every entry on the flash, the way the recorder's verifier does.
///
/// Every value is a hash of its key, so no copy of the data is needed.
/// Returns the number of values written, or `-1`.
///
/// | Index | Value |
/// |---|---|
/// | 0 | entries (all keys, metadata included) |
/// | 1, 2, 3 | readings, alarms, debug traces |
/// | 4 | values that do not match their key (must be 0) |
/// | 5 | keys out of order (must be 0) |
/// | 6 | whole frames: ticks with all four readings |
/// | 7 | torn frames: ticks with some but not all readings (must be 0) |
/// | 8 | `Db::check_invariants` passed (1) or failed (0; message in the error buffer) |
/// | 9 | readings of the archive-boundary frame (see below), or 0 |
///
/// The oldest frame on flash may be partial without being torn. The
/// recorder archives whole tables, and a table can end partway through a
/// frame, so the frame's first readings went to the archive with the table
/// and the rest are still here. Only that oldest frame is excused.
#[unsafe(no_mangle)]
pub extern "C" fn gs_verify() -> i32 {
    if let Err(code) = opened() {
        return code;
    }
    let mut counts = [0u64; 10];
    let (mut k, mut val) = ([0u8; KEY_MAX], [0u8; VAL_MAX]);
    let (mut prev, mut prev_len) = ([0u8; KEY_MAX], 0usize);
    // The frame being counted: its tick and how many readings it has.
    let mut frame: Option<(u64, u8)> = None;
    let mut oldest = true;
    let mut close = |counts: &mut [u64; 10], frame: Option<(u64, u8)>| {
        match frame {
            Some((_, n)) if n == SENSORS => counts[6] += 1,
            Some((_, n)) if oldest => counts[9] = u64::from(n),
            Some(_) => counts[7] += 1,
            None => return,
        }
        oldest = false;
    };
    let mut scan = RecorderScan::new(db());
    if let Err(e) = run(scan.seek(b"", None, u64::MAX)) {
        return fail("seek", &e);
    }
    loop {
        let (kl, vl) = match run(scan.next(&mut k, &mut val)) {
            Ok(Some(n)) => n,
            Ok(None) => break,
            Err(e) => return fail("scan", &e),
        };
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
            b'r' => {
                counts[1] += 1;
                let tick = tick_of(&k);
                match &mut frame {
                    Some((ft, n)) if *ft == tick => *n += 1,
                    _ => {
                        close(&mut counts, frame);
                        frame = Some((tick, 1));
                    }
                }
            }
            b'e' => counts[2] += 1,
            b'd' => counts[3] += 1,
            _ => {}
        }
    }
    close(&mut counts, frame);
    let o = out();
    for (slot, n) in o.iter_mut().zip(counts) {
        *slot = n as f64;
    }
    o[8] = match db().check_invariants() {
        Ok(()) => 1.0,
        Err(why) => {
            fail("invariants", &why);
            0.0
        }
    };
    10
}

/// The recorder's debug-trace lifetime, in ticks, for the page's legend.
#[unsafe(no_mangle)]
pub const extern "C" fn gs_ttl_ticks() -> u64 {
    TTL_TICKS
}

#[panic_handler]
fn panic(_: &core::panic::PanicInfo<'_>) -> ! {
    // horton returns errors instead of panicking; reaching this is a bug
    // in this file. Trap, which JavaScript sees as a `RuntimeError`.
    core::arch::wasm32::unreachable()
}
