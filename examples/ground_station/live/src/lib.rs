//! Live logger: the flight recorder's horton, running in a browser.
//!
//! A laptop in the field takes the device's live telemetry stream and
//! keeps it the way the device does: horton, with the recorder's own
//! [`format`] (its `db_types!` shape, partition layout, key schema and
//! archive object), so what the logger writes is a valid recorder flash
//! image and every archived table is a valid recorder archive object.
//!
//! - **Flash is an OPFS file.** The module's [`BlockDevice`] is three host
//!   imports (`horton_host.lv_read`, `lv_write`, `lv_flush`). In a Worker,
//!   JavaScript backs them with a `FileSystemSyncAccessHandle`, whose
//!   `read`, `write` and `flush` are synchronous, so every horton call
//!   completes in one poll, just as on the device.
//! - **Every frame is durable before it is acknowledged.** [`lv_record`]
//!   commits the four readings (and any alarm) as one `WriteBatch`: one
//!   WAL block written and flushed. It makes room exactly as the
//!   recorder's firmware loop does (flush, compact) and does one bounded
//!   compaction step per frame.
//! - **Cold tables go to `IndexedDB` in two steps.** [`lv_archive_next`]
//!   stages the oldest cold table as an archive object in
//!   [`lv_archive_ptr`]; JavaScript stores it and awaits the transaction;
//!   then [`lv_archive_commit`] drops it from flash. The asynchronous
//!   `await` falls between two horton calls, never inside one. A crash in
//!   between leaves the table on flash and a copy in the archive, which the
//!   next pass stores again (the same bytes: table ids never repeat) and
//!   commits.
//! - **It can prove it lost nothing.** [`lv_cover`] and
//!   [`lv_archive_cover`] mark which parts of each tick's frame the flash
//!   and each archived table hold. JavaScript merges them and checks every
//!   acknowledged tick is whole somewhere. Archived tables are read back by
//!   ingesting them into a scratch database of the same shape, exactly as
//!   the recorder's `restore` does.
//!
//! # Safety
//!
//! The statics are `static mut`. That is sound because
//! `wasm32-unknown-unknown` without the `atomics` feature has one thread,
//! and the host imports only touch the OPFS file (they never call back into
//! this module), so every export has exclusive access for its duration.
//! JavaScript writes frames and archive objects through the raw pointers
//! between calls.

#![no_std]
#![allow(
    clippy::cast_precision_loss,
    reason = "ticks and counts go to JavaScript as f64; they stay far below 2^53"
)]

#[path = "../../../flight_recorder/format.rs"]
#[allow(
    dead_code,
    reason = "shared with the recorder, which uses the parts the logger does not"
)]
mod format;

#[path = "../../src/common.rs"]
#[allow(dead_code, reason = "shared with the dump viewer")]
mod common;

use core::future::poll_fn;
use core::task::{Context, Poll};

use horton::{BlockDevice, Error, OpenReport, WriteBatch};

use common::{COVER_LEN, ErrorText, Failure, OUT_LEN, run};
use format::{
    BLOCK, BOOT_KEY, HOT_TICKS, KEY_MAX, LEVELS, RecorderCompaction, RecorderDb, SECTORS, SENSORS,
    VAL_MAX, config, decode_archive_header, encode_archive_header, key, tick_of,
};

/// Most blocks one archived table may span. The recorder's slots are 27
/// blocks; this leaves room for a reshaped recorder, and [`lv_archive_next`]
/// refuses anything larger rather than truncate it.
const ARCHIVE_BLOCKS: usize = 64;
/// An archive object: one header block, then the table's blocks.
const ARCHIVE_LEN: usize = (ARCHIVE_BLOCKS + 1) * BLOCK;
/// Bytes in the recorder's partition.
const IMAGE_LEN: usize = SECTORS * BLOCK;
/// Frame input: tick, four readings, alarm code (`u64::MAX` for none).
const FRAME_LEN: usize = 2 + SENSORS as usize;
/// How often [`lv_record`] may ask for room before it gives up.
const MAX_ATTEMPTS: usize = 64;

/// [`lv_record`]: the frame is durable.
const RECORDED: i32 = 0;
/// [`lv_record`]: the frame is durable, and cold tables are waiting.
const COLD_WAITING: i32 = 1;
/// [`lv_record`]: the frame is *not* recorded: no slot is free until cold
/// tables leave. Archive, then record the same frame again.
const NEEDS_ARCHIVE: i32 = 2;

#[link(wasm_import_module = "horton_host")]
unsafe extern "C" {
    /// Reads block `id` into `len` bytes at `dst`. Returns 0 on success.
    fn lv_read(id: u64, dst: *mut u8, len: usize) -> i32;
    /// Writes `len` bytes at `src` to block `id`. Returns 0 on success.
    fn lv_write(id: u64, src: *const u8, len: usize) -> i32;
    /// Makes every write so far durable. Returns 0 on success.
    fn lv_flush() -> i32;
}

/// Counters for the page, reset by [`lv_open`].
#[derive(Clone, Copy)]
struct Stats {
    frames: u64,
    flushes: u64,
    compaction_steps: u64,
    archived_tables: u64,
    archived_blocks: u64,
    archive_refusals: u64,
    stalls: u64,
    latest: Option<u64>,
}

impl Stats {
    const fn new() -> Self {
        Self {
            frames: 0,
            flushes: 0,
            compaction_steps: 0,
            archived_tables: 0,
            archived_blocks: 0,
            archive_refusals: 0,
            stalls: 0,
            latest: None,
        }
    }
}

static mut DB: RecorderDb<HostDisk> = RecorderDb::new(HostDisk, config());
/// What the last successful [`lv_open`] recovered; `None` until then.
static mut REPORT: Option<OpenReport> = None;
static mut SCRATCH: RecorderCompaction = RecorderCompaction::new();
static mut STATS: Stats = Stats::new();
/// The table [`lv_archive_next`] staged: its level and id.
static mut PENDING: Option<(usize, u32)> = None;
static mut FRAME: [u64; FRAME_LEN] = [0; FRAME_LEN];
static mut OUT: [f64; OUT_LEN] = [0.0; OUT_LEN];
static mut ERROR: ErrorText = ErrorText::new();
/// An archive object, staged for JavaScript or handed in by it.
static mut ARCHIVE: [u8; ARCHIVE_LEN] = [0; ARCHIVE_LEN];
static mut COVER: [u8; COVER_LEN] = [0; COVER_LEN];
/// The scratch database archived tables are read back through.
static mut VERIFY_IMAGE: [u8; IMAGE_LEN] = [0; IMAGE_LEN];
static mut VERIFY_DB: RecorderDb<RamDisk> = RecorderDb::new(RamDisk, config());

// SAFETY (all accessors): see the crate docs. One thread, and the imports
// never re-enter, so an export holds the only live reference to each
// static. The memory devices borrow their buffers only inside one device
// call, never across one, so they never alias the database borrow that
// makes the call. (`&raw mut` then a reborrow is the form the
// `static_mut_refs` lint asks for.)
macro_rules! accessor {
    ($name:ident, $static:ident, $ty:ty) => {
        #[allow(clippy::deref_addrof, reason = "see static_mut_refs above")]
        fn $name() -> &'static mut $ty {
            unsafe { &mut *(&raw mut $static) }
        }
    };
}
accessor!(db, DB, RecorderDb<HostDisk>);
accessor!(scratch, SCRATCH, RecorderCompaction);
accessor!(stats, STATS, Stats);
accessor!(pending, PENDING, Option<(usize, u32)>);
accessor!(frame, FRAME, [u64; FRAME_LEN]);
accessor!(out, OUT, [f64; OUT_LEN]);
accessor!(error, ERROR, ErrorText);
accessor!(archive, ARCHIVE, [u8; ARCHIVE_LEN]);
accessor!(cover_bits, COVER, [u8; COVER_LEN]);
accessor!(verify_image, VERIFY_IMAGE, [u8; IMAGE_LEN]);
accessor!(verify_db, VERIFY_DB, RecorderDb<RamDisk>);

/// The OPFS file, through the host imports.
struct HostDisk;

/// A host import's non-zero status.
#[derive(Debug)]
struct HostError(#[allow(dead_code, reason = "read through Debug")] i32);

const fn host(rc: i32) -> Poll<Result<(), HostError>> {
    Poll::Ready(if rc == 0 { Ok(()) } else { Err(HostError(rc)) })
}

impl BlockDevice for HostDisk {
    type Error = HostError;
    const BLOCK: usize = BLOCK;

    fn poll_read_block(
        &self,
        _cx: &mut Context<'_>,
        id: u64,
        buf: &mut [u8],
    ) -> Poll<Result<(), HostError>> {
        // SAFETY: the host writes exactly `buf.len()` bytes into `buf`.
        host(unsafe { lv_read(id, buf.as_mut_ptr(), buf.len()) })
    }

    fn poll_write_block(
        &mut self,
        _cx: &mut Context<'_>,
        id: u64,
        buf: &[u8],
    ) -> Poll<Result<(), HostError>> {
        // SAFETY: the host only reads `buf.len()` bytes from `buf`.
        host(unsafe { lv_write(id, buf.as_ptr(), buf.len()) })
    }

    fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), HostError>> {
        // SAFETY: takes no pointers.
        host(unsafe { lv_flush() })
    }
}

/// A block id outside a memory device.
#[derive(Debug)]
struct OutOfRange(#[allow(dead_code, reason = "read through Debug")] u64);

fn range(id: u64, blocks: usize) -> Result<core::ops::Range<usize>, OutOfRange> {
    usize::try_from(id)
        .ok()
        .filter(|&i| i < blocks)
        .map(|i| i * BLOCK..(i + 1) * BLOCK)
        .ok_or(OutOfRange(id))
}

/// The scratch database's disk, in [`VERIFY_IMAGE`].
struct RamDisk;

impl BlockDevice for RamDisk {
    type Error = OutOfRange;
    const BLOCK: usize = BLOCK;

    fn poll_read_block(
        &self,
        _cx: &mut Context<'_>,
        id: u64,
        buf: &mut [u8],
    ) -> Poll<Result<(), OutOfRange>> {
        Poll::Ready(range(id, SECTORS).map(|r| buf.copy_from_slice(&verify_image()[r])))
    }

    fn poll_write_block(
        &mut self,
        _cx: &mut Context<'_>,
        id: u64,
        buf: &[u8],
    ) -> Poll<Result<(), OutOfRange>> {
        Poll::Ready(range(id, SECTORS).map(|r| verify_image()[r].copy_from_slice(buf)))
    }

    fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), OutOfRange>> {
        Poll::Ready(Ok(()))
    }
}

/// An archive object's table blocks, in [`ARCHIVE`] after the header:
/// block `id` of the table is `ARCHIVE[(id + 1) * BLOCK..]`.
struct ArchiveSrc;

impl BlockDevice for ArchiveSrc {
    type Error = OutOfRange;
    const BLOCK: usize = BLOCK;

    fn poll_read_block(
        &self,
        _cx: &mut Context<'_>,
        id: u64,
        buf: &mut [u8],
    ) -> Poll<Result<(), OutOfRange>> {
        Poll::Ready(
            range(id, ARCHIVE_BLOCKS)
                .map(|r| buf.copy_from_slice(&archive()[r.start + BLOCK..r.end + BLOCK])),
        )
    }

    fn poll_write_block(
        &mut self,
        _cx: &mut Context<'_>,
        id: u64,
        _buf: &[u8],
    ) -> Poll<Result<(), OutOfRange>> {
        // An archived table is read, never written.
        Poll::Ready(Err(OutOfRange(id)))
    }

    fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), OutOfRange>> {
        Poll::Ready(Ok(()))
    }
}

/// Fails unless [`lv_open`] has succeeded.
fn opened() -> Result<OpenReport, i32> {
    // SAFETY: see the crate docs; a copy out of the static.
    unsafe { REPORT }.ok_or_else(|| error().set("not open", &"call lv_open first"))
}

/// Maps a read's result to an export's return value.
fn ret<E: core::fmt::Debug>(r: Result<usize, Failure<E>>) -> i32 {
    match r {
        Ok(n) => i32::try_from(n).unwrap_or(i32::MAX),
        Err((what, e)) => error().set(what, &e),
    }
}

/// What making room got to.
enum Room {
    /// Retry the operation.
    Made,
    /// No slot is free until cold tables leave for the archive.
    NeedsArchive,
}

/// Does what a capacity error asks, as the recorder's firmware does:
/// flush a full memtable or WAL, compact a full level 0.
fn make_room(t: u64, e: Error<HostError>) -> Result<Room, Failure<HostError>> {
    match e {
        Error::TableFull | Error::ArenaFull | Error::WalFull => match run(db().flush()) {
            Ok(()) => {
                stats().flushes += 1;
                Ok(Room::Made)
            }
            Err(e) => make_room(t, e),
        },
        Error::NeedsCompaction => compact_all(t),
        Error::RegionFull => Ok(Room::NeedsArchive),
        e => Err(("make room", e)),
    }
}

fn compact_all(t: u64) -> Result<Room, Failure<HostError>> {
    scratch().purge_before = t;
    while db().compaction_pending() {
        match run(db().compact_step(scratch())) {
            Ok(_) => stats().compaction_steps += 1,
            Err(Error::RegionFull) => return Ok(Room::NeedsArchive),
            Err(e) => return Err(("compact", e)),
        }
    }
    Ok(Room::Made)
}

/// The key every archivable table ends before: tick `latest - HOT_TICKS`.
fn horizon() -> Option<[u8; KEY_MAX]> {
    let latest = stats().latest?;
    Some(key(latest.checked_sub(HOT_TICKS)?, 0, 0))
}

/// Whether any table holds only ticks older than the hot window.
fn cold_waiting() -> bool {
    let Some(h) = horizon() else {
        return false;
    };
    (0..LEVELS).any(|level| {
        db().level_tables(level)
            .unwrap_or(&[])
            .iter()
            .any(|t| t.last_key.as_slice() < h.as_slice())
    })
}

/// Bumps the boot counter, as the recorder does on every start.
fn count_boot() -> Result<i32, Failure<HostError>> {
    let mut v = [0u8; VAL_MAX];
    let boots = run(db().get(BOOT_KEY, &mut v))
        .map_err(|e| ("get boot count", e))?
        .map_or(0, |_| u64::from_le_bytes(v))
        + 1;
    for _ in 0..MAX_ATTEMPTS {
        match run(db().put(BOOT_KEY, &boots.to_le_bytes())) {
            Ok(_) => return Ok(RECORDED),
            Err(e) => match make_room(0, e)? {
                Room::Made => {}
                Room::NeedsArchive => return Ok(NEEDS_ARCHIVE),
            },
        }
    }
    Err(("count boot", Error::NeedsCompaction))
}

/// The frame input: [`lv_frame_len`] `u64`s, written by JavaScript before
/// each [`lv_record`]: tick, the four readings (each the 8 value bytes,
/// little-endian), and the alarm code or `u64::MAX` for none.
#[unsafe(no_mangle)]
pub extern "C" fn lv_frame_ptr() -> *mut u64 {
    (&raw mut FRAME).cast()
}

/// Length of the frame input, in `u64`s.
#[unsafe(no_mangle)]
pub const extern "C" fn lv_frame_len() -> usize {
    FRAME_LEN
}

/// The result buffer, read as a `Float64Array` of [`lv_out_len`].
#[unsafe(no_mangle)]
pub extern "C" fn lv_out_ptr() -> *const f64 {
    (&raw const OUT).cast()
}

/// Length of the result buffer, in `f64`s.
#[unsafe(no_mangle)]
pub const extern "C" fn lv_out_len() -> usize {
    OUT_LEN
}

/// The last error's message: [`lv_error_len`] bytes of UTF-8.
#[unsafe(no_mangle)]
pub extern "C" fn lv_error_ptr() -> *const u8 {
    error().buf.as_ptr()
}

/// Length of the last error's message, in bytes.
#[unsafe(no_mangle)]
pub extern "C" fn lv_error_len() -> usize {
    error().len
}

/// The archive object buffer: [`lv_archive_cap`] bytes.
#[unsafe(no_mangle)]
pub extern "C" fn lv_archive_ptr() -> *mut u8 {
    (&raw mut ARCHIVE).cast()
}

/// Largest archive object the buffer holds, in bytes.
#[unsafe(no_mangle)]
pub const extern "C" fn lv_archive_cap() -> usize {
    ARCHIVE_LEN
}

/// The coverage map written by [`lv_cover`] and [`lv_archive_cover`].
#[unsafe(no_mangle)]
pub extern "C" fn lv_cover_ptr() -> *const u8 {
    (&raw const COVER).cast()
}

/// The size of the flash the host must provide, in bytes: the recorder's
/// partition. A fresh file reads as erased flash (`0xFF`).
#[unsafe(no_mangle)]
pub const extern "C" fn lv_flash_len() -> usize {
    IMAGE_LEN
}

/// Opens the flash as the recorder does on boot.
///
/// Recovers the newest manifest, rebuilds the table slots and replays the
/// WAL up to any torn tail; [`lv_count_boot`] then counts the boot. Call
/// it once per start, and again after anything went wrong: it starts over
/// from what is durable.
///
/// Returns the WAL records replayed, or `-1`.
#[unsafe(no_mangle)]
pub extern "C" fn lv_open() -> i32 {
    // SAFETY: see the crate docs.
    unsafe { REPORT = None };
    *pending() = None;
    *stats() = Stats::new();
    *scratch() = RecorderCompaction::new();
    *db() = RecorderDb::new(HostDisk, config());
    let report = match run(db().open()) {
        Ok(r) => r,
        Err(e) => return error().set("open", &e),
    };
    match common::newest_tick(db()) {
        Ok(t) => stats().latest = t,
        Err(e) => return error().set("scan", &e),
    }
    // SAFETY: see the crate docs.
    unsafe { REPORT = Some(report) };
    i32::try_from(report.recovered_records).unwrap_or(i32::MAX)
}

/// Counts this boot in the recorder's boot counter (`\xFFboot`), as the
/// recorder does on every start. Call it once after [`lv_open`].
///
/// Returns 0 when counted; 2 when there is no room until cold tables
/// leave (archive, then call it again, as for [`lv_record`]); or `-1`.
#[unsafe(no_mangle)]
pub extern "C" fn lv_count_boot() -> i32 {
    if let Err(code) = opened() {
        return code;
    }
    match count_boot() {
        Ok(status) => status,
        Err((what, e)) => error().set(what, &e),
    }
}

fn record() -> Result<i32, Failure<HostError>> {
    let [t, readings @ .., alarm] = *frame();
    let mut batch = WriteBatch::<KEY_MAX, VAL_MAX, 5>::new();
    for (s, reading) in (0..SENSORS).zip(readings) {
        batch
            .put(&key(t, b'r', s), &reading.to_le_bytes())
            .map_err(|e| ("frame", e.widen()))?;
    }
    if let Ok(code) = u8::try_from(alarm) {
        let k = key(t, b'e', code);
        batch
            .put(&k, &format::value(&k))
            .map_err(|e| ("frame", e.widen()))?;
    }
    let mut attempts = 0;
    loop {
        match run(db().write(&batch)) {
            Ok(_) => break,
            Err(e) => match make_room(t, e)? {
                Room::Made => {}
                Room::NeedsArchive => {
                    stats().stalls += 1;
                    return Ok(NEEDS_ARCHIVE);
                }
            },
        }
        attempts += 1;
        if attempts == MAX_ATTEMPTS {
            return Err(("record", Error::NeedsCompaction));
        }
    }
    let s = stats();
    s.frames += 1;
    s.latest = s.latest.max(Some(t));
    // Bounded background work: one compaction step per frame.
    if db().compaction_pending() {
        scratch().purge_before = t;
        match run(db().compact_step(scratch())) {
            Ok(_) => stats().compaction_steps += 1,
            // Merging must wait for cold tables to leave; the frame is safe.
            Err(Error::RegionFull) => return Ok(COLD_WAITING),
            Err(e) => return Err(("compact", e)),
        }
    }
    Ok(if cold_waiting() {
        COLD_WAITING
    } else {
        RECORDED
    })
}

/// Records the frame in [`lv_frame_ptr`] durably.
///
/// One `WriteBatch`, whose WAL block is written and flushed before this
/// returns. Recording the
/// same frame twice is harmless (same keys, same values), which is what
/// makes resending unacknowledged frames after a crash safe.
///
/// Returns 0 when the frame is durable; 1 when it is durable and cold
/// tables are waiting for an archive pass; 2 when it is **not** recorded
/// because no slot is free until cold tables leave (archive, then record
/// it again); or `-1`.
#[unsafe(no_mangle)]
pub extern "C" fn lv_record() -> i32 {
    if let Err(code) = opened() {
        return code;
    }
    match record() {
        Ok(status) => status,
        Err((what, e)) => error().set(what, &e),
    }
}

fn stage() -> Result<i32, Failure<HostError>> {
    let Some(h) = horizon() else {
        return Ok(0);
    };
    for level in (0..LEVELS).rev() {
        let Some(table) = db()
            .level_tables(level)
            .unwrap_or(&[])
            .iter()
            .find(|t| t.last_key.as_slice() < h.as_slice())
            .copied()
        else {
            continue;
        };
        let Some(plan) = db().archive_plan(level, table.id) else {
            continue;
        };
        let blocks = usize::try_from(plan.table.block_count).unwrap_or(usize::MAX);
        if blocks > ARCHIVE_BLOCKS {
            return Err((
                "archive",
                Error::BufferTooSmall {
                    need: (blocks + 1) * BLOCK,
                },
            ));
        }
        let object = archive();
        object[..BLOCK].copy_from_slice(&encode_archive_header(&plan.sealed()));
        for (i, dst) in object[BLOCK..]
            .chunks_exact_mut(BLOCK)
            .take(blocks)
            .enumerate()
        {
            let id = plan.table.first_block + i as u64;
            run(poll_fn(|cx| db().device().poll_read_block(cx, id, dst)))
                .map_err(|e| ("archive read", Error::Device(e)))?;
        }
        *pending() = Some((level, table.id));
        let o = out();
        o[0] = f64::from(table.id);
        o[1] = level as f64;
        o[2] = ((blocks + 1) * BLOCK) as f64;
        o[3] = tick_of(table.first_key.as_slice()) as f64;
        o[4] = tick_of(table.last_key.as_slice()) as f64;
        return Ok(1);
    }
    Ok(0)
}

/// Stages the next cold table as an archive object.
///
/// A cold table holds only ticks older than the hot window. The object
/// goes to [`lv_archive_ptr`], and this writes
/// `[table id, level, object bytes, first tick, last tick]` to the result
/// buffer. Store the object durably, then call [`lv_archive_commit`].
///
/// Returns 1 when a table is staged, 0 when nothing is cold, or `-1`.
#[unsafe(no_mangle)]
pub extern "C" fn lv_archive_next() -> i32 {
    if let Err(code) = opened() {
        return code;
    }
    *pending() = None;
    match stage() {
        Ok(n) => n,
        Err((what, e)) => error().set(what, &e),
    }
}

/// Drops the table [`lv_archive_next`] staged from flash, in one atomic
/// manifest commit. Call it only once the archive object is durable.
///
/// Returns 1 when the table left the flash; 0 when it had already gone (a
/// compaction merged it while the object was being stored; the object is
/// then a harmless extra copy); 2 when horton refused because removing it
/// would bring deleted data back; or `-1`.
#[unsafe(no_mangle)]
pub extern "C" fn lv_archive_commit() -> i32 {
    if let Err(code) = opened() {
        return code;
    }
    let Some((level, id)) = pending().take() else {
        return error().set(
            "archive commit",
            &"nothing staged: call lv_archive_next first",
        );
    };
    let blocks = db()
        .archive_plan(level, id)
        .map_or(0, |p| u64::from(p.table.block_count));
    match run(db().archive_commit(level, id)) {
        Ok(true) => {
            let s = stats();
            s.archived_tables += 1;
            s.archived_blocks += blocks;
            1
        }
        Ok(false) => 0,
        Err(Error::WouldResurrect { .. }) => {
            stats().archive_refusals += 1;
            2
        }
        Err(e) => error().set("archive commit", &e),
    }
}

/// Marks which parts of each tick's frame the flash holds.
///
/// The map goes to [`lv_cover_ptr`] (see `common::cover`), and `[first
/// tick, ticks, entries, bad values]` to the result buffer. Returns the
/// ticks covered, or `-1`.
#[unsafe(no_mangle)]
pub extern "C" fn lv_cover() -> i32 {
    if let Err(code) = opened() {
        return code;
    }
    ret(common::cover(db(), cover_bits()).map(|c| coverage_out(&c, None)))
}

fn coverage_out(c: &common::Coverage, table: Option<u32>) -> usize {
    let o = out();
    o[0] = c.first.map_or(f64::NAN, |t| t as f64);
    o[1] = c.len as f64;
    o[2] = c.entries as f64;
    o[3] = c.bad_values as f64;
    o[4] = table.map_or(f64::NAN, f64::from);
    c.len
}

fn read_back(bytes: usize) -> Result<usize, Failure<OutOfRange>> {
    let sealed = decode_archive_header(&archive()[..BLOCK])
        .map_err(|_| ("archive object", Error::CorruptBlock { id: 0 }))?;
    let blocks = usize::try_from(sealed.block_count).unwrap_or(usize::MAX);
    if blocks > ARCHIVE_BLOCKS || bytes != (blocks + 1) * BLOCK {
        return Err((
            "archive object size",
            Error::BufferTooSmall {
                need: blocks.saturating_add(1).saturating_mul(BLOCK),
            },
        ));
    }
    // A fresh scratch database of the recorder's shape, on erased flash.
    verify_image().fill(0xFF);
    *verify_db() = RecorderDb::new(RamDisk, config());
    run(verify_db().open()).map_err(|e| ("scratch open", e))?;
    run(verify_db().ingest_table(&sealed, &ArchiveSrc, 0)).map_err(|e| ("ingest", e))?;
    let c = common::cover(verify_db(), cover_bits())?;
    Ok(coverage_out(&c, Some(sealed.id)))
}

/// Reads back an archive object and marks its coverage.
///
/// The object is the `bytes` JavaScript copied into [`lv_archive_ptr`].
/// This checks its header, ingests it into a
/// scratch database of the recorder's shape (which verifies every block's
/// CRC), and marks its coverage as [`lv_cover`] does, adding the table id
/// as a fifth value. Returns the ticks covered, or `-1`.
#[unsafe(no_mangle)]
pub extern "C" fn lv_archive_cover(bytes: usize) -> i32 {
    ret(read_back(bytes))
}

/// Writes the logger's counters to the result buffer.
///
/// `[frames, flushes, compaction steps, archived tables, archived blocks,
/// archive refusals, stalls, newest tick, slots used, slots free]`.
/// Returns how many values, or `-1`.
#[unsafe(no_mangle)]
pub extern "C" fn lv_stats() -> i32 {
    if let Err(code) = opened() {
        return code;
    }
    let s = *stats();
    let slots = db().slot_stats();
    let o = out();
    o[0] = s.frames as f64;
    o[1] = s.flushes as f64;
    o[2] = s.compaction_steps as f64;
    o[3] = s.archived_tables as f64;
    o[4] = s.archived_blocks as f64;
    o[5] = s.archive_refusals as f64;
    o[6] = s.stalls as f64;
    o[7] = s.latest.map_or(f64::NAN, |t| t as f64);
    o[8] = f64::from(slots.used);
    o[9] = f64::from(slots.free);
    10
}

/// Summary of the flash; `common::summary` has the layout.
#[unsafe(no_mangle)]
pub extern "C" fn lv_summary() -> i32 {
    let report = match opened() {
        Ok(r) => r,
        Err(code) => return code,
    };
    ret(common::summary(db(), &report, out()))
}

/// `count` ticks from `from`, one row per tick; `common::frames` has the
/// layout. Returns the rows written, or `-1`.
#[unsafe(no_mangle)]
pub extern "C" fn lv_frames(from: u64, count: u32, now: u64) -> i32 {
    if let Err(code) = opened() {
        return code;
    }
    ret(common::frames(db(), from, count, now, out()))
}

/// Checks every entry on the flash; `common::verify` has the layout. When
/// the invariants fail, the error buffer holds why.
#[unsafe(no_mangle)]
pub extern "C" fn lv_verify() -> i32 {
    if let Err(code) = opened() {
        return code;
    }
    ret(common::verify(db(), out()).map(|(n, broken)| {
        if let Some(why) = broken {
            error().set("invariants", &why);
        }
        n
    }))
}

#[panic_handler]
fn panic(info: &core::panic::PanicInfo<'_>) -> ! {
    // horton returns errors instead of panicking, so reaching this is a
    // bug. Leave where and why in the error buffer, then trap, which
    // JavaScript sees as a `RuntimeError` and reports with the message.
    let e = error();
    e.len = 0;
    let _ = core::fmt::Write::write_fmt(e, format_args!("panic: {info}"));
    core::arch::wasm32::unreachable()
}
