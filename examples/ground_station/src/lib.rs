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

#[allow(dead_code, reason = "shared with the live logger")]
mod common;

use core::task::{Context, Poll};

use horton::{BlockDevice, OpenReport};

use common::{ErrorText, OUT_LEN, run};
use format::{BLOCK, RecorderDb, SECTORS, TTL_TICKS, config};

/// Bytes in a flash dump: the recorder's whole partition.
const IMAGE_LEN: usize = SECTORS * BLOCK;

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
static mut ERROR: ErrorText = ErrorText::new();

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

/// Fails unless [`gs_open`] has succeeded since the last image load.
fn opened() -> Result<OpenReport, i32> {
    // SAFETY: see the crate docs; a copy out of the static.
    unsafe { REPORT }.ok_or_else(|| error().set("not open", &"call gs_open first"))
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
        Err(e) => error().set("open", &e),
    }
}

/// Fills the result buffer with a summary of the open image (see
/// `common::summary` for the layout). Returns the number of values
/// written, or `-1`.
#[unsafe(no_mangle)]
pub extern "C" fn gs_summary() -> i32 {
    let report = match opened() {
        Ok(r) => r,
        Err(code) => return code,
    };
    match common::summary(db(), &report, out()) {
        Ok(n) => i32::try_from(n).unwrap_or(i32::MAX),
        Err((what, e)) => error().set(what, &e),
    }
}

/// Reads `count` ticks starting at `from` into the result buffer.
///
/// One row per tick; `common::frames` has the layout. The recorder's clock
/// is its tick, so pass the newest tick as `now`. Returns the rows
/// written, or `-1`.
#[unsafe(no_mangle)]
pub extern "C" fn gs_frames(from: u64, count: u32, now: u64) -> i32 {
    if let Err(code) = opened() {
        return code;
    }
    match common::frames(db(), from, count, now, out()) {
        Ok(n) => i32::try_from(n).unwrap_or(i32::MAX),
        Err((what, e)) => error().set(what, &e),
    }
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
    match common::reading(db(), tick, sensor, out()) {
        Ok(found) => i32::from(found),
        Err((what, e)) => error().set(what, &e),
    }
}

/// Checks every entry on the flash, the way the recorder's verifier does.
///
/// `common::verify` has the layout. Returns the number of values written,
/// or `-1`. When the invariants fail, the error buffer holds why.
#[unsafe(no_mangle)]
pub extern "C" fn gs_verify() -> i32 {
    if let Err(code) = opened() {
        return code;
    }
    match common::verify(db(), out()) {
        Ok((n, broken)) => {
            if let Some(why) = broken {
                error().set("invariants", &why);
            }
            i32::try_from(n).unwrap_or(i32::MAX)
        }
        Err((what, e)) => error().set(what, &e),
    }
}

/// The recorder's debug-trace lifetime, in ticks, for the page's legend.
#[unsafe(no_mangle)]
pub const extern "C" fn gs_ttl_ticks() -> u64 {
    TTL_TICKS
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
