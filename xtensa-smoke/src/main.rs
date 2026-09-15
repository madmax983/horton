//! Horton ESP32-S3 smoke test.
//!
//! Bare-metal xtensa binary: the ROM bootloader jumps to `_start`, which
//! installs a stack, zeroes `.bss`, and enters `smoke_main`. That drives a
//! [`Db`] through put/get/delete/flush/compact/scan/snapshot against a
//! RAM-backed [`BlockDevice`] and prints `SMOKE PASS` (or the failure) on
//! UART0, which QEMU captures. Any panic also reports `SMOKE FAIL`.

#![no_std]
#![no_main]
#![feature(asm_experimental_arch)]

use core::arch::asm;
use core::fmt::{self, Write as _};
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use horton::compact::{Compaction, Progress};
use horton::device::BlockDevice;
use horton::{Config, Db, Scan};

// ---------------------------------------------------------------------------
// UART0: the ROM bootloader leaves it clocked and configured; just feed the
// TX FIFO. STATUS[25:16] is the TX FIFO fill level (depth 128).
// ---------------------------------------------------------------------------

const UART0_BASE: usize = 0x6000_0000;

struct Uart;

impl Uart {
    fn putc(b: u8) {
        unsafe {
            while (core::ptr::read_volatile((UART0_BASE + 0x1C) as *const u32) >> 16) & 0xFF >= 128
            {
                core::hint::spin_loop();
            }
            core::ptr::write_volatile(UART0_BASE as *mut u32, u32::from(b));
        }
    }
}

impl fmt::Write for Uart {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        for &b in s.as_bytes() {
            if b == b'\n' {
                Self::putc(b'\r');
            }
            Self::putc(b);
        }
        Ok(())
    }
}

#[panic_handler]
fn panic(info: &core::panic::PanicInfo) -> ! {
    if let Some(loc) = info.location() {
        let _ = writeln!(
            Uart,
            "\nSMOKE FAIL: panic at {}:{}:{}",
            loc.file(),
            loc.line(),
            loc.column()
        );
    } else {
        let _ = writeln!(Uart, "\nSMOKE FAIL: panic (no location)");
    }
    let _ = writeln!(Uart, "msg: {}", info.message());
    halt();
}

fn halt() -> ! {
    loop {
        core::hint::spin_loop();
    }
}

// ---------------------------------------------------------------------------
// Entry: like Tallow's — deaf to interrupts, known PS, own stack.
// ---------------------------------------------------------------------------

const STACK_TOP: u32 = 0x3FCF_FFE0;

#[no_mangle]
pub extern "C" fn _start() -> ! {
    unsafe {
        asm!(
            "movi {t}, 0",
            "wsr {t}, INTENABLE",
            "movi {t}, 0x40",
            "slli {t}, {t}, 12",
            "wsr {t}, PS",
            "rsync",
            "mov a1, {top}",
            t = out(reg) _,
            top = in(reg) STACK_TOP,
            options(nostack, nomem),
        );
    }
    unsafe { smoke_main() }
}

// ---------------------------------------------------------------------------
// Minimal block_on: everything we drive is always Ready.
// ---------------------------------------------------------------------------

fn block_on<F: Future>(f: F) -> F::Output {
    unsafe fn clone(_: *const ()) -> RawWaker {
        RawWaker::new(core::ptr::null(), &vtable())
    }
    unsafe fn noop(_: *const ()) {}
    const fn vtable() -> &'static RawWakerVTable {
        &RawWakerVTable::new(clone, noop, noop, noop)
    }
    let waker = unsafe { Waker::from_raw(RawWaker::new(core::ptr::null(), vtable())) };
    let mut cx = Context::from_waker(&waker);
    let mut f = f;
    let mut pinned = unsafe { Pin::new_unchecked(&mut f) };
    loop {
        match pinned.as_mut().poll(&mut cx) {
            Poll::Ready(v) => return v,
            Poll::Pending => core::hint::spin_loop(),
        }
    }
}

// ---------------------------------------------------------------------------
// RAM-backed block device: 34 blocks of 4 KiB in .bss (136 KiB).
// ---------------------------------------------------------------------------

const NBLOCKS: usize = 34;

static mut RAMDISK: [[u8; 4096]; NBLOCKS] = [[0; 4096]; NBLOCKS];

struct RamDevice;

impl BlockDevice for RamDevice {
    type Error = core::convert::Infallible;
    const BLOCK: usize = 4096;

    fn poll_read_block(
        &self,
        _cx: &mut Context<'_>,
        id: u64,
        buf: &mut [u8],
    ) -> Poll<Result<(), Self::Error>> {
        assert!((id as usize) < NBLOCKS && buf.len() == Self::BLOCK);
        unsafe {
            buf.copy_from_slice(&RAMDISK[id as usize]);
        }
        Poll::Ready(Ok(()))
    }

    fn poll_write_block(
        &mut self,
        _cx: &mut Context<'_>,
        id: u64,
        buf: &[u8],
    ) -> Poll<Result<(), Self::Error>> {
        assert!((id as usize) < NBLOCKS && buf.len() == Self::BLOCK);
        unsafe {
            RAMDISK[id as usize].copy_from_slice(buf);
        }
        Poll::Ready(Ok(()))
    }

    fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

// ---------------------------------------------------------------------------
// The database: small profile, regions manifest 0/1, WAL [2,16), tables
// [16,34). Every put commits its own WAL block ("durable before it
// returns"), so the WAL region must cover every put between flushes.
// ---------------------------------------------------------------------------

type SmokeDb = Db<RamDevice, 4096, 32, 64, 16, 2048, 4, 4, 64, 64>;
type SmokeCompaction = Compaction<4096, 32, 64, 64>;

static mut DB: SmokeDb = SmokeDb::new(RamDevice, Config::new(2, 16, 16, 34, 0, 1));
static mut COMPACTION: SmokeCompaction = SmokeCompaction::new();

fn check(cond: bool, msg: &str) {
    if !cond {
        let _ = writeln!(Uart, "SMOKE FAIL: {msg}");
        halt();
    }
}

/// # Safety
///
/// Call exactly once, from `_start` on the boot stack.
unsafe fn smoke_main() -> ! {
    // .bss is NOLOAD: zero it before any static (RAMDISK, DB) is read.
    unsafe {
        extern "C" {
            static mut _bss_start: u8;
            static mut _bss_end: u8;
        }
        let mut p = core::ptr::addr_of_mut!(_bss_start);
        let end = core::ptr::addr_of_mut!(_bss_end);
        while p < end {
            core::ptr::write_volatile(p, 0);
            p = p.add(1);
        }
    }

    let _ = writeln!(Uart, "horton ESP32-S3 smoke: booted");
    let db = unsafe { &mut *core::ptr::addr_of_mut!(DB) };

    // 1. Open a fresh database.
    let report = block_on(db.open()).expect("open");
    check(
        report.recovered_records == 0 && report.l0_tables == 0,
        "expected a fresh database",
    );
    let _ = writeln!(Uart, "open: fresh ok");

    // 2. Put 8 keys, read them back.
    for i in 0..8u8 {
        let k = [b'k', b'0' + i];
        let v = [b'v', b'0' + i];
        block_on(db.put(&k, &v)).expect("put");
    }
    let mut buf = [0u8; 64];
    for i in 0..8u8 {
        let k = [b'k', b'0' + i];
        let n = block_on(db.get(&k, &mut buf)).expect("get").expect("found");
        check(&buf[..n] == [b'v', b'0' + i], "get mismatch");
    }
    let _ = writeln!(Uart, "put/get: 8 keys ok");

    // 3. Snapshot, then mutate: snapshot must keep seeing the old value.
    let snap = db.snapshot().expect("snapshot");
    block_on(db.put(b"k0", b"NEW")).expect("overwrite");
    block_on(db.delete(b"k1")).expect("delete");
    let n = block_on(db.get_at(b"k0", &mut buf, snap))
        .expect("get_at")
        .expect("found");
    check(&buf[..n] == b"v0", "snapshot isolation broken");
    let n = block_on(db.get(b"k0", &mut buf)).expect("get").expect("found");
    check(&buf[..n] == b"NEW", "live view mismatch");
    check(block_on(db.get(b"k1", &mut buf)).expect("get").is_none(), "delete missed");
    let _ = writeln!(Uart, "snapshot isolation ok");

    // 4. Flush to an SSTable, then compact it down a level.
    block_on(db.flush()).expect("flush");
    let scratch = unsafe { &mut *core::ptr::addr_of_mut!(COMPACTION) };
    loop {
        match block_on(db.compact_step(scratch)).expect("compact_step") {
            Progress::Done => break,
            Progress::More => {}
        }
    }
    let n = block_on(db.get(b"k0", &mut buf)).expect("get").expect("found");
    check(&buf[..n] == b"NEW", "post-compaction mismatch");
    let _ = writeln!(Uart, "flush+compact ok");

    // 5. Full scan: 7 live keys (k1 deleted), in order.
    let mut scan = Scan::new(db);
    block_on(scan.seek(b"", None, u64::MAX)).expect("seek");
    let mut kb = [0u8; 32];
    let mut vb = [0u8; 64];
    let mut count = 0u32;
    let mut prev = [0u8; 32];
    let mut prev_len = 0usize;
    while let Some((kn, _)) = block_on(scan.next(&mut kb, &mut vb)).expect("next") {
        if prev_len > 0 {
            check(&kb[..kn] > &prev[..prev_len], "scan out of order");
        }
        prev[..kn].copy_from_slice(&kb[..kn]);
        prev_len = kn;
        count += 1;
    }
    check(count == 7, "scan count mismatch");
    let _ = writeln!(Uart, "scan: 7 keys in order ok");

    db.release_snapshot(snap);
    let _ = writeln!(Uart, "SMOKE PASS");
    halt();
}
