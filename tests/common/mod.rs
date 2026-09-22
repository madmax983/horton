//! Shared test harnesses: a tiny `block_on`, an in-memory block device,
//! and a crash-injecting wrapper. `std` is fine here — tests only.
//!
//! Each integration-test binary includes this module but uses only a subset
//! of its items, so per-binary dead-code warnings are expected and silenced.
#![allow(dead_code)]

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use horton::BlockDevice;

/// A waker that does nothing. Good for hand-polling a future in a test.
pub fn noop_waker() -> Waker {
    // SAFETY: all callbacks are no-ops that never touch the null data pointer.
    const unsafe fn waker_clone(data: *const ()) -> RawWaker {
        RawWaker::new(data, &WAKER_VTABLE)
    }
    // SAFETY: no-op; never touches the data pointer.
    const unsafe fn waker_noop(_data: *const ()) {}
    static WAKER_VTABLE: RawWakerVTable =
        RawWakerVTable::new(waker_clone, waker_noop, waker_noop, waker_noop);
    // SAFETY: the vtable above is valid and its callbacks never dereference data.
    unsafe { Waker::from_raw(RawWaker::new(core::ptr::null(), &WAKER_VTABLE)) }
}

/// Drives a future to completion on the current thread with a no-op waker.
///
/// Our test devices always return `Poll::Ready`, so this never spins in
/// practice; the `yield_now` is just good manners for `Pending`.
pub fn block_on<F: Future>(future: F) -> F::Output {
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut future = future;
    // SAFETY: `future` is never moved after pinning and is polled to
    // completion on this thread.
    let mut pinned = unsafe { Pin::new_unchecked(&mut future) };
    loop {
        match pinned.as_mut().poll(&mut cx) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

/// In-memory block device: a growable vector of zeroed blocks. Reads of
/// never-written blocks return zeros (sparse). Always `Ready`.
#[derive(Debug)]
pub struct MemDevice<const BLOCK: usize> {
    blocks: Vec<[u8; BLOCK]>,
}

impl<const BLOCK: usize> MemDevice<BLOCK> {
    /// Creates an empty device.
    // Not `const`: `Vec` cannot be built in const context (test-only helper).
    #[allow(clippy::missing_const_for_fn)]
    pub fn new() -> Self {
        Self { blocks: Vec::new() }
    }

    /// Test-only: raw block access for fault injection (fuzzing, torn
    /// writes). Reads of never-written blocks are zeros; the vec only
    /// holds blocks that were actually written.
    // Not `const` on stable: `&mut` receivers in const fn need
    // `const_mut_refs` (test-only helper).
    #[allow(clippy::missing_const_for_fn)]
    pub fn blocks_mut(&mut self) -> &mut Vec<[u8; BLOCK]> {
        &mut self.blocks
    }
}

impl<const BLOCK: usize> Default for MemDevice<BLOCK> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const BLOCK: usize> BlockDevice for MemDevice<BLOCK> {
    type Error = core::convert::Infallible;
    const BLOCK: usize = BLOCK;

    fn poll_read_block(
        &self,
        _cx: &mut Context<'_>,
        id: u64,
        buf: &mut [u8],
    ) -> Poll<Result<(), Self::Error>> {
        let mut block = [0u8; BLOCK];
        if let Some(i) = usize::try_from(id).ok().and_then(|i| self.blocks.get(i)) {
            block.copy_from_slice(i);
        }
        let n = buf.len().min(BLOCK);
        buf[..n].copy_from_slice(&block[..n]);
        Poll::Ready(Ok(()))
    }

    fn poll_write_block(
        &mut self,
        _cx: &mut Context<'_>,
        id: u64,
        buf: &[u8],
    ) -> Poll<Result<(), Self::Error>> {
        if let Ok(i) = usize::try_from(id)
            && buf.len() == BLOCK
        {
            while self.blocks.len() <= i {
                self.blocks.push([0u8; BLOCK]);
            }
            self.blocks[i].copy_from_slice(buf);
        }
        Poll::Ready(Ok(()))
    }

    fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

/// Crash injector: wraps a device and silently drops every `poll_write_block`
/// with index `>= crash_at` (returning `Ok`, as if the process died right
/// after the call). Recovery then sees the truncated prefix.
#[derive(Debug)]
pub struct CrashDevice<D, const BLOCK: usize> {
    inner: D,
    crash_at: usize,
    writes: usize,
}

impl<D, const BLOCK: usize> CrashDevice<D, BLOCK> {
    /// Wraps `inner`; writes numbered `crash_at`, `crash_at + 1`, … are dropped.
    pub const fn new(inner: D, crash_at: usize) -> Self {
        Self {
            inner,
            crash_at,
            writes: 0,
        }
    }

    /// Returns the wrapped device.
    pub fn into_inner(self) -> D {
        self.inner
    }
}

impl<D: BlockDevice, const BLOCK: usize> BlockDevice for CrashDevice<D, BLOCK> {
    type Error = D::Error;
    const BLOCK: usize = BLOCK;

    fn poll_read_block(
        &self,
        cx: &mut Context<'_>,
        id: u64,
        buf: &mut [u8],
    ) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_read_block(cx, id, buf)
    }

    fn poll_write_block(
        &mut self,
        cx: &mut Context<'_>,
        id: u64,
        buf: &[u8],
    ) -> Poll<Result<(), Self::Error>> {
        if self.writes >= self.crash_at {
            return Poll::Ready(Ok(())); // the crash: data never lands
        }
        self.writes += 1;
        self.inner.poll_write_block(cx, id, buf)
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_flush(cx)
    }
}

/// `Db` with the standard test geometry: 4 KiB blocks, 256 B keys,
/// 1 KiB values, 64 slots, 4 KiB arena, 7 levels, 4 L0 tables, 1024-byte
/// bloom filters, and a 4096-entry free list (covers the whole table
/// region).
pub type TestDb<D> = horton::Db<D, 4096, 256, 1024, 64, 4096, 7, 4, 1024, 4096>;

/// Standard region layout: manifest slots 0/1, WAL `[8, 136)`, tables
/// `[136, 4224)`. The regions are disjoint by construction.
pub const fn test_config() -> horton::Config {
    horton::Config::new(8, 136, 136, 4224, 0, 1)
}

/// Tiny deterministic PRNG (LCG) — no `rand` dependency, even for tests.
pub struct Lcg(u64);

impl Lcg {
    /// Creates a generator from `seed`.
    pub const fn new(seed: u64) -> Self {
        Self(seed)
    }

    /// Returns the next pseudorandom value.
    // Not `const`: mutates the generator state (test-only helper).
    #[allow(clippy::missing_const_for_fn)]
    pub fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 33
    }
}
