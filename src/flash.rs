//! SPI-flash-backed [`BlockDevice`] support.
//!
//! NOR flash cannot overwrite in place: a sector must be erased (all bits
//! to 1) before programming (which only clears bits, 1 → 0). [`Flash`] is
//! the tiny hardware trait a board implements — the only half that needs
//! `unsafe` (MMIO register pokes), so it lives outside this crate — and
//! [`FlashBlockDevice`] is the safe erase-aware `BlockDevice` wrapper.
//!
//! Horton's block size equals the flash sector size (4096), so every block
//! write is exactly one sector: erase it, then program it. No
//! read-modify-write and no hidden RAM.
//!
//! # Endurance
//!
//! Every block write costs one sector erase, so wear follows the write
//! pattern of each region (figures assume W25Q-class NOR: 100k P/E
//! cycles, 45–400 ms per 4 KiB erase):
//!
//! - **WAL.** Each durable mutation (`put`, `delete`, one
//!   [`WriteBatch`](crate::WriteBatch)) commits one whole block and moves
//!   on, so a WAL sector is erased once per `wal_blocks` commits: the
//!   region lasts about `100k × wal_blocks` commits, and erase time caps
//!   durable commits at roughly 20 per second. **Group commit** is the
//!   lever: a [`WriteBatch`](crate::WriteBatch) puts every op that fits one
//!   block into a single commit (one erase instead of one per op).
//! - **Manifest.** Every flush, compaction output, archive, and ingest
//!   rewrites one manifest copy. With the default two copies each copy is
//!   erased every other commit, which makes the manifest the first region
//!   to wear out. [`Config::with_manifest_ring`](crate::Config::with_manifest_ring)
//!   rotates commits across `N` copies, multiplying the manifest's life by
//!   `N / 2`.
//! - **Tables.** Slots are allocated next-fit from a rotating hint
//!   ([`SlotMap`](crate::SlotMap)), so successive tables land in
//!   successive slots and erases spread across the whole table region.
//!
//! There is no NOR page-program append path: the WAL never programs into
//! an already-erased sector, so a mutation always pays a full erase. See
//! `docs/adr/0012-nor-flash-endurance.md` for why that stays out of scope
//! for now.

use core::fmt;
use core::marker::PhantomData;
use core::task::{Context, Poll};

use crate::device::BlockDevice;

/// The board-level NOR flash primitive.
///
/// All addresses are byte addresses into the flash region this device owns.
/// Implementors talk to the SPI controller; callers never do.
///
/// Contract:
/// - [`Flash::read`] copies `buf.len()` bytes from `addr`.
/// - [`Flash::erase_sector`] sets the whole `SECTOR`-byte sector containing
///   `addr` to `0xFF`. `addr` must be sector-aligned.
/// - [`Flash::program`] clears bits (`1 → 0`) for `data` at `addr`; setting
///   a bit (`0 → 1`) without an intervening erase is a device error.
///   Callers erase the sector first — [`FlashBlockDevice`] always does.
pub trait Flash {
    /// The board's error type.
    type Error;

    /// Erase-sector size in bytes. Must equal the `BlockDevice::BLOCK`
    /// used with [`FlashBlockDevice`] (4096 on ESP32-S3).
    const SECTOR: usize;

    /// Read `buf.len()` bytes from `addr` into `buf`.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when the range is out of bounds or the
    /// controller reports a failure.
    fn read(&self, addr: u32, buf: &mut [u8]) -> Result<(), Self::Error>;

    /// Erase the sector at `addr` (must be sector-aligned) to `0xFF`.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] on a misaligned address, an out-of-bounds
    /// range, or a controller failure.
    fn erase_sector(&mut self, addr: u32) -> Result<(), Self::Error>;

    /// Program `data` at `addr`, clearing bits only.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when any bit would need setting (`0 → 1`),
    /// the range is out of bounds, or the controller reports a failure.
    fn program(&mut self, addr: u32, data: &[u8]) -> Result<(), Self::Error>;
}

/// Error from [`FlashBlockDevice`]: either the board's flash error or an
/// addressing bug caught before touching hardware.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlashError<E> {
    /// The board's flash primitive failed.
    Device(E),
    /// Block id outside the device's range.
    BlockOutOfRange {
        /// The offending block id.
        id: u64,
    },
    /// Address arithmetic overflowed `u32`.
    AddrOverflow,
}

impl<E: fmt::Debug> fmt::Display for FlashError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Device(e) => write!(f, "flash device error: {e:?}"),
            Self::BlockOutOfRange { id } => write!(f, "block {id} out of range"),
            Self::AddrOverflow => write!(f, "flash address overflow"),
        }
    }
}

/// A [`BlockDevice`] over NOR flash: owns `N` consecutive sectors starting
/// at `base` (a byte address, sector-aligned).
///
/// `BLOCK` must equal `F::SECTOR` — checked in [`new`](Self::new).
/// `poll_write_block` erases the sector then programs it; both complete
/// synchronously, so the `Poll` is always `Ready`. `poll_flush` is a no-op:
/// programmed flash is durable.
pub struct FlashBlockDevice<F: Flash, const BLOCK: usize, const N: usize> {
    flash: F,
    base: u32,
    _pd: PhantomData<F>,
}

impl<F: Flash, const BLOCK: usize, const N: usize> FlashBlockDevice<F, BLOCK, N> {
    /// Owns sectors `[base, base + N * BLOCK)` of `flash`.
    ///
    /// # Panics
    ///
    /// Panics when `base` is not sector-aligned or `BLOCK != F::SECTOR`.
    /// Both are board-configuration bugs; fail fast, never misprogram.
    pub const fn new(flash: F, base: u32) -> Self {
        assert!((base as usize).is_multiple_of(F::SECTOR));
        assert!(BLOCK == F::SECTOR);
        Self {
            flash,
            base,
            _pd: PhantomData,
        }
    }

    /// Byte address of block `id`, or the addressing error.
    fn addr_of(&self, id: u64) -> Result<u32, FlashError<F::Error>> {
        if id >= N as u64 {
            return Err(FlashError::BlockOutOfRange { id });
        }
        let off = id * BLOCK as u64;
        let addr = u64::from(self.base) + off;
        u32::try_from(addr).map_err(|_| FlashError::AddrOverflow)
    }
}

impl<F: Flash, const BLOCK: usize, const N: usize> BlockDevice for FlashBlockDevice<F, BLOCK, N> {
    type Error = FlashError<F::Error>;
    const BLOCK: usize = BLOCK;

    fn poll_read_block(
        &self,
        _cx: &mut Context<'_>,
        id: u64,
        buf: &mut [u8],
    ) -> Poll<Result<(), Self::Error>> {
        let r = self
            .addr_of(id)
            .and_then(|a| self.flash.read(a, buf).map_err(FlashError::Device));
        Poll::Ready(r)
    }

    fn poll_write_block(
        &mut self,
        _cx: &mut Context<'_>,
        id: u64,
        buf: &[u8],
    ) -> Poll<Result<(), Self::Error>> {
        let r = self.addr_of(id).and_then(|a| {
            self.flash
                .erase_sector(a)
                .and_then(|()| self.flash.program(a, buf))
                .map_err(FlashError::Device)
        });
        Poll::Ready(r)
    }

    fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Programmed NOR flash is durable; nothing to flush.
        Poll::Ready(Ok(()))
    }
}
