//! ESP32-S3 SPI flash driver: [`Flash`] over the `SPI_MEM` peripheral.
//!
//! This is the real-silicon half of the v0.6 board seam — the register
//! programming that drives an actual NOR chip through SPI1. It stays
//! `#![no_std]` + `#![no_alloc]` + core-only like the rest of the crate,
//! and it contains no `unsafe`: the driver is generic over [`RegBus`]
//! (register read/write), so the half-dozen lines of volatile MMIO live
//! in the board crate, while every command sequence here is unit-tested
//! on the host against an emulated NOR chip (`tests/esp32s3.rs`).
//!
//! Register map and command sequences follow ESP-IDF v5.2's low-level
//! layer verbatim (`components/hal/esp32s3/include/hal/spimem_flash_ll.h`
//! and `spi_flash_hal_iram.c`), not the TRM prose:
//!
//! - erase: `WREN` → `ADDR` → save `CTRL` / `CTRL = 0` → dedicated
//!   `FLASH_SE` bit → spin on its self-clear → restore `CTRL` → `RDSR`,
//!   poll `WIP`.
//! - program: `WREN`, then per chunk — at most 64 bytes (the `W0`–`W15`
//!   buffer) and never crossing a 256-byte page — `ADDR` with the length
//!   in bits 31:24, words into `W0`.., `usr_dummy = 0`, dedicated
//!   `FLASH_PP` bit → spin → poll `WIP`.
//! - read: user-mode `0x03` transactions (command + 24-bit address +
//!   MISO phases), ≤ 64 bytes each. This deliberately bypasses the DROM
//!   flash cache, so no cache maintenance is needed after program/erase:
//!   reads are correct by construction, not by invalidation protocol.
//!
//! Every `WREN` is followed by a `WEL` check (`Error::WriteEnableFailed`)
//! and every poll is bounded (`Error::Timeout`). QEMU's `esp32s3`
//! machine does not emulate the `SPI_MEM` user-command path (see
//! SPEC.md §9, v0.7), so on-target proof of these sequences needs
//! silicon; the host mock is the executable proof in the meantime.

use crate::flash::Flash;

/// SPI1 (flash) peripheral base on the ESP32-S3.
pub const SPI1_BASE: usize = 0x6000_2000;

/// `SPI_MEM` register offsets (from `SPI1_BASE`).
pub const REG_CMD: usize = 0x00;
/// `SPI_MEM` register offsets (from `SPI1_BASE`).
pub const REG_ADDR: usize = 0x04;
/// `SPI_MEM` register offsets (from `SPI1_BASE`).
pub const REG_CTRL: usize = 0x08;
/// `SPI_MEM` register offsets (from `SPI1_BASE`).
pub const REG_USER: usize = 0x18;
/// `SPI_MEM` register offsets (from `SPI1_BASE`).
pub const REG_USER1: usize = 0x1C;
/// `SPI_MEM` register offsets (from `SPI1_BASE`).
pub const REG_USER2: usize = 0x20;
/// `SPI_MEM` register offsets (from `SPI1_BASE`).
pub const REG_MISO_DLEN: usize = 0x28;
/// `SPI_MEM` register offsets (from `SPI1_BASE`).
pub const REG_RD_STATUS: usize = 0x2C;
/// First `SPI_MEM` data-buffer register (`W0`–`W15` are consecutive words).
pub const REG_W0: usize = 0x58;

/// `CMD` register command bits.
pub const CMD_WREN: u32 = 1 << 30;
/// `CMD` register command bits.
pub const CMD_RDSR: u32 = 1 << 27;
/// `CMD` register command bits.
pub const CMD_PP: u32 = 1 << 25;
/// `CMD` register command bits.
pub const CMD_SE: u32 = 1 << 24;
/// `CMD` register command bits.
pub const CMD_USR: u32 = 1 << 18;

/// Standard SPI "read data" opcode, used for user-mode reads.
pub const OP_READ: u8 = 0x03;

/// `USER` register phase-enable bits.
pub const USER_USR_COMMAND: u32 = 1 << 31;
/// `USER` register phase-enable bits.
pub const USER_USR_ADDR: u32 = 1 << 30;
/// `USER` register phase-enable bits.
pub const USER_USR_DUMMY: u32 = 1 << 29;
/// `USER` register phase-enable bits.
pub const USER_USR_MISO: u32 = 1 << 28;

/// NOR status-register bits (as reported by `RDSR`).
pub const STATUS_WIP: u32 = 1 << 0;
/// NOR status-register bits (as reported by `RDSR`).
pub const STATUS_WEL: u32 = 1 << 1;

/// Bytes per page-program chunk: the `W0`–`W15` buffer holds 16 words.
const PP_CHUNK: u32 = 64;
/// NOR page size: one program chunk never crosses a page boundary.
const PAGE: u32 = 256;
/// Sector size as `u32` for register arithmetic (`Flash::SECTOR` widens it).
const SECTOR_U32: u32 = 4096;
/// Spin cap for controller-side command completion (ns–µs on silicon).
const CMD_SPIN_MAX: u32 = 1_000_000;
/// Cap on `RDSR` polls while `WIP` is set (each poll is one SPI
/// transaction; sector erase is the slow one, up to ~1 s worst case).
const WIP_POLL_MAX: u32 = 1_000_000;

/// The register bus the driver talks through.
///
/// Reads and writes both take `&self`: volatile MMIO is interior
/// mutability by nature, and this keeps [`Flash::read`] (`&self`)
/// implementable. The board crate provides the half-dozen lines of
/// `unsafe` volatile access; tests provide a mock.
pub trait RegBus {
    /// Read the 32-bit register at `SPI1_BASE + offset`.
    fn read(&self, offset: usize) -> u32;
    /// Write the 32-bit register at `SPI1_BASE + offset`.
    fn write(&self, offset: usize, value: u32);
}

/// Driver errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpiError {
    /// Address or length outside the owned `[base, base + len)` region,
    /// or address arithmetic overflow.
    OutOfRange,
    /// `erase_sector` address not sector-aligned.
    Misaligned,
    /// A command bit never self-cleared, or `WIP` never cleared.
    Timeout,
    /// `WREN` did not set the chip's write-enable latch.
    WriteEnableFailed,
}

impl core::fmt::Display for SpiError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::OutOfRange => write!(f, "address outside the owned flash region"),
            Self::Misaligned => write!(f, "erase address not sector-aligned"),
            Self::Timeout => write!(f, "SPI flash command timed out"),
            Self::WriteEnableFailed => write!(f, "WREN did not set the write-enable latch"),
        }
    }
}

/// ESP32-S3 SPI flash, implementing [`Flash`] over a [`RegBus`].
///
/// Owns the flash byte range `[base, base + len)`; every operation is
/// bounds-checked against it. `SECTOR` is 4096, matching Horton's block
/// size, so [`crate::flash::FlashBlockDevice`] writes are exactly one
/// sector erase plus program.
pub struct SpiFlash<B: RegBus> {
    bus: B,
    base: u32,
    len: u32,
}

impl<B: RegBus> SpiFlash<B> {
    /// Owns `[base, base + len)` of flash through `bus`.
    pub const fn new(bus: B, base: u32, len: u32) -> Self {
        Self { bus, base, len }
    }

    /// The underlying register bus (test introspection, board setup).
    pub const fn bus(&self) -> &B {
        &self.bus
    }

    /// Spin until `bit` self-clears in `REG_CMD`.
    fn spin_cmd(&self, bit: u32) -> Result<(), SpiError> {
        let mut i = 0u32;
        loop {
            if self.bus.read(REG_CMD) & bit == 0 {
                return Ok(());
            }
            i += 1;
            if i >= CMD_SPIN_MAX {
                return Err(SpiError::Timeout);
            }
            core::hint::spin_loop();
        }
    }

    /// `WREN`, then verify the latch actually took via `RDSR`.
    fn write_enable(&self) -> Result<(), SpiError> {
        self.bus.write(REG_CMD, CMD_WREN);
        self.spin_cmd(CMD_WREN)?;
        self.bus.write(REG_CMD, CMD_RDSR);
        self.spin_cmd(CMD_RDSR)?;
        if self.bus.read(REG_RD_STATUS) & STATUS_WEL == 0 {
            return Err(SpiError::WriteEnableFailed);
        }
        Ok(())
    }

    /// Poll `RDSR` until `WIP` clears.
    fn poll_wip(&self) -> Result<(), SpiError> {
        let mut n = 0u32;
        loop {
            self.bus.write(REG_CMD, CMD_RDSR);
            self.spin_cmd(CMD_RDSR)?;
            if self.bus.read(REG_RD_STATUS) & STATUS_WIP == 0 {
                return Ok(());
            }
            n += 1;
            if n >= WIP_POLL_MAX {
                return Err(SpiError::Timeout);
            }
        }
    }

    /// Reject anything outside `[base, base + len)`.
    fn check_range(&self, addr: u32, len: u32) -> Result<(), SpiError> {
        let end = addr.checked_add(len).ok_or(SpiError::OutOfRange)?;
        let region_end = self
            .base
            .checked_add(self.len)
            .ok_or(SpiError::OutOfRange)?;
        if addr < self.base || end > region_end {
            return Err(SpiError::OutOfRange);
        }
        Ok(())
    }

    /// One page-program chunk: `1 <= len <= 64`, within a single page,
    /// `data.len() == len`.
    fn program_chunk(&self, addr: u32, data: &[u8], len: u32) -> Result<(), SpiError> {
        self.write_enable()?;
        // ESP-IDF encodes the chunk length in ADDR[31:24].
        self.bus.write(REG_ADDR, (addr & 0x00FF_FFFF) | (len << 24));
        // Words into W0.., zero-padded (`spimem_flash_ll_set_buffer_data`).
        for (wi, word) in data.chunks(4).enumerate() {
            let mut w = 0u32;
            for (j, &b) in word.iter().enumerate() {
                w |= u32::from(b) << (j * 8);
            }
            self.bus.write(REG_W0 + wi * 4, w);
        }
        // `usr_dummy = 0` (`spimem_flash_ll_program_page`).
        let user = self.bus.read(REG_USER);
        self.bus.write(REG_USER, user & !USER_USR_DUMMY);
        self.bus.write(REG_CMD, CMD_PP);
        self.spin_cmd(CMD_PP)?;
        self.poll_wip()
    }

    /// One user-mode `0x03` read: `1 <= n <= 64` bytes into `out`
    /// (`out.len() == n`).
    fn read_chunk(&self, addr: u32, out: &mut [u8], n: u32) -> Result<(), SpiError> {
        debug_assert_eq!(out.len(), n as usize);
        self.bus.write(REG_USER2, (7 << 28) | u32::from(OP_READ)); // 8-bit command
        self.bus.write(REG_USER1, 23 << 26); // 24-bit address phase
        self.bus.write(REG_MISO_DLEN, n * 8 - 1); // bits, minus one
        self.bus
            .write(REG_USER, USER_USR_COMMAND | USER_USR_ADDR | USER_USR_MISO);
        self.bus.write(REG_ADDR, addr);
        self.bus.write(REG_CMD, CMD_USR);
        self.spin_cmd(CMD_USR)?;
        for (word, chunk) in out.chunks_mut(4).enumerate() {
            let bytes = self.bus.read(REG_W0 + word * 4).to_le_bytes();
            for (j, b) in chunk.iter_mut().enumerate() {
                *b = bytes[j];
            }
        }
        Ok(())
    }
}

impl<B: RegBus> Flash for SpiFlash<B> {
    type Error = SpiError;
    const SECTOR: usize = SECTOR_U32 as usize;

    fn read(&self, addr: u32, buf: &mut [u8]) -> Result<(), SpiError> {
        let len = u32::try_from(buf.len()).map_err(|_| SpiError::OutOfRange)?;
        self.check_range(addr, len)?;
        let mut done: u32 = 0;
        while done < len {
            let n = core::cmp::min(PP_CHUNK, len - done);
            // `addr + done` cannot overflow: `check_range` proved
            // `addr + len <= u32::MAX` and `done <= len`.
            let at = addr + done;
            let end = done + n;
            self.read_chunk(at, &mut buf[done as usize..end as usize], n)?;
            done = end;
        }
        Ok(())
    }

    fn erase_sector(&mut self, addr: u32) -> Result<(), SpiError> {
        if !addr.is_multiple_of(SECTOR_U32) {
            return Err(SpiError::Misaligned);
        }
        self.check_range(addr, SECTOR_U32)?;
        self.write_enable()?;
        self.bus.write(REG_ADDR, addr);
        // `spimem_flash_ll_erase_sector`: CTRL is cleared for the erase.
        // Save/restore it so the boot-configured read mode survives.
        let ctrl = self.bus.read(REG_CTRL);
        self.bus.write(REG_CTRL, 0);
        self.bus.write(REG_CMD, CMD_SE);
        self.spin_cmd(CMD_SE)?;
        self.bus.write(REG_CTRL, ctrl);
        self.poll_wip()
    }

    fn program(&mut self, addr: u32, data: &[u8]) -> Result<(), SpiError> {
        let len = u32::try_from(data.len()).map_err(|_| SpiError::OutOfRange)?;
        self.check_range(addr, len)?;
        let mut done = 0u32;
        while done < len {
            // `addr + done` cannot overflow (see `read`).
            let at = addr + done;
            let to_page_end = PAGE - at % PAGE;
            let mut chunk = core::cmp::min(to_page_end, PP_CHUNK);
            chunk = core::cmp::min(chunk, len - done);
            // `chunk` is in `1..=64`: `to_page_end` is `1..=PAGE`,
            // `len - done >= 1` while the loop runs.
            let end = done + chunk;
            self.program_chunk(at, &data[done as usize..end as usize], chunk)?;
            done = end;
        }
        Ok(())
    }
}
