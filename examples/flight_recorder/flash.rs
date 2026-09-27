//! A simulated NOR flash chip: the storage a real flight recorder would
//! have, with the two properties that make it hard.
//!
//! - **NOR rules.** An erase sets a whole sector to `0xFF`; a program can
//!   only clear bits. horton's [`FlashBlockDevice`](horton::flash) erases
//!   before every write, so a violation here would be a driver bug.
//! - **Power cuts.** [`SimFlash::cut_power_after`] arms a cut: the chosen
//!   operation is torn (a program lands only partly, an erase stops
//!   partway) and every later operation fails with [`SimError::PowerLost`],
//!   as if the battery died there.
//!
//! It also counts erases per sector, the number that wears flash out.
//! Storage is RAM, or a file so that killing the process keeps the chip's
//! contents for the next run.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use horton::flash::Flash;

use crate::BLOCK;

/// Why a flash operation failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimError {
    /// The power is off: a cut was armed and reached.
    PowerLost,
    /// The address range falls outside the chip.
    OutOfBounds,
    /// A program tried to set a bit (`0 → 1`) without an erase.
    BitSet,
    /// The backing file failed.
    Io,
}

/// Where the chip's bytes live.
enum Storage {
    Ram(Vec<u8>),
    File(File),
}

/// The simulated chip.
pub struct SimFlash {
    storage: Storage,
    sectors: usize,
    erases: Vec<u32>,
    /// Operations (erase or program) performed while powered.
    ops: u64,
    /// The operation that loses power, if a cut is armed.
    cut_at: Option<u64>,
    /// How far a torn operation gets, as a fraction in 1/256ths.
    tear: u8,
    powered: bool,
}

impl SimFlash {
    /// An erased chip of `sectors` sectors in RAM.
    pub fn ram(sectors: usize) -> Self {
        Self::with(Storage::Ram(vec![0xFF; sectors * BLOCK]), sectors)
    }

    /// A chip backed by `path`, created erased when missing. The file keeps
    /// the chip's contents across runs of the program.
    pub fn file(path: &Path, sectors: usize) -> std::io::Result<Self> {
        let fresh = !path.exists();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        if fresh {
            file.write_all(&vec![0xFF; sectors * BLOCK])?;
            file.sync_all()?;
        }
        Ok(Self::with(Storage::File(file), sectors))
    }

    fn with(storage: Storage, sectors: usize) -> Self {
        Self {
            storage,
            sectors,
            erases: vec![0; sectors],
            ops: 0,
            cut_at: None,
            tear: 0,
            powered: true,
        }
    }

    /// Arms a power cut: the `ops`-th erase or program from now is torn
    /// (`tear` / 256 of it lands) and the chip goes dark after it.
    pub const fn cut_power_after(&mut self, ops: u64, tear: u8) {
        self.cut_at = Some(self.ops + ops);
        self.tear = tear;
    }

    /// Restores power. The contents are whatever the cut left behind.
    pub const fn restore_power(&mut self) {
        self.powered = true;
        self.cut_at = None;
    }

    /// Whether the power is on.
    pub const fn powered(&self) -> bool {
        self.powered
    }

    /// Erases per sector since this value was created.
    pub fn erase_counts(&self) -> &[u32] {
        &self.erases
    }

    /// Checks power and the operation budget. `Ok(false)` means this
    /// operation is the one the cut tears.
    fn spend_op(&mut self) -> Result<bool, SimError> {
        if !self.powered {
            return Err(SimError::PowerLost);
        }
        self.ops += 1;
        if self.cut_at == Some(self.ops) {
            self.powered = false;
            return Ok(false);
        }
        Ok(true)
    }

    const fn range(&self, addr: u32, len: usize) -> Result<usize, SimError> {
        let start = addr as usize;
        if start + len > self.sectors * BLOCK {
            return Err(SimError::OutOfBounds);
        }
        Ok(start)
    }

    fn read_at(&self, start: usize, buf: &mut [u8]) -> Result<(), SimError> {
        match &self.storage {
            Storage::Ram(mem) => {
                buf.copy_from_slice(&mem[start..start + buf.len()]);
                Ok(())
            }
            Storage::File(file) => {
                let mut f = file;
                f.seek(SeekFrom::Start(start as u64))
                    .and_then(|_| f.read_exact(buf))
                    .map_err(|_| SimError::Io)
            }
        }
    }

    fn write_at(&mut self, start: usize, data: &[u8]) -> Result<(), SimError> {
        match &mut self.storage {
            Storage::Ram(mem) => {
                mem[start..start + data.len()].copy_from_slice(data);
                Ok(())
            }
            Storage::File(file) => file
                .seek(SeekFrom::Start(start as u64))
                .and_then(|_| file.write_all(data))
                .map_err(|_| SimError::Io),
        }
    }

    /// The prefix of `len` bytes a torn operation manages to write.
    fn torn_len(&self, len: usize) -> usize {
        len * usize::from(self.tear) / 256
    }
}

impl Flash for SimFlash {
    type Error = SimError;
    const SECTOR: usize = BLOCK;

    fn read(&self, addr: u32, buf: &mut [u8]) -> Result<(), SimError> {
        if !self.powered {
            return Err(SimError::PowerLost);
        }
        let start = self.range(addr, buf.len())?;
        self.read_at(start, buf)
    }

    fn erase_sector(&mut self, addr: u32) -> Result<(), SimError> {
        let start = self.range(addr, BLOCK)?;
        let whole = self.spend_op()?;
        let len = if whole { BLOCK } else { self.torn_len(BLOCK) };
        self.write_at(start, &vec![0xFF; len])?;
        if !whole {
            return Err(SimError::PowerLost);
        }
        self.erases[start / BLOCK] += 1;
        Ok(())
    }

    fn program(&mut self, addr: u32, data: &[u8]) -> Result<(), SimError> {
        let start = self.range(addr, data.len())?;
        let mut old = vec![0u8; data.len()];
        self.read_at(start, &mut old)?;
        if old.iter().zip(data).any(|(o, d)| o & d != *d) {
            return Err(SimError::BitSet);
        }
        let whole = self.spend_op()?;
        let len = if whole {
            data.len()
        } else {
            self.torn_len(data.len())
        };
        self.write_at(start, &data[..len])?;
        if whole {
            Ok(())
        } else {
            Err(SimError::PowerLost)
        }
    }
}
