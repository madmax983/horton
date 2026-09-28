//! A horton [`BlockDevice`] over one ordinary file.
//!
//! Block `i` is bytes `i * BLOCK .. (i + 1) * BLOCK` of the file, read and
//! written with positioned I/O (`pread`/`pwrite` on Unix, `seek_read`/
//! `seek_write` on Windows). The file is created at its full size up
//! front; it is sparse, so a fresh 256 MiB store takes no disk until
//! written. `poll_flush` is `fdatasync` (`FlushFileBuffers` on Windows):
//! horton calls it after every WAL commit and before every manifest
//! commit, so an acknowledged write is on the disk, not in the page cache.
//!
//! Every call completes before it returns, so the futures horton builds
//! on top of it are ready the first time they are polled.

use core::cell::Cell;
use core::task::{Context, Poll};
use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

use horton::BlockDevice;

use crate::BLOCK;

/// What failed, for [`horton::Error::Device`]. horton's error type is
/// `Copy`, so this keeps the I/O error's kind rather than the error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IoError {
    /// The operation: `"read"`, `"write"` or `"sync"`.
    pub op: &'static str,
    /// The block it touched (0 for a sync).
    pub block: u64,
    /// The operating system's reason.
    pub kind: io::ErrorKind,
}

/// I/O counters, for the benchmark's report.
#[derive(Debug, Default, Clone, Copy)]
pub struct IoStats {
    pub reads: u64,
    pub writes: u64,
    pub syncs: u64,
}

/// One file, addressed in blocks of [`BLOCK`] bytes.
pub struct FileDevice {
    file: File,
    blocks: u64,
    /// `false` skips `fdatasync`: writes still reach the operating system
    /// before a call returns, so they survive the process crashing but
    /// not the machine losing power. That is `LevelDB`'s default
    /// (`WriteOptions::sync = false`).
    sync: bool,
    reads: Cell<u64>,
    writes: u64,
    syncs: u64,
}

impl FileDevice {
    /// Opens the store at `path`, creating it with `create_blocks` blocks
    /// if it does not exist. An existing file keeps its size: horton's
    /// layout is a function of the device size, so it must not change.
    pub fn open(path: &Path, create_blocks: u64, sync: bool) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        let block = BLOCK as u64;
        let mut len = file.metadata()?.len();
        if len == 0 {
            len = create_blocks * block;
            file.set_len(len)?;
            file.sync_all()?;
        }
        if len % block != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "{} is {len} bytes, not a whole number of {BLOCK}-byte blocks: not a store",
                    path.display()
                ),
            ));
        }
        Ok(Self {
            file,
            blocks: len / block,
            sync,
            reads: Cell::new(0),
            writes: 0,
            syncs: 0,
        })
    }

    /// The device size in blocks.
    #[must_use]
    pub const fn blocks(&self) -> u64 {
        self.blocks
    }

    pub const fn stats(&self) -> IoStats {
        IoStats {
            reads: self.reads.get(),
            writes: self.writes,
            syncs: self.syncs,
        }
    }

    const fn offset(&self, op: &'static str, id: u64) -> Result<u64, IoError> {
        if id < self.blocks {
            Ok(id * BLOCK as u64)
        } else {
            Err(IoError {
                op,
                block: id,
                kind: io::ErrorKind::InvalidInput,
            })
        }
    }
}

#[cfg(unix)]
fn read_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    std::os::unix::fs::FileExt::read_exact_at(file, buf, offset)
}

#[cfg(unix)]
fn write_at(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
    std::os::unix::fs::FileExt::write_all_at(file, buf, offset)
}

#[cfg(windows)]
fn read_at(file: &File, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !buf.is_empty() {
        match file.seek_read(buf, offset) {
            Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
            Ok(n) => {
                buf = &mut buf[n..];
                offset += n as u64;
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

#[cfg(windows)]
fn write_at(file: &File, mut buf: &[u8], mut offset: u64) -> io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !buf.is_empty() {
        match file.seek_write(buf, offset) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(n) => {
                buf = &buf[n..];
                offset += n as u64;
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

impl BlockDevice for FileDevice {
    type Error = IoError;
    const BLOCK: usize = BLOCK;

    fn poll_read_block(
        &self,
        _cx: &mut Context<'_>,
        id: u64,
        buf: &mut [u8],
    ) -> Poll<Result<(), IoError>> {
        self.reads.set(self.reads.get() + 1);
        Poll::Ready(self.offset("read", id).and_then(|at| {
            read_at(&self.file, buf, at).map_err(|e| IoError {
                op: "read",
                block: id,
                kind: e.kind(),
            })
        }))
    }

    fn poll_write_block(
        &mut self,
        _cx: &mut Context<'_>,
        id: u64,
        buf: &[u8],
    ) -> Poll<Result<(), IoError>> {
        self.writes += 1;
        Poll::Ready(self.offset("write", id).and_then(|at| {
            write_at(&self.file, buf, at).map_err(|e| IoError {
                op: "write",
                block: id,
                kind: e.kind(),
            })
        }))
    }

    fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), IoError>> {
        if !self.sync {
            return Poll::Ready(Ok(()));
        }
        self.syncs += 1;
        Poll::Ready(self.file.sync_data().map_err(|e| IoError {
            op: "sync",
            block: 0,
            kind: e.kind(),
        }))
    }
}
