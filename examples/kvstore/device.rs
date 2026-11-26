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
//!
//! [`FileDevice`] is the database's device: one owner, reads and writes.
//! [`ReadDevice`] is a second, read-only handle to the same file that any
//! number of threads share for the table phase of point reads (horton's
//! detached reads, ADR-0018). Both go through the operating system's
//! page cache, so a block the database has written is what a reader
//! gets, synced or not. [`StoreLock`] keeps a second opener out
//! (ADR-0032).

use core::cell::Cell;
use core::sync::atomic::{AtomicU64, Ordering};
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
            len = create_blocks.checked_mul(block).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("a file of {create_blocks} blocks is too large"),
                )
            })?;
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

/// An exclusive hold on a store: one opener at a time (ADR-0032).
///
/// It locks the file `<store>.lock` beside the store, not the store
/// file. On Windows a lock is mandatory: a lock on the store file stops
/// the reads and writes of the store's other handles. Drop unlocks it.
/// If the process stops, the operating system unlocks it. Do not delete
/// the lock file: then two openers can lock two different files.
pub struct StoreLock {
    file: File,
}

impl Drop for StoreLock {
    /// Unlocks now. On Windows, a lock that only the close releases can
    /// stay for a short time after the close.
    #[allow(
        clippy::incompatible_msrv,
        reason = "the MSRV (1.88) is the library's; examples build on the pinned toolchain"
    )]
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

impl StoreLock {
    /// Locks the store at `path`, and creates it empty if it does not
    /// exist. Call it before [`FileDevice::open`] sets the size.
    ///
    /// # Errors
    ///
    /// [`io::ErrorKind::WouldBlock`] if another opener holds the store,
    /// or the I/O error of the open or the lock.
    #[allow(
        clippy::incompatible_msrv,
        reason = "the MSRV (1.88) is the library's; examples build on the pinned toolchain"
    )]
    pub fn acquire(path: &Path) -> io::Result<Self> {
        // Create the store file: `canonicalize` needs a file that exists.
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        let lock = lock_path(&std::fs::canonicalize(path)?)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock)?;
        match file.try_lock() {
            Ok(()) => Ok(Self { file }),
            Err(std::fs::TryLockError::WouldBlock) => Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                format!(
                    "the store is open already, in this process or another ({} is locked)",
                    lock.display()
                ),
            )),
            Err(std::fs::TryLockError::Error(e)) => Err(e),
        }
    }
}

/// The lock file of the store at `path`: `<path>.lock`.
///
/// # Errors
///
/// [`io::ErrorKind::InvalidInput`] if `path` has no file name.
pub fn lock_path(path: &Path) -> io::Result<std::path::PathBuf> {
    let mut name = path
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "the path has no file name"))?
        .to_os_string();
    name.push(".lock");
    Ok(path.with_file_name(name))
}

/// A read-only handle to a store's file, shared by reader threads.
///
/// It opens the file again and does not duplicate the database's
/// handle: on Windows a duplicated handle shares one file object, and
/// reads through it would take turns.
pub struct ReadDevice {
    file: File,
    blocks: u64,
    reads: AtomicU64,
    /// Tests only: the read this many reads from now fails (0: none).
    #[cfg(test)]
    fail_in: AtomicU64,
}

impl ReadDevice {
    /// Opens the store at `path`, which [`FileDevice::open`] has created.
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        let blocks = file.metadata()?.len() / BLOCK as u64;
        Ok(Self {
            file,
            blocks,
            reads: AtomicU64::new(0),
            #[cfg(test)]
            fail_in: AtomicU64::new(0),
        })
    }

    /// Tests only: makes the `n`th read from now fail with an I/O error
    /// (`0`: no read fails).
    #[cfg(test)]
    pub fn fail_read_in(&self, n: u64) {
        self.fail_in.store(n, Ordering::SeqCst);
    }

    /// Blocks read through this handle.
    pub fn reads(&self) -> u64 {
        self.reads.load(Ordering::Relaxed)
    }
}

impl BlockDevice for ReadDevice {
    type Error = IoError;
    const BLOCK: usize = BLOCK;

    fn poll_read_block(
        &self,
        _cx: &mut Context<'_>,
        id: u64,
        buf: &mut [u8],
    ) -> Poll<Result<(), IoError>> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        #[cfg(test)]
        if self
            .fail_in
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            == Ok(1)
        {
            return Poll::Ready(Err(IoError {
                op: "read",
                block: id,
                kind: io::ErrorKind::Other,
            }));
        }
        let read = if id < self.blocks {
            read_at(&self.file, buf, id * BLOCK as u64).map_err(|e| e.kind())
        } else {
            Err(io::ErrorKind::InvalidInput)
        };
        Poll::Ready(read.map_err(|kind| IoError {
            op: "read",
            block: id,
            kind,
        }))
    }

    /// Refused: the handle is read-only, and horton's read path never
    /// writes.
    fn poll_write_block(
        &mut self,
        _cx: &mut Context<'_>,
        id: u64,
        _buf: &[u8],
    ) -> Poll<Result<(), IoError>> {
        Poll::Ready(Err(IoError {
            op: "write",
            block: id,
            kind: io::ErrorKind::PermissionDenied,
        }))
    }

    fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), IoError>> {
        Poll::Ready(Ok(()))
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
