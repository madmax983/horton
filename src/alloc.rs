//! Bump-pointer block allocation over fixed device regions.
//!
//! Each region (`[start, end)`) gets one [`Bump`]. Allocation only moves
//! forward; reclamation is the open-time sweep's job (it re-derives "free"
//! as "not referenced by the manifest or the live WAL range" and repositions
//! the bump pointer). True free-list reuse is a v0.3 item per the spec.

use crate::error::Error;

/// Bump allocator over a contiguous block-id range `[start, end)`.
///
/// `const`-constructible so handles can be built in `const fn`s.
#[derive(Debug, Clone, Copy)]
pub struct Bump {
    next: u64,
    end: u64,
}

impl Bump {
    /// Creates a bump pointer over `[start, end)`.
    #[must_use]
    pub const fn new(start: u64, end: u64) -> Self {
        Self { next: start, end }
    }

    /// The next block id that would be allocated.
    #[must_use]
    pub const fn next(&self) -> u64 {
        self.next
    }

    /// One past the last allocatable block id.
    #[must_use]
    pub const fn end(&self) -> u64 {
        self.end
    }

    /// Repositions the bump pointer (the open-time sweep's rebuild).
    ///
    /// May move forward or backward; the caller must guarantee the new
    /// position does not alias live data.
    pub const fn set_next(&mut self, next: u64) {
        self.next = next;
    }

    /// Allocates one block.
    ///
    /// # Errors
    ///
    /// [`Error::NoSpace`] when the region is exhausted.
    pub const fn alloc<E>(&mut self) -> Result<u64, Error<E>> {
        if self.next >= self.end {
            return Err(Error::NoSpace);
        }
        let id = self.next;
        self.next += 1;
        Ok(id)
    }

    /// Returns the base of a `count`-block run without advancing.
    ///
    /// # Errors
    ///
    /// [`Error::NoSpace`] when fewer than `count` blocks remain.
    pub fn peek_run<E>(&self, count: u64) -> Result<u64, Error<E>> {
        let end = self.next.checked_add(count).ok_or(Error::NoSpace)?;
        if end > self.end {
            return Err(Error::NoSpace);
        }
        Ok(self.next)
    }

    /// Reserves `count` contiguous blocks, returning the first id.
    ///
    /// # Errors
    ///
    /// [`Error::NoSpace`] when fewer than `count` blocks remain.
    pub fn alloc_run<E>(&mut self, count: u64) -> Result<u64, Error<E>> {
        let base = self.peek_run::<E>(count)?;
        self.next = self.next.checked_add(count).ok_or(Error::NoSpace)?;
        Ok(base)
    }
}
