//! Block allocation over fixed device regions.
//!
//! Each region (`[start, end)`) gets one [`Bump`] plus one [`FreeList`].
//! The bump hands out never-allocated blocks; the free list holds blocks
//! the open-time sweep proved unreferenced (orphans of torn flushes below
//! the bump's resume point). Allocation tries the free list first, then the
//! bump. Reclamation is exact: every unreferenced block is owned by exactly
//! one of the two.
//!
//! Compaction (v0.4) will free whole input tables into the free list —
//! that is what makes it non-vacuous. In v0.3 the write path cannot strand
//! blocks below the bump (a torn flush's run always starts at or above the
//! resume point, where the bump simply overwrites it), so the list only
//! ever holds what the sweep finds; the machinery is real and tested, the
//! production source of such orphans arrives with compaction.

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

/// Fixed-capacity sorted set of free block ids with first-fit contiguous
/// run allocation. `no_std`, no allocation: the backing array lives in the
/// struct.
///
/// `const`-constructible so handles can be built in `const fn`s. Size `CAP`
/// to hold the region's whole block count; [`FreeList::insert`] reports
/// [`Error::NoSpace`] instead of silently leaking when it is undersized.
#[derive(Debug, Clone, Copy)]
pub struct FreeList<const CAP: usize> {
    ids: [u64; CAP],
    len: usize,
}

impl<const CAP: usize> FreeList<CAP> {
    /// An empty free list.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            ids: [0u64; CAP],
            len: 0,
        }
    }
    /// Number of free block ids held.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// True when no block ids are held.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Inserts `id`, keeping ascending order.
    ///
    /// Debug-asserts `id` is not already present: a duplicate would corrupt
    /// run accounting (`claim_run` removes exactly one copy).
    ///
    /// # Errors
    ///
    /// [`Error::NoSpace`] when the list is full — size `CAP` up.
    pub fn insert<E>(&mut self, id: u64) -> Result<(), Error<E>> {
        if self.len >= CAP {
            return Err(Error::NoSpace);
        }
        let mut i = 0;
        while i < self.len && self.ids[i] < id {
            i += 1;
        }
        debug_assert!(
            i >= self.len || self.ids[i] != id,
            "FreeList: duplicate insert"
        );
        let mut j = self.len;
        while j > i {
            self.ids[j] = self.ids[j - 1];
            j -= 1;
        }
        self.ids[i] = id;
        self.len += 1;
        Ok(())
    }

    /// First-fit: the base of the first run of `count` consecutive ids, or
    /// `None`. Does not mutate; pair with [`FreeList::claim_run`] once the
    /// run's use is committed, so a failed operation changes nothing.
    #[must_use]
    pub fn find_run(&self, count: usize) -> Option<u64> {
        if count == 0 || count > self.len {
            return None;
        }
        // `i + count <= self.len` below keeps every index in-bounds.
        let mut i = 0usize;
        while i + count <= self.len {
            let base = self.ids[i];
            let mut k = 0usize;
            while k < count {
                let want = base.checked_add(u64::try_from(k).ok()?)?;
                if self.ids[i + k] != want {
                    break;
                }
                k += 1;
            }
            if k == count {
                return Some(base);
            }
            i += 1;
        }
        None
    }

    /// Removes `[base, base + count)`; returns `false` (without mutating)
    /// when the run is not fully present. The contiguity re-check makes
    /// `claim_run` safe to call on any base, not just
    /// [`FreeList::find_run`] results.
    pub fn claim_run(&mut self, base: u64, count: usize) -> bool {
        let Some(i) = (0..self.len).find(|&i| self.ids[i] == base) else {
            return false;
        };
        let mut k = 0usize;
        while k < count {
            let Some(want) = u64::try_from(k).ok().and_then(|kk| base.checked_add(kk)) else {
                return false;
            };
            // `k` only grows while the gets succeed, so `i + k <= self.len`
            // and the addition cannot overflow.
            if self.ids.get(i + k) != Some(&want) {
                return false;
            }
            k += 1;
        }
        self.ids.copy_within(i + count..self.len, i);
        self.len -= count;
        true
    }
}

impl<const CAP: usize> Default for FreeList<CAP> {
    fn default() -> Self {
        Self::new()
    }
}
