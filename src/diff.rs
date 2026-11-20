//! Scan diffs: the changes that turn one view of a key range into
//! another.
//!
//! A [`Diff`] merge-joins two positioned [`Scan`]s — `old` and `new` —
//! and yields only the keys where they disagree, each as a [`Change`]:
//! [`Added`](Change::Added) (only in `new`), [`Removed`](Change::Removed)
//! (only in `old`) or [`Changed`](Change::Changed) (in both, different
//! values). Keys with equal values on both sides are skipped, so the
//! diff speaks only of *content*: overwrites with the same value,
//! flushes, compactions and block compression never show up.
//!
//! Two uses fall out of the same join:
//!
//! - **Change feed.** Scan one database twice, `old` at a
//!   [`Db::snapshot`](crate::Db::snapshot) watermark and `new` at
//!   `u64::MAX`, and the diff is everything written since the snapshot —
//!   the delta to downlink instead of the whole store.
//! - **Repair.** Scan a database and its replica over a bucket whose
//!   [`Fingerprint`](crate::Fingerprint)s disagreed, and the diff names
//!   exactly the keys to ship. Fingerprints find *where*; the diff says
//!   *what*.
//!
//! [`Scan`] borrows its database shared, so both scans may read one
//! database. Position both first ([`Scan::seek`] or
//! [`Scan::seek_with_time`]); their ranges, watermarks and TTL clocks
//! define the two views. The diff is a lending iterator: after
//! [`next`](Diff::next) returns a change, [`key`](Diff::key),
//! [`old_value`](Diff::old_value) and [`new_value`](Diff::new_value)
//! borrow it until the next call. Memory is one key and one value per
//! side, inline; nothing is allocated.

use crate::device::BlockDevice;
use crate::error::Error;
use crate::scan::Scan;

/// How a key differs between the `old` and `new` views.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Change {
    /// The key is only in `new`.
    Added,
    /// The key is only in `old`.
    Removed,
    /// The key is in both, with different values.
    Changed,
}

/// One side's head entry: the scan's next unconsumed entry.
struct Head<const KEY_MAX: usize, const VAL_MAX: usize> {
    key: [u8; KEY_MAX],
    key_len: usize,
    val: [u8; VAL_MAX],
    val_len: usize,
    /// The head holds an entry.
    full: bool,
    /// The head was consumed (or never filled) and must be refilled from
    /// the scan before the next comparison. Cleared only on a successful
    /// read, so an I/O error leaves the diff retryable.
    stale: bool,
}

impl<const KEY_MAX: usize, const VAL_MAX: usize> Head<KEY_MAX, VAL_MAX> {
    const fn new() -> Self {
        Self {
            key: [0u8; KEY_MAX],
            key_len: 0,
            val: [0u8; VAL_MAX],
            val_len: 0,
            full: false,
            stale: true,
        }
    }

    fn key(&self) -> &[u8] {
        &self.key[..self.key_len]
    }

    fn val(&self) -> &[u8] {
        &self.val[..self.val_len]
    }
}

/// Merge-join of two scans into their differences. See the [module
/// docs](self).
pub struct Diff<
    's,
    'd,
    D: BlockDevice,
    const BLOCK: usize,
    const KEY_MAX: usize,
    const VAL_MAX: usize,
    const CAP: usize,
    const ARENA: usize,
    const LEVELS: usize,
    const TABLES: usize,
    const BLOOM_BYTES: usize,
    const CACHE: usize,
> {
    old: &'s mut Scan<
        'd,
        D,
        BLOCK,
        KEY_MAX,
        VAL_MAX,
        CAP,
        ARENA,
        LEVELS,
        TABLES,
        BLOOM_BYTES,
        CACHE,
    >,
    new: &'s mut Scan<
        'd,
        D,
        BLOCK,
        KEY_MAX,
        VAL_MAX,
        CAP,
        ARENA,
        LEVELS,
        TABLES,
        BLOOM_BYTES,
        CACHE,
    >,
    old_head: Head<KEY_MAX, VAL_MAX>,
    new_head: Head<KEY_MAX, VAL_MAX>,
    /// The change last returned by [`next`](Diff::next), if any.
    current: Option<Change>,
}

impl<
    's,
    'd,
    D: BlockDevice,
    const BLOCK: usize,
    const KEY_MAX: usize,
    const VAL_MAX: usize,
    const CAP: usize,
    const ARENA: usize,
    const LEVELS: usize,
    const TABLES: usize,
    const BLOOM_BYTES: usize,
    const CACHE: usize,
> Diff<'s, 'd, D, BLOCK, KEY_MAX, VAL_MAX, CAP, ARENA, LEVELS, TABLES, BLOOM_BYTES, CACHE>
{
    /// Diffs the positioned `old` scan against the positioned `new` one.
    #[must_use]
    pub const fn new(
        old: &'s mut Scan<
            'd,
            D,
            BLOCK,
            KEY_MAX,
            VAL_MAX,
            CAP,
            ARENA,
            LEVELS,
            TABLES,
            BLOOM_BYTES,
            CACHE,
        >,
        new: &'s mut Scan<
            'd,
            D,
            BLOCK,
            KEY_MAX,
            VAL_MAX,
            CAP,
            ARENA,
            LEVELS,
            TABLES,
            BLOOM_BYTES,
            CACHE,
        >,
    ) -> Self {
        Self {
            old,
            new,
            old_head: Head::new(),
            new_head: Head::new(),
            current: None,
        }
    }

    /// Advances to the next differing key, returning how it differs, or
    /// `Ok(None)` once both scans are exhausted. Identical entries are
    /// skipped.
    ///
    /// # Errors
    ///
    /// Whatever [`Scan::next`] reports. The diff does not advance past a
    /// failed read, so calling `next` again retries it.
    pub async fn next(&mut self) -> Result<Option<Change>, Error<D::Error>> {
        // Release the change returned last time: its heads were kept so
        // the accessors could borrow them.
        match self.current.take() {
            Some(Change::Removed) => self.old_head.stale = true,
            Some(Change::Added) => self.new_head.stale = true,
            Some(Change::Changed) => {
                self.old_head.stale = true;
                self.new_head.stale = true;
            }
            None => {}
        }
        loop {
            refill(self.old, &mut self.old_head).await?;
            refill(self.new, &mut self.new_head).await?;
            let change = match (self.old_head.full, self.new_head.full) {
                (false, false) => return Ok(None),
                (true, false) => Change::Removed,
                (false, true) => Change::Added,
                (true, true) => match self.old_head.key().cmp(self.new_head.key()) {
                    core::cmp::Ordering::Less => Change::Removed,
                    core::cmp::Ordering::Greater => Change::Added,
                    core::cmp::Ordering::Equal if self.old_head.val() == self.new_head.val() => {
                        self.old_head.stale = true;
                        self.new_head.stale = true;
                        continue;
                    }
                    core::cmp::Ordering::Equal => Change::Changed,
                },
            };
            self.current = Some(change);
            return Ok(Some(change));
        }
    }

    /// The key of the change last returned by [`next`](Diff::next);
    /// empty before the first change and after the end.
    #[must_use]
    pub fn key(&self) -> &[u8] {
        match self.current {
            Some(Change::Added) => self.new_head.key(),
            Some(Change::Removed | Change::Changed) => self.old_head.key(),
            None => &[],
        }
    }

    /// The `old` view's value for the current key: `None` when it was
    /// [`Added`](Change::Added) (or there is no current change).
    #[must_use]
    pub fn old_value(&self) -> Option<&[u8]> {
        match self.current {
            Some(Change::Removed | Change::Changed) => Some(self.old_head.val()),
            Some(Change::Added) | None => None,
        }
    }

    /// The `new` view's value for the current key: `None` when it was
    /// [`Removed`](Change::Removed) (or there is no current change).
    #[must_use]
    pub fn new_value(&self) -> Option<&[u8]> {
        match self.current {
            Some(Change::Added | Change::Changed) => Some(self.new_head.val()),
            Some(Change::Removed) | None => None,
        }
    }
}

/// Reads `scan`'s next entry into a stale `head`; a no-op otherwise.
async fn refill<
    D: BlockDevice,
    const BLOCK: usize,
    const KEY_MAX: usize,
    const VAL_MAX: usize,
    const CAP: usize,
    const ARENA: usize,
    const LEVELS: usize,
    const TABLES: usize,
    const BLOOM_BYTES: usize,
    const CACHE: usize,
>(
    scan: &mut Scan<'_, D, BLOCK, KEY_MAX, VAL_MAX, CAP, ARENA, LEVELS, TABLES, BLOOM_BYTES, CACHE>,
    head: &mut Head<KEY_MAX, VAL_MAX>,
) -> Result<(), Error<D::Error>> {
    if !head.stale {
        return Ok(());
    }
    match scan.next(&mut head.key, &mut head.val).await? {
        Some((klen, vlen)) => {
            head.key_len = klen;
            head.val_len = vlen;
            head.full = true;
        }
        None => head.full = false,
    }
    head.stale = false;
    Ok(())
}
