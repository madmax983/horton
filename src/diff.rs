//! Scan diffs: the exact entries two ordered views disagree on.
//!
//! [`fingerprint_buckets`](crate::fingerprint::fingerprint_buckets) tells
//! a device and its replica *which* key ranges disagree; a [`Diff`] tells
//! them *what* to change. It merge-joins two ascending sources in one
//! pass and yields a [`Delta`] for every key whose visible value differs:
//! present only on the left, only on the right, or on both with
//! different values. Matching entries are skipped.
//!
//! Either side is anything that implements [`SortedEntries`]: a
//! positioned [`Scan`] of any database shape (seek both to the same
//! range and snapshot first), or the caller's own ordered source, such
//! as a replica's entries arriving over a link. Treat the left side as
//! the source of truth and a [`Delta`] is a repair: put `OnlyLeft` and
//! `Differs` values, delete `OnlyRight` keys.
//!
//! Memory is four caller-owned buffers (a key and a value per side); the
//! deltas borrow from them until the next call.

use core::cmp::Ordering;
use core::future::Future;

use crate::device::BlockDevice;
use crate::error::Error;
use crate::scan::Scan;

/// An ascending source of key-value pairs, one entry per call.
///
/// Implementations must yield strictly ascending keys and copy each key
/// and value into the buffers, returning their lengths; `Ok(None)` ends
/// the source. An error must leave the source where it was, so a retry
/// yields the same entry.
pub trait SortedEntries {
    /// What a failed read reports.
    type Error;

    /// Yields the next entry into `key_buf` and `val_buf`.
    fn next_entry(
        &mut self,
        key_buf: &mut [u8],
        val_buf: &mut [u8],
    ) -> impl Future<Output = Result<Option<(usize, usize)>, Self::Error>>;
}

impl<
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
> SortedEntries
    for Scan<'_, D, BLOCK, KEY_MAX, VAL_MAX, CAP, ARENA, LEVELS, TABLES, BLOOM_BYTES, CACHE>
{
    type Error = Error<D::Error>;

    /// [`Scan::next`].
    fn next_entry(
        &mut self,
        key_buf: &mut [u8],
        val_buf: &mut [u8],
    ) -> impl Future<Output = Result<Option<(usize, usize)>, Self::Error>> {
        self.next(key_buf, val_buf)
    }
}

/// One key on which the two sides disagree. The slices borrow the
/// [`Diff`]'s buffers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Delta<'b> {
    /// The key is visible on the left side only.
    OnlyLeft {
        /// The key.
        key: &'b [u8],
        /// Its left value.
        val: &'b [u8],
    },
    /// The key is visible on the right side only.
    OnlyRight {
        /// The key.
        key: &'b [u8],
        /// Its right value.
        val: &'b [u8],
    },
    /// Both sides hold the key, with different values.
    Differs {
        /// The key.
        key: &'b [u8],
        /// The left value.
        left: &'b [u8],
        /// The right value.
        right: &'b [u8],
    },
}

impl<'b> Delta<'b> {
    /// The key this delta is about.
    #[must_use]
    pub const fn key(&self) -> &'b [u8] {
        match *self {
            Self::OnlyLeft { key, .. }
            | Self::OnlyRight { key, .. }
            | Self::Differs { key, .. } => key,
        }
    }
}

/// Where one side of the join stands.
#[derive(Clone, Copy)]
enum Head {
    /// The buffered entry was consumed; read the next one.
    Need,
    /// An entry of these key and value lengths is in the buffers.
    Have(usize, usize),
    /// The source is exhausted.
    Done,
}

/// A merge-join of two [`SortedEntries`] that yields their differences
/// in ascending key order. Fused: after `Ok(None)` it keeps returning
/// `Ok(None)`.
pub struct Diff<'a, A, B> {
    left: &'a mut A,
    right: &'a mut B,
    lk: &'a mut [u8],
    lv: &'a mut [u8],
    rk: &'a mut [u8],
    rv: &'a mut [u8],
    l: Head,
    r: Head,
}

impl<'a, A, B> Diff<'a, A, B>
where
    A: SortedEntries,
    B: SortedEntries<Error = A::Error>,
{
    /// Joins `left` and `right`, both already positioned. Size each key
    /// buffer `KEY_MAX` and each value buffer `VAL_MAX` of its side.
    pub const fn new(
        left: &'a mut A,
        right: &'a mut B,
        left_key: &'a mut [u8],
        left_val: &'a mut [u8],
        right_key: &'a mut [u8],
        right_val: &'a mut [u8],
    ) -> Self {
        Self {
            left,
            right,
            lk: left_key,
            lv: left_val,
            rk: right_key,
            rv: right_val,
            l: Head::Need,
            r: Head::Need,
        }
    }

    /// The next difference, or `Ok(None)` when both sides are exhausted.
    ///
    /// # Errors
    ///
    /// Whatever either source reports (for a [`Scan`],
    /// [`Error::BufferTooSmall`] and friends). Neither side advances past
    /// an entry it failed to yield, so a retry, with larger buffers if
    /// need be, loses nothing.
    pub async fn next(&mut self) -> Result<Option<Delta<'_>>, A::Error> {
        loop {
            if matches!(self.l, Head::Need) {
                self.l = head(self.left.next_entry(self.lk, self.lv).await?);
            }
            if matches!(self.r, Head::Need) {
                self.r = head(self.right.next_entry(self.rk, self.rv).await?);
            }
            // After the refill each side is `Have` or `Done`.
            let step = match (self.l, self.r) {
                (Head::Have(lk, lv), Head::Have(rk, rv)) => {
                    match self.lk[..lk].cmp(&self.rk[..rk]) {
                        Ordering::Less => Step::Left(lk, lv),
                        Ordering::Greater => Step::Right(rk, rv),
                        Ordering::Equal => Step::Both(lk, lv, rv),
                    }
                }
                (Head::Have(k, v), _) => Step::Left(k, v),
                (_, Head::Have(k, v)) => Step::Right(k, v),
                _ => return Ok(None),
            };
            match step {
                Step::Left(k, v) => {
                    self.l = Head::Need;
                    return Ok(Some(Delta::OnlyLeft {
                        key: &self.lk[..k],
                        val: &self.lv[..v],
                    }));
                }
                Step::Right(k, v) => {
                    self.r = Head::Need;
                    return Ok(Some(Delta::OnlyRight {
                        key: &self.rk[..k],
                        val: &self.rv[..v],
                    }));
                }
                Step::Both(k, lv, rv) => {
                    self.l = Head::Need;
                    self.r = Head::Need;
                    if self.lv[..lv] != self.rv[..rv] {
                        return Ok(Some(Delta::Differs {
                            key: &self.lk[..k],
                            left: &self.lv[..lv],
                            right: &self.rv[..rv],
                        }));
                    }
                }
            }
        }
    }
}

/// What the join does with the current heads: emit the left one, the
/// right one, or compare the values of a shared key (key length, then
/// the two value lengths).
enum Step {
    Left(usize, usize),
    Right(usize, usize),
    Both(usize, usize, usize),
}

const fn head(entry: Option<(usize, usize)>) -> Head {
    match entry {
        Some((k, v)) => Head::Have(k, v),
        None => Head::Done,
    }
}
