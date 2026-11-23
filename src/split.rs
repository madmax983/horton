//! Equi-depth split keys: where to cut a sorted key set into buckets of
//! near-equal size, found in one pass and fixed memory.
//!
//! [`fingerprint_buckets`](crate::fingerprint::fingerprint_buckets) is
//! only as good as its split keys. Firmware keys are rarely spread over
//! the key space: a telemetry log keyed `tlm/<timestamp>` sits in one
//! narrow corner of it, so splits chosen by byte value put every entry
//! in a single bucket and anti-entropy learns nothing from the rest. A
//! [`Splitter`] watches the keys go by (a positioned [`Scan`](crate::Scan),
//! or any [`SortedEntries`]) and returns split keys that cut them into
//! buckets of near-equal *count*, wherever the keys happen to crowd.
//!
//! It does not need to know the number of keys in advance. It keeps
//! every `stride`-th key in `SLOTS` inline slots; when the slots fill it
//! drops every other one and doubles the stride. After `n` keys at least
//! `SLOTS / 2` evenly spaced samples remain, the stride is under
//! `2n / SLOTS`, and every bucket [`splits`](Splitter::splits) defines
//! holds within two strides of `n / buckets` keys. Memory is
//! `SLOTS * KEY_MAX` bytes plus a length per slot; nothing is
//! allocated.
//!
//! The result is deterministic: the same keys give the same splits,
//! whatever the write history or table layout behind them. So a device
//! can sample its own keys, send the (short) split list to its replica,
//! and both sides fingerprint the same balanced buckets. The same splits
//! serve as shard boundaries or as even progress steps for a long scan.

use core::convert::Infallible;

use crate::diff::SortedEntries;
use crate::error::Error;

/// A fixed-memory sampler of every `stride`-th key of an ascending key
/// stream, from which equi-depth split keys are read.
///
/// `SLOTS` must be at least 2; more slots give finer buckets. Feed it
/// keys in strictly ascending order, with [`observe`](Self::observe) or
/// [`sample`], then read [`splits`](Self::splits).
pub struct Splitter<const KEY_MAX: usize, const SLOTS: usize> {
    keys: [[u8; KEY_MAX]; SLOTS],
    lens: [usize; SLOTS],
    /// Samples held: slot `i` is the key at stream position `i * stride`.
    len: usize,
    stride: u64,
    seen: u64,
}

impl<const KEY_MAX: usize, const SLOTS: usize> Default for Splitter<KEY_MAX, SLOTS> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const KEY_MAX: usize, const SLOTS: usize> Splitter<KEY_MAX, SLOTS> {
    /// An empty sampler.
    #[must_use]
    pub const fn new() -> Self {
        const {
            assert!(SLOTS >= 2, "a Splitter needs at least two slots");
        }
        Self {
            keys: [[0; KEY_MAX]; SLOTS],
            lens: [0; SLOTS],
            len: 0,
            stride: 1,
            seen: 0,
        }
    }

    /// Keys observed since [`new`](Self::new) or [`clear`](Self::clear).
    #[must_use]
    pub const fn seen(&self) -> u64 {
        self.seen
    }

    /// The distance, in keys, between neighboring samples. Every bucket
    /// [`splits`](Self::splits) defines is within two strides of the
    /// ideal size.
    #[must_use]
    pub const fn stride(&self) -> u64 {
        self.stride
    }

    /// Forgets every key, to sample a new stream.
    pub const fn clear(&mut self) {
        self.len = 0;
        self.stride = 1;
        self.seen = 0;
    }

    /// Observes the next key of the stream. Keys must arrive in strictly
    /// ascending order, or the splits will not be.
    ///
    /// # Errors
    ///
    /// [`Error::KeyTooLarge`] when `key` is longer than `KEY_MAX`; the
    /// key is not counted. The sampler touches no device, so its error
    /// type is `Error<Infallible>` ([`Error::widen`] converts it).
    pub fn observe(&mut self, key: &[u8]) -> Result<(), Error<Infallible>> {
        if key.len() > KEY_MAX {
            return Err(Error::KeyTooLarge {
                len: key.len(),
                max: KEY_MAX,
            });
        }
        let pos = self.seen;
        self.seen = self.seen.saturating_add(1);
        if !pos.is_multiple_of(self.stride) {
            return Ok(());
        }
        if self.len == SLOTS {
            self.thin();
            if !pos.is_multiple_of(self.stride) {
                return Ok(());
            }
        }
        self.keys[self.len][..key.len()].copy_from_slice(key);
        self.lens[self.len] = key.len();
        self.len += 1;
        Ok(())
    }

    /// Keeps the even-numbered samples and doubles the stride, so slot
    /// `i` still holds the key at position `i * stride`.
    fn thin(&mut self) {
        let keep = self.len.div_ceil(2);
        for i in 1..keep {
            self.keys[i] = self.keys[2 * i];
            self.lens[i] = self.lens[2 * i];
        }
        self.len = keep;
        self.stride = self.stride.saturating_mul(2);
    }

    /// Writes split keys for `out.len() + 1` buckets of near-equal size
    /// into `out` and returns how many it wrote: the first `n` entries of
    /// `out` are strictly ascending and ready for
    /// [`fingerprint_buckets`](crate::fingerprint::fingerprint_buckets)
    /// with `n + 1` buckets.
    ///
    /// Fewer than `out.len()` splits come back when there are too few
    /// samples to fill that many buckets (with fewer keys than buckets,
    /// one key per bucket) and none for an empty stream.
    pub fn splits<'s>(&'s self, out: &mut [&'s [u8]]) -> usize {
        let buckets = out.len() + 1;
        let mut written = 0;
        let mut prev = 0;
        for j in 1..buckets {
            // `j * len / buckets` in u128, so it cannot overflow; the
            // result is below `len`, so it fits in `usize`.
            let idx = (j as u128 * self.len as u128) / buckets as u128;
            let idx = usize::try_from(idx).unwrap_or(usize::MAX);
            if idx == 0 || idx == prev || idx >= self.len {
                continue;
            }
            prev = idx;
            out[written] = &self.keys[idx][..self.lens[idx]];
            written += 1;
        }
        written
    }
}

/// Feeds every remaining entry of `src` (for example a positioned
/// [`Scan`](crate::Scan)) to `splitter` and returns how many it fed.
///
/// `key_buf` and `val_buf` are scratch for the source; size them
/// `KEY_MAX` and `VAL_MAX`.
///
/// # Errors
///
/// Whatever the source reports, or [`Error::KeyTooLarge`] when a key
/// exceeds the splitter's `KEY_MAX` (that entry has been consumed).
pub async fn sample<S, E, const KEY_MAX: usize, const SLOTS: usize>(
    src: &mut S,
    splitter: &mut Splitter<KEY_MAX, SLOTS>,
    key_buf: &mut [u8],
    val_buf: &mut [u8],
) -> Result<u64, Error<E>>
where
    S: SortedEntries<Error = Error<E>>,
{
    let mut fed = 0u64;
    while let Some((klen, _)) = src.next_entry(key_buf, val_buf).await? {
        splitter.observe(&key_buf[..klen]).map_err(Error::widen)?;
        fed += 1;
    }
    Ok(fed)
}
