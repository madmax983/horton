//! Content fingerprints: a digest of what a database *says*, not how it
//! is laid out.
//!
//! Two databases holding the same visible key-value pairs have the same
//! [`Fingerprint`], however differently they got there: write order,
//! overwrites, tombstones, flushes, compactions and block compression all
//! drop out, because the digest is taken over a [`Scan`]'s output.
//!
//! The digest is a multiset hash (a count plus a wrapping sum of
//! per-entry hashes), so fingerprints of disjoint key ranges
//! [`merge`](Fingerprint::merge) into the fingerprint of their union.
//! That makes anti-entropy cheap: a device and its ground-side replica
//! compare one fingerprint per bucket ([`fingerprint_buckets`], a single
//! scan pass), then bisect only the buckets that disagree. No bytes but
//! the 16-byte fingerprints cross the link until a difference is found.
//!
//! This is an integrity *check*, not a MAC: it detects accidental
//! divergence, and an adversary who controls the data can forge a
//! collision.
//!
//! Position the scan first ([`Scan::seek`] or [`Scan::seek_with_time`]):
//! its range, snapshot watermark and TTL clock define what is
//! fingerprinted.

use crate::device::BlockDevice;
use crate::error::Error;
use crate::scan::Scan;

/// Order-independent, mergeable digest of a set of key-value pairs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Fingerprint {
    count: u64,
    sum: u64,
}

impl Default for Fingerprint {
    fn default() -> Self {
        Self::EMPTY
    }
}

impl Fingerprint {
    /// The fingerprint of no entries; the identity for [`merge`](Self::merge).
    pub const EMPTY: Self = Self { count: 0, sum: 0 };

    /// Number of entries folded in.
    #[must_use]
    pub const fn count(&self) -> u64 {
        self.count
    }

    /// The 64-bit digest of the entries folded in (`0` for none).
    #[must_use]
    pub const fn digest(&self) -> u64 {
        self.sum
    }

    /// Folds in one entry. Key and value are length-prefixed, so
    /// `("ab", "c")` and `("a", "bc")` hash differently.
    pub const fn add(&mut self, key: &[u8], val: &[u8]) {
        self.count = self.count.wrapping_add(1);
        self.sum = self.sum.wrapping_add(entry_hash(key, val));
    }

    /// The fingerprint of the union of two *disjoint* entry sets.
    #[must_use]
    pub const fn merge(self, other: Self) -> Self {
        Self {
            count: self.count.wrapping_add(other.count),
            sum: self.sum.wrapping_add(other.sum),
        }
    }

    /// Big-endian wire form: count, then digest. Sixteen bytes.
    #[must_use]
    pub const fn to_bytes(&self) -> [u8; 16] {
        let c = self.count.to_be_bytes();
        let s = self.sum.to_be_bytes();
        let mut out = [0u8; 16];
        let mut i = 0;
        while i < 8 {
            out[i] = c[i];
            out[8 + i] = s[i];
            i += 1;
        }
        out
    }

    /// Parses [`to_bytes`](Self::to_bytes)' output.
    #[must_use]
    pub const fn from_bytes(b: [u8; 16]) -> Self {
        let mut c = [0u8; 8];
        let mut s = [0u8; 8];
        let mut i = 0;
        while i < 8 {
            c[i] = b[i];
            s[i] = b[8 + i];
            i += 1;
        }
        Self {
            count: u64::from_be_bytes(c),
            sum: u64::from_be_bytes(s),
        }
    }
}

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// FNV-1a over `bytes`, continuing from `h`.
const fn fnv(mut h: u64, bytes: &[u8]) -> u64 {
    let mut i = 0;
    while i < bytes.len() {
        h ^= bytes[i] as u64;
        h = h.wrapping_mul(FNV_PRIME);
        i += 1;
    }
    h
}

/// The 64-bit hash of one entry: length-prefixed FNV-1a, then [`mix`].
/// Shared with [`sketch`](crate::sketch), whose entry ids are these.
pub(crate) const fn entry_hash(key: &[u8], val: &[u8]) -> u64 {
    let mut h = FNV_OFFSET;
    h = fnv(h, &(key.len() as u64).to_le_bytes());
    h = fnv(h, key);
    h = fnv(h, &(val.len() as u64).to_le_bytes());
    h = fnv(h, val);
    mix(h)
}

/// `SplitMix64` finalizer: spreads FNV's output over all 64 bits before
/// summing, so the sum is not a linear function of the inputs.
pub(crate) const fn mix(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// Fingerprints every entry the positioned `scan` yields.
///
/// `key_buf` and `val_buf` are scratch for the scan; size them
/// `KEY_MAX` and `VAL_MAX`.
///
/// # Errors
///
/// Whatever [`Scan::next`] reports.
pub async fn fingerprint<
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
    key_buf: &mut [u8],
    val_buf: &mut [u8],
) -> Result<Fingerprint, Error<D::Error>> {
    let mut out = [Fingerprint::EMPTY];
    fingerprint_buckets(scan, &[], &mut out, key_buf, val_buf).await?;
    Ok(out[0])
}

/// Fingerprints the positioned `scan` into `splits.len() + 1` buckets in
/// one pass.
///
/// Bucket `0` holds keys below `splits[0]`, bucket `i` keys in
/// `[splits[i - 1], splits[i])`, the last bucket keys at or above the
/// last split. Buckets already in `out` are overwritten.
///
/// # Errors
///
/// [`Error::BadBufferLen`] when `out.len() != splits.len() + 1`,
/// [`Error::BadConfig`] when `splits` is not strictly ascending, or
/// whatever [`Scan::next`] reports.
pub async fn fingerprint_buckets<
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
    splits: &[&[u8]],
    out: &mut [Fingerprint],
    key_buf: &mut [u8],
    val_buf: &mut [u8],
) -> Result<(), Error<D::Error>> {
    if out.len() != splits.len() + 1 {
        return Err(Error::BadBufferLen);
    }
    if splits.windows(2).any(|w| w[0] >= w[1]) {
        return Err(Error::BadConfig);
    }
    out.fill(Fingerprint::EMPTY);
    while let Some((klen, vlen)) = scan.next(key_buf, val_buf).await? {
        let key = &key_buf[..klen];
        // Scan output is ascending, but a binary search keeps this
        // independent of that and is cheap for any bucket count.
        let bucket = splits.partition_point(|s| *s <= key);
        out[bucket].add(key, &val_buf[..vlen]);
    }
    Ok(())
}
