//! Immutable sorted-run tables: streaming writer and point-lookup reader.
//!
//! On-device layout (v0.17):
//!
//! ```text
//! [data block]* [rdel block]* [bloom block]+ [index block] [footer block]
//! ```
//!
//! The range-tombstone (rdel) section follows the data so a writer can
//! decide it last: compaction clips each output table's tombstones to the
//! key range the table ends up covering.
//!
//! Every block is `BLOCK` bytes; the last 4 bytes are the CRC32 of the
//! preceding payload bytes. All integers are little-endian.
//!
//! - Data block: entries `key_len u16 | val_len u16 | seq u64 | op u8 |
//!   key | val`, then restart offsets (`u16` each, one every 16 entries),
//!   then the restart count (`u16`). Restart offsets make a block
//!   binary-searchable without a full scan.
//! - Bloom blocks: a bit array in 64-byte buckets, at most `BLOOM_BYTES`
//!   long. A key hashes (hand-rolled splitmix64 over an Fx fold) to one
//!   bucket, and its `k` probes (stored in the footer) are double-hashed
//!   inside those 512 bits, so a lookup reads 64 bytes of one block. A
//!   filter under 64 bytes is one bucket. A filter longer than a block
//!   fills consecutive blocks, `(BLOCK - 4) / 64` buckets each; its
//!   length is chosen per table from the entry count ([`BloomShape`]) and
//!   stored in the footer.
//! - Index block: one entry per data block,
//!   `first_key_len u16 | first_key | block_id u64 | max_seq u64`.
//! - Rdel block: range tombstones `start_len u16 | end_len u16 | seq u64 |
//!   start | end`, sorted by `start` ascending (ties in any sequence
//!   order: compaction clips pieces to its outputs' ranges), then
//!   `count u16`.
//! - Footer block: `magic u64 = "hrtsst04" | index_block u64 |
//!   bloom_block u64 | entry_count u64 | k u8 | rdel_blocks u32 |
//!   min_seq u64 | bloom_len u32`. The rdel section is `[bloom_block -
//!   rdel_blocks, bloom_block)`; the bloom section is `[bloom_block,
//!   index_block)`.
//!
//! A failed CRC is [`Error::CorruptBlock`] for footer, index, data, and
//! range-tombstone blocks: corruption is never silent, and never reads as
//! "absent" (which would let an older version elsewhere win). Only the
//! bloom blocks are advisory — a bad bloom CRC just disables the filter
//! for the keys whose bucket is in that block.
//!
//! Bounds: restart offsets are `u16`, so a data block effectively tops out
//! at 64 KiB, and at most 2048 entries share one data block (restart offsets
//! live in a fixed `[u16; 128]`). A table whose index would overflow one
//! block fails cleanly with [`Error::TableTooLarge`].

use core::future::poll_fn;

use crate::cache::CachePort;
use crate::compress::{CompressScratch, Inflate, decompress};
use crate::crc::crc32;
use crate::device::BlockDevice;
use crate::error::Error;
use crate::manifest::{KeyBound, TableRef};
use crate::wal::Op;

/// Footer magic: ASCII "hrtsst04" as stored on device.
///
/// Pre-1.0 format policy: the magic changes whenever the table layout
/// does. "lsmtable" was the v0.2–v0.16 layout (rdel section first, no
/// `min_seq`); "hrtsst02" is v0.17 (rdel section after the data, footer
/// `min_seq`); "hrtsst03" was an unreleased v0.20 layout (the bloom
/// filter bucketed: an older table's filter read by the new rule would
/// answer "absent" for keys it holds); "hrtsst04" is v0.20 (the filter
/// may span blocks, and the footer records its length). A foreign magic
/// is [`Error::CorruptBlock`], never a misparse.
pub const SSTABLE_MAGIC: u64 = u64::from_le_bytes(*b"hrtsst04");

/// Bytes of a footer block that carry fields; the rest is zero padding.
const FOOTER_LEN: usize = 49;

/// One sorted-run entry: exactly one per key (the memtable already keeps
/// only the newest version of each key).
#[derive(Debug, Clone, Copy)]
pub struct SstEntry<'a> {
    /// Key bytes (non-empty, `<= 0xFFFF`).
    pub key: &'a [u8],
    /// Value bytes (empty for tombstones).
    pub val: &'a [u8],
    /// Sequence number of the mutation.
    pub seq: u64,
    /// True for a deletion marker.
    pub tombstone: bool,
    /// Absolute expiry tick; 0 = no expiry. Stored on the wire only for
    /// non-tombstone entries (op byte [`Op::PutTtl`]).
    pub expire_at: u64,
}

impl<'a, const CAP: usize, const ARENA: usize, const KEY_MAX: usize, const VAL_MAX: usize>
    From<crate::memtable::Entry<'a, CAP, ARENA, KEY_MAX, VAL_MAX>> for SstEntry<'a>
{
    /// Converts a live memtable entry (tombstones already carry no value).
    fn from(e: crate::memtable::Entry<'a, CAP, ARENA, KEY_MAX, VAL_MAX>) -> Self {
        Self {
            key: e.key,
            val: e.val,
            seq: e.seq,
            tombstone: e.tombstone,
            expire_at: e.expire_at,
        }
    }
}

/// Pass-1 result: the table's shape, computed without any I/O.
///
/// [`plan_table`] and [`write_table`] share the packing rule, so the block
/// count here always matches the blocks the writer emits.
#[derive(Debug, Clone)]
pub struct TablePlan<const KEY_MAX: usize> {
    /// Data blocks the table will occupy.
    pub data_blocks: u64,
    /// Total entries (including tombstones).
    pub entry_count: u64,
    /// Highest sequence number.
    pub max_seq: u64,
    /// Lowest sequence number (`u64::MAX` for an empty plan).
    pub min_seq: u64,
    /// Smallest key.
    pub first_key: KeyBound<KEY_MAX>,
    /// Largest key.
    pub last_key: KeyBound<KEY_MAX>,
}

/// Fixed entry header bytes: `key_len u16 | val_len u16 | seq u64 | op u8`.
pub(crate) const ENTRY_HEADER: usize = 13;
/// One restart offset every this many entries.
const RESTART_INTERVAL: u64 = 16;
/// Restart offsets live in a fixed array: the per-block entry cap.
const MAX_ENTRIES_PER_BLOCK: u64 = 128 * RESTART_INTERVAL;
/// CRC32 bytes trailing every block.
pub(crate) const CRC_LEN: usize = 4;

/// Validated entry sizes: `(total_len, key_len, val_len)`.
///
/// Tombstones store no value bytes. Empty keys are rejected: the memtable
/// never holds them, and the reader relies on non-empty keys to tell index
/// entries apart from zero padding.
fn entry_sizes<E>(e: &SstEntry<'_>) -> Result<(usize, u16, u16), Error<E>> {
    if e.key.is_empty() {
        return Err(Error::EmptyKey);
    }
    let kl = u16::try_from(e.key.len()).map_err(|_| Error::KeyTooLarge {
        len: e.key.len(),
        max: usize::from(u16::MAX),
    })?;
    let vl_raw = if e.tombstone { 0 } else { e.val.len() };
    let vl = u16::try_from(vl_raw).map_err(|_| Error::ValueTooLarge {
        len: vl_raw,
        max: usize::from(u16::MAX),
    })?;
    // TTL entries carry an 8-byte expiry after the value.
    let ttl = if !e.tombstone && e.expire_at != 0 {
        8
    } else {
        0
    };
    Ok((ENTRY_HEADER + e.key.len() + vl_raw + ttl, kl, vl))
}

/// Whether an entry of `elen` bytes fits in a data block holding `payload`
/// bytes across `entries` entries. Panic-free: saturating math throughout.
fn entry_fits<const BLOCK: usize>(payload: usize, entries: u64, elen: usize) -> bool {
    if elen > BLOCK {
        return false;
    }
    // Restart offsets live in a fixed [u16; 128].
    if entries >= MAX_ENTRIES_PER_BLOCK {
        return false;
    }
    // Tail if the entry were added: ceil((entries+1)/16) restart offsets,
    // one u16 restart count, one u32 CRC.
    let new_r = entries / RESTART_INTERVAL + 1;
    let tail = new_r.saturating_mul(2).saturating_add(6);
    let Ok(tail) = usize::try_from(tail) else {
        return false;
    };
    payload.saturating_add(elen).saturating_add(tail) <= BLOCK
}

/// Largest restart tail a data block can carry: 128 restart offsets, the
/// restart count, and the CRC.
pub(crate) const MAX_DATA_TAIL: usize = 128 * 2 + 2 + CRC_LEN;

/// Upper bound on the blocks greedy packing uses for `n` entries of at
/// most `emax` bytes each and `total` bytes overall, when a block holds at
/// most `room` payload bytes and `per_block` entries. Every block but the
/// last was sealed because the next entry (at most `emax` bytes) no longer
/// fit — so it holds more than `room - emax` bytes — or because it reached
/// `per_block` entries.
pub(crate) const fn packed_blocks_bound(
    total: usize,
    n: usize,
    emax: usize,
    room: usize,
    per_block: usize,
) -> u64 {
    if n == 0 {
        return 0;
    }
    let by_bytes = if room > emax {
        total / (room - emax + 1)
    } else {
        n
    };
    let bound = by_bytes.saturating_add(n / per_block).saturating_add(1);
    // Every block holds at least one entry.
    let bound = if bound < n { bound } else { n };
    bound as u64
}

/// Upper bound on the blocks one flush can occupy: a full memtable
/// (`cap` entries over `arena` key and value bytes) packed into a data
/// section and a range-tombstone section, plus the bloom blocks a table of
/// `cap` entries gets, the index, and the footer.
/// Both sections are bounded as if the whole memtable went into each, so
/// the bound holds for any mix. `Db::open` refuses a table-slot layout
/// smaller than this: a memtable that could never flush would wedge the
/// write path.
#[must_use]
pub(crate) const fn max_flush_blocks<const BLOCK: usize>(
    cap: usize,
    arena: usize,
    key_max: usize,
    val_max: usize,
    bloom_bytes: usize,
) -> u64 {
    // Point entries: header, key, value, and an optional TTL expiry.
    let emax = ENTRY_HEADER + key_max + val_max + 8;
    let data_total = arena + cap * (ENTRY_HEADER + 8);
    // 2048 entries per block: the conversion is exact.
    #[allow(clippy::cast_possible_truncation)]
    let per_block = MAX_ENTRIES_PER_BLOCK as usize;
    let data = packed_blocks_bound(
        data_total,
        cap,
        emax,
        BLOCK.saturating_sub(MAX_DATA_TAIL),
        per_block,
    );
    // Range tombstones: `start_len u16 | end_len u16 | seq u64 | start | end`.
    let rmax = 12 + 2 * key_max;
    let rdel_total = arena + cap * 12;
    let rdel = packed_blocks_bound(
        rdel_total,
        cap,
        rmax,
        BLOCK.saturating_sub(RDEL_TRAILER),
        usize::MAX,
    );
    // A flush holds at most `cap` entries, and a filter never shrinks as
    // the entry count grows.
    let bloom = BloomShape::for_entries(bloom_bytes, BLOCK, cap as u64).blocks as u64;
    data + rdel + bloom + 2
}

/// Number of bloom probes for `entries` keys in a `bloom_bits`-bit filter.
///
/// Optimal `k ≈ (m/n)·ln 2`, clamped to `[1, 30]`; computed with integer
/// math (`ln 2 ≈ 693/1000`). Past 30 probes the false-positive gain is noise.
#[must_use]
pub fn bloom_k(bloom_bits: usize, entries: u64) -> u8 {
    let k: u64 = if entries > 0
        && let (Ok(m), Some(den)) = (u64::try_from(bloom_bits), entries.checked_mul(1000))
        && let Some(num) = m.checked_mul(693).and_then(|num| num.checked_div(den))
    {
        num
    } else {
        1
    };
    // Clamped to [1, 30], far below `u8::MAX`: the narrowing cast is exact.
    #[allow(clippy::cast_possible_truncation)]
    let narrowed = k.clamp(1, 30) as u8;
    narrowed
}

/// Fx-style fold of the key bytes.
fn fx_fold(key: &[u8]) -> u64 {
    let mut h: u64 = 0x51_7c_c1_b7_27_22_0a_95;
    for &b in key {
        h = h
            .rotate_left(5)
            .wrapping_add(u64::from(b))
            .wrapping_mul(0x51_7c_c1_b7_27_22_0a_95);
    }
    h
}

/// `SplitMix64` finalizer.
const fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// Bytes in a bloom bucket: one cache line, 512 bits.
pub const BLOOM_BUCKET_BYTES: usize = 64;

/// Filter bits per entry that a filter of several blocks is sized for.
///
/// A table gets the fewest bloom blocks that still give each entry this
/// many bits (see [`BloomShape::for_entries`]). Ten bits is about 1% of
/// absent keys passing the filter.
pub const BLOOM_BITS_PER_KEY: u64 = 10;

/// The size a table's bloom filter is stored at: its length in bytes and
/// the blocks that hold it.
///
/// `BLOOM_BYTES` is the most filter a table can have, and what the
/// writer keeps in RAM. A configuration whose filter fits one block
/// always stores all of it: the block is written anyway. One that spans
/// blocks is built at full length and then *folded* down to the table's
/// entry count when the table is finished, so a small table does not pay
/// for a large table's filter. Folding by `f` ORs each run of `f`
/// neighbouring buckets into one; because a key's bucket is its hash
/// scaled to the bucket count, the folded filter is bit for bit the
/// filter that would have been built at the smaller length.
///
/// Folding needs the full bucket count to be a multiple of the folded
/// one. Configure a multi-block `bloom_bytes` as a number of whole blocks
/// with many divisors, such as `16 * ((BLOCK - 4) / 64) * 64`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BloomShape {
    /// Filter bytes: whole 64-byte buckets, or under 64 bytes, which is
    /// one short bucket.
    pub len: usize,
    /// Blocks the filter occupies: at least 1.
    pub blocks: u32,
}

impl BloomShape {
    /// Buckets one bloom block holds.
    #[must_use]
    pub const fn buckets_per_block(block: usize) -> usize {
        block.saturating_sub(CRC_LEN) / BLOOM_BUCKET_BYTES
    }

    /// The largest filter a `max_len`-byte configuration stores: `max_len`
    /// rounded down to whole buckets, or all of it when it is under one
    /// bucket (and, for a block too small to hold a bucket, whatever of
    /// it fits the block).
    #[must_use]
    pub const fn full(max_len: usize, block: usize) -> Self {
        let per_block = Self::buckets_per_block(block);
        let buckets = max_len / BLOOM_BUCKET_BYTES;
        if buckets == 0 || per_block == 0 {
            let room = block.saturating_sub(CRC_LEN);
            let short = if max_len < BLOOM_BUCKET_BYTES {
                max_len
            } else {
                BLOOM_BUCKET_BYTES - 1
            };
            return Self {
                len: if short < room { short } else { room },
                blocks: 1,
            };
        }
        Self {
            len: buckets * BLOOM_BUCKET_BYTES,
            blocks: Self::blocks_for(buckets, per_block),
        }
    }

    /// Blocks that hold `buckets` buckets, `per_block` to a block.
    const fn blocks_for(buckets: usize, per_block: usize) -> u32 {
        let blocks = buckets.div_ceil(per_block);
        if blocks > u32::MAX as usize {
            u32::MAX
        } else {
            // Checked just above.
            #[allow(clippy::cast_possible_truncation)]
            let narrowed = blocks as u32;
            narrowed
        }
    }

    /// The filter a table of `entries` entries stores, out of a
    /// `max_len`-byte configuration: the fewest blocks that give every
    /// entry [`BLOOM_BITS_PER_KEY`] bits, and the longest filter that
    /// fits those blocks and that the full filter folds to. A table with
    /// more entries than the full filter serves at that rate gets the
    /// full filter.
    ///
    /// Never longer than [`full`](Self::full), and never shorter for more
    /// entries.
    #[must_use]
    pub const fn for_entries(max_len: usize, block: usize, entries: u64) -> Self {
        let full = Self::full(max_len, block);
        if full.blocks <= 1 {
            return full;
        }
        let per_block = Self::buckets_per_block(block);
        let buckets = full.len / BLOOM_BUCKET_BYTES;
        let bucket_bits = (BLOOM_BUCKET_BYTES * 8) as u64;
        let need = entries
            .saturating_mul(BLOOM_BITS_PER_KEY)
            .div_ceil(bucket_bits);
        let mut best = full;
        let mut f = 2;
        while f <= buckets {
            if buckets.is_multiple_of(f) {
                let folded = buckets / f;
                // Larger folds only give fewer buckets.
                if (folded as u64) < need {
                    break;
                }
                let blocks = Self::blocks_for(folded, per_block);
                // Only a fold that saves a block is taken, so within a
                // block count the longest filter wins.
                if blocks < best.blocks {
                    best = Self {
                        len: folded * BLOOM_BUCKET_BYTES,
                        blocks,
                    };
                }
            }
            f += 1;
        }
        best
    }

    /// Where bytes `range` of the filter are stored: the block's index
    /// within the bloom section and the range within that block. `range`
    /// is one bucket (see [`BloomProbe::range`]), which never straddles
    /// blocks.
    #[must_use]
    pub const fn locate(
        block: usize,
        range: &core::ops::Range<usize>,
    ) -> (u64, core::ops::Range<usize>) {
        let per_block = Self::buckets_per_block(block);
        if per_block == 0 {
            return (0, range.start..range.end);
        }
        let stride = per_block * BLOOM_BUCKET_BYTES;
        let index = range.start / stride;
        let base = index * stride;
        (index as u64, range.start - base..range.end - base)
    }
}

/// Blocks a stored filter of `len` bytes occupies: what the writer wrote
/// for it, and what a reader requires of a footer.
const fn bloom_section_blocks<const BLOCK: usize>(len: usize) -> u64 {
    let per_block = BloomShape::buckets_per_block(BLOCK);
    let buckets = len / BLOOM_BUCKET_BYTES;
    if buckets == 0 || per_block == 0 {
        1
    } else {
        buckets.div_ceil(per_block) as u64
    }
}

/// Folds a filter of `full_len` bytes down to `len` bytes in place (see
/// [`BloomShape`]): bucket `i` of the result is the OR of buckets
/// `i * f .. (i + 1) * f` of the input, `f = full_len / len`. Lengths
/// that are not whole buckets, or not a multiple of one another, leave
/// the filter as it is.
fn bloom_fold(bloom: &mut [u8], full_len: usize, len: usize) {
    const B: usize = BLOOM_BUCKET_BYTES;
    if len == 0
        || len >= full_len
        || full_len > bloom.len()
        || !len.is_multiple_of(B)
        || !full_len.is_multiple_of(len)
    {
        return;
    }
    let f = full_len / len;
    for i in 0..len / B {
        // Input bucket `i * f` is at or past output bucket `i`, and the
        // input buckets before it were folded into earlier outputs.
        bloom.copy_within(i * f * B..(i * f + 1) * B, i * B);
        for j in 1..f {
            let (out, src) = bloom.split_at_mut((i * f + j) * B);
            if let (Some(out), Some(src)) = (out.get_mut(i * B..(i + 1) * B), src.get(..B)) {
                for (o, s) in out.iter_mut().zip(src) {
                    *o |= *s;
                }
            }
        }
    }
}

/// Where one key's bloom probes fall: every probe lies inside one bucket
/// of the filter, so a reader needs only that bucket's bytes.
///
/// The bucket comes from one hash and the probe sequence from another,
/// so keys sharing a bucket still spread over its bits.
#[derive(Debug, Clone, Copy)]
pub struct BloomProbe {
    /// First byte of the bucket within the filter.
    start: usize,
    /// Bytes in the bucket: [`BLOOM_BUCKET_BYTES`], or the whole filter
    /// when it is smaller than one bucket. Zero for an empty filter.
    len: usize,
    /// First probe and the (odd) stride of the double hashing.
    first: u64,
    stride: u64,
}

impl BloomProbe {
    /// The probe for `key` in a filter of `bloom_len` bytes.
    #[must_use]
    pub fn new(bloom_len: usize, key: &[u8]) -> Self {
        let h1 = splitmix64(fx_fold(key));
        let h2 = splitmix64(h1 ^ 0x9e37_79b9_7f4a_7c15);
        let buckets = bloom_len / BLOOM_BUCKET_BYTES;
        let (start, len) = if buckets == 0 {
            (0, bloom_len)
        } else {
            // Multiply-shift maps the high half of `h1` evenly onto
            // `0..buckets` (the product is below `buckets << 32`).
            let pick = (h1 >> 32).wrapping_mul(buckets as u64) >> 32;
            let bucket = usize::try_from(pick).map_or(0, |b| b.min(buckets - 1));
            (bucket * BLOOM_BUCKET_BYTES, BLOOM_BUCKET_BYTES)
        };
        Self {
            start,
            len,
            first: h2 & 0xFFFF_FFFF,
            // Odd, so in a 512-bit bucket the probes never repeat.
            stride: (h2 >> 32) | 1,
        }
    }

    /// The bucket's bytes within the filter (and within the bloom block,
    /// which the filter starts).
    #[must_use]
    pub const fn range(&self) -> core::ops::Range<usize> {
        self.start..self.start + self.len
    }

    /// Bit positions inside the bucket.
    fn bits(&self, k: u8) -> impl Iterator<Item = usize> {
        let (first, stride) = (self.first, self.stride);
        // `len` is at most 64, so the bit count and every position fit.
        let bits = (self.len * 8) as u64;
        // A whole bucket is 512 bits: the remainder is a mask, which
        // matters when a key takes a dozen probes. A short filter's bit
        // count is whatever it is, and takes the division.
        let mask = if bits.is_power_of_two() {
            Some(bits - 1)
        } else {
            None
        };
        (0..u64::from(k)).filter_map(move |i| {
            let at = first.wrapping_add(i.wrapping_mul(stride));
            let p = match mask {
                Some(mask) => at & mask,
                None => at.checked_rem(bits)?,
            };
            usize::try_from(p).ok()
        })
    }

    /// Sets the key's `k` probe bits in `bucket`, the filter's bytes at
    /// [`range`](Self::range).
    fn add_to(&self, bucket: &mut [u8], k: u8) {
        for p in self.bits(k) {
            if let Some(byte) = bucket.get_mut(p / 8) {
                *byte |= 1u8 << (p % 8);
            }
        }
    }

    /// Reports whether the key may be present, given `bucket`, the
    /// filter's bytes at [`range`](Self::range): all `k` probe bits set.
    /// An empty or short bucket answers "maybe", never a false negative.
    #[must_use]
    pub fn matches(&self, bucket: &[u8], k: u8) -> bool {
        self.bits(k).all(|p| {
            bucket
                .get(p / 8)
                .is_none_or(|byte| byte >> (p % 8) & 1 == 1)
        })
    }
}

/// Sets the `k` probe bits for `key`. A zero-length filter degrades to "no
/// information" (all lookups proceed); it never panics.
fn bloom_add(bloom: &mut [u8], key: &[u8], k: u8) {
    let probe = BloomProbe::new(bloom.len(), key);
    if let Some(bucket) = bloom.get_mut(probe.range()) {
        probe.add_to(bucket, k);
    }
}

/// Reports whether `key` may be in the filter: all `k` probe bits set.
///
/// Never a false negative; false positives possible. `bloom` must be the
/// exact filter bytes (any length); `k` comes from [`bloom_k`]. A reader
/// holding only part of the filter uses [`BloomProbe`] instead.
#[must_use]
pub fn bloom_maybe_contains(bloom: &[u8], key: &[u8], k: u8) -> bool {
    let probe = BloomProbe::new(bloom.len(), key);
    bloom
        .get(probe.range())
        .is_none_or(|bucket| probe.matches(bucket, k))
}

/// Computes the table shape (pass 1, no I/O).
///
/// Entries must arrive in key-ascending order; the caller pre-allocated
/// `data_blocks + 2` blocks from this plan, plus the table's bloom
/// blocks, before calling [`write_table`].
///
/// # Errors
///
/// [`Error::KeyTooLarge`] / [`Error::ValueTooLarge`] for oversize entries,
/// [`Error::TableTooLarge`] when an entry cannot fit any data block.
pub fn plan_table<'a, E, const BLOCK: usize, const KEY_MAX: usize>(
    entries: impl Iterator<Item = SstEntry<'a>>,
) -> Result<TablePlan<KEY_MAX>, Error<E>> {
    let mut data_blocks = 0u64;
    let mut payload = 0usize;
    let mut n = 0u64;
    let mut count = 0u64;
    let mut max_seq = 0u64;
    let mut min_seq = u64::MAX;
    let mut first: Option<&'a [u8]> = None;
    let mut last: Option<&'a [u8]> = None;
    for e in entries {
        let (elen, _, _) = entry_sizes::<E>(&e)?;
        if !entry_fits::<BLOCK>(payload, n, elen) {
            if n == 0 {
                return Err(Error::TableTooLarge);
            }
            data_blocks += 1;
            payload = 0;
            n = 0;
            if !entry_fits::<BLOCK>(0, 0, elen) {
                return Err(Error::TableTooLarge);
            }
        }
        if first.is_none() {
            first = Some(e.key);
        }
        last = Some(e.key);
        if e.seq > max_seq {
            max_seq = e.seq;
        }
        if e.seq < min_seq {
            min_seq = e.seq;
        }
        payload += elen;
        n += 1;
        count += 1;
    }
    if n > 0 {
        data_blocks += 1;
    }
    let bound = |key: Option<&'a [u8]>| {
        key.map_or(Ok(KeyBound::EMPTY), |k| {
            KeyBound::from_slice(k).ok_or(Error::KeyTooLarge {
                len: k.len(),
                max: KEY_MAX,
            })
        })
    };
    Ok(TablePlan {
        data_blocks,
        entry_count: count,
        max_seq,
        min_seq,
        first_key: bound(first)?,
        last_key: bound(last)?,
    })
}

/// Writes one block: optional restart tail, zero padding, trailing CRC32,
/// then a single device write.
///
/// The restart tail (offsets + count) is right-aligned: it ends exactly at
/// `BLOCK - CRC_LEN`, so the reader locates it from the block end without
/// knowing the payload length. Zero padding, if any, sits between the
/// payload and the tail.
///
/// Data blocks (`restarts.is_some()`) are trial-compressed when `compress`
/// is `Some`: the whole logical body (entries, zero fill, restart tail)
/// is fed to the LZ77 codec, and the compressed form is kept when it
/// saves at least [`COMPRESS_MIN_SAVING`](crate::compress::COMPRESS_MIN_SAVING)
/// bytes. A kept block has bit 15 of the trailer count u16 set, with the
/// low 15 bits carrying the compressed length; the reader branches on
/// that bit. Bloom, index, and footer blocks (`restarts.is_none()`) are
/// never compressed.
async fn seal_block<D: BlockDevice, const BLOCK: usize>(
    device: &mut D,
    id: u64,
    buf: &mut [u8; BLOCK],
    payload_len: usize,
    restarts: Option<&[u16]>,
    mut compress: Option<&mut CompressScratch<BLOCK>>,
) -> Result<(), Error<D::Error>> {
    let body_end = BLOCK - CRC_LEN;
    if let Some(rs) = restarts {
        let tail_len = rs
            .len()
            .checked_mul(2)
            .and_then(|n| n.checked_add(2))
            .ok_or(Error::TableTooLarge)?;
        let rstart = body_end.checked_sub(tail_len).ok_or(Error::TableTooLarge)?;
        // `entry_fits` reserves the worst-case tail, so this cannot trigger
        // for writer-produced blocks.
        if payload_len > rstart {
            return Err(Error::TableTooLarge);
        }
        buf[payload_len..rstart].fill(0);
        let mut off = rstart;
        for &r in rs {
            buf[off..off + 2].copy_from_slice(&r.to_le_bytes());
            off += 2;
        }
        // `entry_fits` caps entries per block, so this always fits.
        let rc = u16::try_from(rs.len()).map_err(|_| Error::TableTooLarge)?;
        buf[off..off + 2].copy_from_slice(&rc.to_le_bytes());
        debug_assert_eq!(off + 2, body_end);
    } else {
        buf[payload_len..body_end].fill(0);
    }

    // Trial-compress data blocks. The codec stages its output
    // separately; on success the staged bytes move into the block body,
    // the tail is zeroed, and the last u16 of the logical body carries
    // the flag: bit 15 set, low 15 bits the compressed length. The
    // original restart count already lives inside the compressed
    // payload, so the flag slot holds only the flag and length.
    if restarts.is_some()
        && let Some(cs) = compress.as_mut()
        && let Some(clen) = cs.compress(&buf[..body_end])
    {
        let (body, _) = buf.split_at_mut(body_end);
        body[..clen].copy_from_slice(&cs.compressed()[..clen]);
        body[clen..body_end].fill(0);
        // `compress` guarantees `clen < 1 << 15`; the mask is
        // belt-and-braces so the flag bit can never be clobbered,
        // hence the narrowing cast is exact.
        #[allow(clippy::cast_possible_truncation)]
        let flagged = 0x8000 | (clen as u16 & 0x7FFF);
        body[body_end - 2..body_end].copy_from_slice(&flagged.to_le_bytes());
    }

    let crc = crc32(&buf[..body_end]);
    buf[body_end..BLOCK].copy_from_slice(&crc.to_le_bytes());
    poll_fn(|cx| device.poll_write_block(cx, id, buf))
        .await
        .map_err(Error::Device)?;
    Ok(())
}

/// What [`TableWriter::push`] did with one entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushOutcome {
    /// Buffered in the staging block; no I/O happened.
    Buffered,
    /// The staging block filled up and was sealed to the device (one block
    /// write); the entry went into a fresh block.
    BlockSealed,
}

/// Table statistics reported by [`TableWriter::finish`].
#[derive(Debug, Clone)]
pub struct FinishedTable<const KEY_MAX: usize> {
    /// Data blocks written.
    pub data_blocks: u64,
    /// Bloom blocks written: at least 1 (see [`BloomShape`]).
    pub bloom_blocks: u32,
    /// Bytes of bloom filter in those blocks, as the footer records.
    pub bloom_len: u32,
    /// Bloom probes per key, as the footer records.
    pub bloom_k: u8,
    /// Key/value entries written (including tombstones).
    pub entry_count: u64,
    /// Highest sequence number written.
    pub max_seq: u64,
    /// Lowest sequence number written, across entries and range
    /// tombstones (`u64::MAX` when the table holds neither).
    pub min_seq: u64,
    /// Smallest key written.
    pub first_key: KeyBound<KEY_MAX>,
    /// Largest key written.
    pub last_key: KeyBound<KEY_MAX>,
}

/// Incremental `SSTable` writer: the pausable form of [`write_table`].
///
/// Entries are pushed one at a time; when the staging data block fills,
/// [`push`](TableWriter::push) seals it to the device and reports
/// [`PushOutcome::BlockSealed`], which is the natural quantum for
/// caller-driven bounded work (compaction). [`finish`](TableWriter::finish)
/// seals the trailing partial block and writes the bloom, index, and footer
/// blocks, then flushes the device, so the table is durable before the
/// manifest commit that makes it visible.
///
/// The writer holds the whole bloom filter, `BLOOM_BYTES` of it, while
/// the table is written, and stores as much of it as the table's entry
/// count calls for ([`BloomShape`]).
///
/// Entries must arrive in key-ascending order with at most one entry per
/// key; keys are bounded by `KEY_MAX` (the table's [`KeyBound`] storage).
/// The table occupies a contiguous run starting at `base`: the caller must
/// have reserved at least the blocks the merge will actually emit (see
/// [`plan_table`] for the exact shape of a known entry sequence).
pub struct TableWriter<const BLOCK: usize, const BLOOM_BYTES: usize, const KEY_MAX: usize> {
    base: u64,
    k: u8,
    data: [u8; BLOCK],
    index: [u8; BLOCK],
    bloom: [u8; BLOOM_BYTES],
    /// Bytes of `bloom` the filter occupies now: the full length while
    /// entries are pushed, the stored length once `finish_meta` has
    /// folded it. A `finish_meta` that runs again (its future was
    /// dropped at a device wait and the caller came back) must not fold
    /// a second time: that would move bits to buckets no key hashes to.
    bloom_now: usize,
    restarts: [u16; 128],
    payload: usize,
    n: u64,
    nrestarts: usize,
    index_len: usize,
    data_blocks: u64,
    entry_count: u64,
    block_first: [u8; KEY_MAX],
    block_first_len: usize,
    block_max_seq: u64,
    first_key: [u8; KEY_MAX],
    first_len: usize,
    last_key: [u8; KEY_MAX],
    last_len: usize,
    max_seq: u64,
    min_seq: u64,
    /// Data blocks this table may occupy; `u64::MAX` when unbounded. A
    /// seal past it is refused, so a table can never outgrow the run its
    /// caller reserved (see [`with_block_limit`](Self::with_block_limit)).
    data_limit: u64,
}

impl<const BLOCK: usize, const BLOOM_BYTES: usize, const KEY_MAX: usize>
    TableWriter<BLOCK, BLOOM_BYTES, KEY_MAX>
{
    /// Bloom probes per key for a table expected to hold `entries`
    /// entries: [`bloom_k`] for the filter such a table stores. Pass the
    /// result to [`new`](Self::new). An estimate that is off costs a few
    /// false positives, never a wrong answer.
    #[must_use]
    pub fn probes_for(entries: u64) -> u8 {
        let shape = BloomShape::for_entries(BLOOM_BYTES, BLOCK, entries);
        bloom_k(shape.len.saturating_mul(8), entries)
    }

    /// The filter bytes the writer fills: the full shape's.
    const FULL_BLOOM: usize = BloomShape::full(BLOOM_BYTES, BLOCK).len;

    /// Starts a table at `base` with `k` bloom probes per key.
    #[must_use]
    pub const fn new(base: u64, k: u8) -> Self {
        Self {
            base,
            k,
            data: [0u8; BLOCK],
            index: [0u8; BLOCK],
            bloom: [0u8; BLOOM_BYTES],
            bloom_now: Self::FULL_BLOOM,
            restarts: [0u16; 128],
            payload: 0,
            n: 0,
            nrestarts: 0,
            index_len: 0,
            data_blocks: 0,
            entry_count: 0,
            block_first: [0u8; KEY_MAX],
            block_first_len: 0,
            block_max_seq: 0,
            first_key: [0u8; KEY_MAX],
            first_len: 0,
            last_key: [0u8; KEY_MAX],
            last_len: 0,
            max_seq: 0,
            min_seq: u64::MAX,
            data_limit: u64::MAX,
        }
    }

    /// Starts the writer over on a new table at `base`, in place: what
    /// [`new`](Self::new) builds, without a second writer on the stack
    /// to move in. The writer holds `BLOOM_BYTES` of filter, which for a
    /// filter of several blocks is more than a caller's stack should
    /// carry twice. The block limit is cleared, as in a new writer.
    pub fn restart(&mut self, base: u64, k: u8) {
        self.base = base;
        self.k = k;
        self.bloom.fill(0);
        self.bloom_now = Self::FULL_BLOOM;
        self.payload = 0;
        self.n = 0;
        self.nrestarts = 0;
        self.index_len = 0;
        self.data_blocks = 0;
        self.entry_count = 0;
        self.block_first_len = 0;
        self.block_max_seq = 0;
        self.first_len = 0;
        self.last_len = 0;
        self.max_seq = 0;
        self.min_seq = u64::MAX;
        self.data_limit = u64::MAX;
    }

    /// Caps the data blocks this table may seal, on a writer already in
    /// place (see [`with_block_limit`](Self::with_block_limit)).
    pub const fn set_block_limit(&mut self, data_blocks: u64) {
        self.data_limit = data_blocks;
    }

    /// Caps the data blocks this table may seal. The caller reserved a
    /// run for the table; sealing past it would overwrite whatever follows
    /// the run, so [`push`](Self::push) and
    /// [`seal_data`](Self::seal_data) refuse with
    /// [`Error::TableTooLarge`] instead.
    #[must_use]
    pub const fn with_block_limit(mut self, data_blocks: u64) -> Self {
        self.data_limit = data_blocks;
        self
    }

    /// Entries buffered in the current (unsealed) data block.
    #[must_use]
    pub const fn pending_entries(&self) -> u64 {
        self.n
    }

    /// Entries pushed so far.
    #[must_use]
    pub const fn entry_count(&self) -> u64 {
        self.entry_count
    }

    /// Data blocks sealed so far (bloom/index/footer not included).
    #[must_use]
    pub const fn data_blocks(&self) -> u64 {
        self.data_blocks
    }

    /// Bytes of the index block used so far: one entry of
    /// `18 + first_key_len` bytes per sealed data block. The index must fit
    /// one block, so a caller splitting output checks the headroom.
    #[must_use]
    pub const fn index_len(&self) -> usize {
        self.index_len
    }

    /// The last key pushed ([`KeyBound::EMPTY`] before the first push).
    #[must_use]
    pub fn last_key(&self) -> KeyBound<KEY_MAX> {
        KeyBound::from_slice(&self.last_key[..self.last_len]).unwrap_or(KeyBound::EMPTY)
    }

    /// Pushes one entry, sealing the staging block first when the entry no
    /// longer fits. Returns [`PushOutcome::BlockSealed`] exactly when a
    /// device write happened.
    ///
    /// `compress` is the caller's compression scratch (`None` = store raw).
    /// A sealed data block is trial-compressed; the compressed form is
    /// kept only when it saves at least
    /// [`COMPRESS_MIN_SAVING`](crate::compress::COMPRESS_MIN_SAVING) bytes.
    ///
    /// # Errors
    ///
    /// [`Error::KeyTooLarge`] / [`Error::ValueTooLarge`] for oversize
    /// entries, [`Error::TableTooLarge`] when a single entry cannot fit in an
    /// empty block, the table outgrows its pre-allocated run, or the index
    /// would overflow one block, or [`Error::Device`] on I/O failure.
    pub async fn push<D: BlockDevice>(
        &mut self,
        device: &mut D,
        e: SstEntry<'_>,
        compress: Option<&mut CompressScratch<BLOCK>>,
    ) -> Result<PushOutcome, Error<D::Error>> {
        let (elen, kl, vl) = entry_sizes::<D::Error>(&e)?;
        if e.key.len() > KEY_MAX {
            return Err(Error::KeyTooLarge {
                len: e.key.len(),
                max: KEY_MAX,
            });
        }
        let sealed = if entry_fits::<BLOCK>(self.payload, self.n, elen) {
            PushOutcome::Buffered
        } else {
            if self.n == 0 {
                return Err(Error::TableTooLarge);
            }
            // The block being sealed plus the one this entry opens must
            // both fit the reserved run.
            if self.data_blocks.saturating_add(2) > self.data_limit {
                return Err(Error::TableTooLarge);
            }
            self.seal_current(device, compress).await?;
            if !entry_fits::<BLOCK>(0, 0, elen) {
                return Err(Error::TableTooLarge);
            }
            PushOutcome::BlockSealed
        };
        if self.n.is_multiple_of(RESTART_INTERVAL) {
            // `entry_fits` guarantees this never overflows the array.
            debug_assert!(self.nrestarts < self.restarts.len());
            self.restarts[self.nrestarts] =
                u16::try_from(self.payload).map_err(|_| Error::TableTooLarge)?;
            self.nrestarts += 1;
        }
        let p = self.payload;
        self.data[p..p + 2].copy_from_slice(&kl.to_le_bytes());
        self.data[p + 2..p + 4].copy_from_slice(&vl.to_le_bytes());
        self.data[p + 4..p + 12].copy_from_slice(&e.seq.to_le_bytes());
        let ttl = !e.tombstone && e.expire_at != 0;
        self.data[p + 12] = if e.tombstone {
            Op::Delete
        } else if ttl {
            Op::PutTtl
        } else {
            Op::Put
        }
        .to_u8();
        let ko = p + ENTRY_HEADER;
        self.data[ko..ko + e.key.len()].copy_from_slice(e.key);
        // Tombstones store no value bytes.
        let vlen = if e.tombstone { 0 } else { e.val.len() };
        self.data[ko + e.key.len()..ko + e.key.len() + vlen].copy_from_slice(&e.val[..vlen]);
        if ttl {
            let eo = ko + e.key.len() + vlen;
            self.data[eo..eo + 8].copy_from_slice(&e.expire_at.to_le_bytes());
        }
        if self.block_first_len == 0 {
            self.block_first[..e.key.len()].copy_from_slice(e.key);
            self.block_first_len = e.key.len();
        }
        if e.seq > self.block_max_seq {
            self.block_max_seq = e.seq;
        }
        if self.first_len == 0 {
            self.first_key[..e.key.len()].copy_from_slice(e.key);
            self.first_len = e.key.len();
        }
        self.last_key[..e.key.len()].copy_from_slice(e.key);
        self.last_len = e.key.len();
        if e.seq > self.max_seq {
            self.max_seq = e.seq;
        }
        if e.seq < self.min_seq {
            self.min_seq = e.seq;
        }
        // `bloom_now` is the full length here: no entry is pushed after
        // `seal_data`, and the filter is folded only after that.
        if let Some(filter) = self.bloom.get_mut(..self.bloom_now) {
            bloom_add(filter, e.key, self.k);
        }
        self.payload += elen;
        self.n += 1;
        self.entry_count += 1;
        Ok(sealed)
    }

    /// Seals the trailing partial data block, if any. Returns the table's
    /// data-block count; the caller writes the range-tombstone section at
    /// `base + data_blocks` (if any) before [`finish_meta`](Self::finish_meta).
    /// Idempotent.
    ///
    /// # Errors
    ///
    /// [`Error::TableTooLarge`] when the block would exceed the writer's block
    /// limit, or [`Error::Device`] on I/O failure.
    pub async fn seal_data<D: BlockDevice>(
        &mut self,
        device: &mut D,
        compress: Option<&mut CompressScratch<BLOCK>>,
    ) -> Result<u64, Error<D::Error>> {
        if self.n > 0 {
            if self.data_blocks >= self.data_limit {
                return Err(Error::TableTooLarge);
            }
            self.seal_current(device, compress).await?;
        }
        Ok(self.data_blocks)
    }

    /// Seals the staging block at `base + data_blocks`, records its index
    /// entry, and resets the staging state for the next block.
    async fn seal_current<D: BlockDevice>(
        &mut self,
        device: &mut D,
        compress: Option<&mut CompressScratch<BLOCK>>,
    ) -> Result<(), Error<D::Error>> {
        let id = self
            .base
            .checked_add(self.data_blocks)
            .ok_or(Error::TableTooLarge)?;
        seal_block(
            device,
            id,
            &mut self.data,
            self.payload,
            Some(&self.restarts[..self.nrestarts]),
            compress,
        )
        .await?;
        append_index::<D::Error, BLOCK>(
            &mut self.index,
            &mut self.index_len,
            Some(&self.block_first[..self.block_first_len]),
            id,
            self.block_max_seq,
        )?;
        self.data_blocks += 1;
        self.payload = 0;
        self.n = 0;
        self.nrestarts = 0;
        self.block_first_len = 0;
        self.block_max_seq = 0;
        self.data.fill(0);
        Ok(())
    }

    /// The data staging buffer, idle between [`seal_data`](Self::seal_data)
    /// and [`finish_meta`](Self::finish_meta): the caller may stage the
    /// range-tombstone section in it instead of holding another block.
    /// `finish_meta` clears it before use; no entry may be pushed after
    /// `seal_data`.
    pub(crate) fn spare_block(&mut self) -> &mut [u8; BLOCK] {
        debug_assert_eq!(self.n, 0, "seal_data must run before spare_block");
        &mut self.data
    }

    /// Writes the bloom, index, and footer blocks after the data section
    /// and the `rdel_blocks`-block range-tombstone section the caller
    /// wrote at `base + data_blocks`, then flushes the device so the table
    /// is durable before the manifest commit that makes it visible. The
    /// bloom filter is folded to the entry count first
    /// ([`BloomShape::for_entries`]); the blocks it took are in the
    /// result. Calling it again writes the same blocks again.
    /// `rdel_min_seq` is the lowest range-tombstone sequence (`u64::MAX`
    /// when there are none). Call [`seal_data`](Self::seal_data) first.
    ///
    /// # Errors
    ///
    /// [`Error::TableTooLarge`] on block-id overflow, or [`Error::Device`] on I/O
    /// failure.
    pub async fn finish_meta<D: BlockDevice>(
        &mut self,
        device: &mut D,
        rdel_blocks: u32,
        rdel_min_seq: u64,
    ) -> Result<FinishedTable<KEY_MAX>, Error<D::Error>> {
        debug_assert_eq!(self.n, 0, "seal_data must run before finish_meta");
        let min_seq = self.min_seq.min(rdel_min_seq);
        // Bloom blocks: the filter, folded to this table's entry count,
        // a block's worth of buckets at a time through the data staging
        // buffer (it is free now).
        let bloom_id = self
            .base
            .checked_add(self.data_blocks)
            .and_then(|b| b.checked_add(u64::from(rdel_blocks)))
            .ok_or(Error::TableTooLarge)?;
        let shape = BloomShape::for_entries(BLOOM_BYTES, BLOCK, self.entry_count);
        // Once: a second `finish_meta` finds the filter at its stored
        // length and writes the same blocks again.
        if self.bloom_now != shape.len {
            bloom_fold(&mut self.bloom, self.bloom_now, shape.len);
            self.bloom_now = shape.len;
        }
        let bloom_len = u32::try_from(shape.len).map_err(|_| Error::TableTooLarge)?;
        // A short filter (under one bucket) is one chunk.
        let stride = BloomShape::buckets_per_block(BLOCK) * BLOOM_BUCKET_BYTES;
        let per_block = if stride == 0 {
            shape.len.max(1)
        } else {
            stride
        };
        let filter = self.bloom.get(..shape.len).unwrap_or(&[]);
        let mut chunks = filter.chunks(per_block);
        for i in 0..u64::from(shape.blocks) {
            let chunk = chunks.next().unwrap_or(&[]);
            let id = bloom_id.checked_add(i).ok_or(Error::TableTooLarge)?;
            self.data.fill(0);
            if let Some(dst) = self.data.get_mut(..chunk.len()) {
                dst.copy_from_slice(chunk);
            }
            seal_block(device, id, &mut self.data, chunk.len(), None, None).await?;
        }

        // Index block.
        let index_id = bloom_id
            .checked_add(u64::from(shape.blocks))
            .ok_or(Error::TableTooLarge)?;
        seal_block(
            device,
            index_id,
            &mut self.index,
            self.index_len,
            None,
            None,
        )
        .await?;

        // Footer block: magic | index | bloom | entry_count | k |
        // rdel_blocks | min_seq | bloom_len.
        let footer_id = index_id.checked_add(1).ok_or(Error::TableTooLarge)?;
        self.data.fill(0);
        self.data[0..8].copy_from_slice(&SSTABLE_MAGIC.to_le_bytes());
        self.data[8..16].copy_from_slice(&index_id.to_le_bytes());
        self.data[16..24].copy_from_slice(&bloom_id.to_le_bytes());
        self.data[24..32].copy_from_slice(&self.entry_count.to_le_bytes());
        self.data[32] = self.k;
        self.data[33..37].copy_from_slice(&rdel_blocks.to_le_bytes());
        self.data[37..45].copy_from_slice(&min_seq.to_le_bytes());
        self.data[45..FOOTER_LEN].copy_from_slice(&bloom_len.to_le_bytes());
        seal_block(device, footer_id, &mut self.data, FOOTER_LEN, None, None).await?;

        // Table blocks are durable before the manifest commit makes them
        // visible.
        poll_fn(|cx| device.poll_flush(cx))
            .await
            .map_err(Error::Device)?;

        let first_key = if self.first_len == 0 {
            KeyBound::EMPTY
        } else {
            KeyBound::from_slice(&self.first_key[..self.first_len]).ok_or(Error::EmptyKey)?
        };
        let last_key = if self.last_len == 0 {
            KeyBound::EMPTY
        } else {
            KeyBound::from_slice(&self.last_key[..self.last_len]).ok_or(Error::EmptyKey)?
        };
        Ok(FinishedTable {
            data_blocks: self.data_blocks,
            bloom_blocks: shape.blocks,
            bloom_len,
            bloom_k: self.k,
            entry_count: self.entry_count,
            max_seq: self.max_seq,
            min_seq,
            first_key,
            last_key,
        })
    }

    /// Seals the trailing data block and writes the bloom, index, and
    /// footer blocks for a table with no range-tombstone section, then
    /// flushes the device. Reports the table's shape for the manifest's
    /// [`TableRef`].
    ///
    /// # Errors
    ///
    /// [`Error::TableTooLarge`] when the table outgrows its block limit, or
    /// [`Error::Device`] on I/O failure.
    ///
    /// `compress` is the caller's compression scratch (`None` = store raw);
    /// see [`push`](TableWriter::push).
    pub async fn finish<D: BlockDevice>(
        &mut self,
        device: &mut D,
        compress: Option<&mut CompressScratch<BLOCK>>,
    ) -> Result<FinishedTable<KEY_MAX>, Error<D::Error>> {
        self.seal_data(device, compress).await?;
        self.finish_meta(device, 0, u64::MAX).await
    }
}

/// Streams a table with no range tombstones: the one-shot form of
/// [`TableWriter`].
///
/// Entries must arrive in key-ascending order (the same order
/// [`plan_table`] plans); `base` is a pre-allocated run of
/// `plan.data_blocks + 2` blocks plus the table's bloom blocks
/// ([`BloomShape::for_entries`]: one, unless `BLOOM_BYTES` spans blocks).
/// Returns the blocks written.
///
/// Block order on device: `[data]* [bloom]+ [index] [footer]`. The device is
/// flushed at the end, so the table is durable before the manifest commit
/// that makes it visible.
///
/// `compress` is the caller's compression scratch (`None` = store all data
/// blocks raw); see [`TableWriter::push`].
///
/// # Errors
///
/// [`Error::KeyTooLarge`] / [`Error::ValueTooLarge`] for oversize entries,
/// [`Error::TableTooLarge`] when the table outgrows its pre-allocated run or the
/// index would overflow one block, or [`Error::Device`] on I/O failure.
pub async fn write_table<
    'a,
    D,
    const BLOCK: usize,
    const BLOOM_BYTES: usize,
    const KEY_MAX: usize,
>(
    device: &mut D,
    base: u64,
    k: u8,
    entries: impl Iterator<Item = SstEntry<'a>>,
    compress: Option<&mut CompressScratch<BLOCK>>,
) -> Result<u64, Error<D::Error>>
where
    D: BlockDevice,
{
    let mut w = TableWriter::<BLOCK, BLOOM_BYTES, KEY_MAX>::new(base, k);
    // `compress` is threaded through every seal: reborrow the `&mut`
    // each call since the parameter takes it by value.
    let mut cs = compress;
    for e in entries {
        w.push(device, e, cs.as_deref_mut()).await?;
    }
    let done = w.finish(device, cs).await?;
    Ok(done.data_blocks + u64::from(done.bloom_blocks) + 2)
}

/// One range tombstone staged for a table's rdel section.
#[derive(Debug, Clone, Copy)]
pub struct RdelEntry<'a> {
    /// Inclusive start (non-empty).
    pub start: &'a [u8],
    /// Exclusive end (non-empty).
    pub end: &'a [u8],
    /// Sequence number of the range delete.
    pub seq: u64,
}

/// Trailer bytes of an rdel block: `count u16` + `crc32`.
pub(crate) const RDEL_TRAILER: usize = 6;

/// Planned shape of a table's range-tombstone section.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RdelPlan<'a> {
    /// Blocks the section occupies (0 when there are no tombstones).
    pub(crate) blocks: u32,
    /// Smallest tombstone start (`None` when empty).
    pub(crate) first: Option<&'a [u8]>,
    /// Largest tombstone end (`None` when empty).
    pub(crate) max_end: Option<&'a [u8]>,
    /// Highest tombstone sequence number (0 when empty).
    pub(crate) max_seq: u64,
    /// Lowest tombstone sequence number (`u64::MAX` when empty).
    pub(crate) min_seq: u64,
}

/// Plans a table's range-tombstone section: block count, smallest start,
/// largest end, highest sequence. Uses the exact packing rule as
/// [`write_rdel_blocks`] so plan and write agree.
pub(crate) fn plan_rdel_blocks<'a, E, const BLOCK: usize>(
    rdels: impl Iterator<Item = RdelEntry<'a>>,
) -> Result<RdelPlan<'a>, Error<E>> {
    let mut blocks = 0u32;
    let mut payload = 0usize;
    let mut plan = RdelPlan {
        blocks: 0,
        first: None,
        max_end: None,
        max_seq: 0,
        min_seq: u64::MAX,
    };
    for r in rdels {
        if plan.first.is_none() {
            plan.first = Some(r.start);
        }
        if plan.max_end.is_none_or(|end| r.end > end) {
            plan.max_end = Some(r.end);
        }
        if r.seq > plan.max_seq {
            plan.max_seq = r.seq;
        }
        if r.seq < plan.min_seq {
            plan.min_seq = r.seq;
        }
        let el = rdel_entry_len::<E>(r.start.len(), r.end.len())?;
        if el + RDEL_TRAILER > BLOCK {
            return Err(Error::TableTooLarge);
        }
        if payload + el + RDEL_TRAILER > BLOCK {
            blocks += 1;
            payload = 0;
        }
        payload += el;
    }
    if payload > 0 {
        blocks += 1;
    }
    plan.blocks = blocks;
    Ok(plan)
}

/// Range tombstones of maximal size (`12 + 2 * KEY_MAX` bytes) that fit
/// one rdel block: the per-block floor behind
/// [`rdel_blocks_bound`](crate::compact::rdel_blocks_bound). 0 when not
/// even one fits (`Db` const-asserts it does).
#[must_use]
pub(crate) const fn rdel_entries_per_block<const BLOCK: usize, const KEY_MAX: usize>() -> u64 {
    let max_entry = 12 + 2 * KEY_MAX;
    (BLOCK.saturating_sub(RDEL_TRAILER) / max_entry) as u64
}

/// Wire length of one rdel entry: `start_len u16 | end_len u16 | seq u64 |
/// start | end`.
fn rdel_entry_len<E>(start_len: usize, end_len: usize) -> Result<usize, Error<E>> {
    start_len
        .checked_add(end_len)
        .and_then(|n| n.checked_add(12))
        .ok_or(Error::TableTooLarge)
}

/// Seals one rdel block: entries at `[0..payload]`, `count u16` at
/// `[BLOCK-6..BLOCK-4]`, CRC over `[0..BLOCK-4]`. Rdel blocks are never
/// compressed (like bloom/index/footer).
async fn seal_rdel_block<D: BlockDevice, const BLOCK: usize>(
    device: &mut D,
    id: u64,
    buf: &mut [u8; BLOCK],
    payload: usize,
    count: u32,
) -> Result<(), Error<D::Error>> {
    let body_end = BLOCK - CRC_LEN;
    buf[payload..body_end - 2].fill(0);
    let count16 = u16::try_from(count).map_err(|_| Error::TableTooLarge)?;
    buf[body_end - 2..body_end].copy_from_slice(&count16.to_le_bytes());
    let crc = crc32(&buf[..body_end]);
    buf[body_end..BLOCK].copy_from_slice(&crc.to_le_bytes());
    poll_fn(|cx| device.poll_write_block(cx, id, buf))
        .await
        .map_err(Error::Device)?;
    buf.fill(0);
    Ok(())
}

/// Streaming writer for a table's range-tombstone section: `push` one
/// tombstone at a time, `finish` seals the trailing partial block.
/// Returns block counts, never allocates.
///
/// The section layout is `[rdel block]*`: entries packed greedily at
/// `[0..payload]`, zero padding, `count u16` at `[BLOCK-6..BLOCK-4]`,
/// CRC32 over `[0..BLOCK-4]`. Rdel blocks are never compressed.
pub(crate) struct RdelWriter<'b, const BLOCK: usize> {
    base: u64,
    /// The staging block, borrowed: the section is written between a
    /// table's data and meta blocks, when the table writer's data buffer
    /// is idle (see [`TableWriter::spare_block`]).
    buf: &'b mut [u8; BLOCK],
    payload: usize,
    count: u32,
    blocks: u32,
    /// Blocks the section may occupy; sealing past it is refused.
    limit: u32,
}

impl<'b, const BLOCK: usize> RdelWriter<'b, BLOCK> {
    /// Writer for the section starting at `base`, staging blocks in `buf`
    /// (its prior contents are irrelevant). `const`-constructible.
    #[must_use]
    pub(crate) const fn new(base: u64, buf: &'b mut [u8; BLOCK]) -> Self {
        Self {
            base,
            buf,
            payload: 0,
            count: 0,
            blocks: 0,
            limit: u32::MAX,
        }
    }

    /// Caps the section at `blocks` blocks: a seal past it fails with
    /// [`Error::TableTooLarge`] instead of writing beyond the caller's budget.
    #[must_use]
    pub(crate) const fn with_block_limit(mut self, blocks: u32) -> Self {
        self.limit = blocks;
        self
    }

    /// Appends one tombstone, sealing the current block first when the
    /// entry no longer fits.
    ///
    /// # Errors
    ///
    /// [`Error::TableTooLarge`] when the tombstone cannot fit in an empty
    /// block, or [`Error::Device`] on I/O failure.
    pub(crate) async fn push<D: BlockDevice>(
        &mut self,
        device: &mut D,
        r: RdelEntry<'_>,
    ) -> Result<(), Error<D::Error>> {
        let slen = u16::try_from(r.start.len()).map_err(|_| Error::KeyTooLarge {
            len: r.start.len(),
            max: usize::from(u16::MAX),
        })?;
        let elen = u16::try_from(r.end.len()).map_err(|_| Error::KeyTooLarge {
            len: r.end.len(),
            max: usize::from(u16::MAX),
        })?;
        let el = rdel_entry_len::<D::Error>(usize::from(slen), usize::from(elen))?;
        if el + RDEL_TRAILER > BLOCK {
            return Err(Error::TableTooLarge);
        }
        if self.payload + el + RDEL_TRAILER > BLOCK {
            if self.blocks >= self.limit {
                return Err(Error::TableTooLarge);
            }
            let id = self
                .base
                .checked_add(u64::from(self.blocks))
                .ok_or(Error::TableTooLarge)?;
            seal_rdel_block(device, id, self.buf, self.payload, self.count).await?;
            self.blocks += 1;
            self.payload = 0;
            self.count = 0;
        }
        let sl = usize::from(slen);
        let elen_usize = usize::from(elen);
        self.buf[self.payload..self.payload + 2].copy_from_slice(&slen.to_le_bytes());
        self.buf[self.payload + 2..self.payload + 4].copy_from_slice(&elen.to_le_bytes());
        self.buf[self.payload + 4..self.payload + 12].copy_from_slice(&r.seq.to_le_bytes());
        self.buf[self.payload + 12..self.payload + 12 + sl].copy_from_slice(r.start);
        self.buf[self.payload + 12 + sl..self.payload + 12 + sl + elen_usize]
            .copy_from_slice(r.end);
        self.payload += el;
        self.count += 1;
        Ok(())
    }

    /// Seals the trailing partial block. Returns the total blocks
    /// written — 0 when nothing was pushed (no section exists).
    ///
    /// # Errors
    ///
    /// [`Error::Device`] on I/O failure.
    pub(crate) async fn finish<D: BlockDevice>(
        &mut self,
        device: &mut D,
    ) -> Result<u32, Error<D::Error>> {
        if self.count > 0 {
            if self.blocks >= self.limit {
                return Err(Error::TableTooLarge);
            }
            let id = self
                .base
                .checked_add(u64::from(self.blocks))
                .ok_or(Error::TableTooLarge)?;
            seal_rdel_block(device, id, self.buf, self.payload, self.count).await?;
            self.blocks += 1;
            self.count = 0;
            self.payload = 0;
        }
        Ok(self.blocks)
    }
}

/// Writes a table's range-tombstone section at `[base, base+n)`: `rdels`
/// in `start`-ascending order, packed greedily into blocks. Returns
/// the block count — 0 when `rdels` is empty (no section is written).
///
/// The blocks are durable only after the caller's commit-point flush: the
/// data-section writer's [`TableWriter::finish`] flushes, and both flush
/// and compaction call it after their rdel section is written.
///
/// # Errors
///
/// [`Error::TableTooLarge`] when a single tombstone cannot fit in an empty
/// block, or [`Error::Device`] on I/O failure.
pub(crate) async fn write_rdel_blocks<D, const BLOCK: usize>(
    device: &mut D,
    base: u64,
    buf: &mut [u8; BLOCK],
    rdels: impl Iterator<Item = RdelEntry<'_>>,
) -> Result<u32, Error<D::Error>>
where
    D: BlockDevice,
{
    let mut w = RdelWriter::<BLOCK>::new(base, buf);
    for r in rdels {
        w.push(device, r).await?;
    }
    w.finish(device).await
}

/// Verifies an rdel block's CRC and returns its entry count.
pub(crate) fn rdel_block_count<E, const BLOCK: usize>(
    block: &[u8; BLOCK],
    id: u64,
) -> Result<usize, Error<E>> {
    check_block_crc::<E, BLOCK>(block, id)?;
    rdel_count_verified::<E, BLOCK>(block, id)
}

/// The entry count of an rdel block whose CRC the caller has already
/// verified (it came through [`read_block_verified`]).
fn rdel_count_verified<E, const BLOCK: usize>(
    block: &[u8; BLOCK],
    id: u64,
) -> Result<usize, Error<E>> {
    let raw = u16::from_le_bytes(
        block[BLOCK - CRC_LEN - 2..BLOCK - CRC_LEN]
            .try_into()
            .map_err(|_| Error::CorruptBlock { id })?,
    );
    // Rdel blocks are never compressed: bit 15 set is corruption, not the
    // data-block compression flag.
    if raw & 0x8000 != 0 {
        return Err(Error::CorruptBlock { id });
    }
    Ok(usize::from(raw))
}

/// Parses the rdel entry at `off`: `(entry, next_offset)`. Panic-free:
/// every offset is bounds- and overflow-checked. The caller maps `Err`
/// to [`Error::CorruptBlock`] with the block id it already knows.
pub(crate) fn rdel_parse_at(block: &[u8], off: usize) -> Result<(RdelEntry<'_>, usize), ()> {
    let sl = usize::from(u16::from_le_bytes(
        block
            .get(off..off + 2)
            .ok_or(())?
            .try_into()
            .map_err(|_| ())?,
    ));
    let el = usize::from(u16::from_le_bytes(
        block
            .get(off + 2..off + 4)
            .ok_or(())?
            .try_into()
            .map_err(|_| ())?,
    ));
    // The writer never emits empty bounds; in a CRC-verified block an
    // empty bound is corruption.
    if sl == 0 || el == 0 {
        return Err(());
    }
    let seq = u64::from_le_bytes(
        block
            .get(off + 4..off + 12)
            .ok_or(())?
            .try_into()
            .map_err(|_| ())?,
    );
    let start_at = off.checked_add(12).ok_or(())?;
    let start_end = start_at.checked_add(sl).ok_or(())?;
    let end_end = start_end.checked_add(el).ok_or(())?;
    let start = block.get(start_at..start_end).ok_or(())?;
    let end = block.get(start_end..end_end).ok_or(())?;
    Ok((RdelEntry { start, end, seq }, end_end))
}

/// Inflates a CRC-verified physical data block into its logical form.
///
/// Inspects the compression flag (bit 15 of the trailer count u16). When
/// clear, returns `Ok(false)` and the block is already logical — the
/// caller keeps using the physical buffer. When set, decompresses the
/// flagged payload into `decomp[..BLOCK-4]` and returns `Ok(true)`.
///
/// A set flag with an impossible length, or a decoder rejection, is
/// [`Error::CorruptBlock`]: the CRC already passed, so this is a format
/// violation rather than a torn write — but it is still an error, never
/// a panic. Used by every data-block read path that needs the whole
/// block (scans, compaction cursors, entry streams). A point lookup
/// inflates only as far as its answer instead (`data_lookup_inflating`,
/// ADR-0027), and uses this only to test it.
pub(crate) fn inflate_data_block<E, const BLOCK: usize>(
    raw: &[u8; BLOCK],
    decomp: &mut [u8; BLOCK],
    block_id: u64,
) -> Result<bool, Error<E>> {
    let corrupt = || Error::CorruptBlock { id: block_id };
    let body_end = BLOCK.checked_sub(CRC_LEN).ok_or_else(corrupt)?;
    let trailer = u16::from_le_bytes(
        raw[body_end - 2..body_end]
            .try_into()
            .map_err(|_| corrupt())?,
    );
    if trailer & 0x8000 == 0 {
        return Ok(false);
    }
    let clen = usize::from(trailer & 0x7FFF);
    if clen >= body_end {
        return Err(corrupt());
    }
    decompress(&raw[..clen], &mut decomp[..body_end]).map_err(|_| corrupt())?;
    Ok(true)
}

/// Byte offset where data entries end in a CRC-verified data block: the
/// start of the restart trailer (`[restarts][count u16][crc u32]`).
///
/// Used by the compaction cursor to walk a block's entries without a binary
/// search.
pub(crate) fn data_entries_end<E, const BLOCK: usize>(
    block: &[u8; BLOCK],
    block_id: u64,
) -> Result<usize, Error<E>> {
    let corrupt = || Error::CorruptBlock { id: block_id };
    let body_end = BLOCK.checked_sub(CRC_LEN).ok_or_else(corrupt)?;
    let rcount = usize::from(u16::from_le_bytes(
        block[body_end - 2..body_end]
            .try_into()
            .map_err(|_| corrupt())?,
    ));
    let tail = 2usize
        .checked_add(rcount.checked_mul(2).ok_or_else(corrupt)?)
        .ok_or_else(corrupt)?;
    body_end.checked_sub(tail).ok_or_else(corrupt)
}

/// One parsed data-block entry plus the offset of the next entry.
pub(crate) struct ParsedDataEntry<'a> {
    pub(crate) key: &'a [u8],
    pub(crate) val: &'a [u8],
    pub(crate) seq: u64,
    pub(crate) tombstone: bool,
    /// Absolute expiry tick; 0 = no expiry.
    pub(crate) expire_at: u64,
    pub(crate) next: usize,
}

/// Parses the data entry at `off` in a CRC-verified data block. `end` is
/// the entries end from [`data_entries_end`]; tombstones report an empty
/// value.
///
/// A parse failure is [`Error::CorruptBlock`]: unlike point lookups (which
/// may skip a torn block), compaction must never silently drop entries.
pub(crate) fn parse_data_entry<E, const BLOCK: usize>(
    block: &[u8; BLOCK],
    off: usize,
    end: usize,
    block_id: u64,
) -> Result<ParsedDataEntry<'_>, Error<E>> {
    let corrupt = || Error::CorruptBlock { id: block_id };
    if off >= end {
        return Err(corrupt());
    }
    let (entry, next) = data_entry_parse(&block[..end], off).map_err(|()| corrupt())?;
    let tombstone = entry.op == Op::Delete;
    let val = if tombstone { &[] } else { entry.val };
    Ok(ParsedDataEntry {
        key: entry.key,
        val,
        seq: entry.seq,
        tombstone,
        expire_at: entry.expire_at,
        next,
    })
}

/// Appends one index entry: `first_key_len u16 | first_key | block_id u64 |
/// max_seq u64`. Fails cleanly when the index would overflow one block.
fn append_index<E, const BLOCK: usize>(
    index: &mut [u8; BLOCK],
    index_len: &mut usize,
    first_key: Option<&[u8]>,
    block_id: u64,
    max_seq: u64,
) -> Result<(), Error<E>> {
    let fk = first_key.ok_or(Error::TableTooLarge)?;
    let kl = u16::try_from(fk.len()).map_err(|_| Error::TableTooLarge)?;
    let elen = 2 + fk.len() + 8 + 8;
    if *index_len + elen > BLOCK - CRC_LEN {
        return Err(Error::TableTooLarge);
    }
    let o = *index_len;
    index[o..o + 2].copy_from_slice(&kl.to_le_bytes());
    index[o + 2..o + 2 + fk.len()].copy_from_slice(fk);
    index[o + 2 + fk.len()..o + 10 + fk.len()].copy_from_slice(&block_id.to_le_bytes());
    index[o + 10 + fk.len()..o + 18 + fk.len()].copy_from_slice(&max_seq.to_le_bytes());
    *index_len += elen;
    Ok(())
}

/// Relocates a copied table to its destination block range.
///
/// The table's blocks must already sit at `[dst_base, dst_base +
/// block_count)` in writer order (data, rdel and bloom blocks, index,
/// footer).
/// Data and bloom block contents are position-independent, but index
/// entries carry absolute data-block ids and the footer carries the
/// absolute index/bloom ids of the table's *original* placement, so this
/// rewrites those pointers to the destination layout and re-seals the
/// touched blocks' CRCs. The original placement is derived from the
/// footer's own pointers (old bloom id minus the data-block count; the
/// bloom-block count is the gap between the footer's two pointers) and
/// cross-checked for structural consistency — index entries that do not
/// sit inside the derived original run are [`Error::CorruptBlock`], not
/// silently mis-relocated.
///
/// # Errors
///
/// [`Error::CorruptBlock`] when a block fails its CRC, the footer magic
/// is wrong, an index entry fails to parse, or a pointer does not match
/// the table's self-described original layout; or [`Error::Device`] on
/// I/O failure.
fn relocate_index_entries<E, const BLOCK: usize>(
    scratch: &mut [u8; BLOCK],
    index_id: u64,
    payload_len: usize,
    old_base: u64,
    data_blocks: u64,
    dst_base: u64,
) -> Result<(), Error<E>> {
    let mut off = 0usize;
    while off < payload_len {
        let (key_len, block_id, next) = match index_entry_parse(&scratch[..payload_len], off) {
            Ok((key, block_id, next)) => (key.len(), block_id, next),
            Err(()) if all_zero(&scratch[off..payload_len]) => break,
            Err(()) => return Err(Error::CorruptBlock { id: index_id }),
        };
        let tblock = block_id
            .checked_sub(old_base)
            .ok_or(Error::CorruptBlock { id: index_id })?;
        // Data blocks lead the table: `tblock` is a table-relative index
        // into `[0, data_blocks)`.
        if tblock >= data_blocks {
            return Err(Error::CorruptBlock { id: index_id });
        }
        let new_id = dst_base
            .checked_add(tblock)
            .ok_or(Error::CorruptBlock { id: index_id })?;
        let id_off = off + 2 + key_len;
        scratch[id_off..id_off + 8].copy_from_slice(&new_id.to_le_bytes());
        off = next;
    }
    Ok(())
}

pub(crate) async fn relocate_table<D: BlockDevice, const BLOCK: usize>(
    device: &mut D,
    dst_base: u64,
    block_count: u32,
    rdel_blocks: u32,
    scratch: &mut [u8; BLOCK],
) -> Result<(), Error<D::Error>> {
    let rdel = u64::from(rdel_blocks);
    // Layout `[data]* [rdel]* [bloom]+ [index] [footer]`: the table ends
    // with its index and footer.
    let footer_id = dst_base
        .checked_add(u64::from(block_count))
        .and_then(|end| end.checked_sub(1))
        .ok_or(Error::CorruptBlock { id: dst_base })?;
    let index_id = footer_id
        .checked_sub(1)
        .filter(|&i| i >= dst_base)
        .ok_or(Error::CorruptBlock { id: dst_base })?;
    let payload_len = BLOCK - CRC_LEN;

    // Footer first: it names the table's original bloom/index block ids,
    // from which the original table base is derived.
    poll_fn(|cx| device.poll_read_block(cx, footer_id, scratch))
        .await
        .map_err(Error::Device)?;
    check_block_crc::<D::Error, BLOCK>(scratch, footer_id)?;
    let magic = u64::from_le_bytes(
        scratch[0..8]
            .try_into()
            .map_err(|_| Error::CorruptBlock { id: footer_id })?,
    );
    if magic != SSTABLE_MAGIC {
        return Err(Error::CorruptBlock { id: footer_id });
    }
    let old_index = u64::from_le_bytes(
        scratch[8..16]
            .try_into()
            .map_err(|_| Error::CorruptBlock { id: footer_id })?,
    );
    let old_bloom = u64::from_le_bytes(
        scratch[16..24]
            .try_into()
            .map_err(|_| Error::CorruptBlock { id: footer_id })?,
    );
    // Structural check: the bloom blocks run up to the index, and the
    // footer's filter length accounts for every one of them. What is
    // left of the table before them is rdel blocks and data blocks, so
    // the original base is the bloom id minus those two counts.
    let bloom_len = u32::from_le_bytes(
        scratch[45..FOOTER_LEN]
            .try_into()
            .map_err(|_| Error::CorruptBlock { id: footer_id })?,
    );
    let bloom_blocks = old_index
        .checked_sub(old_bloom)
        .filter(|&n| {
            usize::try_from(bloom_len).is_ok_and(|len| n == bloom_section_blocks::<BLOCK>(len))
        })
        .ok_or(Error::CorruptBlock { id: footer_id })?;
    let data_blocks = u64::from(block_count)
        .checked_sub(rdel)
        .and_then(|n| n.checked_sub(bloom_blocks))
        .and_then(|n| n.checked_sub(2))
        .ok_or(Error::CorruptBlock { id: footer_id })?;
    let old_base = old_bloom
        .checked_sub(data_blocks)
        .and_then(|b| b.checked_sub(rdel))
        .ok_or(Error::CorruptBlock { id: footer_id })?;

    // Index block: rewrite each entry's absolute data-block id from the
    // original layout to the destination layout.
    poll_fn(|cx| device.poll_read_block(cx, index_id, scratch))
        .await
        .map_err(Error::Device)?;
    check_block_crc::<D::Error, BLOCK>(scratch, index_id)?;
    relocate_index_entries::<D::Error, BLOCK>(
        scratch,
        index_id,
        payload_len,
        old_base,
        data_blocks,
        dst_base,
    )?;
    let crc = crc32(&scratch[..payload_len]);
    scratch[payload_len..BLOCK].copy_from_slice(&crc.to_le_bytes());
    poll_fn(|cx| device.poll_write_block(cx, index_id, scratch))
        .await
        .map_err(Error::Device)?;

    // Footer: rewrite the absolute index/bloom block ids.
    poll_fn(|cx| device.poll_read_block(cx, footer_id, scratch))
        .await
        .map_err(Error::Device)?;
    check_block_crc::<D::Error, BLOCK>(scratch, footer_id)?;
    let dst_bloom = dst_base
        .checked_add(rdel)
        .and_then(|b| b.checked_add(data_blocks))
        .ok_or(Error::CorruptBlock { id: footer_id })?;
    let dst_index = dst_bloom
        .checked_add(bloom_blocks)
        .filter(|&i| i == index_id)
        .ok_or(Error::CorruptBlock { id: footer_id })?;
    scratch[8..16].copy_from_slice(&dst_index.to_le_bytes());
    scratch[16..24].copy_from_slice(&dst_bloom.to_le_bytes());
    let crc = crc32(&scratch[..payload_len]);
    scratch[payload_len..BLOCK].copy_from_slice(&crc.to_le_bytes());
    poll_fn(|cx| device.poll_write_block(cx, footer_id, scratch))
        .await
        .map_err(Error::Device)?;
    Ok(())
}

/// Checks a block's trailing CRC32.
pub(crate) fn check_block_crc<E, const BLOCK: usize>(
    block: &[u8; BLOCK],
    id: u64,
) -> Result<(), Error<E>> {
    let stored = u32::from_le_bytes(
        block[BLOCK - CRC_LEN..BLOCK]
            .try_into()
            .map_err(|_| Error::CorruptBlock { id })?,
    );
    if crc32(&block[..BLOCK - CRC_LEN]) != stored {
        return Err(Error::CorruptBlock { id });
    }
    Ok(())
}

/// Checks whether every byte in `bytes` is zero, 8 at a time.
///
/// Equivalent to `bytes.iter().all(|&b| b == 0)`, which LLVM compiles to a
/// scalar byte-at-a-time loop here (confirmed via `objdump`) rather than a
/// vectorized comparison. Chunking into `u64` words cuts the loop trip count
/// by 8x for the same result.
pub(crate) fn all_zero(bytes: &[u8]) -> bool {
    let (chunks, remainder) = bytes.as_chunks::<8>();
    chunks.iter().all(|c| u64::from_ne_bytes(*c) == 0) && remainder.iter().all(|&b| b == 0)
}

/// Bounds-checked slice read.
fn slice_at(payload: &[u8], off: usize, len: usize) -> Result<&[u8], ()> {
    let end = off.checked_add(len).ok_or(())?;
    payload.get(off..end).ok_or(())
}

/// Parses one index entry: `(first_key, block_id, next_offset)`.
///
/// A zero `key_len` is rejected: block first-keys are never empty, so in a
/// CRC-verified block it marks zero padding (end of the index).
fn index_entry_parse(payload: &[u8], off: usize) -> Result<(&[u8], u64, usize), ()> {
    let kl = usize::from(u16::from_le_bytes(
        slice_at(payload, off, 2)?.try_into().map_err(|_| ())?,
    ));
    if kl == 0 {
        return Err(());
    }
    let key = slice_at(payload, off + 2, kl)?;
    let id_off = off.checked_add(2).ok_or(())?.checked_add(kl).ok_or(())?;
    let block_id = u64::from_le_bytes(slice_at(payload, id_off, 8)?.try_into().map_err(|_| ())?);
    let next = id_off.checked_add(16).ok_or(())?;
    if next > payload.len() {
        return Err(());
    }
    Ok((key, block_id, next))
}

/// Index entries between two key comparisons of [`index_partition`]'s
/// forward pass.
const INDEX_STRIDE: usize = 16;

/// What one pass over an index block found, for a predicate that holds
/// for a (possibly empty) prefix of the entries and for none after it.
struct IndexSplit {
    /// Entries the predicate holds for.
    matching: usize,
    /// Data block of the last of them.
    last_matching: Option<u64>,
    /// Data block of the first entry.
    first: Option<u64>,
    /// Entries in the index.
    count: usize,
}

/// Walks the index once and splits it at the first entry whose
/// `first_key` fails `holds`. Entries are sorted by `first_key`, and
/// `holds` must be monotone over them (true, then false).
///
/// Entries are variable-length with no offset table, so position `i` is
/// only reachable by parsing the `i` entries before it. Rather than
/// binary-search by re-parsing from the start for every probe, the pass
/// parses each entry once (it must anyway, to count and validate them),
/// compares only every [`INDEX_STRIDE`]th key, and finishes with a
/// linear scan of the one stride the boundary falls in.
///
/// The block's CRC already passed, so a parse failure is either genuine
/// zero padding (all zeros to the end of the block body: the end of the
/// index) or structural corruption; the two are told apart explicitly.
fn index_partition<E>(
    payload: &[u8],
    index_id: u64,
    holds: impl Fn(&[u8]) -> bool,
) -> Result<IndexSplit, Error<E>> {
    let corrupt = || Error::CorruptBlock { id: index_id };
    let mut split = IndexSplit {
        matching: 0,
        last_matching: None,
        first: None,
        count: 0,
    };
    // The last sampled entry that matched: `(position, offset)`. The
    // boundary is searched for from here once a later sample fails.
    let mut anchor: Option<(usize, usize)> = None;
    // Where the boundary's stride ends, once a sample has failed.
    let mut stride_end: Option<usize> = None;
    let mut off = 0usize;
    while off < payload.len() {
        let Ok((fkey, block_id, next)) = index_entry_parse(payload, off) else {
            if all_zero(&payload[off..]) {
                break;
            }
            return Err(corrupt());
        };
        if split.count == 0 {
            split.first = Some(block_id);
        }
        if stride_end.is_none() && split.count.is_multiple_of(INDEX_STRIDE) {
            if holds(fkey) {
                anchor = Some((split.count, off));
                split.matching = split.count + 1;
                split.last_matching = Some(block_id);
            } else {
                stride_end = Some(split.count);
            }
        }
        off = next;
        split.count += 1;
    }
    // The entries after the anchor, up to the failed sample (or the end),
    // were not compared: the boundary is among them.
    if let Some((at, anchor_off)) = anchor {
        let end = stride_end.unwrap_or(split.count);
        let (_, _, mut off) = index_entry_parse(payload, anchor_off).map_err(|()| corrupt())?;
        for pos in at + 1..end {
            let (fkey, block_id, next) = index_entry_parse(payload, off).map_err(|()| corrupt())?;
            if !holds(fkey) {
                break;
            }
            split.matching = pos + 1;
            split.last_matching = Some(block_id);
            off = next;
        }
    }
    Ok(split)
}

/// Finds the first data block that may hold `key`.
/// Returns the block id and the maximum number of data blocks a point
/// lookup may walk forward from it (the index entry count minus the
/// resolved position: the walk stays inside the table), or `None` when
/// `key` sorts before every block.
/// `pub(crate)` for the scan iterator's seek positioning and the point
/// lookup's cross-block run walk.
///
/// A key's version run can straddle data blocks: every block the run
/// touches carries the key as its restart (first) key, and worse, a block
/// can seal *inside* the run — so the block before the first duplicate
/// restart can hold the run's newest versions. The search therefore lands
/// on the last restart strictly below `key` (entry 0 when there is
/// none): the first block whose key range can contain `key`. Starting
/// any later would miss the run's newest versions.
pub(crate) fn index_lookup<E>(
    payload: &[u8],
    key: &[u8],
    index_id: u64,
) -> Result<Option<(u64, usize)>, Error<E>> {
    let corrupt = || Error::CorruptBlock { id: index_id };
    // Entries strictly below `key`: the last of them is the block to
    // start in, whether or not restarts equal to `key` follow it.
    let below = index_partition(payload, index_id, |fkey| fkey < key)?;
    if below.matching == 0 {
        // No restart below `key`. The table can hold it only if its
        // first restart is `key` itself; the search starts there.
        let first_is_key = index_entry_parse(payload, 0).is_ok_and(|(fkey, _, _)| fkey == key);
        if !first_is_key {
            return Ok(None);
        }
        let block_id = below.first.ok_or_else(corrupt)?;
        return Ok(Some((block_id, below.count)));
    }
    let block_id = below.last_matching.ok_or_else(corrupt)?;
    // The walk may visit every data block from there onward — never past
    // the table's last data block (whose readers must not stray into the
    // bloom/index/footer blocks).
    Ok(Some((block_id, below.count - (below.matching - 1))))
}

/// Returns the last data block whose `first_key` sorts at or before `key`,
/// or `None` when `key` sorts before every block. `pub(crate)` for the
/// reverse scan's seek positioning: the greatest entry `<= key` lives in
/// this block or an earlier one, so the reverse cursor walks backward from
/// here. Unlike [`index_lookup`] there is no walk-back — duplicates of
/// `key` in earlier blocks hold *older* versions, and the backward walk
/// reaches them naturally.
pub(crate) fn index_last_le_block<E>(
    payload: &[u8],
    key: &[u8],
    index_id: u64,
) -> Result<Option<u64>, Error<E>> {
    Ok(index_partition(payload, index_id, |fkey| fkey <= key)?.last_matching)
}

/// One parsed data-block entry, borrowing the block.
struct ParsedEntry<'a> {
    key: &'a [u8],
    val: &'a [u8],
    seq: u64,
    op: Op,
    /// For [`Op::PutTtl`]: the absolute expiry tick; 0 otherwise.
    expire_at: u64,
}

/// Parses the entry at `off`: `(entry, next_offset)`.
///
/// A zero `key_len` is rejected: the writer never emits empty keys, so in a
/// CRC-verified block it marks zero padding, which the lookup scans treat
/// as end-of-entries rather than corruption.
fn data_entry_parse(payload: &[u8], off: usize) -> Result<(ParsedEntry<'_>, usize), ()> {
    let kl = usize::from(u16::from_le_bytes(
        slice_at(payload, off, 2)?.try_into().map_err(|_| ())?,
    ));
    if kl == 0 {
        return Err(());
    }
    let vl = usize::from(u16::from_le_bytes(
        slice_at(payload, off + 2, 2)?.try_into().map_err(|_| ())?,
    ));
    let seq_bytes = slice_at(payload, off + 4, 8)?;
    let seq = u64::from_le_bytes(seq_bytes.try_into().map_err(|_| ())?);
    let op = Op::from_u8(slice_at(payload, off + 12, 1)?[0]).ok_or(())?;
    let key = slice_at(payload, off + ENTRY_HEADER, kl)?;
    let val = slice_at(payload, off + ENTRY_HEADER + kl, vl)?;
    let mut next = off
        .checked_add(ENTRY_HEADER)
        .ok_or(())?
        .checked_add(kl)
        .ok_or(())?
        .checked_add(vl)
        .ok_or(())?;
    // TTL entries carry an 8-byte expiry after the value.
    let expire_at = if op == Op::PutTtl {
        let bytes = slice_at(payload, next, 8)?;
        next = next.checked_add(8).ok_or(())?;
        u64::from_le_bytes(bytes.try_into().map_err(|_| ())?)
    } else {
        0
    };
    if next > payload.len() {
        return Err(());
    }
    Ok((
        ParsedEntry {
            key,
            val,
            seq,
            op,
            expire_at,
        },
        next,
    ))
}

/// What a data-block search found, with the entry's sequence number.
enum DataHit {
    /// Live value: byte length, sequence number, expiry tick (0 = none).
    Value {
        len: usize,
        seq: u64,
        expire_at: u64,
    },
    /// Deletion marker: sequence number.
    Tombstone(u64),
}

/// What one data block contributed to a point lookup. A key's version run
/// can straddle data blocks, so "not in this block" is not "not in the
/// table": the caller walks forward while a block ends inside the run.
enum BlockOutcome {
    /// The newest version with `seq <= max_seq` was found in this block.
    Hit(DataHit),
    /// An entry larger than the key was seen: the key is absent from the
    /// table — later blocks only hold larger keys.
    Absent,
    /// The block ended (or zero padding began) with no larger key seen:
    /// the key was not found in this block, but its version run may
    /// continue in the next data block.
    Truncated,
}

/// Searches one data block: restart-point binary search, then a linear scan.
/// Selects the newest version with `seq <= max_seq`, reporting [`BlockOutcome`]
/// so the caller can continue a version run into the next data block.
///
/// `check(from, to)` is called on every byte range of `payload` before
/// the search acts on it (the restart trailer, each entry it parses, a
/// run of padding); `false` ends the search with `CorruptBlock`. A cache
/// that lends an image unchecked (`CachePort::with_checked`) verifies
/// only those ranges against its RAM checksums: a lookup reads a few
/// entries of a 16 KiB block. Over bytes already verified, `check`
/// always holds.
fn data_lookup<E>(
    payload: &[u8],
    key: &[u8],
    val_buf: &mut [u8],
    block_id: u64,
    max_seq: u64,
    check: &mut impl FnMut(usize, usize) -> bool,
) -> Result<BlockOutcome, Error<E>> {
    let corrupt = || Error::CorruptBlock { id: block_id };
    if payload.len() < 6 || !check(payload.len() - 2, payload.len()) {
        return Err(corrupt());
    }
    let rcount = usize::from(u16::from_le_bytes(
        payload[payload.len() - 2..]
            .try_into()
            .map_err(|_| corrupt())?,
    ));
    let rstart = payload
        .len()
        .checked_sub(2 + rcount.checked_mul(2).ok_or_else(corrupt)?)
        .ok_or_else(corrupt)?;
    if !check(rstart, payload.len()) {
        return Err(corrupt());
    }
    let restart_at = |i: usize| {
        let off = rstart.checked_add(i.checked_mul(2)?)?;
        let bytes: [u8; 2] = payload.get(off..off + 2)?.try_into().ok()?;
        let roff = usize::from(u16::from_le_bytes(bytes));
        if roff >= rstart { None } else { Some(roff) }
    };
    // Binary search over restart points: the first restart whose entry key
    // sorts at or after `key`. A key's version run is contiguous and newest
    // first, so the run starts at or after the previous restart — stepping
    // back one (saturating) can never skip the run's first entry.
    let mut lo = 0usize;
    let mut hi = rcount;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let roff = restart_at(mid).ok_or_else(corrupt)?;
        let (entry, next) = data_entry_parse(payload, roff).map_err(|()| corrupt())?;
        if !check(roff, next) {
            return Err(corrupt());
        }
        if entry.key < key {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    let mut off = if lo == 0 {
        0
    } else {
        restart_at(lo - 1).ok_or_else(corrupt)?
    };
    while off < rstart {
        // The block's CRC already passed. Genuine zero padding (all zeros
        // up to the restart tail) means the block's entries end here — the
        // key may still continue in the next data block, so this is
        // `Truncated`, not `Absent`. Any other unparseable structure is
        // corruption, not padding.
        let Ok((entry, next)) = data_entry_parse(payload, off) else {
            return if check(off, rstart) && all_zero(&payload[off..rstart]) {
                Ok(BlockOutcome::Truncated)
            } else {
                Err(corrupt())
            };
        };
        if !check(off, next) {
            return Err(corrupt());
        }
        match entry.key.cmp(key) {
            core::cmp::Ordering::Less => off = next,
            core::cmp::Ordering::Equal => {
                // Version run, newest first: this read observes the first
                // entry with `seq <= max_seq`. Entries are contiguous, so a
                // parse failure inside the run is corruption — except for
                // genuine zero padding at the entries end, which means the
                // run may continue in the next data block (`Truncated`).
                // Reaching a larger key ends the run: `Absent`.
                let (mut entry, mut next) = (entry, next);
                loop {
                    if entry.seq <= max_seq {
                        return data_hit(&entry, val_buf);
                    }
                    off = next;
                    if off >= rstart {
                        return Ok(BlockOutcome::Truncated);
                    }
                    let Ok((e, n)) = data_entry_parse(payload, off) else {
                        return if check(off, rstart) && all_zero(&payload[off..rstart]) {
                            Ok(BlockOutcome::Truncated)
                        } else {
                            Err(corrupt())
                        };
                    };
                    if !check(off, n) {
                        return Err(corrupt());
                    }
                    if e.key != key {
                        return Ok(BlockOutcome::Absent);
                    }
                    (entry, next) = (e, n);
                }
            }
            core::cmp::Ordering::Greater => return Ok(BlockOutcome::Absent),
        }
    }
    Ok(BlockOutcome::Truncated)
}

/// The outcome [`data_lookup`] gives once it has found the newest version
/// of `key` visible at `max_seq`: its value copied out, or a tombstone.
fn data_hit<E>(entry: &ParsedEntry<'_>, val_buf: &mut [u8]) -> Result<BlockOutcome, Error<E>> {
    if entry.op == Op::Delete {
        return Ok(BlockOutcome::Hit(DataHit::Tombstone(entry.seq)));
    }
    let len = entry.val.len();
    let dst = val_buf
        .get_mut(..len)
        .ok_or(Error::BufferTooSmall { need: len })?;
    dst.copy_from_slice(entry.val);
    Ok(BlockOutcome::Hit(DataHit::Value {
        len,
        seq: entry.seq,
        expire_at: entry.expire_at,
    }))
}

/// Output a partial inflate decodes past what the scan needs, so that the
/// decoder is called once per few entries rather than once per entry.
const INFLATE_STEP: usize = 1024;

/// [`data_lookup`] over a compressed data block, inflating `stream` into
/// `logical` only as far as the answer needs (ADR-0027).
///
/// The restart offsets that let [`data_lookup`] binary-search a block sit
/// at its end, so it needs the whole block inflated. Here the entries are
/// scanned from the first, with the decode kept just ahead of the scan,
/// and the scan stops where the answer is decided: at the newest visible
/// version of `key` (a value or a tombstone), or at a larger key. A point
/// read whose key lies halfway through the block inflates half of it.
///
/// What the scan does not decide is left to [`data_lookup`] itself, after
/// inflating the rest: the end of the entries (zero padding, or the
/// restart offsets, whose first is always 0 and so reads as an empty key:
/// the run may go on in the next block), and anything that does not
/// parse. Those answers, and the corruption they report, are exactly the
/// whole-block search's. The answers the scan gives itself are the ones
/// the whole-block search gives for every block the writer produces: its
/// binary search only chooses where to start the same linear scan.
fn data_lookup_inflating<E, const BLOCK: usize>(
    stream: &[u8],
    logical: &mut [u8; BLOCK],
    key: &[u8],
    val_buf: &mut [u8],
    block_id: u64,
    max_seq: u64,
) -> Result<BlockOutcome, Error<E>> {
    let corrupt = || Error::CorruptBlock { id: block_id };
    let body_end = BLOCK.checked_sub(CRC_LEN).ok_or_else(corrupt)?;
    let out = &mut logical[..body_end];
    let mut inflate = Inflate::new();
    // Decodes until `need` bytes are final; `false` when the block holds
    // fewer.
    let ready = |inflate: &mut Inflate, out: &mut [u8], need: usize| {
        if need > out.len() {
            return Ok(false);
        }
        if inflate.done() < need {
            let until = need.max(inflate.done() + INFLATE_STEP);
            inflate.run(stream, out, until).map_err(|_| corrupt())?;
        }
        Ok::<bool, Error<E>>(inflate.done() >= need)
    };
    let mut off = 0usize;
    loop {
        if !ready(&mut inflate, out, off + ENTRY_HEADER)? {
            break;
        }
        let head = &out[off..off + ENTRY_HEADER];
        let kl = usize::from(u16::from_le_bytes([head[0], head[1]]));
        let vl = usize::from(u16::from_le_bytes([head[2], head[3]]));
        if kl == 0 {
            // The end of the entries: padding or the restart offsets.
            break;
        }
        let ttl = if head[12] == Op::PutTtl.to_u8() { 8 } else { 0 };
        if !ready(&mut inflate, out, off + ENTRY_HEADER + kl + vl + ttl)? {
            break;
        }
        let Ok((entry, next)) = data_entry_parse(&out[..inflate.done()], off) else {
            break;
        };
        match entry.key.cmp(key) {
            core::cmp::Ordering::Greater => return Ok(BlockOutcome::Absent),
            core::cmp::Ordering::Equal if entry.seq <= max_seq => {
                return data_hit(&entry, val_buf);
            }
            // A smaller key, or a version newer than the snapshot (the
            // run goes on): the next entry.
            core::cmp::Ordering::Less | core::cmp::Ordering::Equal => off = next,
        }
    }
    // Not decided by the scan: the whole block, and the whole-block rules.
    if !ready(&mut inflate, out, body_end)? {
        return Err(corrupt());
    }
    data_lookup(out, key, val_buf, block_id, max_seq, &mut |_, _| true)
}

/// [`data_lookup`] over a verified block image as stored: in place when
/// it is raw, inflating only as far as needed when it is compressed.
fn lookup_image<E, const BLOCK: usize>(
    image: &[u8],
    logical: &mut [u8; BLOCK],
    key: &[u8],
    val_buf: &mut [u8],
    block_id: u64,
    max_seq: u64,
) -> Result<BlockOutcome, Error<E>> {
    lookup_image_checked(
        image,
        logical,
        key,
        val_buf,
        block_id,
        max_seq,
        &mut |_, _| true,
    )
}

/// [`lookup_image`] over an image whose bytes `check` verifies a range
/// at a time (see [`data_lookup`]): the flag, then for a raw block the
/// ranges the search reads, for a compressed one its whole stream.
fn lookup_image_checked<E, const BLOCK: usize>(
    image: &[u8],
    logical: &mut [u8; BLOCK],
    key: &[u8],
    val_buf: &mut [u8],
    block_id: u64,
    max_seq: u64,
    check: &mut impl FnMut(usize, usize) -> bool,
) -> Result<BlockOutcome, Error<E>> {
    let corrupt = || Error::CorruptBlock { id: block_id };
    let body_end = BLOCK.checked_sub(CRC_LEN).ok_or_else(corrupt)?;
    let body = image
        .get(..body_end)
        .filter(|_| image.len() == BLOCK && body_end >= 2 && check(body_end - 2, body_end))
        .ok_or_else(corrupt)?;
    let flag = u16::from_le_bytes([body[body_end - 2], body[body_end - 1]]);
    if flag & 0x8000 == 0 {
        return data_lookup(body, key, val_buf, block_id, max_seq, check);
    }
    let clen = usize::from(flag & 0x7FFF);
    let stream = body
        .get(..clen)
        .filter(|_| clen < body_end && check(0, clen))
        .ok_or_else(corrupt)?;
    data_lookup_inflating(stream, logical, key, val_buf, block_id, max_seq)
}

/// One data block of a point lookup: [`data_lookup`]'s answer for block
/// `block_id`. A cache that lends its images has the block searched
/// where it lies (one lookup, counted once); otherwise the block is read
/// into `raw` as [`read_block_verified`] reads it. Either way a
/// compressed block is inflated into `logical` only as far as the answer
/// needs.
#[allow(clippy::too_many_arguments)] // one call site; a struct would only rename them
async fn lookup_data_block<D: BlockDevice, const BLOCK: usize>(
    device: &D,
    cache: Option<&dyn CachePort<BLOCK>>,
    table_id: u32,
    block_id: u64,
    raw: &mut [u8; BLOCK],
    logical: &mut [u8; BLOCK],
    key: &[u8],
    val_buf: &mut [u8],
    max_seq: u64,
) -> Result<BlockOutcome, Error<D::Error>> {
    if let Some(c) = cache {
        let mut found = None;
        // Only the bytes the search reads are checked against the
        // cache's RAM checksums: a raw block's trailer and the few
        // entries the search parses, not all 16 KiB. A check that fails
        // makes the lookup a miss, and `found` is not used.
        // An error may come from bytes the search read before it could
        // check them (an entry that does not parse): the whole image is
        // checked then, so that a change in RAM is a miss and only
        // damage on the device is `CorruptBlock`.
        let lent = c.with_checked(table_id, block_id, &mut |image, check| {
            let outcome = lookup_image_checked::<D::Error, BLOCK>(
                image,
                logical,
                key,
                val_buf,
                block_id,
                max_seq,
                &mut |from, to| check(from, to),
            );
            if outcome.is_err() {
                check(0, BLOCK);
            }
            found = Some(outcome);
        });
        match (lent, found) {
            (Some(true), Some(outcome)) => return outcome,
            (Some(_), _) => {
                // A miss: to the device, without asking the cache again.
                fetch_block(device, cache, table_id, block_id, raw, true).await?;
                return lookup_image(raw, logical, key, val_buf, block_id, max_seq);
            }
            (None, _) => {}
        }
    }
    read_block_verified(device, cache, table_id, block_id, raw, true).await?;
    lookup_image(raw, logical, key, val_buf, block_id, max_seq)
}

/// Result of a point lookup in one table, carrying the entry's sequence
/// number.
///
/// [`TableReader::lookup`] reports all three; [`TableReader::get`] folds
/// tombstones and misses into `None`. `seq` is 0 for [`Lookup::Missing`].
///
/// The sequence number is what lets [`crate::Db`] implement "highest seq
/// wins" across levels: the entry's own seq, not just its table's max.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lookup {
    /// Live value.
    Value {
        /// Byte length written to the caller's buffer.
        len: usize,
        /// The entry's sequence number.
        seq: u64,
        /// Absolute expiry tick; 0 = no expiry. The caller suppresses the
        /// value when `expire_at <= now`.
        expire_at: u64,
    },
    /// Deletion marker: shadows the same key in older tables.
    Tombstone {
        /// The tombstone's sequence number.
        seq: u64,
    },
    /// Key not present in this table.
    Missing,
}

/// Highest sequence number at or below `max_seq` of a range tombstone
/// covering `key` in the rdel section starting at `rdel_first`
/// (`rdel_blocks` blocks), or `None` when no live tombstone covers it.
/// The reader-free form, for callers (like the scan merge) that walk
/// tables without opening a [`TableReader`] per key.
///
/// # Errors
///
/// [`Error::CorruptBlock`] when an rdel block fails verification, or
/// [`Error::Device`] on I/O failure.
#[allow(clippy::too_many_arguments)] // all eight are load-bearing; bundling just moves the arity.
pub(crate) async fn covering_rdel_seq_in<D: BlockDevice, const BLOCK: usize>(
    device: &D,
    cache: Option<&dyn CachePort<BLOCK>>,
    table_id: u32,
    scratch: &mut [u8; BLOCK],
    rdel_first: u64,
    rdel_blocks: u32,
    key: &[u8],
    max_seq: u64,
) -> Result<Option<u64>, Error<D::Error>> {
    let mut best: Option<u64> = None;
    for b in 0..u64::from(rdel_blocks) {
        let id = rdel_first
            .checked_add(b)
            .ok_or(Error::CorruptBlock { id: rdel_first })?;
        // Hot: covering probes run per key per table, so these blocks
        // are re-read constantly — the cache's biggest win.
        read_block_verified(device, cache, table_id, id, scratch, true).await?;
        let count = rdel_count_verified::<D::Error, BLOCK>(scratch, id)?;
        let mut off = 0usize;
        for _ in 0..count {
            let (e, next) =
                rdel_parse_at(&scratch[..], off).map_err(|()| Error::CorruptBlock { id })?;
            off = next;
            if e.seq <= max_seq && e.start <= key && key < e.end && best.is_none_or(|s| e.seq > s) {
                best = Some(e.seq);
            }
        }
    }
    Ok(best)
}

/// Reads one `SSTable` block and verifies it: the funnel for every block
/// a reader parses whole. On `Ok`, `scratch` holds an image whose trailing
/// CRC-32 matched.
///
/// With a cache: a hit copies the cached image into `scratch` and does no
/// device I/O and no CRC. The cache holds only images this module has
/// CRC-checked, and re-checks each hit against a RAM checksum of those
/// bytes (see [`crate::cache`], ADR-0016), so a hit is exactly the
/// verified image or it is a miss. On a miss the block is read from the
/// device, its CRC is checked, and only then is it inserted (`hot` sets
/// the CLOCK reference bit). A block that fails its CRC is never cached:
/// each read of it reaches the device and reports the corruption again.
/// `table_id` is the owning table's manifest id: ids are monotone and
/// never reused, so the key `(table_id, block_id)` names the image
/// immutably.
///
/// # Errors
///
/// [`Error::CorruptBlock`] when the CRC does not match, or
/// [`Error::Device`] on I/O failure.
pub(crate) async fn read_block_verified<D: BlockDevice, const BLOCK: usize>(
    device: &D,
    cache: Option<&dyn CachePort<BLOCK>>,
    table_id: u32,
    block_id: u64,
    scratch: &mut [u8; BLOCK],
    hot: bool,
) -> Result<(), Error<D::Error>> {
    if let Some(c) = cache
        && c.get_into(table_id, block_id, scratch)
    {
        return Ok(());
    }
    fetch_block(device, cache, table_id, block_id, scratch, hot).await
}

/// Like [`read_block_verified`], for a reader that needs only `range` of
/// the block (a footer's fields, one bloom bucket). On `Ok`,
/// `scratch[range]` holds verified bytes of the block; the rest of
/// `scratch` is unspecified and must not be read.
///
/// A cache hit copies and checks just the range (see
/// [`BlockCache::get_range_into`](crate::cache::BlockCache::get_range_into)),
/// which is what makes probing a table that does not hold the key cheap.
/// A miss reads and CRC-checks the whole block, as it must, and caches it.
///
/// # Errors
///
/// [`Error::CorruptBlock`] when the CRC does not match or `range` is not
/// inside a block, or [`Error::Device`] on I/O failure.
pub(crate) async fn read_range_verified<D: BlockDevice, const BLOCK: usize>(
    device: &D,
    cache: Option<&dyn CachePort<BLOCK>>,
    table_id: u32,
    block_id: u64,
    scratch: &mut [u8; BLOCK],
    range: core::ops::Range<usize>,
    hot: bool,
) -> Result<(), Error<D::Error>> {
    let start = range.start;
    let out = scratch
        .get_mut(range)
        .ok_or(Error::CorruptBlock { id: block_id })?;
    if let Some(c) = cache
        && c.get_range_into(table_id, block_id, start, out)
    {
        return Ok(());
    }
    fetch_block(device, cache, table_id, block_id, scratch, hot).await
}

/// The miss path of the two funnels above: device read, CRC check, and
/// only then the cache insert.
async fn fetch_block<D: BlockDevice, const BLOCK: usize>(
    device: &D,
    cache: Option<&dyn CachePort<BLOCK>>,
    table_id: u32,
    block_id: u64,
    scratch: &mut [u8; BLOCK],
    hot: bool,
) -> Result<(), Error<D::Error>> {
    poll_fn(|cx| device.poll_read_block(cx, block_id, scratch))
        .await
        .map_err(Error::Device)?;
    check_block_crc::<D::Error, BLOCK>(scratch, block_id)?;
    if let Some(c) = cache {
        c.put(table_id, block_id, scratch, hot);
    }
    Ok(())
}

/// The funnel for whole data-block reads, shared by both scan directions
/// and compaction (a point lookup has its own, `lookup_data_block`, which
/// inflates only as far as its answer: ADR-0027): reads block `block_id`
/// into `raw`
/// (through the cache when given; `hot` inserts it as a point read
/// would), verifies its CRC, and inflates it into `logical` when its
/// compression flag is set. Returns `true` when `logical` holds the
/// inflated block, `false` when `raw` holds the logical block as stored
/// (callers that need it in `logical` copy it).
///
/// A committed table's blocks were durable before the manifest made them
/// visible, so a bad CRC is media corruption, never a torn write: it is
/// [`Error::CorruptBlock`] everywhere — reporting "absent" instead would
/// let an older version in a deeper table win (a silent stale read). A
/// flag or decoding failure is `CorruptBlock` too.
///
/// A cache that lends its images ([`CachePort::with_range`]) is asked
/// once: the block is checked whole against its RAM checksums and, if
/// compressed, inflated where it lies, with no copy; a block stored raw
/// is copied into `raw`. A miss goes straight to the device. Either way
/// one block read is one lookup, so [`CacheStats`](crate::CacheStats)
/// counts it once. When the inflate succeeds, `raw` holds nothing of the
/// block; no caller reads `raw` after a `true` return.
///
/// # Errors
///
/// [`Error::CorruptBlock`] as above, or [`Error::Device`] on I/O failure.
pub(crate) async fn read_data_block<D: BlockDevice, const BLOCK: usize>(
    device: &D,
    cache: Option<&dyn CachePort<BLOCK>>,
    table_id: u32,
    block_id: u64,
    raw: &mut [u8; BLOCK],
    logical: &mut [u8; BLOCK],
    hot: bool,
) -> Result<bool, Error<D::Error>> {
    if let Some(c) = cache {
        match inflate_lent::<D::Error, BLOCK>(c, table_id, block_id, raw, logical) {
            Lent::Done(inflated) => return inflated,
            Lent::Miss => {
                // Not cached: to the device, without asking the cache a
                // second time.
                fetch_block(device, cache, table_id, block_id, raw, hot).await?;
                return inflate_data_block::<D::Error, BLOCK>(raw, logical, block_id);
            }
            Lent::Copy => {}
        }
    }
    read_block_verified(device, cache, table_id, block_id, raw, hot).await?;
    inflate_data_block::<D::Error, BLOCK>(raw, logical, block_id)
}

/// What [`inflate_lent`] made of one lookup.
enum Lent<E> {
    /// A hit: the block was inflated into `logical` (`Ok(true)`), copied
    /// into `raw` as stored (`Ok(false)`), or failed to decode.
    Done(Result<bool, Error<E>>),
    /// Not cached, or rejected by its checksums: read the device.
    Miss,
    /// The cache does not lend its images: copy one out instead.
    Copy,
}

/// The hit path of [`read_data_block`] for a cache that lends: one
/// lookup of the whole image, which is inflated where it lies (or, when
/// stored raw, copied into `raw`).
fn inflate_lent<E, const BLOCK: usize>(
    cache: &dyn CachePort<BLOCK>,
    table_id: u32,
    block_id: u64,
    raw: &mut [u8; BLOCK],
    logical: &mut [u8; BLOCK],
) -> Lent<E> {
    let mut done = None;
    let lent = cache.with_range(table_id, block_id, 0, BLOCK, &mut |image| {
        done = Some(inflate_image::<E, BLOCK>(image, raw, logical, block_id));
    });
    match (lent, done) {
        (Some(true), Some(result)) => Lent::Done(result),
        (Some(_), _) => Lent::Miss,
        (None, _) => Lent::Copy,
    }
}

/// [`inflate_data_block`] over a lent image: a compressed block inflates
/// from `image` into `logical` (`true`); a raw one is copied into `raw`
/// (`false`).
fn inflate_image<E, const BLOCK: usize>(
    image: &[u8],
    raw: &mut [u8; BLOCK],
    logical: &mut [u8; BLOCK],
    block_id: u64,
) -> Result<bool, Error<E>> {
    let corrupt = || Error::CorruptBlock { id: block_id };
    let body_end = BLOCK.checked_sub(CRC_LEN).ok_or_else(corrupt)?;
    let flag = image
        .get(body_end.wrapping_sub(2)..body_end)
        .and_then(|b| b.try_into().ok())
        .map(u16::from_le_bytes)
        .ok_or_else(corrupt)?;
    if flag & 0x8000 == 0 {
        if image.len() != BLOCK {
            return Err(corrupt());
        }
        raw.copy_from_slice(image);
        return Ok(false);
    }
    let clen = usize::from(flag & 0x7FFF);
    let stream = image
        .get(..clen)
        .filter(|_| clen < body_end)
        .ok_or_else(corrupt)?;
    decompress(stream, &mut logical[..body_end]).map_err(|_| corrupt())?;
    Ok(true)
}

/// Runs `parse` over bytes `..len` of block `block_id`'s verified image,
/// for a reader that parses a block once and keeps nothing of it (the
/// index search). A cache that lends its images (see
/// [`CachePort::with_range`]) has it parsed in place, with no copy;
/// otherwise the block is read into `scratch` as
/// [`read_block_verified`] reads it, and parsed there.
///
/// # Errors
///
/// As [`read_block_verified`].
async fn parse_verified<D: BlockDevice, R, const BLOCK: usize>(
    device: &D,
    cache: Option<&dyn CachePort<BLOCK>>,
    table_id: u32,
    block_id: u64,
    scratch: &mut [u8; BLOCK],
    len: usize,
    mut parse: impl FnMut(&[u8]) -> R,
) -> Result<R, Error<D::Error>> {
    let len = len.min(BLOCK);
    if let Some(c) = cache {
        let mut out = None;
        let lent = c.with_range(table_id, block_id, 0, len, &mut |bytes| {
            out = Some(parse(bytes));
        });
        match (lent, out) {
            (Some(true), Some(r)) => return Ok(r),
            (Some(false), _) => {
                // A miss: straight to the device, without asking the
                // cache a second time.
                fetch_block(device, cache, table_id, block_id, scratch, true).await?;
                return Ok(parse(&scratch[..len]));
            }
            _ => {}
        }
    }
    read_block_verified(device, cache, table_id, block_id, scratch, true).await?;
    Ok(parse(&scratch[..len]))
}

/// Point-lookup reader over one table. Holds a shared device reference;
/// every lookup reuses the caller's scratch block.
///
/// A `Db` builds one from the table's manifest record
/// ([`from_ref`](Self::from_ref)), which costs no I/O: the record names
/// every section of the table. [`open`](Self::open) reads the same facts
/// from the table's footer, for a table no manifest describes yet (a
/// copy being ingested) and for tools.
pub struct TableReader<'d, D: BlockDevice, const BLOCK: usize, const BLOOM_BYTES: usize> {
    device: &'d D,
    /// Owning table's id: first half of the block-cache key. Table ids
    /// are monotone and never reused, so `(table_id, block_id)` names a
    /// block image immutably.
    table_id: u32,
    /// Block cache, or `None` for standalone readers (tests, tooling).
    cache: Option<&'d dyn CachePort<BLOCK>>,
    index_block: u64,
    /// First bloom block; the filter runs to `index_block`.
    bloom_block: u64,
    /// Filter bytes, from the verified footer (see [`BloomShape`]).
    bloom_len: usize,
    entry_count: u64,
    /// Bloom probes per key; 0 when the reader was built from a record
    /// that does not describe the filter, and the gate is then skipped.
    k: u8,
    /// First block of the range-tombstone section: `bloom_block -
    /// rdel_blocks` (the section follows the data blocks).
    rdel_first: u64,
    /// Range-tombstone blocks, from the verified footer.
    rdel_blocks: u32,
    /// Lowest sequence number in the table, from the verified footer.
    min_seq: u64,
}

impl<'d, D: BlockDevice, const BLOCK: usize, const BLOOM_BYTES: usize>
    TableReader<'d, D, BLOCK, BLOOM_BYTES>
{
    /// A reader for the table `tref` describes, with no I/O: the manifest
    /// record gives the table's sections, its entry count, and its bloom
    /// filter's size and probe count, which is everything the footer
    /// says. `Db` point reads use this, so a probe the filter turns away
    /// costs one read of one bloom bucket.
    ///
    /// The record is trusted as far as the manifest is (its blocks are
    /// CRC'd and its magic names this table format), and checked for
    /// self-consistency: a filter length that does not fill exactly the
    /// record's bloom blocks could send a probe into another section.
    /// A record with `bloom_k == 0` does not describe the filter; the
    /// reader then looks every key up in the index.
    ///
    /// # Errors
    ///
    /// [`Error::CorruptManifest`] when the record's counts do not fit
    /// its block range or its filter length disagrees with its bloom
    /// blocks.
    pub fn from_ref<const KEY_MAX: usize>(
        device: &'d D,
        cache: Option<&'d dyn CachePort<BLOCK>>,
        tref: &TableRef<KEY_MAX>,
    ) -> Result<Self, Error<D::Error>> {
        let rdel_first = tref.rdel_first().ok_or(Error::CorruptManifest)?;
        let bloom_block = tref.bloom_first().ok_or(Error::CorruptManifest)?;
        let index_block = tref.index_block().ok_or(Error::CorruptManifest)?;
        let bloom_len = usize::try_from(tref.bloom_len).map_err(|_| Error::CorruptManifest)?;
        if tref.bloom_blocks == 0
            || (tref.bloom_k != 0
                && bloom_section_blocks::<BLOCK>(bloom_len) != u64::from(tref.bloom_blocks))
        {
            return Err(Error::CorruptManifest);
        }
        Ok(Self {
            device,
            table_id: tref.id,
            cache,
            index_block,
            bloom_block,
            bloom_len,
            entry_count: u64::from(tref.entry_count),
            k: tref.bloom_k,
            rdel_first,
            rdel_blocks: tref.rdel_blocks,
            min_seq: tref.min_seq,
        })
    }

    /// Opens the table ending at `footer_block` without a cache: reads and
    /// verifies the footer (magic + CRC); every block is read from the
    /// device. Used for pre-visibility reads (ingest validation) that must
    /// not populate the cache. The range-tombstone section is located from
    /// the footer.
    ///
    /// # Errors
    ///
    /// [`Error::CorruptBlock`] when the footer is missing or fails
    /// verification, or [`Error::Device`] on I/O failure.
    pub async fn open(
        device: &'d D,
        scratch: &mut [u8; BLOCK],
        footer_block: u64,
    ) -> Result<Self, Error<D::Error>> {
        Self::open_cached(device, None, 0, scratch, footer_block).await
    }

    /// Opens a table reader with an optional block cache. `Db` point
    /// reads and scans use this; `table_id` tags every cached block.
    /// Pass `None` (or `CACHE = 0`) to read straight from the device.
    ///
    /// # Errors
    ///
    /// [`Error::CorruptBlock`] when the footer is missing or fails
    /// verification, or [`Error::Device`] on I/O failure.
    pub async fn open_cached(
        device: &'d D,
        cache: Option<&'d dyn CachePort<BLOCK>>,
        table_id: u32,
        scratch: &mut [u8; BLOCK],
        footer_block: u64,
    ) -> Result<Self, Error<D::Error>> {
        // Only the footer's fields are read (and, on a cache hit, only
        // they are fetched): everything parsed below lies inside
        // `FOOTER_LEN`.
        read_range_verified(
            device,
            cache,
            table_id,
            footer_block,
            scratch,
            0..FOOTER_LEN,
            true,
        )
        .await?;
        let magic = u64::from_le_bytes(
            scratch[0..8]
                .try_into()
                .map_err(|_| Error::CorruptBlock { id: footer_block })?,
        );
        if magic != SSTABLE_MAGIC {
            return Err(Error::CorruptBlock { id: footer_block });
        }
        let index_block = u64::from_le_bytes(
            scratch[8..16]
                .try_into()
                .map_err(|_| Error::CorruptBlock { id: footer_block })?,
        );
        let bloom_block = u64::from_le_bytes(
            scratch[16..24]
                .try_into()
                .map_err(|_| Error::CorruptBlock { id: footer_block })?,
        );
        let k = scratch[32];
        if k == 0 {
            return Err(Error::CorruptBlock { id: footer_block });
        }
        let entry_count = u64::from_le_bytes(
            scratch[24..32]
                .try_into()
                .map_err(|_| Error::CorruptBlock { id: footer_block })?,
        );
        let rdel_blocks = u32::from_le_bytes(
            scratch[33..37]
                .try_into()
                .map_err(|_| Error::CorruptBlock { id: footer_block })?,
        );
        let min_seq = u64::from_le_bytes(
            scratch[37..45]
                .try_into()
                .map_err(|_| Error::CorruptBlock { id: footer_block })?,
        );
        let bloom_len = u32::from_le_bytes(
            scratch[45..FOOTER_LEN]
                .try_into()
                .map_err(|_| Error::CorruptBlock { id: footer_block })?,
        );
        let bloom_len =
            usize::try_from(bloom_len).map_err(|_| Error::CorruptBlock { id: footer_block })?;
        // The filter fills `[bloom_block, index_block)` exactly: a footer
        // whose length and block ids disagree is corrupt, and a probe
        // must never reach outside the section.
        if index_block.checked_sub(bloom_block) != Some(bloom_section_blocks::<BLOCK>(bloom_len)) {
            return Err(Error::CorruptBlock { id: footer_block });
        }
        // The section sits right before the bloom block; a count reaching
        // past block 0 is a corrupt footer, never a wrapped read.
        let rdel_first = bloom_block
            .checked_sub(u64::from(rdel_blocks))
            .ok_or(Error::CorruptBlock { id: footer_block })?;
        Ok(Self {
            device,
            table_id,
            cache,
            index_block,
            bloom_block,
            bloom_len,
            entry_count,
            k,
            rdel_first,
            rdel_blocks,
            min_seq,
        })
    }

    /// Range-tombstone blocks in this table (0 when the table has no
    /// range-tombstone section).
    #[must_use]
    pub const fn rdel_blocks(&self) -> u32 {
        self.rdel_blocks
    }

    /// Bloom blocks in this table: at least 1.
    #[must_use]
    pub const fn bloom_blocks(&self) -> u64 {
        self.index_block.saturating_sub(self.bloom_block)
    }

    /// Bytes of bloom filter in this table, as recorded in the verified
    /// footer.
    #[must_use]
    pub const fn bloom_len(&self) -> usize {
        self.bloom_len
    }

    /// Bloom probes per key in this table (0: the reader does not know
    /// the filter and skips it; see [`from_ref`](Self::from_ref)).
    #[must_use]
    pub const fn bloom_k(&self) -> u8 {
        self.k
    }

    /// Lowest sequence number in the table (entries and range
    /// tombstones), as recorded in the verified footer.
    #[must_use]
    pub const fn min_seq(&self) -> u64 {
        self.min_seq
    }

    /// Highest sequence number at or below `max_seq` of a range tombstone
    /// covering `key`, or `None` when no live tombstone covers it. Reads
    /// each rdel block once into `scratch`; corruption is
    /// [`Error::CorruptBlock`], never a silent miss.
    ///
    /// # Errors
    ///
    /// [`Error::CorruptBlock`] when an rdel block fails verification, or
    /// [`Error::Device`] on I/O failure.
    pub async fn covering_rdel_seq(
        &self,
        scratch: &mut [u8; BLOCK],
        key: &[u8],
        max_seq: u64,
    ) -> Result<Option<u64>, Error<D::Error>> {
        covering_rdel_seq_in(
            self.device,
            self.cache,
            self.table_id,
            scratch,
            self.rdel_first,
            self.rdel_blocks,
            key,
            max_seq,
        )
        .await
    }

    /// The table's entry count (values plus tombstones), as recorded in
    /// the verified footer. Used by ingest to cross-check a sealed
    /// descriptor against the copied bytes.
    #[must_use]
    pub const fn entry_count(&self) -> u64 {
        self.entry_count
    }

    /// Looks `key` up: bloom gate → index binary search → data block
    /// restart-point search, selecting the newest version. Tombstones and
    /// misses both yield `Ok(None)`; use [`lookup`](TableReader::lookup)
    /// when the distinction matters. This is the live view — it is
    /// [`get_at`](TableReader::get_at) with `max_seq = u64::MAX`.
    ///
    /// # Errors
    ///
    /// [`Error::BufferTooSmall`] when `val_buf` is smaller than the value,
    /// [`Error::CorruptBlock`] when the index block fails verification, or
    /// [`Error::Device`] on I/O failure.
    ///
    /// `decomp` is the caller's decompression buffer; see
    /// [`lookup_at`](TableReader::lookup_at).
    pub async fn get(
        &self,
        scratch: &mut [u8; BLOCK],
        decomp: &mut [u8; BLOCK],
        key: &[u8],
        val_buf: &mut [u8],
    ) -> Result<Option<usize>, Error<D::Error>> {
        self.get_at(scratch, decomp, key, val_buf, u64::MAX).await
    }

    /// Snapshot read: like [`get`](TableReader::get), but observes only
    /// versions with `seq <= max_seq`.
    ///
    /// # Errors
    ///
    /// [`Error::BufferTooSmall`] when `val_buf` is smaller than the value,
    /// [`Error::CorruptBlock`] when the index block fails verification, or
    /// [`Error::Device`] on I/O failure.
    ///
    /// `decomp` is the caller's decompression buffer; see
    /// [`lookup_at`](TableReader::lookup_at).
    pub async fn get_at(
        &self,
        scratch: &mut [u8; BLOCK],
        decomp: &mut [u8; BLOCK],
        key: &[u8],
        val_buf: &mut [u8],
        max_seq: u64,
    ) -> Result<Option<usize>, Error<D::Error>> {
        Ok(
            match self
                .lookup_at(scratch, decomp, key, val_buf, max_seq)
                .await?
            {
                Lookup::Value { len, .. } => Some(len),
                Lookup::Tombstone { .. } | Lookup::Missing => None,
            },
        )
    }

    /// Looks `key` up, distinguishing a live value from a deletion marker:
    /// bloom gate → index binary search → data block restart-point search,
    /// selecting the newest version. This is the live view — it is
    /// [`lookup_at`](TableReader::lookup_at) with `max_seq = u64::MAX`.
    /// A corrupt bloom block only disables the gate, never the lookup.
    ///
    /// # Errors
    ///
    /// [`Error::BufferTooSmall`] when `val_buf` is smaller than the value,
    /// [`Error::CorruptBlock`] when the index block fails verification, or
    /// [`Error::Device`] on I/O failure.
    ///
    /// `decomp` is the caller's decompression buffer: a data block flagged
    /// compressed is inflated into it as far as the answer needs
    /// (ADR-0027); a raw block is parsed where it lies, in the cache when
    /// the cache lends its images, otherwise in `scratch`.
    pub async fn lookup(
        &self,
        scratch: &mut [u8; BLOCK],
        decomp: &mut [u8; BLOCK],
        key: &[u8],
        val_buf: &mut [u8],
    ) -> Result<Lookup, Error<D::Error>> {
        self.lookup_at(scratch, decomp, key, val_buf, u64::MAX)
            .await
    }

    /// Snapshot read: like [`lookup`](TableReader::lookup), but observes
    /// only versions with `seq <= max_seq` (`u64::MAX` is the live view).
    /// A key's version run can straddle data blocks, and a `max_seq` can
    /// hide every version in the run's first block, so the search walks
    /// forward across data blocks while a block ends inside the run.
    /// A corrupt bloom block only disables the gate, never the lookup.
    ///
    /// # Errors
    ///
    /// [`Error::BufferTooSmall`] when `val_buf` is smaller than the value,
    /// [`Error::CorruptBlock`] when the index block fails verification, or
    /// [`Error::Device`] on I/O failure.
    ///
    /// `decomp` is the caller's decompression buffer: a data block flagged
    /// compressed is inflated into it as far as the answer needs
    /// (ADR-0027); a raw block is parsed where it lies, in the cache when
    /// the cache lends its images, otherwise in `scratch`.
    pub async fn lookup_at(
        &self,
        scratch: &mut [u8; BLOCK],
        decomp: &mut [u8; BLOCK],
        key: &[u8],
        val_buf: &mut [u8],
        max_seq: u64,
    ) -> Result<Lookup, Error<D::Error>> {
        let device = self.device;
        // Bloom gate (advisory: a corrupt bloom block just disables the
        // optimization, never the lookup; a device error is still an
        // error). The key's probes all fall in one bucket of the filter,
        // so only that bucket is read, from the one block that holds it.
        // A reader that was not told the filter's shape skips the gate.
        if self.k != 0 {
            let probe = BloomProbe::new(self.bloom_len, key);
            let (nth, range) = BloomShape::locate(BLOCK, &probe.range());
            let bloom_block = self
                .bloom_block
                .checked_add(nth)
                .filter(|&b| b < self.index_block)
                .ok_or(Error::CorruptBlock {
                    id: self.bloom_block,
                })?;
            match read_range_verified(
                device,
                self.cache,
                self.table_id,
                bloom_block,
                scratch,
                range.clone(),
                true,
            )
            .await
            {
                Ok(()) => {
                    if scratch
                        .get(range)
                        .is_some_and(|bucket| !probe.matches(bucket, self.k))
                    {
                        return Ok(Lookup::Missing);
                    }
                }
                Err(Error::CorruptBlock { .. }) => {}
                Err(e) => return Err(e),
            }
        }
        // Index: structural; corruption is an error, never a silent miss.
        // Searched where it lies in the cache when the cache lends it.
        let index_block = self.index_block;
        let Some((mut block_id, mut remaining)) = parse_verified(
            device,
            self.cache,
            self.table_id,
            index_block,
            scratch,
            BLOCK - CRC_LEN,
            |payload| index_lookup::<D::Error>(payload, key, index_block),
        )
        .await??
        else {
            return Ok(Lookup::Missing);
        };
        // Data blocks are contiguous, so the run walk below only moves
        // forward, and `remaining` keeps it inside the table's data blocks.
        loop {
            // A bad CRC is an error, never a miss (see `read_data_block`).
            // A compressed block inflates into `decomp` only as far as the
            // answer needs; a raw block parses where it lies.
            let outcome = lookup_data_block(
                device,
                self.cache,
                self.table_id,
                block_id,
                scratch,
                decomp,
                key,
                val_buf,
                max_seq,
            )
            .await?;
            match outcome {
                BlockOutcome::Hit(DataHit::Value {
                    len,
                    seq,
                    expire_at,
                }) => {
                    return Ok(Lookup::Value {
                        len,
                        seq,
                        expire_at,
                    });
                }
                BlockOutcome::Hit(DataHit::Tombstone(seq)) => return Ok(Lookup::Tombstone { seq }),
                // A larger key was seen: later blocks only hold larger keys.
                BlockOutcome::Absent => return Ok(Lookup::Missing),
                // The block ended inside (or before) the key's run: the next
                // data block may continue it.
                BlockOutcome::Truncated => {
                    remaining = remaining.saturating_sub(1);
                    if remaining == 0 {
                        return Ok(Lookup::Missing);
                    }
                    block_id = block_id
                        .checked_add(1)
                        .ok_or(Error::CorruptBlock { id: block_id })?;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    extern crate std;

    use super::{
        BLOOM_BITS_PER_KEY, BLOOM_BUCKET_BYTES, BloomShape, CRC_LEN, INDEX_STRIDE, append_index,
        bloom_add, bloom_fold, bloom_maybe_contains, bloom_section_blocks, index_last_le_block,
        index_lookup,
    };
    use std::vec::Vec;
    use std::{format, vec};

    const BLOCK: usize = 4096;

    /// An index block body over `keys` (sorted, duplicates allowed);
    /// entry `i` names data block `100 + i`.
    fn index_of(keys: &[[u8; 3]]) -> [u8; BLOCK] {
        let mut block = [0u8; BLOCK];
        let mut len = 0usize;
        for (i, k) in keys.iter().enumerate() {
            append_index::<(), BLOCK>(&mut block, &mut len, Some(k), 100 + i as u64, 0).unwrap();
        }
        block
    }

    /// The rule the search implements, spelled out over a slice.
    fn model_lookup(keys: &[[u8; 3]], key: &[u8]) -> Option<(u64, usize)> {
        // Last restart at or below the key, then back over duplicates of
        // the key to the restart before the first of them.
        let le = keys.iter().filter(|k| k.as_slice() <= key).count();
        if le == 0 {
            return None;
        }
        let mut pos = le - 1;
        while pos > 0 && keys[pos].as_slice() == key {
            pos -= 1;
        }
        Some((100 + pos as u64, keys.len() - pos))
    }

    fn model_last_le(keys: &[[u8; 3]], key: &[u8]) -> Option<u64> {
        let le = keys.iter().filter(|k| k.as_slice() <= key).count();
        le.checked_sub(1).map(|pos| 100 + pos as u64)
    }

    /// Key `n` as three bytes, order-preserving.
    fn k(n: usize) -> [u8; 3] {
        let [hi, lo] = u16::try_from(n).unwrap().to_be_bytes();
        [b'k', hi, lo]
    }

    /// Every index size around the stride boundaries, with and without
    /// runs of duplicate restarts, probed with every key in and between
    /// the entries: the one-pass search agrees with the model.
    #[test]
    fn index_search_matches_the_model() {
        let sizes = [
            0,
            1,
            2,
            INDEX_STRIDE - 1,
            INDEX_STRIDE,
            INDEX_STRIDE + 1,
            2 * INDEX_STRIDE,
            3 * INDEX_STRIDE + 5,
            100,
        ];
        // `dup_every`: 0 = all keys distinct; otherwise every such entry
        // starts a run of `dup_len` equal restarts.
        for (dup_every, dup_len) in [
            (0usize, 0usize),
            (5, 2),
            (7, 4),
            (1, 3),
            (INDEX_STRIDE, INDEX_STRIDE + 2),
        ] {
            for &n in &sizes {
                // Even numbers only, so odd probes fall between entries.
                let mut keys = Vec::with_capacity(n);
                let mut next = 2usize;
                while keys.len() < n {
                    let run = if dup_every != 0 && keys.len() % dup_every == 0 {
                        dup_len
                    } else {
                        1
                    };
                    for _ in 0..run.min(n - keys.len()) {
                        keys.push(k(next));
                    }
                    next += 2;
                }
                let block = index_of(&keys);
                let payload = &block[..BLOCK - CRC_LEN];
                for probe in 0..next + 2 {
                    let key = k(probe);
                    assert_eq!(
                        index_lookup::<()>(payload, &key, 9).unwrap(),
                        model_lookup(&keys, &key),
                        "lookup: {n} entries, dups ({dup_every}, {dup_len}), probe {probe}"
                    );
                    assert_eq!(
                        index_last_le_block::<()>(payload, &key, 9).unwrap(),
                        model_last_le(&keys, &key),
                        "last_le: {n} entries, dups ({dup_every}, {dup_len}), probe {probe}"
                    );
                }
            }
        }
    }

    /// Bytes that are neither an entry nor zero padding are corruption,
    /// wherever they sit relative to the key searched for.
    #[test]
    fn index_search_rejects_garbage_after_the_entries() {
        let keys: Vec<[u8; 3]> = (1..=40).map(|i| k(2 * i)).collect();
        let mut block = index_of(&keys);
        // One stray byte in the padding, far past the last entry.
        block[BLOCK - CRC_LEN - 1] = 1;
        let payload = &block[..BLOCK - CRC_LEN];
        for probe in [0usize, 3, 40, 81, 500] {
            assert!(index_lookup::<()>(payload, &k(probe), 9).is_err());
            assert!(index_last_le_block::<()>(payload, &k(probe), 9).is_err());
        }
    }

    /// The spine of the multi-block filter: folding a filter built at
    /// full length gives, bit for bit, the filter built at the folded
    /// length. So a reader that knows only the folded length probes it
    /// exactly as it would any filter, and no inserted key is lost.
    #[test]
    fn a_folded_filter_is_the_filter_built_at_the_folded_length() {
        const B: usize = BLOOM_BUCKET_BYTES;
        let keys: Vec<Vec<u8>> = (0..3_000u32)
            .map(|i| format!("fold-{:07}", i.wrapping_mul(2_654_435_761)).into_bytes())
            .collect();
        for (buckets, k) in [
            (1usize, 3u8),
            (2, 1),
            (12, 7),
            (48, 6),
            (255, 30),
            (4080, 7),
        ] {
            let full_len = buckets * B;
            let mut full = vec![0u8; full_len];
            for key in &keys {
                bloom_add(&mut full, key, k);
            }
            for f in (1..=buckets).filter(|f| buckets.is_multiple_of(*f)) {
                let len = full_len / f;
                let mut direct = vec![0u8; len];
                for key in &keys {
                    bloom_add(&mut direct, key, k);
                }
                let mut folded = full.clone();
                bloom_fold(&mut folded, full_len, len);
                assert_eq!(
                    &folded[..len],
                    &direct[..],
                    "{buckets} buckets folded by {f}"
                );
                for key in &keys {
                    assert!(bloom_maybe_contains(&folded[..len], key, k));
                }
            }
        }
    }

    /// A probe's bit positions are `first + i * stride` reduced modulo
    /// the bucket's bit count. For a whole bucket that is computed with
    /// a mask; it must be the same positions the division gives, or
    /// tables written by one build would not be read by another.
    #[test]
    fn probe_positions_are_the_remainder_whatever_the_bucket_size() {
        use super::BloomProbe;
        for len in [1usize, 7, 32, 63, 64, 100, 128, 4032] {
            for i in 0..200u32 {
                let key = format!("bits-{i}").into_bytes();
                let probe = BloomProbe::new(len, &key);
                let bits = (probe.range().len() * 8) as u64;
                let want: Vec<usize> = (0..30u64)
                    .map(|n| {
                        let at = probe.first.wrapping_add(n.wrapping_mul(probe.stride));
                        usize::try_from(at % bits).unwrap()
                    })
                    .collect();
                assert_eq!(
                    probe.bits(30).collect::<Vec<_>>(),
                    want,
                    "{len}-byte filter"
                );
            }
        }
    }

    /// A fold that cannot be exact (a length that is not whole buckets,
    /// or that does not divide the full length) leaves the filter alone:
    /// it must never produce a filter that loses a key.
    #[test]
    fn a_fold_that_cannot_be_exact_changes_nothing() {
        let mut filter: Vec<u8> = (0..640u32).map(|i| (i * 7 + 1).to_le_bytes()[0]).collect();
        let before = filter.clone();
        for (full, len) in [
            (640, 0),
            (640, 640),
            (640, 700),
            (640, 100),
            (640, 192),
            (1280, 64),
        ] {
            bloom_fold(&mut filter, full, len);
            assert_eq!(filter, before, "fold {full} -> {len}");
        }
    }

    /// What `for_entries` promises, over a spread of block sizes, filter
    /// sizes and entry counts.
    #[test]
    fn a_bloom_shape_fits_its_table() {
        for block in [68usize, 128, 256, 4096, 16_384] {
            let per_block = BloomShape::buckets_per_block(block);
            for max_blocks in [1usize, 2, 3, 8, 12, 16] {
                let max_len = max_blocks * per_block * BLOOM_BUCKET_BYTES;
                let full = BloomShape::full(max_len, block);
                assert_eq!(full.len, max_len);
                assert_eq!(full.blocks as usize, max_blocks);
                let mut last = BloomShape { len: 0, blocks: 0 };
                let mut entries = 0u64;
                while entries < 4 * (max_len as u64) {
                    let shape = BloomShape::for_entries(max_len, block, entries);
                    let buckets = shape.len / BLOOM_BUCKET_BYTES;
                    // Whole buckets, a divisor of the full filter, in
                    // exactly the blocks a reader will expect.
                    assert_eq!(shape.len % BLOOM_BUCKET_BYTES, 0);
                    assert_eq!(full.len % shape.len, 0);
                    assert_eq!(
                        u64::from(shape.blocks),
                        bloom_section_blocks_of(block, shape.len)
                    );
                    assert_eq!(shape.blocks as usize, buckets.div_ceil(per_block));
                    // Never more than the configuration, never smaller
                    // for a larger table.
                    assert!(shape.len <= full.len && shape.blocks >= 1);
                    assert!(shape.len >= last.len && shape.blocks >= last.blocks);
                    // The target is met unless the filter is already
                    // as large as it gets.
                    let bits = shape.len as u64 * 8;
                    assert!(
                        shape == full || bits >= entries * BLOOM_BITS_PER_KEY,
                        "{entries} entries got {bits} bits"
                    );
                    // And no block is spent that a smaller legal filter
                    // would have saved.
                    for f in (1..=full.len / BLOOM_BUCKET_BYTES)
                        .filter(|f| (full.len / B).is_multiple_of(*f))
                    {
                        let n = full.len / B / f;
                        if (n * B * 8) as u64 >= entries * BLOOM_BITS_PER_KEY {
                            assert!(n.div_ceil(per_block) >= shape.blocks as usize);
                        }
                        // Within the blocks it takes, it is the longest
                        // filter the full one folds to.
                        if n.div_ceil(per_block) == shape.blocks as usize {
                            assert!(n * B <= shape.len, "{entries} entries: {n} buckets fit too");
                        }
                    }
                    last = shape;
                    entries = entries * 3 / 2 + 1;
                }
                // One block's worth of configuration never folds.
                if max_blocks == 1 {
                    assert_eq!(BloomShape::for_entries(max_len, block, 0), full);
                }
            }
        }
    }

    const B: usize = BLOOM_BUCKET_BYTES;

    fn bloom_section_blocks_of(block: usize, len: usize) -> u64 {
        match block {
            68 => bloom_section_blocks::<68>(len),
            128 => bloom_section_blocks::<128>(len),
            256 => bloom_section_blocks::<256>(len),
            4096 => bloom_section_blocks::<4096>(len),
            16_384 => bloom_section_blocks::<16_384>(len),
            _ => unreachable!("add the block size above"),
        }
    }

    /// Filters under one bucket, and sizes that are not whole buckets,
    /// are one block and are stored as they always were.
    #[test]
    fn a_short_or_ragged_filter_is_one_block() {
        for (max_len, block, len) in [
            (1usize, 4096usize, 1usize),
            (63, 4096, 63),
            (64, 4096, 64),
            (65, 4096, 64),
            (100, 4096, 64),
            (256, 4096, 256),
            (4092, 4096, 4032),
            // A block too small for a bucket holds what fits of the filter.
            (40, 48, 40),
            (60, 48, 44),
            (200, 48, 44),
        ] {
            let full = BloomShape::full(max_len, block);
            assert_eq!((full.len, full.blocks), (len, 1), "{max_len} in {block}");
            for entries in [0, 1, 1_000, u64::MAX] {
                assert_eq!(BloomShape::for_entries(max_len, block, entries), full);
            }
        }
    }

    /// A bucket lies inside one block, and `locate` names it: the block
    /// index within the section and the bytes within that block.
    #[test]
    fn a_bucket_never_straddles_blocks() {
        for block in [68usize, 256, 4096, 16_384] {
            let per_block = BloomShape::buckets_per_block(block);
            for bucket in 0..per_block * 5 {
                let range = bucket * B..(bucket + 1) * B;
                let (nth, within) = BloomShape::locate(block, &range);
                assert_eq!(nth, (bucket / per_block) as u64);
                assert_eq!(within.start, (bucket % per_block) * B);
                assert_eq!(within.len(), B);
                assert!(within.end <= block - CRC_LEN);
            }
        }
        // A short filter is the head of the section's only block.
        assert_eq!(BloomShape::locate(4096, &(0..7)), (0, 0..7));
        assert_eq!(BloomShape::locate(48, &(0..40)), (0, 0..40));
    }
}

/// The partial inflate (ADR-0027) against the whole-block search: the
/// same answer for every key, snapshot and buffer size, on every data
/// block of compressed and raw tables.
#[cfg(test)]
mod partial_inflate_tests {
    extern crate std;

    use super::{
        BlockOutcome, CRC_LEN, CachePort, DataHit, SstEntry, TableWriter, data_lookup,
        inflate_data_block, lookup_data_block, lookup_image, lookup_image_checked,
    };
    use crate::Error;
    use crate::compress::CompressScratch;
    use crate::device::BlockDevice;
    use core::future::Future;
    use core::pin::pin;
    use core::task::{Context, Poll, Waker};
    use std::format;
    use std::vec::Vec;

    const BLOCK: usize = 4096;

    struct Ram(Vec<[u8; BLOCK]>);

    impl BlockDevice for Ram {
        type Error = ();
        const BLOCK: usize = BLOCK;
        fn poll_read_block(
            &self,
            _: &mut Context<'_>,
            id: u64,
            buf: &mut [u8],
        ) -> Poll<Result<(), ()>> {
            buf.copy_from_slice(&self.0[usize::try_from(id).unwrap()]);
            Poll::Ready(Ok(()))
        }
        fn poll_write_block(
            &mut self,
            _: &mut Context<'_>,
            id: u64,
            buf: &[u8],
        ) -> Poll<Result<(), ()>> {
            self.0[usize::try_from(id).unwrap()].copy_from_slice(buf);
            Poll::Ready(Ok(()))
        }
        fn poll_flush(&mut self, _: &mut Context<'_>) -> Poll<Result<(), ()>> {
            Poll::Ready(Ok(()))
        }
    }

    fn block_on<F: Future>(f: F) -> F::Output {
        let mut f = pin!(f);
        let mut cx = Context::from_waker(Waker::noop());
        loop {
            if let Poll::Ready(v) = f.as_mut().poll(&mut cx) {
                return v;
            }
        }
    }

    /// An outcome as comparable data, with the value bytes of a hit.
    fn shape(r: &Result<BlockOutcome, crate::Error<()>>, val: &[u8]) -> std::string::String {
        match r {
            Ok(BlockOutcome::Hit(DataHit::Value {
                len,
                seq,
                expire_at,
            })) => format!("value {seq} {expire_at} {:?}", &val[..*len]),
            Ok(BlockOutcome::Hit(DataHit::Tombstone(seq))) => format!("tombstone {seq}"),
            Ok(BlockOutcome::Absent) => "absent".into(),
            Ok(BlockOutcome::Truncated) => "truncated".into(),
            Err(e) => format!("error {e:?}"),
        }
    }

    /// A table of `keys` keys, each with 1 to 4 versions (newest first),
    /// some of them tombstones and some with an expiry; written with
    /// compression (`compressible`) or with every block raw.
    fn table(keys: u32, compressible: bool) -> (Ram, u64) {
        // Key, value, sequence number, tombstone, expiry.
        type Row = (Vec<u8>, Vec<u8>, u64, bool, u64);
        let mut entries: Vec<Row> = Vec::new();
        let mut seq = 10_000u64;
        let mut x = 0x9E37_79B9u32;
        for k in 0..keys {
            // Every third key leaves a gap, so absent keys fall between.
            let key = format!("key{:05}", k * 3).into_bytes();
            for v in 0..=(k % 4) {
                x ^= x << 13;
                x ^= x >> 17;
                x ^= x << 5;
                let len = 20 + usize::try_from(x % 180).unwrap();
                let fill = b'a' + k.to_le_bytes()[0] % 26;
                let val: Vec<u8> = if compressible {
                    std::iter::repeat_n(fill, len).collect()
                } else {
                    let mut y = x;
                    (0..len)
                        .map(|_| {
                            y = y.wrapping_mul(1_103_515_245).wrapping_add(12_345);
                            y.to_le_bytes()[2]
                        })
                        .collect()
                };
                let tomb = (k + v) % 7 == 0;
                let expire = if (k + v) % 5 == 0 {
                    1_000 + u64::from(k)
                } else {
                    0
                };
                entries.push((key.clone(), val, seq - u64::from(v), tomb, expire));
            }
            seq -= 10;
        }
        let mut dev = Ram(std::vec![[0u8; BLOCK]; 512]);
        let mut w = TableWriter::<BLOCK, 64, 32>::new(0, 3);
        let mut cs = CompressScratch::<BLOCK>::new();
        for (key, val, seq, tomb, expire) in &entries {
            let e = SstEntry {
                key,
                val: if *tomb { &[] } else { val },
                seq: *seq,
                tombstone: *tomb,
                expire_at: *expire,
            };
            let cs = compressible.then_some(&mut cs);
            block_on(w.push(&mut dev, e, cs)).unwrap();
        }
        let done = block_on(w.finish(&mut dev, compressible.then_some(&mut cs))).unwrap();
        (dev, done.data_blocks)
    }

    /// A bit flipped in the RAM copy of a cached data block never changes
    /// what a point lookup of that block answers (ADR-0031): either the
    /// search never acts on the byte, or a check over it fails, the entry
    /// is dropped, and the block is read from the device again. Every
    /// byte of every data block, one bit each, against the keys the block
    /// holds and the absent keys between them, at two read sequence
    /// numbers. In a raw block one lookup misses most flips (it checks
    /// the few chunks it reads); a compressed block's stream is checked
    /// whole.
    #[test]
    fn a_flipped_bit_in_a_cached_data_block_never_changes_a_lookup() {
        use crate::cache::BlockCache;
        use core::cell::RefCell;

        let table_id = 7;
        let mut val = std::vec![0u8; 4096];
        for compressible in [false, true] {
            let (dev, blocks) = table(40, compressible);
            let cache = RefCell::new(BlockCache::<BLOCK, 4>::new());
            let (mut raw, mut logical) = ([0u8; BLOCK], [0u8; BLOCK]);
            let mut answer = |cache: Option<&RefCell<BlockCache<BLOCK, 4>>>,
                              b: u64,
                              key: &[u8],
                              max_seq: u64| {
                let cache = cache.map(|c| c as &dyn CachePort<BLOCK>);
                let r = block_on(lookup_data_block(
                    &dev,
                    cache,
                    table_id,
                    b,
                    &mut raw,
                    &mut logical,
                    key,
                    &mut val,
                    max_seq,
                ));
                shape(&r, &val)
            };
            let (mut unnoticed, mut caught) = (0u32, 0u32);
            for b in 0..blocks {
                // The keys the block holds, and the absent ones around
                // them; and what an uncached lookup answers for each.
                let keys: Vec<Vec<u8>> = (0..40 * 3 + 2)
                    .map(|k| format!("key{k:05}").into_bytes())
                    .collect();
                let held: Vec<bool> = keys
                    .iter()
                    .map(|k| {
                        let a = answer(None, b, k, u64::MAX);
                        a.starts_with("value") || a.starts_with("tombstone")
                    })
                    .collect();
                let mut probes = Vec::new();
                for (i, key) in keys.iter().enumerate() {
                    let near = held[i.saturating_sub(1)..(i + 2).min(keys.len())]
                        .iter()
                        .any(|&h| h);
                    if near {
                        for s in [u64::MAX, 9_800] {
                            probes.push((key.clone(), s, answer(None, b, key, s)));
                        }
                    }
                }
                assert!(probes.len() >= 8, "block {b}: {} probes", probes.len());
                let image = dev.0[usize::try_from(b).unwrap()];
                for at in 0..BLOCK {
                    let damaged = || {
                        cache.borrow_mut().put(table_id, b, &image, true);
                        assert!(cache.borrow_mut().damage(table_id, b, at, 1 << (at % 8)));
                        cache.borrow().stats().rejected
                    };
                    // One lookup alone: did it notice the flip?
                    let rejected = damaged();
                    let (key, s, want) = &probes[at % probes.len()];
                    assert_eq!(&answer(Some(&cache), b, key, *s), want, "byte {at}");
                    if cache.borrow().stats().rejected > rejected {
                        caught += 1;
                    } else {
                        unnoticed += 1;
                    }
                    // Every lookup of the block, over the same flip: at
                    // every byte in a release build, every third in a
                    // debug one (CI runs both).
                    if cfg!(debug_assertions) && at % 3 != 0 {
                        continue;
                    }
                    damaged();
                    for (key, s, want) in &probes {
                        let got = answer(Some(&cache), b, key, *s);
                        assert_eq!(&got, want, "block {b}, byte {at}, max_seq {s}");
                    }
                }
            }
            assert!(caught > 0, "no flip was ever read");
            if !compressible {
                assert!(
                    unnoticed > caught,
                    "a raw block checked whole: one lookup missed {unnoticed} flips, noticed {caught}"
                );
            }
        }
    }

    /// A lookup checked a range at a time, as a lending cache checks it
    /// (`CachePort::with_checked`), answers as the whole-block search
    /// does; a raw block's search asks about a few of its chunks, not
    /// all of them; and a check that fails on any of them ends the
    /// search with `CorruptBlock`.
    #[test]
    fn a_checked_lookup_answers_alike_and_checks_what_it_reads() {
        for compressible in [true, false] {
            let (dev, blocks) = table(700, compressible);
            for b in 0..usize::try_from(blocks).unwrap() {
                let image = dev.0[b];
                let mut whole = [0u8; BLOCK];
                let inflated =
                    inflate_data_block::<(), BLOCK>(&image, &mut whole, b as u64).unwrap();
                let body: &[u8] = if inflated {
                    &whole[..BLOCK - CRC_LEN]
                } else {
                    &image[..BLOCK - CRC_LEN]
                };
                for k in 0..(700 * 3 + 5) {
                    let key = format!("key{k:05}").into_bytes();
                    for max_seq in [u64::MAX, 10_000 - 3_000 - 1, 5_000, 0] {
                        let cap = 4096;
                        let mut want_val = std::vec![0u8; cap];
                        let want = data_lookup::<()>(
                            body,
                            &key,
                            &mut want_val,
                            b as u64,
                            max_seq,
                            &mut |_, _| true,
                        );
                        let mut touched = [false; BLOCK / 256];
                        let mut checked_val = std::vec![0u8; cap];
                        let mut scratch = [0xEEu8; BLOCK];
                        let checked = lookup_image_checked::<(), BLOCK>(
                            &image,
                            &mut scratch,
                            &key,
                            &mut checked_val,
                            b as u64,
                            max_seq,
                            &mut |from, to| {
                                if to > from {
                                    for c in &mut touched[from / 256..=(to - 1) / 256] {
                                        *c = true;
                                    }
                                }
                                true
                            },
                        );
                        assert_eq!(shape(&checked, &checked_val), shape(&want, &want_val));
                        let n = touched.iter().filter(|&&t| t).count();
                        if !inflated {
                            assert!(n <= 16, "block {b}, key {k}: {n} chunks checked");
                        }
                        let Some(c) = touched.iter().rposition(|&t| t) else {
                            continue;
                        };
                        let mut scratch = [0xEEu8; BLOCK];
                        let failed = lookup_image_checked::<(), BLOCK>(
                            &image,
                            &mut scratch,
                            &key,
                            &mut checked_val,
                            b as u64,
                            max_seq,
                            &mut |from, to| to <= from || (to - 1) / 256 < c || from / 256 > c,
                        );
                        assert!(
                            matches!(failed, Err(Error::CorruptBlock { .. })),
                            "block {b}, key {k}: a failed check was ignored"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn partial_inflate_answers_as_the_whole_block_does() {
        for compressible in [true, false] {
            let (dev, blocks) = table(700, compressible);
            assert!(blocks > 4, "the table spans blocks");
            let mut compressed = 0;
            for b in 0..usize::try_from(blocks).unwrap() {
                let image = dev.0[b];
                let mut whole = [0u8; BLOCK];
                let inflated =
                    inflate_data_block::<(), BLOCK>(&image, &mut whole, b as u64).unwrap();
                compressed += usize::from(inflated);
                let body: &[u8] = if inflated {
                    &whole[..BLOCK - CRC_LEN]
                } else {
                    &image[..BLOCK - CRC_LEN]
                };
                for k in 0..(700 * 3 + 5) {
                    let key = format!("key{k:05}").into_bytes();
                    for max_seq in [u64::MAX, 10_000 - 3_000 - 1, 10_000 - 3_000 - 2, 5_000, 0] {
                        for cap in [4096usize, 30] {
                            let mut want_val = std::vec![0u8; cap];
                            let want = data_lookup::<()>(
                                body,
                                &key,
                                &mut want_val,
                                b as u64,
                                max_seq,
                                &mut |_, _| true,
                            );
                            let mut got_val = std::vec![0u8; cap];
                            let mut scratch = [0xEEu8; BLOCK];
                            let got = lookup_image::<(), BLOCK>(
                                &image,
                                &mut scratch,
                                &key,
                                &mut got_val,
                                b as u64,
                                max_seq,
                            );
                            assert_eq!(
                                shape(&got, &got_val),
                                shape(&want, &want_val),
                                "block {b}, key {k}, max_seq {max_seq}, buffer {cap}, compressible {compressible}"
                            );
                        }
                    }
                }
            }
            // The point of it: a hit on a block's first key inflates the
            // first step of the block, not the block. Past that the
            // output buffer is as the caller left it.
            for b in 0..usize::try_from(blocks).unwrap() {
                let image = dev.0[b];
                let mut whole = [0u8; BLOCK];
                if !inflate_data_block::<(), BLOCK>(&image, &mut whole, b as u64).unwrap() {
                    continue;
                }
                let kl = usize::from(u16::from_le_bytes([whole[0], whole[1]]));
                let first = whole[13..13 + kl].to_vec();
                let mut scratch = [0xEEu8; BLOCK];
                let mut val = [0u8; 4096];
                let got = lookup_image::<(), BLOCK>(
                    &image,
                    &mut scratch,
                    &first,
                    &mut val,
                    b as u64,
                    u64::MAX,
                );
                assert!(matches!(got, Ok(BlockOutcome::Hit(_))), "block {b}");
                assert!(
                    scratch[2048..BLOCK - CRC_LEN].iter().all(|&x| x == 0xEE),
                    "block {b}: a hit on the first key inflated the whole block"
                );
            }
            if compressible {
                assert!(compressed > 4, "most blocks are compressed");
            } else {
                assert_eq!(compressed, 0, "written without compression");
            }
        }
    }
}
