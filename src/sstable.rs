//! Immutable sorted-run tables: streaming writer and point-lookup reader.
//!
//! On-device layout:
//!
//! ```text
//! [data block]* [bloom block] [index block] [footer block]
//! ```
//!
//! Every block is `BLOCK` bytes; the last 4 bytes are the CRC32 of the
//! preceding payload bytes. All integers are little-endian.
//!
//! - Data block: entries `key_len u16 | val_len u16 | seq u64 | op u8 |
//!   key | val`, then restart offsets (`u16` each, one every 16 entries),
//!   then the restart count (`u16`). Restart offsets make a block
//!   binary-searchable without a full scan.
//! - Bloom block: a `BLOOM_BYTES * 8`-bit array. `k` (stored in the footer) probes
//!   come from double hashing of a hand-rolled splitmix64-over-Fx-fold hash.
//! - Index block: one entry per data block,
//!   `first_key_len u16 | first_key | block_id u64 | max_seq u64`.
//! - Footer block: `magic u64 = "lsmtable" | index_block u64 |
//!   bloom_block u64 | entry_count u64 | k u8`.
//!
//! A failed CRC means "treat as absent": a bad footer or index block is a
//! [`Error::CorruptBlock`]; a bad data block is skipped. Corruption is never
//! silent.
//!
//! Bounds: restart offsets are `u16`, so a data block effectively tops out
//! at 64 KiB, and at most 2048 entries share one data block (restart offsets
//! live in a fixed `[u16; 128]`). A table whose index would overflow one
//! block fails cleanly with [`Error::NoSpace`].

use core::future::poll_fn;

use crate::cache::CachePort;
use crate::compress::{CompressScratch, decompress};
use crate::crc::crc32;
use crate::device::BlockDevice;
use crate::error::Error;
use crate::manifest::KeyBound;
use crate::wal::Op;

/// Footer magic: ASCII "lsmtable".
pub const SSTABLE_MAGIC: u64 = 0x6C73_6D74_6162_6C65;

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
    /// Smallest key.
    pub first_key: KeyBound<KEY_MAX>,
    /// Largest key.
    pub last_key: KeyBound<KEY_MAX>,
}

/// Fixed entry header bytes: `key_len u16 | val_len u16 | seq u64 | op u8`.
const ENTRY_HEADER: usize = 13;
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

/// Bloom bit positions for `key`: double hashing over the two mixed hashes.
fn probe_positions(key: &[u8], k: u8, bits: u64) -> impl Iterator<Item = u64> {
    let h1 = splitmix64(fx_fold(key));
    let h2 = splitmix64(h1 ^ 0x9e37_79b9_7f4a_7c15);
    (0..k).map(move |i| h1.wrapping_add(u64::from(i).wrapping_mul(h2)) % bits)
}

/// Sets the `k` probe bits for `key`. A zero-length filter degrades to "no
/// information" (all lookups proceed); it never panics.
fn bloom_add(bloom: &mut [u8], key: &[u8], k: u8) {
    let Ok(bits) = u64::try_from(bloom.len() * 8) else {
        return;
    };
    if bits == 0 {
        return;
    }
    for p in probe_positions(key, k, bits) {
        // `p < bits <= bloom.len() * 8`: the position always addresses the
        // filter on 64-bit targets; the fallback only exists for 32-bit.
        let Ok(p) = usize::try_from(p) else { continue };
        bloom[p / 8] |= 1u8 << (p % 8);
    }
}

/// Reports whether `key` may be in the filter: all `k` probe bits set.
///
/// Never a false negative; false positives possible. `bloom` must be the
/// exact filter bytes (any length); `k` comes from [`bloom_k`].
#[must_use]
pub fn bloom_maybe_contains(bloom: &[u8], key: &[u8], k: u8) -> bool {
    let Ok(bits) = u64::try_from(bloom.len() * 8) else {
        return true;
    };
    if bits == 0 {
        return true;
    }
    probe_positions(key, k, bits).all(|p| {
        // Unaddressable on 32-bit only; answering "maybe" keeps the
        // no-false-negative guarantee.
        let Ok(p) = usize::try_from(p) else {
            return true;
        };
        bloom[p / 8] >> (p % 8) & 1 == 1
    })
}

/// Computes the table shape (pass 1, no I/O). Entries must arrive in
/// key-ascending order; the caller pre-allocated `data_blocks + 3` blocks
/// from this plan before calling [`write_table`].
///
/// # Errors
///
/// [`Error::KeyTooLarge`] / [`Error::ValueTooLarge`] for oversize entries,
/// [`Error::NoSpace`] when an entry cannot fit any data block.
pub fn plan_table<'a, E, const BLOCK: usize, const KEY_MAX: usize>(
    entries: impl Iterator<Item = SstEntry<'a>>,
) -> Result<TablePlan<KEY_MAX>, Error<E>> {
    let mut data_blocks = 0u64;
    let mut payload = 0usize;
    let mut n = 0u64;
    let mut count = 0u64;
    let mut max_seq = 0u64;
    let mut first: Option<&'a [u8]> = None;
    let mut last: Option<&'a [u8]> = None;
    for e in entries {
        let (elen, _, _) = entry_sizes::<E>(&e)?;
        if !entry_fits::<BLOCK>(payload, n, elen) {
            if n == 0 {
                return Err(Error::NoSpace);
            }
            data_blocks += 1;
            payload = 0;
            n = 0;
            if !entry_fits::<BLOCK>(0, 0, elen) {
                return Err(Error::NoSpace);
            }
        }
        if first.is_none() {
            first = Some(e.key);
        }
        last = Some(e.key);
        if e.seq > max_seq {
            max_seq = e.seq;
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
            .ok_or(Error::NoSpace)?;
        let rstart = body_end.checked_sub(tail_len).ok_or(Error::NoSpace)?;
        // `entry_fits` reserves the worst-case tail, so this cannot trigger
        // for writer-produced blocks.
        if payload_len > rstart {
            return Err(Error::NoSpace);
        }
        buf[payload_len..rstart].fill(0);
        let mut off = rstart;
        for &r in rs {
            buf[off..off + 2].copy_from_slice(&r.to_le_bytes());
            off += 2;
        }
        // `entry_fits` caps entries per block, so this always fits.
        let rc = u16::try_from(rs.len()).map_err(|_| Error::NoSpace)?;
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
    /// Key/value entries written (including tombstones).
    pub entry_count: u64,
    /// Highest sequence number written.
    pub max_seq: u64,
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
}

impl<const BLOCK: usize, const BLOOM_BYTES: usize, const KEY_MAX: usize>
    TableWriter<BLOCK, BLOOM_BYTES, KEY_MAX>
{
    /// Starts a table at `base` with `k` bloom probes per key.
    #[must_use]
    pub const fn new(base: u64, k: u8) -> Self {
        Self {
            base,
            k,
            data: [0u8; BLOCK],
            index: [0u8; BLOCK],
            bloom: [0u8; BLOOM_BYTES],
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
        }
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
    /// entries, [`Error::NoSpace`] when a single entry cannot fit in an
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
                return Err(Error::NoSpace);
            }
            let id = self
                .base
                .checked_add(self.data_blocks)
                .ok_or(Error::NoSpace)?;
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
            if !entry_fits::<BLOCK>(0, 0, elen) {
                return Err(Error::NoSpace);
            }
            PushOutcome::BlockSealed
        };
        if self.n.is_multiple_of(RESTART_INTERVAL) {
            // `entry_fits` guarantees this never overflows the array.
            debug_assert!(self.nrestarts < self.restarts.len());
            self.restarts[self.nrestarts] =
                u16::try_from(self.payload).map_err(|_| Error::NoSpace)?;
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
        bloom_add(&mut self.bloom, e.key, self.k);
        self.payload += elen;
        self.n += 1;
        self.entry_count += 1;
        Ok(sealed)
    }

    /// Seals the trailing partial block (if any), then writes the bloom,
    /// index, and footer blocks and flushes the device. Reports the table's
    /// shape for the manifest's [`TableRef`](crate::manifest::TableRef).
    ///
    /// `rdel_blocks` is the table's range-tombstone section length,
    /// written by the caller *before* this writer's `base` (see
    /// [`write_rdel_blocks`]); the footer records it so readers can find
    /// the section.
    ///
    /// # Errors
    ///
    /// [`Error::NoSpace`] when the table outgrows its pre-allocated run, or
    /// [`Error::Device`] on I/O failure.
    ///
    /// `compress` is the caller's compression scratch (`None` = store raw);
    /// see [`push`](TableWriter::push).
    pub async fn finish<D: BlockDevice>(
        &mut self,
        device: &mut D,
        compress: Option<&mut CompressScratch<BLOCK>>,
        rdel_blocks: u32,
    ) -> Result<FinishedTable<KEY_MAX>, Error<D::Error>> {
        if self.n > 0 {
            let id = self
                .base
                .checked_add(self.data_blocks)
                .ok_or(Error::NoSpace)?;
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
            self.n = 0;
        }

        // Bloom block: reuse the data staging buffer (it is free now).
        let bloom_id = self
            .base
            .checked_add(self.data_blocks)
            .ok_or(Error::NoSpace)?;
        self.data.fill(0);
        self.data[..self.bloom.len()].copy_from_slice(&self.bloom);
        seal_block(
            device,
            bloom_id,
            &mut self.data,
            self.bloom.len(),
            None,
            None,
        )
        .await?;

        // Index block.
        let index_id = bloom_id.checked_add(1).ok_or(Error::NoSpace)?;
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
        // rdel_blocks.
        let footer_id = index_id.checked_add(1).ok_or(Error::NoSpace)?;
        self.data.fill(0);
        self.data[0..8].copy_from_slice(&SSTABLE_MAGIC.to_le_bytes());
        self.data[8..16].copy_from_slice(&index_id.to_le_bytes());
        self.data[16..24].copy_from_slice(&bloom_id.to_le_bytes());
        self.data[24..32].copy_from_slice(&self.entry_count.to_le_bytes());
        self.data[32] = self.k;
        self.data[33..37].copy_from_slice(&rdel_blocks.to_le_bytes());
        seal_block(device, footer_id, &mut self.data, 37, None, None).await?;

        // Table blocks are durable before the manifest commit makes them
        // visible.
        poll_fn(|cx| device.poll_flush(cx))
            .await
            .map_err(Error::Device)?;

        let first_key =
            KeyBound::from_slice(&self.first_key[..self.first_len]).ok_or(Error::EmptyKey)?;
        let last_key =
            KeyBound::from_slice(&self.last_key[..self.last_len]).ok_or(Error::EmptyKey)?;
        Ok(FinishedTable {
            data_blocks: self.data_blocks,
            entry_count: self.entry_count,
            max_seq: self.max_seq,
            first_key,
            last_key,
        })
    }
}

/// Streams the table: the one-shot form of [`TableWriter`].
///
/// Entries must arrive in key-ascending order (the same order
/// [`plan_table`] plans); `base` is a pre-allocated run of
/// `plan.data_blocks + 3` blocks. Returns the blocks written.
///
/// Block order on device: `[data]* [bloom] [index] [footer]`. The device is
/// flushed at the end, so the table is durable before the manifest commit
/// that makes it visible.
///
/// `compress` is the caller's compression scratch (`None` = store all data
/// blocks raw); see [`TableWriter::push`].
///
/// # Errors
///
/// [`Error::KeyTooLarge`] / [`Error::ValueTooLarge`] for oversize entries,
/// [`Error::NoSpace`] when the table outgrows its pre-allocated run or the
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
    rdel_blocks: u32,
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
    let done = w.finish(device, cs, rdel_blocks).await?;
    Ok(done.data_blocks + 3)
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
const RDEL_TRAILER: usize = 6;

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
        let el = rdel_entry_len::<E>(r.start.len(), r.end.len())?;
        if el + RDEL_TRAILER > BLOCK {
            return Err(Error::NoSpace);
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

/// Wire length of one rdel entry: `start_len u16 | end_len u16 | seq u64 |
/// start | end`.
fn rdel_entry_len<E>(start_len: usize, end_len: usize) -> Result<usize, Error<E>> {
    start_len
        .checked_add(end_len)
        .and_then(|n| n.checked_add(12))
        .ok_or(Error::NoSpace)
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
    let count16 = u16::try_from(count).map_err(|_| Error::NoSpace)?;
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
pub(crate) struct RdelWriter<const BLOCK: usize> {
    base: u64,
    buf: [u8; BLOCK],
    payload: usize,
    count: u32,
    blocks: u32,
}

impl<const BLOCK: usize> RdelWriter<BLOCK> {
    /// Writer for the section starting at `base`. `const`-constructible.
    #[must_use]
    pub(crate) const fn new(base: u64) -> Self {
        Self {
            base,
            buf: [0u8; BLOCK],
            payload: 0,
            count: 0,
            blocks: 0,
        }
    }

    /// Appends one tombstone, sealing the current block first when the
    /// entry no longer fits.
    ///
    /// # Errors
    ///
    /// [`Error::NoSpace`] when the tombstone cannot fit in an empty
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
            return Err(Error::NoSpace);
        }
        if self.payload + el + RDEL_TRAILER > BLOCK {
            let id = self
                .base
                .checked_add(u64::from(self.blocks))
                .ok_or(Error::NoSpace)?;
            seal_rdel_block(device, id, &mut self.buf, self.payload, self.count).await?;
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
            let id = self
                .base
                .checked_add(u64::from(self.blocks))
                .ok_or(Error::NoSpace)?;
            seal_rdel_block(device, id, &mut self.buf, self.payload, self.count).await?;
            self.blocks += 1;
            self.count = 0;
            self.payload = 0;
        }
        Ok(self.blocks)
    }
}

/// Exact block counter mirroring [`RdelWriter`]'s greedy packing: feed it
/// the same entry sequence and [`finish`](RdelCounter::finish) returns the
/// block count [`RdelWriter::finish`] would report. The compaction
/// range-tombstone merge re-sorts entries across inputs, so its repacked
/// block count is not bounded by the inputs' rdel block counts (a sorted
/// merge is not a subsequence of the concatenation); the merge therefore
/// counts exactly in a dry-run pass and reserves that many blocks.
///
/// The boundary logic here must stay identical to [`RdelWriter::push`]:
/// a sealed block holds `payload` bytes with `payload + RDEL_TRAILER <=
/// BLOCK`, sealing exactly when the next entry stops fitting.
pub(crate) struct RdelCounter<const BLOCK: usize> {
    payload: usize,
    count: u32,
    blocks: u32,
}

impl<const BLOCK: usize> RdelCounter<BLOCK> {
    /// Counter for a section that would start at `base` (unused: counting
    /// needs no I/O, but the shape mirrors [`RdelWriter::new`]).
    #[must_use]
    pub(crate) const fn new() -> Self {
        Self {
            payload: 0,
            count: 0,
            blocks: 0,
        }
    }

    /// Accounts for one tombstone, sealing the current block first when
    /// the entry no longer fits — exactly as [`RdelWriter::push`] does.
    pub(crate) const fn push(&mut self, r: RdelEntry<'_>) {
        // `RdelWriter::push` rejects an entry that cannot fit in an empty
        // block with `NoSpace`; merged entries always came out of a valid
        // block, so this cannot trigger and needs no error path.
        let el = 12 + r.start.len() + r.end.len();
        if self.payload + el + RDEL_TRAILER > BLOCK {
            self.blocks += 1;
            self.payload = 0;
            self.count = 0;
        }
        self.payload += el;
        self.count += 1;
    }

    /// Returns the total blocks the counted entries would occupy — 0 when
    /// nothing was pushed, matching [`RdelWriter::finish`].
    #[must_use]
    pub(crate) const fn finish(&self) -> u32 {
        if self.count > 0 {
            self.blocks + 1
        } else {
            self.blocks
        }
    }
}

/// Writes a table's range-tombstone section at `[base, base+n)`: `rdels`
/// in `(start asc, seq desc)` order, packed greedily into blocks. Returns
/// the block count — 0 when `rdels` is empty (no section is written).
///
/// The blocks are durable only after the caller's commit-point flush: the
/// data-section writer's [`TableWriter::finish`] flushes, and both flush
/// and compaction call it after their rdel section is written.
///
/// # Errors
///
/// [`Error::NoSpace`] when a single tombstone cannot fit in an empty
/// block, or [`Error::Device`] on I/O failure.
pub(crate) async fn write_rdel_blocks<D, const BLOCK: usize>(
    device: &mut D,
    base: u64,
    rdels: impl Iterator<Item = RdelEntry<'_>>,
) -> Result<u32, Error<D::Error>>
where
    D: BlockDevice,
{
    let mut w = RdelWriter::<BLOCK>::new(base);
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
/// a panic. Used by every data-block read path (point lookups, scans,
/// compaction cursors, entry streams).
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
    let fk = first_key.ok_or(Error::NoSpace)?;
    let kl = u16::try_from(fk.len()).map_err(|_| Error::NoSpace)?;
    let elen = 2 + fk.len() + 8 + 8;
    if *index_len + elen > BLOCK - CRC_LEN {
        return Err(Error::NoSpace);
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
/// block_count)` in writer order (data blocks, bloom, index, footer).
/// Data and bloom block contents are position-independent, but index
/// entries carry absolute data-block ids and the footer carries the
/// absolute index/bloom ids of the table's *original* placement, so this
/// rewrites those pointers to the destination layout and re-seals the
/// touched blocks' CRCs. The original placement is derived from the
/// footer's own pointers (old bloom id minus the data-block count) and
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
    rdel: u64,
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
        // Data blocks sit after the rdel section: `tblock` is a
        // table-relative index into `[rdel, rdel + data_blocks)`.
        if tblock < rdel
            || tblock
                >= rdel
                    .checked_add(data_blocks)
                    .ok_or(Error::CorruptBlock { id: index_id })?
        {
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
    let data_blocks = u64::from(block_count)
        .checked_sub(rdel)
        .and_then(|n| n.checked_sub(3))
        .ok_or(Error::CorruptBlock { id: dst_base })?;
    // Data section starts after the rdel section: index and footer shift
    // by the rdel count.
    let index_id = dst_base
        .checked_add(rdel)
        .and_then(|b| b.checked_add(data_blocks))
        .and_then(|b| b.checked_add(1))
        .ok_or(Error::CorruptBlock { id: dst_base })?;
    let footer_id = index_id
        .checked_add(1)
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
    // Structural check: bloom, index, footer are consecutive, so the
    // original base is the bloom id minus the data-block count and the
    // rdel-block count.
    if old_index
        != old_bloom
            .checked_add(1)
            .ok_or(Error::CorruptBlock { id: footer_id })?
    {
        return Err(Error::CorruptBlock { id: footer_id });
    }
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
        rdel,
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
        .checked_add(1)
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

/// Binary-searches the index for the first data block that may hold `key`.
/// Parses the index entry at position `i` by re-parsing from the start
/// (O(n) per call, no allocation). Shared by the index searches below.
fn index_entry_at(payload: &[u8], i: usize) -> Result<(&[u8], u64, usize), ()> {
    let mut off = 0usize;
    for _ in 0..i {
        let (_, _, next) = index_entry_parse(payload, off)?;
        off = next;
    }
    index_entry_parse(payload, off)
}

/// Counts index entries, then binary-searches for the first entry whose
/// `first_key` sorts after `key`. Returns `(lo, count)` with `lo >= 1`, or
/// `None` when `key` sorts before every block. Shared by [`index_lookup`]
/// and [`index_last_le_block`].
fn index_upper_bound<E>(
    payload: &[u8],
    key: &[u8],
    index_id: u64,
) -> Result<Option<(usize, usize)>, Error<E>> {
    let corrupt = || Error::CorruptBlock { id: index_id };
    // Count entries with one linear pass; the binary search below then
    // re-parses on demand (O(n log n) byte scans, no allocation). The
    // block's CRC already passed, so a parse failure is either genuine zero
    // padding (all zeros to the end of the block body: the end of the index)
    // or structural corruption — the two are distinguished explicitly.
    let mut count = 0usize;
    let mut off = 0usize;
    while off < payload.len() {
        if let Ok((_, _, next)) = index_entry_parse(payload, off) {
            off = next;
            count += 1;
        } else if all_zero(&payload[off..]) {
            break;
        } else {
            return Err(corrupt());
        }
    }
    let mut lo = 0usize;
    let mut hi = count;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let (fkey, _, _) = index_entry_at(payload, mid).map_err(|()| corrupt())?;
        if fkey <= key {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if lo == 0 {
        return Ok(None);
    }
    Ok(Some((lo, count)))
}
/// Binary-searches the index for the first data block that may hold `key`.
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
/// on the last restart with `first_key <= key` and then walks back over
/// every duplicate of `key`, ending at the last restart strictly below
/// `key` (entry 0 stays put): the first block whose key range can contain
/// `key`. Starting any later would miss the run's newest versions.
pub(crate) fn index_lookup<E>(
    payload: &[u8],
    key: &[u8],
    index_id: u64,
) -> Result<Option<(u64, usize)>, Error<E>> {
    let corrupt = || Error::CorruptBlock { id: index_id };
    let Some((lo, count)) = index_upper_bound(payload, key, index_id)? else {
        return Ok(None);
    };
    // `lo - 1`: last restart with `first_key <= key`. Walk back over
    // duplicates of `key` — the run's newest versions live in the first
    // block it touches, which can be the block *before* the first
    // duplicate restart when a block seals mid-run.
    let mut pos = lo - 1;
    while pos > 0 {
        let (fkey, _, _) = index_entry_at(payload, pos).map_err(|()| corrupt())?;
        if fkey != key {
            break;
        }
        pos -= 1;
    }
    let (_, block_id, _) = index_entry_at(payload, pos).map_err(|()| corrupt())?;
    // The walk may visit every data block from `pos` onward — never past
    // the table's last data block (whose readers must not stray into the
    // bloom/index/footer blocks).
    Ok(Some((block_id, count - pos)))
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
    let corrupt = || Error::CorruptBlock { id: index_id };
    let Some((lo, _)) = index_upper_bound(payload, key, index_id)? else {
        return Ok(None);
    };
    // `lo - 1`: last restart with `first_key <= key`.
    let (_, block_id, _) = index_entry_at(payload, lo - 1).map_err(|()| corrupt())?;
    Ok(Some(block_id))
}

/// Reads and verifies a table footer, returning its index block id.
/// Used by the scan iterator to position cursors via the block index
/// without opening a full [`TableReader`].
///
/// # Errors
///
/// [`Error::CorruptBlock`] when the footer is missing or fails
/// verification, or [`Error::Device`] on I/O failure.
pub(crate) async fn footer_index_block<D: BlockDevice, const BLOCK: usize>(
    device: &D,
    cache: Option<&dyn CachePort<BLOCK>>,
    table_id: u32,
    scratch: &mut [u8; BLOCK],
    footer_block: u64,
) -> Result<u64, Error<D::Error>> {
    read_block_cached(device, cache, table_id, footer_block, scratch, true).await?;
    check_block_crc::<D::Error, BLOCK>(scratch, footer_block)?;
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
    Ok(index_block)
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
fn data_lookup<E>(
    payload: &[u8],
    key: &[u8],
    val_buf: &mut [u8],
    block_id: u64,
    max_seq: u64,
) -> Result<BlockOutcome, Error<E>> {
    let corrupt = || Error::CorruptBlock { id: block_id };
    if payload.len() < 6 {
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
        let (entry, _) = data_entry_parse(payload, roff).map_err(|()| corrupt())?;
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
            return if all_zero(&payload[off..rstart]) {
                Ok(BlockOutcome::Truncated)
            } else {
                Err(corrupt())
            };
        };
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
                        if entry.op == Op::Delete {
                            return Ok(BlockOutcome::Hit(DataHit::Tombstone(entry.seq)));
                        }
                        if entry.val.len() > val_buf.len() {
                            return Err(Error::BufferTooSmall {
                                need: entry.val.len(),
                            });
                        }
                        val_buf[..entry.val.len()].copy_from_slice(entry.val);
                        return Ok(BlockOutcome::Hit(DataHit::Value {
                            len: entry.val.len(),
                            seq: entry.seq,
                            expire_at: entry.expire_at,
                        }));
                    }
                    off = next;
                    if off >= rstart {
                        return Ok(BlockOutcome::Truncated);
                    }
                    let Ok((e, n)) = data_entry_parse(payload, off) else {
                        return if all_zero(&payload[off..rstart]) {
                            Ok(BlockOutcome::Truncated)
                        } else {
                            Err(corrupt())
                        };
                    };
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
        read_block_cached(device, cache, table_id, id, scratch, true).await?;
        let count = rdel_block_count::<D::Error, BLOCK>(scratch, id)?;
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

/// Point-lookup reader over one table. Holds a shared device reference;
/// every lookup reuses the caller's scratch block.
/// Reads one `SSTable` block through the block cache when one is supplied.
///
/// On a hit the cached image is copied into `scratch` and no device I/O
/// happens; on a miss the block is read from the device and then
/// inserted (with `hot` setting the CLOCK reference bit — see
/// [`crate::cache`]). The cached bytes are the physical image exactly as
/// the device returned them, so every check the caller runs afterwards
/// (CRC, magic, bloom, decompression) behaves byte-identically to a
/// re-read, including corruption semantics. `table_id` is the owning
/// table's manifest id: ids are monotone and never reused, so the key
/// `(table_id, block_id)` names the image immutably.
pub(crate) async fn read_block_cached<D: BlockDevice, const BLOCK: usize>(
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
    poll_fn(|cx| device.poll_read_block(cx, block_id, scratch))
        .await
        .map_err(Error::Device)?;
    if let Some(c) = cache {
        c.put(table_id, block_id, scratch, hot);
    }
    Ok(())
}

/// Point-lookup reader over one table. Holds a shared device reference;
/// every lookup reuses the caller's scratch block.
pub struct TableReader<'d, D: BlockDevice, const BLOCK: usize, const BLOOM_BYTES: usize> {
    device: &'d D,
    /// Owning table's id: first half of the block-cache key. Table ids
    /// are monotone and never reused, so `(table_id, block_id)` names a
    /// block image immutably.
    table_id: u32,
    /// Block cache, or `None` for standalone readers (tests, tooling).
    cache: Option<&'d dyn CachePort<BLOCK>>,
    index_block: u64,
    bloom_block: u64,
    entry_count: u64,
    k: u8,
    /// First block of the range-tombstone section (`first_block` of the
    /// table; the section precedes the data blocks).
    rdel_first: u64,
    /// Range-tombstone blocks, from the verified footer.
    rdel_blocks: u32,
}

impl<'d, D: BlockDevice, const BLOCK: usize, const BLOOM_BYTES: usize>
    TableReader<'d, D, BLOCK, BLOOM_BYTES>
{
    /// Opens the table ending at `footer_block`: reads and verifies the
    /// footer (magic + CRC). `rdel_first` is the table's first block: the
    /// range-tombstone section precedes the data blocks. `table_id` is the
    /// table's manifest id — the block-cache key's first half — and
    /// `cache` (if any) serves the footer and every later block read.
    /// Opens a table reader without a cache: every block is read from
    /// the device. This is the original pre-v0.16 API, kept for
    /// pre-visibility reads (ingest validation) that must not populate
    /// the cache.
    ///
    /// # Errors
    ///
    /// [`Error::CorruptBlock`] when the footer is missing or fails
    /// verification, or [`Error::Device`] on I/O failure.
    pub async fn open(
        device: &'d D,
        scratch: &mut [u8; BLOCK],
        footer_block: u64,
        rdel_first: u64,
    ) -> Result<Self, Error<D::Error>> {
        Self::open_cached(device, None, 0, scratch, footer_block, rdel_first).await
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
        rdel_first: u64,
    ) -> Result<Self, Error<D::Error>> {
        read_block_cached(device, cache, table_id, footer_block, scratch, true).await?;
        check_block_crc::<D::Error, BLOCK>(scratch, footer_block)?;
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
        Ok(Self {
            device,
            table_id,
            cache,
            index_block,
            bloom_block,
            entry_count,
            k,
            rdel_first,
            rdel_blocks,
        })
    }

    /// Range-tombstone blocks in this table (0 when the table has no
    /// range-tombstone section).
    #[must_use]
    pub const fn rdel_blocks(&self) -> u32 {
        self.rdel_blocks
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
    /// `decomp` is the caller's decompression buffer: data blocks flagged
    /// compressed are inflated into it before parsing; raw blocks are
    /// parsed in place from `scratch` with no copy.
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
    /// `decomp` is the caller's decompression buffer: data blocks flagged
    /// compressed are inflated into it before parsing; raw blocks are
    /// parsed in place from `scratch` with no copy.
    pub async fn lookup_at(
        &self,
        scratch: &mut [u8; BLOCK],
        decomp: &mut [u8; BLOCK],
        key: &[u8],
        val_buf: &mut [u8],
        max_seq: u64,
    ) -> Result<Lookup, Error<D::Error>> {
        let device = self.device;
        // Bloom gate (advisory: corruption just disables the optimization,
        // never the lookup). The cached image is byte-identical to a
        // re-read, so the advisory-CRC rule below behaves the same.
        read_block_cached(
            device,
            self.cache,
            self.table_id,
            self.bloom_block,
            scratch,
            true,
        )
        .await?;
        if check_block_crc::<D::Error, BLOCK>(scratch, self.bloom_block).is_ok()
            && !bloom_maybe_contains(&scratch[..BLOOM_BYTES], key, self.k)
        {
            return Ok(Lookup::Missing);
        }
        // Index: structural; corruption is an error, never a silent miss.
        read_block_cached(
            device,
            self.cache,
            self.table_id,
            self.index_block,
            scratch,
            true,
        )
        .await?;
        check_block_crc::<D::Error, BLOCK>(scratch, self.index_block)?;
        let payload_end = BLOCK - CRC_LEN;
        let Some((mut block_id, mut remaining)) =
            index_lookup::<D::Error>(&scratch[..payload_end], key, self.index_block)?
        else {
            return Ok(Lookup::Missing);
        };
        // Data blocks are contiguous, so the run walk below only moves
        // forward, and `remaining` keeps it inside the table's data blocks.
        loop {
            // Data: a torn block is treated as absent.
            read_block_cached(device, self.cache, self.table_id, block_id, scratch, true).await?;
            if check_block_crc::<D::Error, BLOCK>(scratch, block_id).is_err() {
                return Ok(Lookup::Missing);
            }
            // A flagged block inflates into `decomp`; a raw block parses
            // in place from `scratch`. A flag/decoding failure here is a
            // format violation, not a torn write — but like a torn write
            // it reads as absent, never as a wrong value.
            let body_end = BLOCK - CRC_LEN;
            let body: &[u8] = match inflate_data_block::<D::Error, BLOCK>(scratch, decomp, block_id)
            {
                Err(_) => return Ok(Lookup::Missing),
                Ok(true) => &decomp[..body_end],
                Ok(false) => &scratch[..body_end],
            };
            match data_lookup::<D::Error>(body, key, val_buf, block_id, max_seq)? {
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
