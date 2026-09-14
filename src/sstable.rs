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
const CRC_LEN: usize = 4;

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
    Ok((ENTRY_HEADER + e.key.len() + vl_raw, kl, vl))
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
    let mut k: u64 = 1;
    if entries > 0 {
        if let (Ok(m), Some(den)) = (u64::try_from(bloom_bits), entries.checked_mul(1000)) {
            if let Some(num) = m.checked_mul(693).and_then(|num| num.checked_div(den)) {
                k = num;
            }
        }
    }
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
async fn seal_block<D: BlockDevice, const BLOCK: usize>(
    device: &mut D,
    id: u64,
    buf: &mut [u8; BLOCK],
    payload_len: usize,
    restarts: Option<&[u16]>,
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
    let crc = crc32(&buf[..body_end]);
    buf[body_end..BLOCK].copy_from_slice(&crc.to_le_bytes());
    poll_fn(|cx| device.poll_write_block(cx, id, buf))
        .await
        .map_err(Error::Device)?;
    Ok(())
}

/// Streams the table (pass 2, the actual I/O).
///
/// Entries must arrive in the same key-ascending order as [`plan_table`];
/// `base` is a pre-allocated run of `plan.data_blocks + 3` blocks.
/// `data`/`index` stage blocks, `bloom` accumulates the filter bits (`bloom`
/// is `BLOOM_BYTES` bytes, i.e. `BLOOM_BYTES * 8` filter bits). Returns the
/// blocks written.
///
/// Block order on device: `[data]* [bloom] [index] [footer]`. The device is
/// flushed at the end, so the table is durable before the manifest commit
/// that makes it visible.
///
/// # Errors
///
/// [`Error::KeyTooLarge`] / [`Error::ValueTooLarge`] for oversize entries,
/// [`Error::NoSpace`] when the table outgrows its pre-allocated run or the
/// index would overflow one block, or [`Error::Device`] on I/O failure.
#[allow(clippy::too_many_arguments)]
pub async fn write_table<'a, D, const BLOCK: usize, const BLOOM_BYTES: usize>(
    device: &mut D,
    base: u64,
    k: u8,
    entries: impl Iterator<Item = SstEntry<'a>>,
    data: &mut [u8; BLOCK],
    index: &mut [u8; BLOCK],
    bloom: &mut [u8; BLOOM_BYTES],
) -> Result<u64, Error<D::Error>>
where
    D: BlockDevice,
{
    data.fill(0);
    index.fill(0);
    bloom.fill(0);

    let mut restarts = [0u16; 128];
    let mut payload = 0usize;
    let mut n = 0u64;
    let mut nrestarts = 0usize;
    let mut index_len = 0usize;
    let mut data_blocks = 0u64;
    let mut entry_count = 0u64;
    let mut block_first: Option<&'a [u8]> = None;
    let mut block_max_seq = 0u64;

    for e in entries {
        let (elen, kl, vl) = entry_sizes::<D::Error>(&e)?;
        if !entry_fits::<BLOCK>(payload, n, elen) {
            if n == 0 {
                return Err(Error::NoSpace);
            }
            let id = base.checked_add(data_blocks).ok_or(Error::NoSpace)?;
            seal_block(device, id, data, payload, Some(&restarts[..nrestarts])).await?;
            append_index::<D::Error, BLOCK>(index, &mut index_len, block_first, id, block_max_seq)?;
            data_blocks += 1;
            payload = 0;
            n = 0;
            nrestarts = 0;
            block_first = None;
            block_max_seq = 0;
            data.fill(0);
            if !entry_fits::<BLOCK>(0, 0, elen) {
                return Err(Error::NoSpace);
            }
        }
        if n.is_multiple_of(RESTART_INTERVAL) {
            // `entry_fits` guarantees this never overflows the array.
            debug_assert!(nrestarts < restarts.len());
            restarts[nrestarts] = u16::try_from(payload).map_err(|_| Error::NoSpace)?;
            nrestarts += 1;
        }
        data[payload..payload + 2].copy_from_slice(&kl.to_le_bytes());
        data[payload + 2..payload + 4].copy_from_slice(&vl.to_le_bytes());
        data[payload + 4..payload + 12].copy_from_slice(&e.seq.to_le_bytes());
        data[payload + 12] = if e.tombstone { Op::Delete } else { Op::Put }.to_u8();
        let ko = payload + ENTRY_HEADER;
        data[ko..ko + e.key.len()].copy_from_slice(e.key);
        // Tombstones store no value bytes.
        let vlen = if e.tombstone { 0 } else { e.val.len() };
        data[ko + e.key.len()..ko + e.key.len() + vlen].copy_from_slice(&e.val[..vlen]);
        if block_first.is_none() {
            block_first = Some(e.key);
        }
        if e.seq > block_max_seq {
            block_max_seq = e.seq;
        }
        bloom_add(bloom, e.key, k);
        payload += elen;
        n += 1;
        entry_count += 1;
    }
    if n > 0 {
        let id = base.checked_add(data_blocks).ok_or(Error::NoSpace)?;
        seal_block(device, id, data, payload, Some(&restarts[..nrestarts])).await?;
        append_index::<D::Error, BLOCK>(index, &mut index_len, block_first, id, block_max_seq)?;
        data_blocks += 1;
    }

    // Bloom block: reuse the data staging buffer (it is free now).
    let bloom_id = base.checked_add(data_blocks).ok_or(Error::NoSpace)?;
    data.fill(0);
    data[..bloom.len()].copy_from_slice(bloom);
    seal_block(device, bloom_id, data, bloom.len(), None).await?;

    // Index block.
    let index_id = bloom_id.checked_add(1).ok_or(Error::NoSpace)?;
    seal_block(device, index_id, index, index_len, None).await?;

    // Footer block: magic | index | bloom | entry_count | k.
    let footer_id = index_id.checked_add(1).ok_or(Error::NoSpace)?;
    data.fill(0);
    data[0..8].copy_from_slice(&SSTABLE_MAGIC.to_le_bytes());
    data[8..16].copy_from_slice(&index_id.to_le_bytes());
    data[16..24].copy_from_slice(&bloom_id.to_le_bytes());
    data[24..32].copy_from_slice(&entry_count.to_le_bytes());
    data[32] = k;
    seal_block(device, footer_id, data, 33, None).await?;

    // Table blocks are durable before the manifest commit makes them visible.
    poll_fn(|cx| device.poll_flush(cx))
        .await
        .map_err(Error::Device)?;
    Ok(data_blocks + 3)
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

/// Checks a block's trailing CRC32.
fn check_block_crc<E, const BLOCK: usize>(block: &[u8; BLOCK], id: u64) -> Result<(), Error<E>> {
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
fn all_zero(bytes: &[u8]) -> bool {
    let mut chunks = bytes.chunks_exact(8);
    chunks.all(|c| u64::from_ne_bytes(c.try_into().unwrap_or([0; 8])) == 0)
        && chunks.remainder().iter().all(|&b| b == 0)
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

/// Binary-searches the index for the last data block with
/// `first_key <= key`. Returns `None` when `key` sorts before every block.
fn index_lookup<E>(payload: &[u8], key: &[u8], index_id: u64) -> Result<Option<u64>, Error<E>> {
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
    let entry_at = |i: usize| {
        let mut off = 0usize;
        for _ in 0..i {
            let (_, _, next) = index_entry_parse(payload, off)?;
            off = next;
        }
        index_entry_parse(payload, off)
    };
    let mut lo = 0usize;
    let mut hi = count;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let (fkey, _, _) = entry_at(mid).map_err(|()| corrupt())?;
        if fkey <= key {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if lo == 0 {
        return Ok(None);
    }
    let (_, block_id, _) = entry_at(lo - 1).map_err(|()| corrupt())?;
    Ok(Some(block_id))
}

/// One parsed data-block entry, borrowing the block.
struct ParsedEntry<'a> {
    key: &'a [u8],
    val: &'a [u8],
    seq: u64,
    op: Op,
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
    let next = off
        .checked_add(ENTRY_HEADER)
        .ok_or(())?
        .checked_add(kl)
        .ok_or(())?
        .checked_add(vl)
        .ok_or(())?;
    if next > payload.len() {
        return Err(());
    }
    Ok((ParsedEntry { key, val, seq, op }, next))
}

/// What a data-block search found, with the entry's sequence number.
enum DataHit {
    /// Live value: byte length and sequence number.
    Value(usize, u64),
    /// Deletion marker: sequence number.
    Tombstone(u64),
}

/// Searches one data block: restart-point binary search, then a linear scan.
fn data_lookup<E>(
    payload: &[u8],
    key: &[u8],
    val_buf: &mut [u8],
    block_id: u64,
) -> Result<Option<DataHit>, Error<E>> {
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
        if roff >= rstart {
            None
        } else {
            Some(roff)
        }
    };
    // Binary search over restart points: the last restart whose entry key
    // does not sort after `key`.
    let mut lo = 0usize;
    let mut hi = rcount;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let roff = restart_at(mid).ok_or_else(corrupt)?;
        let (entry, _) = data_entry_parse(payload, roff).map_err(|()| corrupt())?;
        if entry.key <= key {
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
        // up to the restart tail) means the key is absent from this block;
        // any other unparseable structure is corruption, not padding.
        let Ok((entry, next)) = data_entry_parse(payload, off) else {
            return if all_zero(&payload[off..rstart]) {
                Ok(None)
            } else {
                Err(corrupt())
            };
        };
        match entry.key.cmp(key) {
            core::cmp::Ordering::Less => off = next,
            core::cmp::Ordering::Equal => {
                if entry.op == Op::Delete {
                    return Ok(Some(DataHit::Tombstone(entry.seq)));
                }
                if entry.val.len() > val_buf.len() {
                    return Err(Error::BufferTooSmall {
                        need: entry.val.len(),
                    });
                }
                val_buf[..entry.val.len()].copy_from_slice(entry.val);
                return Ok(Some(DataHit::Value(entry.val.len(), entry.seq)));
            }
            core::cmp::Ordering::Greater => return Ok(None),
        }
    }
    Ok(None)
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
    },
    /// Deletion marker: shadows the same key in older tables.
    Tombstone {
        /// The tombstone's sequence number.
        seq: u64,
    },
    /// Key not present in this table.
    Missing,
}

/// Point-lookup reader over one table. Holds a shared device reference;
/// every lookup reuses the caller's scratch block.
pub struct TableReader<'d, D: BlockDevice, const BLOCK: usize, const BLOOM_BYTES: usize> {
    device: &'d D,
    index_block: u64,
    bloom_block: u64,
    k: u8,
}

impl<'d, D: BlockDevice, const BLOCK: usize, const BLOOM_BYTES: usize>
    TableReader<'d, D, BLOCK, BLOOM_BYTES>
{
    /// Opens the table ending at `footer_block`: reads and verifies the
    /// footer (magic + CRC).
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
        poll_fn(|cx| device.poll_read_block(cx, footer_block, scratch))
            .await
            .map_err(Error::Device)?;
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
        Ok(Self {
            device,
            index_block,
            bloom_block,
            k,
        })
    }

    /// Looks `key` up: bloom gate → index binary search → data block
    /// restart-point search. Tombstones and misses both yield `Ok(None)`;
    /// use [`lookup`](TableReader::lookup) when the distinction matters.
    ///
    /// # Errors
    ///
    /// [`Error::BufferTooSmall`] when `val_buf` is smaller than the value,
    /// [`Error::CorruptBlock`] when the index block fails verification, or
    /// [`Error::Device`] on I/O failure.
    pub async fn get(
        &self,
        scratch: &mut [u8; BLOCK],
        key: &[u8],
        val_buf: &mut [u8],
    ) -> Result<Option<usize>, Error<D::Error>> {
        Ok(match self.lookup(scratch, key, val_buf).await? {
            Lookup::Value { len, .. } => Some(len),
            Lookup::Tombstone { .. } | Lookup::Missing => None,
        })
    }

    /// Looks `key` up, distinguishing a live value from a deletion marker:
    /// bloom gate → index binary search → data block restart-point search.
    /// A corrupt bloom block only disables the gate, never the lookup.
    ///
    /// # Errors
    ///
    /// [`Error::BufferTooSmall`] when `val_buf` is smaller than the value,
    /// [`Error::CorruptBlock`] when the index block fails verification, or
    /// [`Error::Device`] on I/O failure.
    pub async fn lookup(
        &self,
        scratch: &mut [u8; BLOCK],
        key: &[u8],
        val_buf: &mut [u8],
    ) -> Result<Lookup, Error<D::Error>> {
        let device = self.device;
        // Bloom gate (advisory: corruption just disables the optimization,
        // never the lookup).
        poll_fn(|cx| device.poll_read_block(cx, self.bloom_block, scratch))
            .await
            .map_err(Error::Device)?;
        if check_block_crc::<D::Error, BLOCK>(scratch, self.bloom_block).is_ok()
            && !bloom_maybe_contains(&scratch[..BLOOM_BYTES], key, self.k)
        {
            return Ok(Lookup::Missing);
        }
        // Index: structural; corruption is an error, never a silent miss.
        poll_fn(|cx| device.poll_read_block(cx, self.index_block, scratch))
            .await
            .map_err(Error::Device)?;
        check_block_crc::<D::Error, BLOCK>(scratch, self.index_block)?;
        let payload_end = BLOCK - CRC_LEN;
        let Some(block_id) =
            index_lookup::<D::Error>(&scratch[..payload_end], key, self.index_block)?
        else {
            return Ok(Lookup::Missing);
        };
        // Data: a torn block is treated as absent.
        poll_fn(|cx| device.poll_read_block(cx, block_id, scratch))
            .await
            .map_err(Error::Device)?;
        if check_block_crc::<D::Error, BLOCK>(scratch, block_id).is_err() {
            return Ok(Lookup::Missing);
        }
        Ok(
            match data_lookup::<D::Error>(&scratch[..payload_end], key, val_buf, block_id)? {
                Some(DataHit::Value(n, seq)) => Lookup::Value { len: n, seq },
                Some(DataHit::Tombstone(seq)) => Lookup::Tombstone { seq },
                None => Lookup::Missing,
            },
        )
    }
}
