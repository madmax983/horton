//! Caller-owned block cache (v0.16).
//!
//! A fixed-capacity, allocation-free cache of `SSTable` block images on the
//! read path. All memory is caller-provided and compile-time sized via
//! const generics: [`BlockCache`] owns `SLOTS` block images and their
//! tags inline — no heap, no globals, no interior mutability of its own
//! (the `Db` wraps it in a `RefCell`, like `get_scratch`).
//!
//! # Key
//!
//! Entries are keyed by `(table_id, device_block_id)`. Table ids are
//! monotone and never reused within a manifest lineage, and a table's
//! device blocks are immutable once visible, so a cached entry can never
//! name live data it doesn't describe — even after the table is dropped
//! and its blocks are reclaimed. The v0.16 entry of
//! `docs/history/milestones-v0.1-v0.16.md` has the full argument.
//!
//! # Eviction: CLOCK
//!
//! One reference bit per slot, one hand index. Insertion sets the bit
//! (hot) or clears it (cold — used for scan-streamed data blocks, so a
//! scan sweeps its own blocks out behind it instead of displacing the
//! point-read hot set). On a miss the hand advances, clearing set bits,
//! and evicts the first slot it finds clear. O(1) amortized, one byte of
//! policy state per slot, no linked lists.
//!
//! # Placement: one window per block
//!
//! A cache of up to [`WINDOW`] slots is fully associative: any block can
//! take any slot, a lookup compares every tag, and the hand sweeps them
//! all. That is every firmware-sized cache.
//!
//! A larger cache gives each block a window of [`WINDOW`] consecutive
//! slots, chosen by hashing its tag. Lookups and inserts touch only
//! those slots, so their cost does not grow with the cache, and CLOCK
//! runs inside the window: an insert takes a free slot if the window
//! has one, otherwise the window's first slot whose reference bit is
//! clear, clearing the bits it passes. Windows overlap
//! (each slot is in [`WINDOW`] of them), which spreads load the way
//! linear probing does. The price is that a block can be evicted while
//! slots outside its window sit idle.
//!
//! # What is cached
//!
//! Physical block images exactly as the device returned them, and only
//! images whose CRC-32 the reader has already checked (the read funnel,
//! `sstable::read_block_verified`, inserts after the check). Bloom
//! gating, decompression, and TTL/range shadowing all run after the
//! cache on identical bytes. A block that fails its CRC is never
//! inserted: every read of it goes to the device and reports the
//! corruption again. The WAL, manifest, compaction merge reads, and
//! pre-visibility table copies bypass the cache deliberately.
//!
//! # Hits are re-verified, with a RAM checksum (ADR-0016)
//!
//! A cached image can sit in RAM for the life of the process, and
//! firmware RAM has no ECC, so a hit must not hand out bytes that
//! changed since they were verified. Every slot therefore stores
//! [`image_sum`] of the image it was given, and every hit recomputes
//! the sum over what it hands out. A mismatch drops the slot and reads
//! as a miss, so the caller goes back to the device: a flipped bit
//! costs one re-read, never a wrong answer and never an error.
//!
//! The sum replaces a second CRC-32 of the block, which is what a hit
//! paid before v0.20 (ADR-0014). It is five to six times cheaper on a
//! 64-bit host and needs no lookup tables. It is never stored on the
//! device, so it is not part of the format.
//!
//! An image is summed in [`SUM_CHUNKS`] chunks, one sum each, so a
//! reader that needs a few bytes of a block (one bloom bucket, a
//! footer's fields) asks for that range with
//! [`get_range_into`](BlockCache::get_range_into) and pays for the
//! chunks the range touches, not for the block. The rule is the same
//! at either size: a lookup is a hit only if the sum over every chunk
//! it took bytes from still matches.

use core::cell::RefCell;

/// Odd 64-bit multipliers for [`sum_lanes64`] (the xxHash64 primes).
const P64_1: u64 = 0x9E37_79B1_85EB_CA87;
const P64_2: u64 = 0xC2B2_AE3D_27D4_EB4F;
/// Odd 32-bit multipliers for [`sum_lanes32`] (the xxHash32 primes).
const P32_1: u32 = 0x9E37_79B1;
const P32_2: u32 = 0x85EB_CA77;

/// One lane step: mixes word `w` into `acc`. A bijection of `w` for a
/// fixed `acc` and of `acc` for a fixed `w` (an odd multiply, an add
/// and a rotate each permute the 64-bit values), which is what makes a
/// change confined to one word always visible in the sum.
const fn round64(acc: u64, w: u64) -> u64 {
    acc.wrapping_add(w.wrapping_mul(P64_2))
        .rotate_left(31)
        .wrapping_mul(P64_1)
}

/// The 32-bit form of [`round64`], with the same bijection property.
const fn round32(acc: u32, w: u32) -> u32 {
    acc.wrapping_add(w.wrapping_mul(P32_2))
        .rotate_left(13)
        .wrapping_mul(P32_1)
}

/// [`image_sum`] for 64-bit targets: four independent 64-bit lanes, each
/// taking every fourth 8-byte word, so the four multiplies of one
/// 32-byte step overlap in the pipeline.
fn sum_lanes64(data: &[u8]) -> u64 {
    let (steps, tail) = data.as_chunks::<32>();
    let mut a = [
        P64_1.wrapping_add(P64_2),
        P64_2,
        0,
        0u64.wrapping_sub(P64_1),
    ];
    for step in steps {
        let (words, _) = step.as_chunks::<8>();
        for (lane, word) in a.iter_mut().zip(words) {
            *lane = round64(*lane, u64::from_le_bytes(*word));
        }
    }
    // A sum of rotations: a change in exactly one lane always changes it.
    let mut h = a[0]
        .rotate_left(1)
        .wrapping_add(a[1].rotate_left(7))
        .wrapping_add(a[2].rotate_left(12))
        .wrapping_add(a[3].rotate_left(18));
    for &b in tail {
        h = (h ^ u64::from(b)).wrapping_mul(P64_1).rotate_left(11);
    }
    h ^= h >> 33;
    h = h.wrapping_mul(P64_2);
    h ^ (h >> 29)
}

/// [`image_sum`] for 32-bit targets (an ESP32's multiplier is 32 bits
/// wide): four 32-bit lanes over 4-byte words, folded into two halves
/// of the result.
fn sum_lanes32(data: &[u8]) -> u64 {
    let (steps, tail) = data.as_chunks::<16>();
    let mut a = [
        P32_1.wrapping_add(P32_2),
        P32_2,
        0,
        0u32.wrapping_sub(P32_1),
    ];
    for step in steps {
        let (words, _) = step.as_chunks::<4>();
        for (lane, word) in a.iter_mut().zip(words) {
            *lane = round32(*lane, u32::from_le_bytes(*word));
        }
    }
    // Each half is a sum of rotations of all four lanes, with different
    // rotations: a change in exactly one lane changes both halves.
    let mut lo = a[0]
        .rotate_left(1)
        .wrapping_add(a[1].rotate_left(7))
        .wrapping_add(a[2].rotate_left(12))
        .wrapping_add(a[3].rotate_left(18));
    let mut hi = a[0]
        .rotate_left(19)
        .wrapping_add(a[1].rotate_left(3))
        .wrapping_add(a[2].rotate_left(26))
        .wrapping_add(a[3].rotate_left(11));
    for &b in tail {
        lo = (lo ^ u32::from(b)).wrapping_mul(P32_1).rotate_left(11);
        hi = (hi ^ u32::from(b)).wrapping_mul(P32_2).rotate_left(15);
    }
    (u64::from(hi) << 32) | u64::from(lo)
}

/// The RAM checksum a cache slot keeps of its block image (ADR-0016).
///
/// Guarantees, for two images of the same length:
///
/// - **A change confined to one lane word is always detected**: every
///   single-bit flip, and every burst inside one 8-byte word (one
///   4-byte word on 32-bit targets). Each lane step is a bijection of
///   its word, so the lane ends different, and the final combination
///   keeps a difference in exactly one lane.
/// - Any other change goes undetected with probability about 2⁻⁶⁴:
///   the multiplies and rotates are not linear, so unlike a plain sum
///   or XOR there is no simple pattern of flips that cancels.
///
/// It is not a CRC and is never written to a device: the value differs
/// between 32- and 64-bit targets, and may change between versions.
#[must_use]
pub fn image_sum(data: &[u8]) -> u64 {
    if cfg!(target_pointer_width = "64") {
        sum_lanes64(data)
    } else {
        sum_lanes32(data)
    }
}

/// How many chunks a cached image is summed in. A ranged read verifies
/// only the chunks it touches, so this sets its floor: one sixteenth of
/// a block.
pub const SUM_CHUNKS: usize = 16;

// `BlockCache::with_checked` marks the chunks it has summed in a `u32`.
const _: () = assert!(SUM_CHUNKS <= u32::BITS as usize);

/// The bytes of chunk `c` in a `block`-byte image: sixteen equal chunks,
/// the last one taking the remainder.
const fn chunk_bounds(block: usize, c: usize) -> (usize, usize) {
    let len = block / SUM_CHUNKS;
    let end = if c + 1 >= SUM_CHUNKS {
        block
    } else {
        (c + 1) * len
    };
    (c * len, end)
}

/// The chunk that byte `at` of a `block`-byte image falls in.
const fn chunk_of(block: usize, at: usize) -> usize {
    let len = block / SUM_CHUNKS;
    if len == 0 || at / len >= SUM_CHUNKS {
        SUM_CHUNKS - 1
    } else {
        at / len
    }
}

/// [`image_sum`] of every chunk of `image`.
fn chunk_sums<const BLOCK: usize>(image: &[u8; BLOCK]) -> [u64; SUM_CHUNKS] {
    let mut sums = [0u64; SUM_CHUNKS];
    for (c, sum) in sums.iter_mut().enumerate() {
        let (from, to) = chunk_bounds(BLOCK, c);
        *sum = image_sum(&image[from..to]);
    }
    sums
}

/// Slots a block may occupy in a cache larger than this (see the module
/// docs); a cache of at most this many slots is fully associative.
pub const WINDOW: usize = 8;

/// A slot's tag and CLOCK state. Kept apart from the sums and the images
/// so a lookup compares a few small tags, not blocks.
#[derive(Clone, Copy)]
struct Tag {
    /// Which table's which device block.
    table_id: u32,
    block_id: u64,
    /// Slot in use.
    occupied: bool,
    /// CLOCK second-chance bit.
    refbit: bool,
}

impl Tag {
    const EMPTY: Self = Self {
        table_id: 0,
        block_id: 0,
        occupied: false,
        refbit: false,
    };

    const fn names(&self, table_id: u32, block_id: u64) -> bool {
        self.occupied && self.table_id == table_id && self.block_id == block_id
    }
}

/// Cache counters. `Copy`: take a copy at any time.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CacheStats {
    /// Block reads served from the cache.
    pub hits: u64,
    /// Block reads that went to the device. (`RefCell` contention
    /// bypasses in the [`CachePort`] impl are silent and uncounted —
    /// they return `false` without reaching the counters — so a miss
    /// here always means the device did the work.)
    pub misses: u64,
    /// Hits that were turned into misses because the cached image no
    /// longer matched its RAM checksum (a bit flipped in the cache's
    /// memory). The slot was dropped and the block re-read from the
    /// device. Counted in `misses` too.
    pub rejected: u64,
    /// Slots currently holding a block.
    pub len: usize,
    /// Total slots (`SLOTS`).
    pub capacity: usize,
}

/// Fixed-capacity block cache over caller-owned memory.
///
/// `SLOTS = 0` disables the cache: every method is a no-op and every
/// lookup misses. The `Db` exposes this as `CACHE = 0`.
pub struct BlockCache<const BLOCK: usize, const SLOTS: usize> {
    tags: [Tag; SLOTS],
    /// [`image_sum`] of each chunk of slot `i`'s image, as it was
    /// inserted.
    sums: [[u64; SUM_CHUNKS]; SLOTS],
    /// Physical block images, byte-identical to the device's; slot `i`
    /// belongs to `tags[i]`.
    images: [[u8; BLOCK]; SLOTS],
    /// CLOCK hand: next eviction candidate. Used only by a fully
    /// associative cache (`SLOTS <= WINDOW`).
    hand: usize,
    hits: u64,
    misses: u64,
    rejected: u64,
}

impl<const BLOCK: usize, const SLOTS: usize> BlockCache<BLOCK, SLOTS> {
    /// Empty cache. `const` so it can live in a `const`-constructed `Db`.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            tags: [Tag::EMPTY; SLOTS],
            sums: [[0; SUM_CHUNKS]; SLOTS],
            images: [[0u8; BLOCK]; SLOTS],
            hand: 0,
            hits: 0,
            misses: 0,
            rejected: 0,
        }
    }

    /// `false` when `SLOTS == 0`: the cache is compiled out, not just
    /// empty. Every method checks this first so `hand % SLOTS` can never
    /// divide by zero.
    const fn enabled() -> bool {
        SLOTS > 0
    }

    /// The slots `(table_id, block_id)` may occupy: `(first, count)`,
    /// wrapping past the last slot. Every slot for a fully associative
    /// cache; otherwise a hashed window. Only called when enabled.
    const fn window(table_id: u32, block_id: u64) -> (usize, usize) {
        if SLOTS <= WINDOW {
            return (0, SLOTS);
        }
        // A table's blocks have consecutive ids: the multiply scatters
        // them, and the high bits are the well-mixed ones.
        let h = (block_id ^ ((table_id as u64) << 40)).wrapping_mul(P64_1) >> 32;
        // `h` is below 2^32 and the remainder below `SLOTS`: both casts
        // are exact.
        #[allow(clippy::cast_possible_truncation)]
        let first = (h % SLOTS as u64) as usize;
        (first, WINDOW)
    }

    /// The `i`th slot of the window starting at `first`.
    const fn slot(first: usize, i: usize) -> usize {
        let at = first + i;
        if at >= SLOTS { at - SLOTS } else { at }
    }

    /// The slot holding `(table_id, block_id)`, without counting.
    fn locate(&self, table_id: u32, block_id: u64) -> Option<usize> {
        if !Self::enabled() {
            return None;
        }
        let (first, count) = Self::window(table_id, block_id);
        (0..count)
            .map(|i| Self::slot(first, i))
            .find(|&at| self.tags[at].names(table_id, block_id))
    }

    /// Looks `(table_id, block_id)` up, copying the image into `out` on a
    /// hit and setting the slot's reference bit. Returns `false` on a
    /// miss.
    ///
    /// A hit is checked: the bytes copied into `out` must still have the
    /// [`image_sum`]s recorded when the image was inserted. If they do
    /// not, the slot is dropped, the lookup counts as a miss (and in
    /// [`CacheStats::rejected`]), and `out` holds bytes the caller must
    /// not use; it reads the block from the device as on any miss.
    pub fn get_into(&mut self, table_id: u32, block_id: u64, out: &mut [u8; BLOCK]) -> bool {
        let Some(i) = self.find(table_id, block_id) else {
            return false;
        };
        out.copy_from_slice(&self.images[i]);
        // Sum the copy, not the slot: the copy is what the caller parses.
        let ok = chunk_sums(out) == self.sums[i];
        self.settle(i, ok)
    }

    /// Looks `(table_id, block_id)` up and copies `out.len()` bytes of
    /// its image, starting at byte `offset`, into `out`. Returns `false`
    /// on a miss, and when the range does not lie inside a block.
    ///
    /// Only the chunks the range touches are checked against their
    /// sums, so a small range of a cached block costs a small part of
    /// what [`get_into`](Self::get_into) does. A chunk that fails drops
    /// the slot, exactly as there. The bytes outside the range are not
    /// looked at; whatever state they are in, they are not handed out.
    pub fn get_range_into(
        &mut self,
        table_id: u32,
        block_id: u64,
        offset: usize,
        out: &mut [u8],
    ) -> bool {
        let Some(end) = offset.checked_add(out.len()).filter(|&end| end <= BLOCK) else {
            return false;
        };
        let Some(i) = self.find(table_id, block_id) else {
            return false;
        };
        let image = &self.images[i];
        out.copy_from_slice(&image[offset..end]);
        // The chunks are summed where they lie: copying each one out to
        // sum it would cost what the range read exists to save.
        let mut ok = true;
        if end > offset {
            for c in chunk_of(BLOCK, offset)..=chunk_of(BLOCK, end - 1) {
                let (from, to) = chunk_bounds(BLOCK, c);
                ok &= image_sum(&image[from..to]) == self.sums[i][c];
            }
        }
        self.settle(i, ok)
    }

    /// Looks `(table_id, block_id)` up and hands bytes `from..to` of its
    /// image to `read` where they lie, with no copy. Returns `false`,
    /// without calling `read`, on a miss and when the range does not lie
    /// inside a block.
    ///
    /// The chunks the range touches are checked against their sums
    /// first, as [`get_range_into`](Self::get_range_into) checks them; a
    /// chunk that fails drops the slot and `read` is not called. A
    /// reader that parses a block once (an index search, an inflate)
    /// saves the copy it would otherwise parse.
    pub fn with_range(
        &mut self,
        table_id: u32,
        block_id: u64,
        from: usize,
        to: usize,
        read: &mut dyn FnMut(&[u8]),
    ) -> bool {
        if from > to || to > BLOCK {
            return false;
        }
        let Some(i) = self.find(table_id, block_id) else {
            return false;
        };
        let image = &self.images[i];
        let mut ok = true;
        if to > from {
            for c in chunk_of(BLOCK, from)..=chunk_of(BLOCK, to - 1) {
                let (lo, hi) = chunk_bounds(BLOCK, c);
                ok &= image_sum(&image[lo..hi]) == self.sums[i][c];
            }
        }
        if ok {
            read(&image[from..to]);
        }
        self.settle(i, ok)
    }

    /// Looks `(table_id, block_id)` up and hands its whole image to
    /// `read` where it lies, unchecked, with a `check(from, to)` that
    /// sums the chunks overlapping `from..to` (each at most once) and
    /// says whether they still match. `read` calls `check` on the bytes
    /// it is about to act on, and only on those: a point lookup in a raw
    /// block sums the chunk of its trailer, one per restart its binary
    /// search parses and those of the run it scans, not every chunk
    /// (ADR-0031).
    ///
    /// Returns `true` on a hit whose every check held. A miss, or a
    /// check that failed, returns `false`: the slot is dropped as
    /// [`with_range`](Self::with_range) drops it, and whatever `read`
    /// made of the image must be discarded. An empty range checks
    /// nothing and holds; a range past the image fails, and once one
    /// check has failed every later one does. `read` must not call back
    /// into the cache, and `check` is valid only while `read` runs.
    pub fn with_checked(
        &mut self,
        table_id: u32,
        block_id: u64,
        read: &mut CheckedRead<'_>,
    ) -> bool {
        let Some(i) = self.find(table_id, block_id) else {
            return false;
        };
        let image = &self.images[i];
        let sums = &self.sums[i];
        let mut ok = true;
        let mut done = 0u32;
        read(image, &mut |from, to| {
            if to <= from {
                return ok;
            }
            if to > BLOCK {
                ok = false;
            }
            if !ok {
                return false;
            }
            let (first, last) = (chunk_of(BLOCK, from), chunk_of(BLOCK, to - 1));
            for (c, sum) in sums.iter().enumerate().take(last + 1).skip(first) {
                if done & (1 << c) == 0 {
                    let (lo, hi) = chunk_bounds(BLOCK, c);
                    if image_sum(&image[lo..hi]) != *sum {
                        ok = false;
                        return false;
                    }
                    done |= 1 << c;
                }
            }
            true
        });
        self.settle(i, ok)
    }

    /// The slot holding `(table_id, block_id)`. A lookup that finds none
    /// is counted as a miss here.
    fn find(&mut self, table_id: u32, block_id: u64) -> Option<usize> {
        let found = self.locate(table_id, block_id);
        if found.is_none() {
            self.misses = self.misses.saturating_add(1);
        }
        found
    }

    /// Ends a lookup that found slot `i`: a hit when the sums matched,
    /// otherwise the slot is dropped and the lookup is a (rejected) miss.
    const fn settle(&mut self, i: usize, sums_matched: bool) -> bool {
        if sums_matched {
            self.tags[i].refbit = true;
            self.hits = self.hits.saturating_add(1);
        } else {
            self.tags[i] = Tag::EMPTY;
            self.rejected = self.rejected.saturating_add(1);
            self.misses = self.misses.saturating_add(1);
        }
        sums_matched
    }

    /// Inserts (or replaces) the image for `(table_id, block_id)`.
    /// `hot` sets the CLOCK reference bit: point-read blocks (`true`)
    /// defend their slot; scan-streamed data blocks (`false`) are
    /// evicted first, so a scan doesn't displace the hot set.
    ///
    /// The cache hands back exactly these bytes or nothing, so insert
    /// only an image that has been verified: horton's read path inserts
    /// after the block's CRC-32 has matched.
    pub fn put(&mut self, table_id: u32, block_id: u64, block: &[u8; BLOCK], hot: bool) {
        if !Self::enabled() {
            return;
        }
        // Replace in place when the tag already lives here: the bytes are
        // immutable per tag, so this only refreshes the reference bit.
        if let Some(at) = self.locate(table_id, block_id) {
            self.tags[at].refbit |= hot;
            return;
        }
        let at = self.victim(table_id, block_id);
        self.tags[at] = Tag {
            table_id,
            block_id,
            occupied: true,
            refbit: hot,
        };
        // The sums are of the caller's verified bytes, not of the slot's
        // copy: a copy that went wrong fails its first hit.
        self.sums[at] = chunk_sums(block);
        self.images[at].copy_from_slice(block);
    }

    /// CLOCK: picks the slot a new `(table_id, block_id)` takes. Every
    /// set reference bit passed gets a second chance (it is cleared);
    /// the first clear slot is the victim. A free slot reads as clear
    /// (`occupied == false` implies `refbit == false` by construction).
    fn victim(&mut self, table_id: u32, block_id: u64) -> usize {
        if SLOTS <= WINDOW {
            // Fully associative: one hand sweeps every slot, and stays
            // where it stopped.
            loop {
                let hand = self.hand;
                self.hand = if hand + 1 >= SLOTS { 0 } else { hand + 1 };
                if !self.tags[hand].refbit {
                    return hand;
                }
                self.tags[hand].refbit = false;
            }
        }
        // Windowed: a free slot in the window goes first, so nothing is
        // evicted while the window has room. Otherwise the sweep starts
        // at the window's first slot; if every slot had its bit set they
        // are all clear now, and the first is as good as any.
        let (first, count) = Self::window(table_id, block_id);
        if let Some(free) = (0..count)
            .map(|i| Self::slot(first, i))
            .find(|&at| !self.tags[at].occupied)
        {
            return free;
        }
        for i in 0..count {
            let at = Self::slot(first, i);
            if !self.tags[at].refbit {
                return at;
            }
            self.tags[at].refbit = false;
        }
        first
    }

    /// Drops every entry tagged with `table_id`. Called when compaction
    /// retires a table; hygiene, not correctness (ids never repeat, so
    /// stale entries are unreachable — this just frees their slots).
    pub fn invalidate_table(&mut self, table_id: u32) {
        if !Self::enabled() {
            return;
        }
        for t in &mut self.tags {
            if t.occupied && t.table_id == table_id {
                *t = Tag::EMPTY;
            }
        }
    }

    /// Current counters.
    #[must_use]
    pub fn stats(&self) -> CacheStats {
        let len = self.tags.iter().filter(|t| t.occupied).count();
        CacheStats {
            hits: self.hits,
            misses: self.misses,
            rejected: self.rejected,
            len,
            capacity: SLOTS,
        }
    }

    /// Test hook: XORs `mask` into byte `at` of the cached image of
    /// `(table_id, block_id)`, as a fault in RAM would, and leaves its
    /// sums as they were. `false` when the block is not cached. Counts
    /// nothing.
    #[cfg(test)]
    pub(crate) fn damage(&mut self, table_id: u32, block_id: u64, at: usize, mask: u8) -> bool {
        match self.locate(table_id, block_id) {
            Some(i) => {
                self.images[i][at] ^= mask;
                true
            }
            None => false,
        }
    }
}

impl<const BLOCK: usize, const SLOTS: usize> Default for BlockCache<BLOCK, SLOTS> {
    fn default() -> Self {
        Self::new()
    }
}

/// What [`CachePort::with_checked`] hands a cached image to: the image,
/// and a `check(from, to)` that says whether its bytes `from..to` still
/// match their RAM checksums.
pub type CheckedRead<'a> = dyn FnMut(&[u8], &mut dyn FnMut(usize, usize) -> bool) + 'a;

/// The read path's view of a cache: synchronous block-image operations
/// behind `&self`, so `TableReader` and the scans can hold it without
/// another const parameter.
///
/// Implemented for `RefCell<BlockCache<..>>` — the `Db`'s field type.
/// `RefCell` contention (two interleaved `get`s on one executor)
/// degrades to a silent bypass via `try_borrow_mut`: a miss, never a
/// panic, never a wrong byte.
pub trait CachePort<const BLOCK: usize> {
    /// Copy the cached image into `out`; `false` on miss, and on a hit
    /// whose image fails its RAM checksum (see
    /// [`BlockCache::get_into`]).
    fn get_into(&self, table_id: u32, block_id: u64, out: &mut [u8; BLOCK]) -> bool;
    /// Copy `out.len()` bytes of the cached image, from byte `offset`,
    /// into `out`; `false` on a miss (see
    /// [`BlockCache::get_range_into`]).
    fn get_range_into(&self, table_id: u32, block_id: u64, offset: usize, out: &mut [u8]) -> bool;
    /// Hand bytes `from..to` of the cached image, checked against its
    /// RAM checksum, to `read` in place, with no copy (see
    /// [`BlockCache::with_range`]). `Some(true)` on a hit, after `read`
    /// has run; `Some(false)` on a miss, and `read` has not run.
    ///
    /// `None` means the cache does not lend images, and the caller
    /// copies one out with [`get_into`](Self::get_into) or
    /// [`get_range_into`](Self::get_range_into) instead. That is what
    /// this provided method answers, so a cache written before it
    /// existed keeps working unchanged. `read` must not call back into
    /// the cache.
    fn with_range(
        &self,
        table_id: u32,
        block_id: u64,
        from: usize,
        to: usize,
        read: &mut dyn FnMut(&[u8]),
    ) -> Option<bool> {
        let _ = (table_id, block_id, from, to, read);
        None
    }
    /// Hand the whole cached image to `read` in place, unchecked, with a
    /// `check(from, to)` that `read` calls on the bytes it is about to
    /// act on (see [`BlockCache::with_checked`]). `Some(true)` on a hit
    /// whose every check held; `Some(false)` on a miss, and on a check
    /// that failed, in which case the entry is dropped and whatever
    /// `read` made of the image must be discarded. `read` must not call
    /// back into the cache, and `check` is valid only while `read` runs.
    ///
    /// For a cache that lends whole images but cannot check them a
    /// range at a time, this provided method checks the whole image up
    /// front ([`with_range`](Self::with_range)) and hands `read` a
    /// `check` that holds for every range inside the image (and fails
    /// one past it). `None`: the cache does not lend.
    fn with_checked(
        &self,
        table_id: u32,
        block_id: u64,
        read: &mut CheckedRead<'_>,
    ) -> Option<bool> {
        self.with_range(table_id, block_id, 0, BLOCK, &mut |image| {
            read(image, &mut |from, to| to <= from || to <= BLOCK);
        })
    }
    /// Insert a verified image; `hot` sets the CLOCK reference bit.
    fn put(&self, table_id: u32, block_id: u64, block: &[u8; BLOCK], hot: bool);
    /// Drop every entry for `table_id`.
    fn invalidate_table(&self, table_id: u32);
    /// Current counters.
    fn stats(&self) -> CacheStats;
}

impl<const BLOCK: usize, const SLOTS: usize> CachePort<BLOCK>
    for RefCell<BlockCache<BLOCK, SLOTS>>
{
    fn get_into(&self, table_id: u32, block_id: u64, out: &mut [u8; BLOCK]) -> bool {
        self.try_borrow_mut()
            .is_ok_and(|mut c| c.get_into(table_id, block_id, out))
    }

    fn get_range_into(&self, table_id: u32, block_id: u64, offset: usize, out: &mut [u8]) -> bool {
        self.try_borrow_mut()
            .is_ok_and(|mut c| c.get_range_into(table_id, block_id, offset, out))
    }

    fn with_range(
        &self,
        table_id: u32,
        block_id: u64,
        from: usize,
        to: usize,
        read: &mut dyn FnMut(&[u8]),
    ) -> Option<bool> {
        Some(
            self.try_borrow_mut()
                .is_ok_and(|mut c| c.with_range(table_id, block_id, from, to, read)),
        )
    }

    fn with_checked(
        &self,
        table_id: u32,
        block_id: u64,
        read: &mut CheckedRead<'_>,
    ) -> Option<bool> {
        Some(
            self.try_borrow_mut()
                .is_ok_and(|mut c| c.with_checked(table_id, block_id, read)),
        )
    }

    fn put(&self, table_id: u32, block_id: u64, block: &[u8; BLOCK], hot: bool) {
        if let Ok(mut c) = self.try_borrow_mut() {
            c.put(table_id, block_id, block, hot);
        }
    }

    fn invalidate_table(&self, table_id: u32) {
        if let Ok(mut c) = self.try_borrow_mut() {
            c.invalidate_table(table_id);
        }
    }

    fn stats(&self) -> CacheStats {
        self.try_borrow().map_or_else(
            |_| CacheStats {
                capacity: SLOTS,
                ..CacheStats::default()
            },
            |c| c.stats(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BlockCache, SUM_CHUNKS, WINDOW, chunk_bounds, chunk_of, image_sum, sum_lanes32, sum_lanes64,
    };

    /// A deterministic, non-repeating block.
    fn block<const N: usize>(seed: u64) -> [u8; N] {
        let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        let mut out = [0u8; N];
        for b in &mut out {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            *b = x.to_le_bytes()[3];
        }
        out
    }

    /// The guarantee ADR-0016 rests on: no single-bit flip, anywhere in a
    /// block, leaves either sum unchanged. Exhaustive over every bit of
    /// several blocks, including lengths with a tail past the last lane
    /// step.
    #[test]
    fn every_single_bit_flip_changes_both_sums() {
        for (seed, len) in [(1u64, 4096usize), (2, 4096), (3, 1027), (4, 33), (5, 7)] {
            let mut data = block::<4096>(seed);
            let data = &mut data[..len];
            let (s64, s32) = (sum_lanes64(data), sum_lanes32(data));
            for bit in 0..len * 8 {
                data[bit / 8] ^= 1 << (bit % 8);
                assert_ne!(
                    sum_lanes64(data),
                    s64,
                    "64-bit lanes, bit {bit} of {len} bytes"
                );
                assert_ne!(
                    sum_lanes32(data),
                    s32,
                    "32-bit lanes, bit {bit} of {len} bytes"
                );
                data[bit / 8] ^= 1 << (bit % 8);
            }
            assert_eq!(sum_lanes64(data), s64);
        }
    }

    /// A change confined to one lane word is always detected, whatever
    /// the change: every value of one byte, and a sample of whole-word
    /// replacements.
    #[test]
    fn a_change_inside_one_word_always_changes_both_sums() {
        let mut data = block::<512>(9);
        let (s64, s32) = (sum_lanes64(&data), sum_lanes32(&data));
        for at in [0usize, 5, 8, 31, 32, 250, 511] {
            let was = data[at];
            for v in 0..=255u8 {
                if v != was {
                    data[at] = v;
                    assert_ne!(sum_lanes64(&data), s64, "byte {at} = {v}");
                    assert_ne!(sum_lanes32(&data), s32, "byte {at} = {v}");
                }
            }
            data[at] = was;
        }
        // Whole 8-byte words (one 64-bit lane word, two 32-bit ones in
        // different lanes: still detected here, though not guaranteed).
        for (word, delta) in [(0usize, 1u64), (3, u64::MAX), (17, 0x8000_0000_0000_0000)] {
            let at = word * 8;
            let old: [u8; 8] = data[at..at + 8].try_into().unwrap();
            let new = u64::from_le_bytes(old).wrapping_add(delta);
            data[at..at + 8].copy_from_slice(&new.to_le_bytes());
            assert_ne!(sum_lanes64(&data), s64);
            data[at..at + 8].copy_from_slice(&old);
        }
    }

    /// The patterns a plain sum or XOR cannot see: equal and opposite
    /// changes in two words, and two words swapped.
    #[test]
    fn cancelling_patterns_do_not_cancel() {
        let mut data = block::<4096>(11);
        let (s64, s32) = (sum_lanes64(&data), sum_lanes32(&data));
        // The same bit flipped in two words, every pair of the first
        // sixteen words, same lane or not.
        for i in 0..16usize {
            for j in i + 1..16 {
                data[i * 8] ^= 0x10;
                data[j * 8] ^= 0x10;
                assert_ne!(sum_lanes64(&data), s64, "words {i} and {j}");
                assert_ne!(sum_lanes32(&data), s32, "words {i} and {j}");
                data[i * 8] ^= 0x10;
                data[j * 8] ^= 0x10;
            }
        }
        // Two words of the same lane swapped.
        let (a, b) = (64usize, 64 + 32);
        for k in 0..8 {
            data.swap(a + k, b + k);
        }
        assert_ne!(sum_lanes64(&data), s64);
        assert_ne!(sum_lanes32(&data), s32);
    }

    #[test]
    fn sums_depend_on_length_and_are_stable_for_equal_bytes() {
        let data = block::<4096>(21);
        assert_eq!(image_sum(&data), image_sum(&data.clone()));
        assert_ne!(sum_lanes64(&data[..4095]), sum_lanes64(&data));
        assert_ne!(sum_lanes32(&data[..4095]), sum_lanes32(&data));
        // An empty image still sums (no panic, no division).
        let _ = (sum_lanes64(&[]), sum_lanes32(&[]));
    }

    /// The reason the sum exists: a bit that flips in a cached image is
    /// never handed out. The hit turns into a miss, the slot is freed,
    /// and the next insert of the good bytes is served again.
    #[test]
    fn a_flipped_bit_in_a_cached_image_is_a_miss_never_a_hit() {
        let good = block::<4096>(31);
        for bit in [0usize, 7, 4096 * 4 + 3, 4096 * 8 - 1] {
            let mut c = BlockCache::<4096, 2>::new();
            c.put(7, 100, &good, true);
            c.put(7, 101, &block::<4096>(32), true);
            let mut out = [0u8; 4096];
            assert!(c.get_into(7, 100, &mut out));
            assert_eq!(out, good);

            // The RAM under slot 0 loses a bit.
            c.images[0][bit / 8] ^= 1 << (bit % 8);

            assert!(
                !c.get_into(7, 100, &mut out),
                "bit {bit}: served a damaged image"
            );
            let s = c.stats();
            assert_eq!((s.hits, s.misses, s.rejected, s.len), (1, 1, 1, 1));
            // The slot is gone, not stuck: the next lookup is a plain
            // miss, and the neighbour is untouched.
            assert!(!c.get_into(7, 100, &mut out));
            assert_eq!(c.stats().rejected, 1);
            assert!(c.get_into(7, 101, &mut out));
            // The caller re-reads the device and inserts the good bytes.
            c.put(7, 100, &good, true);
            assert!(c.get_into(7, 100, &mut out));
            assert_eq!(out, good);
        }
    }

    /// A flip in the tag's stored sum is as safe as one in the image.
    #[test]
    fn a_flipped_bit_in_the_stored_sum_is_a_miss() {
        let good = block::<4096>(41);
        let mut c = BlockCache::<4096, 1>::new();
        c.put(1, 1, &good, true);
        c.sums[0][3] ^= 1 << 40;
        let mut out = [0u8; 4096];
        assert!(!c.get_into(1, 1, &mut out));
        assert_eq!(c.stats().rejected, 1);
    }

    /// Chunks tile the block exactly, for block sizes that divide by
    /// sixteen and ones that do not, and `chunk_of` agrees with them.
    #[test]
    fn chunks_tile_the_block() {
        for block in [1usize, 15, 16, 17, 512, 1000, 4096, 16384, 65536] {
            let mut next = 0;
            for c in 0..SUM_CHUNKS {
                let (from, to) = chunk_bounds(block, c);
                assert_eq!(from, next, "block {block} chunk {c}");
                assert!(to >= from && to <= block);
                for at in from..to {
                    assert_eq!(chunk_of(block, at), c, "block {block} byte {at}");
                }
                next = to;
            }
            assert_eq!(next, block, "block {block}: chunks stop short");
        }
    }

    /// A ranged read returns exactly the bytes of the range.
    #[test]
    fn range_reads_return_the_bytes_of_the_range() {
        let good = block::<4096>(51);
        let mut c = BlockCache::<4096, 2>::new();
        c.put(3, 9, &good, true);
        for (offset, len) in [
            (0usize, 45usize),
            (0, 4096),
            (4032, 64),
            (250, 12),
            (255, 2),
            (4095, 1),
            (77, 0),
        ] {
            let mut buf = [0u8; 4096];
            let out = &mut buf[..len];
            assert!(c.get_range_into(3, 9, offset, out), "{offset}+{len}");
            assert_eq!(out, &good[offset..offset + len], "{offset}+{len}");
        }
        assert_eq!(c.stats().hits, 7);
        // Out of the block, or of another block: a miss, never a panic.
        let mut out = [0u8; 64];
        assert!(!c.get_range_into(3, 9, 4090, &mut out));
        assert!(!c.get_range_into(3, 9, usize::MAX, &mut out));
        assert!(!c.get_range_into(3, 10, 0, &mut out));
        assert_eq!(
            c.stats().misses,
            1,
            "only the absent block counts as a miss"
        );
    }

    /// The ranged form of the rule: a flipped bit is never handed out.
    /// A range over the damaged chunk misses and drops the slot; a range
    /// that does not touch it is still served, because none of the
    /// damaged bytes leave the cache.
    #[test]
    fn a_flipped_bit_is_never_served_by_a_range_read() {
        let good = block::<4096>(61);
        // 4096 / 16 = 256-byte chunks. Flip a bit in chunk 5.
        let flipped = 5 * 256 + 17;
        for (offset, len, touches) in [
            (flipped, 1usize, true),
            (5 * 256, 256, true),
            (5 * 256 - 1, 2, true),
            (6 * 256 - 1, 64, true),
            (0, 4096, true),
            (0, 45, false),
            (4 * 256, 256, false),
            (6 * 256, 64, false),
        ] {
            let mut c = BlockCache::<4096, 1>::new();
            c.put(1, 1, &good, true);
            c.images[0][flipped] ^= 0x04;
            let mut buf = [0u8; 4096];
            let out = &mut buf[..len];
            let served = c.get_range_into(1, 1, offset, out);
            assert_eq!(served, !touches, "range {offset}+{len}");
            if served {
                assert_eq!(out, &good[offset..offset + len]);
                assert_eq!(c.stats().rejected, 0);
            } else {
                let s = c.stats();
                assert_eq!((s.rejected, s.misses, s.len), (1, 1, 0));
            }
        }
    }

    /// A lent range is exactly the bytes of the range, `read` runs once
    /// per hit and never on a miss, and a range outside the block is a
    /// miss, never a panic.
    #[test]
    fn a_lent_range_is_the_bytes_of_the_range() {
        let good = block::<4096>(71);
        let mut c = BlockCache::<4096, 2>::new();
        c.put(4, 2, &good, true);
        for (from, to) in [
            (0usize, 4096usize),
            (0, 45),
            (4032, 4096),
            (255, 257),
            (9, 9),
        ] {
            let mut seen = None;
            assert!(c.with_range(4, 2, from, to, &mut |b| seen = Some(b.to_vec())));
            assert_eq!(seen.as_deref(), Some(&good[from..to]), "{from}..{to}");
        }
        assert_eq!(c.stats().hits, 5);
        let mut calls = 0;
        assert!(!c.with_range(4, 3, 0, 64, &mut |_| calls += 1));
        assert!(!c.with_range(4, 2, 4090, 4097, &mut |_| calls += 1));
        assert!(!c.with_range(4, 2, 10, 9, &mut |_| calls += 1));
        assert_eq!(calls, 0);
        assert_eq!(c.stats().misses, 1, "only the absent block counts");
    }

    /// The lent form of ADR-0016's rule: a range over a flipped bit is
    /// never lent; the slot is dropped and the lookup is a miss. A range
    /// clear of the damaged chunk is still lent.
    #[test]
    fn a_flipped_bit_is_never_lent() {
        let good = block::<4096>(81);
        let flipped = 9 * 256 + 3;
        for (from, to, touches) in [
            (0usize, 4096usize, true),
            (9 * 256, 10 * 256, true),
            (flipped, flipped + 1, true),
            (0, 9 * 256, false),
            (10 * 256, 4096, false),
        ] {
            let mut c = BlockCache::<4096, 1>::new();
            c.put(1, 1, &good, true);
            c.images[0][flipped] ^= 0x80;
            let mut lent = None;
            let served = c.with_range(1, 1, from, to, &mut |b| lent = Some(b.to_vec()));
            assert_eq!(served, !touches, "{from}..{to}");
            if served {
                assert_eq!(lent.as_deref(), Some(&good[from..to]));
            } else {
                assert!(lent.is_none(), "{from}..{to}: damaged bytes lent");
                let s = c.stats();
                assert_eq!((s.rejected, s.misses, s.len), (1, 1, 0));
            }
        }
    }

    /// `with_checked`: a check over a flipped bit fails, the slot is
    /// dropped and the lookup is a miss; checks clear of it hold and the
    /// lookup is a hit. Each chunk is summed once however often it is
    /// asked about.
    #[test]
    fn a_checked_lend_fails_only_where_it_looks() {
        let good = block::<4096>(83);
        let flipped = 9 * 256 + 3;
        for (ranges, touches) in [
            (&[(0usize, 16usize), (4000, 4096)][..], false),
            (&[(0, 9 * 256), (10 * 256, 4096)][..], false),
            (&[(0, 16), (flipped, flipped + 1)][..], true),
            (&[(8 * 256, 11 * 256)][..], true),
            (&[(5, 5), (4096, 4097)][..], true),
        ] {
            let mut c = BlockCache::<4096, 1>::new();
            c.put(1, 1, &good, true);
            c.images[0][flipped] ^= 0x80;
            let mut all_held = true;
            let served = c.with_checked(1, 1, &mut |image, check| {
                assert_eq!(image.len(), 4096);
                for &(from, to) in ranges {
                    all_held &= check(from, to);
                }
            });
            assert_eq!(served, !touches, "{ranges:?}");
            assert_eq!(all_held, !touches, "{ranges:?}");
            let s = c.stats();
            if touches {
                assert_eq!((s.rejected, s.misses, s.len), (1, 1, 0));
            } else {
                assert_eq!((s.hits, s.len), (1, 1));
            }
        }
        let mut c = BlockCache::<4096, 1>::new();
        let mut calls = 0;
        assert!(!c.with_checked(1, 1, &mut |_, _| calls += 1), "a miss");
        assert_eq!(calls, 0);
    }

    /// Through the trait: a `RefCell` cache lends (never `None`), and a
    /// cache that is already borrowed answers a miss, not a panic.
    #[test]
    fn the_refcell_port_lends_and_a_busy_one_misses() {
        use super::CachePort;
        let good = block::<4096>(91);
        let c = core::cell::RefCell::new(BlockCache::<4096, 1>::new());
        CachePort::put(&c, 7, 7, &good, true);
        let mut n = 0;
        assert_eq!(c.with_range(7, 7, 0, 16, &mut |b| n = b.len()), Some(true));
        assert_eq!(n, 16);
        let _busy = c.borrow_mut();
        assert_eq!(c.with_range(7, 7, 0, 16, &mut |_| n = 0), Some(false));
        assert_eq!(n, 16);
    }

    /// The provided `with_checked`, for a cache that lends only whole
    /// images: the image is checked whole before `read` runs, so a flip
    /// anywhere is a miss and `read` never sees it; on a hit, `check`
    /// holds inside the image and fails past it.
    #[test]
    fn the_provided_checked_lend_checks_the_whole_image_first() {
        use super::{CachePort, CacheStats};
        use core::cell::RefCell;

        /// Lends through `with_range` only.
        struct WholeOnly(RefCell<BlockCache<4096, 1>>);
        impl CachePort<4096> for WholeOnly {
            fn get_into(&self, t: u32, b: u64, out: &mut [u8; 4096]) -> bool {
                self.0.get_into(t, b, out)
            }
            fn get_range_into(&self, t: u32, b: u64, at: usize, out: &mut [u8]) -> bool {
                self.0.get_range_into(t, b, at, out)
            }
            fn with_range(
                &self,
                t: u32,
                b: u64,
                from: usize,
                to: usize,
                read: &mut dyn FnMut(&[u8]),
            ) -> Option<bool> {
                self.0.with_range(t, b, from, to, read)
            }
            fn put(&self, t: u32, b: u64, block: &[u8; 4096], hot: bool) {
                CachePort::put(&self.0, t, b, block, hot);
            }
            fn invalidate_table(&self, t: u32) {
                CachePort::invalidate_table(&self.0, t);
            }
            fn stats(&self) -> CacheStats {
                CachePort::stats(&self.0)
            }
        }

        let good = block::<4096>(97);
        let c = WholeOnly(RefCell::new(BlockCache::new()));
        c.put(3, 3, &good, true);
        let mut answers = [false; 4];
        let hit = c.with_checked(3, 3, &mut |image, check| {
            assert_eq!(image, &good[..]);
            answers = [
                check(0, 16),
                check(4000, 4096),
                check(9, 9),
                check(4090, 4097),
            ];
        });
        assert_eq!(hit, Some(true));
        assert_eq!(answers, [true, true, true, false]);

        // A flip far from any range `read` would check is still a miss.
        assert!(c.0.borrow_mut().damage(3, 3, 3000, 0x04));
        let mut called = false;
        assert_eq!(c.with_checked(3, 3, &mut |_, _| called = true), Some(false));
        assert!(!called, "read ran over a damaged image");
        let s = c.stats();
        assert_eq!((s.hits, s.rejected, s.len), (1, 1, 0));
    }

    #[test]
    fn range_reads_on_a_disabled_cache_miss() {
        let mut c = BlockCache::<4096, 0>::new();
        c.put(1, 1, &block::<4096>(1), true);
        let mut out = [0u8; 8];
        assert!(!c.get_range_into(1, 1, 0, &mut out));
        assert_eq!(c.stats().misses, 1);
    }

    /// A cache larger than one window: every block inserted is found
    /// again until something evicts it, lookups never confuse two
    /// blocks, and a block lives only inside its window.
    #[test]
    fn a_windowed_cache_finds_what_it_holds() {
        const SLOTS: usize = 64;
        let mut c = BlockCache::<512, SLOTS>::new();
        let mut out = [0u8; 512];
        // Half the cache's worth of blocks from a few tables.
        let blocks = || (0..32u64).map(|i| ((i % 3) as u32 + 1, 1000 + i * 7));
        for (t, b) in blocks() {
            c.put(t, b, &block::<512>(u64::from(t) * 1_000_003 + b), true);
            assert!(
                c.get_into(t, b, &mut out),
                "table {t} block {b} just inserted"
            );
        }
        let mut held = 0;
        for (t, b) in blocks() {
            if c.get_into(t, b, &mut out) {
                assert_eq!(out, block::<512>(u64::from(t) * 1_000_003 + b));
                held += 1;
                let at = c.locate(t, b).unwrap();
                let (first, count) = BlockCache::<512, SLOTS>::window(t, b);
                assert_eq!(count, WINDOW);
                assert!((at + SLOTS - first) % SLOTS < WINDOW, "outside its window");
            }
        }
        // At half load, with overlapping windows, nearly everything fits.
        assert!(held >= 28, "only {held} of 32 blocks survived");
        assert_eq!(c.stats().len, held);
        // A block never inserted is a miss.
        assert!(!c.get_into(9, 9, &mut out));
        // Invalidation still reaches every slot.
        c.invalidate_table(1);
        c.invalidate_table(2);
        c.invalidate_table(3);
        assert_eq!(c.stats().len, 0);
    }

    /// CLOCK inside a window: a block that keeps being read survives any
    /// number of inserts into its window, and cold inserts go first.
    #[test]
    fn a_windowed_cache_keeps_what_is_read() {
        const SLOTS: usize = 32;
        let mut c = BlockCache::<512, SLOTS>::new();
        let mut out = [0u8; 512];
        c.put(1, 1, &block::<512>(1), true);
        // Every other block of a long run: many land in block 1's window.
        for b in 2..2000u64 {
            c.put(1, b, &block::<512>(b), b % 2 == 0);
            assert!(
                c.get_into(1, 1, &mut out),
                "the hot block was evicted at insert {b}"
            );
        }
        assert_eq!(out, block::<512>(1));
        // The cache is full of the run's most recent blocks, never more
        // than it has slots.
        assert_eq!(c.stats().len, SLOTS);
    }

    /// The window is the whole cache up to `WINDOW` slots, and exactly
    /// `WINDOW` slots inside the cache above it.
    #[test]
    fn windows_stay_inside_the_cache() {
        for b in 0..500u64 {
            assert_eq!(BlockCache::<512, 1>::window(7, b), (0, 1));
            assert_eq!(BlockCache::<512, WINDOW>::window(7, b), (0, WINDOW));
            let (first, count) = BlockCache::<512, 9>::window(7, b);
            assert!(first < 9 && count == WINDOW);
            let (first, count) = BlockCache::<512, 1000>::window(7, b);
            assert!(first < 1000 && count == WINDOW);
        }
        // Consecutive blocks of one table do not pile into one window.
        let mut firsts = [0u32; 64];
        for b in 0..640u64 {
            firsts[BlockCache::<512, 64>::window(3, b).0] += 1;
        }
        assert!(firsts.iter().all(|&n| n <= 40), "{firsts:?}");
    }
}
