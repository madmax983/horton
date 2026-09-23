//! Caller-owned block cache (v0.16).
//!
//! A fixed-capacity, allocation-free cache of `SSTable` block images on the
//! read path. All memory is caller-provided and compile-time sized via
//! const generics: [`BlockCache`] owns `[CacheEntry; SLOTS]` inline — no
//! heap, no globals, no interior mutability of its own (the `Db` wraps it
//! in a `RefCell`, like `get_scratch`).
//!
//! # Key
//!
//! Entries are keyed by `(table_id, device_block_id)`. Table ids are
//! monotone and never reused within a manifest lineage, and a table's
//! device blocks are immutable once visible, so a cached entry can never
//! name live data it doesn't describe — even after the table is dropped
//! and its blocks are reclaimed. See SPEC §9 (v0.16) for the full
//! argument.
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
//! # What is cached
//!
//! Physical block images exactly as the device returned them — CRC
//! checks, bloom gating, decompression, and TTL/range shadowing all run
//! after the cache on identical bytes, so a hit is indistinguishable
//! from a re-read, including corruption semantics. The WAL, manifest,
//! compaction merge reads, and pre-visibility table copies bypass the
//! cache deliberately.

use core::cell::RefCell;

/// One cached block image plus its tag and CLOCK state.
#[derive(Clone, Copy)]
struct CacheEntry<const BLOCK: usize> {
    /// Tag: which table's which device block.
    table_id: u32,
    block_id: u64,
    /// Slot in use.
    occupied: bool,
    /// CLOCK second-chance bit.
    refbit: bool,
    /// Physical block image, byte-identical to the device's.
    data: [u8; BLOCK],
}

impl<const BLOCK: usize> CacheEntry<BLOCK> {
    const fn empty() -> Self {
        Self {
            table_id: 0,
            block_id: 0,
            occupied: false,
            refbit: false,
            data: [0u8; BLOCK],
        }
    }
}

/// Test-visible cache counters. `Copy`: snapshot them any time.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CacheStats {
    /// Block reads served from the cache.
    pub hits: u64,
    /// Block reads that went to the device. (`RefCell` contention
    /// bypasses in the [`CachePort`] impl are silent and uncounted —
    /// they return `false` without reaching the counters — so a miss
    /// here always means the device did the work.)
    pub misses: u64,
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
    entries: [CacheEntry<BLOCK>; SLOTS],
    /// CLOCK hand: next eviction candidate.
    hand: usize,
    hits: u64,
    misses: u64,
}

impl<const BLOCK: usize, const SLOTS: usize> BlockCache<BLOCK, SLOTS> {
    /// Empty cache. `const` so it can live in a `const`-constructed `Db`.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            entries: [CacheEntry::empty(); SLOTS],
            hand: 0,
            hits: 0,
            misses: 0,
        }
    }

    /// `false` when `SLOTS == 0`: the cache is compiled out, not just
    /// empty. Every method checks this first so `hand % SLOTS` can never
    /// divide by zero.
    const fn enabled() -> bool {
        SLOTS > 0
    }

    /// Looks `(table_id, block_id)` up, copying the image into `out` on a
    /// hit and setting the slot's reference bit. Returns `false` on a
    /// miss (nothing is written to `out`).
    pub fn get_into(&mut self, table_id: u32, block_id: u64, out: &mut [u8; BLOCK]) -> bool {
        if !Self::enabled() {
            self.misses = self.misses.saturating_add(1);
            return false;
        }
        for e in &mut self.entries {
            if e.occupied && e.table_id == table_id && e.block_id == block_id {
                out.copy_from_slice(&e.data);
                e.refbit = true;
                self.hits = self.hits.saturating_add(1);
                return true;
            }
        }
        self.misses = self.misses.saturating_add(1);
        false
    }

    /// Inserts (or replaces) the image for `(table_id, block_id)`.
    /// `hot` sets the CLOCK reference bit: point-read blocks (`true`)
    /// defend their slot; scan-streamed data blocks (`false`) are
    /// evicted first, so a scan doesn't displace the hot set.
    pub fn put(&mut self, table_id: u32, block_id: u64, block: &[u8; BLOCK], hot: bool) {
        if !Self::enabled() {
            return;
        }
        // Replace in place when the tag already lives here: the bytes are
        // immutable per tag, so this only refreshes the reference bit.
        for e in &mut self.entries {
            if e.occupied && e.table_id == table_id && e.block_id == block_id {
                e.refbit = e.refbit || hot;
                return;
            }
        }
        // CLOCK: give every set bit a second chance, evict the first
        // clear slot. A free slot reads as clear (`occupied == false`
        // implies `refbit == false` by construction).
        loop {
            let hand = self.hand;
            let e = &mut self.entries[hand];
            if !e.refbit {
                e.table_id = table_id;
                e.block_id = block_id;
                e.occupied = true;
                e.refbit = hot;
                e.data.copy_from_slice(block);
                self.hand = if hand + 1 >= SLOTS { 0 } else { hand + 1 };
                return;
            }
            e.refbit = false;
            self.hand = if hand + 1 >= SLOTS { 0 } else { hand + 1 };
        }
    }

    /// Drops every entry tagged with `table_id`. Called when compaction
    /// retires a table; hygiene, not correctness (ids never repeat, so
    /// stale entries are unreachable — this just frees their slots).
    pub fn invalidate_table(&mut self, table_id: u32) {
        if !Self::enabled() {
            return;
        }
        for e in &mut self.entries {
            if e.occupied && e.table_id == table_id {
                e.occupied = false;
                e.refbit = false;
            }
        }
    }

    /// Current counters.
    #[must_use]
    pub fn stats(&self) -> CacheStats {
        let len = self.entries.iter().filter(|e| e.occupied).count();
        CacheStats {
            hits: self.hits,
            misses: self.misses,
            len,
            capacity: SLOTS,
        }
    }
}

impl<const BLOCK: usize, const SLOTS: usize> Default for BlockCache<BLOCK, SLOTS> {
    fn default() -> Self {
        Self::new()
    }
}

/// The read path's view of a cache: synchronous block-image operations
/// behind `&self`, so `TableReader` and the scans can hold it without
/// another const parameter.
///
/// Implemented for `RefCell<BlockCache<..>>` — the `Db`'s field type.
/// `RefCell` contention (two interleaved `get`s on one executor)
/// degrades to a silent bypass via `try_borrow_mut`: a miss, never a
/// panic, never a wrong byte.
pub trait CachePort<const BLOCK: usize> {
    /// Copy the cached image into `out`; `false` on miss.
    fn get_into(&self, table_id: u32, block_id: u64, out: &mut [u8; BLOCK]) -> bool;
    /// Insert the image; `hot` sets the CLOCK reference bit.
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
