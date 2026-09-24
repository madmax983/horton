//! Table-region allocation: fixed-size slots, one table per slot.
//!
//! The table region `[start, end)` is divided into `slots` equal slots of
//! [`slot_blocks`](SlotMap::slot_blocks) blocks each, where `slots` is the
//! manifest's table capacity (`LEVELS * TABLES`, at most [`MAX_SLOTS`]).
//! Every table lives entirely inside one slot and every slot holds at most
//! one table, so the number of tables the manifest can describe and the
//! space the region can hold are one budget: there is no free list to
//! overflow, no fragmentation, and a table can never grow into its
//! neighbour — its writer is capped at the slot.
//!
//! State is two bitmaps:
//!
//! - **used**: a table the manifest references lives in the slot;
//! - **reserved**: a compaction output is being written there. The job
//!   spans many `compact_step` calls, and a flush between two of them must
//!   not be handed the same slot, so the job reserves its output slot up
//!   front and [`commit`](SlotMap::commit)s it when the manifest makes the
//!   output live.
//!
//! Flush and ingest need no reservation: they hold `&mut Db` from slot
//! choice to manifest commit, so they pick a free slot with
//! [`find_free`](SlotMap::find_free) and [`claim`](SlotMap::claim) it after
//! the commit. A flush future dropped mid-way therefore leaks nothing.
//!
//! Neither bitmap is persisted: `open()` rebuilds `used` from the manifest,
//! and the blocks of a crashed or abandoned write are simply part of a free
//! slot again. Allocation is next-fit from a rotating hint, so successive
//! tables land in successive slots instead of reusing the lowest free one —
//! erase wear spreads across the region (NOR flash wears per erase).

/// Upper bound on slots: each bitmap is one `u64`.
pub const MAX_SLOTS: usize = 64;

/// Fixed-slot allocator over the table region. `Copy`, 40 bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotMap {
    base: u64,
    slot_blocks: u64,
    slots: u32,
    used: u64,
    reserved: u64,
    /// Next slot to try (next-fit).
    hint: u32,
}

impl Default for SlotMap {
    fn default() -> Self {
        Self::new()
    }
}

impl SlotMap {
    /// An allocator with no region: every search fails until
    /// [`layout`](Self::layout) gives it one.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            base: 0,
            slot_blocks: 0,
            slots: 0,
            used: 0,
            reserved: 0,
            hint: 0,
        }
    }

    /// Lays the region `[start, end)` out as `slots` equal slots, all free.
    /// Blocks past the last whole slot stay unused. `None` when `slots` is
    /// 0 or above [`MAX_SLOTS`], or when a slot would hold fewer than
    /// `min_blocks` blocks.
    #[must_use]
    pub const fn layout(start: u64, end: u64, slots: usize, min_blocks: u64) -> Option<Self> {
        if slots == 0 || slots > MAX_SLOTS || end <= start {
            return None;
        }
        // `slots <= 64`: both conversions are exact.
        #[allow(clippy::cast_possible_truncation)]
        let (n64, n32) = (slots as u64, slots as u32);
        let slot_blocks = (end - start) / n64;
        if slot_blocks == 0 || slot_blocks < min_blocks {
            return None;
        }
        Some(Self {
            base: start,
            slot_blocks,
            slots: n32,
            used: 0,
            reserved: 0,
            hint: 0,
        })
    }

    /// Blocks per slot: the largest table the region can hold.
    #[must_use]
    pub const fn slot_blocks(&self) -> u64 {
        self.slot_blocks
    }

    /// Number of slots.
    #[must_use]
    pub const fn slots(&self) -> u32 {
        self.slots
    }

    /// First block of `slot`.
    #[must_use]
    pub const fn slot_base(&self, slot: u32) -> u64 {
        self.base + slot as u64 * self.slot_blocks
    }

    /// The slot holding the run `[first_block, first_block + blocks)`
    /// entirely, or `None` when the run is empty, lies outside the laid-out
    /// slots, or crosses a slot boundary.
    #[must_use]
    pub const fn slot_of(&self, first_block: u64, blocks: u64) -> Option<u32> {
        if self.slot_blocks == 0 || first_block < self.base || blocks == 0 {
            return None;
        }
        let slot = (first_block - self.base) / self.slot_blocks;
        if slot >= self.slots as u64 {
            return None;
        }
        let slot_end = self.base + (slot + 1) * self.slot_blocks;
        match first_block.checked_add(blocks) {
            // `slot < self.slots <= 64`: the narrowing is exact.
            #[allow(clippy::cast_possible_truncation)]
            Some(end) if end <= slot_end => Some(slot as u32),
            _ => None,
        }
    }

    const fn bit(slot: u32) -> u64 {
        1u64 << slot
    }

    /// Marks `slot` as holding a live table (the open-time rebuild).
    /// Returns `false` — changing nothing — when the slot is out of range
    /// or already used: two tables in one slot means a corrupt manifest.
    pub const fn mark_used(&mut self, slot: u32) -> bool {
        if slot >= self.slots || self.used & Self::bit(slot) != 0 {
            return false;
        }
        self.used |= Self::bit(slot);
        true
    }

    /// Restarts next-fit just past `slot` (`open()` points it past the
    /// newest table, so allocation keeps rotating across reboots).
    pub const fn set_hint_after(&mut self, slot: u32) {
        if self.slots > 0 {
            self.hint = (slot + 1) % self.slots;
        }
    }

    /// The next free slot (neither used nor reserved), next-fit from the
    /// hint, without taking it.
    #[must_use]
    pub const fn find_free(&self) -> Option<u32> {
        let taken = self.used | self.reserved;
        let mut i = 0u32;
        while i < self.slots {
            let slot = (self.hint + i) % self.slots;
            if taken & Self::bit(slot) == 0 {
                return Some(slot);
            }
            i += 1;
        }
        None
    }

    /// A table written into free `slot` became live (its manifest commit
    /// landed): free → used. Next-fit continues past it.
    pub const fn claim(&mut self, slot: u32) {
        if slot < self.slots {
            self.used |= Self::bit(slot);
            self.hint = (slot + 1) % self.slots;
        }
    }

    /// Reserves the next free slot for a compaction output. The slot
    /// leaves the free set immediately.
    pub const fn reserve(&mut self) -> Option<u32> {
        match self.find_free() {
            Some(slot) => {
                self.reserved |= Self::bit(slot);
                self.hint = (slot + 1) % self.slots;
                Some(slot)
            }
            None => None,
        }
    }

    /// The output written into reserved `slot` became live: reserved →
    /// used.
    pub const fn commit(&mut self, slot: u32) {
        if slot < self.slots {
            self.reserved &= !Self::bit(slot);
            self.used |= Self::bit(slot);
        }
    }

    /// A reservation was abandoned before its commit: reserved → free.
    pub const fn release(&mut self, slot: u32) {
        if slot < self.slots {
            self.reserved &= !Self::bit(slot);
        }
    }

    /// Abandons every reservation (the in-flight compaction job was
    /// aborted).
    pub const fn release_all(&mut self) {
        self.reserved = 0;
    }

    /// A live table left the manifest (compaction input, archive): used →
    /// free. Call strictly after the commit that dropped it.
    pub const fn free(&mut self, slot: u32) {
        if slot < self.slots {
            self.used &= !Self::bit(slot);
        }
    }

    /// Whether a live table occupies `slot`.
    #[must_use]
    pub const fn is_used(&self, slot: u32) -> bool {
        slot < self.slots && self.used & Self::bit(slot) != 0
    }

    /// Whether `slot` is reserved by the in-flight compaction job.
    #[must_use]
    pub const fn is_reserved(&self, slot: u32) -> bool {
        slot < self.slots && self.reserved & Self::bit(slot) != 0
    }

    /// Slots holding live tables.
    #[must_use]
    pub const fn used_slots(&self) -> u32 {
        self.used.count_ones()
    }

    /// Slots reserved by the in-flight compaction job.
    #[must_use]
    pub const fn reserved_slots(&self) -> u32 {
        self.reserved.count_ones()
    }

    /// Slots neither used nor reserved.
    #[must_use]
    pub const fn free_slots(&self) -> u32 {
        self.slots - (self.used | self.reserved).count_ones()
    }
}
