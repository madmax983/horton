//! The structural self-check tests run after every operation.

use super::Db;
use crate::compact::ranges_overlap;
use crate::device::BlockDevice;
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
> Db<D, BLOCK, KEY_MAX, VAL_MAX, CAP, ARENA, LEVELS, TABLES, BLOOM_BYTES, CACHE>
{
    /// Structural self-check: verifies the invariants every operation must
    /// preserve and returns the first one violated, or `Ok(())`.
    ///
    /// - Every live table sits wholly inside its own table slot, and the
    ///   slot map's used set is exactly those slots (so no two live tables
    ///   share a block, and no free slot holds a live table).
    /// - At most one slot is reserved, and only while a compaction job is
    ///   in flight (the output it is writing).
    /// - Levels 1 and deeper hold pairwise disjoint key ranges.
    /// - Each table is well-formed (room for bloom, index, and footer after
    ///   its sections; `min_seq <= max_seq`), and the sequence counter
    ///   dominates every stored sequence and both persisted floors.
    /// - The manifest encodes into one block.
    ///
    /// Pure and synchronous: it reads only in-memory state (no device I/O),
    /// in `O(tables²)`. Tests call it after every
    /// operation; firmware can call it after `open()` as a cheap sanity
    /// check.
    ///
    /// # Errors
    ///
    /// A static description of the first violated invariant.
    pub fn check_invariants(&self) -> Result<(), &'static str> {
        let mut seen = 0u64;
        for li in 0..LEVELS {
            let tables = self.manifest.level(li).unwrap_or(&[]);
            for (i, t) in tables.iter().enumerate() {
                let Some(slot) = self.slot_of(t) else {
                    return Err("a live table does not sit inside one table slot");
                };
                if seen & (1u64 << slot) != 0 {
                    return Err("two live tables share a table slot");
                }
                seen |= 1u64 << slot;
                if !self.slots.is_used(slot) {
                    return Err("a live table's slot is not marked used");
                }
                if t.data_blocks().is_none() {
                    return Err("a table has no room for bloom, index, and footer");
                }
                if t.entry_count > 0 && t.min_seq > t.max_seq {
                    return Err("a table's min_seq exceeds its max_seq");
                }
                if t.max_seq > self.next_seq {
                    return Err("a table holds a sequence above the counter");
                }
                if li >= 1 {
                    for u in &tables[i + 1..] {
                        if ranges_overlap(t.first_key, t.last_key, u.first_key, u.last_key) {
                            return Err("two tables in a level >= 1 overlap in key range");
                        }
                    }
                }
            }
        }
        if self.slots.used_slots() != seen.count_ones() {
            return Err("a slot is marked used but holds no live table");
        }
        // A job reserves exactly the slot of the output it is writing:
        // every sealed output commits before the next is reserved.
        if self.slots.reserved_slots() > u32::from(self.job_active) {
            return Err("more slots reserved than a compaction job holds");
        }
        if self.manifest.flushed_seq() > self.next_seq || self.manifest.seq_high() > self.next_seq {
            return Err("a persisted sequence floor exceeds the counter");
        }
        let mut buf = [0u8; BLOCK];
        if self.manifest.encode::<D::Error, BLOCK>(&mut buf).is_err() {
            return Err("the manifest no longer fits one block");
        }
        Ok(())
    }
}
