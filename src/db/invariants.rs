//! The structural self-check tests run after every operation.

use super::Db;
use crate::compact::ranges_overlap;
use crate::device::BlockDevice;
use crate::manifest::Manifest;
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
    /// - A slot is reserved only by a compaction job in flight, one per
    ///   job (the output it is writing). No table is the input of two
    ///   jobs, no two jobs write to one level, and a job that must run
    ///   alone (tight, or serving a request) does.
    /// - Levels 1 and deeper hold pairwise disjoint key ranges.
    /// - Each table is well-formed (room for bloom, index, and footer after
    ///   its sections; no more bloom blocks than `BLOOM_BYTES` fills;
    ///   `min_seq <= max_seq`), and the sequence counter
    ///   dominates every stored sequence and both persisted floors.
    /// - The manifest fits its copies (`Manifest::max_blocks` blocks).
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
                let bloom = crate::sstable::BloomShape::full(BLOOM_BYTES, BLOCK).blocks;
                if t.bloom_blocks == 0 || u32::from(t.bloom_blocks) > bloom {
                    return Err("a table's bloom blocks are not within 1..=the configured filter");
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
        let (mut reserved, mut inputs, mut targets) = (0u32, 0u64, 0u64);
        let in_flight = self.jobs.iter().filter(|j| j.active).count();
        for job in self.jobs.iter().filter(|j| j.active) {
            if job.alone && in_flight > 1 {
                return Err("a job that must run alone has another beside it");
            }
            if targets & (1u64 << job.target) != 0 {
                return Err("two compaction jobs write to one level");
            }
            targets |= 1u64 << job.target;
            if job.reserved != super::Job::NO_SLOT {
                if !self.slots.is_reserved(u32::from(job.reserved)) {
                    return Err("a job's output slot is not marked reserved");
                }
                reserved += 1;
            }
            if inputs & job.inputs != 0 {
                return Err("two compaction jobs share an input table");
            }
            inputs |= job.inputs;
        }
        if self.slots.reserved_slots() != reserved {
            return Err("more slots reserved than the compaction jobs hold");
        }
        if self.manifest.flushed_seq() > self.next_seq || self.manifest.seq_high() > self.next_seq {
            return Err("a persisted sequence floor exceeds the counter");
        }
        if self.manifest.encoded_blocks::<BLOCK>() as u64
            > Manifest::<LEVELS, TABLES, KEY_MAX>::max_blocks::<BLOCK>()
        {
            return Err("the manifest outgrew its copies");
        }
        Ok(())
    }
}
