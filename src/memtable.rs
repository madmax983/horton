//! Fixed-capacity sorted memtable over a bump arena.
//!
//! Slots are kept sorted by key bytes; key/value bytes live in a single
//! bump-allocated arena. Duplicate keys supersede in place when the new
//! value fits the old entry's arena region, otherwise the old slot is
//! marked dead and a fresh slot is appended (at most one dead slot per
//! key; dead slots are skipped by lookups and reclaimed on flush).

use crate::error::Error;

/// One entry. `key_off`/`val_off` are byte offsets into the arena.
#[derive(Debug, Clone, Copy)]
struct Slot {
    key_off: u32,
    key_len: u16,
    val_off: u32,
    val_len: u16,
    seq: u64,
    tombstone: bool,
    dead: bool,
}

impl Slot {
    const EMPTY: Self = Self {
        key_off: 0,
        key_len: 0,
        val_off: 0,
        val_len: 0,
        seq: 0,
        tombstone: false,
        dead: false,
    };
}

/// What [`MemTable::get`] returns for a live slot.
#[derive(Debug, Clone, Copy)]
pub struct Lookup<'a> {
    /// The stored value bytes (empty for tombstones).
    pub val: &'a [u8],
    /// Sequence number of the mutation that wrote this entry.
    pub seq: u64,
    /// True when this entry is a deletion marker.
    pub tombstone: bool,
}

/// One live memtable entry, borrowed. Yielded by [`MemTable::iter`] in
/// key-ascending order (dead slots are skipped).
#[derive(Debug, Clone, Copy)]
pub struct Entry<
    'a,
    const CAP: usize,
    const ARENA: usize,
    const KEY_MAX: usize,
    const VAL_MAX: usize,
> {
    /// Key bytes.
    pub key: &'a [u8],
    /// Value bytes (empty for tombstones).
    pub val: &'a [u8],
    /// Sequence number of the mutation that wrote this entry.
    pub seq: u64,
    /// True when this entry is a deletion marker.
    pub tombstone: bool,
}

/// Key-ascending iterator over live memtable entries.
#[derive(Debug, Clone)]
pub struct Iter<
    'a,
    const CAP: usize,
    const ARENA: usize,
    const KEY_MAX: usize,
    const VAL_MAX: usize,
> {
    table: &'a MemTable<CAP, ARENA, KEY_MAX, VAL_MAX>,
    idx: usize,
}

impl<'a, const CAP: usize, const ARENA: usize, const KEY_MAX: usize, const VAL_MAX: usize> Iterator
    for Iter<'a, CAP, ARENA, KEY_MAX, VAL_MAX>
{
    type Item = Entry<'a, CAP, ARENA, KEY_MAX, VAL_MAX>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.idx < self.table.len {
            let slot = &self.table.slots[self.idx];
            self.idx += 1;
            if slot.dead {
                continue;
            }
            let koff = slot.key_off as usize;
            let kl = usize::from(slot.key_len);
            let voff = slot.val_off as usize;
            let vl = usize::from(slot.val_len);
            return Some(Entry {
                key: &self.table.arena[koff..koff + kl],
                val: &self.table.arena[voff..voff + vl],
                seq: slot.seq,
                tombstone: slot.tombstone,
            });
        }
        None
    }
}

/// Sorted in-memory table. Only the newest entry per key is retained.
#[derive(Debug, Clone)]
pub struct MemTable<
    const CAP: usize,
    const ARENA: usize,
    const KEY_MAX: usize,
    const VAL_MAX: usize,
> {
    slots: [Slot; CAP],
    len: usize,
    arena: [u8; ARENA],
    arena_len: usize,
    max_seq: u64,
}

/// Insertion plan computed by [`MemTable::plan`].
#[derive(Debug, Clone, Copy)]
enum Plan {
    /// Overwrite the value region of `idx` (same key, fits).
    InPlace { idx: usize, val_len: u16 },
    /// Mark `kill` dead and store the new bytes in reused slot `dead`.
    ReuseDead {
        dead: usize,
        kill: usize,
        key_off: u32,
        key_len: u16,
        val_off: u32,
        val_len: u16,
    },
    /// Shift slots and insert a brand-new slot at `pos`, marking `kill` dead.
    NewSlot {
        pos: usize,
        kill: Option<usize>,
        key_off: u32,
        key_len: u16,
        val_off: u32,
        val_len: u16,
    },
}

impl<const CAP: usize, const ARENA: usize, const KEY_MAX: usize, const VAL_MAX: usize> Default
    for MemTable<CAP, ARENA, KEY_MAX, VAL_MAX>
{
    /// An empty table.
    fn default() -> Self {
        Self::new()
    }
}

impl<const CAP: usize, const ARENA: usize, const KEY_MAX: usize, const VAL_MAX: usize>
    MemTable<CAP, ARENA, KEY_MAX, VAL_MAX>
{
    const ASSERT_CAP: () = assert!(CAP >= 1, "CAP must be at least 1");
    const ASSERT_ARENA_FITS: () =
        assert!(ARENA <= u32::MAX as usize, "ARENA must fit in u32 offsets");
    const ASSERT_KEY: () = assert!(
        KEY_MAX >= 1 && KEY_MAX <= 0xFFFF,
        "KEY_MAX must be within 1..=0xFFFF"
    );
    const ASSERT_VAL: () = assert!(VAL_MAX <= 0xFFFF, "VAL_MAX must fit in u16");

    /// Creates an empty table. Everything is caller-owned; this just zeroes it.
    #[must_use]
    pub const fn new() -> Self {
        // Associated consts are lazy: referencing them here forces the
        // parameter checks to be evaluated for every instantiation.
        let () = Self::ASSERT_CAP;
        let () = Self::ASSERT_ARENA_FITS;
        let () = Self::ASSERT_KEY;
        let () = Self::ASSERT_VAL;
        Self {
            slots: [Slot::EMPTY; CAP],
            len: 0,
            arena: [0u8; ARENA],
            arena_len: 0,
            max_seq: 0,
        }
    }

    /// Number of slots in use (live + dead).
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// True when no slots are in use.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Highest sequence number ever inserted (also updated on replay).
    #[must_use]
    pub const fn max_seq(&self) -> u64 {
        self.max_seq
    }

    /// Resets the table to empty. Stale bytes are left in place but unreachable.
    pub const fn clear(&mut self) {
        self.len = 0;
        self.arena_len = 0;
        self.max_seq = 0;
    }

    fn slot_key(&self, slot: &Slot) -> &[u8] {
        let off = slot.key_off as usize;
        let len = usize::from(slot.key_len);
        &self.arena[off..off + len]
    }

    fn find(&self, key: &[u8]) -> Result<usize, usize> {
        self.slots[..self.len].binary_search_by(|slot| self.slot_key(slot).cmp(key))
    }

    /// Scans the equal-key run containing `any_idx`.
    ///
    /// Returns `(live, dead, insert_pos)`: the live slot if any, the first
    /// dead slot if any, and the position just past the run (a valid sorted
    /// insertion point for this key).
    fn scan_run(&self, any_idx: usize, key: &[u8]) -> (Option<usize>, Option<usize>, usize) {
        let mut start = any_idx;
        while start > 0 && self.slot_key(&self.slots[start - 1]) == key {
            start -= 1;
        }
        let mut end = any_idx;
        while end + 1 < self.len && self.slot_key(&self.slots[end + 1]) == key {
            end += 1;
        }
        let mut live = None;
        let mut dead = None;
        let mut i = start;
        while i <= end {
            if self.slots[i].dead {
                if dead.is_none() {
                    dead = Some(i);
                }
            } else if live.is_none() {
                live = Some(i);
            }
            i += 1;
        }
        (live, dead, end + 1)
    }

    /// Decides how an insert would proceed, without mutating anything.
    fn plan<E>(&self, key: &[u8], val: &[u8], tombstone: bool) -> Result<Plan, Error<E>> {
        if key.is_empty() {
            return Err(Error::EmptyKey);
        }
        let key_len = u16::try_from(key.len()).map_err(|_| Error::KeyTooLarge {
            len: key.len(),
            max: KEY_MAX,
        })?;
        if key.len() > KEY_MAX {
            return Err(Error::KeyTooLarge {
                len: key.len(),
                max: KEY_MAX,
            });
        }
        let vlen = if tombstone { 0 } else { val.len() };
        let val_len = u16::try_from(vlen).map_err(|_| Error::ValueTooLarge {
            len: vlen,
            max: VAL_MAX,
        })?;
        if vlen > VAL_MAX {
            return Err(Error::ValueTooLarge {
                len: vlen,
                max: VAL_MAX,
            });
        }

        let (live, dead, insert_pos) = match self.find(key) {
            Ok(any) => self.scan_run(any, key),
            Err(pos) => (None, None, pos),
        };

        // Duplicate key whose new bytes fit the old region: overwrite in place.
        if let Some(li) = live {
            let old = &self.slots[li];
            let old_total = usize::from(old.key_len) + usize::from(old.val_len);
            if key.len() + vlen <= old_total {
                return Ok(Plan::InPlace { idx: li, val_len });
            }
        }

        // Otherwise fresh arena space is required.
        let need = key.len() + vlen;
        if self.arena_len + need > ARENA {
            return Err(Error::ArenaFull);
        }
        let key_off = u32::try_from(self.arena_len).map_err(|_| Error::ArenaFull)?;
        let val_off = key_off
            .checked_add(u32::from(key_len))
            .ok_or(Error::ArenaFull)?;

        if let Some(di) = dead {
            return Ok(Plan::ReuseDead {
                dead: di,
                kill: live.unwrap_or(di),
                key_off,
                key_len,
                val_off,
                val_len,
            });
        }
        if self.len >= CAP {
            return Err(Error::TableFull);
        }
        Ok(Plan::NewSlot {
            pos: insert_pos,
            kill: live,
            key_off,
            key_len,
            val_off,
            val_len,
        })
    }

    fn apply(&mut self, plan: Plan, key: &[u8], val: &[u8], seq: u64, tombstone: bool) {
        match plan {
            Plan::InPlace { idx, val_len } => {
                // Duplicate key: key bytes are identical, so only the value
                // region is overwritten.
                let vl = usize::from(val_len);
                let slot = &mut self.slots[idx];
                let voff = slot.val_off as usize;
                self.arena[voff..voff + vl].copy_from_slice(&val[..vl]);
                slot.val_len = val_len;
                slot.seq = seq;
                slot.tombstone = tombstone;
            }
            Plan::ReuseDead {
                dead,
                kill,
                key_off,
                key_len,
                val_off,
                val_len,
            } => {
                self.slots[kill].dead = true;
                let kl = usize::from(key_len);
                let vl = usize::from(val_len);
                let koff = key_off as usize;
                let voff = val_off as usize;
                self.arena[koff..koff + kl].copy_from_slice(&key[..kl]);
                self.arena[voff..voff + vl].copy_from_slice(&val[..vl]);
                self.arena_len = voff + vl;
                self.slots[dead] = Slot {
                    key_off,
                    key_len,
                    val_off,
                    val_len,
                    seq,
                    tombstone,
                    dead: false,
                };
            }
            Plan::NewSlot {
                pos,
                kill,
                key_off,
                key_len,
                val_off,
                val_len,
            } => {
                if let Some(k) = kill {
                    self.slots[k].dead = true;
                }
                let kl = usize::from(key_len);
                let vl = usize::from(val_len);
                let koff = key_off as usize;
                let voff = val_off as usize;
                self.arena[koff..koff + kl].copy_from_slice(&key[..kl]);
                self.arena[voff..voff + vl].copy_from_slice(&val[..vl]);
                self.arena_len = voff + vl;
                self.slots.copy_within(pos..self.len, pos + 1);
                self.slots[pos] = Slot {
                    key_off,
                    key_len,
                    val_off,
                    val_len,
                    seq,
                    tombstone,
                    dead: false,
                };
                self.len += 1;
            }
        }
        if seq > self.max_seq {
            self.max_seq = seq;
        }
    }

    /// Validates an insert (sizes and capacity) without mutating the table.
    ///
    /// # Errors
    ///
    /// [`Error::EmptyKey`], [`Error::KeyTooLarge`], [`Error::ValueTooLarge`],
    /// [`Error::TableFull`], or [`Error::ArenaFull`].
    pub fn check_insert<E>(&self, key: &[u8], val: &[u8], tombstone: bool) -> Result<(), Error<E>> {
        let _plan = self.plan(key, val, tombstone)?;
        Ok(())
    }

    /// Inserts or supersedes `key`. Tombstone entries store no value bytes.
    ///
    /// # Errors
    ///
    /// [`Error::EmptyKey`], [`Error::KeyTooLarge`], [`Error::ValueTooLarge`],
    /// [`Error::TableFull`], or [`Error::ArenaFull`].
    pub fn insert<E>(
        &mut self,
        key: &[u8],
        val: &[u8],
        seq: u64,
        tombstone: bool,
    ) -> Result<(), Error<E>> {
        let plan = self.plan(key, val, tombstone)?;
        self.apply(plan, key, val, seq, tombstone);
        Ok(())
    }

    /// Looks up `key`, returning the newest live entry, if any.
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<Lookup<'_>> {
        let any = self.find(key).ok()?;
        let (live, _, _) = self.scan_run(any, key);
        let slot = &self.slots[live?];
        let voff = slot.val_off as usize;
        let vl = usize::from(slot.val_len);
        Some(Lookup {
            val: &self.arena[voff..voff + vl],
            seq: slot.seq,
            tombstone: slot.tombstone,
        })
    }

    /// Iterates live entries in key-ascending order (dead slots skipped).
    /// Used by flush to drain the table into an `SSTable`.
    #[must_use]
    pub const fn iter(&self) -> Iter<'_, CAP, ARENA, KEY_MAX, VAL_MAX> {
        Iter {
            table: self,
            idx: 0,
        }
    }
}

impl<'a, const CAP: usize, const ARENA: usize, const KEY_MAX: usize, const VAL_MAX: usize>
    IntoIterator for &'a MemTable<CAP, ARENA, KEY_MAX, VAL_MAX>
{
    type Item = Entry<'a, CAP, ARENA, KEY_MAX, VAL_MAX>;
    type IntoIter = Iter<'a, CAP, ARENA, KEY_MAX, VAL_MAX>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
