//! Fixed-capacity sorted memtable over a bump arena.
//!
//! Slots are kept sorted by key bytes, with sequence numbers descending
//! within each key's run. Key/value bytes live in a single bump-allocated
//! arena. Every mutation appends a fresh slot — nothing is ever replaced
//! in place — so a key's run holds its full version history newest-first.
//! Snapshot reads need those older versions; the live view simply takes
//! the run's first entry. Flush drains the runs into an `SSTable` in
//! order, preserving the per-key sequence ordering the read paths rely
//! on.

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
}

impl Slot {
    const EMPTY: Self = Self {
        key_off: 0,
        key_len: 0,
        val_off: 0,
        val_len: 0,
        seq: 0,
        tombstone: false,
    };
}

/// What [`MemTable::get`] returns for a slot.
#[derive(Debug, Clone, Copy)]
pub struct Lookup<'a> {
    /// The stored value bytes (empty for tombstones).
    pub val: &'a [u8],
    /// Sequence number of the mutation that wrote this entry.
    pub seq: u64,
    /// True when this entry is a deletion marker.
    pub tombstone: bool,
}

/// One memtable entry, borrowed. Yielded by [`MemTable::iter`] in
/// key-ascending, sequence-descending order.
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

/// Key-ascending, sequence-descending iterator over memtable entries.
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
        let slot = self.table.slots[..self.table.len].get(self.idx)?;
        self.idx += 1;
        let koff = slot.key_off as usize;
        let kl = usize::from(slot.key_len);
        let voff = slot.val_off as usize;
        let vl = usize::from(slot.val_len);
        Some(Entry {
            key: &self.table.arena[koff..koff + kl],
            val: &self.table.arena[voff..voff + vl],
            seq: slot.seq,
            tombstone: slot.tombstone,
        })
    }
}

/// Sorted in-memory table. Every mutation appends a new slot at its key's
/// run start, so each key's versions are ordered newest-first.
///
/// Sequence numbers are assigned in increasing order by the owner, which
/// is what keeps the runs descending.
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
struct Plan {
    /// Sorted insertion position: the key's run start, or the lower bound
    /// for a new key.
    pos: usize,
    key_off: u32,
    key_len: u16,
    val_off: u32,
    val_len: u16,
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

    /// Number of slots in use.
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

    /// Locates `key`'s run: `Ok(start)` with the run's first slot when the
    /// key is present, `Err(pos)` with the sorted insertion point when it
    /// is absent.
    fn find_run(&self, key: &[u8]) -> Result<usize, usize> {
        let mut pos =
            self.slots[..self.len].binary_search_by(|slot| self.slot_key(slot).cmp(key))?;
        while pos > 0 && self.slot_key(&self.slots[pos - 1]) == key {
            pos -= 1;
        }
        Ok(pos)
    }

    /// Decides how an insert would proceed, without mutating anything. The
    /// new version always goes at the key's run start, keeping versions
    /// newest-first (the owner assigns increasing sequence numbers).
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

        let pos = match self.find_run(key) {
            Ok(start) => start,
            Err(pos) => pos,
        };
        if self.len >= CAP {
            return Err(Error::TableFull);
        }
        let need = key.len() + vlen;
        if self.arena_len + need > ARENA {
            return Err(Error::ArenaFull);
        }
        let key_off = u32::try_from(self.arena_len).map_err(|_| Error::ArenaFull)?;
        let val_off = key_off
            .checked_add(u32::from(key_len))
            .ok_or(Error::ArenaFull)?;
        Ok(Plan {
            pos,
            key_off,
            key_len,
            val_off,
            val_len,
        })
    }

    fn apply(&mut self, plan: Plan, key: &[u8], val: &[u8], seq: u64, tombstone: bool) {
        let Plan {
            pos,
            key_off,
            key_len,
            val_off,
            val_len,
        } = plan;
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
        };
        self.len += 1;
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

    /// Appends `key`'s new version. Tombstone entries store no value bytes.
    /// The caller must assign increasing sequence numbers; the run stays
    /// newest-first only then.
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

    /// Looks up `key`, returning the newest entry, if any.
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<Lookup<'_>> {
        self.get_at(key, u64::MAX)
    }

    /// Looks up `key` as of `max_seq`: the newest version with
    /// `seq <= max_seq`, if any. `u64::MAX` is the live view.
    #[must_use]
    pub fn get_at(&self, key: &[u8], max_seq: u64) -> Option<Lookup<'_>> {
        let mut idx = self.find_run(key).ok()?;
        while idx < self.len {
            let slot = &self.slots[idx];
            if self.slot_key(slot) != key {
                break;
            }
            if slot.seq <= max_seq {
                let voff = slot.val_off as usize;
                let vl = usize::from(slot.val_len);
                return Some(Lookup {
                    val: &self.arena[voff..voff + vl],
                    seq: slot.seq,
                    tombstone: slot.tombstone,
                });
            }
            idx += 1;
        }
        None
    }

    /// Iterates entries in key-ascending, sequence-descending order. Used
    /// by flush to drain the table into an `SSTable`.
    #[must_use]
    pub const fn iter(&self) -> Iter<'_, CAP, ARENA, KEY_MAX, VAL_MAX> {
        Iter {
            table: self,
            idx: 0,
        }
    }

    /// Number of slots. The scan iterator walks slots by index; the borrow
    /// on the database (not documentation) is what keeps those indices
    /// valid for the scan's lifetime.
    #[must_use]
    pub(crate) const fn slot_len(&self) -> usize {
        self.len
    }

    /// The entry at `idx`, or `None` for out-of-range indices.
    #[must_use]
    pub(crate) fn slot_view(&self, idx: usize) -> Option<SlotView<'_>> {
        let slot = self.slots[..self.len].get(idx)?;
        let koff = slot.key_off as usize;
        let kl = usize::from(slot.key_len);
        let voff = slot.val_off as usize;
        let vl = usize::from(slot.val_len);
        Some(SlotView {
            key: &self.arena[koff..koff + kl],
            val: &self.arena[voff..voff + vl],
            seq: slot.seq,
            tombstone: slot.tombstone,
        })
    }

    /// First slot index whose key is `>= key`: a key's run start when the
    /// key is present, the sorted insertion point when it is absent.
    #[must_use]
    pub(crate) fn lower_bound(&self, key: &[u8]) -> usize {
        match self.find_run(key) {
            Ok(start) => start,
            Err(pos) => pos,
        }
    }
}

/// Crate-internal view of one memtable slot, for the scan iterator.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SlotView<'a> {
    /// Key bytes.
    pub key: &'a [u8],
    /// Value bytes (empty for tombstones).
    pub val: &'a [u8],
    /// Sequence number of the mutation that wrote this entry.
    pub seq: u64,
    /// True when this entry is a deletion marker.
    pub tombstone: bool,
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
