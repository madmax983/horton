//! The reader gate: what lets a point read go to the tables without the
//! engine lock, and keeps flush and compaction out of the tables while
//! any read is in them (ADR-0023).
//!
//! A point read needs two things from the database before its table
//! phase: what the memtable holds for its key, and which tables exist.
//! Both used to be asked under the engine lock. Most of the time the
//! answers are "nothing" and "the same tables as last time", and this
//! module lets a reader learn that from words nobody is writing:
//!
//! - **A filter over the memtable** ([`Gate::note`], [`Gate::enter`]):
//!   one bit pattern per key written since the last flush. A key whose
//!   bits are not all set is not in the memtable.
//! - **The published manifest sequence number**: equal to the one the
//!   reader's own copy of the manifest carries means that copy is
//!   current.
//! - **A pin in the reader's own slot.** Each reader thread has a
//!   counter of its own, registered here. Flush and compaction, which
//!   rewrite tables, first *shut* the gate and wait for every slot to
//!   read zero ([`Gate::shut`]).
//!
//! The pin and the shutting are a store-then-load on each side, in
//! sequentially consistent order: a reader raises its slot and then
//! looks at `changing`; a table change raises `changing` and then looks
//! at the slots. One of the two sees the other. A reader that sees
//! `changing` lowers its slot and takes the engine lock instead, which
//! the table change holds until it is done.
//!
//! While a reader is pinned with the gate open, no flush clears the
//! filter and no table is written, so what it read from the three words
//! stays true until it lets go. Writes go on meanwhile: a key written
//! after the reader looked at the filter is a write concurrent with the
//! read, and the read answering without it is a correct order.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use crate::MEMTABLE_ENTRIES;

/// Words in the memtable filter: one for each entry the memtable can
/// hold, so that a full memtable sets two bits in each word on average.
/// 32 KiB.
const FILTER_WORDS: usize = MEMTABLE_ENTRIES.next_power_of_two();

/// Gate polls between two `yield`s while a table change waits for
/// readers.
const POLLS_PER_YIELD: usize = 64;

/// Names each gate, so that a thread can tell which store its reader
/// state belongs to. Starts at 1: a thread's state starts as store 0.
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// One reader thread's share of a gate, on a cache line of its own: the
/// only thread that writes it is the reader, and the only other that
/// reads it is a table change, when it waits.
#[repr(align(64))]
pub struct Slot {
    /// Table phases this thread is in (0 or 1).
    pins: AtomicUsize,
    /// Point reads that took no lock, and those that took the engine
    /// lock. Kept here, not in a shared total, so counting costs a
    /// reader nothing shared.
    unlocked: AtomicU64,
    locked: AtomicU64,
}

impl Slot {
    /// A slot no gate knows: what a thread holds before its first read.
    pub const fn unregistered() -> Self {
        Self {
            pins: AtomicUsize::new(0),
            unlocked: AtomicU64::new(0),
            locked: AtomicU64::new(0),
        }
    }

    /// Counts one point read by this thread.
    pub fn count(&self, unlocked: bool) {
        // Only this thread writes these: a load and a store, no lock
        // prefix.
        let counter = if unlocked {
            &self.unlocked
        } else {
            &self.locked
        };
        counter.store(counter.load(Ordering::Relaxed) + 1, Ordering::Relaxed);
    }
}

/// A reader in the table phase of a read or a scan page. While any is
/// alive, flush and compaction wait ([`Gate::shut`]): they are what
/// could rewrite a table the reader is in. Dropping it lets them go on.
///
/// A pin is made in one of two ways. Under the engine lock, in the same
/// hold of it as the read's memtable probe and its look at the manifest:
/// nothing changes the tables without that lock. Or by [`Gate::enter`],
/// with no lock, which hands the pin out only if the gate was open after
/// the pin was raised.
pub struct Pin<'a>(&'a Slot);

impl<'a> Pin<'a> {
    /// Raises `slot`. For a caller that holds the engine lock; without
    /// it, [`Gate::enter`].
    pub fn new(slot: &'a Slot) -> Self {
        slot.pins.fetch_add(1, Ordering::SeqCst);
        Self(slot)
    }
}

impl Drop for Pin<'_> {
    fn drop(&mut self) {
        self.0.pins.fetch_sub(1, Ordering::SeqCst);
    }
}

/// The three words a reader looks at, together on one cache line that is
/// written only by a flush, a compaction step or a range delete.
#[repr(align(64))]
struct Flags {
    /// A flush or a compaction step is waiting for readers or running,
    /// or the store is closed.
    changing: AtomicBool,
    /// The memtable may hold something the filter does not say: records
    /// replayed from the WAL at open, or a range tombstone. Until the
    /// next flush.
    unfiltered: AtomicBool,
    /// `Manifest::seq` of the database's manifest.
    tables_seq: AtomicU64,
}

/// The slots of every thread reading this store, and the read counts of
/// those that are gone.
struct Registry {
    slots: Vec<Arc<Slot>>,
    retired: (u64, u64),
}

/// See the module documentation.
pub struct Gate {
    id: u64,
    flags: Flags,
    /// The memtable filter: a key sets two bits of one word.
    filter: Box<[AtomicU64]>,
    registry: Mutex<Registry>,
}

/// A 64-bit hash of `key`, for the memtable filter.
fn hash(key: &[u8]) -> u64 {
    const fn mix(h: u64) -> u64 {
        let h = (h ^ (h >> 33)).wrapping_mul(0xFF51_AFD7_ED55_8CCD);
        let h = (h ^ (h >> 33)).wrapping_mul(0xC4CE_B9FE_1A85_EC53);
        h ^ (h >> 33)
    }
    let mut h = 0x9E37_79B9_7F4A_7C15 ^ key.len() as u64;
    let mut chunks = key.chunks_exact(8);
    for chunk in &mut chunks {
        let mut word = [0u8; 8];
        word.copy_from_slice(chunk);
        h = (h ^ u64::from_le_bytes(word)).wrapping_mul(0x9FB2_1C65_1E98_DF25);
        h ^= h >> 29;
    }
    let rest = chunks.remainder();
    let mut word = [0u8; 8];
    word[..rest.len()].copy_from_slice(rest);
    mix(h ^ u64::from_le_bytes(word))
}

/// Where `key` lives in a filter of `words` words (a power of two): the
/// word, and its two bits in it.
const fn place(h: u64, words: usize) -> (usize, u64) {
    #[allow(
        clippy::cast_possible_truncation,
        reason = "masked to the filter's length"
    )]
    let word = (h >> 32) as usize & (words - 1);
    (word, 1 << (h & 63) | 1 << ((h >> 6) & 63))
}

impl Gate {
    /// The gate of a database whose manifest is at `tables_seq`, open.
    /// `unfiltered` says its memtable is not empty (recovery replayed
    /// records into it).
    pub fn new(tables_seq: u64, unfiltered: bool) -> Self {
        Self {
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
            flags: Flags {
                changing: AtomicBool::new(false),
                unfiltered: AtomicBool::new(unfiltered),
                tables_seq: AtomicU64::new(tables_seq),
            },
            filter: (0..FILTER_WORDS).map(|_| AtomicU64::new(0)).collect(),
            registry: Mutex::new(Registry {
                slots: Vec::new(),
                retired: (0, 0),
            }),
        }
    }

    /// This gate's name: no other gate in the process has it.
    pub const fn id(&self) -> u64 {
        self.id
    }

    fn registry(&self) -> std::sync::MutexGuard<'_, Registry> {
        self.registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// A slot for the calling thread, known to this gate from now on.
    /// Slots whose threads have let go of them are dropped here, and
    /// their read counts kept.
    pub fn register(&self) -> Arc<Slot> {
        let slot = Arc::new(Slot::unregistered());
        let mut registry = self.registry();
        let Registry { slots, retired } = &mut *registry;
        slots.retain(|s| {
            // A slot only the registry holds belongs to no thread, so
            // nothing can raise it again.
            let live = Arc::strong_count(s) > 1 || s.pins.load(Ordering::SeqCst) != 0;
            if !live {
                retired.0 += s.unlocked.load(Ordering::Relaxed);
                retired.1 += s.locked.load(Ordering::Relaxed);
            }
            live
        });
        slots.push(Arc::clone(&slot));
        drop(registry);
        slot
    }

    /// Readers in their table phase right now.
    pub fn pinned(&self) -> usize {
        self.registry()
            .slots
            .iter()
            .map(|s| s.pins.load(Ordering::SeqCst))
            .sum()
    }

    /// Point reads that took no lock, and point reads that took the
    /// engine lock, by every thread so far.
    pub fn reads(&self) -> (u64, u64) {
        let registry = self.registry();
        registry.slots.iter().fold(registry.retired, |(u, l), s| {
            (
                u + s.unlocked.load(Ordering::Relaxed),
                l + s.locked.load(Ordering::Relaxed),
            )
        })
    }

    /// A reader's way to the tables without the engine lock. `slot` is
    /// the calling thread's, registered with this gate; `seen` is
    /// `Manifest::seq` of the manifest copy the thread holds, if it
    /// holds one **of this store**.
    ///
    /// Returns a pin when, with that pin already raised, the gate was
    /// open, the thread's manifest was the current one, and the memtable
    /// could not hold `key`. Until the pin is dropped the tables are
    /// those of that manifest and nothing writes them, and the read's
    /// memtable probe is that of a key the memtable holds nothing for.
    ///
    /// `None` means: ask under the engine lock.
    pub fn enter<'a>(&self, slot: &'a Slot, seen: Option<u64>, key: &[u8]) -> Option<Pin<'a>> {
        let seen = seen?;
        // A first look without the pin: while a table change waits or
        // runs, readers stay out of its way and write nothing. This
        // look promises nothing; the one after the pin does.
        if self.flags.changing.load(Ordering::SeqCst) {
            return None;
        }
        let pin = Pin::new(slot);
        // After the pin, and in one order with `shut`'s store and its
        // look at the slots (both sides `SeqCst`): a table change that
        // this load does not see will see the pin.
        if self.flags.changing.load(Ordering::SeqCst) {
            return None;
        }
        // The gate was open after the pin went up. A table change now
        // waits for the pin, so from here the manifest and the filter
        // change only by writes adding keys.
        if self.flags.tables_seq.load(Ordering::SeqCst) != seen
            || self.flags.unfiltered.load(Ordering::SeqCst)
            || self.may_hold(key)
        {
            return None;
        }
        Some(pin)
    }

    /// True unless `key` has certainly not been written since the last
    /// flush.
    fn may_hold(&self, key: &[u8]) -> bool {
        let (word, bits) = place(hash(key), self.filter.len());
        self.filter
            .get(word)
            .is_none_or(|w| w.load(Ordering::Acquire) & bits == bits)
    }

    /// Records that the memtable is about to hold `key`. Called under
    /// the engine lock, **before** the write that puts it there: a read
    /// that starts once the write has returned then finds the bits.
    pub fn note(&self, key: &[u8]) {
        let (word, bits) = place(hash(key), self.filter.len());
        if let Some(w) = self.filter.get(word) {
            // One writer at a time (the engine lock), so a load and a
            // store; and no store when the bits are there, so that
            // rewriting a key does not take the line from the readers.
            let now = w.load(Ordering::Relaxed);
            if now & bits != bits {
                w.store(now | bits, Ordering::Release);
            }
        }
    }

    /// Records that the memtable is about to hold something no filter
    /// bit stands for (a range tombstone). Every read asks under the
    /// engine lock until the next flush. Called under the engine lock,
    /// before the write.
    pub fn note_unfiltered(&self) {
        if !self.flags.unfiltered.load(Ordering::SeqCst) {
            self.flags.unfiltered.store(true, Ordering::SeqCst);
        }
    }

    /// Publishes `tables_seq` as the database's manifest sequence
    /// number. Called under the engine lock when a manifest commit has
    /// left the tables as they were (a table change publishes through
    /// [`open`](Self::open)).
    pub fn publish(&self, tables_seq: u64) {
        self.flags.tables_seq.store(tables_seq, Ordering::SeqCst);
    }

    /// Shuts the gate and waits until no reader is in its table phase.
    /// Called under the engine lock before each flush and each
    /// compaction step, the only calls that write a table or give a
    /// table's blocks up for reuse.
    ///
    /// When this returns no reader is in the tables and none can enter:
    /// a reader without the engine lock sees the gate shut, and a reader
    /// that wants the engine lock waits for the caller to release it.
    /// The caller must [`open`](Self::open) the gate before it releases
    /// the engine lock.
    ///
    /// Never call this on a thread that holds a pin of its own: it would
    /// wait for itself.
    pub fn shut(&self) {
        self.flags.changing.store(true, Ordering::SeqCst);
        let mut polls = 0usize;
        while self.pinned() != 0 {
            polls += 1;
            if polls.is_multiple_of(POLLS_PER_YIELD) {
                std::thread::yield_now();
            } else {
                core::hint::spin_loop();
            }
        }
    }

    /// Opens the gate after a table change: publishes `tables_seq`, the
    /// manifest's sequence number now, and, when the change was a flush
    /// that emptied the memtable (`flushed`), empties the filter. The
    /// gate opens last, so a reader that finds it open finds the rest in
    /// place.
    pub fn open(&self, tables_seq: u64, flushed: bool) {
        self.flags.tables_seq.store(tables_seq, Ordering::SeqCst);
        if flushed {
            for w in &self.filter {
                if w.load(Ordering::Relaxed) != 0 {
                    w.store(0, Ordering::Release);
                }
            }
            self.flags.unfiltered.store(false, Ordering::SeqCst);
        }
        self.flags.changing.store(false, Ordering::SeqCst);
    }

    /// Shuts the gate for good: the store is closing. Called with the
    /// engine lock held, so no flush or compaction step is under way to
    /// open it again. Readers already in the tables are not waited for
    /// here; see [`pinned`](Self::pinned).
    pub fn close(&self) {
        self.flags.changing.store(true, Ordering::SeqCst);
    }

    /// The published manifest sequence number.
    #[cfg(test)]
    pub fn tables_seq(&self) -> u64 {
        self.flags.tables_seq.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(i: usize) -> Vec<u8> {
        format!("k{i:06}").into_bytes()
    }

    /// The filter's one promise: a key that was noted is never turned
    /// away as absent, whatever else was noted, until a flush.
    #[test]
    fn a_noted_key_never_enters_until_a_flush() {
        let gate = Gate::new(7, false);
        let slot = gate.register();
        for i in 0..MEMTABLE_ENTRIES {
            gate.note(&key(i));
            // Itself, and every key before it.
            assert!(gate.enter(&slot, Some(7), &key(i)).is_none());
            if i % 97 == 0 {
                for j in 0..=i {
                    assert!(gate.enter(&slot, Some(7), &key(j)).is_none(), "{j} of {i}");
                }
            }
        }
        assert_eq!(gate.pinned(), 0);
        // A compaction step leaves the filter alone; a flush empties it.
        gate.shut();
        gate.open(8, false);
        assert!(gate.enter(&slot, Some(8), &key(3)).is_none());
        gate.shut();
        gate.open(9, true);
        for i in 0..MEMTABLE_ENTRIES {
            assert!(gate.enter(&slot, Some(9), &key(i)).is_some(), "{i}");
        }
    }

    /// With the memtable full, nearly every key that is not in it still
    /// enters: a filter that turned most keys away would be correct and
    /// useless. Keys of several shapes, since the hash must spread each.
    #[test]
    fn a_full_memtable_turns_few_other_keys_away() {
        let shapes: [fn(usize) -> Vec<u8>; 4] = [
            key,
            |i| (i as u64).to_be_bytes().to_vec(),
            |i| (i as u64).to_le_bytes().to_vec(),
            |i| format!("user/{i}/profile/settings/with-a-long-common-tail").into_bytes(),
        ];
        for (n, shape) in shapes.iter().enumerate() {
            let gate = Gate::new(1, false);
            let slot = gate.register();
            for i in 0..MEMTABLE_ENTRIES {
                gate.note(&shape(i));
            }
            let others = 100_000;
            let turned_away = (0..others)
                .filter(|&i| {
                    gate.enter(&slot, Some(1), &shape(MEMTABLE_ENTRIES + i))
                        .is_none()
                })
                .count();
            assert!(
                turned_away * 100 < others,
                "shape {n}: {turned_away} of {others}"
            );
        }
    }

    /// Each reason to ask under the lock, alone.
    #[test]
    fn enter_refuses_what_it_cannot_vouch_for() {
        let gate = Gate::new(4, false);
        let slot = gate.register();
        assert!(gate.enter(&slot, Some(4), b"k").is_some());
        // No manifest copy, or not the current one.
        assert!(gate.enter(&slot, None, b"k").is_none());
        assert!(gate.enter(&slot, Some(3), b"k").is_none());
        gate.publish(5);
        assert!(gate.enter(&slot, Some(4), b"k").is_none());
        assert!(gate.enter(&slot, Some(5), b"k").is_some());
        // A range tombstone in the memtable, until a flush.
        gate.note_unfiltered();
        assert!(gate.enter(&slot, Some(5), b"k").is_none());
        gate.shut();
        gate.open(5, false);
        assert!(gate.enter(&slot, Some(5), b"k").is_none());
        gate.shut();
        // Shut: nothing enters, and nothing is left pinned by trying.
        assert!(gate.enter(&slot, Some(5), b"k").is_none());
        assert_eq!(gate.pinned(), 0);
        gate.open(6, true);
        assert!(gate.enter(&slot, Some(6), b"k").is_some());
        // A database recovered with records in its memtable.
        let recovered = Gate::new(4, true);
        let slot = recovered.register();
        assert!(recovered.enter(&slot, Some(4), b"k").is_none());
        // Closed: for good.
        gate.close();
        let slot = gate.register();
        assert!(gate.enter(&slot, Some(6), b"k").is_none());
        assert_ne!(gate.id(), recovered.id());
    }

    /// `shut` returns only once every pin is gone, and a pin is counted
    /// whichever thread's slot holds it.
    #[test]
    fn shut_waits_for_every_slot() {
        let gate = Arc::new(Gate::new(1, false));
        let (pinned_tx, pinned_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let reader = {
            let gate = Arc::clone(&gate);
            std::thread::spawn(move || {
                let slot = gate.register();
                let pin = gate.enter(&slot, Some(1), b"k").expect("open");
                pinned_tx.send(()).expect("send");
                release_rx.recv().expect("recv");
                drop(pin);
            })
        };
        pinned_rx.recv().expect("pinned");
        assert_eq!(gate.pinned(), 1);
        let shut = Arc::new(AtomicBool::new(false));
        let changer = {
            let (gate, shut) = (Arc::clone(&gate), Arc::clone(&shut));
            std::thread::spawn(move || {
                gate.shut();
                shut.store(true, Ordering::SeqCst);
                gate.open(2, true);
            })
        };
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert!(
            !shut.load(Ordering::SeqCst),
            "shut did not wait for the pin"
        );
        // Shut and waiting: a second reader does not get in.
        let slot = gate.register();
        assert!(gate.enter(&slot, Some(1), b"other").is_none());
        release_tx.send(()).expect("release");
        changer.join().expect("changer");
        reader.join().expect("reader");
        assert!(shut.load(Ordering::SeqCst));
        assert_eq!(gate.pinned(), 0);
    }

    /// Slots of threads that are gone leave the registry, and their
    /// counts stay in the totals.
    #[test]
    fn dead_threads_slots_are_dropped_and_their_counts_kept() {
        let gate = Arc::new(Gate::new(1, false));
        for _ in 0..8 {
            let gate = Arc::clone(&gate);
            std::thread::spawn(move || {
                let slot = gate.register();
                slot.count(true);
                slot.count(true);
                slot.count(false);
            })
            .join()
            .expect("join");
        }
        assert_eq!(gate.reads(), (16, 8));
        let mine = gate.register();
        mine.count(false);
        assert_eq!(gate.registry().slots.len(), 1);
        assert_eq!(gate.reads(), (16, 9));
    }

    /// Readers enter and leave while a changer shuts and opens: a reader
    /// that got in never overlaps a change, and never holds a manifest
    /// the change has replaced.
    #[test]
    #[allow(
        clippy::needless_collect,
        reason = "the readers must all be running before the changer starts"
    )]
    fn readers_never_overlap_a_change() {
        const CHANGES: u64 = 2000;
        let gate = Arc::new(Gate::new(0, false));
        // The "tables": the changer makes them odd while it works. A
        // reader in its table phase must only ever see them even, and
        // equal to twice the sequence number it entered by.
        let tables = Arc::new(AtomicU64::new(0));
        let entered = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let readers: Vec<_> = (0..3)
            .map(|_| {
                let (gate, tables, entered, stop) = (
                    Arc::clone(&gate),
                    Arc::clone(&tables),
                    Arc::clone(&entered),
                    Arc::clone(&stop),
                );
                std::thread::spawn(move || {
                    let slot = gate.register();
                    let mut seen = 0u64;
                    while !stop.load(Ordering::Relaxed) {
                        if let Some(pin) = gate.enter(&slot, Some(seen), b"k") {
                            for _ in 0..8 {
                                assert_eq!(tables.load(Ordering::SeqCst), seen * 2);
                                core::hint::spin_loop();
                            }
                            drop(pin);
                            entered.fetch_add(1, Ordering::Relaxed);
                        } else {
                            // The locked path would hand it the current
                            // manifest; here, read what was published.
                            seen = gate.tables_seq();
                        }
                    }
                })
            })
            .collect();
        for seq in 1..=CHANGES {
            gate.shut();
            tables.store(seq * 2 - 1, Ordering::SeqCst);
            for _ in 0..50 {
                core::hint::spin_loop();
            }
            tables.store(seq * 2, Ordering::SeqCst);
            gate.open(seq, false);
            if seq % 10 == 0 {
                // Both sides must run: let a reader through this state
                // of the tables before the next change.
                let had = entered.load(Ordering::Relaxed);
                let since = std::time::Instant::now();
                while entered.load(Ordering::Relaxed) == had {
                    assert!(
                        since.elapsed() < std::time::Duration::from_secs(30),
                        "no reader entered an open gate"
                    );
                    std::thread::yield_now();
                }
            }
        }
        stop.store(true, Ordering::Relaxed);
        for r in readers {
            r.join().expect("reader");
        }
        assert!(entered.load(Ordering::Relaxed) >= CHANGES / 10);
        assert_eq!(gate.pinned(), 0);
    }
}
