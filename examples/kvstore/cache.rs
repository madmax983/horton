//! The readers' block cache. A hit takes no lock and writes nothing that
//! another thread reads.
//!
//! Every reader thread asks this cache for the same few blocks: the
//! bloom and index blocks of the tables a key could be in. In a cache
//! behind locks each of those hits writes shared memory three times (the
//! lock, a reference bit, a hit counter), and the cache lines holding
//! them travel from core to core.
//!
//! Here a hit only reads:
//!
//! - **Entries are immutable.** A cached block is an [`Entry`] behind an
//!   `Arc`: its image and the checksums of the image never change. A
//!   block image is named by `(table id, block id)` for good (horton
//!   never reuses a table id), so an entry is never wrong, only evicted.
//! - **Each thread keeps a view.** For every slot it has looked at, a
//!   thread remembers the slot's epoch, whose block was there, and (once
//!   it has used that block) a reference to the entry. A lookup loads
//!   the epochs of the eight slots a block can be in (one cache line,
//!   written only when one of those slots changes) and compares them
//!   with its view. Equal epochs mean the view is current, and the
//!   thread reads the entry through its own reference.
//! - **Only a change takes a lock.** Inserting or evicting locks the
//!   block's window, swaps the entry and bumps the slot's epoch. A
//!   reader whose view of the window is now stale locks it once to look
//!   again.
//! - **The reference bit is tested before it is set.** CLOCK needs to
//!   know which entries are in use. A hit loads the entry's bit and
//!   stores only when it finds it clear, which is once per sweep of the
//!   hand, not once per hit.
//! - **Counters are kept per thread** and added to the shared totals
//!   every [`FLUSH_EVERY`] lookups, and when the thread ends.
//!
//! horton's rule for cached blocks (ADR-0016) holds unchanged: only
//! verified images are inserted (the read path does that), and every hit
//! is checked against a RAM checksum of the image before its bytes are
//! used. A point read's data block is lent with a check instead
//! ([`SharedCache::with_checked`], ADR-0031): the search checks each
//! range it reads before it acts on it, and only those. The checksum here is kept per [`SUM_BYTES`] bytes, finer than
//! the library's sixteenth of a block, so the 64 bytes of a bloom bucket
//! cost the summing of 256 bytes and not of a kilobyte.
//!
//! The price is memory that outlives its use. A thread's view holds a
//! reference to every entry it has hit, and an entry is freed when the
//! cache and every view have let go of it. A thread drops a reference
//! when it next looks at that window and finds the slot changed, and
//! drops all of them when it ends. So a thread that stops reading but
//! stays alive can keep blocks the cache has since replaced: at most
//! the blocks it had used, and never more than one cache's worth.
//! ADR-0022 has the reasoning.

use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use horton::cache::{CheckedRead, image_sum};
use horton::{CachePort, CacheStats};

use crate::BLOCK;

/// Blocks the cache holds: 1024 x 16 KiB = 16 MiB.
pub const CACHE_BLOCKS: usize = 1024;

/// Slots a block can be in: the eight of its window, as in horton's own
/// cache above eight slots.
const WINDOW: usize = 8;

/// Bytes of an image one RAM checksum covers.
const SUM_BYTES: usize = 256;

/// Checksums per image.
const SUMS: usize = BLOCK.div_ceil(SUM_BYTES);

/// Lookups a thread counts privately before it adds them to the shared
/// totals.
const FLUSH_EVERY: u64 = 1024;

/// The lock guard of plain data that a panic cannot leave half changed.
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// One cached block. Immutable but for the reference bit.
///
/// Aligned to a cache line so that the `Arc`'s reference counts, which
/// change when a thread takes or drops its reference, do not share a
/// line with anything a hit reads.
#[repr(align(64))]
struct Entry {
    table_id: u32,
    block_id: u64,
    /// CLOCK's reference bit: set by a hit that finds it clear, cleared
    /// by the hand as it passes.
    used: AtomicBool,
    /// [`image_sum`] of each [`SUM_BYTES`] of `image`, taken from the
    /// verified image the entry was made from.
    sums: [u64; SUMS],
    image: [u8; BLOCK],
}

impl Entry {
    fn new(table_id: u32, block_id: u64, image: &[u8; BLOCK], hot: bool) -> Self {
        let mut entry = Self {
            table_id,
            block_id,
            used: AtomicBool::new(hot),
            sums: [0u64; SUMS],
            image: *image,
        };
        entry.sum();
        entry
    }

    /// Makes this entry over into another block's, in place. Only an
    /// entry nobody else holds can be: the caller has it by `&mut`.
    fn refill(&mut self, table_id: u32, block_id: u64, image: &[u8; BLOCK], hot: bool) {
        self.table_id = table_id;
        self.block_id = block_id;
        *self.used.get_mut() = hot;
        self.image.copy_from_slice(image);
        self.sum();
    }

    fn sum(&mut self) {
        for (sum, chunk) in self.sums.iter_mut().zip(self.image.chunks(SUM_BYTES)) {
            *sum = image_sum(chunk);
        }
    }

    #[cfg(test)]
    const fn is(&self, table_id: u32, block_id: u64) -> bool {
        self.table_id == table_id && self.block_id == block_id
    }

    /// True when the chunks of `bytes` that overlap `from..to` still have
    /// the sums recorded for the image. `bytes` is the image or a copy of
    /// all of it.
    fn intact(&self, bytes: &[u8; BLOCK], from: usize, to: usize) -> bool {
        if to <= from {
            return true;
        }
        (from / SUM_BYTES..=(to - 1) / SUM_BYTES).all(|c| {
            let start = c * SUM_BYTES;
            let end = (start + SUM_BYTES).min(BLOCK);
            match (bytes.get(start..end), self.sums.get(c)) {
                (Some(chunk), Some(sum)) => image_sum(chunk) == *sum,
                _ => false,
            }
        })
    }
}

/// Totals every thread adds its counts to.
#[derive(Default)]
struct Counters {
    hits: AtomicU64,
    misses: AtomicU64,
    rejected: AtomicU64,
}

/// What a thread remembers of one slot.
struct Seen {
    /// The slot's epoch when this was learned. Epochs start at 0, and so
    /// does a view: an untouched slot is known to be empty.
    epoch: u64,
    /// Whose block was in the slot then.
    tag: Option<(u32, u64)>,
    /// The entry, once this thread has used it.
    entry: Option<Arc<Entry>>,
}

/// A thread's view of one cache, and its counts not yet added to the
/// totals.
struct View {
    cache: u64,
    seen: Vec<Seen>,
    hits: u64,
    misses: u64,
    counters: Arc<Counters>,
}

impl View {
    fn new(cache: &SharedCache) -> Self {
        Self {
            cache: cache.id,
            seen: (0..CACHE_BLOCKS)
                .map(|_| Seen {
                    epoch: 0,
                    tag: None,
                    entry: None,
                })
                .collect(),
            hits: 0,
            misses: 0,
            counters: Arc::clone(&cache.counters),
        }
    }

    fn count(&mut self, hit: bool) {
        if hit {
            self.hits += 1;
        } else {
            self.misses += 1;
        }
        if (self.hits + self.misses).is_multiple_of(FLUSH_EVERY) {
            self.flush();
        }
    }

    fn flush(&mut self) {
        if self.hits > 0 {
            self.counters.hits.fetch_add(self.hits, Ordering::Relaxed);
            self.hits = 0;
        }
        if self.misses > 0 {
            self.counters
                .misses
                .fetch_add(self.misses, Ordering::Relaxed);
            self.misses = 0;
        }
    }
}

impl Drop for View {
    fn drop(&mut self) {
        self.flush();
    }
}

thread_local! {
    /// This thread's view of the cache it last used. A thread reads one
    /// store at a time in practice; one that alternates between two
    /// rebuilds its view on each switch, and is still correct.
    static VIEW: RefCell<Option<View>> = const { RefCell::new(None) };
}

/// Names each cache, so that a thread's view is never applied to another
/// cache's slots.
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// The eight slots a block can be in, as their writers see them.
struct Window {
    /// Where CLOCK looks next.
    hand: usize,
    /// Whose block each slot holds: what `entries` says, kept side by
    /// side so that looking a block up among them reads one cache line
    /// and no entry.
    tags: [Option<(u32, u64)>; WINDOW],
    entries: [Option<Arc<Entry>>; WINDOW],
}

impl Window {
    /// Puts `entry` (or nothing) in slot `at`, tag and all.
    fn set(&mut self, at: usize, entry: Option<Arc<Entry>>) {
        if let (Some(tag), Some(slot)) = (self.tags.get_mut(at), self.entries.get_mut(at)) {
            *tag = entry.as_ref().map(|e| (e.table_id, e.block_id));
            *slot = entry;
        }
    }
}

/// The block cache the store's readers share.
pub struct SharedCache {
    id: u64,
    /// One epoch per slot, bumped (under its window's lock) whenever the
    /// slot's entry changes. A window's eight epochs share a cache line.
    epochs: Box<[AtomicU64]>,
    /// The slots, a window to a lock. Locked to change an entry, and by
    /// a reader whose view of the window is stale. A hit on a current
    /// view does not come here.
    windows: Box<[Mutex<Window>]>,
    len: AtomicUsize,
    counters: Arc<Counters>,
}

impl SharedCache {
    /// An empty cache.
    pub fn new() -> Self {
        Self {
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
            epochs: (0..CACHE_BLOCKS).map(|_| AtomicU64::new(0)).collect(),
            windows: (0..CACHE_BLOCKS / WINDOW)
                .map(|_| {
                    Mutex::new(Window {
                        hand: 0,
                        tags: [None; WINDOW],
                        entries: [const { None }; WINDOW],
                    })
                })
                .collect(),
            len: AtomicUsize::new(0),
            counters: Arc::new(Counters::default()),
        }
    }

    /// The first slot of the window `(table_id, block_id)` lives in.
    fn window(table_id: u32, block_id: u64) -> usize {
        let h = (block_id.rotate_left(17) ^ u64::from(table_id))
            .wrapping_mul(0xD6E8_FEB8_6659_FD93)
            >> 40;
        // The remainder is below the window count: the cast is exact.
        #[allow(clippy::cast_possible_truncation)]
        let window = (h % (CACHE_BLOCKS / WINDOW) as u64) as usize;
        window * WINDOW
    }

    /// Runs `f` on this thread's view of this cache. `None` while the
    /// thread is being torn down and its view is gone.
    fn with_view<R>(&self, f: impl FnOnce(&mut View) -> R) -> Option<R> {
        VIEW.try_with(|cell| {
            let mut cell = cell.borrow_mut();
            let view = match &mut *cell {
                Some(view) if view.cache == self.id => view,
                other => other.insert(View::new(self)),
            };
            f(view)
        })
        .ok()
    }

    /// Looks `(table_id, block_id)` up and, on a hit, runs `read` on its
    /// entry. `read` copies what the caller asked for and reports
    /// whether the entry's checksums still held; if they did not, the
    /// entry is dropped and the lookup is a miss.
    ///
    /// The hit path writes only to this thread's view: it loads epochs
    /// and, when it finds the reference bit clear, sets it.
    fn lookup(&self, table_id: u32, block_id: u64, read: impl FnOnce(&Entry) -> bool) -> bool {
        self.with_view(|view| {
            let Some(i) = self.find(view, table_id, block_id) else {
                view.count(false);
                return false;
            };
            let Some(entry) = view.seen.get(i).and_then(|seen| seen.entry.as_ref()) else {
                view.count(false);
                return false;
            };
            if read(entry) {
                if !entry.used.load(Ordering::Relaxed) {
                    entry.used.store(true, Ordering::Relaxed);
                }
                view.count(true);
                return true;
            }
            // The image changed in RAM (ADR-0016): never serve it, and
            // let the caller read the block from the device again.
            self.reject(i, entry);
            if let Some(seen) = view.seen.get_mut(i) {
                seen.tag = None;
                seen.entry = None;
            }
            self.counters.rejected.fetch_add(1, Ordering::Relaxed);
            view.count(false);
            false
        })
        .unwrap_or(false)
    }

    /// The slot holding `(table_id, block_id)`, with this thread's
    /// reference to its entry in place; `None` when the block is not
    /// cached.
    fn find(&self, view: &mut View, table_id: u32, block_id: u64) -> Option<usize> {
        let start = Self::window(table_id, block_id);
        let key = Some((table_id, block_id));
        let epochs = self.epochs.get(start..start + WINDOW)?;
        let seen = view.seen.get_mut(start..start + WINDOW)?;
        // The whole window's epochs: one cache line, loaded, not locked.
        let current = seen
            .iter()
            .zip(epochs)
            .all(|(seen, epoch)| seen.epoch == epoch.load(Ordering::Acquire));
        if current {
            let at = seen.iter().position(|seen| seen.tag == key)?;
            if seen.get(at)?.entry.is_some() {
                // The hit: nothing shared was written to get here.
                return Some(start + at);
            }
        }
        // A slot changed since this thread looked, or this is its first
        // use of the block: look again, under the window's lock. Epochs
        // only move under that lock, so each one read here belongs with
        // the entry beside it.
        let window = lock(self.windows.get(start / WINDOW)?);
        for ((seen, epoch), tag) in seen.iter_mut().zip(epochs).zip(&window.tags) {
            let now = epoch.load(Ordering::Acquire);
            if seen.epoch != now {
                seen.epoch = now;
                seen.tag = *tag;
                seen.entry = None;
            }
        }
        let at = seen.iter().position(|seen| seen.tag == key)?;
        let seen = seen.get_mut(at)?;
        if seen.entry.is_none() {
            seen.entry.clone_from(window.entries.get(at)?);
        }
        drop(window);
        seen.entry.as_ref().map(|_| start + at)
    }

    /// Empties slot `i` if it still holds `entry`.
    #[allow(
        clippy::significant_drop_tightening,
        reason = "the epoch moves under the window's lock, with the entry"
    )]
    fn reject(&self, i: usize, entry: &Arc<Entry>) {
        let (Some(window), Some(epoch)) = (self.windows.get(i / WINDOW), self.epochs.get(i)) else {
            return;
        };
        let mut window = lock(window);
        let at = i % WINDOW;
        let held = window.entries.get(at).and_then(Option::as_ref);
        if held.is_some_and(|now| Arc::ptr_eq(now, entry)) {
            window.set(at, None);
            epoch.fetch_add(1, Ordering::Release);
            self.len.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Caches a block in its window: nowhere if it is already there,
    /// else in an empty slot, else in the first slot the CLOCK hand
    /// finds whose entry has not been used since the hand last passed.
    ///
    /// An evicted entry that no view still holds is made over in place.
    /// That is every block a scan streamed through and nobody read
    /// again, so a scan does not go to the allocator for each block.
    #[allow(
        clippy::significant_drop_tightening,
        reason = "the epoch moves under the window's lock, with the entry"
    )]
    fn insert(&self, table_id: u32, block_id: u64, image: &[u8; BLOCK], hot: bool) {
        let start = Self::window(table_id, block_id);
        let Some(window) = self.windows.get(start / WINDOW) else {
            return;
        };
        let mut window = lock(window);
        if window.tags.contains(&Some((table_id, block_id))) {
            return;
        }
        let at = window
            .tags
            .iter()
            .position(Option::is_none)
            .unwrap_or_else(|| {
                // Two turns of the hand always find a victim: the first
                // clears every bit it passes.
                let hand = window.hand;
                (0..2 * WINDOW)
                    .map(|step| (hand + step) % WINDOW)
                    .find(|&at| {
                        let spared = window
                            .entries
                            .get(at)
                            .and_then(Option::as_ref)
                            .is_some_and(|e| e.used.swap(false, Ordering::Relaxed));
                        !spared
                    })
                    .unwrap_or(hand % WINDOW)
            });
        let Some(epoch) = self.epochs.get(start + at) else {
            return;
        };
        let old = window.entries.get_mut(at).and_then(Option::take);
        if old.is_none() {
            self.len.fetch_add(1, Ordering::Relaxed);
        }
        // This thread's own view may hold the entry it is replacing (a
        // scan evicts blocks it read a page ago). It lets go first, so
        // that an entry no other thread holds can be made over.
        let _ = self.with_view(|view| {
            if let Some(seen) = view.seen.get_mut(start + at) {
                seen.entry = None;
            }
        });
        let reused = old.and_then(|mut old| {
            // Another view may still read the old entry: it keeps it.
            Arc::get_mut(&mut old)?.refill(table_id, block_id, image, hot);
            Some(old)
        });
        let entry = reused.unwrap_or_else(|| Arc::new(Entry::new(table_id, block_id, image, hot)));
        window.set(at, Some(entry));
        let now = epoch.fetch_add(1, Ordering::Release).wrapping_add(1);
        window.hand = (at + 1) % WINDOW;
        // And it knows what the slot holds now, without looking again.
        let _ = self.with_view(|view| {
            if let Some(seen) = view.seen.get_mut(start + at) {
                seen.epoch = now;
                seen.tag = Some((table_id, block_id));
            }
        });
    }
}

impl CachePort<BLOCK> for SharedCache {
    fn get_into(&self, table_id: u32, block_id: u64, out: &mut [u8; BLOCK]) -> bool {
        self.lookup(table_id, block_id, |entry| {
            out.copy_from_slice(&entry.image);
            // Sum the copy, not the entry: the copy is what the caller
            // parses.
            entry.intact(out, 0, BLOCK)
        })
    }

    fn get_range_into(&self, table_id: u32, block_id: u64, offset: usize, out: &mut [u8]) -> bool {
        let Some(end) = offset.checked_add(out.len()).filter(|&end| end <= BLOCK) else {
            return false;
        };
        self.lookup(table_id, block_id, |entry| {
            let Some(bytes) = entry.image.get(offset..end) else {
                return false;
            };
            out.copy_from_slice(bytes);
            // Summed where it lies: copying each chunk out to sum it
            // would cost what the range read exists to save.
            entry.intact(&entry.image, offset, end)
        })
    }

    /// Lends the entry's bytes in place. An entry is immutable and this
    /// thread holds a reference to it, so the bytes it sums are the
    /// bytes `read` parses, as with a copy, without the copy.
    fn with_range(
        &self,
        table_id: u32,
        block_id: u64,
        from: usize,
        to: usize,
        read: &mut dyn FnMut(&[u8]),
    ) -> Option<bool> {
        if from > to || to > BLOCK {
            return Some(false);
        }
        Some(self.lookup(table_id, block_id, |entry| {
            let Some(bytes) = entry.image.get(from..to) else {
                return false;
            };
            if !entry.intact(&entry.image, from, to) {
                return false;
            }
            read(bytes);
            true
        }))
    }

    /// Lends the whole image unchecked, with a `check` that sums only
    /// the chunks `read` asks about, each once (ADR-0031). A point lookup
    /// in a raw block asks about its trailer, the restarts its binary
    /// search parses and the run it scans: about 8 of the 64 chunks
    /// (docs/THROUGHPUT.md, step 22), where a whole-image check sums all
    /// of them.
    fn with_checked(
        &self,
        table_id: u32,
        block_id: u64,
        read: &mut CheckedRead<'_>,
    ) -> Option<bool> {
        Some(self.lookup(table_id, block_id, |entry| {
            let mut ok = true;
            let mut done = [false; SUMS];
            read(&entry.image, &mut |from, to| {
                if to <= from {
                    return ok;
                }
                if to > BLOCK {
                    ok = false;
                }
                if !ok {
                    return false;
                }
                let (first, last) = (from / SUM_BYTES, (to - 1) / SUM_BYTES);
                let chunks = entry.image.chunks(SUM_BYTES).zip(&entry.sums);
                for (seen, (chunk, sum)) in done.iter_mut().zip(chunks).take(last + 1).skip(first) {
                    if !*seen {
                        if image_sum(chunk) != *sum {
                            ok = false;
                            return false;
                        }
                        *seen = true;
                    }
                }
                true
            });
            ok
        }))
    }

    /// `with_checked` sums only the chunks it is asked about, so a scan
    /// takes just the ranges it reads from a lent block (ADR-0035).
    fn checks_ranges(&self) -> bool {
        true
    }

    fn put(&self, table_id: u32, block_id: u64, block: &[u8; BLOCK], hot: bool) {
        self.insert(table_id, block_id, block, hot);
    }

    #[allow(
        clippy::significant_drop_tightening,
        reason = "the epoch moves under the window's lock, with the entry"
    )]
    fn invalidate_table(&self, table_id: u32) {
        for (w, window) in self.windows.iter().enumerate() {
            let mut window = lock(window);
            for at in 0..WINDOW {
                let of_table = window
                    .tags
                    .get(at)
                    .is_some_and(|tag| tag.is_some_and(|(t, _)| t == table_id));
                if of_table {
                    window.set(at, None);
                    if let Some(epoch) = self.epochs.get(w * WINDOW + at) {
                        epoch.fetch_add(1, Ordering::Release);
                    }
                    self.len.fetch_sub(1, Ordering::Relaxed);
                }
            }
        }
    }

    /// The totals. Each thread's last few lookups (under
    /// [`FLUSH_EVERY`]) are added when it next flushes or ends; the
    /// calling thread's are added now.
    fn stats(&self) -> CacheStats {
        let _ = self.with_view(View::flush);
        CacheStats {
            hits: self.counters.hits.load(Ordering::Relaxed),
            misses: self.counters.misses.load(Ordering::Relaxed),
            rejected: self.counters.rejected.load(Ordering::Relaxed),
            len: self.len.load(Ordering::Relaxed),
            capacity: CACHE_BLOCKS,
        }
    }
}

#[cfg(test)]
impl SharedCache {
    /// Drops every entry, as [`CachePort::invalidate_table`] drops a
    /// table's: the next read of any block goes to the device.
    #[allow(
        clippy::significant_drop_tightening,
        reason = "the epoch moves under the window's lock, with the entry"
    )]
    pub fn clear(&self) {
        for (w, window) in self.windows.iter().enumerate() {
            let mut window = lock(window);
            for at in 0..WINDOW {
                if window.tags.get(at).is_some_and(Option::is_some) {
                    window.set(at, None);
                    if let Some(epoch) = self.epochs.get(w * WINDOW + at) {
                        epoch.fetch_add(1, Ordering::Release);
                    }
                    self.len.fetch_sub(1, Ordering::Relaxed);
                }
            }
        }
    }

    /// Caches `(table_id, block_id)` with an image that no longer
    /// matches its checksums at byte `at`, as a bit flipped in RAM would
    /// leave it.
    #[allow(
        clippy::significant_drop_tightening,
        reason = "a test helper: the entry is borrowed from the locked window"
    )]
    fn put_damaged(&self, table_id: u32, block_id: u64, block: &[u8; BLOCK], at: usize) {
        self.insert(table_id, block_id, block, true);
        let start = Self::window(table_id, block_id);
        let mut window = lock(&self.windows[start / WINDOW]);
        let entry = window
            .entries
            .iter_mut()
            .flatten()
            .find(|e| e.is(table_id, block_id))
            .and_then(Arc::get_mut)
            .expect("the entry, held by no view yet");
        entry.image[at] ^= 0x10;
    }

    /// Every lock in the cache, held: what a hit must not need.
    fn lock_everything(&self) -> Vec<MutexGuard<'_, Window>> {
        self.windows.iter().map(lock).collect()
    }

    fn epochs(&self) -> Vec<u64> {
        self.epochs
            .iter()
            .map(|e| e.load(Ordering::Acquire))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::time::Duration;

    use super::*;

    /// The image of block `(t, b)`: every byte follows from the key, so
    /// a reader can tell a right answer from a wrong one.
    fn image(t: u32, b: u64) -> Box<[u8; BLOCK]> {
        let mut img = Box::new([0u8; BLOCK]);
        let mut x = (u64::from(t) << 32 | b).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        for chunk in img.chunks_mut(8) {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            chunk.copy_from_slice(&x.to_le_bytes()[..chunk.len()]);
        }
        img
    }

    fn whole(cache: &SharedCache, t: u32, b: u64) -> Option<Box<[u8; BLOCK]>> {
        let mut out = Box::new([0u8; BLOCK]);
        cache.get_into(t, b, &mut out).then_some(out)
    }

    fn range(cache: &SharedCache, t: u32, b: u64, at: usize, len: usize) -> Option<Vec<u8>> {
        let mut out = vec![0u8; len];
        cache.get_range_into(t, b, at, &mut out).then_some(out)
    }

    #[test]
    fn a_cached_block_reads_back_whole_and_in_ranges() {
        let cache = SharedCache::new();
        assert!(whole(&cache, 1, 7).is_none());
        cache.put(1, 7, &image(1, 7), true);
        cache.put(2, 7, &image(2, 7), false);
        assert_eq!(whole(&cache, 1, 7), Some(image(1, 7)));
        assert_eq!(whole(&cache, 2, 7), Some(image(2, 7)));
        assert!(whole(&cache, 1, 8).is_none());
        for (at, len) in [
            (0, 49),
            (64, 64),
            (255, 2),
            (BLOCK - 64, 64),
            (100, 0),
            (0, BLOCK),
        ] {
            assert_eq!(
                range(&cache, 1, 7, at, len).as_deref(),
                Some(&image(1, 7)[at..at + len]),
                "{len} bytes at {at}"
            );
        }
        // A range that is not inside a block is a miss, not a panic.
        assert!(range(&cache, 1, 7, BLOCK - 10, 11).is_none());
        assert!(range(&cache, 1, 7, usize::MAX, 2).is_none());
        let s = cache.stats();
        assert_eq!((s.len, s.capacity, s.rejected), (2, CACHE_BLOCKS, 0));
        assert_eq!((s.hits, s.misses), (8, 2), "range errors are not lookups");
    }

    /// The point of the design. Once a thread has used a block, hitting
    /// it again needs none of the cache's locks and changes none of its
    /// epochs: readers finish their hits while this thread holds every
    /// lock there is.
    #[test]
    fn a_hit_takes_no_lock_and_changes_nothing_shared() {
        const KEYS: u64 = 40;
        const READERS: usize = 4;
        let cache = Arc::new(SharedCache::new());
        for b in 0..KEYS {
            cache.put(3, b, &image(3, b), true);
        }
        let (warm_tx, warm_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let mut go = Vec::new();
        let readers: Vec<_> = (0..READERS)
            .map(|_| {
                let cache = Arc::clone(&cache);
                let (warm, done) = (warm_tx.clone(), done_tx.clone());
                let (go_tx, go_rx) = mpsc::channel::<()>();
                go.push(go_tx);
                std::thread::spawn(move || {
                    let pass = |cache: &SharedCache| {
                        for b in 0..KEYS {
                            assert_eq!(whole(cache, 3, b), Some(image(3, b)));
                            let at = (usize::try_from(b).unwrap() * 331) % (BLOCK - 64);
                            assert_eq!(
                                range(cache, 3, b, at, 64).as_deref(),
                                Some(&image(3, b)[at..at + 64])
                            );
                        }
                    };
                    // First use: the view fills, under the slots' locks.
                    pass(&cache);
                    warm.send(()).unwrap();
                    go_rx.recv().unwrap();
                    // And now with every lock taken.
                    for _ in 0..50 {
                        pass(&cache);
                    }
                    done.send(()).unwrap();
                })
            })
            .collect();
        for _ in 0..READERS {
            warm_rx.recv_timeout(Duration::from_secs(60)).unwrap();
        }
        let before = cache.epochs();
        {
            let _held = cache.lock_everything();
            for tx in &go {
                tx.send(()).unwrap();
            }
            for _ in 0..READERS {
                done_rx
                    .recv_timeout(Duration::from_secs(20))
                    .expect("a hit waited for a lock");
            }
        }
        for r in readers {
            r.join().unwrap();
        }
        assert_eq!(cache.epochs(), before, "a hit changed an epoch");
        // Every reader's counts arrived when its thread ended.
        let s = cache.stats();
        assert_eq!(s.hits, (READERS as u64) * 51 * KEYS * 2);
        assert_eq!((s.misses, s.rejected), (0, 0));
    }

    /// A window holds eight blocks. A ninth evicts one that has not been
    /// used since the hand last passed, and spares the ones that have.
    #[test]
    fn eviction_spares_what_is_in_use() {
        let cache = SharedCache::new();
        // Nine blocks of one window.
        let start = SharedCache::window(5, 0);
        let same: Vec<u64> = (0..100_000u64)
            .filter(|&b| SharedCache::window(5, b) == start)
            .take(WINDOW + 1)
            .collect();
        assert_eq!(same.len(), WINDOW + 1);
        // Scans insert cold: none of the eight is marked in use.
        for &b in &same[..WINDOW] {
            cache.put(5, b, &image(5, b), false);
        }
        // All but the fourth are then read. The hand starts at the
        // first slot, so it must pass three blocks in use to reach it.
        let unused = same[3];
        for &b in same[..WINDOW].iter().filter(|&&b| b != unused) {
            assert!(whole(&cache, 5, b).is_some());
        }
        cache.put(5, same[WINDOW], &image(5, same[WINDOW]), true);
        assert_eq!(cache.stats().len, WINDOW);
        assert!(whole(&cache, 5, unused).is_none(), "the unused block went");
        for &b in same.iter().filter(|&&b| b != unused) {
            assert_eq!(whole(&cache, 5, b), Some(image(5, b)), "block {b}");
        }
        // Inserting what is already cached changes nothing.
        let epochs = cache.epochs();
        cache.put(5, same[WINDOW], &image(5, same[WINDOW]), true);
        assert_eq!(cache.epochs(), epochs);
        assert_eq!(cache.stats().len, WINDOW);
    }

    /// An evicted entry that no other thread's view holds is made over
    /// in place for the block that takes its slot, also when the thread
    /// doing the evicting had read it: a scan streaming blocks through a
    /// full cache allocates nothing. An entry another thread holds is
    /// left to that thread, and the newcomer gets an entry of its own.
    #[test]
    fn an_evicted_entry_is_reused_unless_another_thread_holds_it() {
        let cache = Arc::new(SharedCache::new());
        let start = SharedCache::window(6, 0);
        let same: Vec<u64> = (0..200_000u64)
            .filter(|&b| SharedCache::window(6, b) == start)
            .take(WINDOW + 2)
            .collect();
        let entry_at = |at: usize| {
            let window = lock(&cache.windows[start / WINDOW]);
            Arc::as_ptr(window.entries[at].as_ref().expect("a full window"))
        };
        for &b in &same[..WINDOW] {
            cache.put(6, b, &image(6, b), false);
        }
        // Nobody has read the first block: the ninth takes its slot and
        // its memory.
        let first = entry_at(0);
        cache.put(6, same[WINDOW], &image(6, same[WINDOW]), false);
        assert_eq!(entry_at(0), first, "the unheld entry was made over");
        assert_eq!(whole(&cache, 6, same[WINDOW]), Some(image(6, same[WINDOW])));

        // This thread reads the second block, so its view holds it, and
        // then evicts it (the hand is there): still made over in place.
        assert!(whole(&cache, 6, same[1]).is_some());
        // A read marks it in use; the hand must find it unused to take
        // it, so every other block is read too and the hand comes round.
        for &b in &same[2..WINDOW] {
            assert!(whole(&cache, 6, b).is_some());
        }
        let second = entry_at(1);
        cache.put(6, same[WINDOW + 1], &image(6, same[WINDOW + 1]), false);
        assert_eq!(entry_at(1), second, "this thread's own hold is no hold");
        // And the thread knows its insert without looking again.
        let epochs = cache.epochs();
        assert!(whole(&cache, 6, same[1]).is_none());
        assert_eq!(
            whole(&cache, 6, same[WINDOW + 1]),
            Some(image(6, same[WINDOW + 1]))
        );
        assert_eq!(cache.epochs(), epochs);

        // Another thread reads the third block and stays alive, holding
        // it. Evicting it must leave that entry alone.
        let (held_tx, held_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let other = Arc::clone(&cache);
        let third_block = same[2];
        let holder = std::thread::spawn(move || {
            assert_eq!(whole(&other, 6, third_block), Some(image(6, third_block)));
            held_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        held_rx.recv_timeout(Duration::from_secs(60)).unwrap();
        let third = entry_at(2);
        // More blocks of this window turn the hand until it takes the
        // third slot: once round to clear the bit the read set, then in.
        let turn: Vec<u64> = (300_000..900_000u64)
            .filter(|&b| SharedCache::window(6, b) == start)
            .take(3 * WINDOW)
            .collect();
        let mut replaced = false;
        for &b in &turn {
            cache.put(6, b, &image(6, b), false);
            let tag = lock(&cache.windows[start / WINDOW]).tags[2];
            if tag != Some((6, third_block)) {
                assert_ne!(entry_at(2), third, "a held entry was written over");
                replaced = true;
                break;
            }
        }
        assert!(replaced, "the hand never took the third slot");
        release_tx.send(()).unwrap();
        holder.join().unwrap();
        assert_eq!(cache.stats().len, WINDOW);
    }

    /// A thread whose view of a slot is stale finds out at its next
    /// lookup: the block that was evicted is a miss, the block that took
    /// its slot is a hit, and the bytes are each block's own.
    #[test]
    fn a_stale_view_is_refreshed_not_trusted() {
        let cache = Arc::new(SharedCache::new());
        let start = SharedCache::window(9, 0);
        let same: Vec<u64> = (0..200_000u64)
            .filter(|&b| SharedCache::window(9, b) == start)
            .take(3 * WINDOW)
            .collect();
        for &b in &same[..WINDOW] {
            cache.put(9, b, &image(9, b), false);
            assert_eq!(whole(&cache, 9, b), Some(image(9, b)));
        }
        // Another thread replaces the whole window, twice over.
        let other = Arc::clone(&cache);
        let later = same[WINDOW..].to_vec();
        std::thread::spawn(move || {
            for &b in &later {
                other.put(9, b, &image(9, b), false);
            }
        })
        .join()
        .unwrap();
        for &b in &same[..2 * WINDOW] {
            assert!(whole(&cache, 9, b).is_none(), "block {b} was evicted");
        }
        for &b in &same[2 * WINDOW..] {
            assert_eq!(whole(&cache, 9, b), Some(image(9, b)), "block {b}");
        }
        assert_eq!(cache.stats().len, WINDOW);
    }

    /// ADR-0016 holds here: an image that no longer matches its
    /// checksums is never served. The entry is dropped, the lookup is a
    /// miss, and the block can be cached afresh. Only the chunks a range
    /// touches are checked, so a range elsewhere in the block is served.
    #[test]
    fn an_image_that_changed_in_ram_is_never_served() {
        let cache = SharedCache::new();
        let img = image(4, 1);
        cache.put_damaged(4, 1, &img, 1000);
        // A range in another chunk does not see the damage.
        assert_eq!(range(&cache, 4, 1, 0, 64).as_deref(), Some(&img[..64]));
        // The damaged chunk does: bytes 768..1024.
        assert!(range(&cache, 4, 1, 1020, 8).is_none());
        let s = cache.stats();
        assert_eq!((s.rejected, s.len), (1, 0));
        assert!(whole(&cache, 4, 1).is_none(), "the entry is gone");
        // The same through a whole-block read.
        cache.put_damaged(4, 1, &img, 5);
        assert!(whole(&cache, 4, 1).is_none());
        assert_eq!(cache.stats().rejected, 2);
        cache.put(4, 1, &img, true);
        assert_eq!(whole(&cache, 4, 1), Some(img));
    }

    /// A checked lend sums only the chunks asked about: a check clear of
    /// the damage holds and the lookup is a hit; one over it fails, and
    /// the entry is dropped as a copying read drops it.
    #[test]
    fn a_checked_lend_fails_only_where_it_looks() {
        let cache = SharedCache::new();
        let img = image(6, 2);
        let lend = |b: u64, ranges: &[(usize, usize)]| {
            let mut answers = Vec::new();
            let hit = cache.with_checked(6, b, &mut |image, check| {
                assert_eq!(image.len(), BLOCK);
                answers.extend(ranges.iter().map(|&(from, to)| check(from, to)));
            });
            (hit, answers)
        };
        assert_eq!(lend(2, &[(0, 8)]), (Some(false), vec![]), "a miss");
        cache.put_damaged(6, 3, &img, 1000);
        assert_eq!(
            lend(3, &[(0, 700), (BLOCK - 10, BLOCK)]),
            (Some(true), vec![true, true]),
            "clear of the damage"
        );
        assert_eq!(
            lend(3, &[(0, 16), (999, 1001)]),
            (Some(false), vec![true, false]),
            "over it"
        );
        assert_eq!(cache.stats().rejected, 1);
        assert_eq!(lend(3, &[(0, 16)]), (Some(false), vec![]), "dropped");
    }

    /// A lent range is the entry's own bytes, checked: the bytes of the
    /// range on a hit, `read` never called on a miss or on damage, and
    /// a damaged entry dropped as a copying read drops it.
    #[test]
    fn a_lent_range_is_checked_like_a_copy() {
        let cache = SharedCache::new();
        let img = image(5, 2);
        let lend = |b: u64, from, to| {
            let mut got = None;
            let hit = cache.with_range(5, b, from, to, &mut |b| got = Some(b.to_vec()));
            assert_eq!(hit, Some(got.is_some()), "{from}..{to}: read iff hit");
            got
        };
        assert!(lend(2, 0, BLOCK).is_none());
        cache.put(5, 2, &img, true);
        assert_eq!(lend(2, 0, BLOCK).as_deref(), Some(&img[..]));
        assert_eq!(lend(2, 100, 300).as_deref(), Some(&img[100..300]));
        assert!(lend(2, 10, BLOCK + 1).is_none(), "outside the block");
        // A block no view holds yet, damaged in RAM.
        cache.put_damaged(5, 3, &img, 1000);
        assert_eq!(
            lend(3, 0, 64).as_deref(),
            Some(&img[..64]),
            "clear of the damage"
        );
        assert!(lend(3, 990, 1010).is_none(), "the damaged chunk");
        let s = cache.stats();
        assert_eq!((s.rejected, s.len), (1, 1));
    }

    /// The cache checks a lent image a range at a time, and says so: a
    /// scan then takes only the ranges it reads (ADR-0035).
    #[test]
    fn a_checked_lend_checks_ranges() {
        assert!(SharedCache::new().checks_ranges());
    }

    /// Two caches never answer for each other, even from one thread and
    /// for the same keys (a view belongs to one cache).
    #[test]
    fn a_view_belongs_to_one_cache() {
        let (a, b) = (SharedCache::new(), SharedCache::new());
        a.put(1, 1, &image(1, 1), true);
        b.put(1, 1, &image(7, 7), true);
        for _ in 0..3 {
            assert_eq!(whole(&a, 1, 1), Some(image(1, 1)));
            assert_eq!(whole(&b, 1, 1), Some(image(7, 7)));
            assert!(whole(&a, 2, 2).is_none() && whole(&b, 2, 2).is_none());
        }
        assert_eq!((a.stats().hits, a.stats().misses), (3, 3));
        assert_eq!((b.stats().hits, b.stats().misses), (3, 3));
    }

    #[test]
    fn a_retired_table_is_forgotten() {
        let cache = SharedCache::new();
        for b in 0..20 {
            cache.put(1, b, &image(1, b), true);
            cache.put(2, b, &image(2, b), true);
            assert!(whole(&cache, 1, b).is_some());
        }
        cache.invalidate_table(1);
        assert_eq!(cache.stats().len, 20);
        for b in 0..20 {
            assert!(whole(&cache, 1, b).is_none());
            assert_eq!(whole(&cache, 2, b), Some(image(2, b)));
        }
    }

    /// Readers and writers together, over many more blocks than the
    /// cache holds, so entries are evicted under readers that hold them.
    /// Every hit must be the right block's bytes, whole or in part, and
    /// the cache must never hold more than it has room for.
    #[test]
    #[allow(
        clippy::needless_collect,
        reason = "every worker is started before the first is joined"
    )]
    fn hits_are_right_while_entries_come_and_go() {
        const THREADS: u64 = 6;
        const BLOCKS: u64 = 3 * CACHE_BLOCKS as u64;
        let cache = Arc::new(SharedCache::new());
        let hits: u64 = std::thread::scope(|s| {
            let workers: Vec<_> = (0..THREADS)
                .map(|t| {
                    let cache = &cache;
                    s.spawn(move || {
                        let mut x = 0x2545_F491_4F6C_DD1D_u64.wrapping_mul(t + 1);
                        let mut hits = 0u64;
                        let mut out = Box::new([0u8; BLOCK]);
                        for step in 0..40_000u64 {
                            x ^= x << 13;
                            x ^= x >> 7;
                            x ^= x << 17;
                            // A hot tenth and a cold rest.
                            let b = if x.is_multiple_of(4) {
                                x % BLOCKS
                            } else {
                                x % (BLOCKS / 30)
                            };
                            let table = u32::try_from(b % 5).unwrap();
                            let want = image(table, b);
                            let found = if step % 3 == 0 {
                                let ok = cache.get_into(table, b, &mut out);
                                assert!(!ok || *out == *want, "block {b}");
                                ok
                            } else {
                                let at = usize::try_from(x >> 20).unwrap() % (BLOCK - 64);
                                let ok = cache.get_range_into(table, b, at, &mut out[..64]);
                                assert!(!ok || out[..64] == want[at..at + 64], "block {b}");
                                ok
                            };
                            if found {
                                hits += 1;
                            } else {
                                cache.put(table, b, &want, step % 2 == 0);
                            }
                        }
                        hits
                    })
                })
                .collect();
            workers.into_iter().map(|w| w.join().unwrap()).sum()
        });
        let s = cache.stats();
        assert!(s.len <= CACHE_BLOCKS && s.len > CACHE_BLOCKS / 2, "{s:?}");
        assert_eq!(s.rejected, 0);
        assert_eq!(s.hits, hits);
        assert_eq!(s.hits + s.misses, THREADS * 40_000);
        assert!(hits > THREADS * 10_000, "{hits} hits");
        // No block is cached twice.
        let windows = cache.lock_everything();
        let mut tags: Vec<(u32, u64)> = windows
            .iter()
            .flat_map(|w| w.entries.iter().flatten())
            .map(|e| (e.table_id, e.block_id))
            .collect();
        let n = tags.len();
        tags.sort_unstable();
        tags.dedup();
        assert_eq!(tags.len(), n);
        assert_eq!(n, s.len);
    }
}
