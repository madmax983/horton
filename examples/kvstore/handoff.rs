//! The compaction scratch's way between the engine and the compaction
//! thread (ADR-0024).
//!
//! A compaction job is mostly merging: reading the input tables and
//! writing the output's blocks. horton runs that part without the
//! database (`Compaction::merge_detached`), given the job's scratch, a
//! handle to the file and a copy of the manifest. Everything else a job
//! does (choosing its tables, sealing and committing an output, opening
//! the next) needs the database, and so the engine lock.
//!
//! So the scratch goes back and forth. Under the engine lock, a step
//! with the database leaves a job in its merge and the engine *lends*
//! the scratch here. The compaction thread takes it, merges without any
//! lock until the merge needs the database again, and gives it *back*.
//! Whichever thread next has the engine lock takes the next step.
//!
//! ```text
//!            lend                 next                  give_back
//!  Home ───────────▶ Lent ───────────────▶ Out ───────────────────▶ Back
//!   ▲                 │                                              │
//!   └── reclaim ──────┴──────────────────────────────────────────────┘
//! ```
//!
//! The engine can always have the scratch back ([`Handoff::reclaim`]): a
//! write that must make room cannot wait for a whole merge. While the
//! scratch is out it raises `recall`, and the compaction thread stops
//! after the quantum it is in, about one block's work.
//!
//! One holder at a time is the whole of the protocol: the scratch is a
//! value that is in exactly one of these places, so the merge and the
//! database's steps on it can never overlap. That is what horton asks of
//! a caller of `merge_detached`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Condvar, Mutex, MutexGuard, PoisonError};

/// Where the scratch is.
enum Baton<S, M> {
    /// With the engine.
    Home,
    /// Lent with a job in its merge, and not picked up yet. `M` is the
    /// manifest as of the lending.
    Lent(S, M),
    /// With the compaction thread, merging.
    Out,
    /// Back from a merge: what comes next needs the database.
    Back(S),
}

/// What the compaction thread should do next.
pub enum Next<S, M> {
    /// Merge with this scratch, by this manifest.
    Merge(S, M),
    /// Nothing is lent, and compaction may have work: look at the engine.
    Look,
    /// The store is closing.
    Stop,
}

/// See the module documentation. `S` is the scratch and `M` the
/// manifest copy that travels with it.
pub struct Handoff<S, M> {
    baton: Mutex<Baton<S, M>>,
    /// Signalled whenever the baton changes hands, and by
    /// [`wake`](Self::wake).
    moved: Condvar,
    /// The engine is waiting for the scratch.
    recall: AtomicBool,
    /// The last merge failed (its job has been given up on).
    failed: AtomicBool,
    /// The compaction thread has left ([`leave`](Self::leave)): nothing
    /// will pick a lent scratch up.
    gone: AtomicBool,
}

impl<S, M> Handoff<S, M> {
    /// A handoff whose scratch is with the engine.
    pub const fn new() -> Self {
        Self {
            baton: Mutex::new(Baton::Home),
            moved: Condvar::new(),
            recall: AtomicBool::new(false),
            failed: AtomicBool::new(false),
            gone: AtomicBool::new(false),
        }
    }

    fn baton(&self) -> MutexGuard<'_, Baton<S, M>> {
        self.baton.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Engine side: lends `scratch`, whose job is in its merge, with the
    /// manifest of this moment.
    pub fn lend(&self, scratch: S, manifest: M) {
        *self.baton() = Baton::Lent(scratch, manifest);
        self.moved.notify_all();
    }

    /// Engine side: true when the scratch is there for the taking and
    /// nobody else will move it: it has come back from a merge, or it is
    /// lent and the compaction thread has gone. [`reclaim`](Self::reclaim)
    /// then returns it without waiting.
    pub fn is_back(&self) -> bool {
        match *self.baton() {
            Baton::Back(_) => true,
            Baton::Lent(..) => self.gone.load(Ordering::Acquire),
            Baton::Home | Baton::Out => false,
        }
    }

    /// Compaction thread: true when a scratch is lent and waiting for
    /// it.
    pub fn is_lent(&self) -> bool {
        matches!(*self.baton(), Baton::Lent(..))
    }

    /// Engine side: takes the scratch back, waiting for the compaction
    /// thread to let go of it if it is merging (it stops after the
    /// quantum in hand). `None` if the scratch was never lent: the
    /// engine has it.
    pub fn reclaim(&self) -> Option<S> {
        let mut baton = self.baton();
        loop {
            match core::mem::replace(&mut *baton, Baton::Home) {
                Baton::Home => return None,
                Baton::Lent(scratch, _) | Baton::Back(scratch) => {
                    self.recall.store(false, Ordering::Release);
                    return Some(scratch);
                }
                Baton::Out => {
                    *baton = Baton::Out;
                    self.recall.store(true, Ordering::Release);
                    baton = self
                        .moved
                        .wait(baton)
                        .unwrap_or_else(PoisonError::into_inner);
                }
            }
        }
    }

    /// Engine side: waits for the merge to come back by itself, without
    /// recalling it, and takes the scratch. For a caller that needs the
    /// whole job done and can do nothing meanwhile: the merge is better
    /// left where it is running. If the compaction thread has gone, a
    /// lent scratch is taken back as it is (the caller merges). `None`
    /// if the scratch was never lent.
    pub fn await_back(&self) -> Option<S> {
        let mut baton = self.baton();
        loop {
            let gone = self.gone.load(Ordering::Acquire);
            match core::mem::replace(&mut *baton, Baton::Home) {
                Baton::Home => return None,
                Baton::Back(scratch) => return Some(scratch),
                Baton::Lent(scratch, _) if gone => return Some(scratch),
                waiting => {
                    *baton = waiting;
                    baton = self
                        .moved
                        .wait(baton)
                        .unwrap_or_else(PoisonError::into_inner);
                }
            }
        }
    }

    /// Compaction thread: waits for something to do. A lent scratch
    /// comes first; then `due`, the store's word that compaction may
    /// have work; `stop` ends the wait at once.
    pub fn next(&self, stop: &AtomicBool, due: &AtomicBool) -> Next<S, M> {
        let mut baton = self.baton();
        loop {
            if stop.load(Ordering::Acquire) {
                return Next::Stop;
            }
            if matches!(*baton, Baton::Lent(..))
                && let Baton::Lent(scratch, manifest) = core::mem::replace(&mut *baton, Baton::Out)
            {
                return Next::Merge(scratch, manifest);
            }
            if due.load(Ordering::Acquire) {
                return Next::Look;
            }
            baton = self
                .moved
                .wait(baton)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }

    /// Compaction thread: true when the engine wants the scratch back.
    /// Looked at between quanta.
    pub fn recalled(&self) -> bool {
        self.recall.load(Ordering::Acquire)
    }

    /// Compaction thread: gives the scratch back after a merge.
    pub fn give_back(&self, scratch: S) {
        *self.baton() = Baton::Back(scratch);
        self.moved.notify_all();
    }

    /// Wakes the compaction thread in [`next`](Self::next), after `stop`
    /// or `due` was raised. Takes the baton's lock, so the thread is
    /// either before its look at the flags or already waiting.
    pub fn wake(&self) {
        drop(self.baton());
        self.moved.notify_all();
    }

    /// Compaction thread: its last word. From here on a lent scratch is
    /// picked up by nobody, and [`await_back`](Self::await_back) hands
    /// it straight back to the engine. Raised under the baton's lock,
    /// so a waiter sees either the flag or the wake-up.
    pub fn leave(&self) {
        let baton = self.baton();
        self.gone.store(true, Ordering::Release);
        drop(baton);
        self.moved.notify_all();
    }

    /// Compaction thread: records that the merge failed.
    pub fn fail(&self) {
        self.failed.store(true, Ordering::Release);
    }

    /// Engine side: whether a merge failed since this was last asked.
    pub fn take_failed(&self) -> bool {
        self.failed.load(Ordering::Acquire) && self.failed.swap(false, Ordering::AcqRel)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    type H = Handoff<&'static str, u32>;

    fn flags() -> (AtomicBool, AtomicBool) {
        (AtomicBool::new(false), AtomicBool::new(false))
    }

    /// The scratch is in one place at a time, and goes round in order:
    /// lent, taken, given back, reclaimed.
    #[test]
    fn the_scratch_goes_round() {
        let h = H::new();
        let (stop, due) = flags();
        assert_eq!(h.reclaim(), None, "it was never lent");
        assert!(!h.is_back());
        assert!(!h.is_lent());
        h.lend("scratch", 7);
        assert!(!h.is_back());
        assert!(h.is_lent());
        let Next::Merge(scratch, manifest) = h.next(&stop, &due) else {
            panic!("a lent scratch was not handed over");
        };
        assert_eq!((scratch, manifest), ("scratch", 7));
        assert!(!h.is_back() && !h.is_lent());
        assert!(!h.recalled());
        h.give_back(scratch);
        assert!(h.is_back());
        assert_eq!(h.reclaim(), Some("scratch"));
        assert!(!h.is_back());
        assert_eq!(h.reclaim(), None);
    }

    /// A scratch nobody has picked up is taken back as it is.
    #[test]
    fn a_lent_scratch_can_be_taken_back_before_it_is_picked_up() {
        let h = H::new();
        h.lend("scratch", 1);
        assert_eq!(h.reclaim(), Some("scratch"));
        assert!(!h.recalled());
        // And the thread then finds nothing lent.
        let (stop, due) = flags();
        due.store(true, Ordering::Release);
        assert!(matches!(h.next(&stop, &due), Next::Look));
    }

    /// What the thread is told, in order of urgency: stop, then a lent
    /// scratch, then work to look for.
    #[test]
    fn next_puts_stop_first_and_a_lent_scratch_before_a_look() {
        let h = H::new();
        let (stop, due) = flags();
        due.store(true, Ordering::Release);
        h.lend("scratch", 1);
        assert!(matches!(h.next(&stop, &due), Next::Merge("scratch", 1)));
        h.give_back("scratch");
        assert!(matches!(h.next(&stop, &due), Next::Look));
        assert_eq!(h.reclaim(), Some("scratch"));
        h.lend("scratch", 2);
        stop.store(true, Ordering::Release);
        assert!(matches!(h.next(&stop, &due), Next::Stop));
        // Still lent: closing takes nothing.
        assert_eq!(h.reclaim(), Some("scratch"));
    }

    /// Reclaiming a scratch that is out raises `recall` and waits for
    /// the thread to give it back; and a failure is reported once.
    #[test]
    fn reclaim_waits_for_a_scratch_that_is_out() {
        let h = Arc::new(H::new());
        let (stop, due) = flags();
        h.lend("scratch", 1);
        let Next::Merge(scratch, _) = h.next(&stop, &due) else {
            panic!("not handed over");
        };
        let reclaimed = Arc::new(AtomicBool::new(false));
        let engine = {
            let (h, reclaimed) = (Arc::clone(&h), Arc::clone(&reclaimed));
            std::thread::spawn(move || {
                let scratch = h.reclaim();
                reclaimed.store(true, Ordering::SeqCst);
                scratch
            })
        };
        // The engine asks, and waits.
        let since = std::time::Instant::now();
        while !h.recalled() {
            assert!(since.elapsed() < Duration::from_secs(30), "no recall");
            std::thread::yield_now();
        }
        std::thread::sleep(Duration::from_millis(50));
        assert!(!reclaimed.load(Ordering::SeqCst), "reclaim did not wait");
        h.fail();
        h.give_back(scratch);
        assert_eq!(engine.join().expect("engine"), Some("scratch"));
        assert!(!h.recalled(), "the recall outlived the reclaim");
        assert!(h.take_failed());
        assert!(!h.take_failed());
    }

    /// Waiting for a merge does not interrupt it: no recall is raised,
    /// and the wait ends when the thread gives the scratch back.
    #[test]
    fn await_back_waits_without_recalling() {
        let h = Arc::new(H::new());
        let (stop, due) = flags();
        assert_eq!(h.await_back(), None, "it was never lent");
        h.lend("scratch", 1);
        let waiter = {
            let h = Arc::clone(&h);
            std::thread::spawn(move || h.await_back())
        };
        // Lent and not picked up: the waiter leaves it for the thread.
        std::thread::sleep(Duration::from_millis(50));
        assert!(!waiter.is_finished(), "the waiter took a lent scratch");
        let Next::Merge(scratch, _) = h.next(&stop, &due) else {
            panic!("not handed over");
        };
        std::thread::sleep(Duration::from_millis(50));
        assert!(!waiter.is_finished(), "the waiter did not wait");
        assert!(!h.recalled(), "waiting recalled the merge");
        h.give_back(scratch);
        assert_eq!(waiter.join().expect("waiter"), Some("scratch"));
    }

    /// With the compaction thread gone, nobody will pick a lent scratch
    /// up: the waiter takes it back as it is, whether the thread left
    /// before the wait began or during it.
    #[test]
    fn await_back_takes_a_lent_scratch_once_the_thread_has_gone() {
        for leaves_first in [true, false] {
            let h = Arc::new(H::new());
            let stop = Arc::new(AtomicBool::new(false));
            let due = AtomicBool::new(false);
            h.lend("scratch", 1);
            if leaves_first {
                stop.store(true, Ordering::Release);
                assert!(matches!(h.next(&stop, &due), Next::Stop));
                h.leave();
            }
            // Lent, and whether it is there for the taking depends on
            // whether anyone is left to pick it up.
            assert_eq!(h.is_back(), leaves_first);
            let waiter = {
                let h = Arc::clone(&h);
                std::thread::spawn(move || h.await_back())
            };
            if !leaves_first {
                std::thread::sleep(Duration::from_millis(50));
                assert!(!waiter.is_finished());
                stop.store(true, Ordering::Release);
                assert!(matches!(h.next(&stop, &due), Next::Stop));
                h.leave();
            }
            assert_eq!(waiter.join().expect("waiter"), Some("scratch"));
        }
    }

    /// A thread asleep in `next` is woken by a lending, and by `wake`
    /// once `stop` or `due` is up.
    #[test]
    fn a_waiting_thread_is_woken() {
        for what in 0..3 {
            let h = Arc::new(H::new());
            let stop = Arc::new(AtomicBool::new(false));
            let due = Arc::new(AtomicBool::new(false));
            let thread = {
                let (h, stop, due) = (Arc::clone(&h), Arc::clone(&stop), Arc::clone(&due));
                std::thread::spawn(move || match h.next(&stop, &due) {
                    Next::Merge(..) => 0,
                    Next::Look => 1,
                    Next::Stop => 2,
                })
            };
            std::thread::sleep(Duration::from_millis(20));
            match what {
                0 => h.lend("scratch", 1),
                1 => {
                    due.store(true, Ordering::Release);
                    h.wake();
                }
                _ => {
                    stop.store(true, Ordering::Release);
                    h.wake();
                }
            }
            assert_eq!(thread.join().expect("thread"), what);
        }
    }
}
