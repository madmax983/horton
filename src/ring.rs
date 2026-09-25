//! MPMC admission ring for the `multiwriter` feature.
//!
//! SPEC: `docs/multiwriter-spec.md`. This module implements SPEC §§1–5:
//! ticket/gate encoding, the live-window invariants, and the
//! claim/publish/drain protocol. Fencing (§6), the drainer integration, and
//! completion signaling arrive in later slices.
//!
//! Design recap: writers concurrently claim ring slots via a single
//! `compare_exchange` on the ticket counter (the ticket IS the sequence
//! order). A single drainer applies entries strictly in ticket order. The
//! ring is `Sync` through atomics alone — no `UnsafeCell`, no locks, no
//! `Arc` needed in-crate. Payloads are fixed 32-byte records (stand-in for
//! the serialized mutation).
//!
//! Ticket space: 31-bit values in `AtomicU32` cells, uniform on all targets
//! (`AtomicU64` does not exist on `xtensa-esp32s3-none-elf`; `AtomicU32`
//! CAS is real hardware there). Bit 31 is reserved for the fenced state, so
//! no legitimate gate value ever has it set.

use shim::{AtomicU32, Ordering};

/// Atomic + spin shim.
///
/// With the `loom` feature the ring compiles against Loom's modeled atomics
/// so `tests/loom_ring.rs` exercises the *same* claim/publish/drain code as
/// production. Otherwise this is plain `core::sync::atomic`.
mod shim {
    #[cfg(not(feature = "loom"))]
    pub use core::sync::atomic::{AtomicU32, Ordering};
    #[cfg(feature = "loom")]
    pub use loom::sync::atomic::{AtomicU32, Ordering};

    /// One spin iteration of a wait loop.
    ///
    /// The test/drain scaffolding spins here; the real poll-based drainer
    /// yields to the poll loop instead. Under Loom this is a scheduler yield
    /// so the model can make progress on other threads.
    #[cfg(feature = "loom")]
    pub use loom::thread::yield_now as spin_wait;
    #[cfg(not(feature = "loom"))]
    #[inline]
    pub fn spin_wait() {
        core::hint::spin_loop();
    }
}

/// Mask for the 31-bit ticket space: no legitimate gate value has bit 31 set.
pub const TICKET_MASK: u32 = 0x7FFF_FFFF;
/// Reserved bit marking the fenced state (`FENCED(t) = t | FENCED_BIT`).
const FENCED_BIT: u32 = 0x8000_0000;

/// Ticket following `t` in the 31-bit space.
#[inline]
const fn next_ticket(t: u32) -> u32 {
    (t.wrapping_add(1)) & TICKET_MASK
}

/// Gate value meaning "slot holds ticket `t`'s published payload".
#[inline]
const fn published_gate(t: u32) -> u32 {
    next_ticket(t)
}

/// Gate value the consumer writes after draining `t`: frees the slot for the
/// ticket `N` ahead (the next lap).
///
/// Cast-free: `N` is a power of two ≤ 2³¹ (SPEC §1 assert), so
/// `1u32 << N.trailing_zeros()` is exactly `N` as a `u32`, and
/// `(t + N) mod 2³¹ == ((t + N) mod 2³²) mod 2³¹` because 2³¹ divides 2³² —
/// hence `wrapping_add` followed by the mask.
#[inline]
const fn release_gate<const N: usize>(t: u32) -> u32 {
    t.wrapping_add(1u32 << N.trailing_zeros()) & TICKET_MASK
}

/// Outcome of [`Ring::publish`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PublishOutcome {
    /// The payload was published; the drainer will pick it up in ticket order.
    Published,
    /// The ticket was fenced before the publish won the gate (SPEC §6). The
    /// slot was self-released (or already released by the host); the writer
    /// must not touch the slot again. Surfaces as `Error::WriterFenced` at
    /// the `Db` integration layer.
    Fenced,
}

/// One ring slot: a sequence gate plus the fixed-size payload cells.
///
/// The payload lives in atomic cells so the whole ring is `Sync` without
/// `UnsafeCell`. Cell accesses are `Relaxed`; the producer's `Release`
/// publish and the consumer's `Acquire` gate load carry them.
struct Slot {
    gate: AtomicU32,
    cells: [AtomicU32; 8],
}

impl Slot {
    // NOTE: intentionally not `const fn` (clippy nursery's
    // `missing_const_for_fn` suggests it): under the `loom` feature this
    // compiles against loom's modeled atomics, whose `AtomicU32::new` is
    // not const, so a `const fn` here breaks the loom build.
    #[allow(clippy::missing_const_for_fn)]
    fn new() -> Self {
        Self {
            // The gate is seeded by `Ring::with_head`; zero here is just a
            // placeholder (no ticket-0 claim can observe it: every slot's
            // gate is overwritten before the ring is shared).
            gate: AtomicU32::new(0),
            cells: [
                AtomicU32::new(0),
                AtomicU32::new(0),
                AtomicU32::new(0),
                AtomicU32::new(0),
                AtomicU32::new(0),
                AtomicU32::new(0),
                AtomicU32::new(0),
                AtomicU32::new(0),
            ],
        }
    }

    /// Store a 32-byte record. Caller must own the slot (the ring protocol
    /// guarantees exclusive ownership); the `Relaxed` stores become visible
    /// via the caller's `Release` publish.
    fn write_payload(&self, payload: &[u8; 32]) {
        let mut i = 0;
        while i < 8 {
            let o = 4 * i;
            let w = u32::from(payload[o])
                | (u32::from(payload[o + 1]) << 8)
                | (u32::from(payload[o + 2]) << 16)
                | (u32::from(payload[o + 3]) << 24);
            self.cells[i].store(w, Ordering::Relaxed);
            i += 1;
        }
    }

    /// Load a 32-byte record. Caller must have observed the corresponding
    /// `Release` publish via an `Acquire` gate load first.
    fn read_payload(&self) -> [u8; 32] {
        let mut p = [0u8; 32];
        let mut i = 0;
        while i < 8 {
            let wb = self.cells[i].load(Ordering::Relaxed).to_le_bytes();
            p[4 * i..4 * i + 4].copy_from_slice(&wb);
            i += 1;
        }
        p
    }
}

/// MPMC admission ring with `N` slots.
///
/// `N` MUST be a power of two in `[2, 2³¹]` (compile-time asserted). The slot
/// index is `ticket % N` on the 31-bit ticket; for non-power-of-two `N` a
/// live window straddling the 2³¹ wrap maps two live tickets to one slot
/// (`2³¹ mod N ≠ 0`), breaking slot exclusivity. Powers of two divide 2³¹,
/// so every `N`-ticket window is a permutation of the slots — wrap-safe by
/// construction. (`N == 1` is additionally unsound: publish and release
/// collide on the gate encoding.)
///
/// The ring is `Sync` (shared `&Ring` across writer threads) and the drain
/// side is single-consumer: exactly one drainer calls [`Ring::drain`].
pub struct Ring<const N: usize> {
    /// Next ticket to hand out; always masked to 31 bits. Ticket uniqueness
    /// comes from the claim CAS — `Relaxed` is enough.
    head: AtomicU32,
    /// Next ticket to drain; always masked to 31 bits. Single consumer, so
    /// `Relaxed` is enough.
    cursor: AtomicU32,
    slots: [Slot; N],
}

impl<const N: usize> Default for Ring<N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const N: usize> Ring<N> {
    /// Compile-time assertion (SPEC §1): `N` must be a power of two, at least
    /// 2, at most 2³¹ — see the `Ring` docs for why a non-power-of-two `N`
    /// breaks slot exclusivity across the 2³¹ wrap, and `release_gate` for
    /// why the upper bound exists. Referenced by `with_head` so every
    /// monomorphization evaluates it; a bad `N` is a compile error, never a
    /// runtime panic.
    const ASSERT_N_POWER_OF_TWO: () = assert!(N.is_power_of_two() && N >= 2 && N <= (1 << 31));

    /// Create a ring with the ticket counter starting at 0.
    #[must_use]
    pub fn new() -> Self {
        Self::with_head(0)
    }

    /// Create a ring with the ticket counter starting at `seed`
    /// (SPEC O3: reseed from the recovered `next_seq` on `open()`).
    /// The seed is masked to 31 bits — bit 31 is the reserved
    /// `FENCED_BIT` and can never be a ticket.
    #[must_use]
    pub fn new_seeded(seed: u32) -> Self {
        Self::with_head(seed & TICKET_MASK)
    }

    fn with_head(head: u32) -> Self {
        // Evaluate the SPEC §1 compile-time assertion for this `N`.
        let () = Self::ASSERT_N_POWER_OF_TWO;
        let slots: [Slot; N] = core::array::from_fn(|_| Slot::new());
        // The tickets `head..head+N` (mod 2³¹) are initially claimable: seed
        // each of their slots' gates to FREE(t). With N | 2³¹ the residues
        // `t % N` are a permutation of the slots — no two tickets share one.
        let mut t = head;
        let mut i = 0;
        while i < N {
            slots[(t as usize) % N].gate.store(t, Ordering::Relaxed);
            t = next_ticket(t);
            i += 1;
        }
        Self {
            head: AtomicU32::new(head),
            cursor: AtomicU32::new(head),
            slots,
        }
    }

    /// Try to claim a slot. Check-then-CAS, single attempt (SPEC §3):
    /// load the head ticket; if its slot's gate is not `FREE(head)` return
    /// `None`; else one `compare_exchange` to advance the head.
    ///
    /// A failed claim consumes nothing — no ticket, no sequence number, no
    /// slot state changes — which is what makes `Error::NoSpace` honest.
    /// No retry in-crate; the host may retry (see SPEC §8).
    #[must_use]
    pub fn try_claim(&self) -> Option<u32> {
        let t = self.head.load(Ordering::Relaxed);
        let slot = &self.slots[(t as usize) % N];
        // Acquire: synchronizes with the consumer's Release store of FREE(t),
        // so our payload overwrite happens-after the consumer finished
        // reading the previous lap's payload (spike 1, invariant 2).
        if slot.gate.load(Ordering::Acquire) != t {
            return None;
        }
        // Single CAS attempt. On failure return None — no in-crate retry.
        // (The gate cannot have left FREE(t) and returned: gate values per
        // slot are strictly increasing in ticket order modulo 2³¹, and the
        // winner's ticket stays live, so no ABA is possible here.)
        let next = next_ticket(t);
        if self
            .head
            .compare_exchange(t, next, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            Some(t)
        } else {
            None
        }
    }

    /// Publish a claimed ticket's payload (SPEC §4).
    ///
    /// Writes the payload cells (`Relaxed`), then `compare_exchange`s the
    /// gate `FREE(t) -> PUBLISHED(t)` with `Release` — the Release carries
    /// the cell writes to the consumer's `Acquire` gate load. The CAS (not a
    /// store) is the arbiter between publish and the drainer's fence: exactly
    /// one wins, so the fence policy may be heuristic without soundness risk.
    ///
    /// On CAS failure the gate is re-read: `FREE(t)` (spurious failure on
    /// LL/SC targets) retries; `FENCED(t)` (or anything else) means the
    /// ticket is lost to the writer — it attempts the self-release
    /// (`FENCED(t) -> FREE(t+N)`) and returns [`PublishOutcome::Fenced`].
    /// A fenced writer never touches the slot again afterwards.
    #[must_use]
    pub fn publish(&self, ticket: u32, payload: &[u8; 32]) -> PublishOutcome {
        let t = ticket & TICKET_MASK;
        let slot = &self.slots[(t as usize) % N];
        let free = t;
        let published = published_gate(t);
        let fenced = t | FENCED_BIT;

        slot.write_payload(payload);
        loop {
            if slot
                .gate
                .compare_exchange(free, published, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                return PublishOutcome::Published;
            }
            let g = slot.gate.load(Ordering::Acquire);
            if g == free {
                // Spurious CAS failure (LL/SC targets): the gate is still
                // ours — retry. Bounded: each retry either wins or observes
                // the gate move (fence), which exits.
                continue;
            }
            if g == fenced {
                // We lost the race to the drainer's fence. Our cell writes
                // precede this CAS in program order, and the next owner
                // overwrites every cell before its own publish — so
                // self-releasing is sound.
                let _ = slot.gate.compare_exchange(
                    fenced,
                    release_gate::<N>(t),
                    Ordering::Release,
                    Ordering::Relaxed,
                );
                // If the self-release CAS failed, the host raced a
                // `force_release_slot` — the slot is already released to the
                // same value; either way we are done with it.
            }
            // Any other gate value: the ticket is gone (host raced a
            // force-release). Do not touch the slot.
            return PublishOutcome::Fenced;
        }
    }

    /// Drain the next ticket in order: test/drain scaffolding (SPEC §14).
    ///
    /// Single consumer only. Spins until the cursor ticket's gate reads
    /// `PUBLISHED(c)`, reads the payload, releases the slot for the ticket
    /// `N` ahead, and advances the cursor. The poll-based drainer replaces
    /// the spin with `Poll::Pending` on the device `Context` contract.
    #[must_use]
    pub fn drain(&self) -> (u32, [u8; 32]) {
        loop {
            let c = self.cursor.load(Ordering::Relaxed);
            let slot = &self.slots[(c as usize) % N];
            // Acquire: synchronizes with the producer's Release publish, so
            // the payload reads happen-after the producer's cell writes.
            if slot.gate.load(Ordering::Acquire) == published_gate(c) {
                let payload = slot.read_payload();
                // Release: pairs with the next owner's Acquire gate check —
                // our reads happen-before their overwrite.
                slot.gate.store(release_gate::<N>(c), Ordering::Release);
                self.cursor.store(next_ticket(c), Ordering::Relaxed);
                return (c, payload);
            }
            shim::spin_wait();
        }
    }
}
