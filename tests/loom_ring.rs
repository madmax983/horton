//! Loom models for the `multiwriter` admission ring (`horton::ring`).
//!
//! Run with: `cargo test --release --features loom --test loom_ring`
//!
//! Adapted from the spike-1 `loom_ring_a` models to the SPEC's 31-bit
//! ticket design (`docs/multiwriter-spec.md` §§1–4). The models exercise
//! the *real* ring code compiled against Loom's modeled atomics (the
//! `loom` feature swaps the shim in `src/ring.rs`).
//!
//! Each model: N producer threads claim+publish (split into two steps so
//! Loom interleaves the claim→publish window), one consumer thread drains
//! exactly the total number of puts. The blocking claim is a test-side
//! spin on the non-blocking `try_claim` (the SPEC's check-then-CAS claim
//! never spins in-crate). Assertions:
//!   (i)   every ticket 0..TOTAL is claimed exactly once,
//!   (ii)  every published payload is drained exactly once,
//!   (iii) drain order == ticket order, with the payload matching the ticket
//!           (payload carries the ticket twice; any torn write fails this),
//!   (iv)  model completion itself proves no lost wakeup / deadlock within
//!           the explored interleavings.
//!
//! Loom exploration strategy (from the spike): preemption bounding
//! (bound 2) plus minimal models (<= 3 threads, <= 3 tickets), so the
//! interesting claim/publish/drain races exhibit within a few thousand
//! branches. The ticket counts are small enough that the 31-bit wrap is
//! never exercised here — wrap safety is covered by the seeded unit
//! tests in `tests/ring.rs`.

#![cfg(all(feature = "multiwriter", feature = "loom"))]

use horton::ring::{PublishOutcome, Ring};
use std::sync::Arc;

/// Payload redundantly encodes the ticket in both halves: a torn
/// producer write (or a stale-slot read) cannot pass the equality check.
fn payload_of(ticket: u32) -> [u8; 32] {
    let mut p = [0u8; 32];
    p[0..4].copy_from_slice(&ticket.to_le_bytes());
    p[4..8].copy_from_slice(&ticket.to_le_bytes());
    p[8..12].copy_from_slice(&(!ticket).to_le_bytes());
    p[12..16].copy_from_slice(&(!ticket).to_le_bytes());
    p
}

/// Test-side blocking claim: spins on the SPEC's non-blocking `try_claim`.
/// In-crate, a failed claim returns `None` (→ `Error::RingFull`); only the
/// test harness retries.
fn claim_blocking<const N: usize>(r: &Ring<N>) -> u32 {
    loop {
        if let Some(t) = r.try_claim() {
            return t;
        }
        loom::thread::yield_now();
    }
}

fn run_model(f: impl Fn() + Sync + Send + 'static) {
    let mut b = loom::model::Builder::new();
    b.preemption_bound = Some(2);
    b.max_branches = 10_000;
    b.check(f);
}

fn model<const N: usize>(producers: u32, puts_each: u32) {
    let total = producers * puts_each;
    run_model(move || {
        let ring = Arc::new(Ring::<N>::new());

        let mut prod_handles = Vec::new();
        for _ in 0..producers {
            let r = ring.clone();
            prod_handles.push(loom::thread::spawn(move || {
                let mut tickets = Vec::new();
                for _ in 0..puts_each {
                    // Claim and publish are separate steps so Loom interleaves
                    // the claim→publish window across producers.
                    let t = claim_blocking(&r);
                    let outcome = r.publish(t, &payload_of(t));
                    assert!(
                        matches!(outcome, PublishOutcome::Published),
                        "no fencing in this model: publish must succeed"
                    );
                    tickets.push(t);
                }
                tickets
            }));
        }

        // The consumer takes the last owned handle: `ring` is not used after
        // this spawn, so move it in rather than cloning.
        let cons = loom::thread::spawn(move || {
            let mut out = Vec::new();
            for _ in 0..total {
                out.push(ring.drain());
            }
            out
        });

        let mut all_tickets = Vec::new();
        for h in prod_handles {
            all_tickets.extend(h.join().expect("producer panicked"));
        }
        let drained = cons.join().expect("consumer panicked");

        // (i) ticket uniqueness: check-then-CAS hands each ticket out exactly once.
        all_tickets.sort_unstable();
        assert_eq!(
            all_tickets,
            (0..total).collect::<Vec<_>>(),
            "every ticket claimed exactly once"
        );

        // (ii)+(iii) exactly-once drain, in ticket order, payload intact.
        assert_eq!(drained.len(), total as usize, "no lost / duplicated drain");
        for (i, (t, p)) in (0..total).zip(drained.iter()) {
            assert_eq!(*t, i, "drain order == ticket order");
            assert_eq!(*p, payload_of(i), "payload matches its ticket");
        }
    });
}

/// Two producers, one put each, ring of 2: the core race — concurrent
/// claims with out-of-order publishing. The consumer must still drain in
/// ticket order (it blocks on the unpublished earlier ticket).
#[test]
fn two_producers_one_put_cap_two() {
    model::<2>(2, 1);
}

/// NOTE: no cap-1 model: the ring requires N >= 2 (SPEC §1 — the +1/+N seq
/// encoding collides publish(t) and release(t) at N == 1).
///
/// One producer, three puts, ring of 2: slot wrap-around across laps plus
/// backpressure (the third claim retries until the consumer drains),
/// with a minimal 2-thread state space.
#[test]
fn one_producer_three_puts_cap_two() {
    model::<2>(1, 3);
}

/// Fence vs publish race (SPEC §6): the claimer publishes ticket 0 while the
/// fencer attempts the fence. The gate CAS is the arbiter — exactly one
/// wins, and the ring stays consistent either way:
/// - publish wins → fence reports `NotFenced`, the ticket drains intact;
/// - fence wins → publish reports `Fenced` (and self-releases), the ticket
///   is skipped, the ring stays live.
///
/// The fencer waits for the claim (a fence before any claim would kill
/// ticket 0 before it exists — a host bug, not a race).
#[test]
fn fence_vs_publish_race() {
    use horton::ring::{DrainPoll, FenceOutcome};
    use loom::sync::atomic::{AtomicBool, Ordering as LoomOrdering};

    run_model(|| {
        let ring = Arc::new(Ring::<4>::new());
        let claimed = Arc::new(AtomicBool::new(false));

        let r1 = ring.clone();
        let c1 = claimed.clone();
        let writer = loom::thread::spawn(move || {
            let t = claim_blocking(&r1);
            assert_eq!(t, 0, "single claimer gets ticket 0");
            c1.store(true, LoomOrdering::Release);
            r1.publish(t, &payload_of(t))
        });

        let r2 = ring.clone();
        let fencer = loom::thread::spawn(move || {
            while !claimed.load(LoomOrdering::Acquire) {
                loom::thread::yield_now();
            }
            r2.fence_cursor()
        });

        let pub_outcome = writer.join().expect("writer panicked");
        let fence_outcome = fencer.join().expect("fencer panicked");

        match (pub_outcome, fence_outcome) {
            (PublishOutcome::Published, FenceOutcome::NotFenced) => {
                // Publish won the gate: the ticket drains intact, in order.
                match ring.poll_drain() {
                    DrainPoll::Drained(t, p) => {
                        assert_eq!(t, 0);
                        assert_eq!(p, payload_of(0), "payload intact after lost fence");
                    }
                    other => panic!("published ticket must drain, got {other:?}"),
                }
            }
            (PublishOutcome::Fenced, FenceOutcome::Fenced(_)) => {
                // Fence won: the ticket is dead — skipped, never drained —
                // and the ring stays live (cursor advanced past it, head
                // untouched, slot self-released by the losing publish).
                assert!(
                    matches!(ring.poll_drain(), DrainPoll::AwaitingPublish),
                    "cursor advanced past the fenced ticket"
                );
                assert_eq!(
                    ring.try_claim(),
                    Some(1),
                    "head still hands out ticket 1 after a fence"
                );
            }
            other => panic!("CAS arbiter violated: impossible outcome pair {other:?}"),
        }
    });
}
