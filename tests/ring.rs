//! Unit tests for the `multiwriter` admission ring (`horton::ring`).
//!
//! SPEC: `docs/multiwriter-spec.md` §§1–4. These tests pin the claim /
//! publish / drain protocol: per-slot gate arithmetic across laps, the
//! check-then-CAS claim (no ticket consumed on failure), and ticket
//! wrap-around safety for the 31-bit ticket space.

#![cfg(feature = "multiwriter")]

use horton::ring::{PublishOutcome, Ring, TICKET_MASK};

/// Test payload: the ticket in the first two cells, its bitwise complement
/// in the next two — a torn producer write cannot pass the check in
/// `assert_payload`.
fn payload_of(ticket: u32) -> [u8; 32] {
    let mut p = [0u8; 32];
    p[0..4].copy_from_slice(&ticket.to_le_bytes());
    p[4..8].copy_from_slice(&ticket.to_le_bytes());
    p[8..12].copy_from_slice(&(!ticket).to_le_bytes());
    p[12..16].copy_from_slice(&(!ticket).to_le_bytes());
    p
}

fn assert_payload(p: &[u8; 32], ticket: u32) {
    assert_eq!(&p[0..4], &ticket.to_le_bytes(), "payload cell 0 torn");
    assert_eq!(&p[4..8], &ticket.to_le_bytes(), "payload cell 1 torn");
    assert_eq!(&p[8..12], &(!ticket).to_le_bytes(), "payload cell 2 torn");
    assert_eq!(&p[12..16], &(!ticket).to_le_bytes(), "payload cell 3 torn");
    assert_eq!(&p[16..32], &[0u8; 16], "payload tail not zero");
}

fn claim<const N: usize>(r: &Ring<N>) -> u32 {
    r.try_claim().expect("ring should have a free slot")
}

fn publish<const N: usize>(r: &Ring<N>, t: u32) {
    assert_eq!(
        r.publish(t, &payload_of(t)),
        PublishOutcome::Published,
        "publish of a live ticket must succeed"
    );
}

/// Sequential claim → publish → drain: tickets are handed out in order and
/// each payload round-trips intact.
#[test]
fn claim_publish_drain_in_order() {
    let r = Ring::<8>::new();
    let mut tickets = Vec::new();
    for _ in 0..5 {
        let t = claim(&r);
        tickets.push(t);
        publish(&r, t);
    }
    assert_eq!(
        tickets,
        [0, 1, 2, 3, 4],
        "tickets handed out in claim order"
    );
    for want in 0..5 {
        let (t, p) = r.drain();
        assert_eq!(t, want, "drain order == ticket order");
        assert_payload(&p, want);
    }
}

/// Two full laps over a 4-slot ring: the consumer's release (`FREE(t+N)`)
/// frees each slot for the next lap, and the second lap's payloads are
/// not contaminated by the first.
#[test]
fn second_lap_reuses_slots() {
    let r = Ring::<4>::new();
    for lap in 0..2u32 {
        for i in 0..4u32 {
            let t = lap * 4 + i;
            assert_eq!(claim(&r), t, "lap {lap}: ticket order");
            publish(&r, t);
        }
        for i in 0..4u32 {
            let t = lap * 4 + i;
            let (dt, p) = r.drain();
            assert_eq!(dt, t, "lap {lap}: drain order");
            assert_payload(&p, t);
        }
    }
}

/// A full ring refuses the claim *without consuming a ticket*: after the
/// refused claim, draining the two live tickets lets the next claim take
/// exactly the ticket the refused claim would have taken.
#[test]
fn full_ring_claim_fails_without_consuming() {
    let r = Ring::<2>::new();
    let t0 = claim(&r);
    let t1 = claim(&r);
    assert_eq!((t0, t1), (0, 1));
    assert_eq!(r.try_claim(), None, "full ring: claim must fail");

    // The failed claim consumed nothing: the next successful claim after
    // draining is ticket 2, not 3.
    publish(&r, t0);
    publish(&r, t1);
    assert_eq!(r.drain().0, 0);
    assert_eq!(r.drain().0, 1);
    assert_eq!(claim(&r), 2, "failed claim must not consume a ticket");
}

/// Interleaved claim/publish (out-of-order publishing): the drainer still
/// emits tickets strictly in order, blocking on the unpublished head.
#[test]
fn out_of_order_publish_drains_in_order() {
    let r = Ring::<4>::new();
    let t0 = claim(&r);
    let t1 = claim(&r);
    let t2 = claim(&r);
    // Publish 1 and 2 before 0.
    publish(&r, t1);
    publish(&r, t2);
    publish(&r, t0);
    for want in [t0, t1, t2] {
        let (t, p) = r.drain();
        assert_eq!(t, want);
        assert_payload(&p, want);
    }
}

/// Ticket wrap-around: seed the counter at the top of the 31-bit space and
/// run two full laps across the wrap. Gate arithmetic, slot indexing, and
/// payload integrity must all survive `TICKET_MASK -> 0`.
#[test]
fn wrap_around_preserves_protocol() {
    let seed: u32 = TICKET_MASK - 2;
    let r = Ring::<4>::new_seeded(seed);

    // Lap 1: tickets seed, seed+1, seed+2, 0 — crosses the wrap.
    let lap1 = [seed, seed + 1, TICKET_MASK, 0];
    for &want in &lap1 {
        let t = claim(&r);
        assert_eq!(t, want, "wrap lap 1: ticket order");
        publish(&r, t);
    }
    for &want in &lap1 {
        let (t, p) = r.drain();
        assert_eq!(t, want, "wrap lap 1: drain order");
        assert_payload(&p, want);
    }

    // Lap 2: tickets 1..=4 — the slots' second generation.
    for want in 1..=4u32 {
        assert_eq!(claim(&r), want, "wrap lap 2: ticket order");
        publish(&r, want);
    }
    for want in 1..=4u32 {
        let (t, p) = r.drain();
        assert_eq!(t, want, "wrap lap 2: drain order");
        assert_payload(&p, want);
    }
}

/// The seed is masked to 31 bits: a `new_seeded` value with bit 31 set
/// cannot forge a ticket (bit 31 is the reserved `FENCED_BIT`).
#[test]
fn seed_is_masked_to_ticket_space() {
    let r = Ring::<4>::new_seeded(0xFFFF_FFFF);
    // 0xFFFF_FFFF & TICKET_MASK == TICKET_MASK.
    assert_eq!(claim(&r), TICKET_MASK, "seed must be masked to 31 bits");
}

/// Fencing (SPEC §6): a claimed-but-never-published ticket wedges the
/// in-order drainer. The drainer's `fence_cursor` CAS kills it —
/// `FREE(t) -> FENCED(t)` — and the cursor advances past the dead ticket.
/// The late writer's publish observes the fence and self-releases.
#[test]
fn fence_kills_stalled_ticket() {
    use horton::ring::{DrainPoll, FenceOutcome};

    let r = Ring::<4>::new();
    let t0 = claim(&r);
    publish(&r, t0);
    let t1 = claim(&r); // stalled: never published
    let t2 = claim(&r);
    publish(&r, t2);

    // Drain t0; then the cursor sits on the stalled t1.
    let (t, p) = r.drain();
    assert_eq!(t, t0);
    assert_payload(&p, t0);
    assert!(
        matches!(r.poll_drain(), DrainPoll::AwaitingPublish),
        "stalled ticket is not published"
    );

    // The fence wins the race: ticket t1 is dead, cursor advances.
    assert_eq!(r.fence_cursor(), FenceOutcome::Fenced);
    // t2 was already published — it drains next, in order.
    match r.poll_drain() {
        DrainPoll::Drained(t, p) => {
            assert_eq!(t, t2);
            assert_payload(&p, t2);
        }
        other => panic!("expected t2 drained, got {other:?}"),
    }

    // The stalled writer wakes up: its publish loses to the fence and
    // self-releases the slot for the next lap.
    assert_eq!(
        r.publish(t1, &payload_of(t1)),
        PublishOutcome::Fenced,
        "late publish must observe the fence"
    );
    // t1's slot was self-released to FREE(5); the ring stays live.
    // (Ticket 3's slot was never touched; ticket 4 reuses t0's slot.)
    assert_eq!(claim(&r), 3, "ring live after fence");
    assert_eq!(claim(&r), 4, "next lap reuses drained slots");
}

/// If the writer published before the fence CAS, the fence loses and the
/// ticket drains normally — the CAS is the arbiter (SPEC §6).
#[test]
fn fence_loses_to_published_ticket() {
    use horton::ring::{DrainPoll, FenceOutcome};

    let r = Ring::<4>::new();
    let t = claim(&r);
    publish(&r, t);
    assert_eq!(
        r.fence_cursor(),
        FenceOutcome::NotFenced,
        "fence must lose to a publish that won the gate"
    );
    match r.poll_drain() {
        DrainPoll::Drained(dt, p) => {
            assert_eq!(dt, t);
            assert_payload(&p, t);
        }
        other => panic!("expected drained ticket, got {other:?}"),
    }
}

/// Host-forced release (SPEC §6): with proven thread death the host may
/// release a stalled writer's slot. A late publish (host lied — the thread
/// was not dead) observes the released gate and reports `Fenced`.
#[test]
fn force_release_stalled_writer() {
    use horton::ring::{DrainPoll, ForceReleaseOutcome};

    let r = Ring::<4>::new();
    let t = claim(&r);
    assert_eq!(
        r.force_release_slot(t),
        ForceReleaseOutcome::Released,
        "FREE(t) is releasable"
    );
    // The drainer skips the dead ticket: the slot was released for t+N.
    assert!(
        matches!(r.poll_drain(), DrainPoll::Skipped),
        "force-released ticket must be skipped, not stalled on"
    );
    // The slot is reusable when the head reaches t+N: claim the live
    // tickets 1..=3 first (publish/drain them to advance the ring).
    for t in 1..4u32 {
        assert_eq!(claim(&r), t);
        publish(&r, t);
        let (dt, p) = r.drain();
        assert_eq!(dt, t);
        assert_payload(&p, t);
    }
    assert_eq!(claim(&r), t + 4, "released slot reused for t+N");
    // A publish from the supposedly-dead writer finds a foreign gate.
    assert_eq!(
        r.publish(t, &payload_of(t)),
        PublishOutcome::Fenced,
        "publish after force-release must not corrupt the new owner"
    );
}

/// Forcing the release of a published ticket is a no-op: the writer won,
/// the drainer will drain it (SPEC §6).
#[test]
fn force_release_published_is_noop() {
    use horton::ring::{DrainPoll, ForceReleaseOutcome};

    let r = Ring::<4>::new();
    let t = claim(&r);
    publish(&r, t);
    assert_eq!(
        r.force_release_slot(t),
        ForceReleaseOutcome::AlreadyPublished,
        "published ticket must not be released"
    );
    match r.poll_drain() {
        DrainPoll::Drained(dt, p) => {
            assert_eq!(dt, t);
            assert_payload(&p, t);
        }
        other => panic!("published ticket must still drain, got {other:?}"),
    }
}

/// A fenced-then-force-released slot is reusable (the host path after a
/// fence when the writer never wakes to self-release).
#[test]
fn force_release_fenced_slot() {
    use horton::ring::{FenceOutcome, ForceReleaseOutcome};

    let r = Ring::<4>::new();
    let t = claim(&r);
    assert_eq!(r.fence_cursor(), FenceOutcome::Fenced);
    assert_eq!(
        r.force_release_slot(t),
        ForceReleaseOutcome::Released,
        "FENCED(t) is releasable"
    );
    // The fence already advanced the cursor past t; the ring stays live
    // and the released slot is reused when the head reaches t+N.
    for nt in 1..4u32 {
        assert_eq!(claim(&r), nt);
        publish(&r, nt);
        let (dt, _) = r.drain();
        assert_eq!(dt, nt);
    }
    assert_eq!(claim(&r), t + 4, "slot freed for t+N after fence+release");
}

/// Releasing a ticket the drainer already processed is stale — nothing to do.
#[test]
fn force_release_stale_ticket() {
    use horton::ring::ForceReleaseOutcome;

    let r = Ring::<4>::new();
    let t = claim(&r);
    publish(&r, t);
    let (dt, _) = r.drain();
    assert_eq!(dt, t);
    assert_eq!(
        r.force_release_slot(t),
        ForceReleaseOutcome::Stale,
        "already-drained ticket is stale"
    );
}
