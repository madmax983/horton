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
