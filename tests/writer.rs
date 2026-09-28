//! Writer-side put future tests (SPEC §8).
//!
//! `put` claims a ticket, publishes the payload, and returns a future
//! that resolves when the ticket is WAL-durable.

#![cfg(feature = "multiwriter")]

mod common;

use common::{MemDevice, block_on, noop_waker, test_config};
use core::future::Future;
use core::pin::pin;
use core::sync::atomic::{AtomicU32, Ordering};
use core::task::{Context, Poll};
use horton::drainer::{Drainer, payload};
use horton::ring::{PublishOutcome, Ring};
use horton::writer::put;

fn poll_put<const N: usize>(f: &mut horton::writer::Put<'_, N>) -> Poll<u32> {
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    pin!(f).poll(&mut cx)
}

/// Builds a Db-owning drainer for writer tests.
fn make_drainer<'r>(
    ring: &'r Ring<8>,
    durable: &'r AtomicU32,
) -> Drainer<'r, MemDevice<512>, 512, 32, 32, 16, 1024, 2, 4, 64, 0, 8, 8> {
    let mut db = horton::Db::new(MemDevice::<512>::new(), test_config());
    block_on(db.open()).expect("db open must succeed");
    Drainer::new(ring, db, durable)
}

#[test]
fn put_completes_when_durable() {
    let ring = Ring::<8>::new();
    let durable = AtomicU32::new(0);
    let mut drainer = make_drainer(&ring, &durable);

    let p = payload::encode_put(b"k", b"v").expect("fits");
    let mut pending = put(&ring, &durable, &p).expect("ring has space");
    let ticket = pending.ticket();

    // Not durable yet: Pending.
    assert_eq!(poll_put(&mut pending), Poll::Pending);

    // Drain: the ticket becomes durable.
    block_on(drainer.sweep()).expect("sweep ok");
    assert_eq!(durable.load(Ordering::Acquire), ticket + 1);

    // Now Ready.
    assert_eq!(poll_put(&mut pending), Poll::Ready(ticket));
}

#[test]
fn put_returns_no_space_when_full() {
    let ring = Ring::<2>::new();
    let durable = AtomicU32::new(0);
    let p = payload::encode_put(b"k", b"v").expect("fits");

    // Fill the ring.
    let mut puts = [
        put(&ring, &durable, &p).expect("space"),
        put(&ring, &durable, &p).expect("space"),
    ];
    // Avoid unused warnings; the puts are pending.
    for pm in &mut puts {
        assert_eq!(poll_put(pm), Poll::Pending);
    }

    // Ring full: RingFull, no ticket consumed.
    match put(&ring, &durable, &p) {
        Err(horton::Error::RingFull) => {}
        other => panic!("expected RingFull, got {:?}", other.is_ok()),
    }
}

#[test]
fn fenced_ticket_reclaim_pattern() {
    // The pattern `put` uses when publish returns Fenced: the dead ticket
    // is abandoned and a fresh one is claimed. (The race — fence between
    // claim and publish — needs concurrency to hit inside `put` itself;
    // this verifies the ring half of the pattern.)
    let ring = Ring::<8>::new();
    let p = payload::encode_put(b"k", b"v").expect("fits");

    let t0 = ring.try_claim().expect("space");
    // Stall the ticket: fence the cursor (t0).
    let fenced = ring.fence_cursor();
    assert!(matches!(fenced, horton::ring::FenceOutcome::Fenced(t) if t == t0));
    // Publishing the fenced ticket reports Fenced.
    assert_eq!(ring.publish(t0, &p), PublishOutcome::Fenced);
    // Re-claim gets a fresh ticket that publishes fine.
    let t1 = ring.try_claim().expect("space");
    assert_ne!(t0, t1);
    assert_eq!(ring.publish(t1, &p), PublishOutcome::Published);
}

#[test]
fn dropped_put_abandons_observation_not_the_write() {
    // Dropping a pending `Put` stops observation; the accepted write is
    // still drained and made durable by the drainer.
    let ring = Ring::<8>::new();
    let durable = AtomicU32::new(0);
    let mut drainer = make_drainer(&ring, &durable);

    let p = payload::encode_put(b"k", b"v").expect("fits");
    // Scope the observer: dropping it abandons observation, not the write.
    let ticket = {
        let mut pending = put(&ring, &durable, &p).expect("ring has space");
        let ticket = pending.ticket();
        // Pending before the drain.
        assert_eq!(poll_put(&mut pending), Poll::Pending);
        ticket
    };

    // The write was accepted and must still complete.

    block_on(drainer.sweep()).expect("sweep ok");
    assert!(
        horton::drainer::is_ticket_durable(&durable, ticket),
        "dropped put's ticket must still become durable"
    );

    // And the data is in the Db.
    let db = drainer.into_db();
    let mut buf = [0u8; 32];
    let len = block_on(db.get(b"k", &mut buf))
        .expect("get ok")
        .expect("k present");
    assert_eq!(&buf[..len], b"v");
}

/// `put` claims tickets across the 31-bit wrap and the `Put` future
/// resolves as the `durable` watermark steps through `TICKET_MASK -> 0`.
/// This exercises `is_ticket_durable` inside `Put::poll` on both sides of
/// the boundary — the writer-completion path, not just the drain path.
#[test]
fn put_completes_across_ticket_wrap() {
    use horton::ring::TICKET_MASK;

    let seed: u32 = TICKET_MASK - 1;
    let ring = Ring::<8>::new_seeded(seed);
    let durable = AtomicU32::new(seed);
    let mut drainer = make_drainer(&ring, &durable);
    let p = payload::encode_put(b"k", b"v").expect("fits");

    // Ticket TICKET_MASK - 1: Pending until the sweep, then Ready.
    let mut pending = put(&ring, &durable, &p).expect("ring has space");
    assert_eq!(pending.ticket(), seed);
    assert_eq!(poll_put(&mut pending), Poll::Pending);
    block_on(drainer.sweep()).expect("sweep ok");
    assert_eq!(poll_put(&mut pending), Poll::Ready(seed));

    // Ticket TICKET_MASK: the watermark steps onto the boundary.
    let mut pending = put(&ring, &durable, &p).expect("ring has space");
    assert_eq!(pending.ticket(), TICKET_MASK);
    assert_eq!(poll_put(&mut pending), Poll::Pending);
    block_on(drainer.sweep()).expect("sweep ok");
    assert_eq!(durable.load(Ordering::Acquire), 0, "watermark wraps to 0");
    assert_eq!(poll_put(&mut pending), Poll::Ready(TICKET_MASK));

    // Ticket 0: claimed after the wrap, resolves normally.
    let mut pending = put(&ring, &durable, &p).expect("ring has space");
    assert_eq!(pending.ticket(), 0);
    assert_eq!(poll_put(&mut pending), Poll::Pending);
    block_on(drainer.sweep()).expect("sweep ok");
    assert_eq!(poll_put(&mut pending), Poll::Ready(0));
    assert_eq!(drain_watermark_for(&durable), 1);
}

/// Test-local read of the drain watermark (writer.rs has no drainer import).
fn drain_watermark_for(durable: &AtomicU32) -> u32 {
    durable.load(Ordering::Acquire) & horton::ring::TICKET_MASK
}
