//! Drainer integration tests (SPEC §§7, 11).
//!
//! The drainer owns the [`WalWriter`], drains the ring in ticket order,
//! batches through one flush per sweep, and advances the `durable`
//! watermark only over the acknowledged prefix.

#![cfg(feature = "multiwriter")]

mod common;

use common::{MemDevice, block_on};
use core::sync::atomic::{AtomicU32, Ordering};
use core::task::{Context, Poll};
use horton::BlockDevice;
use horton::drainer::{Drainer, SweepOutcome, is_ticket_durable, payload};
use horton::ring::Ring;
use horton::wal::WalWriter;

/// Device wrapper that counts `poll_flush` calls.
struct FlushCount<D, const BLOCK: usize> {
    inner: D,
    flush_count: usize,
}

impl<D, const BLOCK: usize> FlushCount<D, BLOCK> {
    const fn new(inner: D) -> Self {
        Self {
            inner,
            flush_count: 0,
        }
    }

    fn into_inner(self) -> D {
        self.inner
    }

    const fn flushes(&self) -> usize {
        self.flush_count
    }
}

impl<D: BlockDevice, const BLOCK: usize> BlockDevice for FlushCount<D, BLOCK> {
    type Error = D::Error;
    const BLOCK: usize = BLOCK;

    fn poll_read_block(
        &self,
        cx: &mut Context<'_>,
        id: u64,
        buf: &mut [u8],
    ) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_read_block(cx, id, buf)
    }

    fn poll_write_block(
        &mut self,
        cx: &mut Context<'_>,
        id: u64,
        buf: &[u8],
    ) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_write_block(cx, id, buf)
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.flush_count += 1;
        self.inner.poll_flush(cx)
    }
}

/// Test error for failure injection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TestError;

/// Device wrapper whose flush always fails.
struct FailFlush<const BLOCK: usize>;

impl<const BLOCK: usize> FailFlush<BLOCK> {
    const fn new() -> Self {
        Self
    }
}

impl<const BLOCK: usize> BlockDevice for FailFlush<BLOCK> {
    type Error = TestError;
    const BLOCK: usize = BLOCK;

    fn poll_read_block(
        &self,
        _cx: &mut Context<'_>,
        _id: u64,
        _buf: &mut [u8],
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Err(TestError))
    }

    fn poll_write_block(
        &mut self,
        _cx: &mut Context<'_>,
        _id: u64,
        _buf: &[u8],
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Err(TestError))
    }
}

fn setup<const N: usize>() -> (Ring<N>, AtomicU32) {
    (Ring::<N>::new(), AtomicU32::new(0))
}

fn put_payload(key: &[u8], val: &[u8]) -> [u8; 32] {
    payload::encode_put(key, val).expect("test payload must fit")
}

/// Claim, publish, and return the ticket.
fn publish(ring: &Ring<8>, key: &[u8], val: &[u8]) -> u32 {
    let t = ring.try_claim().expect("ring must have space");
    let p = put_payload(key, val);
    assert_eq!(
        ring.publish(t, &p),
        horton::ring::PublishOutcome::Published,
        "publish must succeed"
    );
    t
}

#[test]
fn sweep_drains_batch_with_one_flush() {
    let (ring, durable) = setup::<8>();
    let device = FlushCount::<_, 512>::new(MemDevice::<512>::new());
    let wal = WalWriter::new(device, 0, 16);
    // Fresh WAL: max_seq=0, so the recovered next_seq is 1 (seqnum 0 is
    // the recovery floor and is never used).
    let mut drainer = Drainer::<_, 512, 8, 8>::new(&ring, wal, &durable, 1);

    let t0 = publish(&ring, b"k0", b"v0");
    let t1 = publish(&ring, b"k1", b"v1");
    let t2 = publish(&ring, b"k2", b"v2");
    assert_eq!((t0, t1, t2), (0, 1, 2));

    // Before the sweep, nothing is durable.
    assert!(!is_ticket_durable(&durable, t0));

    let outcome = block_on(drainer.sweep()).expect("sweep must succeed");
    assert_eq!(outcome, SweepOutcome::Swept { tickets: 3 });

    // All three tickets are durable now (O1: flush was acknowledged).
    assert!(is_ticket_durable(&durable, t0));
    assert!(is_ticket_durable(&durable, t1));
    assert!(is_ticket_durable(&durable, t2));
    assert_eq!(durable.load(Ordering::Acquire), 3);
    assert_eq!(drainer.next_seq(), 4);

    // One flush for the whole sweep (SPEC §11).
    let wal = drainer.into_wal();
    let device = wal.into_device();
    assert_eq!(device.flushes(), 1, "one flush per sweep");
    let mem = device.into_inner();

    // The WAL records are exactly the three puts, seqnums 1..=3.
    let mut w2: WalWriter<_, 512> = WalWriter::new(mem, 0, 16);
    let mut t = horton::MemTable::<16, 512, 16, 32>::new();
    let st = block_on(w2.recover(&mut t)).expect("recovery must succeed");
    assert_eq!(st.records, 3);
    assert_eq!(st.max_seq, 3);
    assert_eq!(t.get(b"k0").expect("k0 present").val, b"v0");
    assert_eq!(t.get(b"k1").expect("k1 present").val, b"v1");
    assert_eq!(t.get(b"k2").expect("k2 present").val, b"v2");
}

#[test]
fn sweep_is_idle_on_empty_ring() {
    let (ring, durable) = setup::<8>();
    let wal = WalWriter::new(MemDevice::<512>::new(), 0, 16);
    let mut drainer = Drainer::<_, 512, 8, 8>::new(&ring, wal, &durable, 1);

    // No fence on an empty ring: the cursor must not be poisoned.
    for _ in 0..2000 {
        let outcome = block_on(drainer.sweep()).expect("sweep must succeed");
        assert_eq!(outcome, SweepOutcome::Idle);
    }
    assert_eq!(durable.load(Ordering::Acquire), 0);

    // The ring still accepts claims after idle sweeps (fence refused the
    // unclaimed head ticket).
    let t = publish(&ring, b"k", b"v");
    assert_eq!(t, 0);
    let outcome = block_on(drainer.sweep()).expect("sweep must succeed");
    assert_eq!(outcome, SweepOutcome::Swept { tickets: 1 });
    assert!(is_ticket_durable(&durable, t));
}

#[test]
fn fenced_ticket_skipped_and_durable_advances() {
    let (ring, durable) = setup::<8>();
    let wal = WalWriter::new(MemDevice::<512>::new(), 0, 16);
    let mut drainer = Drainer::<_, 512, 8, 8>::new(&ring, wal, &durable, 1);
    drainer.set_stall_budget(2);

    // t0 claimed but never published (stalled writer). t1 published.
    let t0 = ring.try_claim().expect("claim t0");
    let t1 = publish(&ring, b"k1", b"v1");
    assert_eq!((t0, t1), (0, 1));

    // Sweep: t0 is stalled → stall budget → fence kills t0 → t1 drains.
    // Run sweeps until t1 is durable (fence takes a few polls).
    for _ in 0..10 {
        let _ = block_on(drainer.sweep()).expect("sweep must succeed");
        if is_ticket_durable(&durable, t1) {
            break;
        }
    }

    // t0 was skipped (dead) — durable advanced past it without a WAL
    // record. t1 was drained and is durable.
    assert!(
        is_ticket_durable(&durable, t0),
        "fenced t0 must be resolved"
    );
    assert!(is_ticket_durable(&durable, t1), "t1 must be durable");
    assert_eq!(durable.load(Ordering::Acquire), 2);
    // Only t1 consumed a seqnum (fenced tickets take no seqnums).
    assert_eq!(drainer.next_seq(), 2);
}

#[test]
fn batch_error_poisons_and_acks_nothing() {
    let (ring, durable) = setup::<8>();
    let device = FailFlush::<512>::new();
    let wal = WalWriter::new(device, 0, 16);
    let mut drainer = Drainer::<_, 512, 8, 8>::new(&ring, wal, &durable, 1);

    let t0 = publish(&ring, b"k0", b"v0");
    let _t1 = publish(&ring, b"k1", b"v1");

    let err = block_on(drainer.sweep()).expect_err("flush failure must error");
    assert_eq!(err, horton::Error::Device(TestError));
    assert!(drainer.is_poisoned());

    // O1: nothing was acknowledged — durable did not advance.
    assert!(!is_ticket_durable(&durable, t0));
    assert_eq!(durable.load(Ordering::Acquire), 0);

    // Subsequent sweeps stay stopped.
    let outcome = block_on(drainer.sweep()).expect("poisoned sweep stays idle");
    assert_eq!(outcome, SweepOutcome::Idle);
}

#[test]
fn next_seq_reseed_from_recovery() {
    let (ring, durable) = setup::<8>();
    // Simulate recovery: the WAL's next_seq is 1000, so the head reseeds
    // to 1000 & TICKET_MASK and the drainer starts seqnums at 1000.
    let base: u64 = 1000;
    durable.store((base & 0x7fff_ffff) as u32, Ordering::Release);
    let wal = WalWriter::new(MemDevice::<512>::new(), 0, 16);
    let mut drainer = Drainer::<_, 512, 8, 8>::new(&ring, wal, &durable, base);

    assert_eq!(drainer.next_seq(), 1000);
    let t = publish(&ring, b"k", b"v");
    let outcome = block_on(drainer.sweep()).expect("sweep must succeed");
    assert_eq!(outcome, SweepOutcome::Swept { tickets: 1 });
    assert_eq!(drainer.next_seq(), 1001);
    assert!(is_ticket_durable(&durable, t));
}

#[test]
fn payload_codec_roundtrip() {
    let p = payload::encode_put(b"key", b"value").expect("must fit");
    let (op, key, val) = payload::decode(&p);
    assert_eq!(op, horton::wal::Op::Put);
    assert_eq!(key, b"key");
    assert_eq!(val, b"value");

    let p = payload::encode_delete(b"key").expect("must fit");
    let (op, key, val) = payload::decode(&p);
    assert_eq!(op, horton::wal::Op::Delete);
    assert_eq!(key, b"key");
    assert_eq!(val, &[] as &[u8]);

    // Oversize key rejected.
    assert!(payload::encode_put(&[b'x'; 30], b"v").is_none());
    // Empty key rejected.
    assert!(payload::encode_put(b"", b"v").is_none());
    // Key+value too large rejected.
    assert!(payload::encode_put(&[b'k'; 20], &[b'v'; 10]).is_none());
    // Max-size key+value (29 bytes) accepted.
    assert!(payload::encode_put(&[b'k'; 20], &[b'v'; 9]).is_some());
}
