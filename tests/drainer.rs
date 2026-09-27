//! Drainer integration tests (SPEC §§7, 9, 11).
//!
//! The drainer owns the [`Db`](horton::Db), drains the ring in ticket
//! order, batches through one flush per sweep, advances the `durable`
//! watermark only over the acknowledged prefix, and publishes the drain
//! watermark that snapshots pin (not the claim head).

#![cfg(feature = "multiwriter")]

mod common;

use common::{MemDevice, block_on, noop_waker, test_config};
use core::sync::atomic::{AtomicU32, Ordering};
use core::task::{Context, Poll};
use horton::BlockDevice;
use horton::drainer::{Drainer, SweepOutcome, drain_watermark, is_ticket_durable, payload};
use horton::ring::{ForceReleaseOutcome, PublishOutcome, Ring};

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
        buf: &mut [u8],
    ) -> Poll<Result<(), Self::Error>> {
        // Reads succeed (zeros): Db::open must succeed so the drainer
        // can attempt the write that fails at flush.
        buf.fill(0);
        Poll::Ready(Ok(()))
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

/// Device wrapper whose flush returns `Pending` a set number of times
/// before completing. Lets a test observe the drainer mid-flush.
struct PendingFlush<D, const BLOCK: usize> {
    inner: D,
    pending: usize,
}

impl<D, const BLOCK: usize> PendingFlush<D, BLOCK> {
    const fn new(inner: D, pending: usize) -> Self {
        Self { inner, pending }
    }
}

impl<D: BlockDevice, const BLOCK: usize> BlockDevice for PendingFlush<D, BLOCK> {
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
        if self.pending > 0 {
            self.pending -= 1;
            Poll::Pending
        } else {
            self.inner.poll_flush(cx)
        }
    }
}

fn setup<const N: usize>() -> (Ring<N>, AtomicU32) {
    (Ring::<N>::new(), AtomicU32::new(0))
}

/// Test Db consts: small enough for fast tests.
type TestDrainer<'r, const N: usize, const MAX_WRITES: usize> = Drainer<
    'r,
    FlushCount<MemDevice<512>, 512>,
    512,  // BLOCK
    32,   // KEY_MAX
    32,   // VAL_MAX
    16,   // CAP
    1024, // ARENA
    2,    // LEVELS
    4,    // TABLES
    64,   // BLOOM_BYTES
    0,    // CACHE
    N,
    MAX_WRITES,
>;

/// Builds a drainer owning a fresh Db over a flush-counting device.
fn make_drainer<'a, const N: usize, const MAX_WRITES: usize>(
    ring: &'a Ring<N>,
    durable: &'a AtomicU32,
) -> TestDrainer<'a, N, MAX_WRITES> {
    let device = FlushCount::<_, 512>::new(MemDevice::<512>::new());
    let mut db = horton::Db::new(device, test_config());
    block_on(db.open()).expect("db open must succeed");
    Drainer::new(ring, db, durable)
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
    let mut drainer = make_drainer::<8, 8>(&ring, &durable);

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

    // One flush for the whole sweep (SPEC §11).
    let db = drainer.into_db();
    assert_eq!(db.device().flushes(), 1, "one flush per sweep");

    // The data is in the Db (memtable), visible via get.
    let mut buf = [0u8; 32];
    let len = block_on(db.get(b"k0", &mut buf))
        .expect("get must succeed")
        .expect("k0 present");
    assert_eq!(&buf[..len], b"v0");
    let len = block_on(db.get(b"k1", &mut buf))
        .expect("get must succeed")
        .expect("k1 present");
    assert_eq!(&buf[..len], b"v1");
    let len = block_on(db.get(b"k2", &mut buf))
        .expect("get must succeed")
        .expect("k2 present");
    assert_eq!(&buf[..len], b"v2");
}

#[test]
fn sweep_is_idle_on_empty_ring() {
    let (ring, durable) = setup::<8>();
    let mut drainer = make_drainer::<8, 8>(&ring, &durable);

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
    let mut drainer = make_drainer::<8, 8>(&ring, &durable);
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
    // Only t1 is in the Db (fenced tickets take no WAL records).
}

#[test]
fn batch_error_poisons_and_acks_nothing() {
    let (ring, durable) = setup::<8>();
    let device = FailFlush::<512>::new();
    let mut db = horton::Db::<_, 512, 32, 32, 16, 1024, 2, 4, 64, 0>::new(device, test_config());
    block_on(db.open()).expect("db open must succeed");
    let mut drainer =
        Drainer::<_, 512, 32, 32, 16, 1024, 2, 4, 64, 0, 8, 8>::new(&ring, db, &durable);

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
fn payload_codec_roundtrip() {
    let p = payload::encode_put(b"key", b"value").expect("must fit");
    let (op, key, val) = payload::decode(&p).expect("valid put decodes");
    assert_eq!(op, horton::wal::Op::Put);
    assert_eq!(key, b"key");
    assert_eq!(val, b"value");

    let p = payload::encode_delete(b"key").expect("must fit");
    let (op, key, val) = payload::decode(&p).expect("valid delete decodes");
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

#[test]
fn payload_decode_rejects_malformed() {
    // Malformed payloads must Err, never silently become valid writes.
    // Bad op byte.
    let mut p = [0u8; 32];
    p[0] = 0xFF;
    p[1] = 1;
    p[3] = b'k';
    assert_eq!(payload::decode(&p), Err(payload::DecodeError::BadOp));

    // Zero key length.
    let mut p = payload::encode_put(b"k", b"v").expect("fits");
    p[1] = 0;
    assert_eq!(payload::decode(&p), Err(payload::DecodeError::BadKeyLen));

    // Overlong key length.
    let mut p = payload::encode_put(b"k", b"v").expect("fits");
    p[1] = 30;
    assert_eq!(payload::decode(&p), Err(payload::DecodeError::BadKeyLen));

    // Key+value exceeding the 29-byte budget.
    let mut p = payload::encode_put(b"k", b"v").expect("fits");
    p[1] = 20;
    p[2] = 10; // 20 + 10 > 29
    assert_eq!(payload::decode(&p), Err(payload::DecodeError::BadValLen));

    // Delete with nonzero value length.
    let mut p = payload::encode_delete(b"k").expect("fits");
    p[2] = 1;
    assert_eq!(payload::decode(&p), Err(payload::DecodeError::BadValLen));
}

#[test]
fn malformed_payload_poisons_drainer() {
    // A malformed payload is never silently written: the drainer poisons
    // and durable does not advance.
    let ring = Ring::<8>::new();
    let durable = AtomicU32::new(0);
    let mut drainer = make_drainer::<8, 8>(&ring, &durable);

    // Craft a payload with a bad op byte and publish it directly.
    let mut bad = [0u8; 32];
    bad[0] = 0xFF;
    bad[1] = 1;
    bad[3] = b'k';
    let t = ring.try_claim().expect("ring has space");
    assert_eq!(ring.publish(t, &bad), PublishOutcome::Published);

    let err = block_on(drainer.sweep()).expect_err("malformed must error");
    assert_eq!(err, horton::Error::BadPayload);
    assert!(drainer.is_poisoned());
    assert_eq!(durable.load(Ordering::Acquire), 0);
}

#[test]
fn watermark_pins_drain_not_claim_head() {
    // SPEC §9: snapshots pin the drain watermark, not the claim head.
    // The claim head can run ahead of durability; a snapshot pinned at
    // the head would claim visibility of tickets that are not WAL-durable.
    let ring = Ring::<8>::new();
    let durable = AtomicU32::new(0);
    // MAX_WRITES=2 so one sweep cannot drain everything claimed.
    let mut drainer = make_drainer::<8, 2>(&ring, &durable);

    for i in 0..5u8 {
        let t = ring.try_claim().expect("ring has space");
        let p = payload::encode_put(&[b'k', b'0' + i], &[b'v', b'0' + i]).expect("fits");
        assert_eq!(ring.publish(t, &p), PublishOutcome::Published);
    }

    let outcome = block_on(drainer.sweep()).expect("sweep ok");
    assert_eq!(outcome, SweepOutcome::Swept { tickets: 2 });
    assert_eq!(drainer.durable_watermark(), 2);

    // Claim two more (head advances to 7) but do not sweep.
    for _ in 0..2 {
        let t = ring.try_claim().expect("ring has space");
        let p = payload::encode_put(b"k9", b"v9").expect("fits");
        assert_eq!(ring.publish(t, &p), PublishOutcome::Published);
    }

    // The watermark still pins the drain position: tickets 2..7 are
    // claimed but not durable. Pinning the head (7) would be wrong.
    assert_eq!(drain_watermark(&durable), 2);
    assert_eq!(drainer.durable_watermark(), 2);

    // The Db contains exactly the durable prefix: 2 records.
    let db = drainer.into_db();
    let mut buf = [0u8; 32];
    // k0 and k1 were in the first sweep (tickets 0, 1).
    assert!(block_on(db.get(b"k0", &mut buf)).expect("get ok").is_some());
    assert!(block_on(db.get(b"k1", &mut buf)).expect("get ok").is_some());
}

#[test]
fn watermark_is_monotonic_across_sweeps() {
    let ring = Ring::<8>::new();
    let durable = AtomicU32::new(0);
    let mut drainer = make_drainer::<8, 8>(&ring, &durable);

    let mut prev = drainer.durable_watermark();
    assert_eq!(prev, 0);
    for batch in 0..3u8 {
        for i in 0..3u8 {
            let t = ring.try_claim().expect("ring has space");
            let n = batch * 3 + i;
            let p = payload::encode_put(&[b'k', b'0' + n], &[b'v', b'0' + n]).expect("fits");
            assert_eq!(ring.publish(t, &p), PublishOutcome::Published);
        }
        block_on(drainer.sweep()).expect("sweep ok");
        let w = drainer.durable_watermark();
        assert!(w >= prev, "watermark must never move backward");
        prev = w;
    }
    // 9 tickets drained from a 0 base.
    assert_eq!(prev, 9);
}

#[test]
fn fenced_tickets_advance_watermark_without_wal_records() {
    // A fenced ticket is dead: it advances the watermark (the contiguous
    // resolved prefix) but leaves no WAL trace. A snapshot at the
    // watermark sees exactly the durable records.
    let ring = Ring::<8>::new();
    let durable = AtomicU32::new(0);
    let mut drainer = make_drainer::<8, 8>(&ring, &durable);
    drainer.set_stall_budget(0);

    // t0 published; t1 claimed but never published (stalled writer).
    let t0 = ring.try_claim().expect("space");
    let p0 = payload::encode_put(b"k0", b"v0").expect("fits");
    assert_eq!(ring.publish(t0, &p0), PublishOutcome::Published);
    let _t1 = ring.try_claim().expect("space");

    // First sweep drains t0; the stalled t1 is not fenced yet (progress
    // was made). Second sweep sees only the stalled t1 and fences it.
    let outcome = block_on(drainer.sweep()).expect("sweep ok");
    assert_eq!(outcome, SweepOutcome::Swept { tickets: 1 });
    assert_eq!(drainer.durable_watermark(), 1);
    let outcome = block_on(drainer.sweep()).expect("sweep ok");
    assert_eq!(outcome, SweepOutcome::Idle);
    assert_eq!(drainer.durable_watermark(), 2);

    // The Db holds only t0's record; the fenced ticket left no trace.
    let db = drainer.into_db();
    let mut buf = [0u8; 32];
    let len = block_on(db.get(b"k0", &mut buf))
        .expect("get ok")
        .expect("k0 present");
    assert_eq!(&buf[..len], b"v0");
}

#[test]
fn stalled_on_memtable_full_then_flush_and_retry() {
    // CAP=16: fill the memtable via the drainer, then verify Stalled,
    // flush, and successful retry with no ticket loss.
    let ring = Ring::<32>::new();
    let durable = AtomicU32::new(0);
    let mut drainer = make_drainer::<32, 8>(&ring, &durable);

    // Publish 16 puts (fills the memtable: CAP=16).
    for i in 0..16u8 {
        let t = ring.try_claim().expect("ring has space");
        let p = payload::encode_put(&[b'k', i], &[b'v', i]).expect("fits");
        assert_eq!(ring.publish(t, &p), PublishOutcome::Published);
    }
    // Two sweeps drain all 16 (MAX_WRITES=8).
    assert_eq!(
        block_on(drainer.sweep()).expect("sweep ok"),
        SweepOutcome::Swept { tickets: 8 }
    );
    assert_eq!(
        block_on(drainer.sweep()).expect("sweep ok"),
        SweepOutcome::Swept { tickets: 8 }
    );
    assert_eq!(durable.load(Ordering::Acquire), 16);

    // Publish 2 more. The memtable is full, so the sweep stalls.
    for i in 16..18u8 {
        let t = ring.try_claim().expect("ring has space");
        let p = payload::encode_put(&[b'k', i], &[b'v', i]).expect("fits");
        assert_eq!(ring.publish(t, &p), PublishOutcome::Published);
    }
    let outcome = block_on(drainer.sweep()).expect("sweep ok");
    assert_eq!(outcome, SweepOutcome::Stalled);
    // Nothing advanced; the tickets are buffered, not lost.
    assert_eq!(durable.load(Ordering::Acquire), 16);
    assert_eq!(drainer.pending(), 2);

    // Host flushes the memtable, then re-sweeps: the buffered tickets
    // are applied.
    block_on(drainer.flush()).expect("flush ok");
    let outcome = block_on(drainer.sweep()).expect("sweep ok");
    assert_eq!(outcome, SweepOutcome::Swept { tickets: 2 });
    assert_eq!(durable.load(Ordering::Acquire), 18);

    // All 18 keys are in the Db.
    let db = drainer.into_db();
    let mut buf = [0u8; 32];
    for i in 0..18u8 {
        let len = block_on(db.get(&[b'k', i], &mut buf))
            .expect("get ok")
            .expect("key present");
        assert_eq!(&buf[..len], &[b'v', i]);
    }
}

#[test]
fn ordered_prefix_durable_only_after_flush() {
    // Regression: t0 is WAL-bearing, t1 is skipped (dead). While t0's
    // flush is pending, durable must not advance at all — not even over
    // the skipped t1. After the flush succeeds, durable advances over
    // the entire contiguous resolved prefix (t0 and t1).
    let ring = Ring::<8>::new();
    let durable = AtomicU32::new(0);

    // Flush pends once: Db::open does not flush, so the first flush poll
    // is the sweep's Db::write.
    let device = PendingFlush::<_, 512>::new(MemDevice::<512>::new(), 1);
    let mut db = horton::Db::<_, 512, 32, 32, 16, 1024, 2, 4, 64, 0>::new(device, test_config());
    block_on(db.open()).expect("db open must succeed");
    let mut drainer =
        Drainer::<_, 512, 32, 32, 16, 1024, 2, 4, 64, 0, 8, 8>::new(&ring, db, &durable);

    // t0 published (WAL-bearing).
    let t0 = ring.try_claim().expect("space");
    let p0 = payload::encode_put(b"k0", b"v0").expect("fits");
    assert_eq!(ring.publish(t0, &p0), PublishOutcome::Published);
    // t1 claimed, then its writer died and the host force-released it:
    // the drainer will see it as Skipped.
    let t1 = ring.try_claim().expect("space");
    assert_eq!(ring.force_release_slot(t1), ForceReleaseOutcome::Released);

    // Hand-poll the sweep: the flush pends, so the sweep pends.
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let outcome = {
        let mut sweep = core::pin::pin!(drainer.sweep());
        assert_eq!(
            sweep.as_mut().poll(&mut cx),
            Poll::Pending,
            "sweep must pend on the flush"
        );
        // O1: durable has not advanced — t0's batch is not flush-acknowledged,
        // and the skipped t1 must not resolve early.
        assert_eq!(durable.load(Ordering::Acquire), 0);
        assert!(!is_ticket_durable(&durable, t0));
        assert!(!is_ticket_durable(&durable, t1));

        // Second poll: the flush completes and the sweep resolves both tickets.
        match sweep.as_mut().poll(&mut cx) {
            Poll::Ready(Ok(o)) => o,
            Poll::Ready(Err(e)) => panic!("sweep must succeed, got {e:?}"),
            Poll::Pending => panic!("sweep must complete after the flush"),
        }
    };
    assert_eq!(outcome, SweepOutcome::Swept { tickets: 2 });
    assert_eq!(durable.load(Ordering::Acquire), 2);
    assert!(is_ticket_durable(&durable, t0));
    assert!(is_ticket_durable(&durable, t1));

    // t0's data is in the Db; the skipped t1 left no trace.
    let db = drainer.into_db();
    let mut buf = [0u8; 32];
    let len = block_on(db.get(b"k0", &mut buf))
        .expect("get ok")
        .expect("k0 present");
    assert_eq!(&buf[..len], b"v0");
}
