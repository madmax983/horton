//! WAL batching tests: `append_batch` stages N records and flushes once,
//! with prefix-atomic failure semantics (SPEC §11).

mod common;

use core::task::{Context, Poll};

use common::{MemDevice, TornDevice, block_on};
use horton::memtable::MemTable;
use horton::wal::{BatchRecord, Op, WalWriter};
use horton::{BlockDevice, Error};

type W512<D> = WalWriter<D, 512>;
type T16 = MemTable<16, 512, 16, 32>;

/// Device wrapper that counts successful block writes and flushes.
struct Counting<D, const BLOCK: usize> {
    inner: D,
    writes: usize,
    flushes: usize,
}

impl<D, const BLOCK: usize> Counting<D, BLOCK> {
    const fn new(inner: D) -> Self {
        Self {
            inner,
            writes: 0,
            flushes: 0,
        }
    }

    fn into_inner(self) -> D {
        self.inner
    }
}

impl<D: BlockDevice, const BLOCK: usize> BlockDevice for Counting<D, BLOCK> {
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
        let r = self.inner.poll_write_block(cx, id, buf);
        if matches!(r, Poll::Ready(Ok(()))) {
            self.writes += 1;
        }
        r
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let r = self.inner.poll_flush(cx);
        if matches!(r, Poll::Ready(Ok(()))) {
            self.flushes += 1;
        }
        r
    }
}

const fn put<'a>(seq: u64, key: &'a [u8], val: &'a [u8]) -> BatchRecord<'a> {
    BatchRecord {
        seq,
        op: Op::Put,
        key,
        val,
        expire_at: 0,
    }
}

/// Ten small puts in one batch: one block write, exactly one flush —
/// versus ten writes + ten flushes on the per-put path.
#[test]
fn batch_single_flush() {
    let dev: Counting<MemDevice<512>, 512> = Counting::new(MemDevice::<512>::new());
    let mut w: W512<_> = WalWriter::new(dev, 0, 16);
    let keys: Vec<[u8; 3]> = (0..10).map(|i| [b'k', b'0', b'0' + i]).collect();
    let recs: Vec<BatchRecord<'_>> = keys
        .iter()
        .enumerate()
        .map(|(i, k)| put(i as u64 + 1, k, b"v01"))
        .collect();
    let rep = block_on(w.append_batch(&recs));
    assert!(rep.error.is_none());
    assert_eq!(rep.durable, 10);
    assert_eq!(rep.consumed, 10);

    let dev = w.into_device();
    assert_eq!(dev.writes, 1, "all ten records fit one 512-byte block");
    assert_eq!(dev.flushes, 1, "one flush for the whole batch");
    let mem = dev.into_inner();

    // The batch replays exactly like ten individual puts.
    let mut w2: W512<_> = WalWriter::new(mem, 0, 16);
    let mut t = T16::new();
    let st = block_on(w2.recover(&mut t)).unwrap();
    assert_eq!(st.records, 10);
    assert_eq!(st.max_seq, 10);
    assert_eq!(t.get(b"k09").unwrap().val, b"v01");
}

/// Five 220-byte records span three blocks: three writes, still one flush.
#[test]
fn batch_spans_blocks() {
    let dev: Counting<MemDevice<512>, 512> = Counting::new(MemDevice::<512>::new());
    let mut w: W512<_> = WalWriter::new(dev, 0, 16);
    let val = [7u8; 200];
    let recs = [
        put(1, b"aa", &val),
        put(2, b"bb", &val),
        put(3, b"cc", &val),
        put(4, b"dd", &val),
        put(5, b"ee", &val),
    ];
    let rep = block_on(w.append_batch(&recs));
    assert!(rep.error.is_none());
    assert_eq!(rep.durable, 5);

    let dev = w.into_device();
    // 220 bytes each: two per block; the fifth spills to a third block.
    assert_eq!(dev.writes, 3);
    assert_eq!(dev.flushes, 1);
}

/// An empty batch is a no-op: no writes, no flush, clean report.
#[test]
fn batch_empty_is_noop() {
    let dev: Counting<MemDevice<512>, 512> = Counting::new(MemDevice::<512>::new());
    let mut w: W512<_> = WalWriter::new(dev, 0, 16);
    let rep = block_on(w.append_batch(&[]));
    assert!(rep.error.is_none());
    assert_eq!(rep.durable, 0);
    assert_eq!(rep.consumed, 0);
    let dev = w.into_device();
    assert_eq!(dev.writes, 0);
    assert_eq!(dev.flushes, 0);
}

/// A validation failure mid-batch commits the good prefix and reports the
/// error: prefix-atomic, not all-or-nothing.
#[test]
fn batch_partial_on_value_too_large() {
    let dev: Counting<MemDevice<512>, 512> = Counting::new(MemDevice::<512>::new());
    let mut w: W512<_> = WalWriter::new(dev, 0, 16);
    let big = vec![9u8; 66_000]; // exceeds the u16 wire field
    let recs = [
        put(1, b"k1", b"v1"),
        put(2, b"k2", b"v2"),
        put(3, b"k3", &big),
        put(4, b"k4", b"v4"),
    ];
    let rep = block_on(w.append_batch(&recs));
    assert_eq!(rep.durable, 2, "good prefix is durable");
    assert_eq!(rep.consumed, 2);
    match rep.error {
        Some(Error::ValueTooLarge { .. }) => {}
        other => panic!("expected ValueTooLarge, got {other:?}"),
    }

    let dev = w.into_device();
    assert_eq!(dev.flushes, 1, "the prefix still flushes once");
    let mem = dev.into_inner();
    let mut w2: W512<_> = WalWriter::new(mem, 0, 16);
    let mut t = T16::new();
    let st = block_on(w2.recover(&mut t)).unwrap();
    assert_eq!(st.records, 2);
    assert_eq!(t.get(b"k1").unwrap().val, b"v1");
    assert!(t.get(b"k3").is_none());
}

/// WAL region exhaustion mid-batch: blocks landed but the final flush is
/// impossible, so nothing is acked — yet the landed seqnums are consumed
/// (they may replay on a later flush) and never reusable.
#[test]
fn batch_wal_full_consumes_seqnums() {
    let dev: Counting<MemDevice<512>, 512> = Counting::new(MemDevice::<512>::new());
    // Two-block region: room for two full stage writes, then exhaustion.
    let mut w: W512<_> = WalWriter::new(dev, 0, 2);
    let val = [3u8; 280]; // ~300-byte records: one full block each, roughly
    let recs = [
        put(1, b"a1", &val),
        put(2, b"a2", &val),
        put(3, b"a3", &val),
        put(4, b"a4", &val),
    ];
    let rep = block_on(w.append_batch(&recs));
    assert_eq!(rep.durable, 0, "the flush never happened: nothing acked");
    assert_eq!(rep.consumed, 3, "three records reached the device");
    match rep.error {
        Some(Error::WalFull) => {}
        other => panic!("expected WalFull, got {other:?}"),
    }
    // The failed batch left no staged garbage behind for the next writer:
    // a fresh batch on a fresh region starts clean.
    let dev = w.into_device();
    let mem = dev.into_inner();
    let mut w2: W512<_> = WalWriter::new(mem, 0, 16);
    let recs2 = [put(11, b"b1", b"v")];
    let rep2 = block_on(w2.append_batch(&recs2));
    assert!(rep2.error.is_none());
    assert_eq!(rep2.durable, 1);
}

/// A torn block in the middle of a batch replays only the clean prefix:
/// the batch's crash boundary is the record, via CRC (existing rule).
#[test]
fn batch_torn_tail_recovers_prefix() {
    let inner = MemDevice::<512>::new();
    let dev: TornDevice<MemDevice<512>, 512> = TornDevice::new(inner, 1, 20);
    let mut w: W512<_> = WalWriter::new(dev, 0, 16);
    // ~46-byte records: eleven fit per 512-byte block; thirteen span two.
    let val = [5u8; 20];
    let keys: Vec<[u8; 3]> = (0..13u8)
        .map(|i| [b't', b'0' + i / 10, b'0' + i % 10])
        .collect();
    let recs: Vec<BatchRecord<'_>> = keys
        .iter()
        .enumerate()
        .map(|(i, k)| put(i as u64 + 1, k, &val))
        .collect();
    let rep = block_on(w.append_batch(&recs));
    // The torn write still "succeeds" from the writer's view (power died
    // mid-block); the batch reports complete — recovery decides.
    assert!(rep.error.is_none());
    assert_eq!(rep.durable, 13);

    let torn = w.into_device();
    let mem = torn.into_inner();
    let mut w2: W512<_> = WalWriter::new(mem, 0, 16);
    let mut t = T16::new();
    let st = block_on(w2.recover(&mut t)).unwrap();
    assert_eq!(
        st.records, 11,
        "torn second block stops recovery at its prefix"
    );
    assert_eq!(t.get(b"t00").unwrap().val, val);
    assert_eq!(t.get(b"t10").unwrap().val, val);
    assert!(t.get(b"t11").is_none());
}

/// All four op kinds flow through one batch and replay correctly.
#[test]
fn batch_mixed_ops() {
    let dev: Counting<MemDevice<512>, 512> = Counting::new(MemDevice::<512>::new());
    let mut w: W512<_> = WalWriter::new(dev, 0, 16);
    let recs = [
        put(1, b"m1", b"v1"),
        BatchRecord {
            seq: 2,
            op: Op::Delete,
            key: b"m1",
            val: b"",
            expire_at: 0,
        },
        BatchRecord {
            seq: 3,
            op: Op::RangeDelete,
            key: b"r1",
            val: b"r9",
            expire_at: 0,
        },
        BatchRecord {
            seq: 4,
            op: Op::PutTtl,
            key: b"m2",
            val: b"v2",
            expire_at: 999_999,
        },
    ];
    let rep = block_on(w.append_batch(&recs));
    assert!(rep.error.is_none());
    assert_eq!(rep.durable, 4);
    assert_eq!(rep.consumed, 4);

    let dev = w.into_device();
    assert_eq!(dev.flushes, 1);
    let mem = dev.into_inner();
    let mut w2: W512<_> = WalWriter::new(mem, 0, 16);
    let mut t = T16::new();
    let st = block_on(w2.recover(&mut t)).unwrap();
    assert_eq!(st.records, 4);
    assert_eq!(st.max_seq, 4);
    assert!(t.get(b"m1").unwrap().tombstone, "delete replayed");
    assert_eq!(t.get(b"m2").unwrap().val, b"v2", "ttl put replayed");
}
