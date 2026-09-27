//! WAL tests: encode/decode round-trip, recovery, and torn-tail handling.

mod common;

use core::task::{Context, Poll};

use common::{MemDevice, block_on, noop_waker};
use horton::BlockDevice;
use horton::memtable::MemTable;
use horton::wal::{Op, WalWriter};

type W512 = WalWriter<MemDevice<512>, 512>;
type T16 = MemTable<16, 512, 16, 32>;

fn writer() -> W512 {
    WalWriter::new(MemDevice::<512>::new(), 0, 16)
}

const fn flipped(
    dev: MemDevice<512>,
    block: u64,
    byte: usize,
) -> WalWriter<Flip<MemDevice<512>, 512>, 512> {
    WalWriter::new(
        Flip {
            inner: dev,
            block,
            byte,
        },
        0,
        16,
    )
}

/// Device wrapper that flips one byte of one block on every read.
struct Flip<D, const BLOCK: usize> {
    inner: D,
    block: u64,
    byte: usize,
}

impl<D: BlockDevice, const BLOCK: usize> BlockDevice for Flip<D, BLOCK> {
    type Error = D::Error;
    const BLOCK: usize = BLOCK;

    fn poll_read_block(
        &self,
        cx: &mut Context<'_>,
        id: u64,
        buf: &mut [u8],
    ) -> Poll<Result<(), Self::Error>> {
        let r = self.inner.poll_read_block(cx, id, buf);
        if id == self.block && matches!(&r, Poll::Ready(Ok(()))) {
            buf[self.byte] ^= 0xFF;
        }
        r
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
        self.inner.poll_flush(cx)
    }
}

#[test]
fn round_trip() {
    let mut w = writer();
    block_on(w.append(1, Op::Put, b"k1", b"v1")).unwrap();
    block_on(w.append(2, Op::Delete, b"k2", b"")).unwrap();
    block_on(w.append(3, Op::Put, b"k1", b"v2")).unwrap();
    block_on(w.commit()).unwrap();

    let mut t = T16::new();
    let st = block_on(w.recover(&mut t)).unwrap();
    assert_eq!(st.records, 3);
    assert_eq!(st.max_seq, 3);
    // 27 + 25 + 27 = 79 bytes of records: one 512-byte block.
    assert_eq!(st.blocks_used, 1);
    assert_eq!(t.get(b"k1").unwrap().val, b"v2");
    assert!(t.get(b"k2").unwrap().tombstone);
}

#[test]
fn empty_wal_recovers_empty() {
    let mut w = writer();
    let mut t = T16::new();
    let st = block_on(w.recover(&mut t)).unwrap();
    assert_eq!(st.records, 0);
    assert_eq!(st.blocks_used, 0);
    assert!(t.is_empty());
}

#[test]
fn commit_is_idempotent() {
    let mut w = writer();
    block_on(w.commit()).unwrap(); // nothing staged: just a flush
    block_on(w.append(1, Op::Put, b"k", b"v")).unwrap();
    block_on(w.commit()).unwrap();
    block_on(w.commit()).unwrap(); // already committed: flush only
    let mut t = T16::new();
    let st = block_on(w.recover(&mut t)).unwrap();
    assert_eq!(st.records, 1);
}

/// Sixteen 32-byte records exactly fill a 512-byte block; a corrupt second
/// block must stop recovery after the clean prefix.
#[test]
fn torn_block_stops_at_prefix() {
    let mut w = writer();
    // record_len = 23 + 1 + 8 = 32; sixteen of them fill the block exactly.
    for i in 0..16u8 {
        block_on(w.append(u64::from(i) + 1, Op::Put, &[b'a' + i], b"12345678")).unwrap();
    }
    block_on(w.commit()).unwrap(); // block 0, exactly full
    block_on(w.append(17, Op::Put, b"q", b"12345678")).unwrap();
    block_on(w.commit()).unwrap(); // block 1

    let dev = w.into_device();
    let mut w2 = flipped(dev, 1, 0);
    let mut t = T16::new();
    let st = block_on(w2.recover(&mut t)).unwrap();
    assert_eq!(st.records, 16); // block 0 intact; corrupt block 1 stops recovery
    assert_eq!(st.blocks_used, 1);
    assert_eq!(t.get(b"a").unwrap().val, b"12345678");
    assert_eq!(t.get(b"p").unwrap().val, b"12345678");
    assert!(t.get(b"q").is_none());
}

/// A CRC failure inside the first record drops the whole block: recovery
/// keeps only the records before the first corrupt one (here: none).
#[test]
fn crc_failure_drops_block() {
    let mut w = writer();
    for i in 0..16u8 {
        block_on(w.append(u64::from(i) + 1, Op::Put, &[b'a' + i], b"12345678")).unwrap();
    }
    block_on(w.commit()).unwrap();

    let dev = w.into_device();
    // Flip a byte inside the first record's key area (offset 19): magic and
    // length stay plausible, but the CRC no longer matches.
    let mut w2 = flipped(dev, 0, 19);
    let mut t = T16::new();
    let st = block_on(w2.recover(&mut t)).unwrap();
    assert_eq!(st.records, 0);
    assert!(t.is_empty());
}

/// A commit whose staged bytes are shorter than the previous commit's must
/// still zero-pad its own block correctly: `stage` is a buffer reused
/// across commits, not zeroed fresh each time, so a shorter batch leaves a
/// gap of the previous batch's leftover bytes that only the shrink itself
/// needs to clear.
#[test]
fn shrinking_commit_zero_pads_correctly() {
    // BLOCK = 512; record_len = 23 + 2 + 32 = 57 (VAL_MAX for T16 is 32):
    // block-0 commit.
    let mut w = writer();
    block_on(w.append(1, Op::Put, b"k1", &[b'a'; 32])).unwrap();
    block_on(w.commit()).unwrap();
    // record_len = 23 + 1 + 1 = 25: a smaller block-1 commit. Without
    // re-zeroing the shrunk gap, bytes [25..57) of the reused stage buffer
    // would still hold block 0's tail instead of the zero padding recovery
    // relies on to find the clean end of the log.
    block_on(w.append(2, Op::Put, b"z", b"v")).unwrap();
    block_on(w.commit()).unwrap();

    // Read block 1 back directly: a stray nonzero byte past the record
    // would still get silently classified as an (expected, non-error) torn
    // tail by `recover` below, so check the raw padding itself rather than
    // relying on recovery to notice.
    let dev = w.into_device();
    let mut buf = [0xFFu8; 512]; // poisoned: a no-op read would be caught too
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    assert_eq!(
        dev.poll_read_block(&mut cx, 1, &mut buf),
        Poll::Ready(Ok(()))
    );
    assert!(
        buf[25..].iter().all(|&b| b == 0),
        "stale bytes from the longer previous commit leaked past the record"
    );

    let mut w = WalWriter::<MemDevice<512>, 512>::new(dev, 0, 16);
    let mut t = T16::new();
    let st = block_on(w.recover(&mut t)).unwrap();
    assert_eq!(st.records, 2);
    assert_eq!(st.blocks_used, 2);
    assert_eq!(t.get(b"k1").unwrap().val, &[b'a'; 32][..]);
    assert_eq!(t.get(b"z").unwrap().val, b"v");
}

/// Records that do not fit in one block are rejected, not split (v0.1).
#[test]
fn oversize_record_rejected() {
    let mut w = writer(); // BLOCK = 512
    let big = [7u8; 512];
    // record_len = 23 + 1 + 512 = 536 > 512: rejected, never split.
    assert!(block_on(w.append(1, Op::Put, b"k", &big)).is_err());
}

/// F17 (rollback half): `truncate_stage` discards staged bytes that stay in
/// the buffer. The dirty mark must cover them, or the next, shorter record
/// is written with the discarded record's tail after it — a torn tail that
/// ends recovery early and loses every later block.
#[test]
fn truncated_stage_bytes_never_reach_the_device() {
    let mut w = writer();
    // Stage a long record, then roll it back (a failed commit).
    block_on(w.append(1, Op::Put, b"long-key", &[0xAB; 32])).unwrap();
    w.truncate_stage(0);
    // A shorter record takes its place, then a second block follows.
    block_on(w.append(1, Op::Put, b"k", b"v")).unwrap();
    block_on(w.commit()).unwrap();
    block_on(w.append(2, Op::Put, b"k2", b"v2")).unwrap();
    block_on(w.commit()).unwrap();

    let mut r = WalWriter::<MemDevice<512>, 512>::new(w.into_device(), 0, 16);
    let mut t = T16::new();
    let state = block_on(r.recover(&mut t)).unwrap();
    assert_eq!(state.records, 2, "recovery stopped at a garbage tail");
    assert!(t.get(b"k2").is_some());
}

/// F17 (recovery half): after recovery the staging buffer holds whatever
/// block was read last. When recovery ends at `wal_end` (a full region),
/// that block is not zero, and the first block written afterwards — here
/// after a wrap back to the region start — must still be zero past its
/// records.
#[test]
fn first_block_after_recovery_is_clean() {
    // A two-block region, filled: block 0 = seq 1, block 1 = seq 2 (long).
    let mut w = WalWriter::<MemDevice<512>, 512>::new(MemDevice::<512>::new(), 0, 2);
    block_on(w.append(1, Op::Put, b"a", b"1")).unwrap();
    block_on(w.commit()).unwrap();
    block_on(w.append(2, Op::Put, b"filler", &[0xCD; 32])).unwrap();
    block_on(w.commit()).unwrap();
    // Reopen: a fresh writer (nothing staged yet, so nothing marked dirty)
    // recovers both blocks and stops at wal_end, leaving block 1's bytes in
    // its staging buffer.
    let mut w = WalWriter::<MemDevice<512>, 512>::new(w.into_device(), 0, 2);
    let mut t = T16::new();
    let state = block_on(w.recover(&mut t)).unwrap();
    assert_eq!(state.records, 2);
    // Wrap (everything is flushed) and append two short records.
    w.reset_to(0);
    block_on(w.append(3, Op::Put, b"c", b"d")).unwrap();
    block_on(w.commit()).unwrap();
    block_on(w.append(4, Op::Put, b"e", b"f")).unwrap();
    block_on(w.commit()).unwrap();
    // Recover with the flush floor at 2: both new records must replay.
    let mut r = WalWriter::<MemDevice<512>, 512>::new(w.into_device(), 0, 2);
    let mut t2 = T16::new();
    let state = block_on(r.recover_from(&mut t2, 0, 2)).unwrap();
    assert_eq!(state.records, 2, "a garbage tail ended recovery early");
    assert!(t2.get(b"e").is_some());
}

/// Tears block 0 after its first `keep` bytes: the rest of the block reads
/// as garbage, the way a torn NOR program or erase leaves it.
fn tear_block0(dev: &mut MemDevice<512>, keep: usize) {
    let block = &mut dev.blocks_mut()[0];
    for b in &mut block[keep..] {
        *b = 0xA5;
    }
}

/// F19 (writer half): a torn block that still holds complete records ends
/// recovery, and the writer resumes on the next block. Records written
/// there after the reopen are newer than anything before the tear, so the
/// next recovery must read past the torn block to them.
#[test]
fn recovery_reads_past_a_torn_block_to_newer_records() {
    // Block 0: two single records, then the start of a torn third.
    let mut w = writer();
    block_on(w.append(1, Op::Put, b"a", b"1")).unwrap();
    block_on(w.append(2, Op::Put, b"b", b"2")).unwrap();
    block_on(w.append(3, Op::Put, b"c", b"3")).unwrap();
    block_on(w.commit()).unwrap();
    let mut dev = w.into_device();
    let two = 2 * horton::wal::WAL_RECORD_OVERHEAD + 4;
    tear_block0(&mut dev, two + 5);

    // First reopen: two records, and the writer resumes on block 1.
    let mut w = WalWriter::<MemDevice<512>, 512>::new(dev, 0, 16);
    let mut t = T16::new();
    assert_eq!(block_on(w.recover(&mut t)).unwrap().records, 2);
    block_on(w.append(4, Op::Put, b"d", b"4")).unwrap();
    block_on(w.commit()).unwrap();

    // Second reopen: the acknowledged record behind the torn block is live.
    let mut w = WalWriter::<MemDevice<512>, 512>::new(w.into_device(), 0, 16);
    let mut t = T16::new();
    let state = block_on(w.recover(&mut t)).unwrap();
    assert_eq!(state.records, 3);
    assert!(t.get(b"d").is_some(), "record 4 lost behind the torn block");
    assert!(t.get(b"c").is_none(), "the torn record must not replay");
}

/// The other side of that rule: a block of *older* records after a torn
/// block is stale data from before a wrap, and recovery stops at the tear.
#[test]
fn recovery_stops_at_a_torn_block_before_stale_records() {
    let mut w = writer();
    // Block 1 holds an old record (seq 1) from an earlier pass over the log.
    block_on(w.append(9, Op::Put, b"pad", b"0")).unwrap();
    block_on(w.commit()).unwrap();
    block_on(w.append(1, Op::Put, b"old", b"1")).unwrap();
    block_on(w.commit()).unwrap();
    // Block 0 is rewritten by the current pass: seqs 10, 11, then torn.
    w.reset_to(0);
    block_on(w.append(10, Op::Put, b"x", b"1")).unwrap();
    block_on(w.append(11, Op::Put, b"y", b"2")).unwrap();
    block_on(w.append(12, Op::Put, b"z", b"3")).unwrap();
    block_on(w.commit()).unwrap();
    let mut dev = w.into_device();
    tear_block0(&mut dev, 2 * horton::wal::WAL_RECORD_OVERHEAD + 4 + 5);

    let mut w = WalWriter::<MemDevice<512>, 512>::new(dev, 0, 16);
    let mut t = T16::new();
    let state = block_on(w.recover(&mut t)).unwrap();
    assert_eq!(state.records, 2);
    assert!(
        t.get(b"old").is_none(),
        "a stale record replayed past the tear"
    );
}

/// A group replays only with its closing record: every tear point inside
/// a three-record group recovers all of it or none of it.
#[test]
fn a_group_recovers_all_or_nothing_at_every_tear_point() {
    let rec = horton::wal::WAL_RECORD_OVERHEAD + 2;
    for keep in 0..=3 * rec {
        let mut w = writer();
        block_on(w.append_grouped(1, Op::Put, b"a", b"1", true)).unwrap();
        block_on(w.append_grouped(2, Op::Put, b"b", b"2", true)).unwrap();
        block_on(w.append_grouped(3, Op::Put, b"c", b"3", false)).unwrap();
        block_on(w.commit()).unwrap();
        let mut dev = w.into_device();
        // Zero the tail instead of garbage: a tear on a record boundary
        // then reads as clean padding, the hardest case.
        for b in &mut dev.blocks_mut()[0][keep..] {
            *b = 0;
        }
        let mut w = WalWriter::<MemDevice<512>, 512>::new(dev, 0, 16);
        let mut t = T16::new();
        let n = block_on(w.recover(&mut t)).unwrap().records;
        let expect = if keep == 3 * rec { 3 } else { 0 };
        assert_eq!(n, expect, "tear at byte {keep}");
    }
}

/// A group that would not fit the block is refused before any of it is
/// written, so a group can never straddle two blocks.
#[test]
fn a_group_never_straddles_blocks() {
    let mut w = writer();
    block_on(w.append_grouped(1, Op::Put, b"k", &[7u8; 300], true)).unwrap();
    let err = block_on(w.append_grouped(2, Op::Put, b"k", &[7u8; 300], false)).unwrap_err();
    assert!(
        matches!(err, horton::Error::BatchTooLarge { .. }),
        "got {err:?}"
    );
}
