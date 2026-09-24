//! Manifest tests: encode/decode round trip, double-buffered and ring
//! commits, multi-block copies (torn commits fall back), slot failover,
//! and the both-corrupt failure.

mod common;

use std::future::poll_fn;

use common::{MemDevice, block_on};
use horton::manifest::{KeyBound, Manifest, TableRef};
use horton::{BlockDevice, Error};

const BLOCK: usize = 4096;
const SLOT_A: u64 = 0;
const SLOT_B: u64 = 1;

type TestManifest = Manifest<2, 4, 256>;
type DevError = core::convert::Infallible;

fn tref(id: u32, first_block: u64) -> TableRef<256> {
    TableRef {
        id,
        first_block,
        block_count: 4,
        first_key: KeyBound::from_slice(b"a").unwrap(),
        last_key: KeyBound::from_slice(b"z").unwrap(),
        max_seq: 10,
        min_seq: 0,
        entry_count: 5,
        rdel_blocks: 0,
    }
}

fn read_block(dev: &MemDevice<BLOCK>, id: u64) -> [u8; BLOCK] {
    let mut buf = [0u8; BLOCK];
    block_on(poll_fn(|cx| dev.poll_read_block(cx, id, &mut buf))).unwrap();
    buf
}

fn write_block(dev: &mut MemDevice<BLOCK>, id: u64, buf: &[u8; BLOCK]) {
    let owned = *buf;
    block_on(poll_fn(|cx| dev.poll_write_block(cx, id, &owned))).unwrap();
}

/// Sequence number in `slot`, or `None` when the slot is blank or corrupt.
fn slot_seq(dev: &MemDevice<BLOCK>, slot: u64) -> Option<u64> {
    let blk = read_block(dev, slot);
    TestManifest::decode::<DevError, BLOCK>(&blk)
        .ok()
        .map(|m| m.seq())
}

#[test]
fn fresh_device_recovers_blank() {
    let mut dev = MemDevice::<BLOCK>::new();
    let mut scratch = [0u8; BLOCK];
    let (m, fresh) = block_on(TestManifest::recover(
        &mut dev,
        &mut scratch,
        SLOT_A,
        SLOT_B,
    ))
    .unwrap();
    assert!(fresh);
    assert_eq!(m.seq(), 0);
    assert_eq!(m.l0().len(), 0);
}

#[test]
fn commit_then_recover() {
    let mut dev = MemDevice::<BLOCK>::new();
    let mut m = TestManifest::new();
    m.set_wal_head(8);
    m.add_l0_table::<DevError>(tref(0, 136)).unwrap();
    let mut scratch = [0u8; BLOCK];
    block_on(m.commit(&mut dev, &mut scratch, SLOT_A, SLOT_B)).unwrap();
    assert_eq!(m.seq(), 1);

    let (rec, fresh) = block_on(TestManifest::recover(
        &mut dev,
        &mut scratch,
        SLOT_A,
        SLOT_B,
    ))
    .unwrap();
    assert!(!fresh);
    assert_eq!(rec.seq(), 1);
    assert_eq!(rec.wal_head(), 8);
    assert_eq!(rec.l0().len(), 1);
    let t = &rec.l0()[0];
    assert_eq!(t.id, 0);
    assert_eq!(t.first_block, 136);
    assert_eq!(t.block_count, 4);
    assert_eq!(t.first_key.as_slice(), b"a");
    assert_eq!(t.last_key.as_slice(), b"z");
    assert_eq!(t.max_seq, 10);
    assert_eq!(t.entry_count, 5);
}

#[test]
fn slot_failover() {
    let mut dev = MemDevice::<BLOCK>::new();
    let mut m = TestManifest::new();
    m.add_l0_table::<DevError>(tref(0, 136)).unwrap();
    let mut scratch = [0u8; BLOCK];
    block_on(m.commit(&mut dev, &mut scratch, SLOT_A, SLOT_B)).unwrap();
    // seq 1 is odd → slot B.
    assert_eq!(slot_seq(&dev, SLOT_A), None);
    assert_eq!(slot_seq(&dev, SLOT_B), Some(1));

    // Corrupt the committed slot (flip a byte inside the CRC'd payload).
    let mut blk = read_block(&dev, SLOT_B);
    blk[20] ^= 0xFF;
    write_block(&mut dev, SLOT_B, &blk);
    assert_eq!(slot_seq(&dev, SLOT_B), None);

    // The next commit lands on the alternate slot (seq 2 is even → slot A).
    m.add_l0_table::<DevError>(tref(1, 140)).unwrap();
    block_on(m.commit(&mut dev, &mut scratch, SLOT_A, SLOT_B)).unwrap();
    assert_eq!(slot_seq(&dev, SLOT_A), Some(2));

    // Recovery picks the surviving slot.
    let (rec, fresh) = block_on(TestManifest::recover(
        &mut dev,
        &mut scratch,
        SLOT_A,
        SLOT_B,
    ))
    .unwrap();
    assert!(!fresh);
    assert_eq!(rec.seq(), 2);
    assert_eq!(rec.l0().len(), 2);
}

#[test]
fn both_slots_corrupt_fails() {
    let mut dev = MemDevice::<BLOCK>::new();
    let mut m = TestManifest::new();
    let mut scratch = [0u8; BLOCK];
    block_on(m.commit(&mut dev, &mut scratch, SLOT_A, SLOT_B)).unwrap();
    // seq 1 → slot B holds a valid manifest; poison the blank slot with
    // non-zero garbage so it reads as corrupt rather than blank.
    write_block(&mut dev, SLOT_A, &[0xAB; BLOCK]);
    let mut blk = read_block(&dev, SLOT_B);
    blk[20] ^= 0xFF;
    write_block(&mut dev, SLOT_B, &blk);
    let res = block_on(TestManifest::recover(
        &mut dev,
        &mut scratch,
        SLOT_A,
        SLOT_B,
    ));
    assert!(matches!(res, Err(Error::CorruptManifest)));
}

#[test]
fn blank_paired_with_corrupt_is_fresh() {
    // A torn *first* commit: slot A corrupt, slot B never written.
    let mut dev = MemDevice::<BLOCK>::new();
    write_block(&mut dev, SLOT_A, &[0xAB; BLOCK]);
    let mut scratch = [0u8; BLOCK];
    let (m, fresh) = block_on(TestManifest::recover(
        &mut dev,
        &mut scratch,
        SLOT_A,
        SLOT_B,
    ))
    .unwrap();
    assert!(fresh);
    assert_eq!(m.seq(), 0);
}

#[test]
fn encode_decode_round_trip() {
    let mut m = TestManifest::new();
    m.set_wal_head(42);
    m.add_l0_table::<DevError>(tref(7, 200)).unwrap();
    m.add_l0_table::<DevError>(tref(8, 300)).unwrap();
    let mut buf = [0u8; BLOCK];
    m.encode::<DevError, BLOCK>(&mut buf).unwrap();
    let back = TestManifest::decode::<DevError, BLOCK>(&buf).unwrap();
    // Re-encoding must be byte-identical.
    let mut buf2 = [0u8; BLOCK];
    back.encode::<DevError, BLOCK>(&mut buf2).unwrap();
    assert_eq!(buf, buf2);
    assert_eq!(back.wal_head(), 42);
    assert_eq!(back.l0().len(), 2);
    assert_eq!(back.next_table_id(), 0);
}

#[test]
fn old_magic_is_rejected_not_misparsed() {
    // v0.4.0 wrote this same layout under magic "hrtman01"; v0.4.1 bumped
    // the magic to "hrtman02" (format policy: the magic changes whenever the
    // layout changes). Old bytes must be rejected outright, never decoded
    // into a garbage manifest.
    let mut m = TestManifest::new();
    m.add_table_to_level::<DevError>(0, tref(0, 100)).unwrap();
    let mut buf = [0u8; BLOCK];
    m.encode::<DevError, BLOCK>(&mut buf).unwrap();
    buf[0..8].copy_from_slice(b"hrtman01");
    let res = TestManifest::decode::<DevError, BLOCK>(&buf);
    assert!(
        matches!(res, Err(Error::CorruptManifest)),
        "old magic must be rejected, got {res:?}"
    );
}

#[test]
fn encode_crc_tail_is_bounds_checked() {
    // Regression: the trailing CRC write was not bounds-checked. A payload
    // leaving fewer than 4 bytes for the CRC panicked instead of failing
    // with an error (production code must have no panic paths).
    // Manifest<1, 1, 8> with a 2-byte first key: payload = 91, so the CRC
    // would land at [103..107] in a 106-byte block.
    let mut m = Manifest::<1, 1, 8>::new();
    m.add_l0_table::<DevError>(TableRef {
        id: 7,
        first_block: 100,
        block_count: 4,
        first_key: KeyBound::from_slice(b"ab").unwrap(),
        last_key: KeyBound::from_slice(b"z").unwrap(),
        max_seq: 10,
        min_seq: 0,
        entry_count: 5,
        rdel_blocks: 0,
    })
    .unwrap();
    let mut buf = [0u8; 106];
    let res = m.encode::<DevError, 106>(&mut buf);
    assert!(
        matches!(res, Err(Error::ManifestFull)),
        "CRC without room must be ManifestFull, not a panic"
    );
}

// ---------------------------------------------------------------------------
// Multi-block copies and rings (v0.17). A copy spans as many blocks as the
// worst case needs; every block carries the commit's seq, so a commit torn
// between blocks leaves its copy invalid and the previous one wins.
// ---------------------------------------------------------------------------

/// A table ref with 256-byte bounds: eight of them outgrow one block.
fn long_ref(id: u32) -> TableRef<256> {
    let mut t = tref(id, 100 + u64::from(id) * 10);
    let mut lo = [b'k'; 256];
    lo[255] = u8::try_from(id).unwrap();
    let mut hi = lo;
    hi[254] = b'z';
    t.first_key = KeyBound::from_slice(&lo).unwrap();
    t.last_key = KeyBound::from_slice(&hi).unwrap();
    t
}

/// `TestManifest` holding `n` long refs in L1 (sorted, disjoint).
fn long_manifest(n: u32) -> TestManifest {
    let mut m = TestManifest::new();
    m.set_wal_head(8);
    for id in 0..n {
        m.add_table_to_level::<DevError>(1, long_ref(id)).unwrap();
    }
    m
}

/// Copies are `max_blocks` long: the two-copy layout for this shape.
const fn pair_layout() -> horton::ManifestLayout {
    let stride = TestManifest::max_blocks::<BLOCK>();
    horton::ManifestLayout::pair(0, stride)
}

#[test]
fn max_blocks_covers_the_worst_case() {
    // 2 levels x 4 tables x (44 + 2 * 256) bytes plus the fixed fields.
    assert_eq!(TestManifest::max_blocks::<BLOCK>(), 2);
    let full = long_manifest(8);
    assert_eq!(full.encoded_blocks::<BLOCK>(), 2);
    assert_eq!(long_manifest(7).encoded_blocks::<BLOCK>(), 1);
    assert_eq!(TestManifest::new().encoded_blocks::<BLOCK>(), 1);
}

#[test]
fn multi_block_copy_round_trips() {
    let mut dev = MemDevice::<BLOCK>::new();
    let mut scratch = [0u8; BLOCK];
    let mut m = long_manifest(8);
    assert_eq!(m.encoded_blocks::<BLOCK>(), 2, "setup: two blocks");
    block_on(m.commit_to(&mut dev, &mut scratch, pair_layout())).unwrap();
    let (back, fresh) = block_on(TestManifest::recover_from(
        &mut dev,
        &mut scratch,
        pair_layout(),
    ))
    .unwrap();
    assert!(!fresh);
    assert_eq!(back.seq(), 1);
    assert_eq!(back.level(1).unwrap(), m.level(1).unwrap());
    assert_eq!(back.wal_head(), 8);
}

#[test]
fn torn_multi_block_commit_falls_back_to_the_previous_copy() {
    let layout = pair_layout();
    let stride = TestManifest::max_blocks::<BLOCK>();
    // Commit 1 lands whole in copy 1 (two blocks).
    let mut first = long_manifest(8);
    let mut dev = MemDevice::<BLOCK>::new();
    let mut scratch = [0u8; BLOCK];
    block_on(first.commit_to(&mut dev, &mut scratch, layout)).unwrap();
    let base = dev.clone();
    // Commit 2 (two blocks, copy 0), written whole to a second device so
    // either of its blocks can be landed alone.
    let mut second = first;
    second.set_wal_head(9);
    let mut whole = dev.clone();
    block_on(second.commit_to(&mut whole, &mut scratch, layout)).unwrap();
    assert_eq!(second.seq(), 2);
    let copy = layout.copy_start(0, stride);
    for torn in [copy, copy + 1] {
        let mut dev = base.clone();
        write_block(&mut dev, torn, &read_block(&whole, torn));
        let (back, _) =
            block_on(TestManifest::recover_from(&mut dev, &mut scratch, layout)).unwrap();
        assert_eq!(back.seq(), 1, "block {torn} alone must not win");
        assert_eq!(back.wal_head(), 8);
        assert_eq!(back.level(1).unwrap().len(), 8);
    }
    // Both blocks landed: commit 2 wins.
    let (back, _) = block_on(TestManifest::recover_from(&mut whole, &mut scratch, layout)).unwrap();
    assert_eq!(back.seq(), 2);
    assert_eq!(back.wal_head(), 9);
}

#[test]
fn stale_trailing_blocks_do_not_join_a_shorter_copy() {
    // A two-block commit, then enough commits that the same copy is
    // rewritten with a one-block manifest: its old second block remains
    // on the device but is outside the new count.
    let layout = pair_layout();
    let mut dev = MemDevice::<BLOCK>::new();
    let mut scratch = [0u8; BLOCK];
    let mut m = long_manifest(8);
    block_on(m.commit_to(&mut dev, &mut scratch, layout)).unwrap(); // seq 1
    block_on(m.commit_to(&mut dev, &mut scratch, layout)).unwrap(); // seq 2
    let mut small = TestManifest::new();
    small.set_wal_head(8);
    // Carry the sequence forward: commit until seq 3 lands in copy 1.
    for _ in 0..3 {
        block_on(small.commit_to(&mut dev, &mut scratch, layout)).unwrap();
    }
    let (back, _) = block_on(TestManifest::recover_from(&mut dev, &mut scratch, layout)).unwrap();
    assert_eq!(back.seq(), 3);
    assert_eq!(back.level(1).unwrap().len(), 0);
}

#[test]
fn ring_rotates_and_recovers_the_newest_copy() {
    let stride = TestManifest::max_blocks::<BLOCK>();
    let layout = horton::ManifestLayout::ring(10, 4);
    assert_eq!(layout.copies(), 4);
    let mut dev = MemDevice::<BLOCK>::new();
    let mut scratch = [0u8; BLOCK];
    let mut m = TestManifest::new();
    m.set_wal_head(8);
    for i in 0..9u32 {
        m.add_l0_table::<DevError>(tref(i % 4, 500)).ok();
        m.set_wal_head(8 + u64::from(i));
        block_on(m.commit_to(&mut dev, &mut scratch, layout)).unwrap();
        // Commit `seq` lands in copy `seq % 4`.
        let copy = layout.copy_start(u32::try_from(m.seq() % 4).unwrap(), stride);
        assert_eq!(copy, 10 + (m.seq() % 4) * stride);
        let blk = read_block(&dev, copy);
        assert_eq!(
            TestManifest::decode::<DevError, BLOCK>(&blk).map(|x| x.seq()),
            Ok(m.seq())
        );
        let (back, _) =
            block_on(TestManifest::recover_from(&mut dev, &mut scratch, layout)).unwrap();
        assert_eq!(back.seq(), m.seq());
        assert_eq!(back.wal_head(), 8 + u64::from(i));
    }
    // Corrupt the newest copy: the one before it wins.
    let newest = layout.copy_start(u32::try_from(m.seq() % 4).unwrap(), stride);
    write_block(&mut dev, newest, &[0x5A; BLOCK]);
    let (back, _) = block_on(TestManifest::recover_from(&mut dev, &mut scratch, layout)).unwrap();
    assert_eq!(back.seq(), m.seq() - 1);
}
