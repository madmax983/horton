//! Manifest tests: encode/decode round trip, double-buffered and ring
//! commits, multi-block copies (torn commits fall back), slot failover,
//! and the both-corrupt failure.

mod common;

use std::future::poll_fn;

use common::{Lcg, MemDevice, block_on};
use horton::manifest::{KeyBound, Manifest, TableRef};
use horton::{BlockDevice, Error, ManifestEdit};

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

// ---------------------------------------------------------------------------
// Manifest edits (F8): commits stage a small edit instead of a manifest
// copy. The committed view and the in-memory result must be exactly what
// the classic mutation API produces.
// ---------------------------------------------------------------------------

type EditManifest = Manifest<3, 4, 16>;

fn key(n: u64) -> KeyBound<16> {
    KeyBound::from_slice(format!("k{n:06}").as_bytes()).unwrap()
}

fn ref_span(id: u32, lo: u64, hi: u64) -> TableRef<16> {
    TableRef {
        id,
        first_block: u64::from(id) * 8,
        block_count: 4,
        first_key: key(lo),
        last_key: key(hi),
        max_seq: 10 + u64::from(id),
        min_seq: 1,
        entry_count: 3,
        rdel_blocks: 0,
    }
}

/// Everything observable about a manifest except `seq`.
fn state(m: &EditManifest) -> (Vec<Vec<TableRef<16>>>, [u64; 4]) {
    let levels = (0..3).map(|l| m.level(l).unwrap().to_vec()).collect();
    let scalars = [
        m.wal_head(),
        u64::from(m.next_table_id()),
        m.flushed_seq(),
        m.seq_high(),
    ];
    (levels, scalars)
}

/// A random manifest: up to 4 overlapping L0 tables, and sorted disjoint
/// runs in L1 and L2, within the 12-ref pool.
fn random_manifest(rng: &mut Lcg) -> EditManifest {
    let mut m = EditManifest::new();
    m.set_wal_head(rng.next() % 1000);
    m.note_flushed(rng.next() % 1000);
    m.advance_next_table_id(50);
    let mut id = 1;
    for _ in 0..rng.next_bounded(5) {
        let lo = rng.next() % 900;
        m.add_l0_table::<DevError>(ref_span(id, lo, lo + rng.next() % 90))
            .unwrap();
        id += 1;
    }
    for level in 1..3 {
        let mut at = rng.next() % 50;
        for _ in 0..rng.next_bounded(5) {
            if m.free_refs() == 0 {
                break;
            }
            let hi = at + 1 + rng.next() % 40;
            m.add_table_to_level::<DevError>(level, ref_span(id, at, hi))
                .unwrap();
            id += 1;
            at = hi + 1 + rng.next() % 20;
        }
    }
    m
}

#[test]
fn committed_edits_match_the_classic_mutations() {
    let mut rng = Lcg::new(0xED17);
    let mut scratch = [0u8; BLOCK];
    for round in 0..500 {
        let base = random_manifest(&mut rng);
        let mut edit = ManifestEdit::new();
        let mut expected = base;
        // Scalars.
        if rng.next().is_multiple_of(2) {
            let h = rng.next() % 1000;
            edit.set_wal_head(h);
            expected.set_wal_head(h);
        }
        let flushed = rng.next() % 2000;
        edit.note_flushed(flushed);
        expected.note_flushed(flushed);
        let high = rng.next() % 3000;
        edit.raise_seq_high(high);
        expected.raise_seq_high(high);
        let next_id = u32::try_from(rng.next() % 100).unwrap();
        edit.advance_next_table_id(next_id);
        expected.advance_next_table_id(next_id);
        // Narrow one deeper table's first key within its own range (so the
        // level stays sorted), plus any L0 table containing that key.
        let mut narrowed = Vec::new();
        let deep = rng.next_bounded(2) + 1;
        let deep_tables = base.level(deep).unwrap().to_vec();
        let mut narrow_key = None;
        if !deep_tables.is_empty() && rng.next().is_multiple_of(2) {
            let r = deep_tables[rng.next_bounded(deep_tables.len())];
            let lo: u64 = std::str::from_utf8(&r.first_key.as_slice()[1..])
                .unwrap()
                .parse()
                .unwrap();
            let hi: u64 = std::str::from_utf8(&r.last_key.as_slice()[1..])
                .unwrap()
                .parse()
                .unwrap();
            let k = key(lo + 1 + rng.next() % (hi - lo));
            narrow_key = Some(k);
            narrowed.push((deep, r.id));
            for t in base.l0() {
                if t.first_key.as_slice() < k.as_slice() && k.as_slice() <= t.last_key.as_slice() {
                    narrowed.push((0, t.id));
                }
            }
        }
        for &(l, id) in &narrowed {
            let k = narrow_key.unwrap();
            assert!(base.stage_narrow::<DevError>(&mut edit, l, id, k).unwrap());
            assert!(expected.narrow_table::<DevError>(l, id, k).unwrap());
        }
        // Remove a random subset of the rest.
        for l in 0..3 {
            for t in base.level(l).unwrap() {
                if !narrowed.contains(&(l, t.id)) && rng.next().is_multiple_of(3) {
                    assert!(base.stage_remove::<DevError>(&mut edit, l, t.id).unwrap());
                    assert!(
                        expected
                            .remove_table_from_level::<DevError>(l, t.id)
                            .unwrap()
                    );
                }
            }
        }
        // Add one table somewhere, when it fits.
        if !rng.next().is_multiple_of(4) {
            let level = rng.next_bounded(3);
            let lo = rng.next() % 900;
            let t = ref_span(99, lo, lo + 5);
            if expected.add_table_to_level::<DevError>(level, t).is_ok() {
                edit.add::<DevError>(level, t).unwrap();
            }
        }
        // In memory.
        let mut applied = base;
        applied.apply_edit::<DevError>(&edit).unwrap();
        assert_eq!(state(&applied), state(&expected), "round {round}: apply");
        // Committed: the durable view equals the classic result.
        let mut dev = MemDevice::<BLOCK>::new();
        let mut committed = base;
        let layout = horton::ManifestLayout::ring(0, 3);
        block_on(committed.commit_edit(&edit, &mut dev, &mut scratch, layout)).unwrap();
        assert_eq!(committed.seq(), base.seq() + 1);
        assert_eq!(state(&committed), state(&expected), "round {round}: commit");
        let (back, _) =
            block_on(EditManifest::recover_from(&mut dev, &mut scratch, layout)).unwrap();
        assert_eq!(state(&back), state(&expected), "round {round}: recover");
    }
}

/// A device whose writes fail.
struct FailingDevice {
    writes: usize,
}

impl BlockDevice for FailingDevice {
    type Error = ();
    const BLOCK: usize = BLOCK;
    fn poll_read_block(
        &self,
        _cx: &mut core::task::Context<'_>,
        _id: u64,
        buf: &mut [u8],
    ) -> core::task::Poll<Result<(), ()>> {
        buf.fill(0);
        core::task::Poll::Ready(Ok(()))
    }
    fn poll_write_block(
        &mut self,
        _cx: &mut core::task::Context<'_>,
        _id: u64,
        _buf: &[u8],
    ) -> core::task::Poll<Result<(), ()>> {
        self.writes += 1;
        core::task::Poll::Ready(Err(()))
    }
    fn poll_flush(
        &mut self,
        _cx: &mut core::task::Context<'_>,
    ) -> core::task::Poll<Result<(), ()>> {
        core::task::Poll::Ready(Ok(()))
    }
}

#[test]
fn failed_edit_commit_leaves_the_manifest_unchanged() {
    let mut rng = Lcg::new(3);
    let base = random_manifest(&mut rng);
    let mut m = base;
    let mut edit = ManifestEdit::new();
    edit.set_wal_head(77);
    edit.add::<()>(1, ref_span(99, 990, 995)).unwrap();
    let mut dev = FailingDevice { writes: 0 };
    let mut scratch = [0u8; BLOCK];
    let res = block_on(m.commit_edit(&edit, &mut dev, &mut scratch, pair_layout()));
    assert_eq!(res, Err(Error::Device(())));
    assert_eq!(dev.writes, 1);
    assert_eq!(m.seq(), base.seq());
    assert_eq!(state(&m), state(&base));
}

#[test]
fn unfit_edits_fail_before_any_io() {
    let mut m = EditManifest::new();
    for id in 0..4 {
        m.add_l0_table::<()>(ref_span(id, 0, 9)).unwrap();
    }
    let mut dev = FailingDevice { writes: 0 };
    let mut scratch = [0u8; BLOCK];
    // L0 is full.
    let mut edit = ManifestEdit::new();
    edit.add::<()>(0, ref_span(9, 0, 9)).unwrap();
    let res = block_on(m.commit_edit(&edit, &mut dev, &mut scratch, pair_layout()));
    assert_eq!(res, Err(Error::ManifestFull));
    // ...unless the same edit removes an L0 table.
    assert!(m.stage_remove::<()>(&mut edit, 0, 2).unwrap());
    let mut applied = m;
    applied.apply_edit::<()>(&edit).unwrap();
    assert_eq!(applied.l0().len(), 4);
    // No such level.
    let mut edit = ManifestEdit::new();
    edit.add::<()>(3, ref_span(9, 0, 9)).unwrap();
    let res = block_on(m.commit_edit(&edit, &mut dev, &mut scratch, pair_layout()));
    assert_eq!(res, Err(Error::BadLevel { level: 3 }));
    assert_eq!(
        m.stage_remove::<()>(&mut ManifestEdit::new(), 5, 1),
        Err(Error::BadLevel { level: 5 })
    );
    // One add per edit; one narrowing key per edit.
    let mut edit = ManifestEdit::new();
    edit.add::<()>(1, ref_span(9, 0, 9)).unwrap();
    assert_eq!(
        edit.add::<()>(1, ref_span(10, 20, 29)),
        Err(Error::ManifestFull)
    );
    assert!(m.stage_narrow::<()>(&mut edit, 0, 0, key(3)).unwrap());
    assert_eq!(
        m.stage_narrow::<()>(&mut edit, 0, 1, key(4)),
        Err(Error::ManifestFull)
    );
    assert_eq!(dev.writes, 0, "every refusal came before I/O");
}
