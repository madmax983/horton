//! Manifest tests: encode/decode round trip, double-buffered commit,
//! slot failover, and the both-corrupt failure.

mod common;

use std::future::poll_fn;

use common::{block_on, MemDevice};
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
        entry_count: 5,
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
