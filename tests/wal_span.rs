//! Atomic groups that span WAL blocks (ADR-0029).
//!
//! A group larger than what is left of a block continues into the next
//! one: the last record of a block whose group goes on carries `SPAN`, the
//! first record of the next block `CONT`. Recovery replays a spanning
//! group only when every one of its blocks is intact and its closing
//! record is there; whatever subset of its blocks a crash leaves, torn or
//! not, it replays all of it or none of it, and never takes another
//! write's bytes for part of it.

mod common;

use common::{MemDevice, block_on};
use horton::memtable::MemTable;
use horton::wal::{CONT, MORE, Op, SPAN, WAL_MAGIC, WalWriter};

type W512 = WalWriter<MemDevice<512>, 512>;
/// Up to 64 entries of 32-byte values.
type Table = MemTable<64, 8192, 16, 32>;

/// The WAL region of these tests: blocks `0..32`.
const WAL_END: u64 = 32;

const fn writer(dev: MemDevice<512>) -> W512 {
    WalWriter::new(dev, 0, WAL_END)
}

/// A key and a 30-byte value: 23 + 4 + 30 = 57 bytes a record, eight to
/// a 512-byte block.
fn key(i: u64) -> [u8; 4] {
    u32::try_from(i).expect("small").to_be_bytes()
}

fn val(i: u64) -> [u8; 30] {
    let mut v = [b'.'; 30];
    v[0] = u8::try_from(i % 251).expect("byte");
    v
}

/// Appends a group of `n` puts with sequence numbers `first..first + n`.
fn group(w: &mut W512, first: u64, n: u64) {
    for i in 0..n {
        let seq = first + i;
        block_on(w.append_grouped(seq, Op::Put, &key(seq), &val(seq), i + 1 < n)).expect("append");
    }
}

/// Recovers `dev` and returns the writer and the table.
fn recover(dev: MemDevice<512>) -> (W512, Table, u64) {
    let mut w = writer(dev);
    let mut t = Table::new();
    let n = block_on(w.recover(&mut t)).expect("recover").records;
    (w, t, n)
}

fn has(t: &Table, seq: u64) -> bool {
    t.get(&key(seq))
        .is_some_and(|e| e.seq == seq && e.val == val(seq))
}

/// A group of 20 records spans three blocks; with a group before and one
/// after, recovery replays all three.
#[test]
fn a_spanning_group_replays_whole() {
    let mut w = writer(MemDevice::new());
    group(&mut w, 1, 3);
    block_on(w.commit()).expect("commit");
    group(&mut w, 4, 20);
    block_on(w.commit()).expect("commit");
    group(&mut w, 24, 2);
    block_on(w.commit()).expect("commit");
    let (_, t, n) = recover(w.into_device());
    assert_eq!(n, 25);
    for seq in 1..=25 {
        assert!(has(&t, seq), "seq {seq}");
    }
}

/// The group's blocks, as written: the block of the group before it, the
/// group's own, and the one after.
fn written() -> (MemDevice<512>, core::ops::Range<usize>) {
    let mut w = writer(MemDevice::new());
    group(&mut w, 1, 3);
    block_on(w.commit()).expect("commit");
    let first = usize::try_from(w.next_block()).expect("block");
    group(&mut w, 4, 20);
    block_on(w.commit()).expect("commit");
    let end = usize::try_from(w.next_block()).expect("block");
    (w.into_device(), first..end)
}

/// What recovery may make of a damaged group: all of it, or none of it.
/// Records before it are always there.
fn all_or_nothing(dev: MemDevice<512>, what: &str) -> bool {
    let (_, t, _) = recover(dev);
    for seq in 1..=3 {
        assert!(has(&t, seq), "{what}: the group before was lost");
    }
    let present: Vec<bool> = (4..24).map(|seq| has(&t, seq)).collect();
    assert!(
        present.iter().all(|&p| p) || present.iter().all(|&p| !p),
        "{what}: a partial group {present:?}"
    );
    present[0]
}

/// Any one of the group's blocks lost (never written): none of it.
#[test]
fn a_lost_block_loses_the_whole_group() {
    let (dev, blocks) = written();
    assert!(blocks.len() >= 3, "the group spans {} blocks", blocks.len());
    for lost in blocks {
        let mut d = dev.clone();
        d.blocks_mut()[lost] = [0u8; 512];
        assert!(!all_or_nothing(d, &format!("block {lost} lost")));
    }
}

/// Any one of the group's blocks torn at any byte, the rest of it zeros
/// or other bytes: none of it, unless the tear fell after the last byte
/// the block held. Then the group stands if the block is its last (other
/// bytes after a closing record are a torn tail, as in any block), or if
/// the padding still reads as zeros; a block the group goes on from must
/// end in clean padding after its `SPAN` record.
#[test]
fn a_torn_block_tears_the_whole_group() {
    let (dev, blocks) = written();
    let last = blocks.end - 1;
    for torn in blocks {
        let original = dev.clone().blocks_mut()[torn];
        let used = original.iter().rposition(|&b| b != 0).map_or(0, |i| i + 1);
        for keep in (0..512).step_by(7) {
            for old in [0u8, 0x5A] {
                let mut d = dev.clone();
                d.blocks_mut()[torn][keep..].fill(old);
                let kept =
                    all_or_nothing(d, &format!("block {torn} torn at {keep}, then {old:#x}"));
                let expect = keep >= used && (old == 0 || torn == last);
                assert_eq!(kept, expect, "block {torn} torn at {keep}, then {old:#x}");
            }
        }
    }
}

/// A later block of the group on the medium without an earlier one, in
/// every combination of three blocks: the group is never replayed unless
/// all of them are there.
#[test]
fn any_subset_of_the_blocks_is_all_or_nothing() {
    let (dev, blocks) = written();
    let n = blocks.len();
    for mask in 0..(1u32 << n) {
        let mut d = dev.clone();
        for (i, b) in blocks.clone().enumerate() {
            if mask & (1 << i) == 0 {
                d.blocks_mut()[b] = [0u8; 512];
            }
        }
        let kept = all_or_nothing(d, &format!("blocks kept {mask:#b}"));
        assert_eq!(kept, mask == (1 << n) - 1, "blocks kept {mask:#b}");
    }
}

/// A group broken by a crash is dropped, and writing resumes so that the
/// next recovery sees what was written after it, and still not the
/// group.
#[test]
fn writes_after_a_broken_group_recover() {
    let (mut dev, blocks) = written();
    dev.blocks_mut()[blocks.end - 1] = [0u8; 512];
    let (mut w, t, n) = recover(dev);
    assert_eq!(n, 3);
    assert!(!has(&t, 4));
    // Number above the group, as `Db::open` does.
    group(&mut w, 100, 2);
    block_on(w.commit()).expect("commit");
    let (_, t, n) = recover(w.into_device());
    assert_eq!(n, 5, "the group before, and the two after");
    assert!(has(&t, 100) && has(&t, 101));
    assert!(
        (4..24).all(|seq| !has(&t, seq)),
        "the broken group came back"
    );
}

/// A record that does not fit a block by itself is still refused.
#[test]
fn a_record_larger_than_a_block_is_refused() {
    let mut w = writer(MemDevice::new());
    let err = block_on(w.append_grouped(1, Op::Put, b"k", &[7u8; 600], false)).unwrap_err();
    assert!(
        matches!(err, horton::Error::BatchTooLarge { .. }),
        "got {err:?}"
    );
}

/// Encodes a record of [`key`] and [`val`] for `seq` with `flags` at the
/// start of `out`, as the writer does; returns its length.
fn forge(out: &mut [u8], seq: u64, flags: u8) -> usize {
    let (k, v) = (key(seq), val(seq));
    let len = 17 + k.len() + v.len();
    out[0..2].copy_from_slice(&WAL_MAGIC.to_le_bytes());
    out[2..6].copy_from_slice(&u32::try_from(len).expect("len").to_le_bytes());
    out[6..14].copy_from_slice(&seq.to_le_bytes());
    out[14] = 1 | flags;
    out[15..17].copy_from_slice(&4u16.to_le_bytes());
    out[17..19].copy_from_slice(&30u16.to_le_bytes());
    out[19..23].copy_from_slice(&k);
    out[23..53].copy_from_slice(&v);
    let crc = horton::crc32(&out[2..53]);
    out[53..57].copy_from_slice(&crc.to_le_bytes());
    57
}

/// Forged blocks: each a list of `(seq, flags)`.
type Blocks = Vec<Vec<(u64, u8)>>;

/// Writes blocks of forged records: each block a list of `(seq, flags)`.
fn forged(blocks: &[&[(u64, u8)]]) -> MemDevice<512> {
    let mut dev = MemDevice::<512>::new();
    dev.blocks_mut().resize(blocks.len(), [0u8; 512]);
    for (b, recs) in blocks.iter().enumerate() {
        let mut at = 0;
        for &(seq, flags) in *recs {
            at += forge(&mut dev.blocks_mut()[b][at..], seq, flags);
        }
    }
    dev
}

/// The writer marks a block's last record `SPAN` and re-seals its CRC;
/// the next block's first record carries `CONT`, its others neither.
#[test]
fn the_span_mark_is_resealed() {
    let mut w = writer(MemDevice::new());
    group(&mut w, 1, 20);
    block_on(w.commit()).expect("commit");
    let dev = w.into_device();
    let mut forged_block = [0u8; 512];
    let mut at = 0;
    for seq in 1..=8 {
        let flags = MORE | if seq == 8 { SPAN } else { 0 };
        at += forge(&mut forged_block[at..], seq, flags);
    }
    assert_eq!(
        dev.clone().blocks_mut()[0][..],
        forged_block[..],
        "block 0 as the encoder writes SPAN on its last record"
    );
    let mut forged_block = [0u8; 512];
    let mut at = 0;
    for seq in 9..=16 {
        let flags = MORE | if seq == 9 { CONT } else { 0 } | if seq == 16 { SPAN } else { 0 };
        at += forge(&mut forged_block[at..], seq, flags);
    }
    let mut dev = dev;
    assert_eq!(dev.blocks_mut()[1][..], forged_block[..], "block 1");
}

/// A group that began part way into a block may not go on into the next:
/// recovery would have to resume writing inside it. The writer refuses it
/// and drops it from the stage; what was staged before it commits.
#[test]
fn a_group_begun_mid_block_may_not_span() {
    let mut w = writer(MemDevice::new());
    block_on(w.append(1, Op::Put, &key(1), &val(1))).expect("append");
    let mut err = None;
    for i in 0..20u64 {
        let seq = 2 + i;
        if let Err(e) = block_on(w.append_grouped(seq, Op::Put, &key(seq), &val(seq), i < 19)) {
            err = Some((seq, e));
            break;
        }
    }
    let (seq, err) = err.expect("refused");
    assert_eq!(seq, 9, "at the record that did not fit the block");
    assert!(
        matches!(err, horton::Error::BatchTooLarge { .. }),
        "{err:?}"
    );
    assert_eq!(w.staged_bytes(), 57, "only the record before it is staged");
    block_on(w.commit()).expect("commit");
    let (_, t, n) = recover(w.into_device());
    assert_eq!(n, 1);
    assert!(has(&t, 1));
}

/// A group may span at most `GROUP_BLOCKS_MAX` blocks: the writer refuses
/// the record that would start the 17th, and recovery drops what landed.
#[test]
fn a_group_over_the_block_limit_is_refused() {
    let mut w = writer(MemDevice::new());
    let limit = 8 * horton::GROUP_BLOCKS_MAX as u64;
    let mut err = None;
    for i in 0..limit + 8 {
        let seq = 1 + i;
        if let Err(e) = block_on(w.append_grouped(seq, Op::Put, &key(seq), &val(seq), true)) {
            err = Some((seq, e));
            break;
        }
    }
    let (seq, err) = err.expect("refused");
    assert_eq!(seq, limit + 1, "at the first record of a 17th block");
    assert!(
        matches!(err, horton::Error::BatchTooLarge { .. }),
        "{err:?}"
    );
    assert_eq!(w.staged_bytes(), 0, "the group was dropped from the stage");
    // The next group starts afresh after the dead one, and recovers.
    group(&mut w, 1000, 2);
    block_on(w.commit()).expect("commit");
    let (_, t, n) = recover(w.into_device());
    assert_eq!(n, 2, "only the group after the refused one");
    assert!(has(&t, 1000) && has(&t, 1001));
}

/// Recovery's flag rules, on forged blocks: a span may only begin at a
/// block's start; `CONT` only on a block's first record, and only after a
/// `SPAN`, with the next sequence number; and a chain longer than
/// `GROUP_BLOCKS_MAX` blocks is no group. Each is a tear.
#[test]
fn recovery_refuses_what_no_writer_makes() {
    let cases: [(&str, Blocks, u64); 6] = [
        (
            "a span begun mid-block",
            vec![vec![(1, 0), (2, MORE | SPAN)], vec![(3, CONT)]],
            1,
        ),
        (
            "CONT with no span open",
            vec![vec![(1, 0)], vec![(2, CONT)]],
            1,
        ),
        (
            "CONT after a closed group",
            vec![vec![(1, MORE), (2, 0)], vec![(3, CONT)]],
            2,
        ),
        (
            "CONT but not first",
            vec![vec![(1, MORE | SPAN)], vec![(2, CONT | MORE), (3, CONT)]],
            0,
        ),
        (
            "a gap in the sequence across blocks",
            vec![vec![(1, MORE | SPAN)], vec![(3, CONT)]],
            0,
        ),
        (
            "SPAN without MORE",
            vec![vec![(1, SPAN)], vec![(2, CONT)]],
            0,
        ),
    ];
    for (what, blocks, want) in cases {
        let refs: Vec<&[(u64, u8)]> = blocks.iter().map(Vec::as_slice).collect();
        let (_, _, n) = recover(forged(&refs));
        assert_eq!(n, want, "{what}");
    }
    // A whole and well-formed chain of 16 blocks replays; 17 do not.
    for (blocks, want) in [(16u64, 16u64), (17, 0)] {
        let chain: Vec<Vec<(u64, u8)>> = (1..=blocks)
            .map(|b| {
                let first = if b == 1 { 0 } else { CONT };
                let last = if b == blocks { 0 } else { MORE | SPAN };
                vec![(b, first | last)]
            })
            .collect();
        let refs: Vec<&[(u64, u8)]> = chain.iter().map(Vec::as_slice).collect();
        let (_, _, n) = recover(forged(&refs));
        assert_eq!(n, want, "a chain of {blocks} blocks");
    }
}
