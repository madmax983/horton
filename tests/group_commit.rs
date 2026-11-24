//! Group commit across WAL blocks (ADR-0028): `Db::write_group`.
//!
//! Many atomic units (each a client's batch, each inside one WAL block)
//! become durable under one device flush and visible after it. The
//! committed prefix is the longest one that fits the memtable, the WAL
//! and `GROUP_BLOCKS_MAX` blocks. A crash before the flush may keep any
//! subset of the units, each whole or not at all; open scrubs blocks a
//! reordering device left behind the recovered log, so a later crash
//! never brings back a write an earlier recovery dropped.

mod common;

use std::cell::{Cell, RefCell};
use std::collections::BTreeSet;
use std::task::{Context, Poll};

use common::{MemDevice, block_on, test_config};
use horton::{BlockDevice, Error, GROUP_BLOCKS_MAX, GroupCommit, GroupOp};

/// 4 KiB blocks, 1 KiB values, a 256-entry memtable over a 64 KiB arena:
/// room for groups of large units.
type GroupDb<D> = horton::Db<D, 4096, 256, 1024, 256, 65536, 7, 4, 1024, 8>;

/// The WAL region of `test_config()`.
const WAL: core::ops::Range<u64> = 8..136;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Fault;

/// A device that counts WAL writes and flushes and can drop or fail
/// chosen writes, over a `MemDevice`.
#[derive(Default)]
struct Lab {
    inner: MemDevice<4096>,
    wal_writes: Vec<u64>,
    flushes: usize,
    /// Writes to these blocks are silently lost (a reordering device
    /// that lost power before they reached the medium).
    lose: RefCell<BTreeSet<u64>>,
    /// Fail the write numbered this (counting every write), once.
    fail_write: Cell<Option<usize>>,
    writes: usize,
    fail_flush: Cell<bool>,
}

impl BlockDevice for Lab {
    type Error = Fault;
    const BLOCK: usize = 4096;

    fn poll_read_block(
        &self,
        cx: &mut Context<'_>,
        id: u64,
        buf: &mut [u8],
    ) -> Poll<Result<(), Fault>> {
        self.inner
            .poll_read_block(cx, id, buf)
            .map(|r| r.map_err(|_| Fault))
    }

    fn poll_write_block(
        &mut self,
        cx: &mut Context<'_>,
        id: u64,
        buf: &[u8],
    ) -> Poll<Result<(), Fault>> {
        let n = self.writes;
        self.writes += 1;
        if self.fail_write.get() == Some(n) {
            self.fail_write.set(None);
            return Poll::Ready(Err(Fault));
        }
        if WAL.contains(&id) {
            self.wal_writes.push(id);
        }
        if self.lose.borrow().contains(&id) {
            return Poll::Ready(Ok(()));
        }
        self.inner
            .poll_write_block(cx, id, buf)
            .map(|r| r.map_err(|_| Fault))
    }

    fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Fault>> {
        self.flushes += 1;
        if self.fail_flush.get() {
            return Poll::Ready(Err(Fault));
        }
        Poll::Ready(Ok(()))
    }
}

fn open(dev: Lab) -> GroupDb<Lab> {
    let mut db = GroupDb::new(dev, test_config());
    block_on(db.open()).expect("open");
    db
}

fn get(db: &GroupDb<Lab>, key: &[u8]) -> Option<Vec<u8>> {
    let mut buf = [0u8; 1024];
    block_on(db.get(key, &mut buf))
        .expect("get")
        .map(|n| buf[..n].to_vec())
}

fn key(i: usize) -> Vec<u8> {
    format!("k{i:04}").into_bytes()
}

fn val(i: usize, len: usize) -> Vec<u8> {
    let mut v = format!("v{i}:").into_bytes();
    v.resize(len, b'.');
    v
}

/// `units` units of `ops` puts each, values of `len` bytes; keys numbered
/// from `first`.
fn units(first: usize, units: usize, ops: usize, len: usize) -> Vec<Vec<(Vec<u8>, Vec<u8>)>> {
    (0..units)
        .map(|u| {
            (0..ops)
                .map(|o| {
                    let i = first + u * ops + o;
                    (key(i), val(i, len))
                })
                .collect()
        })
        .collect()
}

fn write_group(
    db: &mut GroupDb<Lab>,
    data: &[Vec<(Vec<u8>, Vec<u8>)>],
) -> Result<GroupCommit, Error<Fault>> {
    let ops: Vec<Vec<GroupOp<'_>>> = data
        .iter()
        .map(|u| u.iter().map(|(k, v)| GroupOp::put(k, v)).collect())
        .collect();
    let refs: Vec<&[GroupOp<'_>]> = ops.iter().map(Vec::as_slice).collect();
    block_on(db.write_group(&refs))
}

/// Six units of one 1,000-byte put: three to a block, so two WAL
/// blocks, and one flush for all of them.
#[test]
fn a_group_across_blocks_is_one_flush() {
    let mut db = open(Lab::default());
    let (flushes, wal) = (db.device().flushes, db.device().wal_writes.len());
    let data = units(0, 6, 1, 1000);
    let done = write_group(&mut db, &data).expect("group");
    assert_eq!(done.units, 6);
    assert_eq!(db.device().flushes - flushes, 1, "one flush for the group");
    assert_eq!(db.device().wal_writes.len() - wal, 2, "two WAL blocks");
    for (u, unit) in data.iter().enumerate() {
        for (k, v) in unit {
            assert_eq!(get(&db, k).as_deref(), Some(v.as_slice()), "unit {u}");
        }
    }
}

/// Units take consecutive sequence numbers from `base`, in order, and a
/// later write follows them.
#[test]
fn units_take_consecutive_sequence_numbers() {
    let mut db = open(Lab::default());
    block_on(db.put(b"before", b"x")).expect("put");
    let data = units(0, 4, 3, 10);
    let done = write_group(&mut db, &data).expect("group");
    assert_eq!(done.units, 4);
    let mut buf = [0u8; 1024];
    for (i, (k, v)) in data.iter().flatten().enumerate() {
        let seq = done.base + i as u64;
        let n = block_on(db.get_at(k, &mut buf, seq))
            .expect("get_at")
            .expect("visible at its seq");
        assert_eq!(&buf[..n], v.as_slice());
        assert!(
            block_on(db.get_at(k, &mut buf, seq - 1))
                .expect("get_at")
                .is_none()
        );
    }
    let after = block_on(db.put(b"after", b"y")).expect("put");
    assert_eq!(after, done.base + 12);
}

/// The committed prefix stops at the memtable's capacity; the rest is
/// left for the caller, and a unit that fits nothing reports the error a
/// lone write would.
#[test]
fn the_prefix_that_fits_the_memtable_commits() {
    let mut db = open(Lab::default());
    // 256 slots: 30 units of 10 puts would need 300.
    let data = units(0, 30, 10, 20);
    let done = write_group(&mut db, &data).expect("group");
    assert_eq!(done.units, 25, "25 units of 10 fill 250 of 256 slots");
    assert_eq!(
        get(&db, &key(249)).as_deref(),
        Some(val(249, 20).as_slice())
    );
    assert!(
        get(&db, &key(250)).is_none(),
        "the 26th unit was not written"
    );
    assert_eq!(
        write_group(&mut db, &data[25..]).map(|d| d.units),
        Err(Error::TableFull)
    );
    block_on(db.flush()).expect("flush");
    assert_eq!(write_group(&mut db, &data[25..]).expect("rest").units, 5);
    assert_eq!(
        get(&db, &key(299)).as_deref(),
        Some(val(299, 20).as_slice())
    );
}

/// A group never spans more than `GROUP_BLOCKS_MAX` WAL blocks.
#[test]
fn a_group_spans_at_most_the_block_limit() {
    let mut db = open(Lab::default());
    // Three 1,000-byte puts: one unit to a block.
    let data = units(0, GROUP_BLOCKS_MAX + 3, 3, 1000);
    let wal = db.device().wal_writes.len();
    let done = write_group(&mut db, &data).expect("group");
    assert_eq!(done.units, GROUP_BLOCKS_MAX);
    assert_eq!(db.device().wal_writes.len() - wal, GROUP_BLOCKS_MAX);
}

/// A unit larger than a block: an error when it is first, the end of the
/// prefix otherwise. Each unit must fit one block to stay atomic.
#[test]
fn a_unit_larger_than_a_block_ends_the_prefix() {
    let mut db = open(Lab::default());
    let big = units(0, 1, 5, 1000);
    assert!(matches!(
        write_group(&mut db, &big),
        Err(Error::BatchTooLarge { .. })
    ));
    assert!(get(&db, &key(0)).is_none());
    let mut data = units(100, 2, 1, 10);
    data.push(big[0].clone());
    data.extend(units(200, 1, 1, 10));
    assert_eq!(write_group(&mut db, &data).expect("group").units, 2);
    assert!(get(&db, &key(200)).is_none());
}

/// Ops are validated as `WriteBatch` validates them; an inverted range
/// delete is a no-op, a range delete and a delete apply in order.
#[test]
fn ops_are_validated_and_applied_like_a_batch() {
    let mut db = open(Lab::default());
    let empty: [GroupOp<'_>; 1] = [GroupOp::put(b"", b"v")];
    assert_eq!(
        block_on(db.write_group(&[&empty])).map(|d| d.units),
        Err(Error::EmptyKey)
    );
    let long = [0u8; 300];
    let too_long = [GroupOp::put(&long, b"v")];
    assert!(matches!(
        block_on(db.write_group(&[&too_long])),
        Err(Error::KeyTooLarge { .. })
    ));
    let a = [
        GroupOp::put(b"a", b"1"),
        GroupOp::put(b"b", b"2"),
        GroupOp::put(b"c", b"3"),
    ];
    let b = [GroupOp::range_delete(b"z", b"a"), GroupOp::delete(b"a")];
    let c = [
        GroupOp::range_delete(b"b", b"c"),
        GroupOp::put_ttl(b"d", b"4", 0),
    ];
    let done = block_on(db.write_group(&[&a, &b, &c])).expect("group");
    assert_eq!(done.units, 3);
    assert_eq!(get(&db, b"a"), None);
    assert_eq!(get(&db, b"b"), None);
    assert_eq!(get(&db, b"c").as_deref(), Some(&b"3"[..]));
    assert_eq!(get(&db, b"d").as_deref(), Some(&b"4"[..]));
    // Empty units are allowed and cost nothing.
    let none: [GroupOp<'_>; 0] = [];
    let wal = db.device().wal_writes.len();
    assert_eq!(
        block_on(db.write_group(&[&none, &none]))
            .expect("empty")
            .units,
        2
    );
    assert_eq!(db.device().wal_writes.len(), wal);
}

/// Every unit of a flushed group survives a crash and a reopen.
#[test]
fn a_committed_group_survives_reopen() {
    let mut db = open(Lab::default());
    let data = units(0, 9, 1, 1000);
    assert_eq!(write_group(&mut db, &data).expect("group").units, 9);
    let db = open(db.into_device());
    for (k, v) in data.iter().flatten() {
        assert_eq!(get(&db, k).as_deref(), Some(v.as_slice()));
    }
}

/// A block write that fails part way through a group: the group is an
/// error, nothing of it becomes visible, and the records that never
/// reached the device are not written later by someone else's commit.
#[test]
fn a_failed_block_write_leaves_nothing_behind() {
    let mut db = open(Lab::default());
    // Three units to a block, nine units: the second block's write fails.
    db.device().fail_write.set(Some(db.device().writes + 1));
    let data = units(0, 9, 1, 1000);
    assert_eq!(
        write_group(&mut db, &data).map(|d| d.units),
        Err(Error::Device(Fault))
    );
    for (k, _) in data.iter().flatten() {
        assert!(
            get(&db, k).is_none(),
            "nothing of a failed group is visible"
        );
    }
    block_on(db.put(b"next", b"write")).expect("a later write");
    let db = open(db.into_device());
    assert_eq!(get(&db, b"next").as_deref(), Some(&b"write"[..]));
    for (k, _) in data[3..].iter().flatten() {
        assert!(
            get(&db, k).is_none(),
            "the unwritten block's units never land"
        );
    }
}

/// A flush that fails: an error, nothing visible; the sequence numbers
/// the landed blocks carried are not reused.
#[test]
fn a_failed_flush_leaves_nothing_visible() {
    let mut db = open(Lab::default());
    db.device().fail_flush.set(true);
    let data = units(0, 6, 1, 1000);
    assert_eq!(
        write_group(&mut db, &data).map(|d| d.units),
        Err(Error::Device(Fault))
    );
    for (k, _) in data.iter().flatten() {
        assert!(get(&db, k).is_none());
    }
    db.device().fail_flush.set(false);
    let seq = block_on(db.put(b"next", b"write")).expect("put");
    assert!(seq > 6, "the failed group's sequence numbers are consumed");
}

/// Power fails mid-group on a device that reorders writes: the group's
/// second block reached the medium, its first did not. Recovery keeps
/// the log up to the hole and drops what lies past it; open scrubs the
/// stranded block, so a write made after recovery, followed by another
/// crash, never brings the dropped units back.
#[test]
fn a_block_stranded_past_a_hole_never_comes_back() {
    let mut db = open(Lab::default());
    block_on(db.put(b"acked", b"1")).expect("put");
    let first = db.device().wal_writes.last().copied().expect("a WAL write") + 1;
    db.device().lose.borrow_mut().insert(first);
    // Three units to a block: units 0-2 lost with block `first`, 3-5
    // landed in `first + 1`.
    let data = units(0, 6, 1, 1000);
    assert_eq!(write_group(&mut db, &data).expect("group").units, 6);
    // Power fails: reopen from what the medium holds.
    let dev = db.into_device();
    dev.lose.borrow_mut().clear();
    let mut db = open(dev);
    assert_eq!(get(&db, b"acked").as_deref(), Some(&b"1"[..]));
    for (k, _) in data.iter().flatten() {
        assert!(get(&db, k).is_none(), "nothing past the hole is replayed");
    }
    // One small write lands in the hole's block, then power fails again.
    block_on(db.put(b"later", b"2")).expect("put");
    let db = open(db.into_device());
    assert_eq!(get(&db, b"later").as_deref(), Some(&b"2"[..]));
    for (k, _) in data[3..].iter().flatten() {
        assert!(get(&db, k).is_none(), "a dropped unit came back");
    }
}

/// The WAL region bounds the prefix as well: near its end, only the
/// units that fit the blocks left commit, and with none left the write
/// reports `WalFull`, as a lone write would.
#[test]
fn the_wal_region_bounds_the_prefix() {
    let mut db = open(Lab::default());
    // Fill the WAL to two blocks before its end with small group commits
    // (each commit takes a block).
    let left = WAL.end
        - db.device()
            .wal_writes
            .last()
            .copied()
            .unwrap_or(WAL.start - 1)
        - 1;
    for i in 0..left - 2 {
        let key = format!("f{i}");
        let one = [GroupOp::put(key.as_bytes(), b"x")];
        assert_eq!(block_on(db.write_group(&[&one])).expect("fill").units, 1);
    }
    // One unit to a block, five units: two blocks are left.
    let data = units(0, 5, 3, 1000);
    assert_eq!(write_group(&mut db, &data).expect("group").units, 2);
    assert_eq!(
        write_group(&mut db, &data[2..]).map(|d| d.units),
        Err(Error::WalFull)
    );
}

/// Review finding: a group that failed after a block of it reached the
/// device leaves records the memtable does not hold, and the next open
/// replays them. Writes after it must leave room for them, or the next
/// open overflows the memtable and fails with `CorruptWal` for good.
#[test]
fn records_a_failed_group_left_behind_keep_their_room() {
    let mut db = open(Lab::default());
    // 10 puts of 20 bytes: eight units to a block. Twenty units: three
    // blocks; the second block's write fails, the first has landed.
    db.device().fail_write.set(Some(db.device().writes + 1));
    let failed = units(10_000, 20, 10, 20);
    assert_eq!(
        write_group(&mut db, &failed).map(|d| d.units),
        Err(Error::Device(Fault))
    );
    // The memtable is empty, but the failed group's 200 records are
    // counted as taken: 80 landed, and the count is the whole group's,
    // an upper bound until a flush.
    let data = units(0, 30, 10, 20);
    let done = write_group(&mut db, &data).expect("group");
    assert_eq!(done.units, 5, "5 units of 10 fit beside the 200 counted");
    assert_eq!(
        write_group(&mut db, &data[5..]).map(|d| d.units),
        Err(Error::TableFull)
    );
    // Six slots left: six single puts, then `TableFull`.
    for i in 0..6 {
        block_on(db.put(format!("one{i}").as_bytes(), b"more")).expect("put");
    }
    assert_eq!(block_on(db.put(b"one6", b"more")), Err(Error::TableFull));
    // A crash now: the open replays both and must fit them.
    let mut db = open(db.into_device());
    for (k, v) in data[..5].iter().flatten() {
        assert_eq!(get(&db, k).as_deref(), Some(v.as_slice()));
    }
    assert_eq!(get(&db, b"one5").as_deref(), Some(&b"more"[..]));
    // A flush moves the WAL head past them, and the room comes back.
    block_on(db.flush()).expect("flush");
    assert_eq!(write_group(&mut db, &data[5..]).expect("rest").units, 25);
    let db = open(db.into_device());
    assert_eq!(
        get(&db, &key(299)).as_deref(),
        Some(val(299, 20).as_slice())
    );
}

/// The same for a single put whose flush failed after its block landed
/// (the path this finding was first latent in), and an empty-memtable
/// flush retires the record: `TableFull` always has a remedy.
#[test]
fn a_failed_put_keeps_its_room_until_a_flush() {
    let mut db = open(Lab::default());
    // Fill the memtable to one slot short with acknowledged writes.
    let data = units(0, 51, 5, 10);
    assert_eq!(write_group(&mut db, &data).expect("fill").units, 51);
    db.device().fail_flush.set(true);
    assert_eq!(block_on(db.put(b"x", b"y")), Err(Error::Device(Fault)));
    db.device().fail_flush.set(false);
    // Its record may replay: the last slot is taken.
    assert_eq!(block_on(db.put(b"z", b"z")), Err(Error::TableFull));
    let mut db = open(db.into_device());
    assert_eq!(
        get(&db, &key(254)).as_deref(),
        Some(val(254, 10).as_slice())
    );
    // After a flush of what open replayed, and of an empty memtable,
    // writes go on.
    block_on(db.flush()).expect("flush");
    block_on(db.flush()).expect("flush, empty");
    block_on(db.put(b"z", b"z")).expect("room again");
}

/// Review finding: after a commit failed with blocks left unflushed, the
/// next write must not add blocks of its own until the device has been
/// flushed, or an unflushed run grows past what open's scrub looks at
/// and a dropped unit can come back.
#[test]
fn a_write_after_an_unflushed_failure_flushes_first() {
    let mut db = open(Lab::default());
    db.device().fail_flush.set(true);
    let first = units(0, 6, 1, 1000);
    assert_eq!(
        write_group(&mut db, &first).map(|d| d.units),
        Err(Error::Device(Fault))
    );
    // The device still cannot flush: the next write writes nothing.
    let wal = db.device().wal_writes.len();
    let next = units(100, 3, 1, 1000);
    assert_eq!(
        write_group(&mut db, &next).map(|d| d.units),
        Err(Error::Device(Fault))
    );
    assert_eq!(
        db.device().wal_writes.len(),
        wal,
        "no block before a good flush"
    );
    // Once it can, the write flushes first, then commits with its own.
    db.device().fail_flush.set(false);
    let flushes = db.device().flushes;
    assert_eq!(write_group(&mut db, &next).expect("group").units, 3);
    assert_eq!(db.device().flushes - flushes, 2);
}

/// Review finding: a group whose last op takes sequence number
/// `u64::MAX` panicked stepping past it. It commits, as `Db::write`
/// would; one more op is `CounterExhausted`. At the counter's top,
/// capacity errors still come first, and empty units still commit.
#[test]
fn a_group_at_the_top_of_the_counter() {
    // A WAL whose newest record is `u64::MAX - 1`.
    let mut wal = horton::WalWriter::<Lab, 4096>::new(Lab::default(), WAL.start, WAL.end);
    block_on(wal.append(u64::MAX - 1, horton::Op::Put, b"old", b"v")).expect("append");
    block_on(wal.commit()).expect("commit");
    let mut db = open(wal.into_device());
    let two = [GroupOp::put(b"a", b"1"), GroupOp::put(b"b", b"2")];
    assert_eq!(
        block_on(db.write_group(&[&two])),
        Err(Error::CounterExhausted)
    );
    let one = [GroupOp::put(b"a", b"1")];
    let done = block_on(db.write_group(&[&one])).expect("the last sequence number");
    assert_eq!(done.base, u64::MAX);
    assert_eq!(get(&db, b"a").as_deref(), Some(&b"1"[..]));
    let none: [GroupOp<'_>; 0] = [];
    assert_eq!(block_on(db.write_group(&[&none])).expect("empty").units, 1);
    assert_eq!(
        block_on(db.write_group(&[&one])),
        Err(Error::CounterExhausted)
    );
    let big: Vec<(Vec<u8>, Vec<u8>)> = (0..300).map(|i| (key(i), val(i, 10))).collect();
    let big_ops: Vec<GroupOp<'_>> = big.iter().map(|(k, v)| GroupOp::put(k, v)).collect();
    assert!(matches!(
        block_on(db.write_group(&[&big_ops])),
        Err(Error::BatchTooLarge { .. } | Error::TableFull)
    ));
}

/// Review finding: recovery numbered new writes from the newest record
/// it took, reusing the numbers of records it dropped while their bytes
/// were still on the medium. A torn rewrite can leave such bytes behind
/// new records, and recovery takes records while their numbers rise. So
/// open numbers new writes above every record it finds past the log.
#[test]
fn new_writes_are_numbered_above_records_left_past_the_log() {
    let mut db = open(Lab::default());
    block_on(db.put(b"acked", b"1")).expect("put");
    let first = db.device().wal_writes.last().copied().expect("a WAL write") + 1;
    db.device().lose.borrow_mut().insert(first);
    let data = units(0, 6, 1, 1000);
    let done = write_group(&mut db, &data).expect("group");
    let dev = db.into_device();
    dev.lose.borrow_mut().clear();
    let mut db = open(dev);
    let next = block_on(db.put(b"later", b"2")).expect("put");
    assert!(
        next > done.base + 5,
        "numbered {next}, below the dropped group's {}..={}",
        done.base,
        done.base + 5
    );
}
