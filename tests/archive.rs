//! Archive spike: the flush-to-object-storage primitive.
//!
//! The protocol under test: [`Db::archive_plan`] hands the caller a sealed
//! table's block range; the caller streams those blocks (read through
//! [`Db::device`]) to a remote sink and confirms the upload; then
//! [`Db::archive_commit`] drops the table from the manifest in one atomic
//! write and reclaims its blocks. Horton never touches the network — the
//! sink is the caller's WiFi/TLS code (Tallow-side).
//!
//! What the spike proves:
//! - the uploaded bytes are a complete, valid `SSTable` (re-opened through
//!   `TableReader` and fully re-read);
//! - the commit is atomic under crash injection at every write: recovery
//!   exposes either "table still local, all keys readable" or "table
//!   gone, sink holds every byte" — never a mix, never silent loss;
//! - freed blocks are truly reused by later flushes;
//! - the documented tombstone rule is real: archiving a tombstone-bearing
//!   table above a deeper level resurrects the older version locally.

mod common;

use std::collections::BTreeMap;
use std::task::{Context, Poll};

use common::{CrashDevice, MemDevice, TestDb, block_on, noop_waker, test_config};
use horton::{BlockDevice, Compaction, Error, Progress, TableReader};

const BLOCK: usize = 4096;

type TestCompaction = Compaction<4096, 256, 1024, 1024>;

/// Reads one block through the poll interface (test devices are `Ready`).
fn read_block<D: BlockDevice>(dev: &D, id: u64) -> [u8; BLOCK] {
    let mut buf = [0u8; BLOCK];
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    match dev.poll_read_block(&mut cx, id, &mut buf) {
        Poll::Ready(Ok(())) => buf,
        Poll::Ready(Err(_)) | Poll::Pending => panic!("test device read failed"),
    }
}

/// Counts block writes; everything else passes through.
struct CountDevice<D> {
    inner: D,
    writes: usize,
}

impl<D: BlockDevice> BlockDevice for CountDevice<D> {
    type Error = D::Error;
    const BLOCK: usize = D::BLOCK;

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
        self.writes += 1;
        self.inner.poll_write_block(cx, id, buf)
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_flush(cx)
    }
}

/// Two flushed tables: `{a0..a3}` then `{b0..b3}` in L0.
fn build_two_tables() -> MemDevice<BLOCK> {
    let mut db = TestDb::new(MemDevice::<BLOCK>::new(), test_config());
    block_on(db.open()).unwrap();
    for tag in *b"ab" {
        for i in 0..4u8 {
            block_on(db.put(&[tag, b'0' + i], &[b'v', tag, b'0' + i])).unwrap();
        }
        block_on(db.flush()).unwrap();
    }
    db.into_device()
}

/// Streams a planned table's blocks into a mock object-store sink:
/// `(block_id, bytes)` pairs, like one S3 object per table.
fn upload<D: BlockDevice>(db: &TestDb<D>, level: usize, table_id: u32) -> Vec<(u64, [u8; BLOCK])> {
    let plan = db
        .archive_plan(level, table_id)
        .expect("table must be present");
    let first = plan.table.first_block;
    let end = plan
        .table
        .first_block
        .checked_add(u64::from(plan.table.block_count))
        .expect("block range must not overflow");
    let mut sink = Vec::new();
    let mut blk = first;
    while blk < end {
        sink.push((blk, read_block(db.device(), blk)));
        blk += 1;
    }
    sink
}

/// All live keys and their values.
fn live_map(db: &TestDb<MemDevice<BLOCK>>) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut map = BTreeMap::new();
    let mut buf = [0u8; 2048];
    for tag in *b"ab" {
        for i in 0..4u8 {
            let key = [tag, b'0' + i];
            if let Some(n) = block_on(db.get(&key, &mut buf)).unwrap() {
                map.insert(key.to_vec(), buf[..n].to_vec());
            }
        }
    }
    map
}

/// The uploaded bytes must be a complete, valid `SSTable`: re-open them
/// through `TableReader` (footer magic + CRC, index verification) and
/// re-read every key. The commit must drop the table, stay idempotent,
/// and hand the freed blocks back to later flushes.
#[test]
fn archive_roundtrip_bytes_verify_as_sstable() {
    let mut db = TestDb::new(build_two_tables(), test_config());
    block_on(db.open()).unwrap();

    let l0 = db.level_tables(0).expect("level 0 exists");
    assert_eq!(l0.len(), 2, "two flushed tables in L0");
    let (id0, first0, count0) = (l0[0].id, l0[0].first_block, l0[0].block_count);
    assert!(db.archive_plan(0, 0xdead_beef).is_none());
    assert!(db.archive_plan(99, id0).is_none());

    // 1. Stream the table to the mock sink.
    let sink = upload(&db, 0, id0);
    assert_eq!(sink.len(), count0 as usize, "every block uploaded");
    for (i, (bid, _)) in sink.iter().enumerate() {
        assert_eq!(*bid, first0 + i as u64, "contiguous block range");
    }

    // 2. The uploaded bytes decode as a valid SSTable holding exactly a0..a3.
    let mut updev = MemDevice::<BLOCK>::new();
    for (bid, bytes) in &sink {
        let i = usize::try_from(*bid).expect("block id fits");
        while updev.blocks_mut().len() <= i {
            updev.blocks_mut().push([0u8; BLOCK]);
        }
        updev.blocks_mut()[i] = *bytes;
    }
    let end = first0.checked_add(u64::from(count0)).expect("end fits");
    let mut scratch = [0u8; BLOCK];
    let mut decomp = [0u8; BLOCK];
    let reader = block_on(TableReader::<MemDevice<BLOCK>, BLOCK, 1024>::open(
        &updev,
        &mut scratch,
        end - 1,
    ))
    .expect("uploaded bytes form a valid SSTable");
    let mut vbuf = [0u8; 2048];
    for i in 0..4u8 {
        let key = [b'a', b'0' + i];
        let n = block_on(reader.get(&mut scratch, &mut decomp, &key, &mut vbuf))
            .expect("uploaded table reads cleanly")
            .expect("key present in uploaded table");
        assert_eq!(&vbuf[..n], &[b'v', b'a', b'0' + i]);
    }

    // 3. Commit: the table leaves the manifest, idempotently.
    assert!(block_on(db.archive_commit(0, id0)).unwrap());
    assert!(db.archive_plan(0, id0).is_none());
    assert!(!block_on(db.archive_commit(0, id0)).unwrap());

    // 4. Local reads: a-keys are gone (by design — they're remote now),
    //    b-keys untouched.
    let mut buf = [0u8; 2048];
    for i in 0..4u8 {
        assert!(
            block_on(db.get(&[b'a', b'0' + i], &mut buf))
                .unwrap()
                .is_none(),
            "archived key must not read locally"
        );
        let n = block_on(db.get(&[b'b', b'0' + i], &mut buf))
            .unwrap()
            .expect("surviving table still readable");
        assert_eq!(&buf[..n], &[b'v', b'b', b'0' + i]);
    }

    // 5. The archived table's slot is free again, and the next flush
    //    takes a slot of its own.
    assert_eq!(db.slot_stats().used, 1, "only the b-table's slot is used");
    assert_eq!(db.check_invariants(), Ok(()));
    for i in 0..4u8 {
        block_on(db.put(&[b'c', b'0' + i], &[b'v', b'c', b'0' + i])).unwrap();
    }
    block_on(db.flush()).unwrap();
    let l0 = db.level_tables(0).expect("level 0 exists");
    assert_eq!(l0.len(), 2, "b-table plus the new c-table");
    assert_eq!(db.slot_stats().used, 2);
    assert_eq!(db.check_invariants(), Ok(()));
}

/// Drives exactly one compaction job to completion (no-op when idle).
fn drive_one<D: BlockDevice>(db: &mut TestDb<D>)
where
    D::Error: std::fmt::Debug,
{
    let mut c = TestCompaction::new();
    loop {
        match block_on(db.compact_step(&mut c)) {
            Ok(Progress::More) => {}
            Ok(Progress::Done) => break,
            Err(e) => panic!("unexpected compaction error: {e:?}"),
        }
    }
}

/// Counts the block writes of open + upload + `archive_commit`.
fn count_archive_writes() -> usize {
    let dev = CountDevice {
        inner: build_two_tables(),
        writes: 0,
    };
    let mut db = TestDb::new(dev, test_config());
    block_on(db.open()).unwrap();
    let id0 = db.level_tables(0).expect("level 0 exists")[0].id;
    let _sink = upload(&db, 0, id0);
    assert!(block_on(db.archive_commit(0, id0)).unwrap());
    db.into_device().writes
}

/// Runs open + upload + `archive_commit` with writes `>= crash_at` dropped.
fn run_crashed(crash_at: usize) -> (MemDevice<BLOCK>, Vec<(u64, [u8; BLOCK])>, u32) {
    let dev: CrashDevice<MemDevice<BLOCK>, BLOCK> = CrashDevice::new(build_two_tables(), crash_at);
    let mut db = TestDb::new(dev, test_config());
    block_on(db.open()).unwrap();
    let l0 = db.level_tables(0).expect("level 0 exists");
    let (id0, count0) = (l0[0].id, l0[0].block_count);
    let sink = upload(&db, 0, id0);
    assert_eq!(sink.len(), count0 as usize);
    let _ = block_on(db.archive_commit(0, id0));
    (db.into_device().into_inner(), sink, id0)
}

/// Exhaustive over the archive boundary: for every crash point of
/// open + upload + `archive_commit`, recovery must expose exactly one of
///
/// - the table still local with all eight keys readable, or
/// - the table gone with the sink holding every uploaded byte —
///
/// never a mix, never silent loss. The upload itself performs no writes,
/// so the sink is always complete; the commit's single manifest write is
/// the only decision point.
#[test]
fn archive_crash_boundary_is_atomic() {
    let w = count_archive_writes();
    assert_eq!(
        w, 1,
        "write count changed (open + commit); oracle below needs updating"
    );

    let mut want_all = BTreeMap::new();
    let mut want_b = BTreeMap::new();
    for tag in *b"ab" {
        for i in 0..4u8 {
            let kv = (vec![tag, b'0' + i], vec![b'v', tag, b'0' + i]);
            want_all.insert(kv.0.clone(), kv.1.clone());
            if tag == b'b' {
                want_b.insert(kv.0, kv.1);
            }
        }
    }

    for crash_at in 0..=w {
        let (dev, sink, id0) = run_crashed(crash_at);
        let mut db = TestDb::new(dev, test_config());
        block_on(db.open()).unwrap();

        // The sink always completes: the upload is reads-only.
        let archived_gone = db.archive_plan(0, id0).is_none();
        assert!(
            !sink.is_empty(),
            "crash_at={crash_at}: upload must complete before any crash matters"
        );

        if archived_gone {
            // Commit landed: a-keys live remotely now; b-keys local.
            assert_eq!(live_map(&db), want_b, "crash_at={crash_at}");
        } else {
            // Commit lost: everything still local; re-running the protocol
            // converges (idempotent sink, idempotent commit).
            assert_eq!(live_map(&db), want_all, "crash_at={crash_at}");
            assert!(block_on(db.archive_commit(0, id0)).unwrap());
            assert_eq!(
                live_map(&db),
                want_b,
                "crash_at={crash_at}: retry converges"
            );
        }
    }
}

/// The tombstone rule, made executable and now enforced (v0.12):
/// archiving a tombstone-bearing L0 table while a deeper level holds an
/// older version is refused with [`Error::WouldResurrect`], because
/// removing the tombstone locally would resurrect that version. Delete
/// workloads must archive from the bottommost level (or re-ingest the
/// tombstone) instead.
#[test]
fn archive_l0_tombstone_resurrects_older_version() {
    let mut db = TestDb::new(MemDevice::<BLOCK>::new(), test_config());
    block_on(db.open()).unwrap();
    // v1 of k lands in L1: four tables fill L0, one compaction drains it.
    for _ in 0..4 {
        block_on(db.put(b"k", b"v1")).unwrap();
        block_on(db.flush()).unwrap();
    }
    assert!(db.compaction_pending());
    drive_one(&mut db);
    // The tombstone for k lands in a fresh L0 table and hides v1.
    block_on(db.delete(b"k")).unwrap();
    block_on(db.flush()).unwrap();
    let mut buf = [0u8; 2048];
    assert!(
        block_on(db.get(b"k", &mut buf)).unwrap().is_none(),
        "tombstone hides v1 before archival"
    );
    // Archive the tombstone table straight off L0: refused, because the
    // tombstone is load-bearing — removing it would resurrect v1.
    let l0 = db.level_tables(0).expect("level 0 exists");
    assert_eq!(l0.len(), 1, "only the tombstone table in L0");
    let (tid, first, count) = (l0[0].id, l0[0].first_block, l0[0].block_count);
    let sink = upload(&db, 0, tid);
    assert_eq!(sink.len(), count as usize);
    assert_eq!(sink[0].0, first);
    assert_eq!(
        block_on(db.archive_commit(0, tid)).unwrap_err(),
        Error::WouldResurrect { table: tid }
    );
    // The tombstone stays local: reads are unchanged.
    assert!(
        block_on(db.get(b"k", &mut buf)).unwrap().is_none(),
        "tombstone still hides v1 after refused archival"
    );
}
