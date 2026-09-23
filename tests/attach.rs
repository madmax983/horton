//! v0.12: combined remote/local read model — table re-attach (`ingest_table`)
//! plus tombstone-rule enforcement in `archive_commit`.
//!
//! These tests exercise the full upload → archive → re-attach lifecycle:
//! an archived table is streamed into a remote [`MemDevice`], the local
//! table is archived away, and the table is later grafted back into L0
//! through an atomic manifest commit. They also pin down the enforcement
//! rule: `archive_commit` must refuse to remove a table whose tombstones
//! shadow a value visible in any live view (the v0.10 "only deeper tables
//! are hazardous" prose was incomplete — a same-level, shallower, or
//! re-ingested older copy can resurrect a value too).

mod common;

use common::{CrashDevice, MemDevice, TestDb, block_on, noop_waker, test_config};
use core::task::{Context, Poll};
use horton::{BlockDevice, Compaction, Error, Progress, Scan, SealedTable};

const BLOCK: usize = 4096;
const KEY_MAX: usize = 256;
const VAL_MAX: usize = 1024;

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

impl<D: BlockDevice> CountDevice<D> {
    const fn new(inner: D) -> Self {
        Self { inner, writes: 0 }
    }
    const fn writes(&self) -> usize {
        self.writes
    }
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

fn write_block<D: BlockDevice>(dev: &mut D, id: u64, buf: &[u8; BLOCK]) {
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    match dev.poll_write_block(&mut cx, id, buf) {
        Poll::Ready(Ok(())) => {}
        Poll::Ready(Err(_)) | Poll::Pending => panic!("test device write failed"),
    }
}

fn get(db: &TestDb<MemDevice<BLOCK>>, key: &[u8]) -> Option<Vec<u8>> {
    let mut buf = [0u8; VAL_MAX];
    block_on(db.get(key, &mut buf))
        .unwrap()
        .map(|n| buf[..n].to_vec())
}

fn put_keys(db: &mut TestDb<MemDevice<BLOCK>>, pairs: &[(&[u8], &[u8])]) {
    for (k, v) in pairs {
        block_on(db.put(k, v)).unwrap();
    }
    block_on(db.flush()).unwrap();
}

/// Streams `table_id`'s blocks (from `level`) into a remote [`MemDevice`]
/// laid out at base 0, and returns the remote plus the table's
/// placement-free sealed descriptor.
fn upload(
    db: &TestDb<MemDevice<BLOCK>>,
    level: usize,
    table_id: u32,
) -> (MemDevice<BLOCK>, SealedTable<256>) {
    let plan = db.archive_plan(level, table_id).unwrap();
    let sealed = plan.sealed();
    let mut remote = MemDevice::<BLOCK>::new();
    for i in 0..plan.table.block_count {
        let buf = read_block(db.device(), plan.table.first_block + u64::from(i));
        write_block(&mut remote, u64::from(i), &buf);
    }
    (remote, sealed)
}

/// Drains every pending compaction job to idle (for test geometries whose
/// compaction output is expected to be one merged table).
fn compact_to_idle(db: &mut TestDb<MemDevice<BLOCK>>) {
    let mut scratch = TestCompaction::new();
    while db.compaction_pending() {
        while block_on(db.compact_step(&mut scratch)).unwrap() == Progress::More {}
    }
}

// ---------------------------------------------------------------------------
// Re-attach lifecycle
// ---------------------------------------------------------------------------

#[test]
fn ingest_roundtrip() {
    let mut db = TestDb::new(MemDevice::<BLOCK>::new(), test_config());
    block_on(db.open()).unwrap();
    put_keys(
        &mut db,
        &[
            (b"a0", b"v0"),
            (b"a1", b"v1"),
            (b"a2", b"v2"),
            (b"a3", b"v3"),
            (b"b0", b"w0"),
            (b"b1", b"w1"),
            (b"b2", b"w2"),
            (b"b3", b"w3"),
        ],
    );

    // Upload the archived bytes before removing the table locally.
    let (remote, sealed) = upload(&db, 0, 0);
    assert!(block_on(db.archive_commit(0, 0)).unwrap());
    assert!(get(&db, b"a0").is_none());

    // Re-attach: every key comes back.
    assert!(block_on(db.ingest_table(&sealed, &remote, 0)).unwrap());
    for i in 0..4u8 {
        assert_eq!(get(&db, &[b'a', b'0' + i]), Some(vec![b'v', b'0' + i]));
        assert_eq!(get(&db, &[b'b', b'0' + i]), Some(vec![b'w', b'0' + i]));
    }

    // Idempotent: the same descriptor attaches exactly once.
    assert!(!block_on(db.ingest_table(&sealed, &remote, 0)).unwrap());
    let tables = db.level_tables(0).unwrap();
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].id, 0);

    // Reclaimed blocks are reusable: a fresh flush reuses id 1 and does
    // not disturb the re-attached table.
    put_keys(&mut db, &[(b"c0", b"z0")]);
    let ids: Vec<u32> = db.level_tables(0).unwrap().iter().map(|t| t.id).collect();
    assert_eq!(ids.len(), 2);
    assert!(ids.contains(&0) && ids.contains(&1));
    assert_eq!(get(&db, b"c0"), Some(b"z0".to_vec()));
    assert_eq!(get(&db, b"a0"), Some(b"v0".to_vec()));
}

#[test]
fn ingest_highest_seq_wins() {
    let mut db = TestDb::new(MemDevice::<BLOCK>::new(), test_config());
    block_on(db.open()).unwrap();
    put_keys(&mut db, &[(b"k1", b"v_old"), (b"k2", b"v2")]);
    let (remote, sealed) = upload(&db, 0, 0);
    assert!(block_on(db.archive_commit(0, 0)).unwrap());

    // Newer local value shadows the re-attached older copy.
    put_keys(&mut db, &[(b"k1", b"v_new")]);
    assert!(block_on(db.ingest_table(&sealed, &remote, 0)).unwrap());
    assert_eq!(get(&db, b"k1"), Some(b"v_new".to_vec()));
    // Keys that exist only remotely come back.
    assert_eq!(get(&db, b"k2"), Some(b"v2".to_vec()));

    // A later delete still wins over the re-attached value.
    block_on(db.delete(b"k1")).unwrap();
    assert!(get(&db, b"k1").is_none());
    // And the scan merge hides the deleted key while showing the other.
    let mut scan = Scan::new(&db);
    block_on(scan.seek(b"", None, u64::MAX)).unwrap();
    let mut kb = [0u8; KEY_MAX];
    let mut vb = [0u8; VAL_MAX];
    let (kl, _) = block_on(scan.next(&mut kb, &mut vb)).unwrap().unwrap();
    assert_eq!(&kb[..kl], b"k2");
    assert!(block_on(scan.next(&mut kb, &mut vb)).unwrap().is_none());
}

#[test]
fn ingest_scan_merges() {
    let mut db = TestDb::new(MemDevice::<BLOCK>::new(), test_config());
    block_on(db.open()).unwrap();
    put_keys(&mut db, &[(b"a", b"1"), (b"c", b"3")]);
    let (remote, sealed) = upload(&db, 0, 0);
    assert!(block_on(db.archive_commit(0, 0)).unwrap());
    put_keys(&mut db, &[(b"b", b"2")]);
    assert!(block_on(db.ingest_table(&sealed, &remote, 0)).unwrap());

    let mut scan = Scan::new(&db);
    block_on(scan.seek(b"", None, u64::MAX)).unwrap();
    let mut kb = [0u8; KEY_MAX];
    let mut vb = [0u8; VAL_MAX];
    let mut got: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    while let Some((kl, vl)) = block_on(scan.next(&mut kb, &mut vb)).unwrap() {
        got.push((kb[..kl].to_vec(), vb[..vl].to_vec()));
    }
    assert_eq!(
        got,
        vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
        ]
    );
}

#[test]
fn ingest_snapshot_coherent() {
    let mut db = TestDb::new(MemDevice::<BLOCK>::new(), test_config());
    block_on(db.open()).unwrap();
    put_keys(&mut db, &[(b"k", b"v_old")]);
    let (remote, sealed) = upload(&db, 0, 0);
    assert!(block_on(db.archive_commit(0, 0)).unwrap());

    let snap = db.snapshot().unwrap();
    put_keys(&mut db, &[(b"k2", b"x")]);
    assert!(block_on(db.ingest_table(&sealed, &remote, 0)).unwrap());

    // The snapshot taken while the table was archived still reads the
    // re-attached value at its watermark.
    let mut buf = [0u8; VAL_MAX];
    let n = block_on(db.get_at(b"k", &mut buf, snap)).unwrap().unwrap();
    assert_eq!(&buf[..n], b"v_old");

    // Newer writes win at live read but not under the snapshot.
    block_on(db.put(b"k", b"v_new")).unwrap();
    assert_eq!(get(&db, b"k"), Some(b"v_new".to_vec()));
    let n = block_on(db.get_at(b"k", &mut buf, snap)).unwrap().unwrap();
    assert_eq!(&buf[..n], b"v_old");
    db.release_snapshot(snap);
}

#[test]
fn ingest_advances_table_id_on_fresh_db() {
    // The sealed table was sealed on a *different* database; the fresh
    // DB must advance its next-table-id floor past the ingested id.
    let mut src = TestDb::new(MemDevice::<BLOCK>::new(), test_config());
    block_on(src.open()).unwrap();
    put_keys(&mut src, &[(b"k", b"v")]);
    let (remote, sealed) = upload(&src, 0, 0);
    drop(src);

    let mut db = TestDb::new(MemDevice::<BLOCK>::new(), test_config());
    block_on(db.open()).unwrap();
    assert!(block_on(db.ingest_table(&sealed, &remote, 0)).unwrap());
    put_keys(&mut db, &[(b"n", b"w")]);
    let ids: Vec<u32> = db.level_tables(0).unwrap().iter().map(|t| t.id).collect();
    assert!(ids.contains(&0) && ids.contains(&1));
    assert_eq!(get(&db, b"k"), Some(b"v".to_vec()));
    assert_eq!(get(&db, b"n"), Some(b"w".to_vec()));
}

#[test]
fn ingest_then_compact() {
    let mut db = TestDb::new(MemDevice::<BLOCK>::new(), test_config());
    block_on(db.open()).unwrap();
    put_keys(&mut db, &[(b"a", b"1"), (b"c", b"3")]);
    let (remote, sealed) = upload(&db, 0, 0);
    assert!(block_on(db.archive_commit(0, 0)).unwrap());
    put_keys(&mut db, &[(b"b", b"2")]);
    assert!(block_on(db.ingest_table(&sealed, &remote, 0)).unwrap());
    put_keys(&mut db, &[(b"d", b"4")]);
    put_keys(&mut db, &[(b"e", b"5")]);
    assert_eq!(db.level_tables(0).unwrap().len(), 4);

    compact_to_idle(&mut db);
    for (k, v) in [
        (b"a", b"1"),
        (b"b", b"2"),
        (b"c", b"3"),
        (b"d", b"4"),
        (b"e", b"5"),
    ] {
        assert_eq!(get(&db, k), Some(v.to_vec()), "key {k:?}");
    }
    let mut scan = Scan::new(&db);
    block_on(scan.seek(b"", None, u64::MAX)).unwrap();
    let mut kb = [0u8; KEY_MAX];
    let mut vb = [0u8; VAL_MAX];
    let mut keys: Vec<Vec<u8>> = Vec::new();
    while let Some((kl, _)) = block_on(scan.next(&mut kb, &mut vb)).unwrap() {
        keys.push(kb[..kl].to_vec());
    }
    assert_eq!(keys, vec![b"a", b"b", b"c", b"d", b"e"]);
}

#[test]
fn ingest_no_space_when_l0_full() {
    let mut db = TestDb::new(MemDevice::<BLOCK>::new(), test_config());
    block_on(db.open()).unwrap();
    put_keys(&mut db, &[(b"k", b"v")]);
    let (remote, sealed) = upload(&db, 0, 0);
    assert!(block_on(db.archive_commit(0, 0)).unwrap());
    // Fill L0 to its limit with other tables; the sealed table's id is
    // not present, so the refusal is about space, not idempotency.
    put_keys(&mut db, &[(b"n1", b"1")]);
    put_keys(&mut db, &[(b"n2", b"2")]);
    put_keys(&mut db, &[(b"n3", b"3")]);
    put_keys(&mut db, &[(b"n4", b"4")]);
    assert_eq!(db.level_tables(0).unwrap().len(), 4);

    assert!(matches!(
        block_on(db.ingest_table(&sealed, &remote, 0)),
        Err(Error::NoSpace)
    ));
    assert_eq!(db.level_tables(0).unwrap().len(), 4);
}

// ---------------------------------------------------------------------------
// Corrupt-remote and descriptor discipline
// ---------------------------------------------------------------------------

#[test]
fn ingest_rejects_corrupt_remote() {
    let mut db = TestDb::new(MemDevice::<BLOCK>::new(), test_config());
    block_on(db.open()).unwrap();
    put_keys(&mut db, &[(b"k", b"v")]);
    let (mut remote, sealed) = upload(&db, 0, 0);
    assert!(block_on(db.archive_commit(0, 0)).unwrap());

    // Corrupt the footer on the "remote" device.
    let foot = sealed.block_count - 1;
    remote.blocks_mut()[foot as usize][100] ^= 0xff;
    assert!(matches!(
        block_on(db.ingest_table(&sealed, &remote, 0)),
        Err(Error::CorruptBlock { .. })
    ));
    // The manifest is untouched: no phantom table.
    assert_eq!(db.level_tables(0).unwrap(), []);

    // Repair and the very same descriptor attaches cleanly.
    remote.blocks_mut()[foot as usize][100] ^= 0xff;
    assert!(block_on(db.ingest_table(&sealed, &remote, 0)).unwrap());
    assert_eq!(get(&db, b"k"), Some(b"v".to_vec()));
}

#[test]
fn ingest_rejects_descriptor_mismatch() {
    let mut db = TestDb::new(MemDevice::<BLOCK>::new(), test_config());
    block_on(db.open()).unwrap();
    put_keys(&mut db, &[(b"k", b"v")]);
    let (remote, sealed) = upload(&db, 0, 0);
    assert!(block_on(db.archive_commit(0, 0)).unwrap());

    let bad = SealedTable {
        entry_count: sealed.entry_count + 1,
        ..sealed
    };
    assert!(matches!(
        block_on(db.ingest_table(&bad, &remote, 0)),
        Err(Error::CorruptBlock { .. })
    ));
    assert_eq!(db.level_tables(0).unwrap(), []);
}

/// A source device with a different block size is refused up front —
/// the copy buffer is `BLOCK` bytes and mixing sizes would silently
/// misplace data.
#[test]
fn ingest_rejects_mismatched_block_size() {
    let mut db = TestDb::new(MemDevice::<BLOCK>::new(), test_config());
    block_on(db.open()).unwrap();
    put_keys(&mut db, &[(b"k", b"v")]);
    let (_remote, sealed) = upload(&db, 0, 0);
    assert!(block_on(db.archive_commit(0, 0)).unwrap());

    let small: MemDevice<512> = MemDevice::new();
    assert_eq!(
        block_on(db.ingest_table(&sealed, &small, 0)).unwrap_err(),
        Error::BadBufferLen
    );
    assert!(
        db.level_tables(0).unwrap().is_empty(),
        "nothing attached on BadBufferLen"
    );
}

#[test]
fn ingest_id_conflict() {
    let mut db = TestDb::new(MemDevice::<BLOCK>::new(), test_config());
    block_on(db.open()).unwrap();
    put_keys(&mut db, &[(b"k", b"v")]);
    let (remote, sealed) = upload(&db, 0, 0);
    assert!(block_on(db.archive_commit(0, 0)).unwrap());
    assert!(block_on(db.ingest_table(&sealed, &remote, 0)).unwrap());

    // Same id, different descriptor: refuse rather than graft garbage.
    let bad = SealedTable {
        block_count: sealed.block_count + 1,
        ..sealed
    };
    assert!(matches!(
        block_on(db.ingest_table(&bad, &remote, 0)),
        Err(Error::IngestConflict { id: 0 })
    ));
    // The good descriptor is still idempotent.
    assert!(!block_on(db.ingest_table(&sealed, &remote, 0)).unwrap());
}

// ---------------------------------------------------------------------------
// Tombstone-rule enforcement in archive_commit
// ---------------------------------------------------------------------------

#[test]
fn archive_commit_rejects_same_level_resurrection() {
    let mut db = TestDb::new(MemDevice::<BLOCK>::new(), test_config());
    block_on(db.open()).unwrap();
    put_keys(&mut db, &[(b"k", b"v")]); // id 0: k -> v
    block_on(db.delete(b"k")).unwrap();
    block_on(db.flush()).unwrap(); // id 1: k -> tombstone (same level!)

    // v0.10's doc said only deeper tables were hazardous; this
    // same-level case resurrects v too and must be refused.
    assert!(matches!(
        block_on(db.archive_commit(0, 1)),
        Err(Error::WouldResurrect { table: 1 })
    ));
    // Nothing changed: both tables still present, key still deleted.
    assert_eq!(db.level_tables(0).unwrap().len(), 2);
    assert!(get(&db, b"k").is_none());

    // Archiving the value table is safe: no tombstones inside it.
    assert!(block_on(db.archive_commit(0, 0)).unwrap());
    // Now the tombstone table archives safely: nothing older to resurrect.
    assert!(block_on(db.archive_commit(0, 1)).unwrap());
    assert!(get(&db, b"k").is_none());
}

#[test]
fn archive_commit_rejects_resurrection_after_reingest() {
    // The case that killed v0.10's "bottommost is safe" rule: a value
    // archived away, deleted, its tombstone compacted to bottommost,
    // then the value table re-ingested into L0 — archiving the
    // bottommost tombstone table now would revive the value.
    let mut db = TestDb::new(MemDevice::<BLOCK>::new(), test_config());
    block_on(db.open()).unwrap();
    put_keys(&mut db, &[(b"k", b"v_old")]); // id 0
    let (remote, sealed) = upload(&db, 0, 0);
    assert!(block_on(db.archive_commit(0, 0)).unwrap());

    block_on(db.delete(b"k")).unwrap();
    let snap = db.snapshot().unwrap(); // pin the tombstone
    put_keys(&mut db, &[(b"x", b"1")]); // id 1 (k -> tomb)
    put_keys(&mut db, &[(b"y", b"2")]); // id 2
    put_keys(&mut db, &[(b"z", b"3")]); // id 3
    put_keys(&mut db, &[(b"w", b"4")]); // id 4: L0 full, compaction can run
    compact_to_idle(&mut db); // merge to L1, tombstone survives the snapshot pin
    let mid = db.level_tables(1).unwrap()[0].id;
    assert_eq!(db.level_tables(0).unwrap(), []);
    db.release_snapshot(snap);
    assert!(get(&db, b"k").is_none());

    // Re-ingest the value table into L0: the delete still wins by seq.
    assert!(block_on(db.ingest_table(&sealed, &remote, 0)).unwrap());
    assert!(get(&db, b"k").is_none());

    // Archiving the tombstone's (bottommost!) table would resurrect v_old.
    assert!(matches!(
        block_on(db.archive_commit(1, mid)),
        Err(Error::WouldResurrect { table }) if table == mid
    ));
    assert_eq!(db.level_tables(1).unwrap().len(), 1);
    assert!(get(&db, b"k").is_none());
}

#[test]
fn archive_commit_tombstone_free_always_ok() {
    let mut db = TestDb::new(MemDevice::<BLOCK>::new(), test_config());
    block_on(db.open()).unwrap();
    // Value and tombstone for the same key live in one table; there is
    // nothing older to resurrect, so archival is safe.
    block_on(db.put(b"k", b"v")).unwrap();
    block_on(db.delete(b"k")).unwrap();
    block_on(db.flush()).unwrap();
    assert!(block_on(db.archive_commit(0, 0)).unwrap());
    assert!(get(&db, b"k").is_none());

    // Plain value tables archive without complaint too.
    put_keys(&mut db, &[(b"a", b"1"), (b"b", b"2")]);
    assert!(block_on(db.archive_commit(0, 1)).unwrap());
    assert_eq!(db.level_tables(0).unwrap(), []);
}

// ---------------------------------------------------------------------------
// Crash atomicity of ingest
// ---------------------------------------------------------------------------

/// Builds the pre-ingest state: T0 uploaded remotely, archived locally,
/// then returns the device plus remote and sealed descriptor for the crash
/// loop below.
fn setup_ingest_crash() -> (MemDevice<BLOCK>, MemDevice<BLOCK>, SealedTable<256>) {
    let mut db = TestDb::new(MemDevice::<BLOCK>::new(), test_config());
    block_on(db.open()).unwrap();
    put_keys(&mut db, &[(b"k0", b"v0"), (b"k1", b"v1"), (b"k2", b"v2")]);
    let (remote, sealed) = upload(&db, 0, 0);
    assert!(block_on(db.archive_commit(0, 0)).unwrap());
    (db.into_device(), remote, sealed)
}

#[test]
fn ingest_crash_atomicity() {
    // Measure the write count so the loop can hit every crash point.
    let (dev, remote, sealed) = setup_ingest_crash();
    let count_dev = CountDevice::new(dev);
    let mut db = TestDb::new(count_dev, test_config());
    block_on(db.open()).unwrap();
    assert!(block_on(db.ingest_table(&sealed, &remote, 0)).unwrap());
    let writes = db.into_device().writes();
    // 4 block copies + index/footer relocation writes + 1 manifest commit.
    assert_eq!(writes, 7);

    // Crash at every point: the table is fully absent or fully attached,
    // and a retry converges to exactly-once attachment.
    for crash_at in 0..=writes {
        let (dev, remote, sealed) = setup_ingest_crash();
        let crash_dev: CrashDevice<_, BLOCK> = CrashDevice::new(dev, crash_at);
        let mut db = TestDb::new(crash_dev, test_config());
        block_on(db.open()).unwrap();
        // The crashed ingest may report success: the device lies.
        let _ = block_on(db.ingest_table(&sealed, &remote, 0));
        let dev = db.into_device().into_inner();

        let mut db = TestDb::new(dev, test_config());
        block_on(db.open()).unwrap();
        if crash_at == writes {
            assert_eq!(db.level_tables(0).unwrap().len(), 1, "crash_at={crash_at}");
            assert_eq!(get(&db, b"k1"), Some(b"v1".to_vec()));
        } else {
            assert!(
                db.level_tables(0).unwrap().is_empty(),
                "crash_at={crash_at}"
            );
        }
        // Retry converges: exactly one copy of the table afterwards.
        let again = block_on(db.ingest_table(&sealed, &remote, 0)).unwrap();
        assert_eq!(again, crash_at != writes, "crash_at={crash_at}");
        let tables = db.level_tables(0).unwrap();
        assert_eq!(tables.len(), 1, "crash_at={crash_at}");
        assert_eq!(tables[0].id, 0);
        assert_eq!(get(&db, b"k2"), Some(b"v2".to_vec()));
    }
}
