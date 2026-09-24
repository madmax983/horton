//! Crash injector for compaction (v0.9): for every crash point of one
//! L0→L1 compaction job, the recovered database must be exactly the
//! pre-compaction state (old manifest, L0 intact) or the post-compaction
//! state (new manifest, inputs replaced by the output table) — never a
//! mix. The logical map is identical in both: compaction only reorganizes,
//! so every crash point must expose all four keys.

mod common;

use std::collections::BTreeMap;
use std::task::{Context, Poll};

use common::{CrashDevice, MemDevice, TestDb, block_on, test_config};
use horton::{BlockDevice, Compaction, Manifest, Progress};

const BLOCK: usize = 4096;

type TestCompaction = Compaction<4096, 256, 1024, 1024>;
type TestManifest = Manifest<7, 4, 256>;

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

/// Builds the pre-compaction database: 4 puts + 4 flushes = full L0.
fn build() -> MemDevice<BLOCK> {
    let mut db = TestDb::new(MemDevice::<BLOCK>::new(), test_config());
    block_on(db.open()).unwrap();
    for i in 0..4u8 {
        block_on(db.put(&[b'k', b'0' + i], &[b'v', b'0' + i])).unwrap();
        block_on(db.flush()).unwrap();
    }
    assert!(db.compaction_pending());
    db.into_device()
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

/// Counts the block writes one L0→L1 job performs on the built database.
fn count_compaction_writes() -> usize {
    let dev = CountDevice {
        inner: build(),
        writes: 0,
    };
    let mut db = TestDb::new(dev, test_config());
    block_on(db.open()).unwrap();
    drive_one(&mut db);
    db.into_device().writes
}

/// Runs build + one compaction job with writes `>= crash_at` dropped.
fn run_crashed(crash_at: usize) -> MemDevice<BLOCK> {
    let dev: CrashDevice<MemDevice<BLOCK>, BLOCK> = CrashDevice::new(build(), crash_at);
    let mut db = TestDb::new(dev, test_config());
    block_on(db.open()).unwrap();
    drive_one(&mut db);
    db.into_device().into_inner()
}

fn live_map(db: &TestDb<MemDevice<BLOCK>>) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut map = BTreeMap::new();
    let mut buf = [0u8; 2048];
    for i in 0..4u8 {
        let key = [b'k', b'0' + i];
        if let Some(n) = block_on(db.get(&key, &mut buf)).unwrap() {
            map.insert(key.to_vec(), buf[..n].to_vec());
        }
    }
    map
}

/// Exhaustive: every crash point of one L0→L1 compaction job.
///
/// The manifest commit is the job's last device write, so a crash either
/// lands it (post-compaction: L0 drained into one L1 table) or drops it
/// (pre-compaction: L0 intact, orphaned output blocks invisible). The
/// logical map is all four keys in every case — no acknowledged write is
/// ever lost, no partial merge is ever visible.
#[test]
fn crash_during_compaction_is_atomic() {
    let w = count_compaction_writes();
    // Sanity on the assumed layout: 4 tiny keys merge into 1 data block +
    // index + bloom + footer, then the manifest commit.
    assert_eq!(w, 5, "write count changed; oracle below needs updating");

    let mut want = BTreeMap::new();
    for i in 0..4u8 {
        want.insert(vec![b'k', b'0' + i], vec![b'v', b'0' + i]);
    }

    for crash_at in 0..=w {
        let dev = run_crashed(crash_at);
        let mut db = TestDb::new(dev, test_config());
        let rep = block_on(db.open()).unwrap();
        assert_eq!(live_map(&db), want, "crash_at={crash_at}");
        assert_eq!(rep.recovered_records, 0, "crash_at={crash_at}");
        if crash_at == w {
            assert_eq!(rep.l0_tables, 0, "crash_at={crash_at}");
        } else {
            assert_eq!(rep.l0_tables, 4, "crash_at={crash_at}");
        }
    }
}

/// Targeted: the post-compaction manifest really holds the output table in
/// L1 with the full key range, and a second compaction finds nothing to do.
#[test]
fn post_compaction_state_is_coherent() {
    let dev = run_crashed(count_compaction_writes());
    let mut dev2 = dev;
    let mut scratch = [0u8; BLOCK];
    let m2 = block_on(TestManifest::recover(&mut dev2, &mut scratch, 0, 4))
        .unwrap()
        .0;
    let l1 = m2.level(1).unwrap();
    assert_eq!(l1.len(), 1, "one output table in L1");
    assert_eq!(l1[0].first_key.as_slice(), b"k0");
    assert_eq!(l1[0].last_key.as_slice(), b"k3");

    let mut db = TestDb::new(dev2, test_config());
    block_on(db.open()).unwrap();
    assert!(!db.compaction_pending());
}

/// Builds a database with range tombstones: 4 puts across 4 flushes (to
/// fill L0 like [`build`]), with a range delete covering two keys in the
/// second flush, so L0 holds tables with rdel sections.
fn build_with_rdel() -> MemDevice<BLOCK> {
    let mut db = TestDb::new(MemDevice::<BLOCK>::new(), test_config());
    block_on(db.open()).unwrap();
    for i in 0..4u8 {
        block_on(db.put(&[b'k', b'0' + i], &[b'v', b'0' + i])).unwrap();
        if i == 1 {
            block_on(db.delete_range(b"k1", b"k3")).unwrap();
        }
        block_on(db.flush()).unwrap();
    }
    assert!(db.compaction_pending());
    db.into_device()
}

/// Counts the block writes one L0→L1 job performs on the rdel database.
fn count_rdel_compaction_writes() -> usize {
    let dev = CountDevice {
        inner: build_with_rdel(),
        writes: 0,
    };
    let mut db = TestDb::new(dev, test_config());
    block_on(db.open()).unwrap();
    drive_one(&mut db);
    db.into_device().writes
}

/// Runs build + one rdel compaction job with writes `>= crash_at` dropped.
fn run_rdel_crashed(crash_at: usize) -> MemDevice<BLOCK> {
    let dev: CrashDevice<MemDevice<BLOCK>, BLOCK> = CrashDevice::new(build_with_rdel(), crash_at);
    let mut db = TestDb::new(dev, test_config());
    block_on(db.open()).unwrap();
    drive_one(&mut db);
    db.into_device().into_inner()
}

/// Exhaustive: every crash point of one L0→L1 compaction job carrying
/// range tombstones (exercises the rdel two-pass count/write: the output
/// reserves and streams rdel blocks before the data merge).
///
/// The manifest commit is the job's last device write, so a crash either
/// lands it (post-compaction) or drops it (pre-compaction, orphans swept
/// on open). The logical map is k0, k2, k3 live — k1 deleted by the range
/// tombstone (k2's put is newer than the tombstone, so it survives) — in
/// every case.
#[test]
fn crash_during_rdel_compaction_is_atomic() {
    let w = count_rdel_compaction_writes();
    // Sanity: the rdel section adds blocks vs the 5-write data-only job.
    assert!(w > 5, "rdel output should write more blocks, got {w}");

    let mut want = BTreeMap::new();
    want.insert(vec![b'k', b'0'], vec![b'v', b'0']);
    want.insert(vec![b'k', b'2'], vec![b'v', b'2']);
    want.insert(vec![b'k', b'3'], vec![b'v', b'3']);

    for crash_at in 0..=w {
        let dev = run_rdel_crashed(crash_at);
        let mut db = TestDb::new(dev, test_config());
        let rep = block_on(db.open()).unwrap();
        assert_eq!(live_map(&db), want, "crash_at={crash_at}");
        assert_eq!(rep.recovered_records, 0, "crash_at={crash_at}");
        if crash_at == w {
            assert_eq!(rep.l0_tables, 0, "crash_at={crash_at}");
        } else {
            assert_eq!(rep.l0_tables, 4, "crash_at={crash_at}");
        }
    }
}

/// Builds a database with two identical range tombstones over k0..=k3
/// (seq 3 and seq 4) shadowing an older put: the bottommost merge must
/// emit only the newer tombstone and drop the older shadowed one.
fn build_with_shadowed_rdel() -> MemDevice<BLOCK> {
    let mut db = TestDb::new(MemDevice::<BLOCK>::new(), test_config());
    block_on(db.open()).unwrap();
    block_on(db.put(b"k0", b"old")).unwrap();
    block_on(db.flush()).unwrap();
    block_on(db.put(b"k0", b"new")).unwrap();
    block_on(db.flush()).unwrap();
    block_on(db.delete_range(b"k0", b"k3")).unwrap();
    block_on(db.flush()).unwrap();
    block_on(db.delete_range(b"k0", b"k3")).unwrap();
    block_on(db.flush()).unwrap();
    assert!(db.compaction_pending());
    db.into_device()
}

/// Counts the block writes one L0→L1 job performs on the shadowed-rdel
/// database.
fn count_shadowed_rdel_writes() -> usize {
    let dev = CountDevice {
        inner: build_with_shadowed_rdel(),
        writes: 0,
    };
    let mut db = TestDb::new(dev, test_config());
    block_on(db.open()).unwrap();
    drive_one(&mut db);
    db.into_device().writes
}

/// Runs build + one shadowed-rdel compaction job with writes `>=
/// crash_at` dropped.
fn run_shadowed_rdel_crashed(crash_at: usize) -> MemDevice<BLOCK> {
    let dev: CrashDevice<MemDevice<BLOCK>, BLOCK> =
        CrashDevice::new(build_with_shadowed_rdel(), crash_at);
    let mut db = TestDb::new(dev, test_config());
    block_on(db.open()).unwrap();
    drive_one(&mut db);
    db.into_device().into_inner()
}

/// Exhaustive: every crash point of a bottommost merge that must drop a
/// shadowed duplicate range tombstone.
///
/// L1..L6 are empty, so the L0→L1 output is bottommost
/// ([`Db::is_bottommost_output`]): the newer `[k0,k3]` tombstone (seq 4)
/// is emitted and the older identical one (seq 3) is dropped by the
/// shadow gate — while both covered puts stay hidden under the surviving
/// tombstone. A crash either lands the commit (post: L0 drained, one L1
/// table carrying exactly the newer tombstone) or drops it (pre: L0
/// intact). The logical map is empty in every case: k0 was deleted, and
/// k1..k3 were never written.
#[test]
fn crash_during_bottommost_rdel_shadow_drop_is_atomic() {
    let w = count_shadowed_rdel_writes();
    // Sanity: the surviving tombstone's table is 5 blocks (rdel + data +
    // index + bloom + footer), then the manifest commit.
    assert_eq!(w, 6, "write count changed; oracle below needs updating");

    let want: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

    for crash_at in 0..=w {
        let dev = run_shadowed_rdel_crashed(crash_at);
        let mut db = TestDb::new(dev, test_config());
        let rep = block_on(db.open()).unwrap();
        assert_eq!(live_map(&db), want, "crash_at={crash_at}");
        assert_eq!(rep.recovered_records, 0, "crash_at={crash_at}");
        if crash_at == w {
            assert_eq!(rep.l0_tables, 0, "crash_at={crash_at}");
            assert_eq!(
                db.level_tables(1).map(<[horton::TableRef<256>]>::len),
                Some(1),
                "crash_at={crash_at}"
            );
        } else {
            assert_eq!(rep.l0_tables, 4, "crash_at={crash_at}");
        }
    }
}

/// Builds a database whose entire contents are point tombstones once
/// merged: two keys, each put then deleted, across four flushes. The
/// bottommost merge drops every tombstone, sealing no output table.
fn build_with_all_tombstones() -> MemDevice<BLOCK> {
    let mut db = TestDb::new(MemDevice::<BLOCK>::new(), test_config());
    block_on(db.open()).unwrap();
    block_on(db.put(b"k0", b"v0")).unwrap();
    block_on(db.flush()).unwrap();
    block_on(db.delete(b"k0")).unwrap();
    block_on(db.flush()).unwrap();
    block_on(db.put(b"k1", b"v1")).unwrap();
    block_on(db.flush()).unwrap();
    block_on(db.delete(b"k1")).unwrap();
    block_on(db.flush()).unwrap();
    assert!(db.compaction_pending());
    db.into_device()
}

/// Runs build + one all-tombstone compaction job with writes `>=
/// crash_at` dropped.
fn run_all_tombstone_crashed(crash_at: usize) -> MemDevice<BLOCK> {
    let dev: CrashDevice<MemDevice<BLOCK>, BLOCK> =
        CrashDevice::new(build_with_all_tombstones(), crash_at);
    let mut db = TestDb::new(dev, test_config());
    block_on(db.open()).unwrap();
    drive_one(&mut db);
    db.into_device().into_inner()
}

/// Exhaustive: every crash point of a bottommost merge that drops every
/// point tombstone and seals no output table.
///
/// Like the TTL purge job, this merge's only device write is the manifest
/// commit: a crash either lands it (post: L0 drained, L1 empty — the
/// tombstones were bottommost-dropped, nothing below can resurrect the
/// deleted keys) or drops it (pre: L0 intact with the tombstones still
/// standing). The logical map is empty in every case, and rerunning the
/// job on the recovered state converges.
#[test]
fn crash_during_bottommost_point_tombstone_drop_is_atomic() {
    let dev = CountDevice {
        inner: build_with_all_tombstones(),
        writes: 0,
    };
    let mut db = TestDb::new(dev, test_config());
    block_on(db.open()).unwrap();
    drive_one(&mut db);
    let w = db.into_device().writes;
    // Nothing survives the merge: the manifest commit is the only write.
    assert_eq!(w, 1, "write count changed; oracle below needs updating");

    let want: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

    for crash_at in 0..=w {
        let dev = run_all_tombstone_crashed(crash_at);
        let mut db = TestDb::new(dev, test_config());
        let rep = block_on(db.open()).unwrap();
        assert_eq!(live_map(&db), want, "crash_at={crash_at}");
        assert_eq!(rep.recovered_records, 0, "crash_at={crash_at}");
        if crash_at == w {
            assert_eq!(rep.l0_tables, 0, "crash_at={crash_at}");
            assert_eq!(
                db.level_tables(1).map(<[horton::TableRef<256>]>::len),
                Some(0),
                "crash_at={crash_at}"
            );
        } else {
            assert_eq!(rep.l0_tables, 4, "crash_at={crash_at}");
        }

        // Rerunning the job on the recovered state converges.
        drive_one(&mut db);
        assert!(!db.compaction_pending(), "crash_at={crash_at}");
        assert_eq!(live_map(&db), want, "crash_at={crash_at}");
    }
}

// ---------------------------------------------------------------------------
// Multi-output jobs. Under 8-block slots (`tight_config`) an output holds a
// few 1000-byte values, so one L0 job writes a string of outputs and commits
// after each, retiring the inputs it has passed and narrowing the one it is
// inside. Every one of those commits is a crash boundary: whatever write the
// crash lands on, the reopened tree must be consistent (invariants hold),
// read exactly the pre-job logical map, and finish the job cleanly.
// ---------------------------------------------------------------------------

/// Key `i` of the multi-output workload.
fn mkey(i: u32) -> Vec<u8> {
    format!("m{i:03}").into_bytes()
}

/// Builds a tree whose next job is a many-output L0 -> L1 merge: L1 holds
/// several split tables of 1000-byte values, L0 four tables that overwrite
/// and delete across all of them — a point delete and a range delete
/// spanning several L1 tables among them.
fn build_multi() -> MemDevice<BLOCK> {
    let mut db = TestDb::new(MemDevice::<BLOCK>::new(), common::tight_config());
    block_on(db.open()).unwrap();
    let mut c = TestCompaction::new();
    let val = |i: u32, round: u32| vec![u8::try_from((i + round) % 251).unwrap(); 1000];
    for round in 0..3u32 {
        for flush in 0..4u32 {
            for j in 0..3u32 {
                let i = (flush * 3 + j) * 3 + round;
                block_on(db.put(&mkey(i), &val(i, round))).unwrap();
            }
            block_on(db.flush()).unwrap();
        }
        while block_on(db.compact_step(&mut c)).unwrap() == Progress::More {}
    }
    assert!(db.level_tables(1).unwrap().len() >= 3, "setup: L1 split");
    for flush in 0..4u32 {
        let i = flush * 9;
        block_on(db.put(&mkey(i), &val(i, 7))).unwrap();
        block_on(db.put(&mkey(i + 4), &val(i + 4, 7))).unwrap();
        if flush == 1 {
            block_on(db.delete(&mkey(3))).unwrap();
            block_on(db.delete_range(&mkey(10), &mkey(25))).unwrap();
        }
        block_on(db.flush()).unwrap();
    }
    assert!(db.compaction_pending(), "setup: L0 full");
    db.into_device()
}

/// The live logical map of the multi-output workload's key space.
fn multi_map<D: BlockDevice>(db: &TestDb<D>) -> BTreeMap<Vec<u8>, Vec<u8>>
where
    D::Error: std::fmt::Debug,
{
    let mut out = BTreeMap::new();
    let mut buf = [0u8; 1024];
    for i in 0..40u32 {
        if let Some(n) = block_on(db.get(&mkey(i), &mut buf)).unwrap() {
            out.insert(mkey(i), buf[..n].to_vec());
        }
    }
    out
}

/// Drains every pending job.
fn drain_all<D: BlockDevice>(db: &mut TestDb<D>)
where
    D::Error: std::fmt::Debug,
{
    let mut c = TestCompaction::new();
    while db.compaction_pending() {
        while block_on(db.compact_step(&mut c)).unwrap() == Progress::More {}
    }
}

#[test]
fn crash_during_multi_output_compaction_leaves_a_consistent_tree() {
    let (want, job_writes, outputs) = {
        let mut db = TestDb::new(
            CountDevice {
                inner: build_multi(),
                writes: 0,
            },
            common::tight_config(),
        );
        block_on(db.open()).unwrap();
        let want = multi_map(&db);
        let before = db.into_device();
        let mut db = TestDb::new(before, common::tight_config());
        block_on(db.open()).unwrap();
        let start = db.device().writes;
        drain_all(&mut db);
        assert_eq!(multi_map(&db), want, "clean run");
        let outputs = db.level_tables(1).unwrap().len();
        (want, db.into_device().writes - start, outputs)
    };
    assert!(outputs >= 3, "the job wrote {outputs} outputs");
    assert!(job_writes > 20, "the job wrote only {job_writes} blocks");
    for crash_at in 0..job_writes {
        let mut db = TestDb::new(
            CrashDevice::<_, BLOCK>::new(build_multi(), crash_at),
            common::tight_config(),
        );
        block_on(db.open()).unwrap();
        drain_all(&mut db);
        let dev = db.into_device().into_inner();
        // Reopen on the bare device: some prefix of the job's commits is
        // durable, the rest never happened.
        let mut db = TestDb::new(dev, common::tight_config());
        block_on(db.open()).unwrap();
        assert_eq!(db.check_invariants(), Ok(()), "crash_at={crash_at}");
        assert_eq!(multi_map(&db), want, "crash_at={crash_at}: after the crash");
        drain_all(&mut db);
        assert_eq!(db.check_invariants(), Ok(()), "crash_at={crash_at}");
        assert_eq!(multi_map(&db), want, "crash_at={crash_at}: after finishing");
        assert_eq!(db.slot_stats().reserved, 0);
    }
}
