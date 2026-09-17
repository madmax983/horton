//! Crash injector for compaction (v0.9): for every crash point of one
//! L0→L1 compaction job, the recovered database must be exactly the
//! pre-compaction state (old manifest, L0 intact) or the post-compaction
//! state (new manifest, inputs replaced by the output table) — never a
//! mix. The logical map is identical in both: compaction only reorganizes,
//! so every crash point must expose all four keys.

mod common;

use std::collections::BTreeMap;
use std::task::{Context, Poll};

use common::{block_on, test_config, CrashDevice, MemDevice, TestDb};
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
    let m2 = block_on(TestManifest::recover(&mut dev2, &mut scratch, 0, 1))
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
