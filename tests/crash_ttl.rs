//! Crash injector for TTL (v0.15).
//!
//! Two races around expiry:
//!
//! 1. TTL expiry racing flush: a key whose TTL lapses while it sits in the
//!    memtable, or mid-flush. Flush never purges expired entries — expiry
//!    is a read-time filter over the stored `expire_at` — so the key must
//!    neither resurrect through WAL replay at any crash point nor lose
//!    its acknowledged write.
//! 2. Crash during a TTL-purging compaction: the opt-in
//!    [`Compaction::purge_before`] turns expired values into tombstones,
//!    which the bottommost drop then removes (no live snapshots). A crash
//!    must leave the pre-purge state (expired-but-stored) or the post-purge
//!    state (inputs drained, nothing sealed); both read identically for
//!    every contract-abiding `now >= purge_before`.

mod common;

use std::collections::BTreeMap;
use std::task::{Context, Poll};

use common::{CrashDevice, MemDevice, TestDb, block_on, test_config};
use horton::{BlockDevice, Compaction, Progress};

const BLOCK: usize = 4096;
const EXPIRE_AT: u64 = 100;
/// A `now` past the expiry: the TTL'd key must read as missing.
const NOW_EXPIRED: u64 = 1000;
/// A `now` before the expiry: the TTL'd key follows the landed prefix.
const NOW_FRESH: u64 = 50;

type TestCompaction = Compaction<4096, 256, 1024, 1024>;

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

/// Drives exactly one compaction job to completion with the given TTL
/// purge cutoff (no-op when idle).
fn drive_purge<D: BlockDevice>(db: &mut TestDb<D>, purge_before: u64)
where
    D::Error: std::fmt::Debug,
{
    let mut c = TestCompaction::new();
    c.purge_before = purge_before;
    loop {
        match block_on(db.compact_step(&mut c)) {
            Ok(Progress::More) => {}
            Ok(Progress::Done) => break,
            Err(e) => panic!("unexpected compaction error: {e:?}"),
        }
    }
}

fn get_now(db: &TestDb<MemDevice<BLOCK>>, key: &[u8], now: u64) -> Option<Vec<u8>> {
    let mut buf = [0u8; 1024];
    block_on(db.get_with_time(key, &mut buf, now))
        .unwrap()
        .map(|n| buf[..n].to_vec())
}

/// Write census of `put_with_ttl` on kt (expired) + `put` on k2 + flush.
fn count_writes() -> usize {
    let dev = CountDevice {
        inner: MemDevice::<BLOCK>::new(),
        writes: 0,
    };
    let mut db = TestDb::new(dev, test_config());
    block_on(db.open()).unwrap();
    block_on(db.put_with_ttl(b"kt", b"vt", EXPIRE_AT)).unwrap();
    block_on(db.put(b"k2", b"v2")).unwrap();
    block_on(db.flush()).unwrap();
    db.into_device().writes
}

/// Runs the TTL script with writes `>= crash_at` dropped.
fn run_crashed(crash_at: usize) -> MemDevice<BLOCK> {
    let dev: CrashDevice<MemDevice<BLOCK>, BLOCK> =
        CrashDevice::new(MemDevice::<BLOCK>::new(), crash_at);
    let mut db = TestDb::new(dev, test_config());
    block_on(db.open()).unwrap();
    block_on(db.put_with_ttl(b"kt", b"vt", EXPIRE_AT)).unwrap();
    block_on(db.put(b"k2", b"v2")).unwrap();
    block_on(db.flush()).unwrap();
    db.into_device().into_inner()
}

/// Exhaustive: every crash point of a flush racing a TTL expiry.
///
/// Write order: WAL(kt)=#0, WAL(k2)=#1, table blocks #2..#5, manifest=#6 —
/// the same shape as the plain flush. The oracle:
/// - for `now >= expire_at` the TTL'd key is missing at EVERY crash
///   point, even when its WAL record landed and replayed: flush stores
///   the expiry, it never purges it, so replay cannot resurrect the key;
/// - for `now < expire_at` the TTL'd key follows the landed prefix
///   exactly: the acknowledged write is never lost;
/// - the plain key follows the landed prefix, as in `crash_flush`.
#[test]
fn crash_during_ttl_flush_never_resurrects_nor_loses() {
    let w = count_writes();
    // Sanity on the assumed layout: 2 WAL writes + 4 table blocks + 1 manifest.
    assert_eq!(w, 7, "write count changed; oracle below needs updating");

    for crash_at in 0..=w {
        let dev = run_crashed(crash_at);
        let mut db = TestDb::new(dev, test_config());
        let rep = block_on(db.open()).unwrap();

        // Expired: missing everywhere, however the crash interleaved.
        assert_eq!(
            get_now(&db, b"kt", NOW_EXPIRED),
            None,
            "crash_at={crash_at}"
        );
        // Not yet expired: visible exactly when the WAL write landed.
        let want_ttl = if crash_at >= 1 {
            Some(b"vt".to_vec())
        } else {
            None
        };
        assert_eq!(
            get_now(&db, b"kt", NOW_FRESH),
            want_ttl,
            "crash_at={crash_at}"
        );
        // The plain key: the landed-prefix rule.
        let want_plain = if crash_at >= 2 {
            Some(b"v2".to_vec())
        } else {
            None
        };
        assert_eq!(
            get_now(&db, b"k2", NOW_EXPIRED),
            want_plain,
            "crash_at={crash_at}"
        );

        if crash_at == w {
            assert_eq!(rep.l0_tables, 1, "crash_at={crash_at}");
            assert_eq!(rep.recovered_records, 0, "crash_at={crash_at}");
            assert_eq!(rep.max_seq, 2, "crash_at={crash_at}");
        } else {
            assert_eq!(rep.l0_tables, 0, "crash_at={crash_at}");
            assert_eq!(
                rep.recovered_records,
                crash_at.min(2) as u64,
                "crash_at={crash_at}"
            );
        }
    }
}

/// Four TTL flushes; every key is expired as of the purge cutoff.
fn build_ttl() -> MemDevice<BLOCK> {
    let mut db = TestDb::new(MemDevice::<BLOCK>::new(), test_config());
    block_on(db.open()).unwrap();
    for i in 0..4u8 {
        block_on(db.put_with_ttl(&[b'k', b'0' + i], &[b'v', b'0' + i], EXPIRE_AT)).unwrap();
        block_on(db.flush()).unwrap();
    }
    assert!(db.compaction_pending());
    db.into_device()
}

/// Runs build + one purging compaction job with writes `>= crash_at`
/// dropped; returns the device.
fn run_purge_crashed(crash_at: usize) -> MemDevice<BLOCK> {
    let dev: CrashDevice<MemDevice<BLOCK>, BLOCK> = CrashDevice::new(build_ttl(), crash_at);
    let mut db = TestDb::new(dev, test_config());
    block_on(db.open()).unwrap();
    drive_purge(&mut db, NOW_EXPIRED);
    db.into_device().into_inner()
}

/// Exhaustive: every crash point of a TTL-purging compaction.
///
/// The purge converts each expired value to a point tombstone at the same
/// sequence; the bottommost drop then removes those tombstones (no live
/// snapshots pin them), so the job seals no output table at all.
/// Pre-crash state holds expired values, post-crash state holds nothing —
/// both read as missing for every contract-abiding `now >= purge_before`,
/// so the logical map is empty in every recovered view and no
/// acknowledged state is lost. Rerunning the job on the recovered state
/// converges idempotently.
#[test]
fn crash_during_ttl_purge_compaction_is_atomic() {
    // Count the job's writes on a non-crashing device.
    let dev = CountDevice {
        inner: build_ttl(),
        writes: 0,
    };
    let mut db = TestDb::new(dev, test_config());
    block_on(db.open()).unwrap();
    drive_purge(&mut db, NOW_EXPIRED);
    let w = db.into_device().writes;
    // The purge drops everything (expired → tombstone → bottommost drop),
    // so the merge seals no output table: the manifest commit is the
    // job's ONLY device write.
    assert_eq!(w, 1, "write count changed; oracle below needs updating");

    let live_map = |db: &TestDb<MemDevice<BLOCK>>| {
        let mut map = BTreeMap::new();
        for i in 0..4u8 {
            let key = [b'k', b'0' + i];
            if let Some(v) = get_now(db, &key, NOW_EXPIRED) {
                map.insert(key.to_vec(), v);
            }
        }
        map
    };

    for crash_at in 0..=w {
        let dev = run_purge_crashed(crash_at);
        let mut db = TestDb::new(dev, test_config());
        let rep = block_on(db.open()).unwrap();
        assert!(live_map(&db).is_empty(), "crash_at={crash_at}");
        assert_eq!(rep.recovered_records, 0, "crash_at={crash_at}");
        if crash_at == w {
            // The commit landed: all four inputs are gone. The purge turned
            // every value into a tombstone and the bottommost drop removed
            // those too, so no output table was sealed.
            assert_eq!(rep.l0_tables, 0, "crash_at={crash_at}");
            assert_eq!(
                db.level_tables(1).map(<[horton::TableRef<256>]>::len),
                Some(0),
                "crash_at={crash_at}"
            );
        } else {
            // The commit was dropped: the four L0 tables stand.
            assert_eq!(rep.l0_tables, 4, "crash_at={crash_at}");
        }

        // Rerunning the job on the recovered state converges; the map
        // stays empty throughout.
        drive_purge(&mut db, NOW_EXPIRED);
        assert!(live_map(&db).is_empty(), "crash_at={crash_at}");
        assert!(!db.compaction_pending(), "crash_at={crash_at}");
        assert_eq!(
            db.level_tables(0).map(<[horton::TableRef<256>]>::len),
            Some(0),
            "crash_at={crash_at}"
        );
        assert_eq!(
            db.level_tables(1).map(<[horton::TableRef<256>]>::len),
            Some(0),
            "crash_at={crash_at}"
        );
    }
}
