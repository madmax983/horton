//! Scan iterator + snapshot reads (v0.5).
//!
//! RED: `Scan`, `Db::snapshot`, `Db::get_at` do not exist yet.

mod common;

use std::collections::BTreeMap;

use common::{Lcg, MemDevice, TestDb, block_on, test_config};
use horton::{BlockDevice, Error, Scan};

type TestScan<'d> = Scan<'d, MemDevice<4096>, 4096, 256, 1024, 64, 4096, 7, 4, 1024, 8>;

fn open<D: BlockDevice>(db: &mut TestDb<D>)
where
    D::Error: std::fmt::Debug,
{
    block_on(db.open()).unwrap();
}

/// Drives compaction to completion.
fn drive<D: BlockDevice>(db: &mut TestDb<D>)
where
    D::Error: std::fmt::Debug,
{
    use horton::{Compaction, Progress};
    let mut c = Compaction::<4096, 256, 1024, 1024>::new();
    while block_on(db.compact_step(&mut c)).unwrap() == Progress::More {}
}

/// Flushes, driving compaction first when level 0 is full. The random
/// workload can fill L0 between explicit drives; the database reports that
/// as `NeedsCompaction` (the documented flush contract — the caller must
/// compact), so the test compacts and retries rather than treating it as a
/// failure. A second refusal still panics.
fn flush(db: &mut TestDb<MemDevice<4096>>) {
    match block_on(db.flush()) {
        Ok(()) => {}
        Err(horton::Error::NeedsCompaction) => {
            drive(db);
            block_on(db.flush()).unwrap();
        }
        Err(e) => panic!("unexpected flush error: {e:?}"),
    }
}

/// Collects a full scan into a vec.
fn collect(db: &TestDb<MemDevice<4096>>, max_seq: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut scan = TestScan::new(db);
    block_on(scan.seek(b"", None, max_seq)).unwrap();
    let mut out = Vec::new();
    let mut kbuf = [0u8; 256];
    let mut vbuf = [0u8; 1024];
    while let Some((klen, vlen)) = block_on(scan.next(&mut kbuf, &mut vbuf)).unwrap() {
        out.push((kbuf[..klen].to_vec(), vbuf[..vlen].to_vec()));
    }
    out
}

#[test]
fn scan_empty_db_yields_nothing() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    assert_eq!(collect(&db, u64::MAX), Vec::new());
}

#[test]
fn scan_memtable_only_is_ascending() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    for k in [b"c", b"a", b"b"] {
        block_on(db.put(k, k)).unwrap();
    }
    let got = collect(&db, u64::MAX);
    assert_eq!(
        got,
        vec![
            (b"a".to_vec(), b"a".to_vec()),
            (b"b".to_vec(), b"b".to_vec()),
            (b"c".to_vec(), b"c".to_vec()),
        ]
    );
}

#[test]
fn scan_spans_memtable_tables_and_levels() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    // Four L0 tables, then compact into L1, then fresh memtable writes.
    for t in 0..4u8 {
        block_on(db.put(&[b'0' + t], &[t])).unwrap();
        block_on(db.flush()).unwrap();
    }
    drive(&mut db);
    block_on(db.put(b"m", b"m")).unwrap();
    let got = collect(&db, u64::MAX);
    let mut expect: Vec<(Vec<u8>, Vec<u8>)> = (0..4u8).map(|t| (vec![b'0' + t], vec![t])).collect();
    expect.push((b"m".to_vec(), b"m".to_vec()));
    expect.sort();
    assert_eq!(got, expect);
}

#[test]
fn scan_dedups_highest_sequence_wins() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    block_on(db.put(b"k", b"old")).unwrap();
    block_on(db.flush()).unwrap();
    block_on(db.put(b"k", b"new")).unwrap();
    block_on(db.flush()).unwrap();
    block_on(db.put(b"k", b"newest")).unwrap();
    let got = collect(&db, u64::MAX);
    assert_eq!(got, vec![(b"k".to_vec(), b"newest".to_vec())]);
}

#[test]
fn scan_skips_tombstones() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    block_on(db.put(b"a", b"1")).unwrap();
    block_on(db.put(b"b", b"2")).unwrap();
    block_on(db.delete(b"a")).unwrap();
    block_on(db.flush()).unwrap();
    block_on(db.put(b"c", b"3")).unwrap();
    let got = collect(&db, u64::MAX);
    assert_eq!(
        got,
        vec![
            (b"b".to_vec(), b"2".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
        ]
    );
}

#[test]
fn scan_respects_range_bounds() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    for k in [b"a", b"b", b"c", b"d"] {
        block_on(db.put(k, k)).unwrap();
    }
    block_on(db.flush()).unwrap();
    let mut scan = TestScan::new(&db);
    // [b, d): b and c, never a or d.
    block_on(scan.seek(b"b", Some(b"d".as_slice()), u64::MAX)).unwrap();
    let mut kbuf = [0u8; 256];
    let mut vbuf = [0u8; 1024];
    let mut out = Vec::new();
    while let Some((klen, vlen)) = block_on(scan.next(&mut kbuf, &mut vbuf)).unwrap() {
        out.push((kbuf[..klen].to_vec(), vbuf[..vlen].to_vec()));
    }
    assert_eq!(
        out,
        vec![
            (b"b".to_vec(), b"b".to_vec()),
            (b"c".to_vec(), b"c".to_vec()),
        ]
    );
}

#[test]
fn scan_buffer_too_small_does_not_consume() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    block_on(db.put(b"key", b"value-is-long")).unwrap();
    let mut scan = TestScan::new(&db);
    block_on(scan.seek(b"", None, u64::MAX)).unwrap();
    let mut kbuf = [0u8; 256];
    let mut tiny = [0u8; 2];
    let err = block_on(scan.next(&mut kbuf, &mut tiny)).unwrap_err();
    assert!(matches!(err, Error::BufferTooSmall { .. }));
    // Retry with a fitting buffer: the same entry must come back.
    let mut vbuf = [0u8; 1024];
    let got = block_on(scan.next(&mut kbuf, &mut vbuf)).unwrap();
    assert_eq!(got, Some((3, 13)));
    assert_eq!(&kbuf[..3], b"key");
    assert_eq!(&vbuf[..13], b"value-is-long");
}

#[test]
fn scan_snapshot_isolation() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    block_on(db.put(b"a", b"1")).unwrap();
    block_on(db.put(b"b", b"1")).unwrap();
    block_on(db.flush()).unwrap();
    let snap = db.snapshot().unwrap();
    // Mutations after the snapshot: update, delete, insert.
    block_on(db.put(b"a", b"2")).unwrap();
    block_on(db.delete(b"b")).unwrap();
    block_on(db.put(b"c", b"3")).unwrap();
    // The snapshot still sees the old world.
    let old = collect(&db, snap);
    assert_eq!(
        old,
        vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"1".to_vec()),
        ]
    );
    // The live view sees the new world.
    let live = collect(&db, u64::MAX);
    assert_eq!(
        live,
        vec![
            (b"a".to_vec(), b"2".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
        ]
    );
    db.release_snapshot(snap);
}

#[test]
fn get_at_respects_snapshot() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    block_on(db.put(b"k", b"v1")).unwrap();
    let snap = db.snapshot().unwrap();
    block_on(db.put(b"k", b"v2")).unwrap();
    let mut buf = [0u8; 1024];
    let old = block_on(db.get_at(b"k", &mut buf, snap)).unwrap();
    assert_eq!(old, Some(2));
    assert_eq!(&buf[..2], b"v1");
    let live = block_on(db.get_at(b"k", &mut buf, u64::MAX)).unwrap();
    assert_eq!(live, Some(2));
    assert_eq!(&buf[..2], b"v2");
    db.release_snapshot(snap);
}

#[test]
fn snapshot_blocks_tombstone_drop() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    block_on(db.put(b"a", b"1")).unwrap();
    block_on(db.flush()).unwrap();
    // Snapshot between the put and the delete.
    let snap = db.snapshot().unwrap();
    block_on(db.delete(b"a")).unwrap();
    // Fill L0 so compaction runs; L1 is the bottommost level holding the
    // range, so without the snapshot floor the tombstone would be dropped.
    for t in 0..3u8 {
        block_on(db.put(&[b'x', t], &[t])).unwrap();
        block_on(db.flush()).unwrap();
    }
    drive(&mut db);
    // The snapshot must still see the value: the tombstone was retained.
    let old = collect(&db, snap);
    assert!(
        old.iter().any(|(k, v)| k == b"a" && v == b"1"),
        "snapshot must still see a=1, got {old:?}"
    );
    // The live view sees the deletion.
    let live = collect(&db, u64::MAX);
    assert!(
        !live.iter().any(|(k, _)| k == b"a"),
        "live view must not see a, got {live:?}"
    );
    db.release_snapshot(snap);
}

#[test]
fn snapshot_registry_exhaustion_is_snapshot_limit() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    let mut snaps = Vec::new();
    for _ in 0..8 {
        snaps.push(db.snapshot().unwrap());
    }
    let err = db.snapshot().unwrap_err();
    assert!(matches!(err, Error::SnapshotLimit));
    db.release_snapshot(snaps[0]);
    // A slot freed up: snapshotting works again.
    let s = db.snapshot().unwrap();
    db.release_snapshot(s);
    for s in snaps.into_iter().skip(1) {
        db.release_snapshot(s);
    }
}

/// Differential property test: random puts/deletes/flushes/compactions and
/// snapshots against a `BTreeMap` oracle of per-key version lists.
#[test]
fn scan_matches_oracle_under_random_ops() {
    type Oracle = BTreeMap<Vec<u8>, Vec<(u64, Option<Vec<u8>>)>>;
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    let mut rng = Lcg::new(0x5eed);
    // Oracle: key -> [(seq, Some(val) | None for tombstone)], seq-ascending.
    let mut oracle: Oracle = BTreeMap::new();
    let mut live_snaps: Vec<u64> = Vec::new();

    // Oracle projection at a snapshot: highest seq <= snap per key.
    let project = |oracle: &Oracle, snap: u64| -> Vec<(Vec<u8>, Vec<u8>)> {
        oracle
            .iter()
            .filter_map(|(k, vs)| {
                vs.iter()
                    .rev()
                    .find(|(s, _)| *s <= snap)
                    .and_then(|(_, v)| v.clone().map(|vv| (k.clone(), vv)))
            })
            .collect()
    };

    let check = |db: &TestDb<MemDevice<4096>>, oracle: &Oracle, snaps: &[u64]| {
        for snap in snaps.iter().copied().chain([u64::MAX]) {
            let got = collect(db, snap);
            let want = project(oracle, snap);
            assert_eq!(got, want, "scan diverged from oracle at snap {snap}");
            // Spot-check get_at on every key too.
            for k in oracle.keys() {
                let mut buf = [0u8; 1024];
                let got = block_on(db.get_at(k, &mut buf, snap)).unwrap();
                let want_len = want.iter().find(|(kk, _)| kk == k).map(|(_, v)| v.len());
                assert_eq!(got, want_len, "get_at diverged at snap {snap} for {k:?}");
                if let Some(n) = want_len {
                    let want_v = want.iter().find(|(kk, _)| kk == k).unwrap().1.clone();
                    assert_eq!(&buf[..n], &want_v[..]);
                }
            }
        }
    };

    for round in 0..30 {
        let op = rng.next() % 10;
        let key = vec![b'k', (rng.next() % 8) as u8];
        match op {
            0..=4 => {
                let val = vec![(rng.next() % 251) as u8; 1 + (rng.next() % 20) as usize];
                let seq = block_on(db.put(&key, &val)).unwrap();
                oracle.entry(key).or_default().push((seq, Some(val)));
            }
            5..=6 => {
                let seq = block_on(db.delete(&key)).unwrap();
                oracle.entry(key).or_default().push((seq, None));
            }
            7 => {
                flush(&mut db);
            }
            8 => {
                drive(&mut db);
            }
            _ => {
                if live_snaps.len() < 8 && rng.next().is_multiple_of(2) {
                    live_snaps.push(db.snapshot().unwrap());
                }
            }
        }
        if round % 5 == 4 {
            flush(&mut db);
            check(&db, &oracle, &live_snaps);
        }
    }
    flush(&mut db);
    drive(&mut db);
    check(&db, &oracle, &live_snaps);
    for s in live_snaps {
        db.release_snapshot(s);
    }
}

/// Scan start positioning on a version run straddling data blocks. Forty
/// 100-byte versions of `k` (~4.6 KiB on disk) force one table's run
/// across two 4 KiB data blocks; seeking exactly at `k` must land on the
/// run's first block and yield the newest visible version exactly once —
/// including for a watermark that hides the run's first block.
#[test]
fn scan_seek_at_cross_block_version_run() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    block_on(db.put(b"a", b"va")).unwrap();
    let mut seqs = [0u64; 40];
    for (v, slot) in seqs.iter_mut().enumerate() {
        let val = [u8::try_from(v).unwrap(); 100];
        *slot = block_on(db.put(b"k", &val)).unwrap();
    }
    block_on(db.put(b"z", b"vz")).unwrap();
    flush(&mut db);

    // Full scan: `k` appears exactly once, with the newest value.
    let all = collect(&db, u64::MAX);
    assert_eq!(all.len(), 3, "a, k, z — k yielded once");
    assert_eq!(all[0].0, b"a");
    assert_eq!(all[1].0, b"k");
    assert_eq!(all[1].1, vec![39u8; 100]);
    assert_eq!(all[2].0, b"z");

    // Seek exactly at `k` with a watermark hiding the run's first block:
    // the scan must continue the run into the next block.
    let mut scan = TestScan::new(&db);
    block_on(scan.seek(b"k", None, seqs[5])).unwrap();
    let mut kbuf = [0u8; 256];
    let mut vbuf = [0u8; 1024];
    let (klen, vlen) = block_on(scan.next(&mut kbuf, &mut vbuf))
        .unwrap()
        .expect("k must be visible at its own watermark");
    assert_eq!(&kbuf[..klen], b"k");
    assert_eq!(&vbuf[..vlen], &[5u8; 100]);
    // `z` was written after every `k` version, so it is newer than the
    // watermark: the scan correctly ends after `k`. (The seek already
    // proved the cross-block walk — it crossed from the run's first block
    // into the second to find `v5` — and the advance walked the run's tail
    // `v4..=v0` to exhaustion without yielding a duplicate.)
    assert_eq!(
        block_on(scan.next(&mut kbuf, &mut vbuf)).unwrap(),
        None,
        "z is newer than the watermark; scan ends after k"
    );
}
