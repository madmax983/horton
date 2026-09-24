//! Reverse scan iterator (v0.14).
//!
//! RED: `RevScan`, `seek_prev`, `prev` do not exist yet.

mod common;

use std::collections::BTreeMap;

use common::{Lcg, MemDevice, TestDb, block_on, test_config};
use horton::{BlockDevice, RevScan};

type TestRevScan<'d> = RevScan<'d, MemDevice<4096>, 4096, 256, 1024, 64, 4096, 7, 4, 1024, 8>;

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

/// Flushes, driving compaction first when level 0 is full (same contract
/// note as in tests/scan.rs).
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

/// Collects a full reverse scan (from the last key, no lower bound) into a
/// vec, in the order `prev` yields.
fn rcollect(db: &TestDb<MemDevice<4096>>, max_seq: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
    rcollect_from(db, b"", None, max_seq)
}

/// Collects a reverse scan from `from` with an optional exclusive lower
/// bound.
fn rcollect_from(
    db: &TestDb<MemDevice<4096>>,
    from: &[u8],
    lower: Option<&[u8]>,
    max_seq: u64,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut scan = TestRevScan::new(db);
    block_on(scan.seek_prev(from, lower, max_seq)).unwrap();
    let mut out = Vec::new();
    let mut kbuf = [0u8; 256];
    let mut vbuf = [0u8; 1024];
    while let Some((klen, vlen)) = block_on(scan.prev(&mut kbuf, &mut vbuf)).unwrap() {
        out.push((kbuf[..klen].to_vec(), vbuf[..vlen].to_vec()));
    }
    out
}

#[test]
fn revscan_empty_db_yields_nothing() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    assert_eq!(rcollect(&db, u64::MAX), Vec::new());
}

#[test]
fn revscan_memtable_only_is_descending() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    for k in [b"a", b"b", b"c"] {
        block_on(db.put(k, k)).unwrap();
    }
    assert_eq!(
        rcollect(&db, u64::MAX),
        vec![
            (b"c".to_vec(), b"c".to_vec()),
            (b"b".to_vec(), b"b".to_vec()),
            (b"a".to_vec(), b"a".to_vec()),
        ]
    );
}

#[test]
fn revscan_seek_prev_positions_at_last_le() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    for k in [b"a", b"c", b"e"] {
        block_on(db.put(k, k)).unwrap();
    }
    // Exactly on a key: starts there.
    assert_eq!(
        rcollect_from(&db, b"c", None, u64::MAX)
            .iter()
            .map(|(k, _)| k.clone())
            .collect::<Vec<_>>(),
        vec![b"c".to_vec(), b"a".to_vec()]
    );
    // Between keys: starts at the greatest key below.
    assert_eq!(
        rcollect_from(&db, b"d", None, u64::MAX)
            .iter()
            .map(|(k, _)| k.clone())
            .collect::<Vec<_>>(),
        vec![b"c".to_vec(), b"a".to_vec()]
    );
    // Above every key: starts at the last key.
    assert_eq!(
        rcollect_from(&db, b"z", None, u64::MAX)
            .iter()
            .map(|(k, _)| k.clone())
            .collect::<Vec<_>>(),
        vec![b"e".to_vec(), b"c".to_vec(), b"a".to_vec()]
    );
    // Below every key: yields nothing.
    assert_eq!(rcollect_from(&db, b"0", None, u64::MAX), Vec::new());
}

#[test]
fn revscan_empty_from_starts_at_last_key() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    for k in [b"b", b"a", b"c"] {
        block_on(db.put(k, k)).unwrap();
    }
    flush(&mut db);
    // Empty `from` mirrors forward `seek(b"", ..)`: the whole key space,
    // descending.
    let got = rcollect(&db, u64::MAX);
    assert_eq!(
        got,
        vec![
            (b"c".to_vec(), b"c".to_vec()),
            (b"b".to_vec(), b"b".to_vec()),
            (b"a".to_vec(), b"a".to_vec()),
        ]
    );
}

#[test]
fn revscan_lower_bound_is_exclusive() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    for k in [b"a", b"b", b"c", b"d"] {
        block_on(db.put(k, k)).unwrap();
    }
    // (lower, from]: lower itself is not yielded.
    let got = rcollect_from(&db, b"d", Some(b"b"), u64::MAX);
    assert_eq!(
        got,
        vec![
            (b"d".to_vec(), b"d".to_vec()),
            (b"c".to_vec(), b"c".to_vec()),
        ]
    );
    // Lower bound at the top yields nothing.
    assert_eq!(rcollect_from(&db, b"d", Some(b"d"), u64::MAX), Vec::new());
}

#[test]
fn revscan_spans_memtable_tables_and_levels() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    for t in 0..4u8 {
        block_on(db.put(&[b'0' + t], &[t])).unwrap();
        block_on(db.flush()).unwrap();
    }
    drive(&mut db);
    block_on(db.put(b"m", b"m")).unwrap();
    let got = rcollect(&db, u64::MAX);
    let mut expect: Vec<(Vec<u8>, Vec<u8>)> = (0..4u8).map(|t| (vec![b'0' + t], vec![t])).collect();
    expect.push((b"m".to_vec(), b"m".to_vec()));
    expect.sort();
    expect.reverse();
    assert_eq!(got, expect);
}

#[test]
fn revscan_dedups_highest_sequence_wins() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    block_on(db.put(b"k", b"old")).unwrap();
    block_on(db.flush()).unwrap();
    block_on(db.put(b"k", b"new")).unwrap();
    block_on(db.flush()).unwrap();
    block_on(db.put(b"k", b"newest")).unwrap();
    let got = rcollect(&db, u64::MAX);
    assert_eq!(got, vec![(b"k".to_vec(), b"newest".to_vec())]);
}

#[test]
fn revscan_skips_tombstones() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    block_on(db.put(b"a", b"1")).unwrap();
    block_on(db.put(b"b", b"2")).unwrap();
    block_on(db.delete(b"a")).unwrap();
    block_on(db.flush()).unwrap();
    block_on(db.put(b"c", b"3")).unwrap();
    let got = rcollect(&db, u64::MAX);
    assert_eq!(
        got,
        vec![
            (b"c".to_vec(), b"3".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
        ]
    );
}

#[test]
fn revscan_snapshot_isolation() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    block_on(db.put(b"a", b"1")).unwrap();
    block_on(db.put(b"b", b"1")).unwrap();
    block_on(db.flush()).unwrap();
    let snap = db.snapshot().unwrap();
    block_on(db.put(b"a", b"2")).unwrap();
    block_on(db.delete(b"b")).unwrap();
    block_on(db.put(b"c", b"3")).unwrap();
    let old = rcollect(&db, snap);
    assert_eq!(
        old,
        vec![
            (b"b".to_vec(), b"1".to_vec()),
            (b"a".to_vec(), b"1".to_vec()),
        ]
    );
    let live = rcollect(&db, u64::MAX);
    assert_eq!(
        live,
        vec![
            (b"c".to_vec(), b"3".to_vec()),
            (b"a".to_vec(), b"2".to_vec()),
        ]
    );
    db.release_snapshot(snap);
}

#[test]
fn revscan_buffer_too_small_does_not_consume() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    block_on(db.put(b"a", b"value-a")).unwrap();
    block_on(db.put(b"b", b"value-b")).unwrap();
    let mut scan = TestRevScan::new(&db);
    block_on(scan.seek_prev(b"", None, u64::MAX)).unwrap();
    let mut kbuf = [0u8; 256];
    let mut tiny = [0u8; 2];
    // Value needs 7 bytes: the error must precede any cursor advance.
    let err = block_on(scan.prev(&mut kbuf, &mut tiny)).unwrap_err();
    assert!(matches!(err, horton::Error::BufferTooSmall { need: 7 }));
    // Retry with room: the same entry comes back, then the scan continues.
    let mut vbuf = [0u8; 1024];
    assert_eq!(
        block_on(scan.prev(&mut kbuf, &mut vbuf)).unwrap(),
        Some((1, 7))
    );
    assert_eq!(&kbuf[..1], b"b");
    assert_eq!(&vbuf[..7], b"value-b");
    assert_eq!(
        block_on(scan.prev(&mut kbuf, &mut vbuf)).unwrap(),
        Some((1, 7))
    );
    assert_eq!(&kbuf[..1], b"a");
    assert_eq!(block_on(scan.prev(&mut kbuf, &mut vbuf)).unwrap(), None);
}

#[test]
fn revscan_reseek_repositions() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    for k in [b"a", b"b", b"c", b"d"] {
        block_on(db.put(k, k)).unwrap();
    }
    flush(&mut db);
    let mut scan = TestRevScan::new(&db);
    block_on(scan.seek_prev(b"c", None, u64::MAX)).unwrap();
    let mut kbuf = [0u8; 256];
    let mut vbuf = [0u8; 1024];
    assert_eq!(
        block_on(scan.prev(&mut kbuf, &mut vbuf)).unwrap(),
        Some((1, 1))
    );
    assert_eq!(&kbuf[..1], b"c");
    // Re-seek to a lower key: the scan restarts there.
    block_on(scan.seek_prev(b"b", None, u64::MAX)).unwrap();
    assert_eq!(
        block_on(scan.prev(&mut kbuf, &mut vbuf)).unwrap(),
        Some((1, 1))
    );
    assert_eq!(&kbuf[..1], b"b");
    assert_eq!(
        block_on(scan.prev(&mut kbuf, &mut vbuf)).unwrap(),
        Some((1, 1))
    );
    assert_eq!(&kbuf[..1], b"a");
    assert_eq!(block_on(scan.prev(&mut kbuf, &mut vbuf)).unwrap(), None);
}

/// Reverse positioning on a version run straddling data blocks. Forty
/// 100-byte versions of `k` force one table's run across two 4 KiB data
/// blocks; seeking at `k` must yield the newest visible version exactly
/// once — including for a watermark that hides the run's newest versions,
/// which live in the run's first block.
#[test]
fn revscan_seek_at_cross_block_version_run() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    block_on(db.put(b"a", b"va")).unwrap();
    let mut seqs = Vec::new();
    for i in 0..40u8 {
        let val = vec![i; 100];
        seqs.push(block_on(db.put(b"k", &val)).unwrap());
    }
    block_on(db.put(b"z", b"vz")).unwrap();
    flush(&mut db);
    // Full reverse scan: `k` appears exactly once, with the newest value.
    let all = rcollect(&db, u64::MAX);
    assert_eq!(all.len(), 3, "z, k, a — k yielded once");
    assert_eq!(all[0].0, b"z");
    assert_eq!(all[1].0, b"k");
    assert_eq!(all[1].1, vec![39u8; 100]);
    assert_eq!(all[2].0, b"a");
    // Seek exactly at `k` with a watermark hiding the run's newest
    // versions: the scan must follow the run into its first block to find
    // the newest visible version, and yield it exactly once.
    let mut scan = TestRevScan::new(&db);
    block_on(scan.seek_prev(b"k", None, seqs[19])).unwrap();
    let mut kbuf = [0u8; 256];
    let mut vbuf = [0u8; 1024];
    let (klen, vlen) = block_on(scan.prev(&mut kbuf, &mut vbuf))
        .unwrap()
        .expect("k must be visible at its own watermark");
    assert_eq!(&kbuf[..klen], b"k");
    assert_eq!(&vbuf[..vlen], &[19u8; 100]);
    // `a` predates the watermark and stays visible; `z` is newer than the
    // watermark and stays hidden. The scan yields `a` then ends — `k`
    // never repeats.
    let (klen, _) = block_on(scan.prev(&mut kbuf, &mut vbuf))
        .unwrap()
        .expect("a must follow k");
    assert_eq!(&kbuf[..klen], b"a");
    assert_eq!(block_on(scan.prev(&mut kbuf, &mut vbuf)).unwrap(), None);
}

/// A version run longer than one restart interval (16 entries): 40
/// versions of `k` in a single memtable flush land in one table with the
/// run spanning several restart points. The reverse scan must still pick
/// the newest visible version, not the first version at/after the
/// binary-searched restart.
#[test]
fn revscan_long_version_run_across_restarts() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    for i in 0..40u8 {
        block_on(db.put(b"k", &[i])).unwrap();
    }
    flush(&mut db);
    let got = rcollect(&db, u64::MAX);
    assert_eq!(got, vec![(b"k".to_vec(), vec![39u8])]);
    // Every watermark sees its own newest version, exactly once.
    for i in (0..40u8).step_by(7) {
        let seq = u64::from(i) + 1;
        let got = rcollect(&db, seq);
        assert_eq!(got, vec![(b"k".to_vec(), vec![i])], "at seq {seq}");
    }
}

#[test]
fn revscan_matches_oracle_under_random_ops() {
    type Oracle = BTreeMap<Vec<u8>, Vec<(u64, Option<Vec<u8>>)>>;
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    let mut rng = Lcg::new(0x5eed);
    let mut oracle: Oracle = BTreeMap::new();
    let mut live_snaps: Vec<u64> = Vec::new();

    // Oracle projection at a snapshot: highest seq <= snap per key,
    // in descending key order.
    let project = |oracle: &Oracle, snap: u64| -> Vec<(Vec<u8>, Vec<u8>)> {
        oracle
            .iter()
            .rev()
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
            let got = rcollect(db, snap);
            let want = project(oracle, snap);
            assert_eq!(got, want, "revscan diverged from oracle at snap {snap}");
        }
    };

    for round in 0..120 {
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
        // Check every round, before any flush: the memtable path must
        // agree with the oracle too (a flush-first check hid a reverse
        // memtable bug that yielded a key's oldest version).
        check(&db, &oracle, &live_snaps);
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

/// Forward and reverse scans agree: reversing the forward range yields the
/// reverse range, under random operations and snapshots.
#[test]
fn revscan_agrees_with_forward_scan() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    let mut rng = Lcg::new(0xbeef);
    for _ in 0..50 {
        let key = vec![b'k', (rng.next() % 12) as u8];
        if rng.next().is_multiple_of(3) {
            block_on(db.delete(&key)).unwrap();
        } else {
            let val = vec![(rng.next() % 251) as u8; 1 + (rng.next() % 10) as usize];
            block_on(db.put(&key, &val)).unwrap();
        }
    }
    flush(&mut db);
    drive(&mut db);
    // Forward collect (mirrors tests/scan.rs).
    let mut fwd = {
        let mut scan =
            horton::Scan::<MemDevice<4096>, 4096, 256, 1024, 64, 4096, 7, 4, 1024, 8>::new(&db);
        block_on(scan.seek(b"", None, u64::MAX)).unwrap();
        let mut out = Vec::new();
        let mut kbuf = [0u8; 256];
        let mut vbuf = [0u8; 1024];
        while let Some((klen, vlen)) = block_on(scan.next(&mut kbuf, &mut vbuf)).unwrap() {
            out.push((kbuf[..klen].to_vec(), vbuf[..vlen].to_vec()));
        }
        out
    };
    let rev = rcollect(&db, u64::MAX);
    fwd.reverse();
    assert_eq!(rev, fwd);
    // Bounded agreement: forward [b, d) reversed == reverse (b, d].
    let mut fwd_bounded = {
        let mut scan =
            horton::Scan::<MemDevice<4096>, 4096, 256, 1024, 64, 4096, 7, 4, 1024, 8>::new(&db);
        block_on(scan.seek(b"k\x05", Some(b"k\x08"), u64::MAX)).unwrap();
        let mut out = Vec::new();
        let mut kbuf = [0u8; 256];
        let mut vbuf = [0u8; 1024];
        while let Some((klen, vlen)) = block_on(scan.next(&mut kbuf, &mut vbuf)).unwrap() {
            out.push((kbuf[..klen].to_vec(), vbuf[..vlen].to_vec()));
        }
        out
    };
    let rev_bounded = rcollect_from(&db, b"k\x08", Some(b"k\x05"), u64::MAX);
    // Forward [k5, k8) reversed is descending; reverse (k5, k8] is
    // descending. Trim both to the open interval (k5, k8): forward keeps
    // k5 but drops k8, reverse keeps k8 but drops k5.
    fwd_bounded.reverse();
    let rev_trimmed: Vec<_> = rev_bounded
        .into_iter()
        .filter(|(k, _)| k.as_slice() < b"k\x08")
        .collect();
    let fwd_trimmed: Vec<_> = fwd_bounded
        .into_iter()
        .filter(|(k, _)| k.as_slice() > b"k\x05")
        .collect();
    assert_eq!(rev_trimmed, fwd_trimmed);
}
