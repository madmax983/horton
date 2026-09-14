//! Compaction (v0.4): L0 -> L1 bounded merge.
//!
//! RED: `Compaction`, `Progress`, and `Db::compact_step` do not exist yet.

use horton::{BlockDevice, Compaction, Error, Manifest, Progress};

mod common;
use common::{block_on, test_config, CrashDevice, MemDevice, TestDb};

/// Caller scratch for `compact_step`, matching the test database shape.
type TestCompaction = Compaction<4096, 256, 1024, 1024>;

fn open<D: BlockDevice>(db: &mut TestDb<D>)
where
    D::Error: std::fmt::Debug,
{
    block_on(db.open()).unwrap();
}

fn get<D: BlockDevice>(db: &TestDb<D>, key: &[u8]) -> Option<Vec<u8>>
where
    D::Error: std::fmt::Debug,
{
    let mut buf = [0u8; 2048];
    block_on(db.get(key, &mut buf))
        .unwrap()
        .map(|n| buf[..n].to_vec())
}

/// Drives compaction to completion.
fn drive<D: BlockDevice>(db: &mut TestDb<D>)
where
    D::Error: std::fmt::Debug,
{
    let mut c = TestCompaction::new();
    while block_on(db.compact_step(&mut c)).unwrap() == Progress::More {}
}

/// Recovers the manifest straight from the device for level inspection.
fn read_manifest(dev: &mut MemDevice<4096>) -> Manifest<7, 4, 256> {
    let mut scratch = [0u8; 4096];
    block_on(Manifest::recover(dev, &mut scratch, 0, 1))
        .unwrap()
        .0
}

#[test]
fn compact_noop_when_l0_not_full() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    // One table: L0 is not full, so compaction must be a no-op.
    block_on(db.put(b"k", b"v")).unwrap();
    block_on(db.flush()).unwrap();
    let mut c = TestCompaction::new();
    assert_eq!(block_on(db.compact_step(&mut c)).unwrap(), Progress::Done);
    let mut dev = db.into_device();
    let man = read_manifest(&mut dev);
    assert_eq!(man.level(0).unwrap().len(), 1);
    assert_eq!(man.level(1).unwrap().len(), 0);
    let mut db = TestDb::new(dev, test_config());
    open(&mut db);
    assert_eq!(get(&db, b"k"), Some(b"v".to_vec()));
}

#[test]
fn compact_drains_l0_into_l1() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    for t in 0..4u8 {
        block_on(db.put(&[t], &[t])).unwrap();
        block_on(db.flush()).unwrap();
    }
    drive(&mut db);
    let mut dev = db.into_device();
    let man = read_manifest(&mut dev);
    assert_eq!(man.level(0).unwrap().len(), 0, "L0 must drain");
    let l1 = man.level(1).unwrap();
    assert_eq!(l1.len(), 1, "one merged L1 table");
    assert_eq!(l1[0].entry_count, 4);
    let mut db = TestDb::new(dev, test_config());
    open(&mut db);
    for t in 0..4u8 {
        assert_eq!(get(&db, &[t]), Some(vec![t]), "key {t} survives");
    }
}

#[test]
fn compact_dedups_highest_sequence_wins() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    // Same two keys in all four tables; later tables carry higher seqs.
    for t in 0..4u8 {
        for k in 0..2u8 {
            block_on(db.put(&[k], &[t, k])).unwrap();
        }
        block_on(db.flush()).unwrap();
    }
    drive(&mut db);
    let mut dev = db.into_device();
    let man = read_manifest(&mut dev);
    let l1 = man.level(1).unwrap();
    assert_eq!(l1.len(), 1);
    assert_eq!(l1[0].entry_count, 2, "one survivor per key");
    assert_eq!(l1[0].max_seq, 8, "last put's sequence");
    let mut db = TestDb::new(dev, test_config());
    open(&mut db);
    // Highest sequence wins: table 3's values (seqs 30, 31).
    assert_eq!(get(&db, &[0]), Some(vec![3, 0]));
    assert_eq!(get(&db, &[1]), Some(vec![3, 1]));
}

#[test]
fn compact_drops_tombstone_at_bottommost() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    block_on(db.put(b"a", b"1")).unwrap();
    block_on(db.put(b"b", b"2")).unwrap();
    block_on(db.put(b"c", b"3")).unwrap();
    block_on(db.delete(b"c")).unwrap();
    block_on(db.flush()).unwrap();
    for t in 1..4u8 {
        block_on(db.put(&[t], &[t])).unwrap();
        block_on(db.flush()).unwrap();
    }
    drive(&mut db);
    let mut dev = db.into_device();
    let man = read_manifest(&mut dev);
    let l1 = man.level(1).unwrap();
    assert_eq!(l1.len(), 1);
    // L1 is the bottommost level holding the range, so the tombstone for
    // "c" is dropped instead of carried down.
    assert_eq!(l1[0].entry_count, 5, "a, b, and one key per filler table");
    let mut db = TestDb::new(dev, test_config());
    open(&mut db);
    assert_eq!(get(&db, b"a"), Some(b"1".to_vec()));
    assert_eq!(get(&db, b"b"), Some(b"2".to_vec()));
    assert_eq!(get(&db, b"c"), None);
}

#[test]
fn compact_reports_bounded_progress() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    // 96 entries x ~117 bytes need 3 output blocks: the merge must take at
    // least two bounded steps (one sealed block per `More`).
    for round in 0..4u8 {
        for i in 0..24u8 {
            let n = round * 24 + i;
            let key = [b'k', b'0' + n / 10, b'0' + n % 10];
            block_on(db.put(&key, &[n; 100])).unwrap();
        }
        block_on(db.flush()).unwrap();
    }
    let mut c = TestCompaction::new();
    let mut steps = 0;
    while block_on(db.compact_step(&mut c)).unwrap() == Progress::More {
        steps += 1;
    }
    assert!(steps >= 2, "expected multiple bounded steps, got {steps}");
    let mut dev = db.into_device();
    let man = read_manifest(&mut dev);
    assert_eq!(man.level(0).unwrap().len(), 0);
    assert_eq!(man.level(1).unwrap()[0].entry_count, 96);
    let mut db = TestDb::new(dev, test_config());
    open(&mut db);
    for n in 0..96u8 {
        let key = [b'k', b'0' + n / 10, b'0' + n % 10];
        assert_eq!(get(&db, &key), Some(vec![n; 100]));
    }
}

#[test]
fn compact_merges_overlapping_l1() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    // Seed L1 with an older table covering [m-r]: four L0 tables so the
    // first drive actually has a full L0 to compact.
    for _ in 0..4u8 {
        for k in *b"mnopqr" {
            block_on(db.put(&[k], b"old")).unwrap();
        }
        block_on(db.flush()).unwrap();
    }
    drive(&mut db); // L0 -> L1, now one L1 table exists
                    // Four overlapping L0 tables covering [n-s], newer.
    for t in 0..4u8 {
        for k in *b"nopqrs" {
            block_on(db.put(&[k], &[t])).unwrap();
        }
        block_on(db.flush()).unwrap();
    }
    drive(&mut db);
    let mut dev = db.into_device();
    let man = read_manifest(&mut dev);
    assert_eq!(man.level(0).unwrap().len(), 0);
    let l1 = man.level(1).unwrap();
    assert_eq!(l1.len(), 1, "L1 tables merge into one run");
    assert_eq!(l1[0].entry_count, 7, "m through s");
    let mut db = TestDb::new(dev, test_config());
    open(&mut db);
    // Newest wins per key: L0's table 3 over the seeded L1 table.
    assert_eq!(get(&db, b"m"), Some(b"old".to_vec()));
    for k in *b"nopqr" {
        assert_eq!(get(&db, &[k]), Some(vec![3]));
    }
    assert_eq!(get(&db, b"s"), Some(vec![3]));
}

#[test]
fn compact_l1_full_returns_nospace() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    // Fill L1 to capacity with four compactions of disjoint... actually
    // overlapping compactions merge, so seed via direct manifest-free
    // route: four L0->L1 rounds each carrying disjoint key ranges would
    // still merge per round. Instead fill L0, compact, repeat with
    // disjoint ranges — each round emits one L1 table.
    for round in 0..4u8 {
        for t in 0..4u8 {
            let base = round * 4 + t;
            block_on(db.put(&[base], b"v")).unwrap();
            block_on(db.flush()).unwrap();
        }
        drive(&mut db);
    }
    let mut dev = db.into_device();
    let man = read_manifest(&mut dev);
    assert_eq!(man.level(1).unwrap().len(), 4, "L1 is full");
    let mut db = TestDb::new(dev, test_config());
    open(&mut db);
    // One more full L0: the L1 output has nowhere to go.
    for t in 0..4u8 {
        block_on(db.put(&[0x80 + t], b"v")).unwrap();
        block_on(db.flush()).unwrap();
    }
    let mut c = TestCompaction::new();
    // First steps merge fine; the commit fails on the full level.
    let mut saw_nospace = false;
    loop {
        match block_on(db.compact_step(&mut c)) {
            Ok(Progress::More) => {}
            Ok(Progress::Done) => break,
            Err(Error::NoSpace) => {
                saw_nospace = true;
                break;
            }
            Err(e) => panic!("unexpected compaction error: {e:?}"),
        }
    }
    assert!(saw_nospace, "L1-full compaction must fail cleanly");
    // Failed commit leaves the manifest untouched: L0 still full, L1 intact.
    let mut dev = db.into_device();
    let man = read_manifest(&mut dev);
    assert_eq!(man.level(0).unwrap().len(), 4);
    assert_eq!(man.level(1).unwrap().len(), 4);
}

#[test]
fn compact_unblocks_flush() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    for t in 0..4u8 {
        block_on(db.put(&[t], b"v")).unwrap();
        block_on(db.flush()).unwrap();
    }
    block_on(db.put(b"x", b"v")).unwrap();
    assert!(matches!(block_on(db.flush()).unwrap_err(), Error::NoSpace));
    drive(&mut db);
    block_on(db.flush()).unwrap();
    assert_eq!(get(&db, b"x"), Some(b"v".to_vec()));
    for i in 0..4u8 {
        assert_eq!(get(&db, &[i]), Some(b"v".to_vec()));
    }
}

/// Counts block writes; everything else passes through.
struct CountDevice<D> {
    inner: D,
    writes: usize,
}

impl<D> CountDevice<D> {
    const fn new(inner: D) -> Self {
        Self { inner, writes: 0 }
    }
}

impl<D: BlockDevice> BlockDevice for CountDevice<D> {
    type Error = D::Error;
    const BLOCK: usize = D::BLOCK;

    fn poll_read_block(
        &self,
        cx: &mut std::task::Context<'_>,
        id: u64,
        buf: &mut [u8],
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_read_block(cx, id, buf)
    }

    fn poll_write_block(
        &mut self,
        cx: &mut std::task::Context<'_>,
        id: u64,
        buf: &[u8],
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.writes += 1;
        self.inner.poll_write_block(cx, id, buf)
    }

    fn poll_flush(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_flush(cx)
    }
}

#[test]
fn compact_crash_never_mixes_state() {
    fn build() -> TestDb<CountDevice<MemDevice<4096>>> {
        let mut db = TestDb::new(CountDevice::new(MemDevice::<4096>::new()), test_config());
        open(&mut db);
        for t in 0..4u8 {
            for k in 0..2u8 {
                block_on(db.put(&[t, k], &[t, k])).unwrap();
            }
            block_on(db.flush()).unwrap();
        }
        db
    }
    // Writes of the setup alone, so the crash loop covers exactly the
    // compaction's writes (setup crashes are the flush injector's job).
    let setup_writes = {
        let db = build();
        db.into_device().writes
    };
    // Writes of a clean setup + compaction.
    let total_writes = {
        let mut db = build();
        drive(&mut db);
        db.into_device().writes
    };
    assert!(total_writes > setup_writes);

    for crash_at in setup_writes..total_writes {
        // Build the pre-compaction state on a plain device.
        let db = build();
        let dev = db.into_device().inner;
        // Crash at write `crash_at` of the compaction (suffix-drop model:
        // writes that "succeed" never land, like power loss).
        let mut db = TestDb::new(CrashDevice::<_, 4096>::new(dev, crash_at), test_config());
        open(&mut db);
        drive(&mut db);
        let dev = db.into_device().into_inner();
        // Reopen on the bare device: orphans are swept, state is exactly
        // pre- or post-compaction, and a clean compaction converges.
        let mut db = TestDb::new(dev, test_config());
        let rep = block_on(db.open()).unwrap();
        assert!(
            rep.l0_tables == 4 || rep.l0_tables == 0,
            "crash_at={crash_at}"
        );
        drive(&mut db);
        for t in 0..4u8 {
            for k in 0..2u8 {
                assert_eq!(get(&db, &[t, k]), Some(vec![t, k]), "crash_at={crash_at}");
            }
        }
        let mut dev = db.into_device();
        let man = read_manifest(&mut dev);
        assert_eq!(man.level(0).unwrap().len(), 0, "crash_at={crash_at}");
        assert_eq!(man.level(1).unwrap().len(), 1, "crash_at={crash_at}");
    }
}

#[test]
fn compact_reclaims_input_blocks_for_reuse() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    for t in 0..4u8 {
        block_on(db.put(&[t], &[t])).unwrap();
        block_on(db.flush()).unwrap();
    }
    // Record the L0 input runs before compaction.
    let mut dev = db.into_device();
    let man = read_manifest(&mut dev);
    let l0 = man.level(0).unwrap();
    assert_eq!(l0.len(), 4);
    let b0 = l0[0].first_block;
    let mut db = TestDb::new(dev, test_config());
    open(&mut db);
    drive(&mut db);
    // A post-compaction flush must reuse the reclaimed input blocks
    // (free-list-first allocation), not fresh bump blocks.
    block_on(db.put(b"new", b"v")).unwrap();
    block_on(db.flush()).unwrap();
    let mut dev = db.into_device();
    let man = read_manifest(&mut dev);
    let l0 = man.level(0).unwrap();
    assert_eq!(l0.len(), 1);
    assert_eq!(
        l0[0].first_block, b0,
        "flushed table must reuse the first reclaimed input run"
    );
}

#[test]
fn compact_reclaims_inputs_when_output_is_empty() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    // Four L0 tables of pure tombstones. L1 is the bottommost level holding
    // the range, so every tombstone is dropped: no output table is written,
    // but the input blocks must still be reclaimed.
    for t in 0..4u8 {
        block_on(db.put(&[t], &[t])).unwrap();
        block_on(db.delete(&[t])).unwrap();
        block_on(db.flush()).unwrap();
    }
    let mut dev = db.into_device();
    let man = read_manifest(&mut dev);
    let l0 = man.level(0).unwrap();
    assert_eq!(l0.len(), 4);
    let b0 = l0[0].first_block;
    let mut db = TestDb::new(dev, test_config());
    open(&mut db);
    drive(&mut db);
    // No reopen in between: the live session's free list must already hold
    // the input runs. A fresh flush must land on the reclaimed blocks.
    block_on(db.put(b"new", b"v")).unwrap();
    block_on(db.flush()).unwrap();
    let mut dev = db.into_device();
    let man = read_manifest(&mut dev);
    assert_eq!(
        man.level(1).unwrap().len(),
        0,
        "no output table was written"
    );
    let l0 = man.level(0).unwrap();
    assert_eq!(l0.len(), 1, "only the fresh table remains in L0");
    assert_eq!(
        l0[0].first_block, b0,
        "flushed table must reuse the reclaimed input run"
    );
}

/// Differential: the compaction keep-set must preserve exactly what the
/// model predicts. Builds known version chains, pins snapshots at chosen
/// watermarks, compacts with the snapshots live, then checks every
/// snapshot's (and the live view's) visible value per key against
/// [`model_winner`](horton::model::model_winner) — the model's analogue of
/// `get_at`. Any version the merge wrongly drops (or keeps wrongly visible)
/// diverges here.
#[test]
#[allow(clippy::similar_names)] // s_a1/s_a2/... are intentionally parallel: seq of key X put N.
fn compact_keep_set_matches_model() {
    use horton::model::{model_keep_set, model_winner, Version};

    type Chain<'a> = (&'a [u8], &'a [(u64, bool, &'a [u8])]);
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    // Key "a": three values. Key "b": a value then a tombstone. Key "c":
    // a single value after the first snapshot.
    let s_a1 = block_on(db.put(b"a", b"a1")).unwrap();
    let s_a2 = block_on(db.put(b"a", b"a2")).unwrap();
    let snap1 = db.snapshot().unwrap(); // observes a = a2, b and c absent
    let s_a3 = block_on(db.put(b"a", b"a3")).unwrap();
    let s_b1 = block_on(db.put(b"b", b"b1")).unwrap();
    let s_b2 = block_on(db.delete(b"b")).unwrap();
    let s_c1 = block_on(db.put(b"c", b"c1")).unwrap();
    let snap2 = db.snapshot().unwrap(); // observes a = a3, b deleted, c = c1
                                        // Spread the versions across four L0 tables so the merge actually runs.
    block_on(db.flush()).unwrap();
    for t in 0..3u8 {
        block_on(db.put(&[b'x', t], &[t])).unwrap();
        block_on(db.flush()).unwrap();
    }
    // L0 is full: this drive compacts with both snapshots live.
    drive(&mut db);

    // The model's version chains, newest-first, with their values.
    let chains: &[Chain<'_>] = &[
        (
            b"a",
            &[
                (s_a3, false, b"a3".as_slice()),
                (s_a2, false, b"a2".as_slice()),
                (s_a1, false, b"a1".as_slice()),
            ],
        ),
        (
            b"b",
            &[
                (s_b2, true, b"".as_slice()),
                (s_b1, false, b"b1".as_slice()),
            ],
        ),
        (b"c", &[(s_c1, false, b"c1".as_slice())]),
    ];
    let mut buf = [0u8; 1024];
    // Full-sequence sweep against the MODEL'S KEEP-SET chains: every
    // `max_seq` from 0 through the last write must agree with
    // `model_winner` over exactly the versions `model_keep_set` retains.
    // (Versions below the oldest live snapshot are droppable by design, so
    // the oracle is the keep-set — not the full pre-compaction history.
    // The live view is `u64::MAX`, so every KEPT version is observable at
    // some watermark; a merge that keeps too much or too little shows up
    // as a divergence at that version's own seq.)
    let max_th = s_c1;
    // Watermarks descending, as the merge holds them; both snapshots are
    // live, so L1 is bottommost and the oldest watermark is `snap1`.
    let snaps = [snap2, snap1];
    for (key, chain) in chains {
        let vs: [Version; 3] = {
            let mut vs = [Version {
                seq: 0,
                tombstone: false,
            }; 3];
            for (i, (seq, tombstone, _)) in chain.iter().enumerate() {
                vs[i] = Version {
                    seq: *seq,
                    tombstone: *tombstone,
                };
            }
            vs
        };
        let vs = &vs[..chain.len()];
        let (kept_idx, n) = model_keep_set(vs, &snaps, true, snap1);
        let kept: Vec<(Version, &[u8])> =
            kept_idx[..n].iter().map(|&i| (vs[i], chain[i].2)).collect();
        let kept_vs: Vec<Version> = kept.iter().map(|k| k.0).collect();
        // The keep-set always covers the protected thresholds: each live
        // view/snapshot sees exactly what it saw before the merge.
        for th in [u64::MAX, snap2, snap1] {
            let before = model_winner(vs, th);
            let after = model_winner(&kept_vs, th);
            assert_eq!(
                before.map(|i| vs[i].seq),
                after.map(|i| kept_vs[i].seq),
                "key {key:?}: keep-set must cover protected threshold {th}"
            );
        }
        let mut th = 0u64;
        loop {
            let want = model_winner(&kept_vs, th).and_then(|i| {
                if kept[i].0.tombstone {
                    None
                } else {
                    Some(kept[i].1)
                }
            });
            let got = block_on(db.get_at(key, &mut buf, th)).unwrap();
            match want {
                None => assert_eq!(got, None, "key {key:?} at max_seq {th}: model says absent"),
                Some(v) => {
                    let n = got
                        .unwrap_or_else(|| panic!("key {key:?} at max_seq {th}: model says {v:?}"));
                    assert_eq!(
                        &buf[..n],
                        v,
                        "key {key:?} at max_seq {th} diverged from model"
                    );
                }
            }
            if th >= max_th {
                break;
            }
            th += 1;
        }
    }
    db.release_snapshot(snap1);
    db.release_snapshot(snap2);
}

/// Differential keep-set, part 2: no live snapshots, plus duplicate
/// watermarks (two snapshots with no writes between them share one
/// watermark). With no snapshots the merge keeps only the newest version
/// per key, and a bottommost tombstone drops its whole key.
#[test]
#[allow(clippy::similar_names)] // s_d1/s_d2/... are intentionally parallel: seq of key X put N.
fn compact_keep_set_no_snapshots_and_duplicate_watermarks() {
    use horton::model::{model_keep_set, model_winner, Version};

    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    // Key "d": value, value, tombstone — newest is a bottommost tombstone
    // with no live snapshots: the whole key must vanish.
    let s_d1 = block_on(db.put(b"d", b"d1")).unwrap();
    let s_d2 = block_on(db.put(b"d", b"d2")).unwrap();
    let s_d3 = block_on(db.delete(b"d")).unwrap();
    // Key "e": two values around a duplicated watermark pair.
    let s_e1 = block_on(db.put(b"e", b"e1")).unwrap();
    let snap_a = db.snapshot().unwrap();
    let snap_b = db.snapshot().unwrap(); // no writes between: same watermark
    assert_eq!(snap_a, snap_b, "back-to-back snapshots share a watermark");
    let s_e2 = block_on(db.put(b"e", b"e2")).unwrap();
    // Flush d/e into L0, then fill L0 with three more tables so `drive`
    // actually compacts.
    block_on(db.flush()).unwrap();
    for t in 0..3u8 {
        block_on(db.put(&[b'x', t], &[t])).unwrap();
        block_on(db.flush()).unwrap();
    }
    db.release_snapshot(snap_a);
    db.release_snapshot(snap_b);
    // No live snapshots during the compaction.
    drive(&mut db);

    // The model agrees: no snapshots, bottommost, tombstone predates the
    // (empty) snapshot set → the keep-set is empty. The snapshots were
    // released before the merge, so their watermarks protect nothing: even
    // `get_at` at the dropped versions' own seqs must stay absent.
    let d_vs = [
        Version {
            seq: s_d3,
            tombstone: true,
        },
        Version {
            seq: s_d2,
            tombstone: false,
        },
        Version {
            seq: s_d1,
            tombstone: false,
        },
    ];
    let (kept_idx, n) = model_keep_set(&d_vs, &[], true, u64::MAX);
    assert_eq!(n, 0, "model must drop the whole key");
    assert_eq!(kept_idx[..n], []);
    let mut buf = [0u8; 1024];
    assert_eq!(block_on(db.get(b"d", &mut buf)).unwrap(), None);
    for th in [0u64, s_d1, s_d2, s_d3] {
        assert_eq!(
            block_on(db.get_at(b"d", &mut buf, th)).unwrap(),
            None,
            "d at {th}: dropped tombstone must stay dropped"
        );
    }

    // Key "e": newest-only retention — the keep-set oracle, not the full
    // chain: `s_e1` is unobservable to every retained view (no live
    // snapshots) and the merge drops it.
    let e_vs = [
        Version {
            seq: s_e2,
            tombstone: false,
        },
        Version {
            seq: s_e1,
            tombstone: false,
        },
    ];
    let (kept_idx, n) = model_keep_set(&e_vs, &[], true, u64::MAX);
    assert_eq!(&kept_idx[..n], &[0], "only the newest version survives");
    let kept_vs = [e_vs[kept_idx[0]]];
    let mut th = 0u64;
    loop {
        let want = model_winner(&kept_vs, th).map(|i| {
            assert!(!kept_vs[i].tombstone);
            b"e2".as_slice()
        });
        let got = block_on(db.get_at(b"e", &mut buf, th)).unwrap();
        match want {
            None => assert_eq!(got, None, "e at {th}: model says absent"),
            Some(v) => {
                let n = got.unwrap_or_else(|| panic!("e at {th}: model says {v:?}"));
                assert_eq!(&buf[..n], v, "e at {th} diverged from model");
            }
        }
        if th >= s_e2 {
            break;
        }
        th += 1;
    }
    // The live view agrees with the model.
    assert_eq!(
        block_on(db.get(b"e", &mut buf))
            .unwrap()
            .map(|n| buf[..n].to_vec()),
        Some(b"e2".to_vec())
    );
}
