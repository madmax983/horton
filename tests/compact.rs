//! Compaction (v0.8): bounded merge down every level.
//!
//! v0.4 RED: `Compaction`, `Progress`, and `Db::compact_step` did not exist.
//! v0.8 RED: `compact_select` only compacts L0 -> L1; deeper levels are not
//! selected, so a full L1 fails the job (then `Error::NoSpace`, now
//! `Error::RegionFull`).

use horton::{BlockDevice, Compaction, Error, Manifest, Progress};

mod common;
use common::{CrashDevice, MemDevice, TestDb, block_on, test_config, tight_config};

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

/// Drives compaction jobs until none is selectable. (`compact_step`
/// reports `Done` both when a job finishes and when idle, so a single
/// `while ... == More` loop only ever runs one job; the
/// [`compaction_pending`](horton::Db::compaction_pending) query closes
/// the loop honestly.)
fn drain<D: BlockDevice>(db: &mut TestDb<D>)
where
    D::Error: std::fmt::Debug,
{
    while db.compaction_pending() {
        drive_one(db);
    }
}

/// A 1000-byte value tagged `tag`. Under `tight_config` four flushes of
/// three such values make an L1 table of three data blocks: three quarters
/// of an 8-block slot, so compaction keeps it whole instead of
/// consolidating it with a neighbour.
fn big(tag: u8) -> Vec<u8> {
    vec![tag; 1000]
}

/// One L0 round under `tight_config`: four flushes of three 1000-byte
/// values each, keys `prefix ++ [flush * 3 + j]`.
fn big_round<D: BlockDevice>(db: &mut TestDb<D>, prefix: &[u8], tag: u8)
where
    D::Error: std::fmt::Debug,
{
    for f in 0..4u8 {
        for j in 0..3u8 {
            let mut k = prefix.to_vec();
            k.push(f * 3 + j);
            block_on(db.put(&k, &big(tag))).unwrap();
        }
        block_on(db.flush()).unwrap();
    }
}

/// Recovers the manifest straight from the device for level inspection.
fn read_manifest(dev: &mut MemDevice<4096>) -> Manifest<7, 4, 256> {
    let mut scratch = [0u8; 4096];
    block_on(Manifest::recover(dev, &mut scratch, 0, 4))
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
    drain(&mut db);
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
    drain(&mut db);
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
    drain(&mut db);
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
    drain(&mut db); // L0 -> L1, now one L1 table exists
    // Four overlapping L0 tables covering [n-s], newer.
    for t in 0..4u8 {
        for k in *b"nopqrs" {
            block_on(db.put(&[k], &[t])).unwrap();
        }
        block_on(db.flush()).unwrap();
    }
    drain(&mut db);
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
fn compact_l1_full_drains_to_l2() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), tight_config());
    open(&mut db);
    // Fill L1 to capacity: four L0->L1 rounds of disjoint keys, each
    // emitting exactly one L1 table.
    for round in 0..4u8 {
        big_round(&mut db, &[round], round);
        // Single jobs only: let L1 accumulate to full instead of draining it.
        drive_one(&mut db);
    }
    let mut dev = db.into_device();
    let man = read_manifest(&mut dev);
    assert_eq!(man.level(1).unwrap().len(), 4, "L1 is full");
    let mut db = TestDb::new(dev, tight_config());
    open(&mut db);
    // One more full L0: deepest-first selection must pick L1->L2, not fail.
    // Nothing in L2 overlaps the lowest L1 table and it fills three
    // quarters of its slot, so it moves down whole.
    for t in 0..4u8 {
        block_on(db.put(&[0x80 + t], b"v")).unwrap();
        block_on(db.flush()).unwrap();
    }
    drive_one(&mut db);
    let mut dev = db.into_device();
    let man = read_manifest(&mut dev);
    assert_eq!(man.level(0).unwrap().len(), 4, "L0 untouched by L1->L2");
    assert_eq!(man.level(1).unwrap().len(), 3, "L1 drained by one table");
    let l2 = man.level(2).unwrap();
    assert_eq!(l2.len(), 1, "L2 gained the moved table");
    assert_eq!(
        l2[0].first_key.as_slice(),
        &[0, 0],
        "lowest L1 table drained first"
    );
    // The remaining job still works: L0->L1 lands on the drained level.
    let mut db = TestDb::new(dev, tight_config());
    open(&mut db);
    drain(&mut db);
    let mut dev = db.into_device();
    let man = read_manifest(&mut dev);
    assert_eq!(man.level(0).unwrap().len(), 0, "L0 drains");
    // Deepest-first keeps pulling: the refilled L1 drains straight into L2.
    assert_eq!(man.level(1).unwrap().len(), 3, "L1 drained again");
    assert_eq!(man.level(2).unwrap().len(), 2, "L2 keeps both tables");
    let mut db = TestDb::new(dev, tight_config());
    open(&mut db);
    for round in 0..4u8 {
        for k in 0..12u8 {
            assert_eq!(get(&db, &[round, k]), Some(big(round)), "key {round}/{k}");
        }
    }
    for t in 0..4u8 {
        assert_eq!(
            get(&db, &[0x80 + t]),
            Some(b"v".to_vec()),
            "key {t} survives"
        );
    }
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
    assert!(matches!(
        block_on(db.flush()).unwrap_err(),
        Error::NeedsCompaction
    ));
    drain(&mut db);
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
        drain(&mut db);
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
        drain(&mut db);
        let dev = db.into_device().into_inner();
        // Reopen on the bare device: orphans are swept, state is exactly
        // pre- or post-compaction, and a clean compaction converges.
        let mut db = TestDb::new(dev, test_config());
        let rep = block_on(db.open()).unwrap();
        assert!(
            rep.l0_tables == 4 || rep.l0_tables == 0,
            "crash_at={crash_at}"
        );
        drain(&mut db);
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
    let slots = db.slot_stats().slots;
    assert_eq!(slots, 28, "7 levels x 4 tables");
    for t in 0..4u8 {
        block_on(db.put(&[t], &[t])).unwrap();
        block_on(db.flush()).unwrap();
    }
    assert_eq!(db.slot_stats().used, 4);
    drain(&mut db);
    // The four inputs' slots are free again; only the output holds one.
    let s = db.slot_stats();
    assert_eq!((s.used, s.reserved, s.free), (1, 0, slots - 1));
    assert_eq!(db.check_invariants(), Ok(()));
    // Reuse for real: overwriting the same keys, far more tables than the
    // region has slots pass through it, so freed slots must be handed out
    // again (each job merges L0 into the one L1 table).
    for round in 0..40u8 {
        for t in 0..4u8 {
            block_on(db.put(&[b'r', t], &[round, t])).unwrap();
            block_on(db.flush()).unwrap();
        }
        drain(&mut db);
        assert_eq!(db.check_invariants(), Ok(()));
    }
    assert!(db.slot_stats().used < slots);
    for t in 0..4u8 {
        assert_eq!(get(&db, &[b'r', t]), Some(vec![39, t]));
    }
}

#[test]
fn compact_reclaims_inputs_when_output_is_empty() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    // Four L0 tables of pure tombstones. L1 is the bottommost level holding
    // the range, so every tombstone is dropped: no output table is written,
    // and the reserved output slot is released along with the inputs'.
    for t in 0..4u8 {
        block_on(db.put(&[t], &[t])).unwrap();
        block_on(db.delete(&[t])).unwrap();
        block_on(db.flush()).unwrap();
    }
    assert_eq!(db.slot_stats().used, 4);
    drain(&mut db);
    let s = db.slot_stats();
    assert_eq!((s.used, s.reserved), (0, 0), "no output, no inputs left");
    assert_eq!(s.free, s.slots);
    assert_eq!(db.check_invariants(), Ok(()));
    let mut dev = db.into_device();
    let man = read_manifest(&mut dev);
    assert_eq!(
        man.level(1).unwrap().len(),
        0,
        "no output table was written"
    );
    assert_eq!(man.level(0).unwrap().len(), 0);
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
    use horton::model::{Version, model_keep_set, model_winner};

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
    drain(&mut db);

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
    use horton::model::{Version, model_keep_set, model_winner};

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
    drain(&mut db);

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

#[test]
fn compact_cascades_down_every_level() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), tight_config());
    open(&mut db);
    // Nine rounds of disjoint keys, draining after each. Rounds 0-3 fill
    // L1, 4-6 fill L2, 7 fills L2 fully, and round 8 forces an L2->L3
    // job. Every round's table holds its twelve keys.
    for round in 0..9u8 {
        big_round(&mut db, &[round], round);
        drain(&mut db);
    }
    let mut dev = db.into_device();
    let man = read_manifest(&mut dev);
    assert_eq!(man.level(0).unwrap().len(), 0, "L0 drains");
    // Every level respects its table budget; deepest-first draining keeps
    // the stack balanced rather than piled up.
    for lvl in 0..7usize {
        assert!(
            man.level(lvl).unwrap().len() <= 4,
            "level {lvl} respects TABLES"
        );
    }
    let l3 = man.level(3).unwrap();
    assert!(!l3.is_empty(), "cascade reached L3 via L2->L3");
    assert_eq!(
        l3[0].first_key.as_slice(),
        &[0, 0],
        "lowest (here also oldest) drains first"
    );
    assert_eq!(l3[0].entry_count, 12);
    // Levels >= 1 never hold overlapping tables.
    for lvl in 1..4usize {
        let tables = man.level(lvl).unwrap();
        for (i, a) in tables.iter().enumerate() {
            for b in tables.iter().skip(i + 1) {
                let overlap = a.first_key.as_slice() <= b.last_key.as_slice()
                    && b.first_key.as_slice() <= a.last_key.as_slice();
                assert!(!overlap, "level {lvl} tables overlap");
            }
        }
    }
    let mut db = TestDb::new(dev, tight_config());
    open(&mut db);
    for round in 0..9u8 {
        for k in 0..12u8 {
            assert_eq!(get(&db, &[round, k]), Some(big(round)), "key {round}/{k}");
        }
    }
}

#[test]
fn compact_cascade_keeps_snapshot_versions() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), tight_config());
    open(&mut db);
    // v0 of keys 0..6, then a snapshot pins them while v1 overwrites: the
    // L1 table ends up with twelve entries — each key's live and snapshot
    // version — in three data blocks.
    for _ in 0..2u8 {
        for half in [0..3u8, 3..6u8] {
            for k in half {
                block_on(db.put(&[k], &big(k))).unwrap();
            }
            block_on(db.flush()).unwrap();
        }
    }
    drain(&mut db);
    let snap = db.snapshot().unwrap();
    for _ in 0..2u8 {
        for half in [0..3u8, 3..6u8] {
            for k in half {
                block_on(db.put(&[k], &big(k + 100))).unwrap();
            }
            block_on(db.flush()).unwrap();
        }
    }
    drain(&mut db);
    // Three filler rounds push the versioned table down: L1 fills, so the
    // next drive must run L1->L2 on it.
    for r in 1..4u8 {
        big_round(&mut db, &[10 * r], r);
        drain(&mut db);
    }
    let mut dev = db.into_device();
    let man = read_manifest(&mut dev);
    assert_eq!(man.level(1).unwrap().len(), 3, "L1 drained one table");
    let l2 = man.level(2).unwrap();
    assert_eq!(l2.len(), 1, "L2 holds the cascaded table");
    assert_eq!(l2[0].entry_count, 12, "live + snapshot version per key");
    let mut db = TestDb::new(dev, tight_config());
    open(&mut db);
    let mut buf = [0u8; 2048];
    for k in 0..6u8 {
        // Live view sees the newest version.
        assert_eq!(get(&db, &[k]), Some(big(k + 100)), "live newest wins");
        // The snapshot still sees the version it pinned.
        let n = block_on(db.get_at(&[k], &mut buf, snap)).unwrap().unwrap();
        assert_eq!(&buf[..n], &big(k)[..], "snapshot version survives L1->L2");
    }
}

#[test]
fn compact_l1_to_l2_crash_never_mixes_state() {
    fn build() -> TestDb<CountDevice<MemDevice<4096>>> {
        let mut db = TestDb::new(CountDevice::new(MemDevice::<4096>::new()), tight_config());
        open(&mut db);
        // Four L0->L1 rounds: L1 ends full, L0 empty, 48 keys total.
        for r in 0..4u8 {
            big_round(&mut db, &[r], r);
            // Single jobs only: L1 must sit full for the crash campaign.
            drive_one(&mut db);
        }
        db
    }
    // Writes of the setup alone, so the crash loop covers exactly the
    // L1->L2 job's writes (setup crashes are the flush injector's job).
    let setup_writes = {
        let db = build();
        db.into_device().writes
    };
    // Writes of a clean setup + the L1->L2 compaction (here a move: the
    // lowest L1 table overlaps nothing below, one manifest write).
    let total_writes = {
        let mut db = build();
        drain(&mut db);
        db.into_device().writes
    };
    assert!(total_writes > setup_writes);

    for crash_at in setup_writes..total_writes {
        // Build the pre-compaction state on a plain device.
        let db = build();
        let dev = db.into_device().inner;
        // Crash at write `crash_at` of the L1->L2 job (suffix-drop model).
        let mut db = TestDb::new(CrashDevice::<_, 4096>::new(dev, crash_at), tight_config());
        open(&mut db);
        drain(&mut db);
        let dev = db.into_device().into_inner();
        // Reopen on the bare device: state is exactly pre- or
        // post-compaction, and a clean compaction converges.
        let mut db = TestDb::new(dev, tight_config());
        block_on(db.open()).unwrap();
        let mut dev = db.into_device();
        let man = read_manifest(&mut dev);
        let l1 = man.level(1).unwrap().len();
        let l2 = man.level(2).unwrap().len();
        assert!(
            (l1, l2) == (4, 0) || (l1, l2) == (3, 1),
            "crash_at={crash_at}: exactly pre- or post-compaction"
        );
        let mut db = TestDb::new(dev, tight_config());
        open(&mut db);
        drain(&mut db);
        for r in 0..4u8 {
            for k in 0..12u8 {
                assert_eq!(get(&db, &[r, k]), Some(big(r)), "crash_at={crash_at}");
            }
        }
        let mut dev = db.into_device();
        let man = read_manifest(&mut dev);
        assert_eq!(man.level(1).unwrap().len(), 3, "crash_at={crash_at}");
        assert_eq!(man.level(2).unwrap().len(), 1, "crash_at={crash_at}");
    }
}

/// A narrow database: 3 levels, 2 tables per level — 6 table slots.
type SmallDb<D> = horton::Db<D, 4096, 256, 1024, 64, 4096, 3, 2, 1024, 8>;

/// 6 slots of 8 blocks: a handful of 1000-byte values fills one.
const fn small_config() -> horton::Config {
    horton::Config::new(8, 136, 136, 136 + 6 * 8, 0, 1)
}

/// Per-level table caps no longer bound the tree: the levels fill until
/// the *region* is used up — every slot but the compaction reserve holds a
/// table — and flush then refuses cleanly. (Before pooled levels and slot
/// allocation, a bottom level holding `TABLES` disjoint tables was a
/// permanent refusal at select time — the "honest ceiling" — reached
/// after a handful of flushes, with most of the region unused.) Near the
/// end flush reaches the reserve while L0 is only partly full; region
/// pressure then selects an L0 merge, which is what lets the last slots
/// fill.
#[test]
fn compact_fills_the_region_not_a_level() {
    let mut db = SmallDb::new(MemDevice::<4096>::new(), small_config());
    block_on(db.open()).unwrap();
    let mut c = TestCompaction::new();
    let mut written: Vec<u16> = Vec::new();
    let mut refused = None;
    'fill: for k in 0u16..2000 {
        block_on(db.put(&k.to_be_bytes(), &big(u8::try_from(k % 251).unwrap()))).unwrap();
        // Four values per flush.
        if k % 4 != 3 {
            written.push(k);
            continue;
        }
        loop {
            match block_on(db.flush()) {
                Ok(()) => break,
                Err(Error::NeedsCompaction) => {
                    while block_on(db.compact_step(&mut c)).unwrap() == Progress::More {}
                }
                Err(Error::RegionFull) => {
                    refused = Some(k);
                    break 'fill;
                }
                Err(e) => panic!("flush: {e:?}"),
            }
        }
        written.push(k);
        while db.compaction_pending() {
            while block_on(db.compact_step(&mut c)).unwrap() == Progress::More {}
        }
        assert_eq!(db.check_invariants(), Ok(()));
    }
    let refused = refused.expect("the region never filled");
    let s = db.slot_stats();
    assert_eq!(
        s.used,
        s.slots - 2,
        "every slot but the reserve holds a table"
    );
    assert!(written.len() >= 40, "only {} values fit", written.len());
    // Everything survives, across a reopen too — including the values
    // whose flush was refused (they are still in the memtable and the WAL).
    let mut db = SmallDb::new(db.into_device(), small_config());
    block_on(db.open()).unwrap();
    assert_eq!(db.check_invariants(), Ok(()));
    let mut buf = [0u8; 2048];
    for k in written.into_iter().chain(core::iter::once(refused)) {
        let n = block_on(db.get(&k.to_be_bytes(), &mut buf))
            .unwrap()
            .unwrap();
        assert_eq!(
            &buf[..n],
            &big(u8::try_from(k % 251).unwrap())[..],
            "key {k}"
        );
    }
}
