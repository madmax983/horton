//! The drainer against a model (issue #48, item 1).
//!
//! Writers publish trickles and bursts of puts, TTL puts, deletes and
//! range deletes. The host sweeps, flushes on `Stalled` and compacts on
//! `NeedsCompaction`, across WAL wraps and reopens. After each epoch
//! every key reads back as a `BTreeMap` model says.

#![cfg(feature = "multiwriter")]

mod common;

use common::{Lcg, MemDevice, block_on};
use core::sync::atomic::{AtomicU32, Ordering};
use horton::drainer::{Drainer, SweepOutcome, payload};
use horton::ring::Ring;
use std::collections::BTreeMap;

/// The read clock. TTL puts expire around it.
const NOW: u64 = 1_000;

/// Keys `k00` to `k31`.
const KEYS: usize = 32;

/// The value and the expiry tick that the model keeps for a key.
type Model = BTreeMap<Vec<u8>, (Vec<u8>, u64)>;

fn key(i: usize) -> Vec<u8> {
    format!("k{i:02}").into_bytes()
}

/// Makes one random payload and applies it to the model.
fn random_op(rng: &mut Lcg, model: &mut Model) -> [u8; 32] {
    let k = key(rng.next_bounded(KEYS));
    match rng.next_bounded(10) {
        0..=4 => {
            let v = vec![
                b'a' + u8::try_from(rng.next_bounded(26)).expect("small");
                rng.next_bounded(21)
            ];
            let p = payload::encode_put(&k, &v).expect("fits");
            model.insert(k, (v, 0));
            p
        }
        5 | 6 => {
            let v = vec![b'T'; rng.next_bounded(19)];
            let expire_at = [0, 1, NOW - 1, NOW, NOW + 1, u64::MAX][rng.next_bounded(6)];
            let p = payload::encode_put_ttl(&k, &v, expire_at).expect("fits");
            model.insert(k, (v, expire_at));
            p
        }
        7 | 8 => {
            model.remove(&k);
            payload::encode_delete(&k).expect("fits")
        }
        _ => {
            // Sometimes inverted: a no-op.
            let end = key(rng.next_bounded(KEYS + 1));
            if k < end {
                model.retain(|m, _| m.as_slice() < k.as_slice() || m.as_slice() >= end.as_slice());
            }
            payload::encode_range_delete(&k, &end).expect("fits")
        }
    }
}

/// The pairs that the model shows at [`NOW`], in key order.
fn visible(model: &Model) -> Vec<(Vec<u8>, Vec<u8>)> {
    model
        .iter()
        .filter(|(_, (_, e))| *e == 0 || *e > NOW)
        .map(|(k, (v, _))| (k.clone(), v.clone()))
        .collect()
}

/// What one model run did, to prove that it reached each path.
#[derive(Default)]
struct Reached {
    stalls: u32,
    compactions: u32,
    splits: u32,
}

/// Defines one model run over a drainer shape.
macro_rules! model_run {
    ($name:ident, $cap:literal, $arena:expr, $n:literal, $w:literal, $seeds:expr, $steps:literal, $splits:literal) => {
        #[test]
        // The shape's types and helpers live inside the test.
        #[allow(clippy::too_many_lines)]
        fn $name() {
            horton::db_types! {
                block: 4096, key_max: 8, val_max: 32, memtable_entries: $cap, memtable_arena: $arena;
                type Db = ShapeDb; type Scan = ShapeScan; type Compaction = ShapeCompaction;
            }
            type ShapeDrainer<'r> = Drainer<
                'r,
                MemDevice<4096>,
                4096,
                8,
                32,
                $cap,
                { $arena },
                4,
                4,
                256,
                0,
                $n,
                $w,
            >;

            /// Flushes. On `NeedsCompaction` it compacts to `Done` first.
            fn flush(d: &mut ShapeDrainer<'_>, c: &mut ShapeCompaction, reached: &mut Reached) {
                for _ in 0..8 {
                    match block_on(d.flush()) {
                        Ok(()) => return,
                        Err(horton::Error::NeedsCompaction) => {
                            reached.compactions += 1;
                            for _ in 0..10_000 {
                                if block_on(d.compact_step(c)).expect("compact ok")
                                    == horton::Progress::Done
                                {
                                    break;
                                }
                            }
                        }
                        Err(e) => panic!("flush failed: {e:?}"),
                    }
                }
                panic!("no flush after 8 compactions");
            }

            /// Point reads and a timed scan agree with the model.
            fn check(db: &ShapeDb<MemDevice<4096>>, model: &Model, at: &str) {
                let mut buf = [0u8; 32];
                for i in 0..KEYS {
                    let k = key(i);
                    let got = block_on(db.get_with_time(&k, &mut buf, NOW))
                        .expect("get ok")
                        .map(|n| buf[..n].to_vec());
                    let want = model
                        .get(&k)
                        .filter(|(_, e)| *e == 0 || *e > NOW)
                        .map(|(v, _)| v.clone());
                    assert_eq!(got, want, "{at}, key {i}");
                }
                let mut scan = ShapeScan::new(db);
                block_on(scan.seek_with_time(b"", None, u64::MAX, NOW)).expect("seek ok");
                let (mut kb, mut vb) = ([0u8; 8], [0u8; 32]);
                let mut got = Vec::new();
                while let Some((kl, vl)) = block_on(scan.next(&mut kb, &mut vb)).expect("next ok") {
                    got.push((kb[..kl].to_vec(), vb[..vl].to_vec()));
                }
                assert_eq!(got, visible(model), "{at}, scan");
            }

            /// Sweeps until the ring is empty. Flushes on `Stalled`.
            fn settle(d: &mut ShapeDrainer<'_>, c: &mut ShapeCompaction, reached: &mut Reached) -> u32 {
                let mut writes = 0;
                for _ in 0..($n * 4) {
                    match block_on(d.sweep()).expect("valid writes never poison") {
                        SweepOutcome::Swept { .. } => {
                            writes += 1;
                            if d.pending() > 0 {
                                reached.splits += 1;
                            }
                        }
                        SweepOutcome::Stalled => {
                            reached.stalls += 1;
                            flush(d, c, reached);
                        }
                        SweepOutcome::Idle => return writes,
                    }
                }
                panic!("the sweeps did not settle");
            }

            let mut reached = Reached::default();
            let cfg = horton::Config::whole_device(512);
            for seed in $seeds {
                let mut rng = Lcg::new(seed);
                let mut model = Model::new();
                let mut device = MemDevice::<4096>::new();
                let mut scratch = ShapeCompaction::new();
                // Each epoch reopens the `Db` on a fresh ring.
                for epoch in 0..3 {
                    let ring = Ring::<$n>::new();
                    let durable = AtomicU32::new(0);
                    let mut db = ShapeDb::new(device, cfg);
                    block_on(db.open()).expect("open ok");
                    check(&db, &model, &format!("seed {seed}, reopen {epoch}"));
                    let wal = db.config().wal_end - db.config().wal_start;
                    let mut drainer: ShapeDrainer<'_> = Drainer::new(&ring, db, &durable);
                    let writer = drainer.writer();
                    let (mut published, mut sweeps) = (0u32, 0u32);
                    for _ in 0..$steps {
                        // A trickle (one write) or a burst (up to the ring).
                        let burst = if rng.next_bounded(4) == 0 { 1 + rng.next_bounded($n) } else { 1 };
                        for _ in 0..burst {
                            writer.put(&random_op(&mut rng, &mut model)).expect("the ring has space");
                            published += 1;
                        }
                        sweeps += settle(&mut drainer, &mut scratch, &mut reached);
                        assert_eq!(drainer.pending(), 0, "seed {seed}");
                        assert_eq!(durable.load(Ordering::Acquire), published, "seed {seed}");
                    }
                    // Each sweep that writes takes at least one WAL block.
                    assert!(u64::from(sweeps) > 2 * wal, "seed {seed}: the WAL must wrap");
                    let db = drainer.into_db();
                    check(&db, &model, &format!("seed {seed}, epoch {epoch}"));
                    device = db.into_device();
                }
                let mut db = ShapeDb::new(device, cfg);
                block_on(db.open()).expect("open ok");
                check(&db, &model, &format!("seed {seed}, last reopen"));
            }
            assert!(reached.stalls > 0, "the run must stall");
            assert!(reached.compactions > 0, "the run must compact");
            assert_eq!(reached.splits > 0, $splits, "splits: {}", reached.splits);
        }
    };
}

// The README quickstart shape: 64 memtable entries, sweeps of 8. A sweep
// never splits.
model_run!(
    quickstart_shape_matches_the_model,
    64,
    horton::defaults::memtable_arena(64, 8, 32),
    8,
    8,
    1..=6u64,
    400,
    false
);

// A memtable of 4 and sweeps of 16: large bursts split.
model_run!(
    small_memtable_matches_the_model,
    4,
    horton::defaults::memtable_arena(4, 8, 32),
    16,
    16,
    1..=6u64,
    200,
    true
);
