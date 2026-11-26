//! The drainer against a model (issue #48, next charter 1).
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
                model.retain(|m, _| {
                    !(m.as_slice() >= k.as_slice() && m.as_slice() < end.as_slice())
                });
            }
            payload::encode_range_delete(&k, &end).expect("fits")
        }
    }
}

/// Defines one model run over a drainer shape.
macro_rules! model_run {
    ($name:ident, $cap:literal, $arena:expr, $n:literal, $w:literal, $seeds:expr, $steps:literal) => {
        #[test]
        fn $name() {
            horton::db_types! {
                block: 4096, key_max: 8, val_max: 32, memtable_entries: $cap, memtable_arena: $arena;
                type Db = ShapeDb; type Compaction = ShapeCompaction;
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
            fn flush(d: &mut ShapeDrainer<'_>, c: &mut ShapeCompaction, compactions: &mut u32) {
                for _ in 0..8 {
                    match block_on(d.flush()) {
                        Ok(()) => return,
                        Err(horton::Error::NeedsCompaction) => {
                            *compactions += 1;
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

            let mut stalls = 0u32;
            let mut compactions = 0u32;
            for seed in $seeds {
                let mut rng = Lcg::new(seed);
                let mut model = Model::new();
                let cfg = horton::Config::whole_device(512);
                let mut device = MemDevice::<4096>::new();
                let mut scratch = ShapeCompaction::new();
                let mut written = 0u32;
                // Each epoch reopens the `Db` on a fresh ring.
                for _epoch in 0..3 {
                    let ring = Ring::<$n>::new();
                    let durable = AtomicU32::new(0);
                    let mut db = ShapeDb::new(device, cfg);
                    block_on(db.open()).expect("open ok");
                    let mut drainer: ShapeDrainer<'_> = Drainer::new(&ring, db, &durable);
                    let writer = drainer.writer();
                    let mut published = 0u32;
                    for _ in 0..$steps {
                        // A trickle (one write) or a burst (up to the ring).
                        let burst = if rng.next_bounded(4) == 0 {
                            1 + rng.next_bounded($n)
                        } else {
                            1
                        };
                        for _ in 0..burst {
                            let p = random_op(&mut rng, &mut model);
                            writer.put(&p).expect("the ring has space");
                            published += 1;
                        }
                        for _ in 0..($n * 4) {
                            match block_on(drainer.sweep()).expect("valid writes never poison") {
                                SweepOutcome::Swept { .. } => {}
                                SweepOutcome::Stalled => {
                                    stalls += 1;
                                    flush(&mut drainer, &mut scratch, &mut compactions);
                                }
                                SweepOutcome::Idle => break,
                            }
                        }
                        assert_eq!(drainer.pending(), 0, "seed {seed}");
                        assert_eq!(durable.load(Ordering::Acquire), published, "seed {seed}");
                    }
                    written += published;
                    let db = drainer.into_db();
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
                        assert_eq!(got, want, "seed {seed}, key {i}, after {written} writes");
                    }
                    device = db.into_device();
                }
            }
            assert!(stalls > 0, "the run must stall");
            assert!(compactions > 0, "the run must compact");
        }
    };
}

// The README quickstart shape: 64 memtable entries, sweeps of 8.
model_run!(
    quickstart_shape_matches_the_model,
    64,
    horton::defaults::memtable_arena(64, 8, 32),
    8,
    8,
    1..=6u64,
    400
);

// A memtable of 4 and sweeps of 16: every burst splits.
model_run!(
    small_memtable_matches_the_model,
    4,
    horton::defaults::memtable_arena(4, 8, 32),
    16,
    16,
    1..=6u64,
    200
);
