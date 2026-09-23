//! Differential fuzzing: randomized `Db` operation streams checked against
//! `src/model.rs`.
//!
//! A seeded PRNG drives a long stream of mixed operations — `put`,
//! `delete`, `put_with_ttl`, `delete_range`, `WriteBatch`, `flush`,
//! `compact_step` (with TTL purge cutoffs), snapshot acquire/release,
//! logical-time advancement, and `into_device` + `open` reopens — against
//! a `TestDb`. An in-test oracle records every accepted mutation (point
//! version chains plus range tombstones); after every meaningful step each
//! key is read back with `get_at_with_time` and compared to
//! [`model_visible_value`](horton::model::model_visible_value) at
//! **value-level agreement**: not just live/absent, but the exact winning
//! value bytes.
//!
//! Soundness notes (why the oracle stays exact across compaction):
//!
//! * Reads happen only at `u64::MAX` or at live snapshot watermarks —
//!   exactly compaction's keep-set thresholds — so versions compaction
//!   drops are never a read's winner. The oracle is therefore never
//!   pruned; pruning would risk oracle/db skew.
//! * Range-tombstone drops are shadow-gated (an identical-range newer
//!   tombstone visible to every live snapshot must already cover the
//!   drop), so the covering-seq maximum at any read watermark is
//!   unchanged.
//! * The TTL purge converts an expired value into a point tombstone at
//!   the same sequence — never a silent drop — so the oracle mirrors it
//!   by flipping `tombstone = true` on purged versions. Purge cutoffs are
//!   always `<= now`, and `now` never moves backward, so every later read
//!   is contract-abiding (`now >= purge_before`).
//! * Rejected mutations (validation errors, `NoSpace`/`TableFull`/
//!   `ArenaFull`) consume no sequence number and leave no trace, so the
//!   oracle only records `Ok` outcomes — with the db's returned sequence
//!   asserted equal to the oracle's own counter on every success.
//! * Snapshots are released before every reopen (snapshot state is
//!   in-memory; recovery replays the WAL, which drops nothing).
//! * `TestDb` wires `CACHE = 8` (see `tests/common/mod.rs`), and every
//!   read in this file — live reads and snapshot-watermark reads alike —
//!   goes through `TableReader::open_cached` with the db's cache port, so
//!   the streams are also a randomized cache fuzzer: 8-slot CLOCK fill
//!   and eviction, invalidation of compacted-away tables, and cold
//!   restarts across `reopen`, all checked at exact value-byte agreement.
//!   Any stale, poisoned, or wrongly invalidated cache hit shows up here
//!   as a loud divergence, not a silent wrong answer.
//!
//! Any divergence between the db and the model is a production bug in the
//! read path, the compaction keep/drop rules, the purge conversion, or
//! WAL replay — not in this file.

mod common;

use std::collections::BTreeMap;

use common::{Lcg, MemDevice, TestDb, block_on, test_config};
use horton::model::{VersionTtl, model_visible_value};
use horton::{Compaction, Error, Progress, WriteBatch};

type TestCompaction = Compaction<4096, 256, 1024, 1024>;
type TestBatch = WriteBatch<256, 1024, 8>;
type TestDev = MemDevice<4096>;

const SEED_A: u64 = 0x00d1_ffe2_e7a1;
const SEED_B: u64 = 0x00c0_ffee_ee71;

/// A planned batch op: (key, value or `None` for delete, seq, tombstone).
type PlannedOp = (Vec<u8>, Option<Vec<u8>>, u64, bool);

/// One accepted point mutation in the oracle.
#[derive(Clone, Debug)]
struct OracleVersion {
    seq: u64,
    tombstone: bool,
    val: Vec<u8>,
    expire_at: u64,
}

/// The differential oracle: every accepted mutation, in sequence order.
#[derive(Debug, Default)]
struct Oracle {
    /// Per-key point versions, newest last.
    versions: BTreeMap<Vec<u8>, Vec<OracleVersion>>,
    /// Range tombstones as `(start, end, seq)`.
    rdels: Vec<(Vec<u8>, Vec<u8>, u64)>,
    /// Last sequence number the db assigned.
    seq: u64,
    /// Caller-owned logical clock; never moves backward.
    now: u64,
    /// Watermarks of live snapshots.
    snaps: Vec<u64>,
}

impl Oracle {
    /// Reads `key` at `(max_seq, now)` from the db and asserts exact
    /// agreement with the model: live/absent plus the winning value bytes.
    #[allow(clippy::too_many_lines)]
    fn check(&self, db: &TestDb<TestDev>, key: &[u8], max_seq: u64, ctx: &str) {
        let now = self.now;
        let vs: Vec<VersionTtl> = self
            .versions
            .get(key)
            .map_or(&[][..], Vec::as_slice)
            .iter()
            .map(|v| VersionTtl {
                seq: v.seq,
                tombstone: v.tombstone,
                expire_at: v.expire_at,
            })
            .collect();
        let covering: Vec<u64> = self
            .rdels
            .iter()
            .filter(|(lo, hi, _)| lo.as_slice() <= key && key < hi.as_slice())
            .map(|(_, _, q)| *q)
            .collect();
        let expect_live = model_visible_value(&vs, &covering, max_seq, now);
        let mut buf = [0u8; 1024];
        let got = block_on(db.get_at_with_time(key, &mut buf, max_seq, now)).unwrap();
        match (expect_live, got) {
            (true, Some(n)) => {
                let winner = self.versions[key]
                    .iter()
                    .filter(|v| v.seq <= max_seq)
                    .max_by_key(|v| v.seq)
                    .expect("model said live but oracle has no point winner");
                assert!(
                    !winner.tombstone,
                    "{ctx}: key {key:?} max_seq {max_seq} now {now}: model said live \
                     but oracle winner is a tombstone"
                );
                assert_eq!(
                    &buf[..n],
                    winner.val.as_slice(),
                    "{ctx}: key {key:?} max_seq {max_seq} now {now}: value bytes diverged"
                );
            }
            (false, None) => {}
            (true, None) => panic!(
                "{ctx}: key {key:?} max_seq {max_seq} now {now}: model says live, db says absent"
            ),
            (false, Some(n)) => panic!(
                "{ctx}: key {key:?} max_seq {max_seq} now {now}: model says absent, \
                 db returned {:?}",
                &buf[..n]
            ),
        }
    }

    /// Checks every oracle key plus a few never-written keys, at the live
    /// view and at every live snapshot watermark.
    fn check_all(&self, db: &TestDb<TestDev>, ctx: &str, extra_keys: &[Vec<u8>]) {
        for key in self.versions.keys().chain(extra_keys.iter()) {
            self.check(db, key, u64::MAX, ctx);
            for &snap in &self.snaps {
                self.check(db, key, snap, ctx);
            }
        }
    }

    /// Mirrors the TTL purge: every non-tombstone version already expired
    /// as of `purge_before` becomes a point tombstone at the same sequence.
    fn apply_purge(&mut self, purge_before: u64) {
        for chain in self.versions.values_mut() {
            for v in chain {
                if !v.tombstone && v.expire_at != 0 && v.expire_at <= purge_before {
                    v.tombstone = true;
                }
            }
        }
    }
}

/// Flushes, compacting first when the device reports `NoSpace` — the
/// documented caller-manages-space contract.
fn flush(db: &mut TestDb<TestDev>) {
    match block_on(db.flush()) {
        Ok(()) => {}
        Err(Error::NoSpace) => {
            drive_compaction(db, 0);
            block_on(db.flush()).unwrap();
        }
        Err(e) => panic!("unexpected flush error: {e:?}"),
    }
}

/// Drives compaction to quiescence with the given TTL purge cutoff.
fn drive_compaction(db: &mut TestDb<TestDev>, purge_before: u64) {
    let mut c = TestCompaction::new();
    c.purge_before = purge_before;
    while block_on(db.compact_step(&mut c)).unwrap() == Progress::More {}
}

/// Runs one mutating db op, flushing and retrying once on
/// `NoSpace`/`TableFull`/`ArenaFull`. A rejected op consumes no sequence
/// number and leaves no trace, so the retry takes exactly the sequence the
/// oracle expects. Returns the assigned sequence number, or `None` when
/// the op was a validation no-op / rejection the oracle must not record.
///
/// Validation failures (`EmptyKey`, `KeyTooLarge`, `ValueTooLarge`,
/// `BatchTooLarge`) are the caller's contract to avoid: they are reported
/// as `None` without retry so the fuzzer can generate them deliberately.
fn attempt(
    db: &mut TestDb<TestDev>,
    oracle: &Oracle,
    mut op: impl FnMut(&mut TestDb<TestDev>) -> Result<u64, Error<core::convert::Infallible>>,
) -> Option<u64> {
    match op(db) {
        Ok(seq) => {
            assert_eq!(
                seq,
                oracle.seq + 1,
                "db sequence jumped: expected {}, got {seq}",
                oracle.seq + 1
            );
            Some(seq)
        }
        Err(Error::NoSpace | Error::TableFull | Error::ArenaFull) => {
            flush(db);
            let seq = op(db).expect("op still failing after flush");
            assert_eq!(
                seq,
                oracle.seq + 1,
                "db sequence jumped after retry: expected {}, got {seq}",
                oracle.seq + 1
            );
            Some(seq)
        }
        Err(
            Error::EmptyKey
            | Error::KeyTooLarge { .. }
            | Error::ValueTooLarge { .. }
            | Error::BatchTooLarge { .. },
        ) => None,
        Err(e) => panic!("unexpected mutation error: {e:?}"),
    }
}

/// Reopens the db on the same device: `into_device` + `open`. The caller
/// releases live snapshots first (snapshot state is in-memory; the WAL
/// replay drops nothing).
fn reopen(db: TestDb<TestDev>, oracle: &mut Oracle) -> TestDb<TestDev> {
    oracle.snaps.clear();
    let dev = db.into_device();
    let mut db2 = TestDb::new(dev, test_config());
    block_on(db2.open()).unwrap();
    db2
}

/// Random key from the small keyspace, with occasional boundary lengths.
#[allow(clippy::cast_possible_truncation)]
fn gen_key(rng: &mut Lcg, keys: &[Vec<u8>]) -> Vec<u8> {
    match rng.next() % 20 {
        // Boundary lengths: empty and overlong are deliberate rejections.
        0 => Vec::new(),
        1 => vec![0x55; 256],
        2 => vec![0x55; 257],
        _ => keys[rng.next_bounded(keys.len())].clone(),
    }
}

/// Random value with boundary lengths (1025 is deliberately overlong).
#[allow(clippy::cast_possible_truncation)]
fn gen_val(rng: &mut Lcg) -> Vec<u8> {
    let len = match rng.next() % 12 {
        0 => 0,
        1 => 1024,
        2 => 1025,
        _ => rng.next_bounded(65),
    };
    let mut v = vec![0u8; len];
    for b in &mut v {
        *b = rng.next() as u8;
    }
    v
}

/// Random absolute expiry tick: 0 (never), 1, around `now`, far future,
/// and `u64::MAX` — the TTL boundary classes.
fn gen_expire_at(rng: &mut Lcg, now: u64) -> u64 {
    match rng.next() % 8 {
        0 => 0,
        1 => 1,
        2 => now,
        3 => now.saturating_add(1),
        4 => now.saturating_sub(1),
        5 => now.saturating_add(100),
        6 => u64::MAX,
        _ => rng.next() % 400,
    }
}

/// Random range bound: keyspace keys, below/above the keyspace, key-adjacent
/// values, empty (deliberate rejection), and overlong (deliberate rejection).
#[allow(clippy::cast_possible_truncation)]
fn gen_bound(rng: &mut Lcg, keys: &[Vec<u8>]) -> Vec<u8> {
    match rng.next() % 16 {
        0 => Vec::new(),
        1 => b"a".to_vec(),
        2 => vec![0xFF; 4],
        3 => vec![0x55; 257],
        _ => {
            let mut b = keys[rng.next_bounded(keys.len())].clone();
            // Adjacent bounds: key, key+0x00, key with last byte bumped.
            match rng.next() % 3 {
                0 => b.push(0x00),
                1 => {
                    if let Some(last) = b.last_mut() {
                        *last = last.saturating_add(1);
                    }
                }
                _ => {}
            }
            b
        }
    }
}

/// Drives one randomized operation stream. `heavy_compact` compacts (with
/// a random purge cutoff) after nearly every mutation to hammer the
/// keep-set / purge / drop paths across compaction crossings.
#[allow(clippy::too_many_lines)]
#[allow(clippy::cast_possible_truncation)]
fn run_stream(seed: u64, iters: usize, heavy_compact: bool) {
    let mut rng = Lcg::new(seed);
    let mut db = TestDb::new(TestDev::new(), test_config());
    block_on(db.open()).unwrap();
    let mut oracle = Oracle::default();
    // Small keyspace: dense overlap between point ops and range deletes.
    let keys: Vec<Vec<u8>> = (0u8..24).map(|i| format!("k{i:02}").into_bytes()).collect();
    // Never-written keys, for absent-read coverage.
    let extra_keys: Vec<Vec<u8>> = ["zzza", "zzzq"]
        .iter()
        .map(|s| s.as_bytes().to_vec())
        .collect();

    for step in 0..iters {
        let ctx = format!("seed {seed:#x} step {step}");
        match rng.next() % 100 {
            0..=28 => {
                // put / put_with_ttl
                let k = gen_key(&mut rng, &keys);
                let v = gen_val(&mut rng);
                let expire_at = if rng.next().is_multiple_of(3) {
                    gen_expire_at(&mut rng, oracle.now)
                } else {
                    0
                };
                let seq = if expire_at == 0 {
                    attempt(&mut db, &oracle, |db| block_on(db.put(&k, &v)))
                } else {
                    attempt(&mut db, &oracle, |db| {
                        block_on(db.put_with_ttl(&k, &v, expire_at))
                    })
                };
                if let Some(seq) = seq {
                    oracle.seq = seq;
                    oracle
                        .versions
                        .entry(k.clone())
                        .or_default()
                        .push(OracleVersion {
                            seq,
                            tombstone: false,
                            val: v,
                            expire_at,
                        });
                    oracle.check(&db, &k, u64::MAX, &ctx);
                }
            }
            29..=38 => {
                // delete
                let k = gen_key(&mut rng, &keys);
                if let Some(seq) = attempt(&mut db, &oracle, |db| block_on(db.delete(&k))) {
                    oracle.seq = seq;
                    oracle
                        .versions
                        .entry(k.clone())
                        .or_default()
                        .push(OracleVersion {
                            seq,
                            tombstone: true,
                            val: Vec::new(),
                            expire_at: 0,
                        });
                    oracle.check(&db, &k, u64::MAX, &ctx);
                }
            }
            39..=48 => {
                // delete_range (empty/inverted ranges are no-ops by contract)
                let a = gen_bound(&mut rng, &keys);
                let b = gen_bound(&mut rng, &keys);
                if a >= b {
                    let before = oracle.seq;
                    let ret = block_on(db.delete_range(&a, &b)).unwrap();
                    assert_eq!(
                        ret, before,
                        "{ctx}: empty/inverted range consumed a sequence number"
                    );
                } else if let Some(seq) =
                    attempt(&mut db, &oracle, |db| block_on(db.delete_range(&a, &b)))
                {
                    oracle.seq = seq;
                    oracle.rdels.push((a, b, seq));
                }
                oracle.check_all(&db, &ctx, &extra_keys);
            }
            49..=56 => {
                // WriteBatch of puts/deletes
                let n = 1 + rng.next_bounded(5);
                let mut batch = TestBatch::new();
                let mut planned: Vec<PlannedOp> = Vec::new();
                let mut build_ok = true;
                let mut seq = oracle.seq;
                for _ in 0..n {
                    let k = gen_key(&mut rng, &keys);
                    if rng.next().is_multiple_of(2) {
                        let v = gen_val(&mut rng);
                        if batch.put(&k, &v).is_err() {
                            build_ok = false;
                            break;
                        }
                        seq += 1;
                        planned.push((k, Some(v), seq, false));
                    } else if batch.delete(&k).is_err() {
                        build_ok = false;
                        break;
                    } else {
                        seq += 1;
                        planned.push((k, None, seq, true));
                    }
                }
                if build_ok && !planned.is_empty() {
                    let base_opt = attempt(&mut db, &oracle, |db| block_on(db.write(&batch)));
                    if let Some(base) = base_opt {
                        assert_eq!(
                            base,
                            oracle.seq + 1,
                            "{ctx}: batch base is not the next sequence"
                        );
                        let n_ops = planned.len() as u64;
                        for (i, (k, v, pseq, tomb)) in planned.into_iter().enumerate() {
                            let expect = base + i as u64;
                            assert_eq!(pseq, expect, "{ctx}: batch op {i} sequence mismatch");
                            oracle.versions.entry(k).or_default().push(OracleVersion {
                                seq: pseq,
                                tombstone: tomb,
                                val: v.unwrap_or_default(),
                                expire_at: 0,
                            });
                        }
                        oracle.seq = base + n_ops - 1;
                    }
                }
                oracle.check_all(&db, &ctx, &extra_keys);
            }
            57..=62 => {
                // flush
                flush(&mut db);
                oracle.check_all(&db, &ctx, &extra_keys);
            }
            63..=67 => {
                // compact to quiescence, sometimes with a TTL purge
                let purge = if rng.next().is_multiple_of(2) {
                    oracle.now
                } else {
                    0
                };
                drive_compaction(&mut db, purge);
                oracle.apply_purge(purge);
                oracle.check_all(&db, &ctx, &extra_keys);
            }
            68..=70 => {
                // reopen on the same device
                for &snap in &oracle.snaps {
                    db.release_snapshot(snap);
                }
                db = reopen(db, &mut oracle);
                oracle.check_all(&db, &ctx, &extra_keys);
            }
            71..=73 => {
                // acquire a snapshot (cap: 3 live)
                if oracle.snaps.len() < 3 {
                    let snap = db.snapshot().unwrap();
                    assert!(
                        snap >= oracle.seq,
                        "{ctx}: snapshot watermark {snap} below last seq {}",
                        oracle.seq
                    );
                    oracle.snaps.push(snap);
                }
                oracle.check_all(&db, &ctx, &extra_keys);
            }
            74..=76 => {
                // release a snapshot
                if !oracle.snaps.is_empty() {
                    let idx = rng.next_bounded(oracle.snaps.len());
                    let snap = oracle.snaps.remove(idx);
                    db.release_snapshot(snap);
                }
                oracle.check_all(&db, &ctx, &extra_keys);
            }
            77..=84 => {
                // advance the logical clock (never backward: TTL contract)
                oracle.now = oracle.now.saturating_add(1 + rng.next() % 20);
                oracle.check_all(&db, &ctx, &extra_keys);
            }
            _ => {
                // read-only: random key at the live view or a live snapshot
                let k = if rng.next().is_multiple_of(5) {
                    extra_keys[rng.next_bounded(extra_keys.len())].clone()
                } else {
                    keys[rng.next_bounded(keys.len())].clone()
                };
                let max_seq = if !oracle.snaps.is_empty() && rng.next().is_multiple_of(2) {
                    oracle.snaps[rng.next_bounded(oracle.snaps.len())]
                } else {
                    u64::MAX
                };
                oracle.check(&db, &k, max_seq, &ctx);
            }
        }
        if heavy_compact && step % 3 == 2 {
            let purge = if rng.next().is_multiple_of(2) {
                oracle.now
            } else {
                0
            };
            drive_compaction(&mut db, purge);
            oracle.apply_purge(purge);
            let ctx2 = format!("{ctx} post-compact");
            oracle.check_all(&db, &ctx2, &extra_keys);
        }
    }
    // Final full verification, then release any held snapshots.
    oracle.check_all(&db, "final", &extra_keys);
    for &snap in &oracle.snaps {
        db.release_snapshot(snap);
    }
}

#[test]
fn differential_fuzz_mixed_ops() {
    let iters = if cfg!(miri) { 8 } else { 300 };
    run_stream(SEED_A, iters, false);
}

#[test]
fn differential_fuzz_compaction_heavy() {
    let iters = if cfg!(miri) { 6 } else { 120 };
    run_stream(SEED_B, iters, true);
}
