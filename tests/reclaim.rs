//! Space reclaim (issue #32): a full table region gives deleted and
//! expired space back.
//!
//! - `Db::request_compaction(level)` compacts every table from `level`
//!   down to the bottom once, below the level triggers too. At the
//!   bottom it rewrites tables in place, so `purge_before` and range
//!   tombstones free space there.
//! - A job that cannot get its estimated slots runs as a *tight* job: it
//!   needs one free slot, and each commit frees at least the slots it
//!   takes. At the compaction reserve, a job that shrinks data always
//!   runs.

mod common;

use std::collections::BTreeMap;

use common::{CrashDevice, MemDevice, TestDb, block_on, test_config, tight_config};
use horton::{BlockDevice, Compaction, Error, Progress, WriteBatch};

// The kvstore shape from the issue: 16 KiB blocks, 4 levels of 7 tables
// (28 slots). A small memtable keeps the slots small and the test fast.
horton::db_types! {
    block: 16384,
    key_max: 64,
    val_max: 256,
    memtable_entries: 64,
    memtable_arena: 16384,
    levels: 4,
    tables_per_level: 7,
    bloom_bytes: 1024;
    type Db = KvDb;
    type Compaction = KvCompaction;
}

const BLOCK: usize = 16384;
const LEVELS: usize = 4;
type Dev = MemDevice<BLOCK>;
type Db = KvDb<Dev>;
type DbError = Error<core::convert::Infallible>;

/// TTL of the first fill, and a clock tick past it.
const TTL: u64 = 100;
const LATER: u64 = 1_000;
/// Step limit for one drain: a livelock fails the test, not the run.
const MAX_STEPS: usize = 100_000;

fn new_db() -> Db {
    let blocks = Db::MIN_DEVICE_BLOCKS;
    let mut db = KvDb::new(Dev::new(), horton::Config::whole_device(blocks));
    block_on(db.open()).unwrap();
    db
}

fn reopen(db: Db) -> Db {
    let config = db.config();
    let mut db = KvDb::new(db.into_device(), config);
    block_on(db.open()).unwrap();
    db
}

/// A distinct key per `i`, spread over the key space.
fn key(prefix: u8, i: u64) -> Vec<u8> {
    let mut k = vec![prefix];
    k.extend_from_slice(format!("{:012}", i.wrapping_mul(7919) % 1_000_003).as_bytes());
    k
}

fn val(i: u64) -> [u8; 200] {
    let mut v = [0u8; 200];
    v[..8].copy_from_slice(&i.to_le_bytes());
    v
}

/// Runs one compaction job to its end.
fn job(db: &mut Db, c: &mut KvCompaction) -> Result<(), DbError> {
    while block_on(db.compact_step(c))? == Progress::More {}
    Ok(())
}

/// Flushes, running jobs while flush asks for them.
fn flush(db: &mut Db, c: &mut KvCompaction) -> Result<(), DbError> {
    for _ in 0..1_000 {
        match block_on(db.flush()) {
            Err(Error::NeedsCompaction) => job(db, c)?,
            r => return r,
        }
    }
    panic!("flush never got room");
}

/// Runs `compact_step` until no job is pending. Every step must succeed.
fn drain(db: &mut Db, c: &mut KvCompaction) {
    for _ in 0..MAX_STEPS {
        if !db.compaction_pending() {
            return;
        }
        block_on(db.compact_step(c)).unwrap();
    }
    panic!("compaction never finished");
}

/// When a fill stops.
#[derive(Clone, Copy)]
enum Stop {
    /// At the first error that making room cannot fix.
    Full,
    /// When a flush would leave this many free slots or fewer (the
    /// plugin's quota).
    Quota(u32),
}

/// Ops per fill batch: one WAL block write each keeps debug runs fast.
const BATCH: usize = 16;
/// `BATCH` as a key count.
const BATCH_KEYS: u64 = 16;

/// Puts `prefix` keys until `stop`, `BATCH` at a time. Returns how many
/// were stored and the error that stopped the fill (`None` for a quota
/// stop).
fn fill(
    db: &mut Db,
    c: &mut KvCompaction,
    prefix: u8,
    expire_at: u64,
    stop: Stop,
) -> (u64, Option<DbError>) {
    let mut n = 0u64;
    let mut batch = WriteBatch::<64, 256, BATCH>::new();
    loop {
        batch.clear();
        for i in n..n + BATCH_KEYS {
            if expire_at == 0 {
                batch.put(&key(prefix, i), &val(i)).unwrap();
            } else {
                batch.put_ttl(&key(prefix, i), &val(i), expire_at).unwrap();
            }
        }
        match block_on(db.write(&batch)) {
            Ok(_) => n += BATCH_KEYS,
            Err(Error::TableFull | Error::ArenaFull | Error::WalFull) => {
                if let Stop::Quota(q) = stop
                    && db.slot_stats().free <= q
                {
                    return (n, None);
                }
                if let Err(e) = flush(db, c) {
                    return (n, Some(e));
                }
            }
            Err(e) => return (n, Some(e)),
        }
        assert!(n < 1_000_000, "the region never filled");
    }
}

/// Writes `(key, value seed, expire_at)` items in batches, making room
/// as needed.
fn write_all(db: &mut Db, c: &mut KvCompaction, items: impl Iterator<Item = (Vec<u8>, u64, u64)>) {
    let items: Vec<_> = items.collect();
    let mut batch = WriteBatch::<64, 256, BATCH>::new();
    for chunk in items.chunks(BATCH) {
        batch.clear();
        for (k, i, exp) in chunk {
            if *exp == 0 {
                batch.put(k, &val(*i)).unwrap();
            } else {
                batch.put_ttl(k, &val(*i), *exp).unwrap();
            }
        }
        loop {
            match block_on(db.write(&batch)) {
                Ok(_) => break,
                Err(Error::TableFull | Error::ArenaFull | Error::WalFull) => flush(db, c).unwrap(),
                Err(e) => panic!("{e:?}"),
            }
        }
    }
}

fn get(db: &Db, k: &[u8], now: u64) -> Option<Vec<u8>> {
    let mut buf = [0u8; 256];
    block_on(db.get_with_time(k, &mut buf, now))
        .unwrap()
        .map(|n| buf[..n].to_vec())
}

fn requested_compaction(db: &mut Db, c: &mut KvCompaction, level: usize) {
    db.request_compaction(level).unwrap();
    drain(db, c);
}

/// AC: fill to `RegionFull` with TTL data, move the clock past every TTL,
/// and show that compaction frees the region and writes work again.
#[test]
fn ttl_wedge_recovers_after_the_clock_passes_every_ttl() {
    let mut db = new_db();
    let mut c = KvCompaction::new();
    let (first, err) = fill(&mut db, &mut c, b's', TTL, Stop::Full);
    assert!(matches!(err, Some(Error::RegionFull)), "{err:?}");
    assert!(first > 2_000, "first fill stored {first}");

    // The wedge survives a reopen: the region is still full.
    let mut db = reopen(db);
    let full = db.slot_stats();
    assert!(full.free <= 2, "{full:?}");

    // The clock passes every TTL: compaction gives the region back.
    c.purge_before = LATER;
    requested_compaction(&mut db, &mut c, 0);
    assert_eq!(db.check_invariants(), Ok(()));
    let s = db.slot_stats();
    assert_eq!((s.used, s.reserved), (0, 0), "{s:?}");
    for i in [0, first / 2, first - 1] {
        assert_eq!(get(&db, &key(b's', i), LATER), None);
    }

    // Writes work again, and a second fill stores about as much.
    flush(&mut db, &mut c).unwrap();
    block_on(db.delete(b"z")).unwrap();
    let (second, err) = fill(&mut db, &mut c, b't', 0, Stop::Full);
    assert!(matches!(err, Some(Error::RegionFull)), "{err:?}");
    assert!(second * 10 >= first * 9, "first {first}, second {second}");
    assert_eq!(get(&db, &key(b't', 0), LATER), Some(val(0).to_vec()));
    assert_eq!(db.check_invariants(), Ok(()));
}

/// AC: at the compaction reserve, `compact_step` runs a job that shrinks
/// data. Before the fix every step failed with `RegionFull`.
#[test]
fn the_reserve_runs_a_job_that_shrinks_data() {
    let mut db = new_db();
    let mut c = KvCompaction::new();
    let (_, err) = fill(&mut db, &mut c, b's', TTL, Stop::Full);
    assert!(matches!(err, Some(Error::RegionFull)), "{err:?}");
    let before = db.slot_stats();
    assert!(db.compaction_pending());

    c.purge_before = LATER;
    job(&mut db, &mut c).unwrap();
    let after = db.slot_stats();
    assert!(after.used < before.used, "{before:?} -> {after:?}");
    drain(&mut db, &mut c);
    assert!(db.slot_stats().free > 2);
    flush(&mut db, &mut c).unwrap();
    assert_eq!(db.check_invariants(), Ok(()));
}

/// Issue problem 2: `delete_range` over all data, then compaction, gives
/// the space back below the level triggers.
#[test]
fn delete_range_then_request_compaction_gives_space_back() {
    let mut db = new_db();
    let mut c = KvCompaction::new();
    let (first, err) = fill(&mut db, &mut c, b's', 0, Stop::Quota(6));
    assert!(err.is_none(), "{err:?}");
    // At the quota, deletes still work.
    flush(&mut db, &mut c).unwrap();
    block_on(db.delete_range(b"s", b"t")).unwrap();
    flush(&mut db, &mut c).unwrap();
    drain(&mut db, &mut c);
    assert!(!db.compaction_pending());

    requested_compaction(&mut db, &mut c, 0);
    let s = db.slot_stats();
    assert_eq!(s.used, 0, "{s:?}");
    assert_eq!(get(&db, &key(b's', 0), 0), None);

    let (second, err) = fill(&mut db, &mut c, b's', 0, Stop::Quota(6));
    assert!(err.is_none(), "{err:?}");
    assert!(second * 10 >= first * 9, "first {first}, second {second}");
    assert_eq!(get(&db, &key(b's', 1), 0), Some(val(1).to_vec()));
    assert_eq!(db.check_invariants(), Ok(()));
}

/// AC: it works when only the bottom level holds data: the bottom-level
/// rewrite applies `purge_before`.
#[test]
fn a_bottom_level_rewrite_purges_expired_data() {
    let mut db = new_db();
    let mut c = KvCompaction::new();
    // Live data and expiring data, interleaved in key order.
    write_all(
        &mut db,
        &mut c,
        (0..1_200u64).map(|i| (key(b's', i), i, if i % 2 == 0 { 0 } else { TTL })),
    );
    flush(&mut db, &mut c).unwrap();
    // Move everything to the bottom without a purge.
    requested_compaction(&mut db, &mut c, 0);
    for l in 0..LEVELS - 1 {
        assert_eq!(db.level_tables(l).unwrap().len(), 0, "level {l}");
    }
    let bottom = db.slot_stats().used;
    assert!(bottom > 0);

    // Only the bottom level holds data. Rewrite it with the purge on.
    c.purge_before = LATER;
    requested_compaction(&mut db, &mut c, LEVELS - 1);
    let after = db.slot_stats().used;
    assert!(after < bottom, "{bottom} -> {after}");
    for i in 0..1_200u64 {
        let want = (i % 2 == 0).then(|| val(i).to_vec());
        assert_eq!(get(&db, &key(b's', i), LATER), want, "key {i}");
    }
    assert_eq!(db.check_invariants(), Ok(()));
}

#[test]
fn request_compaction_checks_the_database_and_the_level() {
    let mut closed = KvDb::new(
        Dev::new(),
        horton::Config::whole_device(Db::MIN_DEVICE_BLOCKS),
    );
    assert!(matches!(closed.request_compaction(0), Err(Error::NotOpen)));
    let mut db = new_db();
    assert!(matches!(
        db.request_compaction(LEVELS),
        Err(Error::BadLevel { level: LEVELS })
    ));
    // Nothing stored: nothing to do.
    db.request_compaction(0).unwrap();
    assert!(!db.compaction_pending());
    let mut c = KvCompaction::new();
    assert_eq!(block_on(db.compact_step(&mut c)), Ok(Progress::Done));
}

/// A request makes work pending below the triggers, and live data reads
/// the same after it.
#[test]
fn a_request_is_pending_until_every_table_was_compacted_once() {
    let mut db = new_db();
    let mut c = KvCompaction::new();
    let mut model = BTreeMap::new();
    // Overwrites too: 800 puts over 600 keys.
    let items: Vec<_> = (0..800u64).map(|i| (key(b's', i % 600), i, 0)).collect();
    for (k, i, _) in &items {
        model.insert(k.clone(), val(*i).to_vec());
    }
    write_all(&mut db, &mut c, items.into_iter());
    flush(&mut db, &mut c).unwrap();
    drain(&mut db, &mut c);
    assert!(!db.compaction_pending());

    db.request_compaction(0).unwrap();
    assert!(db.compaction_pending());
    drain(&mut db, &mut c);
    for l in 0..LEVELS - 1 {
        assert_eq!(db.level_tables(l).unwrap().len(), 0, "level {l}");
    }
    for (k, v) in &model {
        assert_eq!(get(&db, k, 0).as_ref(), Some(v));
    }
    assert_eq!(db.check_invariants(), Ok(()));
    // The request is done: it does not wake up for later tables.
    let mut db = reopen(db);
    assert!(!db.compaction_pending());
    db.request_compaction(LEVELS - 1).unwrap();
    drain(&mut db, &mut c);
    assert!(!db.compaction_pending());
}

/// A region full of live data stays honest: a job that cannot free slots
/// fails with `RegionFull` and changes nothing, and a request never makes
/// the region fuller.
#[test]
fn live_data_is_never_made_larger_by_a_tight_job() {
    let mut db = new_db();
    let mut c = KvCompaction::new();
    let (n, err) = fill(&mut db, &mut c, b's', 0, Stop::Full);
    assert!(matches!(err, Some(Error::RegionFull)), "{err:?}");
    let before = db.slot_stats();

    let r = job(&mut db, &mut c);
    assert!(matches!(r, Err(Error::RegionFull)), "{r:?}");
    assert_eq!(db.slot_stats(), before);

    db.request_compaction(0).unwrap();
    let mut steps = 0;
    while db.compaction_pending() {
        // A step may fail with `RegionFull`; it must not grow the region.
        let _ = block_on(db.compact_step(&mut c));
        assert!(db.slot_stats().used <= before.used);
        steps += 1;
        assert!(steps < MAX_STEPS, "never finished");
        if steps > 10_000 {
            break;
        }
    }
    for i in [0, n / 3, n - 1] {
        assert_eq!(get(&db, &key(b's', i), 0), Some(val(i).to_vec()), "key {i}");
    }
    assert_eq!(db.check_invariants(), Ok(()));
}

/// Manual compaction keeps every version a live snapshot can see.
#[test]
fn a_request_keeps_snapshot_views() {
    let mut db = new_db();
    let mut c = KvCompaction::new();
    for i in 0..600u64 {
        block_on(db.put(&key(b's', i), &val(i))).unwrap_or_else(|_| {
            flush(&mut db, &mut c).unwrap();
            block_on(db.put(&key(b's', i), &val(i))).unwrap()
        });
    }
    flush(&mut db, &mut c).unwrap();
    let snap = db.snapshot().unwrap();
    block_on(db.delete_range(b"s", b"t")).unwrap();
    flush(&mut db, &mut c).unwrap();
    requested_compaction(&mut db, &mut c, 0);
    let mut buf = [0u8; 256];
    for i in [0u64, 300, 599] {
        let n = block_on(db.get_at(&key(b's', i), &mut buf, snap))
            .unwrap()
            .unwrap();
        assert_eq!(&buf[..n], &val(i));
        assert_eq!(get(&db, &key(b's', i), 0), None);
    }
    db.release_snapshot(snap);
    requested_compaction(&mut db, &mut c, 0);
    assert_eq!(db.slot_stats().used, 0);
}

/// Two levels: L0 and the bottom.
type TwoLevelDb<D = MemDevice<4096>> = horton::Db<D, 4096, 256, 1024, 64, 4096, 2, 4, 1024, 8>;
/// One level: no compaction.
type OneLevelDb = horton::Db<MemDevice<4096>, 4096, 256, 1024, 64, 4096, 1, 4, 1024, 8>;

fn read2(db: &TwoLevelDb, k: &[u8]) -> Option<Vec<u8>> {
    let mut buf = [0u8; 1024];
    block_on(db.get(k, &mut buf))
        .unwrap()
        .map(|n| buf[..n].to_vec())
}

/// A foreign table below vetoes the job of a due table. The request skips
/// that table, goes on, and ends.
#[test]
fn a_request_skips_a_table_that_a_foreign_table_vetoes() {
    let mut db = TwoLevelDb::new(MemDevice::<4096>::new(), test_config());
    block_on(db.open()).unwrap();
    let mut c = SmallComp::new();
    block_on(db.put(b"a", b"old")).unwrap();
    block_on(db.put(b"b", b"old")).unwrap();
    block_on(db.flush()).unwrap();
    db.request_compaction(0).unwrap();
    while db.compaction_pending() {
        block_on(db.compact_step(&mut c)).unwrap();
    }
    let bottom = db.level_tables(1).unwrap()[0].id;
    assert!(block_on(db.stamp_table(bottom, 2, 100)).unwrap());
    block_on(db.put(b"a", b"new")).unwrap();
    block_on(db.flush()).unwrap();
    let before = (read2(&db, b"a"), read2(&db, b"b"));

    db.request_compaction(0).unwrap();
    assert!(db.compaction_pending());
    let mut steps = 0;
    while db.compaction_pending() {
        assert_eq!(block_on(db.compact_step(&mut c)), Ok(Progress::Done));
        steps += 1;
        assert!(steps < 10, "the request never ended");
    }
    // The vetoed L0 table stays; the bottom table was rewritten.
    assert_eq!(db.level_tables(0).unwrap().len(), 1);
    assert_eq!(db.level_tables(1).unwrap().len(), 1);
    assert_ne!(db.level_tables(1).unwrap()[0].id, bottom);
    assert_eq!((read2(&db, b"a"), read2(&db, b"b")), before);
    assert_eq!(db.check_invariants(), Ok(()));
}

/// Three levels, 12 slots of 8 blocks: tables split often.
type ThreeLevelDb = horton::Db<MemDevice<4096>, 4096, 256, 1024, 64, 4096, 3, 4, 1024, 8>;

const fn three_config() -> horton::Config {
    horton::Config::new(8, 136, 136, 136 + 12 * 8, 0, 4)
}

fn three_flush(db: &mut ThreeLevelDb, c: &mut SmallComp) {
    while block_on(db.flush()) == Err(Error::NeedsCompaction) {
        while block_on(db.compact_step(c)).unwrap() == Progress::More {}
    }
}

fn three_drain(db: &mut ThreeLevelDb, c: &mut SmallComp) {
    for _ in 0..MAX_STEPS {
        if !db.compaction_pending() {
            return;
        }
        block_on(db.compact_step(c)).unwrap();
    }
    panic!("compaction never finished");
}

/// At a middle level, a veto skips only the vetoed table: every other
/// due table still moves down.
#[test]
fn a_veto_at_a_middle_level_skips_only_the_vetoed_table() {
    let mut db = ThreeLevelDb::new(MemDevice::<4096>::new(), three_config());
    block_on(db.open()).unwrap();
    let mut c = SmallComp::new();
    // A foreign table at "b" and a same-node table at "y", at the bottom.
    for k in [b"b", b"y"] {
        block_on(db.put(k, b"v")).unwrap();
        three_flush(&mut db, &mut c);
        db.request_compaction(0).unwrap();
        three_drain(&mut db, &mut c);
        if k == b"b" {
            let id = db.level_tables(2).unwrap()[0].id;
            assert!(block_on(db.stamp_table(id, 2, 100)).unwrap());
        }
    }
    // Keys around both: L0 jobs leave several tables at level 1.
    for (n, p) in b"acxz".iter().enumerate() {
        for i in 0..150u32 {
            let k = format!("{}{i:03}", char::from(*p));
            let v = [u8::try_from(n).unwrap(); 100];
            if block_on(db.put(k.as_bytes(), &v)).is_err() {
                three_flush(&mut db, &mut c);
                block_on(db.put(k.as_bytes(), &v)).unwrap();
            }
        }
    }
    three_flush(&mut db, &mut c);
    three_drain(&mut db, &mut c);
    assert!(db.level_tables(1).unwrap().len() >= 2);

    db.request_compaction(1).unwrap();
    three_drain(&mut db, &mut c);
    // Only tables that overlap the foreign table stay at level 1.
    let foreign: Vec<_> = db
        .level_tables(2)
        .unwrap()
        .iter()
        .filter(|t| t.node_id == 2)
        .copied()
        .collect();
    for t in db.level_tables(1).unwrap() {
        assert!(
            foreign
                .iter()
                .any(|f| f.first_key.as_slice() <= t.last_key.as_slice()
                    && t.first_key.as_slice() <= f.last_key.as_slice()),
            "table {} stayed at level 1 without a veto",
            t.id
        );
    }
    assert_eq!(db.check_invariants(), Ok(()));
}

/// A tight job for a full level is in flight when a request opens. The
/// request ends that job, so its give-up cannot fail the request.
#[test]
fn a_new_request_ends_a_running_job() {
    let mut db = new_db();
    let mut c = KvCompaction::new();
    let (_, err) = fill(&mut db, &mut c, b's', 0, Stop::Full);
    assert!(matches!(err, Some(Error::RegionFull)), "{err:?}");
    // The first step starts a tight job over live data.
    assert_eq!(block_on(db.compact_step(&mut c)), Ok(Progress::More));
    assert_eq!(db.slot_stats().reserved, 1);
    db.request_compaction(0).unwrap();
    assert_eq!(db.slot_stats().reserved, 0);
    assert_eq!(job(&mut db, &mut c), Ok(()));
    assert!(db.compaction_pending());
}

/// Fails reads while `fail` is set.
struct FailDevice {
    inner: MemDevice<4096>,
    fail: core::cell::Cell<bool>,
}

impl BlockDevice for FailDevice {
    type Error = ();
    const BLOCK: usize = 4096;

    fn poll_read_block(
        &self,
        cx: &mut core::task::Context<'_>,
        id: u64,
        buf: &mut [u8],
    ) -> core::task::Poll<Result<(), ()>> {
        if self.fail.get() {
            return core::task::Poll::Ready(Err(()));
        }
        self.inner.poll_read_block(cx, id, buf).map_err(|_| ())
    }

    fn poll_write_block(
        &mut self,
        cx: &mut core::task::Context<'_>,
        id: u64,
        buf: &[u8],
    ) -> core::task::Poll<Result<(), ()>> {
        self.inner.poll_write_block(cx, id, buf).map_err(|_| ())
    }

    fn poll_flush(&mut self, cx: &mut core::task::Context<'_>) -> core::task::Poll<Result<(), ()>> {
        self.inner.poll_flush(cx).map_err(|_| ())
    }
}

/// A requested job that fails is skipped: the request does not stall on
/// the same table at every step.
#[test]
fn a_request_skips_a_job_that_fails() {
    let dev = FailDevice {
        inner: MemDevice::new(),
        fail: core::cell::Cell::new(false),
    };
    let mut db = TwoLevelDb::new(dev, test_config());
    block_on(db.open()).unwrap();
    block_on(db.put(b"a", b"v")).unwrap();
    block_on(db.flush()).unwrap();
    let mut c = SmallComp::new();
    db.request_compaction(0).unwrap();
    db.device().fail.set(true);
    assert!(matches!(
        block_on(db.compact_step(&mut c)),
        Err(Error::Device(()))
    ));
    db.device().fail.set(false);
    assert!(!db.compaction_pending(), "the failed table is skipped");
    assert_eq!(db.level_tables(0).unwrap().len(), 1);
    let mut buf = [0u8; 1024];
    assert_eq!(block_on(db.get(b"a", &mut buf)), Ok(Some(1)));
    assert_eq!(db.slot_stats().reserved, 0);
}

/// A request does not rewrite the bottom tables that its own jobs wrote:
/// that costs flash wear and frees nothing.
#[test]
fn a_request_does_not_rewrite_its_own_bottom_tables() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), tight_config());
    block_on(db.open()).unwrap();
    for i in 0..3u32 {
        for j in 0..30u32 {
            let k = format!("k{i}{j:03}");
            block_on(db.put(k.as_bytes(), &[b'v'; 100])).unwrap();
        }
        block_on(db.flush()).unwrap();
    }
    let before = (0..SMALL_LEVELS)
        .flat_map(|l| db.level_tables(l).unwrap().iter().map(|t| t.id))
        .max()
        .unwrap();
    let mut c = SmallComp::new();
    db.request_compaction(0).unwrap();
    let mut born = Vec::new();
    while db.compaction_pending() {
        block_on(db.compact_step(&mut c)).unwrap();
        let above: usize = (0..SMALL_LEVELS - 1)
            .map(|l| db.level_tables(l).unwrap().len())
            .sum();
        let bottom = db.level_tables(SMALL_LEVELS - 1).unwrap();
        if above == 0 && born.is_empty() {
            born = bottom
                .iter()
                .map(|t| t.id)
                .filter(|&id| id > before)
                .collect();
        }
    }
    assert!(!born.is_empty());
    let last: Vec<_> = db
        .level_tables(SMALL_LEVELS - 1)
        .unwrap()
        .iter()
        .map(|t| t.id)
        .collect();
    assert_eq!(last, born, "the request rewrote its own bottom tables");
}

/// A rewrite's last output keeps its range-tombstone pieces. They must not
/// reach into the next table: a piece of a split output ends at the
/// successor of the output's last key, which is the next table's first
/// key. Found by the lifecycle fuzzer (seed `0x7167a054`).
#[test]
fn a_rewrite_never_overlaps_the_next_table() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), tight_config());
    block_on(db.open()).unwrap();
    let mut c = SmallComp::new();
    let put_all = |db: &mut TestDb<MemDevice<4096>>, c: &mut SmallComp, tag: u8| {
        for i in 0..120u32 {
            let k = format!("k{i:04}");
            if block_on(db.put(k.as_bytes(), &[tag; 100])).is_err() {
                while block_on(db.flush()) == Err(Error::NeedsCompaction) {
                    small_drain(db, c);
                }
                block_on(db.put(k.as_bytes(), &[tag; 100])).unwrap();
            }
        }
    };
    // Old versions, a snapshot, then a range tombstone over all of them
    // and new versions: the snapshot keeps the tombstone and the old
    // versions, so bottom tables split inside the tombstone, and each
    // piece of a split output ends at the next table's first key.
    put_all(&mut db, &mut c, b'o');
    let snap = db.snapshot().unwrap();
    block_on(db.delete_range(b"k", b"l")).unwrap();
    put_all(&mut db, &mut c, b'n');
    while block_on(db.flush()) == Err(Error::NeedsCompaction) {
        small_drain(&mut db, &mut c);
    }
    db.request_compaction(0).unwrap();
    small_drain(&mut db, &mut c);
    assert!(db.level_tables(SMALL_LEVELS - 1).unwrap().len() >= 2);
    // Each full bottom table is rewritten alone, and the snapshot keeps
    // its tombstone piece: its last output must stop at its last key.
    db.request_compaction(SMALL_LEVELS - 1).unwrap();
    small_drain(&mut db, &mut c);
    assert_eq!(db.check_invariants(), Ok(()));
    db.release_snapshot(snap);
    let mut buf = [0u8; 1024];
    for i in [0u32, 60, 119] {
        let k = format!("k{i:04}");
        let n = block_on(db.get(k.as_bytes(), &mut buf)).unwrap().unwrap();
        assert_eq!(&buf[..n], &[b'n'; 100]);
    }
}

/// With one level there is no compaction: a request does nothing.
#[test]
fn a_request_on_one_level_does_nothing() {
    let mut db = OneLevelDb::new(MemDevice::<4096>::new(), test_config());
    block_on(db.open()).unwrap();
    block_on(db.put(b"a", b"v")).unwrap();
    block_on(db.flush()).unwrap();
    db.request_compaction(0).unwrap();
    assert!(!db.compaction_pending());
    assert!(matches!(
        db.request_compaction(1),
        Err(Error::BadLevel { level: 1 })
    ));
}

// ---- Crash sweep over a bottom-level rewrite (the small test shape). ----

type SmallComp = Compaction<4096, 256, 1024, 1024>;
const SMALL_LEVELS: usize = 7;

fn small_drain<D: BlockDevice>(db: &mut TestDb<D>, c: &mut SmallComp)
where
    D::Error: core::fmt::Debug,
{
    for _ in 0..MAX_STEPS {
        if !db.compaction_pending() {
            return;
        }
        block_on(db.compact_step(c)).unwrap();
    }
    panic!("compaction never finished");
}

/// Keys in the crash sweep.
const SMALL_KEYS: u32 = 400;

fn small_key(i: u32) -> (String, u64) {
    if i.is_multiple_of(3) {
        (format!("l{i:04}"), 0)
    } else {
        (format!("e{i:04}"), TTL)
    }
}

/// Live keys `l*`, and keys `e*` that expire at `TTL`, all at the bottom.
fn small_build() -> MemDevice<4096> {
    let mut db = TestDb::new(MemDevice::<4096>::new(), tight_config());
    block_on(db.open()).unwrap();
    let mut c = SmallComp::new();
    for i in 0..SMALL_KEYS {
        let (k, ttl) = small_key(i);
        if block_on(db.put_with_ttl(k.as_bytes(), &[b'v'; 100], ttl)).is_err() {
            while block_on(db.flush()) == Err(Error::NeedsCompaction) {
                small_drain(&mut db, &mut c);
            }
            block_on(db.put_with_ttl(k.as_bytes(), &[b'v'; 100], ttl)).unwrap();
        }
    }
    while block_on(db.flush()) == Err(Error::NeedsCompaction) {
        small_drain(&mut db, &mut c);
    }
    db.request_compaction(0).unwrap();
    small_drain(&mut db, &mut c);
    assert!(db.level_tables(SMALL_LEVELS - 1).unwrap().len() >= 2);
    db.into_device()
}

/// Runs the bottom rewrite with writes `>= crash_at` dropped.
fn small_rewrite<D: BlockDevice>(dev: D) -> D
where
    D::Error: core::fmt::Debug,
{
    let mut db = TestDb::new(dev, tight_config());
    block_on(db.open()).unwrap();
    let mut c = SmallComp::new();
    c.purge_before = LATER;
    db.request_compaction(SMALL_LEVELS - 1).unwrap();
    small_drain(&mut db, &mut c);
    db.into_device()
}

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
        cx: &mut core::task::Context<'_>,
        id: u64,
        buf: &mut [u8],
    ) -> core::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_read_block(cx, id, buf)
    }

    fn poll_write_block(
        &mut self,
        cx: &mut core::task::Context<'_>,
        id: u64,
        buf: &[u8],
    ) -> core::task::Poll<Result<(), Self::Error>> {
        self.writes += 1;
        self.inner.poll_write_block(cx, id, buf)
    }

    fn poll_flush(
        &mut self,
        cx: &mut core::task::Context<'_>,
    ) -> core::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_flush(cx)
    }
}

/// Every crash point of a bottom-level rewrite recovers to a consistent
/// tree with every live key and no expired key visible.
#[test]
fn every_crash_point_of_a_bottom_rewrite_recovers() {
    let built = small_build();
    let counted = small_rewrite(CountDevice {
        inner: built.clone(),
        writes: 0,
    });
    let writes = counted.writes;
    assert!(writes > 0);
    // The clean run frees slots: the sweep crashes a real rewrite.
    let used = |dev: MemDevice<4096>| {
        let mut db = TestDb::new(dev, tight_config());
        block_on(db.open()).unwrap();
        db.slot_stats().used
    };
    assert!(used(counted.inner) < used(built.clone()));
    for crash_at in 0..=writes {
        let d = small_rewrite(CrashDevice::<_, 4096>::new(built.clone(), crash_at));
        let mut db = TestDb::new(d.into_inner(), tight_config());
        block_on(db.open()).unwrap();
        assert_eq!(db.check_invariants(), Ok(()), "crash at {crash_at}");
        let mut buf = [0u8; 1024];
        for i in 0..SMALL_KEYS {
            let (k, ttl) = small_key(i);
            let live = ttl == 0;
            let got = block_on(db.get_with_time(k.as_bytes(), &mut buf, LATER)).unwrap();
            assert_eq!(got.is_some(), live, "crash at {crash_at}, key {k}");
        }
    }
}
