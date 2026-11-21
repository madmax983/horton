//! Detached point reads (ADR-0018): a read split into a memtable probe,
//! taken from the `Db`, and a table phase that runs over a copy of the
//! manifest, any device handle, and caller-owned buffers, without the
//! `Db`.
//!
//! RED: `Db::probe_memtable`, `Db::manifest`, `MemProbe`, `TableView` and
//! `ReadScratch` did not exist. These tests pin the contract: a detached
//! read answers exactly as `get_at_with_time` would have at the instant
//! of the probe, whatever is written to the WAL and memtable afterwards;
//! a probe the memtable settles needs no I/O; and the table phase is
//! `Send`, so many threads can run it at once.

mod common;

use core::cell::{Cell, RefCell};
use core::task::{Context, Poll};

use common::{Lcg, MemDevice, TestDb, block_on, test_config};
use horton::{BlockCache, BlockDevice, CachePort, Compaction, Error, Manifest, Progress};

horton::db_types! {
    block: 4096,
    key_max: 256,
    val_max: 1024,
    memtable_entries: 64,
    memtable_arena: 4096,
    levels: 7,
    tables_per_level: 4,
    bloom_bytes: 1024,
    cache_blocks: 8;

    type TableView = TestTables;
    type MemProbe = TestProbe;
    type ReadScratch = TestScratch;
}

type TestManifest = Manifest<7, 4, 256>;
type TestCompaction = Compaction<4096, 256, 1024, 1024>;

/// What a read returned, comparable across the two paths.
#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Value(Vec<u8>),
    Missing,
    TooSmall(usize),
    Other(String),
}

fn outcome<E: core::fmt::Debug>(r: Result<Option<usize>, Error<E>>, buf: &[u8]) -> Outcome {
    match r {
        Ok(Some(n)) => Outcome::Value(buf[..n].to_vec()),
        Ok(None) => Outcome::Missing,
        Err(Error::BufferTooSmall { need }) => Outcome::TooSmall(need),
        Err(e) => Outcome::Other(format!("{e:?}")),
    }
}

/// The read as `Db` does it.
fn attached<D: BlockDevice>(db: &TestDb<D>, key: &[u8], seq: u64, now: u64, cap: usize) -> Outcome
where
    D::Error: core::fmt::Debug,
{
    let mut buf = vec![0u8; cap];
    let r = block_on(db.get_at_with_time(key, &mut buf, seq, now));
    outcome(r, &buf)
}

/// The same read, detached: the probe from the `Db`, the table phase over
/// `manifest` and `device` with buffers and a cache of its own.
#[allow(clippy::too_many_arguments)] // the read's inputs, spelled out side by side with `attached`.
fn detached<D: BlockDevice, R: BlockDevice>(
    db: &TestDb<D>,
    device: &R,
    manifest: &TestManifest,
    cache: Option<&dyn CachePort<4096>>,
    key: &[u8],
    seq: u64,
    now: u64,
    cap: usize,
) -> Outcome
where
    D::Error: core::fmt::Debug,
    R::Error: core::fmt::Debug,
{
    let probe: TestProbe = db.probe_memtable(key, seq).unwrap();
    let mut scratch = Box::new(TestScratch::new());
    let mut buf = vec![0u8; cap];
    let view = TestTables::new(device, manifest, cache);
    let r = block_on(view.resolve(&probe, &mut scratch, &mut buf, now));
    outcome(r, &buf)
}

fn drain<D: BlockDevice>(db: &mut TestDb<D>)
where
    D::Error: core::fmt::Debug,
{
    let mut scratch = Box::new(TestCompaction::new());
    while db.compaction_pending() {
        while block_on(db.compact_step(&mut scratch)).unwrap() == Progress::More {}
    }
}

fn key(i: usize) -> Vec<u8> {
    format!("key-{i:04}").into_bytes()
}

/// A store with values, overwrites, deletes, a range delete and TTL
/// values spread over the memtable and several levels of tables.
fn populated() -> (TestDb<MemDevice<4096>>, Vec<u64>) {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    block_on(db.open()).unwrap();
    let mut rng = Lcg::new(0xD37A_C4ED);
    let mut marks = Vec::new();
    for round in 0..9usize {
        for _ in 0..40 {
            let k = key(rng.next_bounded(120));
            let rng_choice = rng.next_bounded(10);
            let ttl_val = format!("ttl-{round}").into_bytes();
            let val = vec![b'a' + u8::try_from(round).unwrap(); 1 + rng.next_bounded(300)];
            // A full memtable is flushed and the write tried again.
            for attempt in 0..2 {
                let r = match rng_choice {
                    0 => block_on(db.delete(&k)),
                    1 => block_on(db.put_with_ttl(&k, &ttl_val, 50 + round as u64 * 10)),
                    _ => block_on(db.put(&k, &val)),
                };
                match r {
                    Ok(_) => break,
                    Err(Error::TableFull | Error::ArenaFull | Error::WalFull) if attempt == 0 => {
                        block_on(db.flush()).unwrap();
                        drain(&mut db);
                    }
                    Err(e) => panic!("{e:?}"),
                }
            }
        }
        if round == 4 {
            block_on(db.delete_range(&key(30), &key(45))).unwrap();
        }
        // A watermark, not a pin: at most eight snapshots can be live.
        let mark = db.snapshot().unwrap();
        db.release_snapshot(mark);
        marks.push(mark);
        // The last round stays in the memtable.
        if round < 8 {
            block_on(db.flush()).unwrap();
            drain(&mut db);
        }
    }
    (db, marks)
}

/// The whole point: for every key, at every snapshot, with and without a
/// clock, with a buffer that fits and one that does not, the detached
/// read is the attached read.
#[test]
fn a_detached_read_answers_as_the_db_does() {
    let (db, marks) = populated();
    let manifest = *db.manifest();
    assert!(
        (0..7)
            .filter(|&l| !manifest.level(l).unwrap_or(&[]).is_empty())
            .count()
            >= 2,
        "the fixture should spread tables over levels"
    );
    let cache = RefCell::new(BlockCache::<4096, 8>::new());
    let mut seqs = marks;
    seqs.extend([0, 1, u64::MAX]);
    let (mut values, mut missing) = (0, 0);
    for i in 0..125 {
        let k = key(i);
        // Every key at the latest view; every fifth key through the whole
        // matrix of snapshots, clocks and buffer sizes.
        let full = i % 5 == 0;
        for &seq in seqs.iter().filter(|&&s| full || s == u64::MAX) {
            for now in [0u64, 75, 10_000].into_iter().filter(|&n| full || n == 0) {
                for cap in [1024usize, 3] {
                    let want = attached(&db, &k, seq, now, cap);
                    // With a cache and without one.
                    let got =
                        detached(&db, db.device(), &manifest, Some(&cache), &k, seq, now, cap);
                    assert_eq!(got, want, "key {i} seq {seq} now {now} cap {cap} (cached)");
                    let got = detached(&db, db.device(), &manifest, None, &k, seq, now, cap);
                    assert_eq!(got, want, "key {i} seq {seq} now {now} cap {cap}");
                    match want {
                        Outcome::Value(_) => values += 1,
                        Outcome::Missing => missing += 1,
                        Outcome::TooSmall(_) => {}
                        Outcome::Other(e) => panic!("unexpected error {e}"),
                    }
                }
            }
        }
    }
    assert!(
        values > 150 && missing > 150,
        "{values} values, {missing} missing"
    );
    assert!(
        cache.borrow().stats().hits > 0,
        "the caller's cache was used"
    );
}

/// The answer is the store as of the probe. Writes that reach only the
/// WAL and the memtable afterwards (which is every `put`, `delete`,
/// `write` and `delete_range`) change nothing for a read already probed:
/// that is what lets the owner keep writing while table phases run.
#[test]
fn writes_after_the_probe_do_not_change_the_answer() {
    let (mut db, _) = populated();
    let manifest = *db.manifest();
    let device = db.device().clone();
    let mut probes = Vec::new();
    let mut before = Vec::new();
    for i in 0..125 {
        let k = key(i);
        before.push(attached(&db, &k, u64::MAX, 0, 1024));
        probes.push(db.probe_memtable(&k, u64::MAX).unwrap());
    }
    // Overwrite and delete everything, staying inside the memtable.
    let mut changed = 0;
    for i in (0..125).rev() {
        let r = if i % 3 == 0 {
            block_on(db.delete(&key(i)))
        } else {
            block_on(db.put(&key(i), b"after"))
        };
        match r {
            Ok(_) => changed += 1,
            Err(Error::TableFull | Error::ArenaFull | Error::WalFull) => break,
            Err(e) => panic!("{e:?}"),
        }
    }
    assert!(changed > 20);
    assert_eq!(
        db.manifest().seq(),
        manifest.seq(),
        "no write touched the tables"
    );

    let mut scratch = Box::new(TestScratch::new());
    let view = TestTables::new(&device, &manifest, None);
    for (i, probe) in probes.iter().enumerate() {
        let mut buf = [0u8; 1024];
        let r = block_on(view.resolve(probe, &mut scratch, &mut buf, 0));
        assert_eq!(outcome(r, &buf), before[i], "key {i}");
    }
    // A fresh probe sees the writes.
    assert_eq!(
        detached(&db, &device, &manifest, None, &key(124), u64::MAX, 0, 1024),
        Outcome::Value(b"after".to_vec())
    );
}

/// A device that counts reads.
struct Counting {
    inner: MemDevice<4096>,
    reads: Cell<u64>,
}

impl BlockDevice for Counting {
    type Error = core::convert::Infallible;
    const BLOCK: usize = 4096;

    fn poll_read_block(
        &self,
        cx: &mut Context<'_>,
        id: u64,
        buf: &mut [u8],
    ) -> Poll<Result<(), Self::Error>> {
        self.reads.set(self.reads.get() + 1);
        self.inner.poll_read_block(cx, id, buf)
    }

    fn poll_write_block(
        &mut self,
        cx: &mut Context<'_>,
        id: u64,
        buf: &[u8],
    ) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_write_block(cx, id, buf)
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_flush(cx)
    }
}

/// A key whose newest version is in the memtable is settled by the
/// probe: the table phase reads nothing, however many tables cover it.
#[test]
fn a_probe_the_memtable_settles_reads_no_table() {
    let (mut db, _) = populated();
    block_on(db.put(&key(7), b"fresh")).unwrap();
    block_on(db.delete(&key(8))).unwrap();
    let manifest = *db.manifest();
    let device = Counting {
        inner: db.device().clone(),
        reads: Cell::new(0),
    };
    let view = TestTables::new(&device, &manifest, None);
    let mut scratch = Box::new(TestScratch::new());
    let mut buf = [0u8; 1024];

    let put = db.probe_memtable(&key(7), u64::MAX).unwrap();
    assert!(put.is_settled());
    let r = block_on(view.resolve(&put, &mut scratch, &mut buf, 0));
    assert_eq!(outcome(r, &buf), Outcome::Value(b"fresh".to_vec()));

    let deleted = db.probe_memtable(&key(8), u64::MAX).unwrap();
    assert!(deleted.is_settled());
    let r = block_on(view.resolve(&deleted, &mut scratch, &mut buf, 0));
    assert_eq!(outcome(r, &buf), Outcome::Missing);
    assert_eq!(device.reads.get(), 0, "a settled probe read a table");

    // A key the memtable does not hold is not settled, and reads tables.
    let cold = (0..125)
        .map(key)
        .find(|k| !db.probe_memtable(k, u64::MAX).unwrap().is_settled())
        .expect("some key is only in tables");
    let probe = db.probe_memtable(&cold, u64::MAX).unwrap();
    let r = block_on(view.resolve(&probe, &mut scratch, &mut buf, 0));
    assert_eq!(outcome(r, &buf), attached(&db, &cold, u64::MAX, 0, 1024));
    assert!(device.reads.get() > 0);
}

/// The table phase holds nothing of the `Db`, and everything it does hold
/// can cross threads: eight threads resolve probes over one manifest copy
/// and one shared device while the owner goes on writing.
#[test]
fn table_phases_run_on_many_threads_while_the_owner_writes() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<TestManifest>();
    assert_send_sync::<TestProbe>();
    assert_send_sync::<TestScratch>();

    let (mut db, _) = populated();
    let manifest = *db.manifest();
    let device = db.device().clone();
    let work: Vec<(TestProbe, Outcome)> = (0..125)
        .map(|i| {
            let k = key(i);
            (
                db.probe_memtable(&k, u64::MAX).unwrap(),
                attached(&db, &k, u64::MAX, 0, 1024),
            )
        })
        .collect();

    std::thread::scope(|s| {
        for t in 0..8usize {
            let (work, device, manifest) = (&work, &device, &manifest);
            s.spawn(move || {
                // Buffers and a cache per thread; the device and the
                // manifest copy are shared.
                let cache = RefCell::new(BlockCache::<4096, 8>::new());
                let mut scratch = Box::new(TestScratch::new());
                let view = TestTables::new(device, manifest, Some(&cache));
                for round in 0..20 {
                    for (i, (probe, want)) in work.iter().enumerate().skip((t + round) % 3) {
                        let mut buf = [0u8; 1024];
                        let r = block_on(view.resolve(probe, &mut scratch, &mut buf, 0));
                        assert_eq!(&outcome(r, &buf), want, "thread {t} key {i}");
                    }
                }
            });
        }
        // Meanwhile the owner writes (WAL and memtable only).
        for i in 0..40 {
            match block_on(db.put(&key(i), b"meanwhile")) {
                Ok(_) | Err(Error::TableFull | Error::ArenaFull | Error::WalFull) => {}
                Err(e) => panic!("{e:?}"),
            }
        }
    });
}

#[test]
fn a_probe_needs_an_open_db_and_a_key_that_fits() {
    let db = TestDb::new(MemDevice::<4096>::new(), test_config());
    assert!(matches!(
        db.probe_memtable(b"k", u64::MAX),
        Err(Error::NotOpen)
    ));
    let mut db = db;
    block_on(db.open()).unwrap();
    // The probe carries its key, so the table phase cannot be handed a
    // different one; a key longer than any stored key is refused here.
    let long = vec![b'k'; 257];
    assert!(matches!(
        db.probe_memtable(&long, u64::MAX),
        Err(Error::KeyTooLarge { len: 257, max: 256 })
    ));
    let probe = db.probe_memtable(&[b'k'; 256], u64::MAX).unwrap();
    assert_eq!(probe.key(), &[b'k'; 256][..]);
    assert!(!probe.is_settled());
    // An empty store: the table phase has nothing to read.
    let manifest = *db.manifest();
    let mut scratch = Box::new(TestScratch::new());
    let mut buf = [0u8; 8];
    let view = TestTables::new(db.device(), &manifest, None);
    assert_eq!(
        block_on(view.resolve(&probe, &mut scratch, &mut buf, 0)).unwrap(),
        None
    );
}
