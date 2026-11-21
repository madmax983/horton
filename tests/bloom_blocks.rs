//! Bloom filters that span blocks (ADR-0020), through the `Db`.
//!
//! RED: a table's filter was one block whatever the table held, so a
//! large table's filter was starved and an absent key read an index and
//! a data block from every deep table. These tests pin what replaced it:
//! every table a flush or a compaction writes carries the bloom blocks
//! its entry count calls for, the manifest records that count across a
//! reopen, a table keeps its filter through archive and re-attach, and
//! absent keys are turned away at the filter.

mod common;

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::future::poll_fn;
use std::task::{Context, Poll};

use common::{Lcg, MemDevice, block_on};
use horton::sstable::{BLOOM_BITS_PER_KEY, BLOOM_BUCKET_BYTES, BloomShape};
use horton::{BlockDevice, Compaction, Config, Db, Error, Progress, Scan, TableReader, TableRef};

const BLOCK: usize = 1024;
const KEY_MAX: usize = 32;
/// Buckets in a 1 KiB bloom block.
const PER_BLOCK: usize = (BLOCK - 4) / BLOOM_BUCKET_BYTES;
/// A filter of up to eight blocks.
const WIDE: usize = 8 * PER_BLOCK * BLOOM_BUCKET_BYTES;
/// The same store with the one-block filter every table used to get.
const NARROW: usize = PER_BLOCK * BLOOM_BUCKET_BYTES;
const DEVICE_BLOCKS: u64 = 16_384;

/// 1 KiB blocks, 4-byte keys in a 2,048-entry memtable, 4 levels of 4
/// tables. A table's index is one block, which here is about 2,500
/// entries: enough for a four-block filter.
type TestDb<D, const BLOOM: usize> = Db<D, BLOCK, KEY_MAX, 64, 2048, 16_384, 4, 4, BLOOM, 0>;
type TestCompaction<const BLOOM: usize> = Compaction<BLOCK, KEY_MAX, 64, BLOOM>;

/// Records the id of every block read.
#[derive(Clone)]
struct Reads {
    inner: MemDevice<BLOCK>,
    log: RefCell<Vec<u64>>,
}

impl Reads {
    fn new() -> Self {
        Self {
            inner: MemDevice::new(),
            log: RefCell::new(Vec::new()),
        }
    }
}

impl BlockDevice for Reads {
    type Error = core::convert::Infallible;
    const BLOCK: usize = BLOCK;

    fn poll_read_block(
        &self,
        cx: &mut Context<'_>,
        id: u64,
        buf: &mut [u8],
    ) -> Poll<Result<(), Self::Error>> {
        self.log.borrow_mut().push(id);
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

/// Key `i`: four bytes, in numeric order.
const fn key(i: u32) -> [u8; 4] {
    i.to_be_bytes()
}

fn drain<D: BlockDevice, const BLOOM: usize>(db: &mut TestDb<D, BLOOM>)
where
    D::Error: core::fmt::Debug,
{
    let mut scratch = Box::new(TestCompaction::<BLOOM>::new());
    while db.compaction_pending() {
        while block_on(db.compact_step(&mut scratch)).unwrap() == Progress::More {
            db.check_invariants().unwrap();
        }
    }
}

/// Puts the even keys below `2 * n`, in a shuffled order, flushing and
/// compacting as the memtable fills, and leaves everything in tables.
fn fill<D: BlockDevice, const BLOOM: usize>(db: &mut TestDb<D, BLOOM>, n: u32)
where
    D::Error: core::fmt::Debug,
{
    let mut order: Vec<u32> = (0..n).collect();
    let mut rng = Lcg::new(0xB100_F117);
    for i in (1..order.len()).rev() {
        order.swap(i, rng.next_bounded(i + 1));
    }
    for i in order {
        let k = key(i * 2);
        for attempt in 0..2 {
            match block_on(db.put(&k, b"v")) {
                Ok(_) => break,
                Err(Error::TableFull | Error::ArenaFull | Error::WalFull) if attempt == 0 => {
                    block_on(db.flush()).unwrap();
                    db.check_invariants().unwrap();
                    drain(db);
                }
                Err(e) => panic!("{e:?}"),
            }
        }
    }
    block_on(db.flush()).unwrap();
    drain(db);
    db.check_invariants().unwrap();
}

fn open<const BLOOM: usize>(dev: Reads) -> Box<TestDb<Reads, BLOOM>> {
    let mut db = Box::new(TestDb::<Reads, BLOOM>::new(
        dev,
        Config::whole_device(DEVICE_BLOCKS),
    ));
    block_on(db.open()).unwrap();
    db
}

fn get<D: BlockDevice, const BLOOM: usize>(db: &TestDb<D, BLOOM>, k: &[u8]) -> Option<Vec<u8>>
where
    D::Error: core::fmt::Debug,
{
    let mut buf = [0u8; 64];
    block_on(db.get(k, &mut buf))
        .unwrap()
        .map(|n| buf[..n].to_vec())
}

fn tables<D: BlockDevice, const BLOOM: usize>(db: &TestDb<D, BLOOM>) -> Vec<TableRef<KEY_MAX>> {
    (0..4)
        .flat_map(|l| db.level_tables(l).unwrap_or(&[]).to_vec())
        .collect()
}

/// Checks every table against the rule and its own footer; returns the
/// set of bloom-block counts seen.
fn check_tables<const BLOOM: usize>(db: &TestDb<Reads, BLOOM>) -> BTreeSet<u32> {
    let mut seen = BTreeSet::new();
    let mut scratch = [0u8; BLOCK];
    for t in tables(db) {
        let shape = BloomShape::for_entries(BLOOM, BLOCK, u64::from(t.entry_count));
        assert_eq!(
            t.bloom_blocks, shape.blocks,
            "table {} ({} entries)",
            t.id, t.entry_count
        );
        assert!(
            shape == BloomShape::full(BLOOM, BLOCK)
                || shape.len as u64 * 8 >= u64::from(t.entry_count) * BLOOM_BITS_PER_KEY
        );
        // The manifest's count is the table's own: data, tombstones,
        // filter, index and footer account for every block.
        let data = t.data_blocks().unwrap();
        assert_eq!(
            data + u64::from(t.rdel_blocks) + u64::from(t.bloom_blocks) + 2,
            u64::from(t.block_count)
        );
        let footer = t.first_block + u64::from(t.block_count) - 1;
        let reader = block_on(TableReader::<Reads, BLOCK, BLOOM>::open(
            db.device(),
            &mut scratch,
            footer,
        ))
        .unwrap();
        assert_eq!(reader.bloom_blocks(), u64::from(t.bloom_blocks));
        assert_eq!(reader.bloom_len(), shape.len);
        assert_eq!(reader.entry_count(), u64::from(t.entry_count));
        seen.insert(t.bloom_blocks);
    }
    seen
}

fn scan_count<const BLOOM: usize>(db: &TestDb<Reads, BLOOM>) -> u32 {
    let mut scan = Box::new(Scan::new(db));
    block_on(scan.seek(b"", None, u64::MAX)).unwrap();
    let (mut kb, mut vb) = ([0u8; KEY_MAX], [0u8; 64]);
    let mut n = 0;
    let mut last: Option<Vec<u8>> = None;
    while let Some((kl, _)) = block_on(scan.next(&mut kb, &mut vb)).unwrap() {
        assert!(last.as_deref().is_none_or(|l| l < &kb[..kl]), "scan order");
        last = Some(kb[..kl].to_vec());
        n += 1;
    }
    n
}

/// Every table a flush or a compaction writes carries the bloom blocks
/// its entry count calls for, small tables one and large tables several;
/// the manifest keeps the count across a reopen; and nothing is lost or
/// invented on the way.
#[test]
fn flushed_and_compacted_tables_carry_the_filter_their_size_calls_for() {
    const N: u32 = 12_000;
    let mut db = open::<WIDE>(Reads::new());
    fill(&mut db, N);
    // A last small flush, left in level 0.
    for i in 0..100u32 {
        block_on(db.put(&key(2 * N + 2 * i), b"v")).unwrap();
    }
    block_on(db.flush()).unwrap();
    db.check_invariants().unwrap();

    let seen = check_tables(&db);
    assert!(
        seen.contains(&1),
        "a small table has a one-block filter: {seen:?}"
    );
    assert!(
        seen.iter().any(|&b| b >= 3),
        "a large table has a filter of several blocks: {seen:?}"
    );

    let verify = |db: &TestDb<Reads, WIDE>| {
        for i in (0..N + 100).step_by(7) {
            assert_eq!(get(db, &key(i * 2)), Some(b"v".to_vec()), "key {i}");
            assert_eq!(get(db, &key(i * 2 + 1)), None, "absent key {i}");
        }
        assert_eq!(scan_count(db), N + 100);
    };
    verify(&db);

    // The bloom-block counts come back from the manifest, not from a
    // rule applied at open.
    let before = tables(&db);
    let dev = db.into_device();
    let db = open::<WIDE>(dev);
    assert_eq!(tables(&db), before);
    db.check_invariants().unwrap();
    assert_eq!(check_tables(&db), seen);
    verify(&db);
}

/// The index blocks of the store's tables.
fn index_blocks<D: BlockDevice, const BLOOM: usize>(db: &TestDb<D, BLOOM>) -> BTreeSet<u64> {
    tables(db)
        .iter()
        .map(|t| t.first_block + u64::from(t.block_count) - 2)
        .collect()
}

/// How many of `asked` absent-key reads went past a bloom filter to an
/// index block, per table probed.
fn false_positive_percent<const BLOOM: usize>(n: u32) -> (u64, usize) {
    let mut db = open::<BLOOM>(Reads::new());
    fill(&mut db, n);
    let index = index_blocks(&db);
    let ntables = tables(&db).len();
    db.device().log.borrow_mut().clear();
    let asked = 2_000u32;
    for i in 0..asked {
        // Odd keys: absent, and inside the key range of the tables.
        assert_eq!(get(&db, &key((i * 5) % n * 2 + 1)), None);
    }
    let log = db.device().log.borrow();
    let passed = log.iter().filter(|id| index.contains(id)).count() as u64;
    (passed * 100 / u64::from(asked), ntables)
}

/// The reason for all of this. With a one-block filter, the large tables
/// that hold most of a store pass a good share of absent keys through to
/// an index and a data block. With a filter sized to the table, nearly
/// all are turned away.
#[test]
fn absent_keys_stop_at_the_filter_of_a_large_table() {
    const N: u32 = 12_000;
    let (wide, tables_wide) = false_positive_percent::<WIDE>(N);
    let (narrow, tables_narrow) = false_positive_percent::<NARROW>(N);
    assert!(tables_wide >= 4 && tables_narrow >= 4, "several tables");
    // Percent of reads that reached an index block somewhere; a read
    // probes one table per level here.
    assert!(
        wide <= 5,
        "sized filters let {wide}% of absent reads through"
    );
    assert!(
        narrow >= 4 * wide.max(5),
        "one-block filters let only {narrow}% through; the contrast is gone"
    );
}

/// A table with a filter of several blocks survives archive and
/// re-attach into another store, at another place on the device: the
/// relocation finds the index and footer behind however many bloom
/// blocks the table has, and the re-attached table's manifest entry
/// names them.
#[test]
fn a_table_with_several_bloom_blocks_survives_archive_and_reattach() {
    const N: u32 = 6_000;
    let mut src = open::<WIDE>(Reads::new());
    fill(&mut src, N);
    let (level, t) = (0..4)
        .flat_map(|l| {
            src.level_tables(l)
                .unwrap_or(&[])
                .iter()
                .map(move |t| (l, *t))
        })
        .max_by_key(|(_, t)| t.bloom_blocks)
        .unwrap();
    assert!(
        t.bloom_blocks >= 2,
        "want a table with several bloom blocks"
    );

    // Upload its blocks, as an archiver would.
    let plan = src.archive_plan(level, t.id).unwrap();
    let sealed = plan.sealed();
    let mut remote = MemDevice::<BLOCK>::new();
    for i in 0..u64::from(t.block_count) {
        let mut buf = [0u8; BLOCK];
        block_on(poll_fn(|cx| {
            src.device()
                .poll_read_block(cx, t.first_block + i, &mut buf)
        }))
        .unwrap();
        block_on(poll_fn(|cx| remote.poll_write_block(cx, i, &buf))).unwrap();
    }

    // A second store, holding other keys, so the table lands elsewhere.
    let mut dst = open::<WIDE>(Reads::new());
    for i in 0..300u32 {
        block_on(dst.put(&key(1_000_000 + i), b"d")).unwrap();
    }
    block_on(dst.flush()).unwrap();
    assert!(block_on(dst.ingest_table(&sealed, &remote, 0)).unwrap());
    dst.check_invariants().unwrap();
    let got = *dst
        .level_tables(0)
        .unwrap()
        .iter()
        .find(|x| x.id == t.id)
        .unwrap();
    assert_ne!(got.first_block, t.first_block, "the table moved");
    assert_eq!(got.bloom_blocks, t.bloom_blocks);
    assert_eq!(got.data_blocks(), t.data_blocks());
    check_tables(&dst);

    // Its keys read back through the relocated index (other tables of
    // the source hold the rest of the range), and its filter still
    // turns absent keys away.
    let first = u32::from_be_bytes(t.first_key.as_slice().try_into().unwrap());
    let last = u32::from_be_bytes(t.last_key.as_slice().try_into().unwrap());
    let index = got.first_block + u64::from(got.block_count) - 2;
    let (mut found, mut passed, mut asked) = (0u32, 0u32, 0u32);
    for i in (first..=last).step_by(2) {
        if let Some(v) = get(&dst, &key(i)) {
            assert_eq!(Some(v), get(&src, &key(i)), "key {i}");
            found += 1;
        }
        dst.device().log.borrow_mut().clear();
        assert_eq!(get(&dst, &key(i + 1)), None);
        asked += 1;
        passed += u32::from(dst.device().log.borrow().contains(&index));
    }
    assert_eq!(found, t.entry_count);
    assert!(
        passed * 100 <= asked * 5,
        "{passed} of {asked} absent keys passed"
    );
}
