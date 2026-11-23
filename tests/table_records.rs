//! The manifest record describes a table in full (ADR-0021): readers
//! take the table's sections, and its bloom filter's size and probe
//! count, from the record, and never read the footer.
//!
//! RED: every point read began with a read of the table's footer, and
//! every scan cursor and compaction cursor read it to find the index
//! block. These tests pin what replaced that: no read, scan or
//! compaction touches a footer block; a store whose footers are all
//! destroyed reads, scans and compacts as before; a reader built from a
//! record answers as one built from the footer; and a record that
//! misdescribes its table is refused, not followed.

mod common;

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::future::poll_fn;
use std::task::{Context, Poll};

use common::{Lcg, MemDevice, block_on};
use horton::sstable::{BLOOM_BUCKET_BYTES, Lookup};
use horton::{
    BlockDevice, Compaction, Config, Db, Error, Progress, RevScan, Scan, TableReader, TableRef,
};

const BLOCK: usize = 1024;
const KEY_MAX: usize = 32;
/// A filter of up to eight 1 KiB blocks.
const BLOOM: usize = 8 * ((BLOCK - 4) / BLOOM_BUCKET_BYTES) * BLOOM_BUCKET_BYTES;
const DEVICE_BLOCKS: u64 = 16_384;

type TestDb<D> = Db<D, BLOCK, KEY_MAX, 64, 2048, 16_384, 4, 4, BLOOM, 0>;
type TestCompaction = Compaction<BLOCK, KEY_MAX, 64, BLOOM>;
type Reader<'d> = TableReader<'d, Reads, BLOCK, BLOOM>;

/// Records the id of every block read.
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

    fn read(&self, id: u64) -> [u8; BLOCK] {
        let mut buf = [0u8; BLOCK];
        block_on(poll_fn(|cx| self.inner.poll_read_block(cx, id, &mut buf))).unwrap();
        buf
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

const fn key(i: u32) -> [u8; 4] {
    i.to_be_bytes()
}

fn open(dev: Reads) -> Box<TestDb<Reads>> {
    let mut db = Box::new(TestDb::new(dev, Config::whole_device(DEVICE_BLOCKS)));
    block_on(db.open()).unwrap();
    db
}

fn drain(db: &mut TestDb<Reads>) {
    let mut scratch = Box::new(TestCompaction::new());
    while db.compaction_pending() {
        while block_on(db.compact_step(&mut scratch)).unwrap() == Progress::More {}
    }
    db.check_invariants().unwrap();
}

/// Puts the keys `2 * i` for `i` in `range`, shuffled, flushing and
/// compacting as the memtable fills; leaves everything in tables.
fn fill(db: &mut TestDb<Reads>, range: core::ops::Range<u32>, val: &[u8]) {
    let mut order: Vec<u32> = range.collect();
    let mut rng = Lcg::new(0x7AB1_E5EC);
    for i in (1..order.len()).rev() {
        order.swap(i, rng.next_bounded(i + 1));
    }
    for i in order {
        for attempt in 0..2 {
            match block_on(db.put(&key(i * 2), val)) {
                Ok(_) => break,
                Err(Error::TableFull | Error::ArenaFull | Error::WalFull) if attempt == 0 => {
                    block_on(db.flush()).unwrap();
                    drain(db);
                }
                Err(e) => panic!("{e:?}"),
            }
        }
    }
    block_on(db.flush()).unwrap();
    drain(db);
}

fn get(db: &TestDb<Reads>, k: &[u8]) -> Option<Vec<u8>> {
    let mut buf = [0u8; 64];
    block_on(db.get(k, &mut buf))
        .unwrap()
        .map(|n| buf[..n].to_vec())
}

fn tables(db: &TestDb<Reads>) -> Vec<TableRef<KEY_MAX>> {
    (0..4)
        .flat_map(|l| db.level_tables(l).unwrap_or(&[]).to_vec())
        .collect()
}

fn footers(db: &TestDb<Reads>) -> BTreeSet<u64> {
    tables(db)
        .iter()
        .map(|t| t.footer_block().unwrap())
        .collect()
}

fn scan_all(db: &TestDb<Reads>) -> Vec<Vec<u8>> {
    let mut scan = Box::new(Scan::new(db));
    block_on(scan.seek(b"", None, u64::MAX)).unwrap();
    let (mut kb, mut vb) = ([0u8; KEY_MAX], [0u8; 64]);
    let mut out = Vec::new();
    while let Some((kl, _)) = block_on(scan.next(&mut kb, &mut vb)).unwrap() {
        out.push(kb[..kl].to_vec());
    }
    out
}

fn scan_all_rev(db: &TestDb<Reads>) -> Vec<Vec<u8>> {
    let mut scan = Box::new(RevScan::new(db));
    block_on(scan.seek_prev(b"", None, u64::MAX)).unwrap();
    let (mut kb, mut vb) = ([0u8; KEY_MAX], [0u8; 64]);
    let mut out = Vec::new();
    while let Some((kl, _)) = block_on(scan.prev(&mut kb, &mut vb)).unwrap() {
        out.push(kb[..kl].to_vec());
    }
    out
}

/// Asserts that nothing since the last `clear` read any of `footers`.
fn assert_no_footer_read(db: &TestDb<Reads>, footers: &BTreeSet<u64>, what: &str) {
    let log = db.device().log.borrow();
    assert!(!log.is_empty(), "{what} read nothing at all");
    let hit: Vec<u64> = log
        .iter()
        .copied()
        .filter(|id| footers.contains(id))
        .collect();
    assert!(hit.is_empty(), "{what} read footer blocks {hit:?}");
}

/// Point reads, forward and reverse scans, and compaction find every
/// section of a table from its manifest record. None of them reads a
/// footer block.
#[test]
fn reads_scans_and_compaction_never_read_a_footer() {
    const N: u32 = 6_000;
    let mut db = open(Reads::new());
    fill(&mut db, 0..N, b"v");
    let before = footers(&db);
    assert!(before.len() >= 3, "several tables");

    db.device().log.borrow_mut().clear();
    for i in (0..N).step_by(5) {
        assert_eq!(get(&db, &key(i * 2)), Some(b"v".to_vec()));
        assert_eq!(get(&db, &key(i * 2 + 1)), None);
    }
    assert_no_footer_read(&db, &before, "point reads");

    db.device().log.borrow_mut().clear();
    let forward = scan_all(&db);
    assert_eq!(forward.len(), N as usize);
    assert_no_footer_read(&db, &before, "a forward scan");

    db.device().log.borrow_mut().clear();
    let mut backward = scan_all_rev(&db);
    backward.reverse();
    assert_eq!(backward, forward);
    assert_no_footer_read(&db, &before, "a reverse scan");

    // Overwrite a spread of keys and compact everything: the merge reads
    // every input table. Blocks are reused as tables retire, so "a
    // footer" is judged per step, against the tables live when the step
    // began (a step reads only those).
    fill(&mut db, 0..N / 3, b"w");
    db.request_compaction(0).unwrap();
    let mut scratch = Box::new(TestCompaction::new());
    let (mut steps, mut read_blocks) = (0, 0);
    while db.compaction_pending() {
        let live = footers(&db);
        db.device().log.borrow_mut().clear();
        let more = block_on(db.compact_step(&mut scratch)).unwrap();
        let log = db.device().log.borrow();
        let hit: Vec<u64> = log.iter().copied().filter(|id| live.contains(id)).collect();
        assert!(
            hit.is_empty(),
            "a compaction step read footer blocks {hit:?}"
        );
        steps += 1;
        read_blocks += log.len();
        drop(log);
        if more == Progress::Done {
            db.check_invariants().unwrap();
        }
    }
    assert!(
        steps > 10 && read_blocks > 100,
        "{steps} steps, {read_blocks} reads"
    );
    assert_eq!(get(&db, &key(0)), Some(b"w".to_vec()));
    assert_eq!(scan_all(&db).len(), N as usize);
}

/// The strongest form of the same claim: destroy every footer on the
/// device, and the store opens, reads, scans and compacts exactly as it
/// did. What still needs a footer is a table no manifest describes: a
/// copy being re-attached is validated by its footer, and a destroyed
/// one is refused.
#[test]
fn a_store_whose_footers_are_destroyed_still_reads_scans_and_compacts() {
    const N: u32 = 6_000;
    let mut db = open(Reads::new());
    fill(&mut db, 0..N, b"v");
    let want = scan_all(&db);
    let victims = tables(&db);

    let mut dev = db.into_device();
    for t in &victims {
        let id = usize::try_from(t.footer_block().unwrap()).unwrap();
        // Garbage with a CRC that cannot match.
        dev.inner.blocks_mut()[id] = [0xA5; BLOCK];
    }
    let mut db = open(dev);
    db.check_invariants().unwrap();
    assert_eq!(tables(&db), victims);
    for i in (0..N).step_by(3) {
        assert_eq!(get(&db, &key(i * 2)), Some(b"v".to_vec()), "key {i}");
        assert_eq!(get(&db, &key(i * 2 + 1)), None, "absent key {i}");
    }
    assert_eq!(scan_all(&db), want);
    let mut backward = scan_all_rev(&db);
    backward.reverse();
    assert_eq!(backward, want);

    // A table that is re-attached has no record yet: its footer is all
    // there is to check it by, and this one is gone.
    let t = victims[0];
    let level = (0..4)
        .find(|&l| {
            db.level_tables(l)
                .unwrap_or(&[])
                .iter()
                .any(|x| x.id == t.id)
        })
        .unwrap();
    let sealed = db.archive_plan(level, t.id).unwrap().sealed();
    let mut remote = MemDevice::<BLOCK>::new();
    for i in 0..u64::from(t.block_count) {
        let buf = db.device().read(t.first_block + i);
        block_on(poll_fn(|cx| remote.poll_write_block(cx, i, &buf))).unwrap();
    }
    let mut other = open(Reads::new());
    assert!(matches!(
        block_on(other.ingest_table(&sealed, &remote, 0)),
        Err(Error::CorruptBlock { .. })
    ));
    assert!(tables(&other).is_empty(), "nothing was attached");

    // Compaction rewrites the tables (with good footers) from inputs
    // whose footers are gone.
    fill(&mut db, 0..N / 3, b"w");
    db.request_compaction(0).unwrap();
    drain(&mut db);
    assert_eq!(scan_all(&db), want);
    for i in (0..N).step_by(7) {
        let v: &[u8] = if i < N / 3 { b"w" } else { b"v" };
        assert_eq!(get(&db, &key(i * 2)), Some(v.to_vec()), "key {i}");
    }
}

fn lookup(reader: &Reader<'_>, k: &[u8]) -> Result<bool, Error<core::convert::Infallible>> {
    let mut scratch = [0u8; BLOCK];
    let mut decomp = [0u8; BLOCK];
    let mut val = [0u8; 64];
    Ok(matches!(
        block_on(reader.lookup(&mut scratch, &mut decomp, k, &mut val))?,
        Lookup::Value { .. }
    ))
}

/// A reader built from the manifest record costs no I/O and answers
/// every key as the reader built from the table's footer does.
#[test]
fn a_reader_from_the_record_answers_as_one_from_the_footer() {
    const N: u32 = 6_000;
    let mut db = open(Reads::new());
    fill(&mut db, 0..N, b"v");
    let dev = db.device();
    let mut scratch = [0u8; BLOCK];
    let mut checked = 0;
    for t in tables(&db) {
        dev.log.borrow_mut().clear();
        let from_ref = Reader::from_ref(dev, None, &t).unwrap();
        assert!(dev.log.borrow().is_empty(), "from_ref read the device");
        let from_footer =
            block_on(Reader::open(dev, &mut scratch, t.footer_block().unwrap())).unwrap();
        assert_eq!(from_ref.bloom_k(), from_footer.bloom_k());
        assert_eq!(from_ref.bloom_len(), from_footer.bloom_len());
        assert_eq!(from_ref.bloom_blocks(), from_footer.bloom_blocks());
        assert_eq!(from_ref.entry_count(), from_footer.entry_count());
        assert_eq!(from_ref.rdel_blocks(), from_footer.rdel_blocks());
        assert_eq!(from_ref.min_seq(), from_footer.min_seq());
        let first = u32::from_be_bytes(t.first_key.as_slice().try_into().unwrap());
        let last = u32::from_be_bytes(t.last_key.as_slice().try_into().unwrap());
        let step = ((last - first) / 300).max(1) as usize;
        for i in (first..=last + 1).step_by(step) {
            let k = key(i);
            assert_eq!(
                lookup(&from_ref, &k).unwrap(),
                lookup(&from_footer, &k).unwrap(),
                "table {} key {i}",
                t.id
            );
            checked += 1;
        }
    }
    assert!(checked > 600);
}

/// A record is checked before it is followed. One whose counts do not
/// fit its block range, or whose filter length does not fill exactly its
/// bloom blocks, is `CorruptManifest`: a probe must never be sent into
/// an index or data block. A record that does not describe its filter
/// (`bloom_k == 0`) is legal, and its reader simply does not filter.
#[test]
fn a_record_that_misdescribes_its_table_is_refused() {
    const N: u32 = 6_000;
    let mut db = open(Reads::new());
    fill(&mut db, 0..N, b"v");
    let dev = db.device();
    let t = *tables(&db).iter().max_by_key(|t| t.bloom_blocks).unwrap();
    assert!(t.bloom_blocks >= 2 && t.bloom_k >= 1);
    assert!(Reader::from_ref(dev, None, &t).is_ok());

    let refused = |what: &str, change: &dyn Fn(&mut TableRef<KEY_MAX>)| {
        let mut bad = t;
        change(&mut bad);
        assert!(
            matches!(
                Reader::from_ref(dev, None, &bad),
                Err(Error::CorruptManifest)
            ),
            "{what} was accepted"
        );
    };
    refused("a filter longer than its blocks", &|b| b.bloom_len *= 4);
    refused("a filter shorter than its blocks", &|b| b.bloom_len = 64);
    refused("an empty filter in several blocks", &|b| b.bloom_len = 0);
    refused("no bloom block", &|b| b.bloom_blocks = 0);
    refused("more bloom blocks than the filter fills", &|b| {
        b.bloom_blocks += 1;
    });
    refused("more sections than blocks", &|b| b.block_count = 3);
    refused("more tombstone blocks than blocks", &|b| {
        b.rdel_blocks = b.block_count;
    });

    // No description of the filter: every key goes to the index, present
    // or not, and the bloom blocks are not read.
    let mut plain = t;
    plain.bloom_k = 0;
    plain.bloom_len = 0;
    let reader = Reader::from_ref(dev, None, &plain).unwrap();
    let index = t.index_block().unwrap();
    let bloom = t.bloom_first().unwrap()..index;
    let first = u32::from_be_bytes(t.first_key.as_slice().try_into().unwrap());
    for i in 0..200u32 {
        dev.log.borrow_mut().clear();
        // Even keys are in the store (some in this table), odd ones are
        // not: the unfiltered reader agrees with the filtered one.
        let k = key(first + i);
        let want = lookup(&Reader::from_ref(dev, None, &t).unwrap(), &k).unwrap();
        dev.log.borrow_mut().clear();
        assert_eq!(lookup(&reader, &k).unwrap(), want, "key {}", first + i);
        let log = dev.log.borrow();
        assert!(log.contains(&index), "an unfiltered lookup reads the index");
        assert!(!log.iter().any(|id| bloom.contains(id)));
    }
}
