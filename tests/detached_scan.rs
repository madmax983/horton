//! Detached scans (ADR-0019): a scan that does not borrow the `Db`. It
//! merges a copy of part of the memtable (`Db::copy_memtable_range`)
//! with the tables a manifest copy names, read through any device
//! handle.
//!
//! RED: `Scan::detached`, `RevScan::detached`, `MemTable::copy_range`,
//! `copy_range_rev` and the `Db` wrappers did not exist. These tests pin
//! the contract: paged detached scans yield exactly what a scan over the
//! `Db` yields, in both directions, for every page size and page shape;
//! the horizon is where a page must stop; writes after the copy change
//! nothing for a page already taken; and scans run on many threads.

mod common;

use common::{Lcg, MemDevice, TestDb, block_on, test_config};
use horton::{BlockDevice, Compaction, Error, Manifest, MemTable, Progress, RevScan, Scan};

type TestManifest = Manifest<7, 4, 256>;
type TestCompaction = Compaction<4096, 256, 1024, 1024>;
/// A scan whose memtable has `CAP` slots over an `ARENA`-byte arena: the
/// `Db`'s own shape for attached scans, any shape for detached ones.
type ScanOver<'d, D, const CAP: usize, const ARENA: usize> =
    Scan<'d, D, 4096, 256, 1024, CAP, ARENA, 7, 4, 1024, 8>;
type RevScanOver<'d, D, const CAP: usize, const ARENA: usize> =
    RevScan<'d, D, 4096, 256, 1024, CAP, ARENA, 7, 4, 1024, 8>;
type Page<const CAP: usize, const ARENA: usize> = MemTable<CAP, ARENA, 256, 1024>;

type Entries = Vec<(Vec<u8>, Vec<u8>)>;

fn key(i: usize) -> Vec<u8> {
    format!("key-{i:04}").into_bytes()
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

/// A store with values, overwrites, deletes, range deletes and TTL
/// values spread over several levels of tables and a memtable that
/// holds several versions of some keys and a range tombstone.
fn populated() -> (TestDb<MemDevice<4096>>, Vec<u64>) {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    block_on(db.open()).unwrap();
    let mut rng = Lcg::new(0x5CA2_D37A);
    let mut marks = Vec::new();
    for round in 0..8usize {
        for n in 0..36 {
            let k = key(rng.next_bounded(150));
            let choice = rng.next_bounded(10);
            let val = vec![b'a' + u8::try_from(round).unwrap(); 1 + rng.next_bounded(60)];
            for attempt in 0..2 {
                let r = match choice {
                    0 => block_on(db.delete(&k)),
                    1 => block_on(db.put_with_ttl(&k, &val, 50 + round as u64 * 10)),
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
            if n == 20 && round % 3 == 1 {
                let from = rng.next_bounded(130);
                block_on(db.delete_range(&key(from), &key(from + 9))).unwrap();
            }
        }
        let mark = db.snapshot().unwrap();
        db.release_snapshot(mark);
        marks.push(mark);
        // The last round stays in the memtable, tombstone and all.
        if round < 7 {
            block_on(db.flush()).unwrap();
            drain(&mut db);
        }
    }
    block_on(db.delete_range(&key(70), &key(76))).unwrap();
    for i in [3, 3, 3, 71, 99] {
        block_on(db.put(&key(i), b"again")).unwrap();
    }
    (db, marks)
}

/// Everything a scan over the `Db` yields for `[start, end)`.
fn attached<D: BlockDevice>(
    db: &TestDb<D>,
    start: &[u8],
    end: Option<&[u8]>,
    seq: u64,
    now: u64,
) -> Entries
where
    D::Error: core::fmt::Debug,
{
    let mut scan = Box::new(ScanOver::<D, 64, 4096>::new(db));
    block_on(scan.seek_with_time(start, end, seq, now)).unwrap();
    let (mut k, mut v) = ([0u8; 256], [0u8; 1024]);
    let mut out = Vec::new();
    while let Some((kl, vl)) = block_on(scan.next(&mut k, &mut v)).unwrap() {
        out.push((k[..kl].to_vec(), v[..vl].to_vec()));
    }
    out
}

/// The same, descending from `from` to above `lower`.
fn attached_rev<D: BlockDevice>(
    db: &TestDb<D>,
    from: &[u8],
    lower: Option<&[u8]>,
    seq: u64,
    now: u64,
) -> Entries
where
    D::Error: core::fmt::Debug,
{
    let mut scan = Box::new(RevScanOver::<D, 64, 4096>::new(db));
    block_on(scan.seek_prev_with_time(from, lower, seq, now)).unwrap();
    let (mut k, mut v) = ([0u8; 256], [0u8; 1024]);
    let mut out = Vec::new();
    while let Some((kl, vl)) = block_on(scan.prev(&mut k, &mut v)).unwrap() {
        out.push((k[..kl].to_vec(), v[..vl].to_vec()));
    }
    out
}

/// The forward scan, detached and paged: each page copies at most
/// `max_keys` memtable keys into a `Page<CAP, ARENA>` and scans up to the
/// copy's horizon. Returns the entries and the number of pages.
fn detached<D: BlockDevice, R: BlockDevice, const CAP: usize, const ARENA: usize>(
    db: &TestDb<D>,
    device: &R,
    manifest: &TestManifest,
    (start, end): (&[u8], Option<&[u8]>),
    (seq, now): (u64, u64),
    max_keys: usize,
) -> (Entries, usize)
where
    D::Error: core::fmt::Debug,
    R::Error: core::fmt::Debug,
{
    let mut page = Box::new(Page::<CAP, ARENA>::new());
    let (mut k, mut v) = ([0u8; 256], [0u8; 1024]);
    let mut out = Vec::new();
    let mut from = start.to_vec();
    let mut pages = 0;
    loop {
        pages += 1;
        let horizon = db
            .copy_memtable_range(&from, end, max_keys, &mut page)
            .unwrap();
        let stop = horizon.as_ref().map(horton::KeyBound::as_slice).or(end);
        let mut scan = Box::new(ScanOver::<R, CAP, ARENA>::detached(
            &page,
            manifest,
            device,
            None,
            db.node_id(),
        ));
        block_on(scan.seek_with_time(&from, stop, seq, now)).unwrap();
        while let Some((kl, vl)) = block_on(scan.next(&mut k, &mut v)).unwrap() {
            out.push((k[..kl].to_vec(), v[..vl].to_vec()));
        }
        match horizon {
            // The next page starts at the horizon, which this one stopped
            // short of.
            Some(h) => {
                assert!(h.as_slice() >= from.as_slice(), "a horizon behind its page");
                from = h.as_slice().to_vec();
            }
            None => return (out, pages),
        }
        assert!(pages < 10_000, "paging does not make progress");
    }
}

/// The reverse scan, detached and paged.
fn detached_rev<D: BlockDevice, R: BlockDevice, const CAP: usize, const ARENA: usize>(
    db: &TestDb<D>,
    device: &R,
    manifest: &TestManifest,
    (from, lower): (&[u8], Option<&[u8]>),
    (seq, now): (u64, u64),
    max_keys: usize,
) -> (Entries, usize)
where
    D::Error: core::fmt::Debug,
    R::Error: core::fmt::Debug,
{
    let mut page = Box::new(Page::<CAP, ARENA>::new());
    let (mut k, mut v) = ([0u8; 256], [0u8; 1024]);
    let mut out = Vec::new();
    let mut from = from.to_vec();
    let mut pages = 0;
    loop {
        pages += 1;
        let horizon = db
            .copy_memtable_range_rev(&from, lower, max_keys, &mut page)
            .unwrap();
        let stop = horizon.as_ref().map(horton::KeyBound::as_slice).or(lower);
        let mut scan = Box::new(RevScanOver::<R, CAP, ARENA>::detached(
            &page,
            manifest,
            device,
            None,
            db.node_id(),
        ));
        block_on(scan.seek_prev_with_time(&from, stop, seq, now)).unwrap();
        while let Some((kl, vl)) = block_on(scan.prev(&mut k, &mut v)).unwrap() {
            out.push((k[..kl].to_vec(), v[..vl].to_vec()));
        }
        match horizon {
            // The next page starts at the horizon (inclusive), which
            // this one stopped above.
            Some(h) => from = h.as_slice().to_vec(),
            None => return (out, pages),
        }
        assert!(pages < 10_000, "paging does not make progress");
    }
}

/// Ranges that start and end in tables, in the memtable, on a range
/// tombstone, on absent keys, and nowhere.
fn ranges() -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
    vec![
        (Vec::new(), None),
        (key(0), Some(key(150))),
        (key(3), Some(key(4))),
        (key(40), Some(key(90))),
        (key(72), Some(key(74))),
        (b"key-0070x".to_vec(), Some(b"key-0099x".to_vec())),
        (key(120), None),
        (key(149), None),
        (key(200), None),
        (key(90), Some(key(40))),
    ]
}

/// The whole point, forward: whatever the range, the snapshot, the clock
/// and the page size, a paged detached scan yields what a scan over the
/// `Db` yields.
#[test]
fn a_detached_scan_yields_what_a_scan_of_the_db_yields() {
    let (db, marks) = populated();
    let manifest = *db.manifest();
    let (mut entries, mut multi_page) = (0, 0);
    for (start, end) in ranges() {
        let end = end.as_deref();
        for seq in [u64::MAX, marks[3], marks[6], 1] {
            for now in [0u64, 85] {
                let want = attached(&db, &start, end, seq, now);
                for max_keys in [1usize, 3, 10_000] {
                    let (got, pages) = detached::<_, _, 64, 4096>(
                        &db,
                        db.device(),
                        &manifest,
                        (&start, end),
                        (seq, now),
                        max_keys,
                    );
                    assert_eq!(
                        got, want,
                        "{start:?}..{end:?} seq {seq} now {now} keys {max_keys}"
                    );
                    multi_page += usize::from(pages > 1);
                }
                entries += want.len();
            }
        }
    }
    assert!(entries > 1000, "the fixture yields too little: {entries}");
    assert!(multi_page > 20, "paging was barely exercised: {multi_page}");
}

/// The same, descending.
#[test]
fn a_detached_reverse_scan_yields_what_a_reverse_scan_of_the_db_yields() {
    let (db, marks) = populated();
    let manifest = *db.manifest();
    let (mut entries, mut multi_page) = (0, 0);
    // `(from, lower)`: descend from `from` (empty: the last key) to
    // above `lower`.
    let ranges: Vec<(Vec<u8>, Option<Vec<u8>>)> = vec![
        (Vec::new(), None),
        (key(149), Some(key(0))),
        (key(90), Some(key(40))),
        (key(74), Some(key(69))),
        (b"key-0099x".to_vec(), Some(b"key-0070x".to_vec())),
        (key(3), None),
        (key(200), Some(key(140))),
        (b"a".to_vec(), None),
    ];
    for (from, lower) in ranges {
        let lower = lower.as_deref();
        for seq in [u64::MAX, marks[3], marks[6]] {
            for now in [0u64, 85] {
                let want = attached_rev(&db, &from, lower, seq, now);
                for max_keys in [1usize, 3, 10_000] {
                    let (got, pages) = detached_rev::<_, _, 64, 4096>(
                        &db,
                        db.device(),
                        &manifest,
                        (&from, lower),
                        (seq, now),
                        max_keys,
                    );
                    assert_eq!(
                        got, want,
                        "{from:?}..{lower:?} seq {seq} now {now} keys {max_keys}"
                    );
                    multi_page += usize::from(pages > 1);
                }
                entries += want.len();
            }
        }
    }
    assert!(entries > 500, "the fixture yields too little: {entries}");
    assert!(multi_page > 20, "paging was barely exercised: {multi_page}");
}

/// A page smaller than the memtable: the forward copy stops early at a
/// key boundary when the page fills, and the scan is still exact. A page
/// that cannot hold one key's versions is an error, not a stalled scan.
#[test]
fn a_small_page_stops_at_a_key_boundary() {
    let (db, _) = populated();
    let manifest = *db.manifest();
    let want = attached(&db, b"", None, u64::MAX, 0);
    // Six slots: a few keys per page, whatever `max_keys` allows.
    let (got, pages) = detached::<_, _, 6, 512>(
        &db,
        db.device(),
        &manifest,
        (b"", None),
        (u64::MAX, 0),
        10_000,
    );
    assert_eq!(got, want);
    assert!(pages > 3, "the small page never filled: {pages} pages");
    // Arena-bound instead of slot-bound.
    let (got, pages) = detached::<_, _, 64, 150>(
        &db,
        db.device(),
        &manifest,
        (b"", None),
        (u64::MAX, 0),
        10_000,
    );
    assert_eq!(got, want);
    assert!(pages > 3, "the small arena never filled: {pages} pages");

    // `key(3)` has four versions in the memtable; two slots cannot hold
    // them, so a page starting there can make no progress.
    let mut tiny = Page::<2, 4096>::new();
    assert!(matches!(
        db.copy_memtable_range(&key(3), None, 100, &mut tiny),
        Err(Error::TableFull)
    ));
    let mut tiny = Page::<64, 8>::new();
    assert!(matches!(
        db.copy_memtable_range(&key(3), None, 100, &mut tiny),
        Err(Error::ArenaFull)
    ));
    // The reverse copy cannot stop early: it reports the page too small.
    let mut small = Page::<6, 4096>::new();
    assert!(matches!(
        db.copy_memtable_range_rev(b"", None, 100, &mut small),
        Err(Error::TableFull)
    ));
    assert!(db.copy_memtable_range_rev(b"", None, 2, &mut small).is_ok());
}

/// What a page holds: its keys with every version, the range tombstones
/// that can cover them, and nothing at or past the horizon.
#[test]
fn a_page_holds_whole_keys_up_to_its_horizon() {
    let (db, _) = populated();
    let mut page = Box::new(Page::<64, 4096>::new());
    // Not open: no copy.
    let closed = TestDb::new(MemDevice::<4096>::new(), test_config());
    assert!(matches!(
        closed.copy_memtable_range(b"", None, 1, &mut page),
        Err(Error::NotOpen)
    ));

    // The memtable's first key is `key(3)`, written three times since
    // the last flush, plus whatever the last round left.
    let horizon = db.copy_memtable_range(&key(3), None, 1, &mut page).unwrap();
    let horizon = horizon.expect("more keys follow");
    assert!(horizon.as_slice() > key(3).as_slice());
    assert_eq!(page.get(&key(3)).unwrap().val, b"again");
    assert!(
        page.get(horizon.as_slice()).is_none(),
        "the horizon key was copied"
    );
    // The range tombstone over [70, 76) starts past this page's horizon:
    // it cannot cover the page's keys and need not be there. A page
    // that starts inside the range does carry it.
    let _ = db
        .copy_memtable_range(&key(73), None, 1, &mut page)
        .unwrap();
    assert!(page.max_covering_rdel(&key(73), u64::MAX).is_some());
    assert!(page.max_covering_rdel(&key(80), u64::MAX).is_none());

    // The whole range in one page: no horizon, and the page answers for
    // every memtable key as the memtable would.
    let none = db
        .copy_memtable_range(b"", None, 10_000, &mut page)
        .unwrap();
    assert!(none.is_none());
    assert_eq!(page.get(&key(99)).unwrap().val, b"again");
    // An end bound: nothing at or past it is copied.
    let none = db
        .copy_memtable_range(b"", Some(&key(50)), 10_000, &mut page)
        .unwrap();
    assert!(none.is_none());
    assert!(page.get(&key(99)).is_none());
    assert!(page.get(&key(3)).is_some());
}

/// A page is the store as of its copy: writes that reach only the WAL
/// and the memtable afterwards do not change what it yields. That is
/// what lets the owner keep writing while detached scans run.
#[test]
fn writes_after_the_copy_do_not_change_the_page() {
    let (mut db, _) = populated();
    let manifest = *db.manifest();
    let device = db.device().clone();
    let want = attached(&db, &key(10), Some(&key(120)), u64::MAX, 0);

    let mut page = Box::new(Page::<64, 4096>::new());
    let horizon = db
        .copy_memtable_range(&key(10), Some(&key(120)), 10_000, &mut page)
        .unwrap();
    assert!(horizon.is_none());
    let node = db.node_id();

    let mut changed = 0;
    for i in 10..120 {
        match block_on(db.put(&key(i), b"after")) {
            Ok(_) => changed += 1,
            Err(Error::TableFull | Error::ArenaFull | Error::WalFull) => break,
            Err(e) => panic!("{e:?}"),
        }
    }
    assert!(changed > 5);
    assert_eq!(
        db.manifest().seq(),
        manifest.seq(),
        "no write touched the tables"
    );

    let mut scan = Box::new(ScanOver::<_, 64, 4096>::detached(
        &page, &manifest, &device, None, node,
    ));
    block_on(scan.seek(&key(10), Some(&key(120)), u64::MAX)).unwrap();
    let (mut k, mut v) = ([0u8; 256], [0u8; 1024]);
    let mut got = Vec::new();
    while let Some((kl, vl)) = block_on(scan.next(&mut k, &mut v)).unwrap() {
        got.push((k[..kl].to_vec(), v[..vl].to_vec()));
    }
    assert_eq!(got, want);
}

/// A detached scan holds nothing of the `Db`, and what it does hold can
/// cross threads: six threads scan at once, forward and in reverse, over
/// one manifest copy and one shared device, while the owner writes.
#[test]
fn detached_scans_run_on_many_threads_while_the_owner_writes() {
    let (mut db, _) = populated();
    let manifest = *db.manifest();
    let device = db.device().clone();
    let node = db.node_id();
    let want = attached(&db, b"", None, u64::MAX, 0);
    let want_rev = attached_rev(&db, b"", None, u64::MAX, 0);
    // One page holding the whole memtable, taken once and shared.
    let mut page = Box::new(Page::<64, 4096>::new());
    assert!(
        db.copy_memtable_range(b"", None, 10_000, &mut page)
            .unwrap()
            .is_none()
    );
    let page = &*page;

    std::thread::scope(|s| {
        for t in 0..6usize {
            let (device, manifest, want, want_rev) = (&device, &manifest, &want, &want_rev);
            s.spawn(move || {
                let (mut k, mut v) = ([0u8; 256], [0u8; 1024]);
                for _ in 0..8 {
                    let mut got = Vec::new();
                    if t % 2 == 0 {
                        let mut scan = Box::new(ScanOver::<_, 64, 4096>::detached(
                            page, manifest, device, None, node,
                        ));
                        block_on(scan.seek(b"", None, u64::MAX)).unwrap();
                        while let Some((kl, vl)) = block_on(scan.next(&mut k, &mut v)).unwrap() {
                            got.push((k[..kl].to_vec(), v[..vl].to_vec()));
                        }
                        assert_eq!(&got, want, "thread {t}");
                    } else {
                        let mut scan = Box::new(RevScanOver::<_, 64, 4096>::detached(
                            page, manifest, device, None, node,
                        ));
                        block_on(scan.seek_prev(b"", None, u64::MAX)).unwrap();
                        while let Some((kl, vl)) = block_on(scan.prev(&mut k, &mut v)).unwrap() {
                            got.push((k[..kl].to_vec(), v[..vl].to_vec()));
                        }
                        assert_eq!(&got, want_rev, "thread {t}");
                    }
                }
            });
        }
        for i in 0..30 {
            match block_on(db.put(&key(i), b"meanwhile")) {
                Ok(_) | Err(Error::TableFull | Error::ArenaFull | Error::WalFull) => {}
                Err(e) => panic!("{e:?}"),
            }
        }
    });
}

// ---------------------------------------------------------------------------
// The block pool: buffers lent to a scan so that each table's block stays
// decoded while the merge is at another table.
// ---------------------------------------------------------------------------

/// A device that counts block reads.
struct Counting {
    inner: MemDevice<4096>,
    reads: core::cell::Cell<u64>,
}

impl BlockDevice for Counting {
    type Error = core::convert::Infallible;
    const BLOCK: usize = 4096;

    fn poll_read_block(
        &self,
        cx: &mut core::task::Context<'_>,
        id: u64,
        buf: &mut [u8],
    ) -> core::task::Poll<Result<(), Self::Error>> {
        self.reads.set(self.reads.get() + 1);
        self.inner.poll_read_block(cx, id, buf)
    }

    fn poll_write_block(
        &mut self,
        cx: &mut core::task::Context<'_>,
        id: u64,
        buf: &[u8],
    ) -> core::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_write_block(cx, id, buf)
    }

    fn poll_flush(
        &mut self,
        cx: &mut core::task::Context<'_>,
    ) -> core::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_flush(cx)
    }
}

/// A full forward and reverse scan over `device` with `pool` lent, no
/// cache: the entries each way, and the blocks read from the device.
fn pooled_scans(
    page: &Page<64, 4096>,
    manifest: &TestManifest,
    device: &Counting,
    node: u32,
    pool: &mut [horton::ScanBlock<4096>],
    rev_pool: &mut [horton::ScanBlock<4096>],
) -> (Entries, Entries, u64) {
    let before = device.reads.get();
    let (mut k, mut v) = ([0u8; 256], [0u8; 1024]);
    let mut fwd = Vec::new();
    let mut scan = Box::new(ScanOver::<_, 64, 4096>::detached(
        page, manifest, device, None, node,
    ));
    scan.use_block_pool(pool);
    block_on(scan.seek(b"", None, u64::MAX)).unwrap();
    while let Some((kl, vl)) = block_on(scan.next(&mut k, &mut v)).unwrap() {
        fwd.push((k[..kl].to_vec(), v[..vl].to_vec()));
    }
    let mut rev = Vec::new();
    let mut scan = Box::new(RevScanOver::<_, 64, 4096>::detached(
        page, manifest, device, None, node,
    ));
    scan.use_block_pool(rev_pool);
    block_on(scan.seek_prev(b"", None, u64::MAX)).unwrap();
    while let Some((kl, vl)) = block_on(scan.prev(&mut k, &mut v)).unwrap() {
        rev.push((k[..kl].to_vec(), v[..vl].to_vec()));
    }
    (fwd, rev, device.reads.get() - before)
}

/// A scan's results do not depend on how many buffers it was lent, and
/// lending it buffers is what stops it reading the same block again each
/// time the merge comes back to a table: with a buffer per table, every
/// block is read about once.
#[test]
fn a_block_pool_changes_how_often_blocks_are_read_and_nothing_else() {
    // Four level-0 tables whose keys interleave, so the merge moves to
    // another table at every entry: the worst case for one buffer.
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    block_on(db.open()).unwrap();
    for table in 0..4usize {
        // One batch per table: one WAL block, not forty.
        let mut batch = Box::new(horton::WriteBatch::<256, 1024, 64>::new());
        for i in (table..160).step_by(4) {
            batch
                .put(&key(i), &[b'a' + u8::try_from(table).unwrap(); 60])
                .unwrap();
        }
        block_on(db.write(&batch)).unwrap();
        block_on(db.flush()).unwrap();
    }
    block_on(db.put(&key(7), b"memtable")).unwrap();
    let manifest = *db.manifest();
    let tables = manifest.level(0).unwrap_or(&[]).len();
    assert_eq!(tables, 4, "four overlapping tables");
    let want = attached(&db, b"", None, u64::MAX, 0);
    let want_rev = attached_rev(&db, b"", None, u64::MAX, 0);
    let mut page = Box::new(Page::<64, 4096>::new());
    assert!(
        db.copy_memtable_range(b"", None, 10_000, &mut page)
            .unwrap()
            .is_none()
    );
    let device = Counting {
        inner: db.device().clone(),
        reads: core::cell::Cell::new(0),
    };

    let mut reads = Vec::new();
    for size in [0usize, 1, 2, 40] {
        let mut pool: Vec<_> = (0..size).map(|_| horton::ScanBlock::new()).collect();
        let mut rev_pool: Vec<_> = (0..size).map(|_| horton::ScanBlock::new()).collect();
        let (fwd, rev, n) = pooled_scans(
            &page,
            &manifest,
            &device,
            db.node_id(),
            &mut pool,
            &mut rev_pool,
        );
        assert_eq!(fwd, want, "forward, pool of {size}");
        assert_eq!(rev, want_rev, "reverse, pool of {size}");
        reads.push(n);
    }
    // More buffers never cost reads. With one buffer nearly every entry
    // re-reads a block (160 entries each way); with a buffer per table
    // each block is read about once.
    assert!(reads.windows(2).all(|w| w[1] <= w[0]), "{reads:?}");
    assert!(
        reads[0] > 250,
        "one buffer should reload constantly: {reads:?}"
    );
    assert!(
        reads[3] < 80,
        "a buffer per table should read each block once: {reads:?}"
    );

    // A pool can be swapped between seeks; the scan starts over cleanly.
    let mut first: Vec<_> = (0..3).map(|_| horton::ScanBlock::new()).collect();
    let mut second: Vec<_> = (0..5).map(|_| horton::ScanBlock::new()).collect();
    let mut scan = Box::new(ScanOver::<_, 64, 4096>::new(&db));
    scan.use_block_pool(&mut first);
    block_on(scan.seek(&key(40), None, u64::MAX)).unwrap();
    let (mut k, mut v) = ([0u8; 256], [0u8; 1024]);
    assert!(block_on(scan.next(&mut k, &mut v)).unwrap().is_some());
    scan.use_block_pool(&mut second);
    block_on(scan.seek(b"", None, u64::MAX)).unwrap();
    let mut got = Vec::new();
    while let Some((kl, vl)) = block_on(scan.next(&mut k, &mut v)).unwrap() {
        got.push((k[..kl].to_vec(), v[..vl].to_vec()));
    }
    assert_eq!(got, want);
}

/// A forward seek reads the tables whose range holds the seek key, and
/// leaves every table that starts after it alone until the scan gets
/// there. Four tables with disjoint keys: a seek into the third and one
/// entry costs one table's blocks, and reading on opens the fourth only
/// when the scan arrives at it.
#[test]
fn a_seek_reads_only_the_tables_around_its_key() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    block_on(db.open()).unwrap();
    for table in 0..4usize {
        let mut batch = Box::new(horton::WriteBatch::<256, 1024, 64>::new());
        for i in 0..20 {
            batch.put(&key(table * 100 + i), b"v").unwrap();
        }
        block_on(db.write(&batch)).unwrap();
        block_on(db.flush()).unwrap();
    }
    let manifest = *db.manifest();
    assert_eq!(manifest.level(0).unwrap_or(&[]).len(), 4, "four tables");
    let page = Box::new(Page::<64, 4096>::new());
    let device = Counting {
        inner: db.device().clone(),
        reads: core::cell::Cell::new(0),
    };
    let (mut k, mut v) = ([0u8; 256], [0u8; 1024]);

    let mut scan = Box::new(ScanOver::<_, 64, 4096>::detached(
        &page,
        &manifest,
        &device,
        None,
        db.node_id(),
    ));
    block_on(scan.seek(&key(205), None, u64::MAX)).unwrap();
    let (kl, _) = block_on(scan.next(&mut k, &mut v)).unwrap().unwrap();
    assert_eq!(&k[..kl], key(205).as_slice());
    // The index and data block of the one table that holds the key (the
    // manifest names the index block; no footer is read); the table
    // after it waits, the two before are pruned.
    assert_eq!(device.reads.get(), 2, "a seek into one table of four");

    // Reading on opens the next table when the scan reaches it.
    let mut got = vec![k[..kl].to_vec()];
    while let Some((kl, _)) = block_on(scan.next(&mut k, &mut v)).unwrap() {
        got.push(k[..kl].to_vec());
    }
    let want: Vec<Vec<u8>> = (205..220).chain(300..320).map(key).collect();
    assert_eq!(got, want);
    assert_eq!(device.reads.get(), 4, "two tables, two blocks each");

    // From the very start nothing is read until the first entry is asked
    // for, and an end bound keeps later tables shut for good.
    let before = device.reads.get();
    block_on(scan.seek(b"", Some(&key(105)), u64::MAX)).unwrap();
    assert_eq!(
        device.reads.get(),
        before,
        "a seek before every table read one"
    );
    let mut n = 0;
    while block_on(scan.next(&mut k, &mut v)).unwrap().is_some() {
        n += 1;
    }
    assert_eq!(n, 25, "twenty keys of the first table, five of the second");
    assert_eq!(
        device.reads.get(),
        before + 4,
        "two tables of four were opened"
    );
}
