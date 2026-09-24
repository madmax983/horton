//! Blocks orphaned by a torn write need no sweep: they sit in a table slot
//! no manifest table references, which is simply free, and the next table
//! written into that slot overwrites them.
//!
//! Geometry: 2 levels x 2 tables = 4 table slots of 8 blocks over
//! `[16, 48)`, WAL `[8, 16)`. A hand-committed manifest references one
//! table in slot 3 (`[40, 45)`, level 1), so next-fit wraps to slot 0 for
//! the next table — the slot holding the orphans.
//!
//! Two tests: the first plants the orphans by hand; the second crashes a
//! real flush between the table-block writes and the manifest commit, so
//! the orphans are genuinely produced by the torn flush.

mod common;

use common::{CrashDevice, MemDevice, block_on};
use horton::{BlockDevice, Config, KeyBound, Manifest, TableRef};

use core::task::{Context, Poll};

type DevError = core::convert::Infallible;

type TinyDb<D> = horton::Db<D, 4096, 256, 1024, 64, 4096, 2, 2, 1024, 8>;

/// Manifest slots 0/1, WAL `[8, 16)`, tables `[16, 48)`: 4 slots of 8.
const fn tiny_config() -> Config {
    Config::new(8, 16, 16, 48, 0, 1)
}

/// First block of table slot 0, where the orphans live.
const SLOT0: u64 = 16;

/// A live table in slot 3 (`[40, 45)`) covering `m..=z`. Its blocks were
/// never written — no read path in these tests may consult it (the test
/// keys sort below `m`, so key-range pruning skips it).
fn live_ref() -> TableRef<256> {
    TableRef {
        id: 0,
        first_block: 40,
        block_count: 5,
        first_key: KeyBound::from_slice(b"m").expect("bound"),
        last_key: KeyBound::from_slice(b"z").expect("bound"),
        max_seq: 0,
        min_seq: 0,
        entry_count: 0,
        rdel_blocks: 0,
    }
}

fn get<D: BlockDevice>(db: &TinyDb<D>, key: &[u8]) -> Option<Vec<u8>>
where
    D::Error: core::fmt::Debug,
{
    let mut buf = [0u8; 1024];
    block_on(db.get(key, &mut buf))
        .expect("get")
        .map(|n| buf[..n].to_vec())
}

/// A fresh device whose manifest references only the live slot-3 table,
/// with junk in every block of slots 0..3 (stand-ins for orphans).
fn base_device() -> MemDevice<4096> {
    let mut dev = MemDevice::<4096>::new();
    let mut manifest = Manifest::<2, 2, 256>::new();
    manifest.set_wal_head(8);
    manifest
        .add_table_to_level::<DevError>(1, live_ref())
        .expect("place table");
    let mut scratch = [0u8; 4096];
    block_on(manifest.commit(&mut dev, &mut scratch, 0, 1)).expect("commit");
    let junk = [0xA5u8; 4096];
    let waker = common::noop_waker();
    let mut cx = Context::from_waker(&waker);
    for id in SLOT0..40 {
        assert!(matches!(
            dev.poll_write_block(&mut cx, id, &junk),
            Poll::Ready(Ok(()))
        ));
    }
    dev
}

/// Records the block id of every write, in order.
struct IdLogDevice<D> {
    inner: D,
    ids: Vec<u64>,
}

impl<D: BlockDevice> BlockDevice for IdLogDevice<D> {
    type Error = D::Error;
    const BLOCK: usize = D::BLOCK;

    fn poll_read_block(
        &self,
        cx: &mut Context<'_>,
        id: u64,
        buf: &mut [u8],
    ) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_read_block(cx, id, buf)
    }

    fn poll_write_block(
        &mut self,
        cx: &mut Context<'_>,
        id: u64,
        buf: &[u8],
    ) -> Poll<Result<(), Self::Error>> {
        self.ids.push(id);
        self.inner.poll_write_block(cx, id, buf)
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_flush(cx)
    }
}

const PUTS: [(&[u8], &[u8]); 4] = [
    (b"a".as_slice(), b"1".as_slice()),
    (b"b".as_slice(), b"2".as_slice()),
    (b"c".as_slice(), b"3".as_slice()),
    (b"d".as_slice(), b"4".as_slice()),
];

/// The write index of the flush's manifest commit: the last write overall,
/// and it must target a manifest slot (0 or 1) — not a table or WAL block.
fn commit_write_index() -> usize {
    let dev = IdLogDevice {
        inner: base_device(),
        ids: Vec::new(),
    };
    let mut db = TinyDb::new(dev, tiny_config());
    block_on(db.open()).expect("open");
    for (k, v) in PUTS {
        block_on(db.put(k, v)).expect("put");
    }
    block_on(db.flush()).expect("flush");
    let dev = db.into_device();
    let idx = dev
        .ids
        .iter()
        .rposition(|&id| id == 0 || id == 1)
        .expect("a manifest commit write");
    assert_eq!(
        idx,
        dev.ids.len() - 1,
        "the manifest commit is the flush's final write"
    );
    idx
}

#[test]
fn crashed_flush_orphans_are_reclaimed_and_reused() {
    // Crash the flush after its table blocks land in slot 0 but before the
    // manifest commit: those blocks become genuine orphans of a torn flush.
    let crash_at = commit_write_index();
    let dev = CrashDevice::<MemDevice<4096>, 4096>::new(base_device(), crash_at);
    let mut db = TinyDb::new(dev, tiny_config());
    block_on(db.open()).expect("open");
    for (k, v) in PUTS {
        block_on(db.put(k, v)).expect("put");
    }
    // Returns `Ok` — the device reported success; the "crash" only dropped
    // the commit write.
    block_on(db.flush()).expect("flush");
    let dev = db.into_device().into_inner();

    // Reopen: slot 0 holds no manifest table, so it is free; the WAL
    // replay restores the puts (durable before the crash), and the retried
    // flush lands in slot 0 again, over the orphans.
    let mut db = TinyDb::new(dev, tiny_config());
    block_on(db.open()).expect("open");
    assert_eq!(db.slot_stats().used, 1, "only the live table's slot");
    assert_eq!(get(&db, b"a"), Some(b"1".to_vec()));
    block_on(db.flush()).expect("retry flush reuses the orphaned slot");
    assert_eq!(db.level_tables(0).unwrap()[0].first_block, SLOT0);
    assert_eq!(get(&db, b"d"), Some(b"4".to_vec()));
    assert_eq!(db.check_invariants(), Ok(()));

    // Reopening rebuilds the same slot state.
    let dev = db.into_device();
    let mut db = TinyDb::new(dev, tiny_config());
    block_on(db.open()).expect("open");
    assert_eq!(db.slot_stats().used, 2);
    assert_eq!(db.check_invariants(), Ok(()));
    assert_eq!(get(&db, b"d"), Some(b"4".to_vec()));
}

#[test]
fn orphans_in_a_free_slot_are_overwritten() {
    let mut db = TinyDb::new(base_device(), tiny_config());
    block_on(db.open()).expect("open");
    let s = db.slot_stats();
    assert_eq!((s.slots, s.slot_blocks, s.used, s.free), (4, 8, 1, 3));

    // Next-fit resumes past the newest table (slot 3) and wraps to slot 0,
    // straight over the junk.
    block_on(db.put(b"a", b"1")).expect("put");
    block_on(db.flush()).expect("flush");
    assert_eq!(db.level_tables(0).unwrap()[0].first_block, SLOT0);
    assert_eq!(get(&db, b"a"), Some(b"1".to_vec()));
    assert_eq!(db.check_invariants(), Ok(()));

    // Reopening rebuilds the same slot state from the manifest alone.
    let dev = db.into_device();
    let mut db = TinyDb::new(dev, tiny_config());
    block_on(db.open()).expect("open");
    assert_eq!(db.slot_stats().used, 2);
    assert_eq!(get(&db, b"a"), Some(b"1".to_vec()));
    assert_eq!(db.check_invariants(), Ok(()));
}

#[test]
fn a_table_outside_its_slot_is_a_corrupt_manifest() {
    // A table straddling slots 0 and 1 cannot have been written under this
    // layout: open() must refuse rather than hand either slot out.
    let mut dev = MemDevice::<4096>::new();
    let mut manifest = Manifest::<2, 2, 256>::new();
    manifest.set_wal_head(8);
    let mut straddler = live_ref();
    straddler.first_block = 20;
    straddler.block_count = 8; // [20, 28)
    manifest
        .add_table_to_level::<DevError>(1, straddler)
        .expect("place table");
    let mut scratch = [0u8; 4096];
    block_on(manifest.commit(&mut dev, &mut scratch, 0, 1)).expect("commit");
    let mut db = TinyDb::new(dev, tiny_config());
    assert_eq!(
        block_on(db.open()).map(|_| ()),
        Err(horton::Error::CorruptManifest)
    );
    assert!(!db.is_open());
}
