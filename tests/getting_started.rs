//! The newcomer path: `db_types!` with only the three required sizes, and
//! `Config::whole_device`, which lays a device out from its block count.

mod common;

use core::task::{Context, Poll};

use common::{MemDevice, block_on};
use horton::{BlockDevice, Config, Error, Progress};

/// A `MemDevice` that is exactly `blocks` long: any access past the end
/// fails instead of silently growing the device.
struct Bounded<const BLOCK: usize> {
    inner: MemDevice<BLOCK>,
    blocks: u64,
}

impl<const BLOCK: usize> Bounded<BLOCK> {
    fn new(blocks: u64) -> Self {
        Self {
            inner: MemDevice::new(),
            blocks,
        }
    }
}

/// An access past the end of a [`Bounded`] device.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OutOfRange(u64);

impl<const BLOCK: usize> BlockDevice for Bounded<BLOCK> {
    type Error = OutOfRange;
    const BLOCK: usize = BLOCK;

    fn poll_read_block(
        &self,
        cx: &mut Context<'_>,
        id: u64,
        buf: &mut [u8],
    ) -> Poll<Result<(), Self::Error>> {
        if id >= self.blocks {
            return Poll::Ready(Err(OutOfRange(id)));
        }
        self.inner
            .poll_read_block(cx, id, buf)
            .map(|r| r.map_err(|e| match e {}))
    }

    fn poll_write_block(
        &mut self,
        cx: &mut Context<'_>,
        id: u64,
        buf: &[u8],
    ) -> Poll<Result<(), Self::Error>> {
        if id >= self.blocks {
            return Poll::Ready(Err(OutOfRange(id)));
        }
        self.inner
            .poll_write_block(cx, id, buf)
            .map(|r| r.map_err(|e| match e {}))
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_flush(cx).map(|r| r.map_err(|e| match e {}))
    }
}

// The quick-start shape: only the three sizes a newcomer knows.
horton::db_types! {
    block: 512,
    key_max: 16,
    val_max: 64;
    type Db = SmallDb;
    type Compaction = SmallCompaction;
}

// Same shape with a different memtable: the layout must not move.
horton::db_types! {
    block: 512,
    key_max: 16,
    val_max: 64,
    memtable_entries: 8,
    memtable_arena: 512;
    type Db = SmallMemtableDb;
}

/// Omitted tuning parameters take the documented defaults: 64 memtable
/// entries, an arena of 128 bytes per entry, 4 levels of 4 tables, a
/// 256-byte bloom filter, and no block cache.
#[test]
fn db_types_defaults_fill_in_the_tuning_knobs() {
    fn db(
        x: horton::Db<MemDevice<512>, 512, 16, 64, 64, 8192, 4, 4, 256, 0>,
    ) -> SmallDb<MemDevice<512>> {
        x
    }
    fn comp(x: horton::Compaction<512, 16, 64, 256>) -> SmallCompaction {
        x
    }
    let _ = (db, comp);
}

/// Any subset of the tuning parameters can be given, in the documented
/// order; the rest keep their defaults.
#[test]
fn db_types_overrides_any_subset_in_order() {
    horton::db_types! {
        block: 4096,
        key_max: 32,
        val_max: 100,
        levels: 3,
        cache_blocks: 2,
    ;
        type Db = Partial;
    }
    fn db(
        x: horton::Db<MemDevice<4096>, 4096, 32, 100, 64, 8192, 3, 4, 256, 2>,
    ) -> Partial<MemDevice<4096>> {
        x
    }
    let _ = db;
}

/// The default arena scales with `memtable_entries` (128 bytes each) and
/// always holds at least one largest entry, so a maximal `put` into an
/// empty memtable can never fail with `ArenaFull`.
#[test]
fn db_types_default_arena_follows_the_memtable() {
    horton::db_types! {
        block: 4096,
        key_max: 16,
        val_max: 16,
        memtable_entries: 16;
        type Db = Scaled;
    }
    horton::db_types! {
        block: 8192,
        key_max: 1000,
        val_max: 7000,
        memtable_entries: 4;
        type Db = BigValues;
    }
    fn scaled(
        x: horton::Db<MemDevice<4096>, 4096, 16, 16, 16, 2048, 4, 4, 256, 0>,
    ) -> Scaled<MemDevice<4096>> {
        x
    }
    fn big(
        x: horton::Db<MemDevice<8192>, 8192, 1000, 7000, 4, 8000, 4, 4, 256, 0>,
    ) -> BigValues<MemDevice<8192>> {
        x
    }
    let _ = (scaled, big);
}

/// Opens a whole-device `SmallDb` of `blocks` blocks.
fn open_small(blocks: u64) -> (Box<SmallDb<Bounded<512>>>, Result<(), Error<OutOfRange>>) {
    let mut db = Box::new(SmallDb::new(
        Bounded::<512>::new(blocks),
        Config::whole_device(blocks),
    ));
    let r = block_on(db.open()).map(|_| ());
    (db, r)
}

/// Blocks per manifest copy for `SmallDb`.
const STRIDE: u64 = horton::Manifest::<4, 4, 16>::max_blocks::<512>();

/// Every device size resolves to disjoint regions inside `[0, blocks)`:
/// the manifest pair first, then the WAL, then a table region of whole
/// slots running to the end. Below `MIN_DEVICE_BLOCKS` `open` reports
/// `BadConfig`; from it up, every size opens.
#[test]
fn whole_device_layout_is_disjoint_and_fills_the_device() {
    let slots = 16; // 4 levels × 4 tables
    let min = SmallDb::<Bounded<512>>::MIN_DEVICE_BLOCKS;
    let sizes = (0..64)
        .chain(min - 64..min + 400)
        .chain([min * 2, min * 10]);
    for blocks in sizes {
        let (db, r) = open_small(blocks);
        let c = db.config();
        assert_eq!(c.device_blocks, blocks);
        if blocks < min {
            assert_eq!(
                r,
                Err(Error::BadConfig),
                "{blocks} blocks is below the minimum {min}"
            );
            continue;
        }
        r.unwrap_or_else(|e| panic!("{blocks} blocks (minimum {min}): {e:?}"));
        assert_eq!(
            (c.manifest_a, c.manifest_b, c.manifest_ring),
            (0, STRIDE, 0)
        );
        assert_eq!(c.wal_start, 2 * STRIDE);
        assert_eq!(c.tbl_start, c.wal_end);
        assert_eq!(c.tbl_end, blocks, "tables run to the end of the device");
        assert_eq!(
            (c.tbl_end - c.tbl_start) % slots,
            0,
            "{blocks}: tables are whole slots"
        );
        // The WAL gets an eighth of the space (at least two blocks), plus
        // the blocks left over from rounding the slots.
        let space = blocks - 2 * STRIDE;
        let share = (space / 8).max(2);
        let wal = c.wal_end - c.wal_start;
        assert!(
            wal >= share && wal < share + slots,
            "{blocks}: WAL {wal}, share {share}"
        );
    }
}

/// A device too small for even the manifest is refused, not misused.
#[test]
fn whole_device_too_small_is_bad_config() {
    for blocks in [0, 1, 2, 3, 10, 2 * STRIDE, 2 * STRIDE + 2] {
        let (_, r) = open_small(blocks);
        assert_eq!(r, Err(Error::BadConfig), "{blocks} blocks");
    }
}

/// The layout depends on the device size and the manifest, not on the
/// memtable: resizing the memtable of an existing database keeps its
/// regions where they were.
#[test]
fn whole_device_layout_ignores_the_memtable() {
    for blocks in [700, 2000, 4096] {
        let a = SmallDb::new(Bounded::<512>::new(blocks), Config::whole_device(blocks)).config();
        let b = SmallMemtableDb::new(Bounded::<512>::new(blocks), Config::whole_device(blocks))
            .config();
        assert_eq!(a, b);
    }
}

/// With a manifest ring, the copies sit back to back at the start and the
/// WAL follows them.
#[test]
fn whole_device_with_a_manifest_ring() {
    let blocks = 2500;
    let config = Config::whole_device(blocks).with_manifest_ring(4);
    let mut db = Box::new(SmallDb::new(Bounded::<512>::new(blocks), config));
    block_on(db.open()).unwrap();
    let c = db.config();
    assert_eq!((c.manifest_a, c.manifest_ring), (0, 4));
    assert_eq!(c.wal_start, 4 * STRIDE);
    assert_eq!(c.tbl_end, blocks);
    block_on(db.put(b"k", b"v")).unwrap();
    block_on(db.flush()).unwrap();
    let device = db.into_device();
    let mut db = Box::new(SmallDb::new(device, config));
    block_on(db.open()).unwrap();
    let mut val = [0u8; 64];
    assert_eq!(block_on(db.get(b"k", &mut val)).unwrap(), Some(1));
}

/// A hand-placed layout passes through `Db::new` unchanged.
#[test]
fn manual_config_is_kept_as_given() {
    let config = Config::new(2, 66, 66, 1000, 0, 1);
    let db = SmallDb::new(Bounded::<512>::new(1000), config);
    assert_eq!(db.config(), config);
    assert_eq!(config.device_blocks, 0);
}

/// End to end on a whole-device layout: writes, flushes, compaction and a
/// reopen, on a device that fails any access past its last block.
#[test]
fn whole_device_database_works_end_to_end() {
    let blocks = SmallDb::<Bounded<512>>::MIN_DEVICE_BLOCKS;
    let (mut db, r) = open_small(blocks);
    r.unwrap();
    let mut scratch = Box::new(SmallCompaction::new());
    for i in 0..2000_u32 {
        let key = format!("key{:05}", i % 700);
        let val = format!("value-{i}");
        loop {
            match block_on(db.put(key.as_bytes(), val.as_bytes())) {
                Ok(_) => break,
                // The memtable or the WAL is full: flush, compacting
                // first whenever the flush has no free slot.
                Err(Error::TableFull | Error::ArenaFull | Error::WalFull) => loop {
                    match block_on(db.flush()) {
                        Ok(()) => break,
                        Err(Error::NeedsCompaction) => {
                            while block_on(db.compact_step(&mut scratch)).unwrap() == Progress::More
                            {
                            }
                        }
                        Err(e) => panic!("flush before put {i}: {e:?}"),
                    }
                },
                Err(e) => panic!("put {i}: {e:?}"),
            }
        }
        if i % 97 == 0 {
            while db.compaction_pending() {
                while block_on(db.compact_step(&mut scratch)).unwrap() == Progress::More {}
            }
        }
    }
    let config = db.config();
    let device = db.into_device();
    let mut db = Box::new(SmallDb::new(device, Config::whole_device(blocks)));
    block_on(db.open()).unwrap();
    assert_eq!(db.config(), config, "the layout is the same after a reopen");
    let mut val = [0u8; 64];
    for k in 0..700_u32 {
        let key = format!("key{k:05}");
        // The last write to key k was the largest i < 2000 with i % 700 == k.
        let last = if k < 2000 % 700 { 1400 + k } else { 700 + k };
        let n = block_on(db.get(key.as_bytes(), &mut val))
            .unwrap()
            .expect("key present");
        assert_eq!(&val[..n], format!("value-{last}").as_bytes());
    }
}
