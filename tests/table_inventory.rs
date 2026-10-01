//! Table-inventory tests: [`horton::Db::tables`] enumerates the sealed
//! tables at each level — the polling endpoint for host-side policy loops
//! (the tiered sweeper, table-shipping replication).

mod common;

use common::{MemDevice, TestDb, block_on, test_config};

fn open<D: horton::BlockDevice>(db: &mut TestDb<D>)
where
    D::Error: std::fmt::Debug,
{
    block_on(db.open()).unwrap();
}

#[test]
fn tables_starts_empty_and_rejects_bad_level() {
    let dev = MemDevice::<4096>::new();
    let mut db = TestDb::new(dev, test_config());
    open(&mut db);
    // The test config has 7 levels; all hold nothing before any flush.
    for level in 0..7 {
        assert_eq!(
            db.tables(level).map(<[horton::TableRef<256>]>::len),
            Some(0),
            "level {level}"
        );
    }
    // Out of range is None, never a panic: the accessor is total.
    assert!(db.tables(7).is_none());
    assert!(db.tables(usize::MAX).is_none());
}

#[test]
fn tables_tracks_flushes_and_archive_commit() {
    let dev = MemDevice::<4096>::new();
    let mut db = TestDb::new(dev, test_config());
    open(&mut db);
    for i in 0..20u8 {
        block_on(db.put(&[i], &[i, i])).unwrap();
    }
    block_on(db.flush()).unwrap();
    for i in 20..25u8 {
        block_on(db.put(&[i], &[i])).unwrap();
    }
    block_on(db.flush()).unwrap();

    // Both sealed tables are visible at L0, oldest first.
    let l0 = db.tables(0).expect("level 0 exists");
    assert_eq!(l0.len(), 2);
    assert_ne!(l0[0].id, l0[1].id);
    assert_eq!(l0[0].entry_count, 20);
    assert_eq!(l0[0].min_seq, 1);
    assert_eq!(l0[0].max_seq, 20);
    assert_eq!(l0[1].entry_count, 5);
    assert_eq!(l0[1].min_seq, 21);
    assert_eq!(l0[1].max_seq, 25);
    assert!(l0[0].block_count > 0);
    assert!(l0[0].first_block > 0);
    assert_eq!(l0[0].first_key.as_slice(), &[0u8]);
    assert_eq!(l0[0].last_key.as_slice(), &[19u8]);
    // Deeper levels are untouched.
    assert_eq!(db.tables(1).map(<[horton::TableRef<256>]>::len), Some(0));

    // The sweeper workflow: enumerate → commit → re-enumerate.
    let archived = block_on(db.archive_commit(0, l0[0].id)).unwrap();
    assert!(archived);
    let l0 = db.tables(0).expect("level 0 exists");
    assert_eq!(l0.len(), 1);
    assert_eq!(l0[0].entry_count, 5);
}
