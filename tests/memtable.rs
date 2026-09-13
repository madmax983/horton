//! `MemTable` unit tests: ordering, supersede paths, tombstones, capacity.

use core::convert::Infallible;

use horton::memtable::MemTable;
use horton::Error;

type Small = MemTable<8, 256, 16, 32>;

fn insert(table: &mut Small, key: &[u8], val: &[u8], seq: u64) {
    table.insert::<Infallible>(key, val, seq, false).unwrap();
}

#[test]
fn sorted_order_and_lookup() {
    let mut t = Small::new();
    assert!(t.is_empty());
    for (i, k) in [b"d", b"a", b"c", b"b"].into_iter().enumerate() {
        insert(&mut t, k, b"v", i as u64 + 1);
    }
    assert_eq!(t.len(), 4);
    for k in [b"a", b"b", b"c", b"d"] {
        let e = t.get(k).unwrap();
        assert!(!e.tombstone);
    }
    assert!(t.get(b"zzz").is_none());
}

#[test]
fn supersede_in_place() {
    let mut t = Small::new();
    insert(&mut t, b"k", b"longvalue", 1);
    // Shorter value fits the old region: overwritten in place, same slot count.
    insert(&mut t, b"k", b"v", 2);
    assert_eq!(t.len(), 1);
    let e = t.get(b"k").unwrap();
    assert_eq!(e.val, b"v");
    assert_eq!(e.seq, 2);
}

#[test]
fn supersede_via_dead_slot() {
    let mut t = Small::new();
    insert(&mut t, b"k", b"v", 1);
    // Longer value does not fit: old slot dies, new slot takes over.
    insert(&mut t, b"k", b"a-much-longer-value", 2);
    let e = t.get(b"k").unwrap();
    assert_eq!(e.val, b"a-much-longer-value");
    assert_eq!(e.seq, 2);
    // At most one dead slot per key: slot count grows by exactly one.
    assert_eq!(t.len(), 2);
    // A third, even longer value reuses the dead slot instead of growing.
    insert(&mut t, b"k", b"an-even-much-longer-value!!", 3);
    assert_eq!(t.len(), 2);
    let e = t.get(b"k").unwrap();
    assert_eq!(e.val, b"an-even-much-longer-value!!");
    assert_eq!(e.seq, 3);
}

#[test]
fn delete_is_tombstone() {
    let mut t = Small::new();
    insert(&mut t, b"k", b"v", 1);
    t.insert::<Infallible>(b"k", b"", 2, true).unwrap();
    let e = t.get(b"k").unwrap();
    assert!(e.tombstone);
    assert_eq!(e.seq, 2);
    // Deleting a missing key still records a tombstone.
    t.insert::<Infallible>(b"nope", b"", 3, true).unwrap();
    assert!(t.get(b"nope").unwrap().tombstone);
}

#[test]
fn validation_errors() {
    let mut t = Small::new();
    assert_eq!(
        t.insert::<Infallible>(b"", b"v", 1, false),
        Err(Error::EmptyKey)
    );
    assert_eq!(
        t.insert::<Infallible>(b"0123456789abcdefg", b"v", 1, false), // 17 > KEY_MAX 16
        Err(Error::KeyTooLarge { len: 17, max: 16 })
    );
    assert_eq!(
        t.insert::<Infallible>(b"k", &[9u8; 33], 1, false),
        Err(Error::ValueTooLarge { len: 33, max: 32 })
    );
    // Tombstones ignore the value entirely.
    t.insert::<Infallible>(b"k", &[9u8; 100], 1, true).unwrap();
}

#[test]
fn table_full() {
    let mut t = Small::new(); // CAP = 8
    for i in 0..8u8 {
        insert(&mut t, &[b'a' + i], b"v", u64::from(i));
    }
    assert_eq!(
        t.insert::<Infallible>(b"z", b"v", 9, false),
        Err(Error::TableFull)
    );
    // Superseding an existing key in place still works when full.
    insert(&mut t, b"a", b"w", 10);
    assert_eq!(t.get(b"a").unwrap().val, b"w");
    // But a growing supersede needs a new slot, so it reports TableFull.
    assert_eq!(
        t.insert::<Infallible>(b"a", b"vv", 11, false),
        Err(Error::TableFull)
    );
}

#[test]
fn arena_full() {
    let mut t = MemTable::<8, 16, 16, 32>::new(); // tiny arena
    t.insert::<Infallible>(b"aaaa", b"bbbb", 1, false).unwrap(); // 8 bytes
    assert_eq!(
        t.insert::<Infallible>(b"cccc", b"dddddddd", 2, false), // needs 12, has 8
        Err(Error::ArenaFull)
    );
}

#[test]
fn max_seq_tracks_inserts() {
    let mut t = Small::new();
    assert_eq!(t.max_seq(), 0);
    insert(&mut t, b"a", b"v", 41);
    insert(&mut t, b"b", b"v", 7);
    assert_eq!(t.max_seq(), 41);
}

#[test]
fn clear_resets() {
    let mut t = Small::new();
    insert(&mut t, b"a", b"v", 1);
    t.clear();
    assert!(t.is_empty());
    assert_eq!(t.max_seq(), 0);
    assert!(t.get(b"a").is_none());
    insert(&mut t, b"a", b"v2", 2);
    assert_eq!(t.get(b"a").unwrap().val, b"v2");
}
