//! `MemTable` unit tests: ordering, versioning, tombstones, capacity.
//!
//! Every mutation appends a fresh slot — updates never overwrite — so each
//! key's versions accumulate newest-first. Snapshots and flush both rely
//! on that history surviving in the table.

use core::convert::Infallible;

use horton::Error;
use horton::memtable::MemTable;

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
fn updates_append_versions_newest_first() {
    let mut t = Small::new();
    insert(&mut t, b"k", b"v1", 1);
    insert(&mut t, b"k", b"v2", 2);
    insert(&mut t, b"k", b"v3", 3);
    // One slot per mutation: no overwriting, no dead slots.
    assert_eq!(t.len(), 3);
    // The live view sees the newest version.
    let e = t.get(b"k").unwrap();
    assert_eq!(e.val, b"v3");
    assert_eq!(e.seq, 3);
    // Iterating yields the key's whole run, newest first.
    let got: Vec<(u64, &[u8])> = t.iter().map(|e| (e.seq, e.val)).collect();
    assert_eq!(
        got,
        vec![
            (3, b"v3".as_slice()),
            (2, b"v2".as_slice()),
            (1, b"v1".as_slice())
        ]
    );
}

#[test]
fn get_at_selects_visible_version() {
    let mut t = Small::new();
    insert(&mut t, b"k", b"v1", 1);
    insert(&mut t, b"k", b"v2", 2);
    insert(&mut t, b"k", b"v3", 3);
    // Each watermark sees the newest version at or below it.
    assert_eq!(t.get_at(b"k", 1).unwrap().val, b"v1");
    assert_eq!(t.get_at(b"k", 2).unwrap().val, b"v2");
    assert_eq!(t.get_at(b"k", u64::MAX).unwrap().val, b"v3");
    // Nothing is visible below the first version.
    assert!(t.get_at(b"k", 0).is_none());
    // A tombstone version is visible as a tombstone, not skipped.
    t.insert::<Infallible>(b"k", b"", 4, true).unwrap();
    let e = t.get_at(b"k", 4).unwrap();
    assert!(e.tombstone);
    assert_eq!(t.get_at(b"k", 3).unwrap().val, b"v3");
}

#[test]
fn versions_sort_within_key_order() {
    let mut t = Small::new();
    insert(&mut t, b"b", b"b1", 1);
    insert(&mut t, b"a", b"a1", 2);
    insert(&mut t, b"b", b"b2", 3);
    insert(&mut t, b"a", b"a2", 4);
    // Key-ascending, and each key's versions newest-first.
    let got: Vec<(&[u8], u64)> = t.iter().map(|e| (e.key, e.seq)).collect();
    assert_eq!(
        got,
        vec![
            (b"a".as_slice(), 4),
            (b"a".as_slice(), 2),
            (b"b".as_slice(), 3),
            (b"b".as_slice(), 1),
        ]
    );
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
    // Every mutation consumes a slot — even a same-key update, since old
    // versions are retained for snapshots. A full table rejects those too.
    assert_eq!(
        t.insert::<Infallible>(b"a", b"w", 10, false),
        Err(Error::TableFull)
    );
    assert_eq!(t.get(b"a").unwrap().val, b"v");
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
