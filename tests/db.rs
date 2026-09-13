//! Db tests: basic operations, randomized oracle comparison, and the
//! exhaustive crash injector (the v0.1 gate).

mod common;

use std::collections::BTreeMap;

use common::{block_on, test_config, CrashDevice, Lcg, MemDevice, TestDb};
use horton::Error;

fn open<D: horton::BlockDevice>(db: &mut TestDb<D>)
where
    D::Error: std::fmt::Debug,
{
    block_on(db.open()).unwrap();
}

fn get<D: horton::BlockDevice>(db: &TestDb<D>, key: &[u8]) -> Option<Vec<u8>>
where
    D::Error: std::fmt::Debug,
{
    let mut buf = [0u8; 2048];
    block_on(db.get(key, &mut buf))
        .unwrap()
        .map(|n| buf[..n].to_vec())
}

#[test]
fn put_get_delete() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);

    let s1 = block_on(db.put(b"name", b"horton")).unwrap();
    let s2 = block_on(db.put(b"name", b"horton!")).unwrap();
    assert!(s2 > s1);
    assert_eq!(get(&db, b"name"), Some(b"horton!".to_vec()));

    // Empty values are distinct from deletions.
    block_on(db.put(b"empty", b"")).unwrap();
    assert_eq!(get(&db, b"empty"), Some(Vec::new()));

    block_on(db.delete(b"name")).unwrap();
    assert_eq!(get(&db, b"name"), None);
    // Deleting a missing key is fine.
    block_on(db.delete(b"ghost")).unwrap();
    assert_eq!(get(&db, b"ghost"), None);

    block_on(db.flush()).unwrap();
}

#[test]
fn seq_numbers_increase() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    let mut last = 0;
    for i in 0..10u8 {
        let s = block_on(db.put(&[i], b"v")).unwrap();
        assert!(s > last);
        last = s;
    }
}

#[test]
fn get_buffer_too_small() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    open(&mut db);
    block_on(db.put(b"k", b"12345678")).unwrap();
    let mut tiny = [0u8; 3];
    assert_eq!(
        block_on(db.get(b"k", &mut tiny)),
        Err(Error::BufferTooSmall { need: 8 })
    );
    // The buffer is untouched on error.
    assert_eq!(tiny, [0u8; 3]);
}

#[test]
fn reopen_recovers_state() {
    let dev = MemDevice::<4096>::new();
    let mut db = TestDb::new(dev, test_config());
    let rep = block_on(db.open()).unwrap();
    assert_eq!(rep.recovered_records, 0);
    block_on(db.put(b"a", b"1")).unwrap();
    block_on(db.put(b"b", b"2")).unwrap();
    block_on(db.delete(b"a")).unwrap();

    let dev = db.into_device();
    let mut db2 = TestDb::new(dev, test_config());
    let rep = block_on(db2.open()).unwrap();
    assert_eq!(rep.recovered_records, 3);
    assert_eq!(rep.max_seq, 3);
    assert_eq!(get(&db2, b"a"), None);
    assert_eq!(get(&db2, b"b"), Some(b"2".to_vec()));

    // New writes continue after the recovered log; seqs keep increasing.
    let s = block_on(db2.put(b"c", b"3")).unwrap();
    assert_eq!(s, 4);
    assert_eq!(get(&db2, b"c"), Some(b"3".to_vec()));
}

/// Randomized differential test against a `BTreeMap` oracle.
#[test]
fn oracle_random() {
    let mut rng = Lcg::new(0xC10C_A8A7);
    let key_pool: [&[u8]; 6] = [b"a", b"b", b"c", b"dd", b"eee", b"f"];
    for round in 0..200 {
        let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
        open(&mut db);
        let mut oracle: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let n = 1 + rng.next() % 20;
        for _ in 0..n {
            // Test RNG plumbing: the value is always < 6; the u64 -> usize
            // narrowing is exact on every target these tests run on.
            #[allow(clippy::cast_possible_truncation)]
            let key = key_pool[rng.next() as usize % key_pool.len()].to_vec();
            if rng.next().is_multiple_of(3) {
                oracle.remove(&key);
                block_on(db.delete(&key)).unwrap();
            } else {
                let vlen = rng.next() % 40;
                let val: Vec<u8> = (0..vlen).map(|_| b'x' + (rng.next() % 3) as u8).collect();
                oracle.insert(key.clone(), val.clone());
                let seq = block_on(db.put(&key, &val)).unwrap();
                assert!(seq > 0, "round {round}");
            }
        }
        // Every oracle key reads back exactly; pool keys not in the oracle
        // read back as missing.
        for (k, v) in &oracle {
            assert_eq!(get(&db, k), Some(v.clone()), "round {round} key {k:?}");
        }
        for k in key_pool {
            if !oracle.contains_key(k) {
                assert_eq!(get(&db, k), None, "round {round} key {k:?}");
            }
        }
        // Reopen and verify the recovered state matches the oracle too.
        let dev = db.into_device();
        let mut db2 = TestDb::new(dev, test_config());
        let rep = block_on(db2.open()).unwrap();
        assert_eq!(rep.recovered_records, n, "round {round}");
        for (k, v) in &oracle {
            assert_eq!(
                get(&db2, k),
                Some(v.clone()),
                "round {round} reopen key {k:?}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Crash injector: the v0.1 gate.
// ---------------------------------------------------------------------------

/// One scripted mutation over the two crash-test keys.
#[derive(Clone)]
enum ScriptOp {
    Put(usize, Vec<u8>),
    Del(usize),
}

/// All 4^3 = 64 scripts of 3 ops over keys {a, b} (put a / put b / del a / del b).
fn all_scripts() -> Vec<Vec<ScriptOp>> {
    let mut scripts = Vec::new();
    for code in 0..64u8 {
        let mut ops = Vec::new();
        for j in 0..3u8 {
            let v = vec![b'v', b'0' + code, b'0' + j];
            match (code >> (2 * j)) & 3 {
                0 => ops.push(ScriptOp::Put(0, v)),
                1 => ops.push(ScriptOp::Put(1, v)),
                2 => ops.push(ScriptOp::Del(0)),
                _ => ops.push(ScriptOp::Del(1)),
            }
        }
        scripts.push(ops);
    }
    scripts
}

fn apply_script(ops: &[ScriptOp], crash_at: usize) -> MemDevice<4096> {
    let keys: [&[u8]; 2] = [b"a", b"b"];
    let dev: CrashDevice<MemDevice<4096>, 4096> =
        CrashDevice::new(MemDevice::<4096>::new(), crash_at);
    let mut db = TestDb::new(dev, test_config());
    open(&mut db);
    for op in ops {
        match op {
            ScriptOp::Put(k, v) => {
                block_on(db.put(keys[*k], v)).unwrap();
            }
            ScriptOp::Del(k) => {
                block_on(db.delete(keys[*k])).unwrap();
            }
        }
    }
    db.into_device().into_inner()
}

fn oracle_prefix(ops: &[ScriptOp], len: usize) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let keys: [&[u8]; 2] = [b"a", b"b"];
    let mut map = BTreeMap::new();
    for op in &ops[..len] {
        match op {
            ScriptOp::Put(k, v) => {
                map.insert(keys[*k].to_vec(), v.clone());
            }
            ScriptOp::Del(k) => {
                map.remove(keys[*k]);
            }
        }
    }
    map
}

/// For every script and every crash point, the recovered database must equal
/// the oracle state of the longest fully-landed prefix.
///
/// Each mutation performs exactly one WAL block write (per-mutation commit),
/// so crashing at write `n` lands precisely the first `n` ops.
#[test]
fn crash_injector() {
    let keys: [&[u8]; 2] = [b"a", b"b"];
    for (si, script) in all_scripts().iter().enumerate() {
        for crash_at in 0..=3 {
            let dev = apply_script(script, crash_at);
            let mut db = TestDb::new(dev, test_config());
            let rep = block_on(db.open()).unwrap();
            assert_eq!(
                rep.recovered_records, crash_at as u64,
                "script {si} crash {crash_at}"
            );

            let oracle = oracle_prefix(script, crash_at);
            for key in keys {
                let got = get(&db, key);
                let want = oracle.get(key).cloned();
                assert_eq!(got, want, "script {si} crash {crash_at} key {key:?}");
            }
        }
    }
}
