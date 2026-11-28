//! Entry points in compressed data blocks (ADR-0040), through the public
//! paths of a store.
//!
//! A table of entry points the writer got wrong, under a valid CRC, is
//! `CorruptBlock` wherever its block is read whole: a scan, forward or
//! reverse, a compaction. A point read, which may decode only part of
//! the block, either fails or answers as the intact block does, never
//! with another answer. And a store whose blocks carry tables reads,
//! everywhere, what a store without them does.
mod common;

use common::{MemDevice, TestDb, block_on, test_config};
use horton::{Compaction, Config, Error, MemTable, Progress, RevScan, Scan, crc32};

type TestCompaction = Compaction<4096, 256, 1024, 1024>;
type Page = MemTable<64, 4096, 256, 1024>;
type Manifest = horton::Manifest<7, 4, 256>;
type Entries = Vec<(Vec<u8>, Vec<u8>)>;
/// Damage done to a block's body.
type Edit = Box<dyn Fn(&mut [u8])>;

const BODY: usize = 4096 - 4;

fn key(i: usize) -> Vec<u8> {
    format!("key-{i:05}").into_bytes()
}

fn drain(db: &mut TestDb<MemDevice<4096>>) {
    let mut scratch = Box::new(TestCompaction::new());
    while db.compaction_pending() {
        while block_on(db.compact_step(&mut scratch)).unwrap() == Progress::More {}
    }
}

const fn config() -> Config {
    test_config().with_compression_from_level(0)
}

/// A store of 4,000 keys with compressible values, flushed and
/// compacted: its data blocks are compressed, and most carry a table.
fn build() -> TestDb<MemDevice<4096>> {
    let mut db = TestDb::new(MemDevice::<4096>::new(), config());
    block_on(db.open()).unwrap();
    for i in 0..4000usize {
        let val = format!("v{i:05}:{}", "abcdefghij".repeat(4 + i % 9)).into_bytes();
        for attempt in 0..2 {
            match block_on(db.put(&key(i * 7 % 4000), &val)) {
                Ok(_) => break,
                Err(Error::TableFull | Error::ArenaFull | Error::WalFull) if attempt == 0 => {
                    block_on(db.flush()).unwrap();
                    drain(&mut db);
                }
                Err(e) => panic!("{e:?}"),
            }
        }
    }
    block_on(db.flush()).unwrap();
    drain(&mut db);
    db
}

/// The compressed data blocks of `manifest`'s tables that carry a table
/// of entry points: `(block, points)`.
fn with_points(dev: &MemDevice<4096>, manifest: &Manifest) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    for t in manifest.tables() {
        for b in t.data_first()..t.data_first() + t.data_blocks().unwrap() {
            let b = usize::try_from(b).unwrap();
            let img = dev.blocks()[b];
            let flag = u16::from_le_bytes([img[BODY - 2], img[BODY - 1]]);
            let n = usize::from(u16::from_le_bytes([img[BODY - 4], img[BODY - 3]]));
            if flag & 0x8000 != 0 && n > 0 {
                out.push((b, n));
            }
        }
    }
    out
}

/// Block `b` of `dev` with `edit` applied to its body and its CRC made
/// good again: the damage a writer bug would leave.
fn rewritten(dev: &MemDevice<4096>, b: usize, edit: impl FnOnce(&mut [u8])) -> MemDevice<4096> {
    let mut dev = dev.clone();
    let block = &mut dev.blocks_mut()[b];
    edit(&mut block[..BODY]);
    let crc = crc32(&block[..BODY]);
    block[BODY..].copy_from_slice(&crc.to_le_bytes());
    dev
}

/// Every entry of a detached scan over `manifest` on `dev`, uncached.
fn scan(
    dev: &MemDevice<4096>,
    manifest: &Manifest,
    node: u32,
    rev: bool,
) -> Result<Entries, Error<()>> {
    let page = Box::new(Page::new());
    let (mut k, mut v) = (vec![0u8; 256], vec![0u8; 1024]);
    let mut out = Vec::new();
    let map = |e: Error<<MemDevice<4096> as horton::BlockDevice>::Error>| match e {
        Error::CorruptBlock { id } => Error::CorruptBlock { id },
        other => panic!("{other:?}"),
    };
    if rev {
        let mut s = Box::new(
            RevScan::<_, 4096, 256, 1024, 64, 4096, 7, 4, 1024, 8>::detached(
                &page, manifest, dev, None, node,
            ),
        );
        block_on(s.seek_prev(b"", None, u64::MAX)).map_err(map)?;
        while let Some((kl, vl)) = block_on(s.prev(&mut k, &mut v)).map_err(map)? {
            out.push((k[..kl].to_vec(), v[..vl].to_vec()));
        }
    } else {
        let mut s = Box::new(
            Scan::<_, 4096, 256, 1024, 64, 4096, 7, 4, 1024, 8>::detached(
                &page, manifest, dev, None, node,
            ),
        );
        block_on(s.seek(b"", None, u64::MAX)).map_err(map)?;
        while let Some((kl, vl)) = block_on(s.next(&mut k, &mut v)).map_err(map)? {
            out.push((k[..kl].to_vec(), v[..vl].to_vec()));
        }
    }
    Ok(out)
}

/// The keys an intact block holds, by scanning everything and keeping
/// what a scan over the block alone would see is too costly to set up;
/// instead, every key of the store is read, and the ones whose read
/// touches the damaged block are the ones that may fail.
fn every_key() -> Vec<Vec<u8>> {
    (0..4000).map(key).collect()
}

/// Every whole path finds a wrong table corrupt, and a point read over
/// one answers truly or fails. Each table of the first blocks that carry
/// one is damaged in each field of each point, by one either way, and in
/// its count.
#[test]
fn a_wrong_table_is_corrupt_whole_and_never_a_wrong_answer() {
    let db = build();
    let manifest = *db.manifest();
    let node = db.node_id();
    let want = scan(db.device(), &manifest, node, false).unwrap();
    assert_eq!(want.len(), 4000);
    let truth: std::collections::HashMap<Vec<u8>, Vec<u8>> = want.iter().cloned().collect();
    let dev = db.into_device();
    let blocks = with_points(&dev, &manifest);
    assert!(blocks.len() > 10, "{} blocks carry a table", blocks.len());
    let (mut edits, mut failed_reads, mut true_reads) = (0, 0, 0);
    for &(b, n) in blocks.iter().step_by(blocks.len() / 4).take(4) {
        let from = BODY - 4 - n * 6;
        let mut damage: Vec<Edit> = Vec::new();
        for field in 0..3 * n {
            for delta in [-1i32, 1] {
                damage.push(Box::new(move |body: &mut [u8]| {
                    let at = from + field * 2;
                    let v = i32::from(u16::from_le_bytes([body[at], body[at + 1]])) + delta;
                    let v = u16::try_from(v.max(0)).unwrap();
                    body[at..at + 2].copy_from_slice(&v.to_le_bytes());
                }));
            }
        }
        // A count one too high reads a point out of the stream or fill.
        damage.push(Box::new(move |body: &mut [u8]| {
            let c = u16::try_from(n + 1).unwrap();
            body[BODY - 4..BODY - 2].copy_from_slice(&c.to_le_bytes());
        }));
        for edit in damage {
            let bad = rewritten(&dev, b, edit);
            if bad.blocks()[b] == dev.blocks()[b] {
                continue;
            }
            edits += 1;
            for rev in [false, true] {
                assert!(
                    matches!(
                        scan(&bad, &manifest, node, rev),
                        Err(Error::CorruptBlock { .. })
                    ),
                    "block {b}: a scan (rev {rev}) read a wrong table whole"
                );
            }
            // A store over the damaged device: point reads, then a
            // compaction that reads every table whole.
            let mut db = TestDb::new(bad, config());
            block_on(db.open()).unwrap();
            let mut v = vec![0u8; 1024];
            for k in every_key() {
                match block_on(db.get(&k, &mut v)) {
                    Ok(Some(len)) => {
                        assert_eq!(v[..len], truth[&k][..], "block {b}: a wrong answer");
                        true_reads += 1;
                    }
                    Ok(None) => panic!("block {b}: {k:?} read as absent"),
                    Err(Error::CorruptBlock { .. }) => failed_reads += 1,
                    Err(e) => panic!("{e:?}"),
                }
            }
            // The level of the table the damaged block is in.
            let level = (0..7)
                .find(|&l| {
                    db.manifest().level(l).is_some_and(|ts| {
                        ts.iter().any(|t| {
                            let first = usize::try_from(t.data_first()).unwrap();
                            (first..first + usize::try_from(t.data_blocks().unwrap()).unwrap())
                                .contains(&b)
                        })
                    })
                })
                .unwrap();
            db.request_compaction(level).unwrap();
            let mut scratch = Box::new(TestCompaction::new());
            let mut corrupt = false;
            'compact: while db.compaction_pending() {
                loop {
                    match block_on(db.compact_step(&mut scratch)) {
                        Ok(Progress::More) => {}
                        Ok(_) => break,
                        Err(Error::CorruptBlock { .. }) => {
                            corrupt = true;
                            break 'compact;
                        }
                        Err(e) => panic!("{e:?}"),
                    }
                }
            }
            assert!(corrupt, "block {b}: a compaction read a wrong table whole");
        }
    }
    assert!(edits >= 20, "{edits} edits");
    assert!(failed_reads > 0, "no point read met the damage");
    assert!(true_reads > 0);
}

/// Old images, sealed with no table, read as they always did: a block
/// whose table is zeroed out (the fill a writer before ADR-0040 left)
/// gives every path the same entries.
#[test]
fn blocks_without_a_table_read_the_same() {
    let db = build();
    let manifest = *db.manifest();
    let node = db.node_id();
    let want = scan(db.device(), &manifest, node, false).unwrap();
    let dev = db.into_device();
    let mut plain = dev.clone();
    let mut stripped = 0;
    for (b, n) in with_points(&dev, &manifest) {
        plain = rewritten(&plain, b, |body| body[BODY - 4 - n * 6..BODY - 2].fill(0));
        stripped += 1;
    }
    assert!(stripped > 10);
    assert_eq!(scan(&plain, &manifest, node, false).unwrap(), want);
    let mut rev = scan(&plain, &manifest, node, true).unwrap();
    rev.reverse();
    assert_eq!(rev, want);
    let db = TestDb::new(plain, config());
    let mut db = db;
    block_on(db.open()).unwrap();
    let mut v = vec![0u8; 1024];
    for (k, val) in &want {
        let len = block_on(db.get(k, &mut v)).unwrap().unwrap();
        assert_eq!(&v[..len], &val[..]);
    }
}
