//! Compression by level: tables written into levels above
//! `Config::compression_from_level` (2 by default: level 0's flushes and
//! level 1's compaction outputs) store their data blocks raw; tables
//! written deeper trial-compress them as before. A table compaction
//! moves down whole keeps its blocks as they are. A table's blocks are a fixed
//! size and entries are packed by their raw size, so a compressed table
//! takes as many blocks as a raw one: compression there costs a pass to
//! write and one to read, and saves no blocks.

mod common;

use std::future::poll_fn;

use common::{MemDevice, TestDb, block_on, test_config, tight_config};
use horton::{BlockDevice, Config, Progress};

const BLOCK: usize = 4096;
/// The CRC trailer at the end of every block.
const CRC_LEN: usize = 4;

/// Whether data block `id` is stored compressed: bit 15 of the restart
/// count, the last `u16` before the CRC (ADR-0008).
fn compressed(dev: &MemDevice<BLOCK>, id: u64) -> bool {
    let mut buf = [0u8; BLOCK];
    block_on(poll_fn(|cx| dev.poll_read_block(cx, id, &mut buf))).expect("read");
    let at = BLOCK - CRC_LEN - 2;
    u16::from_le_bytes([buf[at], buf[at + 1]]) & 0x8000 != 0
}

/// For each level, one entry per table: how many of its data blocks are
/// stored compressed, and how many it has.
fn census(db: &TestDb<MemDevice<BLOCK>>) -> Vec<Vec<(usize, usize)>> {
    (0..7)
        .map(|level| {
            db.level_tables(level)
                .unwrap_or(&[])
                .iter()
                .map(|t| {
                    let end = t.rdel_first().expect("rdel section");
                    let ids = t.first_block..end;
                    let n = ids.clone().count();
                    let c = ids.filter(|&id| compressed(db.device(), id)).count();
                    (c, n)
                })
                .collect()
        })
        .collect()
}

fn key(i: u32) -> Vec<u8> {
    format!("key{i:06}").into_bytes()
}

/// A value that compresses well: its key's number, then a run of one
/// byte.
fn val(i: u32) -> Vec<u8> {
    let mut v = format!("value of {i}:").into_bytes();
    v.resize(48, b'.');
    v
}

/// Writes `rounds` batches of 60 new keys, flushing each and letting
/// compaction run to the end after it. Half way, every table is pushed
/// down to the bottom level (`request_compaction(0)`), so the store ends
/// with tables both in the shallow levels and deep.
fn fill(config: Config, rounds: u32) -> TestDb<MemDevice<BLOCK>> {
    let mut db = TestDb::new(MemDevice::<BLOCK>::new(), config);
    block_on(db.open()).expect("open");
    let mut c = Box::new(horton::Compaction::<BLOCK, 256, 1024, 1024>::new());
    for round in 0..rounds {
        if rounds > 1 && round == rounds / 2 {
            db.request_compaction(0).expect("request");
            while db.compaction_pending() {
                while block_on(db.compact_step(&mut c)).expect("compact") == Progress::More {}
            }
        }
        // One batch a round: about two WAL blocks, not sixty.
        let mut batch = horton::WriteBatch::<256, 1024, 64>::new();
        for i in 0..60 {
            let k = round * 60 + i;
            batch.put(&key(k), &val(k)).expect("op");
        }
        block_on(db.write(&batch)).expect("write");
        block_on(db.flush()).expect("flush");
        while db.compaction_pending() {
            while block_on(db.compact_step(&mut c)).expect("compact") == Progress::More {}
        }
    }
    db
}

fn check_reads(db: &TestDb<MemDevice<BLOCK>>, rounds: u32) {
    let mut buf = [0u8; 1024];
    for k in 0..rounds * 60 {
        let n = block_on(db.get(&key(k), &mut buf))
            .expect("get")
            .unwrap_or_else(|| panic!("key {k} lost"));
        assert_eq!(&buf[..n], val(k).as_slice(), "key {k}");
    }
}

/// By default the tables of levels 0 and 1 are raw and deeper ones
/// compressed, and every key reads back across the mix.
#[test]
fn levels_0_and_1_are_raw_and_deeper_levels_compressed() {
    let rounds = 40;
    let db = fill(test_config(), rounds);
    let levels = census(&db);
    assert!(
        !levels[1].is_empty() && levels[2..].iter().any(|l| !l.is_empty()),
        "setup: tables in level 1 and deeper: {levels:?}"
    );
    for (level, tables) in levels.iter().enumerate() {
        for &(c, n) in tables {
            if level < 2 {
                assert_eq!(c, 0, "level {level}: {c} of {n} data blocks compressed");
            } else {
                assert!(c > 0, "level {level}: none of {n} data blocks compressed");
            }
        }
    }
    check_reads(&db, rounds);
    // And after a reopen, from the device alone.
    let mut db2 = TestDb::new(db.into_device(), test_config());
    block_on(db2.open()).expect("reopen");
    check_reads(&db2, rounds);
}

/// A flush, which writes level 0, stores its table raw by default, and
/// compressed when the threshold is level 0.
#[test]
fn a_flush_follows_the_threshold() {
    for (from, want_compressed) in [(2, false), (1, false), (0, true)] {
        let db = fill(test_config().with_compression_from_level(from), 1);
        let levels = census(&db);
        let &(c, n) = levels[0].first().expect("a level-0 table");
        assert_eq!(c > 0, want_compressed, "from level {from}: {c} of {n}");
        assert_eq!(db.config().compression_from_level, from);
        check_reads(&db, 1);
    }
}

/// With the threshold at 0 every level compresses, as before; with it
/// past the last level none does.
#[test]
fn the_threshold_covers_every_level_or_none() {
    let rounds = 40;
    for (from, all) in [(0u8, true), (7, false)] {
        let db = fill(test_config().with_compression_from_level(from), rounds);
        for (level, tables) in census(&db).iter().enumerate() {
            for &(c, n) in tables {
                assert_eq!(c > 0, all, "from {from}, level {level}: {c} of {n}");
            }
        }
        check_reads(&db, rounds);
    }
}

/// The default is level 2, on both constructors.
#[test]
fn the_default_threshold_is_level_2() {
    assert_eq!(Config::whole_device(1024).compression_from_level, 2);
    assert_eq!(test_config().compression_from_level, 2);
}

/// A table compaction moves down whole, with nothing below to merge it
/// with, keeps the blocks it was written with: a raw level-1 table moved
/// to level 2 or deeper stays raw, and reads the same. The threshold
/// applies to tables being written; rewriting a table only to compress
/// it would cost block writes and save none (ADR-0030).
#[test]
fn a_table_moved_down_keeps_its_blocks() {
    // tight_config: slots of 8 blocks, so a table that overlaps nothing
    // below moves down whole. Keys in order, so nothing overlaps.
    let config = tight_config();
    let mut db = TestDb::new(MemDevice::<BLOCK>::new(), config);
    block_on(db.open()).expect("open");
    let mut c = Box::new(horton::Compaction::<BLOCK, 256, 1024, 1024>::new());
    let rounds = 50;
    for round in 0..rounds {
        let mut batch = horton::WriteBatch::<256, 1024, 64>::new();
        for i in 0..60 {
            let k = round * 60 + i;
            batch.put(&key(k), &val(k)).expect("op");
        }
        block_on(db.write(&batch)).expect("write");
        block_on(db.flush()).expect("flush");
        while db.compaction_pending() {
            while block_on(db.compact_step(&mut c)).expect("compact") == Progress::More {}
        }
    }
    let levels = census(&db);
    let deep_raw = levels[2..]
        .iter()
        .flatten()
        .filter(|&&(c, _)| c == 0)
        .count();
    assert!(
        deep_raw > 0,
        "setup: a raw table moved below level 1: {levels:?}"
    );
    for tables in &levels[..2] {
        for &(c, n) in tables {
            assert_eq!(c, 0, "levels 0 and 1 are written raw: {c} of {n}");
        }
    }
    check_reads(&db, rounds);
}
