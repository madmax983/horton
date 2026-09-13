//! v0.3: the full point-read path.
//!
//! Tables are hand-placed at chosen block offsets and levels (standing in
//! for compaction output, which arrives in v0.4), then `Db::get` must honor
//! highest-sequence-wins across levels, tombstone shadowing, key-range
//! pruning, and sequence pruning.

mod common;

use core::future::Future;
use core::pin::pin;
use core::task::{Context, Poll};
use std::cell::Cell;
use std::rc::Rc;

use common::{block_on, noop_waker, test_config, MemDevice};
use horton::{bloom_k, plan_table, write_table, BlockDevice, Config, Manifest, SstEntry, TableRef};

type DevError = core::convert::Infallible;
type TestManifest = Manifest<7, 4, 256>;
type TestTableRef = TableRef<256>;

/// Writes a single-entry table at `base` and returns its `TableRef`.
fn write_single(
    dev: &mut MemDevice<4096>,
    base: u64,
    key: &[u8],
    val: &[u8],
    seq: u64,
    tombstone: bool,
    id: u32,
) -> TestTableRef {
    let entry = || SstEntry {
        key,
        val,
        seq,
        tombstone,
    };
    let plan = plan_table::<DevError, 4096, 256>(core::iter::once(entry())).expect("plan table");
    let k = bloom_k(1024 * 8, plan.entry_count);
    let mut data = [0u8; 4096];
    let mut index = [0u8; 4096];
    let mut bloom = [0u8; 1024];
    let written = block_on(write_table::<MemDevice<4096>, 4096, 1024>(
        dev,
        base,
        k,
        core::iter::once(entry()),
        &mut data,
        &mut index,
        &mut bloom,
    ))
    .expect("write table");
    let total = plan.data_blocks + 3;
    assert_eq!(written, total);
    TestTableRef {
        id,
        first_block: base,
        block_count: u32::try_from(total).expect("block count fits"),
        first_key: plan.first_key,
        last_key: plan.last_key,
        max_seq: plan.max_seq,
        entry_count: u32::try_from(plan.entry_count).expect("entry count fits"),
    }
}

/// Commits a manifest placing `tables` at their levels.
fn commit_tables(dev: &mut MemDevice<4096>, tables: &[(usize, TestTableRef)]) {
    let mut manifest = TestManifest::new();
    manifest.set_wal_head(8);
    for (level, tref) in tables {
        manifest
            .add_table_to_level::<DevError>(*level, *tref)
            .expect("place table");
    }
    let mut scratch = [0u8; 4096];
    block_on(manifest.commit(dev, &mut scratch, 0, 1)).expect("commit manifest");
}

fn get_str(db: &MemDb, key: &[u8]) -> Option<Vec<u8>> {
    let mut buf = [0u8; 1024];
    block_on(db.get(key, &mut buf))
        .expect("get")
        .map(|n| buf[..n].to_vec())
}

/// In-memory device counting reads against the table region (`>= 136`).
/// Manifest slots and the WAL live below it, so the counter only moves
/// when `get` actually consults a table. The count is shared through an
/// `Rc` because `Db` owns the device.
struct CountingDevice {
    inner: MemDevice<4096>,
    reads: Rc<Cell<u64>>,
}

impl BlockDevice for CountingDevice {
    type Error = DevError;
    const BLOCK: usize = 4096;

    fn poll_read_block(
        &self,
        cx: &mut Context<'_>,
        id: u64,
        buf: &mut [u8],
    ) -> Poll<Result<(), Self::Error>> {
        if id >= 136 {
            self.reads.set(self.reads.get() + 1);
        }
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

type CountDb = horton::Db<CountingDevice, 4096, 256, 1024, 64, 4096, 7, 4, 1024, 4096>;
type MemDb = horton::Db<MemDevice<4096>, 4096, 256, 1024, 64, 4096, 7, 4, 1024, 4096>;

fn open_mem_db(dev: MemDevice<4096>, config: Config) -> MemDb {
    let mut db = MemDb::new(dev, config);
    block_on(db.open()).expect("open");
    db
}

#[test]
fn highest_seq_wins_across_levels() {
    // The newest version sits in the DEEPEST level: level order must not
    // dominate, the entry sequence decides.
    let mut dev = MemDevice::new();
    let t0 = write_single(&mut dev, 136, b"k", b"v-old", 1, false, 0);
    let t1 = write_single(&mut dev, 140, b"k", b"v-mid", 2, false, 1);
    let t2 = write_single(&mut dev, 144, b"k", b"v-new", 3, false, 2);
    commit_tables(&mut dev, &[(0, t0), (1, t1), (2, t2)]);

    let db = open_mem_db(dev, test_config());
    assert_eq!(get_str(&db, b"k"), Some(b"v-new".to_vec()));
}

#[test]
fn older_seq_in_newer_level_loses() {
    // Mirror image: the highest sequence lives in L0 while a deeper level
    // holds a stale version.
    let mut dev = MemDevice::new();
    let t0 = write_single(&mut dev, 136, b"k", b"v-new", 5, false, 0);
    let t2 = write_single(&mut dev, 140, b"k", b"v-stale", 3, false, 1);
    commit_tables(&mut dev, &[(0, t0), (2, t2)]);

    let db = open_mem_db(dev, test_config());
    assert_eq!(get_str(&db, b"k"), Some(b"v-new".to_vec()));
}

#[test]
fn tombstone_at_highest_seq_hides_older_values() {
    let mut dev = MemDevice::new();
    let t0 = write_single(&mut dev, 136, b"k", b"v-old", 1, false, 0);
    let t1 = write_single(&mut dev, 140, b"k", b"", 4, true, 1);
    let t2 = write_single(&mut dev, 144, b"k", b"v-mid", 3, false, 2);
    commit_tables(&mut dev, &[(0, t0), (1, t1), (2, t2)]);

    let db = open_mem_db(dev, test_config());
    assert_eq!(get_str(&db, b"k"), None);
}

#[test]
fn older_tombstone_loses_to_newer_value() {
    let mut dev = MemDevice::new();
    let t0 = write_single(&mut dev, 136, b"k", b"", 2, true, 0);
    let t1 = write_single(&mut dev, 140, b"k", b"v-resurrected", 5, false, 1);
    commit_tables(&mut dev, &[(0, t0), (1, t1)]);

    let db = open_mem_db(dev, test_config());
    assert_eq!(get_str(&db, b"k"), Some(b"v-resurrected".to_vec()));
}

#[test]
fn range_prune_skips_out_of_range_tables() {
    // One table covers "b", the other covers "q". Each consulted table
    // costs exactly 4 block reads (footer, bloom, index, one data block).
    let mut dev = MemDevice::new();
    let t1 = write_single(&mut dev, 136, b"b", b"vb", 1, false, 0);
    let t2 = write_single(&mut dev, 140, b"q", b"vq", 2, false, 1);
    commit_tables(&mut dev, &[(0, t1), (0, t2)]);

    let reads = Rc::new(Cell::new(0u64));
    let dev = CountingDevice {
        inner: dev,
        reads: reads.clone(),
    };
    let mut db = CountDb::new(dev, test_config());
    block_on(db.open()).expect("open");

    // Only the "b" table is read; the "q" table is range-pruned.
    let mut buf = [0u8; 1024];
    let n = block_on(db.get(b"b", &mut buf))
        .expect("get")
        .expect("present");
    assert_eq!(&buf[..n], b"vb");
    assert_eq!(reads.get(), 4);

    // Mirror image: only the "q" table is read.
    let n = block_on(db.get(b"q", &mut buf))
        .expect("get")
        .expect("present");
    assert_eq!(&buf[..n], b"vq");
    assert_eq!(reads.get(), 8);

    // "m" is covered by neither table: zero table I/O, still a miss.
    assert_eq!(block_on(db.get(b"m", &mut buf)).expect("get"), None);
    assert_eq!(reads.get(), 8);
}

/// Returns `Poll::Pending` once, on the first table-region read
/// (`id >= 136`). Every read before or after that returns `Ready`.
/// Reads below 136 are manifest and WAL reads, for example the reads in
/// `Db::open`. They always return `Ready`. This forces a real suspend
/// point inside `Db::get`. A test can then poll two `get` futures at once.
struct PendingOnceDevice {
    inner: MemDevice<4096>,
    yielded: Cell<bool>,
}

impl BlockDevice for PendingOnceDevice {
    type Error = DevError;
    const BLOCK: usize = 4096;

    fn poll_read_block(
        &self,
        cx: &mut Context<'_>,
        id: u64,
        buf: &mut [u8],
    ) -> Poll<Result<(), Self::Error>> {
        if id >= 136 && !self.yielded.replace(true) {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
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

#[test]
fn concurrent_gets_do_not_panic_when_interleaved() {
    // `get` takes `&self`. An executor can run two calls at once.
    // It can interleave their polls.
    // `PendingOnceDevice` forces the first `get` to suspend. This
    // happens right after it claims the shared scratch buffer.
    // The first `get` is still suspended. The second `get` is polled
    // during this time. It must fall back to its own buffer instead of
    // panicking. Both calls must still return the right value.
    let mut dev = MemDevice::new();
    let t0 = write_single(&mut dev, 136, b"alpha", b"AAAA", 1, false, 0);
    commit_tables(&mut dev, &[(0, t0)]);
    let dev = PendingOnceDevice {
        inner: dev,
        yielded: Cell::new(false),
    };
    type PendingDb = horton::Db<PendingOnceDevice, 4096, 256, 1024, 64, 4096, 7, 4, 1024, 4096>;
    let mut db = PendingDb::new(dev, test_config());
    block_on(db.open()).expect("open");

    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut buf_a = [0u8; 1024];
    let mut buf_b = [0u8; 1024];
    let mut done_a = None;
    let mut done_b = None;
    {
        let mut fut_a = pin!(db.get(b"alpha", &mut buf_a));
        let mut fut_b = pin!(db.get(b"alpha", &mut buf_b));
        for _ in 0..64 {
            if done_a.is_none() {
                if let Poll::Ready(r) = fut_a.as_mut().poll(&mut cx) {
                    done_a = Some(r);
                }
            }
            if done_b.is_none() {
                if let Poll::Ready(r) = fut_b.as_mut().poll(&mut cx) {
                    done_b = Some(r);
                }
            }
            if done_a.is_some() && done_b.is_some() {
                break;
            }
        }
    }

    let n_a = done_a
        .expect("fut_a completed")
        .expect("get ok")
        .expect("present");
    let n_b = done_b
        .expect("fut_b completed")
        .expect("get ok")
        .expect("present");
    assert_eq!(&buf_a[..n_a], b"AAAA");
    assert_eq!(&buf_b[..n_b], b"AAAA");
}

#[test]
fn seq_prune_skips_shadowed_tables() {
    // L0 holds two versions; the newer table is consulted first and the
    // older one is sequence-pruned — it cannot beat the hit in hand.
    let mut dev = MemDevice::new();
    let t_old = write_single(&mut dev, 136, b"k", b"v-old", 5, false, 0);
    let t_new = write_single(&mut dev, 140, b"k", b"v-new", 10, false, 1);
    commit_tables(&mut dev, &[(0, t_old), (0, t_new)]);

    let reads = Rc::new(Cell::new(0u64));
    let dev = CountingDevice {
        inner: dev,
        reads: reads.clone(),
    };
    let mut db = CountDb::new(dev, test_config());
    block_on(db.open()).expect("open");

    let mut buf = [0u8; 1024];
    let n = block_on(db.get(b"k", &mut buf))
        .expect("get")
        .expect("present");
    assert_eq!(&buf[..n], b"v-new");
    // Exactly one table's worth of reads: the older table was pruned.
    assert_eq!(reads.get(), 4);
}
