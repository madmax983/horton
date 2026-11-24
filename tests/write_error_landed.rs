//! Review finding (ADR-0029): a block write that reports an error but
//! whose bytes reach the medium (EIO after a partial or complete write, a
//! timeout after the cache took it). `Db::write` and `write_group`
//! treated a failed first block as "nothing landed" and reused the
//! batch's sequence numbers; with spanning batches, a later batch's
//! `CONT` block could then join the failed batch's span head, and
//! recovery replayed half of each. A failed block write now consumes the
//! batch's numbers, as one that landed does.

mod common;

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::task::{Context, Poll};

use common::{block_on, test_config};
use horton::{BlockDevice, Error};

const B: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Fault;

/// A device with a volatile write cache.
#[derive(Default)]
struct Dev {
    durable: BTreeMap<u64, Box<[u8; B]>>,
    pending: Vec<(u64, Box<[u8; B]>)>,
    writes: usize,
    /// This write (by index) reports an error, but its data is cached
    /// like any other write.
    err_but_cached: Cell<Option<usize>>,
    fail_flush: Cell<bool>,
    /// Flushes fail once more than this many writes were made: the
    /// flush that syncs an earlier failure passes, the one after the
    /// next batch's blocks does not.
    fail_flush_after: Cell<Option<usize>>,
}

impl Dev {
    fn view(&self, id: u64) -> [u8; B] {
        let mut b = [0u8; B];
        if let Some(d) = self.durable.get(&id) {
            b.copy_from_slice(&d[..]);
        }
        for (i, d) in &self.pending {
            if *i == id {
                b.copy_from_slice(&d[..]);
            }
        }
        b
    }

    /// Power fails: pending writes number `lose` (by position) are lost,
    /// the rest land, in order.
    fn crash(&mut self, lose: &BTreeSet<usize>) {
        for (n, (id, data)) in std::mem::take(&mut self.pending).into_iter().enumerate() {
            if !lose.contains(&n) {
                self.durable.insert(id, data);
            }
        }
        self.fail_flush.set(false);
        self.fail_flush_after.set(None);
    }
}

impl BlockDevice for Dev {
    type Error = Fault;
    const BLOCK: usize = B;
    fn poll_read_block(
        &self,
        _: &mut Context<'_>,
        id: u64,
        buf: &mut [u8],
    ) -> Poll<Result<(), Fault>> {
        buf[..B].copy_from_slice(&self.view(id));
        Poll::Ready(Ok(()))
    }
    fn poll_write_block(
        &mut self,
        _: &mut Context<'_>,
        id: u64,
        buf: &[u8],
    ) -> Poll<Result<(), Fault>> {
        let n = self.writes;
        self.writes += 1;
        let mut b = Box::new([0u8; B]);
        b.copy_from_slice(&buf[..B]);
        self.pending.push((id, b));
        if self.err_but_cached.get() == Some(n) {
            self.err_but_cached.set(None);
            return Poll::Ready(Err(Fault));
        }
        Poll::Ready(Ok(()))
    }
    fn poll_flush(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Fault>> {
        let after = self.fail_flush_after.get().is_some_and(|w| self.writes > w);
        if self.fail_flush.get() || after {
            return Poll::Ready(Err(Fault));
        }
        for (id, d) in std::mem::take(&mut self.pending) {
            self.durable.insert(id, d);
        }
        Poll::Ready(Ok(()))
    }
}

type Db = horton::Db<Dev, 4096, 256, 1024, 256, 65536, 7, 4, 1024, 8>;

fn batch(tag: &str) -> horton::WriteBatch<256, 1024, 8> {
    // Eight records of 1,028 bytes: three to a block, three blocks.
    let mut b = horton::WriteBatch::new();
    for i in 0..8 {
        b.put(format!("{tag}{i}").as_bytes(), &[tag.as_bytes()[0]; 1000])
            .unwrap();
    }
    b
}

fn get(db: &Db, key: &str) -> bool {
    let mut buf = [0u8; 1024];
    block_on(db.get(key.as_bytes(), &mut buf))
        .expect("get")
        .is_some()
}

/// Through `Db::write`.
#[test]
fn a_failed_span_head_never_joins_a_later_batch() {
    let mut db = Db::new(Dev::default(), test_config());
    block_on(db.open()).expect("open");
    block_on(db.put(b"acked", b"1")).expect("put");
    // D: its first block write reports an error, though the device kept it.
    let w = db.device().writes;
    db.device().err_but_cached.set(Some(w));
    assert_eq!(block_on(db.write(&batch("d"))), Err(Error::Device(Fault)));
    let k = db.device().pending.last().expect("D's head is cached").0;
    // N: another batch, in flight when power fails (its flush fails).
    db.device().fail_flush_after.set(Some(db.device().writes));
    assert_eq!(block_on(db.write(&batch("n"))), Err(Error::Device(Fault)));
    let mut dev = db.into_device();
    // N's write over D's head block is lost; every other write lands.
    let lost = dev
        .pending
        .iter()
        .rposition(|p| p.0 == k)
        .expect("N wrote block k");
    dev.crash(&BTreeSet::from([lost]));
    let mut db = Db::new(dev, test_config());
    block_on(db.open()).expect("reopen");
    assert!(get(&db, "acked"));
    let d: Vec<bool> = (0..8).map(|i| get(&db, &format!("d{i}"))).collect();
    let n: Vec<bool> = (0..8).map(|i| get(&db, &format!("n{i}"))).collect();
    assert!(
        d.iter().all(|&x| x) || d.iter().all(|&x| !x),
        "batch d partial: {d:?}"
    );
    assert!(
        n.iter().all(|&x| x) || n.iter().all(|&x| !x),
        "batch n partial: {n:?}"
    );
}

/// The same through `write_group`.
#[test]
fn a_failed_group_span_head_never_joins_a_later_batch() {
    let mut db = Db::new(Dev::default(), test_config());
    block_on(db.open()).expect("open");
    block_on(db.put(b"acked", b"1")).expect("put");
    let keys: Vec<String> = (0..8).map(|i| format!("d{i}")).collect();
    let val = [b'd'; 1000];
    let unit: Vec<horton::GroupOp<'_>> = keys
        .iter()
        .map(|k| horton::GroupOp::put(k.as_bytes(), &val))
        .collect();
    let w = db.device().writes;
    db.device().err_but_cached.set(Some(w));
    assert_eq!(
        block_on(db.write_group(&[&unit])).map(|c| c.units),
        Err(Error::Device(Fault))
    );
    let k = db.device().pending.last().expect("D's head is cached").0;
    db.device().fail_flush_after.set(Some(db.device().writes));
    assert_eq!(block_on(db.write(&batch("n"))), Err(Error::Device(Fault)));
    let mut dev = db.into_device();
    let lost = dev
        .pending
        .iter()
        .rposition(|p| p.0 == k)
        .expect("N wrote block k");
    dev.crash(&BTreeSet::from([lost]));
    let mut db = Db::new(dev, test_config());
    block_on(db.open()).expect("reopen");
    let d: Vec<bool> = (0..8).map(|i| get(&db, &format!("d{i}"))).collect();
    let n: Vec<bool> = (0..8).map(|i| get(&db, &format!("n{i}"))).collect();
    assert!(
        d.iter().all(|&x| x) || d.iter().all(|&x| !x),
        "unit d partial: {d:?}"
    );
    assert!(
        n.iter().all(|&x| x) || n.iter().all(|&x| !x),
        "batch n partial: {n:?}"
    );
}
