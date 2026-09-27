//! Quick start: open a database on a RAM-backed block device, write, read,
//! scan, flush, and compact. Mirrors the README walkthrough.
//!
//! Run with `cargo run --example quickstart`.
//!
//! `std` is fine here: examples are host programs. The library itself is
//! `#![no_std]` and allocates nothing — the `Vec` below is the *device*,
//! standing in for flash or a disk partition.

use core::future::Future;
use core::pin::pin;
use core::task::{Context, Poll, Waker};

use horton::{BlockDevice, Config, Error, Progress, Scan};

/// Block size shared by the device and the database (`D::BLOCK == BLOCK`
/// is a compile-time check).
const BLOCK: usize = 4096;

// The database types: every size is a const generic, so the whole thing
// is one fixed-size value with no heap behind it. `db_types!` names each
// parameter, so two swapped sizes cannot compile.
horton::db_types! {
    block: BLOCK,
    key_max: 64,
    val_max: 256,
    memtable_entries: 64,
    memtable_arena: 8192,
    levels: 4,
    tables_per_level: 4,
    bloom_bytes: 256,
    cache_blocks: 4;

    /// The database (`MyDb<D>` for any device `D`).
    type Db = MyDb;
    /// Caller-owned compaction scratch for `MyDb`.
    type Compaction = MyCompaction;
}

/// A block device backed by a `Vec` of blocks. Unwritten blocks read as
/// zeros. Every call completes immediately (`Poll::Ready`).
struct RamDisk {
    blocks: Vec<[u8; BLOCK]>,
}

impl RamDisk {
    fn new(n: usize) -> Self {
        Self {
            blocks: vec![[0u8; BLOCK]; n],
        }
    }
}

/// Out-of-range block id.
#[derive(Debug)]
struct OutOfRange;

impl BlockDevice for RamDisk {
    type Error = OutOfRange;
    const BLOCK: usize = BLOCK;

    fn poll_read_block(
        &self,
        _cx: &mut Context<'_>,
        id: u64,
        buf: &mut [u8],
    ) -> Poll<Result<(), Self::Error>> {
        let block = usize::try_from(id).ok().and_then(|i| self.blocks.get(i));
        Poll::Ready(block.map_or(Err(OutOfRange), |b| {
            buf.copy_from_slice(b);
            Ok(())
        }))
    }

    fn poll_write_block(
        &mut self,
        _cx: &mut Context<'_>,
        id: u64,
        buf: &[u8],
    ) -> Poll<Result<(), Self::Error>> {
        let block = usize::try_from(id)
            .ok()
            .and_then(|i| self.blocks.get_mut(i));
        Poll::Ready(block.map_or(Err(OutOfRange), |b| {
            b.copy_from_slice(buf);
            Ok(())
        }))
    }

    fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

/// Minimal executor: horton ships none, so bring your own (embassy, RTIC,
/// a hand-rolled poll loop, …). This one busy-polls, which is fine for a
/// device that is always ready.
fn block_on<F: Future>(fut: F) -> F::Output {
    let mut fut = pin!(fut);
    let mut cx = Context::from_waker(Waker::noop());
    loop {
        if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
            return v;
        }
    }
}

fn main() -> Result<(), Error<OutOfRange>> {
    // Device layout, in block ids: two manifest slots, a WAL region, and
    // a table region. The regions must not overlap.
    let config = Config::new(
        2,   // wal_start
        66,  // wal_end   (64 WAL blocks)
        66,  // tbl_start
        512, // tbl_end   (446 table blocks)
        0,   // manifest slot A
        1,   // manifest slot B
    );

    // `Db` is ~45 KiB with these parameters (`Scan` ~11 KiB, `Compaction`
    // ~58 KiB). On a microcontroller they live in `static`s; on a host, box
    // them so they don't sit on the stack.
    let mut db = Box::new(MyDb::new(RamDisk::new(512), config));
    let report = block_on(db.open())?;
    println!("opened: {report:?}");

    // Writes are durable (WAL block written and flushed) before they return.
    block_on(db.put(b"sensor/001", b"21.5C"))?;
    block_on(db.put(b"sensor/002", b"19.0C"))?;
    block_on(db.put(b"sensor/003", b"22.1C"))?;
    block_on(db.delete(b"sensor/002"))?;

    // Point read into a caller buffer. `BufferTooSmall { need }` instead of
    // truncation if the buffer is short.
    let mut val = [0u8; 256];
    if let Some(n) = block_on(db.get(b"sensor/001", &mut val))? {
        println!("sensor/001 = {}", String::from_utf8_lossy(&val[..n]));
    }
    assert_eq!(block_on(db.get(b"sensor/002", &mut val))?, None);

    // Move the memtable into an immutable on-device table.
    block_on(db.flush())?;

    // Range scan over [sensor/, sensor0) at the latest sequence number.
    // The scan borrows `db`, so writes can't move data under it.
    {
        let mut scan = Box::new(Scan::new(&db));
        block_on(scan.seek(b"sensor/", Some(b"sensor0"), u64::MAX))?;
        let mut key = [0u8; 64];
        while let Some((kl, vl)) = block_on(scan.next(&mut key, &mut val))? {
            println!(
                "scan: {} = {}",
                String::from_utf8_lossy(&key[..kl]),
                String::from_utf8_lossy(&val[..vl])
            );
        }
    }

    // Compaction is caller-driven and bounded: each step seals at most one
    // output block, so firmware can interleave it with real-time work.
    let mut scratch = Box::new(MyCompaction::new());
    while db.compaction_pending() {
        while block_on(db.compact_step(&mut scratch))? == Progress::More {}
    }

    // Reopen from the same device: the WAL replays, tables come back from
    // the manifest.
    let device = db.into_device();
    let mut db = Box::new(MyDb::new(device, config));
    block_on(db.open())?;
    let n = block_on(db.get(b"sensor/003", &mut val))?.unwrap_or(0);
    println!(
        "after reopen: sensor/003 = {}",
        String::from_utf8_lossy(&val[..n])
    );
    Ok(())
}
