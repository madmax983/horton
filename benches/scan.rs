//! Deterministic instruction-count harness for the merge scan iterator
//! (`Scan::seek` / `Scan::next`, `src/scan.rs`), profiled with
//! `valgrind --tool=callgrind`.
//!
//! `harness = false`: this is a plain `main`, not the (nightly-only) libtest
//! bench harness, so it can be built in release mode and run directly under
//! valgrind without any extra dependency. `std` is fine here — this is a
//! benchmark, not library code.
//!
//! Workload: `scan.rs` (added in v0.5) has never had a dedicated benchmark —
//! `write_path.rs`/`write_only.rs`/`wal_recovery.rs`/`compaction.rs` drive
//! `put`/`get`/`delete`/`compact_step` but none of them ever call `Scan`, the
//! same coverage gap `compact.rs` had before #11. This harness closes it:
//! sustained unique-key puts across many small batches (same shape as
//! `compaction.rs`) so the resulting tree spans every level with real
//! cross-level overlap, then a mixed pass of bounded range scans (narrow and
//! wide), a couple of full scans, and one snapshot-pinned scan taken
//! mid-write — the shapes a real range-query workload actually uses. Fixed-
//! seed PRNG, fully deterministic.

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use horton::{Compaction, Config, Db, Progress, Scan};

/// Drives a future to completion. Our device is always `Ready`, so this
/// never actually spins.
fn block_on<F: Future>(future: F) -> F::Output {
    const unsafe fn waker_clone(data: *const ()) -> RawWaker {
        RawWaker::new(data, &WAKER_VTABLE)
    }
    const unsafe fn waker_noop(_data: *const ()) {}
    static WAKER_VTABLE: RawWakerVTable =
        RawWakerVTable::new(waker_clone, waker_noop, waker_noop, waker_noop);
    let waker = unsafe { Waker::from_raw(RawWaker::new(core::ptr::null(), &WAKER_VTABLE)) };
    let mut cx = Context::from_waker(&waker);
    let mut future = future;
    let pinned = unsafe { Pin::new_unchecked(&mut future) };
    match pinned.poll(&mut cx) {
        Poll::Ready(value) => value,
        Poll::Pending => unreachable!("bench device is always Ready"),
    }
}

/// In-memory block device: a growable vector of zeroed blocks.
struct MemDevice<const BLOCK: usize> {
    blocks: Vec<[u8; BLOCK]>,
}

impl<const BLOCK: usize> MemDevice<BLOCK> {
    const fn new() -> Self {
        Self { blocks: Vec::new() }
    }
}

impl<const BLOCK: usize> horton::BlockDevice for MemDevice<BLOCK> {
    type Error = core::convert::Infallible;
    const BLOCK: usize = BLOCK;

    fn poll_read_block(
        &self,
        _cx: &mut Context<'_>,
        id: u64,
        buf: &mut [u8],
    ) -> Poll<Result<(), Self::Error>> {
        let mut block = [0u8; BLOCK];
        if let Some(i) = usize::try_from(id).ok().and_then(|i| self.blocks.get(i)) {
            block.copy_from_slice(i);
        }
        let n = buf.len().min(BLOCK);
        buf[..n].copy_from_slice(&block[..n]);
        Poll::Ready(Ok(()))
    }

    fn poll_write_block(
        &mut self,
        _cx: &mut Context<'_>,
        id: u64,
        buf: &[u8],
    ) -> Poll<Result<(), Self::Error>> {
        if let Ok(i) = usize::try_from(id) {
            if buf.len() == BLOCK {
                while self.blocks.len() <= i {
                    self.blocks.push([0u8; BLOCK]);
                }
                self.blocks[i].copy_from_slice(buf);
            }
        }
        Poll::Ready(Ok(()))
    }

    fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

/// Deterministic LCG PRNG — no `rand` dependency.
struct Lcg(u64);

impl Lcg {
    const fn new(seed: u64) -> Self {
        Self(seed)
    }

    const fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 33
    }

    /// Uniform in `[lo, hi]`.
    const fn range(&mut self, lo: usize, hi: usize) -> usize {
        let span = (hi - lo + 1) as u64; // widening cast: no truncation possible
        #[allow(clippy::cast_possible_truncation)]
        let offset = (self.next() % span) as usize;
        lo + offset
    }
}

fn make_key(buf: &mut [u8; 32], rng: &mut Lcg, len: usize) -> usize {
    for b in &mut buf[..len] {
        #[allow(clippy::cast_possible_truncation)]
        let v = (rng.next() % 26) as u8;
        *b = v + b'a';
    }
    len
}

fn make_val(buf: &mut [u8; 256], rng: &mut Lcg, len: usize) -> usize {
    #[allow(clippy::cast_possible_truncation)]
    for b in &mut buf[..len] {
        let v = (rng.next() % 256) as u8;
        *b = v;
    }
    len
}

/// Same shape as the other benches: 4 KiB blocks, 64 B keys, 256 B values,
/// 512-slot / 64 KiB memtable, 7 levels, 4 tables per level, 1 KiB bloom
/// filters, 8192-slot free list.
type BenchDb = Db<MemDevice<4096>, 4096, 64, 256, 512, 65536, 7, 4, 1024, 8192>;
type BenchCompaction = Compaction<4096, 64, 256, 1024>;
type BenchScan<'d> = Scan<'d, MemDevice<4096>, 4096, 64, 256, 512, 65536, 7, 4, 1024, 8192>;

// Small batches (well under the 512-slot memtable cap) so L0 fills — and
// compaction cascades into deeper levels — repeatedly over the run, the same
// shape `compaction.rs` uses to get real cross-level table overlap instead
// of one flat run of tables.
const WAL_START: u64 = 8;
const WAL_END: u64 = 8 + 600;
const TBL_START: u64 = WAL_END;
const TBL_END: u64 = TBL_START + 200_000;

const fn config() -> Config {
    Config::new(WAL_START, WAL_END, TBL_START, TBL_END, 0, 1)
}

const PUT_BATCH: usize = 100;
const NUM_BATCHES: usize = 60;
/// Bounded range scans over random windows of the written keyspace.
const RANGE_SCANS: usize = 300;
/// Unbounded full scans (`seek(b"", None, ..)`), the most expensive shape.
const FULL_SCANS: usize = 20;

/// Drains every pending compaction job (one fresh scratch per job, matching
/// `tests/compact.rs`'s `drain` helper).
fn drain_compaction(db: &mut BenchDb, batch: usize) {
    while db.compaction_pending() {
        let mut c = BenchCompaction::new();
        loop {
            match block_on(db.compact_step(&mut c)) {
                Ok(Progress::More) => {}
                Ok(Progress::Done) => break,
                Err(e) => panic!("unexpected compaction error at batch {batch}: {e:?}"),
            }
        }
    }
}

/// Runs a scan over `[start, end)` at `max_seq`, returning the number of
/// entries yielded and the total bytes copied (keys + values) — a cheap
/// checksum that keeps the loop from being optimized away without
/// allocating anything.
fn run_scan(
    scan: &mut BenchScan<'_>,
    start: &[u8],
    end: Option<&[u8]>,
    max_seq: u64,
) -> (u64, u64) {
    block_on(scan.seek(start, end, max_seq)).expect("seek");
    let mut kbuf = [0u8; 64];
    let mut vbuf = [0u8; 256];
    let mut count = 0u64;
    let mut bytes = 0u64;
    while let Some((klen, vlen)) = block_on(scan.next(&mut kbuf, &mut vbuf)).expect("next") {
        count += 1;
        bytes += (klen + vlen) as u64;
    }
    (count, bytes)
}

fn main() {
    let device = MemDevice::<4096>::new();
    let mut db = BenchDb::new(device, config());
    block_on(db.open()).expect("open");

    let mut rng = Lcg::new(0xC0FF_EE12_3456_789A);
    let mut kbuf = [0u8; 32];
    let mut vbuf = [0u8; 256];
    let mut written: Vec<Vec<u8>> = Vec::with_capacity(PUT_BATCH * NUM_BATCHES);

    // Mid-write snapshot: pinned after batch 30, so the snapshot-scan pass
    // below has to filter out everything written afterwards (a real
    // "read as of X" workload, not just a full-visibility scan).
    let mut snap_seq = 0u64;

    for batch in 0..NUM_BATCHES {
        for i in 0..PUT_BATCH {
            let klen = rng.range(8, 32);
            make_key(&mut kbuf, &mut rng, klen);
            // Same monotonic-prefix trick as `compaction.rs`: keys grow with
            // `batch`, so tables from different batches mostly don't
            // overlap — but adjacent-batch tables at different levels do,
            // which is exactly the cross-level overlap a scan has to merge.
            #[allow(clippy::cast_possible_truncation)]
            {
                kbuf[0] = b'a' + ((batch / 26) % 26) as u8;
                kbuf[1] = b'a' + (batch % 26) as u8;
                kbuf[2] = b'a' + ((i / 26) % 26) as u8;
                kbuf[3] = b'a' + (i % 26) as u8;
            }
            let vlen = rng.range(16, 200);
            make_val(&mut vbuf, &mut rng, vlen);
            block_on(db.put(&kbuf[..klen], &vbuf[..vlen])).expect("put");
            written.push(kbuf[..klen].to_vec());
        }
        block_on(db.flush()).unwrap_or_else(|e| panic!("flush at batch {batch}: {e:?}"));
        drain_compaction(&mut db, batch);
        if batch == 30 {
            snap_seq = db.snapshot().expect("snapshot");
        }
    }

    // Range-scan pass: windows of random width anchored at a random written
    // key, over the now heavily-compacted, multi-level tree.
    let mut total_count = 0u64;
    let mut total_bytes = 0u64;
    {
        let mut scan = BenchScan::new(&db);
        for i in 0..RANGE_SCANS {
            let start_key = &written[(i * 37) % written.len()];
            let end_idx = (i * 37 + rng.range(1, 200)) % written.len();
            let end_key = &written[end_idx];
            let (start, end): (&[u8], &[u8]) = if start_key.as_slice() <= end_key.as_slice() {
                (start_key, end_key)
            } else {
                (end_key, start_key)
            };
            let (c, b) = run_scan(&mut scan, start, Some(end), u64::MAX);
            total_count += c;
            total_bytes += b;
        }

        // Full-scan pass: no bounds, latest view — the most expensive shape
        // since every table cursor is live for the whole scan.
        for _ in 0..FULL_SCANS {
            let (c, b) = run_scan(&mut scan, b"", None, u64::MAX);
            total_count += c;
            total_bytes += b;
        }

        // Snapshot-pinned full scan: only mutations up to `snap_seq` are
        // visible, exercising the `seq <= max_seq` filter on every cursor.
        let (c, b) = run_scan(&mut scan, b"", None, snap_seq);
        total_count += c;
        total_bytes += b;
    }

    println!(
        "scans={} entries={total_count} bytes={total_bytes} written={}",
        RANGE_SCANS + FULL_SCANS + 1,
        written.len()
    );
}
