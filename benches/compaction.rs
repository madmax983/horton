//! Deterministic instruction-count harness for the compaction path
//! (`Db::compact_step` / `Compaction`), profiled with `valgrind --tool=callgrind`.
//!
//! `harness = false`: this is a plain `main`, not the (nightly-only) libtest
//! bench harness, so it can be built in release mode and run directly under
//! valgrind without any extra dependency. `std` is fine here — this is a
//! benchmark, not library code.
//!
//! Workload: `write_path.rs` and `write_only.rs` both stop at 3 flushes
//! specifically so L0 never fills and compaction (v0.4-v0.8) never runs —
//! that was a deliberate v0.3-era limit, but v0.8 shipped a full leveled
//! compaction engine (`src/compact.rs`, cascading merges down every level)
//! that no existing benchmark drives at all. This harness closes that gap:
//! sustained unique-key puts, flushed in small batches so L0 (and then
//! deeper levels) repeatedly fill, draining every pending compaction job
//! after each flush the way a real long-lived device does. Fixed-seed
//! PRNG, fully deterministic.

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use horton::{Compaction, Config, Db, Progress};

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

/// Same shape as `write_path.rs` / `write_only.rs`: 4 KiB blocks, 64 B
/// keys, 256 B values, 512-slot / 64 KiB memtable, 7 levels, 4 tables per
/// level, 1 KiB bloom filters, 8192-slot free list.
type BenchDb = Db<MemDevice<4096>, 4096, 64, 256, 512, 65536, 7, 4, 1024, 8192>;
type BenchCompaction = Compaction<4096, 64, 256, 1024>;

// Small batches (well under the 512-slot memtable cap) so L0 fills — and
// compaction cascades into deeper levels — repeatedly over the run instead
// of once at the very end.
const WAL_START: u64 = 8;
const WAL_END: u64 = 8 + 600;
const TBL_START: u64 = WAL_END;
const TBL_END: u64 = TBL_START + 200_000;

const fn config() -> Config {
    Config::new(WAL_START, WAL_END, TBL_START, TBL_END, 0, 1)
}

const PUT_BATCH: usize = 100;
const NUM_BATCHES: usize = 60;
const GETS: usize = 2000;

/// Drains every pending compaction job (one fresh scratch per job, matching
/// `tests/compact.rs`'s `drain` helper — the scratch is idle once a job
/// commits, so reuse would work too, but a real caller often does not keep
/// one around between jobs).
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

fn main() {
    let device = MemDevice::<4096>::new();
    let mut db = BenchDb::new(device, config());
    block_on(db.open()).expect("open");

    let mut rng = Lcg::new(0xC0FF_EE12_3456_789A);
    let mut kbuf = [0u8; 32];
    let mut vbuf = [0u8; 256];
    let mut written: Vec<Vec<u8>> = Vec::with_capacity(PUT_BATCH * NUM_BATCHES);

    for batch in 0..NUM_BATCHES {
        for i in 0..PUT_BATCH {
            let klen = rng.range(8, 32);
            make_key(&mut kbuf, &mut rng, klen);
            // The first two bytes are `batch`'s base-26 digits, most
            // significant first, so keys grow monotonically with `batch`
            // (a time- or sequence-ordered real-world key prefix, like
            // `tests/compact.rs`'s `compact_cascades_down_every_level`):
            // every batch's flushed table's range sits strictly above every
            // earlier batch's, so only *adjacent* tables ever overlap
            // during a merge, instead of everything overlapping everything
            // — the next two similarly order each key within the batch.
            // `% 26` bounds each addend to `[0, 26)`; `batch < NUM_BATCHES
            // = 60` and `i < PUT_BATCH = 100` keep every cast in range.
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
    }

    // Read pass over the now heavily-compacted, multi-level tree: mixed
    // hits (written keys, scattered across levels by now) and misses.
    let mut val_buf = [0u8; 256];
    let mut hits = 0u64;
    for i in 0..GETS {
        if i % 3 == 0 {
            let klen = rng.range(8, 32);
            make_key(&mut kbuf, &mut rng, klen);
            kbuf[0] = b'z'; // outside the written keyspace's first-byte range
            if block_on(db.get(&kbuf[..klen], &mut val_buf))
                .expect("get")
                .is_some()
            {
                hits += 1;
            }
        } else {
            let key = &written[(i * 13) % written.len()];
            if block_on(db.get(key, &mut val_buf)).expect("get").is_some() {
                hits += 1;
            }
        }
    }

    println!("hits={hits} written={}", written.len());
}
