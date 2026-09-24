//! Deterministic instruction-count harness for the write path in isolation:
//! `Db::put` / `Db::delete` / `Db::flush`, no `get()` calls at all. Same
//! shape as `benches/write_path.rs` minus its read pass, so WAL/memtable/
//! manifest/flush costs show up without the CRC-on-read traffic (4000
//! `get()`s in that harness) swamping the profile.
//!
//! Purely a profiling scratch harness for now — not wired into any PR.

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use horton::{BlockDevice, Config, Db};

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

struct MemDevice<const BLOCK: usize> {
    blocks: Vec<[u8; BLOCK]>,
}

impl<const BLOCK: usize> MemDevice<BLOCK> {
    const fn new() -> Self {
        Self { blocks: Vec::new() }
    }
}

impl<const BLOCK: usize> BlockDevice for MemDevice<BLOCK> {
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
        if let Ok(i) = usize::try_from(id)
            && buf.len() == BLOCK
        {
            while self.blocks.len() <= i {
                self.blocks.push([0u8; BLOCK]);
            }
            self.blocks[i].copy_from_slice(buf);
        }
        Poll::Ready(Ok(()))
    }

    fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

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

    const fn range(&mut self, lo: usize, hi: usize) -> usize {
        let span = (hi - lo + 1) as u64; // widening cast: no truncation possible
        // The modulo bounds the value below `span` (a small key/value
        // length), so it fits in `usize` on every bench target.
        #[allow(clippy::cast_possible_truncation)]
        let offset = (self.next() % span) as usize;
        lo + offset
    }
}

fn make_key(buf: &mut [u8; 32], rng: &mut Lcg, len: usize) -> usize {
    // `% 26` bounds the value to `[0, 26)`; the `u8` cast cannot truncate.
    for b in &mut buf[..len] {
        #[allow(clippy::cast_possible_truncation)]
        let v = (rng.next() % 26) as u8;
        *b = v + b'a';
    }
    len
}

fn make_val(buf: &mut [u8; 256], rng: &mut Lcg, len: usize) -> usize {
    // `% 256` bounds the value to a byte; the `u8` cast cannot truncate.
    for b in &mut buf[..len] {
        #[allow(clippy::cast_possible_truncation)]
        let v = (rng.next() % 256) as u8;
        *b = v;
    }
    len
}

/// Same shape as `write_path.rs`: 4 KiB blocks, 64 B keys, 256 B values,
/// 512-slot / 64 KiB memtable, 7 levels, 4 L0 tables, 1 KiB bloom filters.
type BenchDb = Db<MemDevice<4096>, 4096, 64, 256, 512, 65536, 7, 4, 1024, 8>;

const WAL_START: u64 = 8;
const WAL_END: u64 = 8 + 4000;
const TBL_START: u64 = WAL_END;
const TBL_END: u64 = TBL_START + 8000;

const fn config() -> Config {
    Config::new(WAL_START, WAL_END, TBL_START, TBL_END, 0, 2)
}

const PUT_BATCH: usize = 400;
const PUT_BATCHES: usize = 3;
const UPDATES: usize = 400;
const DELETES: usize = 200;

fn main() {
    let device = MemDevice::<4096>::new();
    let mut db = BenchDb::new(device, config());
    block_on(db.open()).expect("open");

    let mut rng = Lcg::new(0xC0FF_EE12_3456_789A);
    let mut kbuf = [0u8; 32];
    let mut vbuf = [0u8; 256];
    let mut written: Vec<Vec<u8>> = Vec::with_capacity(PUT_BATCH * PUT_BATCHES);

    for batch in 0..PUT_BATCHES {
        for i in 0..PUT_BATCH {
            let klen = rng.range(8, 32);
            make_key(&mut kbuf, &mut rng, klen);
            // `% 26` bounds the addend to `[0, 26)`; `batch < PUT_BATCHES
            // = 3`. Neither `u8` cast can truncate.
            #[allow(clippy::cast_possible_truncation)]
            {
                kbuf[0] = b'a' + ((batch * PUT_BATCH + i) % 26) as u8;
                kbuf[1] = b'a' + batch as u8;
            }
            let vlen = rng.range(16, 200);
            make_val(&mut vbuf, &mut rng, vlen);
            block_on(db.put(&kbuf[..klen], &vbuf[..vlen])).expect("put");
            written.push(kbuf[..klen].to_vec());
        }
        block_on(db.flush()).expect("flush");
    }

    for i in 0..UPDATES {
        let key = &written[i % written.len()];
        let vlen = rng.range(16, 200);
        make_val(&mut vbuf, &mut rng, vlen);
        block_on(db.put(key, &vbuf[..vlen])).expect("update");
    }
    block_on(db.flush()).expect("flush");

    for i in 0..DELETES {
        let key = &written[(i * 7) % written.len()];
        block_on(db.delete(key)).expect("delete");
    }

    println!("written={}", written.len());
}
