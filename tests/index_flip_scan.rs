//! A scan's seek searches a cached index block range by range
//! (ADR-0034). A detached scan, forward and reverse, through a cache
//! whose RAM copy of an index block has one bit flipped yields exactly
//! what an uncached scan yields: a flip the search reads fails its check
//! and the block is read from the device again.
mod common;

use common::{Lcg, MemDevice, TestDb, block_on, test_config};
use horton::cache::CheckedRead;
use horton::{
    BlockDevice, CachePort, CacheStats, Compaction, Error, MemTable, Progress, RevScan, Scan,
};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;

type TestCompaction = Compaction<4096, 256, 1024, 1024>;
type ScanOver<'d, D> = Scan<'d, D, 4096, 256, 1024, 64, 4096, 7, 4, 1024, 8>;
type RevScanOver<'d, D> = RevScan<'d, D, 4096, 256, 1024, 64, 4096, 7, 4, 1024, 8>;
type Page = MemTable<64, 4096, 256, 1024>;
type Entries = Vec<(Vec<u8>, Vec<u8>)>;

fn key(i: usize) -> Vec<u8> {
    format!("key-{i:05}").into_bytes()
}

fn drain<D: BlockDevice>(db: &mut TestDb<D>)
where
    D::Error: core::fmt::Debug,
{
    let mut scratch = Box::new(TestCompaction::new());
    while db.compaction_pending() {
        while block_on(db.compact_step(&mut scratch)).unwrap() == Progress::More {}
    }
}

#[derive(Default)]
struct FlipCache {
    images: RefCell<HashMap<(u32, u64), [u8; 4096]>>,
    /// (block, byte, mask): lend this block with that byte flipped.
    flip: Cell<Option<(u64, usize, u8)>>,
    rejected: Cell<u64>,
    lent_flipped: Cell<u64>,
}

impl CachePort<4096> for FlipCache {
    fn get_into(&self, t: u32, b: u64, out: &mut [u8; 4096]) -> bool {
        self.images
            .borrow()
            .get(&(t, b))
            .map(|i| *out = *i)
            .is_some()
    }
    fn get_range_into(&self, t: u32, b: u64, off: usize, out: &mut [u8]) -> bool {
        self.images
            .borrow()
            .get(&(t, b))
            .and_then(|i| i.get(off..off + out.len()))
            .map(|s| out.copy_from_slice(s))
            .is_some()
    }
    fn with_checked(&self, t: u32, b: u64, read: &mut CheckedRead<'_>) -> Option<bool> {
        let Some(pristine) = self.images.borrow().get(&(t, b)).copied() else {
            return Some(false);
        };
        let mut lent = pristine;
        if let Some((fb, at, mask)) = self.flip.get()
            && fb == b
        {
            lent[at] ^= mask;
            self.lent_flipped.set(self.lent_flipped.get() + 1);
        }
        let mut ok = true;
        read(&lent, &mut |from, to| {
            let good = to <= from || (to <= 4096 && lent[from..to] == pristine[from..to]);
            ok &= good;
            good
        });
        if !ok {
            self.images.borrow_mut().remove(&(t, b));
            self.rejected.set(self.rejected.get() + 1);
        }
        Some(ok)
    }
    fn put(&self, t: u32, b: u64, block: &[u8; 4096], _hot: bool) {
        self.images.borrow_mut().insert((t, b), *block);
    }
    fn invalidate_table(&self, t: u32) {
        self.images.borrow_mut().retain(|k, _| k.0 != t);
    }
    fn stats(&self) -> CacheStats {
        CacheStats::default()
    }
}

fn scan_fwd<D: BlockDevice>(
    page: &Page,
    m: &horton::Manifest<7, 4, 256>,
    dev: &D,
    cache: Option<&dyn CachePort<4096>>,
    node: u32,
    start: &[u8],
) -> Result<Entries, Error<D::Error>> {
    let mut scan = Box::new(ScanOver::<D>::detached(page, m, dev, cache, node));
    block_on(scan.seek(start, None, u64::MAX))?;
    let (mut k, mut v) = ([0u8; 256], [0u8; 1024]);
    let mut out = Vec::new();
    while let Some((kl, vl)) = block_on(scan.next(&mut k, &mut v))? {
        out.push((k[..kl].to_vec(), v[..vl].to_vec()));
    }
    Ok(out)
}

fn scan_rev<D: BlockDevice>(
    page: &Page,
    m: &horton::Manifest<7, 4, 256>,
    dev: &D,
    cache: Option<&dyn CachePort<4096>>,
    node: u32,
    from: &[u8],
) -> Result<Entries, Error<D::Error>> {
    let mut scan = Box::new(RevScanOver::<D>::detached(page, m, dev, cache, node));
    block_on(scan.seek_prev(from, None, u64::MAX))?;
    let (mut k, mut v) = ([0u8; 256], [0u8; 1024]);
    let mut out = Vec::new();
    while let Some((kl, vl)) = block_on(scan.prev(&mut k, &mut v))? {
        out.push((k[..kl].to_vec(), v[..vl].to_vec()));
    }
    Ok(out)
}

#[test]
fn a_flipped_bit_in_a_cached_index_block_never_changes_a_scan() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    block_on(db.open()).unwrap();
    let mut rng = Lcg::new(0x1D3E_F1A9);
    for _ in 0..6000usize {
        let k = key(rng.next_bounded(3000));
        let val = vec![b'v'; 40 + rng.next_bounded(80)];
        for attempt in 0..2 {
            match block_on(db.put(&k, &val)) {
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
    let manifest = *db.manifest();
    let device = db.device().clone();
    let node = db.node_id();
    let page = Box::new(Page::new());
    let tables: Vec<_> = manifest.tables().to_vec();
    let biggest = tables
        .iter()
        .max_by_key(|t| t.data_blocks().unwrap())
        .unwrap();
    // Past 32 entries: the binary search crosses restarts.
    assert!(biggest.data_blocks().unwrap() > 32);
    let index_id = biggest.index_block().unwrap();
    let starts: Vec<Vec<u8>> = [0usize, 7, 333, 1234, 1999, 2500, 2999, 3500]
        .iter()
        .map(|&i| key(i))
        .collect();
    let want_f: Vec<Entries> = starts
        .iter()
        .map(|s| scan_fwd(&page, &manifest, &device, None, node, s).unwrap())
        .collect();
    let want_r: Vec<Entries> = starts
        .iter()
        .map(|s| scan_rev(&page, &manifest, &device, None, node, s).unwrap())
        .collect();
    let cache = FlipCache::default();
    // Warm: every block in the cache.
    for s in &starts {
        scan_fwd(&page, &manifest, &device, Some(&cache), node, s).unwrap();
        scan_rev(&page, &manifest, &device, Some(&cache), node, s).unwrap();
    }
    let pristine = *cache
        .images
        .borrow()
        .get(&(biggest.id, index_id))
        .expect("index cached");
    let step = if cfg!(debug_assertions) { 61 } else { 3 };
    for at in (0..4096).step_by(step) {
        let mask = 1u8 << ((at * 5) % 8);
        for (i, s) in starts.iter().enumerate() {
            cache
                .images
                .borrow_mut()
                .insert((biggest.id, index_id), pristine);
            cache.flip.set(Some((index_id, at, mask)));
            let f = scan_fwd(&page, &manifest, &device, Some(&cache), node, s);
            assert_eq!(f.as_ref().ok(), Some(&want_f[i]), "fwd byte {at} start {i}");
            cache
                .images
                .borrow_mut()
                .insert((biggest.id, index_id), pristine);
            let r = scan_rev(&page, &manifest, &device, Some(&cache), node, s);
            assert_eq!(r.as_ref().ok(), Some(&want_r[i]), "rev byte {at} start {i}");
        }
    }
    // The flips were lent, and some were caught.
    assert!(cache.lent_flipped.get() > 0);
    assert!(cache.rejected.get() > 0);
}
