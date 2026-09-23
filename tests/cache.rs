//! Block cache (v0.16): caller-owned fixed-capacity CLOCK cache.
//!
//! RED: no `BlockCache`, no `CACHE` const generic on `Db`, no cache
//! stats existed. These tests pin the contract: CLOCK eviction order,
//! hot-vs-cold insertion, `CACHE = 0` disabling, repeat point-read
//! hits with fewer device reads, cached/uncached logical equivalence,
//! range-tombstone and TTL correctness through the cache, scan
//! integration (forward + reverse), compaction invalidation, re-attach
//! safety, corrupt-bloom advisory semantics, and volatile-cache
//! behavior across reopen.

mod common;

use core::cell::Cell;
use core::task::{Context, Poll};

use common::{MemDevice, TestDb, block_on, test_config};
use horton::{
    BlockCache, BlockDevice, CacheStats, Compaction, Db, Progress, RevScan, Scan, SealedTable,
};

type NoCacheDb<D> = Db<D, 4096, 256, 1024, 64, 4096, 7, 4, 1024, 4096, 0>;
type TestScan<'d, D> = Scan<'d, D, 4096, 256, 1024, 64, 4096, 7, 4, 1024, 4096, 8>;
type TestRevScan<'d, D> = RevScan<'d, D, 4096, 256, 1024, 64, 4096, 7, 4, 1024, 4096, 8>;
type TestCompaction = Compaction<4096, 256, 1024, 1024>;

/// In-memory device counting reads against the table region (`>= 136`).
/// Manifest slots and the WAL live below it, so the counter only moves
/// when a read actually consults a table.
struct CountingDevice {
    inner: MemDevice<4096>,
    reads: Cell<u64>,
}

impl CountingDevice {
    fn new() -> Self {
        Self {
            inner: MemDevice::new(),
            reads: Cell::new(0),
        }
    }

    const fn reads(&self) -> u64 {
        self.reads.get()
    }
}

impl BlockDevice for CountingDevice {
    type Error = core::convert::Infallible;
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

const fn mkblock(byte: u8) -> [u8; 4096] {
    [byte; 4096]
}

fn get<D: BlockDevice>(db: &TestDb<D>, key: &[u8]) -> Option<Vec<u8>>
where
    D::Error: core::fmt::Debug,
{
    let mut buf = [0u8; 1024];
    block_on(db.get(key, &mut buf))
        .unwrap()
        .map(|n| buf[..n].to_vec())
}

fn put_keys<D: BlockDevice>(db: &mut TestDb<D>, pairs: &[(&[u8], &[u8])])
where
    D::Error: core::fmt::Debug,
{
    for (k, v) in pairs {
        block_on(db.put(k, v)).unwrap();
    }
    block_on(db.flush()).unwrap();
}

// ---------------------------------------------------------------------------
// `BlockCache` unit tests: CLOCK order, hot vs cold, zero slots,
// invalidation.
// ---------------------------------------------------------------------------

#[test]
fn clock_evicts_in_insertion_order_with_second_chances() {
    let mut c = BlockCache::<4096, 3>::new();
    // Fill all three slots hot; hand wraps to 0.
    c.put(0, 10, &mkblock(10), true);
    c.put(0, 11, &mkblock(11), true);
    c.put(0, 12, &mkblock(12), true);
    assert_eq!(c.stats().len, 3);

    // Fourth insert: CLOCK clears every set refbit (second chance) and
    // evicts slot 0 — the oldest.
    c.put(0, 13, &mkblock(13), false);
    let mut out = [0u8; 4096];
    assert!(!c.get_into(0, 10, &mut out), "oldest hot entry evicted");
    assert!(c.get_into(0, 11, &mut out));
    assert_eq!(out, mkblock(11));
    assert!(c.get_into(0, 13, &mut out));

    // Fresh scenario: refill, touch only 11, then insert. The sweep
    // clears 11's bit (second chance — it survives) and evicts 12,
    // whose bit was already clear.
    let mut c = BlockCache::<4096, 3>::new();
    c.put(0, 10, &mkblock(10), true);
    c.put(0, 11, &mkblock(11), true);
    c.put(0, 12, &mkblock(12), true);
    c.put(0, 13, &mkblock(13), false);
    // State: [{13,cold}, {11,clear}, {12,clear}], hand=1.
    assert!(c.get_into(0, 11, &mut out), "touch 11 for a second chance");
    c.put(0, 14, &mkblock(14), true);
    assert!(!c.get_into(0, 12, &mut out), "12 evicted, 11 kept");
    assert!(c.get_into(0, 11, &mut out));
    assert!(c.get_into(0, 13, &mut out));
    assert!(c.get_into(0, 14, &mut out));
}

#[test]
fn cold_inserts_are_evicted_before_hot_ones() {
    let mut c = BlockCache::<4096, 4>::new();
    // Two hot point-read blocks, then two cold scan blocks.
    c.put(0, 1, &mkblock(1), true);
    c.put(0, 2, &mkblock(2), true);
    c.put(0, 3, &mkblock(3), false);
    c.put(0, 4, &mkblock(4), false);

    // Next insert evicts a cold block first (slot 2), never a hot one:
    // the hot entries' refbits earn them a second chance.
    c.put(0, 5, &mkblock(5), false);
    let mut out = [0u8; 4096];
    assert!(c.get_into(0, 1, &mut out), "hot block 1 survives");
    assert!(c.get_into(0, 2, &mut out), "hot block 2 survives");
    assert!(!c.get_into(0, 3, &mut out), "cold block 3 evicted first");
    assert!(c.get_into(0, 4, &mut out));
    assert!(c.get_into(0, 5, &mut out));
}

#[test]
fn zero_slots_disables_the_cache() {
    let mut c = BlockCache::<4096, 0>::new();
    c.put(0, 1, &mkblock(1), true);
    let mut out = [0u8; 4096];
    assert!(!c.get_into(0, 1, &mut out));
    let s = c.stats();
    assert_eq!(
        s,
        CacheStats {
            hits: 0,
            misses: 1,
            len: 0,
            capacity: 0,
        }
    );
    // Invalidation is a no-op, not a panic.
    c.invalidate_table(0);
}

#[test]
fn invalidate_table_drops_only_tagged_entries() {
    let mut c = BlockCache::<4096, 4>::new();
    c.put(7, 1, &mkblock(1), true);
    c.put(7, 2, &mkblock(2), true);
    c.put(9, 1, &mkblock(9), true);
    c.invalidate_table(7);
    let mut out = [0u8; 4096];
    assert!(!c.get_into(7, 1, &mut out));
    assert!(!c.get_into(7, 2, &mut out));
    assert!(c.get_into(9, 1, &mut out));
    assert_eq!(c.stats().len, 1);
}

#[test]
fn replace_refreshes_refbit_without_moving_hand() {
    let mut c = BlockCache::<4096, 2>::new();
    c.put(0, 1, &mkblock(1), false);
    c.put(0, 2, &mkblock(2), false);
    // Re-put of a resident tag only ORs the refbit; hand is untouched,
    // so the next eviction still takes slot 0 first.
    c.put(0, 2, &mkblock(2), true);
    c.put(0, 3, &mkblock(3), false);
    let mut out = [0u8; 4096];
    assert!(!c.get_into(0, 1, &mut out), "slot 0 evicted first");
    assert!(c.get_into(0, 2, &mut out));
    assert!(c.get_into(0, 3, &mut out));
}

// ---------------------------------------------------------------------------
// `Db`-level tests: hits, equivalence, tombstones, TTL, scans.
// ---------------------------------------------------------------------------

#[test]
fn cache_disabled_db_still_reads_everything() {
    let mut db = NoCacheDb::new(MemDevice::<4096>::new(), test_config());
    block_on(db.open()).unwrap();
    block_on(db.put(b"k1", b"v1")).unwrap();
    block_on(db.put(b"k2", b"v2")).unwrap();
    block_on(db.flush()).unwrap();

    let mut buf = [0u8; 1024];
    let n = block_on(db.get(b"k1", &mut buf)).unwrap().unwrap();
    assert_eq!(&buf[..n], b"v1");
    // Second read works too — and every lookup missed, because there is
    // no cache to hit.
    let n = block_on(db.get(b"k1", &mut buf)).unwrap().unwrap();
    assert_eq!(&buf[..n], b"v1");
    let s = db.cache_stats();
    assert_eq!(s.capacity, 0);
    assert_eq!(s.len, 0);
    assert_eq!(s.hits, 0);
    assert!(s.misses > 0, "uncached reads count as misses");
}

#[test]
fn repeat_point_read_is_served_from_cache() {
    let mut db = TestDb::new(CountingDevice::new(), test_config());
    block_on(db.open()).unwrap();
    block_on(db.put(b"hot", b"value")).unwrap();
    block_on(db.flush()).unwrap();

    let mut buf = [0u8; 1024];
    let base_reads = db.device().reads();
    let n = block_on(db.get(b"hot", &mut buf)).unwrap().unwrap();
    assert_eq!(&buf[..n], b"value");
    let first_get_reads = db.device().reads() - base_reads;
    assert!(first_get_reads > 0, "first get touches the device");
    let misses_after_first = db.cache_stats().misses;

    // Second get of the same key: footer, bloom, index and data are all
    // resident — zero table-region device reads, all hits.
    let n = block_on(db.get(b"hot", &mut buf)).unwrap().unwrap();
    assert_eq!(&buf[..n], b"value");
    assert_eq!(
        db.device().reads() - base_reads - first_get_reads,
        0,
        "repeat point read must not touch the device"
    );
    let s = db.cache_stats();
    assert_eq!(s.misses, misses_after_first, "no new misses on repeat");
    assert!(s.hits > 0, "repeat get served from cache");
    assert!(s.len > 0);
}

#[test]
fn cached_values_match_uncached_values() {
    let pairs: Vec<(Vec<u8>, Vec<u8>)> =
        (0u8..40).map(|i| (vec![b'k', i], vec![b'v', i])).collect();

    let mut cached = TestDb::new(MemDevice::<4096>::new(), test_config());
    block_on(cached.open()).unwrap();
    let mut plain = NoCacheDb::new(MemDevice::<4096>::new(), test_config());
    block_on(plain.open()).unwrap();
    for (k, v) in &pairs {
        block_on(cached.put(k, v)).unwrap();
        block_on(plain.put(k, v)).unwrap();
    }
    block_on(cached.flush()).unwrap();
    block_on(plain.flush()).unwrap();

    // Read everything twice through the cache (warming it), then compare
    // against the uncached database key by key.
    let mut buf = [0u8; 1024];
    for (k, v) in &pairs {
        for _ in 0..2 {
            let n = block_on(cached.get(k, &mut buf)).unwrap().unwrap();
            assert_eq!(&buf[..n], v.as_slice(), "cached read of {k:?}");
        }
        let n = block_on(plain.get(k, &mut buf)).unwrap().unwrap();
        assert_eq!(&buf[..n], v.as_slice(), "uncached read of {k:?}");
    }
    assert_eq!(get(&cached, b"absent"), None);
    let mut pbuf = [0u8; 1024];
    assert!(block_on(plain.get(b"absent", &mut pbuf)).unwrap().is_none());
}

#[test]
fn range_delete_shadow_survives_the_cache() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    block_on(db.open()).unwrap();
    put_keys(&mut db, &[(b"a1", b"v1"), (b"a2", b"v2"), (b"b1", b"w1")]);
    // Warm the cache with plain reads first.
    assert_eq!(get(&db, b"a1"), Some(b"v1".to_vec()));
    block_on(db.delete_range(b"a", b"b")).unwrap();
    block_on(db.flush()).unwrap();

    // The range tombstone shadows a1/a2 even though their data blocks
    // (with the live values) are sitting in the cache.
    assert_eq!(get(&db, b"a1"), None);
    assert_eq!(get(&db, b"a2"), None);
    assert_eq!(get(&db, b"b1"), Some(b"w1".to_vec()));
    // And again — the cached range-tombstone section keeps shadowing.
    assert_eq!(get(&db, b"a1"), None);
    assert_eq!(get(&db, b"b1"), Some(b"w1".to_vec()));
}

#[test]
fn ttl_expiry_hides_the_cached_value() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    block_on(db.open()).unwrap();
    block_on(db.put_with_ttl(b"ephem", b"v", 100)).unwrap();
    block_on(db.put(b"plain", b"w")).unwrap();
    block_on(db.flush()).unwrap();

    // Warm the cache with a read before expiry.
    let mut buf = [0u8; 1024];
    let n = block_on(db.get_at_with_time(b"ephem", &mut buf, u64::MAX, 50))
        .unwrap()
        .unwrap();
    assert_eq!(&buf[..n], b"v");

    // Past expiry the key is invisible, even though its bytes are cached.
    assert!(
        block_on(db.get_at_with_time(b"ephem", &mut buf, u64::MAX, 101))
            .unwrap()
            .is_none()
    );
    assert!(
        block_on(db.get_at_with_time(b"ephem", &mut buf, u64::MAX, 101))
            .unwrap()
            .is_none(),
        "cached TTL expiry is stable"
    );
    // The non-TTL key is unaffected.
    let n = block_on(db.get_at_with_time(b"plain", &mut buf, u64::MAX, 10_000))
        .unwrap()
        .unwrap();
    assert_eq!(&buf[..n], b"w");
}

#[test]
fn forward_scan_streams_through_the_cache() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    block_on(db.open()).unwrap();
    let pairs: Vec<(Vec<u8>, Vec<u8>)> =
        (0u8..24).map(|i| (vec![b'k', i], vec![b'v', i])).collect();
    for (k, v) in &pairs {
        block_on(db.put(k, v)).unwrap();
    }
    block_on(db.flush()).unwrap();

    // Point-read first so the hot set is populated, then scan the whole
    // table: the scan must return every key in order.
    assert_eq!(get(&db, b"k\x07"), Some(b"v\x07".to_vec()));
    let mut scan = TestScan::new(&db);
    block_on(scan.seek(b"", None, u64::MAX)).unwrap();
    let mut seen = 0u8;
    let mut kbuf = [0u8; 256];
    let mut vbuf = [0u8; 1024];
    while let Some((klen, vlen)) = block_on(scan.next(&mut kbuf, &mut vbuf)).unwrap() {
        assert_eq!(&kbuf[..klen], [b'k', seen]);
        assert_eq!(&vbuf[..vlen], [b'v', seen]);
        seen += 1;
    }
    assert_eq!(seen, 24);
    // The hot point-read blocks survived the scan: the key still hits.
    let hits_before = db.cache_stats().hits;
    assert_eq!(get(&db, b"k\x07"), Some(b"v\x07".to_vec()));
    assert!(
        db.cache_stats().hits > hits_before,
        "point read after scan still hits"
    );
}

#[test]
fn reverse_scan_reads_through_the_cache() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    block_on(db.open()).unwrap();
    let pairs: Vec<(Vec<u8>, Vec<u8>)> =
        (0u8..16).map(|i| (vec![b'k', i], vec![b'v', i])).collect();
    for (k, v) in &pairs {
        block_on(db.put(k, v)).unwrap();
    }
    block_on(db.flush()).unwrap();

    let mut scan = TestRevScan::new(&db);
    block_on(scan.seek_prev(b"\xff", None, u64::MAX)).unwrap();
    let mut kbuf = [0u8; 256];
    let mut vbuf = [0u8; 1024];
    let mut expect = 15u8;
    loop {
        match block_on(scan.prev(&mut kbuf, &mut vbuf)).unwrap() {
            Some((klen, vlen)) => {
                assert_eq!(&kbuf[..klen], [b'k', expect]);
                assert_eq!(&vbuf[..vlen], [b'v', expect]);
                if expect == 0 {
                    break;
                }
                expect -= 1;
            }
            None => panic!("reverse scan ended early at {expect}"),
        }
    }
    // Reverse scan populated the cache; point reads still agree.
    assert_eq!(get(&db, b"k\x00"), Some(b"v\x00".to_vec()));
    assert_eq!(get(&db, b"k\x0f"), Some(b"v\x0f".to_vec()));
}

// ---------------------------------------------------------------------------
// Invalidation, re-attach, corruption, reopen.
// ---------------------------------------------------------------------------

/// Drives every pending compaction job to idle.
fn drain<D: BlockDevice>(db: &mut TestDb<D>)
where
    D::Error: core::fmt::Debug,
{
    let mut c = TestCompaction::new();
    while db.compaction_pending() {
        while block_on(db.compact_step(&mut c)).unwrap() == Progress::More {}
    }
}

#[test]
fn compaction_invalidates_retired_tables() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    block_on(db.open()).unwrap();
    // Four single-key flushes fill L0; each key lands in its own table.
    for i in 0u8..4 {
        block_on(db.put(&[b'k', i], &[b'v', i])).unwrap();
        block_on(db.flush()).unwrap();
    }
    // Warm the cache against table 0 only (one table's blocks fit in the
    // 8 slots; warming all four would evict itself): first pass
    // populates, second pass hits.
    assert_eq!(get(&db, b"k\x00"), Some(b"v\x00".to_vec()));
    assert_eq!(get(&db, b"k\x00"), Some(b"v\x00".to_vec()));
    let hits_before_compact = db.cache_stats().hits;
    assert!(hits_before_compact > 0);

    // Merge L0 into L1: tables 0..4 are retired and must be invalidated.
    drain(&mut db);

    // Every key still reads correctly — from the NEW table, not from
    // stale cache entries under the retired ids.
    for i in 0u8..4 {
        assert_eq!(get(&db, &[b'k', i]), Some(vec![b'v', i]));
    }
    // The first post-compaction reads missed (fresh table id), then hit.
    let s = db.cache_stats();
    assert!(s.hits > hits_before_compact, "post-compaction reads hit");
}

#[test]
fn reattach_serves_fresh_tables_through_the_cache() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    block_on(db.open()).unwrap();
    put_keys(&mut db, &[(b"a0", b"v0"), (b"a1", b"v1")]);

    // Upload the table's bytes, archive it away locally, then graft it
    // back: the re-attached table must read correctly through the cache.
    let plan = db.archive_plan(0, 0).unwrap();
    let sealed: SealedTable<256> = plan.sealed();
    let mut remote = MemDevice::<4096>::new();
    for i in 0..plan.table.block_count {
        let mut buf = [0u8; 4096];
        let waker = common::noop_waker();
        let mut cx = Context::from_waker(&waker);
        match db
            .device()
            .poll_read_block(&mut cx, plan.table.first_block + u64::from(i), &mut buf)
        {
            Poll::Ready(Ok(())) => {}
            _ => panic!("remote upload read failed"),
        }
        let waker = common::noop_waker();
        let mut cx = Context::from_waker(&waker);
        match remote.poll_write_block(&mut cx, u64::from(i), &buf) {
            Poll::Ready(Ok(())) => {}
            _ => panic!("remote upload write failed"),
        }
    }
    assert!(block_on(db.archive_commit(0, 0)).unwrap());
    assert_eq!(get(&db, b"a0"), None);

    assert!(block_on(db.ingest_table(&sealed, &remote, 0)).unwrap());
    // First read populates the cache from the re-attached table; the
    // repeat is served from it. Both must agree.
    assert_eq!(get(&db, b"a0"), Some(b"v0".to_vec()));
    assert_eq!(get(&db, b"a1"), Some(b"v1".to_vec()));
    let hits = db.cache_stats().hits;
    assert_eq!(get(&db, b"a0"), Some(b"v0".to_vec()));
    assert!(
        db.cache_stats().hits > hits,
        "re-attached table is cacheable"
    );
}

#[test]
fn corrupt_bloom_is_advisory_through_the_cache() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    block_on(db.open()).unwrap();
    block_on(db.put(b"k", b"v")).unwrap();
    block_on(db.flush()).unwrap();

    // Pull the device back out, corrupt the table's bloom block (footer
    // bytes 16..24 name it), and reopen over the damaged table.
    let plan = db.archive_plan(0, 0).unwrap();
    let footer_id = plan.table.first_block + u64::from(plan.table.block_count) - 1;
    let mut dev = db.into_device();
    // Footer layout: magic[0..8], index[8..16], bloom[16..24].
    let mut footer = [0u8; 4096];
    {
        let waker = common::noop_waker();
        let mut cx = Context::from_waker(&waker);
        match dev.poll_read_block(&mut cx, footer_id, &mut footer) {
            Poll::Ready(Ok(())) => {}
            _ => panic!("footer read failed"),
        }
    }
    let bloom_id = u64::from_le_bytes(footer[16..24].try_into().unwrap());
    {
        let bloom_idx = usize::try_from(bloom_id).unwrap();
        let blocks = dev.blocks_mut();
        while blocks.len() <= bloom_idx {
            blocks.push([0u8; 4096]);
        }
        blocks[bloom_idx][0] ^= 0xFF;
    }

    let mut db = TestDb::new(dev, test_config());
    block_on(db.open()).unwrap();
    // The bloom gate is advisory: the corrupt (CRC-failing) bloom is
    // cached as a physical image, the gate degrades, and the lookup
    // still finds the key — twice, the second time from cache.
    assert_eq!(get(&db, b"k"), Some(b"v".to_vec()));
    assert_eq!(get(&db, b"k"), Some(b"v".to_vec()));
    assert_eq!(get(&db, b"missing"), None);
}

#[test]
fn reopen_starts_with_a_cold_volatile_cache() {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    block_on(db.open()).unwrap();
    block_on(db.put(b"k", b"v")).unwrap();
    block_on(db.flush()).unwrap();
    assert_eq!(get(&db, b"k"), Some(b"v".to_vec()));
    assert_eq!(get(&db, b"k"), Some(b"v".to_vec()));
    assert!(db.cache_stats().hits > 0, "cache was warm");

    // The cache is volatile: reopening starts cold, with zeroed stats,
    // and reads are still correct.
    let dev = db.into_device();
    let mut db = TestDb::new(dev, test_config());
    block_on(db.open()).unwrap();
    let s = db.cache_stats();
    assert_eq!(
        s,
        CacheStats {
            hits: 0,
            misses: 0,
            len: 0,
            capacity: 8,
        }
    );
    assert_eq!(get(&db, b"k"), Some(b"v".to_vec()));
    assert!(db.cache_stats().misses > 0, "cold cache misses first");
    assert_eq!(get(&db, b"k"), Some(b"v".to_vec()));
    assert!(db.cache_stats().hits > 0, "then it warms up again");
}
