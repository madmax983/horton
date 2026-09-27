//! Budget gate: the ESP32-S3 profile's measured static RAM must stay under
//! the 64 KiB budget. Sizes are `size_of` on the host; layout is identical
//! on xtensa (no pointers in the measured structs... the Db holds no heap
//! pointers at all — everything is inline arrays).

use core::convert::Infallible;
use core::task::{Context, Poll};

use horton::device::BlockDevice;
use horton::profile::{ESP32S3_RAM_BUDGET, Esp32S3Compaction, Esp32S3Db, Esp32S3Scan};

struct Dummy;

impl BlockDevice for Dummy {
    type Error = Infallible;
    const BLOCK: usize = 4096;
    fn poll_read_block(
        &self,
        _cx: &mut Context<'_>,
        _id: u64,
        _buf: &mut [u8],
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn poll_write_block(
        &mut self,
        _cx: &mut Context<'_>,
        _id: u64,
        _buf: &[u8],
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

#[test]
fn esp32s3_profile_fits_ram_budget() {
    let db = core::mem::size_of::<Esp32S3Db<Dummy>>();
    let scan = core::mem::size_of::<Esp32S3Scan<'static, Dummy>>();
    let comp = core::mem::size_of::<Esp32S3Compaction>();
    let total = db + scan + comp;
    println!("ESP32-S3 profile: Db={db} Scan={scan} Compaction={comp} total={total}");
    assert!(
        total <= ESP32S3_RAM_BUDGET,
        "profile grew past the {ESP32S3_RAM_BUDGET}-byte budget: re-tune consciously"
    );
}

/// F11 — the profile aliases come from `db_types!`, whose named
/// parameters must land in the right positional slots: each alias is the
/// exact type the positional spelling names.
#[test]
fn db_types_aliases_match_the_positional_spelling() {
    use horton::profile::Esp32S3RevScan;

    fn db(x: horton::Db<Dummy, 4096, 32, 64, 16, 2048, 4, 4, 64, 2>) -> Esp32S3Db<Dummy> {
        x
    }
    fn scan(
        x: horton::Scan<'static, Dummy, 4096, 32, 64, 16, 2048, 4, 4, 64, 2>,
    ) -> Esp32S3Scan<'static, Dummy> {
        x
    }
    fn rev(
        x: horton::RevScan<'static, Dummy, 4096, 32, 64, 16, 2048, 4, 4, 64, 2>,
    ) -> Esp32S3RevScan<'static, Dummy> {
        x
    }
    fn comp(x: horton::Compaction<4096, 32, 64, 64>) -> Esp32S3Compaction {
        x
    }
    // The identity functions type-check only if the types are equal.
    let _ = (db, scan, rev, comp);

    // A shape declared with the macro elsewhere behaves like any `Db`.
    horton::db_types! {
        block: 4096,
        key_max: 16,
        val_max: 32,
        memtable_entries: 8,
        memtable_arena: 512,
        levels: 2,
        tables_per_level: 2,
        bloom_bytes: 32,
        cache_blocks: 0;
        type Db = TinyDb;
        type Compaction = TinyCompaction;
    }
    assert!(core::mem::size_of::<TinyDb<Dummy>>() < core::mem::size_of::<Esp32S3Db<Dummy>>());
    let _ = TinyCompaction::new();
}

/// Sizes of every public future on the ESP32-S3 profile, created but
/// never polled. An executor stores the future (a static task arena, or
/// the stack of a poll loop), so its size is RAM just like the structs'.
fn future_sizes() -> Vec<(&'static str, usize)> {
    use horton::profile::Esp32S3RevScan;
    use horton::{Config, KeyBound, SealedTable, WriteBatch};

    macro_rules! size {
        ($f:expr) => {{
            let f = $f;
            let n = core::mem::size_of_val(&f);
            drop(f);
            n
        }};
    }
    let mut db = Box::new(Esp32S3Db::new(Dummy, Config::new(2, 66, 66, 512, 0, 1)));
    let mut comp = Box::new(Esp32S3Compaction::new());
    let batch = WriteBatch::<32, 64, 8>::new();
    let sealed = SealedTable::<32> {
        id: 1,
        block_count: 4,
        first_key: KeyBound::EMPTY,
        last_key: KeyBound::EMPTY,
        max_seq: 1,
        min_seq: 1,
        entry_count: 1,
        rdel_blocks: 0,
    };
    let (mut k, mut v) = ([0u8; 32], [0u8; 64]);
    let mut out = vec![
        ("open", size!(db.open())),
        ("put", size!(db.put(b"k", b"v"))),
        ("delete", size!(db.delete(b"k"))),
        ("delete_range", size!(db.delete_range(b"a", b"b"))),
        ("put_with_ttl", size!(db.put_with_ttl(b"k", b"v", 9))),
        ("write", size!(db.write(&batch))),
        ("get", size!(db.get(b"k", &mut v))),
        ("flush", size!(db.flush())),
        ("compact_step", size!(db.compact_step(&mut comp))),
        ("archive_commit", size!(db.archive_commit(0, 1))),
        ("ingest_table", size!(db.ingest_table(&sealed, &Dummy, 0))),
    ];
    let mut scan = Box::new(Esp32S3Scan::new(&db));
    out.push(("Scan::seek", size!(scan.seek(b"", None, u64::MAX))));
    out.push(("Scan::next", size!(scan.next(&mut k, &mut v))));
    let mut rev = Box::new(Esp32S3RevScan::new(&db));
    out.push((
        "RevScan::seek_prev",
        size!(rev.seek_prev(b"", None, u64::MAX)),
    ));
    out.push(("RevScan::prev", size!(rev.prev(&mut k, &mut v))));
    out
}

/// F8 — the budget counted the structs but not the futures, and `flush()`
/// alone was 25.7 KiB (29.8 KiB by the time it was measured here). An
/// executor stores the future, so the peak is the structs plus the
/// largest set of futures that can be live at once:
///
/// - a `&mut self` call excludes every other use of the `Db`, scans
///   included (a `Scan` borrows the `Db`), so it stands alone;
/// - `get` (`&self`) can run alongside a scan step on the same task.
///
/// Since the fix, `&mut self` calls borrow the `Db`'s block scratch, the
/// rdel section stages in the table writer's idle data block, manifest
/// commits stage a small `ManifestEdit` instead of a manifest copy, and
/// `get` reports `Busy` instead of carrying fallback buffers.
#[test]
fn esp32s3_futures_fit_ram_budget() {
    let sizes = future_sizes();
    for (name, n) in &sizes {
        println!("future {name}: {n} bytes");
    }
    let structs = core::mem::size_of::<Esp32S3Db<Dummy>>()
        + core::mem::size_of::<Esp32S3Scan<'static, Dummy>>()
        + core::mem::size_of::<Esp32S3Compaction>();
    let max_of = |pred: &dyn Fn(&str) -> bool| {
        sizes
            .iter()
            .filter(|(n, _)| pred(n))
            .map(|&(_, n)| n)
            .max()
            .unwrap_or(0)
    };
    let exclusive = max_of(&|n| !n.contains("Scan::") && n != "get");
    let shared = max_of(&|n| n == "get") + max_of(&|n| n.contains("Scan::"));
    let total = structs + exclusive.max(shared);
    println!(
        "ESP32-S3 peak: structs={structs} + max(&mut future={exclusive}, get+scan={shared}) = {total}"
    );
    assert!(
        total <= ESP32S3_RAM_BUDGET,
        "structs plus peak futures ({total}) exceed the {ESP32S3_RAM_BUDGET}-byte budget"
    );
    // The fixes hold: no future carries a stray block buffer.
    let get = max_of(&|n| n == "get");
    assert!(get < 4096, "get future {get} holds a block buffer again");
    let ingest = max_of(&|n| n == "ingest_table");
    assert!(
        ingest < 4096,
        "ingest future {ingest} holds a block buffer again"
    );
}
