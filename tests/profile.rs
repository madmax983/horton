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
