//! Moving cold tables to object storage and back.
//!
//! An archive object is one header block describing the table (its
//! [`SealedTable`] descriptor), followed by the table's blocks exactly as
//! they sat on flash. Uploading streams those blocks one at a time from
//! [`Db::device`](horton::Db::device), so the recorder never holds more
//! than one block of a table in RAM. Restoring hands the object's blocks to
//! [`Db::ingest_table`](horton::Db::ingest_table), which copies, verifies
//! and relocates them into any database of the same shape.

use core::future::poll_fn;
use std::collections::HashMap;
use std::io::{self, Write};
use std::task::{Context, Poll};

use horton::{ArchivePlan, BlockDevice, Error, KeyBound, SealedTable, crc32};

use crate::{BLOCK, KEY_MAX, RecorderCompaction, RecorderDb, block_on, tick_of};

const MAGIC: &[u8; 8] = b"HRTARCH1";

/// The object key a table is archived under. Table ids never repeat, so
/// re-uploading after a crash overwrites the object with the same bytes.
pub fn object_key(table_id: u32) -> String {
    format!("tables/{table_id:010}.hrt")
}

/// Encodes the header block for `sealed`.
fn header(sealed: &SealedTable<KEY_MAX>) -> [u8; BLOCK] {
    let mut h = [0u8; BLOCK];
    let mut at = 0;
    let mut put = |bytes: &[u8]| {
        h[at..at + bytes.len()].copy_from_slice(bytes);
        at += bytes.len();
    };
    put(MAGIC);
    put(&sealed.id.to_le_bytes());
    put(&sealed.block_count.to_le_bytes());
    put(&sealed.max_seq.to_le_bytes());
    put(&sealed.min_seq.to_le_bytes());
    put(&sealed.entry_count.to_le_bytes());
    put(&sealed.rdel_blocks.to_le_bytes());
    for bound in [&sealed.first_key, &sealed.last_key] {
        put(&bound.len.to_le_bytes());
        put(&bound.bytes);
    }
    let crc = crc32(&h[..at]);
    h[at..at + 4].copy_from_slice(&crc.to_le_bytes());
    h
}

/// Decodes a header block, or says why it is not one.
pub fn parse_header(h: &[u8]) -> Result<SealedTable<KEY_MAX>, String> {
    if h.len() < BLOCK || &h[..8] != MAGIC {
        return Err("not an archive object".into());
    }
    let mut at = 8;
    let mut take = |n: usize| {
        let s = &h[at..at + n];
        at += n;
        s
    };
    let u32_at = |s: &[u8]| u32::from_le_bytes(s.try_into().expect("4 bytes"));
    let u64_at = |s: &[u8]| u64::from_le_bytes(s.try_into().expect("8 bytes"));
    let id = u32_at(take(4));
    let block_count = u32_at(take(4));
    let max_seq = u64_at(take(8));
    let min_seq = u64_at(take(8));
    let entry_count = u32_at(take(4));
    let rdel_blocks = u32_at(take(4));
    let mut bound = || {
        let len = u16::from_le_bytes(take(2).try_into().expect("2 bytes"));
        let mut bytes = [0u8; KEY_MAX];
        bytes.copy_from_slice(take(KEY_MAX));
        KeyBound { len, bytes }
    };
    let first_key = bound();
    let last_key = bound();
    let end = at;
    let crc = u32_at(&h[end..end + 4]);
    if crc != crc32(&h[..end]) {
        return Err("header CRC mismatch".into());
    }
    Ok(SealedTable {
        id,
        block_count,
        first_key,
        last_key,
        max_seq,
        min_seq,
        entry_count,
        rdel_blocks,
    })
}

/// Streams one table to `store`: the header, then every block read
/// straight off the device. Returns the bytes uploaded.
pub fn upload<D: BlockDevice>(
    db: &RecorderDb<D>,
    plan: &ArchivePlan<KEY_MAX>,
    store: &mut dyn crate::cloud::ObjectStore,
) -> io::Result<u64>
where
    D::Error: core::fmt::Debug,
{
    let sealed = plan.sealed();
    let first = plan.table.first_block;
    let blocks = u64::from(plan.table.block_count);
    store.put(&object_key(sealed.id), &mut |w: &mut dyn Write| {
        w.write_all(&header(&sealed))?;
        let mut buf = [0u8; BLOCK];
        for id in first..first + blocks {
            block_on(poll_fn(|cx| db.device().poll_read_block(cx, id, &mut buf)))
                .map_err(|e| io::Error::other(format!("reading block {id}: {e:?}")))?;
            w.write_all(&buf)?;
        }
        Ok(())
    })?;
    Ok((blocks + 1) * BLOCK as u64)
}

/// A table fetched back from the store, ready to ingest.
pub struct Fetched {
    pub sealed: SealedTable<KEY_MAX>,
    pub blocks: RamDevice,
}

impl Fetched {
    /// The recorder ticks the table spans, from its key bounds.
    pub fn ticks(&self) -> (u64, u64) {
        (
            tick_of(self.sealed.first_key.as_slice()),
            tick_of(self.sealed.last_key.as_slice()),
        )
    }
}

/// Downloads and decodes the object at `key`.
pub fn fetch(store: &dyn crate::cloud::ObjectStore, key: &str) -> Result<Fetched, String> {
    let body = store.get(key).map_err(|e| format!("{key}: {e}"))?;
    let sealed = parse_header(&body).map_err(|e| format!("{key}: {e}"))?;
    let expected = (u64::from(sealed.block_count) + 1) * BLOCK as u64;
    if body.len() as u64 != expected {
        return Err(format!("{key}: {} bytes, expected {expected}", body.len()));
    }
    let mut blocks = RamDevice::new(u64::from(sealed.block_count));
    for (i, chunk) in body[BLOCK..].chunks_exact(BLOCK).enumerate() {
        blocks.store(i as u64, chunk);
    }
    Ok(Fetched { sealed, blocks })
}

/// Grafts a fetched table into `db`, compacting whenever level 0 is full.
/// Returns `false` when the table was already there.
pub fn ingest(
    db: &mut RecorderDb<RamDevice>,
    scratch: &mut RecorderCompaction,
    table: &Fetched,
) -> Result<bool, Error<OutOfRange>> {
    loop {
        match block_on(db.ingest_table(&table.sealed, &table.blocks, 0)) {
            Err(Error::NeedsCompaction) => {
                while db.compaction_pending() {
                    while block_on(db.compact_step(scratch))? == horton::Progress::More {}
                }
            }
            other => return other,
        }
    }
}

/// A block id past the end of a [`RamDevice`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutOfRange(pub u64);

/// A sparse RAM block device: the ground station's disk, and the staging
/// area for a downloaded table. Unwritten blocks read as zeros.
pub struct RamDevice {
    blocks: HashMap<u64, Box<[u8; BLOCK]>>,
    len: u64,
}

impl RamDevice {
    pub fn new(len: u64) -> Self {
        Self {
            blocks: HashMap::new(),
            len,
        }
    }

    fn store(&mut self, id: u64, data: &[u8]) {
        let mut block = Box::new([0u8; BLOCK]);
        block.copy_from_slice(data);
        self.blocks.insert(id, block);
    }
}

impl BlockDevice for RamDevice {
    type Error = OutOfRange;
    const BLOCK: usize = BLOCK;

    fn poll_read_block(
        &self,
        _cx: &mut Context<'_>,
        id: u64,
        buf: &mut [u8],
    ) -> Poll<Result<(), OutOfRange>> {
        if id >= self.len {
            return Poll::Ready(Err(OutOfRange(id)));
        }
        match self.blocks.get(&id) {
            Some(block) => buf.copy_from_slice(&block[..]),
            None => buf.fill(0),
        }
        Poll::Ready(Ok(()))
    }

    fn poll_write_block(
        &mut self,
        _cx: &mut Context<'_>,
        id: u64,
        buf: &[u8],
    ) -> Poll<Result<(), OutOfRange>> {
        if id >= self.len {
            return Poll::Ready(Err(OutOfRange(id)));
        }
        self.store(id, buf);
        Poll::Ready(Ok(()))
    }

    fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), OutOfRange>> {
        Poll::Ready(Ok(()))
    }
}
