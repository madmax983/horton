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

use horton::{ArchivePlan, BlockDevice, Error, SealedTable};

use crate::{
    BLOCK, KEY_MAX, ObjectName, RecorderCompaction, RecorderDb, block_on, decode_archive_header,
    encode_archive_header, tick_of,
};

/// The object key a table is archived under. Table ids never repeat, so
/// re-uploading after a crash overwrites the object with the same bytes.
pub fn object_key(table_id: u32) -> String {
    ObjectName::new(table_id).as_str().to_owned()
}

/// Decodes a header block, or says why it is not one.
pub fn parse_header(h: &[u8]) -> Result<SealedTable<KEY_MAX>, String> {
    decode_archive_header(h).map_err(String::from)
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
        w.write_all(&encode_archive_header(&sealed))?;
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
