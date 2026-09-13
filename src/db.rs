//! Database: WAL + memtable + `SSTables` (v0.2).
//!
//! Write path is WAL-first: every mutation is appended to the WAL and
//! committed before it lands in the memtable, so a crash can only lose the
//! un-acknowledged tail. [`Db::flush`] drains the memtable into an immutable
//! `SSTable` in the table region and commits the manifest; the manifest commit
//! is the single atomic visibility point — a crash before it leaves the old
//! manifest plus a replayable WAL, a crash after it leaves the new state.
//! Reads are served from the memtable first, then level 0 (newest table
//! first). The full multi-level read path (levels, ranges) arrives in v0.3.

use crate::alloc::Bump;
use crate::device::BlockDevice;
use crate::error::Error;
use crate::manifest::{Manifest, TableRef};
use crate::memtable::MemTable;
use crate::sstable;
use crate::wal::{Op, RecoverState, WalWriter};

/// Placement of the database regions on the device.
///
/// Three disjoint regions plus two fixed single-block manifest slots.
/// Region overlap is a caller bug.
#[derive(Debug, Clone, Copy)]
pub struct Config {
    /// First block id of the WAL region.
    pub wal_start: u64,
    /// One past the last block id of the WAL region.
    pub wal_end: u64,
    /// First block id of the `SSTable` region.
    pub tbl_start: u64,
    /// One past the last block id of the `SSTable` region.
    pub tbl_end: u64,
    /// First manifest slot (one block).
    pub manifest_a: u64,
    /// Second manifest slot (one block).
    pub manifest_b: u64,
}

impl Config {
    /// Describes the WAL region, the `SSTable` region, and the manifest slots.
    #[must_use]
    pub const fn new(
        wal_start: u64,
        wal_end: u64,
        tbl_start: u64,
        tbl_end: u64,
        manifest_a: u64,
        manifest_b: u64,
    ) -> Self {
        Self {
            wal_start,
            wal_end,
            tbl_start,
            tbl_end,
            manifest_a,
            manifest_b,
        }
    }
}

/// Summary of a [`Db::open`] call.
#[derive(Debug, Clone, Copy)]
pub struct OpenReport {
    /// WAL records replayed into the memtable.
    pub recovered_records: u64,
    /// Highest sequence number found; the next mutation uses `max_seq + 1`.
    pub max_seq: u64,
    /// `SSTables` referenced by level 0 of the recovered manifest.
    pub l0_tables: usize,
}

/// The database handle. Owns the WAL writer (and through it, the device),
/// the memtable, the manifest, and the table-region bump allocator.
pub struct Db<
    D: BlockDevice,
    const BLOCK: usize,
    const KEY_MAX: usize,
    const VAL_MAX: usize,
    const CAP: usize,
    const ARENA: usize,
    const LEVELS: usize,
    const TABLES: usize,
    const BLOOM_BYTES: usize,
> {
    wal: WalWriter<D, BLOCK>,
    table: MemTable<CAP, ARENA, KEY_MAX, VAL_MAX>,
    manifest: Manifest<LEVELS, TABLES, KEY_MAX>,
    tbl_bump: Bump,
    cfg: Config,
    next_seq: u64,
}

impl<
        D: BlockDevice,
        const BLOCK: usize,
        const KEY_MAX: usize,
        const VAL_MAX: usize,
        const CAP: usize,
        const ARENA: usize,
        const LEVELS: usize,
        const TABLES: usize,
        const BLOOM_BYTES: usize,
    > Db<D, BLOCK, KEY_MAX, VAL_MAX, CAP, ARENA, LEVELS, TABLES, BLOOM_BYTES>
{
    const ASSERT_BLOCK: () = assert!(BLOCK == D::BLOCK, "BLOCK must equal D::BLOCK");
    const ASSERT_KEY: () = assert!(
        KEY_MAX >= 1 && KEY_MAX <= 0xFFFF,
        "KEY_MAX must be within 1..=0xFFFF"
    );
    const ASSERT_VAL: () = assert!(VAL_MAX <= 0xFFFF, "VAL_MAX must fit in u16");
    const ASSERT_REC: () = assert!(
        BLOCK >= 23 + KEY_MAX + VAL_MAX,
        "BLOCK must fit the largest WAL record"
    );
    const ASSERT_BLOOM: () = assert!(
        BLOOM_BYTES >= 1 && BLOOM_BYTES + 4 <= BLOCK,
        "BLOOM_BYTES must be within 1..=BLOCK-4"
    );

    /// Creates a closed database handle over `device`.
    #[must_use]
    pub const fn new(device: D, config: Config) -> Self {
        // Associated consts are lazy: referencing them here forces the
        // parameter checks to be evaluated for every instantiation.
        let () = Self::ASSERT_BLOCK;
        let () = Self::ASSERT_KEY;
        let () = Self::ASSERT_VAL;
        let () = Self::ASSERT_REC;
        let () = Self::ASSERT_BLOOM;
        Self {
            wal: WalWriter::new(device, config.wal_start, config.wal_end),
            table: MemTable::new(),
            manifest: Manifest::new(),
            tbl_bump: Bump::new(config.tbl_start, config.tbl_end),
            cfg: config,
            next_seq: 0,
        }
    }

    /// Consumes the handle and returns the underlying device.
    #[must_use]
    pub fn into_device(self) -> D {
        self.wal.into_device()
    }

    /// Opens the database: recovers the manifest, replays the WAL from the
    /// manifest's `wal_head` into a fresh memtable, and resumes the sequence
    /// counter, the WAL append position, and the table-region allocator.
    /// Idempotent.
    ///
    /// The open-time sweep re-derives the table-region bump pointer as
    /// one-past-the-highest manifest-referenced block; orphaned blocks from
    /// torn flushes are simply never referenced again (true free-list reuse
    /// is a v0.3 item).
    ///
    /// # Errors
    ///
    /// [`Error::CorruptManifest`], [`Error::CorruptWal`], or [`Error::Device`].
    pub async fn open(&mut self) -> Result<OpenReport, Error<D::Error>> {
        self.table.clear();
        let mut scratch = [0u8; BLOCK];
        let (manifest, fresh) = Manifest::recover(
            self.wal.device_mut(),
            &mut scratch,
            self.cfg.manifest_a,
            self.cfg.manifest_b,
        )
        .await?;
        self.manifest = manifest;
        if fresh {
            // Nothing was ever committed: the whole WAL region is live.
            self.manifest.set_wal_head(self.cfg.wal_start);
        }
        // Sweep: resume the bump past the highest referenced table block.
        self.tbl_bump.set_next(self.cfg.tbl_start);
        if let Some(end) = self.manifest.table_region_end() {
            self.tbl_bump.set_next(end);
        }
        let state: RecoverState = self
            .wal
            .recover_from(&mut self.table, self.manifest.wal_head())
            .await?;
        self.next_seq = state.max_seq.max(self.manifest.max_seq());
        Ok(OpenReport {
            recovered_records: state.records,
            max_seq: self.next_seq,
            l0_tables: self.manifest.l0().len(),
        })
    }

    /// Stores `key` → `val`, durable before it returns. Returns the sequence
    /// number assigned to the mutation.
    ///
    /// # Errors
    ///
    /// [`Error::EmptyKey`], [`Error::KeyTooLarge`], [`Error::ValueTooLarge`],
    /// [`Error::TableFull`], [`Error::ArenaFull`], [`Error::NoSpace`], or
    /// [`Error::Device`].
    pub async fn put(&mut self, key: &[u8], val: &[u8]) -> Result<u64, Error<D::Error>> {
        self.table.check_insert::<D::Error>(key, val, false)?;
        let seq = self.next_seq.checked_add(1).ok_or(Error::NoSpace)?;
        self.wal.append(seq, Op::Put, key, val).await?;
        self.wal.commit().await?;
        self.next_seq = seq;
        // The table is unchanged since check_insert, so this cannot fail.
        self.table.insert(key, val, seq, false)?;
        Ok(seq)
    }

    /// Deletes `key` via a tombstone, durable before it returns. Returns the
    /// sequence number assigned to the mutation.
    ///
    /// # Errors
    ///
    /// Same as [`put`](Db::put).
    pub async fn delete(&mut self, key: &[u8]) -> Result<u64, Error<D::Error>> {
        self.table.check_insert::<D::Error>(key, &[], true)?;
        let seq = self.next_seq.checked_add(1).ok_or(Error::NoSpace)?;
        self.wal.append(seq, Op::Delete, key, &[]).await?;
        self.wal.commit().await?;
        self.next_seq = seq;
        // The table is unchanged since check_insert, so this cannot fail.
        self.table.insert(key, &[], seq, true)?;
        Ok(seq)
    }

    /// Reads `key` into `val_buf`: memtable first, then level 0 newest table
    /// first (bloom-gated `SSTable` lookups). The full multi-level read path
    /// is a v0.3 item.
    ///
    /// Returns `Ok(None)` for missing keys and tombstones. Never truncates:
    /// an undersized buffer yields [`Error::BufferTooSmall`] with the
    /// required length.
    ///
    /// # Errors
    ///
    /// [`Error::BufferTooSmall`] when `val_buf` is smaller than the value,
    /// [`Error::CorruptBlock`] when a table's index or footer fails
    /// verification, or [`Error::Device`] on I/O failure.
    pub async fn get(
        &self,
        key: &[u8],
        val_buf: &mut [u8],
    ) -> Result<Option<usize>, Error<D::Error>> {
        if let Some(entry) = self.table.get(key) {
            if entry.tombstone {
                return Ok(None);
            }
            if entry.val.len() > val_buf.len() {
                return Err(Error::BufferTooSmall {
                    need: entry.val.len(),
                });
            }
            val_buf[..entry.val.len()].copy_from_slice(entry.val);
            return Ok(Some(entry.val.len()));
        }
        // v0.2 populates only L0: newest table first, each lookup
        // bloom-gated inside the reader. A tombstone in a newer table
        // shadows the same key in older tables.
        let device = self.wal.device();
        let mut scratch = [0u8; BLOCK];
        for tref in self.manifest.l0().iter().rev() {
            let footer = tref
                .first_block
                .checked_add(u64::from(tref.block_count))
                .and_then(|end| end.checked_sub(1))
                .ok_or(Error::CorruptManifest)?;
            let reader =
                sstable::TableReader::<D, BLOCK, BLOOM_BYTES>::open(device, &mut scratch, footer)
                    .await?;
            match reader.lookup(&mut scratch, key, val_buf).await? {
                sstable::Lookup::Value(n) => return Ok(Some(n)),
                sstable::Lookup::Tombstone => return Ok(None),
                sstable::Lookup::Missing => {}
            }
        }
        Ok(None)
    }

    /// Flushes the memtable into a new `SSTable` and commits the manifest.
    ///
    /// Protocol: commit the WAL (every acked mutation is durable) → plan the
    /// table → stream its blocks → commit the manifest (the single atomic
    /// visibility point) → clear the memtable. A crash before the manifest
    /// commit leaves the old manifest plus a replayable WAL; a crash after
    /// leaves the new state. Never a mix.
    ///
    /// A returned I/O error is equally clean: the manifest commit is staged
    /// in a copy and published only after it lands, and the table-region
    /// bump only advances with it, so a failed flush changes nothing and can
    /// simply be retried.
    ///
    /// A flush with an empty memtable still commits the WAL and is otherwise
    /// a no-op.
    ///
    /// # Errors
    ///
    /// [`Error::NoSpace`] when the table region is exhausted, the table's
    /// index would overflow one block, or level 0 is full (compaction is a
    /// v0.4 item), or [`Error::Device`] on I/O failure.
    pub async fn flush(&mut self) -> Result<(), Error<D::Error>> {
        // Every acked mutation must be durable in the WAL or the new table.
        self.wal.commit().await?;
        if self.table.is_empty() {
            return Ok(());
        }
        // Fail before doing I/O when level 0 cannot take another table;
        // `add_l0_table` re-checks authoritatively below.
        if self.manifest.l0_is_full() {
            return Err(Error::NoSpace);
        }
        // Pass 1: pure computation of the table shape.
        let plan = sstable::plan_table::<D::Error, BLOCK, KEY_MAX>(
            self.table.iter().map(sstable::SstEntry::from),
        )?;
        let k = sstable::bloom_k(BLOOM_BYTES * 8, plan.entry_count);
        let total = plan.data_blocks.checked_add(3).ok_or(Error::NoSpace)?;
        // Reserve the run without advancing: the bump only moves once the
        // manifest commit below has landed, so a failed flush leaves no
        // half-reserved region behind for the retry to trip over.
        let base = self.tbl_bump.peek_run::<D::Error>(total)?;
        // Pass 2: stream the blocks. `data` doubles as the manifest scratch
        // below; all three buffers are plain stack locals.
        let mut data = [0u8; BLOCK];
        let mut index = [0u8; BLOCK];
        let mut bloom = [0u8; BLOOM_BYTES];
        let written = sstable::write_table::<D, BLOCK, BLOOM_BYTES>(
            self.wal.device_mut(),
            base,
            k,
            self.table.iter().map(sstable::SstEntry::from),
            &mut data,
            &mut index,
            &mut bloom,
        )
        .await?;
        debug_assert_eq!(written, total);
        // The manifest commit is the atomic visibility point. Stage the new
        // manifest in a copy and publish it only after the commit lands, so
        // a returned I/O error leaves the in-memory state exactly as it was
        // and the flush can simply be retried.
        let mut staged = self.manifest;
        let id = staged.alloc_table_id::<D::Error>()?;
        let tref = TableRef {
            id,
            first_block: base,
            block_count: u32::try_from(total).map_err(|_| Error::NoSpace)?,
            first_key: plan.first_key,
            last_key: plan.last_key,
            max_seq: plan.max_seq,
            entry_count: u32::try_from(plan.entry_count).map_err(|_| Error::NoSpace)?,
        };
        staged.add_l0_table::<D::Error>(tref)?;
        staged.set_wal_head(self.wal.next_block());
        let (slot_a, slot_b) = (self.cfg.manifest_a, self.cfg.manifest_b);
        staged
            .commit(self.wal.device_mut(), &mut data, slot_a, slot_b)
            .await?;
        // Commit point passed: publish the staged state.
        self.manifest = staged;
        self.tbl_bump
            .set_next(base.checked_add(total).ok_or(Error::NoSpace)?);
        // The flushed contents now live in the table; drop the memtable.
        self.table.clear();
        Ok(())
    }
}
