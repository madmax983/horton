//! Point reads: the memtable, then every table newest-first, highest
//! sequence wins.

use super::Db;
use crate::cache::CachePort;
use crate::device::BlockDevice;
use crate::error::Error;
use crate::manifest::TableRef;
use crate::sstable;

/// Best hit seen so far by [`Db::get`]: the highest-sequence lookup result.
/// When `Value`, the winning bytes are staged in `get`'s staging buffer.
enum Best {
    /// No hit yet.
    Missing,
    /// A tombstone beat every value so far.
    Tombstone,
    /// A value won; holds its byte length.
    Value(usize),
}

/// Accumulator for [`Db::get_at`]'s multi-table read: the winning staged
/// value bytes plus the sequence that won them. Bundled into one struct so
/// `consider_table` stays under the argument-count lint; table lookups
/// copy into a per-table buffer first, and only a winning hit is promoted
/// into `stage`, so a losing hit can never clobber the winner.
struct ReadAcc<const VAL_MAX: usize> {
    stage: [u8; VAL_MAX],
    best: Best,
    best_seq: u64,
    /// Expiry tick of the winning value; 0 = no expiry. Checked against
    /// the caller's `now` before the value is returned.
    best_expire_at: u64,
    /// Highest range-tombstone sequence covering the key at/below the
    /// snapshot, across the memtable and every considered table. Beats
    /// the point winner when strictly newer (sequences are unique per
    /// mutation, so equality cannot happen).
    cover_seq: u64,
}

impl<const VAL_MAX: usize> ReadAcc<VAL_MAX> {
    const fn new() -> Self {
        Self {
            stage: [0u8; VAL_MAX],
            best: Best::Missing,
            best_seq: 0,
            best_expire_at: 0,
            cover_seq: 0,
        }
    }
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
    const CACHE: usize,
> Db<D, BLOCK, KEY_MAX, VAL_MAX, CAP, ARENA, LEVELS, TABLES, BLOOM_BYTES, CACHE>
{
    /// Reads `key` into `val_buf`: memtable first, then level 0 newest table
    /// first, then deeper levels in order. This is
    /// [`get_at`](Db::get_at) with `max_seq = u64::MAX`: the latest view.
    ///
    /// Returns `Ok(None)` for missing keys and tombstones. Never truncates:
    /// an undersized buffer yields [`Error::BufferTooSmall`] with the
    /// required length — the length of the *winning* value, even when an
    /// older shadowed version would have fit.
    ///
    /// # Errors
    ///
    /// [`Error::BufferTooSmall`] when `val_buf` is smaller than the winning
    /// value, [`Error::CorruptManifest`] when a table's block range is
    /// malformed, [`Error::CorruptBlock`] when a table's index or footer
    /// fails verification, [`Error::Busy`] when another `get` on this
    /// handle is in flight (reads share the `Db`'s block buffers), or
    /// [`Error::Device`] on I/O failure.
    pub async fn get(
        &self,
        key: &[u8],
        val_buf: &mut [u8],
    ) -> Result<Option<usize>, Error<D::Error>> {
        self.get_at_with_time(key, val_buf, u64::MAX, 0).await
    }

    /// Reads `key` into `val_buf` as of a snapshot: like [`get`](Db::get),
    /// but only mutations with `seq <= max_seq` are visible. Newer versions
    /// (including newer tombstones) are invisible, so an older value — or
    /// a missing key — can correctly win. Pass a watermark from
    /// [`snapshot`](Db::snapshot) for a pinned read, or `u64::MAX` for the
    /// latest view.
    ///
    /// Returns `Ok(None)` for missing keys and tombstones. Never truncates:
    /// an undersized buffer yields [`Error::BufferTooSmall`] with the
    /// required length — the length of the *winning* value, even when an
    /// older shadowed version would have fit.
    ///
    /// # Errors
    ///
    /// Same as [`get`](Db::get).
    pub async fn get_at(
        &self,
        key: &[u8],
        val_buf: &mut [u8],
        max_seq: u64,
    ) -> Result<Option<usize>, Error<D::Error>> {
        self.get_at_with_time(key, val_buf, max_seq, 0).await
    }

    /// Timed read: like [`get`](Db::get), but values whose `expire_at` is
    /// nonzero and `<= now` are suppressed — they read as missing (a
    /// newer live version still wins; an expired winner never falls
    /// through to an older version). Horton owns no clock: `now` is the
    /// caller's tick, compared against the absolute `expire_at` stored by
    /// [`put_with_ttl`](Db::put_with_ttl).
    ///
    /// # Errors
    ///
    /// Same as [`get`](Db::get).
    pub async fn get_with_time(
        &self,
        key: &[u8],
        val_buf: &mut [u8],
        now: u64,
    ) -> Result<Option<usize>, Error<D::Error>> {
        self.get_at_with_time(key, val_buf, u64::MAX, now).await
    }

    /// Timed read: like [`get_at`](Db::get_at), but values whose
    /// `expire_at` is nonzero and `<= now` are suppressed — they read as
    /// missing (a newer live version still wins; an expired winner never
    /// falls through to an older version). Callers without a clock pass
    /// `now = 0`, which is what [`get`](Db::get) and
    /// [`get_at`](Db::get_at) do.
    ///
    /// # Errors
    ///
    /// Same as [`get_at`](Db::get_at).
    pub async fn get_at_with_time(
        &self,
        key: &[u8],
        val_buf: &mut [u8],
        max_seq: u64,
        now: u64,
    ) -> Result<Option<usize>, Error<D::Error>> {
        self.ensure_open()?;
        self.read_point(key, val_buf, max_seq, now, None).await
    }

    /// The point-read rule, shared by every `get` variant and the archive
    /// resurrection review: the memtable, then level 0 newest table first,
    /// then deeper levels; the highest sequence at or below `max_seq`
    /// wins, a strictly newer covering range tombstone hides it, and a
    /// winner expired at `now` reads as missing (`now = 0`: nothing
    /// expires). `exclude` names one table to leave out (the archival
    /// candidate), or `None`.
    ///
    /// Blocks are read through the `Db`'s two shared buffers, so the
    /// future holds no block buffer of its own.
    ///
    /// # Errors
    ///
    /// [`Error::Busy`] when another read holds the shared buffers (two
    /// `get` futures polled concurrently); otherwise as [`get`](Db::get).
    // The borrows are held across this call's own awaits on purpose;
    // `try_borrow_mut` turns a concurrent second read into `Busy`, never a
    // panic.
    #[allow(clippy::await_holding_refcell_ref)]
    pub(super) async fn read_point(
        &self,
        key: &[u8],
        val_buf: &mut [u8],
        max_seq: u64,
        now: u64,
        exclude: Option<u32>,
    ) -> Result<Option<usize>, Error<D::Error>> {
        // The winning value's bytes are staged here; table lookups copy
        // into a per-table buffer first so a losing hit can never clobber
        // the winner. Values are at most VAL_MAX bytes (enforced on the
        // write path), so the staging always fits.
        let mut acc = ReadAcc::<VAL_MAX>::new();
        if let Some(entry) = self.table.get_at(key, max_seq) {
            // The memtable holds the newest mutations; `get_at` already
            // selected the newest version at or below the snapshot, and
            // skips range-tombstone slots (they are not versions of `key`).
            acc.best_seq = entry.seq;
            if entry.tombstone {
                acc.best = Best::Tombstone;
            } else {
                acc.stage[..entry.val.len()].copy_from_slice(entry.val);
                acc.best = Best::Value(entry.val.len());
                acc.best_expire_at = entry.expire_at;
            }
        }
        // A memtable range tombstone covering `key` beats any older point
        // version; the per-table probes happen inside `consider_table`.
        if let Some(q) = self.table.max_covering_rdel(key, max_seq) {
            acc.cover_seq = q;
        }
        let (Ok(mut scratch), Ok(mut decomp)) = (
            self.get_scratch.try_borrow_mut(),
            self.decomp_scratch.try_borrow_mut(),
        ) else {
            return Err(Error::Busy);
        };
        // Level 0, newest table first: its tables overlap, and newer tables
        // hold higher sequence numbers. Deeper levels in order:
        // highest-seq-wins keeps the result exact however tables are
        // placed.
        for li in 0..LEVELS {
            let tables = self.manifest.level(li).unwrap_or(&[]);
            let n = tables.len();
            for i in 0..n {
                let tref = &tables[if li == 0 { n - 1 - i } else { i }];
                if Some(tref.id) != exclude {
                    self.consider_table(tref, key, max_seq, &mut scratch, &mut decomp, &mut acc)
                        .await?;
                }
            }
        }
        match acc.best {
            Best::Missing | Best::Tombstone => Ok(None),
            Best::Value(len) => {
                // A covering range tombstone newer than the point winner
                // hides the key; an expired winner reads as missing and
                // never falls through to an older version.
                if acc.cover_seq > acc.best_seq {
                    return Ok(None);
                }
                if acc.best_expire_at != 0 && acc.best_expire_at <= now {
                    return Ok(None);
                }
                if len > val_buf.len() {
                    return Err(Error::BufferTooSmall { need: len });
                }
                val_buf[..len].copy_from_slice(&acc.stage[..len]);
                Ok(Some(len))
            }
        }
    }

    /// Whether a range tombstone at/below `max_seq` and newer than `above`
    /// covers `key`, across the memtable and every table with a
    /// range-tombstone section — the scans' "is the winning version
    /// (`seq == above`) hidden?" test. Tables that cannot hold such a
    /// tombstone are skipped without I/O: no rdel section, `max_seq <=
    /// above` (nothing in them is newer than the winner), or bounds that
    /// cannot contain `key` (`last_key` carries the greatest exclusive
    /// rdel end inclusively, so that prune is a conservative superset).
    /// The search stops at the first hit. `scratch` is the scan's
    /// physical-read buffer, dead between block reads, so the scan
    /// futures hold no block buffer for it.
    pub(crate) async fn rdel_hides(
        &self,
        key: &[u8],
        max_seq: u64,
        above: u64,
        scratch: &mut [u8; BLOCK],
    ) -> Result<bool, Error<D::Error>> {
        if self
            .table
            .max_covering_rdel(key, max_seq)
            .is_some_and(|q| q > above)
        {
            return Ok(true);
        }
        for li in 0..LEVELS {
            let tables = self.manifest.level(li).unwrap_or(&[]);
            for tref in tables {
                if tref.rdel_blocks == 0
                    || tref.max_seq <= above
                    || tref.first_key.as_slice() > key
                    || tref.last_key.as_slice() < key
                {
                    continue;
                }
                if sstable::covering_rdel_seq_in(
                    self.device(),
                    Some(self.cache_port()),
                    tref.id,
                    scratch,
                    tref.rdel_first().ok_or(Error::CorruptManifest)?,
                    tref.rdel_blocks,
                    key,
                    max_seq,
                )
                .await?
                .is_some_and(|q| q > above)
                {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Considers one table for [`Db::get_at`]: key-range prune, sequence prune,
    /// then a bloom-gated lookup. A hit with a higher sequence number than
    /// the best so far — and visible at `max_seq` — is promoted into `acc`.
    async fn consider_table(
        &self,
        tref: &TableRef<KEY_MAX>,
        key: &[u8],
        max_seq: u64,
        scratch: &mut [u8; BLOCK],
        decomp: &mut [u8; BLOCK],
        acc: &mut ReadAcc<VAL_MAX>,
    ) -> Result<(), Error<D::Error>> {
        // Both prunes are exact: the table's keys all lie within its bounds,
        // and no entry here can carry a seq above the table's max.
        if !tref.covers(key) || tref.max_seq <= acc.best_seq {
            return Ok(());
        }
        let footer = tref.footer_block().ok_or(Error::CorruptManifest)?;
        let reader = sstable::TableReader::<D, BLOCK, BLOOM_BYTES>::open_cached(
            self.wal.device(),
            Some(&self.cache as &dyn CachePort<BLOCK>),
            tref.id,
            scratch,
            footer,
        )
        .await?;
        // `tmp` (not `acc.stage`) receives the value: only a winning hit is
        // promoted, so a losing hit cannot clobber the staged winner.
        let mut tmp = [0u8; VAL_MAX];
        match reader
            .lookup_at(scratch, decomp, key, &mut tmp, max_seq)
            .await?
        {
            sstable::Lookup::Value {
                len,
                seq,
                expire_at,
            } if seq > acc.best_seq && seq <= max_seq => {
                acc.best_seq = seq;
                acc.stage[..len].copy_from_slice(&tmp[..len]);
                acc.best = Best::Value(len);
                acc.best_expire_at = expire_at;
            }
            sstable::Lookup::Tombstone { seq } if seq > acc.best_seq && seq <= max_seq => {
                acc.best_seq = seq;
                acc.best = Best::Tombstone;
            }
            _ => {}
        }
        // A range tombstone in this table covering `key` shadows older
        // point versions; the accumulator keeps the highest covering
        // sequence and compares it against the point winner at the end.
        if reader.rdel_blocks() > 0
            && let Some(q) = reader.covering_rdel_seq(scratch, key, max_seq).await?
            && q > acc.cover_seq
        {
            acc.cover_seq = q;
        }
        Ok(())
    }
}
