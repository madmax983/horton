//! Atomic write batches: caller-owned, fixed-capacity, no allocation.
//!
//! A [`WriteBatch`] queues up to `OPS` mutations — puts, deletes, range
//! deletes, and TTL puts — in caller-owned storage. [`Db::write`](crate::Db::write) then applies every queued op
//! atomically: all become durable and visible together, or none does.
//! Sequence numbers are assigned at write time, consecutively, in queue
//! order — a batch never reserves seqs it does not use.

use core::convert::Infallible;

use crate::error::Error;
use crate::wal::Op;

/// One queued operation: the kind plus its key/value bytes. For
/// [`Op::PutTtl`] the absolute expiry tick rides in `expire_at`
/// (`Op::Put` always stores 0); for [`Op::RangeDelete`] `key` is the
/// inclusive start and `val` the exclusive end.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BatchOp<const KEY_MAX: usize, const VAL_MAX: usize> {
    kind: Op,
    key_len: u16,
    key: [u8; KEY_MAX],
    val_len: u16,
    val: [u8; VAL_MAX],
    expire_at: u64,
}

impl<const KEY_MAX: usize, const VAL_MAX: usize> BatchOp<KEY_MAX, VAL_MAX> {
    /// The mutation kind.
    pub(crate) const fn kind(&self) -> Op {
        self.kind
    }

    /// The key bytes.
    pub(crate) fn key(&self) -> &[u8] {
        self.key[..usize::from(self.key_len)].as_ref()
    }

    /// The value bytes (empty for deletes; the exclusive end bound for
    /// range deletes).
    pub(crate) fn val(&self) -> &[u8] {
        self.val[..usize::from(self.val_len)].as_ref()
    }

    /// The absolute expiry tick for [`Op::PutTtl`] puts; 0 otherwise.
    pub(crate) const fn expire_at(&self) -> u64 {
        self.expire_at
    }
}

/// A fixed-capacity batch of mutations, owned by the caller.
///
/// Created empty with [`new`](WriteBatch::new); ops are queued with
/// [`put`](WriteBatch::put) / [`delete`](WriteBatch::delete) /
/// [`range_delete`](WriteBatch::range_delete) /
/// [`put_ttl`](WriteBatch::put_ttl) and applied atomically with
/// [`Db::write`](crate::Db::write). The batch borrows nothing — it
/// copies key/value bytes into its own storage — so it can be built once
/// and reused via [`clear`](WriteBatch::clear).
///
/// All memory is inline: `OPS * (13 + KEY_MAX + VAL_MAX)` bytes plus a
/// length. Queueing validates sizes eagerly, so a batch handed to
/// [`Db::write`](crate::Db::write) is already well-formed; the write then
/// checks only capacity (memtable slots/arena, one WAL block).
#[derive(Debug, Clone)]
pub struct WriteBatch<const KEY_MAX: usize, const VAL_MAX: usize, const OPS: usize> {
    ops: [BatchOp<KEY_MAX, VAL_MAX>; OPS],
    len: usize,
}

impl<const KEY_MAX: usize, const VAL_MAX: usize, const OPS: usize> Default
    for WriteBatch<KEY_MAX, VAL_MAX, OPS>
{
    fn default() -> Self {
        Self::new()
    }
}

impl<const KEY_MAX: usize, const VAL_MAX: usize, const OPS: usize>
    WriteBatch<KEY_MAX, VAL_MAX, OPS>
{
    /// An empty batch.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            ops: [BatchOp {
                kind: Op::Put,
                key_len: 0,
                key: [0u8; KEY_MAX],
                val_len: 0,
                val: [0u8; VAL_MAX],
                expire_at: 0,
            }; OPS],
            len: 0,
        }
    }

    /// Number of queued operations.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Whether no operations are queued.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Maximum number of queued operations.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        OPS
    }

    /// Discards all queued operations; the batch can be refilled.
    pub const fn clear(&mut self) {
        self.len = 0;
    }

    /// Queues a put of `key` → `val`.
    ///
    /// # Errors
    ///
    /// [`Error::BatchFull`] when `OPS` ops are already queued,
    /// [`Error::EmptyKey`], [`Error::KeyTooLarge`], or
    /// [`Error::ValueTooLarge`]. A batch touches no device, so its error
    /// type is `Error<Infallible>`; [`Error::widen`] converts it for a
    /// `Db` result: `batch.put(k, v).map_err(Error::widen)?`.
    pub fn put(&mut self, key: &[u8], val: &[u8]) -> Result<(), Error<Infallible>> {
        self.push(Op::Put, key, val, 0)
    }

    /// Queues a delete (tombstone) of `key`.
    ///
    /// # Errors
    ///
    /// [`Error::BatchFull`] when `OPS` ops are already queued,
    /// [`Error::EmptyKey`], or [`Error::KeyTooLarge`].
    pub fn delete(&mut self, key: &[u8]) -> Result<(), Error<Infallible>> {
        self.push(Op::Delete, key, &[], 0)
    }

    /// Queues a put of `key` → `val` with an absolute expiry tick.
    ///
    /// Mirrors [`wal::WalWriter::append_ttl`](crate::wal::WalWriter::append_ttl):
    /// `expire_at == 0` means no expiry and queues a plain [`Op::Put`].
    /// Horton owns no clock: timed reads suppress the value once
    /// `expire_at <= now`.
    ///
    /// # Errors
    ///
    /// [`Error::BatchFull`] when `OPS` ops are already queued,
    /// [`Error::EmptyKey`], [`Error::KeyTooLarge`], or
    /// [`Error::ValueTooLarge`].
    pub fn put_ttl(
        &mut self,
        key: &[u8],
        val: &[u8],
        expire_at: u64,
    ) -> Result<(), Error<Infallible>> {
        let kind = if expire_at == 0 { Op::Put } else { Op::PutTtl };
        self.push(kind, key, val, expire_at)
    }

    /// Queues a range delete of `[start, end)`.
    ///
    /// An empty or inverted range (`start >= end`) is an applied no-op —
    /// mirroring [`Db::delete_range`](crate::Db::delete_range) — and
    /// queues nothing: `Ok` with the batch unchanged.
    ///
    /// # Errors
    ///
    /// [`Error::BatchFull`] when `OPS` ops are already queued,
    /// [`Error::EmptyKey`], [`Error::KeyTooLarge`] (either bound), or
    /// [`Error::ValueTooLarge`] (the end bound must fit the value
    /// storage).
    pub fn range_delete(&mut self, start: &[u8], end: &[u8]) -> Result<(), Error<Infallible>> {
        if start >= end {
            return Ok(());
        }
        if end.len() > KEY_MAX {
            return Err(Error::KeyTooLarge {
                len: end.len(),
                max: KEY_MAX,
            });
        }
        self.push(Op::RangeDelete, start, end, 0)
    }

    /// Queued operations in queue order.
    pub(crate) fn ops(&self) -> &[BatchOp<KEY_MAX, VAL_MAX>] {
        &self.ops[..self.len]
    }

    fn push(
        &mut self,
        kind: Op,
        key: &[u8],
        val: &[u8],
        expire_at: u64,
    ) -> Result<(), Error<Infallible>> {
        if self.len >= OPS {
            return Err(Error::BatchFull);
        }
        if key.is_empty() {
            return Err(Error::EmptyKey);
        }
        let key_len = u16::try_from(key.len()).map_err(|_| Error::KeyTooLarge {
            len: key.len(),
            max: usize::from(u16::MAX),
        })?;
        if key.len() > KEY_MAX {
            return Err(Error::KeyTooLarge {
                len: key.len(),
                max: KEY_MAX,
            });
        }
        let vlen = if kind == Op::Delete { 0 } else { val.len() };
        let val_len = u16::try_from(vlen).map_err(|_| Error::ValueTooLarge {
            len: vlen,
            max: usize::from(u16::MAX),
        })?;
        if vlen > VAL_MAX {
            return Err(Error::ValueTooLarge {
                len: vlen,
                max: VAL_MAX,
            });
        }
        let slot = &mut self.ops[self.len];
        slot.kind = kind;
        slot.key_len = key_len;
        slot.key[..key.len()].copy_from_slice(key);
        slot.val_len = val_len;
        slot.val[..vlen].copy_from_slice(&val[..vlen]);
        slot.expire_at = expire_at;
        self.len += 1;
        Ok(())
    }
}
