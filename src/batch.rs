//! Atomic write batches: caller-owned, fixed-capacity, no allocation.
//!
//! A [`WriteBatch`] queues up to `OPS` puts and deletes in caller-owned
//! storage. [`Db::write`](crate::Db::write) then applies every queued op
//! atomically: all become durable and visible together, or none does.
//! Sequence numbers are assigned at write time, consecutively, in queue
//! order — a batch never reserves seqs it does not use.

use core::convert::Infallible;

use crate::error::Error;
use crate::wal::Op;

/// One queued operation: the kind plus its key/value bytes.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BatchOp<const KEY_MAX: usize, const VAL_MAX: usize> {
    kind: Op,
    key_len: u16,
    key: [u8; KEY_MAX],
    val_len: u16,
    val: [u8; VAL_MAX],
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

    /// The value bytes (empty for deletes).
    pub(crate) fn val(&self) -> &[u8] {
        self.val[..usize::from(self.val_len)].as_ref()
    }
}

/// A fixed-capacity batch of puts and deletes, owned by the caller.
///
/// Created empty with [`new`](WriteBatch::new); ops are queued with
/// [`put`](WriteBatch::put) / [`delete`](WriteBatch::delete) and applied
/// atomically with [`Db::write`](crate::Db::write). The batch borrows
/// nothing — it copies key/value bytes into its own storage — so it can be
/// built once and reused via [`clear`](WriteBatch::clear).
///
/// All memory is inline: `OPS * (5 + KEY_MAX + VAL_MAX)` bytes plus a
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
        self.push(Op::Put, key, val)
    }

    /// Queues a delete (tombstone) of `key`.
    ///
    /// # Errors
    ///
    /// [`Error::BatchFull`] when `OPS` ops are already queued,
    /// [`Error::EmptyKey`], or [`Error::KeyTooLarge`].
    pub fn delete(&mut self, key: &[u8]) -> Result<(), Error<Infallible>> {
        self.push(Op::Delete, key, &[])
    }

    /// Queued operations in queue order.
    pub(crate) fn ops(&self) -> &[BatchOp<KEY_MAX, VAL_MAX>] {
        &self.ops[..self.len]
    }

    fn push(&mut self, kind: Op, key: &[u8], val: &[u8]) -> Result<(), Error<Infallible>> {
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
        self.len += 1;
        Ok(())
    }
}
