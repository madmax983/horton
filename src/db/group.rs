//! Group commit across WAL blocks (ADR-0028): [`Db::write_group`].
//!
//! [`Db::write`] makes one batch durable with one device flush. A host
//! that queues writes from many clients (the `examples/kvstore` writer
//! queue) wants one flush to cover all of them. `write_group` takes many
//! atomic units, each a client's batch, places them in WAL blocks (a unit
//! that does not fit what is left of a block starts a fresh one, sealed
//! without a flush; a unit larger than a block spans blocks, ADR-0029),
//! flushes once, and only then applies every unit to the memtable.
//! Nothing is visible before it is durable.

use core::convert::Infallible;

use crate::batch::check_op;
use crate::device::BlockDevice;
use crate::error::Error;
use crate::wal::{Op, WAL_RECORD_OVERHEAD};

use super::{Db, StageMark};

/// Most WAL blocks one [`Db::write_group`] writes before its flush.
///
/// A crash before the flush can leave any of them on the medium without
/// the ones before it (a device may reorder unflushed writes), so open
/// looks this many blocks past the recovered log for stranded records
/// (ADR-0028); the bound keeps that look short.
pub const GROUP_BLOCKS_MAX: usize = 16;

/// One mutation of a [`Db::write_group`] unit, borrowing the caller's
/// bytes. Validated as [`WriteBatch`](crate::WriteBatch) validates the
/// same op.
#[derive(Debug, Clone, Copy)]
pub struct GroupOp<'a> {
    kind: Op,
    key: &'a [u8],
    val: &'a [u8],
    expire_at: u64,
}

impl<'a> GroupOp<'a> {
    /// Insert or overwrite `key`.
    #[must_use]
    pub const fn put(key: &'a [u8], val: &'a [u8]) -> Self {
        Self {
            kind: Op::Put,
            key,
            val,
            expire_at: 0,
        }
    }

    /// Delete `key`.
    #[must_use]
    pub const fn delete(key: &'a [u8]) -> Self {
        Self {
            kind: Op::Delete,
            key,
            val: &[],
            expire_at: 0,
        }
    }

    /// Insert `key` with the absolute expiry tick `expire_at` (0: never
    /// expires, a plain [`put`](Self::put)). See
    /// [`Db::put_with_ttl`](crate::Db::put_with_ttl).
    #[must_use]
    pub const fn put_ttl(key: &'a [u8], val: &'a [u8], expire_at: u64) -> Self {
        Self {
            kind: if expire_at == 0 { Op::Put } else { Op::PutTtl },
            key,
            val,
            expire_at,
        }
    }

    /// Delete every key in `[start, end)`. An empty or inverted range is
    /// a no-op, as in [`WriteBatch::range_delete`](crate::WriteBatch::range_delete).
    #[must_use]
    pub const fn range_delete(start: &'a [u8], end: &'a [u8]) -> Self {
        Self {
            kind: Op::RangeDelete,
            key: start,
            val: end,
            expire_at: 0,
        }
    }

    /// True for an op that writes nothing: an empty range delete.
    fn is_noop(&self) -> bool {
        self.kind == Op::RangeDelete && self.key >= self.val
    }

    /// Bytes of WAL record and of memtable arena this op takes, once
    /// checked.
    fn cost<const KEY_MAX: usize, const VAL_MAX: usize>(
        &self,
    ) -> Result<(usize, usize), Error<Infallible>> {
        let (kl, vl) = check_op::<KEY_MAX, VAL_MAX>(self.kind, self.key, self.val)?;
        let (kl, vl) = (usize::from(kl), usize::from(vl));
        let ttl = if self.kind == Op::PutTtl { 8 } else { 0 };
        Ok((WAL_RECORD_OVERHEAD + kl + vl + ttl, kl + vl))
    }
}

/// What a [`Db::write_group`] committed: its first `units` units, durable
/// and visible.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupCommit {
    /// Units committed: a prefix of the units passed. The rest were not
    /// written; pass them again (after a flush, when the memtable or the
    /// WAL was what stopped them).
    pub units: usize,
    /// Sequence number of the first op of the first committed unit. The
    /// committed units' ops take consecutive numbers from here, in order;
    /// empty units and empty range deletes take none.
    pub base: u64,
}

/// A unit's demands, once its ops are checked.
struct UnitCost {
    /// Ops that write a record.
    ops: usize,
    /// WAL bytes of its largest record.
    largest: usize,
    /// WAL bytes of those records.
    wal: usize,
    /// Memtable arena bytes.
    arena: usize,
}

/// Where WAL records land, block by block, as `WalWriter` places them:
/// a unit that does not fit what is left of the block being filled
/// starts a new one (`WalWriter::make_room`), and within a unit, a record
/// that does not fit starts the next block, the group going on there
/// ([`SPAN`](crate::wal::SPAN), ADR-0029). `Db::write` and
/// [`Db::write_group`] plan with it, so what they check is what the
/// writer will do.
#[derive(Debug, Clone, Copy)]
pub struct Placement<const BLOCK: usize> {
    /// Blocks written out so far.
    pub sealed: usize,
    /// Bytes in the block being filled.
    pub filling: usize,
}

impl<const BLOCK: usize> Placement<BLOCK> {
    /// Places a unit of `wal` bytes in all, whose records are `records`
    /// bytes each (each at most a block). An empty unit takes nothing.
    pub fn unit(&mut self, wal: usize, records: impl Iterator<Item = usize>) {
        if wal == 0 {
            return;
        }
        if self.filling > 0 && self.filling.saturating_add(wal) > BLOCK {
            self.sealed += 1;
            self.filling = 0;
        }
        for rlen in records {
            if self.filling + rlen > BLOCK {
                self.sealed += 1;
                self.filling = rlen;
            } else {
                self.filling += rlen;
            }
        }
    }

    /// Blocks written once the block being filled is written too.
    pub const fn blocks(&self) -> usize {
        self.sealed + if self.filling > 0 { 1 } else { 0 }
    }
}

/// The prefix of a group that fits, and what it takes.
struct GroupPlan {
    /// Units in the prefix.
    units: usize,
    /// Ops of those units that write a record.
    ops: usize,
    /// Memtable arena bytes those records take.
    arena: usize,
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
    /// Commits many atomic units under one device flush: horton's group
    /// commit across WAL blocks (ADR-0028).
    ///
    /// Each unit is one client's batch of ops: all of it becomes durable
    /// and visible, or none of it does, exactly as with [`Db::write`].
    /// Units are placed in WAL blocks in order (a block is written out,
    /// without a flush, when the next unit does not fit what is left of
    /// it, and a unit larger than a block spans blocks from a fresh one,
    /// ADR-0029); then the device is flushed once, and
    /// only then are the units applied to the memtable, in order. A
    /// reader never sees a unit before it is durable.
    ///
    /// The longest prefix of `units` that fits is committed: every unit
    /// must fit [`GROUP_BLOCKS_MAX`] WAL blocks and an empty memtable,
    /// the prefix must fit the memtable's slots and
    /// arena and the WAL blocks left, and it spans at most
    /// [`GROUP_BLOCKS_MAX`] blocks. The units after it are not written;
    /// the result says how many were, and the caller passes the rest
    /// again (after [`flush`](Db::flush) if the memtable or the WAL was
    /// what stopped them). When not even the first unit fits, the call
    /// fails with the error [`Db::write`] would have given that unit
    /// alone. An empty `units` commits nothing, without I/O.
    ///
    /// A crash before the flush returns may keep any of the group's units,
    /// each whole or not at all; none of them was acknowledged. Open
    /// makes the outcome stick: a later crash never brings back a unit
    /// an earlier recovery dropped (see [`GROUP_BLOCKS_MAX`]).
    ///
    /// # Errors
    ///
    /// When the first unit does not fit: [`Error::EmptyKey`],
    /// [`Error::KeyTooLarge`] or [`Error::ValueTooLarge`] for a bad op,
    /// [`Error::BatchTooLarge`] when it exceeds [`GROUP_BLOCKS_MAX`] WAL
    /// blocks or an empty memtable (or one of its records a block),
    /// [`Error::TableFull`] / [`Error::ArenaFull`] when the memtable
    /// cannot take it, [`Error::WalFull`] when the WAL region is out of
    /// blocks. Then [`Error::CounterExhausted`], or [`Error::Device`] when
    /// a block write or the flush fails: the whole group fails, nothing
    /// of it is visible, and if a block had reached the device the
    /// sequence numbers it carried are not reused and its records count
    /// against the memtable until the next [`flush`](Db::flush).
    pub async fn write_group(
        &mut self,
        units: &[&[GroupOp<'_>]],
    ) -> Result<GroupCommit, Error<D::Error>> {
        self.ensure_open()?;
        // Capacity first, then the counter, as `write` checks them.
        let plan = self.plan_group(units)?;
        if plan.ops == 0 {
            // Nothing to write: empty units and empty range deletes.
            return Ok(GroupCommit {
                units: plan.units,
                base: self.next_seq.saturating_add(1),
            });
        }
        let base = self
            .next_seq
            .checked_add(1)
            .ok_or(Error::CounterExhausted)?;
        let nu64 = u64::try_from(plan.ops).map_err(|_| Error::CounterExhausted)?;
        let last = base.checked_add(nu64 - 1).ok_or(Error::CounterExhausted)?;
        // Blocks a failed commit left unflushed are flushed before this
        // group adds its own (see `Unapplied`). Then drain anything left
        // staged, as `write` does: the group's blocks start clean.
        self.sync_unapplied().await?;
        if self.wal.staged_bytes() > 0 {
            self.wal.commit().await?;
        }
        let mark = self.stage_mark();
        // Records appended so far: the group's record `i` takes
        // `base + i`, never stepping past `last`.
        let mut appended = 0u64;
        for unit in &units[..plan.units] {
            if let Err(e) = self.append_unit(unit, base, &mut appended).await {
                return Err(self.abandon_group(mark, appended, &plan, e));
            }
        }
        if let Err(e) = self.wal.commit().await {
            return Err(self.abandon_group(mark, nu64, &plan, e));
        }
        self.next_seq = last;
        // Durable: apply in order. The memtable is unchanged since the
        // plan checked it, so none of this can fail.
        let applied = units[..plan.units]
            .iter()
            .flat_map(|u| u.iter())
            .filter(|op| !op.is_noop());
        for (i, op) in (0u64..).zip(applied) {
            let seq = base + i;
            match op.kind {
                Op::Put => self.table.insert::<D::Error>(op.key, op.val, seq, false)?,
                Op::Delete => self.table.insert::<D::Error>(op.key, op.val, seq, true)?,
                Op::PutTtl => {
                    self.table
                        .insert_ttl::<D::Error>(op.key, op.val, seq, op.expire_at)?;
                }
                Op::RangeDelete => {
                    self.table
                        .insert_range_del::<D::Error>(op.key, op.val, seq)?;
                }
            }
        }
        Ok(GroupCommit {
            units: plan.units,
            base,
        })
    }

    /// The longest prefix of `units` that fits; the error the first unit
    /// would get alone when none does. Capacity counts the records a
    /// failed commit left in the WAL (see `Unapplied`).
    fn plan_group(&mut self, units: &[&[GroupOp<'_>]]) -> Result<GroupPlan, Error<D::Error>> {
        let (mut slots, mut arena) = self.memtable_taken();
        // Blocks written out so far, and bytes in the one being filled.
        let (mut sealed, mut filling) = (0usize, 0usize);
        let left = usize::try_from(self.wal.blocks_left()).unwrap_or(usize::MAX);
        let most = self.span_blocks_max();
        let mut plan = GroupPlan {
            units: 0,
            ops: 0,
            arena: 0,
        };
        for unit in units {
            let fits = Self::unit_cost(unit).and_then(|c| {
                // Where the unit's records land: the writer's own rule
                // (`Placement`), so a unit larger than a block spans
                // blocks exactly as it will be written (ADR-0029).
                let records = || {
                    unit.iter()
                        .filter(|op| !op.is_noop())
                        .map(|op| op.cost::<KEY_MAX, VAL_MAX>().map_or(0, |(wal, _)| wal))
                };
                // A record must fit a block, and a unit must fit the most
                // blocks one commit writes, even alone.
                if c.largest > BLOCK {
                    return Err(Error::BatchTooLarge {
                        bytes: c.largest,
                        max: BLOCK,
                    });
                }
                let mut alone = Placement::<BLOCK> {
                    sealed: 0,
                    filling: 0,
                };
                alone.unit(c.wal, records());
                if alone.blocks() > most {
                    return Err(Error::BatchTooLarge {
                        bytes: c.wal,
                        max: most.saturating_mul(BLOCK),
                    });
                }
                // Nor may it outgrow an empty memtable: a flush would not
                // make room for it.
                if c.ops > CAP {
                    return Err(Error::BatchTooLarge {
                        bytes: c.ops,
                        max: CAP,
                    });
                }
                if c.arena > ARENA {
                    return Err(Error::BatchTooLarge {
                        bytes: c.arena,
                        max: ARENA,
                    });
                }
                let mut place = Placement::<BLOCK> { sealed, filling };
                place.unit(c.wal, records());
                if slots.saturating_add(c.ops) > CAP {
                    return Err(Error::TableFull);
                }
                if arena.saturating_add(c.arena) > ARENA {
                    return Err(Error::ArenaFull);
                }
                Ok((c, place.sealed, place.filling, place.blocks()))
            });
            let (c, s, f, blocks) = match fits {
                Ok(fit) => fit,
                Err(e) if plan.units == 0 => return Err(e),
                Err(_) => break,
            };
            if blocks > left {
                if plan.units == 0 {
                    // The flush that answers it wraps the region now.
                    self.wal_wanted = u64::try_from(blocks).unwrap_or(u64::MAX);
                    return Err(Error::WalFull);
                }
                break;
            }
            if blocks > GROUP_BLOCKS_MAX {
                break;
            }
            slots += c.ops;
            arena += c.arena;
            (sealed, filling) = (s, f);
            plan.units += 1;
            plan.ops += c.ops;
            plan.arena += c.arena;
        }
        Ok(plan)
    }

    /// A unit's ops checked, and what they cost.
    fn unit_cost(unit: &[GroupOp<'_>]) -> Result<UnitCost, Error<D::Error>> {
        let mut cost = UnitCost {
            ops: 0,
            largest: 0,
            wal: 0,
            arena: 0,
        };
        for op in unit.iter().filter(|op| !op.is_noop()) {
            let (wal, arena) = op.cost::<KEY_MAX, VAL_MAX>().map_err(Error::widen)?;
            cost.ops += 1;
            cost.largest = cost.largest.max(wal);
            cost.wal = cost.wal.saturating_add(wal);
            cost.arena = cost.arena.saturating_add(arena);
        }
        Ok(cost)
    }

    /// Appends one unit as an atomic WAL group, sealing the stage first
    /// when the unit does not fit what is left of it. The group's record
    /// `i` takes `base + i`; `*appended` counts the records appended so
    /// far.
    async fn append_unit(
        &mut self,
        unit: &[GroupOp<'_>],
        base: u64,
        appended: &mut u64,
    ) -> Result<(), Error<D::Error>> {
        let cost = Self::unit_cost(unit)?;
        if cost.ops == 0 {
            return Ok(());
        }
        self.wal.make_room(cost.wal).await?;
        let mut left = cost.ops;
        for op in unit.iter().filter(|op| !op.is_noop()) {
            left -= 1;
            let more = left > 0;
            // At most the group's last sequence number, which the caller
            // checked fits.
            let seq = base.checked_add(*appended).ok_or(Error::CounterExhausted)?;
            if op.kind == Op::PutTtl {
                self.wal
                    .append_grouped_ttl(seq, op.key, op.val, op.expire_at, more)
                    .await?;
            } else {
                self.wal
                    .append_grouped(seq, op.kind, op.key, op.val, more)
                    .await?;
            }
            *appended += 1;
        }
        Ok(())
    }

    /// Undoes a group that failed after `appended` records were staged:
    /// the records still in the stage never reached the device and are
    /// dropped, so no later commit writes them. If any block reached the
    /// device, its records may replay after a crash: every sequence
    /// number the group used is consumed and never reused, and the
    /// group's memtable cost is counted as taken until a flush moves
    /// `wal_head` past it (see `Unapplied`). Returns `e`.
    fn abandon_group(
        &mut self,
        mark: StageMark,
        appended: u64,
        plan: &GroupPlan,
        e: Error<D::Error>,
    ) -> Error<D::Error> {
        let landed = self.wal.next_block() != mark.next_block;
        self.wal
            .truncate_stage(mark.stage_len.min(self.wal.staged_bytes()));
        if landed {
            self.note_unapplied(plan.ops, plan.arena);
            match self.next_seq.checked_add(appended) {
                Some(next) => self.next_seq = next,
                None => return Error::CounterExhausted,
            }
        }
        e
    }
}
