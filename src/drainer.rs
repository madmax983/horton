//! Poll-based single drainer for the `multiwriter` feature.
//!
//! SPEC: `docs/multiwriter-spec.md` §§7, 11. The topology is
//!
//! ```text
//! writer threads ──atomics──▶ ring ──drainer──▶ Db ──▶ Device
//! ```
//!
//! The drainer exclusively owns the [`Db`] (SPEC O2: sole
//! device writer — writers share only the atomic ring, and no
//! `BlockDevice` signature changes). Each [`Drainer::sweep`] drains the
//! ring's published tickets in order, converts the 32-byte payloads to a
//! [`WriteBatch`], applies it through
//! [`Db::write`](crate::db::Db::write) (one WAL flush per sweep, memtable
//! updated atomically), and advances the `durable` ticket watermark
//! **only after the write is acknowledged** (SPEC O1: acked ⇒ durable).
//!
//! # Completion signaling
//!
//! A single `durable: AtomicU32` tracks the contiguous WAL-durable ticket
//! prefix, modulo 2³¹. The host owns it and shares `&AtomicU32` with the
//! drainer and the writers. A writer's put completes when `durable` has
//! advanced past its ticket — see [`is_ticket_durable`]. The drainer is
//! the sole writer of `durable` and advances it via `compare_exchange`,
//! one ticket at a time, in drain order. Fenced/skipped tickets carry no
//! WAL record; they are buffered with the batch and the watermark
//! advances past them only when the batch is durable (the contiguous
//! resolved prefix — advancing early would break O1 ordering).
//!
//! Dropping a post-acceptance pending put abandons *observation*, not the
//! write: the drainer still drains and persists it.
//!
//! # Failure policy
//!
//! `Db::write` is atomic. On success, all ops are durable. On error, no
//! op is applied, but after a device error the next open can replay the
//! whole batch. On a write error the drainer:
//!
//! 1. on `TableFull`, `ArenaFull` or `WalFull`, keeps the pending
//!    tickets and returns [`SweepOutcome::Stalled`]. The host calls
//!    [`Drainer::flush`] and sweeps again. If the flush returns
//!    `NeedsCompaction`, the host calls [`Drainer::compact_step`] until
//!    `Progress::Done`, then flushes again. No ticket is lost, and
//!    `durable` does not move.
//! 2. on `BatchTooLarge` with more than one op, tries again with half
//!    of the tickets. The other tickets stay pending (ADR-0037).
//! 3. on all other errors, poisons itself. `sweep` returns the error.
//!    Later sweeps return `Idle` and do nothing. The pending tickets
//!    stay in the drainer.
//!
//! A sweep stops at the first ticket that the `Db` cannot hold (a
//! payload that does not decode, or a key or value too large). It
//! writes the tickets before it. The next sweep poisons the drainer, and
//! [`Drainer::bad_ticket`] names the ticket. A [`Writer`] from
//! [`Drainer::writer`] refuses such a payload before it claims a ticket
//! (issue #48).
//!
//! After poison, the host does one of two things:
//!
//! - It removes the cause (for example, a device fault) and calls
//!   [`Drainer::resume`]. The next sweep writes the pending tickets
//!   again.
//! - It fails the outstanding writers. `durable` shows which tickets
//!   completed. It does not call `resume` after this: the failed writes
//!   then complete.
//!
//! To continue on the same ring and `durable`, keep the drainer.
//! [`Drainer::into_db`] drops the pending tickets. A new drainer does
//! not have them, so `durable` stops at the first of them, and no later
//! ticket completes (issue #47, ADR-0036).

#![forbid(unsafe_code)]

use crate::batch::WriteBatch;
use crate::compact::{Compaction, Progress};
use crate::db::Db;
use crate::ring::{DrainPoll, FenceOutcome, Ring, TICKET_MASK};
use crate::writer::Writer;
use crate::{BlockDevice, Error};
use core::convert::Infallible;
use core::sync::atomic::{AtomicU32, Ordering};

/// 32-byte ring payload codec.
///
/// The ring carries a fixed 32-byte payload per slot — one serialized
/// mutation (SPEC §14). Layouts, with op bytes matching [`Op`](crate::wal::Op)'s
/// discriminants:
///
/// ```text
/// Put:         [1][klen:1][vlen:1][key: klen][val: vlen]
/// Delete:      [2][klen:1][0][key: klen]
/// RangeDelete: [3][slen:1][elen:1][start: slen][end: elen]
/// PutTtl:      [4][klen:1][vlen:1][expiry:8 LE][key: klen][val: vlen]
/// ```
///
/// Codec invariants (PROOF):
/// - Fixed 32-byte slots: the payload bytes after the 3-byte header never
///   exceed 29 (`klen + vlen <= 29`, `slen + elen <= 29`,
///   `8 + klen + vlen <= 29`). Length arithmetic is widened past `u8`
///   before adding, so adversarial lengths return `None`/`Err` instead of
///   overflowing; every slice is in-bounds by the budget test.
/// - Op discriminants are [`Op`](crate::wal::Op)'s wire values; no parallel numbering to
///   drift.
/// - `decode` is total over all 2²⁵⁶ slot values: unknown op bytes,
///   zero/overlong lengths, and budget overflows yield `DecodeError` —
///   never a panic, never a silent reinterpretation as a valid mutation.
/// - Each `encode_*` is the left inverse of `decode` on well-formed
///   inputs (round-trip tests below).
/// - `encode_put_ttl` with `expire_at == 0` encodes a plain Put,
///   mirroring `wal::append_ttl`.
/// - A range delete with `start >= end` encodes and decodes fine but
///   applies as a no-op, mirroring `Db::delete_range`: the ticket
///   resolves without queueing an op or consuming a sequence number.
pub mod payload {
    use crate::Error;
    use crate::batch::check_op;
    use crate::wal::Op;
    use core::convert::Infallible;

    /// Operation byte for a put payload.
    pub const PUT: u8 = Op::Put as u8;
    /// Operation byte for a delete payload.
    pub const DELETE: u8 = Op::Delete as u8;
    /// Operation byte for a range-delete payload.
    pub const RANGE_DELETE: u8 = Op::RangeDelete as u8;
    /// Operation byte for a TTL put payload.
    pub const PUT_TTL: u8 = Op::PutTtl as u8;

    /// One decoded ring payload: a single mutation.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Decoded<'a> {
        /// `key` → `val`.
        Put {
            /// The key bytes.
            key: &'a [u8],
            /// The value bytes.
            val: &'a [u8],
        },
        /// Tombstone for `key`.
        Delete {
            /// The key bytes.
            key: &'a [u8],
        },
        /// Range tombstone for `[start, end)`.
        RangeDelete {
            /// The inclusive start bound.
            start: &'a [u8],
            /// The exclusive end bound.
            end: &'a [u8],
        },
        /// `key` → `val`; timed reads suppress the value once
        /// `expire_at <= now`.
        PutTtl {
            /// The key bytes.
            key: &'a [u8],
            /// The value bytes.
            val: &'a [u8],
            /// The absolute expiry tick.
            expire_at: u64,
        },
    }

    /// Encode a put into a 32-byte payload. Returns `None` when the key is
    /// empty/longer than 29 bytes or key+value exceed 29 bytes.
    #[must_use]
    pub fn encode_put(key: &[u8], val: &[u8]) -> Option<[u8; 32]> {
        let klen = u8::try_from(key.len()).ok()?;
        let vlen = u8::try_from(val.len()).ok()?;
        // Widened arithmetic: `klen + vlen` as `u8` would overflow on
        // adversarial lengths instead of returning `None`.
        if klen == 0 || klen > 29 || u16::from(klen) + u16::from(vlen) > 29 {
            return None;
        }
        let mut p = [0u8; 32];
        p[0] = PUT;
        p[1] = klen;
        p[2] = vlen;
        p[3..3 + key.len()].copy_from_slice(key);
        p[3 + key.len()..3 + key.len() + val.len()].copy_from_slice(val);
        Some(p)
    }

    /// Encode a delete into a 32-byte payload. Returns `None` when the key
    /// is empty or longer than 29 bytes.
    #[must_use]
    pub fn encode_delete(key: &[u8]) -> Option<[u8; 32]> {
        let klen = u8::try_from(key.len()).ok()?;
        if klen == 0 || klen > 29 {
            return None;
        }
        let mut p = [0u8; 32];
        p[0] = DELETE;
        p[1] = klen;
        p[2] = 0;
        p[3..3 + key.len()].copy_from_slice(key);
        Some(p)
    }

    /// Encode a range delete of `[start, end)` into a 32-byte payload.
    ///
    /// Returns `None` when either bound is empty/longer than 29 bytes or
    /// the bounds exceed 29 bytes together. An inverted range
    /// (`start >= end`) still encodes; it applies as a no-op.
    #[must_use]
    pub fn encode_range_delete(start: &[u8], end: &[u8]) -> Option<[u8; 32]> {
        let slen = u8::try_from(start.len()).ok()?;
        let elen = u8::try_from(end.len()).ok()?;
        if slen == 0 || elen == 0 || u16::from(slen) + u16::from(elen) > 29 {
            return None;
        }
        let mut p = [0u8; 32];
        p[0] = RANGE_DELETE;
        p[1] = slen;
        p[2] = elen;
        p[3..3 + start.len()].copy_from_slice(start);
        p[3 + start.len()..3 + start.len() + end.len()].copy_from_slice(end);
        Some(p)
    }

    /// Encode a TTL put into a 32-byte payload. Returns `None` when the key
    /// is empty or `8 + key.len() + val.len()` exceeds 29 bytes.
    /// `expire_at == 0` encodes a plain put, mirroring `wal::append_ttl`.
    #[must_use]
    pub fn encode_put_ttl(key: &[u8], val: &[u8], expire_at: u64) -> Option<[u8; 32]> {
        if expire_at == 0 {
            // No expiry: byte-identical to a plain Put (the TTL layout's
            // 8-byte hole would otherwise desync the key offset).
            return encode_put(key, val);
        }
        let klen = u8::try_from(key.len()).ok()?;
        let vlen = u8::try_from(val.len()).ok()?;
        if klen == 0 || 8 + u16::from(klen) + u16::from(vlen) > 29 {
            return None;
        }
        let mut p = [0u8; 32];
        p[0] = PUT_TTL;
        p[1] = klen;
        p[2] = vlen;
        p[3..11].copy_from_slice(&expire_at.to_le_bytes());
        p[11..11 + key.len()].copy_from_slice(key);
        p[11 + key.len()..11 + key.len() + val.len()].copy_from_slice(val);
        Some(p)
    }

    /// Decode a payload into one mutation.
    ///
    /// Total over every 32-byte input: returns `Err` on any malformed
    /// input — unknown op byte, zero or overlong length, or a length
    /// budget overflow — and never panics. Malformed payloads are never
    /// silently reinterpreted as valid mutations: the drainer poisons on
    /// decode failure rather than writing a clamped mutation.
    ///
    /// # Errors
    ///
    /// Returns [`DecodeError`] on any malformed input: [`DecodeError::BadOp`]
    /// for an unknown op byte, [`DecodeError::BadKeyLen`] for a zero or
    /// overlong key/bound length, [`DecodeError::BadValLen`] for a length
    /// budget overflow (key+value, bound+bound, or 8+key+value over 29
    /// bytes) or a nonzero value length on a delete.
    pub fn decode(p: &[u8; 32]) -> Result<Decoded<'_>, DecodeError> {
        let op = match p[0] {
            x if x == PUT => Op::Put,
            x if x == DELETE => Op::Delete,
            x if x == RANGE_DELETE => Op::RangeDelete,
            x if x == PUT_TTL => Op::PutTtl,
            _ => return Err(DecodeError::BadOp),
        };
        // `usize` arithmetic: no overflow on adversarial bytes.
        let a = usize::from(p[1]);
        let b = usize::from(p[2]);
        match op {
            Op::Put => {
                if a == 0 || a > 29 {
                    return Err(DecodeError::BadKeyLen);
                }
                if a + b > 29 {
                    return Err(DecodeError::BadValLen);
                }
                // a <= 29 and a+b <= 29: all slices in bounds.
                Ok(Decoded::Put {
                    key: &p[3..3 + a],
                    val: &p[3 + a..3 + a + b],
                })
            }
            Op::Delete => {
                if a == 0 || a > 29 {
                    return Err(DecodeError::BadKeyLen);
                }
                if b != 0 {
                    return Err(DecodeError::BadValLen);
                }
                Ok(Decoded::Delete { key: &p[3..3 + a] })
            }
            Op::RangeDelete => {
                if a == 0 || a > 29 || b == 0 || b > 29 {
                    return Err(DecodeError::BadKeyLen);
                }
                if a + b > 29 {
                    return Err(DecodeError::BadValLen);
                }
                Ok(Decoded::RangeDelete {
                    start: &p[3..3 + a],
                    end: &p[3 + a..3 + a + b],
                })
            }
            Op::PutTtl => {
                if a == 0 || a > 21 {
                    return Err(DecodeError::BadKeyLen);
                }
                if 8 + a + b > 29 {
                    return Err(DecodeError::BadValLen);
                }
                // 8+a+b <= 29: the expiry and both slices are in bounds.
                let mut expiry = [0u8; 8];
                expiry.copy_from_slice(&p[3..11]);
                Ok(Decoded::PutTtl {
                    key: &p[11..11 + a],
                    val: &p[11 + a..11 + a + b],
                    expire_at: u64::from_le_bytes(expiry),
                })
            }
        }
    }

    /// Checks that a `Db` with these `KEY_MAX` and `VAL_MAX` can take
    /// `p`. The drainer refuses a ticket when this refuses its payload,
    /// with the same error. An inverted range delete is a no-op, so it
    /// passes.
    ///
    /// # Errors
    ///
    /// [`Error::BadPayload`] when `p` does not decode, else
    /// [`Error::EmptyKey`], [`Error::KeyTooLarge`] or
    /// [`Error::ValueTooLarge`].
    pub fn check<const KEY_MAX: usize, const VAL_MAX: usize>(
        p: &[u8; 32],
    ) -> Result<(), Error<Infallible>> {
        let (kind, key, val) = match decode(p).map_err(|_| Error::BadPayload)? {
            Decoded::Put { key, val } => (Op::Put, key, val),
            Decoded::Delete { key } => (Op::Delete, key, &[][..]),
            Decoded::RangeDelete { start, end } if start < end => (Op::RangeDelete, start, end),
            Decoded::RangeDelete { .. } => return Ok(()),
            Decoded::PutTtl { key, val, .. } => (Op::PutTtl, key, val),
        };
        check_op::<KEY_MAX, VAL_MAX>(kind, key, val).map(|_| ())
    }

    /// A malformed 32-byte ring payload.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum DecodeError {
        /// Unknown operation byte.
        BadOp,
        /// Key (or range bound) length is zero or exceeds its budget.
        BadKeyLen,
        /// Value length is inconsistent with the operation and key length.
        BadValLen,
    }
}

/// Returns `true` when the `durable` watermark has advanced past `ticket`
/// — i.e. the writer's put is WAL-durable and complete.
///
/// Both values are modulo 2³¹. `durable` is "past" the ticket when the
/// forward distance `(durable - ticket) mod 2³¹` is a small positive
/// number: non-zero (the ticket itself is not yet durable) and `< 2³⁰` (a
/// behind-or-equal watermark yields a distance ≥ 2³⁰, or zero).
#[must_use]
pub fn is_ticket_durable(durable: &AtomicU32, ticket: u32) -> bool {
    let dist = durable
        .load(Ordering::Acquire)
        .wrapping_sub(ticket & TICKET_MASK)
        & TICKET_MASK;
    dist != 0 && dist < (1 << 30)
}

/// The drain watermark: the contiguous WAL-durable ticket prefix.
///
/// This is the **writer-completion** watermark (SPEC §9) — **not** the
/// claim head, and **not** a database sequence number. Every ticket `<`
/// the returned value (mod 2³¹) is WAL-durable, so a writer whose ticket
/// is `<` the watermark knows its put is complete.
///
/// Snapshot readers do **not** pin this value: Horton snapshots use the
/// Db's own sequence numbers (`Db::snapshot`), which are independent of
/// tickets — fenced tickets advance the ticket watermark without ever
/// receiving a Db sequence number. The ticket→seqnum mapping comes from
/// `Db::write`'s return (the base seqnum of the drained batch); exposing
/// a seqnum watermark for snapshot pinning is a future slice.
///
/// The drainer publishes the watermark with Release
/// (in [`Drainer::sweep`]); this loads it with Acquire. The value
/// never moves backward: tickets drain in ticket order and the drainer is
/// the sole writer.
#[must_use]
pub fn drain_watermark(durable: &AtomicU32) -> u32 {
    durable.load(Ordering::Acquire) & TICKET_MASK
}

/// Outcome of one [`Drainer::sweep`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SweepOutcome {
    /// Wrote `tickets` pending tickets (one batch, one flush). The
    /// `durable` watermark advanced past each of them. Other tickets
    /// can stay pending: the next sweep writes them first.
    Swept {
        /// Number of tickets made durable in this sweep (including
        /// fenced/skipped tickets, which resolve without a WAL record).
        tickets: usize,
    },
    /// The ring held no published tickets. The drainer may have fenced a
    /// stalled cursor ticket; re-poll to make progress.
    Idle,
    /// The `Db` refused the batch with an error that a flush answers:
    /// `TableFull` or `ArenaFull` (the memtable is full), or `WalFull`
    /// (the WAL region is full). The pending tickets stay in the
    /// drainer, and `durable` does not move. The host calls
    /// [`Drainer::flush`] and sweeps again.
    Stalled,
}

/// The single Db-owning drainer.
///
/// Owns the [`Db`]; shares `&Ring` and `&AtomicU32
/// durable` with the host and the writers. `durable` is the contiguous
/// WAL-durable ticket prefix (mod 2³¹); the drainer is its sole writer.
/// All batch scratch is caller-sized (`MAX_WRITES`); no allocation, no
/// unsafe.
///
/// Tickets drained but not yet applied (after `Stalled` or poison)
/// are buffered in the drainer — `durable` does not advance until the
/// `Db::write` succeeds, so the watermark is always the contiguous
/// durable prefix.
pub struct Drainer<
    'r,
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
    const N: usize,
    const MAX_WRITES: usize,
> {
    ring: &'r Ring<N>,
    db: Db<D, BLOCK, KEY_MAX, VAL_MAX, CAP, ARENA, LEVELS, TABLES, BLOOM_BYTES, CACHE>,
    durable: &'r AtomicU32,
    /// Tickets drained but not yet Db-durable (the last sweep stalled,
    /// or poisoned the drainer). In drain order; `pending_len` entries
    /// are valid.
    pending_tickets: [u32; MAX_WRITES],
    /// Payloads for the pending tickets. Only valid where
    /// `pending_has_data` is set; fenced/skipped tickets have no payload.
    pending_payloads: [[u8; 32]; MAX_WRITES],
    /// Whether `pending_payloads[i]` holds a WAL-bearing payload.
    pending_has_data: [bool; MAX_WRITES],
    pending_len: usize,
    /// Poll-count stall budget before the drainer fences the cursor.
    /// Horton owns no clock; the budget counts `sweep` polls that found
    /// nothing published.
    stall_budget: u32,
    stall_polls: u32,
    /// Set on the first write error that a flush does not answer. The
    /// drainer stops until [`Drainer::resume`]. [`Drainer::sweep`]
    /// returns the error itself.
    poisoned: bool,
    /// The ticket that poisoned the drainer, when the error came from
    /// that ticket. It is the first pending ticket.
    bad_ticket: Option<u32>,
    /// Highest Db sequence number covered by the acknowledged ticket
    /// prefix (SPEC §13). Tickets and Db seqnums are independent counters;
    /// the Db assigns seqnums and the drainer learns the mapping from
    /// [`Db::write`](crate::Db::write)'s return value. Seeded from the
    /// Db's tip at construction; advanced on every sweep that writes.
    durable_seq: u64,
}

impl<
    'r,
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
    const N: usize,
    const MAX_WRITES: usize,
>
    Drainer<
        'r,
        D,
        BLOCK,
        KEY_MAX,
        VAL_MAX,
        CAP,
        ARENA,
        LEVELS,
        TABLES,
        BLOOM_BYTES,
        CACHE,
        N,
        MAX_WRITES,
    >
{
    /// Creates a drainer over `ring` and `db`.
    ///
    /// `durable` must be initialized to the ring's head ticket (0 for a
    /// fresh ring, or the seeded head): it is the next ticket the drainer
    /// expects, and tickets `<` it are already resolved. The `Db` manages
    /// its own WAL sequence numbers (recovered from `open`); tickets and
    /// WAL seqnums are independent counters, and the drainer seeds its
    /// durable-seqnum tracking from the Db's tip here. The host owns
    /// `durable` and shares it with the writers.
    #[must_use]
    pub const fn new(
        ring: &'r Ring<N>,
        db: Db<D, BLOCK, KEY_MAX, VAL_MAX, CAP, ARENA, LEVELS, TABLES, BLOOM_BYTES, CACHE>,
        durable: &'r AtomicU32,
    ) -> Self {
        // Seed the durable-seqnum tracker from the Db's tip before the
        // Db is moved into the drainer (the borrow must end first: const
        // fn cannot drop `Db` on any path).
        let seq = db.next_seq();
        Self {
            ring,
            db,
            durable,
            pending_tickets: [0u32; MAX_WRITES],
            pending_payloads: [[0u8; 32]; MAX_WRITES],
            pending_has_data: [false; MAX_WRITES],
            pending_len: 0,
            stall_budget: 1024,
            stall_polls: 0,
            poisoned: false,
            bad_ticket: None,
            durable_seq: seq,
        }
    }

    /// Sets the poll-count stall budget (default 1024).
    pub const fn set_stall_budget(&mut self, budget: u32) {
        self.stall_budget = budget;
    }

    /// Returns `true` when a write error stopped the drainer.
    #[must_use]
    pub const fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    /// Clears the poison. The pending tickets stay, and the next
    /// [`sweep`](Drainer::sweep) writes them before it drains new
    /// tickets. While poisoned, `sweep` returns `Idle`. Without poison,
    /// this does nothing.
    ///
    /// Call this only when the cause of the error is gone, for example
    /// after a device fault is repaired. If the error came from a ticket
    /// (a bad payload, or a key or value that is too large), the next
    /// sweep poisons the drainer again. Do not call this after the host
    /// failed the outstanding writers: their writes then complete.
    ///
    /// The retry is safe after a device error: the failed batch was not
    /// applied. If its blocks replay at the next open, the retry has the
    /// same operations in the same order, so the result is the same.
    pub const fn resume(&mut self) {
        self.poisoned = false;
        self.bad_ticket = None;
    }

    /// Returns the ticket that poisoned the drainer, when the error came
    /// from that ticket: a payload that does not decode, or an op that
    /// this `Db` cannot hold. It is the first pending ticket. Returns
    /// `None` when the drainer is not poisoned, after other errors, and
    /// after [`resume`](Drainer::resume).
    ///
    /// The drainer wrote the tickets before this ticket. No later ticket
    /// completes. Fail this ticket's writer and all later writers.
    #[must_use]
    pub const fn bad_ticket(&self) -> Option<u32> {
        self.bad_ticket
    }

    /// Returns a [`Writer`] for this drainer's ring and `durable`. It
    /// refuses a payload that this `Db` cannot hold before it claims a
    /// ticket, so one bad writer cannot stop the others. It does not
    /// borrow the drainer: give a copy to each writer thread.
    #[must_use]
    pub const fn writer(&self) -> Writer<'r, N, KEY_MAX, VAL_MAX> {
        Writer::new(self.ring, self.durable)
    }

    /// Returns the number of drained tickets that are not yet in the
    /// `Db`. Tickets stay pending after `Stalled` and after poison.
    #[must_use]
    pub const fn pending(&self) -> usize {
        self.pending_len
    }

    /// The current drain watermark (see [`drain_watermark`]): the
    /// contiguous WAL-durable ticket prefix, for writer completion.
    /// This is a ticket watermark, not a Db sequence number — snapshot
    /// readers use [`snapshot`](Drainer::snapshot) (pinned at
    /// [`durable_seqnum`](Drainer::durable_seqnum)), not this value
    /// (SPEC §9).
    #[must_use]
    pub fn durable_watermark(&self) -> u32 {
        drain_watermark(self.durable)
    }

    /// Highest Db sequence number covered by the acknowledged ticket
    /// prefix (SPEC §13). Fenced/skipped tickets create no seqnums, so
    /// this can lag behind a pure op count; it moves only when a sweep
    /// actually writes. A host that snapshots right after
    /// [`sweep`](Drainer::sweep) sees exactly this watermark.
    #[must_use]
    pub const fn durable_seqnum(&self) -> u64 {
        self.durable_seq
    }

    /// Takes a [`Db`] snapshot pinned at the current Db tip. Called right
    /// after [`sweep`](Drainer::sweep), the tip is exactly
    /// [`durable_seqnum`](Drainer::durable_seqnum): reads pinned to the
    /// returned watermark see precisely the acknowledged ticket prefix,
    /// however much is written afterwards. Allowed while poisoned — it
    /// pins whatever the Db tip is, which is the last acked prefix unless
    /// the failed write consumed sequence numbers.
    ///
    /// # Errors
    ///
    /// [`Error::SnapshotLimit`] when eight snapshots are already live;
    /// [`Error::NotOpen`] if the Db was never opened.
    pub const fn snapshot(&mut self) -> Result<u64, Error<D::Error>> {
        self.db.snapshot()
    }

    /// Consumes the drainer and returns the owned [`Db`].
    ///
    /// This drops the pending tickets (see [`pending`](Drainer::pending)).
    /// Their writes are not in the memtable, and their writers do not
    /// complete. After a device error, the next open can replay them.
    /// A new drainer on the same ring and `durable` stops at the first
    /// dropped ticket. To continue, keep this drainer: use
    /// [`flush`](Drainer::flush), [`compact_step`](Drainer::compact_step),
    /// [`request_compaction`](Drainer::request_compaction) and
    /// [`resume`](Drainer::resume).
    #[must_use]
    pub fn into_db(
        self,
    ) -> Db<D, BLOCK, KEY_MAX, VAL_MAX, CAP, ARENA, LEVELS, TABLES, BLOOM_BYTES, CACHE> {
        self.db
    }

    /// Flushes the memtable to an `SSTable` (via the owned `Db`). The host
    /// calls this when [`sweep`](Drainer::sweep) returns
    /// [`SweepOutcome::Stalled`], then re-sweeps to apply the buffered
    /// tickets. If the WAL region is full, the flush wraps it.
    ///
    /// # Errors
    ///
    /// Returns the error of [`Db::flush`]. The drainer does not poison
    /// itself. On [`Error::NeedsCompaction`], call
    /// [`compact_step`](Drainer::compact_step) until
    /// [`Progress::Done`], then flush again.
    pub async fn flush(&mut self) -> Result<(), Error<D::Error>> {
        self.db.flush().await
    }

    /// Runs one bounded compaction step on the owned `Db`
    /// ([`Db::compact_step`]). The pending tickets stay in the drainer.
    ///
    /// # Errors
    ///
    /// Returns the error of [`Db::compact_step`]. The drainer does not
    /// poison itself.
    pub async fn compact_step(
        &mut self,
        scratch: &mut Compaction<BLOCK, KEY_MAX, VAL_MAX, BLOOM_BYTES>,
    ) -> Result<Progress, Error<D::Error>> {
        self.db.compact_step(scratch).await
    }

    /// Returns `true` when [`compact_step`](Drainer::compact_step) has
    /// work ([`Db::compaction_pending`]).
    #[must_use]
    pub fn compaction_pending(&self) -> bool {
        self.db.compaction_pending()
    }

    /// Requests a compaction from `level` to the bottom level
    /// ([`Db::request_compaction`]). This is part of the remedy for
    /// [`Error::RegionFull`]. The pending tickets stay in the drainer.
    ///
    /// # Errors
    ///
    /// Returns the error of [`Db::request_compaction`]. The drainer
    /// does not poison itself.
    pub fn request_compaction(&mut self, level: usize) -> Result<(), Error<D::Error>> {
        self.db.request_compaction(level)
    }

    /// Advance the `durable` watermark past `ticket` via
    /// `compare_exchange` (SPEC O1: the drainer is the sole writer and
    /// processes tickets in order, so the expected value is `ticket`).
    fn advance_durable(&self, ticket: u32) {
        let t = ticket & TICKET_MASK;
        let next = t.wrapping_add(1) & TICKET_MASK;
        let _ = self
            .durable
            .compare_exchange(t, next, Ordering::Release, Ordering::Relaxed);
    }

    /// Advances `durable` over the first `take` pending tickets, in
    /// order, and moves the rest to the front. Call it only after the
    /// `Db::write` of those tickets succeeded, or when they hold no op.
    fn ack_prefix(&mut self, take: usize) {
        for i in 0..take {
            self.advance_durable(self.pending_tickets[i]);
        }
        let len = self.pending_len;
        self.pending_tickets.copy_within(take..len, 0);
        self.pending_payloads.copy_within(take..len, 0);
        self.pending_has_data.copy_within(take..len, 0);
        self.pending_len = len - take;
    }

    /// Poisons the drainer on an error that comes from the first pending
    /// ticket.
    const fn poison_bad_ticket(&mut self) {
        self.poisoned = true;
        self.bad_ticket = self.pending_tickets.first().copied();
    }

    /// Queues the ops of the first `take` pending tickets in `batch`. It
    /// stops at the first ticket that it cannot queue. Returns the
    /// tickets queued, the ops queued, and the error of the ticket after
    /// them, if any.
    fn fill(
        &self,
        take: usize,
        batch: &mut WriteBatch<KEY_MAX, VAL_MAX, MAX_WRITES>,
    ) -> (usize, usize, Option<Error<Infallible>>) {
        let mut n_ops = 0usize;
        for i in 0..take {
            if !self.pending_has_data[i] {
                continue;
            }
            // A malformed payload is never written as some other op.
            let queued = match payload::decode(&self.pending_payloads[i]) {
                Ok(payload::Decoded::Put { key, val }) => batch.put(key, val).map(|()| true),
                Ok(payload::Decoded::Delete { key }) => batch.delete(key).map(|()| true),
                // An inverted range is a no-op, as in `Db::delete_range`:
                // the ticket resolves and takes no sequence number.
                Ok(payload::Decoded::RangeDelete { start, end }) if start >= end => Ok(false),
                Ok(payload::Decoded::RangeDelete { start, end }) => {
                    batch.range_delete(start, end).map(|()| true)
                }
                Ok(payload::Decoded::PutTtl {
                    key,
                    val,
                    expire_at,
                }) => batch.put_ttl(key, val, expire_at).map(|()| true),
                Err(_) => Err(Error::BadPayload),
            };
            match queued {
                Ok(true) => n_ops += 1,
                Ok(false) => {}
                // A ticket that the `Db` cannot hold. `BatchFull` cannot
                // occur: `take <= MAX_WRITES`.
                Err(e) => return (i, n_ops, Some(e)),
            }
        }
        (take, n_ops, None)
    }

    /// One drain-batch-flush-advance cycle.
    ///
    /// Drains up to `MAX_WRITES` published tickets in ticket order into
    /// the pending buffer. If an earlier sweep left pending tickets,
    /// this sweep writes them and drains no new tickets. Builds a
    /// [`WriteBatch`] from the longest pending prefix that the `Db`
    /// takes. Applies it with one `Db::write` (one WAL flush; the
    /// memtable changes atomically). Then advances `durable` over that
    /// prefix: WAL-bearing, fenced, and skipped tickets alike. The other
    /// tickets stay pending (ADR-0037). When the ring holds nothing
    /// published, the stall budget counts up and the drainer fences the
    /// cursor; a fence that wins returns [`SweepOutcome::Idle`] so the
    /// host re-polls.
    ///
    /// # Errors
    ///
    /// Returns the `Db` write error, or the error of the first pending
    /// ticket ([`Error::BadPayload`], a key or value that is too large).
    /// The drainer then poisons
    /// itself, and [`bad_ticket`](Drainer::bad_ticket) names the
    /// ticket. `durable` covers only the tickets
    /// acknowledged before, and the pending tickets stay (see
    /// [`resume`](Drainer::resume)). `TableFull`, `ArenaFull` and
    /// `WalFull` are not errors here: `sweep` returns
    /// `Ok(SweepOutcome::Stalled)`.
    pub async fn sweep(&mut self) -> Result<SweepOutcome, Error<D::Error>> {
        if self.poisoned {
            // The write error was already reported; stay stopped.
            return Ok(SweepOutcome::Idle);
        }

        // --- Drain phase (unless tickets are pending).
        if self.pending_len == 0 {
            while self.pending_len < MAX_WRITES {
                match self.ring.poll_drain() {
                    DrainPoll::Drained(t, p) => {
                        let i = self.pending_len;
                        self.pending_tickets[i] = t;
                        self.pending_payloads[i] = p;
                        self.pending_has_data[i] = true;
                        self.pending_len += 1;
                        self.stall_polls = 0;
                    }
                    DrainPoll::Skipped(t) => {
                        // Dead ticket: no WAL record. Buffer it with the
                        // batch — the watermark advances over the
                        // contiguous *resolved* prefix only after the
                        // batch is durable (O1 ordering: advancing early
                        // would let a later flush miss its CAS).
                        let i = self.pending_len;
                        self.pending_tickets[i] = t;
                        self.pending_has_data[i] = false;
                        self.pending_len += 1;
                        self.stall_polls = 0;
                    }
                    DrainPoll::AwaitingPublish => break,
                }
            }
        }

        if self.pending_len == 0 {
            // Nothing published. Stall budget → fence the cursor (the
            // fence refuses unclaimed head tickets; see
            // `Ring::fence_cursor`).
            self.stall_polls += 1;
            if self.stall_polls > self.stall_budget {
                self.stall_polls = 0;
                if let FenceOutcome::Fenced(t) = self.ring.fence_cursor() {
                    // The fenced ticket is dead and the cursor moved past
                    // it — no `Skipped` will ever arrive for it. The
                    // pending buffer is empty (we're in the
                    // `pending_len == 0` branch), so advancing immediately
                    // is safe: there is no batch whose flush could be
                    // reordered after this.
                    self.advance_durable(t);
                }
            }
            return Ok(SweepOutcome::Idle);
        }

        // --- Batch phase: the longest pending prefix that the `Db` takes.
        let mut take = self.pending_len;
        loop {
            let mut batch = WriteBatch::<KEY_MAX, VAL_MAX, MAX_WRITES>::new();
            let (queued, n_ops, bad) = self.fill(take, &mut batch);
            if let (Some(e), 0) = (bad, queued) {
                // The first pending ticket is bad. The pipeline stops
                // here; the tickets before it are written.
                self.poison_bad_ticket();
                return Err(e.widen());
            }
            take = queued;

            if n_ops == 0 {
                // No ticket in the prefix holds an op (fenced, skipped or
                // inverted ranges). Advance over them.
                self.ack_prefix(take);
                return Ok(SweepOutcome::Swept { tickets: take });
            }

            // --- Flush phase: one Db::write, one flush (SPEC §11).
            match self.db.write(&batch).await {
                Ok(base) => {
                    // `Db::write` assigned `base .. base + n_ops` and
                    // proved with checked arithmetic that the last fits.
                    let span = u64::try_from(n_ops).map_err(|_| Error::CounterExhausted)?;
                    let last = base.checked_add(span - 1).ok_or(Error::CounterExhausted)?;
                    self.durable_seq = last;
                    self.ack_prefix(take);
                    return Ok(SweepOutcome::Swept { tickets: take });
                }
                Err(Error::TableFull | Error::ArenaFull | Error::WalFull) => {
                    // A flush answers these. `Db::write` refused the batch
                    // before any I/O, so keep the pending tickets: `durable`
                    // does not move and nothing is lost (issue #47).
                    return Ok(SweepOutcome::Stalled);
                }
                Err(Error::BatchTooLarge { .. }) if n_ops > 1 => {
                    // Even an empty memtable cannot hold this many ops, or
                    // the WAL group is too long. `Db::write` refused it
                    // before any I/O. Write half (issue #48). `n_ops > 1`,
                    // so `take >= 2` and the half is not empty. One ring
                    // op always fits an empty memtable (ADR-0037).
                    take /= 2;
                }
                Err(e) => {
                    // Poisoned. The pending tickets stay for `resume`.
                    // `durable` covers exactly the previously acked prefix.
                    self.poisoned = true;
                    return Err(e);
                }
            }
        }
    }
}
