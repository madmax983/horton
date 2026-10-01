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
//! `Db::write` is atomic: on success all ops are durable; on error none
//! are. On a write error the drainer:
//!
//! 1. if the error is `TableFull`/`ArenaFull`, buffers the pending
//!    tickets and returns [`SweepOutcome::Stalled`] — the host flushes
//!    the memtable (via [`Drainer::flush`]) and re-sweeps; no tickets
//!    are lost and `durable` does not advance;
//! 2. otherwise poisons: the error is returned from `sweep` and the
//!    drainer stops. The host owns failing outstanding writers —
//!    `durable` tells it exactly which tickets completed.
//!
//! The drainer stops on error (it does not retry a poisoned device). The
//! host observes the error as the `Err` from `sweep` (and
//! [`Drainer::is_poisoned`]) and is responsible for failing outstanding
//! writers.

#![forbid(unsafe_code)]

use crate::batch::WriteBatch;
use crate::db::Db;
use crate::ring::{DrainPoll, FenceOutcome, Ring, TICKET_MASK};
use crate::{BlockDevice, Error};
use core::convert::Infallible;
use core::sync::atomic::{AtomicU32, Ordering};

/// 32-byte ring payload codec.
///
/// The ring carries a fixed 32-byte payload per slot — one serialized
/// mutation (SPEC §14). Layouts, with op bytes matching [`Op`]'s
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
/// - Op discriminants are [`Op`]'s wire values; no parallel numbering to
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
    use crate::wal::Op;

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
    /// Drained and Db-acknowledged `tickets` tickets (one batch, one
    /// flush). The `durable` watermark advanced past each of them.
    Swept {
        /// Number of tickets drained and made durable in this sweep
        /// (including fenced/skipped tickets, which resolve without a
        /// WAL record).
        tickets: usize,
    },
    /// The ring held no published tickets. The drainer may have fenced a
    /// stalled cursor ticket; re-poll to make progress.
    Idle,
    /// The Db's memtable is full (`TableFull`/`ArenaFull`): the pending
    /// tickets are buffered in the drainer, `durable` did not advance,
    /// and no tickets were lost. The host should flush the memtable via
    /// [`Drainer::flush`] and re-sweep.
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
/// Tickets drained but not yet applied (because the memtable was full)
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
    /// Tickets drained but not yet Db-durable (memtable was full on the
    /// last sweep). In drain order; `pending_len` entries are valid.
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
    /// Set on the first non-capacity write error; the drainer stops
    /// afterwards. The error itself is returned from [`Drainer::sweep`] —
    /// the host owns propagating it to the writers.
    poisoned: bool,
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
            durable_seq: seq,
        }
    }

    /// Sets the poll-count stall budget (default 1024).
    pub const fn set_stall_budget(&mut self, budget: u32) {
        self.stall_budget = budget;
    }

    /// The first write error observed, if the drainer is poisoned.
    #[must_use]
    pub const fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    /// Number of tickets currently buffered (drained but not yet
    /// Db-durable because the memtable was full).
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
    pub fn snapshot(&mut self) -> Result<u64, Error<D::Error>> {
        self.db.snapshot()
    }

    /// Consumes the drainer and returns the owned [`Db`].
    #[must_use]
    pub fn into_db(
        self,
    ) -> Db<D, BLOCK, KEY_MAX, VAL_MAX, CAP, ARENA, LEVELS, TABLES, BLOOM_BYTES, CACHE> {
        self.db
    }

    /// Flushes the memtable to an `SSTable` (via the owned `Db`). The host
    /// calls this when [`sweep`](Drainer::sweep) returns
    /// [`SweepOutcome::Stalled`], then re-sweeps to apply the buffered
    /// tickets.
    ///
    /// # Errors
    ///
    /// Propagates the `Db` flush error; the drainer is not poisoned (a
    /// flush failure is independent of the pending write batch).
    pub async fn flush(&mut self) -> Result<(), Error<D::Error>> {
        self.db.flush().await
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

    /// Advance `durable` over all pending tickets, in order, then clear
    /// the pending buffer. Called only after the `Db::write` for the
    /// batch succeeded (or when the batch held no WAL-bearing payloads).
    fn ack_pending(&mut self) {
        for i in 0..self.pending_len {
            self.advance_durable(self.pending_tickets[i]);
        }
        self.pending_len = 0;
    }

    /// One drain-batch-flush-advance cycle.
    ///
    /// Drains up to `MAX_WRITES` published tickets in ticket order into
    /// the pending buffer (unless a previous sweep left pending tickets
    /// from a full memtable — those are retried first and no new tickets
    /// are drained). Builds a [`WriteBatch`]
    /// from the WAL-bearing payloads and applies it via `Db::write` (one
    /// WAL flush, memtable updated atomically), then advances `durable`
    /// over the entire contiguous resolved prefix — WAL-bearing,
    /// fenced, and skipped tickets alike. When the ring holds nothing
    /// published, the stall budget counts up and the drainer fences the
    /// cursor; a fence that wins returns [`SweepOutcome::Idle`] so the
    /// host re-polls.
    ///
    /// # Errors
    ///
    /// On `TableFull`/`ArenaFull` the pending tickets are kept and
    /// [`SweepOutcome::Stalled`] is returned (the host flushes and
    /// re-sweeps). On any other write error the drainer is poisoned:
    /// `durable` covers exactly the previously acked prefix and the
    /// error is returned. The host owns propagating it to the writers.
    pub async fn sweep(&mut self) -> Result<SweepOutcome, Error<D::Error>> {
        if self.poisoned {
            // The write error was already reported; stay stopped.
            return Ok(SweepOutcome::Idle);
        }

        // --- Drain phase (unless retrying pending from a full memtable).
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

        // --- Batch phase: pending payloads → WriteBatch.
        let mut batch = WriteBatch::<KEY_MAX, VAL_MAX, MAX_WRITES>::new();
        let mut n_ops = 0usize;
        for i in 0..self.pending_len {
            if !self.pending_has_data[i] {
                continue;
            }
            // Queue the decoded mutation. The borrow of the pending
            // payload ends with this match — `bool` and
            // `Error<Infallible>` own their data — so poisoning `self`
            // below is borrowck-clean.
            let queued: Result<bool, Error<Infallible>> =
                match payload::decode(&self.pending_payloads[i]) {
                    Ok(payload::Decoded::Put { key, val }) => batch.put(key, val).map(|()| true),
                    Ok(payload::Decoded::Delete { key }) => batch.delete(key).map(|()| true),
                    Ok(payload::Decoded::RangeDelete { start, end }) => {
                        if start < end {
                            batch.range_delete(start, end).map(|()| true)
                        } else {
                            // Inverted range: applied no-op (mirrors
                            // `Db::delete_range`). The ticket resolves;
                            // no op is queued and no sequence number is
                            // consumed.
                            Ok(false)
                        }
                    }
                    Ok(payload::Decoded::PutTtl {
                        key,
                        val,
                        expire_at,
                    }) => batch.put_ttl(key, val, expire_at).map(|()| true),
                    Err(_) => {
                        // Malformed payload: never silently reinterpret
                        // as a valid mutation. Poison — the host must
                        // intervene.
                        self.poisoned = true;
                        return Err(Error::BadPayload);
                    }
                };
            match queued {
                Ok(true) => n_ops += 1,
                Ok(false) => {}
                Err(e) => {
                    // Oversized for the Db's KEY_MAX/VAL_MAX, or a logic
                    // bug (batch overfull — unreachable: at most
                    // MAX_WRITES ops in a MAX_WRITES-capacity batch).
                    // Configuration error, not a device error: poison —
                    // the host must intervene. `widen` keeps whatever
                    // variant the batch reported.
                    self.poisoned = true;
                    return Err(e.widen());
                }
            }
        }

        if n_ops == 0 {
            // All pending tickets were fenced/skipped: no WAL records
            // needed. Advance the watermark over the resolved prefix.
            let tickets = self.pending_len;
            self.ack_pending();
            return Ok(SweepOutcome::Swept { tickets });
        }

        // --- Flush phase: one Db::write, one flush (SPEC §11).
        match self.db.write(&batch).await {
            Ok(base) => {
                let tickets = self.pending_len;
                // `Db::write` assigned `base .. base + n_ops`; the batch's
                // highest seqnum is now durable. `n_ops > 0` here (the
                // all-skipped case returned above), and `Db::write`
                // already proved `base + (n_ops - 1)` fits with checked
                // arithmetic — `CounterExhausted` is unreachable.
                let span = u64::try_from(n_ops).map_err(|_| Error::CounterExhausted)?;
                let last = base.checked_add(span - 1).ok_or(Error::CounterExhausted)?;
                self.durable_seq = last;
                self.ack_pending();
                Ok(SweepOutcome::Swept { tickets })
            }
            Err(Error::TableFull | Error::ArenaFull) => {
                // Memtable full: keep the pending tickets buffered,
                // `durable` does not advance, nothing is lost. The host
                // flushes via `Drainer::flush` and re-sweeps.
                Ok(SweepOutcome::Stalled)
            }
            Err(e) => {
                // Poisoned: the host must fail the outstanding writers.
                // `durable` covers exactly the previously acked prefix.
                self.poisoned = true;
                Err(e)
            }
        }
    }
}
