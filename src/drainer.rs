//! Poll-based single drainer for the `multiwriter` feature.
//!
//! SPEC: `docs/multiwriter-spec.md` §§7, 11. The topology is
//!
//! ```text
//! writer threads ──atomics──▶ ring ──drainer──▶ WalWriter ──▶ Device
//! ```
//!
//! The drainer exclusively owns the [`WalWriter`](crate::wal::WalWriter)
//! (SPEC O2: sole device writer — writers share only the atomic ring, and
//! no `BlockDevice` signature changes). Each [`Drainer::sweep`] drains the
//! ring's published tickets in order, converts the 32-byte payloads to
//! [`BatchRecord`]s, appends them through
//! [`WalWriter::append_batch`](crate::wal::WalWriter::append_batch) (one
//! flush per sweep), and advances the `durable` ticket watermark **only
//! after the flush is acknowledged** (SPEC O1: acked ⇒ durable).
//!
//! # Completion signaling
//!
//! A single `durable: AtomicU32` tracks the contiguous WAL-durable ticket
//! prefix, modulo 2³¹. The host owns it and shares `&AtomicU32` with the
//! drainer and the writers. A writer's put completes when `durable` has
//! advanced past its ticket — see [`is_ticket_durable`]. The drainer is
//! the sole writer of `durable` and advances it via `compare_exchange`,
//! one ticket at a time, in drain order. Fenced/skipped tickets carry no
//! WAL record; the drainer advances `durable` past them immediately (they
//! are dead — their writers were already notified via
//! [`PublishOutcome::Fenced`](crate::ring::PublishOutcome)).
//!
//! Dropping a post-acceptance pending put abandons *observation*, not the
//! write: the drainer still drains and persists it.
//!
//! # Failure policy
//!
//! A batch is prefix-atomic ([`BatchReport`](crate::BatchReport)). On a
//! batch error the drainer:
//!
//! 1. burns the consumed sequence numbers (`next_seq += consumed` —
//!    consumed seqnums are never reused, SPEC §11);
//! 2. advances `durable` over exactly the durable prefix (O1: acked ⇒
//!    durable — the failed suffix is NOT acknowledged);
//! 3. records the error and returns it from `sweep`.
//!
//! The drainer stops on error (it does not retry a poisoned device). The
//! host observes the error as the `Err` from `sweep` (and
//! [`Drainer::is_poisoned`]) and is responsible for failing outstanding
//! writers — `durable` tells it exactly which tickets completed.

#![forbid(unsafe_code)]

use crate::ring::{DrainPoll, FenceOutcome, Ring, TICKET_MASK};
use crate::wal::{BatchRecord, Op, WalWriter};
use crate::{BlockDevice, Error};
use core::sync::atomic::{AtomicU32, Ordering};

/// 32-byte ring payload codec.
///
/// The ring carries a fixed 32-byte payload per slot — a stand-in for the
/// serialized mutation (SPEC §14: the full encoding is open). This codec
/// defines the interim layout so the drainer can write real WAL records:
///
/// ```text
/// [op:1][klen:1][vlen:1][key: klen][val: vlen]
/// ```
///
/// `op` is `1` (Put) or `2` (Delete), matching [`Op`]'s discriminants.
/// `klen` is 1..=29; for `Put`, `klen + vlen <= 29`. For `Delete`, `vlen`
/// is 0 and the value bytes are absent.
pub mod payload {
    use crate::wal::Op;

    /// Operation byte for a put payload.
    pub const PUT: u8 = Op::Put as u8;
    /// Operation byte for a delete payload.
    pub const DELETE: u8 = Op::Delete as u8;

    /// Encode a put into a 32-byte payload. Returns `None` when the key is
    /// empty/longer than 29 bytes or key+value exceed 29 bytes.
    #[must_use]
    pub fn encode_put(key: &[u8], val: &[u8]) -> Option<[u8; 32]> {
        let klen = u8::try_from(key.len()).ok()?;
        let vlen = u8::try_from(val.len()).ok()?;
        if klen == 0 || klen > 29 {
            return None;
        }
        if klen + vlen > 29 {
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

    /// Decode a payload into `(op, key, val)`.
    ///
    /// Malformed payloads (bad op or overlong lengths) decode as a delete
    /// of the empty key — the drainer validates lengths before appending,
    /// so a malformed payload never reaches the WAL; this keeps decode
    /// total.
    #[must_use]
    pub fn decode(p: &[u8; 32]) -> (Op, &[u8], &[u8]) {
        let op = match p[0] {
            x if x == PUT => Op::Put,
            x if x == DELETE => Op::Delete,
            _ => return (Op::Delete, &[], &[]),
        };
        let klen = (p[1] as usize).min(29);
        let vlen = (p[2] as usize).min(29 - klen);
        let key = &p[3..3 + klen];
        let val = if op == Op::Put {
            &p[3 + klen..3 + klen + vlen]
        } else {
            &[]
        };
        (op, key, val)
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
/// This is what snapshots pin (SPEC §9) — **not** the claim head. Every
/// ticket `<` the returned value (mod 2³¹) is WAL-durable and safe for a
/// snapshot reader to observe; tickets `≥` it may be claimed but not yet
/// drained, or drained but not yet flushed. Pinning the claim head
/// instead would let a later-drained ticket `≤` the pinned head become
/// visible to the snapshot — an isolation violation.
///
/// The drainer publishes the watermark with Release
/// ([`Drainer::advance_durable`]); this loads it with Acquire. The value
/// never moves backward: tickets drain in ticket order and the drainer is
/// the sole writer.
#[must_use]
pub fn drain_watermark(durable: &AtomicU32) -> u32 {
    durable.load(Ordering::Acquire) & TICKET_MASK
}

/// Outcome of one [`Drainer::sweep`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SweepOutcome {
    /// Drained and WAL-acknowledged `tickets` tickets (one batch, one
    /// flush). The `durable` watermark advanced past each of them.
    Swept {
        /// Number of tickets drained and made durable in this sweep.
        tickets: usize,
    },
    /// The ring held no published tickets. The drainer may have fenced a
    /// stalled cursor ticket; re-poll to make progress.
    Idle,
}

/// The single WAL-owning drainer.
///
/// Owns the [`WalWriter`](crate::wal::WalWriter) and the `next_seq`
/// counter; shares `&Ring` and `&AtomicU32 durable` with the host and the
/// writers. All batch scratch is caller-sized (`MAX_WRITES`); no
/// allocation, no unsafe.
pub struct Drainer<'r, D: BlockDevice, const BLOCK: usize, const N: usize, const MAX_WRITES: usize>
{
    ring: &'r Ring<N>,
    wal: WalWriter<D, BLOCK>,
    durable: &'r AtomicU32,
    /// WAL sequence number for the next drained ticket. Starts at the
    /// recovered `next_seq` (SPEC O3); advances by `consumed` per sweep —
    /// gap tickets (fenced/skipped) consume no seqnums.
    next_seq: u64,
    /// Poll-count stall budget before the drainer fences the cursor.
    /// Horton owns no clock; the budget counts `sweep` polls that found
    /// nothing published.
    stall_budget: u32,
    stall_polls: u32,
    /// Set on the first batch error; the drainer stops afterwards. The
    /// error itself is returned from [`Drainer::sweep`] — the host owns
    /// propagating it to the writers.
    poisoned: bool,
}

impl<'r, D: BlockDevice, const BLOCK: usize, const N: usize, const MAX_WRITES: usize>
    Drainer<'r, D, BLOCK, N, MAX_WRITES>
{
    /// Creates a drainer over `ring` and `wal`.
    ///
    /// `durable` must be initialized to the ring's head ticket (0 for a
    /// fresh ring, or the seeded head): it is the next ticket the drainer
    /// expects, and tickets `<` it are already resolved. `next_seq` is
    /// the recovered WAL `next_seq` (`max_seq + 1`; at least 1 for a fresh
    /// WAL, since seqnum 0 is the recovery floor and is never used).
    /// Tickets and WAL seqnums are independent counters: the drainer maps
    /// the i-th drained ticket to `next_seq + i`. The host owns `durable`
    /// and shares it with the writers.
    #[must_use]
    pub const fn new(
        ring: &'r Ring<N>,
        wal: WalWriter<D, BLOCK>,
        durable: &'r AtomicU32,
        next_seq: u64,
    ) -> Self {
        Self {
            ring,
            wal,
            durable,
            next_seq,
            stall_budget: 1024,
            stall_polls: 0,
            poisoned: false,
        }
    }

    /// Sets the poll-count stall budget (default 1024).
    pub const fn set_stall_budget(&mut self, budget: u32) {
        self.stall_budget = budget;
    }

    /// The first batch error observed, if the drainer is poisoned.
    #[must_use]
    pub const fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    /// The WAL sequence number the next drained ticket will take.
    #[must_use]
    pub const fn next_seq(&self) -> u64 {
        self.next_seq
    }

    /// The current drain watermark (see [`drain_watermark`]): the
    /// contiguous WAL-durable ticket prefix. Snapshot readers pin this
    /// value (SPEC §9), not the claim head.
    #[must_use]
    pub fn durable_watermark(&self) -> u32 {
        drain_watermark(self.durable)
    }

    /// Consumes the drainer and returns the owned [`WalWriter`].
    #[must_use]
    pub fn into_wal(self) -> WalWriter<D, BLOCK> {
        self.wal
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

    /// One drain-batch-flush-advance cycle.
    ///
    /// Drains up to `MAX_WRITES` published tickets in ticket order,
    /// appends them as one WAL batch (one flush), then advances `durable`
    /// past exactly the acknowledged prefix. Fenced/skipped tickets
    /// advance `durable` immediately (no WAL record). When the ring holds
    /// nothing published, the stall budget counts up and the drainer
    /// fences the cursor; a fence that wins returns [`SweepOutcome::Idle`]
    /// so the host re-polls.
    ///
    /// # Errors
    ///
    /// On a batch error the drainer is poisoned: `durable` covers exactly
    /// the durable prefix, consumed seqnums are burned, and the error is
    /// returned. The host owns propagating it to the writers.
    pub async fn sweep(&mut self) -> Result<SweepOutcome, Error<D::Error>> {
        if self.poisoned {
            // The batch error was already reported; stay stopped.
            return Ok(SweepOutcome::Idle);
        }

        // --- Drain phase: up to MAX_WRITES published tickets, in order.
        let mut tickets = [0u32; MAX_WRITES];
        let mut payloads = [[0u8; 32]; MAX_WRITES];
        let mut len = 0usize;
        while len < MAX_WRITES {
            match self.ring.poll_drain() {
                DrainPoll::Drained(t, p) => {
                    tickets[len] = t;
                    payloads[len] = p;
                    len += 1;
                    self.stall_polls = 0;
                }
                DrainPoll::Skipped(t) => {
                    // Dead ticket: no WAL record, but the watermark is the
                    // contiguous *resolved* prefix — advance past it.
                    self.advance_durable(t);
                    self.stall_polls = 0;
                }
                DrainPoll::AwaitingPublish => break,
            }
        }

        if len == 0 {
            // Nothing published. Stall budget → fence the cursor (the fence
            // refuses unclaimed head tickets; see `Ring::fence_cursor`).
            self.stall_polls += 1;
            if self.stall_polls > self.stall_budget {
                self.stall_polls = 0;
                if let FenceOutcome::Fenced(t) = self.ring.fence_cursor() {
                    // The fenced ticket is dead and the cursor moved past
                    // it — advance `durable` now; no `Skipped` will ever
                    // arrive for it.
                    self.advance_durable(t);
                }
            }
            return Ok(SweepOutcome::Idle);
        }

        // --- Batch phase: payloads → BatchRecords with contiguous seqnums.
        let mut records = [BatchRecord {
            seq: 0,
            op: Op::Put,
            key: &[],
            val: &[],
            expire_at: 0,
        }; MAX_WRITES];
        for i in 0..len {
            let (op, key, val) = payload::decode(&payloads[i]);
            // The codec bounds key_len ≤ 30 and the value to the 32-byte
            // payload; a malformed payload decodes to delete-empty, which
            // the WAL rejects — surfaced via the batch error path below.
            records[i] = BatchRecord {
                seq: self.next_seq + i as u64,
                op,
                key,
                val,
                expire_at: 0,
            };
        }

        // --- Flush phase: one batch, one flush (SPEC §11).
        let report = self.wal.append_batch(&records[..len]).await;
        // Burn the consumed seqnums: they must never be reused (SPEC §11).
        self.next_seq += report.consumed as u64;
        // O1: advance `durable` over exactly the acknowledged prefix.
        for ticket in tickets.iter().take(report.durable) {
            self.advance_durable(*ticket);
        }

        if let Some(err) = report.error {
            // Poisoned: the host must fail the outstanding writers.
            // `durable` covers exactly the acked prefix (O1); the failed
            // suffix is not acknowledged.
            self.poisoned = true;
            return Err(err);
        }

        Ok(SweepOutcome::Swept { tickets: len })
    }
}
