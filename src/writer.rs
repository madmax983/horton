//! Writer-side multiwriter put (SPEC §8).
//!
//! [`Writer::put`] and [`put`] claim a ring ticket, publish the 32-byte
//! payload, and return a [`Put`] future that resolves when the drainer
//! advances the `durable` watermark past the ticket. Dropping the future
//! abandons *observation*, not the write — the entry was accepted and
//! will be drained and made durable.
//!
//! Use [`Writer`] (from `Drainer::writer`). It knows the `Db`'s key and
//! value limits and refuses a payload that the `Db` cannot hold before it
//! claims a ticket. A refused payload stops no other writer (issue #48).

#![forbid(unsafe_code)]

use crate::drainer::{is_ticket_durable, payload};
use crate::error::Error;
use crate::ring::{PublishOutcome, Ring};
use core::convert::Infallible;
use core::future::Future;
use core::pin::Pin;
use core::sync::atomic::AtomicU32;
use core::task::{Context, Poll};

/// A pending multiwriter put.
///
/// Resolves to the ticket once it is WAL-durable. Polls the shared
/// `durable` watermark; returns [`Poll::Pending`] until the drainer
/// advances past the ticket. While the drainer is poisoned, the
/// watermark does not move. The host calls `Drainer::resume`, or fails
/// the abandoned puts (SPEC §7).
pub struct Put<'r, const N: usize> {
    durable: &'r AtomicU32,
    ticket: u32,
}

impl<const N: usize> Put<'_, N> {
    /// The ticket this put claimed.
    #[must_use]
    pub const fn ticket(&self) -> u32 {
        self.ticket
    }
}

impl<const N: usize> Future for Put<'_, N> {
    type Output = u32;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<u32> {
        if is_ticket_durable(self.durable, self.ticket) {
            Poll::Ready(self.ticket)
        } else {
            // SPEC §8: Pending only post-acceptance, under the host's
            // re-poll contract. No waker is stored.
            Poll::Pending
        }
    }
}

/// A writer's handle to one drainer's ring and `durable` watermark.
///
/// Get it from `Drainer::writer`. It is `Copy`: give one to each writer.
/// `KEY_MAX` and `VAL_MAX` are the drainer's `Db` limits.
#[derive(Clone, Copy)]
pub struct Writer<'r, const N: usize, const KEY_MAX: usize, const VAL_MAX: usize> {
    ring: &'r Ring<N>,
    durable: &'r AtomicU32,
}

impl<'r, const N: usize, const KEY_MAX: usize, const VAL_MAX: usize>
    Writer<'r, N, KEY_MAX, VAL_MAX>
{
    pub(crate) const fn new(ring: &'r Ring<N>, durable: &'r AtomicU32) -> Self {
        Self { ring, durable }
    }

    /// Checks `payload` against the `Db`'s limits
    /// ([`payload::check`]), then does what [`put`] does.
    ///
    /// # Errors
    ///
    /// The error of [`payload::check`], before any ticket is claimed.
    /// [`Error::RingFull`] when the ring is full at claim time.
    pub fn put(&self, payload: &[u8; 32]) -> Result<Put<'r, N>, Error<Infallible>> {
        payload::check::<KEY_MAX, VAL_MAX>(payload)?;
        claim_and_publish(self.ring, self.durable, payload)
    }
}

/// Claim a ticket, publish `payload`, and return a [`Put`] future.
///
/// This does not know the `Db`'s limits. A payload that decodes but
/// that the `Db` cannot hold poisons the drainer, and every later writer
/// waits. Use [`Writer::put`] instead.
///
/// - A payload that does not decode → [`Error::BadPayload`]
///   immediately: no ticket consumed.
///
/// - Ring full at claim time → [`Error::RingFull`] immediately: no ticket
///   consumed, no sequence number consumed, safe to retry (SPEC §8).
/// - `publish` returns `Fenced` → the ticket died while claimed (the
///   writer stalled past the drainer's budget); transparently re-claim a
///   fresh ticket (§16 Q4, silent re-claim option). A writer that claims
///   and immediately publishes is never fenced.
/// - Otherwise returns the pending put; poll it to await durability.
///
/// `payload` is the 32-byte ring payload — build it with one of the
/// [`payload`] constructors: `encode_put`,
/// `encode_delete`, `encode_range_delete`, or `encode_put_ttl`.
///
/// # Errors
///
/// [`Error::BadPayload`] when `payload` does not decode.
/// [`Error::RingFull`] when the ring is full at claim time.
pub fn put<'r, const N: usize>(
    ring: &'r Ring<N>,
    durable: &'r AtomicU32,
    payload: &[u8; 32],
) -> Result<Put<'r, N>, Error<Infallible>> {
    payload::decode(payload).map_err(|_| Error::BadPayload)?;
    claim_and_publish(ring, durable, payload)
}

/// Claims a ticket and publishes `payload`. Re-claims when fenced.
fn claim_and_publish<'r, const N: usize>(
    ring: &'r Ring<N>,
    durable: &'r AtomicU32,
    payload: &[u8; 32],
) -> Result<Put<'r, N>, Error<Infallible>> {
    loop {
        let ticket = ring.try_claim().ok_or(Error::RingFull)?;
        match ring.publish(ticket, payload) {
            PublishOutcome::Published => {
                return Ok(Put { durable, ticket });
            }
            // The ticket was fenced between claim and publish (or the
            // writer stalled). Fall through and re-claim; the dead ticket
            // resolves the watermark without a WAL record.
            PublishOutcome::Fenced => {}
        }
    }
}
