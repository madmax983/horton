//! Writer-side multiwriter put (SPEC §8).
//!
//! [`put`] claims a ring ticket, publishes the 32-byte payload, and
//! returns a [`Put`] future that resolves when the drainer advances the
//! `durable` watermark past the ticket. Dropping the future abandons
//! *observation*, not the write — the entry was accepted and will be
//! drained and made durable.

#![forbid(unsafe_code)]

use crate::drainer::is_ticket_durable;
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
/// advances past the ticket. If the drainer is poisoned the watermark
/// never advances — the host owns failing abandoned puts (SPEC §7).
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

/// Claim a ticket, publish `payload`, and return a [`Put`] future.
///
/// - Ring full at claim time → [`Error::NoSpace`] immediately: no ticket
///   consumed, no sequence number consumed, safe to retry (SPEC §8).
/// - `publish` returns `Fenced` → the ticket died while claimed (the
///   writer stalled past the drainer's budget); transparently re-claim a
///   fresh ticket (§16 Q4, silent re-claim option). A writer that claims
///   and immediately publishes is never fenced.
/// - Otherwise returns the pending put; poll it to await durability.
///
/// `payload` is the 32-byte ring payload — build it with
/// [`payload::encode_put`](crate::drainer::payload::encode_put) or
/// [`payload::encode_delete`](crate::drainer::payload::encode_delete).
///
/// # Errors
///
/// [`Error::NoSpace`] when the ring is full at claim time.
pub fn put<'r, const N: usize>(
    ring: &'r Ring<N>,
    durable: &'r AtomicU32,
    payload: &[u8; 32],
) -> Result<Put<'r, N>, Error<Infallible>> {
    loop {
        let ticket = ring.try_claim().ok_or(Error::NoSpace)?;
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
