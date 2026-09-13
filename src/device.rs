//! The only I/O boundary: an async, poll-based block device trait.
//!
//! Poll-based rather than `async fn` in the trait so hosts can implement it
//! by hand without an executor or unstable features. Callers bridge with
//! [`core::future::poll_fn`] — still `core`-only.

use core::task::{Context, Poll};

/// A fixed-block storage device. All I/O is whole blocks.
///
/// Implementors must guarantee that `buf.len() == Self::BLOCK` on every call
/// (the crate upholds this with a compile-time assertion); anything else is
/// a caller bug.
pub trait BlockDevice {
    /// The device's own error type, passed through as [`crate::Error::Device`].
    type Error;

    /// Block size in bytes (e.g. 4096). Must be at least 512.
    const BLOCK: usize;

    /// Reads block `id` into `buf`.
    fn poll_read_block(
        &self,
        cx: &mut Context<'_>,
        id: u64,
        buf: &mut [u8],
    ) -> Poll<Result<(), Self::Error>>;

    /// Writes `buf` to block `id`.
    fn poll_write_block(
        &mut self,
        cx: &mut Context<'_>,
        id: u64,
        buf: &[u8],
    ) -> Poll<Result<(), Self::Error>>;

    /// Makes all preceding writes durable.
    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>>;
}
