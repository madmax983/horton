//! Wire frames: ascending entries packed for a narrow link.
//!
//! [`fingerprint_buckets`](crate::fingerprint::fingerprint_buckets) finds
//! the key range where a device and its replica disagree, and a
//! [`Diff`](crate::diff::Diff) names the exact entries — but both ends of
//! a diff have to be [`SortedEntries`] on the same machine. A frame is
//! how one side gets there: the device packs a [`Scan`](crate::Scan)'s
//! output into a caller-owned buffer with a [`FrameWriter`], sends the
//! bytes, and the receiver wraps them in a [`FrameReader`], which *is* a
//! [`SortedEntries`] source. No database is needed on the far end, and
//! the received bytes are never copied into one first.
//!
//! The format is small and checked:
//!
//! ```text
//! "HF" version(1)  entry*  crc32-le(4)
//! entry = shared  suffix_len  val_len  suffix  val      (LEB128 lengths)
//! ```
//!
//! Keys are front-coded: each stores only the bytes after the prefix it
//! shares with the previous key, which for hierarchical keys
//! (`"sensor/00041"`, `"sensor/00042"`) is most of them. The CRC-32 seals
//! the header and every entry, so a [`FrameReader`] refuses a damaged
//! frame ([`Error::CorruptFrame`]) before yielding anything from it, and
//! it re-checks the ascending order a [`Diff`](crate::diff::Diff) relies
//! on rather than trusting the sender.

use core::convert::Infallible;
use core::future::Future;
use core::marker::PhantomData;

use crate::crc::crc32;
use crate::diff::SortedEntries;
use crate::error::Error;

/// Bytes a frame spends outside its entries: a three-byte header and a
/// four-byte CRC. An empty frame is exactly this long.
pub const FRAME_OVERHEAD: usize = HEADER.len() + CRC_LEN;

/// Magic and format version. A frame of any other version is refused.
const HEADER: [u8; 3] = [b'H', b'F', 1];
const CRC_LEN: usize = 4;

/// Packs strictly ascending entries into a caller-owned buffer.
///
/// `KEY_MAX` bounds the keys it accepts (it keeps the previous key inline
/// for front coding). A refused [`push`](Self::push) leaves the frame as
/// it was, so a full frame can be [`finish`](Self::finish)ed and the
/// entry pushed into the next one.
pub struct FrameWriter<'b, const KEY_MAX: usize> {
    buf: &'b mut [u8],
    len: usize,
    count: usize,
    last: [u8; KEY_MAX],
    last_len: usize,
}

impl<'b, const KEY_MAX: usize> FrameWriter<'b, KEY_MAX> {
    /// Starts an empty frame in `buf`. A frame can never grow past
    /// `buf.len()` bytes, CRC included.
    ///
    /// # Errors
    ///
    /// [`Error::BufferTooSmall`] when `buf` cannot hold even an empty
    /// frame ([`FRAME_OVERHEAD`] bytes).
    pub fn new(buf: &'b mut [u8]) -> Result<Self, Error<Infallible>> {
        if buf.len() < FRAME_OVERHEAD {
            return Err(Error::BufferTooSmall {
                need: FRAME_OVERHEAD,
            });
        }
        buf[..HEADER.len()].copy_from_slice(&HEADER);
        Ok(Self {
            buf,
            len: HEADER.len(),
            count: 0,
            last: [0; KEY_MAX],
            last_len: 0,
        })
    }

    /// Entries pushed so far.
    #[must_use]
    pub const fn count(&self) -> usize {
        self.count
    }

    /// Whether no entry has been pushed.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// The length the frame would have if finished now, CRC included.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len + CRC_LEN
    }

    /// Appends one entry. Its key must be above every key already in the
    /// frame.
    ///
    /// # Errors
    ///
    /// [`Error::EmptyKey`], [`Error::KeyTooLarge`] past `KEY_MAX`,
    /// [`Error::BadConfig`] when `key` is not above the previous key, or
    /// [`Error::BufferTooSmall`] (with the buffer length the frame would
    /// need) when the entry does not fit. The frame is unchanged by any
    /// of them.
    pub fn push(&mut self, key: &[u8], val: &[u8]) -> Result<(), Error<Infallible>> {
        if key.is_empty() {
            return Err(Error::EmptyKey);
        }
        if key.len() > KEY_MAX {
            return Err(Error::KeyTooLarge {
                len: key.len(),
                max: KEY_MAX,
            });
        }
        let last = &self.last[..self.last_len];
        if self.count > 0 && key <= last {
            return Err(Error::BadConfig);
        }
        let shared = key.iter().zip(last).take_while(|(a, b)| a == b).count();
        let suffix = &key[shared..];
        let size = varint_len(shared)
            + varint_len(suffix.len())
            + varint_len(val.len())
            + suffix.len()
            + val.len();
        let need = self.len + size + CRC_LEN;
        if need > self.buf.len() {
            return Err(Error::BufferTooSmall { need });
        }
        let mut at = self.len;
        at = put_varint(self.buf, at, shared);
        at = put_varint(self.buf, at, suffix.len());
        at = put_varint(self.buf, at, val.len());
        self.buf[at..at + suffix.len()].copy_from_slice(suffix);
        at += suffix.len();
        self.buf[at..at + val.len()].copy_from_slice(val);
        self.len = at + val.len();
        self.last[shared..key.len()].copy_from_slice(suffix);
        self.last_len = key.len();
        self.count += 1;
        Ok(())
    }

    /// Pushes entries from `source` until it ends or the frame fills.
    ///
    /// Returns `Ok(None)` when the source is exhausted. Returns
    /// `Ok(Some((key_len, val_len)))` when the frame is full: that entry
    /// was read but not pushed, and is still in `key_buf` and `val_buf`.
    /// Push it into the next frame before calling `fill` again, which
    /// overwrites the buffers.
    ///
    /// # Errors
    ///
    /// Whatever `source` reports; [`Error::BufferTooSmall`] when an entry
    /// does not fit even an otherwise empty frame (no frame of this size
    /// can carry it); and [`push`](Self::push)'s other errors when the
    /// source yields an entry this writer refuses.
    pub async fn fill<S, E>(
        &mut self,
        source: &mut S,
        key_buf: &mut [u8],
        val_buf: &mut [u8],
    ) -> Result<Option<(usize, usize)>, Error<E>>
    where
        S: SortedEntries<Error = Error<E>>,
    {
        while let Some((k, v)) = source.next_entry(key_buf, val_buf).await? {
            match self.push(&key_buf[..k], &val_buf[..v]) {
                Ok(()) => {}
                Err(Error::BufferTooSmall { .. }) if self.count > 0 => return Ok(Some((k, v))),
                Err(e) => return Err(e.widen()),
            }
        }
        Ok(None)
    }

    /// Seals the frame with its CRC and returns its bytes, a prefix of
    /// the buffer passed to [`new`](Self::new).
    #[must_use]
    pub fn finish(self) -> &'b [u8] {
        let buf = self.buf;
        let crc = crc32(&buf[..self.len]);
        buf[self.len..self.len + CRC_LEN].copy_from_slice(&crc.to_le_bytes());
        &buf[..self.len + CRC_LEN]
    }
}

/// Reads a frame back as a [`SortedEntries`] source.
///
/// `E` is the error type of the other side of the
/// [`Diff`](crate::diff::Diff) it will join (a [`Scan`](crate::Scan)'s
/// device error); a frame itself never touches a device. `KEY_MAX`
/// bounds the keys it accepts. Fused: once exhausted it keeps returning
/// `Ok(None)`.
pub struct FrameReader<'a, E, const KEY_MAX: usize> {
    body: &'a [u8],
    pos: usize,
    last: [u8; KEY_MAX],
    last_len: usize,
    _err: PhantomData<fn() -> E>,
}

impl<'a, E, const KEY_MAX: usize> FrameReader<'a, E, KEY_MAX> {
    /// Checks `frame`'s header and CRC.
    ///
    /// # Errors
    ///
    /// [`Error::CorruptFrame`] when the frame is truncated, has another
    /// magic or version, or fails its CRC.
    pub fn new(frame: &'a [u8]) -> Result<Self, Error<E>> {
        if frame.len() < FRAME_OVERHEAD || frame[..HEADER.len()] != HEADER {
            return Err(Error::CorruptFrame { offset: 0 });
        }
        let end = frame.len() - CRC_LEN;
        let mut stored = [0u8; CRC_LEN];
        stored.copy_from_slice(&frame[end..]);
        if crc32(&frame[..end]) != u32::from_le_bytes(stored) {
            return Err(Error::CorruptFrame { offset: end });
        }
        Ok(Self {
            body: &frame[HEADER.len()..end],
            pos: 0,
            last: [0; KEY_MAX],
            last_len: 0,
            _err: PhantomData,
        })
    }

    /// Decodes the next entry; see [`SortedEntries::next_entry`].
    fn next_sync(
        &mut self,
        key_buf: &mut [u8],
        val_buf: &mut [u8],
    ) -> Result<Option<(usize, usize)>, Error<E>> {
        if self.pos == self.body.len() {
            return Ok(None);
        }
        let offset = HEADER.len() + self.pos;
        let corrupt = || Error::CorruptFrame { offset };
        let mut at = self.pos;
        let (shared, suffix_len, val_len);
        (shared, at) = get_varint(self.body, at).ok_or_else(corrupt)?;
        (suffix_len, at) = get_varint(self.body, at).ok_or_else(corrupt)?;
        (val_len, at) = get_varint(self.body, at).ok_or_else(corrupt)?;
        let suffix_end = at.checked_add(suffix_len).ok_or_else(corrupt)?;
        let val_end = suffix_end.checked_add(val_len).ok_or_else(corrupt)?;
        if shared > self.last_len || suffix_len == 0 || val_end > self.body.len() {
            return Err(corrupt());
        }
        let suffix = &self.body[at..suffix_end];
        // The key is `last[..shared] ++ suffix`; it is above `last` exactly
        // when `suffix` is above the rest of `last`.
        if self.pos > 0 && suffix <= &self.last[shared..self.last_len] {
            return Err(corrupt());
        }
        let key_len = shared + suffix_len;
        if key_len > KEY_MAX {
            return Err(Error::KeyTooLarge {
                len: key_len,
                max: KEY_MAX,
            });
        }
        if key_buf.len() < key_len {
            return Err(Error::BufferTooSmall { need: key_len });
        }
        if val_buf.len() < val_len {
            return Err(Error::BufferTooSmall { need: val_len });
        }
        self.last[shared..key_len].copy_from_slice(suffix);
        self.last_len = key_len;
        key_buf[..key_len].copy_from_slice(&self.last[..key_len]);
        val_buf[..val_len].copy_from_slice(&self.body[suffix_end..val_end]);
        self.pos = val_end;
        Ok(Some((key_len, val_len)))
    }
}

impl<E, const KEY_MAX: usize> SortedEntries for FrameReader<'_, E, KEY_MAX> {
    type Error = Error<E>;

    /// Decodes the next entry. A refused entry ([`Error::CorruptFrame`]
    /// for a body that does not decode or is out of order,
    /// [`Error::KeyTooLarge`], [`Error::BufferTooSmall`]) leaves the
    /// reader where it was.
    fn next_entry(
        &mut self,
        key_buf: &mut [u8],
        val_buf: &mut [u8],
    ) -> impl Future<Output = Result<Option<(usize, usize)>, Self::Error>> {
        core::future::ready(self.next_sync(key_buf, val_buf))
    }
}

/// Bytes LEB128 spends on `n`.
const fn varint_len(mut n: usize) -> usize {
    let mut len = 1;
    while n >= 0x80 {
        n >>= 7;
        len += 1;
    }
    len
}

/// Writes `n` as LEB128 at `buf[at..]`; returns the end. The caller has
/// checked the room.
fn put_varint(buf: &mut [u8], mut at: usize, mut n: usize) -> usize {
    while n >= 0x80 {
        // Low seven bits plus the continuation bit: always below 256.
        #[allow(clippy::cast_possible_truncation)]
        let b = (n as u8) | 0x80;
        buf[at] = b;
        n >>= 7;
        at += 1;
    }
    // `n < 0x80` here.
    #[allow(clippy::cast_possible_truncation)]
    let b = n as u8;
    buf[at] = b;
    at + 1
}

/// Reads a LEB128 `usize` at `buf[at..]`; `None` when it runs off the
/// end or overflows.
fn get_varint(buf: &[u8], mut at: usize) -> Option<(usize, usize)> {
    let mut n: usize = 0;
    let mut shift = 0u32;
    loop {
        let b = *buf.get(at)?;
        at += 1;
        let part = usize::from(b & 0x7f);
        if shift >= usize::BITS || (part << shift) >> shift != part {
            return None;
        }
        n |= part << shift;
        if b & 0x80 == 0 {
            return Some((n, at));
        }
        shift += 7;
    }
}
