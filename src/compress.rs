//! v0.13: hand-rolled LZ77 block compression.
//!
//! A tiny, `no_std`, allocation-free `LZ77` codec for `SSTable` data blocks.
//! There are no panics on any input: the encoder is total over slices,
//! and the decoder bounds-checks every read and write, returning
//! [`DecompressError`] on malformed streams instead of panicking.
//!
//! # Stream format
//!
//! The stream is a sequence of tokens, each laid out in this order:
//! tag byte, literal-length extension bytes (if the high nibble is 15),
//! the literals themselves, match-length extension bytes (if the low
//! nibble is 15), and finally — when the match length is non-zero — a
//! 2-byte little-endian match offset.
//!
//! Each token starts with one tag byte: the high nibble is the
//! literal-run length (0–15), the low nibble is the match length minus
//! `MIN_MATCH` (0–15, so matches are 4–19 bytes before extension). A
//! nibble value of 15 means "15, then extension bytes follow": each
//! `0xFF` extension byte adds 255, and the first non-`0xFF` byte adds
//! its value (LZ4-style). The match offset is 1–32768, a distance back
//! from the current output position.
//!
//! The stream encodes exactly one decompressed block; the decoder stops
//! after emitting the full output buffer. Trailing bytes are ignored.
//!
//! # Why LZ77 here
//!
//! `SSTable` data blocks hold sorted keys with shared prefixes and often
//! repetitive values — exactly what LZ77 eats. The 32 KiB window covers
//! a whole block for `BLOCK <= 32768` (every profile here; larger blocks
//! still compress, with matches reaching back at most 32 KiB), each
//! position probes a single hash bucket (no chains: bounded encode time,
//! 4 KiB table), and both directions run in a single pass over
//! caller-owned buffers.

/// Minimum bytes a compressor-emitted block must save before the writer
/// keeps the compressed form.
///
/// Below this, the block is stored raw: the flag bit, the CPU time, and
/// the (tiny) space win are not worth it.
pub const COMPRESS_MIN_SAVING: usize = 128;

/// Minimum match length the encoder emits.
const MIN_MATCH: usize = 4;
/// Hash-table width: 2^10 single-entry buckets of `u32` (4 KiB). No
/// chains: each position probes only the most recent same-hash position,
/// which bounds encode time and keeps the table small.
const HASH_BITS: u32 = 10;
const HASH_SIZE: usize = 1 << HASH_BITS;
/// Longest match the encoder will emit for one token pair; the
/// extension bytes could describe more, but one 64 KiB cap keeps the
/// per-token work bounded.
const MAX_MATCH: usize = 1 << 16;

/// Reasons a compressed stream can be rejected by [`decompress`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecompressError {
    /// The stream ended mid-token.
    Truncated,
    /// A tag, length, or offset was structurally invalid (e.g. a match
    /// offset of zero, or a match reaching before the output start).
    Invalid,
}

impl core::fmt::Display for DecompressError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Truncated => write!(f, "truncated compressed stream"),
            Self::Invalid => write!(f, "invalid compressed stream"),
        }
    }
}

/// Caller-owned scratch for one compression pass.
///
/// The hash table and the compressed-output staging both live here, so
/// compression allocates nothing: the caller (flush, compaction)
/// owns a `CompressScratch<BLOCK>` for the duration of the job.
///
/// Note: `BLOCK` here sizes the output staging (`BLOCK - 4` usable).
/// The logical block passed to [`CompressScratch::compress`] must be
/// exactly `BLOCK - 4` bytes — the full data-block body the writer
/// seals (entries, zero fill, restart trailer).
pub struct CompressScratch<const BLOCK: usize> {
    /// Last-seen position per hash bucket, `u32::MAX` = empty.
    head: [u32; HASH_SIZE],
    /// Staging for the compressed stream; valid up to `out_len`.
    out: [u8; BLOCK],
    out_len: usize,
}

impl<const BLOCK: usize> Default for CompressScratch<BLOCK> {
    /// Same as [`new`](Self::new).
    fn default() -> Self {
        Self::new()
    }
}

impl<const BLOCK: usize> CompressScratch<BLOCK> {
    /// Creates zeroed scratch. `const` so callers can park it in a
    /// struct or on the stack without runtime initialization cost.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            head: [u32::MAX; HASH_SIZE],
            out: [0u8; BLOCK],
            out_len: 0,
        }
    }

    /// The compressed bytes of the last successful [`compress`](Self::compress)
    /// call: `&self.compressed()[..len]` where `len` is the returned length.
    #[must_use]
    pub fn compressed(&self) -> &[u8] {
        &self.out[..self.out_len]
    }

    /// Compresses `src` (exactly `BLOCK - 4` bytes) into the staging
    /// buffer.
    ///
    /// Returns `Some(clen)` when the encoding is at least
    /// [`COMPRESS_MIN_SAVING`] bytes smaller than the input **and** fits
    /// the 15-bit length field of the block trailer flag; the bytes are
    /// then available via [`compressed`](Self::compressed). Returns
    /// `None` when the block should be stored raw. Never panics.
    pub fn compress(&mut self, src: &[u8]) -> Option<usize> {
        let body = BLOCK.checked_sub(4)?;
        if src.len() != body {
            return None;
        }
        self.head = [u32::MAX; HASH_SIZE];
        self.out_len = 0;

        let mut ip = 0usize; // input position
        let mut anchor = 0usize; // start of pending literals
        while ip < body {
            let (mpos, mlen, h) = self.find_match(src, ip, body);
            if mlen >= MIN_MATCH {
                let lit_len = ip - anchor;
                // Encoded match length E = mlen - MIN_MATCH; the nibble
                // carries min(E, 15) and the extension the rest.
                let e = mlen - MIN_MATCH;
                if !self.emit_token(lit_len, e.min(15)) {
                    return None;
                }
                // Token order: literal extension, literals, match
                // extension, offset (the decoder reads them in this
                // order, and stops after the literals when the output
                // is full — so the trailing all-literal token needs no
                // match part at all).
                if !self.emit_ext(lit_len) {
                    return None;
                }
                if !self.emit_literals(src, anchor, ip) {
                    return None;
                }
                if !self.emit_ext(e) {
                    return None;
                }
                // Offset: distance back from the current output position
                // to the match start.
                let offset = ip - mpos;
                if !self.emit_u16(offset) {
                    return None;
                }
                // Index the bytes the match skipped so later positions
                // can match into them (one pass, no re-scan).
                let end = (ip + mlen).min(body);
                let mut p = ip + 1;
                while p + MIN_MATCH <= end {
                    self.insert(src, p);
                    p += 1;
                }
                ip = end;
                anchor = ip;
            } else {
                // `find_match` already hashed `ip` to probe the table; on
                // this miss, record `ip` there without hashing it again.
                // `h == HASH_SIZE` marks the near-tail case where it
                // couldn't hash at all (mirrors `insert`'s own no-op).
                if h != HASH_SIZE {
                    self.insert_at(h, ip);
                }
                ip += 1;
            }
        }
        // Trailing literals.
        let lit_len = body - anchor;
        if !self.emit_token(lit_len, 0) {
            return None;
        }
        if !self.emit_ext(lit_len) {
            return None;
        }
        if !self.emit_literals(src, anchor, body) {
            return None;
        }

        let clen = self.out_len;
        // Worth it? Must clear the saving threshold and fit the
        // 15-bit trailer length field.
        if body.saturating_sub(clen) >= COMPRESS_MIN_SAVING && clen < (1 << 15) {
            self.out_len = clen;
            Some(clen)
        } else {
            self.out_len = 0;
            None
        }
    }

    /// Hashes 4 bytes at `p` into a bucket.
    ///
    /// `#[inline(always)]` is measured, not decorative — same story as
    /// `crc32` (`src/crc.rs`): LLVM declines to inline this on its own at
    /// either of its two call sites (`find_match`, `insert`), which leaves
    /// each 4-byte little-endian load as four separate bounds-checked
    /// slice indexes instead of one checked against the caller's
    /// already-proven length. Confirmed with `callgrind` on
    /// `benches/compaction.rs`: forcing the inline drops it from
    /// 463,462,751 to 386,303,098 Ir (-16.65%). See the PR for the full
    /// before/after.
    #[allow(clippy::inline_always)] // measured with callgrind (see above), not decorative
    #[inline(always)]
    const fn hash(src: &[u8], p: usize) -> usize {
        let v = u32::from_le_bytes([src[p], src[p + 1], src[p + 2], src[p + 3]]);
        (v.wrapping_mul(0x9E37_79B9) >> (32 - HASH_BITS)) as usize
    }

    /// Records `p` in bucket `h`, which the caller has already hashed.
    const fn insert_at(&mut self, h: usize, p: usize) {
        // `p` indexes `src`, whose length is at most `BLOCK`
        // (a few KiB): the narrowing cast is exact.
        #[allow(clippy::cast_possible_truncation)]
        let p32 = p as u32;
        self.head[h] = p32;
    }

    /// Records position `p` in the hash table. No-op near the tail.
    const fn insert(&mut self, src: &[u8], p: usize) {
        if p + MIN_MATCH <= src.len() {
            let h = Self::hash(src, p);
            self.insert_at(h, p);
        }
    }

    /// Finds the longest match at `ip`: the most recent position with the
    /// same 4-byte hash, within a 32 KiB window. Single probe — bounded
    /// and simple; the table always holds the freshest candidate.
    ///
    /// Also returns the bucket `ip` hashed to, or `HASH_SIZE` on the
    /// early-return path (too close to the tail to hash at all) — so a
    /// caller that misses can record `ip` via [`insert_at`](Self::insert_at)
    /// without hashing the same 4 bytes a second time.
    fn find_match(&self, src: &[u8], ip: usize, body: usize) -> (usize, usize, usize) {
        if ip + MIN_MATCH > body {
            return (0, 0, HASH_SIZE);
        }
        let h = Self::hash(src, ip);
        let cand = self.head[h];
        if cand == u32::MAX {
            return (0, 0, h);
        }
        let pos = cand as usize;
        if pos >= ip || ip - pos > 32768 {
            return (0, 0, h);
        }
        // Count the match, capped so one token pair stays sane.
        let mut len = 0usize;
        let cap = (body - ip).min(MAX_MATCH);
        while len < cap && src[pos + len] == src[ip + len] {
            len += 1;
        }
        (pos, len, h)
    }

    /// Appends one byte; `false` when the staging buffer is full.
    const fn emit(&mut self, b: u8) -> bool {
        if self.out_len >= self.out.len() {
            return false;
        }
        self.out[self.out_len] = b;
        self.out_len += 1;
        true
    }

    /// Appends the token tag byte. `false` on staging overflow.
    const fn emit_token(&mut self, lit_len: usize, tok_ml: usize) -> bool {
        // Nibbles are capped at 15 (`min` is not const-callable, hence
        // the manual clamp), far below `u8::MAX`: the narrowing casts
        // are exact.
        let lit = if lit_len < 15 { lit_len } else { 15 };
        let mat = if tok_ml < 15 { tok_ml } else { 15 };
        #[allow(clippy::cast_possible_truncation)]
        let tag = ((lit as u8) << 4) | (mat as u8);
        self.emit(tag)
    }

    /// Appends LZ4-style extension bytes for `len` when the nibble
    /// saturated at 15. `false` on staging overflow.
    const fn emit_ext(&mut self, len: usize) -> bool {
        if len < 15 {
            return true;
        }
        let mut rem = len - 15;
        while rem >= 255 {
            if !self.emit(0xFF) {
                return false;
            }
            rem -= 255;
        }
        // `rem` is below 255 by loop construction: the cast is exact.
        #[allow(clippy::cast_possible_truncation)]
        let last = rem as u8;
        self.emit(last)
    }

    /// Appends `src[lo..hi]` as literals. `false` on staging overflow.
    const fn emit_literals(&mut self, src: &[u8], lo: usize, hi: usize) -> bool {
        let mut p = lo;
        while p < hi {
            if !self.emit(src[p]) {
                return false;
            }
            p += 1;
        }
        true
    }

    /// Appends a 2-byte little-endian offset. `false` on overflow or
    /// when the offset does not fit 16 bits.
    const fn emit_u16(&mut self, v: usize) -> bool {
        if v == 0 || v > 0xFFFF {
            return false;
        }
        // Masked to 8 bits: the narrowing casts are exact.
        #[allow(clippy::cast_possible_truncation)]
        let (lo, hi) = ((v & 0xFF) as u8, ((v >> 8) & 0xFF) as u8);
        if !self.emit(lo) {
            return false;
        }
        self.emit(hi)
    }
}

/// Decompresses `src` into `dst`, which must be exactly the decompressed
/// block size (`BLOCK - 4`).
///
/// Returns the number of bytes written (always `dst.len()`) on success.
///
/// # Errors
///
/// Returns [`DecompressError::Truncated`] when the stream ends mid-token
/// and [`DecompressError::Invalid`] on a structurally invalid tag,
/// length, or match offset. The decoder never panics, whatever bytes it
/// is handed.
pub fn decompress(src: &[u8], dst: &mut [u8]) -> Result<usize, DecompressError> {
    let mut sp = 0usize; // stream position
    let mut dp = 0usize; // output position
    let out_len = dst.len();

    // Reads one stream byte.
    macro_rules! take {
        () => {{
            if sp >= src.len() {
                return Err(DecompressError::Truncated);
            }
            let b = src[sp];
            sp += 1;
            b
        }};
    }

    while dp < out_len {
        let tag = take!();
        let mut lit_len = usize::from(tag >> 4);
        let mut match_len = usize::from(tag & 0x0F);

        if lit_len == 15 {
            lit_len += read_ext(src, &mut sp)?;
        }
        // Copy literals.
        if lit_len > out_len - dp {
            return Err(DecompressError::Invalid);
        }
        if sp + lit_len > src.len() {
            return Err(DecompressError::Truncated);
        }
        dst[dp..dp + lit_len].copy_from_slice(&src[sp..sp + lit_len]);
        sp += lit_len;
        dp += lit_len;
        if dp == out_len {
            break;
        }

        if match_len == 15 {
            match_len += read_ext(src, &mut sp)?;
        }
        match_len += MIN_MATCH;
        // Offset: 2 bytes, little-endian, 1-based distance back.
        if sp + 2 > src.len() {
            return Err(DecompressError::Truncated);
        }
        let offset = usize::from(src[sp]) | (usize::from(src[sp + 1]) << 8);
        sp += 2;
        if offset == 0 || offset > dp {
            return Err(DecompressError::Invalid);
        }
        if match_len > out_len - dp {
            return Err(DecompressError::Invalid);
        }
        // Byte-by-byte: matches may overlap the write frontier.
        let mut mp = dp - offset;
        let end = dp + match_len;
        while dp < end {
            dst[dp] = dst[mp];
            dp += 1;
            mp += 1;
        }
    }
    Ok(out_len)
}

/// Reads LZ4-style extension bytes: `0xFF` adds 255 each, the first
/// non-`0xFF` byte adds its value. Overflow-saturating; a saturating
/// sum can only make the length check fail, never wrap.
fn read_ext(src: &[u8], sp: &mut usize) -> Result<usize, DecompressError> {
    let mut total = 0usize;
    loop {
        if *sp >= src.len() {
            return Err(DecompressError::Truncated);
        }
        let b = src[*sp];
        *sp += 1;
        total = total.saturating_add(usize::from(b));
        if b != 0xFF {
            return Ok(total);
        }
        // A hostile stream of endless 0xFF bytes must terminate:
        // the saturating add caps the damage and the caller's length
        // check rejects the token.
        if total > (1 << 24) {
            return Ok(total);
        }
    }
}
