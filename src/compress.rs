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
/// Consecutive positions without a match before the encoder steps over
/// more than one byte: after `2^SKIP_TRIGGER` misses the step is 2,
/// after twice that 3, and so on. LZ4 uses the same trigger.
const SKIP_TRIGGER: u32 = 6;
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
        self.compress_chunked(src, &[], &mut [], 0)
    }

    /// [`compress`](Self::compress) in chunks (entry points, ADR-0040):
    /// at each of `bounds` (ascending logical offsets) the encoder forgets
    /// its window. No match crosses a bound, and none after it reaches
    /// below the token that the bound falls in, so a decode can start at
    /// that token with no output before it. That token is recorded in
    /// `points` as `(stream offset, logical offset)`: its literals may
    /// begin a little before the bound. The stream is one stream still,
    /// which [`decompress`] decodes whole as before. `reserve` bytes more
    /// must fit beside the stream (the table of points) for the block to
    /// be worth compressing.
    pub fn compress_chunked(
        &mut self,
        src: &[u8],
        bounds: &[usize],
        points: &mut [(usize, usize)],
        reserve: usize,
    ) -> Option<usize> {
        let body = BLOCK.checked_sub(4)?;
        if src.len() != body || points.len() < bounds.len() {
            return None;
        }
        self.head = [u32::MAX; HASH_SIZE];
        self.out_len = 0;

        let mut ip = 0usize; // input position
        let mut anchor = 0usize; // start of pending literals
        // Positions since the last match. Past `1 << SKIP_TRIGGER` of
        // them the input looks incompressible, and the encoder steps over
        // more than one byte at a time (LZ4's acceleration): a block of
        // random bytes costs a fraction of the hashing it would.
        let mut misses = 0usize;
        // No match reaches below `floor`, nor past `limit`, the next bound.
        let mut floor = 0usize;
        let mut next = 0usize;
        let mut limit = bounds.first().map_or(body, |&b| b.min(body));
        loop {
            if ip + MIN_MATCH > limit {
                if limit >= body {
                    break;
                }
                // A bound: the next token, whose literals start at
                // `anchor`, is where a decode may begin.
                points[next] = (self.out_len, anchor);
                floor = anchor;
                ip = limit;
                misses = 0;
                next += 1;
                limit = bounds.get(next).map_or(body, |&b| b.max(ip).min(body));
                continue;
            }
            let word = load_u32(src, ip);
            let h = hash(word);
            let cand = self.head[h] as usize;
            self.insert_at(h, ip);
            // `u32::MAX` (an empty bucket) is never below `ip`.
            if cand >= ip || cand < floor || ip - cand > 32768 || load_u32(src, cand) != word {
                ip += 1 + (misses >> SKIP_TRIGGER);
                misses += 1;
                continue;
            }
            misses = 0;
            // Four bytes match; count the rest, capped so one token pair
            // stays sane.
            let cap = (limit - ip).min(MAX_MATCH);
            let mlen = MIN_MATCH
                + common_prefix(
                    &src[cand + MIN_MATCH..cand + cap],
                    &src[ip + MIN_MATCH..ip + cap],
                );
            let lit_len = ip - anchor;
            // Encoded match length E = mlen - MIN_MATCH; the nibble
            // carries min(E, 15) and the extension the rest.
            let e = mlen - MIN_MATCH;
            if !self.emit_token(lit_len, e.min(15)) {
                return None;
            }
            // Token order: literal extension, literals, match extension,
            // offset (the decoder reads them in this order, and stops
            // after the literals when the output is full — so the
            // trailing all-literal token needs no match part at all).
            if !self.emit_ext(lit_len) {
                return None;
            }
            if !self.emit_literals(src, anchor, ip) {
                return None;
            }
            if !self.emit_ext(e) {
                return None;
            }
            // Offset: distance back from the current output position to
            // the match start.
            if !self.emit_u16(ip - cand) {
                return None;
            }
            // Index some of the bytes the match skipped so later
            // positions can match into them. Not every one: a long match
            // would cost a hash per byte it covers, more than finding it
            // did. The first two and the last two are where the next
            // match most likely begins (LZ4 indexes one).
            let end = ip + mlen;
            self.insert(src, ip + 1);
            self.insert(src, ip + 2);
            self.insert(src, end - 2);
            self.insert(src, end - 1);
            ip = end;
            anchor = ip;
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
        if body.saturating_sub(clen.saturating_add(reserve)) >= COMPRESS_MIN_SAVING
            && clen < (1 << 15)
        {
            self.out_len = clen;
            Some(clen)
        } else {
            None
        }
    }

    /// Hashes 4 bytes at `p` into a bucket.
    ///
    /// `#[inline(always)]` is measured, not decorative — same story as
    /// `crc32` (`src/crc.rs`): LLVM declined to inline the hash on its
    /// own, which left each 4-byte little-endian load as four separate
    /// bounds-checked slice indexes instead of one checked against the
    /// caller's already-proven length. Confirmed with `callgrind` on
    /// `benches/compaction.rs`: forcing the inline dropped it from
    /// 463,462,751 to 386,303,098 Ir (-16.65%).
    #[allow(clippy::inline_always)] // measured with callgrind (see above), not decorative
    #[inline(always)]
    const fn hash_at(src: &[u8], p: usize) -> usize {
        hash(load_u32(src, p))
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
            let h = Self::hash_at(src, p);
            self.insert_at(h, p);
        }
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
    fn emit_literals(&mut self, src: &[u8], lo: usize, hi: usize) -> bool {
        let n = hi - lo;
        if self.out_len + n > self.out.len() {
            return false;
        }
        self.out[self.out_len..self.out_len + n].copy_from_slice(&src[lo..hi]);
        self.out_len += n;
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

/// The 4 bytes at `p`, little-endian: one bounds check and one load,
/// where indexing the four bytes costs four of each. Every caller has
/// already checked `p + 4 <= src.len()`; past the end it reads as 0,
/// which can only cost a match, never produce a wrong one (a match is
/// counted again from the bytes themselves).
#[allow(clippy::inline_always)] // see `hash_at`
#[inline(always)]
const fn load_u32(src: &[u8], p: usize) -> u32 {
    match src.split_at_checked(p) {
        Some((_, rest)) => match rest.first_chunk::<4>() {
            Some(word) => u32::from_le_bytes(*word),
            None => 0,
        },
        None => 0,
    }
}

/// Multiplicative hash of a 4-byte word into a bucket.
#[allow(clippy::inline_always)] // see `hash_at`
#[inline(always)]
const fn hash(word: u32) -> usize {
    (word.wrapping_mul(0x9E37_79B9) >> (32 - HASH_BITS)) as usize
}

/// Length of the common prefix of `a` and `b`, which have equal
/// length: eight bytes a step, and the first differing byte of a word
/// from its lowest set bit.
fn common_prefix(a: &[u8], b: &[u8]) -> usize {
    let (aw, _) = a.as_chunks::<8>();
    let (bw, _) = b.as_chunks::<8>();
    let mut len = 0usize;
    for (x, y) in aw.iter().zip(bw) {
        let diff = u64::from_le_bytes(*x) ^ u64::from_le_bytes(*y);
        if diff != 0 {
            // At most 7: the cast is exact.
            return len + (diff.trailing_zeros() / 8) as usize;
        }
        len += 8;
    }
    let n = a.len().min(b.len());
    while len < n && a[len] == b[len] {
        len += 1;
    }
    len
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
    let mut inflate = Inflate::new();
    inflate.run(src, dst, dst.len())?;
    Ok(dst.len())
}

/// [`decompress`] of a stream with entry points (ADR-0040), checking
/// them as it goes: each of `points` (`(stream, logical)` offsets,
/// ascending) must be where a token starts, and no match after it may
/// reach below it. A table that does not hold is
/// [`Invalid`](DecompressError::Invalid): a decode begun at a point then
/// makes what the whole decode makes there, or this one fails.
///
/// # Errors
///
/// As [`decompress`], and `Invalid` for a point that does not hold.
pub(crate) fn decompress_points(
    src: &[u8],
    dst: &mut [u8],
    points: impl Iterator<Item = (usize, usize)>,
) -> Result<usize, DecompressError> {
    let mut inflate = Inflate::new();
    for (s, t) in points {
        inflate.run(src, dst, t)?;
        if inflate.dp != t || inflate.sp != s {
            return Err(DecompressError::Invalid);
        }
        inflate.enter();
    }
    inflate.run(src, dst, dst.len())?;
    Ok(dst.len())
}

/// Whether `points` (`(stream, logical)` offsets, ascending) hold for
/// the whole stream `src`, which decodes to `len` bytes, by its token
/// headers alone, decoding nothing: each is where a token starts, and no
/// match after it reaches below it. These are [`decompress_points`]'s
/// checks; a writer makes them on the table it is about to write
/// (ADR-0040), where no buffer is free to decode into.
pub(crate) fn points_hold(src: &[u8], len: usize, points: &[(usize, usize)]) -> bool {
    let (mut sp, mut dp, mut floor, mut next) = (0usize, 0usize, 0usize, 0usize);
    while dp < len {
        if let Some(&(s, t)) = points.get(next)
            && dp >= t
        {
            if dp != t || sp != s {
                return false;
            }
            floor = t;
            next += 1;
            continue;
        }
        let Some(&tag) = src.get(sp) else {
            return false;
        };
        sp += 1;
        let mut lit = usize::from(tag >> 4);
        if lit == 15 {
            let Ok(n) = read_ext(src, &mut sp) else {
                return false;
            };
            lit = lit.saturating_add(n);
        }
        if lit > len - dp || sp.saturating_add(lit) > src.len() {
            return false;
        }
        sp += lit;
        dp += lit;
        if dp == len {
            break;
        }
        let mut m = usize::from(tag & 0x0F);
        if m == 15 {
            let Ok(n) = read_ext(src, &mut sp) else {
                return false;
            };
            m = m.saturating_add(n);
        }
        let m = m.saturating_add(MIN_MATCH);
        let (Some(&lo), Some(&hi)) = (src.get(sp), src.get(sp + 1)) else {
            return false;
        };
        sp += 2;
        let offset = usize::from(lo) | (usize::from(hi) << 8);
        if offset == 0 || offset > dp - floor || m > len - dp {
            return false;
        }
        dp += m;
    }
    next == points.len() && dp == len
}

/// A decode that can stop part way and go on later: [`decompress`] in
/// steps. A point lookup inflates a data block only as far as the entry
/// that answers it (ADR-0027); [`run`](Self::run) is called again, with
/// the same `src` and `dst`, each time the lookup needs more.
///
/// Bytes of `dst` below [`done`](Self::done) are final. Bytes at or past
/// it are not: the decoder copies short runs as whole 16-byte chunks,
/// which write a little past the run, and later tokens write those bytes
/// again.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Inflate {
    /// Stream position: the next token starts here.
    sp: usize,
    /// Output position: `dst[floor..dp]` is final.
    dp: usize,
    /// No match may reach below this: the decode began here, at an entry
    /// point (ADR-0040), and has made nothing before it.
    floor: usize,
}

impl Inflate {
    /// A decode at the start of the stream.
    pub(crate) const fn new() -> Self {
        Self {
            sp: 0,
            dp: 0,
            floor: 0,
        }
    }

    /// A decode begun at an entry point (ADR-0040): the token at stream
    /// offset `sp` writes logical offset `dp` on. Nothing below `dp` is
    /// made, and a match that reaches below it is
    /// [`Invalid`](DecompressError::Invalid).
    pub(crate) const fn at(sp: usize, dp: usize) -> Self {
        Self { sp, dp, floor: dp }
    }

    /// Raises the floor to the output position, which must be an entry
    /// point the decode has just reached (ADR-0040): no later match may
    /// reach below it, as no match of a decode begun there could.
    pub(crate) const fn enter(&mut self) {
        self.floor = self.dp;
    }

    /// Bytes of output that are final.
    pub(crate) const fn done(&self) -> usize {
        self.dp
    }

    /// Decodes whole tokens until at least `until` bytes of `dst` are
    /// final (or `dst` is full). `src` and `dst` must hold the bytes of
    /// every earlier call: the stream from [`consumed`](Self::consumed)
    /// on, and the output below [`done`](Self::done), which later
    /// matches copy from (the stream before `consumed` is not read
    /// again). The rules, and the errors, are [`decompress`]'s. The
    /// state moves only on `Ok`; after an error the decode must not be
    /// resumed.
    ///
    /// # Errors
    ///
    /// As [`decompress`].
    pub(crate) fn run(
        &mut self,
        src: &[u8],
        dst: &mut [u8],
        until: usize,
    ) -> Result<(), DecompressError> {
        let out_len = dst.len();
        let until = until.min(out_len);
        let (sp, dp) = self.decode_with::<false>(src, dst, out_len, until)?;
        self.sp = sp;
        self.dp = dp;
        Ok(())
    }

    /// Stream bytes consumed: the next token starts here.
    pub(crate) const fn consumed(&self) -> usize {
        self.sp
    }

    /// A decode that has consumed `sp` stream bytes and made `dp` bytes
    /// of output final: one [`consumed`](Self::consumed) and
    /// [`done`](Self::done) reported, kept to be gone on with later
    /// (ADR-0039). The output below `dp` must still be that decode's.
    #[cfg(test)]
    pub(crate) const fn resumed(sp: usize, dp: usize) -> Self {
        Self { sp, dp, floor: 0 }
    }

    /// [`resumed`](Self::resumed) for a decode begun at an entry point
    /// `floor` ([`at`](Self::at)).
    pub(crate) const fn resumed_at(sp: usize, dp: usize, floor: usize) -> Self {
        Self { sp, dp, floor }
    }

    /// [`run`](Self::run) over `prefix`, the first bytes of the stream
    /// (each call's a prefix of the next's, of the same stream). A token
    /// that does not end inside `prefix` is not decoded: the decode
    /// stops before it, with every byte it has made final as the whole
    /// stream would have, and a later call with a longer prefix goes on
    /// from there. Only the bytes of `prefix` are ever read, so a caller
    /// that checks a stream as it goes hands the decoder only what it
    /// has checked (ADR-0039). With `whole` (`prefix` is the whole
    /// stream) this is `run`.
    ///
    /// # Errors
    ///
    /// As [`decompress`], for tokens wholly inside `prefix` (and, with
    /// `whole`, a stream that ends mid-token). Only a token cut off by
    /// the end of `prefix` stops the decode without an error.
    pub(crate) fn run_prefix(
        &mut self,
        prefix: &[u8],
        whole: bool,
        dst: &mut [u8],
        until: usize,
    ) -> Result<(), DecompressError> {
        if whole {
            return self.run(prefix, dst, until);
        }
        let out_len = dst.len();
        let until = until.min(out_len);
        let (sp, dp) = self.decode_with::<true>(prefix, dst, out_len, until)?;
        self.sp = sp;
        self.dp = dp;
        Ok(())
    }

    /// [`decode`](Self::decode) with its floor known at compile time to
    /// be zero or not: a decode with no floor, the most of them, pays no
    /// floor check on each match.
    fn decode_with<const PREFIX: bool>(
        &self,
        src: &[u8],
        dst: &mut [u8],
        out_len: usize,
        until: usize,
    ) -> Result<(usize, usize), DecompressError> {
        if self.floor == 0 {
            self.decode::<PREFIX, false>(src, dst, out_len, until)
        } else {
            self.decode::<PREFIX, true>(src, dst, out_len, until)
        }
    }

    /// The decode loop: tokens from `self.sp` while the output is below
    /// `until`. Returns the new stream and output positions. With
    /// `FLOOR`, no match may reach below [`Inflate::floor`].
    fn decode<const PREFIX: bool, const FLOOR: bool>(
        &self,
        src: &[u8],
        dst: &mut [u8],
        out_len: usize,
        until: usize,
    ) -> Result<(usize, usize), DecompressError> {
        let mut sp = self.sp; // stream position
        let mut dp = self.dp; // output position
        // Where the token being decoded starts: over a prefix, a token
        // cut off by its end leaves the decode here.
        let mut tsp;
        let mut tdp;

        // The stream ended mid-token: over a prefix, stop before it.
        macro_rules! truncated {
            () => {{
                if PREFIX {
                    return Ok((tsp, tdp));
                }
                return Err(DecompressError::Truncated);
            }};
        }
        // Reads one stream byte.
        macro_rules! take {
            () => {{
                if sp >= src.len() {
                    truncated!();
                }
                let b = src[sp];
                sp += 1;
                b
            }};
        }

        while dp < until {
            tsp = sp;
            tdp = dp;
            let tag = take!();
            let mut lit_len = usize::from(tag >> 4);
            let mut match_len = usize::from(tag & 0x0F);

            if lit_len == 15 {
                match read_ext(src, &mut sp) {
                    Ok(n) => lit_len += n,
                    Err(DecompressError::Truncated) => truncated!(),
                    Err(e) => return Err(e),
                }
            }
            // Copy literals.
            if lit_len > out_len - dp {
                return Err(DecompressError::Invalid);
            }
            if sp + lit_len > src.len() {
                truncated!();
            }
            copy_literals(dst, dp, src, sp, lit_len);
            sp += lit_len;
            dp += lit_len;
            if dp == out_len {
                break;
            }

            if match_len == 15 {
                match read_ext(src, &mut sp) {
                    Ok(n) => match_len += n,
                    Err(DecompressError::Truncated) => truncated!(),
                    Err(e) => return Err(e),
                }
            }
            match_len += MIN_MATCH;
            // Offset: 2 bytes, little-endian, 1-based distance back.
            if sp + 2 > src.len() {
                truncated!();
            }
            let offset = usize::from(src[sp]) | (usize::from(src[sp + 1]) << 8);
            sp += 2;
            if offset == 0 || offset > if FLOOR { dp - self.floor } else { dp } {
                return Err(DecompressError::Invalid);
            }
            if match_len > out_len - dp {
                return Err(DecompressError::Invalid);
            }
            let mp = dp - offset;
            let end = dp + match_len;
            if offset >= match_len {
                // Source and destination ranges are disjoint (source ends at
                // or before `dp`): a single bulk copy is equivalent to the
                // byte-by-byte rule, since no destination byte is read as a
                // source byte.
                copy_match(dst, mp, dp, match_len);
            } else if offset == 1 {
                // A run of one byte: the zero fill at the end of every block.
                let b = dst[mp];
                dst[dp..end].fill(b);
            } else {
                // Overlapping: each output byte repeats the one `offset`
                // back, so the output is periodic from `mp`. Copying
                // `dst[mp..mp + n]` to `d` with `n <= d - mp` reads only
                // bytes already final, and doubles the distance each round:
                // a few bulk copies where a byte loop takes `match_len`
                // steps.
                let mut d = dp;
                while d < end {
                    let n = (d - mp).min(end - d);
                    dst.copy_within(mp..mp + n, d);
                    d += n;
                }
            }
            dp = end;
        }
        Ok((sp, dp))
    }
}

/// Bytes the decoder copies as one fixed-size chunk when it may.
const SHORT: usize = 16;

/// Copies `src[sp..sp + n]` to `dst[dp..]`. The caller has checked both
/// ranges. A run of up to [`SHORT`] bytes, with room for a whole chunk
/// on both sides, is one fixed 16-byte copy: two loads and two stores
/// where a copy of `n` bytes is a call. The bytes past `n` it writes lie
/// ahead of the output position, and the tokens that follow write them
/// again before anything reads them. (Copying longer runs chunk by
/// chunk measured no faster than the call.)
#[allow(clippy::inline_always)] // the decoder's inner copy: a call here is the cost being removed
#[inline(always)]
fn copy_literals(dst: &mut [u8], dp: usize, src: &[u8], sp: usize, n: usize) {
    if n <= SHORT
        && let Some(to) = dst.get_mut(dp..dp + SHORT)
        && let Some(from) = src.get(sp..sp + SHORT)
    {
        to.copy_from_slice(from);
    } else {
        dst[dp..dp + n].copy_from_slice(&src[sp..sp + n]);
    }
}

/// Copies the match `dst[mp..mp + n]` to `dst[dp..]`, where the two do
/// not overlap (`mp + n <= dp`). A match of up to [`SHORT`] bytes, with
/// room for a whole chunk, is one fixed 16-byte copy: two loads and two
/// stores where a copy of `n` bytes is a call. The bytes it writes past
/// `dp + n` are written again later, as in [`copy_literals`].
#[allow(clippy::inline_always)] // as `copy_literals`
#[inline(always)]
fn copy_match(dst: &mut [u8], mp: usize, dp: usize, n: usize) {
    if n <= SHORT && dp + SHORT <= dst.len() {
        dst.copy_within(mp..mp + SHORT, dp);
    } else {
        dst.copy_within(mp..mp + n, dp);
    }
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

#[cfg(test)]
mod tests {
    //! [`Inflate::run_prefix`] over growing prefixes of a stream decodes
    //! exactly what [`decompress`] does over the whole (ADR-0039).
    use super::{
        CompressScratch, DecompressError, Inflate, decompress, decompress_points, points_hold,
    };
    extern crate std;
    use std::boxed::Box;
    use std::vec::Vec;

    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        fn below(&mut self, n: usize) -> usize {
            usize::try_from(self.next() % u64::try_from(n).unwrap()).unwrap()
        }
    }

    /// A block that compresses: runs of a few words, numbers, and noise.
    fn compressible(rng: &mut Rng, len: usize) -> Vec<u8> {
        let words: [&[u8]; 5] = [b"key-", b"value-", b"sensor/temp/", b"\0\0\0\0", b"zz"];
        let mut out = Vec::with_capacity(len);
        while out.len() < len {
            match rng.below(4) {
                0 => out.extend_from_slice(words[rng.below(words.len())]),
                1 => out.extend_from_slice(std::format!("{:05}", rng.below(99_999)).as_bytes()),
                2 => out.extend((0..rng.below(24)).map(|_| u8::try_from(rng.below(256)).unwrap())),
                _ => out.extend(core::iter::repeat_n(0u8, rng.below(300))),
            }
        }
        out.truncate(len);
        out
    }

    /// Decodes `stream` into `len` bytes through prefixes that grow by
    /// random steps, with random `until`s, checking after each call that
    /// the final bytes are `want`'s and that only the prefix's end stops
    /// a decode without an error. Returns the error the decode ended in.
    fn by_prefixes(
        rng: &mut Rng,
        stream: &[u8],
        len: usize,
        want: &[u8],
        want_err: Option<DecompressError>,
    ) -> Option<DecompressError> {
        let mut out = std::vec![0xA5u8; len];
        let mut inflate = Inflate::new();
        let mut have = 0usize;
        loop {
            have = (have + 1 + rng.below(64)).min(stream.len());
            let whole = have == stream.len();
            // Several calls over one prefix, each to a further `until`.
            for _ in 0..=rng.below(3) {
                let until = inflate.done() + rng.below(len + 1);
                let before = inflate;
                match inflate.run_prefix(&stream[..have], whole, &mut out, until) {
                    Ok(()) => {}
                    Err(e) => {
                        // Only a token wholly inside the prefix errs, and
                        // so the whole stream errs the same way; only the
                        // whole stream is ever cut short.
                        assert_eq!(Some(e), want_err, "prefix {have} of {}", stream.len());
                        assert!(whole || e == DecompressError::Invalid);
                        // The state did not move.
                        assert_eq!(
                            (inflate.consumed(), inflate.done()),
                            (before.consumed(), before.done())
                        );
                        return Some(e);
                    }
                }
                assert!(inflate.done() >= before.done() && inflate.consumed() >= before.consumed());
                assert!(inflate.consumed() <= have, "read past the prefix");
                assert_eq!(
                    out[..inflate.done()],
                    want[..inflate.done()],
                    "prefix {have}"
                );
                if !whole && inflate.done() < until.min(len) {
                    // Stopped short of `until`: by the prefix's end, at
                    // the start of a token it cuts off.
                    assert!(inflate.consumed() < have || have == inflate.consumed());
                }
                if inflate.done() == len {
                    assert_eq!(want_err, None);
                    return None;
                }
            }
            if whole {
                // The whole stream, every `until` reached: nothing more.
                let r = inflate.run_prefix(stream, true, &mut out, len);
                assert_eq!(r.err(), want_err);
                if r.is_ok() {
                    assert_eq!(out[..], want[..]);
                }
                return r.err();
            }
        }
    }

    fn compressed<const B: usize>(block: &[u8]) -> Option<Vec<u8>> {
        let mut cs = Box::new(CompressScratch::<B>::new());
        let clen = cs.compress(block)?;
        Some(cs.compressed()[..clen].to_vec())
    }

    #[test]
    fn run_prefix_matches_decompress() {
        let mut n = 0;
        for seed in 1..400u64 {
            let mut rng = Rng(seed * 0x9E37_79B9);
            let len = [1000usize, 4092, 16380][rng.below(3)];
            let block = compressible(&mut rng, len);
            let stream = match len {
                1000 => compressed::<1004>(&block),
                4092 => compressed::<4096>(&block),
                _ => compressed::<16384>(&block),
            };
            let Some(stream) = stream else { continue };
            let mut want = std::vec![0u8; len];
            assert_eq!(decompress(&stream, &mut want), Ok(len));
            assert_eq!(want, block);
            for _ in 0..8 {
                assert_eq!(by_prefixes(&mut rng, &stream, len, &want, None), None);
            }
            n += 1;
        }
        assert!(n > 300, "{n} blocks compressed");
    }

    /// Streams that do not decode: a valid one cut short, or with bytes
    /// changed (lengths, extension runs of 0xFF, offsets). A prefix decode
    /// fails exactly where the whole decode does, with the same error,
    /// and stops cleanly before every token its prefix cuts off.
    #[test]
    fn a_prefix_decode_fails_only_where_the_whole_does() {
        let mut errs = [0usize; 2];
        for seed in 1..600u64 {
            let mut rng = Rng(seed.wrapping_mul(0xD1B5_4A32_D192_ED03));
            let block = compressible(&mut rng, 4092);
            let Some(mut stream) = compressed::<4096>(&block) else {
                continue;
            };
            match rng.below(4) {
                0 => stream.truncate(rng.below(stream.len())),
                1 => {
                    let at = rng.below(stream.len());
                    let run = 1 + rng.below(40);
                    for b in stream.iter_mut().skip(at).take(run) {
                        *b = 0xFF;
                    }
                }
                _ => {
                    for _ in 0..=rng.below(4) {
                        let at = rng.below(stream.len());
                        stream[at] = u8::try_from(rng.below(256)).unwrap();
                    }
                }
            }
            let mut want = std::vec![0xA5u8; 4092];
            let want_err = decompress(&stream, &mut want).err();
            match want_err {
                Some(DecompressError::Truncated) => errs[0] += 1,
                Some(DecompressError::Invalid) => errs[1] += 1,
                None => {}
            }
            for _ in 0..4 {
                assert_eq!(
                    by_prefixes(&mut rng, &stream, 4092, &want, want_err),
                    want_err
                );
            }
            // The whole stream as a prefix: an invalid token lies wholly
            // inside it, so it is an error, not a stop; only a stream cut
            // mid-token stops short.
            let mut out = std::vec![0u8; 4092];
            let mut inflate = Inflate::new();
            let r = inflate.run_prefix(&stream, false, &mut out, 4092);
            match want_err {
                Some(DecompressError::Invalid) => assert_eq!(r, Err(DecompressError::Invalid)),
                Some(DecompressError::Truncated) => {
                    assert_eq!(r, Ok(()));
                    assert!(inflate.done() < 4092);
                }
                None => assert_eq!(inflate.done(), 4092),
            }
        }
        assert!(errs[0] > 50 && errs[1] > 50, "{errs:?}");
    }

    /// A decode kept as `(consumed, done)` and resumed into the same
    /// output over another copy of the stream from `consumed` on (the
    /// bytes before it changed) makes the same block (ADR-0039).
    #[test]
    fn a_resumed_decode_reads_only_the_stream_ahead() {
        for seed in 1..200u64 {
            let mut rng = Rng(seed.wrapping_mul(0x2545_F491_4F6C_DD1D));
            let block = compressible(&mut rng, 4092);
            let Some(stream) = compressed::<4096>(&block) else {
                continue;
            };
            let mut out = std::vec![0u8; 4092];
            let mut inflate = Inflate::new();
            let cut = rng.below(stream.len());
            inflate
                .run_prefix(&stream[..cut], false, &mut out, 4092)
                .unwrap();
            let kept = (inflate.consumed(), inflate.done());
            let mut other = stream.clone();
            for b in &mut other[..kept.0] {
                *b = !*b;
            }
            let mut again = Inflate::resumed(kept.0, kept.1);
            again.run_prefix(&other, true, &mut out, 4092).unwrap();
            assert_eq!(out, block);
        }
    }

    /// One token of a stream, as an independent parser reads it: where it
    /// starts in the stream and the output, and the lowest output byte
    /// its match copies from (`None`: no match).
    struct Token {
        sp: usize,
        dp: usize,
        reach: Option<usize>,
    }

    /// The tokens of a whole valid stream decoding to `len` bytes, read by
    /// hand (not by [`Inflate`]), for tests of what [`Inflate`] accepts.
    fn tokens(src: &[u8], len: usize) -> Vec<Token> {
        let ext = |sp: &mut usize| {
            let mut n = 0usize;
            loop {
                let b = src[*sp];
                *sp += 1;
                n += usize::from(b);
                if b != 0xFF {
                    return n;
                }
            }
        };
        let (mut sp, mut dp, mut out) = (0usize, 0usize, Vec::new());
        while dp < len {
            let start = (sp, dp);
            let tag = src[sp];
            sp += 1;
            let mut lit = usize::from(tag >> 4);
            if lit == 15 {
                lit += ext(&mut sp);
            }
            sp += lit;
            dp += lit;
            if dp == len {
                out.push(Token {
                    sp: start.0,
                    dp: start.1,
                    reach: None,
                });
                break;
            }
            let mut m = usize::from(tag & 0x0F);
            if m == 15 {
                m += ext(&mut sp);
            }
            let offset = usize::from(src[sp]) | (usize::from(src[sp + 1]) << 8);
            sp += 2;
            out.push(Token {
                sp: start.0,
                dp: start.1,
                reach: Some(dp - offset),
            });
            dp += m + super::MIN_MATCH;
        }
        out
    }

    /// Restart-like bounds about `every` bytes apart, as a writer picks
    /// them (ADR-0040).
    fn bounds_every(len: usize, every: usize) -> Vec<usize> {
        (1..len / every).map(|i| i * every).take(15).collect()
    }

    /// With no bounds, a chunked compress is the plain one, byte for
    /// byte; with bounds, its stream still decompresses to the block, and
    /// each point it records is a token start no later match reaches
    /// below, so a decode begun there makes the rest of the block.
    #[test]
    fn a_chunked_stream_is_one_stream_with_points_a_decode_can_begin_at() {
        let mut chunked_blocks = 0;
        for seed in 1..300u64 {
            let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15));
            let block = compressible(&mut rng, 4092);
            let mut cs = Box::new(CompressScratch::<4096>::new());
            let plain = cs.compress(&block).map(|n| cs.compressed()[..n].to_vec());
            let same = cs
                .compress_chunked(&block, &[], &mut [], 0)
                .map(|n| cs.compressed()[..n].to_vec());
            assert_eq!(plain, same, "no bounds is the plain compress");
            let bounds = bounds_every(4092, 200 + rng.below(900));
            let mut points = std::vec![(0usize, 0usize); bounds.len()];
            let Some(n) = cs.compress_chunked(&block, &bounds, &mut points, 0) else {
                continue;
            };
            chunked_blocks += 1;
            let stream = cs.compressed()[..n].to_vec();
            let mut out = std::vec![0u8; 4092];
            assert_eq!(decompress(&stream, &mut out), Ok(4092));
            assert_eq!(out, block);
            let toks = tokens(&stream, 4092);
            for (&(s, t), &b) in points.iter().zip(&bounds) {
                assert!(t <= b, "a point past its bound");
                assert!(
                    toks.iter().any(|k| k.sp == s && k.dp == t),
                    "a point not at a token"
                );
                assert!(
                    toks.iter()
                        .filter(|k| k.dp >= t)
                        .all(|k| k.reach.is_none_or(|r| r >= t)),
                    "a match after a point reaches below it"
                );
                // A decode begun there, over growing prefixes.
                let mut part = std::vec![0xA5u8; 4092];
                let mut inflate = Inflate::at(s, t);
                let mut have = s;
                while inflate.done() < 4092 {
                    have = (have + 1 + rng.below(300)).min(stream.len());
                    inflate
                        .run_prefix(&stream[..have], have == stream.len(), &mut part, 4092)
                        .unwrap();
                }
                assert_eq!(part[t..], block[t..]);
            }
            let mut whole = std::vec![0u8; 4092];
            assert_eq!(
                decompress_points(&stream, &mut whole, points.iter().copied()),
                Ok(4092)
            );
            assert_eq!(whole, block);
            assert!(
                points_hold(&stream, 4092, &points),
                "the writer's check of its own table"
            );
        }
        assert!(chunked_blocks > 250, "{chunked_blocks} blocks");
    }

    /// The floor: a decode begun, or entered, at a token that a later
    /// match reaches below is `Invalid`, exactly when one does. Taken at
    /// every token of plain streams, whose matches cross freely. (Review
    /// of ADR-0040's panel: dropping the floor check passed every other
    /// test.)
    #[test]
    fn a_match_below_the_floor_is_invalid() {
        let (mut crossed, mut clean) = (0usize, 0usize);
        for seed in 1..80u64 {
            let mut rng = Rng(seed.wrapping_mul(0xD1B5_4A32_D192_ED03));
            let block = compressible(&mut rng, 4092);
            // Plain streams, and chunked ones, whose points are clean.
            let mut cs = Box::new(CompressScratch::<4096>::new());
            let bounds = if seed % 2 == 0 {
                bounds_every(4092, 300)
            } else {
                Vec::new()
            };
            let mut points = std::vec![(0usize, 0usize); bounds.len()];
            let Some(n) = cs.compress_chunked(&block, &bounds, &mut points, 0) else {
                continue;
            };
            let stream = cs.compressed()[..n].to_vec();
            let toks = tokens(&stream, 4092);
            for k in toks.iter().skip(1).step_by(3) {
                let reaches_below = toks
                    .iter()
                    .filter(|j| j.dp >= k.dp)
                    .any(|j| j.reach.is_some_and(|r| r < k.dp));
                let mut out = std::vec![0u8; 4092];
                let mut at = Inflate::at(k.sp, k.dp);
                let begun = at.run(&stream, &mut out, 4092);
                let mut whole = std::vec![0u8; 4092];
                let entered = decompress_points(&stream, &mut whole, [(k.sp, k.dp)].into_iter());
                // The writer's check, by headers alone, agrees.
                assert_eq!(points_hold(&stream, 4092, &[(k.sp, k.dp)]), entered.is_ok());
                if reaches_below {
                    crossed += 1;
                    assert_eq!(
                        begun,
                        Err(DecompressError::Invalid),
                        "begun at {} {}",
                        k.sp,
                        k.dp
                    );
                    assert_eq!(entered, Err(DecompressError::Invalid));
                } else {
                    clean += 1;
                    assert_eq!(begun, Ok(()));
                    assert_eq!(out[k.dp..], block[k.dp..]);
                    assert_eq!(entered, Ok(4092));
                }
            }
        }
        assert!(
            crossed > 300 && clean > 100,
            "{crossed} crossed, {clean} clean"
        );
    }

    /// A table whose points are not where the stream's tokens start, by
    /// any amount, is `Invalid` to a whole decode that checks them.
    #[test]
    fn a_point_off_a_token_is_invalid() {
        let mut tried = 0;
        for seed in 1..120u64 {
            let mut rng = Rng(seed.wrapping_mul(0x2545_F491_4F6C_DD1D));
            let block = compressible(&mut rng, 4092);
            let mut cs = Box::new(CompressScratch::<4096>::new());
            let bounds = bounds_every(4092, 700);
            let mut points = std::vec![(0usize, 0usize); bounds.len()];
            let Some(n) = cs.compress_chunked(&block, &bounds, &mut points, 0) else {
                continue;
            };
            let stream = cs.compressed()[..n].to_vec();
            let toks = tokens(&stream, 4092);
            for i in 0..points.len() {
                for (ds, dt) in [(1isize, 0isize), (-1, 0), (0, 1), (0, -1), (3, 5), (0, 64)] {
                    let (s, t) = points[i];
                    let (Some(s2), Some(t2)) = (s.checked_add_signed(ds), t.checked_add_signed(dt))
                    else {
                        continue;
                    };
                    if toks.iter().any(|k| k.sp == s2 && k.dp == t2) {
                        continue;
                    }
                    let mut bad = points.clone();
                    bad[i] = (s2, t2);
                    let mut out = std::vec![0u8; 4092];
                    assert_eq!(
                        decompress_points(&stream, &mut out, bad.iter().copied()),
                        Err(DecompressError::Invalid),
                        "point {i} moved by {ds},{dt}"
                    );
                    assert!(
                        !points_hold(&stream, 4092, &bad),
                        "the writer's check let it by"
                    );
                    tried += 1;
                }
            }
        }
        assert!(tried > 300, "{tried}");
    }

    /// The writer's check reads only token headers, and is total: over
    /// streams that do not decode, and points anywhere, it never panics,
    /// and it holds only where a checked whole decode succeeds.
    #[test]
    fn the_writers_check_agrees_with_the_decoder_on_any_bytes() {
        for seed in 1..600u64 {
            let mut rng = Rng(seed.wrapping_mul(0xA076_1D64_78BD_642F));
            let block = compressible(&mut rng, 4092);
            let mut cs = Box::new(CompressScratch::<4096>::new());
            let bounds = bounds_every(4092, 400 + rng.below(800));
            let mut points = std::vec![(0usize, 0usize); bounds.len()];
            let Some(n) = cs.compress_chunked(&block, &bounds, &mut points, 0) else {
                continue;
            };
            let mut stream = cs.compressed()[..n].to_vec();
            for _ in 0..rng.below(3) {
                let at = rng.below(stream.len());
                stream[at] = u8::try_from(rng.below(256)).unwrap();
            }
            if rng.below(3) == 0 && !points.is_empty() {
                let i = rng.below(points.len());
                points[i].0 = points[i].0.wrapping_add(rng.below(5)).wrapping_sub(2);
            }
            let mut out = std::vec![0u8; 4092];
            let whole = decompress_points(&stream, &mut out, points.iter().copied());
            assert_eq!(
                points_hold(&stream, 4092, &points),
                whole.is_ok(),
                "seed {seed}"
            );
        }
    }
}
