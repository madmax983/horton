//! Hand-rolled IEEE CRC-32: slicing-by-8, over three independent streams.
//!
//! No dependency; the eight tables are computed at compile time (spec §4.2:
//! "256-entry table is also fine, it's `const`"). The main loop consumes 8
//! bytes per iteration instead of 1.
//!
//! # Why three streams
//!
//! One slicing-by-8 step cannot start until the step before it has
//! produced its `crc`: a load, a shift, eight table loads and an `XOR`
//! tree, about ten cycles for eight bytes on a host core. The core could
//! issue several of those chains at once, but a single CRC is one chain.
//! So a long input is cut into three equal segments, each run as its own
//! chain in the same loop, and the three registers are joined at the end
//! with the CRC's own algebra: the register a segment leaves
//! is what the next segment's register would be had it started from
//! zero, `XOR`ed with the first register carried across the second
//! segment's length. That carry is one multiplication in GF(2)\[x\]
//! modulo the polynomial. The result is bit-identical to the one-chain
//! loop; only the order of the work changes.
//!
//! No `unsafe`, so no carry-less-multiply instructions: the library
//! forbids `unsafe`, and Rust still asks for it to call a
//! `#[target_feature]` function even when the build enables the feature.
//! Three chains give the table-driven loop most of what the core's load
//! ports allow. Inputs shorter than [`STREAMS_MIN`] (a WAL record, a
//! manifest edit) take the one-chain loop, where the join would cost
//! more than it saves.
//!
//! Only 64-bit targets take the three chains. A firmware core such as
//! the ESP32-S3's is in order, with few registers, and gains little from
//! more chains than one; it keeps the one-chain loop.

/// Builds the eight slicing-by-8 tables. `TABLES[0]` is the standard
/// byte-indexed reflected CRC-32 table: `TABLES[0][n]` is the eight-bit-at-
/// a-time update for input byte `n`, via the same bitwise recurrence the
/// original byte loop used. `TABLES[k]` for `k > 0` is `TABLES[0]` applied
/// again to `TABLES[k - 1]`'s output (i.e. the update for a byte that sits
/// `k` positions further back in the stream). That lets the main loop
/// combine 8 bytes' worth of update with 8 independent table lookups —
/// `XOR`ed together, no data dependency between them except on the previous
/// iteration's `crc` — instead of 8 sequential single-byte steps.
const fn make_tables() -> [[u32; 256]; 8] {
    let mut tables = [[0u32; 256]; 8];
    let mut n = 0;
    while n < 256 {
        // `n < 256` by the loop bound, so the cast cannot truncate.
        #[allow(clippy::cast_possible_truncation)]
        let mut crc = n as u32;
        let mut bit = 0;
        while bit < 8 {
            let mask = 0u32.wrapping_sub(crc & 1);
            crc = (crc >> 1) ^ (POLY & mask);
            bit += 1;
        }
        tables[0][n] = crc;
        n += 1;
    }
    let mut k = 1;
    while k < 8 {
        let mut n = 0;
        while n < 256 {
            let c = tables[k - 1][n];
            tables[k][n] = tables[0][(c & 0xFF) as usize] ^ (c >> 8);
            n += 1;
        }
        k += 1;
    }
    tables
}

/// The reflected IEEE polynomial.
const POLY: u32 = 0xEDB8_8320;

const TABLES: [[u32; 256]; 8] = make_tables();

/// Inputs at least this long run as three streams on 64-bit targets.
///
/// Below it the join (two multiplications and the power of `x` they
/// need) costs more than the parallel chains save: measured on x86-64,
/// three streams are level with one chain at 512 bytes and 1.3 times
/// faster at 768.
pub const STREAMS_MIN: usize = 768;

/// `X8N[k]` is `x^(8 * 2^k)` modulo the polynomial, in the reflected
/// representation: the operator that carries a register across `2^k`
/// zero bytes. [`zeros_op`] multiplies together the entries for the bits
/// of a length. One entry per bit of `usize`, so every length has its
/// entries (a 32-entry table panicked at 4 GiB on 64-bit hosts).
const X8N: [u32; usize::BITS as usize] = {
    let mut t = [0u32; usize::BITS as usize];
    // x^8: bit 31 is x^0 in the reflected form, so x^8 is bit 23.
    let mut p = 1u32 << 23;
    let mut k = 0;
    while k < t.len() {
        t[k] = p;
        p = mulmod(p, p);
        k += 1;
    }
    t
};

/// The product of `a` and `b` in GF(2)\[x\] modulo the polynomial, both
/// in the reflected representation (bit 31 is `x^0`). Thirty-two
/// shift-and-`XOR` steps at most.
const fn mulmod(a: u32, mut b: u32) -> u32 {
    let mut p = 0u32;
    let mut m = 1u32 << 31;
    while m != 0 {
        if a & m != 0 {
            p ^= b;
        }
        b = (b >> 1) ^ (POLY & 0u32.wrapping_sub(b & 1));
        m >>= 1;
    }
    p
}

/// The operator that carries a register across `len` zero bytes:
/// `x^(8 * len)` modulo the polynomial.
const fn zeros_op(mut len: usize) -> u32 {
    // x^0.
    let mut p = 1u32 << 31;
    let mut k = 0;
    while len != 0 {
        if len & 1 != 0 {
            p = mulmod(X8N[k], p);
        }
        len >>= 1;
        k += 1;
    }
    p
}

/// One slicing-by-8 step: the register after eight more bytes, given as
/// a little-endian word.
#[allow(clippy::inline_always)] // the three-stream loop is only fast inlined
#[inline(always)]
const fn step(crc: u32, word: u64) -> u32 {
    // Low and high halves of the word: the truncations are the point.
    #[allow(clippy::cast_possible_truncation)]
    let one = (word as u32) ^ crc;
    #[allow(clippy::cast_possible_truncation)]
    let two = (word >> 32) as u32;
    TABLES[7][(one & 0xFF) as usize]
        ^ TABLES[6][((one >> 8) & 0xFF) as usize]
        ^ TABLES[5][((one >> 16) & 0xFF) as usize]
        ^ TABLES[4][(one >> 24) as usize]
        ^ TABLES[3][(two & 0xFF) as usize]
        ^ TABLES[2][((two >> 8) & 0xFF) as usize]
        ^ TABLES[1][((two >> 16) & 0xFF) as usize]
        ^ TABLES[0][(two >> 24) as usize]
}

/// Runs the register `crc` over `data` as one chain: slicing-by-8, then
/// a byte at a time for the last `len % 8` bytes. No pre- or
/// post-inversion.
#[allow(clippy::inline_always)] // see `crc32`
#[inline(always)]
const fn update_serial(mut crc: u32, data: &[u8]) -> u32 {
    let (words, tail) = data.as_chunks::<8>();
    let mut i = 0;
    while i < words.len() {
        crc = step(crc, u64::from_le_bytes(words[i]));
        i += 1;
    }
    let mut j = 0;
    while j < tail.len() {
        crc = TABLES[0][((crc ^ tail[j] as u32) & 0xFF) as usize] ^ (crc >> 8);
        j += 1;
    }
    crc
}

/// Runs the register `crc` over `data` as three chains, one per third,
/// and joins them. Bit-identical to [`update_serial`].
#[allow(clippy::inline_always)] // see `crc32`
#[inline(always)]
const fn update_streams(crc: u32, data: &[u8]) -> u32 {
    // Three segments of `seg` bytes, a whole number of words each; the
    // last `len - 3 * seg` bytes (under 24) run serially after the join.
    let seg = (data.len() / 24) * 8;
    let (a, rest) = data.split_at(seg);
    let (b, rest) = rest.split_at(seg);
    let (c, tail) = rest.split_at(seg);
    let (a, _) = a.as_chunks::<8>();
    let (b, _) = b.as_chunks::<8>();
    let (c, _) = c.as_chunks::<8>();
    // Stream `a` carries the incoming register; `b` and `c` start from
    // zero and are joined by linearity below.
    let (mut ra, mut rb, mut rc) = (crc, 0u32, 0u32);
    let mut i = 0;
    while i < a.len() && i < b.len() && i < c.len() {
        ra = step(ra, u64::from_le_bytes(a[i]));
        rb = step(rb, u64::from_le_bytes(b[i]));
        rc = step(rc, u64::from_le_bytes(c[i]));
        i += 1;
    }
    // reg(a ‖ b) = shift(reg(a), |b|) ^ reg₀(b), and the same again for c.
    let op = zeros_op(seg);
    let joined = mulmod(op, mulmod(op, ra) ^ rb) ^ rc;
    update_serial(joined, tail)
}

/// Runs the register `crc` over `data`: three streams on a 64-bit
/// target for a long input, one chain otherwise. No pre- or
/// post-inversion.
#[allow(clippy::inline_always)] // see `crc32`
#[inline(always)]
const fn update(crc: u32, data: &[u8]) -> u32 {
    if cfg!(target_pointer_width = "64") && data.len() >= STREAMS_MIN {
        update_streams(crc, data)
    } else {
        update_serial(crc, data)
    }
}

/// Zero runs are looked for in steps of this many bytes. A run shorter
/// than two steps may go unnoticed; it is then run through the tables
/// like any other bytes.
const ZERO_STEP: usize = 256;

/// True when every byte of `chunk` is zero. The first word decides most
/// chunks of data, so a chunk that is not zero usually costs one load.
const fn all_zero(chunk: &[u8]) -> bool {
    let (words, tail) = chunk.as_chunks::<8>();
    let mut i = 0;
    while i < words.len() {
        if u64::from_ne_bytes(words[i]) != 0 {
            return false;
        }
        i += 1;
    }
    let mut j = 0;
    while j < tail.len() {
        if tail[j] != 0 {
            return false;
        }
        j += 1;
    }
    true
}

/// Computes the IEEE CRC-32 (polynomial `0xEDB88320`) of `data`.
///
/// On 64-bit targets, stretches of [`STREAMS_MIN`] bytes or more run as
/// three interleaved chains (see the module docs).
///
/// Runs of zero bytes are carried across, not computed through. A block
/// is often half zeros: the fill after a compressed payload, the gap
/// between a data block's entries and its restart offsets, the unused
/// end of an index. Carrying the register across `n` zero bytes is one
/// multiplication by `x^(8n)`, the same algebra that
/// joins the streams, so the result is bit-identical to running every
/// byte through the tables. Zero runs are found in 256-byte
/// steps, and only bytes this function has seen to be zero are carried
/// across: corrupt bytes where zeros belong are run through the tables
/// like any others, and change the result as they must.
#[must_use]
pub const fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFF;
    // Bytes from `from` on have not been run yet; `at` is the next step
    // to look at.
    let mut from = 0;
    let mut at = 0;
    while at + ZERO_STEP <= data.len() {
        let (_, rest) = data.split_at(at);
        let (step, _) = rest.split_at(ZERO_STEP);
        if !all_zero(step) {
            at += ZERO_STEP;
            continue;
        }
        let zeros = at;
        at += ZERO_STEP;
        loop {
            if at + ZERO_STEP > data.len() {
                break;
            }
            let (_, rest) = data.split_at(at);
            let (step, _) = rest.split_at(ZERO_STEP);
            if !all_zero(step) {
                break;
            }
            at += ZERO_STEP;
        }
        let (_, rest) = data.split_at(from);
        let (dense, _) = rest.split_at(zeros - from);
        crc = mulmod(zeros_op(at - zeros), update(crc, dense));
        from = at;
    }
    let (_, rest) = data.split_at(from);
    !update(crc, rest)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The bit-at-a-time definition the tables are built from.
    fn reference(data: &[u8]) -> u32 {
        let mut crc = 0xFFFF_FFFFu32;
        for &b in data {
            crc ^= u32::from(b);
            for _ in 0..8 {
                crc = (crc >> 1) ^ (POLY & 0u32.wrapping_sub(crc & 1));
            }
        }
        !crc
    }

    fn bytes(len: usize, seed: u32) -> [u8; 4200] {
        let mut out = [0u8; 4200];
        let mut x = seed | 1;
        for b in out.iter_mut().take(len) {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            *b = x.to_le_bytes()[0];
        }
        out
    }

    #[test]
    fn known_vectors() {
        assert_eq!(crc32(b""), 0);
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
        assert_eq!(
            crc32(b"The quick brown fox jumps over the lazy dog"),
            0x414F_A339
        );
    }

    /// Every length around the stream threshold and the word and
    /// segment boundaries, against the bit-at-a-time definition.
    #[test]
    fn streams_match_the_definition_at_every_length() {
        for len in (0..1200).chain(4000..4200) {
            let buf = bytes(len, u32::try_from(len).unwrap_or(7));
            let data = &buf[..len];
            assert_eq!(crc32(data), reference(data), "len {len}");
            assert_eq!(
                !update_streams(0xFFFF_FFFF, data),
                reference(data),
                "streams, len {len}"
            );
            assert_eq!(
                !update_serial(0xFFFF_FFFF, data),
                reference(data),
                "serial, len {len}"
            );
        }
    }

    /// A single flipped bit anywhere in a streamed input changes the
    /// result, as it must for a CRC.
    #[test]
    fn streams_see_every_bit() {
        let buf = bytes(1024, 99);
        let base = crc32(&buf[..1024]);
        let mut flipped = buf;
        for bit in 0..1024 * 8 {
            flipped[bit / 8] ^= 1 << (bit % 8);
            assert_ne!(crc32(&flipped[..1024]), base, "bit {bit}");
            flipped[bit / 8] ^= 1 << (bit % 8);
        }
    }

    /// Blocks with zero runs of every length and position (the shapes a
    /// compressed block, a raw data block and an index leave), and
    /// corruption inside a run, against the definition.
    #[test]
    fn zero_runs_match_the_definition() {
        let mut buf = [0u8; 4200];
        let noise = bytes(4200, 5);
        for (lo, hi) in [
            (0usize, 4200usize),
            (0, 0),
            (100, 4000),
            (2048, 4198),
            (1, 4199),
            (0, 3000),
            (1500, 4200),
            (255, 257),
            (256, 768),
            (300, 811),
        ] {
            buf.copy_from_slice(&noise);
            buf[lo..hi].fill(0);
            for len in [4200usize, 4096, 2049, 1000, 512, 300] {
                let data = &buf[..len];
                assert_eq!(crc32(data), reference(data), "zeros {lo}..{hi}, len {len}");
            }
            // A stray bit anywhere in the zero run is seen.
            for at in [lo, usize::midpoint(lo, hi), hi.saturating_sub(1)] {
                if at >= hi {
                    continue;
                }
                buf[at] = 0x40;
                assert_eq!(crc32(&buf), reference(&buf), "flip at {at} in {lo}..{hi}");
                assert_ne!(crc32(&buf), {
                    let mut clean = buf;
                    clean[at] = 0;
                    crc32(&clean)
                });
                buf[at] = 0;
            }
        }
    }

    #[test]
    fn zeros_op_carries_across_zero_bytes() {
        let zeros = [0u8; 300];
        for n in [0usize, 1, 7, 8, 9, 64, 255, 300] {
            let reg = 0x1234_5678;
            assert_eq!(mulmod(zeros_op(n), reg), update_serial(reg, &zeros[..n]));
        }
    }

    /// Every bit of a `usize` length has its operator: a length of 4 GiB
    /// or more (a run of zeros that long, or a segment of a 13 GB input)
    /// used to index past a 32-entry table and panic.
    #[test]
    fn zeros_op_covers_every_length() {
        assert_eq!(X8N.len(), usize::BITS as usize);
        for k in 1..X8N.len() {
            assert_eq!(X8N[k], mulmod(X8N[k - 1], X8N[k - 1]), "entry {k}");
        }
        // No panic for any length; the top bit's operator is the last
        // entry.
        let _ = zeros_op(usize::MAX);
        assert_eq!(zeros_op(1 << (usize::BITS - 1)), X8N[X8N.len() - 1]);
        #[cfg(target_pointer_width = "64")]
        assert_eq!(zeros_op(1 << 32), mulmod(X8N[31], X8N[31]));
    }

    /// `crc32` is usable in constant evaluation.
    #[test]
    fn const_evaluable() {
        const C: u32 = crc32(&[0x5A; 600]);
        assert_eq!(C, reference(&[0x5A; 600]));
    }
}
