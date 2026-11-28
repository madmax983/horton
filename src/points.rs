//! Entry points in compressed data blocks (ADR-0040).
//!
//! A compressed data block is one LZ77 stream. The writer cuts it into
//! chunks at some of the block's restarts: at each cut, the encoder
//! forgets its match window, so no match crosses the cut, and a decode
//! can begin at the token the cut falls in. The block records each cut
//! as an entry point, in what used to be its zero fill:
//!
//! ```text
//! stream[0..clen] | zero fill | n x (s u16, t u16, b u16) | n u16 | flag u16 | crc
//! ```
//!
//! - `s` is the stream offset of the token a decode begins at;
//! - `t` is the logical offset that token writes from;
//! - `b` is the chunk's head: its first entry, a restart, at or past `t`.
//!
//! Chunk 0 begins at `(0, 0, 0)` and is not listed. `n` is 0 (or the fill
//! is all zeros) for a block with one chunk, and for every block a writer
//! before ADR-0040 sealed. A reader that knows nothing of the table
//! decodes the stream as before, and ignores the fill.
//!
//! The table is trusted only after a check. Its shape is checked before
//! any of it is used ([`point_table`]). Whether its points are where
//! tokens start, with no later match reaching below them, and whether
//! its heads are restarts, only a decode can say: every whole decode
//! checks both ([`inflate_image`]), so a table that would mislead a
//! decode begun at one of its points is `CorruptBlock` wherever the
//! block is read whole.

use crate::compress::decompress_points;
use crate::sstable::{CRC_LEN, ENTRY_HEADER};

/// Bytes of one entry point: three `u16` offsets.
pub const POINT_LEN: usize = 6;

/// Most entry points a block has: a block is at most this many chunks
/// and one.
pub const MAX_POINTS: usize = 15;

/// Least logical bytes from one chunk head to the next. Each cut costs
/// the encoder its window, so cuts are kept a few kilobytes apart: a
/// 16 KiB block has about seven (ADR-0040's measurements).
const fn chunk_min<const BLOCK: usize>() -> usize {
    let m = BLOCK / (MAX_POINTS + 1);
    if m < 2048 { 2048 } else { m }
}

/// Whether a `BLOCK`-byte block may carry entry points. Its offsets
/// must fit 16 bits. And it must hold a few chunks: a block under 8 KiB
/// could hold one cut, which measured a read no faster and a stream
/// 1% to 10% longer (ADR-0040). Outside these sizes the fill is fill,
/// to every reader.
pub const fn has_points<const BLOCK: usize>() -> bool {
    carries_points(BLOCK - CRC_LEN)
}

/// [`has_points`] for a block whose body, up to its CRC, is `body` bytes.
const fn carries_points(body: usize) -> bool {
    body >= 8192 - CRC_LEN && body <= u16::MAX as usize
}

/// Entry point `i` of `table` (see [`point_table`]): `(s, t, b)`.
pub fn point_at(table: &[u8], i: usize) -> (usize, usize, usize) {
    let at = i * POINT_LEN;
    let u = |o: usize| {
        table
            .get(at + o..at + o + 2)
            .map_or(0, |b| usize::from(u16::from_le_bytes([b[0], b[1]])))
    };
    (u(0), u(2), u(4))
}

/// The entry points of the compressed data block image `body` (up to its
/// CRC), whose stream is `clen` bytes: empty when it has none, or is of
/// a size that carries none ([`has_points`]); `None` when its bytes
/// cannot be a table. A table has at most [`MAX_POINTS`]
/// points, lies wholly past the stream, and ascends: `s` and `t` never
/// fall, `b` always rises, each `t` is at or below its `b`, each `s` is
/// inside the stream, and each head's entry header ends before the
/// trailer could begin. A stream that reaches the count word leaves no
/// room for a table.
pub fn point_table(body: &[u8], clen: usize) -> Option<&[u8]> {
    if !carries_points(body.len()) {
        return Some(&[]);
    }
    let at = body.len().checked_sub(4)?;
    if clen >= at {
        // The stream covers the count word: the block has no fill, and so
        // no table, as every reader takes it (review of ADR-0040).
        return Some(&[]);
    }
    let n = usize::from(u16::from_le_bytes([*body.get(at)?, *body.get(at + 1)?]));
    if n == 0 {
        return Some(&[]);
    }
    if n > MAX_POINTS {
        return None;
    }
    let from = at.checked_sub(n * POINT_LEN)?;
    if from < clen {
        return None;
    }
    let table = body.get(from..at)?;
    let (mut ps, mut pt, mut pb) = (0, 0, 0);
    for i in 0..n {
        let (s, t, b) = point_at(table, i);
        // A head's entry header must end before the trailer could begin
        // (review of ADR-0040: a head in the last bytes before the count
        // word left a seek probing for a header the block cannot hold).
        if s < ps || s >= clen || t < pt || t > b || b <= pb || b + ENTRY_HEADER > at {
            return None;
        }
        (ps, pt, pb) = (s, t, b);
    }
    Some(table)
}

/// Whether every head of `table` is one of the restarts of the logical
/// block `body` (up to its CRC), before its entries end.
pub fn points_are_restarts(body: &[u8], table: &[u8]) -> bool {
    let Some(at) = body.len().checked_sub(2) else {
        return false;
    };
    let rcount = usize::from(u16::from_le_bytes([body[at], body[at + 1]]));
    let Some(rstart) = at.checked_sub(rcount * 2) else {
        return false;
    };
    let restart_at = |i: usize| {
        usize::from(u16::from_le_bytes([
            body[rstart + 2 * i],
            body[rstart + 2 * i + 1],
        ]))
    };
    (0..table.len() / POINT_LEN).all(|i| {
        let b = point_at(table, i).2;
        let (mut lo, mut hi) = (0usize, rcount);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if restart_at(mid) < b {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        b < rstart && lo < rcount && restart_at(lo) == b
    })
}

/// Inflates the compressed data block image `body` (up to its CRC; its
/// stream `clen` bytes) into `out` (as long), whole, checking its table:
/// each point must be where a token starts, with no later match reaching
/// below it, and each head a restart. `false`: the block is corrupt.
/// Every whole inflate goes through here.
pub fn inflate_image(body: &[u8], clen: usize, out: &mut [u8]) -> bool {
    let Some(table) = point_table(body, clen) else {
        return false;
    };
    let Some(stream) = body.get(..clen) else {
        return false;
    };
    let points = (0..table.len() / POINT_LEN).map(|i| {
        let (s, t, _) = point_at(table, i);
        (s, t)
    });
    decompress_points(stream, out, points).is_ok()
        && (table.is_empty() || points_are_restarts(out, table))
}

/// The table of a lent compressed block `image` (stream `clen` bytes),
/// its bytes checked first by `check` (the cache's RAM sums): empty when
/// it has none. `Err(true)`: a check failed (a miss). `Err(false)`: the
/// bytes cannot be a table, and the block is corrupt.
pub fn table_of<'i, const BLOCK: usize>(
    image: &'i [u8; BLOCK],
    clen: usize,
    check: &mut (dyn FnMut(usize, usize) -> bool + '_),
) -> Result<&'i [u8], bool> {
    let body_end = BLOCK - CRC_LEN;
    let count = body_end - 4;
    if clen >= count || !has_points::<BLOCK>() {
        return Ok(&[]);
    }
    if !check(count, body_end - 2) {
        return Err(true);
    }
    let n = usize::from(u16::from_le_bytes([image[count], image[count + 1]]));
    if n == 0 {
        return Ok(&[]);
    }
    let from = count
        .checked_sub(n.min(MAX_POINTS + 1) * POINT_LEN)
        .ok_or(false)?;
    if !check(from, count) {
        return Err(true);
    }
    point_table(&image[..body_end], clen).ok_or(false)
}

/// The logical offsets a writer cuts a block at: restarts `rs` (the
/// first is the block's start) at least [`chunk_min`] apart, at most
/// [`MAX_POINTS`] of them, into `bounds`. Returns how many.
pub fn bounds_of<const BLOCK: usize>(rs: &[u16], bounds: &mut [usize; MAX_POINTS]) -> usize {
    if !has_points::<BLOCK>() {
        return 0;
    }
    let mut nb = 0usize;
    let mut last = 0usize;
    for &r in rs.iter().skip(1) {
        let r = usize::from(r);
        if nb < MAX_POINTS && r >= last + chunk_min::<BLOCK>() {
            bounds[nb] = r;
            nb += 1;
            last = r;
        }
    }
    nb
}

/// Bytes a table of `n` points takes, with its count.
pub const fn table_len(n: usize) -> usize {
    if n == 0 { 0 } else { 2 + n * POINT_LEN }
}

/// Writes the table of `points` (`(s, t)` each) with heads `bounds` into
/// the block body `body` (up to the CRC), just before its last two bytes
/// (the flag's), and its count. Every offset fits 16 bits: the block
/// does ([`has_points`]).
pub fn write_table(body: &mut [u8], points: &[(usize, usize)], bounds: &[usize]) {
    let n = points.len().min(bounds.len());
    let Some(mut at) = body.len().checked_sub(2 + n * POINT_LEN) else {
        return;
    };
    let mut put = |at: &mut usize, v: usize| {
        let v = u16::try_from(v).unwrap_or(u16::MAX);
        body[*at..*at + 2].copy_from_slice(&v.to_le_bytes());
        *at += 2;
    };
    for (&(s, t), &b) in points.iter().zip(bounds) {
        put(&mut at, s);
        put(&mut at, t);
        put(&mut at, b);
    }
    put(&mut at, n);
}

#[cfg(test)]
mod tests {
    //! The table's shape rules, one test each: every rule fails a table
    //! that breaks only it (ADR-0040).
    use super::{
        MAX_POINTS, POINT_LEN, bounds_of, point_table, points_are_restarts, table_len, write_table,
    };
    extern crate std;
    use std::vec::Vec;

    /// The body of a 16 KiB block, up to its CRC.
    const BODY: usize = 16_380;

    /// A 16 KiB body (up to its CRC) whose stream is `clen` bytes of
    /// 0xC3, with `points` written as its table.
    fn body(clen: usize, points: &[(usize, usize, usize)]) -> Vec<u8> {
        let mut b = std::vec![0u8; BODY];
        b[..clen].fill(0xC3);
        let st: Vec<(usize, usize)> = points.iter().map(|p| (p.0, p.1)).collect();
        let heads: Vec<usize> = points.iter().map(|p| p.2).collect();
        let len = b.len();
        write_table(&mut b[..len - 2], &st, &heads);
        b
    }

    const GOOD: [(usize, usize, usize); 3] =
        [(100, 990, 1000), (300, 1995, 2000), (500, 3000, 3000)];

    #[test]
    fn a_good_table_reads_back() {
        let b = body(600, &GOOD);
        let t = point_table(&b, 600).expect("a table");
        assert_eq!(t.len(), 3 * POINT_LEN);
        assert_eq!(super::point_at(t, 1), (300, 1995, 2000));
    }

    #[test]
    fn zero_fill_is_no_table() {
        let b = body(600, &[]);
        assert_eq!(point_table(&b, 600), Some(&[][..]));
        assert_eq!(table_len(0), 0);
    }

    #[test]
    fn too_many_points_is_no_table() {
        let many: Vec<_> = (1..=MAX_POINTS + 1)
            .map(|i| (i, 100 * i, 100 * i))
            .collect();
        assert!(point_table(&body(20, &many), 20).is_none());
        assert!(point_table(&body(20, &many[..MAX_POINTS]), 20).is_some());
    }

    #[test]
    fn a_table_over_the_stream_is_no_table() {
        // 3 points and the count take 20 bytes before the flag.
        assert!(point_table(&body(BODY - 2 - 20, &GOOD), BODY - 2 - 20).is_some());
        assert!(point_table(&body(600, &GOOD), BODY - 2 - 19).is_none());
    }

    #[test]
    fn falling_stream_offsets_are_no_table() {
        let mut p = GOOD;
        p[1].0 = 99;
        assert!(point_table(&body(600, &p), 600).is_none());
    }

    #[test]
    fn falling_token_offsets_are_no_table() {
        let mut p = GOOD;
        p[1].1 = 980;
        assert!(point_table(&body(600, &p), 600).is_none());
    }

    #[test]
    fn heads_that_do_not_rise_are_no_table() {
        let mut p = GOOD;
        p[1].2 = 1000;
        p[1].1 = 995;
        assert!(point_table(&body(600, &p), 600).is_none());
    }

    #[test]
    fn a_token_past_its_head_is_no_table() {
        let mut p = GOOD;
        p[1].1 = 2001;
        assert!(point_table(&body(600, &p), 600).is_none());
    }

    #[test]
    fn a_point_past_the_stream_is_no_table() {
        let mut p = GOOD;
        p[2].0 = 600;
        assert!(point_table(&body(600, &p), 600).is_none());
    }

    #[test]
    fn a_head_in_the_trailer_is_no_table() {
        let mut p = GOOD;
        p[2] = (500, 4000, BODY - 4);
        assert!(point_table(&body(600, &p), 600).is_none());
    }

    /// A logical block whose restarts are `rs`: heads are restarts only
    /// when they are among them, before the trailer.
    #[test]
    fn heads_must_be_restarts() {
        let mut b = std::vec![0u8; 4092];
        let rs = [0u16, 1000, 2000, 3000];
        let rstart = 4092 - 2 - 2 * rs.len();
        for (i, r) in rs.iter().enumerate() {
            b[rstart + 2 * i..rstart + 2 * i + 2].copy_from_slice(&r.to_le_bytes());
        }
        b[4090..].copy_from_slice(&4u16.to_le_bytes());
        let table = |heads: &[usize]| {
            let mut t = Vec::new();
            for &h in heads {
                for v in [0u16, 0, u16::try_from(h).unwrap()] {
                    t.extend_from_slice(&v.to_le_bytes());
                }
            }
            t
        };
        assert!(points_are_restarts(&b, &table(&[1000, 3000])));
        assert!(!points_are_restarts(&b, &table(&[1000, 2500])));
        assert!(!points_are_restarts(&b, &table(&[3001])));
        assert!(points_are_restarts(&b, &[]));
    }

    /// The writer cuts at restarts a chunk's worth apart, at most
    /// [`MAX_POINTS`] times, and never a block past 16-bit offsets or
    /// under 8 KiB.
    #[test]
    fn bounds_are_restarts_a_chunk_apart() {
        let rs: Vec<u16> = (0..200).map(|i| i * 300).collect();
        let mut out = [0usize; MAX_POINTS];
        let n = bounds_of::<65_536>(&rs, &mut out);
        assert!(
            n >= 13 && out[..n].windows(2).all(|w| w[1] >= w[0] + 4096),
            "{n}"
        );
        assert!(
            out[..n]
                .iter()
                .all(|b| rs.contains(&u16::try_from(*b).unwrap()))
        );
        // At most MAX_POINTS, however many restarts are a chunk apart.
        let dense: Vec<u16> = (0..1090).map(|i| i * 30).collect();
        assert_eq!(bounds_of::<32_768>(&dense, &mut out), MAX_POINTS);
        // A block under 8 KiB is never cut, and its fill is fill.
        let rs4: Vec<u16> = (0..20).map(|i| i * 200).collect();
        assert_eq!(bounds_of::<4096>(&rs4, &mut out), 0);
        assert_eq!(bounds_of::<8192>(&rs4, &mut out), 1);
        assert_eq!(out[0], 2200, "the first restart 2 KiB in");
        let mut small = std::vec![0u8; 4092];
        small[4088..4090].copy_from_slice(&3u16.to_le_bytes());
        assert_eq!(point_table(&small, 100), Some(&[][..]));
        assert_eq!(bounds_of::<131_072>(&rs, &mut out), 0);
        let rs16: Vec<u16> = rs.iter().copied().filter(|&r| r < 16_380).collect();
        let n16 = bounds_of::<16_384>(&rs16, &mut out);
        assert!((5..=7).contains(&n16), "{n16}");
    }

    /// A stream that reaches the count word leaves no room for a table:
    /// every reader takes the block as having none, the whole decode too
    /// (review of ADR-0040: the partial paths said none, the whole one
    /// read a table out of the stream's last bytes).
    #[test]
    fn a_stream_over_the_count_word_has_no_table() {
        let mut b = std::vec![0u8; BODY];
        b[BODY - 4..BODY - 2].copy_from_slice(&5u16.to_le_bytes());
        for clen in [BODY - 4, BODY - 3, BODY - 2] {
            assert_eq!(point_table(&b, clen), Some(&[][..]), "clen {clen}");
        }
    }

    /// A head whose entry header would run into the trailer is no table
    /// (review of ADR-0040: a seek probed for that header forever).
    #[test]
    fn a_head_too_near_the_trailer_is_no_table() {
        let at = BODY - 4;
        for near in [at - 12, at - 6, at - 1] {
            let p = [(100, 990, 1000), (300, 1995, near)];
            assert!(point_table(&body(600, &p), 600).is_none(), "head {near}");
        }
        let p = [(100, 990, 1000), (300, 1995, at - 13)];
        assert!(point_table(&body(600, &p), 600).is_some());
    }
}
