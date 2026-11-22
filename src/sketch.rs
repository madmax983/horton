//! Set-reconciliation sketches: find every entry two replicas disagree
//! on from one fixed-size message.
//!
//! [`fingerprint_buckets`](crate::fingerprint::fingerprint_buckets)
//! answers "do these ranges agree?" and narrows a mismatch by bisection,
//! one round trip per level. A [`Sketch`] answers "*which* entries
//! differ?" in one: it is an invertible Bloom lookup table (Goodrich and
//! Mitzenmacher, 2011) of `CELLS` cells, each holding a count and two
//! XOR sums. Every visible entry is folded into four cells by its
//! 64-bit [`entry_id`]. Subtracting the remote side's sketch cancels
//! every entry both sides hold, whatever the size of the data set, and
//! leaves only the differences, which [`Sketch::decode`] peels out one
//! by one.
//!
//! ```text
//! ground:  sketch(scan) ──to_bytes──▶ radio ──▶ device
//! device:  sketch(scan).subtract(&ground).decode(&mut mine, &mut theirs)
//!          rescan: ship each entry whose entry_id is in `mine`;
//!          the ground deletes each key whose entry_id is in `theirs`
//! ```
//!
//! How many differences a sketch recovers depends on `CELLS` only, not
//! on how many entries match: 64 cells (1280 bytes on the wire, 20 a
//! cell, [`WIRE_BYTES`](Sketch::WIRE_BYTES)) peel 20 differences about
//! 998 times in 1000, and 35 about 99 in 100. When a difference does
//! not peel, decoding says so ([`Error::SketchTooSmall`]) rather than
//! guess, and the caller sends a larger sketch or bisects with
//! fingerprints first.
//!
//! A changed value shows up on both sides: the old entry on one, the
//! new one on the other. Entry ids are a 64-bit hash, so as with
//! [`Fingerprint`](crate::Fingerprint) this detects accidental
//! divergence and is no defense against forged collisions.

use core::convert::Infallible;

use crate::device::BlockDevice;
use crate::error::Error;
use crate::fingerprint::{entry_hash, mix};
use crate::scan::Scan;

/// The 64-bit id a [`Sketch`] knows an entry by: the same length-prefixed
/// hash [`Fingerprint`](crate::Fingerprint) sums. Rescan and compare ids
/// to turn [`Sketch::decode`]'s output back into entries.
#[must_use]
pub const fn entry_id(key: &[u8], val: &[u8]) -> u64 {
    entry_hash(key, val)
}

/// One table cell: how many entries landed here (local minus remote
/// after a subtraction), the XOR of their ids, and the XOR of a second
/// hash of each id that tells a cell holding one entry from a cell
/// holding several.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
struct Cell {
    count: i32,
    ids: u64,
    checks: u64,
}

const CELL_BYTES: usize = 20;
const CHECK_SEED: u64 = 0x5bd1_e995_9e37_79b9;
const INDEX_SEEDS: [u64; 4] = [
    0x243f_6a88_85a3_08d3,
    0x1319_8a2e_0370_7344,
    0xa409_3822_299f_31d0,
    0x082e_fa98_ec4e_6c89,
];

const fn check(id: u64) -> u64 {
    mix(id ^ CHECK_SEED)
}

/// An invertible Bloom lookup table of `CELLS` cells over a set of
/// entries. See the [module docs](self).
///
/// `CELLS` must be at least 4 (a smaller sketch does not compile); a
/// multiple of 4 uses every cell.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Sketch<const CELLS: usize> {
    cells: [Cell; CELLS],
}

impl<const CELLS: usize> Default for Sketch<CELLS> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const CELLS: usize> Sketch<CELLS> {
    const AT_LEAST_FOUR: () = assert!(CELLS >= 4, "a Sketch needs at least 4 cells");

    /// Length of the [`to_bytes`](Self::to_bytes) wire form.
    pub const WIRE_BYTES: usize = CELLS * CELL_BYTES;

    /// The sketch of no entries.
    #[must_use]
    pub const fn new() -> Self {
        let () = Self::AT_LEAST_FOUR;
        Self {
            cells: [Cell {
                count: 0,
                ids: 0,
                checks: 0,
            }; CELLS],
        }
    }

    /// Whether every cell is zero: the sketch of no entries, or the
    /// difference of two sketches of the same set.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.cells.iter().all(|c| *c == Cell::default())
    }

    /// Folds in one entry.
    pub fn insert(&mut self, key: &[u8], val: &[u8]) {
        self.toggle(entry_id(key, val), 1);
    }

    /// The sketch of the union of two *disjoint* entry sets, such as two
    /// key ranges.
    #[must_use]
    pub fn merge(&self, other: &Self) -> Self {
        self.combine(other, i32::wrapping_add)
    }

    /// The difference `self - other`: entries both hold cancel. Decode it
    /// to list the entries only `self` holds and those only `other` does.
    #[must_use]
    pub fn subtract(&self, other: &Self) -> Self {
        self.combine(other, i32::wrapping_sub)
    }

    fn combine(&self, other: &Self, op: fn(i32, i32) -> i32) -> Self {
        let mut out = *self;
        for (c, o) in out.cells.iter_mut().zip(&other.cells) {
            c.count = op(c.count, o.count);
            c.ids ^= o.ids;
            c.checks ^= o.checks;
        }
        out
    }

    /// Adds (`delta = 1`) or removes (`delta = -1`) `id` in its four
    /// cells, one in each quarter of the table so they never coincide.
    fn toggle(&mut self, id: u64, delta: i32) {
        let quarter = CELLS / 4;
        // `quarter >= 1`; the remainder is below it, so it fits a usize.
        let width = quarter as u64;
        for (t, seed) in INDEX_SEEDS.iter().enumerate() {
            let off = usize::try_from(mix(id ^ seed) % width).unwrap_or(0);
            let c = &mut self.cells[t * quarter + off];
            c.count = c.count.wrapping_add(delta);
            c.ids ^= id;
            c.checks ^= check(id);
        }
    }

    /// Peels a difference (see [`subtract`](Self::subtract)) into the ids
    /// of the entries only the left side holds (`left`) and only the
    /// right side holds (`right`), and returns how many of each.
    ///
    /// Device-free: `.map_err(Error::widen)` to join a `Db` result.
    ///
    /// # Errors
    ///
    /// [`Error::SketchTooSmall`] when the difference does not fully peel
    /// (too many differences for `CELLS`); [`Error::BufferTooSmall`],
    /// with the length both buffers need, when it peels but `left` or
    /// `right` cannot hold its side. Nothing is written past either
    /// buffer.
    pub fn decode(
        &self,
        left: &mut [u64],
        right: &mut [u64],
    ) -> Result<(usize, usize), Error<Infallible>> {
        let mut s = *self;
        let (mut nl, mut nr) = (0usize, 0usize);
        // Every real entry adds 4 to this, so a decode that peels more
        // has misread a cell; stop rather than spin.
        let mut budget: u64 = self
            .cells
            .iter()
            .map(|c| u64::from(c.count.unsigned_abs()))
            .sum();
        loop {
            let mut peeled = false;
            for i in 0..CELLS {
                let c = s.cells[i];
                let side = match c.count {
                    1 => (&mut *left, &mut nl),
                    -1 => (&mut *right, &mut nr),
                    _ => continue,
                };
                if check(c.ids) != c.checks {
                    continue;
                }
                if budget == 0 {
                    return Err(Error::SketchTooSmall);
                }
                budget -= 1;
                let (out, n) = side;
                if let Some(slot) = out.get_mut(*n) {
                    *slot = c.ids;
                }
                *n += 1;
                s.toggle(c.ids, -c.count);
                peeled = true;
            }
            if !peeled {
                break;
            }
        }
        if !s.is_empty() {
            return Err(Error::SketchTooSmall);
        }
        if nl > left.len() || nr > right.len() {
            return Err(Error::BufferTooSmall { need: nl.max(nr) });
        }
        Ok((nl, nr))
    }

    /// Writes the big-endian wire form, [`WIRE_BYTES`](Self::WIRE_BYTES)
    /// long: per cell, the count, the id sum, then the check sum.
    ///
    /// # Errors
    ///
    /// [`Error::BadBufferLen`] when `out` is not `WIRE_BYTES` long.
    pub fn to_bytes(&self, out: &mut [u8]) -> Result<(), Error<Infallible>> {
        if out.len() != Self::WIRE_BYTES {
            return Err(Error::BadBufferLen);
        }
        for (c, b) in self.cells.iter().zip(out.chunks_exact_mut(CELL_BYTES)) {
            b[..4].copy_from_slice(&c.count.to_be_bytes());
            b[4..12].copy_from_slice(&c.ids.to_be_bytes());
            b[12..].copy_from_slice(&c.checks.to_be_bytes());
        }
        Ok(())
    }

    /// Parses [`to_bytes`](Self::to_bytes)' output.
    ///
    /// # Errors
    ///
    /// [`Error::BadBufferLen`] when `bytes` is not `WIRE_BYTES` long.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error<Infallible>> {
        if bytes.len() != Self::WIRE_BYTES {
            return Err(Error::BadBufferLen);
        }
        let mut s = Self::new();
        for (c, b) in s.cells.iter_mut().zip(bytes.chunks_exact(CELL_BYTES)) {
            let mut count = [0u8; 4];
            let mut ids = [0u8; 8];
            let mut checks = [0u8; 8];
            count.copy_from_slice(&b[..4]);
            ids.copy_from_slice(&b[4..12]);
            checks.copy_from_slice(&b[12..]);
            *c = Cell {
                count: i32::from_be_bytes(count),
                ids: u64::from_be_bytes(ids),
                checks: u64::from_be_bytes(checks),
            };
        }
        Ok(s)
    }
}

/// Folds every entry the positioned `scan` yields into `sketch` (which
/// it does not clear, so several ranges can share one sketch).
///
/// Position the scan first ([`Scan::seek`] or [`Scan::seek_with_time`]):
/// its range, snapshot watermark and TTL clock define what is sketched.
/// `key_buf` and `val_buf` are scratch for the scan; size them `KEY_MAX`
/// and `VAL_MAX`.
///
/// # Errors
///
/// Whatever [`Scan::next`] reports.
pub async fn sketch<
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
    const CELLS: usize,
>(
    scan: &mut Scan<'_, D, BLOCK, KEY_MAX, VAL_MAX, CAP, ARENA, LEVELS, TABLES, BLOOM_BYTES, CACHE>,
    sketch: &mut Sketch<CELLS>,
    key_buf: &mut [u8],
    val_buf: &mut [u8],
) -> Result<(), Error<D::Error>> {
    while let Some((klen, vlen)) = scan.next(key_buf, val_buf).await? {
        sketch.insert(&key_buf[..klen], &val_buf[..vlen]);
    }
    Ok(())
}
