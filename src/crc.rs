//! Hand-rolled IEEE CRC-32. Slicing-by-8, table-driven — no dependency,
//! tables computed at compile time (spec §4.2: "256-entry table is also
//! fine, it's `const`"; this uses eight such tables so the main loop
//! consumes 8 bytes per iteration instead of 1).

/// Builds the eight slicing-by-8 tables. `TABLES[0]` is the standard
/// byte-indexed reflected CRC-32 table: `TABLES[0][n]` is the eight-bit-at-
/// a-time update for input byte `n`, via the same bitwise recurrence the
/// original byte loop used. `TABLES[k]` for `k > 0` is `TABLES[0]` applied
/// again to `TABLES[k - 1]`'s output (i.e. the update for a byte that sits
/// `k` positions further back in the stream). That lets the main loop
/// combine 8 bytes' worth of update with 8 independent table lookups —
/// XORed together, no data dependency between them except on the previous
/// iteration's `crc` — instead of 8 sequential single-byte steps.
const fn make_tables() -> [[u32; 256]; 8] {
    let mut tables = [[0u32; 256]; 8];
    let mut n = 0;
    while n < 256 {
        let mut crc = n as u32;
        let mut bit = 0;
        while bit < 8 {
            let mask = 0u32.wrapping_sub(crc & 1);
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
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

const TABLES: [[u32; 256]; 8] = make_tables();

/// Computes the IEEE CRC-32 (polynomial `0xEDB88320`) of `data`.
///
/// `#[inline(always)]` is measured, not decorative: this loop body is bigger
/// than the byte-at-a-time version it replaced, and LLVM's inliner declines
/// to inline it into call sites (e.g. `check_block_crc`) on its own — which
/// *loses* the win, because those callers pass a compile-time-known length
/// (`BLOCK` is a const generic) that only pays off once inlining lets LLVM
/// specialize the loop bounds for it. Confirmed with `callgrind` on
/// `benches/write_path.rs`: slicing-by-8 without the hint *regresses*
/// (517,750,659 -> 525,534,917 Ir); with it, 517,750,659 -> 340,918,564 Ir
/// (-34.15%). See the PR for the full before/after.
#[must_use]
#[inline(always)]
pub const fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    let len = data.len();
    let chunks = len / 8;
    let mut i = 0;
    while i < chunks {
        let o = i * 8;
        let one = u32::from_le_bytes([data[o], data[o + 1], data[o + 2], data[o + 3]]) ^ crc;
        let two = u32::from_le_bytes([data[o + 4], data[o + 5], data[o + 6], data[o + 7]]);
        crc = TABLES[7][(one & 0xFF) as usize]
            ^ TABLES[6][((one >> 8) & 0xFF) as usize]
            ^ TABLES[5][((one >> 16) & 0xFF) as usize]
            ^ TABLES[4][((one >> 24) & 0xFF) as usize]
            ^ TABLES[3][(two & 0xFF) as usize]
            ^ TABLES[2][((two >> 8) & 0xFF) as usize]
            ^ TABLES[1][((two >> 16) & 0xFF) as usize]
            ^ TABLES[0][((two >> 24) & 0xFF) as usize];
        i += 1;
    }
    let mut j = chunks * 8;
    while j < len {
        let idx = ((crc ^ data[j] as u32) & 0xFF) as usize;
        crc = TABLES[0][idx] ^ (crc >> 8);
        j += 1;
    }
    !crc
}
