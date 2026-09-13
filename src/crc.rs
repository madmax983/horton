//! Hand-rolled IEEE CRC-32. Table-driven — no dependency, table computed at
//! compile time (spec §4.2: "256-entry table is also fine, it's `const`").

/// Builds the standard byte-indexed reflected CRC-32 table: `table[n]` is
/// the eight-bit-at-a-time update for input byte `n`, via the same bitwise
/// recurrence the byte loop used before this table existed.
const fn make_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut n = 0;
    while n < 256 {
        let mut crc = n as u32;
        let mut bit = 0;
        while bit < 8 {
            let mask = 0u32.wrapping_sub(crc & 1);
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
            bit += 1;
        }
        table[n] = crc;
        n += 1;
    }
    table
}

const TABLE: [u32; 256] = make_table();

/// Computes the IEEE CRC-32 (polynomial `0xEDB88320`) of `data`.
#[must_use]
pub const fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    let mut i = 0;
    while i < data.len() {
        let idx = ((crc ^ data[i] as u32) & 0xFF) as usize;
        crc = TABLE[idx] ^ (crc >> 8);
        i += 1;
    }
    !crc
}
