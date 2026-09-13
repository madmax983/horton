//! CRC-32 known-answer tests.

use horton::crc32;

#[test]
fn empty_input() {
    assert_eq!(crc32(b""), 0x0000_0000);
}

#[test]
fn standard_check_vector() {
    // The canonical CRC-32 check value.
    assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
}

#[test]
fn more_vectors() {
    assert_eq!(crc32(b"hello"), 0x3610_A686);
    assert_eq!(
        crc32(b"The quick brown fox jumps over the lazy dog"),
        0x414F_A339
    );
    assert_eq!(crc32(&[0u8; 64]), 0x758D_6336);
}

#[test]
fn differs_across_inputs() {
    assert_ne!(crc32(b"a"), crc32(b"b"));
    assert_ne!(crc32(b"abc"), crc32(b"abd"));
}

#[test]
fn deterministic() {
    let data = b"horton hears a key";
    assert_eq!(crc32(data), crc32(data));
}

/// Bitwise-per-bit reference implementation, independent of `crc32`'s
/// slicing-by-8 table lookups — the oracle for `matches_bitwise_reference`
/// below.
fn crc32_bitwise_reference(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &byte in data {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            let mask = 0u32.wrapping_sub(crc & 1);
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

/// Slicing-by-8 reads 8 bytes per main-loop iteration and falls back to a
/// byte-at-a-time tail; exercise every remainder (0..=8) and a few lengths
/// past the first full 8-byte chunk so both the main loop and the tail loop
/// (and the boundary between them) are checked against an independent
/// bitwise implementation.
#[test]
fn matches_bitwise_reference_across_lengths() {
    let data: Vec<u8> = (0..64u32)
        .map(|i| (i.wrapping_mul(37) % 251) as u8)
        .collect();
    for len in 0..=data.len() {
        let slice = &data[..len];
        assert_eq!(
            crc32(slice),
            crc32_bitwise_reference(slice),
            "mismatch at len={len}"
        );
    }
}
