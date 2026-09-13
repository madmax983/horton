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
