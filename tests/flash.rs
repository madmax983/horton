//! Flash device tests: the erase-before-write discipline, addressing
//! errors, and a full `Db` running on a flash-backed device.
//!
//! The mock is strict NOR flash: erase sets `0xFF`, program only clears
//! bits, and programming a sector that was not erased since the last
//! program is an error. The overwrite test below fails if
//! `FlashBlockDevice` ever programs without erasing first.

mod common;

use common::{block_on, test_config, TestDb};
use core::future::poll_fn;
use horton::device::BlockDevice;
use horton::flash::{Flash, FlashBlockDevice, FlashError};

const SECTOR: usize = 4096;
const N: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MockError {
    BitSet,
    NotErased,
    OutOfBounds,
    Misaligned,
}

/// Strict mock NOR flash over a byte vector.
struct MockFlash {
    mem: Vec<u8>,
    erased: Vec<bool>,
}

impl MockFlash {
    fn new(sectors: usize) -> Self {
        Self {
            mem: vec![0xFF; sectors * SECTOR],
            erased: vec![true; sectors],
        }
    }

    const fn sector_of(&self, addr: u32, len: usize) -> Result<usize, MockError> {
        let end = addr as usize + len;
        if end > self.mem.len() {
            return Err(MockError::OutOfBounds);
        }
        let s = addr as usize / SECTOR;
        if s != (end - 1) / SECTOR {
            return Err(MockError::OutOfBounds);
        }
        Ok(s)
    }
}

impl Flash for MockFlash {
    type Error = MockError;
    const SECTOR: usize = SECTOR;

    fn read(&self, addr: u32, buf: &mut [u8]) -> Result<(), MockError> {
        self.sector_of(addr, buf.len())?;
        buf.copy_from_slice(&self.mem[addr as usize..addr as usize + buf.len()]);
        Ok(())
    }

    fn erase_sector(&mut self, addr: u32) -> Result<(), MockError> {
        if !(addr as usize).is_multiple_of(SECTOR) {
            return Err(MockError::Misaligned);
        }
        let s = self.sector_of(addr, SECTOR)?;
        self.mem[s * SECTOR..(s + 1) * SECTOR].fill(0xFF);
        self.erased[s] = true;
        Ok(())
    }

    fn program(&mut self, addr: u32, data: &[u8]) -> Result<(), MockError> {
        let s = self.sector_of(addr, data.len())?;
        if !self.erased[s] {
            return Err(MockError::NotErased);
        }
        let base = addr as usize;
        for (i, &b) in data.iter().enumerate() {
            let old = self.mem[base + i];
            if old & b != b {
                return Err(MockError::BitSet);
            }
            self.mem[base + i] = old & b;
        }
        self.erased[s] = false;
        Ok(())
    }
}

type Dev = FlashBlockDevice<MockFlash, SECTOR, N>;

fn dev() -> Dev {
    Dev::new(MockFlash::new(N), 0)
}

fn write(dev: &mut Dev, id: u64, buf: &[u8]) -> Result<(), FlashError<MockError>> {
    block_on(poll_fn(|cx| dev.poll_write_block(cx, id, buf)))
}

fn read(dev: &Dev, id: u64, buf: &mut [u8]) -> Result<(), FlashError<MockError>> {
    block_on(poll_fn(|cx| dev.poll_read_block(cx, id, buf)))
}

#[test]
fn write_read_roundtrip() {
    let mut d = dev();
    let mut w = [0u8; SECTOR];
    for (i, b) in w.iter_mut().enumerate() {
        *b = u8::try_from((i * 7 + 3) % 256).unwrap();
    }
    write(&mut d, 5, &w).unwrap();
    let mut r = [0u8; SECTOR];
    read(&d, 5, &mut r).unwrap();
    assert_eq!(w, r);
}

#[test]
fn overwrite_erases_first() {
    // Write all-zeros (clears every bit), then all-ones. Without an
    // erase between them the readback would be all-zeros.
    let mut d = dev();
    write(&mut d, 2, &[0x00; SECTOR]).unwrap();
    write(&mut d, 2, &[0xFF; SECTOR]).unwrap();
    let mut r = [0u8; SECTOR];
    read(&d, 2, &mut r).unwrap();
    assert_eq!(r, [0xFF; SECTOR]);
}

#[test]
fn overwrite_partial_pattern() {
    let mut d = dev();
    write(&mut d, 0, &[0b1010_1010; SECTOR]).unwrap();
    write(&mut d, 0, &[0b0101_0101; SECTOR]).unwrap();
    let mut r = [0u8; SECTOR];
    read(&d, 0, &mut r).unwrap();
    assert_eq!(r, [0b0101_0101; SECTOR]);
}

#[test]
fn block_out_of_range() {
    let mut d = dev();
    let buf = [0u8; SECTOR];
    assert_eq!(
        write(&mut d, N as u64, &buf),
        Err(FlashError::BlockOutOfRange { id: N as u64 })
    );
    let mut r = [0u8; SECTOR];
    assert_eq!(
        read(&d, N as u64, &mut r),
        Err(FlashError::BlockOutOfRange { id: N as u64 })
    );
}

#[test]
fn flush_is_noop_ok() {
    let mut d = dev();
    block_on(poll_fn(|cx| d.poll_flush(cx))).unwrap();
}

#[test]
#[should_panic(expected = "assertion failed")]
fn unaligned_base_panics() {
    let _ = Dev::new(MockFlash::new(N), 128);
}

#[test]
fn db_runs_on_flash() {
    // The standard test layout needs the WAL + table regions: 4224 blocks.
    const BIG_N: usize = 4224;
    type BigDev = FlashBlockDevice<MockFlash, SECTOR, BIG_N>;
    let d = BigDev::new(MockFlash::new(BIG_N), 0);
    let mut db: TestDb<BigDev> = TestDb::new(d, test_config());
    block_on(db.open()).unwrap();
    block_on(db.put(b"esp32", b"s3")).unwrap();
    block_on(db.put(b"flash", b"nor")).unwrap();
    let mut buf = [0u8; 16];
    let n = block_on(db.get(b"esp32", &mut buf)).unwrap().unwrap();
    assert_eq!(&buf[..n], b"s3");
    block_on(db.flush()).unwrap();
    let n = block_on(db.get(b"flash", &mut buf)).unwrap().unwrap();
    assert_eq!(&buf[..n], b"nor");
}
