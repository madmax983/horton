//! ESP32-S3 SPI flash driver tests.
//!
//! The driver (`horton::esp32s3::SpiFlash`) is generic over a `RegBus`
//! (register read/write). Here the bus is backed by an emulated NOR chip
//! that behaves like the real silicon: WREN sets WEL, SE/PP require WEL
//! and set WIP for a few status polls, RDSR reports WIP/WEL, and the
//! user-mode 0x03 command reads the array. The tests assert the driver's
//! exact register sequences — they are the executable proof of the
//! ESP-IDF LL command flows, since QEMU does not emulate the `SPI_MEM`
//! user-command path (see SPEC.md §9, v0.7).
//!
//! `std` is fine here — tests only.

mod common;

use common::{block_on, test_config, TestDb};
use core::cell::RefCell;
use horton::esp32s3::{
    RegBus, SpiError, SpiFlash, CMD_PP, CMD_RDSR, CMD_SE, CMD_USR, CMD_WREN, REG_ADDR, REG_CMD,
    REG_CTRL, REG_MISO_DLEN, REG_RD_STATUS, REG_USER, REG_USER1, REG_USER2, REG_W0, STATUS_WEL,
    STATUS_WIP, USER_USR_ADDR, USER_USR_COMMAND, USER_USR_MISO,
};
use horton::flash::{Flash, FlashBlockDevice};
use std::rc::Rc;

const SECTOR: usize = 4096;
/// Blocks for the unit tests (small, fast).
const SMALL_SECTORS: usize = 16;
/// Blocks for the full-Db test: the standard test geometry lays out
/// manifest slots 0/1, WAL `[8, 136)`, tables `[136, 4224)`.
const DB_SECTORS: usize = 4224;

/// How many RDSR polls observe WIP=1 after an erase / program in the mock.
const ERASE_POLLS: u32 = 3;
const PROG_POLLS: u32 = 2;

/// What the emulated chip does instead of behaving like silicon.
#[derive(Clone, Copy, PartialEq, Eq)]
enum MockMode {
    /// Behave like a real NOR chip.
    Normal,
    /// Command bits never self-clear and WIP never clears (timeout test).
    Hang,
    /// Only the sector-erase command bit never self-clears, so the
    /// erase's own `spin_cmd` times out (CTRL-restore test).
    HangSe,
    /// `WREN` does not set `WEL` (write-enable failure test).
    DenyWel,
}

struct MockState {
    regs: [u32; 64],
    chip: Vec<u8>,
    wel: bool,
    wip_polls: u32,
    /// Fault injection for the timeout / write-enable failure tests.
    mode: MockMode,
    /// Every register write, in order: the exact-sequence oracle.
    log: Vec<(usize, u32)>,
    /// Program-op observations.
    pp_addrs: Vec<u32>,
    pp_lens: Vec<usize>,
    pp_without_wel: bool,
    se_without_wel: bool,
    usr_reads: u32,
}

impl MockState {
    fn new(sectors: usize) -> Self {
        Self {
            regs: [0; 64],
            chip: vec![0xFF; sectors * SECTOR],
            wel: false,
            wip_polls: 0,
            mode: MockMode::Normal,
            log: Vec::new(),
            pp_addrs: Vec::new(),
            pp_lens: Vec::new(),
            pp_without_wel: false,
            se_without_wel: false,
            usr_reads: 0,
        }
    }

    const fn reg(&self, offset: usize) -> u32 {
        self.regs[offset / 4]
    }

    const fn set_reg(&mut self, offset: usize, v: u32) {
        self.regs[offset / 4] = v;
    }

    /// Emulate one NOR status read: report WIP/WEL, then age the WIP.
    fn rd_status(&mut self) -> u32 {
        let s = u32::from(self.wip_polls > 0) * STATUS_WIP + u32::from(self.wel) * STATUS_WEL;
        if self.mode != MockMode::Hang && self.wip_polls > 0 {
            self.wip_polls -= 1;
        }
        s
    }

    fn cmd_write(&mut self, mut v: u32) {
        if v & CMD_WREN != 0 {
            if self.mode != MockMode::DenyWel {
                self.wel = true;
            }
            v &= !CMD_WREN;
        }
        if v & CMD_SE != 0 {
            if self.mode == MockMode::HangSe {
                // Leave the bit set: the erase command never completes,
                // so the driver's spin on it times out.
            } else if self.wel {
                let a = (self.reg(REG_ADDR) & 0x00FF_FFFF) as usize;
                self.chip[a..a + SECTOR].fill(0xFF);
                self.wel = false;
                self.wip_polls = ERASE_POLLS;
            } else {
                self.se_without_wel = true;
            }
            if self.mode != MockMode::HangSe {
                v &= !CMD_SE;
            }
        }
        if v & CMD_PP != 0 {
            if self.wel {
                let ar = self.reg(REG_ADDR);
                let a = (ar & 0x00FF_FFFF) as usize;
                let len = (ar >> 24) as usize;
                debug_assert!((1..=64).contains(&len));
                for i in 0..len {
                    let w = self.reg(REG_W0 + (i / 4) * 4);
                    let b = w.to_le_bytes()[i % 4];
                    let slot = &mut self.chip[a + i];
                    // Real NOR: programming only clears bits (1 -> 0).
                    *slot &= b;
                }
                self.pp_addrs.push(u32::try_from(a).unwrap());
                self.pp_lens.push(len);
                self.wel = false;
                self.wip_polls = PROG_POLLS;
            } else {
                self.pp_without_wel = true;
            }
            v &= !CMD_PP;
        }
        if v & CMD_RDSR != 0 {
            let s = self.rd_status();
            self.set_reg(REG_RD_STATUS, s);
            v &= !CMD_RDSR;
        }
        if v & CMD_USR != 0 {
            let user = self.reg(REG_USER);
            let user2 = self.reg(REG_USER2);
            let command = user2 & 0xFFFF;
            let miso_bits = self.reg(REG_MISO_DLEN) & 0x3FF;
            if command == 0x03
                && user & USER_USR_COMMAND != 0
                && user & USER_USR_ADDR != 0
                && user & USER_USR_MISO != 0
            {
                let n = ((miso_bits + 1) / 8) as usize;
                let a = (self.reg(REG_ADDR) & 0x00FF_FFFF) as usize;
                // Stage the bytes into W0..W15, little-endian words.
                for (i, w) in self.regs[REG_W0 / 4..REG_W0 / 4 + 16]
                    .iter_mut()
                    .enumerate()
                {
                    let mut word = 0u32;
                    for j in 0..4 {
                        let k = i * 4 + j;
                        if k < n {
                            word |= u32::from(self.chip[a + k]) << (j * 8);
                        }
                    }
                    *w = word;
                }
                self.usr_reads += 1;
            }
            v &= !CMD_USR;
        }
        // Any handled bit self-clears like the hardware SC bits; the
        // driver always writes whole CMD values, so the stored value is
        // just the unhandled remainder.
        self.set_reg(REG_CMD, v);
    }
}

/// Mock `SPI_MEM` register bus backed by the emulated `NOR` chip.
///
/// The state lives behind an `Rc`, so two driver instances can share one
/// emulated chip — that models the flash surviving a reboot, which the
/// reopen test below relies on.
#[derive(Clone)]
struct MockBus {
    state: Rc<RefCell<MockState>>,
}

impl MockBus {
    fn new(sectors: usize) -> Self {
        Self {
            state: Rc::new(RefCell::new(MockState::new(sectors))),
        }
    }

    fn with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&MockState) -> R,
    {
        f(&self.state.borrow())
    }

    fn with_mut<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut MockState) -> R,
    {
        f(&mut self.state.borrow_mut())
    }
}

impl RegBus for MockBus {
    fn read(&self, offset: usize) -> u32 {
        self.with(|s| s.reg(offset))
    }

    fn write(&self, offset: usize, value: u32) {
        self.with_mut(|s| {
            s.log.push((offset, value));
            if offset == REG_CMD {
                s.cmd_write(value);
            } else {
                s.set_reg(offset, value);
            }
        });
    }
}

fn driver_for(sectors: usize) -> SpiFlash<MockBus> {
    SpiFlash::new(
        MockBus::new(sectors),
        0,
        u32::try_from(sectors * SECTOR).unwrap(),
    )
}

fn driver() -> SpiFlash<MockBus> {
    driver_for(SMALL_SECTORS)
}

/// First `n` register writes as (offset, value) pairs.
fn writes(bus: &MockBus, n: usize) -> Vec<(usize, u32)> {
    bus.with(|s| s.log.iter().take(n).copied().collect())
}

#[test]
fn erase_issues_wren_addr_se_sequence() {
    let mut flash = driver();
    // Dirty the sector first so the erase is observable.
    flash.bus().with_mut(|s| s.chip[0x1000..0x2000].fill(0x00));
    flash.erase_sector(0x1000).unwrap();

    let bus = flash.bus();
    let w = writes(bus, 6);
    assert_eq!(w[0], (REG_CMD, CMD_WREN), "first: write enable");
    // WREN self-clears; the driver then checks WEL via RDSR.
    assert_eq!(w[1], (REG_CMD, CMD_RDSR), "second: RDSR for WEL check");
    assert_eq!(w[2], (REG_ADDR, 0x1000), "third: sector address");
    assert_eq!(w[4], (REG_CMD, CMD_SE), "fifth: sector erase");
    // CTRL is saved, cleared for the erase, restored after.
    assert_eq!(w[3].0, REG_CTRL);
    assert_eq!(w[5].0, REG_CTRL);

    // The chip really erased, and WEL cleared after the operation.
    let erased = bus.with(|s| s.chip[0x1000..0x2000].iter().all(|&b| b == 0xFF));
    assert!(erased);
    assert!(!bus.with(|s| s.wel));
    assert!(!bus.with(|s| s.se_without_wel), "erase without WEL");
}

#[test]
fn program_chunks_64_bytes_and_page_boundaries() {
    let mut flash = driver();
    flash.erase_sector(0).unwrap();
    let data = [0xA5u8; 200];
    // Starts mid-page (addr 100), spans the 256-byte page boundary.
    flash.program(100, &data).unwrap();

    let bus = flash.bus();
    // 200 bytes from 100: [100,164) [164,228) [228,256) [256,300).
    assert_eq!(bus.with(|s| s.pp_lens.clone()), [64, 64, 28, 44]);
    assert_eq!(bus.with(|s| s.pp_addrs.clone()), [100, 164, 228, 256]);
    assert!(!bus.with(|s| s.pp_without_wel), "program without WEL");

    let mut back = [0u8; 200];
    flash.read(100, &mut back).unwrap();
    assert_eq!(back, data);
}

#[test]
fn program_only_clears_bits() {
    let mut flash = driver();
    flash.erase_sector(0).unwrap();
    flash.program(0, &[0xF0; 16]).unwrap();
    // Real NOR: 0xF0 & 0x0F = 0x00 — the 0->1 attempts are ignored.
    flash.program(0, &[0x0F; 16]).unwrap();
    let mut back = [0u8; 16];
    flash.read(0, &mut back).unwrap();
    assert_eq!(back, [0x00; 16]);
}

#[test]
fn read_uses_user_mode_transactions() {
    let mut flash = driver();
    flash.erase_sector(0).unwrap();
    let data: Vec<u8> = (0..200u32)
        .map(|i| u8::try_from((i * 7 + 3) % 256).unwrap())
        .collect();
    flash.program(0, &data).unwrap();

    let bus = flash.bus();
    bus.with_mut(|s| s.usr_reads = 0);
    let mut back = vec![0u8; 200];
    flash.read(0, &mut back).unwrap();
    assert_eq!(back, data);
    // 200 bytes in 64-byte user-mode transactions: 4 reads.
    assert_eq!(bus.with(|s| s.usr_reads), 4);

    // The command programmed was 0x03 with command+address+MISO phases.
    let has = |off: usize, val: u32| bus.with(|s| s.log.iter().any(|&(o, v)| o == off && v == val));
    assert!(has(REG_USER2, (7 << 28) | 0x03), "8-bit command 0x03");
    assert!(has(REG_USER1, 23 << 26), "24-bit address phase");
    assert!(
        has(REG_USER, USER_USR_COMMAND | USER_USR_ADDR | USER_USR_MISO),
        "command+address+MISO phases"
    );
    assert!(has(REG_CMD, CMD_USR), "user-mode start");
}

#[test]
fn erase_times_out_when_wip_never_clears() {
    let mut flash = driver();
    flash.bus().with_mut(|s| s.mode = MockMode::Hang);
    assert_eq!(flash.erase_sector(0), Err(SpiError::Timeout));
}

#[test]
fn erase_timeout_restores_ctrl() {
    let mut flash = driver();
    // A distinctive CTRL value so restoration is observable.
    flash.bus().with_mut(|s| {
        s.set_reg(REG_CTRL, 0xDEAD_BEEF);
        s.mode = MockMode::HangSe;
    });
    assert_eq!(flash.erase_sector(0x1000), Err(SpiError::Timeout));
    // The erase's own command spin timed out, but the boot-configured
    // read mode in CTRL must still be restored on the error path.
    assert_eq!(
        flash.bus().with(|s| s.reg(REG_CTRL)),
        0xDEAD_BEEF,
        "CTRL must be restored even when the erase spin times out"
    );
}

#[test]
fn erase_fails_when_write_enable_denied() {
    let mut flash = driver();
    flash.bus().with_mut(|s| s.mode = MockMode::DenyWel);
    assert_eq!(flash.erase_sector(0), Err(SpiError::WriteEnableFailed));
}

#[test]
fn addressing_errors() {
    let mut flash = driver();
    let chip_len = u32::try_from(SMALL_SECTORS * SECTOR).unwrap();
    assert_eq!(flash.erase_sector(0x1001), Err(SpiError::Misaligned));
    assert_eq!(flash.erase_sector(chip_len), Err(SpiError::OutOfRange));
    let mut buf = [0u8; 8];
    assert_eq!(
        flash.read(chip_len - 4, &mut buf),
        Err(SpiError::OutOfRange)
    );
    assert_eq!(
        flash.program(chip_len - 4, &[0u8; 8]),
        Err(SpiError::OutOfRange)
    );
    // Zero-length program at the very end is fine.
    flash.program(chip_len, &[]).unwrap();
}

// ---------------------------------------------------------------------------
// Full database on the real driver code path.
// ---------------------------------------------------------------------------

type SpiDev = FlashBlockDevice<SpiFlash<MockBus>, SECTOR, DB_SECTORS>;

/// Total emulated chip size for the full-Db test (`DB_SECTORS` × 4 KiB
/// sectors); written as literals so `spi_dev` can stay `const`.
const DB_LEN: u32 = 4224 * 4096;

const fn spi_dev(bus: MockBus) -> SpiDev {
    SpiDev::new(SpiFlash::new(bus, 0, DB_LEN), 0)
}

fn db_put(db: &mut TestDb<SpiDev>, k: &[u8], v: &[u8]) {
    block_on(db.put(k, v)).unwrap();
}

fn db_get(db: &TestDb<SpiDev>, k: &[u8]) -> Option<Vec<u8>> {
    let mut buf = [0u8; 2048];
    block_on(db.get(k, &mut buf))
        .unwrap()
        .map(|n| buf[..n].to_vec())
}

#[test]
fn db_on_spi_flash_driver() {
    let bus = MockBus::new(DB_SECTORS);
    let mut db = TestDb::new(spi_dev(bus.clone()), test_config());
    block_on(db.open()).unwrap();

    for i in 0..24u8 {
        db_put(&mut db, &[i], &[i.wrapping_mul(3), i.wrapping_add(7)]);
    }
    for i in 0..24u8 {
        assert_eq!(
            db_get(&db, &[i]),
            Some(vec![i.wrapping_mul(3), i.wrapping_add(7)])
        );
    }
    block_on(db.flush()).unwrap();
    for i in 0..24u8 {
        assert_eq!(
            db_get(&db, &[i]),
            Some(vec![i.wrapping_mul(3), i.wrapping_add(7)])
        );
    }

    // Overwrite through the erase discipline: bits must come back.
    db_put(&mut db, &[7], &[0xFF; 8]);
    assert_eq!(db_get(&db, &[7]), Some(vec![0xFF; 8]));
    drop(db);

    // "Reboot": a fresh driver over the same emulated chip.
    let mut db2 = TestDb::new(spi_dev(bus), test_config());
    block_on(db2.open()).unwrap();
    for i in 0..24u8 {
        let want = if i == 7 {
            vec![0xFF; 8]
        } else {
            vec![i.wrapping_mul(3), i.wrapping_add(7)]
        };
        assert_eq!(db_get(&db2, &[i]), Some(want), "reopened key {i}");
    }
}
