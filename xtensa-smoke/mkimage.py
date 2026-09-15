#!/usr/bin/env python3
"""Build a QEMU-bootable SPI flash image for the horton ESP32-S3 smoke test.

Recipe (same as Tallow's kernel):
  1. `esptool elf2image` -> converts the ELF's LOAD segments into the ESP
     image format (magic 0xE9, segment headers, entry point, checksum) that
     the S3 ROM bootloader understands.
  2. Assemble a 4 MiB flash image (0xFF = erased) with that image at offset
     0x0 (S2/S3/C3 ROM loads the boot image from flash offset 0x0).

QEMU only accepts flash sizes 2/4/8/16 MiB; we use 4 MiB.
"""
import os
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent

ELF = HERE / "target" / "xtensa-esp32s3-none-elf" / "release" / "horton-smoke"
BIN = HERE / "build" / "horton-smoke.bin"
FLASH = HERE / "build" / "flash_image.bin"
FLASH_SIZE = 4 * 1024 * 1024
IMAGE_OFFSET = 0x0


def main() -> None:
    (HERE / "build").mkdir(exist_ok=True)
    if not ELF.exists():
        sys.exit(f"ELF not found: {ELF} (run ./run-smoke.sh first)")

    pylibs = Path.home() / "workspace" / "esptools" / "py"
    env = dict(os.environ)
    env["PYTHONPATH"] = str(pylibs) + os.pathsep + env.get("PYTHONPATH", "")

    subprocess.run(
        [
            sys.executable, "-m", "esptool",
            "--chip", "esp32s3",
            "elf2image",
            "--flash-mode", "dio",
            "--flash-freq", "80m",
            "--flash-size", "4MB",
            "-o", str(BIN),
            str(ELF),
        ],
        check=True,
        env=env,
    )
    app = BIN.read_bytes()
    print(f"boot image: {len(app)} bytes")

    flash = bytearray(b"\xff" * FLASH_SIZE)
    flash[IMAGE_OFFSET : IMAGE_OFFSET + len(app)] = app
    FLASH.write_bytes(flash)
    print(f"flash image: {FLASH} ({FLASH_SIZE} bytes, app at 0x{IMAGE_OFFSET:x})")


if __name__ == "__main__":
    main()
