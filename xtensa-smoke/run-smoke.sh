#!/usr/bin/env bash
# Build the horton ESP32-S3 smoke test, bake a flash image, boot it under
# QEMU's esp32s3 machine, and check UART0 for "SMOKE PASS".
#
# Usage: ./run-smoke.sh   (runs from xtensa-smoke/)
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"

# ESP Rust fork on PATH, plus the Xtensa GCC tools (linker).
source ~/export-esp.sh
export PATH="$HOME/.rustup/toolchains/esp/bin:$PATH"
# Private CARGO_HOME so this build never contends on the shared cargo
# package-cache lock with other builds.
export CARGO_HOME="$HERE/.cargo-home"
# The esp fork mis-validates `callx8` in naked_asm with incremental +
# --emit=link; keep incremental off (Tallow's build.sh does the same).
export CARGO_INCREMENTAL=0
mkdir -p "$CARGO_HOME"

cd "$HERE"
cargo build --release 2>&1 | tail -5
python3 mkimage.py

QEMU="$HOME/workspace/tooling/qemu-esp32/qemu/bin/qemu-system-xtensa"
export LD_LIBRARY_PATH="$HOME/workspace/tooling/qemu-esp32/libs/usr/lib/x86_64-linux-gnu${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
LOG="$HERE/build/uart0.log"

timeout 20 "$QEMU" \
  -nographic \
  -machine esp32s3 \
  -drive file="$HERE/build/flash_image.bin",if=mtd,format=raw \
  -serial file:"$LOG" \
  -monitor none \
  -no-reboot || true

echo "--- uart0.log ---"
cat "$LOG"
echo "-----------------"
if grep -q "SMOKE PASS" "$LOG"; then
  echo "SMOKE PASS: horton runs on ESP32-S3 (QEMU)"
else
  echo "SMOKE FAIL: no SMOKE PASS on UART0"
  exit 1
fi
