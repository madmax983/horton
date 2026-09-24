#!/usr/bin/env bash
# Build the horton ESP32-S3 smoke test, bake a flash image, boot it under
# QEMU's esp32s3 machine, and check UART0 for "SMOKE PASS".
#
# Usage: ./run-smoke.sh   (can be run from any directory)
#
# Scripts note: every machine-specific path is an environment variable. The
# defaults reproduce the original dev-box layout.
#   ESP_EXPORT_SCRIPT         espup's environment script to source.
#                             Default: $HOME/export-esp.sh. Skipped with a
#                             message if the file does not exist.
#   HORTON_XTENSA_CARGO_HOME  Private CARGO_HOME for this build (absolute
#                             path; ../xtensa-check.sh honors it too).
#                             Default: <this script's dir>/.cargo-home
#   HORTON_QEMU               qemu-system-xtensa binary with the esp32s3
#                             machine (Espressif's QEMU fork).
#                             Default: $HOME/workspace/tooling/qemu-esp32/qemu/bin/qemu-system-xtensa
#   HORTON_QEMU_LIBS          Directory prepended to LD_LIBRARY_PATH for
#                             QEMU's shared libraries.
#                             Default: $HOME/workspace/tooling/qemu-esp32/libs/usr/lib/x86_64-linux-gnu
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"

# ESP Rust fork on PATH, plus the Xtensa GCC tools (linker).
ESP_EXPORT_SCRIPT="${ESP_EXPORT_SCRIPT:-$HOME/export-esp.sh}"
if [ -f "$ESP_EXPORT_SCRIPT" ]; then
  # shellcheck source=/dev/null
  source "$ESP_EXPORT_SCRIPT"
else
  echo "note: $ESP_EXPORT_SCRIPT not found; assuming the ESP toolchain is already on PATH"
fi
export PATH="$HOME/.rustup/toolchains/esp/bin:$PATH"
# Private CARGO_HOME so this build never contends on the shared cargo
# package-cache lock with other builds.
export CARGO_HOME="${HORTON_XTENSA_CARGO_HOME:-$HERE/.cargo-home}"
# The esp fork mis-validates `callx8` in naked_asm with incremental +
# --emit=link; keep incremental off (Tallow's build.sh does the same).
export CARGO_INCREMENTAL=0
mkdir -p "$CARGO_HOME"

cd "$HERE"
cargo build --release 2>&1 | tail -5
python3 mkimage.py

QEMU="${HORTON_QEMU:-$HOME/workspace/tooling/qemu-esp32/qemu/bin/qemu-system-xtensa}"
QEMU_LIBS="${HORTON_QEMU_LIBS:-$HOME/workspace/tooling/qemu-esp32/libs/usr/lib/x86_64-linux-gnu}"
export LD_LIBRARY_PATH="$QEMU_LIBS${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
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
