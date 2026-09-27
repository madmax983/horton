#!/usr/bin/env bash
# v0.6 build gate: the horton library must compile for xtensa-esp32s3-none-elf
# with the ESP Rust fork (core built from source; the crate is no_std and
# depends on nothing but core, so this is the whole port surface).
#
# Usage: ./xtensa-check.sh   (can be run from any directory)
#
# Scripts note: every machine-specific path is an environment variable. The
# defaults reproduce the original dev-box setup.
#   ESP_EXPORT_SCRIPT         espup's environment script to source.
#                             Default: $HOME/export-esp.sh. Skipped with a
#                             message if the file does not exist (in CI the
#                             toolchain action has already set PATH).
#   HORTON_XTENSA_CARGO_HOME  Private CARGO_HOME for this build (absolute path).
#                             Default: <this script's dir>/xtensa-smoke/.cargo-home
#   HORTON_XTENSA_OFFLINE     1 (default) passes --offline, which needs
#                             CARGO_HOME to already hold build-std's crates.
#                             Set to 0 on a fresh machine so cargo can fetch
#                             them.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"

ESP_EXPORT_SCRIPT="${ESP_EXPORT_SCRIPT:-$HOME/export-esp.sh}"
if [ -f "$ESP_EXPORT_SCRIPT" ]; then
  # shellcheck source=/dev/null
  source "$ESP_EXPORT_SCRIPT"
else
  echo "note: $ESP_EXPORT_SCRIPT not found; assuming the ESP toolchain is already on PATH"
fi
export PATH="$HOME/.rustup/toolchains/esp/bin:$PATH"
export CARGO_HOME="${HORTON_XTENSA_CARGO_HOME:-$HERE/xtensa-smoke/.cargo-home}"
export CARGO_INCREMENTAL=0
mkdir -p "$CARGO_HOME"
OFFLINE="--offline"
if [ "${HORTON_XTENSA_OFFLINE:-1}" = 0 ]; then
  OFFLINE=""
fi
cd "$HERE"
cargo build -Z build-std=core --target xtensa-esp32s3-none-elf --lib ${OFFLINE:+"$OFFLINE"}
echo "XTENSA OK: horton builds for xtensa-esp32s3-none-elf"
