#!/usr/bin/env bash
# v0.6 build gate: the horton library must compile for xtensa-esp32s3-none-elf
# with the ESP Rust fork (core built from source; the crate is no_std and
# depends on nothing but core, so this is the whole port surface).
#
# Usage: ./xtensa-check.sh   (runs from the horton repo root)
set -euo pipefail
source ~/export-esp.sh
export PATH="$HOME/.rustup/toolchains/esp/bin:$PATH"
export CARGO_HOME="$HOME/workspace/horton/xtensa-smoke/.cargo-home"
export CARGO_INCREMENTAL=0
mkdir -p "$CARGO_HOME"
cd "$(dirname "$0")"
cargo build -Z build-std=core --target xtensa-esp32s3-none-elf --lib --offline
echo "XTENSA OK: horton builds for xtensa-esp32s3-none-elf"
