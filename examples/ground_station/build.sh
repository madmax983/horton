#!/usr/bin/env bash
# Builds the ground station's two wasm modules into web/, where the pages
# and the tests load them: ground_station.wasm (the dump viewer, src/) and
# live.wasm (the live logger, live/). Needs the wasm32-unknown-unknown
# target:
#   rustup target add wasm32-unknown-unknown
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"
cargo build --release --target wasm32-unknown-unknown
cp target/wasm32-unknown-unknown/release/horton_ground_station.wasm web/ground_station.wasm
echo "built web/ground_station.wasm ($(wc -c < web/ground_station.wasm) bytes)"
(cd live && cargo build --release --target wasm32-unknown-unknown)
cp live/target/wasm32-unknown-unknown/release/horton_live_logger.wasm web/live.wasm
echo "built web/live.wasm ($(wc -c < web/live.wasm) bytes)"

# Put the latest recording next to the page, for its "Open the latest
# recording" button. (Ignored by git.)
IMG="$HERE/../../target/flight_recorder/flash.img"
if [ -f "$IMG" ]; then
  cp "$IMG" web/flash.img
  echo "copied the latest recording to web/flash.img"
fi
