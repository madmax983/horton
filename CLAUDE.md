# horton

A `no_std`, no-alloc, zero-dependency LSM-tree key-value store for
firmware. Start with `README.md` (what it is, sizing), `SPEC.md` (format
and protocol) and `docs/adr/` (why it is the way it is).

## How we work

- **One maintainer.** It is Mark and his agents; there is no one else to
  wait on. Do not ask permission to fix something that is broken.
- **Never let red sit.** A red CI job on a PR you are working on is the
  next thing you do, whether or not your change caused it. Fix it in the
  current PR and say so in the PR.
- **Trunk breakage gets fixed where you find it.** If `main` is red, carry
  the fix in your PR. Do not wait for a separate one.
- **Red does not merge.** A PR is done when every CI job is green.
- **Assume your change reaches further than you think.** Code behind a
  feature flag (`multiwriter`, `loom`) and the separate crates
  (`examples/ground_station`, `xtensa-smoke`) are not built by a plain
  `cargo clippy` or `cargo test`. Run the commands below before you
  call anything clean.

## Before pushing: what CI runs

```sh
L="-D warnings -W clippy::pedantic -W clippy::nursery"
cargo fmt --all --check
cargo clippy --all-targets -- $L
cargo clippy --all-targets --features multiwriter -- $L   # tests/drainer.rs etc. only build here
cargo clippy --all-targets --features loom -- $L
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --features multiwriter
cargo test                                   # and --release
cargo test --features multiwriter
cargo test --release --features loom --test loom_ring
cargo run --example quickstart
cargo run --release --example flight_recorder -- torture 300
```

The host key-value store (`examples/kvstore`, CI runs it on Linux, macOS
and Windows; `cargo test` also runs its unit tests, `test = true` in
`Cargo.toml`):

```sh
cargo test --release --example kvstore
cargo run --release --example kvstore -- demo
cargo run --release --example kvstore -- crash 25
cargo run --release --example kvstore -- --no-sync crash 25
cargo run --release --example kvstore -- --no-sync bench --num 30000
# plus the put/get/scan/del/delrange/compact checks in ci.yml's kvstore job
```

The kvstore's `FileDevice` has `cfg(unix)` and `cfg(windows)` halves;
only one of them builds on your machine. Keep them in step.

The ground station (`examples/ground_station` and `examples/ground_station/live`,
two crates of their own, wasm32):

```sh
rustup target add wasm32-unknown-unknown
for c in examples/ground_station examples/ground_station/live; do
  (cd "$c" && cargo fmt --check &&
    cargo clippy --release --target wasm32-unknown-unknown -- $L)
done
cargo run --release --example flight_recorder -- --fresh --fast --ticks 12000
examples/ground_station/build.sh
node examples/ground_station/test.mjs target/flight_recorder/flash.img --newest 11999
node examples/ground_station/live-test.mjs 300 --export target/live-export
cargo run --release --example flight_recorder -- restore --dir target/live-export
(cd examples/ground_station && node browser-test.mjs && node live-browser-test.mjs)   # need playwright
```

`.github/workflows/ci.yml` is the source of truth. If this list and CI
disagree, CI wins; update this file.

## Rules the code keeps

- The library uses only `core`: no dependencies (except the optional,
  test-only `loom`), no `alloc`, `#![forbid(unsafe_code)]`, no panics.
  Every failure is an `Error` variant.
- Every architecture-review finding has a regression test in
  `tests/review_findings.rs`; CI fails if one is `#[ignore]`d.
- The on-disk format may change between minor versions before 1.0, but
  old images must be rejected, never misread.
- `examples/flight_recorder/format.rs` is shared with the ground station's
  two modules. Change the recorder's shape or archive format there, and
  all three follow.
