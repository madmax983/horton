# ADR-0002: `no_std`, no `alloc`, caller-owned const-generic memory

Status: Accepted (v0.1, 2026-09-12; refined the same day)

## Context

horton targets places where `malloc` doesn't exist: firmware, kernels,
bootloaders. The first targets were x86_64 bare metal and a macOS dev
host, with ESP32-S3 (about 320 KiB of RAM) on the v0.6 roadmap. SPEC §10
question 2 asked whether `no_alloc` is a hard line.

## Decision

- `#![no_std]`, no `extern crate alloc`, and an empty `[dependencies]`
  table: only `core` may be used. `std` appears in tests only.
- The default build allocates nothing. One exception is allowed: a tiny,
  dependency-free bump allocator over caller-provided memory, behind the
  default-off `scratch-bump` feature, for internal scratch only. It is
  never required, never global, and never pulls in `alloc`.
- All memory is caller-provided and sized at compile time with const
  generics. Constructors are `const fn` where possible.
- No recursion, no panics in library code, no `unwrap`/`expect`/`assert`
  outside tests. Every failure is a `Result`.
- `unsafe` is disallowed unless SPEC justifies it; the target is zero.
  The crate is `#![forbid(unsafe_code)]` (v0.6). The volatile MMIO half
  of the ESP32-S3 driver lives in the board crate (v0.7).
- Default budget: at most 64 KiB of RAM for memtable plus scratch and
  4 KiB of stack per public call, tunable through consts.

## Consequences

- The RAM bill is known before flashing. The v0.16 ESP32-S3 profile
  measures `Db` 25,576 + `Scan` 10,536 + `Compaction` 56,408 = 92,520
  bytes, asserted against a 98,304-byte budget. That budget was raised
  from 64 KiB to 96 KiB in v0.13.
- Running out of a fixed capacity is a typed error, not an allocation
  failure: `TableFull`/`ArenaFull` (flush), `NoSpace`, and
  `BufferTooSmall { need }` instead of truncation.
- Data structures are chosen to work without allocation: the memtable
  is sorted slots over a bump arena with O(n) insert rather than a
  skiplist (§4.1), and the merge picks winners by linear scan instead
  of a heap (§4.6).
- Capacities are fixed numbers, not growable: for example 8 live
  snapshots (`NoSpace` beyond that, §4.7) and 8 merge inputs (`KMAX`,
  §4.6).
- Scratch moves to the caller or into `Db`: the `Compaction` value
  (v0.4), `CompressScratch` (v0.13), and `Db`'s one-block `get_scratch`
  and `decomp_scratch` buffers (§1, v0.13).
- SPEC permits `scratch-bump` but its first expected use, compaction,
  was built on the caller-owned `Compaction` value instead (§1, §4.6).
- Limit: stack use is not yet within §1's 4 KiB per-call target. In
  debug builds, v0.16 records a ~352 KiB `Manifest::recover` stack
  probe that, together with the inline block cache, pushes stack-heavy
  debug tests near the 2 MiB test-thread limit.

## References

- SPEC §1 (hard constraints), §4.1, §4.6, §4.7, §6, §10 question 2
- [Milestone log](../history/milestones-v0.1-v0.16.md): v0.4, v0.6, v0.7, v0.13, v0.16
