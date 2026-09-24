# ADR-0011: Multi-block manifest copies, an optional ring, and staged edits

Status: Accepted (v0.17, 2026-09-24). Supersedes the one-block manifest
slots of ADR-0003; the manifest stays the single commit point.

## Context

- **F7.** Each manifest slot was one block, so the encoded manifest had
  to fit `BLOCK` bytes, and nothing checked that at compile time.
  `TestDb` (`KEY_MAX = 256`) failed every flush once 7 of its 28 tables
  were live.
- **F8.** Every commit staged a full copy of the manifest
  (`let mut staged = self.manifest`), held across the commit's awaits:
  1.7 KiB of every commit future on the ESP32 profile, 15.5 KiB on
  `TestDb`.
- **F9.** Two copies means the manifest sectors wear out first on NOR
  flash (ADR-0012).

## Decision

- **Multi-block copies (format `hrtman05`).** A copy spans
  `Manifest::max_blocks::<BLOCK>()` blocks, the compile-time worst case
  for `LEVELS × TABLES` refs with `KEY_MAX` bounds. Every block carries
  `magic | seq | index | count | chunk len | chunk | crc`. A copy is
  intact only when every block's CRC is valid and all agree on `seq` and
  `count`, so a commit torn between blocks falls back to the previous
  copy. A commit writes only the blocks the body needs. Recovery streams
  the body through one block of scratch.
- **Layouts.** `ManifestLayout::pair(a, b)` is the classic two copies;
  `ManifestLayout::ring(start, n)` keeps `n` copies back to back.
  Commit `seq` goes to copy `seq % n`; recovery takes the newest intact
  copy. `Db::open` checks that the copies, the WAL and the table region
  are disjoint at their real sizes (`Error::BadConfig`).
- **Staged edits.** A commit is described by a `ManifestEdit`: scalar
  updates, removals and narrowings as 64-bit masks over pool positions
  (every narrowing in a commit shares one new first key), and at most
  one added ref. `Manifest::commit_edit` encodes the manifest *as the
  edit would leave it*, writes it, and applies the edit in memory only
  once the commit has landed. A failed commit leaves the in-memory
  manifest exactly as it was, with no copy.
- Const assertions: the block exceeds the 28-byte block frame, and a
  copy spans at most 65,535 blocks.

## Consequences

- The table count is bounded by slots again, not by the manifest.
- The manifest region grows to `copies × max_blocks` blocks: 2 × 4 for
  `TestDb`, 2 × 1 for the ESP32 profile.
- An edit is a few hundred bytes instead of a manifest copy. Edits
  address pool positions, so the manifest must not change between
  staging and commit; `Db` holds `&mut self` across both.
- The encoder and `apply_edit` must agree. A randomized test
  (`committed_edits_match_the_classic_mutations`) checks both against
  the classic mutation API, through commit and recovery.

## References

- `src/manifest.rs`, `Db::manifest_layout`, `Db::check_regions`
- `docs/ARCHITECTURE_REVIEW.md` F7, F8, F9
- `tests/manifest.rs`, `tests/review_findings.rs` (`f7_*`)
