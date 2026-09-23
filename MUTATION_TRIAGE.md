# Horton mutation-testing triage (reliability pass, 2026-09-23)

Tool: `cargo-mutants 27.1.0` (`~/workspace/tools/cargo-mutants`), `--in-place`,
`CARGO_BUILD_JOBS=1`, `TMPDIR=$HOME/workspace/.mutants-tmp`.
Baseline for the campaigns: `f98f266` + the reliability branches merged.

## Scores

| File | Viable mutants | Killed | Surviving non-equivalent |
|------|---------------|--------|--------------------------|
| `src/batch.rs` | 21 | 21 (tests in `tests/mutants.rs`) | 0 |
| `src/crc.rs` | 100 | 100 (98 caught + 2 infinite-loop timeouts) | 0 |
| `src/memtable.rs` | 118 viable (of 129) | all triaged | 0 (1 equivalent, rest killed; see below) |
| `src/compact.rs` | 72 enumerated | all triaged | 0 (6 equivalent, documented below) |
| `src/wal.rs` | 174 enumerated | all triaged | 0 (4 equivalent, documented below; 8 more killed by new tests, 8 unviable) |

No production bugs were found by any mutant. (Two of the memtable "timeouts"
were infinite-loop mutants — valid kills.) One latent test-oracle subtlety
(tombstone value bytes) was fixed in the tests during the work.

## Equivalent mutants (with the reason, not a shrug)

### `src/memtable.rs:383` — `apply`: `a.seq > max_seq` → `>=`
When `a.seq == max_seq`, the assignment `max_seq = a.seq` writes the same
value back. No observable difference. Equivalent.

### `src/compact.rs:322` — `min_key_cursor`: `<` → `<=`
`best` is consumed only through its key bytes: `head_cursor`'s tie
comparison, `key_len`, and the key `copy_from_slice`/comparison. Two
cursors tied on the minimum key hold byte-identical keys, so promoting the
later tied cursor changes nothing downstream. Equivalent.

### `src/compact.rs:345` — `head_cursor`: `>` → `>=`
Differs only on a sequence tie between two distinct live cursors. Sequence
numbers are globally unique: one monotonic counter (`Db::next_seq`,
resumed above `max_seq` on open), one number per mutation, and each
`(key, seq)` version is resident in exactly one table at a time (flush and
compaction move versions; inputs are dropped from the manifest). Ties are
unreachable, and seqs are not caller-controllable, so no test can construct
one. Equivalent.

### `src/compact.rs:386` — tombstone-drop floor `c.seq < oldest_snapshot` → `<=`
Differs only when `c.seq == oldest_snapshot` (reachable: snapshot taken
after a delete captures `next_seq` equal to the delete's seq). Then the
head — the key's newest version overall — is an effective tombstone at seq
S = oldest; every live snapshot has threshold ≥ S and the live view is
`u64::MAX`, so every reader's newest visible version is the tombstone
itself: all read absent. Dropping the whole key also reads absent. The
distinction (tombstone retained vs key dropped) is not observable through
any public API — `TableReader` exposes only visibility-aware reads and the
archive API works at whole-table granularity. Equivalent.

### `src/compact.rs:412` — emit-check loop `ti < 1 + n_snapshots` → `<=`
The extra iteration reads `snapshots[n_snapshots]`. That slot is always 0:
`Compaction::new` zeroes the array and `Db` overwrites the whole array per
job with `sorted_snapshot_watermarks()`, whose tail beyond `n` is 0.
Mutation seqs are always ≥ 1 (`next_seq` starts at 0, assignment is
`checked_add(1)`), so `seq <= 0` is false and the extra iteration can never
set `emit`. Equivalent.

### `src/compact.rs:443` — served-marking loop `ti < 1 + self.n_snapshots` → `<=`
Same dead slot as above (`seq <= 0` never true), and even a written flag at
`served[1 + n_snapshots]` is never read by any `<`-bounded loop. Equivalent.

### `src/compact.rs:455` — `sealed == PushOutcome::BlockSealed` → `!=`
Changes only `merge_step`'s yield granularity: `More` after every emit
instead of only on a sealed block. `push` already absorbs a seal internally
(the entry goes into a fresh block), and the per-key resume state (`key`,
`key_len`, `KeyState::Merging`, `served`) makes the next `merge_step`
continue exactly, so the output tables and the single manifest commit at
`Exhausted` are identical. The call count of the `pub(crate)` `merge_step`
is not observable through the public API or any integration test.
Equivalent at the API boundary.

## Not yet campaigned (honest gaps)

`src/manifest.rs`, `src/sstable.rs`, `src/scan.rs`, `src/db.rs`
have not been mutation-tested. The WAL decoder is heavily covered by the
structure-aware fuzzers (`tests/fuzz.rs`) and torn-write crash tests
(`tests/crash_torn.rs`, `tests/crash_flush.rs`), which is mitigation, not a
substitute. Rerun per file with the same setup when continuing; note the
suite now includes the slower fuzz corpora, so scope `-- --skip` filters or
raise timeouts accordingly.

## `src/wal.rs` campaign (2026-09-23)

174 mutants enumerated; 154 caught, 8 unviable (`Default::default()`
replacements that do not compile), 12 missed — all 12 triaged:

Killed by new regression tests in `tests/mutants.rs` (each kill verified
by applying the mutant and watching the test fail):

- `174:18` `<` → `==` in `decode_record`: the weakened guard walks past
  the length check into out-of-bounds header indexing (panics at
  `src/wal.rs:190`) on a short torn tail. Killed by
  `mut_wal_short_torn_tail_stops_cleanly`, which packs a 512-byte block
  with 19 records, plants an 18-byte torn tail with valid magic + tiny
  length + valid op, and asserts recovery replays the clean prefix and
  stops without error.
- `301:9` `staged_bytes` → `0`: the getter must report staged bytes;
  `Db::write` drains a non-empty stage before batching (`db.rs:686`).
  Killed by `mut_wal_staged_bytes_tracks_stage`.
- `330:9` `max_seq` → `0` and → `1`: the getter must track the highest
  appended sequence. Killed by `mut_wal_max_seq_tracks_appends`.
- `398:17` `>` → `>=` in `append_inner`: an exact-fit record must pack
  into the current block, not flush early and waste a WAL block. Killed
  by `mut_wal_exact_fit_packs_block`.
- `418:16` `>` → `==` and `>` → `<` in `append_inner`: the max-seq update
  must fire on a new high. Killed by `mut_wal_max_seq_tracks_appends`.
- `549:36` `>` → `>=` in `recover_from`: a record with `seq ==
  seq_floor` is stale (already flushed) and must be skipped, not
  replayed. Killed by `mut_wal_seq_floor_boundary_skips`.

Equivalent (with the reason, not a shrug):

### `src/wal.rs:174` — `decode_record`: `<` → `<=`
Differs only on a buffer of exactly `WAL_HEADER_LEN` (19) bytes. The
original can never return `Some` there: decoding needs `total = len + 6
<= 19`, i.e. `len <= 13`, but the length-consistency check requires `len
= WAL_RECORD_OVERHEAD - 6 + kl + vl + el >= 17` (`kl`, `vl`, `el >= 0`).
`17 > 13` is a contradiction, so the original returns `None` for every
19-byte input — exactly what the mutant does. Equivalent.

### `src/wal.rs:418` — `append_inner`: `>` → `>=`
Differs only when `seq == self.max_seq`; the assignment `self.max_seq =
seq` then writes the identical value back. No observable difference.
Equivalent.

### `src/wal.rs:458` — `write_stage`: `<` → `<=`
Differs only when `stage_len == dirty_to`; the guarded fill then covers
the empty range `stage[stage_len..stage_len]`, which is a no-op. No
observable difference. Equivalent.

### `src/wal.rs:541` — `recover_from`: `while off < BLOCK` → `<=`
Differs only when `off == BLOCK`: the extra iteration scans
`&block[BLOCK..]`, an empty (valid) slice, which is all-zero and yields
`Scan::CleanEnd`, breaking immediately. `off` can never exceed `BLOCK`
(`decode_record` rejects `total > buf.len()`), so no out-of-bounds
access. No observable difference. Equivalent.

Campaign notes: the first run was killed by an exec-service restart at
87/174 with a mutant left applied; the tree was restored and the campaign
restarted from scratch (174/174 in the second run). During triage the
author briefly contaminated the running campaign's tree (an unrelated
edit); the 5 mutants in flight then were re-verified individually on the
clean tree and all 5 are genuinely caught by pre-existing tests
(`torn_block_stops_at_prefix`, `crash_during_flush_is_atomic`,
`crash_across_two_flushes`). No production bugs were found by any
mutant.
