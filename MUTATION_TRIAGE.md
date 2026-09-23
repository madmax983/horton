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
| `src/compact.rs` | 72 enumerated | all triaged | 0 (5 equivalent, documented below) |

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

`src/wal.rs`, `src/manifest.rs`, `src/sstable.rs`, `src/scan.rs`, `src/db.rs`
have not been mutation-tested. The WAL decoder is heavily covered by the
structure-aware fuzzers (`tests/fuzz.rs`) and torn-write crash tests
(`tests/crash_torn.rs`, `tests/crash_flush.rs`), which is mitigation, not a
substitute. Rerun per file with the same setup when continuing; note the
suite now includes the slower fuzz corpora, so scope `-- --skip` filters or
raise timeouts accordingly.
