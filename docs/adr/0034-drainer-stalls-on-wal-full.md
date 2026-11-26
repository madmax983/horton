# ADR-0034: The drainer stalls on `WalFull` and recovers in place

Status: Accepted (unreleased, 2026-11-26)

## Context

Issue #47. On the README quickstart shape (64 memtable entries, 78 WAL
blocks), writers that trickle in give one write a sweep. Each sweep
takes one WAL block. The WAL runs out before the memtable fills.

1. **`WalFull` poisoned the drainer.** `sweep` stalled only on
   `TableFull` and `ArenaFull`. `WalFull` has the same remedy (flush,
   then retry, ADR-0013), and `Db::write` refuses it before any I/O
   (ADR-0029). The pipeline stopped at write 78.
2. **No compaction through the drainer.** After a fix for 1, the next
   flush gets `NeedsCompaction`. The drainer had no way to compact. The
   host had to take the `Db` out with `into_db`.
3. **A rebuild stopped `durable`.** `into_db` drops the pending
   tickets. A new drainer on the same ring and `durable` drains the
   tickets after them. `advance_durable` expects `durable` to be the
   next ticket, so `durable` stays at the first dropped ticket. No
   later ticket completes, but its write is in the `Db`.

## Decision

**The host answers capacity errors and device errors in place. It does
not take the `Db` out of the drainer.**

- `sweep` returns `SweepOutcome::Stalled` on `WalFull`, as on
  `TableFull` and `ArenaFull`. The pending tickets stay. The host calls
  `Drainer::flush` (it wraps the WAL) and sweeps again.
- `Drainer::compact_step(scratch)` runs `Db::compact_step` on the owned
  `Db`. The host uses it when `Drainer::flush` returns
  `NeedsCompaction`.
- `Drainer::resume()` clears the poison. The pending tickets stay, and
  the next sweep writes them again before it drains new tickets. A
  device error that failed a batch left it unapplied. If its blocks
  replay at the next open, the retry has the same operations in the
  same order, so the result is the same.
- The docs of `into_db` say that it drops the pending tickets, and
  what this does to a new drainer.

Not done:

- **`Stalled` does not give its cause.** The remedy is the same for all
  three errors. A new field would change the API for no new action.
- **`into_db` does not refuse pending tickets.** A host that shuts down
  needs the `Db` back. It fails the dropped writers itself.
- **No API sets `durable` past a dropped ticket.** That would complete
  writers whose writes are not durable (SPEC O1).

## Consequences

- A lightly loaded node runs on: the host loop that answers `Stalled`
  with a flush now also answers a full WAL.
- `Stalled` no longer means only "memtable full". A host that counts
  stalls as memtable flushes counts WAL wraps too.
- A host with a device error can retry without a new ring.
- A bad payload or a key that is too large poisons again after
  `resume`. The pipeline cannot skip one ticket.

## References

- Issue #47.
- `src/drainer.rs` (`sweep`, `resume`, `compact_step`, `into_db`).
- `tests/drainer.rs` (`wal_full_stalls_and_keeps_tickets`,
  `one_write_per_sweep_runs_on`,
  `compact_step_answers_needs_compaction_in_place`,
  `resume_retries_the_batch_after_a_device_error`).
- `docs/multiwriter-spec.md` §7; SPEC §6 error table; ADR-0013
  (capacity errors name their remedy); ADR-0029 (`WalFull` before any
  block).
