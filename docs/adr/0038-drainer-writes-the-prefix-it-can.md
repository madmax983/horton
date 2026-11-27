# ADR-0038: The drainer writes the longest prefix that the `Db` takes

Status: Accepted (unreleased, 2026-11-26)

## Context

Issue #48 (items 1 and 2 of its next steps) and issue #52.

1. **A burst poisoned the drainer for good.** A sweep drains up to
   `MAX_WRITES` tickets into one `Db::write`. When `MAX_WRITES` is
   larger than `CAP`, or the ops need more than `ARENA` bytes, the
   write fails with `BatchTooLarge`. A flush does not help. The drainer
   poisoned itself, and `resume` gave the same error again. All the
   writes were valid.
2. **One bad ticket stopped the good tickets before it.** A key too
   long for the `Db`'s `KEY_MAX` poisoned the drainer. The valid
   tickets in the same sweep did not complete.
3. **The host could not find the bad ticket.**
4. **A writer could not check the `Db`'s limits.** `writer::put` is
   generic only over `N`. It published any payload that the codec
   encodes.

## Decision

**The drainer writes the longest pending prefix that the `Db` takes.
The other tickets stay pending.**

- The sweep queues the pending tickets in order and stops at the first
  ticket that the `Db` cannot hold. It writes the tickets before that
  ticket. The next sweep starts at that ticket and poisons the drainer.
- On `BatchTooLarge` with more than one op, the sweep tries again with
  half of the tickets. `Db::write` refuses before any I/O, so the retry
  is safe. At most one `Db::write` in a sweep succeeds.
- One ring op always fits an empty memtable. `Db::new` asserts
  `ARENA >= KEY_MAX + VAL_MAX` and `CAP >= 1`. A ring op's WAL record is
  at most 52 bytes, and a block is at least 512 bytes.
- `Drainer::bad_ticket()` gives the ticket when the error came from a
  ticket. It is the first pending ticket. `resume` clears it.
- `Drainer::writer()` gives a `Writer` that is `Copy`. It knows
  `KEY_MAX` and `VAL_MAX`. `Writer::put` checks the payload with
  `payload::check` before it claims a ticket. The drainer refuses a
  ticket when `payload::check` refuses its payload, with the same
  error.
- `writer::put` refuses a payload that does not decode, before it
  claims a ticket.

Not done:

- **No API skips a bad ticket.** `durable` is one watermark. To move
  it past a ticket completes that writer, but its write is not in the
  `Db` (SPEC O1, ADR-0036). A per-ticket failure signal needs ring
  state and a new `Put` output. A `Writer` stops bad payloads before
  they get a ticket, so a bad ticket comes only from `Ring::publish`
  or `writer::put` with a payload that decodes.
- **No learned batch size.** Each sweep halves from all pending
  tickets. This costs only decode work.
- **`payload::check` does not know `CAP` or `ARENA`.** It does not
  need them: one op always fits an empty memtable.

## Consequences

- Valid writes do not poison the drainer, whatever `MAX_WRITES` is.
- `Swept { tickets }` can be less than the drained tickets. The next
  sweep writes the rest before it drains new tickets.
- A host that gives each writer a `Writer` gets limit errors at once,
  with no ticket used.
- `writer::put` returns `BadPayload` for a payload that does not
  decode. Before, the drainer poisoned itself.
- After a ticket error, the host fails the writer of `bad_ticket()` and
  the writers after it. The pipeline still stops at that ticket.

## References

- Issues #48 and #52.
- `src/drainer.rs` (`sweep`, `fill`, `bad_ticket`, `writer`,
  `payload::check`), `src/writer.rs` (`Writer`, `put`).
- `tests/drainer.rs` (the tests after "Issue #48"),
  `tests/drainer_model.rs`.
- `docs/multiwriter-spec.md` §§7, 8; ADR-0036.
