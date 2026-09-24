# ADR-0006: horton owns no clock; the caller supplies time for TTL

Status: Accepted (v0.15; SPEC records no date)

## Context

v0.15 added `Db::put_with_ttl(key, val, expire_at)`: a value that reads
should suppress once time reaches `expire_at`. horton uses only `core`
(ADR-0002), which has no clock, and its hosts keep time in different
units: seconds, milliseconds, or a logical epoch. SPEC calls the time
model "the one load-bearing decision" of v0.15.

## Decision

- horton never calls a clock. `put_with_ttl` stores an absolute
  `expire_at`, and every read takes a `now: u64` from the caller: a
  monotonic tick in any unit, since only ordering matters. The existing
  read APIs pass `now = 0` ("no time has passed").
- The contract is exactly: suppress iff `expire_at <= now`. A winning
  value with `expire_at != 0 && expire_at <= now` resolves to absent.
- `expire_at == 0` means no expiry and is stored exactly like a plain
  `put` (op `Put`, no expiry field), so non-TTL data pays nothing.
- Formats: WAL `Op::PutTtl = 4` is a `Put` record with an 8-byte
  little-endian `expire_at` after the value. SSTable entries use op byte
  `4` with the same trailing expiry; non-TTL entry headers stay 13 bytes.
- Purge is also caller-timed. Before driving a compaction job the caller
  sets `Compaction::purge_before`. An emitted value with
  `expire_at <= purge_before` becomes a point tombstone at the same
  sequence number, newest or not; `purge_before == 0` disables purging.
  Dropping an expired non-newest version silently would be unsound: at a
  snapshot where it is the newest visible version, reads must see
  absent.

## Consequences

- Any time base works, including a logical epoch, and expiry is
  deterministic given the caller's `now`.
- Clock skew between writers is the caller's problem. horton only
  promises the ordering contract above.
- Expired bytes stay on the device until a compaction with a suitable
  `purge_before` converts them, and the resulting tombstone is then
  kept or dropped by the usual snapshot and bottommost rules (ADR-0004).
- Changing `purge_before` in the middle of a job is safe but incoherent.
- `WriteBatch` carries no TTL (or range-delete) ops; batches stay
  point-only.
- `put_with_ttl` is a single WAL record, atomic exactly like `put`.

## References

- [Milestone log](../history/milestones-v0.1-v0.16.md) v0.15 (Scope, Time model, Durable formats, Visibility
  ordering, Compaction, Crash model, Honest limits)
- SPEC §1
