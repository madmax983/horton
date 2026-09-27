# Horton `multiwriter` — SPEC

Feature-flagged multi-writer support for Horton. Status: SPEC (2026-09-24).
Research complete — see `~/workspace/horton-research/multiwriter-spikes-REPORT.md`
and `docs/multi-writer-design.md` §4.3. This SPEC is normative for the
implementation track; the design doc is background.

Notation: **MUST** / **MUST NOT** are requirements. **PROOF-OBLIGATION**
marks something the implementation track must argue or model before the
flag is considered shippable. **OPEN** marks a decision Mark has not made.

## 0. Scope and non-goals

- The `multiwriter` feature (default off) adds an MPMC admission ring in
  front of the existing single-threaded core. The default build is
  byte-identical in behavior to Horton without the flag.
- In scope: the ring, the drainer integration, completion signaling, the
  crash model of the ring, Loom models of the ring protocol.
- Non-goals: changing the `&mut self` core API; changing `BlockDevice`;
  in-crate thread management; in-crate fairness; in-crate clocks or
  timeouts; changing the on-disk format. The single-writer profile is
  untouched — the flag adds code, never changes the default path.

## 1. Ticket and gate encoding

- Tickets are **31-bit values** held in `AtomicU32` cells, uniform on all
  targets (spike 2: `AtomicU64` does not exist on `xtensa-esp32s3-none-elf`;
  `AtomicU32` CAS is real `s32c1i` hardware). Ticket arithmetic wraps
  modulo 2³¹.
- Bit 31 (`0x8000_0000`, `FENCED_BIT`) is reserved: **no legitimate gate
  value ever has bit 31 set.** `TICKET_MASK = 0x7FFF_FFFF`.
- Per-slot gate values (all `< 0x8000_0000`):
  - `FREE(t) = t` — slot `t % N` may be written by ticket `t`'s owner.
  - `PUBLISHED(t) = (t + 1) & TICKET_MASK` — slot holds ticket `t`'s payload.
  - `FENCED(t) = t | FENCED_BIT` — ticket `t` was fenced (§6); set only by
    the drainer's fence CAS.
- Slot index for ticket `t` is `(t as usize) % N`, computed identically by
  producer and consumer from the u32 ticket value.
- `N` MUST be a power of two in `[2, 2³¹]` (compile-time assertion). For
  non-power-of-two `N`, a live window straddling the 2³¹ wrap maps two live
  tickets to one slot: when `2³¹ mod N ≠ 0`, tickets at claim distance
  `2³¹ mod N (< N)` are congruent mod `N` (e.g. `N = 3`: tickets `2³¹-1`
  and `1` both land on slot 1), breaking I2 — and worse, the collided
  ticket's `FREE` gate value is then never written, so its claims fail
  forever and the ring wedges permanently. Found during implementation,
  2026-09-24; this is why the requirement is what it is. Powers of two
  divide 2³¹, so the residues of every `N`-ticket window are a permutation
  of the slots — wrap-safe by construction. (`N == 1` is additionally
  unsound: `PUBLISHED(t)` and the release value `FREE(t+N)` coincide on
  the gate encoding, spike 1.)

Design note (recorded, not hand-waved): spike 5 proposed a single global
`FENCED` sentinel (`u64::MAX`, "tickets never reach it"). That is sound
for 64-bit tickets and **unsound** for wrapping 31/32-bit tickets: every
u32 value is eventually a legitimate ticket value, so a fixed sentinel is
eventually indistinguishable from a free slot — the fence CAS becomes a
no-op for the colliding ticket and its write is then silently lost (never
drained, reporter told `Ok`). The high-bit reservation fixes this by
construction: the fenced state is unforgeable by any present or future
ticket. The price is 31-bit tickets; the wrap-safety argument (§2) is
unchanged.

## 2. Invariants

- **I1 (live window).** At any instant, all live tickets (claimed, slot not
  yet released) lie in a claim-order interval of length `< N`. Enforced by
  the claim protocol (§3): a claim for `t` succeeds only after observing
  `FREE(t)`, which the consumer wrote only after draining `t-N` (or at
  init for `t < N`); the consumer drains in claim order.
- **I2 (slot exclusivity).** At most one live ticket uses a slot. The slot
  index is `t % N` and `N` is a power of two dividing 2³¹ (§1), so
  `t ↦ t % N` is injective on every claim-order window of `N` tickets —
  including windows straddling the wrap — and no two live tickets (I1:
  window `< N`) share a slot. The old hand-argument ("live tickets sharing
  a slot differ by a multiple of `N`, contradicting I1") is false across
  the wrap without the power-of-two requirement; the requirement is the
  fix.
- **I3 (u32/u64 observational equivalence).** No execution distinguishes
  the 31-bit ring from an infinite-ticket ring: at most `N` tickets are
  live (I1) and `N << 2³¹`, so no two live tickets are congruent mod 2³¹
  and every gate comparison is unambiguous across the wrap. PROOF-OBLIGATION:
  mechanize or hand-prove the reduction; the Loom models (§15) cover the
  protocol logic, the seeded-wrap unit tests cover the wrap empirically.
- **I4 (drain lease).** Exactly one drainer executes the sweep at a time,
  and it drains strictly in ticket order. This is load-bearing for WAL
  replay correctness, not just scheduling: `wal.rs` `recover_from`'s
  `seq_floor` skip is sound only if WAL append order is ticket order; a
  second concurrent drainer admits an interleaving where an acknowledged
  write is skipped at recovery and lost (spike 3). The core's existing
  proofs survive untouched iff I4 holds.

## 3. Claim protocol

- `try_claim() -> Option<u32>`: **check-then-CAS, single attempt.**
  Load `head`; if the ticket's slot gate is not `FREE(head)`, return
  `None`; else one `compare_exchange(head -> (head+1) & TICKET_MASK)`.
  On CAS failure return `None` — no retry in-crate (the host may retry;
  see §8).
- A failed claim consumes **nothing**: no ticket, no sequence number, no
  slot state changes. This is what makes `Error::NoSpace` honest (§8).
- The check-then-CAS race (gate freed between another producer's check
  and ours) resolves in the CAS: at most one producer wins a ticket, and
  the winner's gate observation is still valid — the gate can only leave
  `FREE(t)` via the winner's own publish or the drainer's fence of `t`,
  both of which the winner survives correctly (§4, §6).
- **Error precedence:** argument validation (key/value lengths, batch
  well-formedness, closed-DB, etc.) is checked **before** the claim. A
  caller MUST NOT observe `NoSpace` for a request that would have failed
  validation anyway.

## 4. Publish protocol

- The owner of ticket `t` writes the fixed-size payload cells (Relaxed),
  then publishes with `compare_exchange(FREE(t) -> PUBLISHED(t), Release)`.
  The Release carries the Relaxed cell writes to the consumer's Acquire
  gate load (I1–I3's memory-ordering core, as prototyped in spike 1).
- Publish is a CAS, not a store (spike 5): exactly one of {publish, fence}
  wins the gate, so the fence *policy* may be heuristic without soundness
  risk — the CAS is the arbiter.
- On CAS failure the writer re-reads the gate:
  - If it still reads `FREE(t)`, retry the CAS (covers spurious failure
    on LL/SC targets; bounded by contention, no alloc, no blocking).
  - If it reads `FENCED(t)`, the writer was fenced: attempt self-release
    `compare_exchange(FENCED(t) -> FREE((t+N) & TICKET_MASK))` and return
    `Fenced`. If the self-release CAS fails, the gate already moved on
    (host `force_release_slot`, §6) — return `Fenced` without touching
    the slot again.
  - Any other gate value means the ticket is lost to the writer (host
    raced a force-release, §6) — return `Fenced`; MUST NOT touch the slot.
- A fenced writer MUST NOT write payload cells after observing the fence,
  and MUST NOT touch the slot after its self-release attempt. (Its cell
  writes precede the publish CAS in program order; a fenced-then-released
  slot is fully overwritten by the next owner before that owner's publish,
  so no torn payload is ever drained.)
- `Fenced` surfaces to the caller as `Error::WriterFenced`. The ticket's
  sequence number is abandoned — a gap (§13).

## 5. Drain protocol

- The single lease-holder drains strictly in ticket order: for its cursor
  `c`, wait until the slot gate reads `PUBLISHED(c)`, read the payload
  cells, apply, then `store(FREE((c+N) & TICKET_MASK), Release)`. The
  Release pairs with the next owner's Acquire gate load: the consumer's
  payload reads happen-before the next producer's overwrite (spike 1,
  invariant 2).
- The drainer is the sole device writer (O2, §7): memtable apply, WAL
  append, and flush all happen inside the lease. Flush/compaction keep
  their existing linearization-point treatment; the single-writer crash
  proofs are reused verbatim (spike 5).
- Draining is cooperative-first (§10): a writer that enqueued its entry
  attempts the lease and drains (bounded: `max_writes` entries per
  attempt, flush at end) until its own ticket is durable. The host's
  `drainer_poll` is the liveness watchdog for failure cases (dead writer,
  claimed-but-never-published holes) — cheap when idle (one atomic load
  plus a cursor compare).
- The sweep MUST absorb (drain up to `max_writes`, not stop at the
  holder's own ticket): stopping at one's own ticket fragments group
  commit and measurably worsens p99 and flush count (spike 3, sim).

## 6. Generation fencing

- **Problem:** in-order drain plus a writer that claimed the head ticket
  but never publishes wedges the drainer forever (fundamental to
  ticket-based MPMC: the ticket is claimed before the slot index is
  known).
- **Fence (drainer, at head `t`, gate still `FREE(t)`):**
  `compare_exchange(FREE(t) -> FENCED(t))`. Success: the ticket is dead —
  advance the cursor past `t`, do **not** drain, do **not** release the
  slot. The slot stays `FENCED(t)` until the fenced writer self-releases
  (§4) or the host proves death (§6, `force_release_slot`). Failure: the
  gate is `PUBLISHED(t)` (the writer won the race) — drain normally.
- **Timeouts:** a timeout (or any heuristic — stall budget, watchdog
  poll) MAY trigger the fence, because the CAS decides: if the writer
  already published, the fence CAS fails and nothing bad happens. A
  timeout MUST NEVER trigger the *release*: releasing the slot while the
  writer might still write payload cells admits torn payloads drained as
  valid. Horton owns no clock; any in-library stall budget is poll-count
  based, never wall-clock.
- **Host-owned death policy:** detecting death is impossible in-library
  (no clock, no OS thread-health API) — the policy lives with the host,
  matching Horton's caller-owned philosophy. `force_release_slot(t)`:
  the host asserts proven thread death (join semantics, never a timeout);
  if the gate reads `FREE(t)` or `FENCED(t)`, store
  `FREE((t+N) & TICKET_MASK)`; if it reads `PUBLISHED(t)`, the writer
  published before dying — do NOT release; the drainer will drain it.
  Calling `force_release_slot` without proven death is a host bug and
  voids the protocol guarantees (stated, not hidden).
- Fenced tickets never reach the device: **no `t ∈ fenced` has any effect
  present after recovery** (spike 5, assertion for the Loom×injector
  composition).

## 7. Completion, durability, proof obligations

> **Implemented** (2026-09-26): `src/drainer.rs` — the `Drainer` owns the
> `WalWriter`, drains the ring in ticket order, batches through
> `append_batch` (one flush per sweep), and advances `durable` only over
> the acknowledged prefix. `tests/drainer.rs` (6 tests) covers the
> batch+flush+durable flow, WAL content recovery, idle/fence behavior,
> batch-error poisoning, and `next_seq` reseeding.

- Completion signal: a single `durable: AtomicU32` watermark = the
  contiguous WAL-durable ticket prefix (mod 2³¹; unwrapped by the drainer
  to u64). A writer's put completes when `durable` has advanced past its
  ticket (`drainer::is_ticket_durable`).
- **O1 — acked ⇒ durable.** The watermark advances only via
  `compare_exchange` **after the WAL batch's flush is acknowledged**
  (`append_batch` returns; the drainer advances over exactly
  `report.durable`). A put MUST NOT complete before its entry's WAL unit
  is flush-acknowledged. On batch error the drainer advances `durable`
  over exactly the durable prefix, burns `consumed` seqnums, and poisons
  itself (returns the error; the host fails outstanding writers).
- **O2 — drainer is sole device writer.** Holds by ownership: the
  `WalWriter` (and through it the `Device`) is moved to and exclusively
  owned by the drainer. No `&WalWriter` is ever shared. (The full `Db`
  move is a later slice; the WAL-owning drainer is the device-writer
  boundary.)
- **O3 — counter reseed.** On `open()`, the drainer's u64 seqnum base is
  the recovered WAL `next_seq` (`max_seq + 1`; seqnum 0 is the recovery
  floor and is never used). Tickets and WAL seqnums are independent
  counters: the drainer maps the i-th drained ticket to `next_seq + i`.
  The `durable` ticket watermark is initialized to the ring's head ticket
  (0 for a fresh ring); the ring itself starts unseeded unless the host
  reseeds it via `Ring::new_seeded`. Gap sequence numbers (never claimed,
  or claimed-but-fenced) left no trace and are safe to skip; recovery
  MUST NOT treat gaps as corruption and MUST NOT ack anything. Fenced/
  skipped tickets consume no seqnums and advance `durable` immediately
  (they are dead; their writers were notified via
  `PublishOutcome::Fenced`).
- The 32-byte ring payload uses the interim codec
  (`drainer::payload`: `[op:1][klen:1][vlen:1][key][val]`, 29-byte
  key+value budget) — a stand-in for the serialized mutation (§14).
- With WAL batching (§11), the durability granularity is the batch; torn
  tails still truncate at record boundaries via CRC (existing rule).
- The drainer's stall budget is poll-count based (Horton owns no clock);
  `fence_cursor` refuses unclaimed head tickets so idle sweeps cannot
  poison the ring.

## 8. Backpressure

- Ring full at claim time → the `put` future resolves
  `Ready(Err(Error::NoSpace))` **immediately**: no ticket consumed (§3),
  no sequence number consumed, safe to retry. (Spike 4: a `Pending`-on-full
  promise cannot be honored in-crate — the only entity that observes "slot
  freed" is the drainer, and storing `Waker`s needs interior mutability,
  the forbidden thing.)
- Principled rule: **the crate returns `Pending` only when the
  crate-or-device contract owns the wake path.** `Pending` is returned
  only post-acceptance (drain progress / device I/O), under the existing
  device `Context` wake contract.
- **Drop semantics:** dropping a pending (post-acceptance) put future
  abandons *observation*, not the write — the entry was accepted and will
  be drained and made durable; the caller simply stops waiting for the
  completion signal.
- A full ring is morally identical to 8 live snapshots → `NoSpace`:
  bounded exhaustion → explicit error → caller retries. No thundering
  herd is possible (no wakes exist on the full path).
- Whether `NoSpace` is reused or a dedicated `RingFull` variant is added
  is OPEN (observability call, Mark's).

> **Implemented** (2026-09-26): `src/writer.rs` — `put(ring, durable,
> payload)` claims a ticket, publishes the 32-byte payload, and returns a
> `Put` future resolving to the ticket once WAL-durable. Ring-full at
> claim → `Err(Error::NoSpace)` immediately (no ticket consumed).
> `publish` → `Fenced` transparently re-claims a fresh ticket (silent
> re-claim; §16 Q4). `Put` polls the `durable` watermark and returns
> `Pending` post-acceptance only, per the rule above; dropping it
> abandons observation, not the write. If the drainer is poisoned the
> watermark never advances and the host owns failing abandoned puts.

## 9. Snapshots and reads

> **Implemented** (2026-09-26): `drainer::drain_watermark` (free function)
> and `Drainer::durable_watermark` — the Acquire side of the Release/
> Acquire watermark publication. `tests/drainer.rs` proves the three
> invariants: the watermark pins the drain position (not the claim head —
> the head can run ahead of durability), it never moves backward, and
> fenced tickets advance it without leaving WAL records.

- **Snapshot watermarks MUST pin the drain watermark, not the claim
  counter** (spike 5): pinning the claim counter lets a later-drained
  ticket `≤` watermark become visible to a snapshot reader — an isolation
  violation. The drainer publishes the watermark with Release; `snapshot()`
  load-Acquires it.
- The watermark is the contiguous *resolved* ticket prefix: every ticket
  `<` it is WAL-durable, fenced tickets included (dead tickets resolve
  the prefix without a WAL record). A snapshot at the watermark observes
  exactly the WAL-durable records — verified by recovery in the tests.
- Read-your-writes for the writing thread through the ring is OPEN (spec
  gap, spike 5): undecided whether a writer sees its own un-drained put.
- Full `Db::snapshot` integration (snapshot reads at the pinned
  watermark) awaits the Db-owning drainer slice; the watermark API is the
  boundary it will use.

## 10. Topology (ownership)

```
writer threads ──atomics──▶ ring ──drainer thread──▶ Db ──▶ Device
```

- Writers share **only the ring** (`Sync` through atomics; no locks, no
  in-crate `Arc` — a host may `Arc` the ring itself to hand out handles).
- The drainer **exclusively owns the `Db`** (`Db` is `Send` — verified by
  compile assertion 2026-09-23 — but `!Sync`; the compiler prevents
  accidental sharing). `BlockDevice`'s `&mut self` write/flush signatures
  are unchanged: no shared `&Db` exists.
- Readers vs. drainer exclusion is the host's: a host-owned mutex or
  strict confinement (drainer-paused windows). `Db: !Sync` means an
  ordinary shared-reader `RwLock` around `&Db` does not work; Horton does
  not provide the primitive and does not pretend to.

## 11. WAL batching is prerequisite

The v0.13 one-device-block-per-put wart gets worse under concurrency and
interacts badly with cooperative draining (spike 3 sim: 7× flushes).
**Batching WAL appends is prerequisite work for the `multiwriter` flag** —
it is also the single biggest benchmark win available. The sweep drains
up to `max_writes`, appends the batch, flushes once.

### Batch API (`WalWriter::append_batch`)

The batch is **prefix-atomic** — forced, not chosen: once a full staging
block lands on the device it cannot be unwritten, so a failed batch can
only ever resolve to a durable prefix. The single-record `append` /
`commit` path is unchanged; the batch is a convenience over it with
batch-granularity error semantics.

- `append_batch(&[BatchRecord]) -> BatchReport`: stages each record in
  order (writing full blocks as the staging fills, exactly like the
  single-record path), then commits — one device flush for the whole
  batch. An empty batch is a no-op (no flush).
- `BatchRecord { seq, op, key, val, expire_at }`: `expire_at` is used
  only for `Op::PutTtl` (0 = never, stored as `Op::Put`, matching
  `append_ttl`).
- `BatchReport { durable, consumed, error }`:
  - `durable`: `records[..durable]` are flush-acknowledged. The drainer
    advances the durable watermark over exactly these tickets (O1).
  - `consumed`: `records[..consumed]` have their seqnums consumed —
    either durable or possibly-replayed (blocks landed, flush failed:
    the "device lied" case, same rule as the single-put
    `rollback_commit`). Never reuse them. `records[consumed..]` never
    touched the device and may be retried with fresh seqnums. Always
    `consumed >= durable`.
  - `error`: `None` iff the whole batch is durable; otherwise the error
    that determined the outcome — a commit failure subsumes an earlier
    append failure, because the commit is the durability step.
- Failure matrix (all covered by `tests/wal_batch.rs`):

  | append      | commit | blocks landed | report                                            |
  |-------------|--------|---------------|---------------------------------------------------|
  | ok          | ok     | —             | durable=len, consumed=len, no error               |
  | fail at k   | ok     | —             | durable=k, consumed=k, append error               |
  | fail at k   | fail   | no            | durable=0, consumed=0 (stage truncated; seqnums reusable), commit error |
  | fail at k   | fail   | yes           | durable=0, consumed=k (may replay), commit error  |
  | ok          | fail   | no            | durable=0, consumed=0, commit error               |
  | ok          | fail   | yes           | durable=0, consumed=len (may replay), commit error|

- Crash semantics: unchanged from the single-record path. A torn batch
  tail truncates at record boundaries via CRC; the acknowledgment
  granularity is the batch, but the WAL itself stays prefix-consistent
  (existing rule, §7).

The single-writer `put` path is untouched: per-put durability is its
contract, and group commit would weaken it. The benchmark win lands in
the drainer sweep (and already exists for `WriteBatch`).

## 12. Fairness

None in-crate — stated, not hidden. Under immediate retry, a writer can
be overtaken indefinitely by other writers. Host-side serialization (a
mutex around the ring handle, a semaphore, admission quotas) is the
fairness mechanism. The in-crate protocol is lock-free (some writer always
makes progress: the drainer never blocks a claim, and claims are
single-CAS).

## 13. Ticket → seqnum mapping

- The ticket IS the sequence order. The drainer unwraps the 31-bit ticket
  to a u64 seqnum: the unique `s ≥ seq_base` with
  `(s & TICKET_MASK as u64) == t`, where `seq_base` advances with the
  drain cursor. Unique by I1 (`N << 2³¹`).
- Fenced tickets create seqnum gaps. Gaps are benign: recovery, snapshots,
  and compaction key on seqnum comparison, never density. (Spike 5: gap
  seqs left no trace.)

## 14. API surface (initial)

- `ring::Ring<const N: usize>` — `new()`, `new_seeded(u32)` (O3),
  `try_claim() -> Option<u32>`, `publish(u32, &[u8; 32]) -> PublishOutcome`
  (`Published` | `Fenced`), `drain() -> (u32, [u8; 32])` (single consumer;
  test/drain scaffolding — the poll-based drainer replaces the spin).
- Payload is a fixed 32-byte record (stand-in for the serialized
  mutation; multi-op tickets / `WriteBatch` atomicity through the ring is
  OPEN — one slot per batch vs. consecutive tickets with a drainer-side
  atomicity boundary, spike 5).
- Everything is `#[cfg(feature = "multiwriter")]`, const-generic,
  no_alloc, safe Rust. No new `Error` variants are added by the ring
  itself except surfacing `WriterFenced` at the `Db` integration layer
  (naming OPEN).

## 15. Verification plan

- Loom models of claim/publish/drain interleavings (adapted from
  spike-1's, ported to 31-bit tickets), behind the `loom` feature:
  ticket uniqueness, exactly-once in-order drain, payload integrity, plus
  a stalled-writer model asserting liveness for all other puts and O1.
- Loom × crash-injector composition at the ticket level (spike 5): Loom
  emits `(applied, fenced, acked)` ticket sets; the injector replays them
  through the real single-threaded drain path, enumerates truncations at
  every write position, recovers with the real `Db::open`, and asserts
  durable linearizability (every acked put's effect present; no fenced
  ticket present) plus prefix-oracle equality.
- Miri for the safe-Rust atomics usage. Mutation testing for the ring
  module once GREEN.

## 16. Open questions (Mark's calls)

1. Read-your-writes for the writing thread through the ring.
2. `NoSpace` reuse vs. dedicated `RingFull` variant.
3. `WriteBatch` / range-delete / TTL through the ring: one slot per batch
  vs. consecutive tickets with drainer-side atomicity.
4. `WriterFenced` surfacing: silent transparent re-claim vs. surfaced
  error (a ticket claimed across `Pending` polls can be fenced — the put
  future must handle `Fenced → re-claim` or surface it).
5. Ack/wakeup mechanism detail (spike-3 seam): host re-poll contract vs.
  opt-in fixed waiter array.
6. Dual-core `s32c1i` cross-core behavior on ESP32-S3: TRM/hardware
  confirmation still wanted (spike 2: instruction selection verified,
  silicon behavior not).
