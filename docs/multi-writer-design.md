# Horton beyond single-writer: staged concurrency design

Scope doc — 2026-09-23, revised 2026-09-24. Status: research complete
(all five §4.3 spikes done — verdicts in §4.3 and the synthesis report at
`~/workspace/horton-research/multiwriter-spikes-REPORT.md`); the normative
document for the implementation track is now `docs/multiwriter-spec.md`.
No implementation code yet.

## 0. Where we stand (grounded in `src/`)

- Writes are `&mut self` (`put`, `delete`, `delete_range`, `put_with_ttl`, `write`) —
  the borrow checker is the write lock. (`src/db.rs`)
- Reads are already `&self` (`get`, `get_at`): the point-read scratch buffer, the
  decompression buffer, and the v0.16 block cache all live in `RefCell`s, and `CachePort`
  is implemented for `RefCell<BlockCache>` with `try_borrow_mut` — contention degrades
  to silent bypass, never a panic. The doc comments explicitly bless "two `get` calls at
  once (interleaved awaits on one executor)". (`src/db.rs`, `src/cache.rs`)
- Memtable: `insert` takes `&mut self`, `get`/`get_at` take `&self`. (`src/memtable.rs`)
- `next_seq: u64` is a plain counter; snapshots are a plain bounded array. (`src/db.rs`)
- `BlockDevice`: `poll_read_block(&self)`, `poll_write_block(&mut self)`,
  `poll_flush(&mut self)`. (`src/device.rs`)
- Constraints: `no_std`, `no_alloc` (hard line), zero production dependencies, `unsafe`
  forbidden, no `unwrap`/`expect`/panic in non-test production code,
  SPEC–PROOF–RED–GREEN–REFACTOR, fixed-capacity caller-owned memory via const generics,
  poll-based I/O.

## 1. The load-bearing fact

`RefCell` is `!Sync`. Every piece of shared mutation in Horton today is
single-threaded-shared (interleaved polls on one executor). Crossing to threads means
replacing each `RefCell` with a `Sync` mechanism — and safe Rust has no `Sync`
interior mutability without `unsafe` (`UnsafeCell` is the one primitive; every
`Mutex`/`RwLock` ever written builds on it).

So in-crate thread sharing has exactly two honest shapes:

- **(a)** a tiny, audited, `unsafe`-isolated synchronization module (spinlock), or
- **(b)** keep Horton `!Sync` and let the host provide exclusion (interrupt masking on
  ESP32-S3, executor confinement on servers) — zero rule-bending, very "caller-owned".

Decision (2026-09-23, Mark): **(a) is off the table entirely — no `unsafe`, period.**
(b) is the standing rule. §4's ring is safe-Rust, so nothing is lost by this.

## 2. Stage 1 — REMOVED (decision settled 2026-09-23)

Mark: no `unsafe` at all, full stop. So there is no in-crate spinlock, no `SyncDb`
wrapper, and no Stage 1 release. The former Shape B is not a shape anymore — it is
the law:

- Horton core stays `!Sync` and single-threaded. Thread-sharing is the host's job —
  interrupt masking on ESP32-S3, executor confinement on servers.
- The `BlockDevice` `&mut self` write/flush signatures stay as they are; no trait
  change is needed, because no shared `&Db` will exist.
- Concurrency, when it comes, arrives through the safe-Rust ring of §4 — which never
  needed Stage 1 anyway (the drainer exclusively owns the `Db`).

### What "host provides exclusion" concretely means

The ring design's ownership topology:

```
writer threads ──atomics──▶ ring ──drainer thread──▶ Db ──▶ Device
```

- **Writers share only the ring.** The ring is `Sync` through atomics — no locks, no
  `Arc` needed inside Horton. (If a host wants owned handles to hand to its threads,
  the host may `Arc` the ring itself; that is host code, not Horton's.)
- **The drainer exclusively owns the `Db`, and through it the `Device`.** Nothing is
  shared, so nothing needs shared ownership — `Arc` answers a question this design
  does not ask. The `BlockDevice` `&mut self` signatures are fine as they are.
- **`!Sync` is doing work here, not causing it:** the compiler *prevents* accidental
  sharing of the `Db`. `Db` is `Send` (it can be moved to the drainer thread —
  verified by compile assertion 2026-09-23) but not `Sync` (it cannot be shared
  across threads — the `RefCell` fields guarantee that).
- **Readers vs. drainer:** a reader thread and the drainer must never touch the `Db`
  at once. With no in-crate lock, that exclusion is the host's — a host-owned
  mutex around drainer-paused windows, or strict confinement (reads only when
  the drainer is idle). Note `Db` is `!Sync`, so an ordinary shared-reader
  `RwLock` around `&Db` does not work — the host owns a mutex it locks
  exclusively, or confines access. Horton does not provide the primitive and
  does not pretend to.

## 3. Stage 2 — concurrent readers, v0.17 (deferred)

Goal: readers don't serialize against each other. **No `unsafe` anywhere,
so this is atomics + host-provided exclusion only** — there is no
audited-`unsafe` reader-writer lock (that story died with Stage 1 on
2026-09-23):

- Replace the read-path `RefCell`s with `Sync` equivalents, preserving the v0.16
  "degrade, don't block" philosophy — now with atomics instead of
  `RefCell::try_borrow_mut`:
  - Block-cache CLOCK use-bits → per-slot atomics; hit/miss counters → `AtomicU64`.
    (Cache images are immutable once published.)
  - `get_scratch` / `decomp_scratch` → try-lock with silent bypass to a stack buffer
    — exactly today's fallback behavior, made thread-safe.
- Reader-vs-drainer exclusion is host-provided (see §2). `next_seq`,
  memtable, manifest, and snapshots stay under it — no semantic changes.
- Snapshot reads pin the **drain watermark**, not the claim counter (spike 5
  finding — pinning the claim counter is an isolation violation once the ring
  exists). Snapshot semantics otherwise untouched.

Stage 2 is deferred behind the `multiwriter` track: the ring (§4) is the
priority, and §4 needs only the drain-watermark part of this section.

## 4. Stage 3 — lock-free multi-writer, feature-flagged (research track)

Direction from Mark (2026-09-23): lock-free multi-writer as a **default-off feature
flag** (precedent: `scratch-bump`), the **ring buffer** as the leading structure, and
the research spikes decide the details — no premature lock-in.

### 4.1 The ring-buffer insight — why it beats a lock-free skiplist

Instead of making the memtable itself lock-free (months of proof burden), put a
**fixed-capacity MPMC ring** in front of the *existing single-threaded core*:

- Writers concurrently claim ring slots via atomic `fetch_add` (ticket). The ticket
  **is** the sequence order — seqnums fall out of the ring for free. No separate
  atomic counter, no commit-watermark protocol.
- A single drainer — the existing `&mut self` write path, *unchanged* — applies ring
  entries to the memtable/WAL in ticket order. Every existing proof (sorted-slot
  memtable, snapshots, tombstones, range deletes, TTL, the v0.15 compaction rules)
  survives untouched, because the core never sees concurrency.
- The ring is DRAM-only staging; durability still comes from the WAL blocks the
  drainer writes. `put` doesn't return until its entry is drained *and* WAL-durable —
  the poll-based API already models exactly this await.
- Fixed capacity via const generic: ring full → `Error::NoSpace`, returned
  **immediately** — no `Poll::Pending` on a full ring (spike 4 verdict: an
  in-crate `Pending` there is a liveness lie, since storing `Waker`s needs
  interior mutability, the forbidden thing). `Pending` is returned only
  post-acceptance, for drain progress / device I/O. Bounded and explicit,
  very Horton.
- An MPMC ring over a fixed array — index arithmetic plus atomic slots — is
  writable in **safe Rust**: no raw pointers, no `UnsafeCell`. The "unsafe forbidden"
  rule survives Stage 3.

### 4.2 What the flag contains

- A `multiwriter` feature (default off): the ring, the drainer integration, and the
  `Sync` read-path pieces it needs from §3. The default build is byte-identical to
  today's Horton — the flag adds code, never changes the default path.
- With the flag on, `put`/`delete`/batches flow through the ring. Same `Error`
  surface, same durability contract.
- Stage 3 now carries the entire concurrency story, and it can: the ring's
  `Sync`-ness comes from atomics, not from sharing the `Db` — the core stays `!Sync`
  and the drainer owns it exclusively. Skipping Stage 1 cost nothing.

### 4.3 Research spikes — DONE (2026-09-24)

All five spikes completed. Verdicts (full synthesis:
`~/workspace/horton-research/multiwriter-spikes-REPORT.md`):

1. **Ring flavor — DONE: ring_a wins.** Disruptor-style per-slot sequence
   gates, safe-Rust/`no_alloc`, Loom-modeled + stress-tested. The tiebreaker
   came from spike 5: ring_a takes generation fencing with a minimal delta
   (publish store→CAS + sentinel); ring_b would need a protocol change.
   `N >= 2` is a compile-time assert (N=1 aliases publish/release gates).
2. **Xtensa atomics — DONE: u32 tickets, uniform on all targets.**
   `AtomicU64` does **not** exist on `xtensa-esp32s3-none-elf`
   (`max-atomic-width: 32`) — compile error, no fallback. `AtomicU32` CAS
   is real `s32c1i` hardware (asm-verified). Wraparound is safe by
   construction: the live ticket window is structurally bounded below ring
   capacity, so u32 values are unambiguous across a wrap. The SPEC refines
   this to 31-bit tickets + a reserved high bit for the `FENCED` state
   (a global sentinel is unsound under wrap — see SPEC §1).
3. **Drainer scheduling — DONE: hybrid.** Cooperative draining as the
   primary progress engine (writers drain inside `poll_put`; tighter p99,
   device stays saturated), host-driven `drainer_poll` as the liveness
   watchdog. One lease-guarded in-order sweep routine. **Load-bearing
   finding:** the drain lease is required for WAL *replay* correctness —
   `wal.rs` `recover_from`'s `seq_floor` skip assumes ticket-order appends;
   concurrent drainers admit an interleaving that loses an acknowledged
   write at recovery. Single in-order drainer preserves every existing
   proof untouched.
4. **Backpressure semantics — DONE: immediate `Error::NoSpace`.** No
   `Pending`-on-full (unhonorable in-crate: no `Waker` storage without
   `UnsafeCell`). `Pending` only post-acceptance, under the device wake
   contract. Fairness: none in-crate (stated; host's job).
5. **Crash model — DONE: one new durable LP.** The ring adds exactly one
   durable linearization point (drain commit = flush-ack). Loom and the
   crash injector compose at the ticket level. The dead-writer gap has a
   sound answer: generation fencing (CAS publish, fenced-writer
   self-release, host-owned death policy via `force_release_slot` —
   timeout may fence, never release). Proof obligations O1–O3 recorded in
   the SPEC.

### 4.4 Verification

Loom becomes the primary tool — bounded models of claim/publish/drain interleavings —
plus SPEC–PROOF–RED–GREEN with linearizability arguments for the ring, and Miri for
the safe-Rust atomics usage.

### 4.5 Scheduling

Mark authorized starting the track on 2026-09-24. Research is done; the
SPEC (`docs/multiwriter-spec.md`) is written; the implementation proceeds
behind the default-off `multiwriter` flag. Mainline never waits on it —
the flag adds code, never changes the default path.

## 5. Cross-cutting notes

- The v0.13 WAL wart (one device block per put) gets *worse* under concurrency — ring
  or not, batching WAL appends is prerequisite work for any multi-writer story. It is
  also the single biggest benchmark win available (Horton trails on bulk load partly
  because of per-mutation flush/fsync).
- `unsafe` audit boundary: there is none — no `unsafe` anywhere in the crate, and the
  §4 ring adds none either.
- `no_alloc` is non-negotiable throughout — and it's what makes safe-Rust lock-free
  possible (fixed arenas, index links, no pointer-reclamation problem).
- The `&mut self` core API stays for single-threaded hosts. Everything here is
  additive.

## 6. Decisions — all settled

1. ~~Stage 1 Shape A vs. Shape B~~ — settled 2026-09-23: no `unsafe`, Stage 1
   skipped, host provides exclusion.
2. ~~`BlockDevice` `&mut` → `&self`~~ — moot while no shared `&Db` exists.
3. ~~Loom as the first dev-dependency~~ — approved 2026-09-23, wired
   2026-09-24: yes. Loom is Horton's first dev-dependency (zero *production*
   deps preserved — it is optional and off by default), used to model the §4
   ring's claim/publish/drain interleavings behind the `loom` feature.
4. ~~Flag name~~ — settled 2026-09-23: `multiwriter`. It names the *capability*,
   not the *mechanism*, so it survives whatever the research spikes decide.
5. ~~Ring flavor / tickets / drainer / backpressure / crash model~~ — settled
   2026-09-24 by the five research spikes (§4.3); normative SPEC is
   `docs/multiwriter-spec.md`.
