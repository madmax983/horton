# horton milestone log, v0.1–v0.16

The milestone log that `SPEC.md` §9 carried through v0.16, moved here
verbatim when v0.17 split the SPEC into a normative spec, `CHANGELOG.md`,
and `docs/adr/` (architecture review, "Documentation and process"). Section
references (§4.6 and so on) point at the v0.16 SPEC, which git history
keeps. It is a historical record, not a description of current behavior.


- v0.1 — MemTable + WAL + recovery. In-memory `BlockDevice` in tests.
  Gate: crash-injector green over 3-op scripts.
- ~~v0.2 — SSTable writer/reader + `flush()` + manifest commit protocol.~~
  DONE 2026-09-13 (commit 963f5f1): plus bump-pointer block allocation and
  L0-newest-first point reads in `get()`. 61 tests green, clippy
  pedantic+nursery clean, fmt clean.
- ~~v0.3 — Full read path (levels, bloom, ranges) + block allocator sweep.~~
  DONE 2026-09-13: `get()` scans memtable → L0 newest-first → deeper levels
  with key-range pruning, bloom gating, and sequence pruning; entry seqs are
  threaded through the SSTable lookup so highest-seq wins across levels and
  the highest-seq tombstone hides older values. `FreeList` with first-fit
  run allocation (free-list-first, claim-after-commit); open-time sweep
  reclaims orphaned table blocks. WAL wraps atomically with the filling
  flush; recovery skips stale pre-wrap blocks via a sequence floor. 74 tests
  green (13 new: 6 read-path, 5 free-list, WAL wrap, orphan reclaim),
  clippy pedantic+nursery clean, fmt clean. v0.4+ exclusions held: no
  compaction, no scan/snapshots.
- ~~v0.4 — Leveled `compact_step` with bounded work + tombstone rule.~~
  DONE 2026-09-14: `Db::compact_step(&mut Compaction) -> Result<Progress,
  Error>` with caller-owned typed scratch; L0→L1 only (cascade deferred);
  linear-scan k-way merge (KMAX = 8), highest-seq-wins dedup, tombstone drop
  only when no level ≥ 2 overlaps the output span; one output block per call
  (`Progress::{Done, More}`); crash-injector green (state exactly pre- or
  post-compaction, never mixed). Also fixed a v0.3 manifest-encoding bug the
  new tests exposed: key bounds were written as full 256-byte arrays
  (544 bytes/table, overflowing the 4 KiB manifest block at 8 tables) and are
  now length-prefixed variable-length. 87 tests green (9 new compaction
  tests + merged perf work: slicing-by-8 CRC, `get` scratch reuse, release
  free-list claim fix, word-chunked zero scans, callgrind bench harnesses),
  clippy pedantic+nursery clean, fmt clean.
- v0.4.1 — Compaction input-run reclamation + on-disk format policy.
  Reclaim input block runs into the free list strictly after the manifest
  commit (best-effort: a full free list can't fail the already-committed
  job; orphans are swept by the next open). Manifest magic `hrtman01` →
  `hrtman02` for the v0.4 key-bound encoding change; pre-1.0 policy is no
  format compat across minor versions — a foreign magic is
  `CorruptManifest`, never a misparse.
- v0.5 — `scan` iterator + snapshot reads + executable models of the core
  invariants (seq-ordered visibility, per-key keep-set) with property and
  differential tests. Machine-checked proofs are deferred: the Verus
  toolchain is not installed on the dev host (confirmed 2026-09-14), so
  the models are pure total `no_alloc` functions written to lift into
  Verus `spec` fns later. Full version chains are retained in the memtable
  and SSTables; compaction emits the per-key keep-set (live view plus the
  newest version at or below each live snapshot watermark) and drops a
  bottommost newest tombstone only when it predates every live snapshot.
- v0.6 — ESP32-S3 / Tallow port.
  - **Target build gate**: the library builds for
    `xtensa-esp32s3-none-elf` with the ESP Rust fork (`-Z build-std=core`);
    `xtensa-check.sh` reproduces it. The crate is `#![no_std]` +
    `#![forbid(unsafe_code)]` + core-only, so the port needs no `unsafe`
    and no new dependencies.
  - **On-target smoke proof**: `xtensa-smoke/` is a bare-metal xtensa
    binary (own reset vector, linker script, UART0 console, panic
    handler) that boots under `qemu-system-xtensa -machine esp32s3`,
    drives a `Db` through put/get/delete/flush/compact/scan/snapshot
    against a RAM-backed `BlockDevice`, and prints `SMOKE PASS`/`FAIL`
    over UART0. `run-smoke.sh` builds the flash image and runs QEMU.
    This is the "it runs on the chip" proof — not just "it compiles".
  - **SPI-flash `BlockDevice`**: `src/flash.rs` defines the `Flash`
    trait (sector erase + program + read — the only `unsafe`-needing
    half, implemented per-board) and `FlashBlockDevice<F>`, the safe
    erase-aware `BlockDevice` wrapper. Horton block writes are
    whole-sector (BLOCK = 4096 = flash sector size), so a write is
    erase-sector then program — no read-modify-write, no hidden RAM.
    The ESP32-S3 SPI register implementation is v0.7's
    `src/esp32s3.rs` (below); on-silicon verification of the command
    sequences still needs real hardware.
    The wrapper's erase discipline is property-tested on host against a
    strict mock flash (erase sets 0xFF, program only clears bits,
    programming an unerased sector is an error).
  - **Budget re-tune**: the `ESP32S3` const profile
    (`BLOCK=4096, KEY_MAX=32, VAL_MAX=64, CAP=16, ARENA=2048, ...`)
    sizes a database at well under 64 KiB of RAM; `BUDGET.md` shows the
    accounting and the profile carries compile-time size assertions.
  - Honest limits: QEMU proves logic on the target ISA, not flash
    programming or timing; the SPI MMIO primitive and power-loss
    behavior need hardware. Throughput numbers, if any, are measured —
    never estimated.
- v0.7 — Real ESP32-S3 SPI flash driver (`src/esp32s3.rs`).
  - **Scope**: the `Flash` trait's ESP32-S3 implementation — the half
    v0.6 explicitly deferred as "Tallow driver work". It lives in this
    crate (not Tallow) because it is Horton's board seam, and it stays
    `#![no_std]` + `#![no_alloc]` + core-only like everything else.
  - **Design**: `SpiFlash<B: RegBus>` is generic over a tiny register
    bus trait (`read`/`write` at SPI1 offsets). All of the
    driver's logic is safe (`#![forbid(unsafe_code)]` holds for the
    whole crate): the volatile-MMIO adapter — the half-dozen lines that
    actually touch `0x6000_2000` — lives in the board/Tallow crate,
    which owns the `unsafe`. Host tests plug in a mock bus backed by
    an emulated NOR chip.
  - **Register sequences** follow ESP-IDF v5.2's LL layer verbatim
    (`components/hal/esp32s3/include/hal/spimem_flash_ll.h`,
    `spi_flash_hal_iram.c`), not the TRM prose:
    - `erase_sector`: WREN → ADDR → save CTRL / CTRL=0 → dedicated SE
      bit → spin on CMD-bit clear → restore CTRL → RDSR, poll WIP with
      a bounded spin cap (`Error::Timeout`).
    - `program`: WREN, then per chunk (≤ 64 bytes — the W0–W15 buffer —
      and never crossing a 256-byte page): ADDR = `addr | (len << 24)`,
      words into W0.., `usr_dummy = 0`, dedicated PP bit → spin →
      WIP poll. Chunking matches ESP-IDF's `set_buffer_data`.
    - `read`: user-mode `0x03` transactions (CMD 8b, ADDR 24b, MISO),
      ≤ 64 bytes each, straight from the chip. This deliberately
      bypasses the DROM flash cache: a cached read path would need a
      cache invalidate after every program/erase (ROM
      `Cache_Invalidate_Addr`, unverifiable in this environment and
      fragile to link by hand), while user-mode reads are
      correct-by-construction and fully provable in the host mock.
      Cost is bounded and measurable on silicon later; the cached-read
      optimization is future work, not a v0.7 claim.
    - Every WREN is followed by a WEL check via RDSR
      (`Error::WriteEnableFailed`); every poll is bounded.
  - **Proof** (this is the leg that changed vs the plan): a bare-metal
    probe run 2026-09-15 showed QEMU's `esp32s3` machine does **not**
    emulate the SPI_MEM user-command path — DROM CPU reads return
    `0xDEADBEEF` regardless of flash contents, CMD bits clear
    instantly with no observable effect, and MISO reads return `0xFF`
    for regions known to hold other bytes. So the proof is:
    (1) host tests against the mock NOR — exact register-write
    sequences asserted, WEL/WIP semantics emulated, plus a full `Db`
    on `FlashBlockDevice<SpiFlash<MockBus>>` doing puts/gets/flush/
    overwrite/reopen; (2) the Xtensa build gate — the driver builds
    for `xtensa-esp32s3-none-elf`; (3) QEMU runs the probe without bus
    faults (register addresses are at least mapped), with the
    non-emulation recorded here, not hidden.
  - Honest limits: on-silicon verification is still pending — actual
    command timing, WIP behavior, and power-loss during program/erase
    cannot be proven without hardware. No timing or power-loss claims
    are made.
- v0.8 — Compaction down every level (`Db::compact_step` generalization).
  - **Scope**: retire the v0.4–v0.7 L0→L1-only ceiling. `compact_select`
    takes the deepest full level (`>= TABLES` tables, scanning
    `LEVELS - 2` down to 0): a full L0 compacts all of L0; a full deeper
    `Ln` compacts its oldest table (index 0 — FIFO with no extra state)
    plus the `L(n+1)` tables overlapping the closure of its key range.
    Output lands in `L(n+1)`; `bottommost` is recomputed against the
    levels below the target; the merge, commit, crash-safety, and
    snapshot keep-set logic are untouched (all already level-generic).
  - **NoSpace moves to select time**: the target-level capacity check
    (`len - overlapping_inputs + 1 <= TABLES`) now runs before any merge
    I/O. Deepest-first selection means a full non-bottom target can never
    be picked (it would have been selected itself); the only honest
    ceiling left is a full bottommost level whose tables the merge does
    not absorb.
  - **Proof**: RED suite in `tests/compact.rs` — L1→L2 drain when L1 is
    full (the old `NoSpace` test retired), deepest-first priority with L0
    and L1 both full, version/snapshot correctness through a two-level
    cascade, the non-overlap invariant on L2+, crash-injector for the
    L1→L2 path (state exactly pre- or post-compaction, never mixed), and
    `NoSpace` only for a genuinely full bottom level with disjoint ranges.
  - **Measured**: 140 debug + 140 release tests green; `cargo fmt --check`
    clean; clippy pedantic + nursery zero warnings on all targets;
    `xtensa-esp32s3-none-elf` build passes; the v0.6 bare-metal QEMU
    ESP32-S3 smoke regression still ends in `SMOKE PASS`; no
    `unwrap`/`expect`/`panic!` in non-test source.
  - **Driving to idle**: `compact_step` returns `Progress::Done` both when
    a selected job finishes and when no job was selected, so a
    `while ... == Progress::More` loop drives exactly one job. The new
    `Db::compaction_pending()` query reports whether a job is selectable
    right now; firmware idle loops and test drivers loop
    `while db.compaction_pending() { drive_one_job(); }` to drain.
  - **v0.7 audit corrections folded into this release** (audited
    2026-09-15, before v0.8 shipped):
      - `SpiFlash::erase_sector` now restores `CTRL` on every error path —
        the `spin_cmd(CMD_SE)` timeout previously returned with `CTRL`
        still cleared, destroying the boot-configured read mode. RED test
        `erase_timeout_restores_ctrl` (new `MockMode::HangSe`), then the
        fix: capture the spin result, restore `CTRL`, then propagate.
      - User-address encoding verified against the ESP-IDF v5.2 reference
        (`spimem_flash_ll_set_usr_address` writes the raw address to
        `dev->addr`; no bit-shifting) — Horton's raw `REG_ADDR` write is
        silicon-correct per the reference. The mock mirroring it is
        therefore not circular on this point.
      - SPEC claimed `RegBus` had `read`/`write`/`read_word`; the trait has
        only `read`/`write` — the doc, not the code, was wrong (fixed).

- v0.9 — The proof leg (§8 test strategy, in full).
  - **Scope**: close the three remaining verification gaps named in §8.
    (1) `cargo miri` over the test suite — the crate is
    `#![forbid(unsafe_code)]`, so miri is a pure UB-freedom check; the
    gate is green on every test binary miri can run in the sandbox
    (documented below with the exclusions, if any). (2) Structure-aware,
    in-tree, dependency-free fuzzers for the WAL record decoder and the
    SSTable block decoders: a seeded deterministic PRNG drives
    bit-flips, truncations, and splice mutations over valid encoded
    inputs; the assertion is never "correct output" but "no panic, no
    UB, only clean `Error`s". (3) Crash-injector exhaustiveness beyond
    flush: enumerate every block write as a crash point over compaction
    merges and manifest commits (the surface v0.4–v0.8 added), asserting
    the recovered DB equals the prefix-oracle state — never a mixture
    of pre- and post-commit.
  - **Measured**: miri 18/20 test binaries green (lib, alloc, compact,
    crash_compact, crash_flush, crc, db incl. the exhaustive 64-script ×
    four-crash-position injector and `oracle_random`, wal, wal_wrap,
    memtable, manifest, sstable, scan, read_path, flush, flash, profile,
    orphan_reclaim); fuzz 5/6 tests green under miri (wal_corpus,
    wal_decoder, sstable_corpus, manifest_corpus, manifest_decoder).
    Two environmental exclusions, both infrastructure hangs rather than
    test failures, in a sandbox that rebooted 3× during the campaign:
    (a) `esp32s3` — miri-as-rustc hangs on a futex during compilation
    (cold sysroot rebuild did not help; plain rustc compiles fine);
    (b) `sstable_decoder_never_panics` — miri hangs after ~17 min CPU
    on the 300-mutation SSTable decoder fuzz (the other five fuzz tests,
    same decoder families, are green under miri). All 900 decoder
    mutations (300 each over WAL records, SSTable blocks, manifest
    entries; seeded LCG, bit-flips/smears/truncations/splices, CRC
    recomputed on some SSTable mutations to reach deeper parsing) pass
    natively. Crash-compaction injector: 5 writes × crash positions
    0..=5, recovery exposes either four L0 tables or one L1 table with
    all four acknowledged keys intact — 2/2 green. Full suite:
    148 debug + 148 release green. `cargo fmt --check` clean;
    clippy `--all-targets` pedantic+nursery zero warnings;
    `#![forbid(unsafe_code)]` holds (no production unsafe, no
    non-test unwrap/expect). Xtensa ESP32-S3 build gate: PASS (see
    log). QEMU smoke: PASS.

- v0.10 — Archive API: seal a table, stream it to remote storage, forget
  it locally.
  - **Scope**: the flush-to-object-storage primitive for Tallow's Wi-Fi/TLS
    side. `Db::level_tables(level)` lists the archive candidates at a level,
    oldest first; `Db::archive_plan(level, table_id)` returns an
    `ArchivePlan` — the level plus the `TableRef` (id and block range). The
    caller streams every block in `[first_block, end_block())` through the
    now-public `Db::device()` to durable remote storage, confirms the
    upload, then calls `Db::archive_commit(level, table_id)`, which drops
    the table from the manifest in one atomic manifest write and reclaims
    its blocks into the free list strictly after the visibility point
    (best-effort, exactly like compaction: a full free list cannot fail the
    already-committed job; orphans are swept by the next open). Horton
    performs no networking — the sink is caller code, and the table's bytes
    are immutable once sealed, so the upload needs no coordination with
    horton beyond "all bytes, then commit".
  - **Crash ordering** is what makes this safe: a crash anywhere before the
    commit leaves the table in the manifest, so the upload simply repeats —
    the sink must therefore be idempotent per table id (re-uploading a fully
    uploaded table is always allowed). A crash during the commit is decided
    by the atomic manifest write: old slot (table still local, re-upload)
    or new slot (table gone, bytes already remote). The commit never runs
    before the upload is confirmed, so no crash can lose acknowledged data.
    `archive_commit` is itself idempotent: `Ok(false)` when the table is
    already gone — retry after an ambiguous crash, or a concurrent
    compaction merged it away (the uploaded bytes remain a valid copy of
    that data).
  - **Tombstone rule** (the sharp edge, stated plainly): the commit drops
    the table's tombstones from the local view. Archiving a tombstone-
    bearing table above a deeper table resurrects the older version
    locally. Insert-only workloads — sensor logs with timestamp keys — may
    archive from any level; delete-bearing workloads must archive only from
    the bottommost level, where nothing is deeper and nothing can
    resurrect. There is no combined remote/local read model yet, so this
    is a caller-side discipline enforced by documentation, not by code.
    (v0.12: the discipline is now enforced by `archive_commit` itself —
    and the check is strictly stronger than the documented rule, which
    missed resurrection via same-level or shallower tables; see below.)
  - **Proof**: `tests/archive.rs` — (1) round-trip: stream all planned
    blocks into a mock sink, reopen the uploaded bytes through
    `TableReader`, verify every key, commit idempotently, confirm archived
    keys disappear locally and that reclaimed blocks are reused by a later
    flush; (2) crash boundary: crash-inject the upload/manifest boundary at
    every write position — recovery exposes either the complete local table
    or the committed remote copy plus the remaining local table, and retry
    converges when the commit was lost; (3) an executable demonstration of
    the tombstone-resurrection hazard above.
  - **Honest limits**: horton never sees the network; upload durability is
    the caller's claim, and table identity across re-uploads is the sink's
    per-table-id idempotency contract, not horton's. The archive API moves
    data one sealed table at a time — no multi-table transaction.
  - **Measured**: 151 debug + 151 release tests green (148 carried from
    v0.9 plus the 3 new archive tests); `cargo fmt --check` clean; clippy
    `--all-targets` pedantic+nursery zero warnings with `-D warnings`;
    `#![forbid(unsafe_code)]` holds (the single `unwrap` in `src/` is
    inside `model.rs`'s `#[cfg(test)]` module); zero dependencies, no `std`
    in `src/`; release benches compile; `xtensa-esp32s3-none-elf` build
    gate passes; `cargo +nightly miri test --test archive` 3/3 green.
    Edition 2021 → 2024 (standing order) with rustfmt normalization and
    9 collapsible-`if` collapses the newer clippy demanded — all
    semantics-preserving; full suite re-greened after each. The v0.9 miri
    qualifications (esp32s3 compile hang, sstable_decoder_never_panics
    hang — both infra, not assertions) are unchanged and not re-litigated
    here.

- v0.11 — Atomic write batches.
  - **Scope**: `WriteBatch<const KEY_MAX, VAL_MAX, OPS>` — a caller-owned,
    fixed-capacity batch of puts and deletes, no allocation.
    `Db::write(&mut self, batch) -> Result<u64, Error>` applies every op
    atomically and returns the base sequence number (op `i` gets
    `base + i`); an empty batch is a no-op returning the current
    `next_seq`. Validation is total and up front — key/value sizes at build
    time, then WAL-block fit (structural, so an unatomically-large batch is
    rejected the same way regardless of DB state), then memtable slot/arena
    capacity for the whole batch — so a rejected batch is refused before a
    single byte is staged and
    leaves no trace: no WAL records, no staged bytes, no consumed seqs.
  - **Crash ordering** (atomicity without a new WAL record type): the
    batch is staged into the WAL's RAM buffer and made durable by ONE
    `commit()` — a single block write plus device flush. Crash before the
    commit: nothing durable, batch absent. Crash during the block write:
    torn block, CRC fails, recovery stops before the batch — batch
    absent. Crash after: recovery replays every record — batch present,
    whole. The existing torn-tail rule gives all-or-nothing for free,
    provided a batch never triggers an intermediate block write;
    `Db::write` enforces this by requiring the batch's total encoded size
    to fit one block (`Error::BatchTooLarge` otherwise) and committing
    any previously staged data first. An over-block batch is rejected,
    never split — splitting would silently void the atomicity contract.
  - **Commit-failure rollback** (folded-in fix): `put`/`delete`/`write`
    snapshot the WAL stage before staging; if the block write fails, the
    stage is truncated back to the snapshot, so a failed mutation can
    never resurrect through a later commit. If the block landed but the
    device flush failed — the device lied, indistinguishable from a crash
    at that instant — the mutation's sequence numbers are consumed and
    the batch may surface atomically on the next recovery, exactly as a
    crash would. Previously a failed `put` left its record staged *and*
    reused its sequence number: a latent resurrection + seq-reuse bug,
    now closed.
  - **Proof**: `tests/write_batch.rs` — happy-path atomicity (all keys
    visible, consecutive seqs, base seq returned), duplicate keys
    last-wins within the batch, empty batch is a seq-conserving no-op,
    over-block batch rejected with no trace (later ops take the expected
    seqs), memtable-full rejection leaves WAL and seqs untouched, deletes
    land as tombstones; crash injector over `Db::write` at every
    block-write position — recovery exposes all or none of each batch,
    and later writes never reuse a batch's seqs.
  - **Honest limits**: an atomic batch is bounded by one WAL block —
    worst case `OPS * (23 + KEY_MAX + VAL_MAX) <= BLOCK` bytes; larger
    batches must be split by the caller into multiple `write()` calls,
    each atomic alone but not atomic together. No cross-batch
    transactions; that remains future work.
  - **Measured**: 159/159 tests green in debug and release (8 new in
    `tests/write_batch.rs`, including the crash-injector atomicity proof at
    every block-write position); `cargo fmt --check` clean; clippy
    pedantic+nursery zero warnings; `xtensa-check.sh` PASS;
    `cargo +nightly miri test --test write_batch` 8/8 green; no
    `unwrap`/`expect` in non-test source.

- v0.12 — Combined remote/local read model + table re-attach + tombstone-rule enforcement.
  - **Scope**: completes v0.10's archive lifecycle. (a) The combined
    remote/local read model: a table archived to remote storage and later
    re-attached is consulted by `get`/`get_at`/`scan` alongside
    never-archived tables, with highest-sequence-wins across both. This is
    delivered as a specified-and-proven model, not new read-path
    machinery: a re-attached table re-enters as an ordinary L0 table, so
    the existing seq-ordered machinery covers it (see Design). (b)
    `Db::ingest_table`: grafts an externally-stored sealed table back into
    the LSM. The caller returns the `SealedTable` descriptor
    (`ArchivePlan::sealed` — the placement-free half of the original plan:
    id, block count, key bounds, max seq, entry count) and a `&R:
    BlockDevice` source positioned at the table's first block; horton
    copies the blocks into the local table region, verifies the footer
    (magic + CRC32) and the entry count against the descriptor, then
    grafts the table into L0 in one atomic manifest commit — the visibility
    point, mirroring `archive_commit`. Returns `Ok(true)` on ingest,
    `Ok(false)` when the table id is already attached with a matching
    descriptor (the idempotent retry, mirroring `archive_commit`'s
    `Ok(false)`); `Error::IngestConflict` when the id is attached with a
    different shape (the caller mixed up tables). (c) `archive_commit` now
    enforces the tombstone rule mechanically, replacing v0.10's
    caller-side discipline: a table whose removal would resurrect a
    deleted key in any live view is refused with
    `Error::WouldResurrect { table }` before anything is mutated.
  - **Design** — the three load-bearing choices:
    - *Ingest copies; it does not reference.* A reference-attached cold
      tier (the manifest recording remote handles, reads hitting the
      network) would put a second device in `Db`'s type signature, thread
      remote reads through `get`/`scan`/compaction, and explode the crash
      model — for an embedded store whose reads must be bounded and
      offline-capable. Copying the sealed table back into the local table
      region keeps the manifest shape, the read path, compaction, and
      recovery structurally identical, so every existing proof still
      holds. Cost: one bounded, caller-driven table copy per re-attach. A
      reference-attached tier is future work, explicitly not claimed.
    - *Re-attach grafts at L0, not the original level.* L0 is the
      overlap-tolerant level — newest-first + highest-seq-wins reads where
      table position is only a pruning hint, whole-level compaction
      merging by seq — so an old-seq table grafted at L0's newest position
      is exactly what a flush does, and every ordering stays seq-exact.
      Grafting at the original level could violate the levels-≥1
      non-overlapping invariant (compaction has merged and drained levels
      since the archive), on which compaction's tombstone-drop logic
      relies: a new resurrection vector. The archived level is therefore
      informational on re-attach.
    - *A copied table is relocated, not just copied.* SSTable CRCs are
      position-independent, but index entries store absolute data-block
      ids and the footer stores the absolute bloom/index ids — a
      byte-for-byte copy into a different local run is structurally valid
      yet points back at the old placement (found the hard way: identical
      bytes, matching CRCs, reader still lost). `sstable::relocate_table`
      rewrites those pointers to the destination layout and re-seals the
      index/footer CRCs. The table's original base is derived from the
      footer's own pointers (old bloom id minus the data-block count, with
      the bloom/index consecutiveness cross-checked) — never from the
      caller's remote offset, which is a device position, not a placement.
      Index entries that do not land inside the derived original run are
      `CorruptBlock`, not silently mis-relocated.
  - **Crash ordering**: ingest streams blocks into a reserved-but-unclaimed
    run (free list first, then the bump — mirroring flush), verifying each
    block's CRC32 as it lands, so a crash mid-copy leaves only orphans:
    free-list blocks stay free-listed, bump blocks sit above the resume
    point, and the next `open()` sweep reclaims both — exactly flush's
    torn-table story. After the copy, the index/footer relocation rewrites
    (two more block writes) and the footer/entry-count validation all
    happen BEFORE the manifest commit, so a corrupt or misplaced copy can
    never become visible; re-running ingest after any crash re-copies from
    the source first, so a half-relocated copy is always overwritten
    before relocation runs again. The manifest commit is the atomic
    visibility point (it flushes the device, so the table blocks are
    durable first): crash before it leaves the table absent and retry
    converges via the idempotent id check; crash during it leaves the old
    or the new slot, never a mix. `archive_commit`'s enforcement check is
    pure reads before the staged manifest write — no new write positions,
    so v0.10's crash proof for the commit stands unchanged.
  - **The enforcement check, exactly**: a table with no tombstones is
    always safe to archive — with no tombstones in `T`, the pre-archive
    winner of any key deleted in any view is a tombstone outside `T`,
    which still wins post-archive, so no deleted key can go live (this
    fast path falls out of the loop below: no tombstone entries, no
    checks). Otherwise, for each tombstone `(k, s)` in the table
    (streamed via the compaction entry cursor, one at a time, no
    accumulation) and each live view `t ∈ {u64::MAX} ∪ {live snapshot
    watermarks}` with `s ≤ t` (a view that predates the tombstone cannot
    see it): `cur = get_at(k, t)`; when `cur` is `None` — the tombstone is
    the view's winner — compute `alt = get_at(k, t)` excluding the table,
    with the SAME watermark; when `alt` is `Some`, archiving would
    resurrect a live value in that view and the commit is refused. The
    same-watermark comparison is load-bearing: reading the alternate at
    `min(t, s)` would hide later protective tombstones and falsely report
    a resurrection. The check is exact: `cur = None ∧ alt = Some` at the
    same watermark holds exactly when the archived tombstone was hiding a
    live value. It is strictly stronger than v0.10's documented
    discipline, which warned only about deeper levels: a same-level or
    shallower table holding a pre-delete value resurrects just as well.
    The sharpest case is v0.12-native — archive `T(k→v@s1)`, delete `k`
    `(s2)`, compact the tombstone to the bottommost level, re-ingest `T`
    at L0, then archive the bottommost table: v0.10's "bottommost is
    always safe" would revive `v@s1`; the check refuses it. The v0.10
    proof `archive_l0_tombstone_resurrects_older_version` now asserts the
    refusal (`Err(WouldResurrect)`, reads unchanged) instead of the
    resurrection it used to demonstrate.
  - **Proof**: `tests/attach.rs` — ingest round-trip (upload to a mock
    remote `MemDevice`, archive, re-ingest, every key readable; second
    ingest is `Ok(false)`; reclaimed blocks reused by a later flush);
    highest-seq-wins across re-attached and local tables (a newer local
    value wins, remote-only keys reappear, a later delete wins); `scan`
    merges the re-attached keyspace; snapshot coherence (a re-attached
    entry with `seq ≤ watermark` is visible at the snapshot — the
    seq-based contract, undisturbed by the remote round trip);
    enforcement: same-level and deeper-level resurrection both refused
    with `WouldResurrect`, the v0.10-doc correction (bottommost archival
    refused after re-ingest), tombstone-free tables archive freely, and a
    refused table archives cleanly once the shadowing value is gone
    (covered in `tests/archive.rs`, which now asserts the refusal);
    footer/entry-count verification (corrupt remote bytes →
    `CorruptBlock`, manifest untouched; descriptor mismatch →
    `CorruptBlock`); mismatched source block size → `BadBufferLen` up
    front; id conflict (`IngestConflict`); `NoSpace` when L0 is full;
    `next_table_id` advancing past an ingested id on a fresh
    database; crash injector over the ingest copy loop, the two relocation
    writes, and the manifest commit — recovery exposes the table fully
    present or fully absent, and retry converges to exactly-once.
  - **Honest limits**: re-attach is a full table copy — there is no
    network-attached cold tier; every re-attached read is local.
    `ingest_table` requires the source's `BLOCK` to equal the database's
    (`Error::BadBufferLen` up front — the copy buffer is one block and
    the trait contract pins `buf.len()` to the device's own size) and its
    `Error` type to convert into `D::Error` (`R::Error: Into<D::Error>`,
    one unified error enum is the expected caller shape). Every copied
    block's CRC is verified eagerly as it lands (so a torn remote block
    fails the ingest, never a later read); the index/footer pointers are
    then relocated and re-sealed, and data/index/bloom CRCs are re-verified
    lazily on the read paths exactly like locally-flushed tables. The
    tombstone check costs up to `tombstones × (1 + live snapshots)` point
    reads; archival is caller-driven and rare, so this is bounded
    but not free.
  - **Measured**: 174/174 tests green in debug and release (159 carried
    from v0.11 plus the 15 new `tests/attach.rs` proofs, and the v0.10
    `archive_l0_tombstone_resurrects_older_version` proof updated to assert
    the new `WouldResurrect` refusal); `cargo fmt --check` clean; clippy
    `--all-targets` pedantic+nursery zero warnings with `-D warnings`;
    `xtensa-check.sh` PASS; `cargo +nightly miri test --test attach` 15/15
    green; no `unwrap`/`expect`/`panic!` in non-test source
    (`debug_assert!` only, side-effect-free); zero dependencies, no `std`
    in `src/`, `#![forbid(unsafe_code)]` holds.

- v0.13 — Hand-rolled block compression.
  - **Scope**: LZ77 block compression for SSTable data blocks, written
    from scratch (no dependency, no `std`, no allocation, no panics):
    new module `src/compress.rs` with `compress` / `decompress` and a
    caller-owned `CompressScratch<BLOCK>` (hash table + compressed-output
    staging). The writer trial-compresses every sealed data block and
    keeps the compressed form only when it saves at least
    `COMPRESS_MIN_SAVING` (128) bytes — otherwise the block is stored
    raw. Tables therefore mix compressed and uncompressed blocks freely,
    and pre-v0.13 all-raw tables read unchanged. Bloom, index, and footer
    blocks are never compressed.
  - **Format** — the per-block flag: data blocks already end with a
    restart trailer whose last u16 is the restart count (writer-capped at
    128, so bit 15 is always free). A sealed data block whose trailer u16
    has bit 15 set is compressed: the low 15 bits are the compressed
    payload length `clen`, and bytes `[0..clen]` are the compressed
    stream. Bit 15 clear is the legacy raw layout, unchanged. The
    compressed stream encodes the *entire* logical block `[0..BLOCK-4]`
    (entries + zero fill + restart trailer), so the decompressed size is
    always exactly `BLOCK - 4` — no length prefix, no ambiguity, and the
    existing parsers run on the decompressed bytes untouched. The stream
    itself is token/literal/match triples (4-bit literal length, 4-bit
    match-length-minus-4, LZ4-style extension bytes, u16-LE match offset,
    minimum match 4). The CRC still covers the physical block, so random
    corruption is caught before the decoder ever runs; the decoder
    additionally bounds-checks every read and write and returns
    `CorruptBlock` on any malformed stream instead of panicking.
  - **Caller scratch, both directions**: compressing needs working
    memory, so `TableWriter::push` / `finish` and `write_table` take
    `Option<&mut CompressScratch<BLOCK>>` (`None` = store raw, for
    callers that want no compression). `Db::flush` owns one scratch per
    flush on its stack; compaction owns one per job. Decompressing needs
    a second block buffer on the read path: `Db` gains
    `decomp_scratch: RefCell<[u8; BLOCK]>` beside `get_scratch`
    (same borrow discipline), and scan/compaction cursors thread their
    own. `sstable::read_data_block` is the single funnel — read the
    physical block, verify its CRC, branch on the flag bit, decompress
    into the caller buffer when flagged — used by point reads, scans,
    compaction cursors, `EntryStream`, and the v0.12 tombstone check.
  - **Crash model**: unchanged. Compression is a pure function applied
    at seal time; the physical block (one block, CRC-sealed) and the
    table's block count are identical in shape to raw blocks, so flush's
    and compaction's crash stories hold verbatim. A torn compressed
    block fails its CRC exactly like a torn raw block.
  - **Proof**: `tests/compress.rs` — codec round-trips (empty, 1-byte,
    all-zero, all-random, structured KV-ish data, max-size blocks);
    decoder fuzz: a deterministic xorshift stream mutates valid streams
    (bit flips, truncations, splices) and asserts the decoder never
    panics — it returns `Err` or a fully-formed block; `cargo +nightly
    miri test --test compress` green (Miri turns any UB or panic into a
    failure). Ratio measurement on realistic KV data (common key
    prefixes, JSON-ish values, prose): asserted to beat raw by a real
    margin, with the measured ratio recorded here. Integration:
    flush/scan/compact/ingest round-trips over compressed tables, plus a
    mixed table (forced raw + forced compressed blocks) reading exactly.
  - **Honest limits**: compression is best-effort per block — random or
    already-compressed values store raw, and the 128-byte saving
    threshold means marginally-compressible blocks stay raw too. No
    dictionary, no training, no cross-block matches (each block is
    independent, so random access never decompresses a neighbor). Every
    read of a compressed block pays a decompression pass; write pays one
    trial compression per data block plus 8 KiB of transient scratch
    (4 KiB hash table + 4 KiB output staging, caller-owned). `clen` fits
    15 bits: blocks whose compressed form exceeds 32767 bytes are stored
    raw (irrelevant at `BLOCK = 4096`; documented, not silent).
  - **Measured** (2026-09-23):
    - 186 debug + 186 release tests green (174 carried from v0.12, 12 new
      in `tests/compress.rs`); `cargo fmt --check` clean; `cargo clippy
      --all-targets -- -D warnings -W clippy::pedantic -W clippy::nursery`
      zero warnings; `./xtensa-check.sh` PASS (xtensa-esp32s3-none-elf);
      `cargo +nightly miri test --test compress` 12/12 green.
    - Ratios (BLOCK=4096, body 4092): structured KV data (common key
      prefixes, JSON-ish values) compresses to 905/4092 = 0.221 (78%
      saving); all-zero block to ~0.001; incompressible random stays raw
      (codec declines, `COMPRESS_MIN_SAVING` = 128 enforced).
    - End-to-end: flush of realistic data → 3/3 data blocks flagged;
      4-table compaction → merged output 4/4 data blocks flagged, all 64
      keys read exactly; archive→ingest round-trip preserves flags
      bit-for-bit (payloads never re-compressed).
    - Sizes: `CompressScratch<4096>` = 8,200 bytes (4,096 hash + 4,096
      staging + 8 bookkeeping). ESP32-S3 profile: Db 17,064 + Scan
      10,392 + Compaction 56,192 = 83,648 bytes, under the re-tuned
      96 KiB `ESP32S3_RAM_BUDGET` (was 64 KiB; raised for v0.13 — see
      `src/profile.rs` and `BUDGET.md` for the accounting).
    - Audits: zero `unwrap`/`expect`/`panic!` in non-test `src/`;
      `#![no_std]` + `#![forbid(unsafe_code)]` hold; zero dependencies;
      every data-block read funnels through the flag check
      (`read_data_block`/`inflate_data_block`); index/footer/bloom/manifest/WAL
      paths verified never to touch the flag bit.
- v0.14 — Reverse iteration.
  - **Scope**: `RevScan` — the descending mirror of `Scan`.
    `seek_prev(from, lower, max_seq)` positions at the last entry `<= from`
    (an empty `from` starts at the last key; `lower` is an exclusive lower
    bound, `None` scans to the first key); `prev(key_buf, val_buf)` yields
    entries in descending key order. Merges the memtable and every SSTable
    with the same highest-sequence-wins rule; tombstones are skipped
    silently; entries with `seq > max_seq` are invisible, so scans at a
    `Db::snapshot` watermark are repeatable. The borrow discipline matches
    `Scan`: the scan borrows the database, so `put`/`flush`/`compact` cannot
    shift cursors mid-scan. The buffer contract matches too:
    `BufferTooSmall` fires before any cursor advances, so a retry yields the
    same entry.
  - **Design** — three load-bearing choices:
    - *Ceiling-parked cursors, not backward links.* Blocks are
      forward-linked only, so each table cursor parks at the greatest
      visible entry satisfying a ceiling (`<= from` at seek,
      `< yielded_key` after each yield). Parking binary-searches the
      block's restart points for the last restart that can lead to a
      qualifying entry, then scans regions backward (newest-first version
      runs mean a backward region walk with a `>=` merge keeps the newest
      visible version of each key). Per park: O(log R + regions), not
      O(block).
    - *The block walk goes down.* Initial positioning resolves the last
      block with `first_key <= from` (new
      `sstable::index_last_le_block`, sharing the index binary search with
      `index_lookup`); a cursor that finds nothing visible in its block
      steps to the previous data block. Every entry in block N-1 sorts at
      or below block N's first entry, so the backward walk is complete,
      and the ceiling still applies per block — a version run straddling a
      block boundary cannot resurrect an already-yielded key.
    - *Version runs are followed across blocks.* A block can seal
      mid-run, so the run's newest versions may live in an earlier block
      than the one the index resolves. Whenever a park's candidate is the
      block's first key, the cursor follows the run backward
      (`resolve_run`), adopting each earlier block's newest visible
      version of the same key until a block's first key differs. Without
      this, a backward walker parks on a stale version it met first —
      caught by `revscan_seek_at_cross_block_version_run` during
      development (seeking at `k` with a watermark hiding the run's head
      returned v4 instead of v19).
    - *Separate `RevScan`, shared block format.* The forward `Scan` is
      untouched; `RevScan` reuses the verified readers
      (`parse_data_entry`, `data_entries_end`, the CRC + inflation funnel)
      and walks the memtable's sorted slots downward from `lower_bound`.
      No format change, so the crash model is unchanged: reverse iteration
      only reads.
  - **Crash model**: unchanged — `RevScan` performs no writes. A torn data
    block is `CorruptBlock`, never a silent skip, exactly like the forward
    scan.
  - **Proof**: `tests/revscan.rs` (15 tests) — mirrors `tests/scan.rs`:
    empty DB; memtable-only descending; `seek_prev` at/above/below keys
    and at empty `from`; exclusive lower bound; multi-table merge with
    overlapping keys; dedup (highest sequence wins); tombstone
    suppression; snapshot isolation; `BufferTooSmall`-before-advance on
    both buffers; re-seek repositioning; cross-block version runs
    (newest-visible-wins at snapshot watermarks, including the
    mid-run-seal case that caught a real stale-version bug); runs longer
    than one restart interval; randomized differential test against a
    `BTreeMap` oracle; forward/reverse agreement (reversing the forward
    range yields the reverse range, bounded and unbounded);
    `cargo +nightly miri test --test revscan` green.
  - **Honest limits**: no descending point lookup; reverse scans hold the
    same `&Db` borrow as forward scans. Per-`prev()` cost is linear in the
    source count, like the forward scan.
  - **Measured** (2026-09-23):
    - 201 debug + 201 release tests green (186 carried from v0.13, 15 new
      in `tests/revscan.rs`); `cargo fmt --check` clean;
      `cargo clippy --all-targets -- -D warnings -W clippy::pedantic
      -W clippy::nursery` zero warnings; `./xtensa-check.sh` PASS
      (xtensa-esp32s3-none-elf); `cargo +nightly miri test --test revscan`
      15/15 green.
    - Correctness: `revscan_seek_at_cross_block_version_run` caught a real
      stale-version bug during development — a backward walker parking at
      a block's first key settled on an older version when a block sealed
      mid-run; the fix (`resolve_run` follows the run into earlier
      blocks) is proven by the same test, including at a snapshot
      watermark hiding the run's head (v19 selected over v4).
    - Audits: zero `unwrap`/`expect`/`panic!` in non-test `src/`;
      `#![no_std]` + `#![forbid(unsafe_code)]` hold; zero dependencies.
- v0.15 — Range deletes + TTL.
  - **Scope**: `Db::delete_range(start, end)` writes a range tombstone —
    one sequence number shadowing every key in `[start, end)` — and
    `Db::put_with_ttl(key, val, expire_at)` writes a value that reads
    suppress once a caller-supplied time reaches `expire_at`. Horton owns
    no clock: every read takes a `now: u64` (monotonic caller tick —
    seconds, millis, or a logical epoch; only ordering matters), defaulting
    to 0 ("no time has passed") on the existing APIs. Range tombstones
    flow through the WAL, memtable, SSTables, point reads, forward and
    reverse scans, flush, archive/ingest, and compaction.
  - **Time model** (the one load-bearing decision): Horton never calls a
    clock. `put_with_ttl` stores an absolute `expire_at`; reads compare it
    against the `now` the caller passes. A value with
    `expire_at <= now` is suppressed — the read behaves as if the newest
    visible version were absent — but the bytes stay on device until
    compaction's purge removes them. `expire_at == 0` means "no expiry"
    and is stored exactly like a plain `put` (op byte `Put`, no expiry
    field), so non-TTL data pays nothing. Clock skew between writers is
    the caller's problem; Horton only promises the ordering contract:
    suppress iff `expire_at <= now`.
  - **Durable formats**:
    - WAL: `Op::RangeDelete = 3` reuses the record shape with
      key = range start, val = range end. `Op::PutTtl = 4` is a `Put`
      record with an 8-byte little-endian `expire_at` appended after the
      value (before the CRC); `Put`/`Delete`/`RangeDelete` records are
      byte-identical to v0.14.
    - SSTable data entries: op byte `4` marks a TTL entry, with the same
      8-byte expiry after the value. Entry header stays 13 bytes for
      non-TTL entries.
    - SSTable layout gains a range-tombstone section **before** the data
      blocks: `[rdel]* [data]* [bloom] [index] [footer]`. Rdel blocks
      hold `[start_len u16][end_len u16][seq u64][start][end]` entries
      sorted by `(start asc, seq desc)`, a `count u16` trailer, and the
      standard CRC. The footer grows by `rdel_blocks u32`; `TableRef`
      (and the manifest encoding) gains `rdel_blocks: u32` so readers
      find the section without extra I/O (`rdel_first = first_block`,
      `data_first = first_block + rdel_blocks`).
  - **Visibility ordering**: a range tombstone is a pseudo-version. The
    winner for a key at `max_seq` is the highest-`seq <= max_seq` entry
    among the key's point versions *and* every range tombstone covering
    the key (memtable slots scanned linearly; SSTable rdel blocks scanned
    per table with a one-block cache on the scan). A winning value with
    `expire_at != 0 && expire_at <= now` resolves to absent. Table
    `first_key`/`last_key`/`max_seq` expand to cover range-tombstone
    ranges and sequences, so pruning (`covers`, `max_seq <= best_seq`)
    stays sound.
  - **Compaction** — two mechanisms:
    - *TTL purge never silently drops.* Dropping a non-newest expired
      version is unsound: at a snapshot view where the expired version is
      the newest visible, reads must see absent, and only that version's
      presence (or a tombstone at its seq) guarantees it. So an emitted
      value with `expire_at <= purge_before` is **converted to a point
      tombstone at the same sequence number**, newest or not; the existing
      threshold/bottommost machinery then keeps or drops the tombstone
      exactly as if the caller had deleted the key. `purge_before == 0`
      disables the purge. The cutoff lives on
      `Compaction::purge_before` (set before driving a job; changing it
      mid-job is safe but incoherent — documented).
    - *Range-tombstone merge at select time.* The output's rdel section is
      fully determined by the inputs, so `compact_select` stream-merges
      the inputs' sorted rdel sections and writes the output rdel blocks
      before the data merge starts (same crash story as data blocks:
      invisible until the manifest commit, orphans swept on open).
      Same-`seq` overlapping tombstones coalesce to their union (exact
      for equal sequences). An older identical `(start, end)` tombstone is
      dropped only when the output is bottommost, its `seq` is below the
      oldest snapshot, **and** the newer identical tombstone shadowing it
      has `seq <= oldest_snapshot` — then no live snapshot falls between
      the two sequences, so the older was never decisive. Otherwise the
      older is retained: a live snapshot between the two sequences still
      needs it, and dropping it would resurrect covered keys at that
      snapshot. A tombstone that is the newest covering its range is never
      dropped (unlike a point tombstone, whose whole key goes with it —
      dropping a range tombstone alone would resurrect its covered
      values). Partial shadowing between different-`seq` tombstones is
      kept — correct, merely uncompacted (documented limit).
  - **Flush**: writes the memtable's range tombstones (sorted by start)
    as the table's rdel section, then data blocks; `TableRef` bounds and
    `max_seq` fold in the rdel ranges. A flush carrying only range
    tombstones still seals a table (zero data blocks).
  - **Archive interplay**: `archive_commit` refuses a candidate carrying
    range tombstones with `WouldResurrect` unless the tombstones provably
    shadow nothing outside the candidate — no other table's key range
    overlaps the tombstone range, and no memtable key inside the range
    changes visibility when the candidate is excluded (checked per key
    against the live and snapshot views). Conservative by design:
    enumerating shadowed keys across SSTables would be O(database) per
    tombstone. The documented remedy is to compact first (merging the
    tombstone into the overlapping table), then archive.
  - **Crash model**: `delete_range`/`put_with_ttl` are single-WAL-record
    atomic commits, exactly like `put`. Rdel blocks are ordinary block
    writes before the manifest commit — torn ones are orphans, swept on
    open, and the crash injector enumerates them automatically.
  - **Proof**: `model_visible` (model.rs) — the pure winner rule over
    version lists and range tombstones with TTL suppression — plus
    `tests/range_ttl.rs`: range shadowing incl. exclusive end,
    newer-put-wins, snapshot views, flush/compaction traversal, bottommost
    range-tombstone drop, TTL suppression before/after expiry in
    `get`/`scan`/reverse-scan, WAL recovery of both op kinds, compaction
    TTL→tombstone conversion (live view absent, snapshot view intact),
    archive refusal/acceptance, and a randomized differential test against
    the model; `cargo +nightly miri test --test range_ttl` green.
  - **Honest limits**: `WriteBatch` carries no range/TTL ops (documented;
    batches stay point-only). Range-tombstone dedup is limited to exact
    duplicates and same-seq coalescing — differently-sequenced overlaps
    are kept. Archive is conservative around range tombstones (refuses
    unless provably safe). Scans pay a per-table rdel-block scan per key
    (one-block cache); range-heavy tables make this linear in tombstone
    count — measured, not hidden.

- v0.16 — Caller-owned block cache.
  - **Scope**: a fixed-capacity, allocation-free SSTable block cache
    (`cache::BlockCache<const BLOCK: usize, const SLOTS: usize>`) that sits
    on the read path — point reads (`TableReader`), forward scans and
    reverse scans. Data, index, bloom-filter, footer, and range-tombstone
    blocks are all served from it; the WAL, manifest, and compaction merge
    reads bypass it deliberately (below). The cache is caller-owned memory
    in the strictest sense: it lives inside `Db` as a const-generic field
    (`CACHE` slots, `0` disables it), constructed by `Db::new`, counted in
    the RAM budget, never global, never allocated.
  - **Cache key**: `(table_id: u32, device_block_id: u64)`. Table ids are
    monotone and never reused within a manifest lineage
    (`next_table_id` only bumps; re-attach advances the floor past the
    ingested id), and a table's device blocks are immutable from the
    moment its id becomes visible. So a cached entry can never name live
    data it doesn't describe: even after a table is dropped and its blocks
    are reclaimed by the free list, the new table's fresh id misses the
    old entries. Correctness rests on id monotonicity, not on timely
    invalidation.
  - **Invalidation protocol** (explicit hygiene, not correctness): when
    compaction drops input tables — the only path that retires live table
    ids — `Db` calls `cache.invalidate_table(id)` for each dropped id,
    freeing the slots for the hot set. Flush creates tables under fresh
    ids (nothing to invalidate); re-attach relocates blocks *before* the
    manifest commit, so no read can have cached the destination yet, and
    the source blocks are copied verbatim (identical bytes would hit
    correctly anyway).
  - **Eviction policy: CLOCK (second-chance)**. Each slot carries one
    reference bit and the cache keeps one hand index — O(1) amortized,
    one byte of policy state per slot, no linked lists, no allocation.
    Chosen over direct-mapped (conflict misses: a sequential scan would
    evict hot index blocks it collides with, repeatedly) and over true
    LRU (needs a doubly-linked list — more mutable state, more proof
    surface — for a marginal win: scan streams defeat LRU and CLOCK
    equally, and the hot set behaves the same under both). Scan streaming
    gets one refinement: data blocks pulled by a scan insert *cold*
    (reference bit clear), so a full-table scan sweeps its own blocks out
    behind it instead of displacing the point-read hot set; index, bloom,
    footer, and rdel blocks insert hot. Compaction bypasses the cache
    entirely — it streams whole tables once with no reuse, and inserting
    that stream would churn the cache for zero benefit.
  - **Byte-identity rule**: the cache stores the physical block image
    exactly as the device returned it. CRC verification, the
    bloom-is-advisory rule, compression-flag inflation, and TTL/range
    shadowing all run *after* the cache, on identical bytes — a hit is
    indistinguishable from a re-read, including all corruption semantics.
    The v0.15 per-key rdel-block scan is the biggest winner: repeated
    covering probes across keys now hit the cache instead of the device.
  - **Concurrency**: `Db::get` is `&self`; the cache sits behind the same
    `RefCell` discipline as `get_scratch`. Contention (two interleaved
    `get`s on one executor) degrades to a silent bypass via
    `try_borrow_mut` — never a panic, never a wrong byte.
  - **Crash model**: the cache is DRAM-only and introduces no durable
    state and no new commit points — a crash simply empties it, and
    recovery never consults it. The crash injector needs no new cases;
    the new test pins the invariant (crash mid-write with the cache hot,
    reopen, reads are exact).
  - **Proof**: `tests/cache.rs` — hit/miss accounting against a counting
    `BlockDevice` (identical logical reads, strictly fewer device reads),
    byte-identity vs uncached reads incl. corrupt-bloom behavior,
    CLOCK eviction order, cold-insert scan behavior, `invalidate_table`
    slot reclamation, `CACHE = 0` disabled-cache correctness, compaction
    invalidation (post-compaction reads served fresh), re-attach safety,
    TTL/range-tombstone reads through the cache, and the crash-reopen
    invariant; `cargo +nightly miri test --test cache` green.
  - **Measured** (2026-09-23): `BlockCache<4096, 8>` = 32,920 bytes,
    `BlockCache<4096, 2>` = 8,248 bytes (4096-byte image + tag +
    bookkeeping per slot). ESP32-S3 profile (`CACHE = 2`, 2 slots):
    Db 25,576 + Scan 10,536 + Compaction 56,408 = 92,520 ≤ 98,304
    budget — 5,784 bytes of headroom, asserted by `tests/profile.rs`.
    Standard test profile (`CACHE = 8`): repeat point reads do zero
    table-region device reads (`tests/cache.rs`
    `repeat_point_read_is_served_from_cache`). Debug-stack note: the
    pre-existing ~352 KiB `Manifest::recover` stack probe plus an inline
    cache pushes stack-heavy debug tests near the 2 MiB test-thread
    limit; `CACHE = 8` on the standard `TestDb` keeps ~300 KiB of margin
    (fails at 1.625 MiB, passes at 1.75 MiB `RUST_MIN_STACK`).
  - **Honest limits**: the cache does not reduce the *first* read of a
    block, and a scan larger than the cache still streams from the device
    (cold insert only bounds the damage to the hot set). Hit rate is
    workload-shaped; the metrics (`Db::cache_stats`) are there so the
    caller can see it instead of trusting us.

