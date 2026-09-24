# ADR-0005: Re-attached archive tables are copied in and grafted at L0

Status: Accepted (v0.12; SPEC records no date)

## Context

v0.10 added the archive API: seal a table, let the caller stream its
blocks to remote storage, then drop it from the manifest with
`archive_commit`. horton performs no networking; the sink is caller
code. v0.12 had to bring archived tables back (`Db::ingest_table`) and
define how reads combine re-attached and never-archived data. Reads in
an embedded store must stay bounded and work offline.

## Decision

SPEC v0.12 names three load-bearing choices:

- **Ingest copies; it does not reference.** The caller passes the
  `SealedTable` descriptor and a `BlockDevice` source. horton copies the
  blocks into the local table region. A reference-attached cold tier,
  where the manifest records remote handles and reads hit the network,
  would put a second device in `Db`'s type, thread remote reads through
  `get`, `scan` and compaction, and explode the crash model.
- **Re-attach grafts at L0, not at the original level.** L0 tolerates
  overlap: reads are newest-first with highest-sequence-wins, and
  compaction merges it by sequence. An old-sequence table at L0's newest
  position is exactly what a flush produces. Grafting at the original
  level could break the non-overlap invariant of levels 1 and deeper,
  which tombstone dropping relies on, and so open a new resurrection
  vector. The archived level is informational only.
- **A copied table is relocated.** Index entries and the footer store
  absolute block ids, so `sstable::relocate_table` rewrites them for the
  destination and re-seals the CRCs. The original base comes from the
  footer's own pointers, never from the caller's remote offset.

The copy goes into a reserved-but-unclaimed run with each block's CRC
checked as it lands. The footer (magic and CRC) and the entry count
(against the descriptor) are validated before the single manifest
commit (ADR-0003). `Ok(false)` means the same table is already
attached; `Error::IngestConflict` means the id is attached with a
different shape.

## Consequences

- The manifest, read path, compaction and recovery stay structurally
  unchanged, so the existing proofs still hold. The combined
  remote/local read model is the existing sequence-ordered machinery.
- A crash mid-ingest leaves only orphans (flush's crash story), and a
  retry converges through the idempotent id check.
- Re-ingesting at L0 can put an old value above a newer tombstone that
  compaction has moved to the bottommost level, so v0.10's "archiving
  from the bottommost level is always safe" no longer holds (SPEC's
  "sharpest case"). The same release made `archive_commit` refuse any
  removal that would resurrect a deleted key (`Error::WouldResurrect`).
- Limits: re-attach is a full table copy and there is no
  network-attached cold tier. A reference-attached tier is future work.
- The source's `BLOCK` must equal the database's (`BadBufferLen`), and
  its error type must satisfy `R::Error: Into<D::Error>`.
- A re-attached table takes an L0 slot: ingest returns `NoSpace` when
  L0 is full.
- The resurrection check costs up to `tombstones × (1 + live snapshots)`
  point reads per archive.
- From v0.10: the sink must be idempotent per table id, and archive
  moves one sealed table at a time, with no multi-table transaction.

## References

- [Milestone log](../history/milestones-v0.1-v0.16.md) v0.12 (Scope, Design, Crash ordering, Proof, Honest limits)
- [Milestone log](../history/milestones-v0.1-v0.16.md) v0.10 (archive API, crash ordering, honest limits)
- SPEC §4.5, §4.6
