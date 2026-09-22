//! Executable models of horton's two core invariants.
//!
//! The Verus toolchain is not installed on this host, so these models are
//! validated by property tests — here for the pure functions themselves,
//! and differentially against the implementation in `tests/scan.rs` —
//! rather than by machine-checked proofs. They are written as pure, total
//! functions over plain value types (no I/O, no allocation), so each one
//! lifts directly into a Verus `spec` function when the toolchain is
//! available.
//!
//! The two core invariants:
//!
//! 1. **Commit atomicity.** A crash at any point leaves exactly the
//!    pre-commit or the post-commit state, never a mix. Both flush and
//!    compaction publish new state through a single double-buffered
//!    manifest commit; [`model_manifest_recover`] is the recovery rule the
//!    crash injectors validate.
//! 2. **Sequence-ordered visibility.** A read observes, per key, the
//!    highest-sequence mutation at or below its snapshot watermark; a
//!    tombstone there hides the key. [`model_winner`] is the pure core of
//!    `get_at`'s and `Scan`'s per-key logic, [`model_scan_step`] the pure
//!    core of one scan step, [`model_keep_set`] the exact per-key keep-set
//!    the compaction merge emits, and [`model_may_drop_tombstone`] the rule
//!    that keeps compaction from breaking snapshot isolation.

/// One key-version in the model: a mutation's sequence number and whether
/// it was a deletion.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Version {
    /// Sequence number assigned to the mutation.
    pub seq: u64,
    /// True for a deletion marker.
    pub tombstone: bool,
}

/// Maximum snapshots the keep-set model accepts (mirrors the `Db` bound).
pub const MODEL_MAX_SNAPSHOTS: usize = 8;

/// Models the compaction per-key keep-set: which of a key's versions survive
/// the merge.
///
/// `versions` is the key's full version chain in newest-first order (the
/// implementation guarantees this: every table's version runs are
/// newest-first, and each merge cursor sits at its run start).
///
/// Threshold 0 is the live view (`u64::MAX`); the rest are the live
/// snapshot watermarks. Each threshold is served by the first version at or
/// below it — the newest visible version — and a version is kept exactly
/// when it serves at least one threshold. Older versions are dead to every
/// reader and are not emitted.
///
/// When `bottommost` is true and the newest version is a tombstone older
/// than every live snapshot, the whole key drops (see
/// [`model_may_drop_tombstone`]): the keep-set would be just the tombstone,
/// and deletion is observationally identical to absence there.
///
/// Returns the kept indices in ascending order and how many are valid.
/// Panics are impossible: at most one version per threshold is kept.
#[must_use]
pub fn model_keep_set(
    versions: &[Version],
    snapshots: &[u64],
    bottommost: bool,
    oldest_snapshot: u64,
) -> ([usize; MODEL_MAX_SNAPSHOTS + 1], usize) {
    let mut kept = [0usize; MODEL_MAX_SNAPSHOTS + 1];
    // Bottommost tombstone drop.
    if bottommost
        && let Some(newest) = versions.first()
        && newest.tombstone
        && model_may_drop_tombstone(newest.seq, true, oldest_snapshot)
    {
        return (kept, 0);
    }
    let mut n_kept = 0usize;
    let nsnap = snapshots.len().min(MODEL_MAX_SNAPSHOTS);
    let mut ti = 0usize;
    while ti <= nsnap {
        let th = if ti == 0 { u64::MAX } else { snapshots[ti - 1] };
        // The first version (newest-first) at or below the threshold.
        let mut vi = 0usize;
        while vi < versions.len() {
            if versions[vi].seq <= th {
                if !kept[..n_kept].contains(&vi) {
                    kept[n_kept] = vi;
                    n_kept += 1;
                }
                break;
            }
            vi += 1;
        }
        ti += 1;
    }
    // Canonical ascending order. (The walk already produces it: thresholds
    // descend, so a later threshold's first-visible version is never
    // newer than an earlier one's.)
    let mut i = 1usize;
    while i < n_kept {
        let mut j = i;
        while j > 0 && kept[j - 1] > kept[j] {
            kept.swap(j - 1, j);
            j -= 1;
        }
        i += 1;
    }
    (kept, n_kept)
}

/// Models winner selection for one key: among versions with
/// `seq <= max_seq`, the highest sequence number wins. Returns the winning
/// version's index, or `None` when nothing is visible at `max_seq`.
///
/// A tombstone winner hides the key: the caller maps "winner is a
/// tombstone" to "key absent", exactly like `get_at` and `Scan` do.
#[must_use]
pub fn model_winner(versions: &[Version], max_seq: u64) -> Option<usize> {
    let mut best: Option<usize> = None;
    for (i, v) in versions.iter().enumerate() {
        if v.seq > max_seq {
            continue;
        }
        let better = best.is_none_or(|b| v.seq > versions[b].seq);
        if better {
            best = Some(i);
        }
    }
    best
}

/// Models snapshot visibility: a mutation is visible at a snapshot iff it
/// was sequenced at or before the watermark.
#[must_use]
pub const fn model_visible(mutation_seq: u64, snapshot: u64) -> bool {
    mutation_seq <= snapshot
}

/// Models the bottommost tombstone-drop rule: a tombstone with sequence
/// `tomb_seq` may be dropped by a bottommost compaction only when every
/// live snapshot is strictly newer than it.
///
/// `oldest_snapshot` is
/// `u64::MAX` when no snapshot is live (then every tombstone may drop, as
/// before snapshots existed).
#[must_use]
pub const fn model_may_drop_tombstone(
    tomb_seq: u64,
    bottommost: bool,
    oldest_snapshot: u64,
) -> bool {
    bottommost && tomb_seq < oldest_snapshot
}

/// One source head in the scan model. Key bytes are modeled as a rank: the
/// model only needs their total order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Head {
    /// Key rank (models the total order on key bytes).
    pub key: u64,
    /// The head entry's sequence number.
    pub seq: u64,
    /// Whether the head entry is a tombstone.
    pub tombstone: bool,
}

/// Models one scan step: the winning head's index, or `None` when every
/// source is exhausted.
///
/// The winner sits on the minimum visible key with
/// the highest sequence number. Heads with `seq > max_seq` are filtered
/// first — the model's analogue of cursor parking in `Scan`.
#[must_use]
pub fn model_scan_step(heads: &[Option<Head>], max_seq: u64) -> Option<usize> {
    // Minimum visible key.
    let mut min_key: Option<u64> = None;
    for h in heads.iter().flatten() {
        if h.seq > max_seq {
            continue;
        }
        if min_key.is_none_or(|m| h.key < m) {
            min_key = Some(h.key);
        }
    }
    let min_key = min_key?;
    // Highest seq on the minimum key.
    let mut winner: Option<usize> = None;
    let mut winner_seq = 0u64;
    for (i, h) in heads.iter().enumerate() {
        let Some(h) = h else { continue };
        if h.seq > max_seq || h.key != min_key {
            continue;
        }
        if winner.is_none() || h.seq > winner_seq {
            winner = Some(i);
            winner_seq = h.seq;
        }
    }
    winner
}

/// State of one manifest slot in the model.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SlotState {
    /// Slot holds a complete, CRC-valid manifest with this sequence.
    Valid(u64),
    /// Slot is blank or torn (failed CRC).
    Invalid,
}

/// Models double-buffered manifest recovery: the valid slot with the
/// highest sequence wins.
///
/// Two valid slots never disagree about "which is newer": sequences are
/// monotonic and each commit advances exactly one slot, so the max is
/// always the last committed state.
#[must_use]
pub const fn model_manifest_recover(a: SlotState, b: SlotState) -> Option<u64> {
    match (a, b) {
        (SlotState::Valid(sa), SlotState::Valid(sb)) => {
            // `u64::max` is not const on stable; the comparison is.
            Some(if sa >= sb { sa } else { sb })
        }
        (SlotState::Valid(sa), SlotState::Invalid) => Some(sa),
        (SlotState::Invalid, SlotState::Valid(sb)) => Some(sb),
        (SlotState::Invalid, SlotState::Invalid) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn winner_is_highest_visible_seq() {
        let vs = [
            Version {
                seq: 1,
                tombstone: false,
            },
            Version {
                seq: 5,
                tombstone: false,
            },
            Version {
                seq: 3,
                tombstone: false,
            },
        ];
        assert_eq!(model_winner(&vs, u64::MAX), Some(1));
        // A watermark between versions hides the newer ones.
        assert_eq!(model_winner(&vs, 3), Some(2));
        assert_eq!(model_winner(&vs, 1), Some(0));
        assert_eq!(model_winner(&vs, 0), None);
        assert_eq!(model_winner(&[], u64::MAX), None);
    }

    #[test]
    fn winner_tombstone_hides_key() {
        let vs = [
            Version {
                seq: 1,
                tombstone: false,
            },
            Version {
                seq: 2,
                tombstone: true,
            },
        ];
        // The winner is the tombstone: the caller maps this to "absent".
        assert_eq!(
            model_winner(&vs, u64::MAX).map(|i| vs[i].tombstone),
            Some(true)
        );
        // Before the tombstone, the value is visible.
        assert_eq!(model_winner(&vs, 1).map(|i| vs[i].tombstone), Some(false));
    }

    #[test]
    fn visibility_is_seq_order() {
        assert!(model_visible(7, 7));
        assert!(model_visible(6, 7));
        assert!(!model_visible(8, 7));
    }

    #[test]
    fn tombstone_drop_needs_predating_snapshot() {
        // A snapshot at or below the tombstone's seq blocks the drop.
        assert!(!model_may_drop_tombstone(7, true, 7));
        assert!(!model_may_drop_tombstone(7, true, 6));
        // A strictly newer oldest-snapshot allows it.
        assert!(model_may_drop_tombstone(7, true, 8));
        // No live snapshots: the pre-v0.5 behavior.
        assert!(model_may_drop_tombstone(7, true, u64::MAX));
        // Not bottommost: never drops, snapshots or not.
        assert!(!model_may_drop_tombstone(7, false, u64::MAX));
    }

    #[test]
    fn scan_step_picks_min_key_max_seq() {
        let heads = [
            Some(Head {
                key: 2,
                seq: 9,
                tombstone: false,
            }),
            Some(Head {
                key: 1,
                seq: 3,
                tombstone: false,
            }),
            Some(Head {
                key: 1,
                seq: 5,
                tombstone: true,
            }),
            None,
        ];
        // Minimum key is 1; highest seq there is the tombstone at index 2.
        assert_eq!(model_scan_step(&heads, u64::MAX), Some(2));
        // A watermark hiding seq 5 falls back to seq 3.
        assert_eq!(model_scan_step(&heads, 4), Some(1));
        // Everything exhausted / invisible.
        assert_eq!(model_scan_step(&[None, None], u64::MAX), None);
        assert_eq!(model_scan_step(&heads, 0), None);
    }

    #[test]
    fn manifest_recovery_picks_newest_valid() {
        use SlotState::{Invalid, Valid};
        assert_eq!(model_manifest_recover(Valid(3), Valid(5)), Some(5));
        assert_eq!(model_manifest_recover(Valid(5), Valid(3)), Some(5));
        assert_eq!(model_manifest_recover(Valid(3), Invalid), Some(3));
        assert_eq!(model_manifest_recover(Invalid, Valid(4)), Some(4));
        assert_eq!(model_manifest_recover(Invalid, Invalid), None);
    }

    /// Builds the exact version chain described by `seq_flags` (newest
    /// first). Returns the fixed backing array plus the real length so
    /// callers slice off the zero-filled tail: phantom `seq: 0` entries
    /// must never reach the model.
    fn versions(seq_flags: &[(u64, bool)]) -> ([Version; 5], usize) {
        let mut vs = [Version {
            seq: 0,
            tombstone: false,
        }; 5];
        for (i, (seq, tombstone)) in seq_flags.iter().enumerate() {
            vs[i] = Version {
                seq: *seq,
                tombstone: *tombstone,
            };
        }
        (vs, seq_flags.len())
    }

    /// The keep-set always covers every threshold: each snapshot's (and the
    /// live view's) visible version survives the merge.
    #[test]
    fn keep_set_covers_every_threshold() {
        // Newest-first: seqs 9, 7, 5, 3, 1.
        let (vs, nv) = versions(&[(9, false), (7, false), (5, false), (3, false), (1, false)]);
        let vs = &vs[..nv];
        let snaps = [8, 6, 2];
        let (kept, n) = model_keep_set(vs, &snaps, true, 2);
        let kept = &kept[..n];
        // Live view keeps 9 (idx 0); snap 8 keeps 7 (idx 1); snap 6 keeps
        // 5 (idx 2); snap 2 keeps 1 (idx 4). Version 3 (idx 3) serves no
        // threshold and is dropped.
        assert_eq!(kept, &[0, 1, 2, 4]);
        // Every threshold's model_winner is in the keep-set.
        for th in [u64::MAX, 8, 6, 2] {
            let w = model_winner(vs, th).unwrap();
            assert!(kept.contains(&w), "threshold {th} not covered");
        }
    }

    #[test]
    fn keep_set_without_snapshots_keeps_only_newest() {
        let (vs, nv) = versions(&[(5, false), (3, true), (1, false)]);
        let vs = &vs[..nv];
        let (kept, n) = model_keep_set(vs, &[], true, u64::MAX);
        assert_eq!(&kept[..n], &[0]);
    }

    #[test]
    fn keep_set_shares_versions_across_thresholds() {
        // One version can serve several thresholds: seq 5 is visible at
        // the live view and at snapshots 9 and 6.
        let (vs, nv) = versions(&[(5, false), (2, false)]);
        let vs = &vs[..nv];
        let (kept, n) = model_keep_set(vs, &[9, 6], false, 6);
        assert_eq!(&kept[..n], &[0]);
    }

    #[test]
    fn keep_set_drop_needs_bottommost_predating_tombstone() {
        let (vs, nv) = versions(&[(4, true), (2, false)]);
        let vs = &vs[..nv];
        // Tombstone predates every snapshot: the whole key drops.
        let (_, n) = model_keep_set(vs, &[9, 7], true, 7);
        assert_eq!(n, 0);
        // A snapshot at the tombstone's seq blocks the drop: the tombstone
        // is that snapshot's visible version, so it is kept.
        let (kept, n) = model_keep_set(vs, &[9, 4], true, 4);
        assert_eq!(&kept[..n], &[0]);
        // Not bottommost: the tombstone always survives (deeper levels may
        // hide older versions behind it).
        let (kept, n) = model_keep_set(vs, &[9, 7], false, 7);
        assert_eq!(&kept[..n], &[0]);
        // No live snapshots: oldest is u64::MAX, the tombstone drops.
        let (_, n) = model_keep_set(vs, &[], true, u64::MAX);
        assert_eq!(n, 0);
    }

    #[test]
    fn keep_set_never_exceeds_threshold_count() {
        // More versions than thresholds: at most 1 + n_snapshots survive.
        let (vs, nv) = versions(&[(10, false), (9, false), (8, false), (7, false), (6, false)]);
        let vs = &vs[..nv];
        let (kept, n) = model_keep_set(vs, &[9, 5], true, 5);
        assert!(n <= 3);
        // Snap 5 covers nothing (oldest version is seq 6); snap 9 keeps
        // idx 1 and the live view keeps idx 0.
        assert_eq!(&kept[..n], &[0, 1]);
    }

    #[test]
    fn keep_set_empty_key_stays_empty() {
        let (kept, n) = model_keep_set(&[], &[5], true, 5);
        assert_eq!(n, 0);
        assert_eq!(kept, [0usize; MODEL_MAX_SNAPSHOTS + 1]);
    }
}
