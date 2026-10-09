//! Replica-side LWW versions: deterministic cross-primary ordering.
//!
//! A horton database can hold sealed tables from several primaries (see
//! `ingest_table`): each primary numbers its own mutations, so sequence
//! numbers are meaningless across nodes. Every table therefore carries an
//! origin — the [`node_id`](crate::manifest::TableRef#structfield.node_id)
//! of the primary that sealed it and the
//! [`seal_wall`](crate::manifest::TableRef#structfield.seal_wall)
//! wall-clock (seconds) the host observed at the seal — and a key's
//! version is `(node, wall, seq)`.
//!
//! [`version_gt`] is the merge rule, used identically by point reads,
//! scans, and the compaction tombstone-drop gate so every replica with
//! the same table set converges on the same winners:
//!
//! - same node: the higher sequence wins — bit-identical to the
//!   single-node rule, so an unstamped database (`node_id` 0 everywhere)
//!   behaves exactly as before;
//! - different nodes: the higher `(seal_wall, node_id)` wins —
//!   deterministic last-writer-wins on seal time, convergent but not
//!   linearizable (wall clocks skew; the seal is a per-table upper bound,
//!   so this is table-granularity LWW, documented in
//!   `docs/tiered-sweeper.md`).
//!
//! Sequence 0 is reserved (the counter issues 1 and up) and never wins,
//! exactly as on the single-node path. The memtable — the node's own
//! unsealed mutations — versions as `(own_node, u64::MAX, seq)`: newer
//! than any sealed table, so read-your-writes holds and the memtable
//! keeps its "newest mutations" invariant without consulting the read's
//! TTL `now`.

/// One version of a key: the writer's node, a wall-clock upper bound on
/// the write (the table's seal time; `u64::MAX` for the memtable), and
/// the writer's sequence number.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Version {
    /// Origin node of the table holding this version (`0` = unstamped).
    pub node: u32,
    /// Seal wall-clock (seconds) of the table holding this version.
    pub wall: u64,
    /// The writer's sequence number (`0` = reserved, never wins).
    pub seq: u64,
}

impl Version {
    /// No version yet: sorts below every real version.
    pub const ZERO: Self = Self {
        node: 0,
        wall: 0,
        seq: 0,
    };

    /// The memtable's version for a mutation at `seq`: the node's own
    /// newest, newer than any sealed table from any node.
    #[must_use]
    pub const fn memtable(node: u32, seq: u64) -> Self {
        Self {
            node,
            wall: u64::MAX,
            seq,
        }
    }

    /// A sealed table's version for an entry at `seq`.
    #[must_use]
    pub const fn table(node: u32, wall: u64, seq: u64) -> Self {
        Self { node, wall, seq }
    }
}

/// Strictly-greater under the replica LWW merge rule (see the module
/// docs). A total order: ties are impossible — same-node sequences are
/// unique per writer, and cross-node tuples differ in `node`.
#[must_use]
pub const fn version_gt(a: Version, b: Version) -> bool {
    // Sequence 0 is reserved and never wins, whatever the origin.
    if a.seq == 0 {
        return false;
    }
    if b.seq == 0 {
        return true;
    }
    if a.node == b.node {
        return a.seq > b.seq;
    }
    if a.wall != b.wall {
        return a.wall > b.wall;
    }
    a.node > b.node
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_node_is_sequence_order() {
        let older = Version::table(7, 100, 41);
        let newer = Version::table(7, 100, 42);
        assert!(version_gt(newer, older));
        assert!(!version_gt(older, newer));
        assert!(!version_gt(older, older));
    }

    #[test]
    fn same_node_ignores_wall() {
        // A re-sealed (compaction output) table keeps node 7: sequence
        // still decides, even if the wall moved.
        let a = Version::table(7, 50, 9);
        let b = Version::table(7, 900, 8);
        assert!(version_gt(a, b));
        assert!(!version_gt(b, a));
    }

    #[test]
    fn cross_node_is_wall_then_node() {
        let early = Version::table(9, 100, 9999);
        let late = Version::table(3, 200, 12);
        assert!(version_gt(late, early));
        assert!(!version_gt(early, late));
        // Wall tie: higher node wins, deterministically.
        let lo = Version::table(3, 200, 12);
        let hi = Version::table(9, 200, 12);
        assert!(version_gt(hi, lo));
        assert!(!version_gt(lo, hi));
    }

    #[test]
    fn unstamped_sorts_below_stamped() {
        let legacy = Version::table(0, 0, 5000);
        let stamped = Version::table(7, 1, 2);
        assert!(version_gt(stamped, legacy));
        assert!(!version_gt(legacy, stamped));
        // Legacy vs legacy is pure sequence order (today's behavior).
        let a = Version::table(0, 0, 7);
        let b = Version::table(0, 0, 8);
        assert!(version_gt(b, a));
    }

    #[test]
    fn seq_zero_never_wins() {
        let reserved = Version::table(7, 500, 0);
        let zero = Version::ZERO;
        let real = Version::table(0, 0, 1);
        assert!(!version_gt(reserved, zero));
        assert!(!version_gt(reserved, real));
        assert!(!version_gt(zero, zero));
        assert!(version_gt(real, zero));
    }

    #[test]
    fn memtable_beats_every_sealed_table() {
        let mem = Version::memtable(7, 3);
        let foreign = Version::table(9, u64::MAX - 1, u64::MAX);
        assert!(version_gt(mem, foreign));
        // Another write on the same node still orders by sequence.
        let mem2 = Version::memtable(7, 4);
        assert!(version_gt(mem2, mem));
    }
}
