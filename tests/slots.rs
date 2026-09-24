//! Table-slot allocator tests: layout, slot lookup, next-fit rotation,
//! reservations, and the used/reserved/free accounting.

use horton::slots::{MAX_SLOTS, SlotMap};

#[test]
fn layout_divides_the_region_into_equal_slots() {
    // 100 blocks, 8 slots: 12 blocks each, the trailing 4 unused.
    let m = SlotMap::layout(10, 110, 8, 5).expect("layout");
    assert_eq!(m.slots(), 8);
    assert_eq!(m.slot_blocks(), 12);
    assert_eq!(m.slot_base(0), 10);
    assert_eq!(m.slot_base(7), 10 + 7 * 12);
    assert_eq!(m.free_slots(), 8);
    assert_eq!(m.used_slots(), 0);
}

#[test]
fn layout_rejects_bad_shapes() {
    assert_eq!(SlotMap::layout(0, 100, 0, 1), None, "zero slots");
    assert_eq!(SlotMap::layout(0, 1000, MAX_SLOTS + 1, 1), None, "too many");
    assert_eq!(SlotMap::layout(50, 50, 4, 1), None, "empty region");
    assert_eq!(SlotMap::layout(60, 50, 4, 1), None, "inverted region");
    assert_eq!(SlotMap::layout(0, 3, 4, 1), None, "slots of 0 blocks");
    // 40 blocks / 8 slots = 5 per slot: a 6-block minimum refuses it.
    assert_eq!(SlotMap::layout(0, 40, 8, 6), None, "below the minimum");
    assert!(
        SlotMap::layout(0, 40, 8, 5).is_some(),
        "exactly the minimum"
    );
    assert!(
        SlotMap::layout(0, 64 * 4, MAX_SLOTS, 4).is_some(),
        "64 slots"
    );
}

#[test]
fn slot_of_requires_a_run_inside_one_slot() {
    let m = SlotMap::layout(100, 200, 4, 1).expect("layout"); // 25 each
    assert_eq!(m.slot_of(100, 25), Some(0), "a full slot");
    assert_eq!(m.slot_of(110, 5), Some(0), "inside slot 0");
    assert_eq!(m.slot_of(125, 1), Some(1), "first block of slot 1");
    assert_eq!(m.slot_of(199, 1), Some(3), "last block of the region");
    assert_eq!(m.slot_of(120, 6), None, "straddles slots 0 and 1");
    assert_eq!(m.slot_of(99, 1), None, "before the region");
    assert_eq!(m.slot_of(200, 1), None, "past the last slot");
    assert_eq!(m.slot_of(110, 0), None, "empty run");
    assert_eq!(m.slot_of(u64::MAX, 2), None, "overflowing run");
    // Trailing blocks past the last whole slot belong to no slot.
    let m = SlotMap::layout(0, 10, 3, 1).expect("layout"); // 3 each, block 9 unused
    assert_eq!(m.slot_of(9, 1), None);
    assert_eq!(m.slot_of(6, 4), None, "runs into the unused tail");
}

#[test]
fn claim_rotates_next_fit_across_the_region() {
    let mut m = SlotMap::layout(0, 40, 4, 1).expect("layout");
    let mut order = Vec::new();
    for _ in 0..4 {
        let s = m.find_free().expect("free slot");
        m.claim(s);
        order.push(s);
    }
    assert_eq!(
        order,
        [0, 1, 2, 3],
        "successive tables take successive slots"
    );
    assert_eq!(m.find_free(), None, "full");
    // Freeing slot 1 then slot 3: next-fit resumes after the last claim
    // (slot 3), wraps, and finds slot 1 before slot 3.
    m.free(1);
    m.free(3);
    assert_eq!(m.find_free(), Some(1));
    m.claim(1);
    assert_eq!(m.find_free(), Some(3), "rotation continues past slot 1");
}

#[test]
fn find_free_does_not_take_the_slot() {
    let mut m = SlotMap::layout(0, 40, 4, 1).expect("layout");
    assert_eq!(m.find_free(), Some(0));
    assert_eq!(m.find_free(), Some(0), "pure query");
    assert_eq!(m.free_slots(), 4);
    m.claim(0);
    assert!(m.is_used(0));
    assert_eq!(m.free_slots(), 3);
}

#[test]
fn reservations_hide_slots_until_commit_or_release() {
    let mut m = SlotMap::layout(0, 40, 4, 1).expect("layout");
    let r = m.reserve().expect("reserve");
    assert!(m.is_reserved(r) && !m.is_used(r));
    assert_eq!(m.reserved_slots(), 1);
    assert_eq!(m.free_slots(), 3);
    // A flush between compaction steps never gets the reserved slot.
    for _ in 0..3 {
        let s = m.find_free().expect("free");
        assert_ne!(s, r, "reserved slot handed out");
        m.claim(s);
    }
    assert_eq!(m.find_free(), None);
    m.commit(r);
    assert!(m.is_used(r) && !m.is_reserved(r));
    assert_eq!(m.used_slots(), 4);

    let mut m = SlotMap::layout(0, 40, 4, 1).expect("layout");
    let a = m.reserve().expect("a");
    let b = m.reserve().expect("b");
    assert_ne!(a, b);
    m.release(a);
    assert!(!m.is_reserved(a));
    assert_eq!(m.free_slots(), 3);
    m.release_all();
    assert_eq!(m.reserved_slots(), 0);
    assert_eq!(m.free_slots(), 4);
}

#[test]
fn mark_used_rebuilds_and_rejects_double_use() {
    let mut m = SlotMap::layout(0, 40, 4, 1).expect("layout");
    assert!(m.mark_used(2));
    assert!(!m.mark_used(2), "two tables in one slot");
    assert!(!m.mark_used(4), "out of range");
    assert_eq!(m.used_slots(), 1);
    m.set_hint_after(2);
    assert_eq!(
        m.find_free(),
        Some(3),
        "next-fit resumes past the newest table"
    );
    m.set_hint_after(3);
    assert_eq!(m.find_free(), Some(0), "and wraps");
}

#[test]
fn out_of_range_operations_are_no_ops() {
    let mut m = SlotMap::layout(0, 40, 4, 1).expect("layout");
    let before = m;
    m.claim(9);
    m.commit(9);
    m.release(9);
    m.free(9);
    assert_eq!(m, before);
    assert!(!m.is_used(9) && !m.is_reserved(9));
    let empty = SlotMap::new();
    assert_eq!(empty.find_free(), None, "no layout, no slots");
    assert_eq!(empty.slot_of(0, 1), None);
}
