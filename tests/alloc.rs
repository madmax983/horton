//! Bump allocator tests: runs, exhaustion, overflow, repositioning.

use horton::Error;
use horton::alloc::Bump;

type DevError = core::convert::Infallible;

#[test]
fn run_allocation() {
    let mut b = Bump::new(10, 20);
    assert_eq!(b.alloc_run::<DevError>(5).unwrap(), 10);
    assert_eq!(b.next(), 15);
    assert_eq!(b.alloc::<DevError>().unwrap(), 15);
    assert_eq!(b.next(), 16);
    assert_eq!(b.alloc_run::<DevError>(4).unwrap(), 16);
    assert_eq!(b.next(), 20);
    // Exhausted: single and run allocations both fail.
    assert!(matches!(b.alloc::<DevError>(), Err(Error::NoSpace)));
    assert!(matches!(b.alloc_run::<DevError>(1), Err(Error::NoSpace)));
}

#[test]
fn failed_alloc_does_not_move_pointer() {
    let mut b = Bump::new(10, 20);
    assert!(matches!(b.alloc_run::<DevError>(11), Err(Error::NoSpace)));
    assert_eq!(b.next(), 10);
    assert_eq!(b.alloc_run::<DevError>(10).unwrap(), 10);
}

#[test]
fn checked_add_overflow_is_nospace() {
    let mut b = Bump::new(u64::MAX - 1, u64::MAX);
    assert!(matches!(b.alloc_run::<DevError>(2), Err(Error::NoSpace)));
    assert_eq!(b.alloc_run::<DevError>(1).unwrap(), u64::MAX - 1);
    assert!(matches!(b.alloc::<DevError>(), Err(Error::NoSpace)));
}

#[test]
fn set_next_repositions() {
    // The open-time sweep's rebuild: forward after the highest live block.
    let mut b = Bump::new(136, 4224);
    b.alloc_run::<DevError>(100).unwrap();
    assert_eq!(b.next(), 236);
    b.set_next(136);
    assert_eq!(b.next(), 136);
    assert_eq!(b.alloc_run::<DevError>(4088).unwrap(), 136);
    assert_eq!(b.next(), 4224);
}

use horton::alloc::FreeList;

#[test]
fn free_list_insert_keeps_sorted_order() {
    let mut f = FreeList::<16>::new();
    assert!(f.is_empty());
    for id in [5u64, 3, 7, 1] {
        f.insert::<DevError>(id).unwrap();
    }
    assert_eq!(f.len(), 4);
    // Runs: [1], [3], [5], [7] — no run of 2.
    assert_eq!(f.find_run(2), None);
    assert_eq!(f.find_run(1), Some(1));
    // Insert 4: runs become [1], [3, 4, 5], [7]; first-fit finds 3.
    f.insert::<DevError>(4).unwrap();
    assert_eq!(f.find_run(2), Some(3));
    assert_eq!(f.find_run(3), Some(3));
    assert_eq!(f.find_run(4), None);
}

#[test]
fn free_list_claim_run_removes_exactly() {
    let mut f = FreeList::<16>::new();
    for id in [1u64, 2, 3, 5, 6, 9] {
        f.insert::<DevError>(id).unwrap();
    }
    assert!(f.claim_run(1, 2));
    assert_eq!(f.len(), 4);
    // [3], [5, 6], [9]: first fit for 2 is now 5.
    assert_eq!(f.find_run(2), Some(5));
    assert_eq!(f.find_run(1), Some(3));
}

#[test]
fn free_list_claim_missing_or_partial_run_fails_cleanly() {
    let mut f = FreeList::<16>::new();
    for id in [1u64, 2, 4] {
        f.insert::<DevError>(id).unwrap();
    }
    // Base absent.
    assert!(!f.claim_run(7, 1));
    // Run not fully present: [1, 2] then a gap at 3.
    assert!(!f.claim_run(1, 3));
    // Nothing mutated by the failed claims.
    assert_eq!(f.len(), 3);
    assert_eq!(f.find_run(2), Some(1));
    // Claiming zero blocks at a present base is a no-op success.
    assert!(f.claim_run(4, 0));
    assert_eq!(f.len(), 3);
}

#[test]
fn free_list_edge_cases() {
    let f = FreeList::<16>::new();
    assert_eq!(f.find_run(0), None);
    assert_eq!(f.find_run(1), None);
    assert_eq!(f.find_run(usize::MAX), None);

    let mut full = FreeList::<2>::new();
    full.insert::<DevError>(10).unwrap();
    full.insert::<DevError>(20).unwrap();
    assert!(matches!(full.insert::<DevError>(30), Err(Error::NoSpace)));
    // Non-contiguous ids never form a run.
    assert_eq!(full.find_run(2), None);
    assert_eq!(full.find_run(1), Some(10));
}

#[test]
fn free_list_first_fit_prefers_lowest_base() {
    let mut f = FreeList::<16>::new();
    // Fragmented: [8, 9] sits below [2, 3, 4] in insertion order, but the
    // scan is over sorted ids, so the lowest run wins.
    for id in [8u64, 9, 2, 3, 4] {
        f.insert::<DevError>(id).unwrap();
    }
    assert_eq!(f.find_run(3), Some(2));
    assert!(f.claim_run(2, 3));
    assert_eq!(f.find_run(2), Some(8));
}
