//! Bump allocator tests: runs, exhaustion, overflow, repositioning.

use horton::alloc::Bump;
use horton::Error;

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
