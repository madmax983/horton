//! `Error` prints a reason and works as `std::error::Error`.

use horton::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Dev;

impl core::fmt::Display for Dev {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("dev failed")
    }
}

#[test]
fn display_names_the_reason_and_the_remedy() {
    let e: Error<Dev> = Error::KeyTooLarge { len: 9, max: 8 };
    assert_eq!(e.to_string(), "key is 9 bytes; the limit is 8");
    assert!(Error::<Dev>::WalFull.to_string().contains("flush"));
    assert!(
        Error::<Dev>::NeedsCompaction
            .to_string()
            .contains("compact")
    );
    assert_eq!(Error::Device(Dev).to_string(), "device error: dev failed");
}

#[test]
fn every_variant_has_a_message() {
    let all: [Error<Dev>; 30] = [
        Error::KeyTooLarge { len: 1, max: 0 },
        Error::ValueTooLarge { len: 1, max: 0 },
        Error::EmptyKey,
        Error::TableFull,
        Error::ArenaFull,
        Error::BufferTooSmall { need: 1 },
        Error::BadBufferLen,
        Error::CorruptBlock { id: 1 },
        Error::CorruptWal { offset: 1 },
        Error::CorruptManifest,
        Error::WalFull,
        Error::NeedsCompaction,
        Error::RegionFull,
        Error::SnapshotLimit,
        Error::ManifestFull,
        Error::TableTooLarge,
        Error::CounterExhausted,
        Error::BadLevel { level: 9 },
        Error::BatchFull,
        Error::BatchTooLarge { bytes: 2, max: 1 },
        Error::WouldResurrect { table: 1 },
        Error::IngestConflict { id: 1 },
        Error::StampConflict { id: 1 },
        Error::BadConfig,
        Error::Busy,
        Error::NotOpen,
        Error::RingFull,
        Error::BadPayload,
        Error::Device(Dev),
        Error::EmptyKey,
    ];
    for e in all {
        assert!(!e.to_string().is_empty(), "{e:?}");
    }
}

#[test]
fn is_a_std_error() {
    let e: Box<dyn std::error::Error> = Box::new(Error::Device(Dev));
    assert_eq!(e.to_string(), "device error: dev failed");
}
