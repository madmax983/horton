//! Wire frames: a CRC-sealed, front-coded run of ascending entries that a
//! device downlinks and the ground side reads back as a `SortedEntries`
//! source — so a `Diff` can join a local `Scan` against bytes that came
//! off a radio, with no database on the far end.

mod common;

use core::convert::Infallible;

use common::{Lcg, MemDevice, TestDb, block_on, test_config};
use horton::diff::{Delta, Diff, SortedEntries};
use horton::frame::{FRAME_OVERHEAD, FrameReader, FrameWriter};
use horton::{Compaction, Error, Progress, Scan};

type TestScan<'d> = Scan<'d, MemDevice<4096>, 4096, 256, 1024, 64, 4096, 7, 4, 1024, 8>;
type Reader<'a> = FrameReader<'a, Infallible, 256>;

fn fresh() -> TestDb<MemDevice<4096>> {
    let mut db = TestDb::new(MemDevice::<4096>::new(), test_config());
    block_on(db.open()).unwrap();
    db
}

/// Puts, flushing (and compacting) when the memtable or WAL fills.
fn put(db: &mut TestDb<MemDevice<4096>>, k: &[u8], v: &[u8]) {
    match block_on(db.put(k, v)) {
        Ok(_) => {}
        Err(Error::TableFull | Error::ArenaFull | Error::WalFull) => {
            if block_on(db.flush()) == Err(Error::NeedsCompaction) {
                let mut c = Compaction::<4096, 256, 1024, 1024>::new();
                while block_on(db.compact_step(&mut c)).unwrap() == Progress::More {}
                block_on(db.flush()).unwrap();
            }
            block_on(db.put(k, v)).unwrap();
        }
        Err(e) => panic!("unexpected put error: {e:?}"),
    }
}

fn key(i: usize) -> Vec<u8> {
    format!("sensor/{i:05}").into_bytes()
}

/// Decodes a whole frame into owned pairs.
fn decode(frame: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut r = Reader::new(frame).unwrap();
    let (mut k, mut v) = ([0u8; 256], [0u8; 1024]);
    let mut out = Vec::new();
    while let Some((kl, vl)) = block_on(r.next_entry(&mut k, &mut v)).unwrap() {
        out.push((k[..kl].to_vec(), v[..vl].to_vec()));
    }
    out
}

#[test]
fn round_trip_preserves_every_entry_in_order() {
    let mut buf = [0u8; 2048];
    let mut w = FrameWriter::<256>::new(&mut buf).unwrap();
    let pairs: Vec<_> = (0..40)
        .map(|i| (key(i), vec![u8::try_from(i).unwrap(); i % 7]))
        .collect();
    for (k, v) in &pairs {
        w.push(k, v).unwrap();
    }
    assert_eq!(w.count(), 40);
    let frame = w.finish();
    assert_eq!(decode(frame), pairs);
}

#[test]
fn an_empty_frame_is_valid_and_yields_nothing() {
    let mut buf = [0u8; FRAME_OVERHEAD];
    let w = FrameWriter::<256>::new(&mut buf).unwrap();
    assert!(w.is_empty());
    let frame = w.finish();
    assert_eq!(frame.len(), FRAME_OVERHEAD);
    assert!(decode(frame).is_empty());
}

#[test]
fn front_coding_beats_the_raw_bytes_for_shared_prefixes() {
    let mut buf = [0u8; 4096];
    let mut w = FrameWriter::<256>::new(&mut buf).unwrap();
    let mut raw = 0;
    for i in 0..100 {
        let k = key(i);
        w.push(&k, b"").unwrap();
        raw += k.len();
    }
    // "sensor/000NN": most of each 12-byte key is shared with the last.
    assert!(w.len() * 2 < raw, "{} vs {raw}", w.len());
}

#[test]
fn writer_rejects_bad_input_and_changes_nothing() {
    let mut buf = [0u8; 64];
    let mut w = FrameWriter::<8>::new(&mut buf).unwrap();
    w.push(b"b", b"1").unwrap();
    let before = w.len();
    assert_eq!(w.push(b"", b"x"), Err(Error::EmptyKey));
    assert_eq!(w.push(b"a", b"x"), Err(Error::BadConfig)); // not ascending
    assert_eq!(w.push(b"b", b"x"), Err(Error::BadConfig)); // duplicate
    assert_eq!(
        w.push(b"ccccccccc", b"x"),
        Err(Error::KeyTooLarge { len: 9, max: 8 })
    );
    assert!(matches!(
        w.push(b"c", &[0u8; 100]),
        Err(Error::BufferTooSmall { .. })
    ));
    assert_eq!(w.len(), before);
    assert_eq!(w.count(), 1);
    w.push(b"c", b"2").unwrap();
    assert_eq!(
        decode(w.finish()),
        vec![
            (b"b".to_vec(), b"1".to_vec()),
            (b"c".to_vec(), b"2".to_vec())
        ]
    );
}

#[test]
fn a_buffer_smaller_than_the_overhead_is_refused() {
    let mut buf = [0u8; FRAME_OVERHEAD - 1];
    assert_eq!(
        FrameWriter::<8>::new(&mut buf).err(),
        Some(Error::BufferTooSmall {
            need: FRAME_OVERHEAD
        })
    );
}

#[test]
fn every_single_bit_flip_is_caught() {
    let mut buf = [0u8; 512];
    let mut w = FrameWriter::<256>::new(&mut buf).unwrap();
    for i in 0..10 {
        w.push(&key(i), b"payload").unwrap();
    }
    let frame = w.finish().to_vec();
    for byte in 0..frame.len() {
        for bit in 0..8 {
            let mut bad = frame.clone();
            bad[byte] ^= 1 << bit;
            assert!(
                matches!(Reader::new(&bad), Err(Error::CorruptFrame { .. })),
                "flip at {byte}.{bit} went unnoticed"
            );
        }
    }
    // A truncated frame fails too.
    for cut in 0..frame.len() {
        assert!(Reader::new(&frame[..cut]).is_err(), "cut at {cut}");
    }
}

#[test]
fn reader_reports_a_short_buffer_and_retries_cleanly() {
    let mut buf = [0u8; 256];
    let mut w = FrameWriter::<256>::new(&mut buf).unwrap();
    w.push(b"a", b"0123456789").unwrap();
    w.push(b"b", b"x").unwrap();
    let frame = w.finish();
    let mut r = Reader::new(frame).unwrap();
    let (mut k, mut small, mut big) = ([0u8; 256], [0u8; 4], [0u8; 64]);
    assert_eq!(
        block_on(r.next_entry(&mut k, &mut small)),
        Err(Error::BufferTooSmall { need: 10 })
    );
    assert_eq!(block_on(r.next_entry(&mut k, &mut big)), Ok(Some((1, 10))));
    assert_eq!(&big[..10], b"0123456789");
    assert_eq!(block_on(r.next_entry(&mut k, &mut big)), Ok(Some((1, 1))));
    assert_eq!(block_on(r.next_entry(&mut k, &mut big)), Ok(None));
    assert_eq!(block_on(r.next_entry(&mut k, &mut big)), Ok(None));
}

#[test]
fn a_frame_with_a_valid_crc_but_unsorted_body_is_refused() {
    // Hand-forge: two entries, "b" then "a", with a correct CRC. The
    // reader must not trust the writer's ordering promise blindly.
    let mut body = Vec::from(&b"HF\x01"[..]);
    body.extend_from_slice(&[0, 1, 0, b'b']); // shared 0, suffix 1, val 0
    body.extend_from_slice(&[0, 1, 0, b'a']);
    let crc = horton::crc32(&body);
    body.extend_from_slice(&crc.to_le_bytes());
    let mut r = Reader::new(&body).unwrap();
    let (mut k, mut v) = ([0u8; 256], [0u8; 16]);
    assert_eq!(block_on(r.next_entry(&mut k, &mut v)), Ok(Some((1, 0))));
    assert!(matches!(
        block_on(r.next_entry(&mut k, &mut v)),
        Err(Error::CorruptFrame { .. })
    ));
}

#[test]
fn fill_pages_a_scan_into_frames_that_reassemble_exactly() {
    let mut db = fresh();
    let mut rng = Lcg::new(7);
    let mut model = Vec::new();
    for i in 0..200 {
        let v = vec![u8::try_from(rng.next_bounded(256)).unwrap(); rng.next_bounded(40)];
        put(&mut db, &key(i), &v);
        model.push((key(i), v));
    }
    let mut scan = TestScan::new(&db);
    block_on(scan.seek(b"", None, u64::MAX)).unwrap();
    let (mut k, mut v) = ([0u8; 256], [0u8; 1024]);
    let mut pending = None;
    let mut got = Vec::new();
    let mut frames = 0;
    loop {
        let mut buf = [0u8; 300]; // a small radio MTU
        let mut w = FrameWriter::<256>::new(&mut buf).unwrap();
        if let Some((kl, vl)) = pending.take() {
            w.push(&k[..kl], &v[..vl]).unwrap();
        }
        pending = block_on(w.fill(&mut scan, &mut k, &mut v)).unwrap();
        assert!(w.len() <= 300);
        frames += 1;
        got.extend(decode(w.finish()));
        if pending.is_none() {
            break;
        }
    }
    assert!(frames > 5, "only {frames} frames");
    assert_eq!(got, model);
}

#[test]
fn fill_refuses_an_entry_that_no_frame_could_carry() {
    let mut db = fresh();
    block_on(db.put(b"huge", &[7u8; 500])).unwrap();
    let mut scan = TestScan::new(&db);
    block_on(scan.seek(b"", None, u64::MAX)).unwrap();
    let (mut k, mut v) = ([0u8; 256], [0u8; 1024]);
    let mut buf = [0u8; 100];
    let mut w = FrameWriter::<256>::new(&mut buf).unwrap();
    assert!(matches!(
        block_on(w.fill(&mut scan, &mut k, &mut v)),
        Err(Error::BufferTooSmall { .. })
    ));
}

#[test]
fn a_received_frame_diffs_directly_against_the_local_scan() {
    // The ground side holds last pass's frame; the device has moved on.
    let mut db = fresh();
    for i in 0..30 {
        block_on(db.put(&key(i), b"v1")).unwrap();
    }
    let mut buf = [0u8; 4096];
    let mut w = FrameWriter::<256>::new(&mut buf).unwrap();
    {
        let mut scan = TestScan::new(&db);
        block_on(scan.seek(b"", None, u64::MAX)).unwrap();
        let (mut k, mut v) = ([0u8; 256], [0u8; 1024]);
        assert_eq!(block_on(w.fill(&mut scan, &mut k, &mut v)).unwrap(), None);
    }
    let frame = w.finish().to_vec();

    block_on(db.put(&key(3), b"v2")).unwrap();
    block_on(db.delete(&key(17))).unwrap();
    block_on(db.put(&key(99), b"new")).unwrap();

    let mut scan = TestScan::new(&db);
    block_on(scan.seek(b"", None, u64::MAX)).unwrap();
    let mut remote = Reader::new(&frame).unwrap();
    let (mut lk, mut lv, mut rk, mut rv) = ([0u8; 256], [0u8; 1024], [0u8; 256], [0u8; 1024]);
    let mut d = Diff::new(&mut scan, &mut remote, &mut lk, &mut lv, &mut rk, &mut rv);
    let mut seen = Vec::new();
    while let Some(delta) = block_on(d.next()).unwrap() {
        seen.push(match delta {
            Delta::OnlyLeft { key, .. } => (key.to_vec(), 'L'),
            Delta::OnlyRight { key, .. } => (key.to_vec(), 'R'),
            Delta::Differs { key, .. } => (key.to_vec(), 'D'),
        });
    }
    assert_eq!(seen, vec![(key(3), 'D'), (key(17), 'R'), (key(99), 'L')]);
}

#[test]
fn random_frames_round_trip() {
    let mut rng = Lcg::new(42);
    for _ in 0..200 {
        let n = rng.next_bounded(30);
        let mut keys: Vec<Vec<u8>> = (0..n)
            .map(|_| {
                (0..=rng.next_bounded(20))
                    .map(|_| b"aab"[rng.next_bounded(3)])
                    .collect()
            })
            .collect();
        keys.sort();
        keys.dedup();
        let pairs: Vec<_> = keys
            .into_iter()
            .map(|k| {
                let v: Vec<u8> = (0..rng.next_bounded(300))
                    .map(|_| u8::try_from(rng.next_bounded(256)).unwrap())
                    .collect();
                (k, v)
            })
            .collect();
        let mut buf = vec![0u8; 16_384];
        let mut w = FrameWriter::<256>::new(&mut buf).unwrap();
        for (k, v) in &pairs {
            w.push(k, v).unwrap();
        }
        assert_eq!(decode(w.finish()), pairs);
    }
}
