# ADR-0039: A seek inflates a compressed block only as far as it reads

Status: Accepted (unreleased, 2026-11-27)

## Context

A data block in levels 2 and down is LZ77-compressed (ADR-0008,
ADR-0030). Its restart offsets, which a seek binary-searches, sit at
the end of the logical block, so a seek has to inflate the whole block
before it can search it. On the host store (16 KiB blocks) that inflate
was 27% of a cached seek at 1 M keys after step 25 of
`docs/THROUGHPUT.md`, and the check of the whole compressed stream
against the cache's RAM sums (ADR-0016, ADR-0035) came on top of it. A
seek of one page reads two entries.

A point read already inflates only to its key (ADR-0027). That works
inside one lend, over a stream checked whole first. A scan differs in
two ways. Its cursor keeps the block and reads on later, in other
lends. And ADR-0035 lets it keep in its buffer only the grains it
checked, so that a bit flipped in the cache's RAM is a miss and never a
wrong answer.

## Decision

**A forward seek checks and inflates a compressed block's stream only
as far as it reads.** This is `Take::Prefix`, `lend_prefix` and
`Gate::Prefix`. It applies when the cache checks ranges.

- The decoder can be handed a prefix of the stream
  (`Inflate::run_prefix`). It stops before a token the prefix cuts off.
  Every byte it has made final is then what the whole stream makes
  there. It reads no byte past the prefix.
- The lend checks the stream a few grains at a time, from the start,
  just before it hands those bytes to the decoder (`Stream::grow`). No
  stream byte reaches the decoder unchecked.
- The seek's parse runs over `Gate::Prefix`, which lets it act only on
  output already final. A parse that reads past that is a miss. The
  decode goes on past the furthest byte it asked for, by 1 KiB, and the
  parse runs again.

**The seek walks from the block's first entry.** The restarts are not
inflated yet. The walk (`seek_linear`, `skip_below`) steps by entry
headers alone and compares a key every 16th entry. Keys are sorted, so
the head lies at or past the last compared key below the start. From
there a full walk parses and checks each entry.

- The stepping part reads only bytes already final, and asks for none,
  so it never makes the decode go further than the head needs.
- The entries end is not known yet. The cursor parks against a
  provisional end, `BLOCK - CRC - 4`, which no block's entries pass.
  Once the block's count word is in, a park resolves the real end
  first.
- What the walk cannot decide is left to the whole block, as for a
  point read: the end of the entries, an entry that does not parse,
  and a head that is the block's last entry. The rest of the stream is
  checked and inflated, and today's binary-search seek runs over all
  of it. Its answer stands, and so do its errors.

**An error is never believed from a prefix.** A parse error with no
miss goes to the whole block as above. A stream that does not decode
is `CorruptBlock` only once the whole image passes its checks, as
before.

**The buffer keeps what the seek made, and the decode with it.**

- The lend ends by finishing the grain it was in. The buffer keeps the
  whole grains below the decode's output position, so `valid` still
  means grains made from checked bytes in a lend that held.
- It also keeps the decode itself (`Resume`: stream position, output
  position, stream length and a lend count, in 8 bytes). This is
  cleared everywhere `valid` is.
- A block over 64 KiB, whose positions do not fit, is inflated whole,
  as before.

**A step past what the seek made goes on with the same decode**
(`Blocks::extend`). One lend checks that the image's flag names the
same stream, then decodes on from where the decode stood. It checks
every stream byte it hands the decoder. The output it copies from is
the buffer's own, made in an earlier lend that held. Table ids are not
reused, so the stream is the same bytes under the same tag (the
composition rule of ADR-0035). The first such step decodes 2 KiB past
what it reads. The next takes the rest of the block: a scan that has
gone on twice is reading through it. A lend that misses has the block
read from the device whole, as before.

**Reverse scans are unchanged.** They start from the restarts at the
block's end (`Take::Ranged`). A load that finds a partial inflate and
is not a forward scan's would complete it whole from the stream ahead
of the decode (`inflate_rest`). No load does today; it is a backstop. Completing a raw block's grains refuses an
image whose flag says compressed. The format is unchanged.

## Alternatives

- **Check the whole stream, inflate only a prefix.** This is simpler:
  every decoded byte comes from a stream checked whole in the same
  lend. But a step past the prefix checks the stream again, and the
  design panel measured it 17.6% slower than the whole inflate at ten
  entries a page.
- **Entry points in the block**: pairs of stream and output offsets in
  the zero fill of a compressed block, so a decode can start mid-way.
  It measured −21% on short seeks, and −26% on point reads, which this
  change does not touch. But it changes SPEC and the writer, adds a
  table the CRC vouches for and every whole decode must agree with,
  and was +12% at ten entries a page as built. It stays a lever
  (`docs/THROUGHPUT.md`).
- **The first step past the seek takes the rest of the block.**
  Measured +6.7% at ten entries a page.
- **Each step decodes twice as far as the last.** Measured the same at
  ten entries a page and 0.4% worse at a hundred: every step is a lend.

## Consequences

- **Speed** (`docs/THROUGHPUT.md`, step 26). A cached seek on the host
  store at 1 M keys, hot range:
  - one entry a page: −9.4% in instructions;
  - four entries: −6.7%;
  - ten entries: −1.8%;
  - a hundred: +3.1%. Each step past what the seek inflated costs a
    lend, a re-check of the flag's chunk and a re-run parse, and a page
    that reads the block through pays them on top of the same inflate.

  On the full key range most compressed blocks arrive by a device read,
  which still inflates whole, and seeks are level. Point reads, reverse
  scans and raw blocks do not take the new path. Point reads and
  reverse scans are level. Seeks over raw blocks (300 K keys) are 0.6%
  slower, and forward scans 1.0%, from the code around the path.
- **Less is checked on the way to an early answer.** A seek that stops
  early validates neither the restart trailer nor the part of the
  stream it did not reach, as a point read does not (ADR-0027). That
  part has passed its block CRC or RAM sums. Damage a writer bug put
  there under a valid CRC is reported once the scan reaches it, or not
  at all, where the whole-block seek reported it at once. Over the same
  table a seek through a cache that checks ranges can then succeed
  where one without a cache fails. A restart array that does not begin
  at 0 is one such case (found in review).
- **RAM.** `ScanBlock` grows by 8 bytes (`Resume`). The scan futures
  grow by 24 bytes (`Scan::seek` 1,232, `Scan::next` 1,352). The
  ESP32-S3 peak is 114,176 bytes of 114,688.
- **Trust.** For the first time a stream is used before it is checked
  whole. The argument is the one ADR-0035 makes for grains: a byte
  acts only after a check in the same lend held, or after a lend that
  held produced it. The kept decode position lives in the scan's RAM,
  as `valid` and the cursors do.
- **Tests.**
  - `compress::tests`:
    - a prefix decode over growing prefixes matches `decompress`;
    - it fails only where the whole decode does, with the same error,
      and stops only before a token its prefix cuts off;
    - a decode resumed over another copy of the stream from its
      position makes the same block.
  - `scan::tests::prefix`, over random blocks of 1,000, 4,096 and
    16,384 bytes:
    - a partial seek, its value and 17 parks match the whole block,
      through `complete` and `extend`;
    - every grain a buffer keeps is the block's;
    - a flip in the stream on the 1st, 2nd or 9th lend is a miss.
  - `tests/data_flip_scan.rs`, over compressed tables, in both
    directions, with and without a pool, short and long: flips stuck,
    on the first lend only, on every lend but the first, with checks
    deferred, and in kept states with a twin store. Also lookup counts
    for short and long seeks.
  - `tests/scan_retry.rs` over compressed tables: every read of a scan
    fails once in turn, and the retry yields the same.
  - Mutants: skipping the stream checks, checking nothing on a resumed
    lend, believing a prefix parse error, rounding the kept grains up,
    letting the prefix gate pass bytes past its end, not keeping the
    decode without the raw-completion guard, and a wrong resume point
    all fail these tests. Dropping the resumed lend's flag check
    survives: it backs up a tag that is never reused.
- **Review.** An adversarial review found two defects; each has a
  regression test that fails without its fix.
  - A block over 64 KiB was marked whole with its end not inflated.
  - A cursor parsed against the provisional end even once its block was
    in, so a trailer that reads as an entry could be yielded.

## References

- [ADR-0027](0027-point-reads-inflate-to-the-key.md) (the point read's
  partial inflate),
  [ADR-0035](0035-scans-copy-only-what-they-check.md) (grains a buffer
  checked, the composition rule),
  [ADR-0037](0037-scans-over-a-kept-state.md),
  [ADR-0008](0008-hand-rolled-lz77-block-compression.md)
- `src/compress.rs` (`Inflate::run_prefix`, `Inflate::resumed`),
  `src/scan.rs` (`Take`, `Gate::Prefix`, `Resume`, `lend_prefix`,
  `Stream`, `seek_linear`, `skip_below`, `provisional_end`,
  `Blocks::extend`, `Blocks::inflate_rest`, `Blocks::copy_rest`)
- [`docs/THROUGHPUT.md`](../THROUGHPUT.md), step 26
