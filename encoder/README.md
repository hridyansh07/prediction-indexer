# Shared Zstandard codec

One codec boundary, used by the Python archiver and the Rust finalizer. It has
exactly one job: move exact NDJSON bytes into and out of exactly one Zstandard
frame while measuring both sides of the operation.

```text
encoder/compression.py   Python: encode_stream / decode_stream
encoder/whole_buffer.py  test-only conveniences built on those two
encoder/rust/            the same contract over Read and Write
encoder/fixtures/        the cross-language proof
```

There is **no message-encoding layer**. `ZSTD_MATERIALIZATION_PIPELINE_V1.md` §1
settles that V1 stores NDJSON inside Zstandard frames and does not use Simple
Binary Encoding, a custom binary event schema, record blocks, or any additional
framing. The evidence model is the reason: canonical evidence is the *original
envelope lines copied byte for byte*, and a codec that re-encodes them would
make the archive a lossy interpretation of the tape rather than a compressed
copy of it.

## The format

| Setting | Required value |
|---|---|
| algorithm | Zstandard |
| compression level | `3` |
| frame checksum | enabled |
| dictionary | none |
| frames per object | exactly one |
| logical payload | exact NDJSON bytes |
| stream buffer | ≤ 1 MiB |

An empty payload is a valid non-empty frame that decodes to zero bytes, whose
logical digest is SHA-256 of the empty string. A non-empty payload must end in
LF, because the LF count *is* the record count.

## Two identities

```text
logical  sha256, byte length and LF count of the decoded NDJSON
stored   sha256 and byte length of the complete frame
```

Both are calculated in the same pass, on the caller's bytes as they arrive.
Logical identity proves what can be reconstructed; stored identity proves which
physical object was committed. A receipt carrying one without the other proves
half of what it claims, so the encoders return them together.

Python and Rust frames are not required to be byte-identical, and nothing may
depend on their being so. What is required is that each decodes the other to
identical logical bytes, which `encoder/fixtures/roundtrip_v1.*` proves in both
languages without either test suite shelling out to the other toolchain.
`scripts/build_codec_fixtures.py` documents the two-step regeneration; a fixture
any test run can silently rewrite would prove nothing.

## Decoding is adversarial

Success means one frame ended exactly at end-of-input with every identity
matched. These are rejected, in both languages:

- a truncated frame, or a bad frame checksum;
- concatenated frames, or any trailing byte after the expected one;
- a dictionary-dependent frame;
- a stored length or digest that disagrees with the object;
- a decoded length, digest, or LF count that disagrees with the expectation;
- output above the caller's hard maximum — which aborts *before* the sink
  receives byte `maximum + 1`.

Python's permissive `read_across_frames=True` is prohibited. The `.zst` suffix
is never evidence that this contract was followed, and neither is a successful
decompression.

Both decoders walk the frame structure themselves to find where the frame ends,
because libzstd's streaming readers buffer ahead: "how many bytes did the source
hand over" does not answer "where did the frame stop", and without that answer a
padded or concatenated object decodes as one healthy stream.

## Streaming only

No production path may call an API whose source or result is one complete
`bytes`/`Vec<u8>` object. `encoder/whole_buffer.py` exists for tests and probes
and lives in its own module so the prohibition is checkable —
`tests/test_no_sbe.py` asserts nothing outside tests and scripts imports it.

Decode is bounded on the output side, which is the side that can be made to
explode: one 128 KiB compressed chunk of run-length blocks can expand to
gigabytes, so feeding a push decoder in small bites is not a bound at all.

## Durability boundary

Raw evidence is not compressed in place. The active splice file remains exact
NDJSON and the seal commits its byte length, line count and SHA-256. Only after
that commit may the archiver derive a `.zst` object, verify it remotely, and
publish an archive receipt. Deleting the raw segment additionally requires a
canonical receipt naming the same source — see
`archive/PHASE_4_RAW_ARCHIVE_REAPER_V1.md`.

## Python

```python
from encoder import decode_stream, encode_stream

with open(segment, "rb") as source, open(derivative, "wb") as sink:
    result = encode_stream(source, sink)

with open(derivative, "rb") as source, open(restored, "wb") as sink:
    decode_stream(
        source,
        sink,
        expected_logical=result.logical,
        expected_stored=result.stored,
        max_decoded_bytes=result.logical.byte_length,
    )
```

## Rust

```rust
use prediction_encoder::{DEFAULT_ZSTD_LEVEL, decode_stream, encode_stream};

let result = encode_stream(File::open(segment)?, File::create(derivative)?, DEFAULT_ZSTD_LEVEL)?;
decode_stream(
    File::open(derivative)?,
    File::create(restored)?,
    &result.logical,
    Some(&result.stored),
    Some(result.logical.byte_length),
)?;
```

The crate lives at `encoder/rust` and stays outside the `ingester` workspace
until the Rust finalizer consumes it in Zstd Step 2.
