# Shared Zstandard codec

One codec boundary, used by the Python archiver and the Rust finalizer. Its one
job is to move exact NDJSON bytes into and out of exactly one Zstandard frame
while measuring both sides of the operation.

```text
encoder/compression.py   Python: encode_stream / decode_stream, identities
encoder/whole_buffer.py  test-only conveniences built on those two
encoder/rust/            same contract over Read and Write (crate prediction-encoder)
encoder/rust/src/bin/decode_v1.rs   CLI wrapper around the Rust decoder
encoder/node/            TypeScript RustV1Decoder that spawns decode_v1 (no codec of its own)
encoder/fixtures/        cross-language proof: roundtrip_v1.{ndjson,python.ndjson.zst,rust.ndjson.zst,json}
```

There is no message-encoding layer. Canonical evidence is the original envelope
lines copied byte for byte, and a codec that re-encoded them would make the archive
a lossy interpretation of the tape instead of a compressed copy of it. The format
is NDJSON inside Zstandard frames, with no Simple Binary Encoding, custom binary
schema, record blocks or extra framing; `tests/test_no_sbe.py` asserts none exists
and that nothing outside tests and scripts imports `whole_buffer`.

## The format

| Setting | Required value |
|---|---|
| algorithm | Zstandard |
| compression level | `3` (both `encode_stream` implementations refuse any other level) |
| frame checksum | enabled |
| dictionary | none |
| frames per object | exactly one |
| logical payload | exact NDJSON bytes |
| stream buffer | at most 1 MiB (`DEFAULT_BUFFER_BYTES`) |

An empty payload is a valid, non-empty frame that decodes to zero bytes, whose
logical digest is SHA-256 of the empty string. A non-empty payload must end in LF,
because the LF count is the record count. Receipts record the compression contract
as `{algorithm: "zstd", level: 3, frame_checksum: true, dictionary: null,
frame_count: 1, encoder: "<library>/<version>"}`. The encoder string is diagnostic
and never selects a decoder. Changing the level is a format change and needs a new
receipt version.

## Two identities

```text
logical  sha256, byte length and LF count of the decoded NDJSON
stored   sha256 and byte length of the complete frame
```

Both are computed in the same streaming pass, on the caller's bytes as they arrive.
Logical identity proves what can be reconstructed; stored identity proves which
physical object was committed. A receipt carrying one without the other proves half
of what it claims, so the encoders return them together (`EncodeResult.logical`,
`.stored`). SHA-256 digests are lowercase hex and plain (not domain-separated).

Python and Rust frames need not be byte-identical and nothing may depend on it.
Each must decode the other's output to identical logical bytes, which the fixtures
prove in both languages without either suite shelling out to the other toolchain.
`scripts/build_codec_fixtures.py` documents the two-step manual regeneration
(Python writes payload and frame; `cargo test --manifest-path encoder/rust/Cargo.toml
-- --ignored regenerate` writes the Rust frame; the script runs again to record
identities). A fixture any test run could rewrite would prove nothing.

## Decoding is adversarial

Success means one frame ended exactly at end-of-input with every identity matched.
These are rejected, in both languages:

- a truncated frame or a bad frame checksum;
- concatenated frames, or any trailing byte after the one frame;
- a dictionary-dependent frame;
- a stored length or digest that disagrees with the object;
- a decoded length, digest or LF count that disagrees with the expectation;
- output above the caller's hard maximum, which aborts before the sink receives
  byte `maximum + 1`.

Python's `read_across_frames=True` is never used. The `.zst` suffix and a successful
decompression are not evidence that this contract was followed.

Both decoders walk the frame structure themselves (magic, descriptor, block headers
by length, optional checksum) to find where the frame ends, because libzstd's
streaming readers buffer ahead and "how many bytes the source handed over" does not
say where the frame stopped; without this a padded or concatenated object decodes
as one healthy stream. Decode is bounded on the output side (`DECODE_INPUT_BYTES`
pulls, not push-feeding), because one 128 KiB compressed chunk of run-length blocks
can expand to gigabytes. Writes are full: a sink that accepts fewer bytes than
offered is retried, a sink accepting zero is an error, and the stored length counts
only bytes that reached the sink.

### Structural decode (Rust only)

`prediction_encoder::StructuralDecoder` is the same bounded decoder with both
SHA-256 digests removed. It keeps every other rule (one checksummed dictionary-free
frame, content checksum, no truncation or trailing bytes, output bound, final LF,
expected stored length, decoded length and LF count). It does not prove which bytes
were committed, so it exists only for a consumer whose stored bytes were already
bound to their receipt by SHA-256 at an earlier boundary: the Replay pinned
derivative read (`engine/DERIVATIVES.md`). Archive, canonical,
finalizer and audit decodes use the identity-checking decoder.

## Streaming only

No production path may call an API whose source or result is one complete
`bytes`/`Vec<u8>`. `whole_buffer.py` (`compress_bytes`, `decompress_bytes`,
`encode_identity`) exists for tests and probes in its own module so the ban is
checkable.

## Usage

```python
from encoder import decode_stream, encode_stream

with open(segment, "rb") as source, open(derivative, "wb") as sink:
    result = encode_stream(source, sink)

with open(derivative, "rb") as source, open(restored, "wb") as sink:
    decode_stream(source, sink,
                  expected_logical=result.logical,
                  expected_stored=result.stored,
                  max_decoded_bytes=result.logical.byte_length)
```

```rust
use prediction_encoder::{DEFAULT_ZSTD_LEVEL, decode_stream, encode_stream};

let result = encode_stream(File::open(segment)?, File::create(derivative)?, DEFAULT_ZSTD_LEVEL)?;
decode_stream(File::open(derivative)?, File::create(restored)?,
              &result.logical, Some(&result.stored), Some(result.logical.byte_length))?;
```

`ingester/` consumes the crate as a path dependency
(`prediction-encoder = { path = "../encoder/rust" }`); the crate has its own
manifest and tests.

## Where the contract is used

- Raw archive objects: `<segment>.ndjson.zst` derived from a sealed segment, with
  the logical identity checked against the seal (`archive/README.md`).
- Canonical windows: `evidence.ndjson.zst` and `provenance.ndjson.zst`, each one
  frame, committed by `receipt.json` (`ingester/FORMATS.md`).

Raw evidence is never compressed in place: the active segment stays exact NDJSON
and its seal commits byte length, line count and SHA-256. Only after that commit
may the archiver derive a `.zst` object.

## Tests

```bash
.venv/bin/python -m unittest tests.test_encoder tests.test_no_sbe
cargo test --manifest-path encoder/rust/Cargo.toml
```
