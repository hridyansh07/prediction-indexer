# Zstd Materialization Pipeline V1

Status: approved implementation specification. The implementation is deliberately
split into two independently testable changes: raw archival first, canonical
materialization second.

The implementation-level contract for Step 1 is
[`PHASE_4_RAW_ARCHIVE_REAPER_V1.md`](../archive/PHASE_4_RAW_ARCHIVE_REAPER_V1.md). It binds
the shared codec, object-store boundary, archiver, reaper, deployment safety gate,
and test-first acceptance criteria into one changeset.
The production AWS implementation of that backend is specified in
[`S3_RAW_ARCHIVE_ADAPTER_V1.md`](../archive/S3_RAW_ARCHIVE_ADAPTER_V1.md).

This document refines `SEALED_CAPTURE_PIPELINE_V1.md` without changing its
evidence model. A splice still commits exact NDJSON with a seal, and the
finalizer still copies those original envelope lines into
`(visible_ns, lane_rank, delivery_index)` order. Zstandard changes only the
durable representation after those logical bytes have been decided.

Where the two documents overlap, this document is authoritative for compressed
object names, compression parameters, compressed-object identities, archive
receipts, and canonical output receipts. The sealed-capture document remains
authoritative for segment validity, merge ordering, lane classification,
finalization deadlines, and the dual-receipt deletion rule.

This is a clean pre-deployment cutover. Existing generated canonical artifacts
and codec fixtures are deleted and regenerated. There is no legacy reader,
converter, dual-read period, or compatibility migration.

## 1. Decisions and non-goals

V1 stores NDJSON inside Zstandard frames. It does **not** use Simple Binary
Encoding (SBE), a custom binary event schema, record blocks, or an additional
message-framing layer.

The materialization and archival steps are:

1. **Raw archiver:** a valid sealed splice segment becomes one independently
   addressable `.ndjson.zst` object in S3, while the original seal remains
   available beside it.
2. **Canonical ingester/finalizer:** a finalized window becomes
   `evidence.ndjson.zst`, `provenance.ndjson.zst`, and the `receipt.json` that
   commits them on the local durable filesystem.
3. **Canonical archive sink:** a receipt-committed window is independently
   decoded against that receipt, then its two existing Zstandard frames and
   unchanged `receipt.json` are copied to immutable object storage. A separate
   local canonical archive receipt is published last.

The following are out of scope:

- changing envelope schemas or canonical merge order;
- normalizing venue payloads before canonical evidence is committed;
- combining a day of segments into one Zstandard frame or S3 object;
- replay or correction-dataset policy beyond defining the verified decoder
  boundary;
- retaining uncompressed canonical files as compatibility copies.

## 2. Shared Zstandard contract

### 2.1 Frame profile

Every stored data file contains exactly one complete Zstandard frame with:

| Setting | Required value |
|---|---|
| Algorithm | Zstandard |
| Compression level | `3` |
| Frame checksum | enabled |
| Dictionary | none |
| Frame count | exactly one |
| Logical payload | exact NDJSON bytes |

An empty logical file is encoded as a valid, non-empty Zstandard frame whose
decoded payload has length zero and SHA-256 equal to SHA-256 of the empty byte
string.

Encoders and decoders operate incrementally over fixed-size buffers. No
production API may require the complete source, compressed object, or decoded
object in memory. A buffer may be implementation-specific but must be bounded
independently of file size and no larger than 1 MiB per stream by default.

One frame is also the integrity boundary. A decoder rejects concatenated frames,
trailing bytes, a truncated frame, a bad frame checksum, and an unsupported
dictionary request. The `.zst` suffix alone is never evidence that this contract
was followed.

Python and Rust compressed byte streams are not required to be identical. Both
implementations must decode the other implementation's output to byte-identical
NDJSON. Within one deployed producer, the Zstandard library and settings are
pinned so retrying the same input is deterministic. Receipts record the concrete
encoder library/version for diagnosis; it is not used to select a decoder.

### 2.2 Two identities

Every compressed object has two independent identities:

```text
logical identity = SHA-256, byte length, and line count of decoded NDJSON
stored identity  = SHA-256 and byte length of the complete Zstandard frame
```

`line_count` is the number of LF (`0x0a`) bytes in the logical payload. Every
non-empty committed NDJSON payload must end with LF, so this is also the number
of records. Compression metadata and filenames are not included in either hash.

Writers calculate both identities during the same streaming operation:

```text
source bytes
  -> logical SHA-256 / byte count / LF count
  -> Zstandard encoder
  -> stored SHA-256 / byte count
  -> durable file or S3 upload
```

Logical identity proves what can be reconstructed. Stored identity proves which
physical object was committed. A receipt is invalid if it omits either identity
or if either identity fails verification.

### 2.3 Shared codec boundary

The `encoder` package becomes Zstandard-only:

- remove the Python SBE module and all SBE exports;
- remove Rust `SbeMessage`, SBE headers, encode/decode helpers, and SBE-specific
  errors;
- remove SBE fixtures, probes, examples, tests, and documentation;
- expose streaming Python encode/decode functions over binary readers and
  writers;
- expose streaming Rust encoder/decoder wrappers over `Read` and `Write`;
- keep whole-buffer Zstandard helpers only as small test/convenience wrappers
  implemented on top of the streaming path, never as production file APIs.

The Rust finalizer consumes the shared Rust encoder crate rather than adding a
second Zstandard wrapper under `ingester`. The Python archiver similarly consumes
the shared Python package.

## 3. Step 1 — raw segment archiver

### 3.1 Eligibility and paths

The archiver discovers only a segment with both:

```text
<segment>.ndjson
<segment>.seal.json
```

An `.ndjson.open`, a renamed segment without a seal, an unreadable seal, or a
seal that fails the existing segment validator is not eligible. The content
length, line count, SHA-256, lane, window, filename, and ordering fields are
validated using the same rules shared by the ingester and finalizer.

The local derived object and receipt are:

```text
<segment>.ndjson.zst
<segment>.archive.json
```

The S3 keys are:

```text
raw/lane=<lane>/date=<YYYY-MM-DD>/<segment>.ndjson.zst
raw/lane=<lane>/date=<YYYY-MM-DD>/<segment>.seal.json
```

`<segment>` is the existing segment stem, including its UTC window stamp,
zero-padded segment index, and segment ID. The archiver never concatenates two
segments, even when its hourly sweep discovers several at once.

### 3.2 Materialization and upload

For each eligible segment, the archiver performs these steps in order:

1. Parse the seal and validate its structure and relationship to the path.
2. Create `<segment>.ndjson.zst.open` without modifying the sealed source.
3. Stream the source into the Python Zstandard encoder while calculating both
   identities.
4. Finish the frame and verify the logical identity exactly matches the seal.
5. Fsync the compressed temporary file, rename it to `.ndjson.zst`, and fsync
   the containing directory.
6. Upload the compressed data and unchanged seal using conditional/immutable
   writes.
7. Read the remote object attributes and verify the compressed byte length and
   explicit S3 SHA-256 checksum. Verify the seal object's byte length and
   SHA-256 as well.
8. Write and fsync `<segment>.archive.json.open`, rename it to
   `<segment>.archive.json`, and fsync the directory. The archive receipt is the
   local proof that remote verification completed.

The data upload requests S3 SHA-256 checksum validation and compares the returned
checksum with the locally calculated stored SHA-256. An ETag is never accepted
as a SHA-256 digest. User-defined object metadata may duplicate identities for
operations, but metadata alone is not verification.

The data object uses `Content-Type: application/x-ndjson` and
`Content-Encoding: zstd`. Readers still use the receipt and seal, not HTTP
metadata, as the expected identity.

Object keys for data and seals are immutable. If a retry finds an existing
object with the expected stored identity, it may continue idempotently. An
existing key with a different length or SHA-256 is an integrity conflict: the
archiver stops, emits a high-priority error, preserves every local file, and
does not publish a receipt.

### 3.3 Archive receipt

The archive receipt uses this shape; field names are normative:

```json
{
  "archive_receipt_version": 1,
  "lane_id": "polymarket",
  "window_start_ns": 0,
  "window_end_ns": 0,
  "segment_id": "...",
  "segment_index": 0,
  "source": {
    "file": "<segment>.ndjson",
    "byte_length": 0,
    "line_count": 0,
    "sha256": "..."
  },
  "seal": {
    "file": "<segment>.seal.json",
    "byte_length": 0,
    "sha256": "...",
    "bucket": "...",
    "key": "raw/lane=polymarket/date=YYYY-MM-DD/<segment>.seal.json"
  },
  "object": {
    "bucket": "...",
    "key": "raw/lane=polymarket/date=YYYY-MM-DD/<segment>.ndjson.zst",
    "byte_length": 0,
    "sha256": "...",
    "s3_checksum_sha256": "...",
    "content_encoding": "zstd"
  },
  "compression": {
    "algorithm": "zstd",
    "level": 3,
    "frame_checksum": true,
    "dictionary": null,
    "frame_count": 1,
    "encoder": "python-zstandard/<version>"
  },
  "verified_at_ns": 0,
  "archiver_version": 1
}
```

Hexadecimal SHA-256 fields use lowercase 64-character hex. The S3 checksum field
records the exact service-returned representation in addition to the normalized
hex digest. Times are UTC Unix nanoseconds.

The local receipt is immutable once committed. A valid existing receipt causes
the segment to be skipped only after its structure, local source identity, and
referenced remote object attributes have been checked. A malformed receipt or a
receipt referring to a missing/mismatched local or remote object fails closed.

### 3.4 Failure, retry, manifest, and deletion

Any failure before step 8 leaves no archive receipt. In particular:

- compression or source validation failure leaves the raw segment and seal
  unchanged;
- an upload or remote-checksum failure leaves the raw segment and seal
  unchanged;
- a crash may leave `.ndjson.zst.open` or a complete `.ndjson.zst` without a
  receipt; both are uncommitted derivatives and may be removed and rebuilt;
- cleanup never removes a source merely because its S3 key exists.

A daily manifest is a replay catalog, not a new evidence or commit boundary. It
is generated from verified archive receipts and contains one entry per segment,
including the data key, seal key, logical identity, and stored identity. It does
not combine frames or replace the individual receipt checks. Because it is a
derived catalog, it may be rebuilt from receipts; data and seal objects remain
immutable.

The reaper may delete the local raw `.ndjson` and its seal only when:

1. the segment has a valid archive receipt whose remote objects still verify;
   and
2. a committed canonical receipt lists the same lane and source SHA-256 in its
   `inputs`.

The local `.ndjson.zst` derivative may be deleted after condition 1 because the
sealed raw segment can reproduce it until the dual-receipt reaper runs. Archive
receipts are retained after source deletion so the deletion decision remains
auditable. S3 raw data retention and expiry remain governed by the 5–7 day
investigation/replay policy in the sealed-capture specification.

### 3.5 Raw replay decoder

A raw archive object is never yielded directly to replay. The decoder:

1. obtains the expected identities from the manifest/receipt and original seal;
2. downloads the compressed object into a bounded-memory staged file while
   calculating its stored length and SHA-256;
3. verifies the stored identity and S3 checksum;
4. decodes exactly one frame with the seal's `byte_length` as a hard maximum and
   exact expected final length;
5. calculates logical SHA-256 and LF count during decode;
6. verifies all logical values against both the archive receipt and seal;
7. only then exposes the staged decoded segment as trusted replay evidence.

This may use disk twice but never buffers a segment in memory and never exposes
partially verified records. A limit breach aborts before writing byte
`seal.byte_length + 1` to the decoded staging file.

## 4. Step 2 — canonical ingester/finalizer

### 4.1 Output and logical semantics

A committed window contains only:

```text
canonical/date=<YYYY-MM-DD>/window=<window_start_ns>/
  evidence.ndjson.zst
  provenance.ndjson.zst
  receipt.json
```

There are no durable `evidence.ndjson` or `provenance.ndjson` compatibility
copies. Temporary files contain compressed bytes and are named:

```text
evidence.ndjson.zst.open
provenance.ndjson.zst.open
receipt.json.open
```

Decoded `evidence.ndjson.zst` is byte-for-byte the same logical canonical
evidence the existing finalizer produces: original envelope lines, each ending
in LF, in canonical merge order. The finalizer does not parse and reserialize an
envelope to create these bytes.

Decoded `provenance.ndjson.zst` keeps the existing one-record-per-canonical-line
schema and order. Its `canonical_seq`, source segment identity, source line,
record ID, content hash, continuity verdict, and visible tie group semantics are
unchanged.

### 4.2 Streaming writer and commit order

The finalizer owns two dual-hash writers. For every logical write, each writer:

1. updates decoded SHA-256, decoded byte length, and decoded LF count;
2. writes the same bytes into its Zstandard encoder;
3. hashes and counts the compressed bytes emitted to its `.open` file.

If the merge invalidates a lane and restarts without that lane, both partial
compressed files are abandoned and both identity accumulators are discarded.
No data from the failed merge attempt may enter the committed frame.

Once the window merge succeeds, commit order is:

1. finish both Zstandard frames, including their frame checksums;
2. flush and fsync both compressed `.open` files;
3. rename both files to their final `.zst` names;
4. fsync the window directory, making both data-file names durable;
5. serialize, write, and fsync `receipt.json.open`;
6. rename it to `receipt.json`;
7. fsync the window directory again.

The receipt is the sole commit marker. A crash before step 6 leaves no committed
window even if one or both compressed files have final names. On retry, those
unreceipted outputs may be deleted and deterministically rebuilt. Once a valid
receipt exists, the window and its named files are immutable.

Directory creation retains the existing durable parent-directory fsync rules.
Any file or directory fsync error is a failed commit, not a warning.

### 4.3 Canonical receipt

This is a pre-deployment schema replacement, not a migration. The receipt keeps
`receipt_version: 1`; old generated receipts are invalid because their output
objects do not have the required fields and are deleted with their generated
artifacts.

All existing window, completeness, lane, deadline, input, sequence, and
finalizer fields remain. The `evidence` and `provenance` objects use this
normative shape:

```json
{
  "receipt_version": 1,
  "window_start_ns": 0,
  "window_end_ns": 0,
  "completeness": "complete",
  "certified": true,
  "expected_lanes": ["polymarket", "kalshi", "limitless"],
  "present_lanes": ["polymarket", "kalshi", "limitless"],
  "unexpected_lanes": [],
  "missing_lanes": [],
  "invalid_lanes": [],
  "finalization_deadline_seconds": 0,
  "deadline_expired": false,
  "finalized_at_ns": 0,
  "inputs": [],
  "evidence": {
    "file": "evidence.ndjson.zst",
    "content_encoding": "zstd",
    "decoded": {
      "byte_length": 0,
      "line_count": 0,
      "sha256": "..."
    },
    "stored": {
      "byte_length": 0,
      "sha256": "..."
    },
    "compression": {
      "algorithm": "zstd",
      "level": 3,
      "frame_checksum": true,
      "dictionary": null,
      "frame_count": 1,
      "encoder": "zstd-rs/<crate-version>; libzstd/<version>"
    }
  },
  "provenance": {
    "file": "provenance.ndjson.zst",
    "content_encoding": "zstd",
    "decoded": {
      "byte_length": 0,
      "line_count": 0,
      "sha256": "..."
    },
    "stored": {
      "byte_length": 0,
      "sha256": "..."
    },
    "compression": {
      "algorithm": "zstd",
      "level": 3,
      "frame_checksum": true,
      "dictionary": null,
      "frame_count": 1,
      "encoder": "zstd-rs/<crate-version>; libzstd/<version>"
    }
  },
  "first_canonical_seq": 1,
  "last_canonical_seq": 1,
  "finalizer_version": 1
}
```

An empty window has `first_canonical_seq: null` and
`last_canonical_seq: null`, with decoded line count zero in both outputs. The
stored identities still describe two valid Zstandard frames and therefore have
non-zero byte lengths.

The receipt parser fails closed unless:

- every required compression field has the V1 value;
- both stored files exist and match their stored lengths, or the intentional
  canonical-reaper tombstone of §4.5 proves why they are absent;
- decoded evidence and provenance line counts are equal;
- the canonical sequence range agrees with the decoded evidence line count;
- empty/non-empty sequence fields follow the rule above.

Routine startup may continue using stored lengths for its bounded scan. Full
stored hashes, frame validity, decoded identities, and provenance alignment are
checked by the explicit canonical integrity/audit path before replay or export.

### 4.4 Canonical decoder and integrity audit

The canonical decoder uses the receipt as its expected contract. For each file
it:

1. verifies the stored byte length and SHA-256 while reading the complete
   compressed object;
2. rejects anything other than exactly one valid V1 frame;
3. decompresses with `decoded.byte_length` as both a hard ceiling and required
   final length;
4. recalculates decoded SHA-256 and LF count;
5. compares every decoded value with the receipt.

The integrity pass then verifies:

- evidence and provenance decoded line counts are equal;
- provenance `canonical_seq` values are dense and match their evidence line
  ordinals;
- provenance source segment hashes occur in the receipt inputs;
- record IDs and content hashes match the corresponding decoded evidence
  envelopes;
- the receipt's first/last sequence fields match the decoded range.

No decoded line is exposed to replay or export as trusted canonical evidence
until the complete file and receipt have passed. Implementations may stage the
decoded result on disk or keep it quarantined behind the audit iterator; they
must not solve this requirement by buffering the file in memory.

A receipt referencing a missing, truncated, corrupt, multi-frame, or logically
mismatched object is an integrity failure. It is never treated as an open window
and is never silently regenerated over the committed receipt.

### 4.5 Canonical archive retention and local reaping

Canonical archival and local deletion are separate commands. The archiver has
no removal primitive. The canonical reaper defaults to audit and refuses a
retention floor below **18 hours** or destructive operation without an
independently durable backend.

For a receipt-committed window the reaper requires, at decision time:

1. a strict production `canonical_archive_receipt.json`; local conformance
   receipts authorize nothing;
2. an independently durable configured store matching that receipt;
3. age of at least 18 hours from the latest of window end, finalization time,
   archive verification time, canonical-receipt mtime, and archive-receipt
   mtime;
4. exact identity agreement between the local canonical receipt, both output
   identities, and the archive receipt;
5. fresh object heads verifying all three immutable archive objects; and
6. explicit delete mode.

Only `evidence.ndjson.zst` and `provenance.ndjson.zst` are removed, evidence
first and with a directory fsync after each unlink. The canonical `receipt.json`
and production archive receipt remain as a compact tombstone. This is required,
not optional bookkeeping: the finalizer rebuilds its watermark, global sequence,
continuity, and carried clock state from canonical receipts. Deleting that
receipt would allow committed history to be forgotten and re-finalized.

A crash after removing evidence but before provenance is a recognizable partial
cleanup and may finish only after all gates are re-established. The reverse
partial state is not produced by this reaper and fails closed. Finalizer startup
accepts missing frames only when the strict production archive marker binds the
unchanged local receipt and both frame identities. Canonical integrity reports
such windows separately as archived-and-reaped; it does not claim their records
were locally decoded and verified.

## 5. Implementation order and proof gates

### Step 1 changeset — shared codec and raw archive

The first changeset removes SBE, adds the Python and Rust streaming Zstandard
interfaces, and implements raw archival. It lands only when the following tests
first demonstrate the unsafe or missing old behavior and then pass:

- Python output decodes in Rust and Rust output decodes in Python to exact
  bytes;
- empty NDJSON round-trips through a valid frame;
- truncated frames, trailing bytes, concatenated frames, bad checksums, and
  dictionary-dependent frames fail;
- a generated input larger than available codec buffers proves bounded-memory
  streaming;
- a changed or unsealed segment cannot produce an archive receipt;
- source, compressed, upload, and remote-checksum failures preserve the raw
  segment and produce no receipt;
- an identical retry succeeds and an immutable-key conflict fails closed;
- the decoder rejects output above the sealed byte length;
- the reaper refuses deletion until both archive and canonical evidence are
  proven;
- repository search finds no SBE code, imports, dependencies, fixtures, probes,
  or active documentation.

### Step 2 changeset — compressed canonical materialization

The second changeset replaces the finalizer's uncompressed output with the two
compressed primary files and regenerates all canonical fixtures. It lands only
when:

- decoded evidence is byte-identical to the pre-compression expected merge;
- canonical ordering, tie groups, lane-invalid retry, and `EvidenceSeq` are
  unchanged;
- both stored and decoded identities verify independently;
- decoded evidence and provenance remain one-to-one and provenance alignment
  passes;
- empty complete and empty incomplete windows produce valid frames and coherent
  receipts;
- injected crashes at every finish, fsync, rename, and receipt boundary leave
  either no receipt or a fully verifiable committed window;
- a valid receipt with a missing or corrupt compressed file fails closed;
- no committed uncompressed canonical data file exists;
- finalization and verification use bounded memory for multi-gigabyte windows.

After Step 2, deployment documentation and Compose paths are updated to show the
new filenames. The archive sink consumes the receipt-committed `.zst` files
without recompressing or reserializing them. Before upload it runs both files
through the shared strict decoder with the receipt's stored and decoded
identities and decoded-byte ceilings. It then uploads, in order:

```text
canonical/date=<YYYY-MM-DD>/window=<window_start_ns>/evidence.ndjson.zst
canonical/date=<YYYY-MM-DD>/window=<window_start_ns>/provenance.ndjson.zst
canonical/date=<YYYY-MM-DD>/window=<window_start_ns>/receipt.json
```

The unchanged canonical `receipt.json` is uploaded after the two data objects.
Fresh object-store heads must prove all three SHA-256 identities, lengths, and
content metadata. Only then may the sink durably publish
`canonical_archive_receipt.json` beside the local window (or the explicitly
non-authoritative `.local.json` conformance form). A failed decode, upload,
head, or immutable-key check leaves no canonical archive receipt and never
modifies the committed window.
