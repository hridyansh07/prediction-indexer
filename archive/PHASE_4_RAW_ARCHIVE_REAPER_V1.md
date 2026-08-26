# Phase 4 — Shared Zstd Codec, Raw Archiver, and Reaper V1

Status: proposed implementation specification, contingent on the durability gate
in §5.3. This phase combines phase 4 of
[`SEALED_CAPTURE_PIPELINE_V1.md`](../docs/SEALED_CAPTURE_PIPELINE_V1.md) with Step 1 of
[`ZSTD_MATERIALIZATION_PIPELINE_V1.md`](../encoder/ZSTD_MATERIALIZATION_PIPELINE_V1.md).
The production AWS backend that follows this phase is specified separately in
[`S3_RAW_ARCHIVE_ADAPTER_V1.md`](S3_RAW_ARCHIVE_ADAPTER_V1.md).

This document turns those two architectural contracts into one implementable
changeset. It does not change the evidence model: a splice still commits exact
NDJSON through a seal, and a canonical receipt still proves which source
segments were incorporated into a finalized window.

Where the documents overlap:

- the Zstd specification remains authoritative for compression parameters,
  logical and stored identities, S3 object names, and the production archive
  receipt schema;
- the sealed-capture specification remains authoritative for segment validity,
  canonical input membership, late data, retention, and the two-receipt deletion
  rule;
- this document is authoritative for phase boundaries, the object-store
  abstraction, failure handling, deployment gates, and acceptance tests.

## 1. Outcome and scope

Phase 4 adds four things:

1. a shared Python/Rust, Zstandard-only streaming codec;
2. an immutable object-store boundary with a local conformance adapter;
3. a raw-segment archiver that publishes a receipt only after verified storage;
4. a separate reaper that removes local raw data only after both archive and
   canonical receipts prove it is safe.

The intended lifecycle is:

```text
sealed raw segment
  -> validated and compressed
  -> immutable archive data + unchanged seal
  -> remotely verified archive receipt
  -> canonical receipt names the same source segment
  -> local raw segment and seal become deletion-eligible
```

The unit of archive, verification, retry, and deletion is one sealed segment.
An hourly sweep may process many segments, but it never combines their frames or
their commit boundaries.

This phase does **not** compress canonical evidence. Until Step 2 of the Zstd
specification lands, `canonical/` remains an uncompressed second logical copy of
the captured lines. Phase 4 bounds `spool/` only after a production-grade archive
backend is configured; it does not by itself solve the complete local capacity
problem.

### 1.1 Out of scope

- `evidence.ndjson.zst` and `provenance.ndjson.zst`;
- uploading canonical windows;
- changing envelope schemas, seals, merge order, or continuity classification;
- combining segments into an hourly or daily compressed object;
- a production S3 adapter in the initial changeset;
- object-store lifecycle expiry and phase 5 daily replay;
- correction datasets for late segments;
- the Linux throughput and clock re-measurement.

## 2. Non-negotiable safety invariants

1. **The seal is a claim, not a substitute for reading the source.** The
   archiver recomputes logical SHA-256, byte length, and LF count while the source
   bytes pass through the encoder, then compares all three with the seal.
2. **The archive receipt is the archive commit marker.** A compressed file, an
   object-store key, or a successful upload call is not sufficient.
3. **Deletion is a separate decision.** Archival code never removes raw input.
4. **Raw deletion requires two independent proofs:** a verified archive receipt
   and a committed canonical receipt naming the same lane and source digest.
5. **No full-file buffering.** Codec, archive, verification, and replay decode
   memory is bounded independently of segment size.
6. **Every immutable-key conflict fails closed.** Existing different content is
   never overwritten, versioned in place, or accepted as a retry.
7. **An archive is a different durability domain.** A test directory on the
   capture filesystem cannot authorize production deletion merely because it
   implements the object-store interface.
8. **Derived artifacts are not authorities.** Local `.ndjson.zst` files and
   daily manifests can be deleted and rebuilt. Seals, archive receipts,
   canonical receipts, and verified archive objects are the authorities.

## 3. Segment state machine

Each segment is in one of these externally observable states:

| State | Required artifacts | Meaning |
|---|---|---|
| open | `.ndjson.open` | writer-owned and ineligible |
| sealed | `.ndjson` + valid `.seal.json` | eligible for archive/finalization |
| materialized | sealed state + local `.ndjson.zst` | rebuildable derivative only |
| archived | valid `.archive.json` + verified archive data and seal objects | archive commit complete |
| canonicalized | a valid canonical `receipt.json` names lane + source SHA-256 | source bytes occur in committed canonical evidence |
| reapable | archived + canonicalized + production durability gate | local source may be deleted |
| reaped | raw `.ndjson` and local seal absent; archive receipt retained | local deletion completed |

State is inferred from proofs, not from filenames alone. In particular,
`materialized` is never equivalent to `archived`, and `archived` is never
equivalent to `reapable`.

A late or invalid segment may remain permanently in `archived` without becoming
`canonicalized`. That is expected fail-closed behaviour: it must follow the
correction policy before local raw deletion can become eligible. The reaper
reports these retained segments; it does not guess them into a canonical window.

## 4. Phase 4a — shared Zstandard codec

### 4.1 Format profile

Every encoded object is exactly one complete Zstandard frame:

| Parameter | Value |
|---|---|
| compression level | `3` |
| frame checksum | enabled |
| dictionary | none |
| frame count | exactly one |
| logical payload | exact NDJSON bytes |
| maximum default stream buffer | 1 MiB |

An empty input produces a valid non-empty frame that decodes to zero bytes.
Python and Rust encoders need not produce identical compressed bytes. They must
decode one another's output to identical logical NDJSON.

### 4.2 Streaming interfaces

The Python package exposes streaming encode and decode operations over binary
readers and writers. The Rust crate exposes equivalent generic operations over
`Read` and `Write`. Production file callers use only these streaming paths.

Both encoders return:

```text
logical = {sha256, byte_length, line_count}
stored  = {sha256, byte_length}
```

The logical counters observe source bytes before compression. Stored counters
observe the complete frame emitted by the encoder. `line_count` is the number of
LF bytes and every non-empty accepted NDJSON input must end in LF.

Decode requires an expected stored identity and an exact logical identity. It
must accept a hard maximum decoded byte length before producing output. A limit
breach fails before byte `maximum + 1` is written to the caller's sink.

Whole-buffer helpers may remain only as test conveniences implemented on top of
the streaming API. No archiver, finalizer, replay, or deployment path may call
an API whose source or result is one complete `bytes`/`Vec<u8>` segment.

### 4.3 Strict decoding

Decoders reject:

- truncated frames;
- bad frame checksums;
- concatenated frames;
- any trailing byte after the one expected frame;
- dictionary-dependent frames;
- stored length or SHA-256 mismatch;
- decoded length, SHA-256, or LF-count mismatch;
- output above the configured hard maximum.

Python's permissive `read_across_frames=True` behaviour is prohibited. Success
means one frame ended exactly at end-of-input and every identity matched.

### 4.4 Cross-language fixtures

Commit these fixtures:

```text
encoder/fixtures/roundtrip_v1.ndjson
encoder/fixtures/roundtrip_v1.python.ndjson.zst
encoder/fixtures/roundtrip_v1.rust.ndjson.zst
```

Each language decodes the other fixture and compares exact decoded bytes with
`roundtrip_v1.ndjson`. Tests do not shell out to the other toolchain. Fixture
metadata records the encoder library/version and both identities so fixture
changes are reviewable rather than silently regenerated.

### 4.5 SBE removal

Delete all SBE modules, exports, Rust types, dependencies, probes, hex fixtures,
tests, examples, and active documentation. Search results containing ordinary
words such as `misbehaving` are not SBE references and must not be mechanically
edited.

The standalone Rust encoder crate remains outside the ingester workspace during
this phase. It joins that workspace only when the Rust finalizer consumes it in
Zstd Step 2.

## 5. Phase 4b — immutable object-store boundary

### 5.1 Protocol

The write-side storage protocol is deliberately smaller than an S3 client:

```text
put_immutable(key, reader, expected_identity) -> object metadata
head(key)                                      -> object metadata or absent
verify(expectation)                            -> verified object metadata
open(key)                                      -> bounded byte reader
open_verified(expectation)                     -> receipt-verified byte reader
```

`expected_identity` is known before publication. The adapter verifies that the
reader supplies exactly those bytes. This makes retry and conflict semantics
explicit and gives a future S3 adapter the expected checksum needed for a
conditional write.

Object metadata includes at least:

```text
key, byte_length, sha256, provider checksum, content type, content encoding
```

The provider checksum is separate from normalized lowercase SHA-256. An S3 ETag
is never a provider SHA-256 checksum. GCS records the SHA-256 calculated over
the exact upload stream as custom metadata and records the service-validated
CRC32C separately as provider evidence. Normal GCS verification compares both
with the receipt without downloading the object body.

Keys are normalized relative POSIX paths. Empty components, `.`, `..`, absolute
paths, backslashes, and traversal outside the configured root are rejected.

### 5.2 Immutable put semantics

- Absent key: stream to a unique temporary object, verify the expected identity,
  then publish atomically or conditionally.
- Existing key with the expected identity: return success without rewriting it.
- Existing key with a different identity: raise an integrity conflict and make
  no change.
- Transport or verification failure: return failure; never report the key as
  committed.

`head` used for receipt verification must obtain current provider metadata. S3
returns server SHA-256; GCS returns server CRC32C and the application SHA-256
bound to its checksum-validated immutable upload. `verify` compares that
provider-specific result with one complete receipt expectation; archive
consumers map their strict receipt schemas into this one operation rather than
reproducing identity, checksum, and content-metadata comparisons.

Replay uses `open_verified` instead of `head` followed by `open`. The adapter
checks receipt-owned length, SHA-256, provider checksum, content metadata, and
the provider's immutable generation/version while one bounded stream is
consumed. Leaving that stream before EOF is a verification failure. `head`
remains metadata-only where the provider supplies sufficient receipt evidence.

### 5.3 LocalObjectStore

`LocalObjectStore` is the first adapter. It writes under a configured root using
the same discipline as a seal:

```text
unique .open -> write -> fsync file -> atomic rename -> fsync directory
```

It reopens and re-hashes stored objects for `head`, detects concurrent mutation,
and never follows a key outside its root. Its purpose is to exercise real
immutability, retry, verification, and crash behaviour without making tests
depend on cloud credentials.

The local adapter is **not production deletion authority by default**. The
production reaper must refuse destructive operation unless the selected backend
declares an explicitly configured independent durability class. A directory on
the same capture volume remains suitable for tests and compression-ratio probes,
but losing that volume would otherwise lose both copies at once.

Legacy production S3 archive receipt version 1 remains readable. New
independently durable stores publish provider-neutral production receipt version
2 as specified by the Zstd and provider-adapter specifications.
Local conformance metadata must not be serialized so that it can be mistaken for
an S3-verified `archive_receipt_version: 1`. Tests may exercise the reaper library
against a temporary backend, but the deployment CLI keeps the durability gate.

An end-to-end local archiver may write
`<segment>.archive.local.json` with an explicitly separate
`local_archive_receipt_version`. It carries the same source, logical, stored, and
compression identities plus a local store identifier and key, but has no
`bucket` or `s3_checksum_sha256` claim. Production manifest builders and reapers
must ignore this file. This keeps the local path testable without creating an
artifact that a later S3 deployment could mistake for deletion authority.

## 6. Phase 4c — raw segment archiver

### 6.1 Discovery and validation

The archiver discovers through the existing sealed-segment rules. Eligible input
has both:

```text
<segment>.ndjson
<segment>.seal.json
```

It ignores `.ndjson.open` and an `.ndjson` whose seal has not appeared yet. An
unreadable, malformed, path-incoherent, or digest-invalid seal is a reported
integrity fault, not pending work.

The Python validator must enforce the same seal fields and relationships as the
shared Rust segment crate: filename, lane, window, segment index, lengths,
ordering flags, delivery bounds, and digest. Replay's Gate 1 may consume the
shared result, but the archiver must not copy a partial second validator from the
replay audit.

### 6.2 Object keys

Every source becomes two independently immutable objects:

```text
raw/lane=<lane>/date=<YYYY-MM-DD>/<segment>.ndjson.zst
raw/lane=<lane>/date=<YYYY-MM-DD>/<segment>.seal.json
```

The seal object contains the exact unchanged local seal bytes. The data object
uses `Content-Type: application/x-ndjson` and `Content-Encoding: zstd`.

### 6.3 Ordered commit procedure

For one segment:

1. Parse and structurally validate the seal against its path.
2. Create a unique local `.ndjson.zst.open` derivative.
3. Stream raw bytes through the shared encoder, calculating logical and stored
   identities.
4. Finish the frame and compare logical SHA-256, byte length, and LF count with
   the seal.
5. Fsync the derivative, rename it to `.ndjson.zst`, and fsync the directory.
6. Calculate the unchanged seal object's byte length and SHA-256.
7. Publish data and seal through immutable object-store writes.
8. Call `head` for both objects and verify their actual remote identities and
   provider checksums.
9. Encode the normative provider-neutral production archive receipt from §3.3
   of the Zstd specification, or the explicitly non-authoritative local
   conformance receipt from §5.3.
10. Write the selected receipt through `.open`, fsync it, rename it to its final
    `.archive.json` or `.archive.local.json` name, and fsync the directory.

For an independent cloud backend, Step 10 is the sole archive commit. A local conformance receipt proves
the same control flow in tests but does not enable production deletion.

### 6.4 Retry and existing receipts

A valid existing archive receipt is idempotent only after:

- its schema and compression contract validate;
- its source identity matches the present raw source and seal when those local
  files still exist;
- both named archive objects exist and match their receipt identities;
- the seal object decodes to the exact unchanged seal bytes;
- the object provider's checksum proof is valid.

If the segment has already been reaped, manifest rebuilding verifies the remote
objects and retained receipts without requiring the local raw source.

A malformed receipt, missing object, checksum mismatch, or immutable-key
conflict fails closed. It is never repaired by overwriting the receipt or object.

An unreceipted `.ndjson.zst` is untrusted. A retry either revalidates it completely
against the current sealed source or deletes and deterministically rebuilds it;
it never uploads it merely because the filename exists.

### 6.5 Failure isolation

| Failure | Required result |
|---|---|
| source/seal mismatch | no upload receipt; preserve raw and seal |
| compression failure | no upload receipt; preserve raw and seal |
| transient object-store failure | no receipt; preserve raw and retry later |
| remote verification mismatch | no receipt; preserve every local artifact |
| existing different immutable object | fatal integrity conflict; stop sweep |
| crash before receipt rename | no committed receipt; raw remains ineligible for reaping |
| crash after receipt rename | retry revalidates receipt and objects idempotently |

One invalid segment does not erase or rewrite another segment's successful
receipt. A key conflict is fatal for the sweep because it indicates namespace or
integrity failure rather than an isolated malformed source.

### 6.6 Archive receipt and daily manifest

New production archive receipts use the exact `archive_receipt_version: 2`
schema in the Zstd specification. Hex digests are lowercase, byte counts are
integers, times are UTC Unix nanoseconds, and provider checksum evidence is
recorded separately from normalized SHA-256. Strict readers retain legacy
version 1 support.

The daily manifest is a derived replay catalog built only from revalidated
archive receipts. It contains one entry per segment with lane, window, data key,
seal key, logical identity, and stored identity. Entries sort by
`(window_start_ns, lane rank, segment_index, segment_id)`.

The manifest is not an archive receipt, does not replace per-object checks, and
may be atomically regenerated. An open UTC day may have a changing local
manifest; an immutable published daily manifest is produced only after the day
closes and every included receipt revalidates.

## 7. Phase 4d — dual-receipt reaper

### 7.1 Separate authority

The reaper is a separate command and module from the archiver. The archiver may
report eligibility but cannot invoke deletion as a success callback.

For each raw segment, the reaper requires all of the following at decision time:

1. a structurally valid archive receipt;
2. archive data and seal objects that still match that receipt under `head`;
3. an archive backend authorized as an independent durability domain;
4. a structurally valid committed canonical `receipt.json`;
5. a canonical `inputs` entry matching the source lane, source SHA-256,
   `data_file`, and segment index recorded by the archive receipt;
6. the current local raw source and seal still matching the archive receipt, if
   present.

Lane plus SHA-256 is the minimum identity required by the sealed-capture
contract. Matching filename and segment index as well makes accidental
cross-segment authorization fail loudly.

The presence of canonical files without `receipt.json`, an incomplete archive
upload, a daily manifest entry, or a matching object-store key satisfies none of
these conditions.

### 7.2 Deletion behaviour

The local compressed derivative may be deleted after the verified archive
receipt alone. Raw `.ndjson` and `.seal.json` require the full §7.1 gate.

Deletion is idempotent and fsyncs the containing directory after each material
unlink. A crash between removing the raw file and its seal leaves a recognizable
partial cleanup that the next run may finish only after rechecking all receipts
and remote identities. Archive receipts are never deleted by the reaper.

The reaper emits a durable/reportable decision containing at least:

```text
lane, source file, source SHA-256, archive receipt path,
canonical receipt path, decision, reason, verified_at_ns
```

This report is an audit aid, not a third deletion authority. The two retained
receipts remain the proof.

### 7.3 Fail-closed cases

The reaper retains local raw data when:

- only one of the two receipts exists;
- either receipt is malformed or unverifiable;
- either archive object is missing or mismatched;
- the canonical receipt names the lane but not the exact source segment;
- the segment was late, excluded, invalid, or never canonicalized;
- the backend is a default local conformance store;
- an I/O error prevents complete verification.

These cases are surfaced as metrics and structured results. They are not silently
treated as ordinary retention backlog.

## 8. Phase 4e — commands and deployment

### 8.1 Command behaviour

The archiver and reaper support one-shot sweeps so tests and schedulers observe a
complete result. A watch mode may call the same sweep implementation at a
configured interval; it must not introduce different eligibility logic.

Recommended defaults:

```text
archiver sweep interval: 1 hour
archive unit:            1 sealed segment
reaper sweep:            after finalizer/archive sweeps or an independent schedule
```

Both commands expose structured counts for discovered, archived, skipped,
pending, conflicted, reapable, reaped, and retained segments. Integrity conflicts
produce a non-zero exit. No command logs credentials, private keys, or signed
request material.

### 8.2 Compose services

Add `archiver` and `reaper` services to the `ops` profile beside `finalizer`.
They share the spool/canonical bind mount read-write only where required. The
archive destination is configured separately from `CAPTURE_DATA_ROOT`.

The initial local adapter may be used in CI, development, and a compression
probe. Compose must not enable destructive production reaping against it by
default. Until the S3 adapter or an explicitly approved independent durable
backend exists, the reaper runs in audit/dry-run mode.

Deployment documentation must state:

- raw local deletion is not active merely because Phase 4 code is installed;
- how the archive durability gate is enabled;
- archive object and receipt layouts;
- how immutable conflicts are alerted and repaired operationally;
- how retained late/invalid segments are monitored;
- that canonical storage remains uncompressed until Zstd Step 2;
- that a killed finalizer may leave `.finalize.lease` requiring operator review.

### 8.3 Capacity statement

After a production archive backend and reaper are enabled, `spool/` is bounded by
finalization delay, archive delay, exceptional retained segments, and an
operational safety margin. The archive tier grows according to compressed size
until its 5–7 day lifecycle policy is enabled.

With only `LocalObjectStore` on the capture disk, total local storage is not
bounded; bytes have merely changed representation and directory. With Step 1 but
not Step 2, `canonical/` still grows at the uncompressed logical rate. Deployment
capacity claims must keep both facts visible.

## 9. Test-first implementation sequence

Every production change begins with a test that demonstrates the current unsafe
or missing behaviour.

### 9.1 Codec tests

- Python decodes the committed Rust frame and Rust decodes the committed Python
  frame to exact fixture bytes.
- Empty NDJSON produces and decodes from a valid frame.
- Truncated, trailing, concatenated, bad-checksum, and dictionary frames fail.
- Wrong stored hash and wrong logical hash/length/LF count fail independently.
- Decode limit aborts before writing the first byte beyond the limit.
- A generated input much larger than 1 MiB proves memory does not scale with
  input size.
- Whole-buffer production use is absent and repository search finds no SBE.

### 9.2 Object-store tests

- A new immutable key commits durably.
- An identical put is idempotent.
- A different value at the same key is an integrity conflict and preserves the
  first value.
- `head` detects post-write mutation rather than echoing cached metadata.
- Path traversal and symlink escape attempts fail.
- Injected failures before write, fsync, rename, and directory fsync never expose
  a partially committed final key.

### 9.3 Archiver tests

- Open, unsealed, malformed-seal, changed, and out-of-path segments publish no
  archive receipt and remain untouched.
- Logical identity is recomputed during compression and must equal the seal.
- Upload and remote verification failures publish no receipt.
- Data and seal immutable-key conflicts fail closed.
- A crash at every ordered step leaves either no receipt or a fully revalidatable
  archive.
- Existing valid receipts are idempotent; corrupt receipts and missing objects
  fail closed.
- An unreceipted local `.ndjson.zst` is not trusted.
- Daily manifests contain only revalidated receipts and are deterministic.

### 9.4 Reaper tests

- Archive receipt only: retain.
- Canonical receipt only: retain.
- Both receipts but mismatched lane/digest/file/index: retain.
- Both receipts but missing/mutated archive object: retain.
- Both valid receipts on a non-authoritative local backend: production CLI
  retains and reports the durability gate.
- Both valid receipts with an authorized test backend: delete raw and seal while
  retaining the archive receipt.
- Upload failure or checksum mismatch always prevents deletion.
- Crashes between the two unlinks resume idempotently after revalidation.
- Late or lane-invalid segments remain retained and visible in the report.

### 9.5 Regression and real-data gates

All of the following must pass:

```text
python -m unittest discover -s tests
python -m unittest discover -s replay/tests
cargo test --workspace                     # from ingester/
cargo test --manifest-path encoder/rust/Cargo.toml
docker compose config --quiet
```

Against a real captured segment, without modifying the fixture source:

1. archive through the local conformance adapter;
2. decode the stored frame and compare exact bytes, length, LF count, and digest
   with the source seal;
3. report compressed/uncompressed byte lengths and compression ratio;
4. demonstrate that decode cannot exceed the seal's declared length;
5. record peak RSS while processing a segment substantially larger than the
   codec buffer.

Real-data gates must name the fixture path and identities but must not commit
credentials or private venue payloads to the repository.

## 10. Completion criteria

Phase 4 code is complete when:

- SBE is absent and both streaming codec implementations meet §4;
- the cross-language fixtures prove byte-exact interoperability;
- raw archiving is bounded-memory, immutable, retry-safe, and receipt-last;
- the local object-store adapter passes the same identity/conflict tests expected
  of S3;
- no failed or partially verified archive can authorize deletion;
- reaper logic proves both receipts and remote identity immediately before
  deletion;
- local test storage cannot accidentally enable production deletion;
- daily manifests are deterministic derived catalogs;
- deployment docs describe the remaining capacity and durability limits;
- every regression and real-segment gate in §9 passes.

Production raw-spool deletion is a separate readiness statement. It becomes
enabled only when an independent durable backend produces the production archive
receipt contract and the operator explicitly enables the reaper's destructive
mode. Until then, Phase 4 may be merged and exercised, but local raw data remains
the recovery authority.

---

# Implementation status (2026-07-31)

Landed. What follows records what was built, what the specification did not
anticipate, and what remains deliberately undone.

```text
encoder/compression.py      streaming Python codec, two identities, strict decode
encoder/whole_buffer.py     test-only helpers, isolated so the ban is checkable
encoder/rust/src/lib.rs     the same contract over Read and Write
encoder/fixtures/           roundtrip_v1: payload, both frames, both identities
archive/storage/base.py     immutable provider-neutral boundary
archive/storage/local.py    LocalObjectStore and filesystem helpers
archive/storage/s3.py       production AWS S3 adapter
archive/storage/factory.py  shared backend selection
archive/common/seal.py      Python mirror of the Rust segment validator
archive/common/receipts.py  archive and canonical receipt readers
archive/common/verify.py    re-proving an archive against the store
archive/archiver/           service, derived daily manifest, and CLI
archive/reaper/             dual-receipt service and audit-first CLI
scripts/archive_probe.py    §9.5 against a real captured segment
```

## Three things the specification did not say, found by building it

**Bounding decode output is not the same as bounding decode input.** §4.2 asks
for memory bounded independently of segment size, and the obvious Python
implementation — feed `decompressobj` in fixed chunks — does not provide it. A
push decoder returns everything one input chunk expands to, so a 128 KiB chunk
of run-length blocks can return gigabytes. The first bounded-memory test failed
at 16 MiB peak on a 16 MiB payload, which is exactly the failure invariant 5
exists to prevent. Both decoders now pull fixed-size *output* instead.

**Finding the end of one frame needs the frame structure, not the byte count.**
Strict single-frame decoding (§4.3) requires knowing where the frame ended, and
libzstd's streaming readers buffer ahead — so "how many bytes did the source
hand over" does not answer it. Both implementations walk the frame structure
themselves: magic, descriptor, block headers by length, optional checksum. The
walk is checked directly against frames this codec did not produce (levels 1, 3
and 19, with and without a declared content size, over raw, compressed and
run-length blocks), because everything else in the decoder rests on it.

**The `st_dev` check makes invariant 7 enforceable rather than advisory.** §5.3
gates deletion on a declared durability class, which is configuration and can
therefore be declared wrongly. Both commands additionally refuse
`ARCHIVE_DURABILITY=independent` when the archive root and the spool resolve
to the same filesystem: a second copy that dies with the first is not a
durability domain whatever the flag says. The reaper *library* has no such check,
per §5.3's allowance for tests against a temporary backend — which is where the
deletion path is actually proven.

## Review disposition (2026-07-31)

Five findings, all reproduced against the merged code before being fixed and all
now permanent regression tests.

| # | Finding | Disposition |
|---|---|---|
| P1 | A failed directory fsync could later be read as a successful commit | **Fixed, both halves.** An idempotent `put_immutable` now fsyncs the directory before returning — a key can exist *because* the sync that would have made it durable failed, and accepting it is a durability claim the failed attempt never established. `write_json_durable` takes back a marker it could not sync, and `confirm_durable` re-establishes durability wherever a run promotes an existing marker to a commit (the archiver's skip path, and the reaper before deleting the bytes the receipt describes). |
| P1 | A failed decode left a complete-looking file under the destination name | **Fixed.** `decode_archived_segment` stages to a unique `.open`, fsyncs, renames and fsyncs the directory, removing the staged file on every failure. The logical identity is only known at the end of the decode, so writing straight to the destination published exactly the "partially verified records exposed as trusted evidence" §3.5 forbids. A caller passing its own writer still owns that guarantee, and the docstring says so. |
| P1 | The Python codec ignored short writes | **Fixed.** `write` returning less than it was offered is documented behaviour, and an unbuffered file does it under pressure. Both paths now write in full and count only what reached the sink; a sink accepting zero bytes is an error rather than a spin. The encoder had reported 29 stored bytes for 28 on disk — an identity describing bytes that are not there is the worst failure available here, because it verifies against itself and fails later against the object. |
| P2 | A receipt could claim a level the archiver did not use | **Fixed by removing the choice.** `Archiver` no longer takes `level`, and both `encode_stream` implementations refuse anything but level 3. V1 receipts state `"level": 3` and readers check it, so a configurable level could only ever produce receipts describing frames that were not written. Changing the level is a format change and belongs in a new receipt version. |
| P2 | The hourly sweep was specified but not deployed | **Fixed.** The `archiver` service is long-lived and passes `--interval-seconds ${ARCHIVER_INTERVAL_SECONDS:-3600}`; `archiver-once` keeps the one-shot form for an operator or an external scheduler. Both call the same sweep. Documented with it: a *restarting* archiver means an immutable-key conflict, not a busy spool. |

The first three share a shape worth naming: each was a place where a failure had
been reported honestly and then quietly promoted to success by a later, more
trusting reader. A commit marker's durability, a destination filename, and a
byte count are all claims — and each has to be established by whoever is about
to rely on it, not inherited from an attempt that did not finish.

## Decisions taken where the specification left room

- **Two receipt kinds are chosen by the backend, never by the caller.** A store
  declaring `local_conformance` writes `.archive.local.json`; only an explicitly
  independent one writes a production receipt (version 2 for new receipts). A
  caller that *could* choose would eventually choose wrongly, and §5.3's whole
  concern is a conformance artifact being read as deletion authority. The local receipt also
  carries `authorizes_deletion: false` and is refused outright if renamed to the
  production filename.
- **The reaper discovers receipts, not segments.** A partially reaped segment has
  no `.ndjson` left to find and still needs its seal removed, and a retained
  receipt is what makes a completed deletion auditable. §7.2's crash window is
  therefore an ordinary state the sweep resumes from, after re-proving
  everything from the top.
- **The archiver compares lengths on retry; the reaper rehashes in full.** The
  archiver runs hourly over every retained segment, and rehashing all of them
  each hour is the unbounded scan this pipeline refuses elsewhere. The reaper
  runs once per segment, immediately before the bytes stop existing.
- **The derivative is deleted on the archive receipt alone** (§7.2), before the
  canonical half of the gate is consulted — it costs a recompression, and the
  segment it derives from is still on disk.
- **`LocalObjectStore` publishes with `os.link`, not `os.replace`.** Replace
  would silently overwrite a key holding different content, and the absence
  check before it is a time-of-check race. Link fails with `EEXIST`, so
  immutability is enforced by the filesystem rather than by our own timing.

## Measured

`scripts/archive_probe.py` against `replay/tests/fixtures/polymarket_sports_20260730.ndjson`
(164 records, 106,274 bytes, sha256 `be95a9d3…feb603`):

```text
compressed          7,109 bytes      14.9x, 6.7% of original
decoded             byte-identical to the source, identities equal to the seal
decode ceiling      aborted at 53,137 bytes; the sink never passed it
26.0 MB segment     archived and decoded with 0 bytes of peak RSS growth
```

The ratio is a single reference feed, not a capacity claim. Kalshi ladder
traffic at the §4 sizing rate is the number that matters and has not been
measured on Linux.

## Regression gates, all passing

```text
python -m unittest discover -s tests           390 tests  (282 before this phase)
python -m unittest discover -s replay/tests     34 tests
cargo test --manifest-path ingester/Cargo.toml --workspace   125 tests, unchanged
cargo test --manifest-path encoder/rust/Cargo.toml            13 tests
docker compose config --quiet
```

The 117 new Python tests are §9.1–§9.4, the deployment gate, and the review
findings above: 25 codec, 5 absence gates, 17 object store, 37 archiver and
manifest, 24 reaper, 9 command. The capture image was rebuilt and both commands
were run inside it.

## Not done, deliberately

- No object-store lifecycle expiry. S3 and GCS adapters now implement the same
  identity, conflict, retry, receipt-last, and no-delete boundary.
- `canonical/` remains uncompressed until Zstd Step 2, so `spool/` is bounded by
  Phase 4 only once a genuinely separate backend is configured; on the local
  conformance backend the bytes have changed representation and directory and
  nothing more.
- No object-store lifecycle expiry, no correction datasets for late segments, no
  phase 5 daily replay.
- The Rust encoder crate stays outside the ingester workspace until the
  finalizer consumes it (§4.5).
