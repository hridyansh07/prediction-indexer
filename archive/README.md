# Capture archive and reaper

Copies receipt-committed sealed capture segments and finalized canonical windows
to immutable object storage, verifies them, publishes local archive receipts and
derived daily manifests, and separately decides whether local data may be removed.
The archiver never deletes anything. Receipt, key and manifest formats are in
[`FORMATS.md`](FORMATS.md).

```text
archiver/  service.py (raw)  canonical.py  manifest.py  publish.py  cli.py
reaper/    service.py + cli.py (raw, dual-receipt)   canonical.py + canonical_cli.py
storage/   base.py (contract)  factory.py  local.py  gcs.py  verification.py
common/    durable.py  receipts.py  seal.py  verify.py
stream.py  canonical_stream.py  retrieval.py  canonical_restore.py   verified read paths
```

`archive/` imports nothing from `targeter/`; Targeter v2 run archiving reuses this
package's object-store protocol, store factory and durable filesystem primitives
from `targeter/v2/`. A test asserts the one-way dependency.

## Commands

All read configuration from the process environment (they do not parse `.env`;
Compose passes it). Install with `.venv/bin/python -m pip install -e .`.

```bash
# Archive sealed segments (and, with --canonical-root, canonical windows); one sweep,
# or repeat every N seconds
.venv/bin/python -m archive.archiver.cli --spool-root data/spool \
    [--canonical-root data/canonical] [--manifest-root PATH] \
    [--interval-seconds 3600] [--report PATH]

# Raw reaper: audit by default
.venv/bin/python -m archive.reaper.cli --spool-root data/spool --canonical-root data/canonical \
    [--mode audit|delete] [--interval-seconds N] [--report PATH]

# Canonical reaper: audit by default, 18 hour floor
.venv/bin/python -m archive.reaper.canonical_cli --canonical-root data/canonical \
    [--mode audit|delete] [--retention-hours 18] [--interval-seconds N] [--report PATH]
```

Compose services (profile `ops`): `archiver`, `archiver-once`, `reaper`,
`reaper-once`, `canonical-reaper`, `canonical-reaper-once`; for example
`docker compose --profile ops run --rm archiver-once`. Compose reads
`ARCHIVER_INTERVAL_SECONDS`, `REAPER_MODE`, `REAPER_INTERVAL_SECONDS`,
`CANONICAL_REAPER_MODE`, `CANONICAL_REAPER_RETENTION_HOURS` and
`CANONICAL_REAPER_INTERVAL_SECONDS` (see `archive/.env.example`, reference only).

Archiver exit codes: 0 ok, 1 some segments failed, 2 an immutable-key conflict or
halted sweep (never retried in watch mode; repeating would bury the alert). The raw
and canonical reaper exit 1 when a receipt or object failed to verify or an I/O
error occurred; ordinary backlog (no canonical receipt yet) is not a fault.

### Backend selection (`storage/factory.py`)

| `ARCHIVE_BACKEND` | Required | Durability |
|---|---|---|
| `local` (default) | `ARCHIVE_ROOT`; optional `ARCHIVE_STORE_ID`, `ARCHIVE_DURABILITY` | `conformance` (default) cannot authorize deletion. `independent` is refused when the archive root shares a filesystem (`st_dev`) with the spool or canonical root |
| `gcs` | `ARCHIVE_GCS_BUCKET` | independent |

Options for another backend are rejected, never guessed past. The same variables
configure the raw, canonical and Targeter v2 archive consumers.

## Safety model

1. The seal is a claim, not a substitute for reading the source: the archiver
   recomputes logical SHA-256, byte length and LF count while streaming through the
   encoder and compares all three with the seal.
2. The local archive receipt is the archive commit marker. A compressed file, an
   object key, or a successful upload call is not.
3. Archival never deletes. Raw deletion requires two independent proofs (an
   archive receipt and a canonical receipt) plus an independent backend.
4. Every immutable-key conflict fails closed; existing different content is never
   overwritten.
5. No full-file buffering anywhere (codec, upload, verification, retrieval).
6. Derived artifacts (local `.ndjson.zst`, daily manifests) are rebuildable; seals,
   receipts and verified remote objects are the authorities.
7. A local backend on the capture filesystem is not a durability domain whatever a
   flag claims.

Segment states are inferred from proofs, not filenames: open, sealed, materialized
(a local `.ndjson.zst`; never equivalent to archived), archived (valid receipt plus
verified remote data and seal), canonicalized (a canonical `receipt.json` lists the
lane and source SHA-256 in `inputs`), reapable (archived and canonicalized and an
independent backend), reaped. A late, invalid or excluded segment may stay
`archived` forever; the reaper reports it and does not guess it into a window.

## Raw archiver

Eligible input is `<segment>.ndjson` plus a valid `<segment>.seal.json` (validated
by `common/seal.py`, a mirror of the Rust segment crate, including path coherence).
`.open` files, renamed-but-unsealed files and unreadable or digest-invalid seals
are ineligible; malformed seals are reported as integrity faults, not pending work.
The unit is one segment; the sweep never concatenates segments.

Per segment, in order: validate the seal; stream the source through the Zstandard
encoder into `<segment>.ndjson.zst.open` while computing both identities; require
the logical identity to equal the seal; fsync, rename to `.ndjson.zst`, fsync the
directory; publish the data object and the unchanged seal through
`put_immutable`; `verify_metadata` both against full expectations; write
`<segment>.archive.json.open`, fsync, rename, fsync the directory. The receipt is
last. Any earlier failure leaves no receipt and the raw segment untouched; an
unreceipted `.ndjson.zst` is untrusted and is revalidated or rebuilt, never
uploaded because the filename exists. Retry of identical content is idempotent. A
valid existing receipt is skipped only after its schema, local source identity (by
length) and remote objects have been re-checked. A key holding different content
raises an integrity conflict and halts the sweep.

Objects: `raw/lane=<lane>/date=<YYYY-MM-DD>/<segment>.ndjson.zst`
(`Content-Type: application/x-ndjson`, `Content-Encoding: zstd`) and `.seal.json`.
The backend declares whether it writes a production receipt (`.archive.json`) or
a conformance receipt (`.archive.local.json`, `authorizes_deletion: false`, ignored
by manifests and the reaper, refused if renamed to the production name). The caller
cannot choose.

With `--manifest-root`, `date=<YYYY-MM-DD>/manifest.json` is regenerated from
revalidated receipts: a deterministic catalog, never a commit boundary, safe to
delete and rebuild. A stale manifest whose receipts no longer verify is removed.

## Canonical archiver

With `--canonical-root`, each receipt-committed window is strictly decoded against
`receipt.json` (stored and logical identities, decoded-byte ceilings, one frame).
It is not recompressed. The archiver then uploads, in order,
`canonical/date=<d>/window=<start_ns>/evidence.ndjson.zst`, `provenance.ndjson.zst`
and the exact `receipt.json`, verifies fresh metadata for all three, and only then
writes `canonical_archive_receipt.json` beside the window (local backends write the
non-authoritative `canonical_archive_receipt.local.json`). A failure leaves no
receipt and never modifies the window.

## Raw reaper (`archive.reaper.cli`)

Audit by default. Delete requires `--mode delete` (or the `--delete` alias, not
combinable with `--mode delete`) and an independent backend. At decision time it
re-establishes all of:

1. a structurally valid archive receipt;
2. archive data and seal objects that still match it (`verify_metadata`);
3. an independent durability domain;
4. a structurally valid committed canonical `receipt.json`;
5. a canonical `inputs` entry matching the receipt's lane, source SHA-256,
   `data_file` and segment index;
6. the local source and seal still matching the receipt (rehashed in full).

The reaper discovers receipts, not segments, so a half-finished deletion (source
gone, seal remaining) is an ordinary state it finishes after re-proving everything.
The local `.ndjson.zst` derivative may be removed on the archive receipt alone.
Archive receipts are never deleted. Each decision reports lane, source file, SHA-256,
receipt paths, decision, reason and `verified_at_ns`. Retained reasons:
`archive_receipt_invalid`, `archive_object_unverified`, `canonical_receipt_missing`,
`canonical_segment_mismatch`, `local_source_changed`, `durability_gate`, `io_error`,
`audit_mode`. Only one receipt, a malformed or unverifiable receipt, a missing or
mismatched object, a never-canonicalized segment, or a conformance backend all mean
retain. Deletion fsyncs the directory after each unlink.

## Canonical reaper (`archive.reaper.canonical_cli`)

Audit by default; refuses a retention floor below 18 hours and delete mode against a
non-independent backend. For a window it requires: a strict production
`canonical_archive_receipt.json` matching the configured store; age of at least the
retention floor measured from the latest of window end, finalization time, archive
verification time and the mtimes of both receipts; exact agreement between the local
receipt, both output identities and the archive receipt; fresh metadata verification
of all three archive objects; and explicit delete mode. It removes only
`evidence.ndjson.zst` then `provenance.ndjson.zst` with a directory fsync after each
unlink. `receipt.json`, the archive receipt and the window directory remain as the
finalizer's tombstone (see `ingester/FORMATS.md`). A crash after the first unlink is
a recognised partial state resumed only after the gates are re-established.

## Verified retrieval

`stream.py` and `canonical_stream.py` expose archived data through replay's
structural `ByteStreamer` contract (`object_keys`, `iter_bytes`):
`ArchivedSegmentByteStreamer` (raw NDJSON and seals) and
`ArchivedCanonicalByteStreamer` (decoded evidence, provenance, exact `receipt.json`).
Targeter v2 run artifacts have equivalent streamers in `targeter/v2/replay_stream.py`.
They stage each object once through one verified stream (stored SHA-256, length,
provider checksum, content metadata and immutable generation checked while
downloading), decode single-frame Zstandard against the receipt with the seal's
`byte_length` as hard ceiling, and expose bytes only after the complete identities
pass; leaving the stream before EOF is a verification failure. A failed decode
cannot leave a complete-looking file under the destination name. The caller
supplies receipts (the commit markers); prefix listings and manifests are not
accepted as substitutes. Storage prefixes are stripped, so
`raw/lane=kalshi/date=D/a.ndjson.zst` appears to replay as
`lane=kalshi/date=D/a.ndjson`.

`canonical_restore.py` (`preflight_canonical_window`, `restore_canonical_window`)
verifies the remote `receipt.json` then restores a window under an owned root, frames
first and `receipt.json` last, refusing a destination that is already committed.

## Object-store contract (`storage/base.py`)

```text
put_immutable(key, reader, identity, content metadata) -> ObjectMetadata
head(key)                                               -> ObjectMetadata | None
verify_metadata(expectation)                            -> ObjectMetadata
verify(expectation)                                     -> None   (complete bytes)
open(key, max_bytes)                                    -> bounded reader
open_verified(expectation)                              -> verified reader
list_keys(prefix)                                       -> key iterator
```

Keys are normalized relative POSIX paths (no empty components, `.`, `..`, absolute
paths, backslashes or traversal). The protocol has no overwrite, move or delete.
`ObjectMetadata` carries key, length, normalized SHA-256, separate provider
checksum and algorithm, content type and content encoding. `verify_metadata` is the
normal archive and reaper proof; `verify` is an explicit deep audit; retrieval uses
`open_verified`. `head` and `list_keys` are discovery, not authority. Errors:
`ObjectStoreError`, `VerificationFailure`, `IntegrityConflict`, `ObjectKeyError`.

Immutable put: an absent key is streamed to a unique temporary and published
atomically or conditionally; an existing key with the expected identity succeeds
without rewriting (after re-establishing durability); a different identity raises
`IntegrityConflict` and changes nothing.

**`LocalObjectStore`** writes `unique .open -> fsync -> link -> fsync directory` and
publishes with `os.link` (fails with `EEXIST`; `os.replace` would overwrite). It
reopens and rehashes for `head`. It is for tests, development and compression
probes.

**`GCSObjectStore`**: GCS has no server-side SHA-256. Each create is a resumable
upload with `ifGenerationMatch=0` in 1 MiB chunks, checking the expected SHA-256 and
length while the client validates CRC32C; the SHA-256 and length are attached as
object metadata. `head`/`verify_metadata` compare current generation and
metageneration, GCS length and CRC32C, custom SHA-256 and length, content type and
encoding without downloading the body. A conditional-create conflict retains one
generation-pinned full readback before an existing key is accepted as an interrupted
retry. Retrieval pins the generation and rechecks generation/metageneration at EOF.
CRC32C plus the bound SHA-256 protects against corruption, truncation and
wrong-object selection; it does not claim SHA-256 resistance against a deliberately
constructed CRC32C collision. The adapter never calls an object delete API.

Credentials come from Application Default Credentials (an attached service account
on Compute Engine, or workload identity federation); never put credentials in `.env`,
the repository or an image. The runtime principal needs only
`storage.objects.create`, `storage.objects.get` and `storage.objects.list` on the
archive bucket (Storage Object Creator plus Viewer): no delete, update, bucket,
IAM or retention-policy administration. Use a private, dedicated bucket with uniform
bucket-level access and public access prevention; versioning is recommended; do not
enable lifecycle expiry before the archive has soaked.

## Rolling out reaping

Starting capture or archival does not authorize reaping. Run both reapers in audit
mode, confirm production receipts report provider `gcs` and `CRC32C`, run the
integrity audit, sample lanes by strict decode against retained local evidence, and
soak for at least 24 hours with local raw retained before a separate explicit
decision to set `REAPER_MODE=delete` / `CANONICAL_REAPER_MODE=delete`. The local
backend cannot authorize deletion. See `docs/DEPLOYMENT.md`.

## Adding a backend

1. Implement `storage.base.ObjectStore` with immutable conditional publication and
   bounded streaming reads.
2. Return the SHA-256, separate provider checksum evidence and a durability class
   from `head`; implement receipt comparison in `verify_metadata` and full-byte
   verification in `verify`/`open_verified`.
3. Preserve key normalization, identity checks, error types and the no-delete
   surface.
4. Add one branch in `storage/factory.py` and export it from `storage/__init__.py`.
5. Run the shared object-store tests plus archiver, verifier, manifest, reaper,
   crash-boundary and deployment tests.

**S3.** A complete `S3ObjectStore` (boto3, conditional `If-None-Match` puts,
SHA-256 checksums, owner checks) was removed while unused. To bring it back,
restore it from commit `6411ab3`:

```bash
git checkout 6411ab3 -- archive/storage/s3.py tests/test_s3store.py \
  tests/test_s3_pipeline.py archive/S3_RAW_ARCHIVE_ADAPTER_V1.md
```

Then do steps 4–5, re-add `boto3` to `pyproject.toml`, and re-add the
`ARCHIVE_S3_*` variables to the compose files and `.env.example`. Receipts
still carry the `s3_checksum_sha256` field.

## Tests

```bash
.venv/bin/python -m unittest tests.test_archiver tests.test_canonical_archiver \
    tests.test_reaper tests.test_canonical_reaper tests.test_archive_commands \
    tests.test_archive_stream tests.test_canonical_restore tests.test_gcsstore
.venv/bin/python scripts/archive_probe.py   # archive one real segment through the local store
```
