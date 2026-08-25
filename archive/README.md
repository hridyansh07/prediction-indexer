# Capture archive and reaper

This subsystem copies receipt-committed sealed capture segments and finalized
canonical windows to immutable object storage. It verifies them, publishes local
archive receipts and derived raw daily manifests, then separately audits whether
local raw data may be removed. The archiver never deletes capture evidence. The
reaper requires both a currently verified raw archive receipt/object pair and a
committed canonical-ingestion receipt.

## Layout and flow

```text
archiver/  service.py, canonical.py, manifest.py, publish.py, cli.py
           raw seal -> objects + receipt + manifest; canonical receipt -> objects + receipt
reaper/    service.py, cli.py                dual-receipt raw audit/deletion decision
           canonical.py, canonical_cli.py    18-hour canonical frame reaper
storage/   base.py, factory.py      provider-neutral contract and configuration
           local.py, s3.py, gcs.py  provider adapters
common/    durable.py, receipts.py, seal.py, verify.py
stream.py  verified archive objects -> replay ByteStreamer boundary
```

Input must be a final `.ndjson` segment with its valid `.seal.json` commit
marker; open or unsealed data is ineligible. Install the project package and
Python dependencies first. S3 and GCS dependencies are included by the project
install: `.venv/bin/python -m pip install -e .`.

## Production S3 prerequisites

Use a dedicated, private, general-purpose AWS S3 bucket, preferably in the same
region as capture. Enable versioning, Block Public Access, default SSE-S3,
TLS-only access, and a bucket policy requiring `If-None-Match: *` for writes.
Do not configure lifecycle expiry initially. Configure and verify the 12-digit
bucket owner account ID; every request sends `ExpectedBucketOwner`.

The runtime role needs exactly:

```text
s3:ListBucket                 on arn:aws:s3:::BUCKET
s3:GetObject
s3:PutObject                 on BUCKET/raw/*, BUCKET/canonical/*, BUCKET/targeter-v2/*
```

Do **not** grant `s3:DeleteObject`, `s3:DeleteObjectVersion`, bucket-policy,
lifecycle, ACL, encryption-admin, or other bucket-administration permissions.
Use boto3's standard credential provider chain; an EC2 instance role (or other
workload role) is preferred. Containers need network access to EC2 IMDS when an
instance role supplies credentials—do not bake static credentials into images.

Copy variable *names/defaults* from [`archive/.env.example`](.env.example) into
your deployment configuration. Do not put credentials there. The repository-root
`.env` is what Compose consumes at runtime; the archive-local file is reference
documentation only.

Preflight and build:

```bash
docker compose config --quiet
docker compose build archiver
```

One-shot archive through Compose:

```bash
docker compose --profile ops run --rm archiver-once
```

Direct project-root invocation is:

```bash
.venv/bin/python -m archive.archiver.cli --spool-root data/spool \
  --canonical-root data/canonical \
  --archive-backend s3 --s3-bucket "$ARCHIVE_S3_BUCKET" \
  --s3-region "$ARCHIVE_S3_REGION" --s3-expected-owner "$ARCHIVE_S3_EXPECTED_OWNER"
```

## Production GCS prerequisites

GCS does not provide server-side SHA-256. The native adapter calculates SHA-256
while streaming a CRC32C-checked upload and attaches that identity to the
object. Receipt verification then streams the exact generation once to prove
the metadata against its bytes. See
[`GCS_RAW_ARCHIVE_ADAPTER_V1.md`](GCS_RAW_ARCHIVE_ADAPTER_V1.md)
for the complete integrity, IAM, bucket, and rollout contract.

On GCE, use a dedicated attached service account with only
`storage.objects.create`, `storage.objects.get`, and `storage.objects.list` on
the archive bucket. Do not grant object deletion. The client uses Application
Default Credentials; no GCP credential belongs in `.env`.

Direct project-root invocation is:

```bash
.venv/bin/python -m archive.archiver.cli --spool-root data/spool \
  --canonical-root data/canonical \
  --archive-backend gcs --gcs-bucket "$ARCHIVE_GCS_BUCKET"
```

Objects are stored as
`raw/lane=<lane>/date=<date>/<segment>.{ndjson.zst,seal.json}`. Beside each local
segment, `.archive.json` is production verification authority; local conformance
uses `.archive.local.json`, which authorizes nothing. Daily manifests are derived
catalogs and can be rebuilt from verified receipts.

Canonical windows are already encoded by the Rust finalizer. The archiver does
not recompress them: it strictly decodes both frames against `receipt.json`, then
immutably publishes `evidence.ndjson.zst`, `provenance.ndjson.zst`, and the exact
receipt under `canonical/date=<date>/window=<start>/`. It writes
`canonical_archive_receipt.json` beside the local window only after fresh heads
verify all three objects. A local backend instead writes the non-authoritative
`canonical_archive_receipt.local.json`.

The separate canonical reaper bounds this local canonical storage. It is audit
only by default and requires a production receipt, an independent backend,
fresh heads for all three archive objects, unchanged local identities, an age of
at least 18 hours by every recorded clock, and explicit delete mode. It removes
only the two `.ndjson.zst` frames. Both small receipts and the window directory
remain as the finalizer's restart/watermark tombstone. Run one audit sweep with:

```bash
docker compose --profile ops run --rm canonical-reaper-once
```

After the selected cloud-backend rollout has soaked and audit reports the
expected reapable count,
set `CANONICAL_REAPER_MODE=delete`; the 18-hour floor may be raised but not
lowered.

## Streaming archived segments to replay

Archive retrieval uses one provider-neutral verified stream. The selected store
adapter checks receipt SHA-256, length, provider checksum, content metadata, and
immutable generation/version while downloading each selected object once.
Plain objects and strict single-frame Zstandard decodes stay private until the
complete stored and logical identities pass, then implement replay's structural
`ByteStreamer` contract (`object_keys`, `iter_bytes`).

`ArchivedSegmentByteStreamer` exposes raw NDJSON and seals.
`ArchivedCanonicalByteStreamer` exposes decoded canonical evidence and
provenance plus exact `receipt.json`. `ArchivedTargeterRunByteStreamer` exposes
every receipted Targeter run artifact, while `ArchivedTargetRecordByteStreamer`
retains the capture-window-specific target-record view. Replay remains unaware
of S3, GCS, receipts, and Zstandard.

The caller supplies receipts because they are the archive commit markers. Cloud
prefix listings and daily manifests are not accepted as substitutes. Storage
prefixes are removed from logical keys, so for example
`raw/lane=kalshi/date=2026-07-30/a.ndjson.zst` appears to replay as
`lane=kalshi/date=2026-07-30/a.ndjson`.

## Reaper safety and rollout

Run `python -m archive.reaper.cli` and
`python -m archive.reaper.canonical_cli` in `audit` mode (the default). They
re-verify their archived objects and receipts before reporting eligibility.
Archive and audit for at least 24 hours, inspect every retention reason, retain
local raw, and keep lifecycle expiry disabled. Do not enable `REAPER_MODE=delete`
until a separate, explicit rollout enables destructive operation.

The local backend is for conformance and development. On the capture filesystem
it is not an independent durability domain and cannot authorize deletion; even a
separate local device lacks S3's service-side conditional-write and checksum
controls.

## Adding a backend

1. Implement `storage.base.ObjectStore` in a new adapter using immutable,
   conditional publication and bounded streaming reads.
2. Return SHA-256 verified from actual provider bytes, separate provider
   checksum evidence, and an explicit durability declaration.
3. Preserve key normalization, identity checks, errors, and no-delete behavior.
4. Add one import/selection branch in `storage/factory.py` and export the adapter
   intentionally from `storage/__init__.py`.
5. Run the shared object-store tests plus archiver, verifier, manifest, reaper,
   crash-boundary, and deployment tests.

Normative detail: [raw archive/reaper spec](PHASE_4_RAW_ARCHIVE_REAPER_V1.md),
[S3 adapter spec](S3_RAW_ARCHIVE_ADAPTER_V1.md),
[GCS adapter spec](GCS_RAW_ARCHIVE_ADAPTER_V1.md), and
[deployment guide](../docs/DEPLOYMENT.md).
