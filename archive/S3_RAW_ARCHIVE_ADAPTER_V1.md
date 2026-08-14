# S3 Raw Archive Adapter V1

Status: **implemented**. This document specifies the smallest production
S3 adapter for the Phase 4 raw archiver and reaper.

This specification is subordinate to
[`PHASE_4_RAW_ARCHIVE_REAPER_V1.md`](PHASE_4_RAW_ARCHIVE_REAPER_V1.md) and
[`ZSTD_MATERIALIZATION_PIPELINE_V1.md`](../encoder/ZSTD_MATERIALIZATION_PIPELINE_V1.md).
Where this document is more specific about AWS S3 calls, this document controls.

## 1. Goal

Replace `LocalObjectStore` with an AWS S3 implementation of the existing
`ObjectStore` protocol without changing the archiver, receipt, manifest, replay,
or reaper data model.

After this phase:

```text
sealed segment
  -> local .ndjson.zst derivative
  -> immutable S3 data object + unchanged seal object
  -> local production archive receipt
  -> reaper audit
```

The adapter is complete when a successful `put_immutable` proves all of these:

1. the key was not overwritten;
2. S3 accepted the exact expected byte count;
3. S3 verified the expected full-object SHA-256;
4. a fresh `HeadObject` returns the same byte count and full-object SHA-256;
5. the object metadata is the metadata the archiver requested.

The local production archive receipt remains the archive commit marker. An S3
response, S3 key, or ETag is not a commit marker.

The Event Universe addendum may publish an immutable
`.archive-receipt-mirror.json` wrapper after that local receipt commits. The
wrapper preserves the exact receipt bytes for a separately deployed derivative
worker, but declares itself non-authoritative and unable to authorize deletion.
It does not change this adapter's raw commit marker or reaper contract.

## 2. Non-goals

V1 does **not** add:

- canonical-window upload;
- raw-object expiry or any S3 delete path;
- multipart upload;
- S3-compatible providers such as MinIO or R2;
- cross-region replication;
- Object Lock;
- automatic destructive reaper scheduling;
- replay orchestration;
- a new receipt version or a legacy reader.

The adapter targets an AWS S3 **general purpose bucket**. Directory buckets are
out of scope because the conditional-write contract is different.

## 3. Fixed V1 decisions

These choices are intentionally fixed so an implementer does not have to invent
policy while writing the adapter.

| Question | V1 decision |
|---|---|
| Archive unit | one sealed segment per S3 data object, plus its seal object |
| Bucket layout | two archive prefixes: `raw/lane=<lane>/date=<date>/...` for sealed capture and `targeter-v2/runs/date=<date>/run=<run_id>/...` for target runs |
| Bucket sharing | one dedicated archive bucket per deployment |
| Immutability | every upload uses `IfNoneMatch="*"` |
| Integrity | explicit full-object SHA-256; ETag is ignored |
| Upload form | one `PutObject`, no multipart |
| Maximum object | reject a stored object above the single-PUT 5 GB limit |
| Encryption | use the bucket's default encryption; start with SSE-S3 |
| Credentials | the standard boto3 credential provider chain |
| S3 durability class | always `INDEPENDENT` and production-receipt capable |
| Lifecycle expiry | disabled until replay from S3 has been proven |
| Reaper | audit only during rollout; `--delete` remains an explicit later act |

Define the conservative boundary as
`MAX_SINGLE_PUT_BYTES = 5_000_000_000`. The check applies to the compressed
`.ndjson.zst` and to the seal object. Crossing it is a hard failure: write no
receipt and leave the local raw segment untouched. Multipart upload can be a
later version if real 30-minute segments approach this limit.

## 4. Phase 4 prerequisites

The S3 adapter must not be called production-ready until the following Phase 4
review findings have regression tests and fixes. They are not reasons to change
the S3 object format.

1. Archive receipt `source.file` and `seal.file` must be basenames, must contain
   no `/` or `\`, and must share one segment stem. The reaper must never resolve
   an absolute or traversing receipt path outside its discovered date directory.
2. `verify_archive` must require `receipt.location == store.store_id`. A receipt
   for one bucket must not verify against a differently configured bucket even
   if the same keys and bytes happen to exist there.
3. A daily-manifest rebuild with zero valid receipts for a previously written
   date must remove or replace the stale manifest durably. Leaving the old file
   advertises objects the rebuild just excluded.
4. `build_daily_manifests` must classify `ObjectStoreError` as an exclusion and
   continue. A transient S3 `HeadObject` failure must not terminate the
   long-lived archiver after its segment receipts were already committed.

These are Gate 0 tests. Implement them before the S3 adapter tests.

## 5. Existing protocol and new class

Create `archive/storage/s3.py` with:

```python
class S3ObjectStore:
    store_id: str                 # exactly the bucket name
    durability = INDEPENDENT

    def put_immutable(
        self,
        key: str,
        reader: BinaryIO,
        expected_identity: StoredIdentity,
        *,
        content_type: str | None = None,
        content_encoding: str | None = None,
    ) -> ObjectMetadata: ...

    def head(self, key: str) -> ObjectMetadata | None: ...

    def open(self, key: str, *, max_bytes: int | None = None) -> BinaryIO: ...

    def list_keys(self, prefix: str) -> Iterator[str]: ...
```

Do not add S3 methods to `Archiver` or `Reaper`. They continue to depend only on
`ObjectStore`. `list_keys` is the later Event Universe discovery extension: it
uses paginated `ListObjectsV2`, but a listed key is not a commit marker and must
still be verified through its owning receipt or manifest.

Constructor inputs:

```python
S3ObjectStore(
    bucket: str,
    region: str,
    expected_bucket_owner: str,
    client=None,                 # dependency injection for tests only
)
```

Rules:

- `bucket`, `region`, and the 12-digit `expected_bucket_owner` are required.
- `store_id` is exactly `bucket`, because the current production receipt field
  is named `bucket` and `Archiver` fills it from `store.store_id`.
- Every S3 request includes `ExpectedBucketOwner`.
- The default client is `boto3.client("s3", region_name=region)`.
- Supplying `client` is only for unit tests; production configuration must not
  expose an arbitrary endpoint URL in V1.
- Add `boto3>=1.43,<2`; its `PutObject` model exposes `IfNoneMatch`, and the upper
  bound prevents an unreviewed major-version change.

## 6. Identity translation

The repository stores SHA-256 as lowercase hexadecimal. S3 represents
`ChecksumSHA256` as base64 of the 32 digest bytes.

```text
StoredIdentity.sha256       lowercase 64-character hex
S3 ChecksumSHA256           base64(raw 32-byte SHA-256)
ObjectMetadata.sha256       decoded back to lowercase hex
ObjectMetadata.provider_checksum
                            the unchanged S3 base64 value
ObjectMetadata.provider_checksum_algorithm
                            exactly "SHA256"
```

Use the existing `provider_checksum_of` helper for hex-to-base64. Add one strict
inverse helper for base64-to-hex. The inverse must reject malformed base64 and
decoded values that are not exactly 32 bytes.

Never derive identity from:

- `ETag`;
- object name or suffix;
- caller-provided user metadata;
- a cached result from the upload request.

## 7. `head` algorithm

`head(key)` is the only way the archiver, manifest builder, and reaper ask S3
what currently exists.

Perform exactly this operation:

```python
client.head_object(
    Bucket=bucket,
    Key=normalize_key(key),
    ChecksumMode="ENABLED",
    ExpectedBucketOwner=expected_bucket_owner,
)
```

Interpret the response as follows:

1. `404` or `NoSuchKey` returns `None`.
2. `403` is an `ObjectStoreError`, never absence.
3. Any other client, network, or service error is an `ObjectStoreError`.
4. `ContentLength` must be a non-negative integer.
5. `ChecksumSHA256` is required. Missing or malformed checksum metadata is a
   `VerificationFailure`.
6. `ChecksumType`, when present, must be `FULL_OBJECT`. `COMPOSITE` is refused.
7. Return `ContentType` and `ContentEncoding` without inventing defaults.

The role receives `s3:ListBucket` on the dedicated bucket so S3 can return 404
for a missing key. Without it, S3 may return 403, which the adapter correctly
treats as a configuration or authorization failure. Object read and write
permissions remain restricted to the archive prefixes of §12 — `raw/*` and
`targeter-v2/*` — and to nothing else in the bucket.

## 8. `put_immutable` algorithm

The implementation must follow this order. Do not perform a normal
unconditional `PutObject`, even after a preceding `head` says the key is absent.

### 8.1 Validate locally

1. Normalize the key with the existing `normalize_key`.
2. Validate the expected hex digest and byte length.
3. Reject a byte length above the V1 single-PUT limit.
4. Require a seekable reader. Record its current position, seek to the end, and
   prove that the remaining byte length equals `expected_identity.byte_length`.
   Restore the initial position before upload.
5. Convert the expected SHA-256 from hex to base64.

The seek is an O(1) file operation. It does not read or buffer the object. The
archiver already passes regular files for both the compressed data and seal.

### 8.2 Attempt one conditional upload

Call `PutObject` with:

```python
{
    "Bucket": bucket,
    "Key": normalized_key,
    "Body": reader,
    "ContentLength": expected_identity.byte_length,
    "ChecksumAlgorithm": "SHA256",
    "ChecksumSHA256": expected_checksum_base64,
    "IfNoneMatch": "*",
    "ExpectedBucketOwner": expected_bucket_owner,
    # Include only when non-null:
    "ContentType": content_type,
    "ContentEncoding": content_encoding,
}
```

Do not use the high-level transfer manager in V1; it can silently choose
multipart upload. Do not load the reader into `bytes`.

### 8.3 Interpret the result

- Success: call `head(key)` and validate the fresh result as described below.
- `412 PreconditionFailed`: another object already owns the key. Call
  `head(key)` and compare it; do not upload unconditionally.
- `409 ConditionalRequestConflict`: raise `ObjectStoreError`. The hourly sweep
  retries from a newly opened reader. Do not retry a partially consumed reader
  inside this call.
- `BadDigest`, `InvalidDigest`, or an explicit checksum failure:
  `VerificationFailure`.
- Authentication, authorization, timeout, connection, throttling, and other S3
  failures: `ObjectStoreError`.

The archiver already maps `IntegrityConflict` to a fatal sweep stop and other
store errors to a per-segment failure that leaves the source and receipt
untouched.

### 8.4 Validate the fresh `HeadObject`

The object is accepted only when all requested properties match:

```text
byte_length       == expected_identity.byte_length
sha256            == expected_identity.sha256
provider checksum == base64(expected_identity.sha256)
checksum algorithm == SHA256
content type      == the requested content_type
content encoding  == the requested content_encoding
```

For a `412` response:

- every property matching means an idempotent success;
- any byte-identity or metadata difference is `IntegrityConflict`;
- absence after the `412` is `ObjectStoreError`, because concurrent state
  changed and the next sweep must retry from the beginning.

For a successful new upload, any mismatch is `VerificationFailure`.

## 9. `open` algorithm

`open` performs one `GetObject` with the normalized key and
`ExpectedBucketOwner`. It returns a context-manageable bounded streaming reader
around the response `Body`.

Rules:

- never return the whole object as `bytes`;
- closing the wrapper closes the S3 `StreamingBody`;
- negative `max_bytes` is rejected;
- if the response `ContentLength` already exceeds `max_bytes`, close it and
  raise `VerificationFailure` before exposing a byte;
- otherwise, refuse to return byte `max_bytes + 1`, using the same behavior as
  the local `BoundedReader`;
- 404 is `ObjectStoreError` here, because a caller asked to open a specific key;
- the codec still verifies the full stored SHA-256 while decoding.

`verify_archive` calls `head` before replay opens an object. The staged decode
path remains unchanged: a destination filename appears only after stored and
logical identities both verify.

## 10. Production verification tightening

For a production receipt, `verify_archive` must additionally require:

```text
receipt.location == store.store_id
data.provider_checksum_algorithm == "SHA256"
seal.provider_checksum_algorithm == "SHA256"
data.content_type == "application/x-ndjson"
data.content_encoding == "zstd"
seal.content_type == "application/json"
seal.content_encoding is null
```

Both S3 objects must therefore have a retrievable full-object SHA-256. The data
checksum is recorded verbatim in the receipt; the seal checksum is proved by
the `ObjectMetadata.sha256` returned from its S3 checksum.

Local conformance receipts may retain their current metadata tolerance, but the
shared immutable-put contract must test requested metadata on an idempotent
retry.

## 11. Shared configuration factory

Create one `archive/storage/factory.py`. Both `archive/archiver/cli.py` and
`archive/reaper/cli.py` must use it so the two commands cannot interpret backend
configuration differently.

CLI contract:

```text
--archive-backend local|s3             default: local

local only:
--archive-root PATH
--archive-durability conformance|independent
--store-id TEXT

s3 only:
--s3-bucket NAME
--s3-region REGION
--s3-expected-owner 12_DIGIT_ACCOUNT_ID
```

Validation:

- local preserves the current `st_dev` safety check;
- S3 requires all three S3 fields and always returns `INDEPENDENT`;
- `--archive-durability` cannot downgrade or upgrade S3;
- S3 `store_id` cannot be overridden;
- the static Compose command may still pass its local archive root and local
  store ID while S3 is selected; the factory ignores them for S3, and tests
  prove they do not affect the bucket or durability class;
- non-empty S3 options while `local` is selected fail at startup, rather than
  making an operator think two backends are active.

Compose and `.env.example` add:

```dotenv
ARCHIVE_BACKEND=local
ARCHIVE_S3_BUCKET=
ARCHIVE_S3_REGION=
ARCHIVE_S3_EXPECTED_OWNER=
```

Both the archiver and reaper receive the same values. Keep the local archive
bind mount for the local backend; it is unused by S3. Do not place AWS secret
keys in the repository, image, or `.env`.

Compose forwards `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and
`AWS_SESSION_TOKEN` from the invoking shell to `archiver`, `archiver-once`, and
`reaper` only. It must not expose them to targeters or venue splices. Host
shared-config files are not mounted into the containers. On AWS, prefer an
instance or task role; an EC2 deployment must make IMDS reachable from bridge
containers, including an adequate IMDSv2 response hop limit.

## 12. Bucket and IAM requirements

Use a dedicated, private general purpose bucket in the same region as the
capture host for V1.

### 12.1 Archive prefixes

One bucket holds two independent archive namespaces, written by different
commands with different cadences but under identical immutability rules:

```text
raw/lane=<lane>/date=<date>/<segment>            sealed capture segments and seals
targeter-v2/runs/date=<date>/run=<run_id>/<file> target-run artifacts and manifest
```

`raw/` is written by `archive.archiver.cli` (`archive/archiver/service.py:98`).
`targeter-v2/` is written by `targeter/v2/run_archive.py:245`, reached from a
scheduled `targeter/run_v2.py --mode publish|archive` and from
`targeter.v2.run_archiver_cli`.

**Every rule in this section applies to both prefixes.** They are two namespaces
rather than one because they have different producers, different retention
questions, and different reapers; they are in one bucket because they share a
durability domain and one bucket-owner identity. A policy written for `raw/`
alone leaves target-run archival unauthorized, and — worse than a plain
failure — leaves its writes unprotected by the conditional-write enforcement
that makes the archive immutable.

### 12.2 Bucket checklist

- all four S3 Block Public Access settings enabled;
- bucket versioning enabled as recovery defense;
- default encryption enabled, initially SSE-S3;
- bucket policy denies non-TLS requests;
- bucket policy requires `If-None-Match: *` for writes under **both** `raw/`
  and `targeter-v2/`;
- no expiration lifecycle rule on either prefix yet;
- no application permission to delete objects or object versions.

The conditional-write condition, applied to both prefixes:

```json
{
  "Sid": "RequireConditionalWritesOnArchivePrefixes",
  "Effect": "Deny",
  "Principal": "*",
  "Action": "s3:PutObject",
  "Resource": [
    "arn:aws:s3:::BUCKET/raw/*",
    "arn:aws:s3:::BUCKET/targeter-v2/*"
  ],
  "Condition": {
    "StringNotEquals": { "s3:if-none-match": "*" }
  }
}
```

### 12.3 Runtime role

Restricted to this bucket and these two prefixes:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ListForAccurate404",
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::BUCKET"
    },
    {
      "Sid": "ArchiveObjectReadWrite",
      "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:GetObject"],
      "Resource": [
        "arn:aws:s3:::BUCKET/raw/*",
        "arn:aws:s3:::BUCKET/targeter-v2/*"
      ]
    }
  ]
}
```

`s3:ListBucket` is granted on the bucket rather than per prefix so a missing key
returns 404 rather than 403, which §7 depends on to distinguish absence from an
authorization fault.

Do not grant `s3:DeleteObject`, `s3:DeleteObjectVersion`, ACL management, bucket
policy management, or lifecycle management to the capture service. Neither
reaper needs any of them: the raw reaper deletes local spool files, and the
Targeter v2 run reaper deletes local run artifacts. **No component in this
repository ever deletes an S3 object**, and withholding the permission is what
makes that a property of the deployment rather than a property of the code.

If the bucket later moves to SSE-KMS, add and test the KMS permissions required
for checksum-enabled `HeadObject` before changing production. V1 recommends
SSE-S3 because it keeps the first deployment's checksum path simple.

## 13. Implementation order

Each numbered step must begin with a failing test and end with its targeted
suite passing.

### Step 0 — close Phase 4 review findings

Add the four Gate 0 regressions from §4, then fix only those behaviors.

### Step 1 — adapter unit

1. Add boto3 and `archive/storage/s3.py`.
2. Build a small injected fake S3 client. It must model success, 404, 403, 409,
   412, checksum mismatch, metadata mismatch, and streaming `GetObject`.
3. Implement `head`, then `put_immutable`, then `open` in that order.
4. Re-run the existing object-store contract against both
   `LocalObjectStore` and the fake-backed `S3ObjectStore` where applicable.

### Step 2 — command wiring

1. Add `archive/storage/factory.py` and its argument-validation tests.
2. Wire both commands through it.
3. Update Compose, `.env.example`, the Python image dependency, and deployment
   documentation.
4. Prove the default local configuration remains byte-for-byte compatible in
   its receipts and reports.

### Step 3 — credentialed smoke test

Run against a disposable AWS test bucket with a unique key path:

1. put one data object and one seal;
2. head both and compare full-object SHA-256 and length;
3. read and decode the data to exact NDJSON;
4. retry the identical puts and observe idempotent success;
5. attempt different bytes at the same key and observe `IntegrityConflict`;
6. run the archiver twice and confirm one unchanged production receipt;
7. run the reaper in audit mode and confirm it verifies S3 but deletes nothing.

Then repeat the authorization half against `targeter-v2/`, because a policy that
authorizes `raw/` proves nothing about the other prefix:

8. run `targeter/run_v2.py --mode archive` and confirm the run receives
   `archive_receipt.json`, not `archive_receipt.local.json` — a conformance
   receipt here means the store was not recognized as independent;
9. confirm the receipt's `prefix` is `targeter-v2/runs/date=<date>/run=<run_id>`
   and that `run_manifest.json` is the last object committed;
10. run `targeter.v2.run_archiver_cli` against the same output root and confirm
    the run reports `skipped` rather than re-uploading;
11. run `targeter.v2.run_reaper_cli` in its default audit mode and confirm it
    heads every archived object and deletes nothing.

An `AccessDenied` at step 8 is the expected failure when the runtime role was
written for `raw/*` only; §12.3 is what fixes it.

Do not run the conflict test in a production prefix.

## 14. Required tests

### Adapter tests

- new conditional put succeeds and sends `IfNoneMatch="*"`;
- identical existing object succeeds after 412;
- different existing object becomes `IntegrityConflict`;
- existing matching bytes with different metadata becomes `IntegrityConflict`;
- 409 is retryable at the next sweep and writes no receipt;
- 403 is not treated as absence;
- missing or malformed `ChecksumSHA256` fails closed;
- composite checksum fails closed;
- ETag is never inspected as identity;
- incorrect reader length fails before any request;
- a non-seekable reader is refused;
- `open` is streaming, bounded, and closes its response body;
- expected bucket owner is sent on every request;
- no method calls an S3 delete API.

### Pipeline tests

- successful S3 archival writes `.archive.json`, never
  `.archive.local.json`;
- receipt `bucket` is the configured S3 bucket;
- data and seal receipt identities come from fresh S3 heads;
- upload, head, checksum, or metadata failure leaves raw, seal, and derivative
  intact and writes no receipt;
- a receipt naming another bucket cannot verify;
- manifest build excludes a transiently unavailable object without crashing;
- stale manifests disappear when their last valid entry disappears;
- reaper audit verifies both S3 objects and keeps local raw;
- destructive reaping still requires explicit `--delete`.

### Full gates

```text
python -m unittest discover -s tests
python -m unittest discover -s replay/tests
cargo test --manifest-path ingester/Cargo.toml --workspace
cargo test --manifest-path encoder/rust/Cargo.toml
docker compose config --quiet
docker compose build archiver
```

## 15. Rollout gate

Production rollout is deliberately one-way and observable:

1. deploy the S3 archiver with reaper deletion disabled;
2. archive for at least 24 hours;
3. sample every lane and verify exact S3 decode against retained local raw;
4. confirm retries do not change receipt bytes or create new object versions;
5. run the reaper in audit mode and inspect every retention reason;
6. keep S3 lifecycle expiry disabled;
7. only then consider a separate change enabling explicit destructive reaper
   runs.

No automatic raw deletion is part of adapter completion.

## 16. Acceptance checklist

The phase is complete only when every answer is yes:

- Does every write carry `If-None-Match: *`?
- Does S3 validate the caller's explicit SHA-256?
- Does a fresh checksum-enabled head prove the stored object?
- Is ETag absent from every integrity decision?
- Can a retry distinguish identical content from a conflict?
- Can a receipt verify only against the bucket it names?
- Can no receipt path escape the spool date directory?
- Does any failure leave the raw segment and seal untouched?
- Do both commands construct the backend through one factory?
- Does the runtime role lack S3 deletion authority?
- Are the reapers still audit-only by default?
- Is object lifecycle expiry still disabled?
- Do the runtime role and the conditional-write bucket policy cover both `raw/*`
  and `targeter-v2/*`?
- Has a real target run produced `archive_receipt.json` under `targeter-v2/`?

## 17. AWS references

- [Conditional S3 writes and 409/412 behavior](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html)
- [Enforcing conditional writes with bucket policy](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes-enforce.html)
- [`PutObject`: `IfNoneMatch` and `ChecksumSHA256`](https://docs.aws.amazon.com/boto3/latest/reference/services/s3/client/put_object.html)
- [`HeadObject`: checksum mode and returned checksum fields](https://docs.aws.amazon.com/botocore/latest/reference/services/s3/client/head_object.html)
- [S3 checksum integrity behavior](https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html)
- [Why ETag is not a full-object checksum for multipart data](https://docs.aws.amazon.com/AmazonS3/latest/userguide/tutorial-s3-mpu-additional-checksums.html)
- [Single-PUT 5 GB limit](https://docs.aws.amazon.com/AmazonS3/latest/userguide/upload-objects.html)
- [Required S3 API permissions](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-with-s3-policy-actions.html)

## 18. Review decisions requested

Approve or change these before implementation:

1. one dedicated bucket per deployment, holding the `raw/` and `targeter-v2/`
   archive prefixes of §12.1 under identical immutability rules;
2. single `PutObject` only in V1, failing closed above 5 GB;
3. SSE-S3 bucket-default encryption for the first deployment;
4. bucket owner account ID required in configuration;
5. bucket policy enforcement of `If-None-Match: *`;
6. no S3 delete permission and no lifecycle expiry;
7. 24-hour S3 archive/audit soak before any local raw deletion.
