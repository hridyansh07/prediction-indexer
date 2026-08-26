# Google Cloud Storage Archive Adapter V1

Status: **implemented**. This document specifies the native Google Cloud
Storage adapter used by the raw, canonical, and Targeter v2 archivers.

This specification is subordinate to
[`PHASE_4_RAW_ARCHIVE_REAPER_V1.md`](PHASE_4_RAW_ARCHIVE_REAPER_V1.md) and
[`ZSTD_MATERIALIZATION_PIPELINE_V1.md`](../encoder/ZSTD_MATERIALIZATION_PIPELINE_V1.md).
Where this document is more specific about GCS calls and integrity evidence,
this document controls.

## 1. Integrity contract

GCS does not expose a server-side SHA-256 for an object. The adapter keeps the
provider-specific work at the object-store boundary:

1. start a resumable upload with `ifGenerationMatch=0`;
2. stream the input once in 1 MiB chunks, checking its expected SHA-256 and byte
   length while the GCS client validates CRC32C;
3. attach that SHA-256 and byte length as object metadata for cheap identity and
   retry checks;
4. fetch the uploaded generation, length, CRC32C, and content metadata;
5. require that provider metadata to agree with the requested identity and
   content metadata before publishing the receipt last.

The application's SHA-256 is bound to the exact upload stream, while the GCS
client's CRC32C handshake proves that GCS accepted that stream without transfer
corruption. Custom SHA-256 metadata and service-returned CRC32C are therefore
the normal verification evidence. This protects against accidental corruption,
truncation, and wrong-object selection; it does not claim SHA-256's resistance
to a deliberately constructed CRC32C collision.

The resulting production receipt records normalized SHA-256 as the actual
stored-byte identity and records the service-returned CRC32C separately as:

```json
{
  "provider_checksum": "base64-encoded-crc32c",
  "provider_checksum_algorithm": "CRC32C"
}
```

Normal `head` and `verify_metadata` operations are metadata-only. They require a
current generation and metageneration and compare GCS length and CRC32C plus
custom SHA-256, custom length, content type, and content encoding with the
receipt. `verify` is the explicit full-byte audit operation. The closed local
receipt remains the commit marker; object metadata or a successful upload alone
is not one.

Retrieval does not call `head` and then download the same object again. It loads
metadata, pins the named generation, and returns one verified stream that
calculates SHA-256 and CRC32C while the decoder or plain-file stager consumes
it. The stream checks receipt identity and rechecks generation/metageneration at
EOF. This is the full-byte verification path because retrieval already requires
the complete object.

## 2. Immutability and streaming

`ifGenerationMatch=0` is mandatory on every create. A conditional-create
conflict without a committed receipt retains one generation-pinned full
readback before an existing key is accepted as an identical interrupted retry.
Different bytes or content metadata produce an integrity conflict. The adapter
never calls an object delete API.

Uploads use GCS resumable upload file I/O even for small objects. Upload and
retrieval are bounded to 1 MiB application buffers; the implementation does not
select the client library's small-object multipart path, which can buffer a
whole object. Retrieval and interrupted-retry readbacks select an exact
generation and request raw stored bytes so content encoding does not alter the
identity calculation.

## 3. Configuration and credentials

Select the backend with:

```dotenv
ARCHIVE_BACKEND=gcs
ARCHIVE_GCS_BUCKET=my-dedicated-archive-bucket
```

`ARCHIVE_GCS_BUCKET` is required for `gcs` and rejected for `local` or `s3`.
S3 options are likewise rejected when GCS is selected. The bucket name is the
store location persisted in provider-neutral production receipts. The same
`ARCHIVE_BACKEND` configures raw, canonical, and Targeter archive consumers.

The client uses Application Default Credentials. On Compute Engine, attach a
dedicated service account to the VM. Outside GCP, prefer Workload Identity
Federation. Do not put service-account JSON keys in `.env`, the repository, or
the container image.

The runtime principal needs only these bucket-level permissions on the archive
bucket:

```text
storage.objects.create
storage.objects.get
storage.objects.list
```

For a same-day rollout, grant the predefined Storage Object Creator and Storage
Object Viewer roles at the bucket level; together they provide create, get, and
list without object deletion. An exact custom role may narrow this further. Do
not grant Storage Object User or Storage Object Admin, `storage.objects.delete`,
`storage.objects.update`, bucket administration, IAM administration, or
retention-policy administration.

## 4. Bucket and rollout requirements

Use one private, dedicated bucket in the capture region with uniform
bucket-level access, public access prevention, and Google-managed encryption or
an already-operational CMEK policy. Object versioning is recommended as an
additional recovery defense. Do not enable lifecycle expiry while validating
the migration.

For the first rollout:

1. keep `REAPER_MODE=audit` and `CANONICAL_REAPER_MODE=audit`;
2. run one archiver sweep and confirm raw and canonical production receipts use
   receipt version 2, provider `gcs`, and checksum algorithm `CRC32C`;
3. run archive integrity and both reaper audits;
4. retain local raw and soak for at least 24 hours;
5. sample every lane by strict decode against retained local evidence;
6. only then consider a separate, explicit decision to enable local deletion.

Starting capture or archival does not authorize reaping. A failed upload or
metadata verification publishes no receipt and leaves local evidence intact.

### Deferred reaper optimizations

The provider-neutral verification boundary is intentionally complete before it
is optimized. Follow-up work must preserve the same receipt and deletion gates:

- route or filter historical S3 receipts separately from current GCS receipts
  during a provider transition;
- make an already-reaped raw tombstone complete the decision with zero object
  requests, and run other cheap local eligibility checks before GCS metadata
  requests;
- add credentialed GCS raw, canonical, and Targeter reaper acceptance coverage;
  fake-backed request-count regressions prevent normal verification from
  downloading object bodies.

## 5. Receipt compatibility

Provider-neutral production receipt versions are:

- raw archive receipt version 2;
- canonical archive receipt version 2;
- Targeter v2 run archive receipt version 3.

Strict readers retain legacy S3 receipt support. New receipts identify the
store with `{provider, location}` and carry generic provider checksum fields,
while the SHA-256 field always describes the exact object bytes regardless of
provider.

## 6. References

- [GCS request preconditions](https://cloud.google.com/storage/docs/request-preconditions)
- [GCS data validation and CRC32C](https://cloud.google.com/storage/docs/data-validation)
- [Object generations and versioning](https://cloud.google.com/storage/docs/metadata#generation-number)
- [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials)
