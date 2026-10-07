# Archive formats

Object keys, receipts and manifests written by `archive/`. All receipt schemas are
closed; strict readers reject unknown fields. Hex digests are lowercase, byte
counts are integers, times are UTC Unix nanoseconds. Behavior is in
[`README.md`](README.md). The codec contract is in `encoder/README.md`.

## Object keys

```text
raw/lane=<lane>/date=<YYYY-MM-DD>/<segment>.ndjson.zst
raw/lane=<lane>/date=<YYYY-MM-DD>/<segment>.seal.json        exact local seal bytes
canonical/date=<YYYY-MM-DD>/window=<window_start_ns>/evidence.ndjson.zst
canonical/date=<YYYY-MM-DD>/window=<window_start_ns>/provenance.ndjson.zst
canonical/date=<YYYY-MM-DD>/window=<window_start_ns>/receipt.json
```

`<segment>` is the segment stem (UTC window stamp, zero-padded segment index,
segment id). Data objects use `Content-Type: application/x-ndjson` and
`Content-Encoding: zstd`; seals and receipts use `application/json`. Readers take
the expected identity from the receipt and seal, never from HTTP metadata. Keys are
immutable: an existing key with a different length or SHA-256 is an integrity
conflict.

Provider checksum evidence is separate from the SHA-256, which always describes the
exact stored bytes: for GCS, `provider_checksum_algorithm` is `CRC32C` and the value
is GCS's base64 CRC32C.

## Local files beside a segment

```text
<segment>.ndjson.zst.open   in-progress derivative (uncommitted)
<segment>.ndjson.zst        derivative; rebuildable, never authority
<segment>.archive.json      production receipt (deletion authority)
<segment>.archive.local.json  conformance receipt (authorizes nothing)
```

## Raw archive receipt (`archive_receipt_version` 2)

```json
{
  "archive_receipt_version": 2,
  "store": {"provider": "gcs", "location": "<bucket>"},
  "lane_id": "polymarket",
  "window_start_ns": 0, "window_end_ns": 0,
  "segment_id": "...", "segment_index": 0,
  "source": {"file": "<segment>.ndjson", "byte_length": 0, "line_count": 0, "sha256": "..."},
  "seal": {"file": "<segment>.seal.json", "byte_length": 0, "sha256": "...",
           "key": "raw/lane=.../<segment>.seal.json",
           "provider_checksum": "...", "provider_checksum_algorithm": "CRC32C"},
  "object": {"key": "raw/lane=.../<segment>.ndjson.zst", "byte_length": 0, "sha256": "...",
             "provider_checksum": "...", "provider_checksum_algorithm": "CRC32C",
             "content_encoding": "zstd"},
  "compression": {"algorithm": "zstd", "level": 3, "frame_checksum": true,
                  "dictionary": null, "frame_count": 1, "encoder": "python-zstandard/<version>"},
  "verified_at_ns": 0,
  "archiver_version": 1
}
```

`source` is the logical identity (what the seal commits); `object` is the stored
identity of the frame. The receipt is immutable once committed and retained after
the raw source is deleted so the deletion stays auditable.

## Conformance receipt (`local_archive_receipt_version` 1)

Same `lane_id`, window, segment, `source`, `seal`, `object` and `compression`
sections but with a `store` identifier and key instead of provider/checksum fields,
plus `"durability": "local_conformance"` and `"authorizes_deletion": false`. It uses
a different version key and filename on purpose so it cannot be mistaken for a
production receipt.

## Canonical archive receipt (`canonical_archive_receipt_version` 2)

Written beside the local window as `canonical_archive_receipt.json`, last, after
fresh metadata verification of all three objects:

```json
{
  "canonical_archive_receipt_version": 2,
  "store": {"provider": "gcs", "location": "<bucket>"},
  "window_start_ns": 0, "window_end_ns": 0,
  "evidence":          {"file": "evidence.ndjson.zst", "key": "...", "byte_length": 0, "sha256": "...",
                        "content_type": "application/x-ndjson", "content_encoding": "zstd",
                        "provider_checksum": "...", "provider_checksum_algorithm": "CRC32C"},
  "provenance":        {"file": "provenance.ndjson.zst", "...": "same shape"},
  "canonical_receipt": {"file": "receipt.json", "key": "...", "byte_length": 0, "sha256": "...",
                        "content_type": "application/json", "content_encoding": null,
                        "provider_checksum": "...", "provider_checksum_algorithm": "CRC32C"},
  "verified_at_ns": 0,
  "archiver_version": 1
}
```

The `evidence` and `provenance` stored identities equal those in the canonical
`receipt.json`; `canonical_receipt` identifies the unchanged receipt bytes. The
conformance form (`canonical_archive_receipt.local.json`,
`local_canonical_archive_receipt_version`) carries a `store` string per entry
instead of provider fields, plus `durability` and `authorizes_deletion: false`.

## Daily manifest (`manifest_version` 1)

`<manifest-root>/date=<YYYY-MM-DD>/manifest.json`: `manifest_version`, `date`,
`receipt_kind`, `authorizes_deletion` (always false), `day_closed`,
`segment_count`, and `segments`, one entry per segment (lane, window, data key,
seal key, logical identity, stored identity) sorted by
`(window_start_ns, lane rank, segment_index, segment_id)` using the lane ranks in
`ingester/README.md`. It is a pure function of the revalidated receipts (no
generation timestamp), so two runs produce byte-identical files. It is a replay
catalog, not a commit or evidence boundary; a manifest for an open UTC day changes
as segments are archived.
