"""Immutable selected-bundle projection for Targeter runs predating the index."""

from __future__ import annotations

import io
import json
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

from analysis.storage import write_ndjson_zstd
from archive.storage.base import (
    JSON_CONTENT_TYPE,
    NDJSON_CONTENT_TYPE,
    ZSTD_CONTENT_ENCODING,
    ObjectMetadata,
    ObjectStore,
    VerificationFailure,
)
from encoder import (
    DEFAULT_ZSTD_LEVEL,
    LogicalIdentity,
    StoredIdentity,
    decode_stream,
    encoder_version,
    stored_identity_of,
)
from targeter.v2.selected_bundles import SELECTED_BUNDLE_INDEX_VERSION

DERIVATIVE_RECEIPT_VERSION = 1
DERIVATIVE_INDEX_FILE = "selected_bundle_index.ndjson.zst"
DERIVATIVE_RECEIPT_FILE = "selected_bundle_index.receipt.json"
MAX_DERIVATIVE_RECEIPT_BYTES = 64 * 1024


class SelectedBundleDerivativeError(ValueError):
    """A derivative receipt or its immutable source binding is invalid."""


@dataclass(frozen=True)
class SelectedBundleArtifact:
    file: str
    key: str
    stored: StoredIdentity
    logical: LogicalIdentity
    content_type: str = NDJSON_CONTENT_TYPE
    content_encoding: str = ZSTD_CONTENT_ENCODING

    def as_source_record(self) -> dict[str, Any]:
        return {
            "name": self.file,
            "key": self.key,
            "content_type": self.content_type,
            "content_encoding": self.content_encoding,
            "decoded": self.logical.as_record(),
            "stored": self.stored.as_record(),
        }


def derivative_keys(manifest_key: str) -> tuple[str, str]:
    prefix, separator, filename = manifest_key.rpartition("/")
    if not separator or filename != "run_manifest.json":
        raise SelectedBundleDerivativeError(
            f"Targeter manifest key has the wrong shape: {manifest_key}"
        )
    return (
        f"{prefix}/{DERIVATIVE_INDEX_FILE}",
        f"{prefix}/{DERIVATIVE_RECEIPT_FILE}",
    )


def ensure_selected_bundle_derivative(
    store: ObjectStore,
    *,
    manifest_key: str,
    manifest_stored: StoredIdentity,
    report_key: str,
    report_stored: StoredIdentity,
    report_logical: LogicalIdentity | None,
    rows: Iterable[Mapping[str, Any]],
    temporary_directory: Path,
) -> SelectedBundleArtifact:
    """Return a committed derivative, publishing it exactly once when absent."""
    index_key, receipt_key = derivative_keys(manifest_key)
    receipt_metadata = store.head(receipt_key)
    if receipt_metadata is not None:
        return _read_receipt(
            store,
            receipt_key=receipt_key,
            manifest_key=manifest_key,
            manifest_stored=manifest_stored,
            report_key=report_key,
            report_stored=report_stored,
            report_logical=report_logical,
        )

    if report_logical is None:
        raise SelectedBundleDerivativeError(
            "publishing a derivative requires the verified report identity"
        )
    temporary_directory.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        dir=temporary_directory, prefix="selected-bundle-"
    ) as directory:
        artifact_path = Path(directory) / DERIVATIVE_INDEX_FILE
        encoded = write_ndjson_zstd(artifact_path, rows)
        existing = store.head(index_key)
        if existing is None:
            with artifact_path.open("rb") as source:
                metadata = store.put_immutable(
                    index_key,
                    source,
                    encoded.stored,
                    content_type=NDJSON_CONTENT_TYPE,
                    content_encoding=ZSTD_CONTENT_ENCODING,
                )
            artifact = SelectedBundleArtifact(
                file=DERIVATIVE_INDEX_FILE,
                key=index_key,
                stored=metadata.stored,
                logical=encoded.logical,
            )
        else:
            _verify_artifact_metadata(existing, index_key)
            with store.open(index_key, max_bytes=existing.byte_length) as source:
                with tempfile.TemporaryFile(mode="w+b", dir=temporary_directory) as sink:
                    decode_stream(
                        source,
                        sink,
                        expected_logical=encoded.logical,
                        expected_stored=existing.stored,
                        max_decoded_bytes=encoded.logical.byte_length,
                    )
            artifact = SelectedBundleArtifact(
                file=DERIVATIVE_INDEX_FILE,
                key=index_key,
                stored=existing.stored,
                logical=encoded.logical,
            )

    receipt = {
        "selected_bundle_derivative_receipt_version": DERIVATIVE_RECEIPT_VERSION,
        "authoritative_run_commit": False,
        "authorizes_run_publication": False,
        "source": {
            "manifest": {
                "key": manifest_key,
                "stored": manifest_stored.as_record(),
            },
            "selection_report": {
                "key": report_key,
                "stored": report_stored.as_record(),
                "decoded": report_logical.as_record(),
            },
        },
        "projection": {
            "name": "selected_bundle_index",
            "version": SELECTED_BUNDLE_INDEX_VERSION,
        },
        "artifact": {
            **artifact.as_source_record(),
            "compression": {
                "algorithm": "zstd",
                "level": DEFAULT_ZSTD_LEVEL,
                "frame_checksum": True,
                "dictionary": None,
                "frame_count": 1,
                "encoder": encoder_version(),
            },
        },
    }
    payload = _canonical_json(receipt)
    with io.BytesIO(payload) as source:
        store.put_immutable(
            receipt_key,
            source,
            stored_identity_of(io.BytesIO(payload)),
            content_type=JSON_CONTENT_TYPE,
        )
    return _read_receipt(
        store,
        receipt_key=receipt_key,
        manifest_key=manifest_key,
        manifest_stored=manifest_stored,
        report_key=report_key,
        report_stored=report_stored,
        report_logical=report_logical,
    )


def _read_receipt(
    store: ObjectStore,
    *,
    receipt_key: str,
    manifest_key: str,
    manifest_stored: StoredIdentity,
    report_key: str,
    report_stored: StoredIdentity,
    report_logical: LogicalIdentity | None,
) -> SelectedBundleArtifact:
    metadata = store.head(receipt_key)
    if metadata is None:
        raise SelectedBundleDerivativeError(f"derivative receipt is absent: {receipt_key}")
    if (
        metadata.byte_length > MAX_DERIVATIVE_RECEIPT_BYTES
        or metadata.content_type != JSON_CONTENT_TYPE
        or metadata.content_encoding is not None
    ):
        raise SelectedBundleDerivativeError(
            f"derivative receipt has invalid metadata: {receipt_key}"
        )
    with store.open(receipt_key, max_bytes=metadata.byte_length) as source:
        payload = source.read(metadata.byte_length + 1)
    if stored_identity_of(io.BytesIO(payload)) != metadata.stored:
        raise VerificationFailure(f"derivative receipt identity drifted: {receipt_key}")
    try:
        document = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise SelectedBundleDerivativeError(
            f"derivative receipt is invalid JSON: {receipt_key}"
        ) from error
    _exact(
        document,
        {
            "selected_bundle_derivative_receipt_version",
            "authoritative_run_commit",
            "authorizes_run_publication",
            "source",
            "projection",
            "artifact",
        },
        "derivative receipt",
    )
    if (
        document["selected_bundle_derivative_receipt_version"]
        != DERIVATIVE_RECEIPT_VERSION
        or document["authoritative_run_commit"] is not False
        or document["authorizes_run_publication"] is not False
    ):
        raise SelectedBundleDerivativeError("derivative receipt policy fields are invalid")
    source = document["source"]
    _exact(source, {"manifest", "selection_report"}, "derivative source")
    manifest = source["manifest"]
    report = source["selection_report"]
    _exact(manifest, {"key", "stored"}, "derivative manifest source")
    _exact(
        report, {"key", "stored", "decoded"}, "derivative report source"
    )
    if (
        manifest.get("key") != manifest_key
        or StoredIdentity.from_record(manifest.get("stored")) != manifest_stored
        or report.get("key") != report_key
        or StoredIdentity.from_record(report.get("stored")) != report_stored
    ):
        raise SelectedBundleDerivativeError(
            "derivative receipt does not bind the committed source run"
        )
    receipt_report_logical = LogicalIdentity.from_record(report.get("decoded"))
    if report_logical is not None and receipt_report_logical != report_logical:
        raise SelectedBundleDerivativeError(
            "derivative receipt does not bind the verified report payload"
        )
    projection = document["projection"]
    _exact(projection, {"name", "version"}, "derivative projection")
    if projection != {
        "name": "selected_bundle_index",
        "version": SELECTED_BUNDLE_INDEX_VERSION,
    }:
        raise SelectedBundleDerivativeError("derivative projection contract is invalid")

    artifact_record = document["artifact"]
    _exact(
        artifact_record,
        {
            "name",
            "key",
            "content_type",
            "content_encoding",
            "decoded",
            "stored",
            "compression",
        },
        "derivative artifact",
    )
    index_key, _ = derivative_keys(manifest_key)
    artifact = SelectedBundleArtifact(
        file=_required_text(artifact_record, "name", "derivative artifact"),
        key=_required_text(artifact_record, "key", "derivative artifact"),
        stored=StoredIdentity.from_record(artifact_record["stored"]),
        logical=LogicalIdentity.from_record(artifact_record["decoded"]),
    )
    if (
        artifact.file != DERIVATIVE_INDEX_FILE
        or artifact.key != index_key
        or artifact_record["content_type"] != NDJSON_CONTENT_TYPE
        or artifact_record["content_encoding"] != ZSTD_CONTENT_ENCODING
    ):
        raise SelectedBundleDerivativeError("derivative artifact contract is invalid")
    compression = artifact_record["compression"]
    _exact(
        compression,
        {
            "algorithm",
            "level",
            "frame_checksum",
            "dictionary",
            "frame_count",
            "encoder",
        },
        "derivative compression",
    )
    assert isinstance(compression, dict)
    if {
        key: compression[key]
        for key in ("algorithm", "level", "frame_checksum", "dictionary", "frame_count")
    } != {
        "algorithm": "zstd",
        "level": DEFAULT_ZSTD_LEVEL,
        "frame_checksum": True,
        "dictionary": None,
        "frame_count": 1,
    } or not isinstance(compression["encoder"], str) or not compression["encoder"]:
        raise SelectedBundleDerivativeError("derivative compression contract is invalid")
    artifact_metadata = store.head(artifact.key)
    if artifact_metadata is None or artifact_metadata.stored != artifact.stored:
        raise VerificationFailure(f"derivative artifact identity drifted: {artifact.key}")
    _verify_artifact_metadata(artifact_metadata, artifact.key)
    return artifact


def _verify_artifact_metadata(metadata: ObjectMetadata, key: str) -> None:
    if (
        metadata.content_type != NDJSON_CONTENT_TYPE
        or metadata.content_encoding != ZSTD_CONTENT_ENCODING
    ):
        raise SelectedBundleDerivativeError(
            f"derivative artifact has invalid metadata: {key}"
        )


def _exact(value: Any, fields: set[str], label: str) -> None:
    if not isinstance(value, dict) or set(value) != fields:
        raise SelectedBundleDerivativeError(f"{label} fields are invalid")


def _required_text(value: dict[str, Any], field: str, label: str) -> str:
    item = value.get(field)
    if not isinstance(item, str) or not item:
        raise SelectedBundleDerivativeError(f"{label} {field} must be non-empty text")
    return item


def _canonical_json(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")
