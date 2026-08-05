"""Immutable archival of one complete Targeter v2 phase-5 run directory.

The remote ``run_manifest.json`` is the commit marker.  Every catalogue and
selection artifact is uploaded and verified before that key is published; the
local receipt is written only after the manifest itself verifies in the object
store.  An interrupted prefix without the manifest is therefore incomplete and
an identical retry is safe.
"""

from __future__ import annotations

import json
import re
import stat
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from archive.common.durable import confirm_durable, write_json_durable
from archive.storage.base import (
    JSON_CONTENT_TYPE,
    NDJSON_CONTENT_TYPE,
    ObjectMetadata,
    ObjectStore,
    VerificationFailure,
    provider_checksum_of,
)
from encoder import StoredIdentity, stored_identity_of
from targeter.v2.domain import parse_timestamp


RUN_MANIFEST_VERSION = 1
RUN_ARCHIVE_RECEIPT_VERSION = 1
LOCAL_RUN_ARCHIVE_RECEIPT_VERSION = 1
RUN_MANIFEST_FILE = "run_manifest.json"
PRODUCTION_RECEIPT_FILE = "archive_receipt.json"
LOCAL_RECEIPT_FILE = "archive_receipt.local.json"

SELECTION_REPORT_FILE = "selection_report.json"

_CATALOGUE_FILE = re.compile(
    r"catalog_(kalshi|polymarket|limitless)_(events|markets)\.ndjson\Z"
)
REQUIRED_FILES = frozenset(
    {SELECTION_REPORT_FILE, "rule_templates.ndjson", "rule_drift.ndjson"}
)
_REQUIRED_FILES = REQUIRED_FILES

#: ``targeter/v2/run.py`` mints run ids as ``%Y%m%dT%H%M%S.%fZ``, which sorts
#: lexicographically in time order.
RUN_ID = re.compile(r"\d{8}T\d{6}\.\d{6}Z\Z")
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


class RunArchiveError(ValueError):
    """The run directory, manifest, or receipt does not prove one archive."""


def parse_run_id_ns(name: str) -> int | None:
    """The instant a run id names, or None if it does not name one."""
    if RUN_ID.fullmatch(name) is None:
        return None
    try:
        moment = datetime.strptime(name[:-1], "%Y%m%dT%H%M%S.%f")
    except ValueError:
        return None
    delta = moment.replace(tzinfo=timezone.utc) - _EPOCH
    return (delta.days * 86_400 + delta.seconds) * 1_000_000_000 + delta.microseconds * 1_000


def discover_runs(output_root: Path) -> list[Path]:
    """Every run directory under an output root, oldest first.

    Run directories rather than receipts are the discovery axis for both the
    sweep and the reaper.  A run nothing archived has no receipt, and that run
    is exactly the one an operator most needs to see: it is the one the sweep
    exists to fix and the one the reaper can never reclaim.
    """
    root = Path(output_root)
    if not root.is_dir():
        return []
    found = [
        path
        for path in root.iterdir()
        if path.is_dir() and RUN_ID.fullmatch(path.name) is not None
    ]
    return sorted(found, key=lambda path: path.name)


def unrecognized_directories(output_root: Path) -> list[str]:
    """Directories under an output root that are not run directories at all."""
    root = Path(output_root)
    if not root.is_dir():
        return []
    return sorted(
        path.name
        for path in root.iterdir()
        if path.is_dir() and RUN_ID.fullmatch(path.name) is None
    )


@dataclass(frozen=True)
class ArchivedRunObject:
    file: str
    key: str
    stored: StoredIdentity
    content_type: str
    provider_checksum: str | None
    provider_checksum_algorithm: str | None


@dataclass(frozen=True)
class RunArchiveReceipt:
    kind: str
    path: Path
    run_id: str
    location: str
    prefix: str
    manifest: ArchivedRunObject
    objects: tuple[ArchivedRunObject, ...]
    archived_at_ns: int
    document: dict[str, Any]

    @property
    def is_production(self) -> bool:
        return self.kind == "production"


def _identity(path: Path) -> StoredIdentity:
    with path.open("rb") as handle:
        return stored_identity_of(handle)


def _content_type(name: str) -> str:
    return NDJSON_CONTENT_TYPE if name.endswith(".ndjson") else JSON_CONTENT_TYPE


def _read_json(path: Path, description: str) -> dict[str, Any]:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except OSError as error:
        raise RunArchiveError(f"cannot read {description} {path}: {error}") from error
    except json.JSONDecodeError as error:
        raise RunArchiveError(f"invalid {description} {path}: {error}") from error
    if not isinstance(document, dict):
        raise RunArchiveError(f"{description} {path} is not a JSON object")
    return document


def _run_report(run_directory: Path) -> dict[str, Any]:
    report = _read_json(run_directory / "selection_report.json", "selection report")
    run_id = report.get("run_id")
    if not isinstance(run_id, str) or run_id != run_directory.name:
        raise RunArchiveError(
            f"selection report run_id {run_id!r} does not match directory {run_directory.name!r}"
        )
    if report.get("report_version") != 1 or report.get("mode") != "shadow":
        raise RunArchiveError("selection report is not a phase-5 shadow report")
    selection = report.get("selection")
    if not isinstance(selection, dict) or selection.get("publication_performed") is not False:
        raise RunArchiveError("selection report already claims publication")
    if not isinstance(report.get("input_complete"), bool):
        raise RunArchiveError("selection report input_complete must be boolean")
    if not isinstance(report.get("discovery_failures"), dict):
        raise RunArchiveError("selection report discovery_failures must be an object")
    if parse_timestamp(report.get("generated_at")) is None:
        raise RunArchiveError("selection report generated_at is invalid")
    return report


def _source_files(run_directory: Path, report: dict[str, Any]) -> tuple[Path, ...]:
    files: list[Path] = []
    for path in sorted(run_directory.iterdir(), key=lambda item: item.name):
        if path.name in {
            RUN_MANIFEST_FILE,
            PRODUCTION_RECEIPT_FILE,
            LOCAL_RECEIPT_FILE,
        }:
            continue
        try:
            mode = path.lstat().st_mode
        except OSError as error:
            raise RunArchiveError(f"cannot stat run artifact {path}: {error}") from error
        if not stat.S_ISREG(mode):
            raise RunArchiveError(f"run artifact is not a regular file: {path}")
        if path.name not in _REQUIRED_FILES and _CATALOGUE_FILE.fullmatch(path.name) is None:
            raise RunArchiveError(f"unexpected run artifact: {path.name}")
        files.append(path)

    names = {path.name for path in files}
    missing = sorted(_REQUIRED_FILES - names)
    if missing:
        raise RunArchiveError(f"run is missing required artifacts: {', '.join(missing)}")

    catalogues = report.get("catalogs")
    if not isinstance(catalogues, list):
        raise RunArchiveError("selection report catalogs must be an array")
    for summary in catalogues:
        if not isinstance(summary, dict) or summary.get("venue") not in {
            "kalshi", "polymarket", "limitless"
        }:
            raise RunArchiveError("selection report has an invalid catalog summary")
        venue = str(summary["venue"])
        for kind in ("events", "markets"):
            expected = f"catalog_{venue}_{kind}.ndjson"
            if expected not in names:
                raise RunArchiveError(f"catalog summary for {venue} has no {expected}")
    return tuple(files)


def build_run_manifest(run_directory: Path) -> tuple[dict[str, Any], Path]:
    run_directory = Path(run_directory)
    report = _run_report(run_directory)
    files = _source_files(run_directory, report)
    document = {
        "targeter_run_manifest_version": RUN_MANIFEST_VERSION,
        "run_id": run_directory.name,
        "generated_at": report["generated_at"],
        "input_complete": report["input_complete"],
        "files": [
            {
                "file": path.name,
                "byte_length": (identity := _identity(path)).byte_length,
                "sha256": identity.sha256,
                "content_type": _content_type(path.name),
            }
            for path in files
        ],
    }
    path = run_directory / RUN_MANIFEST_FILE
    if path.exists():
        if _read_json(path, "run manifest") != document:
            raise RunArchiveError("existing run manifest disagrees with current run artifacts")
        confirm_durable(path)
    else:
        write_json_durable(path, document)
    return document, path


def _date_and_prefix(report: dict[str, Any], run_id: str) -> tuple[str, str]:
    generated = parse_timestamp(report.get("generated_at"))
    assert generated is not None
    date = generated.astimezone(timezone.utc).date().isoformat()
    return date, f"targeter-v2/runs/date={date}/run={run_id}"


def archive_run(
    run_directory: Path,
    store: ObjectStore,
    *,
    now: datetime | None = None,
) -> RunArchiveReceipt:
    run_directory = Path(run_directory)
    report = _run_report(run_directory)
    _manifest_document, manifest_path = build_run_manifest(run_directory)
    _date, prefix = _date_and_prefix(report, run_directory.name)
    kind = "production" if store.durability.independent else "local"
    receipt_path = run_directory / (
        PRODUCTION_RECEIPT_FILE if kind == "production" else LOCAL_RECEIPT_FILE
    )
    if receipt_path.exists():
        receipt = read_run_archive_receipt(receipt_path)
        verify_run_archive(store, receipt)
        validate_local_run(run_directory, receipt)
        return receipt

    uploaded: list[ArchivedRunObject] = []
    source_files = list(_source_files(run_directory, report)) + [manifest_path]
    for path in source_files:
        stored = _identity(path)
        key = f"{prefix}/{path.name}"
        content_type = _content_type(path.name)
        with path.open("rb") as reader:
            metadata = store.put_immutable(
                key,
                reader,
                stored,
                content_type=content_type,
                content_encoding=None,
            )
        _verify_metadata(metadata, stored, content_type, key)
        uploaded.append(_archived_object(path.name, metadata))

    manifest = next(item for item in uploaded if item.file == RUN_MANIFEST_FILE)
    instant = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    location_field = "bucket" if kind == "production" else "store"
    version_field = (
        "targeter_run_archive_receipt_version"
        if kind == "production"
        else "local_targeter_run_archive_receipt_version"
    )
    document: dict[str, Any] = {
        version_field: (
            RUN_ARCHIVE_RECEIPT_VERSION
            if kind == "production"
            else LOCAL_RUN_ARCHIVE_RECEIPT_VERSION
        ),
        "run_id": run_directory.name,
        location_field: store.store_id,
        "prefix": prefix,
        "archived_at_ns": int(instant.timestamp() * 1_000_000_000),
        "manifest": _object_record(manifest, production=kind == "production"),
        "objects": [
            _object_record(item, production=kind == "production") for item in uploaded
        ],
        "durability": store.durability.name,
        "authorizes_publication": kind == "production",
    }
    write_json_durable(receipt_path, document)
    receipt = parse_run_archive_receipt(document, path=receipt_path)
    verify_run_archive(store, receipt)
    return receipt


def _verify_metadata(
    metadata: ObjectMetadata,
    stored: StoredIdentity,
    content_type: str,
    key: str,
) -> None:
    if not metadata.matches_request(stored, content_type, None):
        raise VerificationFailure(f"archived targeter object {key} failed identity verification")
    if metadata.provider_checksum_algorithm != "SHA256":
        raise VerificationFailure(f"archived targeter object {key} lacks a SHA256 checksum")
    if metadata.provider_checksum != provider_checksum_of(stored.sha256):
        raise VerificationFailure(f"archived targeter object {key} has the wrong provider checksum")


def _archived_object(name: str, metadata: ObjectMetadata) -> ArchivedRunObject:
    return ArchivedRunObject(
        file=name,
        key=metadata.key,
        stored=metadata.stored,
        content_type=metadata.content_type or "",
        provider_checksum=metadata.provider_checksum,
        provider_checksum_algorithm=metadata.provider_checksum_algorithm,
    )


def _object_record(item: ArchivedRunObject, *, production: bool) -> dict[str, Any]:
    record: dict[str, Any] = {
        "file": item.file,
        "key": item.key,
        "byte_length": item.stored.byte_length,
        "sha256": item.stored.sha256,
        "content_type": item.content_type,
        "content_encoding": None,
    }
    if production:
        record["provider_checksum"] = item.provider_checksum
        record["provider_checksum_algorithm"] = item.provider_checksum_algorithm
    return record


def read_run_archive_receipt(path: Path) -> RunArchiveReceipt:
    return parse_run_archive_receipt(_read_json(Path(path), "run archive receipt"), path=Path(path))


def parse_run_archive_receipt(document: Any, *, path: Path) -> RunArchiveReceipt:
    if not isinstance(document, dict):
        raise RunArchiveError("run archive receipt is not an object")
    production = "targeter_run_archive_receipt_version" in document
    local = "local_targeter_run_archive_receipt_version" in document
    if production == local:
        raise RunArchiveError("run archive receipt must name exactly one receipt version")
    if production:
        if document["targeter_run_archive_receipt_version"] != RUN_ARCHIVE_RECEIPT_VERSION:
            raise RunArchiveError("unsupported targeter run archive receipt version")
        if path.name != PRODUCTION_RECEIPT_FILE:
            raise RunArchiveError("production run archive receipt has the wrong filename")
        kind = "production"
        location_field = "bucket"
    else:
        if document["local_targeter_run_archive_receipt_version"] != LOCAL_RUN_ARCHIVE_RECEIPT_VERSION:
            raise RunArchiveError("unsupported local targeter run archive receipt version")
        if path.name != LOCAL_RECEIPT_FILE:
            raise RunArchiveError("local run archive receipt has the wrong filename")
        kind = "local"
        location_field = "store"

    run_id = _text(document, "run_id")
    location = _text(document, location_field)
    prefix = _text(document, "prefix")
    archived_at_ns = _integer(document, "archived_at_ns")
    if document.get("authorizes_publication") is not production:
        raise RunArchiveError("run archive receipt publication authority is inconsistent")
    raw_objects = document.get("objects")
    if not isinstance(raw_objects, list) or not raw_objects:
        raise RunArchiveError("run archive receipt objects must be a non-empty array")
    objects = tuple(_parse_object(item, production=production) for item in raw_objects)
    if len({item.file for item in objects}) != len(objects) or len(
        {item.key for item in objects}
    ) != len(objects):
        raise RunArchiveError("run archive receipt contains duplicate files or keys")
    manifest = _parse_object(document.get("manifest"), production=production)
    matching = [item for item in objects if item.file == RUN_MANIFEST_FILE]
    if len(matching) != 1 or matching[0] != manifest:
        raise RunArchiveError("run archive receipt manifest does not match its object list")
    if any(not item.key.startswith(prefix + "/") for item in objects):
        raise RunArchiveError("run archive object escapes the recorded prefix")
    return RunArchiveReceipt(
        kind=kind,
        path=path,
        run_id=run_id,
        location=location,
        prefix=prefix,
        manifest=manifest,
        objects=objects,
        archived_at_ns=archived_at_ns,
        document=document,
    )


def _parse_object(value: Any, *, production: bool) -> ArchivedRunObject:
    if not isinstance(value, dict):
        raise RunArchiveError("run archive object entry is not an object")
    name = _text(value, "file")
    if Path(name).name != name:
        raise RunArchiveError(f"run archive object file is not a basename: {name!r}")
    key = _text(value, "key")
    content_type = _text(value, "content_type")
    if content_type != _content_type(name) or value.get("content_encoding") is not None:
        raise RunArchiveError(f"run archive object {name} has invalid content metadata")
    stored = StoredIdentity(
        sha256=_digest(value, "sha256"), byte_length=_integer(value, "byte_length")
    )
    provider_checksum = None
    provider_algorithm = None
    if production:
        provider_checksum = _text(value, "provider_checksum")
        provider_algorithm = _text(value, "provider_checksum_algorithm")
        if provider_algorithm != "SHA256" or provider_checksum != provider_checksum_of(
            stored.sha256
        ):
            raise RunArchiveError(f"run archive object {name} has invalid provider checksum")
    elif "provider_checksum" in value or "provider_checksum_algorithm" in value:
        raise RunArchiveError("local run archive receipt must not claim provider checksums")
    return ArchivedRunObject(
        file=name,
        key=key,
        stored=stored,
        content_type=content_type,
        provider_checksum=provider_checksum,
        provider_checksum_algorithm=provider_algorithm,
    )


def verify_run_archive(store: ObjectStore, receipt: RunArchiveReceipt) -> None:
    if receipt.location != store.store_id:
        raise VerificationFailure(
            f"run archive receipt names {receipt.location!r}, not store {store.store_id!r}"
        )
    if receipt.is_production and not store.durability.independent:
        raise VerificationFailure("production run archive receipt requires an independent store")
    for item in receipt.objects:
        metadata = store.head(item.key)
        if metadata is None:
            raise VerificationFailure(f"archived targeter object is absent: {item.key}")
        _verify_metadata(metadata, item.stored, item.content_type, item.key)
        if receipt.is_production and (
            metadata.provider_checksum != item.provider_checksum
            or metadata.provider_checksum_algorithm != item.provider_checksum_algorithm
        ):
            raise VerificationFailure(f"archived targeter object checksum drifted: {item.key}")


def validate_local_run(run_directory: Path, receipt: RunArchiveReceipt) -> None:
    run_directory = Path(run_directory)
    if run_directory.name != receipt.run_id:
        raise RunArchiveError("run directory does not match archive receipt run_id")
    for item in receipt.objects:
        path = run_directory / item.file
        if not path.is_file() or _identity(path) != item.stored:
            raise RunArchiveError(f"local run artifact does not match archive receipt: {item.file}")


def _text(document: dict[str, Any], field: str) -> str:
    value = document.get(field)
    if not isinstance(value, str) or not value:
        raise RunArchiveError(f"run archive receipt field {field} must be non-empty text")
    return value


def _integer(document: dict[str, Any], field: str) -> int:
    value = document.get(field)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise RunArchiveError(f"run archive receipt field {field} must be non-negative integer")
    return value


def _digest(document: dict[str, Any], field: str) -> str:
    value = _text(document, field)
    if len(value) != 64 or value != value.lower() or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise RunArchiveError(f"run archive receipt field {field} is not a SHA-256")
    return value
