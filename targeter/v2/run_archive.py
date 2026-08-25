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
from typing import Any, Iterable

from archive.archiver.publish import ArchiveFile, publish_files
from archive.common.durable import confirm_durable, write_json_durable
from analysis.storage import decoded_zstd_file, read_json_zstd
from archive.storage.base import (
    JSON_CONTENT_TYPE,
    NDJSON_CONTENT_TYPE,
    ObjectMetadata,
    ObjectStore,
    VerificationFailure,
    provider_checksum_of,
)
from encoder import (
    CodecError,
    LogicalIdentity,
    StoredIdentity,
    logical_identity_of,
    stored_identity_of,
)
from targeter.v2.models import SUPPORTED_VENUES, parse_timestamp

RUN_MANIFEST_VERSION = 2
RUN_ARCHIVE_RECEIPT_VERSION = 3
LOCAL_RUN_ARCHIVE_RECEIPT_VERSION = 2
RUN_MANIFEST_FILE = "run_manifest.json"
PRODUCTION_RECEIPT_FILE = "archive_receipt.json"
LOCAL_RECEIPT_FILE = "archive_receipt.local.json"

SELECTION_REPORT_FILE = "selection_report.json"
SELECTION_REPORT_ZSTD_FILE = "selection_report.json.zst"
SELECTION_REPORT_METADATA_FILE = "selection_report.meta.json"

REQUIRED_FILES = frozenset({SELECTION_REPORT_METADATA_FILE})

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
    return (
        delta.days * 86_400 + delta.seconds
    ) * 1_000_000_000 + delta.microseconds * 1_000


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
    logical: LogicalIdentity | None
    content_type: str
    content_encoding: str | None
    compression: dict[str, Any] | None
    provider_checksum: str | None
    provider_checksum_algorithm: str | None


@dataclass(frozen=True)
class RunArchiveReceipt:
    kind: str
    path: Path
    run_id: str
    provider: str | None
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


def _content_metadata(name: str) -> tuple[str, str | None]:
    if name.endswith(".ndjson.zst"):
        return NDJSON_CONTENT_TYPE, "zstd"
    if name.endswith(".json.zst"):
        return JSON_CONTENT_TYPE, "zstd"
    if name.endswith(".ndjson"):
        return NDJSON_CONTENT_TYPE, None
    return JSON_CONTENT_TYPE, None


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


def read_run_report(run_directory: Path) -> dict[str, Any]:
    report_files = _report_files(run_directory)
    compressed = report_files.get(SELECTION_REPORT_ZSTD_FILE)
    if compressed is not None:
        try:
            report = read_json_zstd(
                run_directory / SELECTION_REPORT_ZSTD_FILE,
                expected_logical=LogicalIdentity.from_record(compressed["decoded"]),
                expected_stored=StoredIdentity.from_record(compressed["stored"]),
            )
        except (CodecError, OSError, ValueError) as error:
            raise RunArchiveError(
                f"invalid compressed selection report: {error}"
            ) from error
        if not isinstance(report, dict):
            raise RunArchiveError("selection report is not a JSON object")
    else:
        report = _read_json(run_directory / SELECTION_REPORT_FILE, "selection report")
    run_id = report.get("run_id")
    if not isinstance(run_id, str) or run_id != run_directory.name:
        raise RunArchiveError(
            f"selection report run_id {run_id!r} does not match directory {run_directory.name!r}"
        )
    if report.get("report_version") not in {1, 2, 3} or report.get("mode") != "shadow":
        raise RunArchiveError("selection report is not a phase-5 shadow report")
    selection = report.get("selection")
    if (
        not isinstance(selection, dict)
        or selection.get("publication_performed") is not False
    ):
        raise RunArchiveError("selection report already claims publication")
    if not isinstance(report.get("input_complete"), bool):
        raise RunArchiveError("selection report input_complete must be boolean")
    if not isinstance(report.get("discovery_failures"), dict):
        raise RunArchiveError("selection report discovery_failures must be an object")
    if parse_timestamp(report.get("generated_at")) is None:
        raise RunArchiveError("selection report generated_at is invalid")
    has_format = "artifact_format" in report
    has_inventory = "artifacts" in report
    if has_format != has_inventory:
        raise RunArchiveError(
            "selection report must carry both artifact_format and artifacts"
        )
    if has_inventory:
        _artifact_inventory(report)
    return report


def _report_files(run_directory: Path) -> dict[str, dict[str, Any] | None]:
    metadata_path = Path(run_directory) / SELECTION_REPORT_METADATA_FILE
    plain_path = Path(run_directory) / SELECTION_REPORT_FILE
    if metadata_path.is_file():
        if plain_path.exists():
            raise RunArchiveError(
                "run contains both compressed and plain selection reports"
            )
        metadata = _read_json(metadata_path, "selection report metadata")
        _require_exact_keys(
            metadata,
            {"targeter_selection_report_metadata_version", "run_id", "report"},
            "selection report metadata",
        )
        if metadata["targeter_selection_report_metadata_version"] != 1:
            raise RunArchiveError("unsupported selection report metadata version")
        if metadata.get("run_id") != Path(run_directory).name:
            raise RunArchiveError("selection report metadata names another run")
        report = metadata.get("report")
        _require_exact_keys(
            report,
            {
                "file",
                "content_type",
                "content_encoding",
                "decoded",
                "stored",
                "compression",
            },
            "selection report metadata report",
        )
        if (
            report["file"] != SELECTION_REPORT_ZSTD_FILE
            or report["content_type"] != JSON_CONTENT_TYPE
            or report["content_encoding"] != "zstd"
            or not _valid_compression(report["compression"])
        ):
            raise RunArchiveError(
                "selection report metadata has invalid content metadata"
            )
        _require_exact_keys(
            report["decoded"],
            {"sha256", "byte_length", "line_count"},
            "selection report decoded identity",
        )
        _require_exact_keys(
            report["stored"],
            {"sha256", "byte_length"},
            "selection report stored identity",
        )
        try:
            LogicalIdentity.from_record(report["decoded"])
            StoredIdentity.from_record(report["stored"])
        except CodecError as error:
            raise RunArchiveError(
                f"selection report metadata identity is invalid: {error}"
            ) from error
        if not (Path(run_directory) / SELECTION_REPORT_ZSTD_FILE).is_file():
            raise RunArchiveError("compressed selection report is missing")
        return {
            SELECTION_REPORT_ZSTD_FILE: report,
            SELECTION_REPORT_METADATA_FILE: None,
        }
    if plain_path.is_file():
        return {SELECTION_REPORT_FILE: None}
    raise RunArchiveError("selection report commit marker is missing")


def _artifact_inventory(report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    artifact_format = report.get("artifact_format")
    if artifact_format not in {"zstd", "ndjson"}:
        raise RunArchiveError("selection report artifact_format is invalid")
    raw = report.get("artifacts")
    if not isinstance(raw, dict) or not raw:
        raise RunArchiveError("selection report artifacts must be a non-empty object")
    suffix = ".ndjson.zst" if artifact_format == "zstd" else ".ndjson"
    expected = {f"rule_templates{suffix}", f"rule_drift{suffix}"}
    # Target records are written for every supported venue, not only the ones
    # that produced a catalogue: a venue that discovered nothing still has to be
    # distinguishable from a venue whose artifact went missing.
    records = {f"target_records_{venue}{suffix}" for venue in SUPPORTED_VENUES}
    # A run committed by the previous build carries an inventory naming none of
    # them, and a run is archived by whichever build is deployed when its turn
    # comes -- after an upgrade, this one. Demanding them of a report written
    # before they existed fails that run closed, permanently, for a reason no
    # retry clears. So absent as a whole set is the older inventory and is
    # accepted; a partial set is an inventory that really is incomplete and
    # still fails. `_legacy_artifact_names` carries the same tolerance for the
    # format one generation further back.
    if set(raw) & records:
        expected |= records
    catalogues = report.get("catalogs")
    if not isinstance(catalogues, list):
        raise RunArchiveError("selection report catalogs must be an array")
    for summary in catalogues:
        if not isinstance(summary, dict) or summary.get("venue") not in {
            "kalshi",
            "polymarket",
            "limitless",
        }:
            raise RunArchiveError("selection report has an invalid catalog summary")
        venue = str(summary["venue"])
        expected.update(
            {f"catalog_{venue}_events{suffix}", f"catalog_{venue}_markets{suffix}"}
        )
    if set(raw) != expected:
        raise RunArchiveError(
            "selection report artifact inventory is incomplete or unexpected"
        )
    for name, entry in raw.items():
        if not isinstance(entry, dict) or Path(name).name != name:
            raise RunArchiveError(f"selection report artifact {name!r} is invalid")
        _require_exact_keys(
            entry,
            {"content_type", "content_encoding", "decoded", "stored", "compression"},
            f"selection report artifact {name}",
        )
        content_type, content_encoding = _content_metadata(name)
        if (
            entry.get("content_type") != content_type
            or entry.get("content_encoding") != content_encoding
        ):
            raise RunArchiveError(
                f"selection report artifact {name} has invalid content metadata"
            )
        try:
            _require_exact_keys(
                entry.get("decoded"),
                {"sha256", "byte_length", "line_count"},
                f"selection report artifact {name} decoded identity",
            )
            _require_exact_keys(
                entry.get("stored"),
                {"sha256", "byte_length"},
                f"selection report artifact {name} stored identity",
            )
            LogicalIdentity.from_record(entry.get("decoded"))
            StoredIdentity.from_record(entry.get("stored"))
        except CodecError as error:
            raise RunArchiveError(
                f"selection report artifact {name} has invalid identity: {error}"
            ) from error
        compression = entry.get("compression")
        if content_encoding == "zstd":
            if not _valid_compression(compression):
                raise RunArchiveError(
                    f"selection report artifact {name} has invalid compression"
                )
        elif compression is not None:
            raise RunArchiveError(f"plain artifact {name} must not claim compression")
    return raw


def required_run_files(run_directory: Path) -> frozenset[str]:
    """The report-committed artifact inventory for one completed shadow run."""
    report = read_run_report(Path(run_directory))
    names = (
        set(_artifact_inventory(report))
        if "artifacts" in report
        else _legacy_artifact_names(report)
    )
    return frozenset({*_report_files(run_directory), *names})


def _legacy_artifact_names(report: dict[str, Any]) -> set[str]:
    names = {"rule_templates.ndjson", "rule_drift.ndjson"}
    for summary in report.get("catalogs", []):
        if not isinstance(summary, dict) or summary.get("venue") not in {
            "kalshi",
            "polymarket",
            "limitless",
        }:
            raise RunArchiveError("selection report has an invalid catalog summary")
        venue = str(summary["venue"])
        names.update(
            {f"catalog_{venue}_events.ndjson", f"catalog_{venue}_markets.ndjson"}
        )
    return names


def _valid_compression(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and set(value)
        == {
            "algorithm",
            "level",
            "frame_checksum",
            "dictionary",
            "frame_count",
            "encoder",
        }
        and value.get("algorithm") == "zstd"
        and value.get("level") == 3
        and value.get("frame_checksum") is True
        and value.get("dictionary") is None
        and value.get("frame_count") == 1
        and isinstance(value.get("encoder"), str)
        and bool(value["encoder"])
    )


def _source_files(run_directory: Path, report: dict[str, Any]) -> tuple[Path, ...]:
    inventory = _artifact_inventory(report) if "artifacts" in report else {}
    report_files = _report_files(run_directory)
    artifact_names = set(inventory) if inventory else _legacy_artifact_names(report)
    source_names = artifact_names | set(report_files)
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
            raise RunArchiveError(
                f"cannot stat run artifact {path}: {error}"
            ) from error
        if not stat.S_ISREG(mode):
            raise RunArchiveError(f"run artifact is not a regular file: {path}")
        if path.name not in source_names:
            raise RunArchiveError(f"unexpected run artifact: {path.name}")
        files.append(path)

    names = {path.name for path in files}
    artifact_missing = sorted(source_names - names)
    if artifact_missing:
        raise RunArchiveError(
            f"run is missing required artifacts: {', '.join(artifact_missing)}"
        )
    return tuple(files)


def _artifact_record(path: Path, report: dict[str, Any]) -> dict[str, Any]:
    if path.name in {SELECTION_REPORT_FILE, SELECTION_REPORT_METADATA_FILE}:
        stored = _identity(path)
        return {
            "file": path.name,
            "byte_length": stored.byte_length,
            "sha256": stored.sha256,
            "content_type": JSON_CONTENT_TYPE,
            "content_encoding": None,
        }
    if path.name == SELECTION_REPORT_ZSTD_FILE:
        entry = _report_files(path.parent)[SELECTION_REPORT_ZSTD_FILE]
        assert entry is not None
        stored = StoredIdentity.from_record(entry["stored"])
        if _identity(path) != stored:
            raise RunArchiveError("selection report stored identity drifted")
        return {"file": path.name, **entry}
    if "artifacts" not in report:
        with path.open("rb") as source:
            logical = logical_identity_of(source)
        stored = _identity(path)
        return {
            "file": path.name,
            "content_type": NDJSON_CONTENT_TYPE,
            "content_encoding": None,
            "decoded": logical.as_record(),
            "stored": stored.as_record(),
            "compression": None,
        }
    entry = _artifact_inventory(report)[path.name]
    logical = LogicalIdentity.from_record(entry["decoded"])
    stored = StoredIdentity.from_record(entry["stored"])
    if _identity(path) != stored:
        raise RunArchiveError(f"run artifact stored identity drifted: {path.name}")
    if entry["content_encoding"] == "zstd":
        try:
            with decoded_zstd_file(
                path,
                expected_logical=logical,
                expected_stored=stored,
            ):
                pass
        except (CodecError, OSError) as error:
            raise RunArchiveError(
                f"run artifact {path.name} is not valid Zstd NDJSON: {error}"
            ) from error
    else:
        with path.open("rb") as source:
            if logical_identity_of(source) != logical:
                raise RunArchiveError(
                    f"run artifact logical identity drifted: {path.name}"
                )
    return {"file": path.name, **entry}


def build_run_manifest(run_directory: Path) -> tuple[dict[str, Any], Path]:
    run_directory = Path(run_directory)
    report = read_run_report(run_directory)
    files = _source_files(run_directory, report)
    document = {
        "targeter_run_manifest_version": RUN_MANIFEST_VERSION,
        "run_id": run_directory.name,
        "generated_at": report["generated_at"],
        "input_complete": report["input_complete"],
        "files": [_artifact_record(path, report) for path in files],
    }
    path = run_directory / RUN_MANIFEST_FILE
    if path.exists():
        existing = _read_json(path, "run manifest")
        legacy = (
            _legacy_run_manifest(report, files) if "artifacts" not in report else None
        )
        if existing != document and existing != legacy:
            raise RunArchiveError(
                "existing run manifest disagrees with current run artifacts"
            )
        confirm_durable(path)
        return existing, path
    else:
        write_json_durable(path, document)
    return document, path


def _legacy_run_manifest(
    report: dict[str, Any],
    files: tuple[Path, ...],
) -> dict[str, Any]:
    """Exact version-1 manifest, for resuming a pre-upgrade interrupted archive."""
    return {
        "targeter_run_manifest_version": 1,
        "run_id": str(report["run_id"]),
        "generated_at": report["generated_at"],
        "input_complete": report["input_complete"],
        "files": [
            {
                "file": path.name,
                "byte_length": (identity := _identity(path)).byte_length,
                "sha256": identity.sha256,
                "content_type": _content_metadata(path.name)[0],
            }
            for path in files
        ],
    }


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
    report = read_run_report(run_directory)
    kind = "production" if store.durability.independent else "local"
    receipt_path = run_directory / (
        PRODUCTION_RECEIPT_FILE if kind == "production" else LOCAL_RECEIPT_FILE
    )
    if receipt_path.exists():
        receipt = read_run_archive_receipt(receipt_path)
        verify_run_archive(store, receipt)
        validate_local_run(run_directory, receipt)
        return receipt

    _manifest_document, manifest_path = build_run_manifest(run_directory)
    _date, prefix = _date_and_prefix(report, run_directory.name)
    source_files = list(_source_files(run_directory, report)) + [manifest_path]
    inventory = _artifact_inventory(report) if "artifacts" in report else {}
    report_files = _report_files(run_directory)
    prepared: list[
        tuple[ArchiveFile, LogicalIdentity | None, dict[str, Any] | None]
    ] = []
    for path in source_files:
        stored = _identity(path)
        key = f"{prefix}/{path.name}"
        content_type, content_encoding = _content_metadata(path.name)
        artifact = inventory.get(path.name) or report_files.get(path.name)
        logical = (
            LogicalIdentity.from_record(artifact["decoded"])
            if artifact is not None
            else (
                LogicalIdentity.from_record(_artifact_record(path, report)["decoded"])
                if path.name.endswith(".ndjson")
                else None
            )
        )
        compression = artifact.get("compression") if artifact is not None else None
        prepared.append(
            (
                ArchiveFile(path, key, stored, content_type, content_encoding),
                logical,
                compression,
            )
        )

    published = publish_files(
        store, (archive_file for archive_file, _logical, _compression in prepared)
    )
    uploaded: list[ArchivedRunObject] = []
    for (archive_file, logical, compression), remote in zip(
        prepared, published, strict=True
    ):
        _verify_metadata(
            remote,
            archive_file.identity,
            archive_file.content_type,
            archive_file.content_encoding,
            archive_file.key,
        )
        uploaded.append(
            _archived_object(
                archive_file.path.name,
                remote,
                logical=logical,
                compression=compression,
            )
        )

    manifest = next(item for item in uploaded if item.file == RUN_MANIFEST_FILE)
    instant = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
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
        "prefix": prefix,
        "archived_at_ns": int(instant.timestamp() * 1_000_000_000),
        "manifest": _object_record(manifest, production=kind == "production"),
        "objects": [
            _object_record(item, production=kind == "production") for item in uploaded
        ],
        "durability": store.durability.name,
        "authorizes_publication": kind == "production",
    }
    if kind == "production":
        document["store"] = {"provider": store.provider, "location": store.store_id}
    else:
        document["store"] = store.store_id
    receipt = parse_run_archive_receipt(document, path=receipt_path)
    verify_run_archive(store, receipt)
    write_json_durable(receipt_path, document)
    return receipt


def _verify_metadata(
    metadata: ObjectMetadata,
    stored: StoredIdentity,
    content_type: str,
    content_encoding: str | None,
    key: str,
) -> None:
    if not metadata.matches_request(stored, content_type, content_encoding):
        raise VerificationFailure(
            f"archived targeter object {key} failed identity verification"
        )
    if not metadata.provider_checksum or not metadata.provider_checksum_algorithm:
        raise VerificationFailure(
            f"archived targeter object {key} lacks provider checksum evidence"
        )


def _archived_object(
    name: str,
    metadata: ObjectMetadata,
    *,
    logical: LogicalIdentity | None,
    compression: dict[str, Any] | None,
) -> ArchivedRunObject:
    return ArchivedRunObject(
        file=name,
        key=metadata.key,
        stored=metadata.stored,
        logical=logical,
        content_type=metadata.content_type or "",
        content_encoding=metadata.content_encoding,
        compression=compression,
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
        "content_encoding": item.content_encoding,
    }
    if item.logical is not None:
        record["decoded"] = item.logical.as_record()
        record["compression"] = item.compression
    if production:
        record["provider_checksum"] = item.provider_checksum
        record["provider_checksum_algorithm"] = item.provider_checksum_algorithm
    return record


def read_run_archive_receipt(path: Path) -> RunArchiveReceipt:
    return parse_run_archive_receipt(
        _read_json(Path(path), "run archive receipt"), path=Path(path)
    )


def parse_run_archive_receipt(document: Any, *, path: Path) -> RunArchiveReceipt:
    if not isinstance(document, dict):
        raise RunArchiveError("run archive receipt is not an object")
    production = "targeter_run_archive_receipt_version" in document
    local = "local_targeter_run_archive_receipt_version" in document
    if production == local:
        raise RunArchiveError(
            "run archive receipt must name exactly one receipt version"
        )
    if production:
        version = document["targeter_run_archive_receipt_version"]
        if version not in {
            1,
            2,
            RUN_ARCHIVE_RECEIPT_VERSION,
        }:
            raise RunArchiveError("unsupported targeter run archive receipt version")
        if path.name != PRODUCTION_RECEIPT_FILE:
            raise RunArchiveError(
                "production run archive receipt has the wrong filename"
            )
        kind = "production"
        version_field = "targeter_run_archive_receipt_version"
        if version == 3:
            store = document.get("store")
            if not isinstance(store, dict) or set(store) != {"provider", "location"}:
                raise RunArchiveError(
                    "version-3 run archive receipt has invalid store identity"
                )
            provider = _text(store, "provider")
            location = _text(store, "location")
            location_field = "store"
        else:
            provider = None
            location_field = "bucket"
            location = _text(document, location_field)
    else:
        version = document["local_targeter_run_archive_receipt_version"]
        if version not in {
            1,
            LOCAL_RUN_ARCHIVE_RECEIPT_VERSION,
        }:
            raise RunArchiveError(
                "unsupported local targeter run archive receipt version"
            )
        if path.name != LOCAL_RECEIPT_FILE:
            raise RunArchiveError("local run archive receipt has the wrong filename")
        kind = "local"
        provider = "local"
        location_field = "store"
        version_field = "local_targeter_run_archive_receipt_version"

    _require_exact_keys(
        document,
        {
            version_field,
            "run_id",
            location_field,
            "prefix",
            "archived_at_ns",
            "manifest",
            "objects",
            "durability",
            "authorizes_publication",
        },
        "run archive receipt",
    )

    run_id = _text(document, "run_id")
    if not production:
        location = _text(document, location_field)
    prefix = _text(document, "prefix")
    archived_at_ns = _integer(document, "archived_at_ns")
    if document.get("authorizes_publication") is not production:
        raise RunArchiveError(
            "run archive receipt publication authority is inconsistent"
        )
    raw_objects = document.get("objects")
    if not isinstance(raw_objects, list) or not raw_objects:
        raise RunArchiveError("run archive receipt objects must be a non-empty array")
    objects = tuple(
        _parse_object(item, production=production, version=version)
        for item in raw_objects
    )
    if len({item.file for item in objects}) != len(objects) or len(
        {item.key for item in objects}
    ) != len(objects):
        raise RunArchiveError("run archive receipt contains duplicate files or keys")
    object_files = {item.file for item in objects}
    report_files = object_files & {SELECTION_REPORT_FILE, SELECTION_REPORT_ZSTD_FILE}
    if len(report_files) != 1:
        raise RunArchiveError(
            "run archive receipt must contain exactly one selection report"
        )
    if SELECTION_REPORT_ZSTD_FILE in report_files:
        if SELECTION_REPORT_METADATA_FILE not in object_files:
            raise RunArchiveError(
                "compressed selection report has no metadata commit marker"
            )
    elif SELECTION_REPORT_METADATA_FILE in object_files:
        raise RunArchiveError(
            "plain selection report must not have compressed-report metadata"
        )
    manifest = _parse_object(
        document.get("manifest"), production=production, version=version
    )
    matching = [item for item in objects if item.file == RUN_MANIFEST_FILE]
    if len(matching) != 1 or matching[0] != manifest:
        raise RunArchiveError(
            "run archive receipt manifest does not match its object list"
        )
    if any(not item.key.startswith(prefix + "/") for item in objects):
        raise RunArchiveError("run archive object escapes the recorded prefix")
    return RunArchiveReceipt(
        kind=kind,
        path=path,
        run_id=run_id,
        provider=provider,
        location=location,
        prefix=prefix,
        manifest=manifest,
        objects=objects,
        archived_at_ns=archived_at_ns,
        document=document,
    )


def _parse_object(
    value: Any,
    *,
    production: bool,
    version: int,
) -> ArchivedRunObject:
    if not isinstance(value, dict):
        raise RunArchiveError("run archive object entry is not an object")
    name = _text(value, "file")
    if Path(name).name != name:
        raise RunArchiveError(f"run archive object file is not a basename: {name!r}")
    key = _text(value, "key")
    content_type = _text(value, "content_type")
    expected_type, expected_encoding = _content_metadata(name)
    normalized = (
        name.endswith(".ndjson")
        or name.endswith(".ndjson.zst")
        or expected_encoding == "zstd"
    )
    expected_keys = {
        "file",
        "key",
        "byte_length",
        "sha256",
        "content_type",
        "content_encoding",
    }
    if version >= 2 and normalized:
        expected_keys.update({"decoded", "compression"})
    if production:
        expected_keys.update({"provider_checksum", "provider_checksum_algorithm"})
    _require_exact_keys(value, expected_keys, f"run archive object {name}")
    if (
        content_type != expected_type
        or value.get("content_encoding") != expected_encoding
    ):
        raise RunArchiveError(f"run archive object {name} has invalid content metadata")
    if version == 1 and expected_encoding is not None:
        raise RunArchiveError(
            "version-1 run archive receipts cannot name compressed artifacts"
        )
    stored = StoredIdentity(
        sha256=_digest(value, "sha256"), byte_length=_integer(value, "byte_length")
    )
    logical = None
    compression = None
    if version >= 2 and normalized:
        try:
            _require_exact_keys(
                value.get("decoded"),
                {"sha256", "byte_length", "line_count"},
                f"run archive object {name} decoded identity",
            )
            logical = LogicalIdentity.from_record(value.get("decoded"))
        except CodecError as error:
            raise RunArchiveError(
                f"run archive object {name} has invalid decoded identity"
            ) from error
        compression = value.get("compression")
        if expected_encoding == "zstd" and not _valid_compression(compression):
            raise RunArchiveError(f"run archive object {name} has invalid compression")
        if expected_encoding is None and compression is not None:
            raise RunArchiveError(f"plain run archive object {name} claims compression")
    elif "decoded" in value or "compression" in value:
        raise RunArchiveError(
            f"version-{version} run archive object {name} has unexpected decoded metadata"
        )
    provider_checksum = None
    provider_algorithm = None
    if production:
        provider_checksum = _text(value, "provider_checksum")
        provider_algorithm = _text(value, "provider_checksum_algorithm")
        if version < 3 and (
            provider_algorithm != "SHA256"
            or provider_checksum != provider_checksum_of(stored.sha256)
        ):
            raise RunArchiveError(
                f"run archive object {name} has invalid provider checksum"
            )
    elif "provider_checksum" in value or "provider_checksum_algorithm" in value:
        raise RunArchiveError(
            "local run archive receipt must not claim provider checksums"
        )
    return ArchivedRunObject(
        file=name,
        key=key,
        stored=stored,
        logical=logical,
        content_type=content_type,
        content_encoding=expected_encoding,
        compression=compression,
        provider_checksum=provider_checksum,
        provider_checksum_algorithm=provider_algorithm,
    )


def verify_run_archive(store: ObjectStore, receipt: RunArchiveReceipt) -> None:
    verify_run_archive_objects(store, receipt, receipt.objects)


def verify_run_archive_objects(
    store: ObjectStore,
    receipt: RunArchiveReceipt,
    objects: Iterable[ArchivedRunObject],
) -> None:
    """Freshly verify a receipt-owned subset of one committed run archive.

    Full integrity and deletion audits pass ``receipt.objects`` through
    :func:`verify_run_archive`. Replay may consume only the remote manifest and
    target-record artifacts; forcing it to head unrelated catalogues and reports
    would make intact metadata unreadable because an unrelated artifact failed.
    The local receipt remains the semantic inventory and the remote manifest the
    commit marker, so every selected object must be an exact member of that
    closed-parsed receipt.
    """
    if receipt.location != store.store_id:
        raise VerificationFailure(
            f"run archive receipt names {receipt.location!r}, not store {store.store_id!r}"
        )
    if (
        receipt.is_production
        and receipt.provider is not None
        and receipt.provider != store.provider
    ):
        raise VerificationFailure(
            f"run archive receipt names provider {receipt.provider!r}, not {store.provider!r}"
        )
    if receipt.is_production and not store.durability.independent:
        raise VerificationFailure(
            "production run archive receipt requires an independent store"
        )
    inventory = {item.key: item for item in receipt.objects}
    for item in objects:
        if inventory.get(item.key) != item:
            raise VerificationFailure(
                f"targeter object is not an exact member of the run receipt: {item.key}"
            )
        metadata = store.head(item.key)
        if metadata is None:
            raise VerificationFailure(f"archived targeter object is absent: {item.key}")
        _verify_metadata(
            metadata,
            item.stored,
            item.content_type,
            item.content_encoding,
            item.key,
        )
        if receipt.is_production and (
            metadata.provider_checksum != item.provider_checksum
            or metadata.provider_checksum_algorithm != item.provider_checksum_algorithm
        ):
            raise VerificationFailure(
                f"archived targeter object checksum drifted: {item.key}"
            )


def validate_local_run(run_directory: Path, receipt: RunArchiveReceipt) -> None:
    run_directory = Path(run_directory)
    if run_directory.name != receipt.run_id:
        raise RunArchiveError("run directory does not match archive receipt run_id")
    for item in receipt.objects:
        path = run_directory / item.file
        if not path.is_file() or _identity(path) != item.stored:
            raise RunArchiveError(
                f"local run artifact does not match archive receipt: {item.file}"
            )


def _text(document: dict[str, Any], field: str) -> str:
    value = document.get(field)
    if not isinstance(value, str) or not value:
        raise RunArchiveError(
            f"run archive receipt field {field} must be non-empty text"
        )
    return value


def _integer(document: dict[str, Any], field: str) -> int:
    value = document.get(field)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise RunArchiveError(
            f"run archive receipt field {field} must be non-negative integer"
        )
    return value


def _digest(document: dict[str, Any], field: str) -> str:
    value = _text(document, field)
    if (
        len(value) != 64
        or value != value.lower()
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise RunArchiveError(f"run archive receipt field {field} is not a SHA-256")
    return value


def _require_exact_keys(value: Any, expected: set[str], description: str) -> None:
    if not isinstance(value, dict) or set(value) != expected:
        missing = (
            sorted(expected - set(value))
            if isinstance(value, dict)
            else sorted(expected)
        )
        unexpected = sorted(set(value) - expected) if isinstance(value, dict) else []
        raise RunArchiveError(
            f"{description} has invalid fields; missing={missing}, unexpected={unexpected}"
        )
