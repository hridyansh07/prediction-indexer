"""Public, provider-neutral parser for committed Targeter run manifests."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import PurePath
from typing import Any, Mapping

from archive.storage.base import ObjectExpectation
from encoder import LogicalIdentity, StoredIdentity
from targeter.v2.models import parse_timestamp

RUN_MANIFEST_VERSION = 2


class RunManifestError(ValueError):
    """A run manifest does not satisfy the Targeter v2 commit contract."""


@dataclass(frozen=True)
class RunObject:
    file: str
    key: str
    stored: StoredIdentity
    logical: LogicalIdentity | None
    content_type: str
    content_encoding: str | None

    def expectation(self) -> ObjectExpectation:
        return ObjectExpectation(
            self.key,
            self.stored,
            None,
            None,
            self.content_type,
            self.content_encoding,
        )


@dataclass(frozen=True)
class RunManifest:
    key: str
    run_id: str
    generated_at: str
    input_complete: bool
    objects: tuple[RunObject, ...]


def parse_run_manifest(document: Any, *, key: str) -> RunManifest:
    """Parse one version-2 commit marker without retrieving provider bytes."""
    if not isinstance(document, dict) or set(document) != {
        "targeter_run_manifest_version",
        "run_id",
        "generated_at",
        "input_complete",
        "files",
    }:
        raise RunManifestError(f"run manifest {key} has invalid fields")
    if document["targeter_run_manifest_version"] != RUN_MANIFEST_VERSION:
        raise RunManifestError(f"run manifest {key} is not version 2")
    run_id = _text(document, "run_id", "run manifest")
    if manifest_run_id(key) != run_id:
        raise RunManifestError(f"run manifest key does not match run_id {run_id}")
    generated_at = _text(document, "generated_at", "run manifest")
    generated = parse_timestamp(generated_at)
    if generated is None or generated != manifest_run_instant(key):
        raise RunManifestError(
            f"run {run_id} generated_at disagrees with its run id"
        )
    input_complete = document["input_complete"]
    if not isinstance(input_complete, bool):
        raise RunManifestError("run manifest input_complete must be boolean")
    files = document["files"]
    if not isinstance(files, list) or not files:
        raise RunManifestError("run manifest files must be a non-empty array")
    prefix = key.rsplit("/", 1)[0]
    objects = tuple(_parse_object(prefix, value) for value in files)
    names = [item.file for item in objects]
    if len(names) != len(set(names)):
        raise RunManifestError(f"run manifest {key} repeats an object")
    return RunManifest(key, run_id, generated_at, input_complete, objects)


def manifest_run_id(key: str) -> str:
    parts = key.split("/")
    if (
        len(parts) != 5
        or parts[:2] != ["targeter-v2", "runs"]
        or not parts[2].startswith("date=")
        or not parts[3].startswith("run=")
        or parts[4] != "run_manifest.json"
    ):
        raise RunManifestError(f"invalid Targeter run manifest key: {key}")
    run_id = parts[3].removeprefix("run=")
    instant = _run_id_instant(run_id)
    if instant.date().isoformat() != parts[2].removeprefix("date="):
        raise RunManifestError(
            f"Targeter run manifest date disagrees with run id: {key}"
        )
    return run_id


def manifest_run_instant(key: str) -> datetime:
    return _run_id_instant(manifest_run_id(key))


def _parse_object(prefix: str, value: Any) -> RunObject:
    if not isinstance(value, dict):
        raise RunManifestError("run manifest object must be an object")
    name = _text(value, "file", "run manifest object")
    if PurePath(name).name != name:
        raise RunManifestError(f"run manifest object is not a basename: {name!r}")
    rich = name.endswith((".ndjson", ".ndjson.zst", ".json.zst"))
    expected = (
        {
            "file",
            "content_type",
            "content_encoding",
            "decoded",
            "stored",
            "compression",
        }
        if rich
        else {
            "file",
            "byte_length",
            "sha256",
            "content_type",
            "content_encoding",
        }
    )
    if set(value) != expected:
        raise RunManifestError(f"run object {name} has invalid fields")
    content_type = _text(value, "content_type", f"run object {name}")
    encoding = value.get("content_encoding")
    if encoding not in {None, "zstd"}:
        raise RunManifestError(f"run object {name} has invalid content encoding")
    if rich:
        stored_record = value.get("stored")
        logical_record = value.get("decoded")
        if not isinstance(stored_record, dict) or not isinstance(logical_record, dict):
            raise RunManifestError(f"run object {name} has invalid identities")
        stored = _stored(stored_record, f"run object {name} stored")
        logical = _logical(logical_record, f"run object {name} decoded")
        if name.endswith(".zst") != (encoding == "zstd"):
            raise RunManifestError(f"run object {name} suffix and encoding disagree")
        compression = value.get("compression")
        if encoding == "zstd":
            if not _valid_compression(compression):
                raise RunManifestError(f"run object {name} has invalid compression")
        elif compression is not None:
            raise RunManifestError(f"plain run object {name} claims compression")
    else:
        stored = _stored(value, f"run object {name}")
        logical = None
    return RunObject(name, f"{prefix}/{name}", stored, logical, content_type, encoding)


def _run_id_instant(run_id: str) -> datetime:
    try:
        instant = datetime.strptime(run_id, "%Y%m%dT%H%M%S.%fZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError as error:
        raise RunManifestError(f"invalid Targeter run id: {run_id}") from error
    if instant.strftime("%Y%m%dT%H%M%S.%fZ") != run_id:
        raise RunManifestError(f"non-canonical Targeter run id: {run_id}")
    return instant


def _stored(document: Mapping[str, Any], label: str) -> StoredIdentity:
    return StoredIdentity(
        _digest(document.get("sha256"), label),
        _integer(document.get("byte_length"), label),
    )


def _logical(document: Mapping[str, Any], label: str) -> LogicalIdentity:
    if set(document) != {"sha256", "byte_length", "line_count"}:
        raise RunManifestError(f"{label} has invalid identity fields")
    return LogicalIdentity(
        _digest(document.get("sha256"), label),
        _integer(document.get("byte_length"), label),
        _integer(document.get("line_count"), label),
    )


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


def _text(document: Mapping[str, Any], field: str, label: str) -> str:
    value = document.get(field)
    if not isinstance(value, str) or not value:
        raise RunManifestError(f"{label} {field} must be non-empty text")
    return value


def _integer(value: Any, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise RunManifestError(f"{label} byte count must be a non-negative integer")
    return value


def _digest(value: Any, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or value != value.lower()
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise RunManifestError(f"{label} SHA-256 is invalid")
    return value
