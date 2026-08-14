"""Closed, environment-expandable configuration for Event Universe jobs."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from archive.storage.base import ObjectStore, normalize_key
from archive.storage.s3 import S3ObjectStore

CONFIG_VERSION = 1
CONFIG_ENVIRONMENT_VARIABLE = "EVENT_UNIVERSE_CONFIG"
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[1] / "configs/event_universe.json"


class UniverseConfigError(ValueError):
    pass


@dataclass(frozen=True)
class ArchiveConfig:
    bucket: str
    region: str
    expected_owner: str


@dataclass(frozen=True)
class ApiConfig:
    host: str
    port: int


@dataclass(frozen=True)
class BackupConfig:
    directory: Path
    object_prefix: str


@dataclass(frozen=True)
class UniverseConfig:
    path: Path
    database_path: Path
    archive: ArchiveConfig
    api: ApiConfig
    temporary_directory: Path
    backup: BackupConfig

    def object_store(self) -> ObjectStore:
        return S3ObjectStore(
            self.archive.bucket,
            self.archive.region,
            self.archive.expected_owner,
        )


def load_config(path: Path | None = None) -> UniverseConfig:
    source = Path(
        path
        or os.environ.get(CONFIG_ENVIRONMENT_VARIABLE, "")
        or DEFAULT_CONFIG_PATH
    )
    try:
        document = json.loads(source.read_text(encoding="utf-8"))
    except OSError as error:
        raise UniverseConfigError(f"cannot read Event Universe config {source}: {error}") from error
    except json.JSONDecodeError as error:
        raise UniverseConfigError(f"invalid Event Universe config {source}: {error}") from error
    document = _expand_environment(document)
    _exact(
        document,
        {
            "event_universe_config_version",
            "database_path",
            "archive",
            "api",
            "backfill",
            "backup",
        },
        "config",
    )
    if document["event_universe_config_version"] != CONFIG_VERSION:
        raise UniverseConfigError("unsupported Event Universe config version")

    archive = _section(
        document, "archive", {"bucket", "region", "expected_owner"}
    )
    api = _section(document, "api", {"host", "port"})
    backfill = _section(document, "backfill", {"temporary_directory"})
    backup = _section(document, "backup", {"directory", "object_prefix"})
    port = api["port"]
    if not isinstance(port, int) or isinstance(port, bool) or not 1 <= port <= 65535:
        raise UniverseConfigError("api.port must be an integer between 1 and 65535")
    expected_owner = _text(archive, "expected_owner", "archive")
    if len(expected_owner) != 12 or not expected_owner.isdigit():
        raise UniverseConfigError("archive.expected_owner must be a 12-digit AWS account id")
    object_prefix = normalize_key(_text(backup, "object_prefix", "backup").rstrip("/"))
    base = source.resolve().parent
    return UniverseConfig(
        path=source.resolve(),
        database_path=_path(document, "database_path", base, "config"),
        archive=ArchiveConfig(
            bucket=_text(archive, "bucket", "archive"),
            region=_text(archive, "region", "archive"),
            expected_owner=expected_owner,
        ),
        api=ApiConfig(
            host=_text(api, "host", "api"),
            port=port,
        ),
        temporary_directory=_path(
            backfill, "temporary_directory", base, "backfill"
        ),
        backup=BackupConfig(
            directory=_path(backup, "directory", base, "backup"),
            object_prefix=object_prefix,
        ),
    )


def _expand_environment(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _expand_environment(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_expand_environment(item) for item in value]
    if not isinstance(value, str):
        return value
    expanded = os.path.expandvars(value)
    unresolved = re.search(r"\$(?:[A-Za-z_][A-Za-z0-9_]*|\{[^}]+\})", expanded)
    if unresolved is not None:
        raise UniverseConfigError(
            f"configuration environment reference is unset: {unresolved.group(0)}"
        )
    return expanded


def _exact(value: Any, fields: set[str], label: str) -> None:
    if not isinstance(value, dict) or set(value) != fields:
        raise UniverseConfigError(f"{label} fields are invalid")


def _section(
    document: dict[str, Any], field: str, fields: set[str]
) -> dict[str, Any]:
    value = document.get(field)
    _exact(value, fields, field)
    assert isinstance(value, dict)
    return value


def _text(document: dict[str, Any], field: str, label: str) -> str:
    value = document.get(field)
    if not isinstance(value, str) or not value:
        raise UniverseConfigError(f"{label}.{field} must be non-empty text")
    return value


def _path(document: dict[str, Any], field: str, base: Path, label: str) -> Path:
    value = Path(_text(document, field, label))
    return value if value.is_absolute() else (base / value).resolve()
