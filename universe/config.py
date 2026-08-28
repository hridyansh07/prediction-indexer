"""Closed, environment-expandable configuration for Event Universe jobs."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from archive.storage.base import ObjectStore, normalize_key
from archive.storage.factory import build_store
from targeter.v2.models import isoformat, parse_timestamp

CONFIG_VERSION = 1
CONFIG_ENVIRONMENT_VARIABLE = "EVENT_UNIVERSE_CONFIG"
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[1] / "configs/event_universe.json"


class UniverseConfigError(ValueError):
    pass


@dataclass(frozen=True)
class ApiConfig:
    host: str
    port: int


@dataclass(frozen=True)
class BackupConfig:
    directory: Path
    object_prefix: str


@dataclass(frozen=True)
class BackfillConfig:
    temporary_directory: Path
    generated_start: datetime | None
    generated_end: datetime | None


@dataclass(frozen=True)
class UniverseConfig:
    path: Path
    database_path: Path
    api: ApiConfig
    backfill: BackfillConfig
    backup: BackupConfig

    @property
    def temporary_directory(self) -> Path:
        return self.backfill.temporary_directory

    def object_store(self) -> ObjectStore:
        return build_store((self.database_path.parent,))


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
            "api",
            "backfill",
            "backup",
        },
        "config",
    )
    if document["event_universe_config_version"] != CONFIG_VERSION:
        raise UniverseConfigError("unsupported Event Universe config version")

    api = _section(document, "api", {"host", "port"})
    backfill = _section(
        document,
        "backfill",
        {"temporary_directory", "generated_start", "generated_end"},
    )
    backup = _section(document, "backup", {"directory", "object_prefix"})
    port = api["port"]
    if not isinstance(port, int) or isinstance(port, bool) or not 1 <= port <= 65535:
        raise UniverseConfigError("api.port must be an integer between 1 and 65535")
    object_prefix = normalize_key(_text(backup, "object_prefix", "backup").rstrip("/"))
    base = source.resolve().parent
    generated_start = _optional_timestamp(
        backfill.get("generated_start"), "backfill.generated_start"
    )
    generated_end = _optional_timestamp(
        backfill.get("generated_end"), "backfill.generated_end"
    )
    if (
        generated_start is not None
        and generated_end is not None
        and generated_start >= generated_end
    ):
        raise UniverseConfigError(
            "backfill.generated_start must be before backfill.generated_end"
        )
    return UniverseConfig(
        path=source.resolve(),
        database_path=_path(document, "database_path", base, "config"),
        api=ApiConfig(
            host=_text(api, "host", "api"),
            port=port,
        ),
        backfill=BackfillConfig(
            temporary_directory=_path(
                backfill, "temporary_directory", base, "backfill"
            ),
            generated_start=generated_start,
            generated_end=generated_end,
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


def _optional_timestamp(value: Any, label: str) -> datetime | None:
    if value is None:
        return None
    parsed = parse_timestamp(value)
    if parsed is None or value != isoformat(parsed):
        raise UniverseConfigError(f"{label} must be null or a canonical UTC timestamp")
    return parsed
