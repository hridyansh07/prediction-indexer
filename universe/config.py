"""Closed, environment-expandable configuration for Event Universe jobs."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from eth_utils import is_checksum_address

from archive.storage.base import ObjectStore, normalize_key
from archive.storage.factory import build_store
from targeter.v2.models import isoformat, parse_timestamp

CONFIG_VERSION = 3
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
class AuthConfig:
    siwe_domain: str
    siwe_uri: str
    siwe_statement: str
    chain_id: int
    admin_address: str
    nonce_ttl_seconds: int
    session_ttl_seconds: int


@dataclass(frozen=True)
class ReplayJobsConfig:
    runner_config_path: Path
    max_active_jobs_total: int
    max_active_jobs_per_submitter: int
    max_queued_jobs_total: int


@dataclass(frozen=True)
class ReplayConfig:
    database_path: Path
    auth: AuthConfig
    jobs: ReplayJobsConfig


@dataclass(frozen=True)
class UniverseConfig:
    path: Path
    database_path: Path
    api: ApiConfig
    backfill: BackfillConfig
    backup: BackupConfig
    replay: ReplayConfig

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
    if not isinstance(document, dict) or document.get("event_universe_config_version") != CONFIG_VERSION:
        raise UniverseConfigError("unsupported Event Universe config version; version 3 is required")
    _exact(
        document,
        {
            "event_universe_config_version",
            "database_path",
            "api",
            "backfill",
            "backup",
            "replay",
        },
        "config",
    )
    api = _section(document, "api", {"host", "port"})
    backfill = _section(
        document,
        "backfill",
        {"temporary_directory", "generated_start", "generated_end"},
    )
    backup = _section(document, "backup", {"directory", "object_prefix"})
    replay = _section(document, "replay", {"database_path", "auth", "jobs"})
    auth = replay.get("auth")
    _exact(
        auth,
        {
            "siwe_domain",
            "siwe_uri",
            "siwe_statement",
            "chain_id",
            "admin_address",
            "nonce_ttl_seconds",
            "session_ttl_seconds",
        },
        "replay.auth",
    )
    assert isinstance(auth, dict)
    jobs = replay.get("jobs")
    _exact(
        jobs,
        {
            "runner_config_path",
            "max_active_jobs_total",
            "max_active_jobs_per_submitter",
            "max_queued_jobs_total",
        },
        "replay.jobs",
    )
    assert isinstance(jobs, dict)
    port = api["port"]
    if not isinstance(port, int) or isinstance(port, bool) or not 1 <= port <= 65535:
        raise UniverseConfigError("api.port must be an integer between 1 and 65535")
    object_prefix = normalize_key(_text(backup, "object_prefix", "backup").rstrip("/"))
    base = source.resolve().parent
    domain = _text(auth, "siwe_domain", "replay.auth")
    try:
        parsed_domain = urlsplit(f"//{domain}")
        domain_port_valid = parsed_domain.port is None or parsed_domain.port > 0
    except ValueError:
        domain_port_valid = False
        parsed_domain = urlsplit("//invalid")
    if (
        "://" in domain
        or any(character.isspace() or ord(character) < 32 for character in domain)
        or not parsed_domain.netloc
        or parsed_domain.hostname is None
        or parsed_domain.path
        or parsed_domain.query
        or parsed_domain.fragment
        or parsed_domain.username is not None
        or not domain_port_valid
    ):
        raise UniverseConfigError("replay.auth.siwe_domain must be an authority without scheme or path")
    uri = _text(auth, "siwe_uri", "replay.auth")
    try:
        parsed_uri = urlsplit(uri)
        uri_port_valid = parsed_uri.port is None or parsed_uri.port > 0
    except ValueError:
        uri_port_valid = False
        parsed_uri = urlsplit("invalid:")
    if (
        parsed_uri.scheme not in {"http", "https"}
        or not parsed_uri.netloc
        or parsed_uri.hostname is None
        or parsed_uri.username is not None
        or parsed_uri.query
        or parsed_uri.fragment
        or parsed_uri.geturl() != uri
        or any(character.isspace() or ord(character) < 32 for character in uri)
        or not uri_port_valid
    ):
        raise UniverseConfigError("replay.auth.siwe_uri must be a canonical absolute HTTP(S) URI")
    statement = _text(auth, "siwe_statement", "replay.auth")
    if len(statement) > 256 or any(not 32 <= ord(character) <= 126 for character in statement):
        raise UniverseConfigError(
            "replay.auth.siwe_statement must be at most 256 printable ASCII characters"
        )
    chain_id = _positive_integer(auth, "chain_id", "replay.auth")
    nonce_ttl = _bounded_ttl(auth, "nonce_ttl_seconds")
    session_ttl = _bounded_ttl(auth, "session_ttl_seconds")
    max_active_total = _positive_integer(jobs, "max_active_jobs_total", "replay.jobs")
    max_active_submitter = _positive_integer(
        jobs, "max_active_jobs_per_submitter", "replay.jobs"
    )
    max_queued_total = _positive_integer(jobs, "max_queued_jobs_total", "replay.jobs")
    if max_queued_total > max_active_total:
        raise UniverseConfigError(
            "replay.jobs.max_queued_jobs_total must not exceed max_active_jobs_total"
        )
    admin_address = _text(auth, "admin_address", "replay.auth")
    if not is_checksum_address(admin_address):
        raise UniverseConfigError("replay.auth.admin_address must be a valid EIP-55 address")
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
        replay=ReplayConfig(
            database_path=_path(replay, "database_path", base, "replay"),
            auth=AuthConfig(
                siwe_domain=domain,
                siwe_uri=uri,
                siwe_statement=statement,
                chain_id=chain_id,
                admin_address=admin_address,
                nonce_ttl_seconds=nonce_ttl,
                session_ttl_seconds=session_ttl,
            ),
            jobs=ReplayJobsConfig(
                runner_config_path=_path(
                    jobs, "runner_config_path", base, "replay.jobs"
                ),
                max_active_jobs_total=max_active_total,
                max_active_jobs_per_submitter=max_active_submitter,
                max_queued_jobs_total=max_queued_total,
            ),
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


def _positive_integer(document: dict[str, Any], field: str, label: str) -> int:
    value = document.get(field)
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise UniverseConfigError(f"{label}.{field} must be a positive integer")
    return value


def _bounded_ttl(document: dict[str, Any], field: str) -> int:
    value = _positive_integer(document, field, "replay.auth")
    if value > 604_800:
        raise UniverseConfigError(f"replay.auth.{field} must not exceed 604800")
    return value
