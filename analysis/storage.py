from __future__ import annotations

import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def stable_job_id(specification: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        specification,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:16]


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def parse_iso8601(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def iso_to_unix_seconds(value: str | None) -> int | None:
    parsed = parse_iso8601(value)
    return int(parsed.timestamp()) if parsed else None


def fsync_directory(path: Path) -> None:
    """Makes a rename or creation inside `path` durable.

    Fsyncing a file commits its *contents*; the directory entry naming it is a
    separate object with its own dirty state. Without this, a crash can leave a
    file whose bytes are on disk under a name that is not, which for an atomic
    replace means the old name and the new one can both be absent.

    Best-effort by design: some filesystems refuse `O_RDONLY` fsync on a
    directory, and failing the write for that would be worse than the durability
    gap it is guarding against.
    """
    try:
        descriptor = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        pass
    finally:
        os.close(descriptor)


def _atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=path.parent,
        prefix=f".{path.name}.",
        delete=False,
    ) as handle:
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())
        temporary_path = Path(handle.name)
    os.replace(temporary_path, path)
    # The replace is only as durable as the directory entry recording it.
    fsync_directory(path.parent)


def write_json(path: Path, value: Any) -> None:
    _atomic_write(
        path,
        json.dumps(
            value,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
    )


def write_ndjson(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    content = "".join(
        json.dumps(
            row,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
        for row in rows
    )
    _atomic_write(path, content)


def in_time_window(
    value: str | None,
    minimum: datetime | None,
    maximum: datetime | None,
) -> bool:
    parsed = parse_iso8601(value)
    if minimum is not None and (parsed is None or parsed < minimum):
        return False
    if maximum is not None and (parsed is None or parsed > maximum):
        return False
    return True

