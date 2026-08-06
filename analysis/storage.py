from __future__ import annotations

import hashlib
import json
import os
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, BinaryIO, Iterable, Iterator, Mapping

from archive.common.durable import fsync_directory as fsync_directory_strict
from encoder import EncodeResult, LogicalIdentity, StoredIdentity, decode_stream, encode_stream


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
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            delete=False,
        ) as handle:
            temporary_path = Path(handle.name)
            for row in rows:
                json.dump(
                    row,
                    handle,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                )
                handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
        temporary_path = None
        fsync_directory_strict(path.parent)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def write_ndjson_zstd(path: Path, rows: Iterable[Mapping[str, Any]]) -> EncodeResult:
    """Durably write exact NDJSON inside the repository's one-frame Zstd profile."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded_path: Path | None = None
    try:
        with tempfile.TemporaryFile(
            mode="w+",
            encoding="utf-8",
            dir=path.parent,
        ) as raw:
            for row in rows:
                json.dump(
                    row,
                    raw,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                )
                raw.write("\n")
            raw.flush()
            os.fsync(raw.fileno())
            with open(os.dup(raw.fileno()), "rb", closefd=True) as source:
                source.seek(0)
                with tempfile.NamedTemporaryFile(
                    mode="w+b",
                    dir=path.parent,
                    prefix=f".{path.name}.",
                    delete=False,
                ) as encoded:
                    encoded_path = Path(encoded.name)
                    result = encode_stream(source, encoded)
                    encoded.flush()
                    os.fsync(encoded.fileno())
        os.replace(encoded_path, path)
        encoded_path = None
        fsync_directory_strict(path.parent)
        return result
    finally:
        if encoded_path is not None:
            encoded_path.unlink(missing_ok=True)


@contextmanager
def decoded_zstd_file(
    path: Path,
    *,
    expected_logical: LogicalIdentity,
    expected_stored: StoredIdentity,
) -> Iterator[BinaryIO]:
    """Stage one strictly verified frame on disk and yield its decoded bytes."""
    with Path(path).open("rb") as source, tempfile.TemporaryFile(mode="w+b") as decoded:
        decode_stream(
            source,
            decoded,
            expected_logical=expected_logical,
            expected_stored=expected_stored,
            max_decoded_bytes=expected_logical.byte_length,
        )
        decoded.seek(0)
        yield decoded


def write_json_zstd(path: Path, value: Any) -> EncodeResult:
    """Durably write canonical JSON plus LF through the shared streaming encoder."""
    return write_ndjson_zstd(path, (value,))


def read_json_zstd(
    path: Path,
    *,
    expected_logical: LogicalIdentity,
    expected_stored: StoredIdentity,
) -> Any:
    with decoded_zstd_file(
        path,
        expected_logical=expected_logical,
        expected_stored=expected_stored,
    ) as decoded:
        return json.load(decoded)


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
