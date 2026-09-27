"""Consistent immutable backup and verified restore for the Replay jobs database."""

from __future__ import annotations

import hashlib
import io
import json
import os
import re
import secrets
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from archive.storage.base import (
    JSON_CONTENT_TYPE,
    ObjectExpectation,
    ObjectStoreError,
    VerificationFailure,
    normalize_key,
)
from encoder import StoredIdentity

SQLITE_CONTENT_TYPE = "application/vnd.sqlite3"
BACKUP_VERSION = 1
MAX_RECEIPT_BYTES = 64 * 1024
_HASH = re.compile(r"[0-9a-f]{64}\Z")


class BackupError(RuntimeError):
    pass


@dataclass(frozen=True)
class BackupReceipt:
    source: str
    backup_type: str
    created_at_ns: int
    object_key: str
    sha256: str
    byte_length: int
    store_id: str
    provider: str
    provider_checksum: str
    provider_checksum_algorithm: str
    integrity_check: str
    receipt_key: str = ""

    def document(self) -> dict[str, object]:
        return {
            "replay_jobs_backup_version": BACKUP_VERSION,
            "source": self.source,
            "backup_type": self.backup_type,
            "created_at_ns": str(self.created_at_ns),
            "object_key": self.object_key,
            "sha256": self.sha256,
            "byte_length": self.byte_length,
            "store_id": self.store_id,
            "provider": self.provider,
            "provider_checksum": self.provider_checksum,
            "provider_checksum_algorithm": self.provider_checksum_algorithm,
            "integrity_check": self.integrity_check,
        }

    def bytes(self) -> bytes:
        return json.dumps(
            self.document(), ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")


def _identity(path: Path) -> StoredIdentity:
    digest = hashlib.sha256()
    length = 0
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
            length += len(chunk)
    return StoredIdentity(digest.hexdigest(), length)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _integrity(path: Path) -> None:
    try:
        with closing(sqlite3.connect(f"file:{path}?mode=ro", uri=True)) as connection:
            rows = connection.execute("PRAGMA integrity_check").fetchall()
    except sqlite3.DatabaseError as error:
        raise BackupError("backup is not a readable SQLite database") from error
    if rows != [("ok",)]:
        raise BackupError("backup failed SQLite integrity_check")


def _online_copy(source: Path, destination: Path) -> None:
    source = Path(source)
    if not source.is_file() or source.is_symlink():
        raise BackupError("jobs database must be a regular file")
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(
        f".{destination.name}.{os.getpid()}.{secrets.token_hex(4)}.open"
    )
    try:
        with closing(
            sqlite3.connect(f"file:{source}?mode=ro", uri=True, timeout=30)
        ) as reader, closing(sqlite3.connect(temporary)) as writer:
            reader.backup(writer)
            writer.commit()
        _integrity(temporary)
        with temporary.open("rb") as handle:
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
        _fsync_directory(destination.parent)
    except (OSError, sqlite3.DatabaseError) as error:
        raise BackupError("SQLite online backup failed") from error
    finally:
        temporary.unlink(missing_ok=True)


def _expectation(metadata) -> ObjectExpectation:
    if not metadata.provider_checksum or not metadata.provider_checksum_algorithm:
        raise BackupError("backup object lacks provider checksum metadata")
    return ObjectExpectation(
        metadata.key,
        metadata.stored,
        metadata.provider_checksum,
        metadata.provider_checksum_algorithm,
        metadata.content_type,
        metadata.content_encoding,
    )


def _consume_verified(store, metadata, target=None) -> None:
    with store.open_verified(_expectation(metadata)) as source:
        while chunk := source.read(1024 * 1024):
            if target is not None:
                target.write(chunk)


def _timestamp(value: int) -> str:
    seconds, nanos = divmod(value, 1_000_000_000)
    moment = datetime.fromtimestamp(seconds, timezone.utc)
    return moment.strftime("%Y%m%dT%H%M%S") + f".{nanos:09d}Z"


def backup_jobs_database(
    source: Path,
    staging_root: Path,
    store,
    *,
    prefix: str,
    created_at_ns: int,
) -> BackupReceipt:
    """Use SQLite's online backup API, publish immutable bytes, then a receipt."""
    if type(created_at_ns) is not int or created_at_ns < 0:
        raise BackupError("created_at_ns must be a nonnegative integer")
    prefix = normalize_key(prefix.rstrip("/"))
    operational_prefixes = (
        "replay/jobs",
        "replay/bundles",
        "replay/derivatives",
    )
    if any(
        prefix == item
        or prefix.startswith(item + "/")
        or item.startswith(prefix + "/")
        for item in operational_prefixes
    ):
        raise BackupError("backup prefix must be separate from Replay object prefixes")
    staging_root = Path(staging_root)
    staging_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    stage = staging_root / f"jobs-{_timestamp(created_at_ns)}.sqlite3"
    try:
        _online_copy(Path(source), stage)
        identity = _identity(stage)
        stem = f"jobs-{_timestamp(created_at_ns)}-{identity.sha256}"
        object_key = f"{prefix}/{stem}.sqlite3"
        receipt_key = f"{prefix}/{stem}.receipt.json"
        with stage.open("rb") as reader:
            metadata = store.put_immutable(
                object_key,
                reader,
                identity,
                content_type=SQLITE_CONTENT_TYPE,
            )
        if not metadata.matches(identity):
            raise BackupError("published backup metadata disagrees with source identity")
        _consume_verified(store, metadata)
        receipt = BackupReceipt(
            "jobs.sqlite3",
            "sqlite_online_backup",
            created_at_ns,
            object_key,
            identity.sha256,
            identity.byte_length,
            store.store_id,
            store.provider,
            metadata.provider_checksum,
            metadata.provider_checksum_algorithm,
            "ok",
            receipt_key,
        )
        raw = receipt.bytes()
        receipt_identity = StoredIdentity(hashlib.sha256(raw).hexdigest(), len(raw))
        receipt_metadata = store.put_immutable(
            receipt_key,
            io.BytesIO(raw),
            receipt_identity,
            content_type=JSON_CONTENT_TYPE,
        )
        _consume_verified(store, receipt_metadata)
        return receipt
    except (ObjectStoreError, VerificationFailure, OSError) as error:
        raise BackupError("immutable backup publication or verification failed") from error
    finally:
        stage.unlink(missing_ok=True)


def _parse_receipt(raw: bytes, receipt_key: str) -> BackupReceipt:
    if len(raw) > MAX_RECEIPT_BYTES:
        raise BackupError("backup receipt exceeds byte limit")
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise BackupError("backup receipt is invalid JSON") from error
    expected = set(BackupReceipt("", "", 0, "", "", 0, "", "", "", "", "").document())
    if type(value) is not dict or set(value) != expected:
        raise BackupError("backup receipt has an invalid closed schema")
    try:
        receipt = BackupReceipt(
            source=value["source"],
            backup_type=value["backup_type"],
            created_at_ns=int(value["created_at_ns"]),
            object_key=normalize_key(value["object_key"]),
            sha256=value["sha256"],
            byte_length=value["byte_length"],
            store_id=value["store_id"],
            provider=value["provider"],
            provider_checksum=value["provider_checksum"],
            provider_checksum_algorithm=value["provider_checksum_algorithm"],
            integrity_check=value["integrity_check"],
            receipt_key=receipt_key,
        )
    except (TypeError, ValueError, KeyError) as error:
        raise BackupError("backup receipt fields are invalid") from error
    if (
        receipt.source != "jobs.sqlite3"
        or receipt.backup_type != "sqlite_online_backup"
        or type(value["created_at_ns"]) is not str
        or str(receipt.created_at_ns) != value["created_at_ns"]
        or receipt.created_at_ns < 0
        or _HASH.fullmatch(receipt.sha256) is None
        or type(receipt.byte_length) is not int
        or isinstance(receipt.byte_length, bool)
        or receipt.byte_length <= 0
        or not all(
            type(item) is str and item
            for item in (
                receipt.store_id,
                receipt.provider,
                receipt.provider_checksum,
                receipt.provider_checksum_algorithm,
            )
        )
        or receipt.integrity_check != "ok"
        or receipt.bytes() != raw
    ):
        raise BackupError("backup receipt fields are invalid")
    return receipt


def verify_backup(store, receipt_key: str) -> BackupReceipt:
    receipt_key = normalize_key(receipt_key)
    if not receipt_key.endswith(".receipt.json"):
        raise BackupError("backup verification requires a receipt key")
    try:
        metadata = store.head(receipt_key)
        if metadata is None or metadata.byte_length > MAX_RECEIPT_BYTES:
            raise BackupError("backup receipt is absent or oversized")
        buffer = io.BytesIO()
        _consume_verified(store, metadata, buffer)
        receipt = _parse_receipt(buffer.getvalue(), receipt_key)
        if receipt.store_id != store.store_id or receipt.provider != store.provider:
            raise BackupError("backup receipt belongs to another object store")
        data = store.head(receipt.object_key)
        expected = StoredIdentity(receipt.sha256, receipt.byte_length)
        if (
            data is None
            or not data.matches(expected)
            or data.content_type != SQLITE_CONTENT_TYPE
            or data.content_encoding is not None
            or data.provider_checksum != receipt.provider_checksum
            or data.provider_checksum_algorithm != receipt.provider_checksum_algorithm
        ):
            raise BackupError("backup object metadata disagrees with its receipt")
        _consume_verified(store, data)
        return receipt
    except BackupError:
        raise
    except (ObjectStoreError, VerificationFailure, OSError) as error:
        raise BackupError("backup verification failed") from error


def restore_jobs_database(store, receipt_key: str, destination: Path) -> Path:
    """Restore to a new path only; stopping ingress/runner is an operator gate."""
    receipt = verify_backup(store, receipt_key)
    destination = Path(destination)
    if destination.exists() or destination.with_name(destination.name + "-wal").exists() or destination.with_name(destination.name + "-shm").exists():
        raise BackupError("restore destination and SQLite sidecars must not exist")
    destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{os.getpid()}.open")
    metadata = store.head(receipt.object_key)
    assert metadata is not None
    try:
        with temporary.open("xb") as writer:
            _consume_verified(store, metadata, writer)
            writer.flush()
            os.fsync(writer.fileno())
        if _identity(temporary) != StoredIdentity(receipt.sha256, receipt.byte_length):
            raise BackupError("restored bytes disagree with receipt")
        _integrity(temporary)
        os.chmod(temporary, 0o600)
        os.replace(temporary, destination)
        _fsync_directory(destination.parent)
        return destination
    except BackupError:
        raise
    except (ObjectStoreError, VerificationFailure, OSError) as error:
        raise BackupError("backup restore failed") from error
    finally:
        temporary.unlink(missing_ok=True)
