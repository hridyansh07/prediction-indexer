"""Opt-in metadata-only export of retained raw archive receipts to S3."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from archive.archiver.manifest import discover_archive_receipts
from archive.archiver.universe import PUBLISHED, publish_archive_receipt_mirror
from archive.common.receipts import PRODUCTION, read_archive_receipt
from archive.storage.base import ObjectStore
from archive.storage.s3 import S3ObjectStore

CONFIG_VERSION = 1
CONFIG_ENVIRONMENT_VARIABLE = "ARCHIVE_RECEIPT_MIRROR_CONFIG"
DEFAULT_CONFIG_PATH = (
    Path(__file__).resolve().parents[2] / "configs/archive_receipt_mirror.json"
)


class ReceiptMirrorConfigError(ValueError):
    pass


@dataclass(frozen=True)
class ReceiptMirrorConfig:
    receipt_root: Path
    bucket: str
    region: str
    expected_owner: str

    def object_store(self) -> ObjectStore:
        return S3ObjectStore(self.bucket, self.region, self.expected_owner)


@dataclass
class ReceiptMirrorResult:
    discovered: int = 0
    published: int = 0
    skipped: int = 0
    failures: list[str] = field(default_factory=list)

    def as_record(self) -> dict[str, object]:
        return {
            "discovered": self.discovered,
            "published": self.published,
            "skipped": self.skipped,
            "failures": list(self.failures),
        }


def mirror_retained_receipts(
    receipt_root: Path, objects: ObjectStore
) -> ReceiptMirrorResult:
    """Mirror receipt documents only; never read local or remote raw bytes."""
    paths = discover_archive_receipts(Path(receipt_root), kind=PRODUCTION)
    result = ReceiptMirrorResult(discovered=len(paths))
    for path in paths:
        try:
            publication = publish_archive_receipt_mirror(
                objects, read_archive_receipt(path)
            )
            if publication.status == PUBLISHED:
                result.published += 1
            else:
                result.skipped += 1
        except Exception as error:  # noqa: BLE001 - preserve every failed receipt
            result.failures.append(f"{path}: {type(error).__name__}: {error}")
    return result


def load_config(path: Path | None = None) -> ReceiptMirrorConfig:
    source = Path(
        path
        or os.environ.get(CONFIG_ENVIRONMENT_VARIABLE, "")
        or DEFAULT_CONFIG_PATH
    )
    try:
        document = json.loads(source.read_text(encoding="utf-8"))
    except OSError as error:
        raise ReceiptMirrorConfigError(f"cannot read receipt mirror config {source}: {error}") from error
    except json.JSONDecodeError as error:
        raise ReceiptMirrorConfigError(f"invalid receipt mirror config {source}: {error}") from error
    document = _expand_environment(document)
    _exact(
        document,
        {"archive_receipt_mirror_config_version", "receipt_root", "archive"},
        "config",
    )
    if document["archive_receipt_mirror_config_version"] != CONFIG_VERSION:
        raise ReceiptMirrorConfigError("unsupported receipt mirror config version")
    archive = document["archive"]
    _exact(archive, {"bucket", "region", "expected_owner"}, "archive")
    assert isinstance(archive, dict)
    owner = _text(archive, "expected_owner", "archive")
    if len(owner) != 12 or not owner.isdigit():
        raise ReceiptMirrorConfigError("archive.expected_owner must be a 12-digit AWS account id")
    root = Path(_text(document, "receipt_root", "config"))
    if not root.is_absolute():
        root = (source.resolve().parent / root).resolve()
    return ReceiptMirrorConfig(
        receipt_root=root,
        bucket=_text(archive, "bucket", "archive"),
        region=_text(archive, "region", "archive"),
        expected_owner=owner,
    )


def _expand_environment(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _expand_environment(item) for key, item in value.items()}
    if not isinstance(value, str):
        return value
    expanded = os.path.expandvars(value)
    unresolved = re.search(r"\$(?:[A-Za-z_][A-Za-z0-9_]*|\{[^}]+\})", expanded)
    if unresolved is not None:
        raise ReceiptMirrorConfigError(
            f"configuration environment reference is unset: {unresolved.group(0)}"
        )
    return expanded


def _exact(value: Any, fields: set[str], label: str) -> None:
    if not isinstance(value, dict) or set(value) != fields:
        raise ReceiptMirrorConfigError(f"{label} fields are invalid")


def _text(document: dict[str, Any], field: str, label: str) -> str:
    value = document.get(field)
    if not isinstance(value, str) or not value:
        raise ReceiptMirrorConfigError(f"{label}.{field} must be non-empty text")
    return value
