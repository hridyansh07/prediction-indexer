"""Shared file-to-object publication; adapters own provider behavior."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from encoder import StoredIdentity

from archive.storage.base import ObjectMetadata, ObjectStore

__all__ = ["ArchiveFile", "publish_files"]


@dataclass(frozen=True)
class ArchiveFile:
    path: Path
    key: str
    identity: StoredIdentity
    content_type: str
    content_encoding: str | None = None


def publish_files(
    store: ObjectStore, files: Iterable[ArchiveFile]
) -> tuple[ObjectMetadata, ...]:
    """Publish exact files in caller-defined order through one store adapter."""
    published = []
    for item in files:
        with item.path.open("rb") as reader:
            published.append(
                store.put_immutable(
                    item.key,
                    reader,
                    item.identity,
                    content_type=item.content_type,
                    content_encoding=item.content_encoding,
                )
            )
    return tuple(published)
