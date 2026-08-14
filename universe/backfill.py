"""Remote, resumable control-sidecar backfill from committed S3 evidence.

The universe host never reads a capture spool or a local archive receipt.  It
discovers non-authoritative receipt mirrors published by the capture-side raw
archiver, reverifies each mirror's referenced archive objects, and streams the
compressed raw segment through the shared strict archive decoder.  Only control
lines are staged temporarily; decoded raw segments never accumulate on disk.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from archive.archiver.universe import (
    publish_segment_universe_from_archive,
    read_archive_receipt_mirror,
)
from archive.storage.base import ObjectStore
from universe.store import UniverseStore


@dataclass
class BackfillResult:
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


def backfill_segment_universe(
    *,
    objects: ObjectStore,
    database: UniverseStore,
    temp_root: Path | None = None,
    now_ns: Callable[[], int] = time.time_ns,
) -> BackfillResult:
    """Publish every missing control derivative from remote receipt mirrors."""
    result = BackfillResult()
    try:
        keys = sorted(
            key
            for key in objects.list_keys("raw/")
            if key.endswith(".archive-receipt-mirror.json")
        )
    except Exception as error:  # noqa: BLE001 - discovery failure belongs in the report
        result.failures.append(f"raw/: {type(error).__name__}: {error}")
        return result

    result.discovered = len(keys)
    checkpoint_name = f"raw-universe-backfill:{objects.store_id}"
    for key in keys:
        try:
            mirror = read_archive_receipt_mirror(objects, key)
            publication = publish_segment_universe_from_archive(
                objects,
                mirror,
                now_ns=now_ns(),
                temp_root=temp_root,
            )
            if publication.status == "published":
                result.published += 1
            else:
                result.skipped += 1
            database.set_checkpoint(checkpoint_name, key)
        except Exception as error:  # noqa: BLE001 - preserve every failed mirror in the report
            result.failures.append(f"{key}: {type(error).__name__}: {error}")
    return result
