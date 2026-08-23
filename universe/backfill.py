"""Bounded Targeter v3 history backfill through the normal projector."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from archive.storage.base import ObjectStore
from universe.store import UniverseStore
from universe.sync import SyncResult, UniverseSync


def backfill_targeter_history(
    *,
    objects: ObjectStore,
    database: UniverseStore,
    generated_start: datetime,
    generated_end: datetime,
    temporary_directory: Path | None = None,
) -> SyncResult:
    """Index committed runs in the half-open generated-time range."""
    return UniverseSync(
        database,
        objects,
        temporary_directory=temporary_directory,
    ).sync_range(generated_start, generated_end)
