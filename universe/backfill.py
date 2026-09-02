"""Bounded Targeter v3 history backfill through the normal projector."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Callable

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
    batch_size: int = 100,
    progress: Callable[[dict[str, Any]], None] | None = None,
) -> SyncResult:
    """Checkpoint and batch committed runs in the half-open generated-time range."""
    return UniverseSync(
        database,
        objects,
        temporary_directory=temporary_directory,
    ).backfill_range(
        generated_start,
        generated_end,
        batch_size=batch_size,
        progress=progress,
    )
