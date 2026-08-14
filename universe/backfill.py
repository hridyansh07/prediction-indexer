"""Resumable publication of universe artifacts for historical raw receipts.

The input axis is retained local production receipts (or an explicit inventory
of those paths), never an arbitrary S3 listing.  If local raw bytes were reaped,
the exact receipted archive object is strictly decoded to temporary storage first.
"""

from __future__ import annotations

import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterable

from archive.archiver.manifest import discover_archive_receipts
from archive.archiver.universe import publish_segment_universe
from archive.common.receipts import PRODUCTION, read_archive_receipt
from archive.common.verify import decode_archived_segment
from archive.storage.base import ObjectStore
from universe.store import UniverseStore


@dataclass
class BackfillResult:
    discovered: int = 0
    published: int = 0
    skipped: int = 0
    reconstructed: int = 0
    failures: list[str] = field(default_factory=list)

    def as_record(self) -> dict[str, object]:
        return {
            "discovered": self.discovered,
            "published": self.published,
            "skipped": self.skipped,
            "reconstructed": self.reconstructed,
            "failures": list(self.failures),
        }


def receipt_inventory(
    roots: Iterable[Path], inventory_files: Iterable[Path] = ()
) -> list[Path]:
    """Return a stable, de-duplicated production-receipt inventory."""
    paths: set[Path] = set()
    for root in roots:
        paths.update(discover_archive_receipts(Path(root), kind=PRODUCTION))
    for inventory in inventory_files:
        source = Path(inventory)
        for line_number, raw in enumerate(source.read_text(encoding="utf-8").splitlines(), 1):
            value = raw.strip()
            if not value or value.startswith("#"):
                continue
            path = Path(value)
            if not path.is_absolute():
                path = source.parent / path
            if path.name.endswith(".archive.local.json") or not path.name.endswith(
                ".archive.json"
            ):
                raise ValueError(
                    f"{source}:{line_number} is not a production archive receipt path"
                )
            paths.add(path.resolve())
    return sorted(paths, key=lambda path: str(path))


def backfill_segment_universe(
    *,
    receipt_paths: Iterable[Path],
    objects: ObjectStore,
    database: UniverseStore,
    temp_root: Path | None = None,
    now_ns: Callable[[], int] = time.time_ns,
) -> BackfillResult:
    """Publish missing sidecars, retry safely, and checkpoint every receipt.

    The scan intentionally revisits paths before the high-water cursor. A
    receipt may be copied into an older directory after a previous run, while
    immutable publication makes revisiting already committed work cheap and
    safe. The checkpoint is operational progress evidence, not permission to
    pretend an unscanned path does not exist.
    """
    paths = sorted({Path(path) for path in receipt_paths}, key=lambda path: str(path))
    result = BackfillResult(discovered=len(paths))
    checkpoint_name = f"raw-universe-backfill:{objects.store_id}"
    for path in paths:
        try:
            receipt = read_archive_receipt(path)
            if not receipt.is_production:
                raise ValueError(f"not a production archive receipt: {path}")
            local_source = path.with_name(receipt.source_file)
            if local_source.is_file():
                publication = publish_segment_universe(
                    objects,
                    receipt,
                    local_source,
                    now_ns=now_ns(),
                )
            else:
                with tempfile.TemporaryDirectory(
                    prefix="universe-backfill-",
                    dir=Path(temp_root) if temp_root is not None else None,
                ) as directory:
                    reconstructed = Path(directory) / receipt.source_file
                    decode_archived_segment(objects, receipt, reconstructed)
                    result.reconstructed += 1
                    publication = publish_segment_universe(
                        objects,
                        receipt,
                        reconstructed,
                        now_ns=now_ns(),
                    )
            if publication.status == "published":
                result.published += 1
            else:
                result.skipped += 1
            database.set_checkpoint(checkpoint_name, str(path))
        except Exception as error:  # noqa: BLE001 - preserve every failed receipt in the report
            result.failures.append(f"{path}: {type(error).__name__}: {error}")
    return result
