"""Daily archive manifest: a derived catalog, not a commit boundary.

§6.6 is explicit about what this is *not*. It is not an archive receipt, it does
not replace per-object checks, and it may be regenerated at any time. What it
gives replay is one logical dataset per UTC day without anyone trying to append
to an immutable object.

Because it is derived, it is a pure function of the receipts it was built from —
no generation timestamp, no ordering that depends on how the filesystem listed a
directory. Two runs over the same verified receipts produce byte-identical
files, which is what makes "delete it and rebuild it" a safe instruction.

Only receipts that revalidate against the store are included. An entry for an
object that is no longer there would be worse than a missing entry: replay would
discover the absence halfway through a dataset it had already started trusting.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from archive.common.durable import remove_durable, write_json_durable
from archive.storage.base import ObjectStore, ObjectStoreError
from archive.common.receipts import (
    ARCHIVE_RECEIPT_SUFFIX,
    LOCAL_ARCHIVE_RECEIPT_SUFFIX,
    PRODUCTION,
    ArchiveReceipt,
    ReceiptError,
    read_archive_receipt,
)
from archive.common.verify import VerificationError, verify_archive
from replay.lanes import lane_rank

__all__ = [
    "MANIFEST_FILE",
    "MANIFEST_VERSION",
    "DailyManifest",
    "ManifestResult",
    "build_daily_manifests",
    "discover_archive_receipts",
    "write_daily_manifests",
]

MANIFEST_VERSION = 1
MANIFEST_FILE = "manifest.json"
NANOSECONDS = 1_000_000_000


@dataclass(frozen=True)
class DailyManifest:
    date: str
    receipt_kind: str
    entries: tuple[dict[str, Any], ...]
    #: True once the UTC day is over. Until then the manifest is expected to
    #: change as later segments are archived, and §6.6 keeps it local and
    #: unpublished.
    closed: bool

    def as_record(self) -> dict[str, Any]:
        return {
            "manifest_version": MANIFEST_VERSION,
            "date": self.date,
            "receipt_kind": self.receipt_kind,
            # A manifest built from local conformance receipts describes a test
            # archive. Recorded so a consumer cannot mistake it for a catalog
            # over durable storage.
            "authorizes_deletion": False,
            "day_closed": self.closed,
            "segment_count": len(self.entries),
            "segments": list(self.entries),
        }


@dataclass
class ManifestResult:
    manifests: list[DailyManifest] = field(default_factory=list)
    written: list[Path] = field(default_factory=list)
    #: Stale manifests removed because their date has zero valid receipts in
    #: this rebuild. Never left behind: an absent date is silence, but a
    #: manifest naming objects the rebuild just excluded is a false claim.
    removed: list[Path] = field(default_factory=list)
    #: Receipts excluded, with why. Never silently dropped: an excluded receipt
    #: is either an integrity problem or a reaped segment whose objects moved.
    excluded: list[str] = field(default_factory=list)

    @property
    def counts(self) -> dict[str, int]:
        return {
            "manifests": len(self.manifests),
            "segments": sum(len(manifest.entries) for manifest in self.manifests),
            "removed": len(self.removed),
            "excluded": len(self.excluded),
        }


def discover_archive_receipts(spool_root: Path, *, kind: str = PRODUCTION) -> list[Path]:
    """Every archive receipt of one kind under a spool, in a stable order.

    Production builders take `.archive.json` only. §5.3 requires them to ignore
    `.archive.local.json` entirely — a conformance receipt is proof that the
    control flow works, not that a durable copy exists.
    """
    suffix = ARCHIVE_RECEIPT_SUFFIX if kind == PRODUCTION else LOCAL_ARCHIVE_RECEIPT_SUFFIX
    other = LOCAL_ARCHIVE_RECEIPT_SUFFIX if kind == PRODUCTION else ARCHIVE_RECEIPT_SUFFIX
    root = Path(spool_root)
    if not root.is_dir():
        return []
    found = [
        path
        for path in root.rglob(f"*{suffix}")
        if path.is_file() and not path.name.endswith(other)
    ]
    return sorted(found, key=lambda path: (path.name, str(path)))


def build_daily_manifests(
    store: ObjectStore,
    receipt_paths: Iterable[Path],
    *,
    kind: str = PRODUCTION,
    now_ns: int | None = None,
) -> ManifestResult:
    """Revalidates each receipt against the store and groups what survives.

    A transient store failure — an S3 `HeadObject` timeout or throttle, say —
    raises `ObjectStoreError` rather than `VerificationError`. §4 finding 4
    requires it to exclude that one receipt and continue rather than
    propagate: a long-lived archiver rebuilding manifests after every sweep
    must not be terminated by one flaky head against a segment whose receipt
    was already committed.
    """
    result = ManifestResult()
    by_date: dict[str, list[tuple[tuple[Any, ...], dict[str, Any]]]] = {}

    for path in receipt_paths:
        try:
            receipt = read_archive_receipt(path)
        except ReceiptError as error:
            result.excluded.append(str(error))
            continue
        if receipt.kind != kind:
            result.excluded.append(f"{path.name}: a {receipt.kind} receipt in a {kind} build")
            continue
        try:
            verify_archive(store, receipt)
        except (VerificationError, ObjectStoreError) as error:
            result.excluded.append(f"{path.name}: {error}")
            continue
        by_date.setdefault(_date_of(receipt), []).append((_sort_key(receipt), _entry(receipt)))

    for date, entries in sorted(by_date.items()):
        entries.sort(key=lambda item: item[0])
        result.manifests.append(
            DailyManifest(
                date=date,
                receipt_kind=kind,
                entries=tuple(entry for _, entry in entries),
                closed=_day_is_closed(date, now_ns),
            )
        )
    return result


def write_daily_manifests(
    manifest_root: Path,
    result: ManifestResult,
) -> ManifestResult:
    """Writes each manifest atomically under `date=<YYYY-MM-DD>/manifest.json`.

    `result` is expected to come from a full rebuild — the CLI always passes
    `discover_archive_receipts` over the whole spool, never a date-scoped
    slice. On that assumption, a `date=<...>/manifest.json` already on disk
    whose date is absent from `result.manifests` means every receipt that once
    justified it is gone or now fails to verify (§4 finding 3). Leaving that
    file in place would advertise objects the rebuild just excluded, so it is
    removed durably rather than left stale.
    """
    root = Path(manifest_root)
    for manifest in result.manifests:
        directory = root / f"date={manifest.date}"
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / MANIFEST_FILE
        write_json_durable(path, manifest.as_record())
        result.written.append(path)

    rebuilt_dates = {manifest.date for manifest in result.manifests}
    if root.is_dir():
        for directory in sorted(root.iterdir()):
            if not directory.is_dir() or not directory.name.startswith("date="):
                continue
            if directory.name[len("date=") :] in rebuilt_dates:
                continue
            stale = directory / MANIFEST_FILE
            if stale.is_file() and remove_durable(stale):
                result.removed.append(stale)
    return result


def _entry(receipt: ArchiveReceipt) -> dict[str, Any]:
    return {
        "lane": receipt.lane_id,
        "window_start_ns": receipt.window_start_ns,
        "window_end_ns": receipt.window_end_ns,
        "segment_index": receipt.segment_index,
        "segment_id": receipt.segment_id,
        "source_file": receipt.source_file,
        "data_key": receipt.data_key,
        "seal_key": receipt.seal_key,
        "logical": receipt.source.as_record(),
        "stored": receipt.data_stored.as_record(),
    }


def _sort_key(receipt: ArchiveReceipt) -> tuple[Any, ...]:
    """§6.6's order: window, lane rank, segment index, segment id."""
    return (
        receipt.window_start_ns,
        lane_rank(receipt.lane_id),
        receipt.lane_id,
        receipt.segment_index,
        receipt.segment_id,
    )


def _date_of(receipt: ArchiveReceipt) -> str:
    moment = datetime.fromtimestamp(receipt.window_start_ns / NANOSECONDS, tz=timezone.utc)
    return moment.strftime("%Y-%m-%d")


def _day_is_closed(date: str, now_ns: int | None) -> bool:
    if now_ns is None:
        return False
    day = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_ns = int(day.timestamp()) * NANOSECONDS + 24 * 60 * 60 * NANOSECONDS
    return now_ns >= end_ns
