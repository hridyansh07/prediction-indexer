"""Audit-first deletion of canonical frames proven durable in object storage.

The two Zstandard frames are the large local artifacts. ``receipt.json`` and
``canonical_archive_receipt.json`` remain forever as a compact tombstone: the
first preserves immutable sequence and watermark-rebuild state, while the
second proves why absent frames are intentional. The archiver remains unable to
delete; this separate service re-establishes every proof immediately before an
unlink.
"""

from __future__ import annotations

import stat
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable

from archive.archiver.canonical import (
    LOCAL_RECEIPT_FILE,
    PRODUCTION_RECEIPT_FILE,
    CanonicalArchiveReceipt,
    CanonicalArchiveReceiptError,
    CanonicalArchiveVerificationError,
    read_canonical_archive_receipt,
    verify_canonical_archive,
)
from archive.common.durable import confirm_durable, remove_durable
from archive.common.receipts import (
    CANONICAL_RECEIPT_FILE,
    CanonicalReceipt,
    ReceiptError,
    iter_canonical_receipts,
    read_canonical_receipt,
)
from archive.storage.base import ObjectStore, ObjectStoreError
from encoder import stored_identity_of

REAPED = "reaped"
RETAINED = "retained"
ALREADY_REAPED = "already_reaped"

NOT_ARCHIVED = "canonical_archive_receipt_missing"
DURABILITY_GATE = "durability_gate"
RECEIPT_INVALID = "canonical_archive_receipt_invalid"
UNEXPECTED_WINDOW_ARTIFACT = "unexpected_window_artifact"
RETENTION_FLOOR = "retention_floor"
ARCHIVE_OBJECT_UNVERIFIED = "archive_object_unverified"
LOCAL_WINDOW_CHANGED = "local_window_changed"
IO_ERROR = "io_error"
AUDIT_MODE = "audit_mode"
PROVEN = "canonical_archive_verified"
ARTIFACTS_ABSENT = "canonical_artifacts_absent"

RETENTION_FLOOR_HOURS = 18
RETENTION_FLOOR_NS = RETENTION_FLOOR_HOURS * 3600 * 1_000_000_000

_ARCHIVED_FILES = frozenset(("evidence.ndjson.zst", "provenance.ndjson.zst"))
_ALLOWED_FILES = _ARCHIVED_FILES | frozenset(
    (CANONICAL_RECEIPT_FILE, PRODUCTION_RECEIPT_FILE, LOCAL_RECEIPT_FILE)
)

__all__ = [
    "ALREADY_REAPED",
    "ARCHIVE_OBJECT_UNVERIFIED",
    "ARTIFACTS_ABSENT",
    "AUDIT_MODE",
    "DURABILITY_GATE",
    "IO_ERROR",
    "LOCAL_WINDOW_CHANGED",
    "NOT_ARCHIVED",
    "PROVEN",
    "REAPED",
    "RECEIPT_INVALID",
    "RETAINED",
    "RETENTION_FLOOR",
    "RETENTION_FLOOR_HOURS",
    "RETENTION_FLOOR_NS",
    "UNEXPECTED_WINDOW_ARTIFACT",
    "CanonicalDecision",
    "CanonicalReapResult",
    "CanonicalReaper",
]


@dataclass(frozen=True)
class CanonicalDecision:
    window_start_ns: int
    window_directory: str
    archive_receipt: str | None
    decision: str
    reason: str
    detail: str
    verified_at_ns: int
    age_ns: int | None = None
    artifacts_deleted: int = 0
    bytes_reclaimed: int = 0

    def as_record(self) -> dict[str, Any]:
        return {
            "window_start_ns": self.window_start_ns,
            "window_directory": self.window_directory,
            "archive_receipt": self.archive_receipt,
            "decision": self.decision,
            "reason": self.reason,
            "detail": self.detail,
            "verified_at_ns": self.verified_at_ns,
            "age_ns": self.age_ns,
            "artifacts_deleted": self.artifacts_deleted,
            "bytes_reclaimed": self.bytes_reclaimed,
        }


@dataclass
class CanonicalReapResult:
    decisions: list[CanonicalDecision] = field(default_factory=list)
    destructive: bool = False

    @property
    def counts(self) -> dict[str, int]:
        retained = [item for item in self.decisions if item.decision == RETAINED]
        return {
            "considered": len(self.decisions),
            "reaped": sum(item.decision == REAPED for item in self.decisions),
            "already_reaped": sum(item.decision == ALREADY_REAPED for item in self.decisions),
            "retained": len(retained),
            "reapable": sum(item.reason == AUDIT_MODE for item in retained),
            "unarchived": sum(item.reason == NOT_ARCHIVED for item in retained),
            "artifacts_deleted": sum(item.artifacts_deleted for item in self.decisions),
            "bytes_reclaimed": sum(item.bytes_reclaimed for item in self.decisions),
        }

    def retained_by_reason(self) -> dict[str, int]:
        reasons: dict[str, int] = {}
        for item in self.decisions:
            if item.decision == RETAINED:
                reasons[item.reason] = reasons.get(item.reason, 0) + 1
        return reasons

    def as_record(self) -> dict[str, Any]:
        return {
            "destructive": self.destructive,
            "counts": self.counts,
            "retained_by_reason": self.retained_by_reason(),
            "decisions": [item.as_record() for item in self.decisions],
        }


class CanonicalReaper:
    def __init__(
        self,
        canonical_root: Path | str,
        store: ObjectStore,
        *,
        destructive: bool = False,
        retention_ns: int = RETENTION_FLOOR_NS,
        now_ns: Callable[[], int] = time.time_ns,
    ) -> None:
        self.canonical_root = Path(canonical_root)
        self.store = store
        self.destructive = bool(destructive)
        self.retention_ns = int(retention_ns)
        self._now_ns = now_ns

    def sweep(self, receipts: Iterable[Path] | None = None) -> CanonicalReapResult:
        paths = iter_canonical_receipts(self.canonical_root) if receipts is None else receipts
        return CanonicalReapResult(
            decisions=[self.consider(path) for path in paths], destructive=self.destructive
        )

    def consider(self, source_path: Path) -> CanonicalDecision:
        source_path = Path(source_path)
        directory = source_path.parent
        now = self._now_ns()
        start = _window_start_hint(directory)
        production_path = directory / PRODUCTION_RECEIPT_FILE

        if not production_path.is_file():
            reason = DURABILITY_GATE if (directory / LOCAL_RECEIPT_FILE).is_file() else NOT_ARCHIVED
            detail = (
                "only a local conformance receipt exists; it authorizes nothing"
                if reason == DURABILITY_GATE
                else "no production canonical archive receipt proves another durable copy"
            )
            return self._unproven(start, directory, now, reason, detail)

        try:
            archive = read_canonical_archive_receipt(production_path)
        except CanonicalArchiveReceiptError as error:
            return self._unproven(start, directory, now, RECEIPT_INVALID, str(error))

        try:
            present = self._inventory(directory)
        except _Unexpected as error:
            return self._retain(archive, now, UNEXPECTED_WINDOW_ARTIFACT, str(error))
        except OSError as error:
            return self._retain(archive, now, IO_ERROR, str(error))

        if not present:
            try:
                source = read_canonical_receipt(source_path)
                self._validate_binding(source, archive, set())
            except (ReceiptError, OSError) as error:
                return self._retain(archive, now, LOCAL_WINDOW_CHANGED, str(error))
            return CanonicalDecision(
                window_start_ns=archive.window_start_ns,
                window_directory=str(directory),
                archive_receipt=str(production_path),
                decision=ALREADY_REAPED,
                reason=ARTIFACTS_ABSENT,
                detail="both canonical frames are absent and the two retained receipts agree",
                verified_at_ns=now,
            )

        if not self.store.durability.independent:
            return self._retain(
                archive,
                now,
                DURABILITY_GATE,
                f"{self.store.store_id} is not an independent durability domain",
            )

        # Evidence is deliberately removed first. The opposite subset is not a
        # crash state this command can produce, so it cannot authorize finishing
        # around unexplained loss.
        if present == {"evidence.ndjson.zst"}:
            return self._retain(
                archive,
                now,
                LOCAL_WINDOW_CHANGED,
                "provenance.ndjson.zst is absent while evidence remains; this reaper "
                "always deletes evidence first",
            )

        try:
            source = read_canonical_receipt(source_path)
            self._validate_binding(source, archive, present)
            basis = max(
                source.window_end_ns,
                source.finalized_at_ns,
                archive.verified_at_ns,
                source.path.stat().st_mtime_ns,
                archive.path.stat().st_mtime_ns,
            )
        except (ReceiptError, OSError) as error:
            return self._retain(archive, now, LOCAL_WINDOW_CHANGED, str(error))
        age = now - basis
        if age < self.retention_ns:
            return self._retain(
                archive,
                now,
                RETENTION_FLOOR,
                f"the window is {age / 3_600_000_000_000:.1f}h old and the floor is "
                f"{self.retention_ns / 3_600_000_000_000:.1f}h",
                age,
            )

        # Fresh remote proof is the final potentially slow check before unlink.
        try:
            verify_canonical_archive(self.store, archive)
        except (CanonicalArchiveVerificationError, ObjectStoreError) as error:
            return self._retain(archive, now, ARCHIVE_OBJECT_UNVERIFIED, str(error), age)

        if not self.destructive:
            return self._retain(
                archive,
                now,
                AUDIT_MODE,
                "every condition holds; deletion is disabled",
                age,
            )

        confirm_durable(source.path)
        confirm_durable(archive.path)
        deleted = 0
        reclaimed = 0
        for name in ("evidence.ndjson.zst", "provenance.ndjson.zst"):
            if name not in present:
                continue
            path = directory / name
            size = path.stat().st_size
            if remove_durable(path):
                deleted += 1
                reclaimed += size
        return CanonicalDecision(
            window_start_ns=archive.window_start_ns,
            window_directory=str(directory),
            archive_receipt=str(archive.path),
            decision=REAPED,
            reason=PROVEN,
            detail="archive objects and unchanged local canonical identities all verified",
            verified_at_ns=now,
            age_ns=age,
            artifacts_deleted=deleted,
            bytes_reclaimed=reclaimed,
        )

    def _inventory(self, directory: Path) -> set[str]:
        present: set[str] = set()
        for path in sorted(directory.iterdir(), key=lambda item: item.name):
            if path.name not in _ALLOWED_FILES:
                raise _Unexpected(f"unexpected file in canonical window: {path.name}")
            if not stat.S_ISREG(path.lstat().st_mode):
                raise _Unexpected(f"canonical window entry is not a regular file: {path.name}")
            if path.name in _ARCHIVED_FILES:
                present.add(path.name)
        return present

    def _validate_binding(
        self,
        source: CanonicalReceipt,
        archive: CanonicalArchiveReceipt,
        present: set[str],
    ) -> None:
        if (source.window_start_ns, source.window_end_ns) != (
            archive.window_start_ns,
            archive.window_end_ns,
        ):
            raise ReceiptError("canonical archive receipt names another window")
        with source.path.open("rb") as handle:
            if stored_identity_of(handle) != archive.canonical_receipt.stored:
                raise ReceiptError("receipt.json no longer matches its archive identity")
        expected = {
            source.evidence.file: (source.evidence.stored, archive.evidence.stored),
            source.provenance.file: (source.provenance.stored, archive.provenance.stored),
        }
        for name, (canonical_identity, archive_identity) in expected.items():
            if canonical_identity != archive_identity:
                raise ReceiptError(f"{name} identities disagree between receipts")
            if name in present:
                with (source.path.parent / name).open("rb") as handle:
                    if stored_identity_of(handle) != canonical_identity:
                        raise ReceiptError(f"{name} no longer matches its canonical receipt")

    def _retain(
        self,
        archive: CanonicalArchiveReceipt,
        now: int,
        reason: str,
        detail: str,
        age_ns: int | None = None,
    ) -> CanonicalDecision:
        return CanonicalDecision(
            window_start_ns=archive.window_start_ns,
            window_directory=str(archive.path.parent),
            archive_receipt=str(archive.path),
            decision=RETAINED,
            reason=reason,
            detail=detail,
            verified_at_ns=now,
            age_ns=age_ns,
        )

    def _unproven(
        self, start: int, directory: Path, now: int, reason: str, detail: str
    ) -> CanonicalDecision:
        return CanonicalDecision(
            window_start_ns=start,
            window_directory=str(directory),
            archive_receipt=None,
            decision=RETAINED,
            reason=reason,
            detail=detail,
            verified_at_ns=now,
        )


def _window_start_hint(directory: Path) -> int:
    try:
        return int(directory.name.removeprefix("window="))
    except ValueError:
        return 0


class _Unexpected(Exception):
    pass
