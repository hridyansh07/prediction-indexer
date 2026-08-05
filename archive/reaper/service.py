"""Local raw deletion requiring two receipts to authorize it.

A separate module and a separate command from the archiver, deliberately. §7.1:
the archiver may report that a segment looks eligible, but it cannot invoke
deletion as a success callback — a pipeline in which uploading is the last step
before deleting is one refactor away from deleting on an upload that only looked
like it worked.

Six conditions, all re-established at decision time:

```text
1  a structurally valid archive receipt
2  archive data and seal objects that still match it under head
3  a backend authorized as an independent durability domain
4  a structurally valid committed canonical receipt
5  a canonical inputs entry matching lane, source sha256, data_file and index
6  the local raw source and seal still matching the archive receipt
```

Lane plus digest is the minimum the sealed-capture contract requires. The file
name and segment index are matched as well so that an accidental cross-segment
authorization fails loudly rather than quietly deleting the wrong tape.

**Absence of proof is retention, never permission.** A late, excluded, invalid
or never-canonicalized segment stays on disk and stays visible in the report;
the reaper does not guess it into a canonical window, and it never deletes an
archive receipt — after the source is gone the receipt is the only thing that
makes the deletion auditable.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable

from archive.common.durable import confirm_durable, remove_durable
from archive.storage.base import ObjectStore, ObjectStoreError
from archive.common.receipts import (
    ARCHIVE_RECEIPT_SUFFIX,
    LOCAL_ARCHIVE_RECEIPT_SUFFIX,
    ArchiveReceipt,
    CanonicalIndex,
    ReceiptError,
    read_archive_receipt,
)
from archive.common.seal import SealError, read_sealed_segment
from archive.common.verify import VerificationError, verify_archive
from encoder import logical_identity_of, stored_identity_of

__all__ = [
    "ALREADY_REAPED",
    "ARCHIVE_OBJECT_UNVERIFIED",
    "ARCHIVE_RECEIPT_INVALID",
    "AUDIT_MODE",
    "CANONICAL_MISMATCH",
    "CANONICAL_MISSING",
    "DURABILITY_GATE",
    "LOCAL_SOURCE_CHANGED",
    "REAPED",
    "RETAINED",
    "Decision",
    "ReapResult",
    "Reaper",
    "discover_receipts",
]

#: Raw source and seal were deleted by this run.
REAPED = "reaped"
#: Local raw data is still there, and this run did not authorize removing it.
RETAINED = "retained"
#: The source was already gone and every proof still verifies.
ALREADY_REAPED = "already_reaped"

# Reasons. Stable strings, because they are what an operator alerts on.
DURABILITY_GATE = "durability_gate"
ARCHIVE_RECEIPT_INVALID = "archive_receipt_invalid"
ARCHIVE_OBJECT_UNVERIFIED = "archive_object_unverified"
CANONICAL_MISSING = "canonical_receipt_missing"
CANONICAL_MISMATCH = "canonical_segment_mismatch"
LOCAL_SOURCE_CHANGED = "local_source_changed"
IO_ERROR = "io_error"
AUDIT_MODE = "audit_mode"
PROVEN = "both_receipts_verified"


@dataclass(frozen=True)
class Decision:
    """One segment's outcome, in the shape §7.2 asks a report to carry."""

    lane: str
    source_file: str
    source_sha256: str
    archive_receipt: str
    canonical_receipt: str | None
    decision: str
    reason: str
    detail: str
    verified_at_ns: int
    #: Whether the rebuildable `.ndjson.zst` was removed. Governed by the
    #: archive receipt alone (§7.2), so it can happen while the raw segment is
    #: retained for want of a canonical receipt.
    derivative_deleted: bool = False

    def as_record(self) -> dict[str, Any]:
        return {
            "lane": self.lane,
            "source_file": self.source_file,
            "source_sha256": self.source_sha256,
            "archive_receipt": self.archive_receipt,
            "canonical_receipt": self.canonical_receipt,
            "decision": self.decision,
            "reason": self.reason,
            "detail": self.detail,
            "verified_at_ns": self.verified_at_ns,
            "derivative_deleted": self.derivative_deleted,
        }


@dataclass
class ReapResult:
    decisions: list[Decision] = field(default_factory=list)
    #: Canonical receipts that could not be read at all. Not a per-segment
    #: fault: an unreadable canonical root means no segment can be proven, so it
    #: is surfaced on its own rather than repeated under every retention.
    canonical_faults: list[str] = field(default_factory=list)
    destructive: bool = False

    def count(self, decision: str) -> int:
        return sum(1 for item in self.decisions if item.decision == decision)

    def retained_by_reason(self) -> dict[str, int]:
        reasons: dict[str, int] = {}
        for item in self.decisions:
            if item.decision != RETAINED:
                continue
            reasons[item.reason] = reasons.get(item.reason, 0) + 1
        return reasons

    @property
    def counts(self) -> dict[str, int]:
        return {
            "considered": len(self.decisions),
            "reaped": self.count(REAPED),
            "already_reaped": self.count(ALREADY_REAPED),
            "retained": self.count(RETAINED),
            "reapable": sum(
                1 for item in self.decisions if item.decision == RETAINED and item.reason == AUDIT_MODE
            ),
            "derivatives_deleted": sum(1 for item in self.decisions if item.derivative_deleted),
            "canonical_faults": len(self.canonical_faults),
        }

    def as_record(self) -> dict[str, Any]:
        return {
            "destructive": self.destructive,
            "counts": self.counts,
            "retained_by_reason": self.retained_by_reason(),
            "canonical_faults": self.canonical_faults,
            "decisions": [decision.as_record() for decision in self.decisions],
        }


def discover_receipts(spool_root: Path) -> list[Path]:
    """Every archive receipt under a spool, of either kind, in a stable order.

    Receipts rather than segments are the reaper's discovery axis, because a
    partially reaped segment has no `.ndjson` left to find and still needs its
    seal removed — and because a retained receipt is exactly what makes an
    already-completed deletion auditable.
    """
    root = Path(spool_root)
    if not root.is_dir():
        return []
    found = [
        path
        for path in root.rglob("*.archive*.json")
        if path.is_file()
        and (
            path.name.endswith(ARCHIVE_RECEIPT_SUFFIX)
            or path.name.endswith(LOCAL_ARCHIVE_RECEIPT_SUFFIX)
        )
    ]
    return sorted(found, key=lambda path: (path.name, str(path)))


class Reaper:
    """Decides, and only then deletes.

    `destructive` defaults to False. Deployment keeps it that way until an
    operator has explicitly enabled it against a backend that is explicitly an
    independent durability domain — installing this code does not make raw
    deletion active (§8.2).
    """

    def __init__(
        self,
        spool_root: Path | str,
        canonical_root: Path | str,
        store: ObjectStore,
        *,
        destructive: bool = False,
        now_ns: Callable[[], int] = time.time_ns,
    ) -> None:
        self.spool_root = Path(spool_root)
        self.canonical_root = Path(canonical_root)
        self.store = store
        self.destructive = bool(destructive)
        self._now_ns = now_ns

    def sweep(self, receipts: Iterable[Path] | None = None) -> ReapResult:
        index = CanonicalIndex.build(self.canonical_root)
        result = ReapResult(destructive=self.destructive, canonical_faults=list(index.faults))
        for path in discover_receipts(self.spool_root) if receipts is None else receipts:
            result.decisions.append(self.consider(path, index))
        return result

    # -- one segment -------------------------------------------------------

    def consider(self, receipt_path: Path, index: CanonicalIndex) -> Decision:
        receipt_path = Path(receipt_path)
        now = self._now_ns()

        if receipt_path.name.endswith(LOCAL_ARCHIVE_RECEIPT_SUFFIX):
            # A conformance receipt is proof that the control flow works, not
            # that a second durable copy exists. §5.3 requires the production
            # reaper to ignore it — reported, because "we are archiving to a
            # test backend" is the single most important thing an operator
            # expecting deletion could be wrong about.
            return self._unproven(
                receipt_path,
                now,
                DURABILITY_GATE,
                "archived only to a local conformance store; this receipt authorizes nothing",
            )

        try:
            receipt = read_archive_receipt(receipt_path)
        except ReceiptError as error:
            return self._unproven(receipt_path, now, ARCHIVE_RECEIPT_INVALID, str(error))

        directory = receipt_path.parent
        source_path = directory / receipt.source_file
        seal_path = directory / receipt.seal_file

        # The files about to be deleted are named by the receipt and resolved
        # against the directory it was found in, so those two have to agree
        # about the lane before either is trusted. A receipt copied into another
        # lane's directory would otherwise authorize deleting whatever sits
        # there under the same name.
        if directory.parent.name != f"lane={receipt.lane_id}":
            return self._unproven(
                receipt_path,
                now,
                ARCHIVE_RECEIPT_INVALID,
                f"receipt names lane {receipt.lane_id!r} but sits under "
                f"{directory.parent.name!r}",
            )

        if not self.store.durability.independent:
            return self._retain(
                receipt,
                None,
                now,
                DURABILITY_GATE,
                f"{self.store.store_id} is declared {self.store.durability.name}; deletion "
                "requires a backend configured as an independent durability domain",
            )

        try:
            verify_archive(self.store, receipt)
        except (VerificationError, ObjectStoreError) as error:
            return self._retain(receipt, None, now, ARCHIVE_OBJECT_UNVERIFIED, str(error))

        # §7.2: the local `.ndjson.zst` goes on the verified archive receipt
        # *alone*, before the canonical half of the gate is even consulted. It
        # is a derivative of a segment that is still on disk, so losing it costs
        # a recompression and nothing else — which is exactly why it is not
        # governed by the same rule as the evidence it was derived from.
        derivative = directory / (receipt.source_file[: -len(".ndjson")] + ".ndjson.zst")
        derivative_deleted = False
        if self.destructive and derivative.exists():
            derivative_deleted = remove_durable(derivative)

        canonical = index.find(
            receipt.lane_id, receipt.source.sha256, receipt.source_file, receipt.segment_index
        )
        if canonical is None:
            if index.names_digest(receipt.lane_id, receipt.source.sha256):
                return self._retain(
                    receipt,
                    None,
                    now,
                    CANONICAL_MISMATCH,
                    "a canonical window names this lane and digest under a different file "
                    "or segment index",
                    derivative_deleted,
                )
            return self._retain(
                receipt,
                None,
                now,
                CANONICAL_MISSING,
                "no committed canonical receipt names this lane, digest, file and index",
                derivative_deleted,
            )

        source_present = source_path.exists()
        seal_present = seal_path.exists()
        if not source_present and not seal_present:
            return Decision(
                lane=receipt.lane_id,
                source_file=receipt.source_file,
                source_sha256=receipt.source.sha256,
                archive_receipt=str(receipt_path),
                canonical_receipt=str(canonical),
                decision=ALREADY_REAPED,
                reason=PROVEN,
                detail="local raw data is absent and both receipts still verify",
                verified_at_ns=now,
                derivative_deleted=derivative_deleted,
            )

        if not source_present:
            # The crash window §7.2 designs for: raw gone, seal still there.
            # Everything above re-established the proofs, which is the condition
            # on finishing it — the next run may complete a partial cleanup, but
            # only from the top of this same path.
            if not self.destructive:
                return self._retain(
                    receipt,
                    canonical,
                    now,
                    AUDIT_MODE,
                    "a partial cleanup is pending; deletion is disabled for this run",
                    derivative_deleted,
                )
            remove_durable(seal_path)
            return Decision(
                lane=receipt.lane_id,
                source_file=receipt.source_file,
                source_sha256=receipt.source.sha256,
                archive_receipt=str(receipt_path),
                canonical_receipt=str(canonical),
                decision=REAPED,
                reason=PROVEN,
                detail="finished a partial cleanup after re-verifying both receipts",
                verified_at_ns=now,
                derivative_deleted=derivative_deleted,
            )

        if not seal_present:
            # The reverse order never happens through this code, so something
            # else removed it. Without the sidecar the segment is not committed
            # evidence and nothing here will delete it.
            return self._retain(
                receipt,
                canonical,
                now,
                LOCAL_SOURCE_CHANGED,
                f"{seal_path.name} is absent while {source_path.name} remains; the seal is "
                "the commit marker and this is not a state deletion produced",
                derivative_deleted,
            )

        try:
            unchanged = self._local_matches(receipt, source_path, seal_path)
        except (SealError, OSError) as error:
            return self._retain(
                receipt, canonical, now, IO_ERROR, str(error), derivative_deleted
            )
        if unchanged is not None:
            return self._retain(
                receipt, canonical, now, LOCAL_SOURCE_CHANGED, unchanged, derivative_deleted
            )

        if not self.destructive:
            return self._retain(
                receipt,
                canonical,
                now,
                AUDIT_MODE,
                "every condition holds; deletion is disabled for this run",
                derivative_deleted,
            )

        # The receipt is about to become the only record that this segment ever
        # existed locally, so its own durability is established before the bytes
        # it describes are removed.
        confirm_durable(receipt_path)

        # Raw first, then the seal. A crash between the two leaves a
        # recognizable partial cleanup, and the next run finishes it only after
        # rechecking every receipt and remote identity — which is this same
        # path, from the top.
        remove_durable(source_path)
        remove_durable(seal_path)
        return Decision(
            lane=receipt.lane_id,
            source_file=receipt.source_file,
            source_sha256=receipt.source.sha256,
            archive_receipt=str(receipt_path),
            canonical_receipt=str(canonical),
            decision=REAPED,
            reason=PROVEN,
            detail="archive receipt, archive objects and canonical receipt all verified",
            verified_at_ns=now,
            derivative_deleted=derivative_deleted,
        )

    # -- condition 6 -------------------------------------------------------

    def _local_matches(
        self, receipt: ArchiveReceipt, source_path: Path, seal_path: Path
    ) -> str | None:
        """The local files against the receipt. Returns a complaint, or None.

        A full rehash of the raw source, deliberately. The archiver's hourly
        retry compares lengths because it runs over every retained segment every
        hour; this runs once per segment, immediately before the bytes stop
        existing, and it is the last opportunity to notice that what is about to
        be deleted is not what was archived.
        """
        segment = read_sealed_segment(receipt.lane_id, source_path)
        if segment.logical != receipt.source:
            return f"{source_path.name}: the seal beside it no longer matches the receipt"
        with seal_path.open("rb") as handle:
            if stored_identity_of(handle) != receipt.seal_stored:
                return f"{seal_path.name}: no longer matches the archived seal object"
        with source_path.open("rb") as handle:
            logical = logical_identity_of(handle)
        if logical != receipt.source:
            return (
                f"{source_path.name}: reads as {logical.byte_length} bytes / sha256 "
                f"{logical.sha256}, the receipt records {receipt.source.byte_length} bytes / "
                f"sha256 {receipt.source.sha256}"
            )
        return None

    # -- decision helpers --------------------------------------------------

    def _retain(
        self,
        receipt: ArchiveReceipt,
        canonical: Path | None,
        now: int,
        reason: str,
        detail: str,
        derivative_deleted: bool = False,
    ) -> Decision:
        return Decision(
            lane=receipt.lane_id,
            source_file=receipt.source_file,
            source_sha256=receipt.source.sha256,
            archive_receipt=str(receipt.path),
            canonical_receipt=str(canonical) if canonical else None,
            decision=RETAINED,
            reason=reason,
            detail=detail,
            verified_at_ns=now,
            derivative_deleted=derivative_deleted,
        )

    def _unproven(self, path: Path, now: int, reason: str, detail: str) -> Decision:
        """A retention for a receipt too broken to say what it describes."""
        return Decision(
            lane=_lane_from_path(path),
            source_file=path.name,
            source_sha256="",
            archive_receipt=str(path),
            canonical_receipt=None,
            decision=RETAINED,
            reason=reason,
            detail=detail,
            verified_at_ns=now,
        )


def _lane_from_path(path: Path) -> str:
    for part in path.parts:
        if part.startswith("lane="):
            return part[len("lane=") :]
    return "unknown"
