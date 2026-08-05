"""Local deletion of Targeter v2 run directories that are provably archived.

A separate module and a separate command from anything that writes runs, for
the reason ``archive/PHASE_4_RAW_ARCHIVE_REAPER_V1.md`` §7.1 gives: a pipeline
in which uploading is the last step before deleting is one refactor away from
deleting on an upload that only looked like it worked.

The raw reaper next door needs two receipts because a sealed segment passes
through canonical ingestion.  A run directory does not; its second proof is
that it is not the generation the splices are currently subscribed to.  Eleven
conditions, all re-established at decision time:

```text
1  a production archive receipt (a conformance receipt authorizes nothing)
2  a receipt that parses and names this directory
3  a directory holding nothing the receipt did not name
4  (or: no receipted artifact left at all, which is an earlier reaping)
5  a backend authorized as an independent durability domain
6  a readable publication pointer that names some other run
7  a run older than the retention floor by every clock available
8  archived objects that still match the receipt under head
9  (or: a partial cleanup to finish, from the top of this same path)
10 local artifacts still matching the receipt byte for byte
11 an operator who explicitly enabled deletion
```

**Absence of proof is retention, never permission.**  An unarchived run, an
unreadable pointer, a stray file, a clock that will not parse — each stays on
disk and stays visible in the report.

The receipt is never deleted.  Every artifact it names goes; the receipt and
the directory holding it remain as a tombstone, because after the artifacts
are gone the receipt is the only thing that makes the deletion auditable, and
because its presence is what lets the next sweep recognize the run as already
reaped without asking the object store anything.
"""

from __future__ import annotations

import stat
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable

from archive.common.durable import confirm_durable, remove_durable
from archive.storage.base import ObjectStore, ObjectStoreError, VerificationFailure
from targeter.v2.publication import PublicationError, read_publication_pointer
from targeter.v2.run_archive import (
    LOCAL_RECEIPT_FILE,
    PRODUCTION_RECEIPT_FILE,
    RUN_MANIFEST_FILE,
    SELECTION_REPORT_FILE,
    RunArchiveError,
    RunArchiveReceipt,
    discover_runs,
    parse_run_id_ns,
    read_run_archive_receipt,
    unrecognized_directories,
    validate_local_run,
    verify_run_archive,
)

__all__ = [
    "ALREADY_REAPED",
    "ARCHIVE_OBJECT_UNVERIFIED",
    "ARTIFACTS_ABSENT",
    "AUDIT_MODE",
    "DURABILITY_GATE",
    "IO_ERROR",
    "LOCAL_RUN_CHANGED",
    "NOT_ARCHIVED",
    "POINTER_UNREADABLE",
    "PROVEN",
    "PUBLISHED_GENERATION",
    "REAPED",
    "RECEIPT_INVALID",
    "RETAINED",
    "RETENTION_FLOOR",
    "RETENTION_FLOOR_HOURS",
    "RUN_CLOCK_UNREADABLE",
    "UNEXPECTED_RUN_ARTIFACT",
    "PublicationPointer",
    "RunDecision",
    "TargetRunReapResult",
    "TargetRunReaper",
    "discover_runs",
    "parse_run_id_ns",
    "run_instant_ns",
    "unrecognized_directories",
]

#: Every artifact the receipt named was deleted by this run.
REAPED = "reaped"
#: The run is still on disk, and this run did not authorize removing it.
RETAINED = "retained"
#: The artifacts were already gone and the receipt still describes them.
ALREADY_REAPED = "already_reaped"

# Reasons. Stable strings, because they are what an operator alerts on.
NOT_ARCHIVED = "run_archive_receipt_missing"
DURABILITY_GATE = "durability_gate"
RECEIPT_INVALID = "run_archive_receipt_invalid"
UNEXPECTED_RUN_ARTIFACT = "unexpected_run_artifact"
POINTER_UNREADABLE = "publication_pointer_unreadable"
PUBLISHED_GENERATION = "published_generation"
RUN_CLOCK_UNREADABLE = "run_clock_unreadable"
RETENTION_FLOOR = "retention_floor"
ARCHIVE_OBJECT_UNVERIFIED = "archive_object_unverified"
LOCAL_RUN_CHANGED = "local_run_changed"
IO_ERROR = "io_error"
AUDIT_MODE = "audit_mode"
PROVEN = "archive_verified_and_unpublished"
ARTIFACTS_ABSENT = "run_artifacts_absent"

#: Hours a run is retained no matter what else is proven about it.  The command
#: refuses to be configured below this; it may be raised.
RETENTION_FLOOR_HOURS = 18
RETENTION_FLOOR_NS = RETENTION_FLOOR_HOURS * 3600 * 1_000_000_000


@dataclass(frozen=True)
class RunDecision:
    """One run's outcome, in the shape an audit of a deletion needs."""

    run_id: str
    run_directory: str
    archive_receipt: str | None
    prefix: str | None
    decision: str
    reason: str
    detail: str
    verified_at_ns: int
    age_ns: int | None = None
    artifacts_deleted: int = 0
    bytes_reclaimed: int = 0

    def as_record(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "run_directory": self.run_directory,
            "archive_receipt": self.archive_receipt,
            "prefix": self.prefix,
            "decision": self.decision,
            "reason": self.reason,
            "detail": self.detail,
            "verified_at_ns": self.verified_at_ns,
            "age_ns": self.age_ns,
            "artifacts_deleted": self.artifacts_deleted,
            "bytes_reclaimed": self.bytes_reclaimed,
        }


@dataclass(frozen=True)
class PublicationPointer:
    """Which run the splices are subscribed to, or why that is unknown.

    An unreadable pointer is not "nothing is published".  It is equally
    consistent with a live-root that failed to mount, and the two call for
    opposite actions, so this carries the fault rather than a guess.
    """

    run_id: str | None
    fault: str | None

    @classmethod
    def read(cls, live_root: Path) -> "PublicationPointer":
        try:
            return cls(run_id=read_publication_pointer(live_root), fault=None)
        except PublicationError as error:
            return cls(run_id=None, fault=str(error))


@dataclass
class TargetRunReapResult:
    decisions: list[RunDecision] = field(default_factory=list)
    #: Why the publication pointer could not be read.  Not a per-run fault: an
    #: unreadable pointer means no run can be proven unpublished, so it is
    #: surfaced on its own rather than repeated under every retention.
    pointer_faults: list[str] = field(default_factory=list)
    #: Directories under the output root that are not run directories at all.
    #: Reported so an operator can see them; never considered for deletion.
    unrecognized: list[str] = field(default_factory=list)
    published_run_id: str | None = None
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

    def _retained_for(self, reason: str) -> int:
        return sum(
            1
            for item in self.decisions
            if item.decision == RETAINED and item.reason == reason
        )

    @property
    def counts(self) -> dict[str, int]:
        return {
            "considered": len(self.decisions),
            "reaped": self.count(REAPED),
            "already_reaped": self.count(ALREADY_REAPED),
            "retained": self.count(RETAINED),
            # Everything the gate would open on if an operator enabled deletion.
            "reapable": self._retained_for(AUDIT_MODE),
            # Runs nothing has archived. These can never be reaped, so a number
            # that climbs here means the archive sweep is not running and disk
            # is not actually bounded.
            "unarchived": self._retained_for(NOT_ARCHIVED),
            "artifacts_deleted": sum(item.artifacts_deleted for item in self.decisions),
            "bytes_reclaimed": sum(item.bytes_reclaimed for item in self.decisions),
            "pointer_faults": len(self.pointer_faults),
            "unrecognized": len(self.unrecognized),
        }

    def as_record(self) -> dict[str, Any]:
        return {
            "destructive": self.destructive,
            "published_run_id": self.published_run_id,
            "counts": self.counts,
            "retained_by_reason": self.retained_by_reason(),
            "pointer_faults": self.pointer_faults,
            "unrecognized": self.unrecognized,
            "decisions": [decision.as_record() for decision in self.decisions],
        }


def run_instant_ns(run_directory: Path, receipt: RunArchiveReceipt) -> int | None:
    """How old a run is allowed to claim to be — the latest of every clock.

    The run id and ``archived_at_ns`` both derive from ``--now``, which
    ``targeter/v2/run.py`` documents as a probe and test flag.  A run created
    with ``--now 2020-01-01`` would otherwise be older than any floor the
    moment it is written.  The receipt's own mtime is the one clock argv
    cannot set.

    ``max`` is the safe combinator: age is measured back from the basis, so a
    later basis retains longer.  Adding a clock here can never shorten
    retention below what the most recent one says.
    """
    named = parse_run_id_ns(run_directory.name)
    if named is None:
        return None
    return max(named, receipt.archived_at_ns, receipt.path.stat().st_mtime_ns)


def _deletion_order(receipt: RunArchiveReceipt) -> tuple[str, ...]:
    """Selection report first, run manifest last, the rest in between.

    The report is what every reader of a run opens first, so removing it first
    makes each interrupted state fail at the same check with the same message
    instead of a different one per crash point.  It is also the largest file.
    The manifest goes last because it is the local twin of the remote commit
    marker.
    """
    names = sorted(item.file for item in receipt.objects)
    ends = {SELECTION_REPORT_FILE, RUN_MANIFEST_FILE}
    return tuple(
        [name for name in names if name == SELECTION_REPORT_FILE]
        + [name for name in names if name not in ends]
        + [name for name in names if name == RUN_MANIFEST_FILE]
    )


class TargetRunReaper:
    """Decides, and only then deletes.

    ``destructive`` defaults to False, and the retention floor is a plain
    parameter here so a test can drive the gate directly.  The command is
    where an operator has to say both things out loud.
    """

    def __init__(
        self,
        output_root: Path | str,
        live_root: Path | str,
        store: ObjectStore,
        *,
        destructive: bool = False,
        retention_ns: int = RETENTION_FLOOR_NS,
        now_ns: Callable[[], int] = time.time_ns,
    ) -> None:
        self.output_root = Path(output_root)
        self.live_root = Path(live_root)
        self.store = store
        self.destructive = bool(destructive)
        self.retention_ns = int(retention_ns)
        self._now_ns = now_ns

    def sweep(self, runs: Iterable[Path] | None = None) -> TargetRunReapResult:
        # Read the pointer exactly once. A re-read mid-sweep can only name a
        # newer run, which would withdraw protection from the older one it
        # replaced while this sweep was still deciding about it.
        pointer = PublicationPointer.read(self.live_root)
        result = TargetRunReapResult(
            destructive=self.destructive, published_run_id=pointer.run_id
        )
        if pointer.fault is not None:
            result.pointer_faults.append(pointer.fault)
        if runs is None:
            runs = discover_runs(self.output_root)
            result.unrecognized.extend(unrecognized_directories(self.output_root))
        for run_directory in runs:
            result.decisions.append(self.consider(run_directory, pointer))
        return result

    # -- one run -----------------------------------------------------------

    def consider(self, run_directory: Path, pointer: PublicationPointer) -> RunDecision:
        run_directory = Path(run_directory)
        now = self._now_ns()
        run_id = run_directory.name
        production = run_directory / PRODUCTION_RECEIPT_FILE

        if not production.is_file():
            if (run_directory / LOCAL_RECEIPT_FILE).is_file():
                # A conformance receipt proves the control flow works, not that
                # a second durable copy exists. Reported rather than ignored,
                # because "we are archiving to a test backend" is the single
                # most important thing an operator expecting deletion could be
                # wrong about.
                return self._unproven(
                    run_id,
                    run_directory,
                    now,
                    DURABILITY_GATE,
                    "archived only to a local conformance store; this receipt authorizes nothing",
                )
            return self._unproven(
                run_id,
                run_directory,
                now,
                NOT_ARCHIVED,
                "no run archive receipt; nothing proves this run exists anywhere else",
            )

        try:
            receipt = read_run_archive_receipt(production)
        except RunArchiveError as error:
            return self._unproven(run_id, run_directory, now, RECEIPT_INVALID, str(error))
        if receipt.run_id != run_id:
            # A receipt copied into another run's directory would otherwise
            # authorize deleting whatever sits there under the same names.
            return self._unproven(
                run_id,
                run_directory,
                now,
                RECEIPT_INVALID,
                f"receipt names run {receipt.run_id!r} but sits in {run_id!r}",
            )

        archived = {item.file for item in receipt.objects}
        allowed = archived | {PRODUCTION_RECEIPT_FILE, LOCAL_RECEIPT_FILE}
        try:
            present = self._inventory(run_directory, archived, allowed)
        except _Unexpected as error:
            return self._retain(receipt, now, UNEXPECTED_RUN_ARTIFACT, str(error))
        except OSError as error:
            return self._retain(receipt, now, IO_ERROR, str(error))

        if not present:
            # Nothing is about to be deleted, so nothing needs proving. This is
            # also why a tombstone costs no object-store request, however many
            # sweeps run over it.
            return RunDecision(
                run_id=run_id,
                run_directory=str(run_directory),
                archive_receipt=str(receipt.path),
                prefix=receipt.prefix,
                decision=ALREADY_REAPED,
                reason=ARTIFACTS_ABSENT,
                detail="every archived artifact is already absent; the receipt remains",
                verified_at_ns=now,
            )

        if not self.store.durability.independent:
            return self._retain(
                receipt,
                now,
                DURABILITY_GATE,
                f"{self.store.store_id} is declared {self.store.durability.name}; deletion "
                "requires a backend configured as an independent durability domain",
            )

        if pointer.fault is not None:
            return self._retain(receipt, now, POINTER_UNREADABLE, pointer.fault)
        if run_id == pointer.run_id:
            return self._retain(
                receipt,
                now,
                PUBLISHED_GENERATION,
                "this run is the published generation; its local artifacts are what "
                "the integrity audit re-reads",
            )

        try:
            instant = run_instant_ns(run_directory, receipt)
        except OSError as error:
            return self._retain(receipt, now, IO_ERROR, str(error))
        if instant is None:
            return self._retain(
                receipt, now, RUN_CLOCK_UNREADABLE, f"run id {run_id!r} names no instant"
            )
        age = now - instant
        if age < self.retention_ns:
            return self._retain(
                receipt,
                now,
                RETENTION_FLOOR,
                f"the run is {age / 3_600_000_000_000:.1f}h old and the floor is "
                f"{self.retention_ns / 3_600_000_000_000:.1f}h",
                age_ns=age,
            )

        # Deliberately here rather than earlier. There is no rebuildable
        # derivative to release on the receipt alone, so nothing needs the
        # proof before this point, and verifying immediately before the
        # irreversible act is strictly stronger than verifying at the top.
        try:
            verify_run_archive(self.store, receipt)
        except (VerificationFailure, ObjectStoreError) as error:
            return self._retain(
                receipt, now, ARCHIVE_OBJECT_UNVERIFIED, str(error), age_ns=age
            )

        if len(present) < len(archived):
            # The crash window: some artifacts gone, some still here. Every
            # proof above was just re-established, which is the condition on
            # finishing it — the next run may complete a partial cleanup, but
            # only from the top of this same path.
            if not self.destructive:
                return self._retain(
                    receipt,
                    now,
                    AUDIT_MODE,
                    "a partial cleanup is pending; deletion is disabled for this run",
                    age_ns=age,
                )
            deleted, reclaimed = self._delete(run_directory, receipt, present)
            return self._reaped(
                receipt,
                now,
                "finished a partial cleanup after re-verifying the archive",
                age,
                deleted,
                reclaimed,
            )

        try:
            validate_local_run(run_directory, receipt)
        except RunArchiveError as error:
            return self._retain(receipt, now, LOCAL_RUN_CHANGED, str(error), age_ns=age)

        if not self.destructive:
            return self._retain(
                receipt,
                now,
                AUDIT_MODE,
                "every condition holds; deletion is disabled for this run",
                age_ns=age,
            )
        deleted, reclaimed = self._delete(run_directory, receipt, present)
        return self._reaped(
            receipt,
            now,
            "archive receipt, archive objects and local artifacts all verified",
            age,
            deleted,
            reclaimed,
        )

    # -- condition 3 -------------------------------------------------------

    def _inventory(
        self, run_directory: Path, archived: set[str], allowed: set[str]
    ) -> set[str]:
        """Which archived artifacts are present, refusing anything else.

        ``validate_local_run`` walks the receipt, so it cannot see a file the
        receipt never named.  A leftover atomic-write temporary means a process
        died mid-write in this directory, which is the one state where "what is
        on disk is not what was archived" is most plausible — so it retains the
        whole run rather than deleting around it.  Deleting around it would
        leave a directory the next sweep calls already reaped, turning a
        visible anomaly into permanent silent residue.
        """
        present: set[str] = set()
        for path in sorted(run_directory.iterdir(), key=lambda item: item.name):
            if path.name not in allowed:
                raise _Unexpected(f"unexpected file in the run directory: {path.name}")
            if not stat.S_ISREG(path.lstat().st_mode):
                raise _Unexpected(f"run artifact is not a regular file: {path.name}")
            if path.name in archived:
                present.add(path.name)
        return present

    # -- deletion ----------------------------------------------------------

    def _delete(
        self, run_directory: Path, receipt: RunArchiveReceipt, present: set[str]
    ) -> tuple[int, int]:
        # The receipt is about to become the only record that these artifacts
        # were ever here, so its own durability is established before the bytes
        # it describes are removed.
        confirm_durable(receipt.path)
        deleted = 0
        reclaimed = 0
        for name in _deletion_order(receipt):
            if name not in present:
                continue
            path = run_directory / name
            try:
                size = path.stat().st_size
            except OSError:
                size = 0
            if remove_durable(path):
                deleted += 1
                reclaimed += size
        return deleted, reclaimed

    # -- decision helpers --------------------------------------------------

    def _reaped(
        self,
        receipt: RunArchiveReceipt,
        now: int,
        detail: str,
        age_ns: int,
        deleted: int,
        reclaimed: int,
    ) -> RunDecision:
        return RunDecision(
            run_id=receipt.run_id,
            run_directory=str(receipt.path.parent),
            archive_receipt=str(receipt.path),
            prefix=receipt.prefix,
            decision=REAPED,
            reason=PROVEN,
            detail=detail,
            verified_at_ns=now,
            age_ns=age_ns,
            artifacts_deleted=deleted,
            bytes_reclaimed=reclaimed,
        )

    def _retain(
        self,
        receipt: RunArchiveReceipt,
        now: int,
        reason: str,
        detail: str,
        age_ns: int | None = None,
    ) -> RunDecision:
        return RunDecision(
            run_id=receipt.run_id,
            run_directory=str(receipt.path.parent),
            archive_receipt=str(receipt.path),
            prefix=receipt.prefix,
            decision=RETAINED,
            reason=reason,
            detail=detail,
            verified_at_ns=now,
            age_ns=age_ns,
        )

    def _unproven(
        self,
        run_id: str,
        run_directory: Path,
        now: int,
        reason: str,
        detail: str,
    ) -> RunDecision:
        """A retention for a run with no usable receipt to describe it."""
        return RunDecision(
            run_id=run_id,
            run_directory=str(run_directory),
            archive_receipt=None,
            prefix=None,
            decision=RETAINED,
            reason=reason,
            detail=detail,
            verified_at_ns=now,
        )


class _Unexpected(Exception):
    """An entry in a run directory that the receipt does not account for."""
