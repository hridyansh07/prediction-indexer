"""Archival of complete Targeter v2 run directories that hold no receipt yet.

``targeter/v2/run.py`` archives the run it just produced, inside the same
transaction.  That covers the healthy path and nothing else: a run whose upload
failed, or one produced by a ``--mode shadow`` deployment, keeps its evidence
locally and has no receipt.  The reaper cannot reclaim such a run — proof is
what authorizes deletion and there is none — so without this sweep those runs
accumulate forever and local disk is not actually bounded.

**This module cannot delete anything and must never learn how.**
``archive/PHASE_4_RAW_ARCHIVE_REAPER_V1.md`` §7.1 keeps archival and deletion in
separate commands precisely so that uploading can never become the last step
before deleting; ``AGENTS.md`` states the same rule.  A test asserts that this
file imports no removal primitive.

Completeness is decided structurally rather than by interpreting an error
message. ``run_shadow`` writes every catalogue/rule artifact and the compressed
report first, then writes ``selection_report.meta.json`` as its commit marker. A
run missing the marker or one of its named files is either in progress or was
abandoned by a process that died. Age separates those two, and the difference
matters: one is ordinary backlog and the other is a fault an operator has to
clear.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable

from archive.storage.base import IntegrityConflict, ObjectStore, ObjectStoreError
from targeter.v2.run_archive import (
    LOCAL_RECEIPT_FILE,
    PRODUCTION_RECEIPT_FILE,
    REQUIRED_FILES,
    SELECTION_REPORT_FILE,
    SELECTION_REPORT_METADATA_FILE,
    RunArchiveError,
    archive_run,
    discover_runs,
    parse_run_id_ns,
    required_run_files,
)

__all__ = [
    "ARCHIVED",
    "CONFLICT",
    "FAILED",
    "INCOMPLETE_RUN_FLOOR_HOURS",
    "PENDING",
    "SKIPPED",
    "RunOutcome",
    "RunSweepResult",
    "TargetRunArchiveSweep",
]

#: A receipt was published for this run by this sweep.
ARCHIVED = "archived"
#: This store's receipt already exists.  Not re-verified here — see the class
#: docstring for why that is a division of labour rather than laziness.
SKIPPED = "skipped"
#: Structurally incomplete and young enough to still be running.  Not a fault,
#: and not zero-information either: a pending count that never falls is how a
#: stuck scheduler looks from here.
PENDING = "pending"
#: A fault an operator has to clear.
FAILED = "failed"
#: A key holds different bytes than this run would write.  Fatal to the sweep.
CONFLICT = "conflict"

#: How long a run may stay structurally incomplete before that stops being
#: "still running" and becomes "a process died".  The same number as the
#: reaper's retention floor, from the other side: one is too young to delete,
#: the other is too old to still be in progress.
INCOMPLETE_RUN_FLOOR_HOURS = 18
INCOMPLETE_RUN_FLOOR_NS = INCOMPLETE_RUN_FLOOR_HOURS * 3600 * 1_000_000_000


@dataclass(frozen=True)
class RunOutcome:
    run_id: str
    run_directory: str
    status: str
    detail: str
    receipt: str | None = None
    prefix: str | None = None
    object_count: int | None = None

    def as_record(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "run_directory": self.run_directory,
            "status": self.status,
            "detail": self.detail,
            "receipt": self.receipt,
            "prefix": self.prefix,
            "object_count": self.object_count,
        }


@dataclass
class RunSweepResult:
    outcomes: list[RunOutcome] = field(default_factory=list)
    halted: str | None = None
    #: False when a scheduled run held the lease and this sweep deferred to it.
    lease_acquired: bool = True

    @property
    def discovered(self) -> int:
        return len(self.outcomes)

    def count(self, status: str) -> int:
        return sum(1 for item in self.outcomes if item.status == status)

    @property
    def counts(self) -> dict[str, int]:
        return {
            "discovered": self.discovered,
            "archived": self.count(ARCHIVED),
            "skipped": self.count(SKIPPED),
            "pending": self.count(PENDING),
            "failed": self.count(FAILED),
            "conflicted": self.count(CONFLICT),
        }

    def as_record(self) -> dict[str, Any]:
        return {
            "lease_acquired": self.lease_acquired,
            "counts": self.counts,
            "halted": self.halted,
            "runs": [outcome.as_record() for outcome in self.outcomes],
        }


class TargetRunArchiveSweep:
    """Archives what has no receipt, and touches nothing that has one.

    Skipping a receipted run is the point, not an optimization detail.
    ``archive_run`` on an already-archived run re-verifies every remote object
    and rehashes every local artifact; across a full output root that is
    gigabytes of hashing per sweep, and it would raise on a run the reaper has
    already reduced to a tombstone.  The archiver's periodic pass does the
    cheap check; the reaper rehashes in full, once, at the only point where
    being wrong is unrecoverable.
    """

    def __init__(
        self,
        output_root: Path | str,
        store: ObjectStore,
        *,
        pending_floor_ns: int = INCOMPLETE_RUN_FLOOR_NS,
        now_ns: Callable[[], int] = time.time_ns,
    ) -> None:
        self.output_root = Path(output_root)
        self.store = store
        self.pending_floor_ns = int(pending_floor_ns)
        self._now_ns = now_ns

    @property
    def receipt_file(self) -> str:
        return (
            PRODUCTION_RECEIPT_FILE
            if self.store.durability.independent
            else LOCAL_RECEIPT_FILE
        )

    def sweep(self, runs: Iterable[Path] | None = None) -> RunSweepResult:
        result = RunSweepResult()
        for run_directory in (
            discover_runs(self.output_root) if runs is None else runs
        ):
            outcome = self.consider(run_directory)
            result.outcomes.append(outcome)
            if outcome.status == CONFLICT:
                # An immutable key holding unexpected content is a namespace
                # failure, not one bad run. Continuing would write more objects
                # into a namespace already known to disagree with this host.
                result.halted = (
                    f"stopped after an immutable-key conflict on run {outcome.run_id}"
                )
                break
        return result

    # -- one run -----------------------------------------------------------

    def consider(self, run_directory: Path) -> RunOutcome:
        run_directory = Path(run_directory)
        run_id = run_directory.name
        receipt_path = run_directory / self.receipt_file

        if receipt_path.is_file():
            return self._outcome(
                run_id,
                run_directory,
                SKIPPED,
                f"{self.receipt_file} already records this run in {self.store.store_id}",
                receipt=str(receipt_path),
            )

        try:
            present = {
                path.name for path in run_directory.iterdir() if path.is_file()
            }
        except OSError as error:
            return self._outcome(
                run_id, run_directory, FAILED, f"cannot read the run directory: {error}"
            )

        if {SELECTION_REPORT_FILE, SELECTION_REPORT_METADATA_FILE} & present:
            try:
                required = required_run_files(run_directory)
            except RunArchiveError as error:
                return self._outcome(
                    run_id, run_directory, FAILED, f"RunArchiveError: {error}"
                )
        else:
            required = REQUIRED_FILES
        missing = sorted(required - present)
        if missing:
            age = self._age_ns(run_id)
            if age is None:
                return self._outcome(
                    run_id,
                    run_directory,
                    FAILED,
                    f"incomplete, and run id {run_id!r} names no instant to age it by",
                )
            if age < self.pending_floor_ns:
                return self._outcome(
                    run_id,
                    run_directory,
                    PENDING,
                    f"missing {', '.join(missing)}; the run may still be in progress",
                )
            return self._outcome(
                run_id,
                run_directory,
                FAILED,
                f"still missing {', '.join(missing)} after "
                f"{age / 3_600_000_000_000:.1f}h; a run process died before finishing it",
            )

        try:
            receipt = archive_run(run_directory, self.store)
        except IntegrityConflict as error:
            return self._outcome(run_id, run_directory, CONFLICT, str(error))
        except (RunArchiveError, ObjectStoreError, OSError) as error:
            return self._outcome(
                run_id, run_directory, FAILED, f"{type(error).__name__}: {error}"
            )
        return self._outcome(
            run_id,
            run_directory,
            ARCHIVED,
            f"{len(receipt.objects)} objects committed behind the remote manifest",
            receipt=str(receipt.path),
            prefix=receipt.prefix,
            object_count=len(receipt.objects),
        )

    def _age_ns(self, run_id: str) -> int | None:
        named = parse_run_id_ns(run_id)
        return None if named is None else self._now_ns() - named

    @staticmethod
    def _outcome(
        run_id: str,
        run_directory: Path,
        status: str,
        detail: str,
        *,
        receipt: str | None = None,
        prefix: str | None = None,
        object_count: int | None = None,
    ) -> RunOutcome:
        return RunOutcome(
            run_id=run_id,
            run_directory=str(run_directory),
            status=status,
            detail=detail,
            receipt=receipt,
            prefix=prefix,
            object_count=object_count,
        )
