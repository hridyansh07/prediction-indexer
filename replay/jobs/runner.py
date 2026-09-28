"""One-job, nonblocking durable Replay runner tick."""

from __future__ import annotations

import fcntl
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from archive.storage.factory import build_store
from replay.jobs import contracts as c
from replay.jobs.bundle import ensure_bundle
from replay.jobs.stages import (
    HistoryClient,
    LocalStateError,
    StageFailure,
    adopt_published_for_row,
    archive_stage,
    bundle_stage,
    initialize_job_root,
    prepare_stage,
    read_regular,
    read_stage,
    record_archive_diagnostic,
    resolve_stage,
    run_stage,
    supervisor_config,
    validate_committed_markers,
    validate_job_root,
)
from replay.preparation import load_snapshot
from universe.replay_jobs import ReplayJobStore


@dataclass(frozen=True)
class Runtime:
    data_root: Path
    materializer: Path
    publisher: Path
    python: Path
    redis_url: str
    image_revision: str


class _UnusedSubmissionLimits:
    max_active_jobs_total = 1
    max_active_jobs_per_submitter = 1
    max_queued_jobs_total = 1


class Runner:
    def __init__(
        self,
        job_store,
        archive_store,
        config: c.RunnerConfig,
        runtime: Runtime,
        *,
        clock=time.time_ns,
        history=None,
        ensure=ensure_bundle,
    ):
        self.jobs = job_store
        self.archive = archive_store
        self.config = config
        self.runtime = runtime
        self.clock = clock
        self.history = history or HistoryClient(config.universe_base_url)
        self.ensure = ensure
        # (receipt, receipt_raw, pins) per job, obtained at most once per tick.
        self._tick_bundles = {}

    @property
    def jobs_root(self):
        return self.runtime.data_root / "jobs"

    def tick(self) -> None:
        self._tick_bundles = {}
        try:
            self._tick()
        finally:
            self._tick_bundles = {}

    def _tick(self) -> None:
        claim = self.jobs.claim_next(self.config.orchestration, self.clock())
        if claim is None or claim.row.status == c.ARCHIVE_BLOCKED:
            return
        row = claim.row
        root = self.jobs_root / row.job_id
        if row.status == c.ARCHIVING:
            try:
                frozen = adopt_published_for_row(self.archive, row)
                if frozen is not None:
                    self._save(row, c.finish(row, frozen, self.clock()))
                    return
            except StageFailure as error:
                if error.code == "archive_conflict":
                    self._block(row, "archive_conflict")
                    return
                # Availability is handled by the ordinary local resume path;
                # if local state is gone, archival retry remains retryable.
        try:
            retrying_initialization = (
                row.status == c.RUNNING
                and row.stage == "resolve"
                and row.reason_code == "resource_exhausted"
                and not root.exists()
            )
            if claim.mode == "initialize" or retrying_initialization:
                root = initialize_job_root(self.jobs_root, row.job_id, claim.request_bytes)
            else:
                validate_job_root(root, claim.request_bytes, row.request_sha256)
            validate_committed_markers(root, row.stage)
        except OSError as error:
            self._save(
                row,
                c.retry_later(
                    row,
                    "resource_exhausted",
                    str(error),
                    self.config.orchestration,
                    self.clock(),
                ),
            )
            return
        except LocalStateError as error:
            row = self._save(row, c.lose_local_state(row, error.detail, self.clock()))
            return self._archive(row, None)

        if row.status == c.ARCHIVING:
            return self._archive(row, root)

        try:
            request = c.parse_request(
                claim.request_bytes, self.config, accept_retired=True
            )
        except Exception as error:
            row = self._save(
                row, c.fail(row, "internal_failure", str(error), self.clock())
            )
            return self._archive(row, root)

        while row.status == c.RUNNING:
            try:
                row = self._work_stage(row, root, request)
            except LocalStateError as error:
                row = self._save(row, c.lose_local_state(row, error.detail, self.clock()))
            except StageFailure as error:
                if error.code in c.RETRYABLE_CODES:
                    row = self._save(
                        row,
                        c.retry_later(
                            row,
                            error.code,
                            error.detail,
                            self.config.orchestration,
                            self.clock(),
                        ),
                    )
                    return
                row = self._save(row, c.fail(row, error.code, error.detail, self.clock()))
            except OSError as error:
                row = self._save(
                    row,
                    c.retry_later(
                        row,
                        "resource_exhausted",
                        str(error),
                        self.config.orchestration,
                        self.clock(),
                    ),
                )
                return
            except Exception as error:
                row = self._save(
                    row,
                    c.fail(row, "internal_failure", str(error), self.clock()),
                )
        if row.status == c.ARCHIVING:
            self._archive(row, root)

    def _work_stage(self, row, root, request):
        if row.stage == "resolve":
            resolve_stage(root, row.job_id, request, self.config, self.history)
            return self._save(row, c.advance(row, "bundle", self.clock()))

        resolved = c.parse_resolved_job(
            read_regular(root / "resolved.json", c.MAX_RESOLVED_JOB_BYTES)
        )
        bundled = self._tick_bundles.get(row.job_id)
        if bundled is None:
            bundled = bundle_stage(
                root,
                request,
                resolved,
                self.ensure,
                store=self.archive,
                work_root=self.runtime.data_root / "bundle-work",
                derivatives_root=self.runtime.data_root / "derivatives",
                materializer=self.runtime.materializer,
                window_seconds=self.config.canonical_window_seconds,
            )
            # Later stages in this tick reuse these pins; a new tick (a
            # resume after a crash or retry) obtains them again.
            self._tick_bundles[row.job_id] = bundled
        receipt, receipt_raw, pins = bundled
        if row.stage == "bundle":
            return self._save(row, c.advance(row, "prepare", self.clock()))

        snapshot = prepare_stage(
            root, request, resolved, receipt, pins, self.config
        )
        if row.stage == "prepare":
            return self._save(row, c.advance(row, "run", self.clock()))

        document = supervisor_config(
            row.job_id,
            request,
            self.config,
            receipt,
            pins,
            snapshot,
            root / "context",
            publisher=self.runtime.publisher,
            python=self.runtime.python,
            image_revision=self.runtime.image_revision,
        )
        run_stage(
            root,
            document,
            python=self.runtime.python,
            redis_url=self.runtime.redis_url,
            scratch_root=self.runtime.data_root / ".runner",
        )
        if row.stage == "run":
            return self._save(row, c.advance(row, "read", self.clock()))

        snapshot = load_snapshot(root / "context")
        read_stage(root, row.job_id, request, receipt_raw, snapshot, self.config)
        return self._save(row, c.succeed(row, self.clock()))

    def _archive(self, row, root):
        try:
            frozen = archive_stage(
                root,
                row,
                self.archive,
                self.runtime.image_revision,
                self.clock(),
            )
            self._save(row, c.finish(row, frozen, self.clock()))
        except LocalStateError as error:
            if root is not None:
                replaced = c.lose_local_state(row, error.detail, self.clock())
                row = self._save(row, replaced)
                return self._archive(row, None)
            self._block(row, "archive_conflict")
        except StageFailure as error:
            if error.code == "archive_unavailable":
                if root is not None and (root / "archive_state.json").exists():
                    try:
                        record_archive_diagnostic(
                            root, f"archive_unavailable: {error.detail or ''}"
                        )
                    except OSError:
                        pass
                self._save(
                    row,
                    c.retry_later(
                        row,
                        error.code,
                        error.detail,
                        self.config.orchestration,
                        self.clock(),
                    ),
                )
            else:
                self._block(row, "archive_conflict")
        except c.ContractError:
            self._block(row, "archive_conflict")

    def _block(self, row, code):
        self._save(row, c.block_archive(row, code, self.clock()))

    def _save(self, before, after):
        return self.jobs.save(before, after)


def run_tick(runner: Runner, lock_path: Path) -> bool:
    """Run one tick under a global nonblocking flock. Returns false when busy."""
    lock_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        runner.tick()
        return True
    finally:
        os.close(descriptor)


def runtime_from_environment(environ=None) -> Runtime:
    environment = os.environ if environ is None else environ
    required = {
        name: environment.get(name, "")
        for name in (
            "REPLAY_DATA_ROOT",
            "REPLAY_MATERIALIZER",
            "REPLAY_PUBLISHER",
            "REPLAY_IMAGE_REVISION",
            "REDIS_URL",
        )
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        raise SystemExit("missing required runner environment: " + ", ".join(missing))
    return Runtime(
        Path(required["REPLAY_DATA_ROOT"]),
        Path(required["REPLAY_MATERIALIZER"]),
        Path(required["REPLAY_PUBLISHER"]),
        Path(sys.executable),
        required["REDIS_URL"],
        required["REPLAY_IMAGE_REVISION"],
    )


def production_runner(config_path: Path, environ=None) -> Runner:
    runtime = runtime_from_environment(environ)
    config = c.parse_runner_config(Path(config_path).read_bytes())
    jobs = ReplayJobStore(runtime.data_root / "jobs.sqlite3", _UnusedSubmissionLimits())
    jobs.initialize()
    archive = build_store([runtime.data_root], environ=environ)
    return Runner(jobs, archive, config, runtime)


def main(argv=None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if not arguments or arguments[0] != "tick" or len(arguments) > 2:
        raise SystemExit("usage: python -m replay.jobs tick [RUNNER_CONFIG]")
    config_path = Path(arguments[1] if len(arguments) == 2 else "configs/replay_runner.json")
    runner = production_runner(config_path)
    run_tick(runner, runner.runtime.data_root / "runner.lock")
    return 0
