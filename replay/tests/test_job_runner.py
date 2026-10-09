from __future__ import annotations

import errno
import fcntl
import json
import os
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from types import MappingProxyType
from unittest import mock

from archive.storage import LocalObjectStore
from archive.storage.base import ObjectStoreError
from replay.jobs import contracts as c
from replay.jobs.runner import Runner, Runtime, run_tick
from replay.jobs.stages import (
    HistoryClient,
    LocalStateError,
    StageFailure,
    _archive_files,
    archive_stage,
    initialize_job_root,
    parse_archive_state,
    validate_committed_markers,
    validate_job_root,
    verify_published_receipt,
    write_marker,
)
from targeter.v2.models import isoformat, parse_timestamp
from universe.jobs.auth import Principal
from universe.jobs.store import ReplayJobStore


ROOT = Path(__file__).resolve().parents[2]
ADDRESS = "0x" + "11" * 20


class Limits:
    max_active_jobs_total = 10
    max_active_jobs_per_submitter = 10
    max_queued_jobs_total = 10


def runner_config():
    return c.parse_runner_config((ROOT / "configs/replay_runner.json").read_bytes())


def renamed_runner_config():
    original = runner_config()
    return replace(
        original,
        limits=MappingProxyType({"standard": original.limits["small"]}),
    )


def retired_runner_config():
    original = runner_config()
    entry = original.strategies["bundle_coverage"]
    return replace(
        original,
        strategies=MappingProxyType(
            {"bundle_coverage": replace(entry, status=c.STRATEGY_RETIRED)}
        ),
    )


def request_bytes():
    return json.dumps(
        {
            "replay_request_version": 1,
            "bundle_id": "bundle-1",
            "probe_markets": None,
            "interval": None,
            "strategy": {"name": "bundle_coverage", "config": {}},
            "limits": "small",
        },
        indent=2,
    ).encode()


def source(run_id):
    prefix = f"targeter-v2/runs/date=2026-09-20/run={run_id}"
    return {
        "manifest_key": prefix + "/run_manifest.json",
        "manifest_sha256": "a" * 64,
        "report_key": prefix + "/selection_report.json.zst",
        "report_sha256": "b" * 64,
    }


def selection(run_id, generated_at, retired_at=None):
    retirement = None
    if retired_at is not None:
        retirement = {
            "retired_at": retired_at,
            "disposition": "all_markets_terminal",
            "terminal_observed_at": retired_at,
            "source": {"run_id": "retiring-run", **source("retiring-run")},
        }
    return {
        "run_id": run_id,
        "generated_at": generated_at,
        "bundle_id": "bundle-1",
        "occurrence_kind": "complete",
        "continuity_selected": True,
        "continuity_disposition": "held_current_candidate",
        "sport": "esports",
        "game": "lol",
        "topology": "series",
        "activation_at": generated_at,
        "capture_start_at": generated_at,
        "retirement": retirement,
        "source": source(run_id),
        "origin": {"run_id": run_id, "generated_at": generated_at, **source(run_id)},
    }


class Response:
    status = 200

    def __init__(self, value):
        self.raw = json.dumps(value, separators=(",", ":")).encode()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return None

    def read(self, maximum):
        return self.raw[:maximum]


class Opener:
    def __init__(self, pages):
        self.pages = iter(pages)
        self.urls = []

    def open(self, url, timeout):
        self.urls.append(url)
        return Response(next(self.pages))


class RecordingStore:
    def __init__(self, inner, fail_at=None):
        self.inner = inner
        self.fail_at = fail_at
        self.puts = []

    def __getattr__(self, name):
        return getattr(self.inner, name)

    def put_immutable(self, key, reader, identity, **kwargs):
        self.puts.append(key)
        if self.fail_at == len(self.puts):
            raise ObjectStoreError("injected archive outage")
        return self.inner.put_immutable(key, reader, identity, **kwargs)


class LocalStateTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.raw = request_bytes()
        self.request = c.parse_request(self.raw, runner_config())
        self.job_id = "20260924T120000Z-0123456789abcdef"

    def tearDown(self):
        self.temp.cleanup()

    def test_initialize_writes_exact_private_fsynced_shape_and_resume_validates(self):
        job = initialize_job_root(self.root / "jobs", self.job_id, self.raw)
        self.assertEqual((job / "request.json").read_bytes(), self.raw)
        self.assertEqual(stat_mode(job), 0o700)
        self.assertEqual(stat_mode(job / "request.json"), 0o600)
        validate_job_root(job, self.raw, self.request.sha256)
        self.assertEqual(
            initialize_job_root(self.root / "jobs", self.job_id, self.raw), job
        )
        with self.assertRaises(LocalStateError):
            initialize_job_root(self.root / "jobs", self.job_id, b"{}")

    def test_fixed_marker_temp_from_crash_is_removed_before_create(self):
        path = self.root / "bundle.json"
        stale = path.with_name(f".{path.name}.{os.getpid()}.open")
        stale.write_bytes(b"partial")
        write_marker(path, b"committed")
        self.assertEqual(path.read_bytes(), b"committed")
        self.assertFalse(stale.exists())

    def test_resume_removes_only_top_level_open_temps(self):
        job = initialize_job_root(self.root / "jobs", self.job_id, self.raw)
        stale = job / ".bundle.json.4242.open"
        stale.write_bytes(b"partial")
        validate_job_root(job, self.raw, self.request.sha256)
        self.assertFalse(stale.exists())

    def test_resume_rejects_missing_tampered_symlink_and_nonregular_request(self):
        cases = ("missing", "tampered", "symlink", "fifo")
        for index, case in enumerate(cases):
            with self.subTest(case=case):
                job = initialize_job_root(self.root / f"jobs-{index}", self.job_id, self.raw)
                request = job / "request.json"
                if case == "missing":
                    request.unlink()
                elif case == "tampered":
                    request.write_bytes(b"{}")
                elif case == "symlink":
                    request.unlink()
                    request.symlink_to(self.root / "outside")
                else:
                    request.unlink()
                    os.mkfifo(request)
                with self.assertRaises(LocalStateError):
                    validate_job_root(job, self.raw, self.request.sha256)

    def test_uncommitted_preparation_directory_is_local_state_lost(self):
        job = initialize_job_root(self.root / "jobs", self.job_id, self.raw)
        context = job / "context"
        context.mkdir()
        (context / "context.json").write_bytes(b"{}")
        with self.assertRaises(LocalStateError):
            validate_committed_markers(job, "prepare")


class HistoryTests(unittest.TestCase):
    def test_paginates_selected_history_and_closes_interval_at_coherent_retirement(self):
        first = "2026-09-20T12:00:00Z"
        second = "2026-09-20T12:30:00Z"
        retired = "2026-09-20T13:00:00Z"
        opener = Opener(
            [
                {"selections": [selection("run-a", first, retired)], "sort": "selected", "next_cursor": "next"},
                {"selections": [selection("run-b", second, retired)], "sort": "selected", "next_cursor": None},
            ]
        )
        client = HistoryClient("http://universe", opener=opener)
        request = c.parse_request(request_bytes(), runner_config())
        resolved = client.resolve("20260924T120000Z-0123456789abcdef", request, runner_config())
        self.assertEqual(len(resolved.occurrences), 2)
        self.assertEqual(resolved.bundle_interval, (ns(first), ns(retired)))
        self.assertIn("sort=selected&limit=100", opener.urls[0])
        self.assertIn("cursor=next", opener.urls[1])

    def test_rejects_cursor_loops_duplicates_incoherent_retirement_and_active_bundle(self):
        at = "2026-09-20T12:00:00Z"
        later = "2026-09-20T13:00:00Z"
        request = c.parse_request(request_bytes(), runner_config())
        cases = {
            "loop": [
                {"selections": [selection("run-a", at, later)], "sort": "selected", "next_cursor": "same"},
                {"selections": [], "sort": "selected", "next_cursor": "same"},
            ],
            "duplicate": [
                {"selections": [selection("run-a", at, later)], "sort": "selected", "next_cursor": "next"},
                {"selections": [selection("run-a", at, later)], "sort": "selected", "next_cursor": None},
            ],
            "incoherent": [
                {
                    "selections": [
                        selection("run-a", at, later),
                        selection("run-b", "2026-09-20T12:30:00Z", "2026-09-20T13:30:00Z"),
                    ],
                    "sort": "selected",
                    "next_cursor": None,
                }
            ],
        }
        for name, pages in cases.items():
            with self.subTest(name=name), self.assertRaises(StageFailure) as caught:
                HistoryClient("http://universe", opener=Opener(pages)).resolve(
                    "20260924T120000Z-0123456789abcdef", request, runner_config()
                )
            self.assertEqual(caught.exception.code, "bundle_history_invalid")
        active = [{"selections": [selection("run-a", at)], "sort": "selected", "next_cursor": None}]
        with self.assertRaises(StageFailure) as caught:
            HistoryClient("http://universe", opener=Opener(active)).resolve(
                "20260924T120000Z-0123456789abcdef", request, runner_config()
            )
        self.assertEqual(caught.exception.code, "bundle_not_retired")


class ArchiveTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.raw = request_bytes()
        request = c.parse_request(self.raw, runner_config())
        self.row = c.cancel(
            c.new_job(
                "20260924T120000Z-0123456789abcdef",
                submitted_by=ADDRESS,
                request_sha256=request.sha256,
                now_ns=10,
            ),
            20,
        )
        self.job = initialize_job_root(self.root / "jobs", self.row.job_id, self.raw)

    def tearDown(self):
        self.temp.cleanup()

    def test_receipt_is_last_archive_state_is_excluded_and_remote_is_adopted(self):
        inner = LocalObjectStore(self.root / "objects")
        store = RecordingStore(inner)
        frozen = archive_stage(self.job, self.row, store, "revision", 30)
        self.assertEqual(frozen, 30)
        self.assertEqual(store.puts[-1], c.job_receipt_key(self.row.job_id))
        keys = tuple(inner.list_keys(f"replay/jobs/{self.row.job_id}/"))
        self.assertFalse(any(key.endswith("archive_state.json") for key in keys))
        before = tuple(store.puts)
        self.assertEqual(archive_stage(self.job, self.row, store, "revision", 999), 30)
        self.assertEqual(tuple(store.puts), before)
        verified = verify_published_receipt(inner, self.row.job_id)
        self.assertEqual(verified.job_id, self.row.job_id)
        self.assertEqual(verified.final_outcome, "cancelled")

    def test_crash_before_each_upload_resumes_with_same_frozen_receipt(self):
        for fail_at in (1, 2):
            with self.subTest(fail_at=fail_at):
                case = self.root / str(fail_at)
                job = initialize_job_root(case / "jobs", self.row.job_id, self.raw)
                inner = LocalObjectStore(case / "objects")
                store = RecordingStore(inner, fail_at=fail_at)
                with self.assertRaises(StageFailure) as caught:
                    archive_stage(job, self.row, store, "revision", 30)
                self.assertEqual(caught.exception.code, "archive_unavailable")
                frozen = (job / "job_receipt.json").read_bytes()
                store.fail_at = None
                self.assertEqual(archive_stage(job, self.row, store, "revision-b", 999), 30)
                self.assertEqual((job / "job_receipt.json").read_bytes(), frozen)
                self.assertEqual(c.parse_job_receipt(frozen).image_revision, "revision")

    def test_archive_removes_top_level_open_temp_before_traversal(self):
        (self.job / ".bundle.json.4242.open").write_bytes(b"partial")
        archive_stage(
            self.job,
            self.row,
            LocalObjectStore(self.root / "objects-open"),
            "revision",
            30,
        )
        self.assertFalse((self.job / ".bundle.json.4242.open").exists())

    def test_every_multi_object_upload_boundary_is_idempotent(self):
        from dataclasses import replace

        from replay.tests.test_jobs_contracts import bundle_receipt, resolved

        failed = c.fail(
            c.claim(
                c.new_job(
                    self.row.job_id,
                    submitted_by=ADDRESS,
                    request_sha256=c.parse_request(self.raw, runner_config()).sha256,
                    now_ns=10,
                ),
                runner_config().orchestration,
                20,
            ),
            "tool_failure",
            "failed after bundle",
            21,
        )
        for fail_at in range(1, 5):
            with self.subTest(fail_at=fail_at):
                case = self.root / f"multi-{fail_at}"
                job = initialize_job_root(case / "jobs", failed.job_id, self.raw)
                (job / "resolved.json").write_bytes(
                    c.resolved_job_bytes(replace(resolved(), request_sha256=failed.request_sha256))
                )
                (job / "bundle.json").write_bytes(c.bundle_receipt_bytes(bundle_receipt()))
                inner = LocalObjectStore(case / "objects")
                store = RecordingStore(inner, fail_at=fail_at)
                with self.assertRaises(StageFailure):
                    archive_stage(job, failed, store, "revision", 30)
                frozen = (job / "job_receipt.json").read_bytes()
                store.fail_at = None
                self.assertEqual(archive_stage(job, failed, store, "revision", 99), 30)
                self.assertEqual((job / "job_receipt.json").read_bytes(), frozen)

    def test_archive_rejects_symlink_hardlink_fifo_unexpected_and_limits(self):
        context = self.job / "context"
        context.mkdir()
        original = context / "a"
        original.write_bytes(b"x")
        for name, make in (
            ("symlink", lambda path: path.symlink_to(original)),
            ("hardlink", lambda path: os.link(original, path)),
            ("fifo", lambda path: os.mkfifo(path)),
        ):
            path = context / name
            make(path)
            with self.subTest(name=name), self.assertRaises(LocalStateError):
                _archive_files(self.job)
            path.unlink()
        original.unlink()
        (self.job / "unexpected").write_bytes(b"x")
        with self.assertRaises(LocalStateError):
            _archive_files(self.job)


class CancelledRunnerAcceptanceTests(unittest.TestCase):
    def test_cancelled_job_initializes_archives_and_finishes_in_one_tick(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            jobs = ReplayJobStore(root / "jobs.sqlite3", Limits(), suffix=lambda: "0123456789abcdef")
            jobs.initialize()
            config = runner_config()
            raw = request_bytes()
            submitted = jobs.submit(
                raw,
                c.parse_request(raw, config),
                Principal(ADDRESS, "member"),
                "cancelled",
                10,
                bundle_exists=True,
            )
            jobs.cancel_job(submitted.row.job_id, Principal(ADDRESS, "member"), 20)
            archive = LocalObjectStore(root / "objects")
            times = iter(range(30, 100))
            runtime = Runtime(root, Path("/unused"), Path("/unused"), Path(os.sys.executable), "redis://unused", "revision")
            Runner(jobs, archive, config, runtime, clock=lambda: next(times)).tick()
            row, _ = jobs.get_job(submitted.row.job_id)
            self.assertEqual(row.status, c.CANCELLED)
            self.assertIsNotNone(archive.head(c.job_receipt_key(row.job_id)))
            self.assertEqual((root / "jobs" / row.job_id / "request.json").read_bytes(), raw)

    def test_published_receipt_is_adopted_after_local_state_loss_before_sqlite_finish(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            jobs = ReplayJobStore(root / "jobs.sqlite3", Limits(), suffix=lambda: "0123456789abcdef")
            jobs.initialize()
            config = runner_config()
            raw = request_bytes()
            submitted = jobs.submit(
                raw,
                c.parse_request(raw, config),
                Principal(ADDRESS, "member"),
                "adopt",
                10,
                bundle_exists=True,
            )
            row = jobs.cancel_job(submitted.row.job_id, Principal(ADDRESS, "member"), 20)
            claimed = jobs.claim_next(config.orchestration, 30).row
            job = initialize_job_root(root / "jobs", row.job_id, raw)
            archive = LocalObjectStore(root / "objects")
            self.assertEqual(archive_stage(job, claimed, archive, "revision", 40), 40)
            for path in sorted(job.rglob("*"), reverse=True):
                path.unlink() if path.is_file() else path.rmdir()
            job.rmdir()

            runtime = Runtime(root, Path("/unused"), Path("/unused"), Path(os.sys.executable), "redis://unused", "revision-b")
            Runner(jobs, archive, config, runtime, clock=lambda: 100_000_000_000).tick()
            finished, _ = jobs.get_job(row.job_id)
            self.assertEqual(finished.status, c.CANCELLED)
            self.assertEqual(finished.finished_at_ns, 40)

    def test_cancelled_archive_blocked_resume_reuses_identical_initialized_root(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            jobs = ReplayJobStore(root / "jobs.sqlite3", Limits(), suffix=lambda: "0123456789abcdef")
            jobs.initialize()
            config = replace(
                runner_config(),
                orchestration=replace(runner_config().orchestration, max_stage_attempts=1),
            )
            raw = request_bytes()
            submitted = jobs.submit(
                raw,
                c.parse_request(raw, config),
                Principal(ADDRESS, "member"),
                "cancelled-blocked",
                10,
                bundle_exists=True,
            )
            jobs.cancel_job(submitted.row.job_id, Principal(ADDRESS, "member"), 20)
            archive = RecordingStore(LocalObjectStore(root / "objects"), fail_at=1)
            runtime = Runtime(root, Path("/unused"), Path("/unused"), Path(os.sys.executable), "redis://unused", "revision")
            Runner(jobs, archive, config, runtime, clock=iter((30, 31, 32, 33)).__next__).tick()
            Runner(jobs, archive, config, runtime, clock=lambda: 100_000_000_000).tick()
            blocked, _ = jobs.get_job(submitted.row.job_id)
            self.assertEqual(blocked.status, c.ARCHIVE_BLOCKED)
            jobs.save(blocked, c.resume_blocked(blocked, 100_000_000_001))
            archive.fail_at = None
            Runner(jobs, archive, config, runtime, clock=lambda: 200_000_000_000).tick()
            finished, _ = jobs.get_job(submitted.row.job_id)
            self.assertEqual(finished.status, c.CANCELLED)

    def test_request_initialization_enospc_is_retryable_and_does_not_escape_tick(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            jobs = ReplayJobStore(root / "jobs.sqlite3", Limits(), suffix=lambda: "0123456789abcdef")
            jobs.initialize()
            config = runner_config()
            raw = request_bytes()
            submitted = jobs.submit(
                raw,
                c.parse_request(raw, config),
                Principal(ADDRESS, "member"),
                "request-enospc",
                10,
                bundle_exists=True,
            )
            runtime = Runtime(root, Path("/unused"), Path("/unused"), Path(os.sys.executable), "redis://unused", "revision")
            with mock.patch(
                "replay.jobs.stages.write_marker",
                side_effect=OSError(errno.ENOSPC, "disk full"),
            ):
                Runner(
                    jobs,
                    LocalObjectStore(root / "objects"),
                    config,
                    runtime,
                    clock=iter((20, 21)).__next__,
                ).tick()
            waiting, _ = jobs.get_job(submitted.row.job_id)
            self.assertEqual(
                (waiting.status, waiting.stage, waiting.reason_code),
                (c.RUNNING, "resolve", "resource_exhausted"),
            )
            with mock.patch.object(
                Runner,
                "_work_stage",
                side_effect=StageFailure("universe_unavailable", "still waiting"),
            ):
                Runner(
                    jobs,
                    LocalObjectStore(root / "objects"),
                    config,
                    runtime,
                    clock=iter((100_000_000_000, 100_000_000_001)).__next__,
                ).tick()
            retried, _ = jobs.get_job(submitted.row.job_id)
            self.assertEqual(
                (retried.status, retried.reason_code),
                (c.RUNNING, "universe_unavailable"),
            )
            self.assertEqual(
                (root / "jobs" / retried.job_id / "request.json").read_bytes(), raw
            )

    def test_crash_after_initialize_claim_before_root_fails_local_state_lost_on_resume(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            jobs = ReplayJobStore(root / "jobs.sqlite3", Limits(), suffix=lambda: "0123456789abcdef")
            jobs.initialize()
            config = runner_config()
            raw = request_bytes()
            submitted = jobs.submit(
                raw,
                c.parse_request(raw, config),
                Principal(ADDRESS, "member"),
                "claimed-no-root",
                10,
                bundle_exists=True,
            )
            claimed = jobs.claim_next(config.orchestration, 20)
            self.assertEqual(claimed.mode, "initialize")
            archive = LocalObjectStore(root / "objects")
            runtime = Runtime(root, Path("/unused"), Path("/unused"), Path(os.sys.executable), "redis://unused", "revision")
            Runner(jobs, archive, config, runtime, clock=lambda: 100_000_000_000).tick()
            finished, _ = jobs.get_job(submitted.row.job_id)
            self.assertEqual((finished.status, finished.reason_code), (c.FAILED, "local_state_lost"))
            with archive.open(
                c.job_receipt_key(finished.job_id),
                max_bytes=c.MAX_JOB_RECEIPT_BYTES,
            ) as source:
                receipt = c.parse_job_receipt(source.read())
            self.assertEqual(receipt.objects, ())

    def test_archive_retry_diagnostic_is_local_and_pending_detail_is_unchanged(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            jobs = ReplayJobStore(root / "jobs.sqlite3", Limits(), suffix=lambda: "0123456789abcdef")
            jobs.initialize()
            config = runner_config()
            raw = request_bytes()
            submitted = jobs.submit(
                raw,
                c.parse_request(raw, config),
                Principal(ADDRESS, "member"),
                "retry",
                10,
                bundle_exists=True,
            )
            original = jobs.cancel_job(submitted.row.job_id, Principal(ADDRESS, "member"), 20)
            unavailable = RecordingStore(LocalObjectStore(root / "objects"), fail_at=1)
            runtime = Runtime(root, Path("/unused"), Path("/unused"), Path(os.sys.executable), "redis://unused", "revision")
            Runner(jobs, unavailable, config, runtime, clock=iter(range(30, 100)).__next__).tick()
            waiting, _ = jobs.get_job(original.job_id)
            self.assertEqual(waiting.reason_code, original.reason_code)
            self.assertEqual(waiting.reason_detail, original.reason_detail)
            state = parse_archive_state(
                (root / "jobs" / original.job_id / "archive_state.json").read_bytes()
            )
            self.assertIn("archive_unavailable", state.diagnostic)
            self.assertFalse(any(key.endswith("archive_state.json") for key in unavailable.inner.list_keys("replay/")))

    def test_archiving_uses_frozen_local_receipt_after_current_preset_is_renamed(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            jobs = ReplayJobStore(root / "jobs.sqlite3", Limits(), suffix=lambda: "0123456789abcdef")
            jobs.initialize()
            original_config = runner_config()
            raw = request_bytes()
            submitted = jobs.submit(
                raw,
                c.parse_request(raw, original_config),
                Principal(ADDRESS, "member"),
                "renamed-after-freeze",
                10,
                bundle_exists=True,
            )
            jobs.cancel_job(submitted.row.job_id, Principal(ADDRESS, "member"), 20)
            archive = RecordingStore(LocalObjectStore(root / "objects"), fail_at=1)
            runtime = Runtime(root, Path("/unused"), Path("/unused"), Path(os.sys.executable), "redis://unused", "revision")
            Runner(
                jobs,
                archive,
                original_config,
                runtime,
                clock=iter(range(30, 100)).__next__,
            ).tick()
            job = root / "jobs" / submitted.row.job_id
            frozen_receipt = (job / "job_receipt.json").read_bytes()
            frozen = c.parse_job_receipt(frozen_receipt)
            self.assertEqual(frozen.finished_at_ns, 31)
            self.assertEqual(
                [item.key.rsplit("/", 1)[-1] for item in frozen.objects],
                ["request.json"],
            )

            archive.fail_at = None
            Runner(
                jobs,
                archive,
                renamed_runner_config(),
                runtime,
                clock=lambda: 1_001_000_000_000,
            ).tick()
            finished, _ = jobs.get_job(submitted.row.job_id)
            self.assertEqual(finished.status, c.CANCELLED)
            self.assertEqual(finished.finished_at_ns, frozen.finished_at_ns)
            self.assertEqual((job / "job_receipt.json").read_bytes(), frozen_receipt)
            with archive.inner.open(
                c.job_receipt_key(finished.job_id), max_bytes=c.MAX_JOB_RECEIPT_BYTES
            ) as source:
                self.assertEqual(source.read(), frozen_receipt)

    def test_archiving_success_skips_current_request_validation_and_keeps_root(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            jobs = ReplayJobStore(root / "jobs.sqlite3", Limits(), suffix=lambda: "0123456789abcdef")
            jobs.initialize()
            original_config = runner_config()
            raw = request_bytes()
            submitted = jobs.submit(
                raw,
                c.parse_request(raw, original_config),
                Principal(ADDRESS, "member"),
                "succeeded-before-rename",
                10,
                bundle_exists=True,
            )
            row = jobs.claim_next(original_config.orchestration, 20).row
            job = initialize_job_root(root / "jobs", row.job_id, raw)
            for stage, now in zip(c.WORK_STAGES[1:], range(21, 25)):
                row = jobs.save(row, c.advance(row, stage, now))
            row = jobs.save(row, c.succeed(row, 25))
            runtime = Runtime(root, Path("/unused"), Path("/unused"), Path(os.sys.executable), "redis://unused", "revision")
            with mock.patch("replay.jobs.runner.archive_stage", return_value=31) as archive:
                Runner(
                    jobs,
                    LocalObjectStore(root / "objects"),
                    renamed_runner_config(),
                    runtime,
                    clock=lambda: 100_000_000_000,
                ).tick()
            self.assertEqual(archive.call_args.args[0], job)
            finished, _ = jobs.get_job(submitted.row.job_id)
            self.assertEqual((finished.status, finished.finished_at_ns), (c.SUCCEEDED, 31))

    def test_running_request_revalidation_failure_archives_existing_request_evidence(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            jobs = ReplayJobStore(root / "jobs.sqlite3", Limits(), suffix=lambda: "0123456789abcdef")
            jobs.initialize()
            original_config = runner_config()
            raw = request_bytes()
            submitted = jobs.submit(
                raw,
                c.parse_request(raw, original_config),
                Principal(ADDRESS, "member"),
                "running-before-rename",
                10,
                bundle_exists=True,
            )
            claimed = jobs.claim_next(original_config.orchestration, 20)
            job = initialize_job_root(root / "jobs", claimed.row.job_id, raw)
            archive = LocalObjectStore(root / "objects")
            runtime = Runtime(root, Path("/unused"), Path("/unused"), Path(os.sys.executable), "redis://unused", "revision")
            Runner(
                jobs,
                archive,
                renamed_runner_config(),
                runtime,
                clock=lambda: 100_000_000_000,
            ).tick()
            finished, _ = jobs.get_job(submitted.row.job_id)
            self.assertEqual((finished.status, finished.reason_code), (c.FAILED, "internal_failure"))
            with archive.open(
                c.job_receipt_key(finished.job_id), max_bytes=c.MAX_JOB_RECEIPT_BYTES
            ) as source:
                receipt = c.parse_job_receipt(source.read())
            self.assertEqual(
                [item.key.rsplit("/", 1)[-1] for item in receipt.objects],
                ["request.json"],
            )
            self.assertEqual((job / "request.json").read_bytes(), raw)

    def test_job_accepted_before_strategy_retirement_keeps_running(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            jobs = ReplayJobStore(root / "jobs.sqlite3", Limits(), suffix=lambda: "0123456789abcdef")
            jobs.initialize()
            raw = request_bytes()
            submitted = jobs.submit(
                raw,
                c.parse_request(raw, runner_config()),
                Principal(ADDRESS, "member"),
                "accepted-before-retirement",
                10,
                bundle_exists=True,
            )
            runtime = Runtime(root, Path("/unused"), Path("/unused"), Path(os.sys.executable), "redis://unused", "revision")
            with mock.patch.object(
                Runner,
                "_work_stage",
                side_effect=StageFailure("universe_unavailable", "injected"),
            ) as work:
                Runner(
                    jobs,
                    LocalObjectStore(root / "objects"),
                    retired_runner_config(),
                    runtime,
                    clock=lambda: 100_000_000_000,
                ).tick()
            self.assertEqual(work.call_args.args[2].strategy, "bundle_coverage")
            row, _ = jobs.get_job(submitted.row.job_id)
            self.assertEqual(
                (row.status, row.stage, row.reason_code),
                (c.RUNNING, "resolve", "universe_unavailable"),
            )

    def test_archive_contract_error_blocks_instead_of_escaping(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            jobs = ReplayJobStore(root / "jobs.sqlite3", Limits(), suffix=lambda: "0123456789abcdef")
            jobs.initialize()
            config = runner_config()
            raw = request_bytes()
            submitted = jobs.submit(
                raw,
                c.parse_request(raw, config),
                Principal(ADDRESS, "member"),
                "contract-error",
                10,
                bundle_exists=True,
            )
            jobs.cancel_job(submitted.row.job_id, Principal(ADDRESS, "member"), 20)
            runtime = Runtime(root, Path("/unused"), Path("/unused"), Path(os.sys.executable), "redis://unused", "revision")
            with mock.patch(
                "replay.jobs.runner.archive_stage",
                side_effect=c.ContractError("invalid receipt"),
            ):
                Runner(
                    jobs,
                    LocalObjectStore(root / "objects"),
                    config,
                    runtime,
                    clock=iter(range(30, 100)).__next__,
                ).tick()
            blocked, _ = jobs.get_job(submitted.row.job_id)
            self.assertEqual(
                (blocked.status, blocked.blocked_reason_code),
                (c.ARCHIVE_BLOCKED, "archive_conflict"),
            )

    def test_bundle_pins_are_obtained_once_per_job_per_tick(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            jobs = ReplayJobStore(root / "jobs.sqlite3", Limits(), suffix=lambda: "0123456789abcdef")
            jobs.initialize()
            config = runner_config()
            raw = request_bytes()
            submitted = jobs.submit(
                raw,
                c.parse_request(raw, config),
                Principal(ADDRESS, "member"),
                "bundle-once",
                10,
                bundle_exists=True,
            )
            runtime = Runtime(root, Path("/unused"), Path("/unused"), Path(os.sys.executable), "redis://unused", "revision")
            bundled = (object(), b"receipt", (object(),))
            failures = [StageFailure("resource_exhausted", "injected")]
            now = [100_000_000_000]
            runner = Runner(
                jobs, LocalObjectStore(root / "objects"), config, runtime, clock=lambda: now[0]
            )
            # Stage bodies are faked here, so their local markers are too.
            with mock.patch("replay.jobs.runner.resolve_stage"), mock.patch(
                "replay.jobs.runner.validate_committed_markers"
            ), mock.patch(
                "replay.jobs.runner.read_regular", return_value=b"resolved"
            ), mock.patch.object(c, "parse_resolved_job", return_value="resolved"), mock.patch(
                "replay.jobs.runner.bundle_stage", return_value=bundled
            ) as bundle, mock.patch(
                "replay.jobs.runner.prepare_stage", return_value="snapshot"
            ) as prepare, mock.patch(
                "replay.jobs.runner.supervisor_config", return_value="document"
            ) as supervisor, mock.patch(
                "replay.jobs.runner.run_stage",
                side_effect=lambda *a, **k: failures and (_ for _ in ()).throw(failures.pop()),
            ), mock.patch(
                "replay.jobs.runner.load_snapshot", return_value="loaded"
            ), mock.patch(
                "replay.jobs.runner.read_stage"
            ) as read, mock.patch.object(Runner, "_archive"):
                runner.tick()
                waiting, _ = jobs.get_job(submitted.row.job_id)
                self.assertEqual(
                    (waiting.status, waiting.stage, waiting.reason_code),
                    (c.RUNNING, "run", "resource_exhausted"),
                )
                self.assertEqual(bundle.call_count, 1)
                self.assertEqual(prepare.call_args.args[3:5], (bundled[0], bundled[2]))
                self.assertEqual(supervisor.call_args.args[3:5], (bundled[0], bundled[2]))

                now[0] += 1_000_000_000_000
                runner.tick()
                finished, _ = jobs.get_job(submitted.row.job_id)
                self.assertEqual((finished.pending_outcome, finished.reason_code), (c.SUCCEEDED, None))
                self.assertEqual(bundle.call_count, 2)
                self.assertEqual(read.call_args.args[3], bundled[1])

    def test_global_lock_contention_skips_without_running_tick(self):
        with tempfile.TemporaryDirectory() as temporary:
            lock = Path(temporary) / "runner.lock"
            descriptor = os.open(lock, os.O_RDWR | os.O_CREAT, 0o600)
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)

            class Never:
                def tick(self):
                    raise AssertionError("contended tick ran")

            try:
                self.assertFalse(run_tick(Never(), lock))
            finally:
                os.close(descriptor)


def stat_mode(path):
    return path.stat().st_mode & 0o777


def ns(value):
    parsed = parse_timestamp(value)
    assert parsed is not None and isoformat(parsed) == value
    epoch = parse_timestamp("1970-01-01T00:00:00Z")
    delta = parsed - epoch
    return (delta.days * 86400 + delta.seconds) * 1_000_000_000 + delta.microseconds * 1000


if __name__ == "__main__":
    unittest.main()
