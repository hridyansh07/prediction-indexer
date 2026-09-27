from __future__ import annotations

import errno
import hashlib
import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from archive.storage import LocalObjectStore
from archive.storage.base import JSON_CONTENT_TYPE
from encoder import StoredIdentity
from replay.jobs import contracts as c
from replay.jobs.bundle import BundleFailure, BundlePin, BundleReady, NotReady, StaleCache
from replay.jobs import stages
from replay.jobs.stages import (
    LocalStateError,
    StageFailure,
    bundle_stage,
    run_stage,
    supervisor_config,
)
from replay.tests.test_jobs_contracts import bundle_receipt, request as request_document, resolved


ROOT = Path(__file__).resolve().parents[2]


def config():
    return c.parse_runner_config((ROOT / "configs/replay_runner.json").read_bytes())


def request():
    return c.parse_request(
        json.dumps(request_document(), separators=(",", ":")).encode(), config()
    )


def json_bytes(value):
    return json.dumps(value, separators=(",", ":")).encode()


class BundleStageTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.receipt = bundle_receipt()
        self.raw = c.bundle_receipt_bytes(self.receipt)
        self.pin = BundlePin(self.root / "derivative", "0" * 64, "2" * 64)

    def tearDown(self):
        self.temp.cleanup()

    def test_ready_writes_byte_identical_bundle_marker_and_resume_recovers_pins(self):
        calls = []

        def ensure(*args, **kwargs):
            calls.append((args, kwargs))
            return BundleReady(self.receipt, self.raw, (self.pin,))

        outcome = bundle_stage(self.root, request(), resolved(), ensure)
        self.assertEqual(outcome, (self.receipt, self.raw, (self.pin,)))
        self.assertEqual((self.root / "bundle.json").read_bytes(), self.raw)
        resumed = bundle_stage(self.root, request(), resolved(), ensure)
        self.assertEqual(resumed, outcome)
        self.assertEqual(len(calls), 2)

    def test_not_ready_stale_and_typed_bundle_failure_are_preserved(self):
        outcomes = (
            (lambda *a, **k: NotReady("canonical_not_archived", "missing"), "canonical_not_archived"),
            (lambda *a, **k: StaleCache(self.receipt.producer, self.receipt.producer), "stale_bundle_cache"),
            (lambda *a, **k: (_ for _ in ()).throw(BundleFailure("tool_failure", "bad tool")), "tool_failure"),
        )
        for ensure, code in outcomes:
            with self.subTest(code=code), self.assertRaises(StageFailure) as caught:
                bundle_stage(self.root, request(), resolved(), ensure)
            self.assertEqual(caught.exception.code, code)

    def test_existing_marker_rejects_nonidentical_bundle_result(self):
        (self.root / "bundle.json").write_bytes(self.raw)
        other = c.bundle_receipt_bytes(bundle_receipt(bundle_id="other"))
        with self.assertRaises(StageFailure) as caught:
            bundle_stage(
                self.root,
                request(),
                resolved(),
                lambda *a, **k: BundleReady(c.parse_bundle_receipt(other), other, (self.pin,)),
            )
        self.assertEqual(caught.exception.code, "integrity_failure")

    def test_prepare_oserror_is_resource_exhausted(self):
        window = c.BundleWindow(
            window_start_ns=0,
            window_end_ns=1_800_000_000_000,
            canonical_receipt_sha256="0" * 64,
            derivative_address="2" * 64,
            receipt_sha256="4" * 64,
        )
        receipt = c.BundleReceipt(
            bundle_id=self.receipt.bundle_id,
            start_ns=window.window_start_ns,
            end_ns=window.window_end_ns,
            canonical_window_seconds=self.receipt.canonical_window_seconds,
            producer=self.receipt.producer,
            windows=(window,),
        )
        pins = tuple(
            BundlePin(
                self.root / f"derivative-{index}",
                window.derivative_address,
                window.receipt_sha256,
            )
            for index, window in enumerate(receipt.windows)
        )
        with mock.patch.object(stages, "prepare", side_effect=OSError("disk full")):
            with self.assertRaises(StageFailure) as caught:
                stages.prepare_stage(
                    self.root,
                    request(),
                    resolved(),
                    receipt,
                    pins,
                    config(),
                )
        self.assertEqual(caught.exception.code, "resource_exhausted")

    def test_resolved_marker_enospc_is_resource_exhausted(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw_request = json_bytes(request_document())
            parsed_request = c.parse_request(raw_request, config())
            job_id = "20260924T120000Z-0123456789abcdef"
            job = stages.initialize_job_root(root / "jobs", job_id, raw_request)
            resolved_job = resolved()
            history = mock.Mock()
            history.resolve.return_value = resolved_job
            with mock.patch.object(
                stages,
                "write_marker",
                side_effect=OSError(errno.ENOSPC, "disk full"),
            ), self.assertRaises(StageFailure) as caught:
                stages.resolve_stage(job, job_id, parsed_request, config(), history)
            self.assertEqual(caught.exception.code, "resource_exhausted")


class SupervisorStageTests(unittest.TestCase):
    def test_fixed_argv_minimal_environment_and_closed_exit_mapping(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            document = {"limits": {"run_seconds": 1, "stop_seconds": 1}}
            captured = {}

            def execute(argv, *, env, timeout):
                captured.update(argv=argv, env=env, timeout=timeout)
                return 0

            success = {"identity": "a" * 64, "attempt": "b" * 32}
            with mock.patch.object(stages, "_run_bounded", side_effect=execute), mock.patch.object(
                stages, "read_success", return_value=success
            ):
                self.assertEqual(
                    run_stage(
                        root,
                        document,
                        python=Path(os.sys.executable),
                        redis_url="redis://secret@redis",
                        scratch_root=root / "scratch",
                    ),
                    success,
                )
            self.assertEqual(captured["argv"][0:3], [str(Path(os.sys.executable).resolve()), "-m", "replay.supervisor"])
            self.assertEqual(captured["argv"][-1], str(root / "run"))
            self.assertEqual(set(captured["env"]), {"LANG", "PATH", "REDIS_URL"})

            for exit_code, reason in ((20, "supervisor_failed"), (21, "supervisor_exhausted"), (9, "resource_exhausted")):
                with self.subTest(exit_code=exit_code), mock.patch.object(
                    stages, "_run_bounded", return_value=exit_code
                ), self.assertRaises(StageFailure) as caught:
                    run_stage(
                        root,
                        document,
                        python=Path(os.sys.executable),
                        redis_url="redis://unused",
                        scratch_root=root / f"scratch-{exit_code}",
                    )
                self.assertEqual(caught.exception.code, reason)

    def test_supervisor_config_has_only_registry_factory_and_runner_snapshot_keys(self):
        receipt = bundle_receipt()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            pin = BundlePin(root / "derivative", receipt.windows[0].derivative_address, receipt.windows[0].receipt_sha256)
            snapshot = {
                "config": {"start_ns": str(receipt.start_ns), "end_ns": str(receipt.windows[0].window_end_ns)},
                "plans": (),
            }
            with mock.patch.object(stages, "snapshot_sha256", return_value="9" * 64), mock.patch.object(
                stages, "validate_supervisor"
            ) as validate:
                document = supervisor_config(
                    "20260924T120000Z-0123456789abcdef",
                    request(),
                    config(),
                    receipt,
                    (pin,),
                    snapshot,
                    root / "context",
                    publisher=Path("/bin/true"),
                    python=Path(os.sys.executable),
                    image_revision="revision",
                )
            validate.assert_called_once_with(document)
            strategy = document["strategies"]["bundle_coverage"]
            self.assertEqual(strategy["factory"], "replay.bundle_coverage:build")
            self.assertEqual(
                set(strategy["config"]),
                {"version", "snapshot_directory", "snapshot_sha256"},
            )
            self.assertEqual(document["transport"]["groups"], ["bundle_coverage"])


class ArchiveConflictTests(unittest.TestCase):
    def test_existing_nonreceipt_at_receipt_key_is_archive_conflict(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw_request = b'{"bundle_id":"x"}'
            request_hash = hashlib.sha256(raw_request).hexdigest()
            row = c.cancel(
                c.new_job(
                    "20260924T120000Z-0123456789abcdef",
                    submitted_by="0x" + "11" * 20,
                    request_sha256=request_hash,
                    now_ns=1,
                ),
                2,
            )
            job = stages.initialize_job_root(root / "jobs", row.job_id, raw_request)
            store = LocalObjectStore(root / "objects")
            conflict = b"{}"
            store.put_immutable(
                c.job_receipt_key(row.job_id),
                io.BytesIO(conflict),
                StoredIdentity(hashlib.sha256(conflict).hexdigest(), len(conflict)),
                content_type=JSON_CONTENT_TYPE,
            )
            with self.assertRaises(StageFailure) as caught:
                stages.archive_stage(job, row, store, "revision", 3)
            self.assertEqual(caught.exception.code, "archive_conflict")

    def test_file_count_aggregate_and_depth_limits_are_enforced(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "context").mkdir()
            (root / "request.json").write_bytes(b"x")
            (root / "context" / "a").write_bytes(b"xx")
            with mock.patch.object(stages, "MAX_JOB_OBJECTS", 1), self.assertRaises(LocalStateError):
                stages._archive_files(root)
            with mock.patch.object(stages, "MAX_ARCHIVE_BYTES", 2), self.assertRaises(LocalStateError):
                stages._archive_files(root)
            deep = root / "context"
            for index in range(3):
                deep = deep / str(index)
                deep.mkdir()
            with mock.patch.object(stages, "MAX_ARCHIVE_DEPTH", 2), self.assertRaises(LocalStateError):
                stages._archive_files(root)


if __name__ == "__main__":
    unittest.main()
