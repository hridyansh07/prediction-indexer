from __future__ import annotations

from contextlib import redirect_stdout
import fcntl
import hashlib
import io
import json
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from archive.storage import CONFORMANCE, INDEPENDENT, LocalObjectStore
from replay.ops.backup import (
    BackupError,
    backup_jobs_database,
    restore_jobs_database,
    verify_backup,
)
from replay.ops.preflight import PreflightError, check_capacity, check_private_root
from replay.ops import preflight
from replay.ops import __main__ as operations
from tests.test_replay_jobs import Limits
from universe.replay_jobs import ReplayJobStore


class ReplayJobsBackupTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.database = self.root / "jobs.sqlite3"
        with sqlite3.connect(self.database) as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("CREATE TABLE values_table(value INTEGER PRIMARY KEY)")
            connection.commit()
        self.store = LocalObjectStore(
            self.root / "objects", store_id="backup-test", durability=INDEPENDENT
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_online_backup_during_writes_uploads_and_restores_verified_snapshot(self) -> None:
        stop = threading.Event()
        writing = threading.Event()

        def writer() -> None:
            value = 0
            while not stop.is_set():
                with sqlite3.connect(self.database, timeout=30) as connection:
                    connection.execute(
                        "INSERT OR IGNORE INTO values_table VALUES (?)", (value,)
                    )
                    connection.commit()
                writing.set()
                value += 1

        thread = threading.Thread(target=writer)
        thread.start()
        try:
            self.assertTrue(writing.wait(timeout=5))
            receipt = backup_jobs_database(
                self.database,
                self.root / "staging",
                self.store,
                prefix="replay/jobs-db-backups",
                created_at_ns=1_800_000_000_000_000_000,
            )
        finally:
            stop.set()
            thread.join()

        verified = verify_backup(self.store, receipt.receipt_key)
        self.assertEqual(verified, receipt)
        self.assertEqual(receipt.source, "jobs.sqlite3")
        self.assertEqual(receipt.backup_type, "sqlite_online_backup")
        self.assertEqual(receipt.integrity_check, "ok")
        self.assertTrue(receipt.object_key.startswith("replay/jobs-db-backups/"))
        self.assertNotIn("jobs/", receipt.object_key)

        restored = self.root / "restored" / "jobs.sqlite3"
        restore_jobs_database(self.store, receipt.receipt_key, restored)
        with sqlite3.connect(restored) as connection:
            self.assertEqual(connection.execute("PRAGMA integrity_check").fetchone()[0], "ok")
            count, minimum, maximum = connection.execute(
                "SELECT count(*),min(value),max(value) FROM values_table"
            ).fetchone()
            self.assertGreater(count, 0)
            self.assertEqual(minimum, 0)
            self.assertEqual(maximum, count - 1)
            self.assertEqual(connection.execute("PRAGMA journal_mode").fetchone()[0], "delete")
        self.assertEqual(restored.stat().st_mode & 0o777, 0o600)
        self.assertFalse(restored.with_name(restored.name + "-wal").exists())
        self.assertFalse(restored.with_name(restored.name + "-shm").exists())

    def test_backup_is_immutable_and_corruption_is_rejected(self) -> None:
        receipt = backup_jobs_database(
            self.database,
            self.root / "staging",
            self.store,
            prefix="replay/jobs-db-backups",
            created_at_ns=1_800_000_000_000_000_000,
        )
        second = backup_jobs_database(
            self.database,
            self.root / "staging",
            self.store,
            prefix="replay/jobs-db-backups",
            created_at_ns=1_800_000_000_000_000_000,
        )
        self.assertEqual(receipt, second)

        object_path = self.root / "objects" / receipt.object_key
        object_path.write_bytes(b"corrupt")
        with self.assertRaises(BackupError):
            verify_backup(self.store, receipt.receipt_key)

    def test_restore_refuses_to_replace_database_or_restore_receipt_as_data(self) -> None:
        receipt = backup_jobs_database(
            self.database,
            self.root / "staging",
            self.store,
            prefix="replay/jobs-db-backups",
            created_at_ns=1_800_000_000_000_000_000,
        )
        with self.assertRaises(BackupError):
            restore_jobs_database(self.store, receipt.receipt_key, self.database)
        with self.assertRaises(BackupError):
            restore_jobs_database(self.store, receipt.object_key, self.root / "bad.sqlite3")

    def test_verification_requires_independent_durability(self) -> None:
        store = LocalObjectStore(
            self.root / "conformance", durability=CONFORMANCE
        )
        with self.assertRaisesRegex(BackupError, "independently durable"):
            verify_backup(store, "replay/jobs-db-backups/missing.receipt.json")

    def test_backup_requires_independent_durability_before_staging(self) -> None:
        store = LocalObjectStore(self.root / "default-store")
        staging = self.root / "must-not-exist"
        with self.assertRaisesRegex(BackupError, "independently durable"):
            backup_jobs_database(
                self.database,
                staging,
                store,
                prefix="replay/jobs-db-backups",
                created_at_ns=1,
            )
        self.assertFalse(staging.exists())

    def test_restore_refuses_stale_rollback_journal_sidecar(self) -> None:
        receipt = backup_jobs_database(
            self.database,
            self.root / "staging",
            self.store,
            prefix="replay/jobs-db-backups",
            created_at_ns=1_800_000_000_000_000_000,
        )
        destination = self.root / "restore-journal.sqlite3"
        destination.with_name(destination.name + "-journal").write_bytes(b"stale")
        with self.assertRaisesRegex(BackupError, "sidecars"):
            restore_jobs_database(self.store, receipt.receipt_key, destination)

    def test_backup_prefix_must_not_overlap_replay_artifacts(self) -> None:
        for prefix in ("replay", "replay/jobs", "replay/jobs/nested"):
            with self.subTest(prefix), self.assertRaises(BackupError):
                backup_jobs_database(
                    self.database,
                    self.root / "staging",
                    self.store,
                    prefix=prefix,
                    created_at_ns=1,
                )


class ReplayPreflightTests(unittest.TestCase):
    def test_image_revision_accepts_stable_safe_identifiers_only(self) -> None:
        for revision in ("local", "release-2026.09+hotfix:1", "a" * 40):
            with self.subTest(revision=revision):
                self.assertIsNotNone(preflight._REVISION.fullmatch(revision))
        for revision in ("", "has space", "slash/name", "x" * 129, "line\nbreak"):
            with self.subTest(revision=revision):
                self.assertIsNone(preflight._REVISION.fullmatch(revision))

    def test_private_root_rejects_group_or_world_access(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            root.chmod(0o700)
            check_private_root(root)
            root.chmod(0o750)
            with self.assertRaises(PreflightError):
                check_private_root(root)

    def test_capacity_checks_bytes_inodes_and_declared_quota(self) -> None:
        stat = type("Stat", (), {"f_bavail": 100, "f_frsize": 10, "f_favail": 20})()
        check_capacity(stat, required_bytes=999, required_inodes=20, quota_bytes=1000)
        matrix = (
            {"required_bytes": 1001, "required_inodes": 20, "quota_bytes": 2000},
            {"required_bytes": 999, "required_inodes": 21, "quota_bytes": 2000},
            {"required_bytes": 1001, "required_inodes": 20, "quota_bytes": 1000},
        )
        for arguments in matrix:
            with self.subTest(arguments), self.assertRaises(PreflightError):
                check_capacity(stat, **arguments)

    def test_http_preflight_checks_private_and_public_health_without_nonce_probe(self) -> None:
        origin = "https://ui.example"
        granted = (204, origin)
        with mock.patch.object(
            preflight, "_request", side_effect=[200, 200]
        ) as request, mock.patch.object(
            preflight, "_cors_preflight", return_value=granted
        ) as cors:
            preflight._http_checks("http://event-universe:8080", "replay.example", origin)
        self.assertEqual(request.call_count, 2)
        cors.assert_called_once_with("https://replay.example/v1/replay/jobs", origin)
        with mock.patch.object(preflight, "_request", side_effect=[200, 503]):
            with self.assertRaises(PreflightError):
                preflight._http_checks("http://event-universe:8080", "replay.example", origin)
        for response in ((204, None), (204, "https://evil.example"), (501, None)):
            with self.subTest(response), mock.patch.object(
                preflight, "_request", side_effect=[200, 200]
            ), mock.patch.object(preflight, "_cors_preflight", return_value=response):
                with self.assertRaisesRegex(PreflightError, "CORS preflight"):
                    preflight._http_checks(
                        "http://event-universe:8080", "replay.example", origin
                    )

    def test_cors_origin_is_one_https_origin_matching_siwe(self) -> None:
        preflight.check_cors_origin(
            "https://ui.example.app", "ui.example.app", "https://ui.example.app/login"
        )
        for origin, domain, uri in (
            ("http://ui.example.app", "ui.example.app", "http://ui.example.app/login"),
            ("https://ui.example.app/", "ui.example.app", "https://ui.example.app/login"),
            ("https://ui.example.app:8443", "ui.example.app:8443", "https://ui.example.app:8443/login"),
            ("https://*.vercel.app", "*.vercel.app", "https://*.vercel.app/login"),
            ("*", "ui.example.app", "https://ui.example.app/login"),
            ("https://ui.example.app", "api.example.app", "https://ui.example.app/login"),
            ("https://ui.example.app", "ui.example.app", "https://ui.example.app.evil/login"),
            ("https://ui.example.app", "ui.example.app", "https://other.example/login"),
        ):
            with self.subTest(origin=origin, domain=domain, uri=uri):
                with self.assertRaises(PreflightError):
                    preflight.check_cors_origin(origin, domain, uri)

    def test_publisher_contract_probe_executes_expected_binary_protocol(self) -> None:
        result = SimpleNamespace(
            returncode=20,
            stdout=b"",
            stderr=b"replay-publish: usage: replay-publish CONFIG.json\n",
        )
        with mock.patch.object(preflight.subprocess, "run", return_value=result) as run:
            preflight._publisher_contract(Path("/usr/local/bin/replay-publish"))
        self.assertEqual(run.call_args.args[0], ["/usr/local/bin/replay-publish"])

    def test_required_environment_diagnostic_names_only(self) -> None:
        secret = "do-not-print-this-value"
        with self.assertRaises(PreflightError) as caught:
            preflight._required({"PRESENT": secret}, "PRESENT", "MISSING")
        self.assertIn("MISSING", str(caught.exception))
        self.assertNotIn(secret, str(caught.exception))

    def test_image_reference_must_bind_the_declared_digest(self) -> None:
        environment = {
            "REPLAY_IMAGE_REVISION": "a" * 40,
            "REPLAY_IMAGE_DIGEST": "sha256:" + "b" * 64,
            "REPLAY_RUNNER_IMAGE": "registry.example/replay@sha256:" + "c" * 64,
        }
        with mock.patch.object(preflight, "_required", return_value=environment):
            with self.assertRaisesRegex(PreflightError, "must end with"):
                preflight.run_preflight(Path("unused"), environment)

    def test_missing_image_revision_file_fails_preflight(self) -> None:
        environment = {
            "REPLAY_IMAGE_REVISION": "a" * 40,
            "REPLAY_IMAGE_DIGEST": "sha256:" + "b" * 64,
            "REPLAY_RUNNER_IMAGE": "registry.example/replay@sha256:" + "b" * 64,
            "REPLAY_PUBLIC_HOST": "replay.example",
        }
        with mock.patch.object(
            preflight, "_required", return_value=environment
        ), mock.patch.object(Path, "is_file", return_value=False):
            with self.assertRaisesRegex(PreflightError, "revision file is missing"):
                preflight.run_preflight(Path("unused"), environment)

    def test_backup_receipt_contains_no_environment_or_secret_values(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            database = root / "jobs.sqlite3"
            with sqlite3.connect(database) as connection:
                connection.execute("CREATE TABLE t(x)")
            store = LocalObjectStore(root / "objects", store_id="test", durability=INDEPENDENT)
            receipt = backup_jobs_database(
                database,
                root / "staging",
                store,
                prefix="replay/jobs-db-backups",
                created_at_ns=1,
            )
            raw = (root / "objects" / receipt.receipt_key).read_bytes()
            document = json.loads(raw)
            self.assertEqual(hashlib.sha256(raw).hexdigest(), hashlib.sha256(raw).hexdigest())
            self.assertEqual(
                set(document),
                {
                    "replay_jobs_backup_version",
                    "source",
                    "backup_type",
                    "created_at_ns",
                    "object_key",
                    "sha256",
                    "byte_length",
                    "store_id",
                    "provider",
                    "provider_checksum",
                    "provider_checksum_algorithm",
                    "integrity_check",
                },
            )


class ReplayAuditTests(unittest.TestCase):
    def test_audit_reports_integrity_and_actual_lock_state(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            ReplayJobStore(root / "jobs.sqlite3", Limits()).initialize()
            self.assertEqual(operations._audit(root)["status"], "ok")
            self.assertFalse(operations._audit(root)["runner_lock_held"])
            output = io.StringIO()
            with mock.patch.object(operations, "_root", return_value=root), redirect_stdout(
                output
            ):
                self.assertEqual(operations.main(["audit"]), 0)
            self.assertEqual(json.loads(output.getvalue())["status"], "ok")
            lock = root / "runner.lock"
            descriptor = open(lock, "w")
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                self.assertTrue(operations._audit(root)["runner_lock_held"])
            finally:
                descriptor.close()

    def test_corrupt_database_fails_audit_and_cli_exit(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "jobs.sqlite3").write_bytes(b"not sqlite")
            report = operations._audit(root)
            self.assertEqual(report["status"], "failed")
            output = io.StringIO()
            with mock.patch.object(operations, "_root", return_value=root), redirect_stdout(
                output
            ):
                self.assertEqual(operations.main(["audit"]), 1)
            self.assertEqual(json.loads(output.getvalue())["status"], "failed")

    def test_existing_job_store_modes_never_create_a_missing_database(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = SimpleNamespace(
                replay=SimpleNamespace(
                    database_path=root / "jobs.sqlite3", jobs=Limits()
                )
            )
            with mock.patch.object(operations, "_config", return_value=config):
                for read_only in (True, False):
                    with self.subTest(read_only=read_only), self.assertRaises(ValueError):
                        operations._jobs(root, read_only=read_only)
            self.assertFalse((root / "jobs.sqlite3").exists())

    def test_resume_blocked_cli_uses_existing_compare_and_set_store(self) -> None:
        before = SimpleNamespace(job_id="job", status="archive_blocked")
        after = SimpleNamespace(job_id="job", status="archiving", stage="archive")
        store = mock.Mock()
        store.get_job.return_value = (before, b"request")
        store.save.return_value = after
        with mock.patch.object(operations, "_root", return_value=Path("/unused")), mock.patch.object(
            operations, "_jobs", return_value=store
        ), mock.patch.object(operations.jobs, "resume_blocked", return_value=after):
            output = io.StringIO()
            with redirect_stdout(output):
                self.assertEqual(operations.main(["resume-blocked", "job"]), 0)
        store.save.assert_called_once_with(before, after)
        self.assertEqual(json.loads(output.getvalue())["status"], "archiving")

    def test_terminal_receipt_verification_uses_terminal_row_binding(self) -> None:
        row = SimpleNamespace(
            status="failed",
            pending_outcome=None,
            request_sha256="1" * 64,
            submitted_by="0x" + "2" * 40,
            reason_code="tool_failure",
            reason_detail="synthetic",
            created_at_ns=10,
            finished_at_ns=20,
        )
        receipt = SimpleNamespace(
            request_sha256=row.request_sha256,
            submitted_by=row.submitted_by,
            final_outcome="failed",
            reason_code=row.reason_code,
            reason_detail=row.reason_detail,
            created_at_ns=row.created_at_ns,
            finished_at_ns=row.finished_at_ns,
            image_revision="a" * 40,
        )
        job_store = mock.Mock()
        job_store.get_job.return_value = (row, b"request")
        with mock.patch.object(operations, "_jobs", return_value=job_store), mock.patch.object(
            operations, "_store", return_value=object()
        ), mock.patch.object(operations, "verify_published_receipt", return_value=receipt):
            result = operations._verify_job(Path("/unused"), "job-id")
        self.assertTrue(result["receipt_verified"])
        self.assertEqual(result["finished_at_ns"], "20")


if __name__ == "__main__":
    unittest.main()
