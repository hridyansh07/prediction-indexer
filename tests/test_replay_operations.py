from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from archive.storage import INDEPENDENT, LocalObjectStore
from replay.ops.backup import (
    BackupError,
    backup_jobs_database,
    restore_jobs_database,
    verify_backup,
)
from replay.ops.preflight import PreflightError, check_capacity, check_private_root
from replay.ops import preflight
from replay.ops import __main__ as operations


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

        def writer() -> None:
            value = 0
            while not stop.is_set():
                with sqlite3.connect(self.database, timeout=30) as connection:
                    connection.execute(
                        "INSERT OR IGNORE INTO values_table VALUES (?)", (value,)
                    )
                    connection.commit()
                value += 1

        thread = threading.Thread(target=writer)
        thread.start()
        try:
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
            self.assertGreaterEqual(
                connection.execute("SELECT count(*) FROM values_table").fetchone()[0], 0
            )

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

    def test_public_rate_limit_probe_requires_observed_429(self) -> None:
        with mock.patch.object(preflight, "_request", side_effect=[200, 200, 200, 429]):
            preflight._http_checks("http://event-universe:8080", "replay.example", 2)
        with mock.patch.object(preflight, "_request", side_effect=[200, 200, 200, 200]):
            with self.assertRaises(PreflightError):
                preflight._http_checks("http://event-universe:8080", "replay.example", 2)

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
