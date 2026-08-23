"""Canonical deletion retains restart state and requires fresh archive proof."""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path

from archive.archiver.canonical import CanonicalArchiver, read_canonical_archive_receipt
from archive.common.receipts import CanonicalIndex, ReceiptError, read_canonical_receipt
from archive.reaper.canonical import (
    ALREADY_REAPED,
    ARCHIVE_OBJECT_UNVERIFIED,
    AUDIT_MODE,
    DURABILITY_GATE,
    LOCAL_WINDOW_CHANGED,
    REAPED,
    RETAINED,
    RETENTION_FLOOR,
    RETENTION_FLOOR_NS,
    UNEXPECTED_WINDOW_ARTIFACT,
    CanonicalReaper,
)
from archive.reaper.canonical_cli import build_parser as canonical_reaper_parser
from archive.reaper.canonical_cli import main as canonical_reaper_main
from archive.storage import INDEPENDENT, LocalObjectStore
from archive.storage.s3 import S3ObjectStore
from tests.archive_fixtures import BASE_NS, WINDOW_SECONDS, write_canonical_receipt
from tests.test_s3store import BUCKET, OWNER, REGION, FakeS3Client


NANOSECONDS = 1_000_000_000
HOUR_NS = 3600 * NANOSECONDS


class CanonicalReaperCase(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.canonical = self.root / "canonical"
        self.source_receipt = write_canonical_receipt(self.canonical, evidence_lines=2)
        self.window = self.source_receipt.parent
        self.evidence = self.window / "evidence.ndjson.zst"
        self.provenance = self.window / "provenance.ndjson.zst"
        self.archive_ns = BASE_NS + WINDOW_SECONDS * NANOSECONDS + HOUR_NS
        self.now_ns = self.archive_ns + 19 * HOUR_NS
        self.store = LocalObjectStore(self.root / "archive", durability=INDEPENDENT)
        result = CanonicalArchiver(
            self.canonical, self.store, now_ns=lambda: self.archive_ns
        ).sweep()
        self.assertEqual(result.counts["archived"], 1, result.as_record())
        self.archive_receipt = self.window / "canonical_archive_receipt.json"
        old_seconds = self.archive_ns / NANOSECONDS
        os.utime(self.source_receipt, (old_seconds, old_seconds))
        os.utime(self.archive_receipt, (old_seconds, old_seconds))

    def reaper(self, *, destructive: bool = True, now_ns: int | None = None):
        return CanonicalReaper(
            self.canonical,
            self.store,
            destructive=destructive,
            now_ns=lambda: self.now_ns if now_ns is None else now_ns,
        )

    def only(self, result):
        self.assertEqual(len(result.decisions), 1, result.as_record())
        return result.decisions[0]


class CanonicalReaperTests(CanonicalReaperCase):
    def test_audit_reports_reapable_but_deletes_nothing(self) -> None:
        decision = self.only(self.reaper(destructive=False).sweep())
        self.assertEqual((decision.decision, decision.reason), (RETAINED, AUDIT_MODE))
        self.assertTrue(self.evidence.is_file())
        self.assertTrue(self.provenance.is_file())

    def test_delete_removes_only_large_frames_and_leaves_restart_tombstone(self) -> None:
        expected_bytes = self.evidence.stat().st_size + self.provenance.stat().st_size
        decision = self.only(self.reaper().sweep())
        self.assertEqual(decision.decision, REAPED)
        self.assertEqual(decision.artifacts_deleted, 2)
        self.assertEqual(decision.bytes_reclaimed, expected_bytes)
        self.assertFalse(self.evidence.exists())
        self.assertFalse(self.provenance.exists())
        self.assertTrue(self.source_receipt.is_file())
        self.assertTrue(self.archive_receipt.is_file())

        committed = read_canonical_receipt(self.source_receipt)
        self.assertFalse(committed.outputs_present)
        index = CanonicalIndex.build(self.canonical)
        self.assertEqual(index.faults, [])

        again = self.only(self.reaper().sweep())
        self.assertEqual(again.decision, ALREADY_REAPED)

    def test_window_younger_than_eighteen_hours_is_retained(self) -> None:
        decision = self.only(
            self.reaper(now_ns=self.archive_ns + RETENTION_FLOOR_NS - 1).sweep()
        )
        self.assertEqual((decision.decision, decision.reason), (RETAINED, RETENTION_FLOOR))
        self.assertTrue(self.evidence.is_file())

    def test_receipt_mtime_can_only_extend_retention(self) -> None:
        recent = (self.now_ns - HOUR_NS) / NANOSECONDS
        os.utime(self.archive_receipt, (recent, recent))
        decision = self.only(self.reaper().sweep())
        self.assertEqual(decision.reason, RETENTION_FLOOR)

    def test_conformance_archive_never_authorizes_deletion(self) -> None:
        self.archive_receipt.unlink()
        local_store = LocalObjectStore(self.root / "conformance")
        result = CanonicalArchiver(
            self.canonical, local_store, now_ns=lambda: self.archive_ns
        ).sweep()
        self.assertEqual(result.counts["archived"], 1, result.as_record())
        decision = self.only(
            CanonicalReaper(
                self.canonical,
                local_store,
                destructive=True,
                now_ns=lambda: self.now_ns,
            ).sweep()
        )
        self.assertEqual((decision.decision, decision.reason), (RETAINED, DURABILITY_GATE))
        self.assertTrue(self.evidence.is_file())

    def test_missing_remote_object_retains_local_frames(self) -> None:
        receipt = read_canonical_archive_receipt(self.archive_receipt)
        (Path(self.store.root) / receipt.evidence.key).unlink()
        decision = self.only(self.reaper().sweep())
        self.assertEqual(
            (decision.decision, decision.reason), (RETAINED, ARCHIVE_OBJECT_UNVERIFIED)
        )
        self.assertTrue(self.evidence.is_file())
        self.assertTrue(self.provenance.is_file())

    def test_changed_local_frame_retains(self) -> None:
        data = bytearray(self.evidence.read_bytes())
        data[-1] ^= 0xFF
        self.evidence.write_bytes(data)
        decision = self.only(self.reaper().sweep())
        self.assertEqual((decision.decision, decision.reason), (RETAINED, LOCAL_WINDOW_CHANGED))
        self.assertTrue(self.evidence.is_file())

    def test_partial_cleanup_finishes_only_after_fresh_remote_verification(self) -> None:
        self.evidence.unlink()
        receipt = read_canonical_archive_receipt(self.archive_receipt)
        remote = Path(self.store.root) / receipt.provenance.key
        remote.unlink()
        decision = self.only(self.reaper().sweep())
        self.assertEqual(decision.reason, ARCHIVE_OBJECT_UNVERIFIED)
        self.assertTrue(self.provenance.is_file())

    def test_partial_cleanup_can_finish_after_all_proofs_are_reestablished(self) -> None:
        self.evidence.unlink()
        decision = self.only(self.reaper().sweep())
        self.assertEqual(decision.decision, REAPED)
        self.assertEqual(decision.artifacts_deleted, 1)
        self.assertFalse(self.provenance.exists())

    def test_reverse_partial_state_is_not_attributed_to_this_reaper(self) -> None:
        self.provenance.unlink()
        decision = self.only(self.reaper().sweep())
        self.assertEqual(decision.reason, LOCAL_WINDOW_CHANGED)
        self.assertTrue(self.evidence.is_file())

    def test_unexpected_window_entry_retains(self) -> None:
        (self.window / "unknown.bin").write_bytes(b"unexpected")
        decision = self.only(self.reaper().sweep())
        self.assertEqual(decision.reason, UNEXPECTED_WINDOW_ARTIFACT)
        self.assertTrue(self.evidence.is_file())

    def test_changed_tombstone_receipt_is_not_accepted_as_committed_history(self) -> None:
        self.reaper().sweep()
        document = json.loads(self.archive_receipt.read_text(encoding="utf-8"))
        document["canonical_receipt"]["sha256"] = "a" * 64
        self.archive_receipt.write_text(json.dumps(document), encoding="utf-8")
        with self.assertRaises(ReceiptError):
            read_canonical_receipt(self.source_receipt)


class CanonicalReaperCommandTests(unittest.TestCase):
    def test_parser_refuses_a_retention_floor_below_eighteen_hours(self) -> None:
        with self.assertRaises(SystemExit):
            canonical_reaper_parser().parse_args(
                ["--canonical-root", "/canonical", "--retention-hours", "1"]
            )

    def test_command_refuses_a_retention_floor_below_eighteen_hours(self) -> None:
        with tempfile.TemporaryDirectory() as directory, self.assertRaises(SystemExit):
            canonical_reaper_main(
                [
                    "--canonical-root",
                    str(Path(directory) / "canonical"),
                    "--archive-root",
                    str(Path(directory) / "archive"),
                    "--retention-hours",
                    "17",
                ]
            )

    def test_command_refuses_a_missing_canonical_root(self) -> None:
        with tempfile.TemporaryDirectory() as directory, self.assertRaisesRegex(
            SystemExit, "canonical root.*is not a directory"
        ):
            canonical_reaper_main(
                [
                    "--canonical-root",
                    str(Path(directory) / "missing"),
                    "--archive-root",
                    str(Path(directory) / "archive"),
                ]
            )

    def test_command_refuses_delete_against_conformance_storage(self) -> None:
        with tempfile.TemporaryDirectory() as directory, self.assertRaises(SystemExit):
            (Path(directory) / "canonical").mkdir()
            canonical_reaper_main(
                [
                    "--canonical-root",
                    str(Path(directory) / "canonical"),
                    "--archive-root",
                    str(Path(directory) / "archive"),
                    "--mode",
                    "delete",
                ]
            )

    def test_missing_canonical_root_is_an_index_fault(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            missing = Path(directory) / "missing"
            index = CanonicalIndex.build(missing)
        self.assertEqual(len(index.faults), 1)
        self.assertIn("is not a directory", index.faults[0])


class CanonicalReapingThroughS3Tests(unittest.TestCase):
    def test_real_s3_adapter_heads_authorize_local_frame_reaping(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            canonical = root / "canonical"
            source_receipt = write_canonical_receipt(canonical, evidence_lines=1)
            store = S3ObjectStore(BUCKET, REGION, OWNER, client=FakeS3Client())
            archive_ns = BASE_NS + WINDOW_SECONDS * NANOSECONDS + HOUR_NS
            archived = CanonicalArchiver(
                canonical, store, now_ns=lambda: archive_ns
            ).sweep()
            self.assertEqual(archived.counts["archived"], 1, archived.as_record())
            marker = source_receipt.with_name("canonical_archive_receipt.json")
            old_seconds = archive_ns / NANOSECONDS
            os.utime(source_receipt, (old_seconds, old_seconds))
            os.utime(marker, (old_seconds, old_seconds))

            result = CanonicalReaper(
                canonical,
                store,
                destructive=True,
                now_ns=lambda: archive_ns + 19 * HOUR_NS,
            ).sweep()
            decision = result.decisions[0]
            self.assertEqual(decision.decision, REAPED, result.as_record())
            self.assertFalse(source_receipt.with_name("evidence.ndjson.zst").exists())
            self.assertTrue(source_receipt.is_file())

if __name__ == "__main__":
    unittest.main()
