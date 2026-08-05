"""§9.4 — the dual-receipt reaper.

Deletion is the one irreversible act in this pipeline, so almost every test here
asserts that it did *not* happen. The two that do delete exist to prove the gate
can open at all: a reaper that never deletes passes every safety test and is
useless.
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from archive.archiver import Archiver
from archive.storage import INDEPENDENT, LocalObjectStore
from archive.reaper import (
    ALREADY_REAPED,
    ARCHIVE_OBJECT_UNVERIFIED,
    ARCHIVE_RECEIPT_INVALID,
    AUDIT_MODE,
    CANONICAL_MISMATCH,
    CANONICAL_MISSING,
    DURABILITY_GATE,
    LOCAL_SOURCE_CHANGED,
    REAPED,
    RETAINED,
    Reaper,
)
from archive.common.receipts import read_archive_receipt
from tests.archive_fixtures import (
    BASE_NS,
    WINDOW_SECONDS,
    canonical_input_for,
    write_canonical_receipt,
    write_sealed_segment,
)

NANOSECONDS = 1_000_000_000


class ReaperCase(unittest.TestCase):
    """One archived segment against an authorized backend, ready to be reaped."""

    #: Overridden by the conformance case, which is what a default deployment
    #: actually runs.
    durability = INDEPENDENT

    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.spool = self.root / "spool"
        self.canonical = self.root / "canonical"
        self.segment = write_sealed_segment(self.spool)
        self.seal = self.segment.with_name(self.segment.name[: -len(".ndjson")] + ".seal.json")
        self.store = LocalObjectStore(self.root / "archive", durability=self.durability)
        self.archiver = Archiver(self.spool, self.store)

    def archive(self) -> Path:
        result = self.archiver.sweep()
        self.assertEqual(result.counts["archived"], 1, result.as_record())
        return self.receipt_path()

    def receipt_path(self) -> Path:
        suffix = ".archive.json" if self.durability is INDEPENDENT else ".archive.local.json"
        return self.segment.with_name(self.segment.name[: -len(".ndjson")] + suffix)

    def canonicalize(self, **overrides) -> Path:
        entry = canonical_input_for(self.segment)
        entry.update(overrides)
        return write_canonical_receipt(self.canonical, inputs=[entry])

    def reaper(self, *, destructive: bool = True) -> Reaper:
        return Reaper(self.spool, self.canonical, self.store, destructive=destructive)

    def only(self, result):
        self.assertEqual(len(result.decisions), 1, result.as_record())
        return result.decisions[0]


class RetentionTests(ReaperCase):
    def test_an_archive_receipt_alone_retains(self) -> None:
        self.archive()
        decision = self.only(self.reaper().sweep())
        self.assertEqual(decision.decision, RETAINED)
        self.assertEqual(decision.reason, CANONICAL_MISSING)
        self.assertTrue(self.segment.exists())
        self.assertTrue(self.seal.exists())

    def test_a_canonical_receipt_alone_retains(self) -> None:
        self.canonicalize()
        result = self.reaper().sweep()
        # Nothing is even considered: without an archive receipt there is no
        # proof that a second copy exists, and discovery is receipt-driven.
        self.assertEqual(result.decisions, [])
        self.assertTrue(self.segment.exists())

    def test_a_canonical_receipt_naming_a_different_digest_retains(self) -> None:
        self.archive()
        self.canonicalize(sha256="b" * 64)
        decision = self.only(self.reaper().sweep())
        self.assertEqual(decision.decision, RETAINED)
        self.assertEqual(decision.reason, CANONICAL_MISSING)
        self.assertTrue(self.segment.exists())

    def test_a_canonical_receipt_naming_a_different_lane_retains(self) -> None:
        self.archive()
        self.canonicalize(lane="kalshi")
        decision = self.only(self.reaper().sweep())
        self.assertEqual(decision.decision, RETAINED)
        self.assertTrue(self.segment.exists())

    def test_the_right_digest_under_the_wrong_file_or_index_retains_loudly(self) -> None:
        """Lane plus digest is the minimum; the file and index catch a mix-up."""
        self.archive()
        for label, override in (
            ("file", {"data_file": "20260730T000000000000-000-other000.ndjson"}),
            ("index", {"segment_index": 7}),
        ):
            with self.subTest(label):
                for path in self.canonical.rglob("receipt.json"):
                    path.unlink()
                self.canonicalize(**override)
                decision = self.only(self.reaper().sweep())
                self.assertEqual(decision.decision, RETAINED)
                self.assertEqual(decision.reason, CANONICAL_MISMATCH)
                self.assertTrue(self.segment.exists())

    def test_a_missing_archive_object_retains(self) -> None:
        receipt_path = self.archive()
        self.canonicalize()
        receipt = read_archive_receipt(receipt_path)
        (Path(self.store.root) / receipt.data_key).unlink()
        decision = self.only(self.reaper().sweep())
        self.assertEqual(decision.decision, RETAINED)
        self.assertEqual(decision.reason, ARCHIVE_OBJECT_UNVERIFIED)
        self.assertTrue(self.segment.exists())

    def test_a_mutated_archive_object_retains(self) -> None:
        receipt_path = self.archive()
        self.canonicalize()
        receipt = read_archive_receipt(receipt_path)
        path = Path(self.store.root) / receipt.data_key
        path.write_bytes(path.read_bytes() + b"tampered")
        decision = self.only(self.reaper().sweep())
        self.assertEqual(decision.decision, RETAINED)
        self.assertEqual(decision.reason, ARCHIVE_OBJECT_UNVERIFIED)
        self.assertTrue(self.segment.exists())

    def test_a_mutated_archived_seal_retains(self) -> None:
        receipt_path = self.archive()
        self.canonicalize()
        receipt = read_archive_receipt(receipt_path)
        path = Path(self.store.root) / receipt.seal_key
        path.write_bytes(b"{}")
        decision = self.only(self.reaper().sweep())
        self.assertEqual(decision.decision, RETAINED)
        self.assertEqual(decision.reason, ARCHIVE_OBJECT_UNVERIFIED)
        self.assertTrue(self.seal.exists())

    def test_a_corrupt_archive_receipt_retains(self) -> None:
        receipt_path = self.archive()
        self.canonicalize()
        receipt_path.write_text('{"archive_receipt_version": 1}', encoding="utf-8")
        decision = self.only(self.reaper().sweep())
        self.assertEqual(decision.decision, RETAINED)
        self.assertEqual(decision.reason, ARCHIVE_RECEIPT_INVALID)
        self.assertTrue(self.segment.exists())

    def test_a_canonical_receipt_whose_evidence_is_gone_is_not_a_committed_window(self) -> None:
        self.archive()
        receipt = self.canonicalize()
        (receipt.parent / "evidence.ndjson.zst").unlink()
        result = self.reaper().sweep()
        decision = self.only(result)
        self.assertEqual(decision.decision, RETAINED)
        self.assertEqual(decision.reason, CANONICAL_MISSING)
        self.assertEqual(len(result.canonical_faults), 1)
        self.assertTrue(self.segment.exists())

    def test_a_canonical_receipt_with_wrong_compression_metadata_is_not_committed(self) -> None:
        self.archive()
        receipt = self.canonicalize()
        document = json.loads(receipt.read_text(encoding="utf-8"))
        document["evidence"]["compression"]["level"] = 1
        receipt.write_text(json.dumps(document), encoding="utf-8")

        result = self.reaper().sweep()
        decision = self.only(result)
        self.assertEqual(decision.decision, RETAINED)
        self.assertEqual(decision.reason, CANONICAL_MISSING)
        self.assertEqual(len(result.canonical_faults), 1)
        self.assertTrue(self.segment.exists())

    def test_a_canonical_receipt_cannot_resolve_an_output_outside_its_window(self) -> None:
        self.archive()
        receipt = self.canonicalize()
        document = json.loads(receipt.read_text(encoding="utf-8"))
        evidence = receipt.parent / document["evidence"]["file"]
        outside = self.root / "outside-evidence.ndjson.zst"
        outside.write_bytes(evidence.read_bytes())
        document["evidence"]["file"] = "../../../outside-evidence.ndjson.zst"
        receipt.write_text(json.dumps(document), encoding="utf-8")

        result = self.reaper().sweep()
        decision = self.only(result)
        self.assertEqual(decision.decision, RETAINED)
        self.assertEqual(decision.reason, CANONICAL_MISSING)
        self.assertEqual(len(result.canonical_faults), 1)
        self.assertTrue(self.segment.exists())
        self.assertTrue(outside.exists())

    def test_a_changed_local_segment_retains_even_with_both_receipts(self) -> None:
        self.archive()
        self.canonicalize()
        original = self.segment.read_bytes()
        self.segment.write_bytes(original[:-2] + b"X\n")
        decision = self.only(self.reaper().sweep())
        self.assertEqual(decision.decision, RETAINED)
        self.assertEqual(decision.reason, LOCAL_SOURCE_CHANGED)
        self.assertTrue(self.segment.exists())

    def test_a_receipt_sitting_in_another_lanes_directory_retains(self) -> None:
        """The receipt names the files; the directory decides which lane's they are."""
        receipt_path = self.archive()
        self.canonicalize()
        elsewhere = self.spool / "lane=kalshi" / "date=2026-07-30"
        elsewhere.mkdir(parents=True)
        planted = elsewhere / receipt_path.name
        planted.write_bytes(receipt_path.read_bytes())
        (elsewhere / self.segment.name).write_bytes(self.segment.read_bytes())
        (elsewhere / self.seal.name).write_bytes(self.seal.read_bytes())

        result = self.reaper().sweep()
        by_receipt = {Path(d.archive_receipt): d for d in result.decisions}
        self.assertEqual(by_receipt[planted].decision, RETAINED)
        self.assertEqual(by_receipt[planted].reason, ARCHIVE_RECEIPT_INVALID)
        self.assertTrue((elsewhere / self.segment.name).exists())

    def test_a_receipt_naming_a_traversing_source_file_is_retained_not_resolved(self) -> None:
        """S3 adapter Gate 0, finding 1: no receipt path escapes its date directory."""
        receipt_path = self.archive()
        self.canonicalize()
        outside = self.spool / "outside.ndjson"
        outside.write_bytes(b"not the archived segment\n")
        document = json.loads(receipt_path.read_text(encoding="utf-8"))
        document["source"]["file"] = "../../outside.ndjson"
        receipt_path.write_text(json.dumps(document), encoding="utf-8")

        decision = self.only(self.reaper().sweep([receipt_path]))
        self.assertEqual(decision.decision, RETAINED)
        self.assertEqual(decision.reason, ARCHIVE_RECEIPT_INVALID)
        self.assertTrue(outside.exists(), "the reaper touched a path outside its directory")

    def test_an_upload_that_never_produced_a_receipt_leaves_nothing_to_reap(self) -> None:
        """No receipt, no discovery, no deletion — whatever the canonical side says."""
        self.canonicalize()
        result = self.reaper().sweep()
        self.assertEqual(result.decisions, [])
        self.assertTrue(self.segment.exists())
        self.assertTrue(self.seal.exists())

    def test_a_late_segment_with_no_canonical_window_stays_visible_in_the_report(self) -> None:
        """§5's `late_after_finalization`: archived, never canonicalized, retained."""
        late = write_sealed_segment(
            self.spool,
            start_ns=BASE_NS + WINDOW_SECONDS * NANOSECONDS,
            segment_id="late0000",
        )
        self.archiver.sweep()
        self.canonicalize()
        result = self.reaper().sweep()
        by_file = {decision.source_file: decision for decision in result.decisions}
        self.assertEqual(by_file[late.name].decision, RETAINED)
        self.assertEqual(by_file[late.name].reason, CANONICAL_MISSING)
        self.assertTrue(late.exists())
        self.assertEqual(result.retained_by_reason()[CANONICAL_MISSING], 1)


class DurabilityGateTests(ReaperCase):
    durability = None  # set in setUp; a default conformance store

    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.spool = self.root / "spool"
        self.canonical = self.root / "canonical"
        self.segment = write_sealed_segment(self.spool)
        self.seal = self.segment.with_name(self.segment.name[: -len(".ndjson")] + ".seal.json")
        self.store = LocalObjectStore(self.root / "archive")
        self.archiver = Archiver(self.spool, self.store)

    def receipt_path(self) -> Path:
        return self.segment.with_name(
            self.segment.name[: -len(".ndjson")] + ".archive.local.json"
        )

    def test_both_receipts_on_a_conformance_backend_retain_and_report_the_gate(self) -> None:
        self.archive()
        self.canonicalize()
        decision = self.only(self.reaper().sweep())
        self.assertEqual(decision.decision, RETAINED)
        self.assertEqual(decision.reason, DURABILITY_GATE)
        self.assertTrue(self.segment.exists())
        self.assertTrue(self.seal.exists())

    def test_a_production_receipt_against_a_conformance_store_still_retains(self) -> None:
        """The store's declared class gates, not the receipt found beside it."""
        self.archive()
        self.canonicalize()
        local_receipt = self.receipt_path()
        forged = local_receipt.with_name(
            local_receipt.name.replace(".archive.local.json", ".archive.json")
        )
        document = json.loads(local_receipt.read_text(encoding="utf-8"))
        document["archive_receipt_version"] = document.pop("local_archive_receipt_version")
        forged.write_text(json.dumps(document), encoding="utf-8")
        result = self.reaper().sweep()
        self.assertEqual({decision.decision for decision in result.decisions}, {RETAINED})
        self.assertTrue(self.segment.exists())


class DeletionTests(ReaperCase):
    def test_audit_mode_proves_eligibility_without_touching_anything(self) -> None:
        self.archive()
        self.canonicalize()
        decision = self.only(self.reaper(destructive=False).sweep())
        self.assertEqual(decision.decision, RETAINED)
        self.assertEqual(decision.reason, AUDIT_MODE)
        self.assertTrue(self.segment.exists())
        self.assertTrue(self.seal.exists())
        self.assertFalse(decision.derivative_deleted)

    def test_both_receipts_and_an_authorized_backend_delete_raw_and_seal(self) -> None:
        receipt_path = self.archive()
        canonical = self.canonicalize()
        derivative = self.segment.with_name(
            self.segment.name[: -len(".ndjson")] + ".ndjson.zst"
        )
        self.assertTrue(derivative.exists())

        decision = self.only(self.reaper().sweep())
        self.assertEqual(decision.decision, REAPED)
        self.assertEqual(decision.canonical_receipt, str(canonical))
        self.assertFalse(self.segment.exists())
        self.assertFalse(self.seal.exists())
        self.assertFalse(derivative.exists())
        # The receipt is what keeps the deletion auditable, so it stays.
        self.assertTrue(receipt_path.exists())

    def test_a_second_sweep_after_a_reaping_is_idempotent(self) -> None:
        self.archive()
        self.canonicalize()
        self.reaper().sweep()
        decision = self.only(self.reaper().sweep())
        self.assertEqual(decision.decision, ALREADY_REAPED)

    def test_a_crash_between_the_two_unlinks_resumes_after_revalidation(self) -> None:
        self.archive()
        self.canonicalize()
        # Exactly the state §7.2 describes: raw gone, seal still present.
        self.segment.unlink()
        decision = self.only(self.reaper().sweep())
        self.assertEqual(decision.decision, REAPED)
        self.assertIn("partial cleanup", decision.detail)
        self.assertFalse(self.seal.exists())

    def test_a_partial_cleanup_is_not_finished_when_the_archive_stops_verifying(self) -> None:
        receipt_path = self.archive()
        self.canonicalize()
        self.segment.unlink()
        receipt = read_archive_receipt(receipt_path)
        (Path(self.store.root) / receipt.data_key).unlink()
        decision = self.only(self.reaper().sweep())
        self.assertEqual(decision.decision, RETAINED)
        self.assertEqual(decision.reason, ARCHIVE_OBJECT_UNVERIFIED)
        self.assertTrue(self.seal.exists())

    def test_a_seal_removed_without_its_segment_is_never_completed_into_a_deletion(self) -> None:
        self.archive()
        self.canonicalize()
        self.seal.unlink()
        decision = self.only(self.reaper().sweep())
        self.assertEqual(decision.decision, RETAINED)
        self.assertEqual(decision.reason, LOCAL_SOURCE_CHANGED)
        self.assertTrue(self.segment.exists())

    def test_the_report_carries_what_an_audit_needs(self) -> None:
        receipt_path = self.archive()
        canonical = self.canonicalize()
        record = self.reaper().sweep().as_record()
        decision = record["decisions"][0]
        self.assertEqual(
            set(decision),
            {
                "lane",
                "source_file",
                "source_sha256",
                "archive_receipt",
                "canonical_receipt",
                "decision",
                "reason",
                "detail",
                "verified_at_ns",
                "derivative_deleted",
            },
        )
        self.assertEqual(decision["archive_receipt"], str(receipt_path))
        self.assertEqual(decision["canonical_receipt"], str(canonical))
        self.assertGreater(decision["verified_at_ns"], 0)
        self.assertTrue(record["destructive"])


class DerivativeTests(ReaperCase):
    def test_the_derivative_goes_on_the_archive_receipt_alone(self) -> None:
        """§7.2 — it is rebuildable from the sealed source, which is still here."""
        self.archive()
        self.canonicalize(sha256="c" * 64)  # canonical proof deliberately absent
        derivative = self.segment.with_name(
            self.segment.name[: -len(".ndjson")] + ".ndjson.zst"
        )
        self.assertTrue(derivative.exists())
        decision = self.only(self.reaper().sweep())
        self.assertEqual(decision.decision, RETAINED)
        # Retained for want of a canonical receipt, and the raw segment is
        # untouched — but the rebuildable derivative is gone.
        self.assertTrue(self.segment.exists())
        self.assertTrue(self.seal.exists())
        self.assertFalse(derivative.exists())
        self.assertTrue(decision.derivative_deleted)


if __name__ == "__main__":
    unittest.main()
