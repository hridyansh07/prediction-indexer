from __future__ import annotations

import json
import hashlib
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

from archive.storage import INDEPENDENT, LocalObjectStore, ObjectStoreError
from encoder import StoredIdentity
from targeter.targets import TargetsError, load_targets
from targeter.v2.domain import CatalogSnapshot
from targeter.v2.publication import (
    PublicationError,
    audit_current_publication,
    publish_run,
)
from targeter.v2.registry import load_strategy
from targeter.v2.run import run_shadow
from targeter.v2.run_archive import (
    RunArchiveError,
    archive_run,
    parse_run_archive_receipt,
    read_run_archive_receipt,
    verify_run_archive,
)
from tests.test_targeter_v2 import NOW, STRATEGY_PATH, snapshot


class _Adapter:
    def __init__(self, catalog: CatalogSnapshot) -> None:
        self.venue = catalog.venue
        self.catalog = catalog

    def discover(self, _client, *, now):
        return self.catalog


class _BrokenAdapter:
    venue = "limitless"

    def discover(self, _client, *, now):
        raise RuntimeError("catalogue unavailable")


class _FailingManifestStore:
    def __init__(self, delegate: LocalObjectStore) -> None:
        self.delegate = delegate
        self.store_id = delegate.store_id
        self.durability = delegate.durability

    def put_immutable(self, key, reader, expected_identity, **kwargs):
        if key.endswith("/run_manifest.json"):
            raise ObjectStoreError("injected manifest failure")
        return self.delegate.put_immutable(key, reader, expected_identity, **kwargs)

    def head(self, key):
        return self.delegate.head(key)

    def open(self, key, *, max_bytes=None):
        return self.delegate.open(key, max_bytes=max_bytes)


class TargeterV2DeliveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.output_root = self.root / "runs"
        self.live_root = self.root / "live"
        self.strategy = load_strategy(STRATEGY_PATH)
        self.store = LocalObjectStore(
            self.root / "object-store",
            store_id="targeter-test-bucket",
            durability=INDEPENDENT,
        )

    def run_directory(
        self,
        *,
        now=NOW,
        empty: bool = False,
        broken: bool = False,
        artifact_format: str = "zstd",
    ) -> Path:
        catalogs = (
            CatalogSnapshot("kalshi", (), ()) if empty else snapshot("kalshi", "k", "km"),
            CatalogSnapshot("polymarket", (), ()) if empty else snapshot("polymarket", "p", "pm"),
        )
        adapters = [_Adapter(catalog) for catalog in catalogs]
        if broken:
            adapters.append(_BrokenAdapter())
        else:
            adapters.append(_Adapter(CatalogSnapshot("limitless", (), ())))
        result = run_shadow(
            strategy=self.strategy,
            output_root=self.output_root,
            cache_root=self.root / "cache",
            now=now,
            adapters=adapters,
            client=object(),
            artifact_format=artifact_format,
        )
        return result.directory

    def test_phase6_archives_every_artifact_and_verifies_idempotently(self) -> None:
        run_directory = self.run_directory()
        receipt = archive_run(run_directory, self.store, now=NOW)

        self.assertTrue(receipt.is_production)
        self.assertTrue(receipt.manifest.key.endswith("/run_manifest.json"))
        self.assertGreaterEqual(len(receipt.objects), 6)
        verify_run_archive(self.store, receipt)

        retry = archive_run(run_directory, self.store, now=NOW + timedelta(minutes=1))
        self.assertEqual(retry.document, receipt.document)
        self.assertEqual(
            read_run_archive_receipt(run_directory / "archive_receipt.json").document,
            receipt.document,
        )

    def test_v2_receipt_requires_decoded_identity_for_every_ndjson_artifact(self) -> None:
        run_directory = self.run_directory()
        receipt = archive_run(run_directory, self.store, now=NOW)
        document = json.loads(json.dumps(receipt.document))
        artifact = next(
            item for item in document["objects"] if item["file"].endswith(".ndjson.zst")
        )
        del artifact["decoded"]
        if document["manifest"]["file"] == artifact["file"]:
            del document["manifest"]["decoded"]

        with self.assertRaisesRegex(RunArchiveError, "decoded"):
            parse_run_archive_receipt(
                document,
                path=run_directory / "archive_receipt.json",
            )

    def test_interrupted_v1_manifest_archive_resumes_without_rewriting_it(self) -> None:
        run_directory = self.run_directory(artifact_format="ndjson")
        report_path = run_directory / "selection_report.json"
        report = json.loads(report_path.read_text())
        report.pop("artifact_format")
        report.pop("artifacts")
        report_path.write_text(
            json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        source_files = sorted(run_directory.iterdir(), key=lambda path: path.name)
        legacy_manifest = {
            "targeter_run_manifest_version": 1,
            "run_id": run_directory.name,
            "generated_at": report["generated_at"],
            "input_complete": report["input_complete"],
            "files": [],
        }
        for path in source_files:
            content = path.read_bytes()
            legacy_manifest["files"].append(
                {
                    "file": path.name,
                    "byte_length": len(content),
                    "sha256": hashlib.sha256(content).hexdigest(),
                    "content_type": (
                        "application/x-ndjson"
                        if path.name.endswith(".ndjson")
                        else "application/json"
                    ),
                }
            )
        manifest_path = run_directory / "run_manifest.json"
        manifest_path.write_text(
            json.dumps(legacy_manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        prefix = f"targeter-v2/runs/date={NOW:%Y-%m-%d}/run={run_directory.name}"
        for path in [*source_files, manifest_path]:
            content = path.read_bytes()
            identity = StoredIdentity(
                sha256=hashlib.sha256(content).hexdigest(),
                byte_length=len(content),
            )
            with path.open("rb") as reader:
                self.store.put_immutable(
                    f"{prefix}/{path.name}",
                    reader,
                    identity,
                    content_type=(
                        "application/x-ndjson"
                        if path.name.endswith(".ndjson")
                        else "application/json"
                    ),
                    content_encoding=None,
                )

        before = manifest_path.read_bytes()
        receipt = archive_run(run_directory, self.store, now=NOW)
        self.assertEqual(manifest_path.read_bytes(), before)
        self.assertEqual(receipt.document["targeter_run_archive_receipt_version"], 2)
        verify_run_archive(self.store, receipt)

    def test_remote_manifest_is_the_commit_marker(self) -> None:
        run_directory = self.run_directory()
        failing = _FailingManifestStore(self.store)
        with self.assertRaisesRegex(ObjectStoreError, "manifest failure"):
            archive_run(run_directory, failing, now=NOW)
        self.assertFalse((run_directory / "archive_receipt.json").exists())
        self.assertIsNone(
            self.store.head(
                f"targeter-v2/runs/date={NOW:%Y-%m-%d}/run={run_directory.name}/run_manifest.json"
            )
        )

    def test_phase7_publishes_one_atomic_generation_consumable_by_every_splice(self) -> None:
        run_directory = self.run_directory()
        receipt = archive_run(run_directory, self.store, now=NOW)
        generation = publish_run(
            run_directory,
            receipt,
            self.store,
            live_root=self.live_root,
            strategy=self.strategy,
            now=NOW,
        )

        pointer = self.live_root / "targeter-v2" / "current.json"
        kalshi = load_targets(pointer, venue="kalshi")
        polymarket = load_targets(pointer, venue="polymarket")
        limitless = load_targets(pointer, venue="limitless")
        self.assertEqual(kalshi.asset_ids(), ("km-subscription",))
        self.assertEqual(polymarket.asset_ids(), ("pm-subscription",))
        self.assertEqual(limitless.asset_ids(), ())
        self.assertEqual(kalshi.targets[0].resolution["run_id"], run_directory.name)
        self.assertEqual(generation.run_id, run_directory.name)

        audited = audit_current_publication(
            live_root=self.live_root,
            output_root=self.output_root,
            store=self.store,
            strategy=self.strategy,
        )
        self.assertEqual(audited.run_id, run_directory.name)
        self.assertEqual(audited.venue_counts, {"kalshi": 1, "limitless": 0, "polymarket": 1})

    def test_a_pointer_failure_exposes_no_partial_generation_and_retry_is_safe(self) -> None:
        run_directory = self.run_directory()
        receipt = archive_run(run_directory, self.store, now=NOW)
        from targeter.v2 import publication

        original = publication.write_json_durable

        def fail_pointer(path, document):
            if Path(path).name == "current.json":
                raise OSError("pointer fsync failed")
            return original(path, document)

        with patch("targeter.v2.publication.write_json_durable", side_effect=fail_pointer):
            with self.assertRaisesRegex(OSError, "pointer fsync"):
                publish_run(
                    run_directory,
                    receipt,
                    self.store,
                    live_root=self.live_root,
                    strategy=self.strategy,
                    now=NOW,
                )

        pointer = self.live_root / "targeter-v2" / "current.json"
        self.assertFalse(pointer.exists())
        self.assertTrue(
            (self.live_root / "targeter-v2" / "generations" / run_directory.name / "manifest.json").exists()
        )
        with self.assertRaises(TargetsError):
            load_targets(pointer, venue="kalshi")

        publish_run(
            run_directory,
            receipt,
            self.store,
            live_root=self.live_root,
            strategy=self.strategy,
            now=NOW + timedelta(minutes=1),
        )
        self.assertEqual(load_targets(pointer, venue="kalshi").asset_ids(), ("km-subscription",))

    def test_empty_or_incomplete_runs_never_replace_the_current_generation(self) -> None:
        good_directory = self.run_directory()
        good_receipt = archive_run(good_directory, self.store, now=NOW)
        publish_run(
            good_directory,
            good_receipt,
            self.store,
            live_root=self.live_root,
            strategy=self.strategy,
            now=NOW,
        )
        pointer = self.live_root / "targeter-v2" / "current.json"
        before = pointer.read_bytes()

        for offset, options, message in (
            (1, {"empty": True}, "empty"),
            (2, {"broken": True}, "incomplete"),
        ):
            run_directory = self.run_directory(now=NOW + timedelta(minutes=offset), **options)
            receipt = archive_run(run_directory, self.store, now=NOW + timedelta(minutes=offset))
            with self.subTest(message=message):
                with self.assertRaises(PublicationError):
                    publish_run(
                        run_directory,
                        receipt,
                        self.store,
                        live_root=self.live_root,
                        strategy=self.strategy,
                        now=NOW + timedelta(minutes=offset),
                    )
                self.assertEqual(pointer.read_bytes(), before)

    def test_conformance_archive_is_not_publication_authority(self) -> None:
        run_directory = self.run_directory()
        conformance = LocalObjectStore(self.root / "conformance")
        receipt = archive_run(run_directory, conformance, now=NOW)
        self.assertFalse(receipt.is_production)
        with self.assertRaisesRegex(PublicationError, "independent"):
            publish_run(
                run_directory,
                receipt,
                conformance,
                live_root=self.live_root,
                strategy=self.strategy,
                now=NOW,
            )

    def test_publication_rejects_a_selection_target_forged_outside_the_catalog(self) -> None:
        run_directory = self.run_directory(artifact_format="ndjson")
        report_path = run_directory / "selection_report.json"
        report = json.loads(report_path.read_text(encoding="utf-8"))
        report["selection"]["targets"]["kalshi"][0]["subscription_ids"] = ["forged-id"]
        report_path.write_text(json.dumps(report, sort_keys=True) + "\n", encoding="utf-8")
        receipt = archive_run(run_directory, self.store, now=NOW)
        with self.assertRaisesRegex(PublicationError, "catalog"):
            publish_run(
                run_directory,
                receipt,
                self.store,
                live_root=self.live_root,
                strategy=self.strategy,
                now=NOW,
            )

    def test_pointer_reader_rejects_manifest_or_target_corruption(self) -> None:
        run_directory = self.run_directory()
        receipt = archive_run(run_directory, self.store, now=NOW)
        generation = publish_run(
            run_directory,
            receipt,
            self.store,
            live_root=self.live_root,
            strategy=self.strategy,
            now=NOW,
        )
        pointer = self.live_root / "targeter-v2" / "current.json"
        target_path = generation.directory / "targets_kalshi.json"
        target_path.write_text("{}\n", encoding="utf-8")
        with self.assertRaisesRegex(TargetsError, "identity"):
            load_targets(pointer, venue="kalshi")

    def test_pointer_reader_rejects_path_traversal(self) -> None:
        pointer = self.live_root / "targeter-v2" / "current.json"
        pointer.parent.mkdir(parents=True)
        pointer.write_text(
            json.dumps(
                {
                    "target_generation_pointer_version": 1,
                    "run_id": "run",
                    "manifest_path": "../../outside.json",
                    "manifest": {"byte_length": 1, "sha256": "0" * 64},
                }
            ),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(TargetsError, "escapes"):
            load_targets(pointer, venue="kalshi")

    def test_pointer_reader_bounds_metadata_to_the_committed_generation(self) -> None:
        run_directory = self.run_directory()
        receipt = archive_run(run_directory, self.store, now=NOW)
        generation = publish_run(
            run_directory,
            receipt,
            self.store,
            live_root=self.live_root,
            strategy=self.strategy,
            now=NOW,
        )
        target_path = generation.directory / "targets_kalshi.json"
        target_document = json.loads(target_path.read_text(encoding="utf-8"))
        original_metadata = generation.directory / target_document["metadata_path"]
        escaped_metadata = generation.directory.parent / "outside.json"
        escaped_metadata.write_bytes(original_metadata.read_bytes())
        target_document["metadata_path"] = "../outside.json"
        target_path.write_text(json.dumps(target_document, sort_keys=True) + "\n", encoding="utf-8")

        manifest = json.loads(generation.manifest_path.read_text(encoding="utf-8"))
        target_bytes = target_path.read_bytes()
        manifest["venues"]["kalshi"]["target_file"] = {
            "file": target_path.name,
            "byte_length": len(target_bytes),
            "sha256": hashlib.sha256(target_bytes).hexdigest(),
        }
        generation.manifest_path.write_text(json.dumps(manifest, sort_keys=True) + "\n", encoding="utf-8")
        manifest_bytes = generation.manifest_path.read_bytes()
        pointer = json.loads(generation.pointer_path.read_text(encoding="utf-8"))
        pointer["manifest"] = {
            "byte_length": len(manifest_bytes),
            "sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        }
        generation.pointer_path.write_text(json.dumps(pointer, sort_keys=True) + "\n", encoding="utf-8")

        with self.assertRaisesRegex(TargetsError, "metadata snapshot path escapes"):
            load_targets(generation.pointer_path, venue="kalshi")

    def test_one_shot_publish_command_archives_then_publishes(self) -> None:
        from targeter.v2.run import ShadowRun, main

        run_directory = self.run_directory()
        shadow = ShadowRun(
            run_id=run_directory.name,
            directory=run_directory,
            selection=run_shadow(
                strategy=self.strategy,
                output_root=self.root / "unused-runs",
                cache_root=self.root / "unused-cache",
                now=NOW + timedelta(seconds=1),
                adapters=(
                    _Adapter(snapshot("kalshi", "k2", "km2")),
                    _Adapter(snapshot("polymarket", "p2", "pm2")),
                    _Adapter(CatalogSnapshot("limitless", (), ())),
                ),
                client=object(),
            ).selection,
            discovery_failures={},
            input_complete=True,
        )
        with (
            patch("targeter.v2.run.run_shadow", return_value=shadow),
            patch("targeter.v2.run.build_store", return_value=self.store),
        ):
            status = main(
                [
                    "--mode", "publish",
                    "--strategy", str(STRATEGY_PATH),
                    "--output-root", str(self.output_root),
                    "--live-root", str(self.live_root),
                    "--archive-backend", "s3",
                    "--s3-bucket", "targeter-test-bucket",
                    "--s3-region", "us-east-1",
                    "--s3-expected-owner", "123456789012",
                ]
            )
        self.assertEqual(status, 0)
        self.assertTrue((run_directory / "archive_receipt.json").is_file())
        self.assertEqual(
            load_targets(self.live_root / "targeter-v2" / "current.json", venue="kalshi").asset_ids(),
            ("km-subscription",),
        )

    def test_publish_command_archives_incomplete_evidence_but_does_not_publish(self) -> None:
        from targeter.v2.run import main

        run_directory = self.run_directory(broken=True)
        shadow = run_shadow(
            strategy=self.strategy,
            output_root=self.root / "other-runs",
            cache_root=self.root / "other-cache",
            now=NOW + timedelta(seconds=2),
            adapters=(
                _Adapter(snapshot("kalshi", "k3", "km3")),
                _BrokenAdapter(),
            ),
            client=object(),
        )
        shadow = type(shadow)(
            run_directory.name,
            run_directory,
            shadow.selection,
            {"limitless": "failed"},
            input_complete=False,
        )
        with (
            patch("targeter.v2.run.run_shadow", return_value=shadow),
            patch("targeter.v2.run.build_store", return_value=self.store),
        ):
            status = main(
                [
                    "--mode", "publish",
                    "--strategy", str(STRATEGY_PATH),
                    "--output-root", str(self.output_root),
                    "--live-root", str(self.live_root),
                    "--archive-backend", "s3",
                    "--s3-bucket", "bucket",
                    "--s3-region", "us-east-1",
                    "--s3-expected-owner", "123456789012",
                ]
            )
        self.assertEqual(status, 1)
        self.assertTrue((run_directory / "archive_receipt.json").is_file())
        self.assertFalse((self.live_root / "targeter-v2" / "current.json").exists())

    def test_audit_command_does_not_perform_live_discovery(self) -> None:
        from targeter.v2.run import main

        run_directory = self.run_directory()
        receipt = archive_run(run_directory, self.store, now=NOW)
        publish_run(
            run_directory,
            receipt,
            self.store,
            live_root=self.live_root,
            strategy=self.strategy,
            now=NOW,
        )
        with (
            patch("targeter.v2.run.run_shadow", side_effect=AssertionError("discovery ran")),
            patch("targeter.v2.run.build_store", return_value=self.store),
        ):
            status = main(
                [
                    "--mode", "audit",
                    "--strategy", str(STRATEGY_PATH),
                    "--output-root", str(self.output_root),
                    "--live-root", str(self.live_root),
                    "--archive-backend", "s3",
                    "--s3-bucket", "bucket",
                    "--s3-region", "us-east-1",
                    "--s3-expected-owner", "123456789012",
                ]
            )
        self.assertEqual(status, 0)

    def test_one_shot_command_refuses_an_overlapping_discovery(self) -> None:
        from targeter.v2.lease import TargeterRunLease
        from targeter.v2.run import main

        with TargeterRunLease.acquire(self.output_root):
            with patch("targeter.v2.run.run_shadow") as discovery:
                status = main(
                    [
                        "--mode", "shadow",
                        "--strategy", str(STRATEGY_PATH),
                        "--output-root", str(self.output_root),
                    ]
                )
        self.assertEqual(status, 2)
        discovery.assert_not_called()


class CoverageLedgerTests(unittest.TestCase):
    """Coverage-from-inception must survive the v1 to v2 move.

    v1 kept this ledger from its discovery loop; v2 shipped without it, so
    `discovery_coverage` in `replay/gate1.py` read 453 subscribed assets and
    zero covered against a real 19-hour capture. The number it protects —
    how much of a market's life the tape contains — cannot be recovered later
    from the frames, which all look healthy whether or not the open was missed.
    """

    def setUp(self) -> None:
        self.case = TargeterV2DeliveryTests("run")
        self.case.setUp()
        self.addCleanup(self.case.directory.cleanup)

    def publish(self, *, now=NOW):
        run_directory = self.case.run_directory(now=now)
        receipt = archive_run(run_directory, self.case.store, now=now)
        return publish_run(
            run_directory,
            receipt,
            self.case.store,
            live_root=self.case.live_root,
            strategy=self.case.strategy,
            now=now,
        )

    def ledger_document(self) -> dict:
        path = self.case.live_root / "coverage.json"
        self.assertTrue(path.exists(), "publication wrote no coverage ledger")
        return json.loads(path.read_text(encoding="utf-8"))

    def sightings(self) -> dict[tuple[str, str], dict]:
        return {
            (item["venue"], item["asset_id"]): item
            for item in self.ledger_document()["sightings"]
        }

    def test_publishing_records_a_first_sighting_for_every_subscribed_asset(self) -> None:
        generation = self.publish()
        subscribed = {
            (venue, asset)
            for venue in generation.venue_counts
            for asset in load_targets(
                self.case.live_root / "targeter-v2" / "current.json", venue=venue
            ).asset_ids()
        }
        self.assertEqual(subscribed, set(self.sightings()))
        self.assertEqual(generation.newly_seen, {"kalshi": 1, "limitless": 0, "polymarket": 1})

    def test_the_ledger_is_where_gate_one_looks_and_carries_the_fields_it_reads(self) -> None:
        # `_AuditState.observe_coverage` (replay/gate1.py:414-423) rejects a
        # document without `sightings` and keys each entry on venue + asset_id;
        # `gate1_object` admits it only at exactly this path.
        self.publish()
        self.assertTrue((self.case.live_root / "coverage.json").exists())
        document = self.ledger_document()
        self.assertIsInstance(document.get("sightings"), list)
        for item in document["sightings"]:
            self.assertIsInstance(item.get("venue"), str)
            self.assertIsInstance(item.get("asset_id"), str)
            self.assertIn("created_at", item)

    def test_a_venue_creation_time_makes_discovery_lag_measurable(self) -> None:
        self.publish()
        kalshi = self.sightings()[("kalshi", "km-subscription")]
        self.assertIsNotNone(
            kalshi["created_at"], "the archived catalogue record carries created_at"
        )
        # The fixture market is created a day before the run.
        self.assertAlmostEqual(kalshi["discovery_lag_seconds"], 86_400, delta=1)

    def test_a_later_generation_never_overwrites_an_existing_first_sighting(self) -> None:
        self.publish()
        before = self.sightings()[("kalshi", "km-subscription")]["first_seen_at"]
        later = self.publish(now=NOW + timedelta(hours=3))
        after = self.sightings()[("kalshi", "km-subscription")]["first_seen_at"]
        self.assertEqual(before, after)
        # Nothing new to see, so the second publication claims no fresh coverage.
        self.assertEqual(later.newly_seen, {"kalshi": 0, "limitless": 0, "polymarket": 0})

    def test_a_venue_selecting_nothing_contributes_no_sightings(self) -> None:
        self.publish()
        self.assertNotIn(
            "limitless", {venue for venue, _ in self.sightings()},
            "an unsubscribed venue must not claim coverage it does not have",
        )


class CoverageBackfillTests(unittest.TestCase):
    """Reconstructing sightings for generations published before the ledger existed.

    The failure this prevents is not a missing number, it is a wrong one. A
    ledger started today stamps assets subscribed days ago with today's date,
    and because `first_seen_at` bounds how far back the tape counts as covered,
    that makes real captured frames look like they predate coverage.
    """

    def setUp(self) -> None:
        self.case = TargeterV2DeliveryTests("run")
        self.case.setUp()
        self.addCleanup(self.case.directory.cleanup)

    def publish_at(self, moment):
        run_directory = self.case.run_directory(now=moment)
        receipt = archive_run(run_directory, self.case.store, now=moment)
        return publish_run(
            run_directory,
            receipt,
            self.case.store,
            live_root=self.case.live_root,
            strategy=self.case.strategy,
            now=moment,
        )

    def backfill(self) -> dict:
        from scripts.backfill_coverage import backfill

        return backfill(self.case.live_root, self.case.output_root)

    def sightings(self) -> dict[tuple[str, str], dict]:
        document = json.loads(
            (self.case.live_root / "coverage.json").read_text(encoding="utf-8")
        )
        return {(item["venue"], item["asset_id"]): item for item in document["sightings"]}

    def test_a_ledger_deleted_after_the_fact_is_rebuilt_from_the_generations(self) -> None:
        self.publish_at(NOW)
        self.publish_at(NOW + timedelta(hours=6))
        expected = self.sightings()
        (self.case.live_root / "coverage.json").unlink()

        summary = self.backfill()
        self.assertEqual(summary["generations"], 2)
        self.assertEqual(summary["unreadable"], [])
        # A rebuild from empty must *report* what it wrote. Counting only the
        # repairs made a first run indistinguishable from a no-op second one,
        # which is the one thing an operator reads this summary to tell apart.
        self.assertEqual(summary["recorded"], {"kalshi": 1, "limitless": 0, "polymarket": 1})
        self.assertEqual(summary["sightings"], 2)
        self.assertEqual(summary["repaired"], 0)
        rebuilt = self.sightings()
        self.assertEqual(set(rebuilt), set(expected))
        for key, item in rebuilt.items():
            self.assertEqual(item["first_seen_at"], expected[key]["first_seen_at"], key)
            self.assertEqual(item["created_at"], expected[key]["created_at"], key)

    def test_the_earliest_generation_wins_not_the_most_recent(self) -> None:
        self.publish_at(NOW)
        self.publish_at(NOW + timedelta(hours=6))
        (self.case.live_root / "coverage.json").unlink()
        self.backfill()
        first_seen = self.sightings()[("kalshi", "km-subscription")]["first_seen_at"]
        self.assertEqual(first_seen[:19], NOW.isoformat()[:19])

    def test_it_repairs_a_sighting_an_earlier_generation_contradicts(self) -> None:
        # The state a live-only ledger leaves behind: capture ran for hours, the
        # writer was deployed late, and the first sighting it recorded is the
        # deployment time rather than the subscription time.
        self.publish_at(NOW)
        late = self.publish_at(NOW + timedelta(hours=6))
        self.assertEqual(late.newly_seen["kalshi"], 0)
        stale = {
            "version": 1,
            "updated_at": (NOW + timedelta(hours=6)).isoformat(),
            "sightings": [
                {
                    "asset_id": "km-subscription",
                    "venue": "kalshi",
                    "first_seen_at": (NOW + timedelta(hours=6)).isoformat(),
                    "created_at": None,
                    "discovery_lag_seconds": None,
                }
            ],
        }
        (self.case.live_root / "coverage.json").write_text(json.dumps(stale), encoding="utf-8")

        summary = self.backfill()
        self.assertEqual(summary["repaired"], 1)
        repaired = self.sightings()[("kalshi", "km-subscription")]
        self.assertEqual(repaired["first_seen_at"][:19], NOW.isoformat()[:19])
        # The repair also recovers the creation time the stale entry lacked.
        self.assertIsNotNone(repaired["created_at"])

    def test_running_it_twice_changes_nothing(self) -> None:
        self.publish_at(NOW)
        self.backfill()
        once = (self.case.live_root / "coverage.json").read_text(encoding="utf-8")
        second = self.backfill()
        self.assertEqual(second["repaired"], 0)
        self.assertEqual(second["recorded"], {"kalshi": 0, "limitless": 0, "polymarket": 0})
        twice = json.loads((self.case.live_root / "coverage.json").read_text(encoding="utf-8"))
        self.assertEqual(json.loads(once)["sightings"], twice["sightings"])

    def test_a_reaped_run_still_yields_its_sighting(self) -> None:
        # The reaper removes run artifacts but keeps the receipt as a tombstone.
        # Coverage must survive that, because the generation is the evidence for
        # what was subscribed; the run directory only carries created_at.
        self.publish_at(NOW)
        run_directory = self.case.output_root / sorted(
            item.name for item in self.case.output_root.iterdir() if item.is_dir()
        )[0]
        for artifact in run_directory.iterdir():
            if artifact.name != "archive_receipt.json":
                artifact.unlink()
        (self.case.live_root / "coverage.json").unlink()

        summary = self.backfill()
        self.assertEqual(summary["unreadable"], [])
        sighting = self.sightings()[("kalshi", "km-subscription")]
        self.assertEqual(sighting["first_seen_at"][:19], NOW.isoformat()[:19])
        self.assertIsNone(sighting["created_at"], "a reaped catalogue cannot supply one")


if __name__ == "__main__":
    unittest.main()
