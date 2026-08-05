from __future__ import annotations

import json
import hashlib
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

from archive.storage import INDEPENDENT, LocalObjectStore, ObjectStoreError
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
    archive_run,
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

    def run_directory(self, *, now=NOW, empty: bool = False, broken: bool = False) -> Path:
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
        run_directory = self.run_directory()
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


if __name__ == "__main__":
    unittest.main()
