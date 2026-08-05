from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from targeter.targets import Target, TargetsError, load_targets, write_targets


class TargetsTests(unittest.TestCase):
    def setUp(self) -> None:
        self._directory = tempfile.TemporaryDirectory()
        self.path = Path(self._directory.name) / "targets.json"
        self.addCleanup(self._directory.cleanup)

    def test_digest_ignores_order_and_annotation(self) -> None:
        """Reordering the file must not force a reconnect, because a reconnect
        costs a book resync for an edit that changed nothing."""
        first = write_targets(self.path, venue="polymarket",
                              targets=[Target("a", note="x"), Target("b")])
        second = write_targets(self.path, venue="polymarket",
                               targets=[Target("b"), Target("a", note="different")])
        self.assertEqual(first, second)

    def test_digest_moves_when_an_asset_is_added(self) -> None:
        first = write_targets(self.path, venue="polymarket", targets=[Target("a")])
        second = write_targets(self.path, venue="polymarket",
                               targets=[Target("a"), Target("b")])
        self.assertNotEqual(first, second)

    def test_duplicate_asset_is_refused(self) -> None:
        """The socket would collapse them and coverage would overstate what was
        actually subscribed."""
        self.path.write_text(json.dumps(
            {"venue": "polymarket", "targets": [{"asset_id": "a"}, {"asset_id": "a"}]}
        ))
        with self.assertRaises(TargetsError):
            load_targets(self.path, venue="polymarket")

    def test_venue_mismatch_is_refused(self) -> None:
        write_targets(self.path, venue="kalshi", targets=[Target("a")])
        with self.assertRaises(TargetsError):
            load_targets(self.path, venue="polymarket")

    def test_empty_target_set_is_legal(self) -> None:
        write_targets(self.path, venue="polymarket", targets=[])
        self.assertEqual(len(load_targets(self.path, venue="polymarket")), 0)

    def test_raw_catalogue_evidence_is_durable_without_moving_subscription_identity(self) -> None:
        first = write_targets(
            self.path,
            venue="polymarket",
            targets=[
                Target(
                    "a",
                    resolution={
                        "version": 1,
                        "venue": "polymarket",
                        "catalogue_record_hash": "first",
                        "catalogue_record": {"description": "Resolves against feed A"},
                    },
                )
            ],
        )
        before = load_targets(self.path, venue="polymarket")

        second = write_targets(
            self.path,
            venue="polymarket",
            targets=[
                Target(
                    "a",
                    resolution={
                        "version": 1,
                        "venue": "polymarket",
                        "catalogue_record_hash": "second",
                        "catalogue_record": {"description": "Resolves against feed B"},
                    },
                )
            ],
        )
        after = load_targets(self.path, venue="polymarket")

        self.assertEqual(first, second)
        self.assertNotEqual(before.metadata_digest, after.metadata_digest)
        self.assertNotEqual(before.metadata_path, after.metadata_path)
        self.assertEqual(
            after.targets[0].resolution["catalogue_record"]["description"],
            "Resolves against feed B",
        )
        self.assertTrue(Path(after.metadata_path).exists())
        immutable = json.loads(Path(after.metadata_path).read_text(encoding="utf-8"))
        self.assertEqual(immutable["metadata_digest"], after.metadata_digest)

    def test_a_corrupt_metadata_snapshot_is_refused(self) -> None:
        write_targets(self.path, venue="polymarket", targets=[Target("a")])
        loaded = load_targets(self.path, venue="polymarket")
        Path(loaded.metadata_path).write_text("{}\n", encoding="utf-8")
        with self.assertRaises(TargetsError):
            load_targets(self.path, venue="polymarket")


if __name__ == "__main__":
    unittest.main()
