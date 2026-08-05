"""`archive/S3_RAW_ARCHIVE_ADAPTER_V1.md` §11 — the one shared backend factory.

Both `run_archiver.py` and `run_reaper.py` build their store through
`build_store`, so this is where the CLI contract for `--archive-backend` is
actually proven: what each combination of flags does, and — just as
important — what it refuses rather than guesses about.
"""

from __future__ import annotations

import argparse
import tempfile
import unittest
from pathlib import Path

from archive.storage import factory as store_factory
from archive.storage import CONFORMANCE, INDEPENDENT, LocalObjectStore, S3ObjectStore
from archive.storage.factory import add_store_arguments, build_store


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--spool-root", type=Path, required=True)
    add_store_arguments(parser)
    return parser


class FactoryCase(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.spool = self.root / "spool"
        self.spool.mkdir()
        self.archive_root = self.root / "archive"
        self.parser = make_parser()

    def parse(self, *extra: str) -> argparse.Namespace:
        return self.parser.parse_args(["--spool-root", str(self.spool), *extra])


class LocalBackendTests(FactoryCase):
    def test_the_default_backend_is_local_conformance(self) -> None:
        store = build_store(self.parse("--archive-root", str(self.archive_root)))
        self.assertIsInstance(store, LocalObjectStore)
        self.assertEqual(store.durability, CONFORMANCE)

    def test_local_requires_an_archive_root(self) -> None:
        with self.assertRaises(SystemExit) as raised:
            build_store(self.parse("--archive-backend", "local"))
        self.assertIn("--archive-root", str(raised.exception))

    def test_independence_is_refused_when_it_shares_the_capture_disk(self) -> None:
        with self.assertRaises(SystemExit) as raised:
            build_store(
                self.parse(
                    "--archive-root",
                    str(self.archive_root),
                    "--archive-durability",
                    "independent",
                )
            )
        self.assertIn("same filesystem", str(raised.exception))

    def test_independence_is_granted_on_a_genuinely_separate_device(self) -> None:
        original = store_factory._device_of
        devices = {str(self.spool.resolve()): 1, str(self.archive_root.resolve()): 2}

        def fake_device_of(path: Path) -> int:
            resolved = str(Path(path).resolve())
            return devices.get(resolved, original(path))

        store_factory._device_of = fake_device_of
        try:
            store = build_store(
                self.parse(
                    "--archive-root",
                    str(self.archive_root),
                    "--archive-durability",
                    "independent",
                )
            )
        finally:
            store_factory._device_of = original
        self.assertEqual(store.durability, INDEPENDENT)

    def test_a_live_s3_option_while_local_is_selected_fails_at_startup(self) -> None:
        with self.assertRaises(SystemExit) as raised:
            build_store(
                self.parse(
                    "--archive-root", str(self.archive_root), "--s3-bucket", "some-bucket"
                )
            )
        self.assertIn("--s3-bucket", str(raised.exception))

    def test_store_id_is_honored_for_local(self) -> None:
        store = build_store(
            self.parse("--archive-root", str(self.archive_root), "--store-id", "my-archive")
        )
        self.assertEqual(store.store_id, "my-archive")


class S3BackendTests(FactoryCase):
    def s3_args(self, *extra: str) -> list[str]:
        return [
            "--archive-backend",
            "s3",
            "--s3-bucket",
            "prediction-indexer-raw",
            "--s3-region",
            "us-east-1",
            "--s3-expected-owner",
            "123456789012",
            *extra,
        ]

    def test_s3_requires_all_three_fields(self) -> None:
        with self.assertRaises(SystemExit) as raised:
            build_store(self.parse("--archive-backend", "s3"))
        message = str(raised.exception)
        self.assertIn("--s3-bucket", message)
        self.assertIn("--s3-region", message)
        self.assertIn("--s3-expected-owner", message)

    def test_s3_requires_each_missing_field_individually(self) -> None:
        with self.assertRaises(SystemExit) as raised:
            build_store(
                self.parse(
                    "--archive-backend",
                    "s3",
                    "--s3-bucket",
                    "prediction-indexer-raw",
                    "--s3-region",
                    "us-east-1",
                )
            )
        self.assertIn("--s3-expected-owner", str(raised.exception))
        self.assertNotIn("--s3-bucket", str(raised.exception))

    def test_a_complete_s3_configuration_builds_an_independent_store(self) -> None:
        store = build_store(self.parse(*self.s3_args()))
        self.assertIsInstance(store, S3ObjectStore)
        self.assertEqual(store.store_id, "prediction-indexer-raw")
        self.assertEqual(store.durability, INDEPENDENT)

    def test_an_invalid_expected_owner_is_refused(self) -> None:
        with self.assertRaises(SystemExit) as raised:
            build_store(
                self.parse(
                    "--archive-backend",
                    "s3",
                    "--s3-bucket",
                    "b",
                    "--s3-region",
                    "us-east-1",
                    "--s3-expected-owner",
                    "not-twelve-digits",
                )
            )
        self.assertIn("expected_bucket_owner", str(raised.exception))

    def test_archive_durability_cannot_downgrade_or_upgrade_s3(self) -> None:
        store = build_store(self.s3_store_arguments("conformance"))
        self.assertEqual(store.durability, INDEPENDENT)
        store = build_store(self.s3_store_arguments("independent"))
        self.assertEqual(store.durability, INDEPENDENT)

    def s3_store_arguments(self, durability: str) -> argparse.Namespace:
        return self.parse(*self.s3_args("--archive-durability", durability))

    def test_the_static_compose_command_can_still_pass_local_archive_root_and_store_id(
        self,
    ) -> None:
        """§11: those flags are ignored for S3, not rejected, so one command line works."""
        store = build_store(
            self.parse(
                *self.s3_args(
                    "--archive-root",
                    str(self.archive_root),
                    "--store-id",
                    "local-archive",
                    "--archive-durability",
                    "conformance",
                )
            )
        )
        self.assertIsInstance(store, S3ObjectStore)
        self.assertEqual(store.store_id, "prediction-indexer-raw")
        self.assertEqual(store.durability, INDEPENDENT)


if __name__ == "__main__":
    unittest.main()
