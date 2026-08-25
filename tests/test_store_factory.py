"""The shared archive store is configured only through environment values."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from archive.storage import factory as store_factory
from archive.storage import (
    CONFORMANCE,
    INDEPENDENT,
    GCSObjectStore,
    LocalObjectStore,
    S3ObjectStore,
)
from archive.storage import gcs as gcs_store
from archive.storage.factory import build_store


class FactoryCase(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.spool = self.root / "spool"
        self.spool.mkdir()
        self.archive_root = self.root / "archive"

    def local_environment(self, **extra: str) -> dict[str, str]:
        return {
            "ARCHIVE_BACKEND": "local",
            "ARCHIVE_ROOT": str(self.archive_root),
            **extra,
        }

    def s3_environment(self, **extra: str) -> dict[str, str]:
        return {
            "ARCHIVE_BACKEND": "s3",
            "ARCHIVE_S3_BUCKET": "prediction-indexer-raw",
            "ARCHIVE_S3_REGION": "us-east-1",
            "ARCHIVE_S3_EXPECTED_OWNER": "123456789012",
            **extra,
        }


class LocalBackendTests(FactoryCase):
    def test_local_conformance_is_the_default_backend(self) -> None:
        store = build_store(
            (self.spool,), environ={"ARCHIVE_ROOT": str(self.archive_root)}
        )
        self.assertIsInstance(store, LocalObjectStore)
        self.assertEqual(store.durability, CONFORMANCE)

    def test_local_requires_an_archive_root(self) -> None:
        with self.assertRaisesRegex(SystemExit, "ARCHIVE_ROOT"):
            build_store((self.spool,), environ={})

    def test_invalid_backend_and_durability_are_refused(self) -> None:
        with self.assertRaisesRegex(SystemExit, "ARCHIVE_BACKEND"):
            build_store((self.spool,), environ={"ARCHIVE_BACKEND": "azure"})
        with self.assertRaisesRegex(SystemExit, "ARCHIVE_DURABILITY"):
            build_store(
                (self.spool,),
                environ=self.local_environment(ARCHIVE_DURABILITY="maybe"),
            )

    def test_independence_is_refused_on_the_primary_filesystem(self) -> None:
        with self.assertRaisesRegex(SystemExit, "same filesystem"):
            build_store(
                (self.spool,),
                environ=self.local_environment(ARCHIVE_DURABILITY="independent"),
            )

    def test_independence_checks_every_primary_root(self) -> None:
        canonical = self.root / "canonical"
        canonical.mkdir()
        original = store_factory._device_of
        devices = {
            str(self.spool.resolve()): 1,
            str(canonical.resolve()): 2,
            str(self.archive_root.resolve()): 2,
        }

        def fake_device_of(path: Path) -> int:
            resolved = str(Path(path).resolve())
            return devices[resolved] if resolved in devices else original(path)

        store_factory._device_of = fake_device_of
        try:
            with self.assertRaisesRegex(SystemExit, str(canonical)):
                build_store(
                    (self.spool, canonical),
                    environ=self.local_environment(ARCHIVE_DURABILITY="independent"),
                )
        finally:
            store_factory._device_of = original

    def test_independence_is_granted_on_a_separate_device(self) -> None:
        original = store_factory._device_of
        store_factory._device_of = lambda path: (
            1 if Path(path).resolve() == self.spool.resolve() else 2
        )
        try:
            store = build_store(
                (self.spool,),
                environ=self.local_environment(ARCHIVE_DURABILITY="independent"),
            )
        finally:
            store_factory._device_of = original
        self.assertEqual(store.durability, INDEPENDENT)

    def test_local_store_id_and_mixed_cloud_configuration(self) -> None:
        store = build_store(
            (self.spool,),
            environ=self.local_environment(ARCHIVE_STORE_ID="my-archive"),
        )
        self.assertEqual(store.store_id, "my-archive")
        with self.assertRaisesRegex(SystemExit, "ARCHIVE_S3_BUCKET"):
            build_store(
                (self.spool,),
                environ=self.local_environment(ARCHIVE_S3_BUCKET="wrong"),
            )


class S3BackendTests(FactoryCase):
    def test_s3_requires_all_provider_fields(self) -> None:
        with self.assertRaises(SystemExit) as raised:
            build_store((self.spool,), environ={"ARCHIVE_BACKEND": "s3"})
        message = str(raised.exception)
        self.assertIn("ARCHIVE_S3_BUCKET", message)
        self.assertIn("ARCHIVE_S3_REGION", message)
        self.assertIn("ARCHIVE_S3_EXPECTED_OWNER", message)

    def test_s3_builds_an_independent_store(self) -> None:
        store = build_store((self.spool,), environ=self.s3_environment())
        self.assertIsInstance(store, S3ObjectStore)
        self.assertEqual(store.store_id, "prediction-indexer-raw")
        self.assertEqual(store.durability, INDEPENDENT)

    def test_s3_rejects_invalid_owner_and_gcs_configuration(self) -> None:
        with self.assertRaisesRegex(SystemExit, "expected_bucket_owner"):
            build_store(
                (self.spool,),
                environ=self.s3_environment(
                    ARCHIVE_S3_EXPECTED_OWNER="not-twelve-digits"
                ),
            )
        with self.assertRaisesRegex(SystemExit, "ARCHIVE_GCS_BUCKET"):
            build_store(
                (self.spool,),
                environ=self.s3_environment(ARCHIVE_GCS_BUCKET="wrong"),
            )


class GCSBackendTests(FactoryCase):
    def test_gcs_requires_a_bucket(self) -> None:
        with self.assertRaisesRegex(SystemExit, "ARCHIVE_GCS_BUCKET"):
            build_store((self.spool,), environ={"ARCHIVE_BACKEND": "gcs"})

    def test_gcs_builds_an_independent_store_with_adc(self) -> None:
        original = gcs_store._default_client
        gcs_store._default_client = lambda: object()
        try:
            store = build_store(
                (self.spool,),
                environ={
                    "ARCHIVE_BACKEND": "gcs",
                    "ARCHIVE_GCS_BUCKET": "prediction-archive",
                },
            )
        finally:
            gcs_store._default_client = original
        self.assertIsInstance(store, GCSObjectStore)
        self.assertEqual(store.store_id, "prediction-archive")
        self.assertEqual(store.durability, INDEPENDENT)

    def test_gcs_rejects_s3_configuration(self) -> None:
        with self.assertRaisesRegex(SystemExit, "cannot be combined"):
            build_store(
                (self.spool,),
                environ={
                    "ARCHIVE_BACKEND": "gcs",
                    "ARCHIVE_GCS_BUCKET": "prediction-archive",
                    "ARCHIVE_S3_BUCKET": "wrong-provider",
                },
            )


if __name__ == "__main__":
    unittest.main()
