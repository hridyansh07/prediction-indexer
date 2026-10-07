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
        with self.assertRaisesRegex(SystemExit, "local or gcs"):
            build_store((self.spool,), environ={"ARCHIVE_BACKEND": "s3"})
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
        with self.assertRaisesRegex(SystemExit, "ARCHIVE_GCS_BUCKET"):
            build_store(
                (self.spool,),
                environ=self.local_environment(ARCHIVE_GCS_BUCKET="wrong"),
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


if __name__ == "__main__":
    unittest.main()
