import importlib
import json
import sys
import tempfile
import unittest
from pathlib import Path

from archive.archiver.canonical import CanonicalArchiver
from archive.canonical_restore import (
    CanonicalRestoreError,
    preflight_canonical_window,
    restore_canonical_window,
)
from archive.common.receipts import read_canonical_receipt
from archive.storage import LocalObjectStore
from tests.archive_fixtures import BASE_NS, WINDOW_SECONDS, write_canonical_receipt


class CanonicalRestoreTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.source_root = self.root / "source"
        self.store = LocalObjectStore(self.root / "objects")
        self.source = write_canonical_receipt(self.source_root, evidence_lines=2)
        outcome = CanonicalArchiver(self.source_root, self.store).archive_window(self.source)
        self.assertEqual(outcome.status, "archived")

    def tearDown(self):
        self.temporary.cleanup()

    def test_verified_receipt_bootstrap_and_receipt_last_restore(self):
        remote = preflight_canonical_window(self.store, BASE_NS, WINDOW_SECONDS)
        destination = self.root / "restored"
        restored = restore_canonical_window(self.store, remote, destination)
        parsed = read_canonical_receipt(restored)
        self.assertEqual(parsed.window_start_ns, BASE_NS)
        self.assertEqual(parsed.evidence.decoded.line_count, 2)
        self.assertEqual(restored.read_bytes(), self.source.read_bytes())

    def test_restore_rejects_symlinked_partition_without_writing_outside(self):
        remote = preflight_canonical_window(self.store, BASE_NS, WINDOW_SECONDS)
        destination = self.root / "restored"
        destination.mkdir()
        outside = self.root / "outside"
        outside.mkdir()
        (destination / self.source.parent.parent.name).symlink_to(outside, target_is_directory=True)
        with self.assertRaises(CanonicalRestoreError):
            restore_canonical_window(self.store, remote, destination)
        self.assertEqual(tuple(outside.iterdir()), ())

    def test_missing_oversized_malformed_noncanonical_and_tampered_fail(self):
        absent = LocalObjectStore(self.root / "absent")
        self.assertIsNone(preflight_canonical_window(absent, BASE_NS, WINDOW_SECONDS))

        path = next(self.store.root.glob(f"canonical/date=*/window={BASE_NS}/receipt.json"))
        original = path.read_bytes()
        cases = (
            b"{" + b"x" * (1024 * 1024),
            b"not-json\n",
            json.dumps(json.loads(original)).encode() + b"\n",
            original.replace(b'"receipt_version": 1', b'"receipt_version": 2'),
        )
        for index, body in enumerate(cases):
            with self.subTest(index=index):
                path.write_bytes(body)
                with self.assertRaises(CanonicalRestoreError):
                    preflight_canonical_window(self.store, BASE_NS, WINDOW_SECONDS)
                path.write_bytes(original)

    def test_module_has_no_replay_or_targeter_import(self):
        before = set(sys.modules)
        sys.modules.pop("archive.canonical_restore", None)
        importlib.import_module("archive.canonical_restore")
        loaded = set(sys.modules) - before
        self.assertFalse(any(name == "replay" or name.startswith("replay.") for name in loaded))
        self.assertFalse(any(name == "targeter" or name.startswith("targeter.") for name in loaded))


if __name__ == "__main__":
    unittest.main()
