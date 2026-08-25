from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from archive.archiver.publish import ArchiveFile, publish_files
from archive.storage.local import LocalObjectStore
from encoder import stored_identity_of


class ArchivePublisherTests(unittest.TestCase):
    def test_files_publish_in_order_through_one_store_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = root / "first.json"
            second = root / "second.zst"
            first.write_bytes(b"{}\n")
            second.write_bytes(b"compressed")
            store = LocalObjectStore(root / "objects")

            files = (
                ArchiveFile(
                    first,
                    "runs/first.json",
                    _identity(first),
                    "application/json",
                ),
                ArchiveFile(
                    second,
                    "runs/second.zst",
                    _identity(second),
                    "application/x-ndjson",
                    "zstd",
                ),
            )

            published = publish_files(store, files)

            self.assertEqual(
                tuple(item.key for item in published), tuple(item.key for item in files)
            )
            self.assertEqual(published[1].content_encoding, "zstd")


def _identity(path: Path):
    with path.open("rb") as reader:
        return stored_identity_of(reader)


if __name__ == "__main__":
    unittest.main()
