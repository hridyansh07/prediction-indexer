from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from analysis.storage import decoded_zstd_file, stable_job_id, write_json_zstd


class StorageTests(unittest.TestCase):
    def test_job_id_is_independent_of_dictionary_order(self) -> None:
        self.assertEqual(
            stable_job_id({"venue": "kalshi", "status": "settled"}),
            stable_job_id({"status": "settled", "venue": "kalshi"}),
        )

    def test_decoded_zstd_staging_uses_the_source_directory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            frame = Path(directory) / "selection_report.json.zst"
            identity = write_json_zstd(frame, {"selected": True})

            with patch(
                "analysis.storage.tempfile.TemporaryFile",
                wraps=tempfile.TemporaryFile,
            ) as temporary_file:
                with decoded_zstd_file(
                    frame,
                    expected_logical=identity.logical,
                    expected_stored=identity.stored,
                ) as decoded:
                    self.assertEqual(decoded.read(), b'{"selected":true}\n')

            temporary_file.assert_called_once_with(mode="w+b", dir=frame.parent)


if __name__ == "__main__":
    unittest.main()
