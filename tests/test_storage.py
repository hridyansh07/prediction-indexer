from __future__ import annotations

import unittest

from analysis.storage import stable_job_id


class StorageTests(unittest.TestCase):
    def test_job_id_is_independent_of_dictionary_order(self) -> None:
        self.assertEqual(
            stable_job_id({"venue": "kalshi", "status": "settled"}),
            stable_job_id({"status": "settled", "venue": "kalshi"}),
        )


if __name__ == "__main__":
    unittest.main()
