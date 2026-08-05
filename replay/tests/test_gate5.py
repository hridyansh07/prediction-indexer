from __future__ import annotations

import json
import unittest
from pathlib import Path

from replay.gate5 import POLICY_PATH, freeze_policy


class FrozenPolicyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.document = json.loads(
            Path(POLICY_PATH).read_text(encoding="utf-8")
        )

    def test_policy_hash_is_independent_of_mapping_insertion_order(self) -> None:
        reversed_document = dict(reversed(list(self.document.items())))

        self.assertEqual(
            freeze_policy(self.document).sha256,
            freeze_policy(reversed_document).sha256,
        )

    def test_unknown_policy_field_is_rejected(self) -> None:
        invalid = {**self.document, "after_looking": True}

        with self.assertRaises(ValueError):
            freeze_policy(invalid)


if __name__ == "__main__":
    unittest.main()
