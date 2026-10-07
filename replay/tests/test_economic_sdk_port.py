"""SDK port acceptance: complement policy V1 reproduces the pre-SDK output.

The expected hashes were recorded from the pre-SDK implementation (commit
"Record same-venue complement V1 golden output hashes") on deterministic
synthetic tapes; see ``economic_scenarios``.
"""

import json
import tempfile
import unittest
from pathlib import Path

from replay.tests import economic_scenarios

GOLDEN = json.loads((Path(__file__).parent / "fixtures" / "complement_v1_golden.json").read_text())


class ComplementV1PortTests(unittest.TestCase):
    def test_every_v1_file_is_byte_identical_to_the_pre_sdk_implementation(self):
        self.assertEqual(set(GOLDEN), set(economic_scenarios.SCENARIOS))
        for name in economic_scenarios.SCENARIOS:
            with self.subTest(scenario=name), tempfile.TemporaryDirectory() as tmp:
                _, hashes = economic_scenarios.run(name, Path(tmp), legacy_snapshot=True)
                self.assertEqual(hashes, GOLDEN[name])

    def test_retries_reproduce_semantics_with_distinct_receipts(self):
        receipts = []
        for attempt in ("a" * 32, "b" * 32):
            with tempfile.TemporaryDirectory() as tmp:
                kwargs, drive = economic_scenarios.SCENARIOS["pairs_rich_known"]
                h = economic_scenarios.Harness(Path(tmp), attempt=attempt, **kwargs)
                drive(h)
                h.finish()
                manifest = (h.output / "manifest.json").read_bytes()
                receipts.append((manifest, json.loads((h.output / "content_receipt.json").read_bytes())))
        self.assertEqual(receipts[0][0], receipts[1][0])
        self.assertEqual(receipts[0][1]["semantic_sha256"], receipts[1][1]["semantic_sha256"])
        self.assertNotEqual(receipts[0][1]["attempt_id"], receipts[1][1]["attempt_id"])


if __name__ == "__main__":
    unittest.main()
