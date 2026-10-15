"""Pinned delay observations; no live catalogue or retained user evidence."""

import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from replay.outcome_model import validate_document
from replay.preparation import encoded, load_snapshot, prepare
from replay.streams.protocol import ProtocolError
from replay.tests.test_preparation import config, detail
from replay.tests.test_preparation_outcomes import document


def delay_document():
    doc = document()
    doc["version"] = 2
    for market in doc["markets"]:
        market["execution_terms"] = {"status": "unavailable", "changes": []}
    doc["markets"][-1]["execution_terms"] = {
        "status": "observed",
        "changes": [
            {"from": "1970-01-01T00:00:00.0000001Z", "taker_delay_ms": 1000},
            {"from": "1970-01-01T00:00:00.0000002Z", "taker_delay_ms": None},
            {"from": "1970-01-01T00:00:00.0000003Z", "taker_delay_ms": 0},
        ],
    }
    return doc


class ExecutionTermsTests(unittest.TestCase):
    def test_version_two_reload_and_pure_book_mapping(self):
        from replay.economic_sdk import Strategy
        from replay.economic_sdk.execution_terms import execution_delays

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "context"
            policy = config()
            policy["end_ns"] = policy["occurrences"][0]["end_ns"] = "400"
            snapshot = prepare(policy, root, universe=lambda *_: detail(),
                               outcomes=lambda _: delay_document())
            self.assertEqual(snapshot, load_snapshot(root))
            self.assertEqual(snapshot["version"], 2)
            legs = (("polymarket:123", "outcome"),
                    ("polymarket:987", "outcome"), ("kalshi:series", "complement"))
            for time, expected in ((99, (None, None, None)),
                                   (100, (10**9, 10**9, None)),
                                   (200, (None, None, None)), (300, (0, 0, None))):
                self.assertEqual(execution_delays(snapshot, legs, time), expected)
            strategy = Strategy()
            strategy.snapshot = snapshot
            entity = type("Entity", (), {"legs": legs})()
            self.assertEqual(strategy.fill_delays(entity, 100), (10**9, 10**9, None))

    def test_v1_and_unavailable_are_unknown_without_rewriting(self):
        from replay.economic_sdk.execution_terms import execution_delays

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "context"
            original = document()
            snapshot = prepare(config(), root, universe=lambda *_: detail(),
                               outcomes=lambda _: original)
            self.assertEqual(snapshot["outcomes"]["document"]["version"], 1)
            self.assertNotIn("execution_terms", snapshot["outcomes"]["document"]["markets"][0])
            self.assertEqual(execution_delays(snapshot, (("polymarket:123", "outcome"),), 100), (None,))

    def test_v2_closed_types_order_and_collapsed_changes(self):
        validate_document(delay_document(), "bundle-1")
        mutations = [
            lambda d: d["markets"][0].pop("execution_terms"),
            lambda d: d["markets"][0]["execution_terms"].update(extra=1),
            lambda d: d["markets"][0]["execution_terms"].update(status="observed"),
            lambda d: d["markets"][-1]["execution_terms"].update(status="unavailable"),
            lambda d: d["markets"][-1]["execution_terms"]["changes"].reverse(),
            lambda d: d["markets"][-1]["execution_terms"]["changes"][1].update(taker_delay_ms=1000),
            lambda d: d["markets"][-1]["execution_terms"]["changes"][0].update(taker_delay_ms=True),
            lambda d: d["markets"][-1]["execution_terms"]["changes"][0].update(taker_delay_ms=-1),
            lambda d: d["markets"][-1]["execution_terms"]["changes"][0].update(taker_delay_ms="1000"),
            lambda d: d["markets"][-1]["execution_terms"]["changes"][0].update(taker_delay_ms=1.5),
            lambda d: d["markets"][-1]["execution_terms"]["changes"][0].update(extra=1),
            lambda d: d["markets"][-1]["execution_terms"]["changes"][0].update(**{"from": "2026-02-30T00:00:00Z"}),
            lambda d: d["markets"][-1]["execution_terms"]["changes"][0].update(**{"from": "1970-01-01T00:00:00+00:00"}),
            lambda d: d["markets"][-1]["execution_terms"]["changes"][1].update(**{"from": "1970-01-01T00:00:00.000000100Z"}),
        ]
        for mutate in mutations:
            doc = copy.deepcopy(delay_document())
            mutate(doc)
            with self.subTest(mutation=mutate), self.assertRaises(ProtocolError):
                validate_document(doc, "bundle-1")
        v1 = document()
        v1["markets"][0]["execution_terms"] = {"status": "unavailable", "changes": []}
        with self.assertRaises(ProtocolError):
            validate_document(v1, "bundle-1")

    def test_rehashed_snapshot_still_rejects_malformed_terms(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "context"
            prepare(config(), root, universe=lambda *_: detail(),
                    outcomes=lambda _: delay_document())
            context = json.loads((root / "context.json").read_bytes())
            context["outcomes"]["document"]["markets"][0]["execution_terms"]["extra"] = 1
            payload = encoded(context)
            receipt = json.loads((root / "receipt.json").read_bytes())
            receipt.update(snapshot_sha256=hashlib.sha256(payload).hexdigest(),
                           snapshot_byte_length=len(payload))
            (root / "context.json").write_bytes(payload)
            (root / "receipt.json").write_bytes(encoded(receipt))
            with self.assertRaises(ProtocolError):
                load_snapshot(root)
