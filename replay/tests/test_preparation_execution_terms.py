"""Offline outcomes-v2 compatibility at the strict preparation boundary."""

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from replay.outcome_model import validate_document
from replay.preparation import encoded, load_snapshot, prepare
from replay.strategy_sdk import plain
from replay.streams.protocol import ProtocolError
from replay.tests.test_preparation import config, detail
from replay.tests.test_preparation_outcomes import document


def execution_document():
    result = document()
    result["version"] = 2
    for market in result["markets"]:
        market["execution_terms"] = {"status": "unavailable", "changes": []}
    result["markets"][-1]["execution_terms"] = {
        "status": "observed",
        "changes": [
            {"from": "1970-01-01T00:00:00.0000001Z", "taker_delay_ms": 1000},
            {"from": "1970-01-01T00:00:00.0000002Z", "taker_delay_ms": None},
            {"from": "1970-01-01T00:00:00.0000003Z", "taker_delay_ms": 0},
        ],
    }
    return result


def replace_outcomes(directory, outcomes):
    """Model an already committed producer snapshot without calling its reader."""
    context_path, receipt_path = directory / "context.json", directory / "receipt.json"
    context = json.loads(context_path.read_bytes())
    context["outcomes"]["document"] = outcomes
    payload = encoded(context)
    receipt = json.loads(receipt_path.read_bytes())
    receipt.update(snapshot_sha256=hashlib.sha256(payload).hexdigest(), snapshot_byte_length=len(payload))
    context_path.write_bytes(payload)
    receipt_path.write_bytes(encoded(receipt))
    return receipt["snapshot_sha256"]


class ExecutionTermsPreparationTest(unittest.TestCase):
    def test_accepts_valid_v2_outcomes(self):
        value = execution_document()
        self.assertIs(validate_document(value, "bundle-1"), value)

    def test_loads_an_existing_v2_context_without_rewriting(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary) / "context"
            prepare(config(), directory, universe=lambda *_: detail(), outcomes=lambda _: document())
            pin = replace_outcomes(directory, execution_document())
            before = {name: (directory / name).read_bytes() for name in ("context.json", "receipt.json")}
            snapshot = load_snapshot(directory, expected_sha256=pin)
            self.assertEqual(snapshot["outcomes"]["document"]["version"], 2)
            self.assertEqual(before, {name: (directory / name).read_bytes() for name in before})

    def test_prepares_v2_and_keeps_v1_bytes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for version, source in ((1, document), (2, execution_document)):
                with self.subTest(version=version):
                    directory = root / str(version)
                    snapshot = prepare(config(), directory, universe=lambda *_: detail(), outcomes=lambda _: source())
                    self.assertEqual(snapshot, load_snapshot(directory))
                    self.assertEqual(snapshot["outcomes"]["document"]["version"], version)
                    if version == 1:
                        self.assertNotIn("execution_terms", snapshot["outcomes"]["document"]["markets"][0])
                        self.assertEqual((directory / "context.json").read_bytes(),
                                         (json.dumps(plain(snapshot), sort_keys=True) + "\n").encode())

    def test_closed_terms_types_order_and_availability(self):
        mutations = [
            lambda d: d["markets"][0].pop("execution_terms"),
            lambda d: d["markets"][0]["execution_terms"].update(extra=True),
            lambda d: d["markets"][0]["execution_terms"].update(status="observed"),
            lambda d: d["markets"][-1]["execution_terms"].update(status="unavailable"),
            lambda d: d["markets"][-1]["execution_terms"]["changes"].reverse(),
            lambda d: d["markets"][-1]["execution_terms"]["changes"][1].update(taker_delay_ms=1000),
        ]
        for invalid in (True, -1, "1000", 1.5):
            mutations.append(lambda d, invalid=invalid: d["markets"][-1]["execution_terms"]["changes"][0].update(taker_delay_ms=invalid))
        for timestamp in ("2026-02-30T00:00:00Z", "1970-01-01T00:00:00+00:00", "1969-12-31T23:59:59Z", "9999-12-31T00:00:00Z", "1970-01-01T00:00:00.000000100Z"):
            mutations.append(lambda d, timestamp=timestamp: d["markets"][-1]["execution_terms"]["changes"][1].update(**{"from": timestamp}))
        mutations.append(lambda d: d["markets"][-1]["execution_terms"]["changes"][0].update(extra=True))
        for mutate in mutations:
            value = execution_document()
            mutate(value)
            with self.subTest(mutate=mutate), self.assertRaises(ProtocolError):
                validate_document(value, "bundle-1")
        value = document()
        value["markets"][0]["execution_terms"] = {"status": "unavailable", "changes": []}
        with self.assertRaises(ProtocolError):
            validate_document(value, "bundle-1")

    def test_rehashed_context_rejects_invalid_terms(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary) / "context"
            prepare(config(), directory, universe=lambda *_: detail(), outcomes=lambda _: document())
            value = execution_document()
            value["markets"][0]["execution_terms"]["extra"] = True
            pin = replace_outcomes(directory, value)
            with self.assertRaises(ProtocolError):
                load_snapshot(directory, expected_sha256=pin)
