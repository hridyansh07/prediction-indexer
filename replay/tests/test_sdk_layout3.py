"""Layout-3 falsifiers: many scopes, deduplication and closed table identities."""
import copy
import hashlib
import tempfile
import unittest
from pathlib import Path

from replay.preparation import encoded
from replay.streams.protocol import ProtocolError
from replay.tests.test_cross_venue_metadata import WideHarness
from replay.tests.economic_scenarios import ladder
from replay.strategies.cross_venue_arbitrage.output import validate_content


class Layout3Tests(unittest.TestCase):
    def test_layout2_and3_have_identical_streams_for_the_same_input_with_controls(self):
        streams = []
        for layout in (2, 3):
            with tempfile.TemporaryDirectory() as temporary:
                h = WideHarness(Path(temporary), scope_count=3, controls=True, layout=layout)
                try:
                    h.populate()
                    h.finish()
                    streams.append({str(path.relative_to(h.output)): path.read_bytes()
                                    for path in h.output.rglob("*.ndjson")
                                    if path.name not in ("entities.ndjson", "descriptors.ndjson", "reasons.ndjson")})
                finally:
                    h.close()
        self.assertEqual(streams[0], streams[1])

    def test_row_and_line_caps_fail_before_any_output_is_opened(self):
        from unittest.mock import patch
        from replay.economic_sdk import bounds
        from replay.economic_sdk.entity_tables import preflight
        from replay.economic_sdk.runtime import Runtime
        with tempfile.TemporaryDirectory() as temporary:
            h = WideHarness(Path(temporary), scope_count=2)
            try:
                output = Path(temporary) / "empty-output"
                output.mkdir()
                for limit in (1, 341):
                    with patch.object(bounds, "MAX_ROWS", limit):
                        with self.assertRaisesRegex(ProtocolError, "row budget"):
                            Runtime(h.strategy.strategy, {**h.context, "output_directory": str(output)})
                        self.assertEqual(list(output.iterdir()), [])
                with patch.object(bounds, "MAX_ROWS", 342):
                    preflight(h.strategy.strategy)
                with patch.object(bounds, "MAX_LINE", 64):
                    with self.assertRaisesRegex(ProtocolError, "line budget"):
                        Runtime(h.strategy.strategy, {**h.context, "output_directory": str(output)})
                    self.assertEqual(list(output.iterdir()), [])
            finally:
                h.close()

    def test_64_scopes_keep_shared_semantics_and_store_descriptors_once(self):
        results = []
        for count in (64, 15):
            with tempfile.TemporaryDirectory() as temporary:
                h = WideHarness(Path(temporary), scope_count=count)
                try:
                    h.populate()
                    # End positive time before the smaller run's terminal scope,
                    # whose RUN_END censoring intentionally differs from SCOPE_END.
                    for plan in h.initial["plans"]:
                        ladder(h, 38, plan["instrument"], plan["orientation"],
                               why={"kind": "connection_closed"})
                    result = h.finish()
                    self.assertEqual(result["manifest"]["layout"], 3)
                    descriptors = h.records("descriptors.ndjson")
                    indexes = h.records("entities.ndjson")
                    self.assertEqual([d["hash"] for d in descriptors],
                                     sorted({d["hash"] for d in descriptors}))
                    self.assertEqual(len(descriptors), 171)
                    self.assertEqual(len(indexes), count * 171)
                    self.assertEqual([(r["scope"], r["entity"]) for r in indexes],
                                     [(s, i) for s in range(count) for i in range(171)])
                    self.assertFalse((h.output / "entities.json").exists())
                    shared = {name: [r for r in h.records(name) if r["scope"] < 15]
                              for name in ("denominators.ndjson", "episodes.ndjson")}
                    # Fee assessment IDs bind the entire snapshot/experiment.
                    # Different scope counts change those IDs, not the amounts.
                    for row in shared["episodes.ndjson"]:
                        for values in (row["open"]["values"], row["at_max"]["values"]):
                            for leg in values["native_legs"]:
                                leg.pop("assessment_ids")
                    results.append(shared)
                finally:
                    h.close()
        for name in results[0]:
            self.assertEqual(hashlib.sha256(encoded(results[0][name])).hexdigest(),
                             hashlib.sha256(encoded(results[1][name])).hexdigest(), name)

    def test_rehashed_descriptor_and_unknown_layout_are_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            h = WideHarness(Path(temporary), scope_count=2)
            try:
                h.populate()
                result = h.finish()
                bad = copy.deepcopy(result["manifest"])
                bad["layout"] = 4
                with self.assertRaisesRegex(ProtocolError, "layout"):
                    validate_content(h.output, h.snapshot, bad)
                rows = h.records("descriptors.ndjson")
                rows[0]["descriptor"]["market_id"] = "tampered"
                payload = b"".join(encoded(row) + b"\n" for row in rows)
                (h.output / "descriptors.ndjson").write_bytes(payload)
                bad = copy.deepcopy(result["manifest"])
                bad["files"]["descriptors.ndjson"] = {
                    "sha256": hashlib.sha256(payload).hexdigest(),
                    "byte_length": len(payload), "records": len(rows)}
                with self.assertRaisesRegex(ProtocolError, "descriptor"):
                    validate_content(h.output, h.snapshot, bad)
            finally:
                h.close()
