import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from replay.complement_output import _quantiles, _signed, read_completed, validate_content
from replay.preparation import digest, load_snapshot
from replay.streams.protocol import ProtocolError
from replay.tests.test_same_venue_complement import Harness as ComplementHarness


class ComplementOutputPrimitiveTests(unittest.TestCase):
    """Independent arithmetic/schema checks; integration fixtures live with runtime."""

    def test_nearest_rank_quantiles_are_calculated_from_hand_authored_durations(self):
        # Sorted durations are 2, 5, 9, 12.  Nearest ranks are 2, 4, 4.
        self.assertEqual(
            _quantiles([12, 2, 9, 5]),
            {"p50": "5", "p90": "12", "p99": "12", "max": "12"},
        )
        self.assertEqual(
            _quantiles([]),
            {"p50": None, "p90": None, "p99": None, "max": None},
        )

    def test_signed_integers_are_canonical(self):
        self.assertEqual(_signed("0"), 0)
        self.assertEqual(_signed("-17"), -17)
        for value in ("", "+1", "01", "-0", "-01", " 1"):
            with self.subTest(value=value), self.assertRaises(ProtocolError):
                _signed(value)


class ComplementOutputCorruptionTests(unittest.TestCase):
    """Rehashed corruptions exercise semantics rather than file checksums."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def fixture(self):
        harness = ComplementHarness(self.root, known=True)
        for writer in harness.strategy.writers.values():
            self.addCleanup(writer.stream.close)
        harness.window(); harness.quote(12, 0); harness.quote(12, 1); harness.finish()
        snapshot = load_snapshot(self.root / "context", expected_sha256=harness.sha)
        manifest = json.loads((harness.output / "manifest.json").read_bytes())
        return harness, snapshot, manifest

    def rewrite(self, harness, manifest, name, mutate):
        path = harness.output / name
        rows = [json.loads(line) for line in path.read_bytes().splitlines()]
        mutate(rows)
        payload = b"".join(json.dumps(row, sort_keys=True, separators=(",", ":")).encode() + b"\n"
                           for row in rows)
        path.write_bytes(payload)
        manifest["files"][name] = {"sha256": hashlib.sha256(payload).hexdigest(),
                                    "byte_length": len(payload), "records": len(rows)}

    def test_rehashed_missing_measurement_and_wrong_parent_slice_are_rejected(self):
        h, snapshot, manifest = self.fixture()
        self.rewrite(h, manifest, "measurements.ndjson", lambda rows: rows.pop(0))
        with self.assertRaisesRegex(ProtocolError, "measurement|unknown scoped"):
            validate_content(h.output, snapshot, manifest)

        # Rebuild independently so the second corruption is not masked by the first.
        with tempfile.TemporaryDirectory() as tmp:
            h = ComplementHarness(Path(tmp), known=True)
            h.window(); h.quote(12, 0); h.quote(12, 1); h.finish()
            snapshot = load_snapshot(Path(tmp) / "context", expected_sha256=h.sha)
            manifest = json.loads((h.output / "manifest.json").read_bytes())
            def wrong_parent(rows):
                target = rows[0]
                target["entity"] = next(r["entity"] for r in rows
                                        if r["scope"] == target["scope"] and r["entity"] != target["entity"])
            self.rewrite(h, manifest, "slices.ndjson", wrong_parent)
            with self.assertRaisesRegex(ProtocolError, "parent scope/entity|slice/class|close order"):
                validate_content(h.output, snapshot, manifest)

    def test_rehashed_invalid_q_predicate_and_end_reason_are_rejected(self):
        mutations = [
            ("episodes.ndjson", lambda rows: rows[0]["qualified_ns"].update({"1": "999"}), "episode Q"),
            ("episodes.ndjson", lambda rows: rows[0].update({"end_reason": "PREDICATE_FALSE", "censored": False}), "end reason|censor"),
            ("slices.ndjson", lambda rows: rows[0]["open_values"].update({"gap_gross": "1"}), "arithmetic"),
        ]
        for name, mutation, error in mutations:
            with self.subTest(name=name, error=error), tempfile.TemporaryDirectory() as tmp:
                h = ComplementHarness(Path(tmp), known=True)
                h.window(); h.quote(12, 0); h.quote(12, 1); h.finish()
                snapshot = load_snapshot(Path(tmp) / "context", expected_sha256=h.sha)
                manifest = json.loads((h.output / "manifest.json").read_bytes())
                self.rewrite(h, manifest, name, mutation)
                with self.assertRaisesRegex(ProtocolError, error):
                    validate_content(h.output, snapshot, manifest)

    def test_reader_rejects_noncanonical_episode_and_slice_facts(self):
        def interior_start(rows):
            row = rows[0]
            row["start_ns"] = str(int(row["start_ns"]) + 1)
            row["gap_lifetime_ns"] = str(int(row["end_ns"]) - int(row["start_ns"]))
            row["episode_id"] = digest([row["scope"], row["entity"], row["kind"], row["start_ns"]])

        def wrong_fee_class(rows):
            row = next(r for r in rows if r["kind"] == "net")
            row["open_values"].update({"fee_status": "UNKNOWN", "gap_net": None,
                                       "assessments": [[], []]})

        mutations = [
            ("episodes.ndjson", interior_start, "invalid wire|aligned to measurement start"),
            ("episodes.ndjson", wrong_fee_class, "episode open fee class"),
            ("slices.ndjson", lambda rows: rows[0].update(kind="other"), "slice kind"),
            ("slices.ndjson", lambda rows: rows[0]["consumed"][0].append(["999", "1"]),
             "extra unconsumed level"),
        ]
        for name, mutation, error in mutations:
            with self.subTest(error=error), tempfile.TemporaryDirectory() as tmp:
                h = ComplementHarness(Path(tmp), known=True)
                h.window(); h.quote(12, 0); h.quote(12, 1); h.finish()
                snapshot = load_snapshot(Path(tmp) / "context", expected_sha256=h.sha)
                manifest = json.loads((h.output / "manifest.json").read_bytes())
                self.rewrite(h, manifest, name, mutation)
                with self.assertRaisesRegex(ProtocolError, error):
                    validate_content(h.output, snapshot, manifest)

    def test_rehashed_net_absence_cannot_leave_positive_gross_slices(self):
        h, snapshot, manifest = self.fixture()
        positive_entities = set()
        def erase_positive_measurements(rows):
            for row in rows:
                if row["value_class"] == "NET_POSITIVE":
                    positive_entities.add(row["entity"])
                    row["value_class"] = "NET_NONPOSITIVE"
        self.rewrite(h, manifest, "measurements.ndjson", erase_positive_measurements)
        self.rewrite(h, manifest, "episodes.ndjson",
                     lambda rows: rows.__setitem__(slice(None),
                         [row for row in rows if row["kind"] != "net"]))
        self.rewrite(h, manifest, "slices.ndjson",
                     lambda rows: rows.__setitem__(slice(None),
                         [row for row in rows if row["kind"] != "net"]))

        self.assertTrue(positive_entities)
        with self.assertRaisesRegex(ProtocolError, "slice/measurement eligibility"):
            validate_content(h.output, snapshot, manifest)

    def test_kalshi_projected_buy_slices_are_ascending_and_use_payout_minus_cost(self):
        h = ComplementHarness(self.root, mixed=True, known=True)
        for writer in h.strategy.writers.values():
            self.addCleanup(writer.stream.close)
        h.window()
        for index in (0, 1):
            plan = h.initial["plans"][index]
            transition = h.transition(plan, 12, None)
            transition["decision"] = {"kind": "snapshot",
                                      "bids": [["700", "2000000"], ["600", "5000000"]],
                                      "asks": []}
            ref = h.ref(12)
            h.send("cut", {"origin": {"kind": "group", "pin": h.pin,
                "first": ref["address"], "last": ref["address"], "visible_ns": "12"},
                "market_events": [], "book_transitions": [transition]})
        h.finish()
        descriptors = h.strategy.layout
        slices = [row for row in h.records("slices.ndjson")
                  if descriptors[row["entity"]]["venue"] == "kalshi"
                  and descriptors[row["entity"]]["size_contracts"] == "3"
                  and not descriptors[row["entity"]]["placebo"]]
        self.assertTrue(slices)
        self.assertEqual(slices[0]["consumed"][0], [["300", "2000000"], ["400", "5000000"]])
        self.assertEqual(slices[0]["open_values"]["gap_gross"], "1000000000")

    def test_completed_reader_requires_success_and_reader_budget_is_pre_growth(self):
        h, snapshot, manifest = self.fixture()
        with self.assertRaises(ProtocolError):
            read_completed(self.root, "coverage")
        with patch("replay.economic_sdk.bounds.MAX_STATE", 1):
            with self.assertRaisesRegex(ProtocolError, "reader state budget"):
                validate_content(h.output, snapshot, manifest)
            with self.assertRaisesRegex(ProtocolError, "quantile working copy"):
                _quantiles([1])


if __name__ == "__main__":
    unittest.main()
