"""Market profile: exact time partitions, histograms, incidents, gating, parity."""

import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from replay.strategies.bundle_coverage.strategy import Coverage
from replay.strategies.market_profile.strategy import MarketProfile, read_provisional, validate_content
from replay.preparation import load_snapshot
from replay.streams.protocol import Book, ProtocolError, freeze
from replay.tests.economic_scenarios import PROFILE_POLICY, M, ladder, operations, v2_policy
from replay.tests.test_bundle_coverage import Harness as CoverageHarness
from replay.tests.test_same_venue_complement import Harness as ComplementHarness


class _Tee:
    def __init__(self, *strategies):
        self.strategies = strategies

    def __call__(self, cut):
        for strategy in self.strategies:
            strategy(cut)

    def finish(self):
        for strategy in self.strategies:
            strategy.finish()


class Harness(CoverageHarness):
    """Coverage's hand-authored tape, with the profile (and optionally coverage) as groups."""

    def __init__(self, root, *, policy=None, with_coverage=False, **kwargs):
        self.profile_output = root / "profile"
        self.profile_output.mkdir()

        def factory(context):
            config = {**context["config"], "policy": policy or PROFILE_POLICY}
            self.profile = MarketProfile(freeze({**context, "config": config,
                                                 "output_directory": str(self.profile_output)}))
            if not with_coverage:
                return self.profile
            self.coverage = Coverage(context)
            return _Tee(self.coverage, self.profile)

        with patch("replay.tests.test_bundle_coverage.build", side_effect=factory):
            super().__init__(root, **kwargs)

    plan_index = ComplementHarness.plan_index

    def finish(self):
        self.terminal()
        self.decoder.finish()
        self.strategy.finish()
        return read_provisional(self.profile_output, self.root / "context", expected_sha256=self.sha)

    def records(self, name):
        return [json.loads(line) for line in (self.profile_output / name).read_bytes().splitlines()]


def book_rows(h, instrument, orientation="outcome"):
    return [r for r in h.records("profile.ndjson")
            if (r["instrument"], r["orientation"]) == (instrument, orientation)]


class ProfileTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def test_time_partitions_sum_to_each_clipped_bucket(self):
        h = Harness(self.root, mixed=True)
        h.window()
        ladder(h, 12, "polymarket:123", bids=((400, 2 * M),), asks=((450, M),))
        ladder(h, 16, "polymarket:123", bids=(), asks=((450, M),))
        ladder(h, 19, "polymarket:123", why={"kind": "connection_closed"})
        h.finish()
        rows = book_rows(h, "polymarket:123")
        self.assertEqual([(r["start_ns"], r["end_ns"]) for r in rows],
                         [("10", "14"), ("14", "21"), ("21", "28"), ("28", "35"), ("35", "40")])
        for row in rows:
            s = {k: int(v) for k, v in row["state"].items() if k != "unusable_ns_by_reason"}
            duration = int(row["end_ns"]) - int(row["start_ns"])
            self.assertEqual(s["usable_ns"] + s["not_initialized_ns"] + s["unusable_ns"], duration)
            self.assertEqual(s["two_sided_ns"] + s["bid_empty_ns"] + s["ask_empty_ns"] - s["both_empty_ns"],
                             s["usable_ns"])
        second = rows[1]["state"]
        self.assertEqual((second["two_sided_ns"], second["bid_empty_ns"], second["unusable_ns"]), ("2", "3", "2"))
        self.assertEqual(second["unusable_ns_by_reason"], {"connection_closed": "2"})
        self.assertEqual(rows[0]["state"]["not_initialized_ns"], "2")

    def test_spread_histogram_quantiles_and_integrals_are_exact(self):
        h = Harness(self.root)
        h.window()
        ladder(h, 10, "polymarket:123", bids=((400, M),), asks=((410, M),))   # spread 10 for 3 ns
        ladder(h, 13, "polymarket:123", bids=((400, M),), asks=((430, M),))   # spread 30 for 1 ns
        ladder(h, 14, "polymarket:123", bids=((400, 2 * M),), asks=((420, M),))
        h.finish()
        first, second = book_rows(h, "polymarket:123")[:2]
        top = first["top_of_book"]
        self.assertEqual(top["spread_histogram"], [["10", "3"], ["30", "1"]])
        self.assertEqual((top["spread_p50_atoms"], top["spread_p90_atoms"]), ("10", "30"))
        self.assertEqual(top["spread_atoms_ns"], str(10 * 3 + 30))
        self.assertEqual(top["mid2_atoms_ns"], str(810 * 3 + 830))
        self.assertEqual(top["open"], {"bid": ["400", "1000000"], "ask": ["410", "1000000"]})
        self.assertEqual(top["close"], {"bid": ["400", "1000000"], "ask": ["430", "1000000"]})
        self.assertEqual(second["top_of_book"]["bid_top_quantity_ns"], str(2 * M * 7))
        stability = second["quote_stability"]
        self.assertEqual(stability["ask"], [1, 0, 0, 0])  # 430 lasted 1 ns: below the 2 ns edge
        depth = first["depth"]["ask"]
        self.assertEqual(depth["filled_ns"], ["4", "0"])          # 1 contract filled, 3 never
        self.assertEqual(depth["depth_limited_ns"], ["0", "4"])

    def test_self_crossing_incident_rows_match_crossed_time(self):
        h = Harness(self.root, scopes=True)
        h.window()
        ladder(h, 12, "polymarket:123", bids=((450, M),), asks=((440, M),))
        ladder(h, 15, "polymarket:123", bids=((460, M),), asks=((440, 2 * M),))
        ladder(h, 17, "polymarket:123", bids=((440, M),), asks=((440, M),))
        ladder(h, 18, "polymarket:123", bids=((400, M),), asks=((440, M),))
        ladder(h, 20, "polymarket:123", bids=((500, M),), asks=((440, M),))
        h.finish()
        incidents = [(r["start_ns"], r["end_ns"], r["end_reason"], r["max_cross_atoms"], r["quotes_at_max"]["bid"])
                     for r in h.records("incidents.ndjson") if r["instrument"] == "polymarket:123"]
        self.assertEqual(incidents, [("12", "18", "UNCROSSED", "20", ["460", "1000000"]),
                                     ("20", "23", "SCOPE_END", "60", ["500", "1000000"]),
                                     ("23", "40", "RUN_END", "60", ["500", "1000000"])])
        rows = [r for r in book_rows(h, "polymarket:123") if r["scope"] == 0]
        crossed = sum(int(r["self_crossing"]["crossed_ns"]) for r in rows)
        locked = sum(int(r["self_crossing"]["locked_ns"]) for r in rows)
        self.assertEqual((crossed, locked), (5 + 3, 1))

    def test_kalshi_asks_are_the_labelled_complement_projection(self):
        h = Harness(self.root, mixed=True)
        h.window()
        ladder(h, 12, "kalshi:series", "outcome", bids=((560, 2 * M),))
        ladder(h, 12, "kalshi:series", "complement", bids=((400, M),))
        h.finish()
        row = book_rows(h, "kalshi:series")[0]
        self.assertEqual(row["ask_source"], "projected")
        self.assertEqual(row["top_of_book"]["close"], {"bid": ["560", "2000000"], "ask": ["600", "1000000"]})
        pair = [p for p in h.records("pair_profile.ndjson") if p["market_id"] == "kalshi:series"][0]
        self.assertEqual((pair["both_bids_ns"], pair["bid_sum_dev_ns"]), ("2", str((960 - 1000) * 2)))
        self.assertEqual(pair["bid_sum_above_unit_ns"], "0")
        self.assertEqual(book_rows(h, "polymarket:123")[0]["ask_source"], "native")

    def test_trade_dispositions_reproduce_coverage_totals(self):
        h = Harness(self.root, mixed=True, with_coverage=True)
        h.window()
        ladder(h, 11, "polymarket:123", bids=((380, M),), asks=((420, M),))
        h.group(12, trades=("applied", "observed", "duplicate"))
        h.group(14, trades=("not_authority", "invalidated", "applied"))
        result = h.finish()
        coverage = json.loads((h.output / "manifest.json").read_bytes())
        profile = {}
        for book in result["summary"]["books"]:
            for name, count in book["trades"].items():
                profile[name] = profile.get(name, 0) + count
        self.assertEqual(profile, coverage["trades"])
        activities = [r["activity"] for r in book_rows(h, "polymarket:123")]
        self.assertEqual([a["trades_priced"] for a in activities[:2]], [2, 3])  # buckets [10,14), [14,21)
        # Trades print at 400 against a prevailing 380/420 book: 2*400 - 800 = 0.
        self.assertEqual({a["trade_mid2_deviation_atoms"] for a in activities}, {"0"})
        self.assertEqual(activities[1]["aggressor"], {"none": 3})

    def test_disabled_groups_cost_nothing_and_are_absent(self):
        policy = {**PROFILE_POLICY, "groups": ["self_crossing"]}
        h = Harness(self.root, policy=policy)
        h.window()
        with patch.object(Book, "levels", side_effect=AssertionError("levels read")):
            ladder(h, 12, "polymarket:123", bids=((450, M),), asks=((440, M),))
            ladder(h, 14, "polymarket:123", bids=((400, M),), asks=((440, M),))
        h.terminal()  # the terminal book digest itself reads levels
        h.decoder.finish()
        h.strategy.finish()
        row = book_rows(h, "polymarket:123")[0]
        self.assertNotIn("top_of_book", row)
        self.assertNotIn("depth", row)
        self.assertNotIn("activity", row)
        self.assertEqual(row["self_crossing"], {"crossed_ns": "2", "locked_ns": "0"})
        self.assertEqual(h.records("pair_profile.ndjson"), [])

    def test_quantity_time_integrals_beyond_64_bits_are_exact(self):
        h = Harness(self.root)
        h.window()
        huge = 10**18  # quantity atoms: 30 ns of it exceeds 2**64
        ladder(h, 10, "polymarket:123", bids=((400, huge),), asks=((450, huge),))
        h.finish()
        total = sum(int(r["top_of_book"]["bid_top_quantity_ns"]) for r in book_rows(h, "polymarket:123"))
        self.assertEqual(total, huge * 30)
        self.assertGreater(total, 2**64)

    def test_s5_equal_final_state_keeps_intermediate_trades_depth_and_scope_open(self):
        from replay.tests.test_market_profile_v2 import v2
        from replay.tests.test_market_profile_transitions import PM, op, operations, send, trade_event

        h = Harness(self.root, policy=v2("availability", "levels", "transitions"), scopes=True)
        h.window()
        a = {"bids": ((400, 2 * M), (390, M)), "asks": ((450, M), (470, 2 * M))}
        ladder(h, 10, PM[0], **a)
        ladder(h, 11, PM[0], **a)  # equal snapshot must still write stream anchors
        ladder(h, 12, PM[0], bids=((410, 2 * M), (390, M)), asks=a["asks"])
        # The restoring cut's trade is priced against the pending B, not final A.
        from replay.tests.test_market_profile_transitions import snapshot
        send(h, 12, (PM, snapshot(a["bids"], a["asks"])),
             events=[trade_event(h, 12, PM, "applied", M)])
        send(h, 12, events=[trade_event(h, 12, PM, "observed", M)])
        # Same top and band quantities, different 3-contract slippage.
        send(h, 13, (PM, operations(op(PM, "ask", 470, "delete"), op(PM, "ask", 480, "set", 2 * M))))
        send(h, 14, (PM, operations(op(PM, "bid", 400, "set", 3 * M))))
        ladder(h, 26, PM[0], bids=((400, 3 * M), (390, M)), asks=((450, M), (480, 2 * M)))
        h.finish()
        first, second = book_rows(h, PM[0])[:2]
        self.assertEqual(first["top_of_book"]["mid2_atoms_ns"], str(850 * 4))
        self.assertEqual(first["top_of_book"]["bid_top_quantity_ns"], str(2 * M * 4))
        self.assertEqual(first["depth"]["ask"]["within_ticks_quantity_ns"], [str(M * 4), str(3 * M * 4)])
        self.assertEqual(first["depth"]["ask"]["slippage_cost_ns"], ["0", str(40 * M * 3 + 60 * M)])
        self.assertEqual(first["activity"]["trade_mid2_deviation_atoms"], "-110")
        self.assertEqual(first["activity"]["transitions"], 5)
        self.assertEqual(first["activity"]["snapshots"], 4)
        self.assertEqual(first["activity"]["operations"], 2)
        self.assertEqual(second["top_of_book"]["bid_top_quantity_ns"], str(3 * M * 7))
        opening = next(r for r in book_rows(h, PM[0]) if r["scope"] == 1)
        self.assertEqual(opening["top_of_book"]["open"]["bid"], ["400", str(3 * M)])

    def test_s5_depth_prefix_includes_band_edges_and_keeps_large_exact_integers(self):
        policy = {**PROFILE_POLICY, "depth_ticks": [str(k) for k in range(1, 9)]}
        h = Harness(self.root, policy=policy)
        h.window()
        huge = 10**18
        ladder(h, 10, "polymarket:123",
               bids=((400, huge), (391, 2 * M), (390, 3 * M), (389, 5 * M), (380, 7 * M),
                     (379, 11 * M), (320, 13 * M), (319, 17 * M)),
               asks=((450, 19 * M), (459, 23 * M), (460, 29 * M), (461, 31 * M), (470, 37 * M),
                     (471, 41 * M), (530, 43 * M), (531, 47 * M)))
        h.finish()
        rows = book_rows(h, "polymarket:123")
        for side, expected in (("bid", [huge + n * M for n in (5, 17, 28, 28, 28, 28, 28, 41)]),
                               ("ask", [n * M for n in (71, 139, 180, 180, 180, 180, 180, 223)])):
            totals = [sum(int(r["depth"][side]["within_ticks_quantity_ns"][i]) for r in rows)
                      for i in range(8)]
            self.assertEqual(totals, [q * 30 for q in expected])
        self.assertGreater(sum(int(r["depth"]["bid"]["within_ticks_quantity_ns"][0]) for r in rows), 2**64)

    def test_reader_rejects_a_rehashed_partition_gap(self):
        h = Harness(self.root)
        h.window()
        ladder(h, 12, "polymarket:123", bids=((400, M),), asks=((450, M),))
        h.finish()
        manifest = json.loads((h.profile_output / "manifest.json").read_bytes())
        snapshot = load_snapshot(self.root / "context", expected_sha256=h.sha)
        path = h.profile_output / "profile.ndjson"
        rows = [json.loads(line) for line in path.read_bytes().splitlines()]
        rows.pop(1)
        payload = b"".join(json.dumps(r, sort_keys=True, separators=(",", ":")).encode() + b"\n" for r in rows)
        path.write_bytes(payload)
        manifest["files"]["profile.ndjson"] = {"sha256": hashlib.sha256(payload).hexdigest(),
                                               "byte_length": len(payload), "records": len(rows)}
        with self.assertRaisesRegex(ProtocolError, "partition|close order"):
            validate_content(h.profile_output, snapshot, manifest)


class EmbeddedProfileTests(unittest.TestCase):
    def test_strategy_flag_turns_the_profile_on_and_off(self):
        for profile in (None, PROFILE_POLICY):
            with self.subTest(enabled=profile is not None), tempfile.TemporaryDirectory() as tmp:
                h = ComplementHarness(Path(tmp), policy=v2_policy(profile=copy.deepcopy(profile)))
                h.window(); h.quote(12, 0); h.quote(12, 1)
                operations(h, 13, "polymarket:123", "outcome", "ask", 470, M)
                result = h.finish()
                present = (h.output / "profile.ndjson").exists()
                self.assertEqual(present, profile is not None)
                self.assertEqual("profile" in result["summary"], profile is not None)
                if profile is not None:
                    self.assertEqual(set(result["manifest"]["files"]) >= {"profile.ndjson", "incidents.ndjson"}, True)


if __name__ == "__main__":
    unittest.main()
