"""Economic SDK runtime, controls, detail levels, bounds and reader behaviour."""

import dataclasses
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from replay.complement_output import _skew_artifact, validate_content
from replay.economic_fills import Fill, walk
from replay.economic_intervals import CutClock
from replay.economic_sdk import EVALUATED, Observation, bounds
from replay.economic_sdk.views import BookView, unavailable_view
from replay.preparation import load_snapshot
from replay.same_venue_complement import SameVenueComplement, build
from replay.streams.protocol import ProtocolError
from replay.tests.economic_scenarios import M, ladder, operations, v2_policy
from replay.tests.test_same_venue_complement import Harness


def deep_size(value, seen=None):
    """Real recursive retained size, used only to check the closed-form bounds."""
    seen = set() if seen is None else seen
    if id(value) in seen:
        return 0
    seen.add(id(value))
    size = sys.getsizeof(value)
    if isinstance(value, dict):
        size += sum(deep_size(k, seen) + deep_size(v, seen) for k, v in value.items())
    elif isinstance(value, (list, tuple, set, frozenset)):
        size += sum(deep_size(v, seen) for v in value)
    elif dataclasses.is_dataclass(value) or hasattr(value, "__slots__"):
        for cls in type(value).__mro__:
            for name in getattr(cls, "__slots__", ()):
                if hasattr(value, name):
                    size += deep_size(getattr(value, name), seen)
    return size


def rows(h, name):
    path = h.output / name
    return [json.loads(line) for line in path.read_bytes().splitlines()] if path.exists() else []


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def harness(self, **kwargs):
        h = Harness(self.root, **kwargs)
        for writer in h.strategy.writers.values():
            self.addCleanup(writer.stream.close)
        return h

    @staticmethod
    def entity(h, **match):
        return next(e for e in h.strategy.entities.values()
                    if all(e.descriptor.get(k) == v for k, v in match.items()))


class SharedViewTests(Base):
    def test_real_and_control_entities_share_one_detached_view_per_book(self):
        h = self.harness(pairs=True)
        h.window()
        for index, plan in enumerate(h.initial["plans"]):
            h.quote(12, index, ask=None if plan["venue"] == "kalshi" else 490)
        seen = {}
        original = SameVenueComplement.evaluate

        def record(strategy, entity, views, context):
            for key, view in zip(entity.legs, views):
                seen.setdefault(key, set()).add(id(view))
            return original(strategy, entity, views, context)

        with patch.object(SameVenueComplement, "evaluate", record):
            h.quote(13, h.plan_index("polymarket:123"), ask=480)
        self.assertEqual(seen[("polymarket:123", "outcome")], {id(h.strategy.views["polymarket:123", "outcome"])})
        self.assertIsInstance(h.strategy.views["polymarket:123", "outcome"], BookView)
        h.finish()

    def test_one_walk_per_changed_side_per_cut(self):
        h = self.harness(pairs=True)
        h.window()
        ladder(h, 12, "polymarket:123", bids=((400, 4 * M),), asks=((490, 4 * M),))
        calls = []

        def counting(levels, sizes):
            calls.append(levels)
            return walk(levels, sizes)

        with patch("replay.economic_sdk.views.walk", side_effect=counting):
            operations(h, 13, "polymarket:123", "outcome", "ask", 495, M)
            self.assertEqual(len(calls), 1)           # one touched side, one walk
            ladder(h, 14, "polymarket:123", bids=((400, M),), asks=((490, M),))
            self.assertEqual(len(calls), 3)           # snapshot: both sides
            ladder(h, 15, "kalshi:series", "outcome", bids=((400, M),))
            self.assertEqual(len(calls), 4)           # Kalshi requires bids only
        h.finish()

    def test_unchanged_declared_inputs_reuse_the_observation(self):
        h = self.harness(known=True)
        h.window()
        ladder(h, 12, "polymarket:123", bids=((400, 9 * M),), asks=((600, 9 * M),))
        ladder(h, 12, "polymarket:987", bids=((400, 9 * M),), asks=((600, 9 * M),))
        with patch.object(SameVenueComplement, "evaluate", wraps=h.strategy.strategy.evaluate) as evaluate:
            # A deep bid change leaves every short fill (and every long input) unchanged.
            operations(h, 14, "polymarket:123", "outcome", "bid", 100, M)
            self.assertEqual(evaluate.call_count, 0)
            operations(h, 15, "polymarket:123", "outcome", "bid", 401, M)
            self.assertGreater(evaluate.call_count, 0)
        h.finish()

    def test_predicates_contradicting_the_class_map_are_rejected(self):
        h = self.harness()
        h.window()
        wrong = Observation(EVALUATED, (), "GROSS_NONPOSITIVE", ("SKIPPED", None), frozenset({"gross"}))
        with patch.object(SameVenueComplement, "evaluate", return_value=wrong):
            with self.assertRaisesRegex(ProtocolError, "contradict"):
                h.quote(12, 0)

    def test_consumed_levels_per_slice_fail_closed(self):
        h = self.harness()
        h.window()
        with patch("replay.economic_sdk.bounds.MAX_CONSUMED_LEVELS", 0):
            h.quote(12, 0)
            with self.assertRaisesRegex(ProtocolError, "consumed levels per slice"):
                h.quote(12, 1)


class ClockTests(unittest.TestCase):
    def test_cut_clock_retains_only_bounded_window_fields(self):
        snapshot = {"config": {"start_ns": "10", "end_ns": "40", "lower_bound": "requested"},
                    "scopes": [{"end_ns": "40"}]}
        clock = CutClock(snapshot)

        class _Cut:
            kind = "cut"
            body = {"origin": {"kind": "window", "start_ns": "0", "end_ns": "40", "pin": "p",
                               "large_callback_owned_field": [object()] * 10_000}}

        clock.observe(_Cut())
        self.assertEqual(clock.window, ("0", "40", "p"))


class ControlTests(Base):
    def control_rows(self, h, entity):
        return [(r["start_ns"], r["end_ns"], r["status"], r["value_class"])
                for r in rows(h, "control_measurements.ndjson") if r["entity"] == entity]

    def test_time_shift_leg_changes_exactly_shift_later(self):
        h = self.harness(policy=v2_policy())
        h.window(); h.quote(12, 0); h.quote(12, 1)
        h.quote(20, 1, ask=700)
        control = self.entity(h, direction="long", size_contracts="1",
                              control={"kind": "time_shift", "leg": 1, "shift_ns": "5"})
        real = self.entity(h, direction="long", size_contracts="1", control=None)
        h.finish()
        self.assertEqual(
            [(s, e, st) for s, e, st, _ in self.control_rows(h, control.id)],
            [("10", "12", "UNUSABLE"), ("12", "17", "UNUSABLE"),
             ("17", "25", "DEPTH_SUFFICIENT"), ("25", "40", "DEPTH_SUFFICIENT")])
        self.assertEqual(self.control_rows(h, control.id)[2][3], "FEE_UNKNOWN")
        self.assertEqual(self.control_rows(h, control.id)[3][3], "GROSS_NONPOSITIVE")
        real_rows = [(r["start_ns"], r["end_ns"]) for r in rows(h, "measurements.ndjson")
                     if r["entity"] == real.id and r["status"] == "DEPTH_SUFFICIENT"]
        self.assertEqual(real_rows, [("12", "20"), ("20", "40")])

    def test_time_shift_history_without_a_staged_entity_keeps_its_exact_time(self):
        h = self.harness(policy=v2_policy(controls=[{"kind": "time_shift", "shift_ns": ["5"]}]))
        h.window(); h.quote(12, 0); h.quote(12, 1)
        # A deep change stages no entity, but the ring still records time 14.
        operations(h, 14, "polymarket:987", "outcome", "bid", 100, M)
        h.group(30)
        control = self.entity(h, direction="short", size_contracts="1",
                              control={"kind": "time_shift", "leg": 1, "shift_ns": "5"})
        self.assertEqual(h.strategy.rings["polymarket:987", "outcome"].times[-1], 14)
        h.finish()
        self.assertTrue(self.control_rows(h, control.id))

    def test_time_shift_ring_bound_fails_closed(self):
        h = self.harness(policy=v2_policy(time_shift_ring_entries="2"))
        h.window(); h.quote(12, 0); h.quote(12, 1)
        h.quote(13, 1, ask=480)
        with self.assertRaisesRegex(ProtocolError, "time_shift ring bound"):
            h.quote(14, 1, ask=470)

    def test_controls_and_real_rows_never_share_a_file(self):
        h = self.harness(pairs=True, known=True, policy=v2_policy(
            controls=[{"kind": "cyclic_neighbor"}, {"kind": "time_shift", "shift_ns": ["2", "5"]}]))
        h.window()
        for index, plan in enumerate(h.initial["plans"]):
            h.quote(12, index, ask=None if plan["venue"] == "kalshi" else 490,
                    bid=600 if plan["venue"] == "kalshi" else 400)
        h.finish()
        real = {e.id for e in h.strategy.entities.values() if e.cls == "real"}
        control = {e.id for e in h.strategy.entities.values() if e.cls == "control"}
        self.assertTrue(rows(h, "measurements.ndjson") and rows(h, "control_measurements.ndjson"))
        self.assertTrue({r["entity"] for r in rows(h, "measurements.ndjson")} <= real)
        self.assertTrue({r["entity"] for r in rows(h, "control_measurements.ndjson")} <= control)
        self.assertFalse((h.output / "control_episodes.ndjson").exists())
        cyclic = [e.descriptor for e in h.strategy.entities.values()
                  if e.cls == "control" and e.descriptor["control"]["kind"] == "cyclic_neighbor"
                  and e.admission is None]
        self.assertTrue(cyclic and all(d["control"]["replaced_leg"] for d in cyclic))
        summary = json.loads((h.output / "summary.json").read_bytes())
        labels = {row["control"] for row in summary["rows"]}
        self.assertEqual(labels, {None, "cyclic_neighbor", "time_shift:2", "time_shift:5"})


class DetailTests(Base):
    def snapshot(self, h):
        return load_snapshot(self.root / "context", expected_sha256=h.sha)

    def test_episode_detail_writes_compact_slices_and_opening_quotes(self):
        h = self.harness(known=True, policy=v2_policy())
        h.window(); h.quote(12, 0); h.quote(12, 1); h.quote(13, 0, ask=480)
        result = h.finish()
        slices, episodes = rows(h, "slices.ndjson"), rows(h, "episodes.ndjson")
        self.assertTrue(slices and episodes)
        self.assertTrue(all(set(r) == {"episode_id", "start_ns", "end_ns", "end_reason", "censored"}
                            for r in slices))
        self.assertTrue(all("open_quotes" in r for r in episodes))
        net = [r for r in episodes if r["kind"] == "net"
               and h.strategy.layout[r["entity"]]["size_contracts"] == "1"
               and h.strategy.layout[r["entity"]]["direction"] == "long"][0]
        self.assertEqual(net["qualified_ns"], {"1": "26", "5": "22", "10": "17"})
        self.assertEqual(result["summary"]["verdicts"][0]["qualified_ns"], "22")

    def test_reader_rejects_a_moved_compact_slice_and_a_bad_opening_quote(self):
        h = self.harness(known=True, policy=v2_policy())
        h.window(); h.quote(12, 0); h.quote(12, 1); h.quote(13, 0, ask=480)
        h.finish()
        manifest = json.loads((h.output / "manifest.json").read_bytes())
        del manifest["summary_sha256"]
        snapshot = self.snapshot(h)
        from replay.tests.test_complement_output import ComplementOutputCorruptionTests as C
        rewrite = C.rewrite.__get__(self)
        rewrite(h, manifest, "slices.ndjson",
                lambda rs: rs[0].update(end_ns=str(int(rs[0]["end_ns"]) + 1)))
        with self.assertRaises(ProtocolError):
            validate_content(h.output, snapshot, manifest)

    def test_intervals_detail_controls_report_time_fractions_only(self):
        h = self.harness(known=True, policy=v2_policy())
        h.window(); h.quote(12, 0); h.quote(12, 1)
        result = h.finish()
        controls = [r for r in result["summary"]["rows"] if r["control"] == "time_shift:5"]
        self.assertTrue(controls)
        self.assertTrue(all(r["episode_count"] == {"gross": 0, "net": 0} for r in controls))
        self.assertTrue(any(int(r["evaluated_ns"]) > 0 for r in controls))


class ComplementV2Tests(Base):
    def test_self_crossed_leg_is_a_diagnostic_status(self):
        h = self.harness(policy=v2_policy())
        h.window(); h.quote(12, 0, bid=600, ask=490); h.quote(12, 1)
        long = self.entity(h, direction="long", size_contracts="1", control=None)
        h.finish()
        statuses = [(r["status"], r["reasons"]) for r in rows(h, "measurements.ndjson")
                    if r["entity"] == long.id and r["start_ns"] == "12"]
        self.assertEqual(statuses, [("SELF_CROSSED_LEG", ["leg:0"])])

    def test_unusable_reasons_are_structured_json_text(self):
        h = self.harness(policy=v2_policy())
        h.window(); h.quote(12, 0); h.quote(12, 1)
        h.quote(20, 1, why={"kind": "connection_closed"})
        h.finish()
        reasons = [reason for r in rows(h, "measurements.ndjson") if r["status"] == "UNUSABLE"
                   for reason in r["reasons"]]
        self.assertIn('leg:1:unusable:{"kind":"connection_closed"}', reasons)
        self.assertFalse(any("b'" in reason for reason in reasons))

    def test_verdict_counts_only_unknown_fees_inside_qualifying_gross_slices(self):
        for policy, expected in ((None, "INCONCLUSIVE_FIXTURE"),
                                 (v2_policy(), "INTRA_INSTRUMENT_GAPS_ABSENT_IN_FIXTURE")):
            with self.subTest(version=1 if policy is None else 2), tempfile.TemporaryDirectory() as tmp:
                h = Harness(Path(tmp), policy=policy)
                h.window(); h.quote(12, 0); h.quote(12, 1)
                h.quote(14, 1, ask=700)  # a 2 ns positive: shorter than the 5 ns headline
                verdict = h.finish()["summary"]["verdicts"][0]
                self.assertEqual(verdict["verdict"], expected)
                if policy is not None:
                    self.assertEqual(verdict["fee_unknown_ns"], "0")
                    self.assertEqual(verdict["fee_unknown_total_ns"], "2")

    def test_qualifying_unknown_slice_stays_inconclusive(self):
        h = self.harness(policy=v2_policy())
        h.window(); h.quote(12, 0); h.quote(12, 1); h.quote(20, 1, ask=700)
        verdict = h.finish()["summary"]["verdicts"][0]
        self.assertEqual((verdict["verdict"], verdict["reason"], verdict["fee_unknown_ns"]),
                         ("INCONCLUSIVE_FIXTURE", "UNRESOLVED_POSITIVE_GROSS", "8"))

    def test_skew_artifact_label_requires_positive_time_only_at_or_above_one_second(self):
        policy = {"leg_skew_buckets_ns": ["100000000", "1000000000", "5000000000"]}
        positive = lambda bucket: {"skew_bucket": bucket, "gross_positive_ns": "7"}
        self.assertTrue(_skew_artifact([positive("2"), positive("3")], policy))
        self.assertFalse(_skew_artifact([positive("0"), positive("2")], policy))
        self.assertFalse(_skew_artifact([positive("1")], policy))
        self.assertFalse(_skew_artifact([{"skew_bucket": "2", "gross_positive_ns": "0"}], policy))


class BoundTests(unittest.TestCase):
    """Closed-form costs must not undercount a real recursive traversal."""

    def fill(self, levels):
        consumed = tuple((10**6 + i, 10**12 + i) for i in range(levels))
        return Fill(10**15, 10**30, False, consumed, consumed)

    def test_view_cost_is_conservative(self):
        sizes = 16
        fills = {side: {s: self.fill(bounds.MAX_CONSUMED_LEVELS // sizes) for s in range(sizes)}
                 for side in ("bid", "ask")}
        transformed = {"kalshi_complement_ask": {s: self.fill(bounds.MAX_CONSUMED_LEVELS // sizes)
                                                 for s in range(sizes)}}
        levels = sum(len(f.taken) + len(f.consumed) for d in (*fills.values(), *transformed.values())
                     for f in d.values())
        view = BookView("unusable", {"kind": "lane_invalid", "detail": "x" * 200}, 10**18, True, True,
                        (10**6, 10**12), (10**6, 10**12), fills, transformed, levels)
        self.assertGreaterEqual(bounds.view_cost(view, sizes), deep_size(view))
        self.assertGreaterEqual(bounds.view_cost(unavailable_view(0), 1), deep_size(unavailable_view(0)))

    def test_observation_episode_and_fingerprint_costs_are_conservative(self):
        payload = {"gap_gross": "9" * 40, "gap_net": "-" + "9" * 40, "gross_scale": 9, "net_scale": 18,
                   "fee_status": "KNOWN", "assessments": [["a" * 64] * 8, ["b" * 64] * 8],
                   "assumptions": ["PER_LEVEL_DECLARED_PARTITION_ESTIMATE"] * 3, "evidence": ["x" * 40] * 3}
        quotes = tuple(tuple((10**6 + i, 10**12 + i) for i in range(64)) for _ in range(2))
        observation = Observation(EVALUATED, tuple("reason-" + "r" * 60 for _ in range(6)),
                                  "NET_POSITIVE", ("KNOWN", None), frozenset({"gross", "net"}),
                                  payload, quotes)
        self.assertGreaterEqual(bounds.observation_cost(observation), deep_size(observation))
        self.assertGreaterEqual(bounds.episode_cost(observation, 8),
                                deep_size([payload, quotes, {str(t): 10**18 for t in range(8)}]))
        fingerprint = (((self.fill(32), True), (self.fill(32), False), ((10**6, 10**12), None)),
                       ("unusable", {"kind": "lane_invalid", "detail": "y" * 100}))
        self.assertGreaterEqual(bounds.fingerprint_cost(fingerprint), deep_size(fingerprint))

    def test_json_cost_is_conservative_for_snapshot_shaped_values(self):
        value = {"plans": [{"instrument": "polymarket:" + "1" * 70, "orientation": "outcome",
                            "price_scale": "4", "quantity_scale": "6"}] * 50,
                 "scopes": [{"members": [{"market_id": "m" * 40, "books": [], "capture_selected": True}] * 20}]}
        self.assertGreaterEqual(bounds.json_cost(value), deep_size(value))

    def test_state_budget_fails_before_retention(self):
        budget = bounds.StateBudget(limit=100)
        budget.charge(60)
        with self.assertRaisesRegex(ProtocolError, "detached state budget"):
            budget.charge(41)
        self.assertEqual(budget.used, 60)


class ProfilingHarnessTests(unittest.TestCase):
    def test_profiled_factory_dumps_callbacks_without_changing_output(self):
        from replay.economic_sdk.profiling import profiled
        from replay.tests import economic_scenarios
        golden = json.loads((Path(__file__).parent / "fixtures" / "complement_v1_golden.json").read_text())
        with tempfile.TemporaryDirectory() as tmp:
            dumps = Path(tmp) / "profile"
            with patch("replay.tests.test_same_venue_complement.build",
                       side_effect=profiled(build, dumps, every=5)):
                _, hashes = economic_scenarios.run("single_known_slices", Path(tmp) / "run")
            self.assertTrue((dumps / "callbacks.prof").is_file())
            self.assertEqual(hashes, golden["single_known_slices"])


if __name__ == "__main__":
    unittest.main()
