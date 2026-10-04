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
            self.assertEqual(len(calls), 0)           # beyond every consumed ask level: no read
            operations(h, 13, "polymarket:123", "outcome", "ask", 490, 2 * M)
            self.assertEqual(len(calls), 1)           # one touched side, one walk (bids untouched)
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


def table(h, name):
    return json.loads((h.output / name).read_bytes())


def denominators(h, group=""):
    return {(r["scope"], r["entity"]): r for r in rows(h, group + "denominators.ndjson")}


def entity_index(h, group="", scope=0, **match):
    entities = table(h, group + "entities.json")["scopes"][scope]
    return next(i for i, e in enumerate(entities)
                if all(e["descriptor"].get(k) == v for k, v in match.items()))


class AggregateOutputTests(Base):
    """Layout 2: denominators, real episodes, tables, opt-in controls and audit."""

    def test_denominators_sum_to_scope_length_and_split_sufficient_time_by_class(self):
        h = self.harness(scopes=True, policy=v2_policy())
        h.window(); h.quote(12, 0); h.quote(12, 1)
        h.quote(20, 1, why={"kind": "connection_closed"}); h.quote(26, 1)
        h.finish()
        snapshot = h.snapshot
        for (scope, _), row in denominators(h).items():
            length = int(snapshot["scopes"][scope]["end_ns"]) - int(snapshot["scopes"][scope]["start_ns"])
            self.assertEqual(sum(int(v) for v in row["status_ns"].values()), length)
            self.assertEqual(sum(int(v) for v in row.get("class_ns", {}).values()),
                             int(row["status_ns"].get("DEPTH_SUFFICIENT", "0")))
        long = entity_index(h, scope=0, direction="long", size_contracts="1")
        row = denominators(h)[0, long]
        reasons = table(h, "reasons.json")["reasons"]
        unusable = {tuple(i): v for s, i, v in row["reason_ns"] if s == "UNUSABLE"}
        closed = reasons.index({"kind": "connection_closed", "leg": 1, "validity": "unusable"})
        self.assertEqual(unusable[(closed,)], "3")  # 20..23, then scope 1 takes over

    def test_episodes_are_contained_in_and_exactly_cover_positive_class_time(self):
        h = self.harness(known=True, policy=v2_policy())
        h.window(); h.quote(12, 0); h.quote(12, 1); h.quote(13, 0, ask=480); h.quote(30, 0, ask=700)
        h.finish()
        dens = denominators(h)
        for episode in rows(h, "episodes.ndjson"):
            row = dens[episode["scope"], episode["entity"]]
            self.assertIn("DEPTH_SUFFICIENT", row["status_ns"])
            for name, amount in episode["class_ns"].items():
                self.assertLessEqual(int(amount), int(row["class_ns"][name]))
        long = entity_index(h, direction="long", size_contracts="1")
        net = [e for e in rows(h, "episodes.ndjson") if e["entity"] == long and e["kind"] == "net"]
        self.assertEqual([(e["start_ns"], e["end_ns"]) for e in net], [("12", "30")])
        self.assertEqual(dens[0, long]["class_ns"]["NET_POSITIVE"], "18")

    def test_reader_rejects_an_episode_outside_positive_time(self):
        h = self.harness(known=True, policy=v2_policy())
        h.window(); h.quote(12, 0); h.quote(12, 1); h.quote(30, 0, ask=700)
        h.finish()
        manifest = json.loads((h.output / "manifest.json").read_bytes())
        del manifest["summary_sha256"]
        snapshot = load_snapshot(self.root / "context", expected_sha256=h.sha)
        from replay.tests.test_complement_output import ComplementOutputCorruptionTests as C
        rewrite = C.rewrite.__get__(self)

        def lengthen(rs):
            r = rs[0]
            r["end_ns"] = str(int(r["end_ns"]) + 1)
            r["class_ns"] = {k: str(int(v) + 1) for k, v in r["class_ns"].items()}
        rewrite(h, manifest, "episodes.ndjson", lengthen)
        with self.assertRaisesRegex(ProtocolError, "episode|slice|class"):
            validate_content(h.output, snapshot, manifest)

    def test_entity_and_reason_tables_round_trip(self):
        h = self.harness(pairs=True, policy=v2_policy())
        h.window(); h.quote(12, 0, why={"kind": "connection_closed"})
        h.finish()
        entities = table(h, "entities.json")["scopes"][0]
        expected = sorted(h.strategy.entities.values(), key=lambda e: e.order)
        self.assertEqual(entities, [{"hash": e.id, "descriptor": e.descriptor} for e in expected])
        self.assertNotIn("control", entities[0]["descriptor"])  # no always-null fields
        reasons = table(h, "reasons.json")["reasons"]
        self.assertTrue(all(type(r) is dict for r in reasons))
        indexes = {i for r in rows(h, "denominators.ndjson") for _, ix, _ in r.get("reason_ns", [])
                   for i in ix}
        self.assertEqual(indexes, set(range(len(reasons))))
        text = (h.output / "denominators.ndjson").read_text()
        self.assertNotIn("experiment_sha256", text)
        self.assertNotIn("b'", text + (h.output / "reasons.json").read_text())
        manifest = json.loads((h.output / "manifest.json").read_bytes())
        snapshot = load_snapshot(self.root / "context", expected_sha256=h.sha)
        stored = table(h, "entities.json")
        stored["scopes"][0][0]["descriptor"]["market_id"] = "tampered"
        payload = json.dumps(stored, sort_keys=True, separators=(",", ":")).encode() + b"\n"
        (h.output / "entities.json").write_bytes(payload)
        import hashlib
        manifest["files"]["entities.json"] = {"sha256": hashlib.sha256(payload).hexdigest(),
                                              "byte_length": len(payload), "records": 1}
        del manifest["summary_sha256"]
        with self.assertRaisesRegex(ProtocolError, "entity table"):
            validate_content(h.output, snapshot, manifest)

    def test_compact_slices_partition_episodes_and_carry_open_skew(self):
        h = self.harness(known=True, policy=v2_policy())
        h.window(); h.quote(12, 0); h.quote(12, 1); h.quote(13, 0, ask=480)
        # An unconsumed change on leg 1 moves its last change (skew 1 -> 7) mid-slice.
        operations(h, 20, "polymarket:987", "outcome", "bid", 100, M)
        result = h.finish()
        slices = rows(h, "slices.ndjson")
        self.assertTrue(slices)
        self.assertTrue(all(set(r) == {"episode_id", "start_ns", "end_ns", "end_reason", "censored",
                                       "leg_skew_ns", "skew_bucket"} for r in slices))
        long = entity_index(h, direction="long", size_contracts="1")
        net = [e for e in rows(h, "episodes.ndjson") if e["entity"] == long and e["kind"] == "net"][0]
        self.assertEqual(net["qualified_ns"], {"1": "26", "5": "22", "10": "17"})
        # Entry time follows the skew in force at each entry instant.
        self.assertEqual({t: sum(int(v) for v in m.values()) for t, m in net["qualified_by_skew_ns"].items()},
                         {"1": 26, "5": 22, "10": 17})
        self.assertEqual(net["qualified_by_skew_ns"], {"1": {"1": "7", "2": "19"},
                                                       "5": {"1": "7", "2": "15"},
                                                       "10": {"1": "7", "2": "10"}})
        self.assertEqual([(r["start_ns"], r["skew_bucket"]) for r in slices
                          if r["episode_id"] == net["episode_id"]], [("12", 0), ("13", 1)])
        self.assertEqual(result["summary"]["verdicts"][0]["qualified_ns"], "22")

    def test_audit_intervals_reproduce_the_v1_partition_without_skew_splits(self):
        def partition(h, path, version):
            descriptors = ({e.id: e.descriptor for e in h.strategy.entities.values()} if version == 1
                           else None)
            entities = None if version == 1 else table(h, "entities.json")["scopes"][0]
            merged = {}
            for r in rows(h, path):
                d = descriptors[r["entity"]] if version == 1 else entities[r["entity"]]["descriptor"]
                if version == 1 and d["placebo"]:
                    continue
                key = (d["market_id"], d["direction"], d["size_contracts"])
                runs = merged.setdefault(key, [])
                value = (r["status"], r["value_class"] if version == 1 else r.get("value_class"))
                if runs and runs[-1][2] == value and runs[-1][1] == r["start_ns"]:
                    runs[-1] = (runs[-1][0], r["end_ns"], value)
                else:
                    runs.append((r["start_ns"], r["end_ns"], value))
            return merged

        results = []
        for policy in (None, v2_policy(audit_intervals=True)):
            with tempfile.TemporaryDirectory() as tmp:
                h = Harness(Path(tmp), known=True, policy=policy)
                from replay.tests.economic_scenarios import scenario_single_known_slices
                scenario_single_known_slices(h)
                h.finish()
                results.append(partition(h, "measurements.ndjson" if policy is None
                                         else "audit/measurements.ndjson", 1 if policy is None else 2))
        self.assertEqual(results[0], results[1])

    def test_control_files_appear_only_when_enabled(self):
        for controls, episodes in (([], False), ([{"kind": "time_shift", "shift_ns": ["5"]}], False),
                                   ([{"kind": "time_shift", "shift_ns": ["5"]}], True)):
            with self.subTest(controls=bool(controls), episodes=episodes), tempfile.TemporaryDirectory() as tmp:
                h = Harness(Path(tmp), policy=v2_policy(controls=controls, controls_episodes=episodes))
                h.window(); h.quote(12, 0); h.quote(12, 1)
                result = h.finish()
                files = {str(p.relative_to(h.output)) for p in h.output.rglob("*") if p.is_file()}
                control = {f for f in files if f.startswith("controls/")}
                if not controls:
                    self.assertEqual(control, set())
                    self.assertNotIn("controls", result["summary"])
                else:
                    expected = {"controls/time_shift_5/entities.json",
                                "controls/time_shift_5/denominators.ndjson",
                                "controls/time_shift_5/summary.json"}
                    if episodes:
                        expected.add("controls/time_shift_5/episodes.ndjson")
                    self.assertEqual(control, expected)
                    real = {r["entity"] for r in rows(h, "denominators.ndjson")}
                    self.assertEqual(len(real), len(table(h, "entities.json")["scopes"][0]))
                    self.assertEqual(json.loads((h.output / "controls/time_shift_5/summary.json").read_bytes()),
                                     result["summary"]["controls"]["time_shift_5"])
                self.assertFalse(any(f.startswith("audit/") for f in files))


class ControlTests(Base):
    def control_denominator(self, h, name, **match):
        index = entity_index(h, f"controls/{name}/", **match)
        return denominators(h, f"controls/{name}/")[0, index]

    def test_time_shift_leg_changes_exactly_shift_later(self):
        h = self.harness(policy=v2_policy(controls=[{"kind": "time_shift", "shift_ns": ["5"]}],
                                          controls_episodes=True, controls_slices=True))
        h.window(); h.quote(12, 0); h.quote(12, 1)
        h.quote(20, 1, ask=700)
        h.finish()
        row = self.control_denominator(h, "time_shift_5", direction="long", size_contracts="1")
        # [10,17) UNUSABLE (shifted leg before history), [17,25) positive, [25,40) nonpositive.
        self.assertEqual(row["status_ns"], {"DEPTH_SUFFICIENT": "23", "UNUSABLE": "7"})
        self.assertEqual(row["class_ns"], {"FEE_UNKNOWN": "8", "GROSS_NONPOSITIVE": "15"})
        index = entity_index(h, "controls/time_shift_5/", direction="long", size_contracts="1")
        episodes = [(e["start_ns"], e["end_ns"]) for e in rows(h, "controls/time_shift_5/episodes.ndjson")
                    if e["entity"] == index]
        self.assertEqual(episodes, [("17", "25")])
        real = denominators(h)[0, entity_index(h, direction="long", size_contracts="1")]
        self.assertEqual(real["class_ns"], {"FEE_UNKNOWN": "8", "GROSS_NONPOSITIVE": "20"})

    def test_time_shift_control_skew_uses_live_legs_and_never_splits_time(self):
        h = self.harness(policy=v2_policy(controls=[{"kind": "time_shift", "shift_ns": ["5"]}],
                                          controls_episodes=True))
        h.window(); h.quote(12, 0); h.quote(12, 1)
        # Only the live (unshifted) leg updates, at unconsumed depth.
        for time in range(13, 35):
            operations(h, time, "polymarket:123", "outcome", "bid", 100 + time, M)
        h.finish()
        name = "controls/time_shift_5/"
        self.assertEqual(len(rows(h, name + "denominators.ndjson")),
                         len(table(h, name + "entities.json")["scopes"][0]))
        index = entity_index(h, name, direction="long", size_contracts="1")
        episode = [e for e in rows(h, name + "episodes.ndjson") if e["entity"] == index][0]
        # Opened by the timer at 17, when the live legs' last changes were 17 and 12.
        self.assertEqual((episode["start_ns"], episode["open"]["leg_skew_ns"]), ("17", "5"))
        real = entity_index(h, direction="long", size_contracts="1")
        real_episode = [e for e in rows(h, "episodes.ndjson") if e["entity"] == real][0]
        self.assertEqual(real_episode["open"]["leg_skew_ns"], "0")

    def test_time_shift_history_without_a_staged_entity_keeps_its_exact_time(self):
        h = self.harness(policy=v2_policy(controls=[{"kind": "time_shift", "shift_ns": ["5"]}]))
        h.window(); h.quote(12, 0); h.quote(12, 1)
        # A deep change stages no entity, but the ring still records time 14.
        operations(h, 14, "polymarket:987", "outcome", "bid", 100, M)
        h.group(30)
        self.assertEqual(h.strategy.rings["polymarket:987", "outcome"].times[-1], 14)
        h.finish()

    def test_time_shift_ring_bound_fails_closed(self):
        h = self.harness(policy=v2_policy(controls=[{"kind": "time_shift", "shift_ns": ["5"]}],
                                          time_shift_ring_entries="2"))
        h.window(); h.quote(12, 0); h.quote(12, 1)
        h.quote(13, 1, ask=480)
        with self.assertRaisesRegex(ProtocolError, "time_shift ring bound"):
            h.quote(14, 1, ask=470)

    def test_cyclic_neighbor_controls_name_the_replaced_leg(self):
        h = self.harness(pairs=True, known=True, policy=v2_policy(controls=[{"kind": "cyclic_neighbor"}]))
        h.window()
        for index, plan in enumerate(h.initial["plans"]):
            h.quote(12, index, ask=None if plan["venue"] == "kalshi" else 490,
                    bid=600 if plan["venue"] == "kalshi" else 400)
        result = h.finish()
        entities = table(h, "controls/cyclic_neighbor/entities.json")["scopes"][0]
        admitted = [e["descriptor"] for e in entities if e["descriptor"]["admission"] is None]
        self.assertTrue(admitted and all(d["control"]["replaced_leg"] for d in admitted))
        self.assertTrue(result["summary"]["controls"]["cyclic_neighbor"]["rows"])


class ComplementV2Tests(Base):
    def test_self_crossed_leg_is_a_diagnostic_status(self):
        h = self.harness(policy=v2_policy())
        h.window(); h.quote(12, 0, bid=600, ask=490); h.quote(12, 1)
        h.finish()
        row = denominators(h)[0, entity_index(h, direction="long", size_contracts="1")]
        self.assertEqual(row["status_ns"]["SELF_CROSSED_LEG"], "28")
        reasons = table(h, "reasons.json")["reasons"]
        self.assertIn({"kind": "self_crossed", "leg": 0}, reasons)
        self.assertEqual(rows(h, "episodes.ndjson"), [])

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
        self.assertTrue(_skew_artifact({"2": 7, "3": 7}, policy))
        self.assertFalse(_skew_artifact({"0": 7, "2": 7}, policy))
        self.assertFalse(_skew_artifact({"1": 7}, policy))
        self.assertFalse(_skew_artifact({"2": 0}, policy))


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
        fingerprint = [self.fill(32), self.fill(32), (10**6, 10**12), None,
                       "unusable", {"kind": "lane_invalid", "detail": "y" * 100}]
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
