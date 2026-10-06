"""SDK fill checks (spec §13): one priced fill per episode, kill prices, no-fill time.

A small synthetic two-leg strategy buys both Polymarket token books of one
market through the real preparation, Decoder, SDK runtime and independent
layout-2 reader. Prices are at scale 3 (one contract pays 1,000 price atoms)
and quantities at scale 6, so values are at scale 9.
"""

import hashlib
import json
import random
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from replay.economic_fills import Fill, kill_price, walk_basket
from replay.economic_sdk import (
    EVALUATED, ONE_SIDED, UNUSABLE, Basket, BookRequirement, Experiment, FillSpec, Observation,
    Requirements, Strategy, aggregate_reader, bounds, factory,
)
from replay.economic_sdk import fills as fill_mode
from replay.economic_sdk.fills import fill_policy, step_atoms
from replay.economic_sdk.types import Control
from replay.economic_sdk import views
from replay.economic_sdk.views import BookView, ViewBuilder
from replay.preparation import digest, encoded, load_snapshot
from replay.strategy_sdk import PreparedInput, plain
from replay.streams.protocol import ProtocolError, obj, require
from replay.tests.economic_scenarios import M, ladder, operations
from replay.tests.test_bundle_coverage import Harness as CoverageHarness
from replay.tests.test_economic_sdk import deep_size

P = 1_000  # price atoms per contract at scale 3
LEG0, LEG1 = "polymarket:123", "polymarket:987"
KIND = "gross"


ONE = {"name": "one", "role": "governs", "target_contracts": "1"}


def policy(*sizings, **overrides):
    fills = {"version": 1, "step_contracts": "1", "max_levels": 8, "sizings": list(sizings or (ONE,))}
    fills.update(overrides.pop("fills", {}))
    value = {"version": 1, "fills": fills, "fee_per_contract": "10", "fee_from_ns": None,
             "unknown_at": None, "trigger": "best", "audit_intervals": False, "controls": [],
             "side": "buy"}
    value.update(overrides)
    return value


def reason(value):
    return encoded(value).decode()


class PairFills(Strategy):
    """Buys one contract of each token per step; trigger: best asks sum below one unit.

    With policy ``side: "sell"`` it instead sells one contract of each token per
    step, against the unit payout one complete set costs; trigger: best bids sum
    above one unit.
    """

    name = "synthetic_pair_fills"

    def __init__(self, config):
        config = obj(plain(config), "version snapshot_directory snapshot_sha256 policy")
        self.prepared = PreparedInput({k: config[k] for k in ("version", "snapshot_directory",
                                                               "snapshot_sha256")})
        self.snapshot = plain(self.prepared.snapshot)
        self.snapshot_sha256 = self.prepared.sha256
        self.policy = obj(config["policy"], "version fills fee_per_contract fee_from_ns unknown_at "
                                            "trigger audit_intervals controls side")
        self.side = "ask" if self.policy["side"] == "buy" else "bid"
        self.fee = int(self.policy["fee_per_contract"])
        # A dated fee: one more price atom per contract for fills opened from this time.
        dated = self.policy["fee_from_ns"]
        self.fee_from = None if dated is None else int(dated)
        unknown = self.policy["unknown_at"]
        self.unknown_at = None if unknown is None else int(unknown)
        self.plans = {(p["instrument"], p["orientation"]): p for p in self.snapshot["plans"]}
        fills = fill_policy(self.policy["fills"], KIND)
        self.units = (step_atoms(fills, 6), step_atoms(fills, 6))
        self.experiment = Experiment(
            strategy=self.name, policy=self.policy, policy_sha256=digest(self.policy),
            experiment_sha256=digest({"strategy": self.name, "policy": self.policy,
                                      "snapshot_sha256": self.snapshot_sha256}),
            tiers_ns=("1", "5", "10"), skew_edges_ns=(1, 5, 10), kinds=(KIND,),
            episode_classes={KIND: ("POSITIVE",)}, value_classes=("NONPOSITIVE", "POSITIVE"),
            diagnostic_statuses=(), measurement_fields=(), unevaluated_fields=(),
            maxima=("edge",), slice_invariant=(), layout=2,
            audit_intervals=self.policy["audit_intervals"],
            controls=tuple(Control("time_shift", int(s)) for s in self.policy["controls"]),
            ring_entries=100 if self.policy["controls"] else 0, fills=fills)

    def bind(self, initial):
        self.prepared.bind(initial)

    @property
    def bound(self):
        return self.prepared.bound

    def requirements(self, snapshot, policy):
        return Requirements({key: BookRequirement((self.side,), (1,), ladders=(self.side,))
                             for key in self.plans})

    def baskets(self, snapshot, policy, scope_index):
        legs = ((LEG0, "outcome"), (LEG1, "outcome"))
        source = "crossed" if self.policy["trigger"] == "always" else "best"
        return (Basket({"name": "pair", "legs": [{"instrument": i, "orientation": o} for i, o in legs]},
                       legs, (0,), control_leg=1, peer_group=0, peer_order=0,
                       inputs=(((source, None),), ((source, None),))),)

    def control_descriptor(self, basket, control, replacement, admission):
        return {**basket.descriptor, "control": control.shift_ns}

    def evaluate(self, entity, views, context):
        if any(view.validity != "usable" for view in views):
            return Observation(UNUSABLE, tuple(reason({"leg": i, "validity": v.validity})
                                               for i, v in enumerate(views) if v.validity != "usable"),
                               context_free=True)
        if any(not view.present(self.side) for view in views):
            return Observation(ONE_SIDED, context_free=True)
        asks = tuple(view.best_ask if self.side == "ask" else view.best_bid for view in views)
        edge = P - sum(ask[0] for ask in asks)
        if self.side == "bid":
            edge = -edge
        if edge <= 0 and self.policy["trigger"] == "best":
            return Observation(EVALUATED, value_class="NONPOSITIVE", context_free=True)
        return Observation(EVALUATED, value_class="POSITIVE", predicates=frozenset({KIND}),
                           payload={"edge": str(edge)}, quotes=tuple((ask,) for ask in asks),
                           context_free=True)

    # -- fill hooks ------------------------------------------------------------
    def fill_spec(self, entity):
        return FillSpec((self.side, self.side), self.units)

    def fill_value(self, entity, steps, legs, time):
        if self.unknown_at is not None and any(price >= self.unknown_at
                                               for leg in legs for price, _ in leg.taken):
            return None
        fee = self.fee + (self.fee_from is not None and time >= self.fee_from)
        if self.side == "bid":
            return sum(leg.cost for leg in legs) - steps * self.units[0] * (P + fee)
        return steps * self.units[0] * (P - fee) - sum(leg.cost for leg in legs)

    # -- completion and reader hooks --------------------------------------------
    def manifest(self, files, instantaneous):
        return {"version": 1, "strategy": self.name, "snapshot_sha256": self.snapshot_sha256,
                "policy": self.policy, "experiment_sha256": self.experiment.experiment_sha256,
                "files": files, "instantaneous_positive": instantaneous}

    def validate(self, directory, snapshot, manifest):
        return aggregate_reader.validate(directory, snapshot, manifest, self)

    def open_facts(self, values, entity, kind):
        return int(obj(values, "edge")["edge"])

    def check_open(self, values, facts, entity, kind, opening_class, where):
        require(opening_class == "POSITIVE", "synthetic opening class")

    def check_quotes(self, quotes, values, entity):
        require(type(quotes) is list and len(quotes) == 2, "synthetic quotes")
        edge = P - sum(int(leg[0][0]) for leg in quotes)
        require((edge if self.side == "ask" else -edge) == int(values["edge"]), "synthetic edge")

    def summary_key(self, entity):
        return ("pair",)

    def summarize_aggregate(self, aggregates, manifest, snapshot):
        facts = aggregates.groups[""][("pair",)]
        return {"status_ns": {k: str(v) for k, v in sorted(facts.status_ns.items())},
                "class_ns": {k: str(v) for k, v in sorted(facts.class_ns.items())},
                "fill_ns": {k: str(v) for k, v in sorted(facts.fill_ns.items())},
                "fill_ends": dict(sorted(facts.fill_ends.items())),
                "episodes": facts.episode_count[KIND],
                "q_ns": {t: str(v) for t, v in facts.q[KIND].items()}}


build = factory(PairFills)


class FillHarness(CoverageHarness):
    def __init__(self, root, value, **kwargs):
        def make(context):
            return build({**context, "config": {**plain(context["config"]), "policy": value}})

        with patch("replay.tests.test_bundle_coverage.build", side_effect=make):
            super().__init__(root, **kwargs)

    def plan_index(self, instrument, orientation="outcome"):
        return next(i for i, p in enumerate(self.initial["plans"])
                    if p["instrument"] == instrument and p["orientation"] == orientation)

    def asks(self, time, instrument, *levels):
        return ladder(self, time, instrument, asks=levels)

    def both(self, time, first, second):
        """One cut that snapshots both legs' asks, so their last changes stay equal."""
        transitions = []
        for instrument, levels in ((LEG0, first), (LEG1, second)):
            plan = self.initial["plans"][self.plan_index(instrument)]
            transition = self.transition(plan, time)
            transition["decision"] = {"kind": "snapshot", "bids": [],
                                      "asks": [[str(p), str(q)] for p, q in levels]}
            transitions.append(transition)
        ref = self.ref(time)
        return self.send("cut", {"origin": {"kind": "group", "pin": self.pin, "first": ref["address"],
                                            "last": ref["address"], "visible_ns": str(time)},
                                 "market_events": [], "book_transitions": transitions})

    def finish(self):
        self.terminal()
        self.decoder.finish()
        self.strategy.finish()
        return json.loads((self.output / "summary.json").read_bytes())

    def rows(self, name):
        path = self.output / name
        return [json.loads(line) for line in path.read_bytes().splitlines()] if path.exists() else []


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.count = 0

    def harness(self, *sizings, scopes=False, **overrides):
        self.count += 1
        root = Path(self.tmp.name) / str(self.count)
        root.mkdir()
        h = FillHarness(root, policy(*sizings, **overrides), scopes=scopes)
        for writer in h.strategy.writers.values():
            self.addCleanup(writer.stream.close)
        h.window()
        return h

    @staticmethod
    def spans(h):
        return [(e["start_ns"], e["end_ns"], e["end_reason"]) for e in h.rows("episodes.ndjson")]

    @staticmethod
    def denominator(h):
        (row,) = h.rows("denominators.ndjson")
        return row

    def walks(self):
        return patch("replay.economic_sdk.fills.walk_basket", side_effect=walk_basket)


class FillEpisodeTests(Base):
    def test_fill_is_priced_once_and_book_changes_below_kill_prices_change_nothing(self):
        h = self.harness()
        with self.walks() as walks:
            h.asks(12, LEG0, (450, 5 * M))
            h.asks(12, LEG1, (500, 5 * M))
            operations(h, 13, LEG0, "outcome", "ask", 470, M)        # deeper level
            h.asks(15, LEG0, (460, 2 * M), (470, M))                  # best moves, below 490
            h.asks(17, LEG1, (480, M))                                # improvement on leg 1
            operations(h, 19, LEG1, "outcome", "bid", 100, M)         # an unread side
            summary = h.finish()
        self.assertEqual(walks.call_count, 1)
        self.assertEqual(self.spans(h), [("12", "40", "RUN_END")])
        (episode,) = h.rows("episodes.ndjson")
        fill = episode["fill"]
        # Kill prices: 1000 - 10 - 500 = 490 on leg 0; 1000 - 10 - 450 = 540 on leg 1.
        self.assertEqual(fill["kill_prices"], ["490", "540"])
        self.assertEqual((fill["kill_leg"], fill["kill_best"]), (None, None))
        (result,) = fill["results"]
        self.assertEqual((result["steps"], result["stop"], result["value"]), ("1", "target", "40000000"))
        self.assertEqual(result["before"], [["450", "5000000"], ["500", "5000000"]])
        self.assertEqual(result["after"], [["450", "4000000"], ["500", "4000000"]])
        self.assertEqual(result["impact_ppm"], ["0", "0"])
        self.assertEqual([s["episode_id"] for s in h.rows("slices.ndjson")], [episode["episode_id"]])
        self.assertEqual(episode["opening_slice_survival_ns"], "28")
        self.assertEqual(self.denominator(h)["fill_ns"], {"FILL_LIVE": "28"})
        self.assertEqual(summary["fill_ends"], {"RUN_END": 1})

    def test_kill_price_crossing_on_either_leg_ends_the_fill_at_that_nanosecond(self):
        for leg, instrument, price in ((0, LEG0, 490), (1, LEG1, 540)):
            with self.subTest(leg=leg):
                h = self.harness()
                h.asks(12, LEG0, (450, 5 * M))
                h.asks(12, LEG1, (500, 5 * M))
                h.asks(20, instrument, (price - 1, 5 * M))   # one atom below: still live
                h.asks(23, instrument, (price, 5 * M))       # at the kill price
                h.finish()
                self.assertEqual(self.spans(h), [("12", "23", "KILL_PRICE")])
                fill = h.rows("episodes.ndjson")[0]["fill"]
                self.assertEqual((fill["kill_leg"], fill["kill_best"]), (leg, [str(price), "5000000"]))
                # The trigger stays on (sum 990 < 1000); the repriced fill is not positive.
                self.assertEqual(self.denominator(h)["fill_ns"],
                                 {"FILL_LIVE": "11", "FILL_NONPOSITIVE": "17"})

    def test_trigger_turning_false_ends_the_fill(self):
        for why, expected in (("price", "PREDICATE_FALSE"), ("invalid", UNUSABLE)):
            with self.subTest(why=why):
                h = self.harness()
                h.asks(12, LEG0, (450, 5 * M))
                h.asks(12, LEG1, (500, 5 * M))
                if why == "price":
                    h.asks(20, LEG1, (560, 5 * M))   # sum 1010: trigger false before any kill
                else:
                    ladder(h, 20, LEG1, why={"kind": "connection_closed"})
                h.finish()
                self.assertEqual(self.spans(h), [("12", "20", expected)])
                self.assertEqual(self.denominator(h)["fill_ns"], {"FILL_LIVE": "8"})

    def test_nontradeable_fill_opens_no_episode_and_its_time_stays_visible(self):
        h = self.harness()
        with self.walks() as walks:
            h.asks(12, LEG0, (495, 5 * M))
            h.asks(12, LEG1, (500, 5 * M))   # trigger on (995); value 1000-10-995 < 0
            h.both(15, ((495, 5 * M),), ((500, 5 * M),))   # identical ladders: no retry
            # Pricing happens at commit, when a later time arrives.
            # A deeper ask in a cut that keeps the declared inputs and the skew: a retry.
            h.both(17, ((495, 5 * M), (600, M)), ((500, 5 * M),))
            self.assertEqual(walks.call_count, 1)      # 12 priced; 15 committed without a walk
            h.asks(20, LEG1, (480, 5 * M))              # the improvement opens a fill
            self.assertEqual(walks.call_count, 2)      # 17 retried
            summary = h.finish()
        self.assertEqual(walks.call_count, 3)          # 20 opened the fill
        self.assertEqual(self.spans(h), [("20", "40", "RUN_END")])
        row = self.denominator(h)
        self.assertEqual(row["class_ns"], {"POSITIVE": "28"})
        self.assertEqual(row["fill_ns"], {"FILL_LIVE": "20", "FILL_NONPOSITIVE": "8"})
        self.assertEqual(summary["fill_ns"], {"FILL_LIVE": "20", "FILL_NONPOSITIVE": "8"})

    def test_unknown_fill_value_is_its_own_no_fill_reason(self):
        h = self.harness(unknown_at="600")
        h.asks(12, LEG0, (650, 5 * M))
        h.asks(12, LEG1, (300, 5 * M))
        h.asks(25, LEG0, (550, 5 * M))
        h.finish()
        self.assertEqual(self.spans(h), [("25", "40", "RUN_END")])
        self.assertEqual(self.denominator(h)["fill_ns"], {"FILL_LIVE": "15", "FILL_VALUE_UNKNOWN": "13"})
        # Unknown counts as not positive: leg 0 is killed at the unknown boundary.
        self.assertEqual(h.rows("episodes.ndjson")[0]["fill"]["kill_prices"], ["600", "440"])

    def test_after_a_kill_with_the_trigger_on_a_new_fill_opens_at_the_same_time(self):
        h = self.harness()
        with self.walks() as walks:
            h.asks(12, LEG0, (450, 5 * M))
            h.asks(12, LEG1, (500, 5 * M))
            h.asks(20, LEG0, (495, 5 * M))   # crosses leg 0's 490 kill price...
            h.asks(20, LEG1, (400, 5 * M))   # ...while leg 1 improves at the same time
            h.finish()
        self.assertEqual(walks.call_count, 2)
        self.assertEqual(self.spans(h), [("12", "20", "KILL_PRICE"), ("20", "40", "RUN_END")])
        second = h.rows("episodes.ndjson")[1]["fill"]
        self.assertEqual(second["results"][0]["before"], [["495", "5000000"], ["400", "5000000"]])
        self.assertEqual(self.denominator(h)["fill_ns"], {"FILL_LIVE": "28"})

    def test_same_time_staging_prices_the_final_state_at_that_time(self):
        h = self.harness()
        with self.walks() as walks:
            h.asks(12, LEG0, (450, 5 * M))
            h.asks(12, LEG1, (700, 5 * M))   # trigger off
            h.asks(20, LEG1, (500, 5 * M))   # trigger on...
            h.asks(20, LEG0, (470, M), (480, M))   # ...and a second same-time change
            self.assertEqual(walks.call_count, 0)   # nothing is priced before commit
            h.asks(21, LEG0, (470, M), (480, M))
            self.assertEqual(walks.call_count, 1)
            h.finish()
        (episode,) = h.rows("episodes.ndjson")
        self.assertEqual(episode["start_ns"], "20")
        self.assertEqual(episode["fill"]["results"][0]["before"], [["470", "1000000"], ["500", "5000000"]])

    def test_kill_check_runs_when_evaluation_is_skipped(self):
        h = self.harness(trigger="always")   # declares only the "crossed" flags as inputs
        h.both(12, ((450, 5 * M),), ((500, 5 * M),))
        h.both(13, ((450, 5 * M),), ((500, 5 * M),))
        with patch.object(PairFills, "evaluate", wraps=h.strategy.strategy.evaluate) as evaluate:
            # One cut changes both legs, so neither the declared inputs nor the leg
            # skew change: the entity is staged only because its fill is live.
            h.both(20, ((520, 5 * M),), ((500, 5 * M),))
            h.both(21, ((520, 5 * M),), ((500, 5 * M),))
            self.assertEqual(evaluate.call_count, 0)
        h.finish()
        self.assertEqual(self.spans(h)[0], ("12", "20", "KILL_PRICE"))

    def test_scope_end_closes_the_fill_and_the_next_scope_prices_its_own(self):
        h = self.harness(scopes=True)
        with self.walks() as walks:
            h.asks(12, LEG0, (450, 5 * M))
            h.asks(12, LEG1, (500, 5 * M))
            h.finish()
        self.assertEqual(walks.call_count, 2)
        self.assertEqual([(e["scope"], e["start_ns"], e["end_ns"], e["end_reason"])
                          for e in h.rows("episodes.ndjson")],
                         [(0, "12", "23", "SCOPE_END"), (1, "23", "40", "RUN_END")])
        self.assertEqual([r["fill_ns"] for r in h.rows("denominators.ndjson")],
                         [{"FILL_LIVE": "11"}, {"FILL_LIVE": "17"}])

    def test_sizings_share_one_shape_and_records_are_written_whatever_their_value(self):
        h = self.harness({"name": "edge", "role": "governs", "edge": True},
                         {"name": "t6", "role": "records", "target_contracts": "6"},
                         {"name": "t1", "role": "records", "target_contracts": "1"})
        with self.walks() as walks:
            h.asks(12, LEG0, (400, M), (420, M), (470, 5 * M))
            h.asks(12, LEG1, (450, 2 * M), (600, 5 * M))
            h.finish()
        self.assertEqual(walks.call_count, 3)   # one walk per sizing, once, at open
        (episode,) = h.rows("episodes.ndjson")
        edge, t6, t1 = episode["fill"]["results"]
        self.assertEqual(len({tuple(sorted(r)) for r in (edge, t6, t1)}), 1)
        self.assertEqual([(r["name"], r["role"], r["mode"], r["steps"], r["stop"]) for r in (edge, t6, t1)],
                         [("edge", "governs", "edge", "2", "edge"),
                          ("t6", "records", "target", "6", "target"),
                          ("t1", "records", "target", "1", "target")])
        # Six contracts cost 2,700 + 3,300 against a 5,940 payout: written although not positive.
        self.assertEqual((t6["value"], t6["tradeable"], t6["kill_prices"]), ("-60000000", False, None))
        self.assertEqual((edge["value"], t1["value"], t1["tradeable"]), ("260000000", "140000000", True))
        # A recording sizing's kill prices are its own: t1's leg-0 kill (540) is not the fill's.
        self.assertEqual(t1["kill_prices"], ["540", "590"])
        # Only governing sizings set the effective kill prices.
        self.assertEqual(episode["fill"]["kill_prices"], edge["kill_prices"])

    def test_recording_sizings_never_end_or_block_the_episode(self):
        h = self.harness(ONE, {"name": "t3", "role": "records", "target_contracts": "3"},
                         {"name": "t9", "role": "records", "target_contracts": "9"})
        h.asks(12, LEG0, (400, M), (480, 5 * M))
        h.asks(12, LEG1, (450, 5 * M))
        h.asks(20, LEG1, (540, 5 * M))   # past t3's leg-1 kill price (537), below one's (590)
        h.finish()
        self.assertEqual(self.spans(h), [("12", "40", "RUN_END")])
        one, t3, t9 = h.rows("episodes.ndjson")[0]["fill"]["results"]
        self.assertEqual((one["kill_prices"], t3["kill_prices"]), (["540", "590"], ["540", "537"]))
        self.assertEqual(h.rows("episodes.ndjson")[0]["fill"]["kill_prices"], ["540", "590"])
        # Leg 1 serves five of nine contracts: a short record, still written.
        self.assertEqual((t9["steps"], t9["stop"], t9["tradeable"]), ("5", "book_exhausted", False))

    def test_two_governing_sizings_take_the_lower_kill_and_either_one_ends_the_fill(self):
        h = self.harness(ONE, {"name": "t3", "role": "governs", "target_contracts": "3"})
        h.asks(12, LEG0, (400, M), (480, 5 * M))
        h.asks(12, LEG1, (450, 5 * M))
        h.asks(20, LEG1, (537, 5 * M))   # one is still worth taking; t3 is not
        h.finish()
        self.assertEqual(self.spans(h), [("12", "20", "KILL_PRICE")])
        fill = h.rows("episodes.ndjson")[0]["fill"]
        self.assertEqual(fill["kill_prices"], ["540", "537"])
        self.assertEqual((fill["kill_leg"], fill["kill_best"]), (1, ["537", "5000000"]))
        self.assertEqual(self.denominator(h)["fill_ns"], {"FILL_LIVE": "8", "FILL_NONPOSITIVE": "20"})

    def test_a_governing_target_the_book_cannot_serve_is_depth_short(self):
        h = self.harness(ONE, {"name": "t3", "role": "governs", "target_contracts": "3"})
        h.asks(12, LEG0, (495, 2 * M))
        h.asks(12, LEG1, (500, 5 * M))   # one is not positive and t3 is short: short wins
        h.asks(16, LEG0, (495, 5 * M))   # t3 is served, neither is positive
        h.asks(20, LEG1, (450, 5 * M))   # both tradeable
        h.finish()
        self.assertEqual(self.spans(h), [("20", "40", "RUN_END")])
        self.assertEqual(self.denominator(h)["fill_ns"],
                         {"FILL_DEPTH_SHORT": "4", "FILL_LIVE": "20", "FILL_NONPOSITIVE": "4"})

    def test_fill_value_receives_the_open_time_in_the_runtime_and_the_reader(self):
        times = []
        original = PairFills.fill_value

        def recording(strategy, entity, steps, legs, time):
            times.append(time)
            return original(strategy, entity, steps, legs, time)

        with patch.object(PairFills, "fill_value", recording):
            h = self.harness(fee_from_ns="20")
            h.asks(12, LEG0, (450, 5 * M))
            h.asks(12, LEG1, (500, 5 * M))
            h.asks(22, LEG0, (490, 5 * M))   # kill (fee 10 at open); repriced at fee 11
            h.asks(25, LEG1, (480, 5 * M))   # opens under the dated fee
            runtime = set(times[:])
            h.finish()
            self.assertEqual(runtime, {12, 22})
            self.assertTrue({12, 22, 25} <= set(times))
            times.clear()
            manifest = json.loads((h.output / "manifest.json").read_bytes())
            snapshot = load_snapshot(h.root / "context", expected_sha256=h.sha)
            aggregate_reader.validate(h.output, snapshot, manifest, h.strategy.strategy)
            self.assertEqual(set(times), {12, 25})   # each episode's start_ns
        first, second = h.rows("episodes.ndjson")
        self.assertEqual(first["fill"]["kill_prices"], ["490", "540"])    # 1000 - 10 - ...
        self.assertEqual(second["fill"]["kill_prices"], ["509", "499"])   # 1000 - 11 - ...


class FillConstructionTests(Base):
    def test_controls_with_fill_checks_are_rejected_at_construction(self):
        root = Path(self.tmp.name) / "controls"
        root.mkdir()
        with self.assertRaisesRegex(ProtocolError, "controls are not supported with fill checks"):
            FillHarness(root, policy(controls=["5"]))
        self.assertEqual(list((root / "output").iterdir()), [])

    def test_policy_is_closed_with_one_mode_per_sizing_and_a_governing_sizing(self):
        edge = {"name": "edge", "role": "governs", "edge": True}
        base = {"version": 1, "step_contracts": "0.5", "max_levels": 4,
                "sizings": [edge, {"name": "t1", "role": "records", "target_contracts": "1"}]}
        parsed = fill_policy(base, KIND)
        self.assertEqual([(z.name, z.role, z.mode, z.target_steps) for z in parsed.sizings],
                         [("edge", "governs", "edge", None), ("t1", "records", "target", 2)])
        sizings = lambda *z: {**base, "sizings": list(z)}
        for bad in ({**base, "extra": 1}, {**base, "version": 2}, {**base, "step_contracts": "1.50"},
                    {**base, "max_levels": 0}, {**base, "max_levels": bounds.MAX_CONSUMED_LEVELS + 1},
                    sizings(), sizings(*[{**edge, "name": f"e{i}"} for i in range(9)]),
                    sizings({**edge, "role": "records"}),                          # nothing governs
                    sizings(edge, {**edge}),                                       # duplicate name
                    sizings({**edge, "role": "observes"}),
                    sizings({**edge, "name": ""}),
                    sizings({**edge, "target_contracts": "1"}),                    # two modes
                    sizings({"name": "x", "role": "governs"}),                     # no mode
                    sizings({**edge, "edge": False}),
                    sizings({**edge, "extra": 1}),
                    sizings({"name": "t", "role": "governs", "target_contracts": "0.25"}),
                    sizings({"name": "t", "role": "governs", "target_contracts": 1}),
                    sizings({"name": "m", "role": "governs", "max_price_move_ppm": "50000"})):
            with self.subTest(bad=bad), self.assertRaises(ProtocolError):
                fill_policy(bad, KIND)
        with self.assertRaisesRegex(ProtocolError, "whole number of quantity atoms"):
            step_atoms(fill_policy({**base, "step_contracts": "0.0000001", "sizings": [edge]}, KIND), 6)

    def test_views_retain_the_ladder_they_read_including_the_kalshi_projection(self):
        class Book:
            validity, reason = "usable", None

            def __init__(self, bids, asks):
                self.sides = {"bid": bids, "ask": asks}

            def levels(self, side):
                return self.sides[side]

        plan = {"price_scale": "3", "quantity_scale": "6"}
        requirement = BookRequirement(("bid",), (1,), True, ("kalshi_complement_ask",),
                                      ladders=("kalshi_complement_ask",))
        builder = ViewBuilder(requirement, plan)
        bids = ((600, M), (550, 3 * M))
        with patch("replay.economic_sdk.views.read_side", wraps=views.read_side) as reads:
            view = builder.build(Book(bids, ()), 12)
            self.assertEqual(reads.call_count, 1)   # the one full read also feeds the ladder
        self.assertEqual(view.ladders["kalshi_complement_ask"], ((400, M), (450, 3 * M)))
        self.assertEqual(view.ladders["bid"], bids)
        # A deep bid touch beyond consumed depth re-reads a retained ladder side.
        again = builder.build(Book(bids + ((100, M),), ()), 13, view, {"bid": 100})
        self.assertEqual(again.ladders["kalshi_complement_ask"][-1], (900, M))
        same = builder.build(Book(bids, ()), 14, view, {"bid": 500})
        self.assertIs(same.ladders["kalshi_complement_ask"], view.ladders["kalshi_complement_ask"])
        with self.assertRaisesRegex(ProtocolError, "retained ladder"):
            ViewBuilder(BookRequirement(("bid",), (1,), ladders=("ask",)), plan)

    def test_unretained_fill_ladder_is_rejected(self):
        root = Path(self.tmp.name) / "unretained"
        root.mkdir()
        with patch.object(PairFills, "requirements", lambda self, s, p: Requirements(
                {key: BookRequirement(("ask",), (1,)) for key in self.plans})):
            with self.assertRaisesRegex(ProtocolError, "fill ladder not retained"):
                FillHarness(root, policy())


def rewrite(h, manifest, name, mutate):
    path = h.output / name
    rows = [json.loads(line) for line in path.read_bytes().splitlines()]
    mutate(rows)
    payload = b"".join(json.dumps(row, sort_keys=True, separators=(",", ":")).encode() + b"\n"
                       for row in rows)
    path.write_bytes(payload)
    manifest["files"][name] = {"sha256": hashlib.sha256(payload).hexdigest(),
                               "byte_length": len(payload), "records": len(rows)}


class FillReaderTests(Base):
    def fixture(self, audit=False):
        h = self.harness(ONE, {"name": "t4", "role": "records", "target_contracts": "4"},
                         audit_intervals=audit)
        h.asks(12, LEG0, (450, 2 * M), (460, M), (480, M))
        h.asks(12, LEG1, (500, 5 * M))
        h.asks(20, LEG0, (495, 5 * M))
        h.asks(26, LEG0, (450, 2 * M))
        h.finish()
        manifest = json.loads((h.output / "manifest.json").read_bytes())
        snapshot = load_snapshot(h.root / "context", expected_sha256=h.sha)
        return h, snapshot, manifest

    def reject(self, mutate, name="episodes.ndjson", pattern="fill", audit=False):
        h, snapshot, manifest = self.fixture(audit)
        rewrite(h, manifest, name, mutate)
        with self.assertRaisesRegex(ProtocolError, pattern):
            aggregate_reader.validate(h.output, snapshot, manifest, h.strategy.strategy)

    def test_fixture_reads_and_audit_places_fills_inside_trigger_runs(self):
        for audit in (False, True):
            with self.subTest(audit=audit):
                h, snapshot, manifest = self.fixture(audit)
                summary = aggregate_reader.validate(h.output, snapshot, manifest, h.strategy.strategy)
                self.assertEqual(self.spans(h), [("12", "20", "KILL_PRICE"), ("26", "40", "RUN_END")])
                self.assertEqual(summary["fill_ns"], {"FILL_LIVE": "22", "FILL_NONPOSITIVE": "6"})
                self.assertEqual(summary["fill_ends"], {"KILL_PRICE": 1, "RUN_END": 1})

    def test_reader_rejects_a_tampered_record_and_a_wrong_record_stop(self):
        def record_cost(rows):
            leg = rows[0]["fill"]["results"][1]["legs"][1]
            leg["cost"] = str(int(leg["cost"]) + 1)
        self.reject(record_cost, pattern="fill cost")

        def record_value(rows):
            record = rows[0]["fill"]["results"][1]
            record["value"] = str(int(record["value"]) + 1)
        self.reject(record_value, pattern="fill value")

        def stop(rows):
            record = rows[0]["fill"]["results"][1]
            self.assertEqual((record["steps"], record["stop"], record["value"]), ("4", "target", "120000000"))
            record["stop"] = "book_exhausted"   # the full target was served
        self.reject(stop, pattern="fill stop")

    def test_reader_rejects_a_tampered_fill_cost(self):
        def mutate(rows):
            leg = rows[0]["fill"]["results"][0]["legs"][0]
            leg["cost"] = str(int(leg["cost"]) - 1)
        self.reject(mutate, pattern="fill cost")

    def test_reader_rejects_a_wrong_after_level_and_impact(self):
        def after(rows):
            rows[0]["fill"]["results"][0]["after"][0] = ["450", "2000000"]
        self.reject(after, pattern="fill after level")

        def impact(rows):
            rows[0]["fill"]["results"][0]["impact_ppm"][0] = "1"
        self.reject(impact, pattern="fill impact")

    def test_reader_rejects_a_wrong_kill_price(self):
        def mutate(rows):
            for target in (rows[0]["fill"]["results"][0]["kill_prices"], rows[0]["fill"]["kill_prices"]):
                target[0] = str(int(target[0]) + 1)
        self.reject(mutate, pattern="fill kill price")

        def crossing(rows):
            rows[0]["fill"]["kill_best"] = ["489", "5000000"]
        self.reject(crossing, pattern="fill kill crossing")

    def test_reader_rejects_a_fill_episode_outside_trigger_time(self):
        h, snapshot, manifest = self.fixture()
        entity = next(iter(h.strategy.entities.values())).id

        def earlier(rows):
            # The second fill opened at 26; claim it opened at 25, inside no-fill time.
            row = rows[-1]
            row["start_ns"], row["episode_id"] = "25", digest([0, entity, KIND, "25"])
            row["class_ns"] = {"POSITIVE": "15"}
            row["opening_slice_survival_ns"] = "15"
        rewrite(h, manifest, "episodes.ndjson", earlier)
        with self.assertRaisesRegex(ProtocolError, "live fill time"):
            aggregate_reader.validate(h.output, snapshot, manifest, h.strategy.strategy)

    def test_reader_rejects_a_broken_no_fill_partition(self):
        def mutate(rows):
            rows[0]["fill_ns"] = {"FILL_LIVE": "21", "FILL_NONPOSITIVE": "7"}
        self.reject(mutate, name="denominators.ndjson", pattern="live fill time")

        def short(rows):
            rows[0]["fill_ns"] = {"FILL_LIVE": "22", "FILL_NONPOSITIVE": "5"}
        self.reject(short, name="denominators.ndjson", pattern="partitions trigger-positive time")

    def test_reader_rejects_a_split_fill_episode(self):
        h, snapshot, manifest = self.fixture()

        def split(rows):
            first = rows[0]
            rows.insert(1, {**first, "start_ns": "16"})
            first["end_ns"], first["end_reason"] = "16", "CONSUMED_CHANGED"
        rewrite(h, manifest, "slices.ndjson", split)
        with self.assertRaisesRegex(ProtocolError, "single slice|slice"):
            aggregate_reader.validate(h.output, snapshot, manifest, h.strategy.strategy)


class SellFillTests(Base):
    """Sold legs walk bids downward and are killed at or below their kill price."""

    def bids(self, h, time, instrument, *levels):
        return ladder(h, time, instrument, bids=levels)

    def test_a_sold_leg_is_killed_when_its_best_bid_falls_to_its_kill_price(self):
        for leg, instrument, price in ((0, LEG0, 510), (1, LEG1, 460)):
            with self.subTest(leg=leg):
                h = self.harness(side="sell")
                self.bids(h, 12, LEG0, (550, 5 * M))
                self.bids(h, 12, LEG1, (500, 5 * M))
                self.bids(h, 15, LEG1 if leg == 0 else LEG0, (600, 5 * M))   # a better bid
                self.bids(h, 20, instrument, (price + 1, 5 * M))   # one atom above: still live
                self.bids(h, 23, instrument, (price, 5 * M))       # at the kill price
                h.finish()
                # The better bid on the other leg keeps the repriced basket positive at
                # the kill, so a new fill opens at that same instant.
                self.assertEqual(self.spans(h), [("12", "23", "KILL_PRICE"), ("23", "40", "RUN_END")])
                fill = h.rows("episodes.ndjson")[0]["fill"]
                # Kill prices: 1000 + 10 - 500 = 510 on leg 0; 1000 + 10 - 550 = 460 on leg 1.
                self.assertEqual(fill["kill_prices"], ["510", "460"])
                self.assertEqual((fill["kill_leg"], fill["kill_best"]), (leg, [str(price), "5000000"]))
                self.assertEqual(fill["end_books"][leg]["best"], [str(price), "5000000"])
                (result,) = fill["results"]
                self.assertEqual((result["steps"], result["value"]), ("1", "40000000"))
                self.assertEqual(self.denominator(h)["fill_ns"], {"FILL_LIVE": "28"})
                manifest = json.loads((h.output / "manifest.json").read_bytes())
                snapshot = load_snapshot(h.root / "context", expected_sha256=h.sha)
                aggregate_reader.validate(h.output, snapshot, manifest, h.strategy.strategy)

    def test_the_edge_walk_sells_down_the_bids_until_the_marginal_edge_is_gone(self):
        h = self.harness({"name": "edge", "role": "governs", "edge": True}, side="sell")
        self.bids(h, 12, LEG0, (600, M), (560, M), (500, 5 * M))
        self.bids(h, 12, LEG1, (520, 2 * M), (450, 5 * M))
        h.finish()
        (result,) = h.rows("episodes.ndjson")[0]["fill"]["results"]
        # Marginal sets: 600 + 520 and 560 + 520 beat 1,010; 500 + 450 does not.
        self.assertEqual((result["steps"], result["stop"], result["value"]), ("2", "edge", "180000000"))
        self.assertEqual(result["legs"][0]["taken"], [["600", "1000000"], ["560", "1000000"]])
        self.assertEqual(result["after"], [["500", "5000000"], ["450", "5000000"]])
        self.assertEqual(result["impact_ppm"], ["166666", "134615"])
        # Selling both contracts of a leg at one price k: 2k + 1,040 (or 1,160) <= 2,020.
        self.assertEqual(result["kill_prices"], ["490", "430"])
        manifest = json.loads((h.output / "manifest.json").read_bytes())
        snapshot = load_snapshot(h.root / "context", expected_sha256=h.sha)
        aggregate_reader.validate(h.output, snapshot, manifest, h.strategy.strategy)

    def test_two_governing_sell_sizings_take_the_higher_kill(self):
        h = self.harness(ONE, {"name": "t3", "role": "governs", "target_contracts": "3"}, side="sell")
        self.bids(h, 12, LEG0, (600, M), (520, 5 * M))
        self.bids(h, 12, LEG1, (550, 5 * M))
        self.bids(h, 20, LEG1, (463, 5 * M))   # one is still worth selling; t3 is not
        h.finish()
        fill = h.rows("episodes.ndjson")[0]["fill"]
        one, t3 = fill["results"]
        # one: 1,010 - 600 = 410 on leg 1; t3: 3k + 1,640 <= 3,030, so k = 463.
        self.assertEqual((one["kill_prices"][1], t3["kill_prices"][1]), ("410", "463"))
        self.assertEqual(fill["kill_prices"], [t3["kill_prices"][0], "463"])
        self.assertEqual(self.spans(h), [("12", "20", "KILL_PRICE")])

    def test_reader_rejects_a_tampered_sell_kill_price_and_crossing(self):
        def fixture():
            h = self.harness(side="sell")
            self.bids(h, 12, LEG0, (550, 5 * M))
            self.bids(h, 12, LEG1, (500, 5 * M))
            self.bids(h, 23, LEG0, (505, 5 * M))
            h.finish()
            manifest = json.loads((h.output / "manifest.json").read_bytes())
            return h, load_snapshot(h.root / "context", expected_sha256=h.sha), manifest

        def kill(rows):
            for target in (rows[0]["fill"]["results"][0]["kill_prices"], rows[0]["fill"]["kill_prices"]):
                target[0] = str(int(target[0]) - 1)

        def crossing(rows):
            rows[0]["fill"]["kill_best"] = ["511", "5000000"]
            rows[0]["fill"]["end_books"][0]["best"] = ["511", "5000000"]

        def ascending(rows):
            # A sold leg's taken levels must descend.
            leg = rows[0]["fill"]["results"][0]["legs"][0]
            leg["taken"] = leg["consumed"] = [["550", "500000"], ["560", "500000"]]
            leg["cost"] = str(550 * 500000 + 560 * 500000)

        for mutate, pattern in ((kill, "fill kill price"), (crossing, "fill kill crossing"),
                                (ascending, "fill taken/consumed levels")):
            with self.subTest(pattern=pattern):
                h, snapshot, manifest = fixture()
                aggregate_reader.validate(h.output, snapshot, manifest, h.strategy.strategy)
                rewrite(h, manifest, "episodes.ndjson", mutate)
                with self.assertRaisesRegex(ProtocolError, pattern):
                    aggregate_reader.validate(h.output, snapshot, manifest, h.strategy.strategy)

    def test_views_project_kalshi_asks_to_complement_bids(self):
        class Book:
            validity, reason = "usable", None

            def levels(self, side):
                return {"bid": (), "ask": ((400, M), (450, 3 * M))}[side]

        plan = {"price_scale": "3", "quantity_scale": "6"}
        builder = ViewBuilder(BookRequirement(("ask",), (1,), True, ("kalshi_complement_bid",),
                                              ladders=("kalshi_complement_bid",)), plan)
        view = builder.build(Book(), 12)
        self.assertEqual(view.ladders["kalshi_complement_bid"], ((600, M), (550, 3 * M)))
        projected = view.transformed["kalshi_complement_bid"][1]
        self.assertEqual((projected.cost, projected.taken), (600 * M, ((600, M),)))
        again = builder.build(Book(), 13, view, {"ask": 900})
        self.assertIs(again.ladders["kalshi_complement_bid"], view.ladders["kalshi_complement_bid"])
        self.assertIs(again.transformed["kalshi_complement_bid"], view.transformed["kalshi_complement_bid"])

    def test_a_basket_may_buy_one_leg_and_sell_another(self):
        spec = FillSpec(("ask", "bid"), (M, M))
        ladders = (((400, M), (450, 5 * M)), ((520, M), (480, 5 * M)))

        def value(steps, legs):   # buy leg 0, sell leg 1, ten atoms of fee per contract each
            return legs[1].cost - legs[0].cost - steps * M * 20

        priced = fill_mode.price(fill_policy({"version": 1, "step_contracts": "1", "max_levels": 8,
                                              "sizings": [ONE]}, KIND),
                                 spec, ladders, (P, P), value)
        self.assertEqual(priced.state, fill_mode.FILL_LIVE)
        # Buy kill: 520 - 20 = 500, the price where buying leg 0 stops paying; sell kill:
        # 400 + 20 = 420, the price where selling leg 1 stops paying.
        self.assertEqual(priced.kill, (500, 420))
        live = BookView("usable", None, 0, True, True, None, None, {}, {}, 0,
                        {"ask": ((499, M),), "bid": ((421, M),)})
        self.assertIsNone(fill_mode.crossed(priced.kill, spec.sources, (live, live)))
        dropped = BookView("usable", None, 0, True, True, None, None, {}, {}, 0,
                           {"ask": ((499, M),), "bid": ((420, M),)})
        self.assertEqual(fill_mode.crossed(priced.kill, spec.sources, (live, dropped)), (1, (420, M)))


class FillEndBookTests(Base):
    def test_every_fill_end_records_each_legs_book_and_a_dropped_book_says_why(self):
        h = self.harness()
        h.asks(12, LEG0, (450, 5 * M))
        h.asks(12, LEG1, (500, 5 * M))
        ladder(h, 20, LEG1, why={"kind": "connection_closed"})
        h.asks(25, LEG1, (500, 5 * M))
        h.asks(30, LEG0, (490, 5 * M))
        h.finish()
        self.assertEqual(self.spans(h), [("12", "20", UNUSABLE), ("25", "30", "KILL_PRICE")])
        dropped, killed = (row["fill"]["end_books"] for row in h.rows("episodes.ndjson"))
        self.assertEqual(dropped[0], {"validity": "usable", "reason": None, "crossed": False,
                                      "best": ["450", "5000000"]})
        self.assertNotEqual(dropped[1]["validity"], "usable")
        self.assertEqual((dropped[1]["reason"], dropped[1]["best"]), ("connection_closed", None))
        self.assertEqual(killed[0]["best"], ["490", "5000000"])
        manifest = json.loads((h.output / "manifest.json").read_bytes())
        snapshot = load_snapshot(h.root / "context", expected_sha256=h.sha)
        aggregate_reader.validate(h.output, snapshot, manifest, h.strategy.strategy)

        def best_on_dropped(rows):
            rows[0]["fill"]["end_books"][1]["best"] = ["500", "5000000"]

        def kill_level(rows):
            rows[1]["fill"]["end_books"][0]["best"] = ["491", "5000000"]

        def missing(rows):
            del rows[0]["fill"]["end_books"]

        for mutate, pattern in ((best_on_dropped, "fill end book best"),
                                (kill_level, "fill end book kill level"), (missing, "closed schema")):
            with self.subTest(pattern=pattern):
                manifest = json.loads((h.output / "manifest.json").read_bytes())
                original = (h.output / "episodes.ndjson").read_bytes()
                rewrite(h, manifest, "episodes.ndjson", mutate)
                with self.assertRaisesRegex(ProtocolError, pattern):
                    aggregate_reader.validate(h.output, snapshot, manifest, h.strategy.strategy)
                (h.output / "episodes.ndjson").write_bytes(original)


class KillPriceTests(unittest.TestCase):
    @staticmethod
    def value(fee):
        def value(steps, legs):
            cost = sum(leg.cost for leg in legs)
            charge = sum(fee * quantity * price * (P - price) for leg in legs for price, quantity in leg.taken)
            return steps * P * P - cost * P - charge // P
        return value

    def test_binary_search_matches_brute_force_on_the_price_grid(self):
        rng = random.Random(11)
        for _ in range(150):
            units = (rng.choice((1, 2, 5)), rng.choice((1, 3)))
            ladders = []
            for _ in units:
                price, levels = rng.randrange(100, 500), []
                for _ in range(rng.randrange(1, 4)):
                    price += rng.randrange(0, 80)
                    levels.append((min(price, P), rng.randrange(1, 30)))
                ladders.append(tuple(levels))
            value = self.value(rng.choice((0, 7, 70)))
            (fill,) = walk_basket(tuple(ladders), units, targets=(rng.randrange(1, 6),), value=value)
            if not fill.steps:
                continue
            for leg in range(2):
                quantity = fill.legs[leg].filled_atoms

                def positive(price):
                    single = Fill(quantity, price * quantity, False, ((price, quantity),),
                                  ((price, quantity),))
                    trial = fill.legs[:leg] + (single,) + fill.legs[leg + 1:]
                    return value(fill.steps, trial) > 0

                expected = next((p for p in range(P + 1) if not positive(p)), None)
                self.assertEqual(kill_price(value, fill.steps, fill.legs, leg, P), expected)

    def test_sell_binary_search_matches_brute_force_on_the_price_grid(self):
        rng = random.Random(17)
        checked = 0
        for _ in range(150):
            units = (rng.choice((1, 2, 5)), rng.choice((1, 3)))
            ladders = []
            for _ in units:
                price, levels = rng.randrange(500, 900), []
                for _ in range(rng.randrange(1, 4)):
                    price -= rng.randrange(0, 80)
                    levels.append((max(price, 0), rng.randrange(1, 30)))
                ladders.append(tuple(levels))
            fee = rng.choice((0, 7, 70))

            def value(steps, legs):   # sell a complete set: proceeds less fees, less the payout
                charge = sum(fee * q * p * (P - p) for leg in legs for p, q in leg.taken)
                return sum(leg.cost for leg in legs) * P - charge // P - steps * P * P

            (fill,) = walk_basket(tuple(ladders), units, targets=(rng.randrange(1, 6),), value=value)
            if not fill.steps:
                continue
            for leg in range(2):
                quantity = fill.legs[leg].filled_atoms

                def positive(price):
                    single = Fill(quantity, price * quantity, False, ((price, quantity),),
                                  ((price, quantity),))
                    return value(fill.steps, fill.legs[:leg] + (single,) + fill.legs[leg + 1:]) > 0

                expected = next((p for p in range(P, -1, -1) if not positive(p)), None)
                self.assertEqual(kill_price(value, fill.steps, fill.legs, leg, P, sell=True), expected)
                checked += 1
        self.assertGreater(checked, 100)

    def test_sell_edge_walk_matches_brute_force_on_descending_bids(self):
        rng = random.Random(23)
        for _ in range(200):
            units = (rng.choice((1, 2)), rng.choice((1, 3)))
            ladders = []
            for _ in units:
                price, levels = rng.randrange(450, 800), []
                for _ in range(rng.randrange(1, 5)):
                    levels.append((price, rng.randrange(1, 20)))
                    price -= rng.randrange(1, 60)
                ladders.append(tuple(levels))
            fee = rng.choice((0, 7, 70))

            def value(steps, legs):
                charge = sum(fee * q * p * (P - p) for leg in legs for p, q in leg.taken)
                return sum(leg.cost for leg in legs) * P - charge // P - steps * P * P

            (edge,) = walk_basket(tuple(ladders), units, value=value, edge=True)
            limit = min(sum(q for _, q in ladder) // unit for ladder, unit in zip(ladders, units))
            values = [0] + [walk_basket(tuple(ladders), units, targets=(n,), value=value)[0].value
                            for n in range(1, limit + 1)]
            best = max(values)
            self.assertEqual(edge.steps, values.index(best))
            self.assertEqual(edge.value, best if edge.steps else None)

    def test_result_satisfies_the_threshold_property_even_when_value_is_not_monotone(self):
        legs = (Fill(3, 1_500, False, ((500, 3),), ((500, 3),)), Fill(3, 900, False, ((300, 3),), ((300, 3),)))

        def bumpy(steps, trial):
            price = trial[0].taken[0][0]
            return None if 700 <= price < 720 else (-1 if price >= 900 or price % 97 == 0 else 1)

        kill = kill_price(bumpy, 3, legs, 0, P)
        self.assertTrue(bumpy(3, (Fill(3, 3 * kill, False, ((kill, 3),), ((kill, 3),)), legs[1])) in (None, -1))
        self.assertEqual(bumpy(3, (Fill(3, 3 * (kill - 1), False, ((kill - 1, 3),), ((kill - 1, 3),)),
                                   legs[1])), 1)
        self.assertIsNone(kill_price(lambda steps, trial: 1, 3, legs, 1, P))


class FillBoundTests(unittest.TestCase):
    def test_retained_ladders_and_live_fills_are_charged_conservatively(self):
        ladder_levels = tuple((10**6 + i, 10**12 + i) for i in range(400))
        fill = Fill(10**15, 10**30, False, ladder_levels[:8], ladder_levels[:8])
        fills = {"bid": {1: fill}, "ask": {1: fill}}
        transformed = {"kalshi_complement_ask": {1: fill}}
        ladders = {"bid": ladder_levels, "ask": ladder_levels,
                   "kalshi_complement_ask": tuple((p + 1, q) for p, q in ladder_levels)}
        levels = 3 * 16 + sum(len(ladder) for ladder in ladders.values())
        view = BookView("usable", None, 10**18, True, True, ladder_levels[0], ladder_levels[0],
                        fills, transformed, levels, ladders)
        self.assertGreaterEqual(bounds.view_cost(view, 1), deep_size(view))

        deep = tuple(tuple((100 + i, 10**12 + i) for i in range(64)) for _ in range(2))
        policy_value = {"version": 1, "step_contracts": "1", "max_levels": 64, "sizings": [
            {"name": "edge", "role": "governs", "edge": True},
            {"name": "t2", "role": "governs", "target_contracts": "2"},
            {"name": "t3", "role": "records", "target_contracts": "3"}]}
        priced = fill_mode.price(
            fill_policy(policy_value, KIND), FillSpec(("ask", "kalshi_complement_ask"), (10**12, 10**12)),
            deep, (10**18, 10**18), lambda steps, legs: steps * 10**18 - sum(l.cost for l in legs))
        self.assertEqual(priced.state, fill_mode.FILL_LIVE)
        self.assertGreaterEqual(bounds.live_fill_cost(priced), deep_size(priced))
        state = [10**18, fill_mode.FILL_NONPOSITIVE, (2**40, 2**40), 10**6]
        self.assertGreaterEqual(bounds.fill_state_cost(2), deep_size(state))


if __name__ == "__main__":
    unittest.main()
