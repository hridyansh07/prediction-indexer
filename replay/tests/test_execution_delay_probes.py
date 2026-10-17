"""Offline causal delayed fills through preparation, wire v3, SDK and its reader."""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from replay.economic_sdk import (BookRequirement, Requirements, Strategy, FillSpec, Observation,
                                  EVALUATED, ONE_SIDED, UNUSABLE, factory, bounds)
from replay.economic_sdk.fills import fill_policy
from replay.economic_sdk.probes import MS
from replay.economic_sdk.views import quote_ages
from replay.tests.test_streams import fields
from replay.streams.protocol import ProtocolError
from replay.tests.economic_scenarios import M, ladder, operations
from replay.tests.test_economic_sdk_fills import (KIND, LEG0, LEG1, ONE, FillHarness,
                                                  PairFills, policy)
from replay.tests.test_preparation import config
from replay.tests.test_preparation_outcomes import document
from replay.tests.test_economic_sdk import deep_size


def probes(**overrides):
    result = {"version": 1, "sizings": ["one"], "confirm_ms": [0], "arrival_ms": [5],
              "sequencing": ["simultaneous"], "unknown_taker_delay_ms": {"polymarket": 0}}
    return result | overrides


class DelayedPair(PairFills):
    """Synthetic pure terms hook and explicit exit ladder retention."""

    delays = (None, None)

    def fill_delays(self, entity, time_ns):
        return self.delays

    def requirements(self, snapshot, policy):
        return Requirements({key: BookRequirement(("bid", "ask"), (1,),
                                                   ladders=("bid", "ask")) for key in self.plans})


class ComplementPair(DelayedPair):
    """Both Kalshi orientations buy their counterpart's projected ask."""

    def requirements(self, snapshot, policy):
        transforms = ("kalshi_complement_ask", "kalshi_complement_bid")
        return Requirements({key: BookRequirement(("bid", "ask"), (1,), transforms=transforms,
                                                   ladders=("bid", "ask") + transforms)
                             for key in self.plans})

    def baskets(self, snapshot, policy, scope_index):
        from replay.economic_sdk import Basket
        legs = (("kalshi:series", "outcome"), ("kalshi:series", "complement"))
        return (Basket({"name": "complement", "legs": [{"instrument": i, "orientation": o}
                                                        for i, o in legs]}, legs, (0,)),)

    def fill_spec(self, entity):
        return FillSpec(("kalshi_complement_ask",) * 2, self.units)

    def evaluate(self, entity, views, context):
        if any(view.validity != "usable" for view in views):
            return Observation(UNUSABLE)
        levels = [view.ladders["kalshi_complement_ask"] for view in views]
        if any(not ladder for ladder in levels):
            return Observation(ONE_SIDED)
        quotes = tuple((ladder[0],) for ladder in levels)
        edge = 1000 - sum(leg[0][0] for leg in quotes)
        if edge <= 0:
            return Observation(EVALUATED, value_class="NONPOSITIVE")
        return Observation(EVALUATED, value_class="POSITIVE", predicates=frozenset({KIND}),
                           payload={"edge": str(edge)}, quotes=quotes)


class MixedPair(DelayedPair):
    def fill_spec(self, entity):
        return FillSpec(("ask", "bid"), self.units)

    def fill_value(self, entity, steps, legs, time):
        return legs[1].cost - legs[0].cost - steps * self.units[0] * 20

    def evaluate(self, entity, views, context):
        if any(view.validity != "usable" for view in views):
            return Observation(UNUSABLE)
        best = (views[0].best_ask, views[1].best_bid)
        if any(level is None for level in best):
            return Observation(ONE_SIDED)
        edge = best[1][0] - best[0][0]
        if edge <= 0:
            return Observation(EVALUATED, value_class="NONPOSITIVE")
        return Observation(EVALUATED, value_class="POSITIVE", predicates=frozenset({KIND}),
                           payload={"edge": str(edge)}, quotes=tuple((level,) for level in best))

    def check_quotes(self, quotes, values, entity):
        edge = int(quotes[1][0][0]) - int(quotes[0][0][0])
        if edge != int(values["edge"]):
            raise ProtocolError("synthetic mixed quotes")


def milliseconds_config():
    value = config()
    for item in (value, *value["occurrences"]):
        for field in ("start_ns", "end_ns"):
            item[field] = str(int(item[field]) * MS)
    return value


class ProbeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.count = 0

    def harness(self, probe=None, *sizings, delays=(None, None), terms=None,
                strategy_class=DelayedPair, mixed=False, scopes=False, fill_overrides=None, **overrides):
        self.count += 1
        root = Path(self.tmp.name) / str(self.count)
        root.mkdir()
        class FixedPair(strategy_class):
            pass
        FixedPair.delays = delays
        if terms is not None:
            FixedPair.fill_delays = Strategy.fill_delays
        value = policy(*(sizings or (ONE,)), fills={"probes": probe or probes()} | (fill_overrides or {}),
                       **overrides)
        build = factory(FixedPair)
        def tracked(context):
            runtime = build(context)
            for writer in runtime.writers.values():
                self.addCleanup(writer.stream.close)
            return runtime
        from replay.preparation import prepare
        def prepared(config, directory, **kwargs):
            if scopes:
                for occurrence in config["occurrences"]:
                    for field in ("start_ns", "end_ns"):
                        if int(occurrence[field]) < MS:
                            occurrence[field] = str(int(occurrence[field]) * MS)
            return prepare(config, directory, outcomes=(lambda _: terms) if terms else None, **kwargs)
        with patch("replay.tests.test_bundle_coverage.config", side_effect=milliseconds_config), \
             patch("replay.tests.test_bundle_coverage.prepare", side_effect=prepared), \
             patch("replay.tests.test_economic_sdk_fills.build", tracked):
            h = FillHarness(root, value, mixed=mixed, scopes=scopes)
        for writer in h.strategy.writers.values():
            self.addCleanup(writer.stream.close)
        h.window(end=40 * MS)
        return h

    def book(self, h, time, instrument, *asks, bids=((300, 10 * M),)):
        return ladder(h, time * MS, instrument, asks=asks, bids=bids)

    def open(self, h, quantity=2 * M):
        self.book(h, 12, LEG0, (400, quantity))
        self.book(h, 12, LEG1, (500, quantity))

    def row(self, h, index=0):
        h.finish()
        return h.rows("probes.ndjson")[index]

    def test_zero_arrival_is_the_opening_fill(self):
        h = self.harness(probes(arrival_ms=[0]))
        self.open(h)
        row = self.row(h)
        fill = h.rows("episodes.ndjson")[0]["fill"]
        opening = fill["results"][0]
        self.assertEqual((row["status"], row["matched_steps"], row["value"]),
                         ("filled", opening["steps"], opening["value"]))
        self.assertEqual([leg["consumed"] for leg in row["legs"]],
                         [leg["taken"] for leg in opening["legs"]])
        self.assertEqual(row["decision"]["limits"], ["400", "500"])
        self.assertEqual(row["decision"]["ages"], fill["ages"])

    def test_depth_eaten_is_partial_and_unwinds_the_excess(self):
        two = {"name": "two", "role": "governs", "target_contracts": "2"}
        h = self.harness(probes(sizings=["two"]), two)
        self.open(h)
        self.book(h, 15, LEG1, (500, M))
        row = self.row(h)
        self.assertEqual((row["status"], row["matched_steps"]), ("partial", "1"))
        self.assertEqual((row["legs"][0]["excess_atoms"], row["legs"][0]["exit_consumed"]),
                         (str(M), [["300", str(M)]]))
        self.assertEqual(row["leg_loss_gross"], str(100 * M))

    def test_polymarket_thin_exit_stops_at_cap_and_preserves_unexited_cost(self):
        two = {"name": "two", "role": "governs", "target_contracts": "2"}
        h = self.harness(probes(sizings=["two"]), two, fill_overrides={"max_levels": 2})
        self.book(h, 12, LEG0, (400, 2 * M), bids=((300, M // 4), (200, M // 4), (100, M)))
        self.book(h, 12, LEG1, (500, 2 * M))
        self.book(h, 15, LEG1, (500, M))
        row = self.row(h)  # Includes independent aggregate-reader validation.
        self.assertEqual((row["status"], row["matched_steps"]), ("partial", "1"))
        self.assertEqual((row["legs"][0]["excess_atoms"], row["legs"][0]["exit_consumed"],
                          row["legs"][0]["exit_proceeds"], row["leg_loss_gross"]),
                         (str(M), [["300", str(M // 4)], ["200", str(M // 4)]],
                          str(125 * M), str(275 * M)))

    def test_prices_beyond_limit_are_empty_and_better_prices_are_taken(self):
        for price, status, matched in ((410, "empty", "0"), (390, "filled", "1")):
            with self.subTest(price=price):
                h = self.harness()
                self.open(h)
                self.book(h, 15, LEG0, (price, 2 * M))
                row = self.row(h)
                self.assertEqual((row["status"], row["matched_steps"]), (status, matched))
                self.assertEqual(row["legs"][0]["consumed"],
                                 [] if not int(matched) else [[str(price), str(M)]])

    def test_observed_and_default_delays_are_distinct(self):
        h = self.harness(probes(arrival_ms=[1], unknown_taker_delay_ms={"polymarket": 4}),
                         delays=(2 * MS, None))
        self.open(h)
        row = self.row(h)
        self.assertEqual([(leg["venue_delay_ms"], leg["delay_source"], leg["arrival_ns"])
                          for leg in row["legs"]], [(2, "terms", str(15 * MS)),
                                                    (4, "default", str(17 * MS))])

    def test_default_hook_reads_pinned_v2_market_terms_for_native_tokens_at_decision(self):
        terms = document()
        terms["version"] = 2
        for market in terms["markets"]:
            market["execution_terms"] = {"status": "unavailable", "changes": []}
            if market["market_id"] == "polymarket:series":
                market["execution_terms"] = {"status": "observed", "changes": [
                    {"from": "1970-01-01T00:00:00.011Z", "taker_delay_ms": 1},
                    {"from": "1970-01-01T00:00:00.016Z", "taker_delay_ms": 3}]}
        h = self.harness(probes(confirm_ms=[0, 5], arrival_ms=[1],
                               unknown_taker_delay_ms={"polymarket": 999}), terms=terms)
        self.open(h)
        h.finish()
        rows = h.rows("probes.ndjson")
        self.assertEqual([(r["confirm_ms"], [leg["venue_delay_ms"] for leg in r["legs"]],
                           [leg["arrival_ns"] for leg in r["legs"]]) for r in rows],
                         [(0, [1, 1], [str(14 * MS)] * 2),
                          (5, [3, 3], [str(21 * MS)] * 2)])
        self.assertTrue(all(leg["delay_source"] == "terms" for row in rows for leg in row["legs"]))
        # Reuse the same resolved entity in non-monotone order. A cached
        # time-selected term would incorrectly keep either 1 or 3 ms here.
        entity = next(iter(h.strategy.entities.values()))
        hook = h.strategy.strategy.fill_delays
        for time, delay in ((16, 3 * MS), (15, MS), (10, None), (40, None), (12, MS)):
            self.assertEqual(hook(entity, time * MS), (delay, delay))

    def test_c0_recording_sizing_prices_its_opening_walk_even_when_untradeable(self):
        record = {"name": "record", "role": "records", "target_contracts": "3"}
        h = self.harness(probes(sizings=["record"], arrival_ms=[0]), ONE, record)
        self.open(h)
        row = self.row(h)
        result = h.rows("episodes.ndjson")[0]["fill"]["results"][1]
        self.assertFalse(result["tradeable"])
        self.assertEqual((row["status"], row["matched_steps"], row["value"]),
                         ("filled", result["steps"], result["value"]))

    def test_c0_zero_step_recording_sizing_is_empty_with_no_orders(self):
        governing = {"name": "two", "role": "governs", "target_contracts": "2"}
        record = {"name": "record", "role": "records", "edge": True}
        h = self.harness(probes(sizings=["record"], arrival_ms=[0]), governing, record)
        # The mechanism allows a pure strategy valuation with a nonpositive
        # first step and a positive fixed target; the edge stops at zero.
        h.strategy.strategy.fill_value = lambda entity, steps, legs, time: 1 if steps >= 2 else 0
        self.book(h, 12, LEG0, (400, M), (410, M))
        self.book(h, 12, LEG1, (500, M), (510, M))
        row = self.row(h)
        self.assertEqual((row["status"], row["matched_steps"], row["value"]), ("empty", "0", "0"))
        self.assertTrue(all(leg["sent_ns"] is None and leg["arrival_ns"] is None for leg in row["legs"]))

    def test_slow_first_sends_other_leg_at_first_arrival_with_floored_size(self):
        two = {"name": "two", "role": "governs", "target_contracts": "2"}
        h = self.harness(probes(sizings=["two"], arrival_ms=[1], sequencing=["slow_first"]),
                         two, delays=(4 * MS, 0))
        self.open(h)
        self.book(h, 15, LEG0, (400, M + M // 2))
        row = self.row(h)
        first, second = row["legs"]
        self.assertEqual((first["sent_ns"], first["arrival_ns"]), (str(12 * MS), str(17 * MS)))
        self.assertEqual((second["sent_ns"], second["arrival_ns"], second["filled_atoms"]),
                         (str(17 * MS), str(18 * MS), str(M)))
        self.assertEqual((row["status"], row["matched_steps"], first["excess_atoms"]),
                         ("partial", "1", str(M // 2)))

    def test_slow_first_empty_does_not_send_other_legs(self):
        h = self.harness(probes(sequencing=["slow_first"]), delays=(2 * MS, 0))
        self.open(h)
        self.book(h, 15, LEG0, (410, 2 * M))
        row = self.row(h)
        self.assertEqual(row["status"], "empty")
        self.assertEqual((row["legs"][1]["sent_ns"], row["legs"][1]["arrival_ns"],
                          row["legs"][1]["filled_atoms"]), (None, None, "0"))

    def test_all_nonfill_statuses_and_confirmation_exclusive_episode_end(self):
        h = self.harness(probes(confirm_ms=[5]))
        self.open(h)
        self.book(h, 17, LEG0, (600, 2 * M))
        self.assertEqual(self.row(h)["status"], "not_confirmed")

        record = {"name": "record", "role": "records", "target_contracts": "2"}
        h = self.harness(probes(sizings=["record"], confirm_ms=[5]), ONE, record)
        self.open(h)
        self.book(h, 16, LEG1, (500, M))  # Live governing 1; recording 2 no longer tradeable.
        self.assertEqual(self.row(h)["status"], "decision_untradeable")

        h = self.harness()
        self.open(h)
        ladder(h, 15 * MS, LEG0, why={"kind": "connection_closed"})
        self.assertEqual(self.row(h)["status"], "unusable")

        h = self.harness(probes(arrival_ms=[30]))
        self.open(h)
        row = self.row(h)
        self.assertEqual((row["status"], row["matched_steps"]), ("censored", None))

    def test_a_later_cut_cannot_change_an_overdue_arrival_and_probe_outlives_episode(self):
        h = self.harness(probes(arrival_ms=[5]))
        self.open(h)
        self.book(h, 13, LEG0, (410, 2 * M))  # Closes fill, still before arrival.
        self.book(h, 20, LEG0, (390, 2 * M))  # Would fill if a later book leaked backwards.
        row = self.row(h)
        self.assertEqual((row["status"], row["legs"][0]["arrival_ns"]), ("empty", str(17 * MS)))

    def test_full_and_sparse_v4_tapes_have_identical_probe_bytes_without_new_boundaries(self):
        def run(full):
            h = self.harness(probes(confirm_ms=[0, 2], arrival_ms=[0, 1, 5]))
            self.open(h)
            risk_sequence = h.seq - 1

            def send(kind, body):
                nonlocal risk_sequence
                if kind == "cut":
                    risk_sequence += 1
                    body = {**body, "risk_sequence": str(risk_sequence)}
                elif kind == "terminal":
                    body = {**body, "risk_cuts": str(risk_sequence)}
                cut = h.decoder.apply(fields({
                    "version": "4", "run_id": "coverage-test",
                    "attempt_id": h.context["attempt_id"], "sequence": str(h.seq),
                    "kind": kind, "body": body}))
                h.seq += 1
                h.strategy(cut)
                return cut

            h.send = send
            for time in (13, 14, 15, 17, 18):
                if full:
                    ref = h.ref(time * MS)
                    h.send("cut", {"origin": {"kind": "group", "pin": h.pin,
                                                "first": ref["address"], "last": ref["address"],
                                                "visible_ns": str(time * MS)},
                                   "market_events": [], "book_transitions": []})
                else:
                    # Omitted wire entries still count as the same Risk cuts.
                    risk_sequence += 1
            self.book(h, 20, LEG0, (450, 2 * M))
            h.finish()
            self.assertEqual(h.strategy.risk_sequence, risk_sequence + 1)
            self.assertEqual(risk_sequence, 9)
            self.assertEqual(h.initial["retain_boundaries_ns"], [str(10 * MS), str(40 * MS)])
            self.assertEqual(h.initial["retain_shift_offsets_ns"], [])
            return (h.output / "probes.ndjson").read_bytes()
        self.assertEqual(run(True), run(False))

    def test_shuffling_same_time_independent_updates_preserves_bytes(self):
        outputs = []
        for reverse in (False, True):
            h = self.harness(probes(arrival_ms=[0, 5], sequencing=["simultaneous", "slow_first"]))
            first = (LEG0, (400, 2 * M))
            second = (LEG1, (500, 2 * M))
            for instrument, level in ((second, first) if reverse else (first, second)):
                self.book(h, 12, instrument, level)
            h.finish()
            outputs.append((h.output / "probes.ndjson").read_bytes())
        self.assertEqual(*outputs)

    def test_quote_ages_measure_book_updates_and_committed_best_changes(self):
        h = self.harness(probes(confirm_ms=[5], arrival_ms=[0]))
        self.open(h)
        operations(h, 14 * MS, LEG0, "outcome", "ask", 600, 2 * M)  # Best unchanged.
        self.book(h, 15, LEG1, (510, 2 * M))
        self.book(h, 15, LEG1, (500, 2 * M))  # Same-time changed then restored best.
        row = self.row(h)
        self.assertEqual(row["decision"]["ages"], [
            {"book_ns": str(3 * MS), "best_ns": str(5 * MS)},
            {"book_ns": str(2 * MS), "best_ns": str(5 * MS)}])

    def test_pending_probe_bound_fails_at_open(self):
        h = self.harness(probes(arrival_ms=[0, 1]))
        self.open(h)
        with patch.object(bounds, "MAX_PENDING_PROBES", 1):
            with self.assertRaisesRegex(ProtocolError, "pending probe bound"):
                self.book(h, 13, LEG0, (400, 2 * M))

    def test_probe_state_is_conservatively_reserved_and_survives_episode_close(self):
        sizing = {"name": "large", "role": "governs", "target_contracts": "64"}
        h = self.harness(probes(sizings=["large"]), sizing, delays=(0, 20 * MS),
                         fill_overrides={"max_levels": 64})
        asks0 = tuple((100 + i, M) for i in range(64))
        asks1 = tuple((200 + i, M) for i in range(64))
        exit_ladder = tuple((80 - i, M) for i in range(65))
        self.book(h, 12, LEG0, *asks0, bids=exit_ladder)
        self.book(h, 12, LEG1, *asks1, bids=exit_ladder)
        self.book(h, 18, LEG0, *asks0, bids=exit_ladder)  # Leg 0 arrived at 17.
        manager = h.strategy.probes
        retained = next(entry[-1] for entry in manager.heap if not entry[-1].finished)
        self.assertGreaterEqual(retained.cost, deep_size(retained))
        ladder(h, 20 * MS, LEG1, why={"kind": "connection_closed"})
        self.book(h, 21, LEG0, *asks0, bids=exit_ladder)
        self.assertFalse(h.strategy.episodes)
        self.assertEqual(manager.pending, 1)
        self.assertGreaterEqual(retained.cost, deep_size(retained))
        h.finish()

    def test_probe_state_budget_fails_before_a_new_probe_is_retained(self):
        h = self.harness()
        self.open(h)
        self.book(h, 13, LEG0, (400, 2 * M))
        manager = h.strategy.probes
        episode = next(iter(h.strategy.episodes.values()))
        entity = h.strategy.entities[episode.entity]
        spec, maxima = h.strategy.fill_specs[episode.entity]
        h.strategy.budget.limit = h.strategy.budget.used
        previous = manager.pending
        with self.assertRaisesRegex(ProtocolError, "pending probe state budget"):
            manager.add("new", entity, spec, maxima, episode.start, 40 * MS,
                        episode.fill, episode.fill.json["ages"])
        self.assertEqual(manager.pending, previous)
        self.assertNotIn("new", manager.episodes)

    def test_an_arrival_that_needs_more_than_max_levels_fails_without_truncation(self):
        h = self.harness(fill_overrides={"max_levels": 1})
        self.open(h)
        self.book(h, 15, LEG0, (390, M // 2), (400, M // 2))
        with self.assertRaisesRegex(ProtocolError, "probe consumed level bound"):
            h.finish()

    def test_kalshi_complement_excess_exits_through_the_projected_bid(self):
        two = {"name": "two", "role": "governs", "target_contracts": "2"}
        h = self.harness(probes(sizings=["two"], unknown_taker_delay_ms={"kalshi": 0}), two,
                         strategy_class=ComplementPair, mixed=True)
        ladder(h, 12 * MS, "kalshi:series", "outcome", bids=((600, 2 * M),), asks=((700, 2 * M),))
        ladder(h, 12 * MS, "kalshi:series", "complement", bids=((600, 2 * M),), asks=((700, 2 * M),))
        ladder(h, 15 * MS, "kalshi:series", "complement", bids=((600, M),), asks=((700, 2 * M),))
        row = self.row(h)
        self.assertEqual((row["status"], row["matched_steps"], row["leg_loss_gross"]),
                         ("partial", "1", str(100 * M)))
        self.assertEqual(row["legs"][0]["exit_consumed"], [["300", str(M)]])

    def test_mixed_buy_and_sell_sources_use_opposite_limit_directions(self):
        h = self.harness(strategy_class=MixedPair)
        self.book(h, 12, LEG0, (400, M))
        self.book(h, 12, LEG1, (700, M), bids=((600, M),))
        self.book(h, 15, LEG0, (390, M))
        self.book(h, 15, LEG1, (700, M), bids=((610, M),))
        row = self.row(h)
        self.assertEqual((row["status"], row["value"]), ("filled", str(200 * M)))
        self.assertEqual([leg["consumed"] for leg in row["legs"]],
                         [[["390", str(M)]], [["610", str(M)]]])

    def test_exact_scope_and_run_end_arrivals_are_censored_with_partial_evidence(self):
        for scopes, arrival, boundary in ((True, 11, 23), (False, 28, 40)):
            with self.subTest(scopes=scopes):
                h = self.harness(probes(arrival_ms=[arrival]), scopes=scopes,
                                 delays=(None, None))
                self.open(h)
                if scopes:
                    self.book(h, 24, LEG0, (400, 2 * M))
                row = self.row(h)
                self.assertEqual(row["status"], "censored")
                self.assertTrue(all(leg["arrival_ns"] == str(boundary * MS)
                                    and leg["filled_atoms"] is None for leg in row["legs"]))
                self.assertIsNone(row["matched_steps"])

        h = self.harness(probes(arrival_ms=[27]), delays=(0, MS))
        self.open(h)
        row = self.row(h)
        self.assertEqual(row["status"], "censored")
        self.assertEqual((row["legs"][0]["arrival_ns"], row["legs"][0]["filled_atoms"]),
                         (str(39 * MS), str(M)))
        self.assertEqual((row["legs"][1]["arrival_ns"], row["legs"][1]["filled_atoms"]),
                         (str(40 * MS), None))

    def test_scope_censor_releases_pending_capacity_before_new_scope_opening(self):
        h = self.harness(probes(arrival_ms=[11]), scopes=True)
        self.open(h)
        with patch.object(bounds, "MAX_PENDING_PROBES", 1):
            self.book(h, 24, LEG0, (400, 2 * M))
            h.finish()
        self.assertEqual([r["status"] for r in h.rows("probes.ndjson")], ["censored", "filled"])

    def test_sold_excess_unwind_is_signed_and_matched_keeps_highest_sales(self):
        two = {"name": "two", "role": "governs", "target_contracts": "2"}
        for repurchase, quantity, expected in ((750, M, 100 * M), (600, M, -50 * M),
                                                (None, 0, 350 * M), (750, M // 4, 575 * M // 2)):
            with self.subTest(repurchase=repurchase):
                h = self.harness(probes(sizings=["two"]), two, side="sell")
                asks = () if repurchase is None else ((repurchase, quantity),)
                self.book(h, 12, LEG0, *asks, bids=((700, M), (650, M)))
                self.book(h, 12, LEG1, (800, 2 * M), bids=((550, 2 * M),))
                self.book(h, 15, LEG1, (800, 2 * M), bids=((550, M),))
                row = self.row(h)
                self.assertEqual((row["status"], row["matched_steps"]), ("partial", "1"))
                # The matched sale is 700, so the excess sale is 650.
                self.assertEqual(row["value"], str((700 + 550 - 1010) * M))
                self.assertEqual(row["legs"][0]["excess_atoms"], str(M))
                self.assertEqual(row["legs"][0]["exit_consumed"],
                                 [] if repurchase is None else [[str(repurchase), str(quantity)]])
                self.assertEqual(row["legs"][0]["exit_proceeds"],
                                 "0" if repurchase is None else str(repurchase * quantity))
                self.assertEqual(row["leg_loss_gross"], str(expected))

    def test_all_four_retained_ladders_keep_committed_best_change_timestamps(self):
        h = self.harness(probes(unknown_taker_delay_ms={"kalshi": 0}),
                         strategy_class=ComplementPair, mixed=True)
        for orientation in ("outcome", "complement"):
            ladder(h, 12 * MS, "kalshi:series", orientation,
                   bids=((600, 2 * M),), asks=((700, 2 * M),))
        key = ("kalshi:series", "outcome")
        operations(h, 14 * MS, *key, "ask", 800, M)  # Deep update, no best change.
        ladder(h, 15 * MS, *key, bids=((600, 3 * M),), asks=((700, 2 * M), (800, M)))
        ladder(h, 16 * MS, *key, bids=((600, 3 * M),), asks=((690, 2 * M),))
        ladder(h, 16 * MS, *key, bids=((600, 3 * M),), asks=((700, 2 * M),))
        ladder(h, 16 * MS + 1, LEG0, asks=((400, M),))  # Commit the restored best.
        self.assertEqual(h.strategy.views[key].best_changes, {
            "bid": 15 * MS, "ask": 12 * MS,
            "kalshi_complement_ask": 15 * MS, "kalshi_complement_bid": 12 * MS})
        ladder(h, 17 * MS, *key, bids=((600, 3 * M),), asks=((700, 4 * M),))
        ladder(h, 18 * MS, *key, bids=((610, 3 * M),), asks=((700, 4 * M),))
        ladder(h, 19 * MS, *key, bids=((600, 3 * M),), asks=((700, 4 * M),))
        self.book(h, 20, LEG0, (400, M))  # Flushes the final Kalshi stage.
        view = h.strategy.views[key]
        expected = {"bid": 19 * MS, "ask": 17 * MS,
                    "kalshi_complement_ask": 19 * MS, "kalshi_complement_bid": 17 * MS}
        self.assertEqual(view.best_changes, expected)
        for source, changed in expected.items():
            self.assertEqual(quote_ages((source,), (view,), 20 * MS),
                             [{"book_ns": str(MS), "best_ns": str(20 * MS - changed)}])
        self.assertGreaterEqual(bounds.view_cost(view, 1), deep_size(view))
        self.assertGreaterEqual(bounds.age_commit_cost(4), deep_size(h.strategy.age_commits[key]))
        h.finish()

    def test_age_timestamps_count_declared_transform_and_exit_ladders(self):
        class TransformOnly(ComplementPair):
            def requirements(self, snapshot, policy):
                transforms = ("kalshi_complement_ask", "kalshi_complement_bid")
                return Requirements({key: BookRequirement(("bid", "ask"), (1,), transforms=transforms,
                                                           ladders=transforms) for key in self.plans})
        h = self.harness(probes(unknown_taker_delay_ms={"kalshi": 0}),
                         strategy_class=TransformOnly, mixed=True)
        for orientation in ("outcome", "complement"):
            ladder(h, 12 * MS, "kalshi:series", orientation,
                   bids=((600, 2 * M),), asks=((700, 2 * M),))
        self.book(h, 13, LEG0, (400, M))
        view = h.strategy.views["kalshi:series", "outcome"]
        self.assertEqual(set(view.best_changes), {"kalshi_complement_ask", "kalshi_complement_bid"})
        self.assertEqual(set(view.ladders), {"ask", "bid", *view.best_changes})
        for count in range(5):
            self.assertGreaterEqual(bounds.age_timestamps_cost(count), (1 + count) * bounds.INT)
            if count:
                self.assertGreaterEqual(bounds.age_timestamps_cost(count) - bounds.age_timestamps_cost(count - 1),
                                        bounds.INT)
        self.assertGreaterEqual(bounds.view_cost(view, 1), deep_size(view))
        h.finish()

    def test_missing_exit_and_defaults_fail_closed(self):
        with self.assertRaisesRegex(ProtocolError, "missing venue"):
            self.harness(probes(unknown_taker_delay_ms={}))
        value = policy(fills={"probes": probes()})
        root = Path(self.tmp.name) / "missing-exit"
        root.mkdir()
        build = factory(PairFills)
        def tracked(context):
            runtime = build(context)
            for writer in runtime.writers.values():
                self.addCleanup(writer.stream.close)
            return runtime
        with patch("replay.tests.test_bundle_coverage.config", side_effect=milliseconds_config), \
             patch("replay.tests.test_economic_sdk_fills.build", tracked):
            with self.assertRaisesRegex(ProtocolError, "exit ladder not retained"):
                FillHarness(root, value)


class ProbePolicyTests(unittest.TestCase):
    def test_manually_constructed_probe_grid_cannot_bypass_the_hard_product_bound(self):
        from dataclasses import replace
        from replay.economic_sdk.fills import check_experiment
        from replay.economic_sdk.types import ProbePolicy
        invalid = ProbePolicy(("one",), tuple(range(8)), tuple(range(8)),
                              ("simultaneous", "slow_first"), {"polymarket": 0})
        parsed = fill_policy(policy()["fills"], KIND)
        from replay.economic_sdk.types import Experiment
        experiment = Experiment("test", {}, "", "", (), (), (KIND,), {}, (), (), (), (), (), ())
        experiment = replace(experiment, fills=replace(parsed, probes=invalid))
        with self.assertRaisesRegex(ProtocolError, "probe product bound"):
            check_experiment(experiment)

    def test_closed_grid_rejects_bad_shapes_and_oversized_products(self):
        invalid = [{"extra": True}, {"confirm_ms": [1, 0]}, {"arrival_ms": [0, 0]},
                   {"confirm_ms": [True]}, {"sizings": ["undeclared"]},
                   {"sequencing": ["unknown"]}, {"unknown_taker_delay_ms": {"polymarket": -1}},
                   {"confirm_ms": list(range(8)), "arrival_ms": list(range(8)),
                    "sequencing": ["simultaneous", "slow_first"]}]
        for change in invalid:
            with self.subTest(change=change):
                value = policy(fills={"probes": probes(**change)})
                with self.assertRaises(ProtocolError):
                    fill_policy(value["fills"], KIND)
