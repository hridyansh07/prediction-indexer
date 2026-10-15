"""Contract-shaped probe rows checked independently of the probe scheduler."""

import copy
from dataclasses import replace
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from replay.economic_sdk.fills import fill_policy
from replay.economic_sdk.probe_reader import validate
from replay.economic_sdk.reader import Budget
from replay.economic_sdk.types import FillSpec
from replay.economic_sdk import BookRequirement, Requirements, aggregate_reader, factory
from replay.preparation import digest, encoded, load_snapshot
from replay.streams.protocol import ProtocolError
from replay.tests.test_economic_sdk_fills import (FillHarness, LEG0, LEG1, ONE, PairFills,
                                                policy as pair_policy, rewrite)
from replay.tests.economic_scenarios import M

MS = 1_000_000
AGES = [{"book_ns": "10", "best_ns": "5"}, {"book_ns": "10", "best_ns": "5"}]


class Value:
    def fill_spec(self, entity):
        return FillSpec(("ask", "ask"), (5, 5))

    def fill_delays(self, entity, time):
        return 25 * MS, None

    def fill_value(self, entity, steps, legs, time):
        return steps * 5 * 990 - sum(leg.cost for leg in legs)


class SoldValue(Value):
    def fill_spec(self, entity):
        return FillSpec(("bid", "ask"), (5, 5))

    def fill_value(self, entity, steps, legs, time):
        return legs[0].cost - legs[1].cost


class ProbeReaderTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.policy = self.make_policy()
        legs = (("polymarket:A", "outcome"), ("polymarket:B", "outcome"))
        entity = SimpleNamespace(id="a" * 64, legs=legs)
        self.ep = {"scope": 0, "kind": "edge", "entity": entity,
                   "start": 100 * MS, "end": 200 * MS, "fill": True,
                   "probe_opening": {"two": {"steps": "2", "limits": ["450", "500"],
                                               "tradeable": True, "ages": AGES}}}
        self.plans = {key: {"venue": "polymarket", "price_scale": "3"} for key in legs}
        self.scopes = [{"end_ns": str(1_000 * MS)}]
        self.strategy = Value()
        self.row = {
            "episode_id": digest([0, entity.id, "edge", str(self.ep["start"])]),
            "sizing": "two", "confirm_ms": 0, "arrival_ms": 0,
            "sequencing": "simultaneous", "status": "partial",
            "decision": {"time_ns": str(100 * MS), "steps": "2", "limits": ["450", "500"],
                         "ages": copy.deepcopy(AGES)},
            "legs": [
                {"sent_ns": str(100 * MS), "arrival_ns": str(125 * MS), "venue_delay_ms": 25,
                 "delay_source": "terms", "usable": True, "filled_atoms": "10", "cost": "4000",
                 "consumed": [["400", "10"]], "excess_atoms": "5",
                 "exit_consumed": [["390", "5"]], "exit_proceeds": "1950"},
                {"sent_ns": str(100 * MS), "arrival_ns": str(140 * MS), "venue_delay_ms": 40,
                 "delay_source": "default", "usable": True, "filled_atoms": "5", "cost": "2400",
                 "consumed": [["480", "5"]], "excess_atoms": "0", "exit_consumed": [],
                 "exit_proceeds": "0"}],
            "matched_steps": "1", "value": "550", "leg_loss_gross": "50"}

    def make_policy(self, **changes):
        probes = {"version": 1, "sizings": ["two"], "confirm_ms": [0], "arrival_ms": [0],
                  "sequencing": ["simultaneous"], "unknown_taker_delay_ms": {"polymarket": 40}}
        probes.update(changes)
        return fill_policy({"version": 1, "step_contracts": "1", "max_levels": 3,
                            "sizings": [{"name": "two", "role": "governs", "target_contracts": "2"}],
                            "probes": probes}, "edge")

    def read(self, rows=None, *, episodes=None):
        rows = [self.row] if rows is None else rows
        payload = b"".join(encoded(row) + b"\n" for row in rows)
        (self.root / "probes.ndjson").write_bytes(payload)
        identity = {"sha256": hashlib.sha256(payload).hexdigest(), "byte_length": len(payload),
                    "records": len(rows)}
        return validate(self.root, identity, [self.ep] if episodes is None else episodes,
                        self.scopes, self.policy, self.strategy, self.plans, Budget())

    def reject(self, mutate, message):
        original = copy.deepcopy(self.row)
        mutate(self.row)
        try:
            with self.assertRaisesRegex(ProtocolError, message):
                self.read()
        finally:
            self.row = original

    def test_partial_better_prices_default_delay_and_gross_unwind_read(self):
        self.read()

    def test_cost_value_excess_and_unwind_are_independently_recomputed(self):
        for field, amount, message in (("cost", "4001", "probe fill cost"),
                                       ("excess_atoms", "4", "probe excess atoms"),
                                       ("exit_proceeds", "1951", "probe exit proceeds")):
            with self.subTest(field=field):
                self.reject(lambda row: row["legs"][0].update({field: amount}), message)
        self.reject(lambda row: row.update(value="551"), "probe matched value")
        self.reject(lambda row: row.update(matched_steps="2"), "probe matched steps")
        self.reject(lambda row: row.update(leg_loss_gross="49"), "probe gross loss")

    def test_limits_best_first_caps_and_quantities_are_closed(self):
        self.reject(lambda row: row["legs"][0].update(consumed=[["451", "10"]]), "probe limit")
        self.reject(lambda row: row["legs"][0].update(consumed=[["400", "5"], ["390", "5"]]),
                    "probe walk best-first")
        self.reject(lambda row: row["legs"][0].update(consumed=[[str(p), "1"] for p in range(400, 404)]),
                    "probe walk level bound")
        self.reject(lambda row: row["legs"][0].update(filled_atoms="11"), "probe filled quantity")
        self.reject(lambda row: row["legs"][0].update(exit_consumed=[["390", "6"]]), "probe exit quantity")

    def test_policy_grid_has_exact_rows_and_no_orphans(self):
        for rows, episodes, message in (([], None, "probe coverage"),
                                        ([self.row, self.row], None, "probe duplicate combination"),
                                        ([self.row], [], "probe orphan episode")):
            with self.subTest(message=message), self.assertRaisesRegex(ProtocolError, message):
                self.read(rows, episodes=episodes)
        self.reject(lambda row: row.update(arrival_ms=1), "probe configured combination")
        self.reject(lambda row: row.update(confirm_ms=True), "probe combination types")

    def test_c0_decision_steps_limits_and_ages_equal_the_opening_fill(self):
        for field, value in (("steps", "1"), ("limits", ["451", "500"]),
                             ("ages", [{"book_ns": "11", "best_ns": "5"}, AGES[1]])):
            with self.subTest(field=field):
                self.reject(lambda row: row["decision"].update({field: value}),
                            "probe opening decision equality")

    def test_send_arrival_and_delay_sources_are_from_the_pure_hook(self):
        for changes, message in (({"sent_ns": str(101 * MS)}, "probe send/arrival time"),
                                  ({"arrival_ns": str(126 * MS)}, "probe send/arrival time"),
                                  ({"delay_source": "default"}, "probe venue delay"),
                                  ({"venue_delay_ms": True}, "probe venue delay")):
            with self.subTest(changes=changes):
                self.reject(lambda row: row["legs"][0].update(changes), message)

    def test_malformed_unknown_and_noncanonical_values_are_rejected(self):
        self.reject(lambda row: row.update(extra=True), "closed schema")
        self.reject(lambda row: row["legs"][0].update(extra=True), "closed schema")
        self.reject(lambda row: row["decision"]["ages"][0].update(extra=True), "closed schema")
        self.reject(lambda row: row["legs"][0].update(filled_atoms="010"), "canonical natural")
        self.reject(lambda row: row.update(value="-0"), "canonical signed")
        self.reject(lambda row: row.update(value="1" * 81), "probe signed integer bound")
        self.reject(lambda row: row.update(status="unknown"), "probe status")
        self.reject(lambda row: row["legs"].__setitem__(1, None), "closed schema")

    def test_status_is_derived_from_leg_evidence(self):
        self.reject(lambda row: row.update(status="filled"), "probe status")
        self.reject(lambda row: row["legs"][0].update(usable=False), "probe unusable fill")
        self.row["legs"][1].update(usable=False, filled_atoms="0", cost="0", consumed=[])
        self.row["legs"][0].update(excess_atoms="10", exit_consumed=[["390", "10"]], exit_proceeds="3900")
        self.row.update(status="unusable", matched_steps="0", value="0", leg_loss_gross="100")
        self.read()
        self.reject(lambda row: row.update(status="empty"), "probe status")

    def test_not_confirmed_and_decision_untradeable_have_no_arrival_metrics(self):
        self.policy = self.make_policy(confirm_ms=[100])
        self.row.update(confirm_ms=100, status="not_confirmed", decision=None, legs=[],
                        matched_steps=None, value=None, leg_loss_gross=None)
        self.read()
        self.reject(lambda row: row.update(matched_steps="0"), "probe skipped metrics")
        self.ep["end"] = 300 * MS
        self.reject(lambda row: None, "probe not_confirmed")
        self.row.update(status="decision_untradeable",
                        decision={"time_ns": str(200 * MS), "steps": "2", "limits": ["450", "500"],
                                  "ages": copy.deepcopy(AGES)})
        self.read()

    def test_c0_recording_walk_is_probed_even_when_its_value_is_not_tradeable(self):
        self.ep["probe_opening"]["two"]["tradeable"] = False
        self.read()
        self.reject(lambda row: row.update(status="decision_untradeable", legs=[],
                                           matched_steps=None, value=None, leg_loss_gross=None),
                    "probe c0 opening decision")

    def test_c0_zero_steps_are_empty_without_sending_any_order(self):
        self.ep["probe_opening"]["two"].update(steps="0", limits=[None, None], tradeable=False)
        self.row["decision"].update(steps="0", limits=[None, None])
        self.row.update(status="empty", matched_steps="0", value="0", leg_loss_gross="0")
        for leg in self.row["legs"]:
            leg.update(sent_ns=None, arrival_ns=None, usable=None, filled_atoms="0", cost="0", consumed=[],
                       excess_atoms="0", exit_consumed=[], exit_proceeds="0")
        self.read()

    def test_censored_retains_completed_walks_but_never_fabricates_matching_or_unwind(self):
        self.scopes[0]["end_ns"] = str(130 * MS)
        self.ep["end"] = 130 * MS
        self.row.update(status="censored", matched_steps=None, value=None, leg_loss_gross=None)
        for leg in self.row["legs"]:
            leg.update(excess_atoms=None, exit_consumed=None, exit_proceeds=None)
        self.row["legs"][1].update(usable=None, filled_atoms=None, cost=None, consumed=None)
        self.read()
        self.reject(lambda row: row["legs"][1].update(filled_atoms="0"), "probe unresolved leg")
        self.reject(lambda row: row["legs"][0].update(excess_atoms="10"), "probe censored unwind")

    def censor(self, *unfinished):
        self.row.update(status="censored", matched_steps=None, value=None, leg_loss_gross=None)
        for index, leg in enumerate(self.row["legs"]):
            leg.update(excess_atoms=None, exit_consumed=None, exit_proceeds=None)
            if index in unfinished:
                leg.update(usable=None, filled_atoms=None, cost=None, consumed=None)

    def test_arrival_at_original_scope_end_is_censored_with_completed_earlier_leg_preserved(self):
        for later_scope in (False, True):
            with self.subTest(later_scope=later_scope):
                self.scopes = [{"end_ns": str(140 * MS)}]
                if later_scope:
                    self.scopes.append({"end_ns": str(1_000 * MS)})
                # Probes outlive their episode, while retaining that episode's
                # original scope bound rather than borrowing the next scope.
                self.ep["end"] = 120 * MS
                self.censor(1)
                self.read()
                self.assertEqual(self.row["legs"][0]["filled_atoms"], "10")

    def test_reader_rejects_fabricated_completion_at_original_scope_end(self):
        self.scopes = [{"end_ns": str(140 * MS)}, {"end_ns": str(1_000 * MS)}]
        self.ep["end"] = 120 * MS
        # The fixture claims its second arrival at 140 ms was priced and
        # matched. That instant belongs to the next scope and cannot be used.
        with self.assertRaisesRegex(ProtocolError, "probe unresolved leg"):
            self.read()

    def test_slow_first_at_exact_scope_end_censors_without_sending_later_legs(self):
        self.policy = self.make_policy(sequencing=["slow_first"])
        self.row["sequencing"] = "slow_first"
        self.scopes = [{"end_ns": str(140 * MS)}, {"end_ns": str(1_000 * MS)}]
        self.ep["end"] = 120 * MS
        self.censor(0, 1)
        self.row["legs"][0].update(sent_ns=None, arrival_ns=None)
        self.read()

    def test_slow_first_later_leg_at_exact_run_end_is_unresolved(self):
        self.policy = self.make_policy(sequencing=["slow_first"])
        self.row["sequencing"] = "slow_first"
        self.scopes[0]["end_ns"] = str(165 * MS)
        self.ep["end"] = 120 * MS
        self.row["legs"][0].update(sent_ns=str(140 * MS), arrival_ns=str(165 * MS))
        self.censor(0)
        self.read()

    def test_bought_excess_uses_worst_atoms_and_loss_stays_signed(self):
        self.row["legs"][0].update(consumed=[["390", "5"], ["400", "5"]], cost="3950")
        self.row["value"] = "600"  # The cheaper 390 level belongs to the matched set.
        for exit_price, loss in ((390, 50), (410, -50)):
            with self.subTest(exit_price=exit_price):
                self.row["legs"][0].update(exit_consumed=[[str(exit_price), "5"]],
                                           exit_proceeds=str(exit_price * 5))
                self.row["leg_loss_gross"] = str(loss)
                self.read()
                self.reject(lambda row: row.update(leg_loss_gross=str(-loss)), "probe gross loss")
                self.reject(lambda row: row.update(value="550"), "probe matched value")

    def test_sold_excess_uses_highest_sales_for_matching_and_direction_aware_signed_loss(self):
        self.strategy = SoldValue()
        self.ep["probe_opening"]["two"]["limits"] = ["550", "500"]
        self.row["decision"]["limits"] = ["550", "500"]
        self.row["legs"][0].update(consumed=[["600", "5"], ["550", "5"]], cost="5750")
        self.row["value"] = "600"  # Highest-priced sales: 600*5 - 480*5.
        for exit_price, loss in ((560, 50), (540, -50)):
            with self.subTest(exit_price=exit_price):
                self.row["legs"][0].update(exit_consumed=[[str(exit_price), "5"]],
                                           exit_proceeds=str(exit_price * 5))
                self.row["leg_loss_gross"] = str(loss)
                self.read()
                self.reject(lambda row: row.update(leg_loss_gross=str(-loss)), "probe gross loss")
                self.reject(lambda row: row.update(value="350"), "probe matched value")

    def test_millisecond_fields_reject_noninteger_and_out_of_bound_values(self):
        for field in ("confirm_ms", "arrival_ms", "venue_delay_ms"):
            for value in (True, False, 0.0, 25.0, "0", "25", None, -1, (2**64 - 1) // MS + 1):
                with self.subTest(field=field, value=value):
                    if field == "venue_delay_ms":
                        mutate = lambda row: row["legs"][0].update({field: value})
                    else:
                        mutate = lambda row: row.update({field: value})
                    self.reject(mutate, "probe")

    def test_slow_first_send_and_size_come_from_first_leg_observation(self):
        self.policy = self.make_policy(sequencing=["slow_first"])
        self.row["sequencing"] = "slow_first"
        # Leg 1 is slow (40 ms versus 25 ms). Its one filled step determines
        # leg 0's later five-atom order and send time.
        self.row["legs"][0].update(sent_ns=str(140 * MS), arrival_ns=str(165 * MS),
                                   filled_atoms="5", cost="2000", consumed=[["400", "5"]],
                                   excess_atoms="0", exit_consumed=[], exit_proceeds="0")
        self.row["leg_loss_gross"] = "0"
        self.read()
        self.reject(lambda row: row["legs"][0].update(filled_atoms="10"), "probe filled quantity")

    def test_probe_file_identity_is_verified_separately_from_arithmetic(self):
        self.read()
        identity = {"sha256": "b" * 64, "byte_length": (self.root / "probes.ndjson").stat().st_size,
                    "records": 1}
        with self.assertRaisesRegex(ProtocolError, "file identity"):
            validate(self.root, identity, [self.ep], self.scopes, self.policy,
                     self.strategy, self.plans, Budget())

    def test_rows_follow_computed_resolution_order(self):
        self.policy = self.make_policy(arrival_ms=[0, 5])
        late = copy.deepcopy(self.row)
        late["arrival_ms"] = 5
        for leg in late["legs"]:
            leg["arrival_ns"] = str(int(leg["arrival_ns"]) + 5 * MS)
        self.read([self.row, late])
        with self.assertRaisesRegex(ProtocolError, "probe resolution order"):
            self.read([late, self.row])


class RetainedPair(PairFills):
    def requirements(self, snapshot, policy):
        return Requirements({key: BookRequirement(("bid", "ask"), (1,), ladders=("bid", "ask"))
                             for key in self.plans})


class ProbeOutputReaderTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def fixture(self, *, short_record=False, no_episode=False):
        root = Path(self.tmp.name) / "run"
        root.mkdir()
        names = ["one", "four"] if short_record else ["one"]
        probes = {"version": 1, "sizings": names, "confirm_ms": [0], "arrival_ms": [0],
                  "sequencing": ["simultaneous"], "unknown_taker_delay_ms": {"polymarket": 0}}
        records = ({"name": "four", "role": "records", "target_contracts": "4"},) if short_record else ()
        with patch("replay.tests.test_economic_sdk_fills.build", factory(RetainedPair)):
            h = FillHarness(root, pair_policy(ONE, *records, fills={"probes": probes}))
        for writer in h.strategy.writers.values():
            self.addCleanup(writer.stream.close)
        h.window()
        h.asks(12, LEG0, (900 if no_episode else 450, M))
        h.asks(12, LEG1, (900 if no_episode else 500, M))
        h.finish()
        manifest = json.loads((h.output / "manifest.json").read_bytes())
        snapshot = load_snapshot(h.root / "context", expected_sha256=h.sha)
        return h, manifest, snapshot

    def test_runtime_output_is_listed_and_its_c0_fills_recompute_independently(self):
        h, manifest, snapshot = self.fixture()
        self.assertIn("probes.ndjson", manifest["files"])
        rows = h.rows("probes.ndjson")
        self.assertEqual(len(rows), 1)
        self.assertEqual((rows[0]["status"], rows[0]["matched_steps"]), ("filled", "1"))
        self.assertEqual(rows[0]["value"], h.rows("episodes.ndjson")[0]["fill"]["results"][0]["value"])
        aggregate_reader.validate(h.output, snapshot, manifest, h.strategy.strategy)

    def test_rehashed_probe_file_cannot_hide_semantic_tampering(self):
        h, manifest, snapshot = self.fixture()
        rewrite(h, manifest, "probes.ndjson", lambda rows: rows[0]["legs"][0].update(cost="1"))
        with self.assertRaisesRegex(ProtocolError, "probe fill cost"):
            aggregate_reader.validate(h.output, snapshot, manifest, h.strategy.strategy)

    def test_rehashed_probe_file_cannot_omit_a_configured_row(self):
        h, manifest, snapshot = self.fixture()
        rewrite(h, manifest, "probes.ndjson", lambda rows: rows.clear())
        with self.assertRaisesRegex(ProtocolError, "probe coverage"):
            aggregate_reader.validate(h.output, snapshot, manifest, h.strategy.strategy)

    def test_c0_short_recording_result_is_probed_without_a_tradeability_veto(self):
        h, manifest, snapshot = self.fixture(short_record=True)
        episodes = h.rows("episodes.ndjson")
        self.assertFalse(episodes[0]["fill"]["results"][1]["tradeable"])
        self.assertEqual([row["status"] for row in h.rows("probes.ndjson")], ["filled", "filled"])
        aggregate_reader.validate(h.output, snapshot, manifest, h.strategy.strategy)

    def test_empty_probe_file_still_requires_defaults_for_every_tradable_venue(self):
        h, manifest, snapshot = self.fixture(no_episode=True)
        self.assertEqual(h.rows("probes.ndjson"), [])
        strategy = h.strategy.strategy
        strategy.experiment = replace(strategy.experiment, fills=replace(
            strategy.experiment.fills, probes=replace(strategy.experiment.fills.probes,
                                                      unknown_taker_delay_ms={})))
        with self.assertRaisesRegex(ProtocolError, "probe delay default venue"):
            aggregate_reader.validate(h.output, snapshot, manifest, strategy)
