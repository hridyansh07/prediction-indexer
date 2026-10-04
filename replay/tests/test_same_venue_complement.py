import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from replay.economic_fills import walk
from replay.economic_intervals import EpisodeMath
from replay.complement_output import read_provisional, validate_content
from replay.fees.artifacts import build_catalog, source_from_bytes
from replay.fees.domain import Asset, AssetAmount, AssetKind, Component, Fixed, InstrumentEconomics, Product, Venue, canonical
from replay.fees.schedules import Catalog, Schedule, Scope, ZeroFee
from replay.preparation import encoded, load_snapshot
from replay.complement_contract import layouts
from replay.same_venue_complement import build
from replay.streams.protocol import ProtocolError
from replay.tests.test_bundle_coverage import Harness as CoverageHarness
from replay.tests.test_preparation import config as preparation_config, detail as preparation_detail


def paired_detail():
    """Two genuine preparation-shaped pairs for each supported venue."""
    value = preparation_detail()
    markets = value["context"]["markets"]
    targets = value["context"]["targets"]
    markets.extend([
        {"target_id": "kalshi:series-two", "venue": "kalshi", "selected": True},
        {"target_id": "polymarket:series-two", "venue": "polymarket", "selected": True},
    ])
    targets.extend([
        {"venue": "kalshi", "target_id": "kalshi:series-two",
         "canonical_class": "esports.series_moneyline", "subscription_ids": ["series-two"],
         "source_ref": "kalshi:event-two"},
        {"venue": "polymarket", "target_id": "polymarket:series-two",
         "canonical_class": "esports.series_moneyline", "subscription_ids": ["456", "654"],
         "source_ref": "polymarket:event-two"},
    ])
    markets.sort(key=lambda row: row["target_id"])
    targets.sort(key=lambda row: row["target_id"])
    return value


def strategy_config(base, root, known=False):
    snapshot = load_snapshot(base["snapshot_directory"], expected_sha256=base["snapshot_sha256"])
    source_bytes = b"synthetic zero-fee contract, not live evidence"
    source = source_from_bytes("https://example.invalid/synthetic", 0, source_bytes)
    schedules, bindings, assets = [], [], {}
    for plan in snapshot["plans"]:
        venue = plan["venue"]
        quote = Asset(AssetKind.USD if venue == "kalshi" else AssetKind.USDC, "synthetic", venue)
        outcome = Asset(AssetKind.OUTCOME, "synthetic", plan["instrument"] + plan["orientation"])
        econ = InstrumentEconomics(quote, outcome, AssetAmount(quote, Fixed(1, 0)), 18,
                                   int(plan["quantity_scale"]), int(plan["price_scale"]))
        if known is True or (isinstance(known, set) and plan["instrument"] in known):
            assets[venue] = {"kind": quote.kind.value, "ledger": quote.chain, "token": quote.token}
            bindings.append({"instrument": plan["instrument"], "orientation": plan["orientation"], "economics": json.loads(canonical(econ))})
            schedules.append(Schedule(Component.PLATFORM,
                Scope(Venue(venue), Product.CLOB, instrument=plan["instrument"].split(":", 1)[1], orientation=plan["orientation"]),
                econ, quote, 6, ZeroFee("synthetic"), (source,), 0, None, "synthetic", "test", "test"))
    catalog = Catalog.build(tuple(schedules))
    directory = build_catalog(root / "fees", catalog, {source.sha256: source_bytes} if known else {})
    return {**base, "fees": {"catalog_directory": str(directory), "catalog_identity": catalog.identity,
        "reference_ns": "20", "limitless_buy_bps": 300, "limitless_sell_bps": 150,
        "kalshi_member_class": "NON_DIRECT", "assets": assets,
        "instrument_bindings": sorted(bindings, key=lambda b: (b["instrument"], b["orientation"]))},
        "policy": {"version": 1, "sizes_contracts": ["1", "3"], "headline_size_contracts": "1",
            "latency_tiers_ns": ["1", "5", "10"], "headline_latency_ns": "5",
            "minimum_net_gap_per_contract_e18": "0", "leg_skew_buckets_ns": ["1", "5", "10"],
            "verdict": {"maximum_positive_time_fraction_ppm": "0", "minimum_evaluated_ns": "1"}}}


class Harness(CoverageHarness):
    def __init__(self, root, *, known=False, pairs=False, policy=None, **kwargs):
        def factory(context):
            self.complement_config = strategy_config(dict(context["config"]), root, known)
            if policy:
                self.complement_config["policy"] = {**self.complement_config["policy"], **policy}
            return build({**context, "config": self.complement_config})
        detail_factory = paired_detail if pairs else preparation_detail
        if pairs:
            kwargs["mixed"] = True
        with patch("replay.tests.test_bundle_coverage.build", side_effect=factory), \
             patch("replay.tests.test_bundle_coverage.detail", side_effect=detail_factory), \
             patch("replay.tests.test_bundle_coverage.config", side_effect=lambda: preparation_config(detail_factory())):
            super().__init__(root, **kwargs)

    def plan_index(self, instrument, orientation="outcome"):
        return next(i for i, p in enumerate(self.initial["plans"])
                    if p["instrument"] == instrument and p["orientation"] == orientation)

    def quote(self, time, index, *, bid=400, ask=490, quantity=5_000_000, why=None):
        plan = self.initial["plans"][index]
        transition = self.transition(plan, time, why)
        if why is None:
            transition["decision"] = {"kind": "snapshot", "bids": [[str(bid), str(quantity)]] if bid is not None else [],
                                       "asks": [[str(ask), str(quantity)]] if ask is not None else []}
        ref = self.ref(time)
        return self.send("cut", {"origin": {"kind": "group", "pin": self.pin,
            "first": ref["address"], "last": ref["address"], "visible_ns": str(time)},
            "market_events": [], "book_transitions": [transition]})

    def finish(self):
        self.terminal()
        self.decoder.finish()
        self.strategy.finish()
        return read_provisional(self.output, self.root / "context", expected_sha256=self.sha)

    def records(self, name):
        return [json.loads(line) for line in (self.output / name).read_bytes().splitlines()]


class ComplementIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def harness(self, **kwargs):
        h = Harness(self.root, **kwargs)
        for writer in h.strategy.writers.values():
            self.addCleanup(writer.stream.close)
        return h

    def real_long(self, h, name="episodes.ndjson", kind="gross"):
        descriptors = {**h.strategy.layout}
        return [r for r in h.records(name) if not descriptors[r["entity"]]["placebo"]
                and descriptors[r["entity"]]["direction"] == "long"
                and descriptors[r["entity"]]["size_contracts"] == "1" and r.get("kind") == kind]

    def test_unknown_positive_is_inconclusive_known_zero_is_present(self):
        for known in (False, True):
            with self.subTest(known=known), tempfile.TemporaryDirectory() as tmp:
                h = Harness(Path(tmp), known=known)
                h.window(); h.quote(12, 0); h.quote(12, 1)
                result = h.finish()
                row = result["summary"]["verdicts"][0]
                self.assertEqual(row["evaluated_ns"], "56")  # 28 long + 28 short
                self.assertEqual(row["fee_unknown_ns"], "0" if known else "28")
                self.assertEqual(row["verdict"], "INTRA_INSTRUMENT_GAPS_PRESENT_INVESTIGATE" if known else "INCONCLUSIVE_FIXTURE")
                episodes = self.real_long(h)
                self.assertEqual([(r["start_ns"], r["end_ns"], r["open_values"]["gap_gross"]) for r in episodes], [("12", "40", "20000000")])

    def test_real_decoder_substituted_leg_re_evaluates_only_placebo_dependency(self):
        for venue, market, replacement, changed_quote in (
            ("polymarket", "polymarket:series", "polymarket:series-two", {"ask": 300}),
            ("kalshi", "kalshi:series", "kalshi:series-two", {"bid": 700}),
        ):
            with self.subTest(venue=venue), tempfile.TemporaryDirectory() as tmp:
                h = Harness(Path(tmp), pairs=True)
                h.window()
                for index, plan in enumerate(h.initial["plans"]):
                    if plan["venue"] == "polymarket":
                        h.quote(12, index, bid=400, ask=600)
                    else:
                        h.quote(12, index, bid=400, ask=None)
                target = next((entity, desc) for entity, desc in h.strategy.layout.items()
                              if desc["market_id"] == market and desc["placebo"]
                              and desc["size_contracts"] == "1"
                              and desc["direction"] in {"long", "both_bids"})
                entity, descriptor = target
                self.assertEqual(descriptor["legs"][1]["market_id"], replacement)
                leg = descriptor["legs"][1]
                h.quote(20, h.plan_index(leg["instrument"], leg["orientation"]),
                        ask=None if venue == "kalshi" else 300,
                        bid=changed_quote.get("bid", 400))
                h.finish()
                rows = h.records("measurements.ndjson")
                placebo = [r for r in rows if r["entity"] == entity]
                real_entity = next(e for e, d in h.strategy.layout.items()
                                   if d["market_id"] == market and not d["placebo"]
                                   and d["size_contracts"] == "1"
                                   and d["direction"] == descriptor["direction"])
                real = [r for r in rows if r["entity"] == real_entity]
                self.assertEqual([(r["start_ns"], r["end_ns"], r["value_class"])
                                  for r in placebo if r["status"] == "DEPTH_SUFFICIENT"],
                                 [("12", "20", "GROSS_NONPOSITIVE"),
                                  ("20", "40", "FEE_UNKNOWN")])
                self.assertEqual([(r["start_ns"], r["end_ns"], r["value_class"])
                                  for r in real if r["status"] == "DEPTH_SUFFICIENT"],
                                 [("12", "40", "GROSS_NONPOSITIVE")])

    def test_verdict_insufficient_evaluation_precedes_positive_and_unknown(self):
        h = self.harness(policy={"verdict": {
            "maximum_positive_time_fraction_ppm": "0", "minimum_evaluated_ns": "1000"}})
        h.window(); h.quote(12, 0); h.quote(12, 1)
        verdict = h.finish()["summary"]["verdicts"][0]
        self.assertEqual((verdict["verdict"], verdict["reason"]),
                         ("INCONCLUSIVE_FIXTURE", "INSUFFICIENT_EVALUATED_TIME"))

    def test_known_positive_above_threshold_precedes_other_unknown_time(self):
        known = {"polymarket:123", "polymarket:987"}
        h = self.harness(pairs=True, known=known)
        h.window()
        for index, plan in enumerate(h.initial["plans"]):
            if plan["venue"] == "polymarket":
                h.quote(12, index, ask=490)
        verdict = next(v for v in h.finish()["summary"]["verdicts"]
                       if v["venue"] == "polymarket")
        self.assertGreater(int(verdict["fee_unknown_ns"]), 0)
        self.assertGreater(int(verdict["qualified_ns"]), 0)
        self.assertEqual((verdict["verdict"], verdict["reason"]),
                         ("INTRA_INSTRUMENT_GAPS_PRESENT_INVESTIGATE", None))

    def test_break_even_gross_opens_no_net_episode(self):
        h = self.harness(known=True); h.window()
        h.quote(12, 0, ask=500); h.quote(12, 1, ask=500)
        h.finish()
        self.assertEqual(self.real_long(h, kind="gross"), [])
        self.assertEqual(self.real_long(h, kind="net"), [])

    def test_same_timestamp_false_true_and_slice_restore(self):
        h = self.harness(known=True); h.window(); h.quote(12, 0); h.quote(12, 1)
        h.quote(17, 0, ask=700); h.quote(17, 0, ask=490)
        h.finish()
        self.assertEqual(len(self.real_long(h)), 1)
        self.assertEqual([(r["start_ns"], r["end_ns"]) for r in self.real_long(h, "slices.ndjson")], [("12", "40")])

    def test_successive_slices_and_entry_skew(self):
        h = self.harness(known=True); h.window(); h.quote(12, 0); h.quote(12, 1)
        h.quote(13, 0, ask=480)
        result = h.finish()
        episode = self.real_long(h, kind="net")[0]
        self.assertEqual(episode["opening_slice_survival_ns"], "1")
        self.assertEqual(episode["qualified_ns"], {"1": "26", "5": "22", "10": "17"})
        self.assertEqual([(r["start_ns"], r["end_ns"], r["opening_skew_bucket"]) for r in self.real_long(h, "slices.ndjson", "net")], [("12", "13", 0), ("13", "40", 1)])
        self.assertEqual(result["summary"]["verdicts"][0]["qualified_ns"], "22")

    def test_quiet_scope_boundaries_retain_prior_prices(self):
        h = self.harness(known=True, scopes=True); h.window(); h.quote(12, 0); h.quote(12, 1)
        h.quote(29, 0, ask=800)
        h.finish()
        self.assertEqual([(r["scope"], r["start_ns"], r["end_ns"]) for r in self.real_long(h)], [(0, "12", "23"), (1, "23", "29")])

    def test_same_time_scope_change_uses_final_state(self):
        h = self.harness(known=True, scopes=True); h.window(); h.quote(12, 0); h.quote(12, 1)
        h.quote(23, 0, ask=800); h.quote(23, 0, ask=480)
        h.finish()
        self.assertEqual([(r["scope"], r["start_ns"], r["end_ns"]) for r in self.real_long(h)], [(0, "12", "23"), (1, "23", "40")])

    def test_unusable_and_depth_are_not_negative_evidence(self):
        h = self.harness(); h.window(); h.quote(12, 0); h.quote(12, 1, quantity=2_000_000)
        h.quote(20, 1, why={"kind": "connection_closed"})
        h.quote(25, 1, ask=None)
        h.finish()
        episodes = self.real_long(h)
        self.assertEqual((episodes[0]["end_ns"], episodes[0]["end_reason"]), ("20", "UNUSABLE"))
        statuses = {r["status"] for r in h.records("measurements.ndjson")}
        self.assertTrue({"DEPTH_LIMITED", "UNUSABLE", "ONE_SIDED"} <= statuses)

    def test_unrelated_cuts_do_not_evaluate_or_call_fees(self):
        h = self.harness(); h.window(); h.quote(12, 0); h.quote(12, 1)
        with patch.object(h.strategy, "_evaluate", wraps=h.strategy._evaluate) as evaluate:
            h.group(15); h.group(19)
            self.assertEqual(evaluate.call_count, 0)
        h.finish()

    def test_missing_terminal_and_budget_poison(self):
        h = self.harness(); h.window()
        with self.assertRaisesRegex(ProtocolError, "missing terminal"):
            h.strategy.finish()
        self.assertFalse((h.output / "content_receipt.json").exists())

    def test_prologue_and_deterministic_retries(self):
        outputs = []
        for attempt in ("a" * 32, "b" * 32):
            with tempfile.TemporaryDirectory() as tmp:
                h = Harness(Path(tmp), attempt=attempt, lower_bound="expand_to_window_start")
                h.window(); h.quote(5, 0); h.quote(5, 1); h.finish()
                self.assertEqual(self.real_long(h)[0]["start_ns"], "10")
                outputs.append({name: (h.output / name).read_bytes() for name in ("measurements.ndjson", "episodes.ndjson", "slices.ndjson", "manifest.json", "summary.json")})
        self.assertEqual(outputs[0], outputs[1])


class EconomicRuntimeUnitTests(unittest.TestCase):
    def test_admission_precedence_uncaptured_shape_then_scale(self):
        books = lambda *keys: [{"instrument": instrument, "orientation": orientation}
                               for instrument, orientation in keys]
        members = [
            {"market_id": "polymarket:uncaptured", "capture_selected": False,
             "books": books(("polymarket:u", "outcome"))},
            {"market_id": "polymarket:shape", "capture_selected": True,
             "books": books(("polymarket:s", "outcome"))},
            {"market_id": "polymarket:scale", "capture_selected": True,
             "books": books(("polymarket:a", "outcome"), ("polymarket:b", "outcome"))},
        ]
        plans = [
            {"instrument": "polymarket:u", "orientation": "outcome", "price_scale": "2", "quantity_scale": "0"},
            {"instrument": "polymarket:s", "orientation": "outcome", "price_scale": "2", "quantity_scale": "0"},
            {"instrument": "polymarket:a", "orientation": "outcome", "price_scale": "2", "quantity_scale": "0"},
            {"instrument": "polymarket:b", "orientation": "outcome", "price_scale": "3", "quantity_scale": "0"},
        ]
        resolved = layouts({"plans": plans, "scopes": [{"members": members}]},
                           {"sizes_contracts": ["1"]}, 0)
        admissions = {d["market_id"]: d["admission"] for d in resolved.values()
                      if not d["placebo"]}
        self.assertEqual(admissions, {
            "polymarket:uncaptured": "NOT_CAPTURED",
            "polymarket:shape": "UNSUPPORTED_SHAPE",
            "polymarket:scale": "UNSUPPORTED_SCALE",
        })

    def test_exact_multisize_walk_keeps_displayed_survival_quantity(self):
        one, three = walk(((9, 2), (10, 5)), (1, 3))
        self.assertEqual((one.cost, one.taken, one.consumed), (9, ((9, 1),), ((9, 2),)))
        self.assertEqual((three.cost, three.taken), (28, ((9, 2), (10, 1))))
        self.assertEqual(three.consumed, ((9, 2), (10, 5)))

    def test_projection_identity_and_latency_math(self):
        levels = ((20, 2), (10, 3))
        fill = walk(levels, (4,))[0]
        payout = 100
        projected = payout * fill.filled_atoms - fill.cost
        self.assertEqual(projected, (80 * 2) + (90 * 2))
        survival, reached, qualified = EpisodeMath.close(10_000_000, 9_010_000_000,
                                                          ["250000000", "1000000000", "5000000000"])
        self.assertEqual(survival, 9_000_000_000)
        self.assertEqual(reached, ["250000000", "1000000000", "5000000000"])
        self.assertEqual(qualified["1000000000"], "8000000000")


if __name__ == "__main__":
    unittest.main()
