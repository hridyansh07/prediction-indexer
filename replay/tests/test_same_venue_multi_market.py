"""Same-venue multi-market complete sets: offline contract-shaped masks and books.

One Bo3 series space. Kalshi lists four markets on it: series (Alpha wins),
beta (Beta wins), h20 (Alpha wins 2-0) and h21 (Alpha wins 2-1); each has an
outcome and a complement book. Polymarket lists the series market (tokens 123
Alpha, 987 Beta) and a Yes/No market on Alpha (456 Yes, 654 No). Kalshi prices
are cents (scale 2, whole contracts); Polymarket prices are scale 3 with
quantity scale 6.
"""

import copy
import hashlib
import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from analysis.claims import claim_id, space_shape_id
from analysis.outcome_space import build_series_space
from replay.cross_venue_contract import UNIT
from replay.economic_sdk.types import Context
from replay.fees.artifacts import build_catalog, load_catalog, source_from_bytes
from replay.fees.domain import Multiplier
from replay.fees.schedules import Catalog, Kalshi, KalshiKind
from replay.preparation import encoded, prepare
from replay.same_venue_multi_market import build
from replay.same_venue_multi_market_contract import baskets, policy_config
from replay.same_venue_multi_market_output import read_provisional, validate_content
from replay.streams.protocol import ProtocolError
from replay.tests.economic_scenarios import ladder
from replay.tests.test_bundle_coverage import Harness as BaseHarness
from replay.tests.test_preparation import config, detail
from replay.tests.test_same_venue_complement import strategy_config

_PREPARE = prepare
EDGE = {"name": "edge", "role": "governs", "edge": True}
ONE = {"name": "one", "role": "governs", "target_contracts": "1"}
KALSHI = (("series", ["series"], "home"), ("beta", ["beta"], "away"),
          ("h20", ["h20"], "home20"), ("h21", ["h21"], "home21"))


def sibling_detail():
    d = detail()
    context = d["context"]
    for name, subs, _ in KALSHI[1:]:
        context["markets"].append({"target_id": "kalshi:" + name, "venue": "kalshi", "selected": True})
        context["targets"].append({"venue": "kalshi", "target_id": "kalshi:" + name,
                                   "canonical_class": "esports.series_moneyline", "subscription_ids": subs,
                                   "source_ref": "kalshi:event"})
    context["markets"].append({"target_id": "polymarket:alpha", "venue": "polymarket", "selected": True})
    context["targets"].append({"venue": "polymarket", "target_id": "polymarket:alpha",
                               "canonical_class": "esports.series_moneyline", "subscription_ids": ["456", "654"],
                               "source_ref": "polymarket:event"})
    for field in ("markets", "targets"):
        context[field].sort(key=lambda row: row["target_id"])
    return d


def sibling_document(*, drop=(), void=()):
    space = build_series_space("bundle-1", best_of=3, home="Alpha", away="Beta")
    shape = space_shape_id(space)
    keys = {"home": space.select(lambda p: p["winner_side"] == "home"),
            "away": space.select(lambda p: p["winner_side"] == "away"),
            "home20": space.select(lambda p: p["home_wins"] == 2 and p["maps_played"] == 2),
            "home21": space.select(lambda p: p["home_wins"] == 2 and p["maps_played"] == 3)}
    claims = {name: {"claim_id": claim_id(sorted(value), shape), "space_shape_id": shape,
                     "outcome_keys": sorted(value)} for name, value in keys.items()}

    def market(mid, subs, labels, claim_names, tokens):
        if mid in void:
            return {"market_id": mid, "venue": mid.split(":")[0], "market_type": "series_moneyline",
                    "market_status": "open", "subscription_ids": subs, "outcome_labels": labels,
                    "mask_status": "VOID_UNSUPPORTED", "reason": None, "claims": [], "tokens": []}
        return {"market_id": mid, "venue": mid.split(":")[0], "market_type": "series_moneyline",
                "market_status": "open", "subscription_ids": subs, "outcome_labels": labels,
                "mask_status": "MASKED", "reason": None,
                "claims": [{"claim_key": f"claim={i}", "claim_id": claims[c]["claim_id"]}
                           for i, c in enumerate(claim_names)],
                "tokens": tokens}

    markets = [market("kalshi:" + name, subs, ["Alpha"], [claim],
                      [{"subscription_id": subs[0], "claim_key": "claim=0", "negated": False}])
               for name, subs, claim in KALSHI]
    markets.append(market("polymarket:series", ["123", "987"], ["Alpha", "Beta"], ["home", "away"],
                          [{"subscription_id": "123", "claim_key": "claim=0", "negated": False},
                           {"subscription_id": "987", "claim_key": "claim=1", "negated": False}]))
    markets.append(market("polymarket:alpha", ["456", "654"], ["Yes", "No"], ["home"],
                          [{"subscription_id": "456", "claim_key": "claim=0", "negated": False},
                           {"subscription_id": "654", "claim_key": "claim=0", "negated": True}]))
    markets = [m for m in markets if m["market_id"] not in drop]
    used = {c["claim_id"] for m in markets for c in m["claims"]}
    return {"version": 1, "bundle_id": "bundle-1", "event_id": "event:d1:" + "a" * 64,
            "identities": {"claim_identity_version": 2, "claim_algebra_version": 1},
            "status": "complete", "diagnostics": [], "participants": ["Alpha", "Beta"],
            "spaces": [{"space_shape_id": shape, "scope": "series", "coverage": "EXHAUSTIVE", "best_of": 3,
                        "outcome_keys": sorted(space.keys)}],
            "claims": sorted((c for c in claims.values() if c["claim_id"] in used), key=lambda c: c["claim_id"]),
            "markets": sorted(markets, key=lambda m: m["market_id"])}


def policy(*sizings, step="1", max_legs=4, **overrides):
    value = {"version": 1, "fills": {"version": 1, "step_contracts": step, "max_levels": 64,
                                     "sizings": list(sizings or (EDGE,))},
             "max_legs": max_legs, "latency_tiers_ns": ["1", "5", "10"], "headline_latency_ns": "5",
             "minimum_net_gap_per_contract_e18": "0", "leg_skew_buckets_ns": ["1", "5", "10"],
             "audit_intervals": False, "profile": None}
    value.update(overrides)
    return value


class Harness(BaseHarness):
    def __init__(self, root, *, doc=None, fees="zero", value=None, **kwargs):
        d = sibling_detail()
        document = sibling_document() if doc is None else doc

        def prep_config():
            c = config(d)
            for authority in c["authorities"]:
                if authority["venue"] == "kalshi":
                    authority.update(price_scale="2", quantity_scale="0")
            return c

        def prepared(c, directory, **kw):
            if document == "unavailable":
                return _PREPARE(c, directory, **kw)
            return _PREPARE(c, directory, **kw, outcomes=lambda _: document)

        def factory(context):
            cfg = strategy_config(dict(context["config"]), root, known=True)
            # ZeroFee is not a Kalshi model: Kalshi gets its quadratic schedule, with
            # multiplier 0, or 1 ("collateral", on the non-direct cent grid).
            catalog = load_catalog(Path(cfg["fees"]["catalog_directory"]))
            catalog = Catalog.build(tuple(
                replace(s, model=Kalshi(KalshiKind.QUADRATIC, Multiplier(1 if fees == "collateral" else 0, 0)))
                if s.scope.venue.value == "kalshi" else s for s in catalog.schedules))
            raw = b"synthetic zero-fee contract, not live evidence"
            source = source_from_bytes("https://example.invalid/synthetic", 0, raw)
            directory = build_catalog(root / "kalshi-fees", catalog, {source.sha256: raw})
            cfg["fees"].update(catalog_directory=str(directory), catalog_identity=catalog.identity)
            cfg["policy"] = value or policy()
            self.multi_config = cfg
            return build({**context, "config": cfg})

        with patch("replay.tests.test_bundle_coverage.detail", side_effect=lambda: copy.deepcopy(d)), \
             patch("replay.tests.test_bundle_coverage.config", side_effect=prep_config), \
             patch("replay.tests.test_bundle_coverage.prepare", side_effect=prepared), \
             patch("replay.tests.test_bundle_coverage.build", side_effect=factory):
            super().__init__(root, mixed=True, **kwargs)

    def plan_index(self, instrument, orientation="outcome"):
        return next(i for i, p in enumerate(self.initial["plans"])
                    if (p["instrument"], p["orientation"]) == (instrument, orientation))

    def kalshi_ask(self, time, market, orientation, *levels):
        """Asks for buying ``orientation``: the opposite orientation's bids at ``100 - p``."""
        other = "complement" if orientation == "outcome" else "outcome"
        ladder(self, time, "kalshi:" + market, other, bids=tuple((100 - p, q) for p, q in levels))

    def pm_ask(self, time, token, *levels):
        ladder(self, time, "polymarket:" + token, bids=((100, 3 * 10**6),),
               asks=tuple((p, q * 10**6) for p, q in levels))

    def records(self, name):
        path = self.output / name
        return [json.loads(line) for line in path.read_bytes().splitlines()] if path.exists() else []

    def finish(self):
        self.terminal()
        self.decoder.finish()
        self.strategy.finish()
        return read_provisional(self.output, self.root / "context", expected_sha256=self.sha,
                                bridge=self.strategy.strategy.bridge)

    def close(self):
        for writer in self.strategy.writers.values():
            writer.stream.close()


def legs_of(basket):
    return tuple((leg["instrument"], leg["orientation"]) for leg in basket.descriptor["legs"])


class MultiMarketTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def harness(self, **kw):
        h = Harness(self.root, **kw)
        self.addCleanup(h.close)
        h.window()
        return h

    def entity(self, h, legs):
        return next(e for e in h.strategy.entities.values() if e.admission is None
                    and tuple((d["instrument"], d["orientation"]) for d in e.descriptor["legs"]) == legs)

    def fills(self, h, legs):
        index = h.strategy.index[self.entity(h, legs).id]
        return [r for r in h.records("episodes.ndjson") if r["kind"] == "net" and r["entity"] == index]

    def test_sets_partition_one_space_across_markets_and_never_inside_one_market(self):
        h = self.harness()
        snapshot = h.strategy.strategy.snapshot
        admitted = {legs_of(b) for b in baskets(snapshot, policy(), 0) if b.admission is None}
        k = lambda m, o="outcome": ("kalshi:" + m, o)
        self.assertEqual(admitted, {
            (k("beta"), k("series")),                                  # YES Beta + YES Alpha
            (k("beta", "complement"), k("series", "complement")),     # NO Beta + NO Alpha
            (k("beta"), k("h20"), k("h21")),                           # Beta + Alpha 2-0 + Alpha 2-1
            (k("h20"), k("h21"), k("series", "complement")),
            (("polymarket:123", "outcome"), ("polymarket:654", "outcome")),
            (("polymarket:456", "outcome"), ("polymarket:987", "outcome")),
        })
        two = {legs_of(b) for b in baskets(snapshot, policy(max_legs=2), 0) if b.admission is None}
        self.assertEqual(two, {legs for legs in admitted if len(legs) == 2})
        rejected = [b for b in baskets(snapshot, policy(), 0) if b.admission is not None]
        self.assertEqual([(b.admission, json.loads(b.admission_reasons[0])["kind"]) for b in rejected],
                         [("NOT_CAPTURED", "not_captured")])   # polymarket:map-one
        h.finish()

    def test_unmasked_books_and_venues_without_a_set_are_visible(self):
        doc = sibling_document(drop=("polymarket:alpha",), void=("kalshi:h21",))
        h = self.harness(doc=doc)
        rows = baskets(h.strategy.strategy.snapshot, policy(), 0)
        rejected = sorted((b.descriptor["venue"], b.admission, json.loads(b.admission_reasons[0])["kind"],
                           tuple(legs_of(b))) for b in rows if b.admission is not None)
        self.assertIn(("kalshi", "UNSUPPORTED_SHAPE", "VOID_UNSUPPORTED", (("kalshi:h21", "complement"),)), rejected)
        self.assertIn(("kalshi", "UNSUPPORTED_SHAPE", "VOID_UNSUPPORTED", (("kalshi:h21", "outcome"),)), rejected)
        # Without polymarket:alpha's masks, its books are rejected and the series
        # tokens alone are one market: no multi-market set on Polymarket.
        self.assertIn(("polymarket", "UNSUPPORTED_SHAPE", "no_multi_market_set", ()), rejected)
        admitted = {legs_of(b) for b in rows if b.admission is None}
        self.assertTrue(admitted and all(len(legs) == 2 for legs in admitted))
        h.finish()

    def test_unavailable_outcomes_reject_each_venue_once(self):
        h = self.harness(doc="unavailable")
        rows = baskets(h.strategy.strategy.snapshot, policy(), 0)
        self.assertFalse(any(b.admission is None for b in rows))
        kinds = sorted((b.descriptor["venue"], json.loads(b.admission_reasons[0])["kind"]) for b in rows)
        self.assertIn(("kalshi", "outcomes_unavailable"), kinds)
        self.assertIn(("polymarket", "outcomes_unavailable"), kinds)
        h.finish()

    def test_edge_fill_walks_every_leg_and_the_reader_rechecks_it(self):
        h = self.harness()
        # YES Alpha 0.45 x 3 then 0.50 x 10; YES Beta 0.50 x 5.
        h.kalshi_ask(12, "series", "outcome", (45, 3), (50, 10))
        h.kalshi_ask(12, "beta", "outcome", (50, 5))
        result = h.finish()
        (row,) = self.fills(h, (("kalshi:beta", "outcome"), ("kalshi:series", "outcome")))
        (edge,) = row["fill"]["results"]
        self.assertEqual((edge["steps"], edge["stop"], edge["value"]), ("3", "edge", str(UNIT * 15 // 100)))
        self.assertEqual([leg["taken"] for leg in edge["legs"]], [[["50", "3"]], [["45", "3"]]])
        # Three sets at one price k: 3k + 1.35 <= 3 on Beta, 3k + 1.50 <= 3 on Alpha.
        self.assertEqual(edge["kill_prices"], ["55", "50"])
        self.assertTrue(any("fill_ns" in r for r in result["summary"]["rows"]))

    def test_three_leg_set_prices_every_market(self):
        h = self.harness()
        h.kalshi_ask(12, "h20", "outcome", (30, 4))
        h.kalshi_ask(12, "h21", "outcome", (25, 4))
        h.kalshi_ask(12, "series", "complement", (40, 4))
        legs = (("kalshi:h20", "outcome"), ("kalshi:h21", "outcome"), ("kalshi:series", "complement"))
        entity = self.entity(h, legs)
        obs = h.strategy.strategy.evaluate(entity, tuple(h.strategy.views[k] for k in entity.legs),
                                           Context(12, 8, 0, h.strategy.experiment.experiment_sha256))
        self.assertEqual((obs.value_class, int(obs.payload["gap_gross"])), ("NET_POSITIVE", UNIT * 5 // 100))
        self.assertEqual(len(obs.payload["native_legs"]), 3)
        h.finish()
        (row,) = self.fills(h, legs)
        self.assertEqual(row["fill"]["results"][0]["steps"], "4")   # every level shares the 0.05 edge

    def test_fill_value_prices_fees_like_the_trigger(self):
        h = self.harness(fees="collateral", value=policy(ONE))
        h.kalshi_ask(12, "series", "outcome", (45, 3))
        h.kalshi_ask(12, "beta", "outcome", (50, 3))
        legs = (("kalshi:beta", "outcome"), ("kalshi:series", "outcome"))
        entity = self.entity(h, legs)
        obs = h.strategy.strategy.evaluate(entity, tuple(h.strategy.views[k] for k in entity.legs),
                                           Context(12, 8, 0, h.strategy.experiment.experiment_sha256))
        # 0.45 and 0.50 BUYs each pay 0.02 on Kalshi's non-direct cent grid.
        self.assertEqual(int(obs.payload["gap_net"]), UNIT // 100)
        h.finish()
        (row,) = self.fills(h, legs)
        self.assertEqual(row["fill"]["results"][0]["value"], str(UNIT // 100))

    def test_reader_needs_the_fee_catalog_and_rejects_a_tampered_fill(self):
        h = self.harness()
        h.kalshi_ask(12, "series", "outcome", (45, 3))
        h.kalshi_ask(12, "beta", "outcome", (50, 3))
        result = h.finish()
        manifest = result["manifest"]
        with self.assertRaisesRegex(ProtocolError, "fee engine identity"):
            validate_content(h.output, h.strategy.snapshot, manifest, None)
        rows = h.records("episodes.ndjson")
        net = next(r for r in rows if r["kind"] == "net")
        net["fill"]["results"][0]["value"] = str(int(net["fill"]["results"][0]["value"]) + 1)
        raw = b"".join(encoded(row) + b"\n" for row in rows)
        (h.output / "episodes.ndjson").write_bytes(raw)
        m = copy.deepcopy(manifest)
        m["files"]["episodes.ndjson"] = {"sha256": hashlib.sha256(raw).hexdigest(),
                                         "byte_length": len(raw), "records": len(rows)}
        with self.assertRaisesRegex(ProtocolError, "fill value"):
            validate_content(h.output, h.strategy.snapshot, m, h.strategy.strategy.bridge)

    def test_policy_is_closed(self):
        base = policy()
        policy_config(base)
        for bad in ({**base, "max_legs": 1}, {**base, "max_legs": 5}, {**base, "version": 2},
                    {**base, "sizes_contracts": ["1"]}, {**base, "headline_latency_ns": "2"},
                    {**base, "fills": {**base["fills"], "sizings": []}}):
            with self.subTest(bad=sorted(set(bad) ^ set(base)) or bad), self.assertRaises(ProtocolError):
                policy_config(bad)


if __name__ == "__main__":
    unittest.main()
