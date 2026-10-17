"""New-policy rollout: exact contracts, flags, decisions and production readers."""
import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from replay.preparation import encoded
from replay.streams.protocol import ProtocolError
from replay.tests.test_cross_venue_arbitrage import EDGE, fill_policy_v3
from replay.tests.economic_scenarios import M, kalshi_books, ladder, v2_policy
from replay.tests.test_streams import fields

MS = 1_000_000
FLAGS = {"version": 1, "stale_ms": 5000, "settlement_allowlist": ["series_moneyline"]}
T10 = {"name": "t10", "role": "records", "target_contracts": "10"}
PROBES = {"version": 1, "sizings": ["edge", "t10"], "confirm_ms": [0, 500, 1000],
          "arrival_ms": [100, 250, 500, 1000, 2000],
          "sequencing": ["simultaneous", "slow_first"],
          "unknown_taker_delay_ms": {"kalshi": 0, "limitless": 0, "polymarket": 1000}}


def policy(base, version=4, **changes):
    value = copy.deepcopy(base)
    value.update(version=version, flags=copy.deepcopy(FLAGS))
    value["fills"]["sizings"] = [EDGE, T10]
    value["fills"]["probes"] = copy.deepcopy(PROBES)
    value.update(changes)
    return value


class PolicyTests(unittest.TestCase):
    def test_new_closed_policy_versions_and_old_closed_parsing(self):
        from replay.strategies.cross_venue_arbitrage.contract import policy_config
        from replay.strategies.same_venue_multi_market.contract import policy_config as multi_config
        from replay.tests.test_same_venue_multi_market import policy as multi_policy
        base = json.loads(Path("replay/strategies/cross_venue_arbitrage/config.example.json").read_text())["policy"]
        base = {k: v for k, v in base.items() if k != "flags"}
        base["version"] = 3
        for parser, old, version in ((policy_config, base, 4), (multi_config, multi_policy(), 2)):
            new = policy(old, version)
            self.assertEqual(parser(new), new)
            for name, bad in (("flags", None), ("flags", {**FLAGS, "stale_ms": True}),
                              ("flags", {**FLAGS, "settlement_allowlist": [True]})):
                candidate = copy.deepcopy(new)
                candidate[name] = bad
                with self.assertRaises(ProtocolError):
                    parser(candidate)
            with self.assertRaises(ProtocolError):
                parser({**old, "flags": FLAGS})
            with self.assertRaises(ProtocolError):
                parser({**new, "extra": True})
            with self.assertRaises(ProtocolError):
                parser({**new, "version": float(version)})

    def test_descriptor_market_mapping_and_terms_are_decision_time_pure(self):
        from replay.strategies._shared.execution_realism import market_delays, record_flags, check_flags
        snapshot = {"outcomes": {"document": {"version": 2, "markets": [
            {"market_id": "polymarket:market", "market_type": "map_winner", "execution_terms": {
                "changes": [{"from": "1970-01-01T00:00:00.01Z", "taker_delay_ms": 1000},
                            {"from": "1970-01-01T00:00:00.02Z", "taker_delay_ms": None}]}},
            {"market_id": "kalshi:market", "market_type": "series_moneyline",
             "execution_terms": {"changes": []}}]}}}
        entity = SimpleNamespace(legs=(("polymarket:token", "outcome"), ("kalshi:market", "complement")),
            descriptor={"legs": [{"market_id": "polymarket:market"}, {"market_id": "kalshi:market"}]})
        self.assertEqual(market_delays(snapshot, entity, 15 * MS), (1000 * MS, None))
        self.assertEqual(market_delays(snapshot, entity, 25 * MS), (None, None))
        ages = [{"book_ns": "9000000000", "best_ns": "5000000001"},
                {"book_ns": "9000000000", "best_ns": "5000000000"}]
        flags = record_flags(snapshot, entity, FLAGS, ages)
        self.assertEqual(flags, {"pairing_class": "mixed:map_winner+series_moneyline",
                                 "settlement_review": True, "stale_legs": [0]})
        check_flags(flags, snapshot, entity, FLAGS, ages)
        for field, bad in (("pairing_class", "map_winner"), ("settlement_review", False),
                           ("stale_legs", [1]), ("stale_legs", [False])):
            with self.assertRaises(ProtocolError):
                check_flags({**flags, field: bad}, snapshot, entity, FLAGS, ages)
        snapshot["outcomes"]["document"]["markets"] = []
        self.assertEqual(record_flags(snapshot, entity, FLAGS, ages)["pairing_class"], "unknown")


def seconds(original):
    def config(*args, **kwargs):
        result = original(*args, **kwargs)
        for row in (result, *result["occurrences"]):
            for field in ("start_ns", "end_ns"):
                row[field] = str(int(row[field]) * 10**9)
        return result
    return config


def observed(doc, *, zero=False):
    doc = copy.deepcopy(doc)
    doc["version"] = 2
    for market in doc["markets"]:
        market["execution_terms"] = {"status": "unavailable", "changes": []}
        if market["venue"] == "polymarket":
            market["execution_terms"] = {"status": "observed", "changes": [
                {"from": "1970-01-01T00:00:15Z", "taker_delay_ms": 1000},
                {"from": "1970-01-01T00:00:16.5Z", "taker_delay_ms": 2000}]}
            if zero:
                market["execution_terms"]["changes"] = [
                    {"from": "1970-01-01T00:00:15Z", "taker_delay_ms": 0}]
    return doc


class IntegratedTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.counter = 0

    def harness(self, kind, *, terms=False, step="1", quantity=20, populate=True, max_levels=64, **kwargs):
        import replay.tests.test_cross_venue_arbitrage as cross
        import replay.tests.test_same_venue_multi_market as multi
        import replay.tests.test_implication_cover as imp
        self.counter += 1
        root = Path(self.tmp.name) / str(self.counter)
        root.mkdir()
        if kind == "multi":
            value = policy(multi.policy(step=step), 2)
            value["fills"]["max_levels"] = max_levels
            with patch.object(multi, "config", seconds(multi.config)):
                h = multi.Harness(root, value=value,
                                  doc=observed(multi.sibling_document()) if terms else multi.sibling_document())
        else:
            old = cross.fill_policy_v3
            def new_policy(*args, **kwargs):
                value = policy(old(*args, **kwargs))
                value["fills"]["max_levels"] = max_levels
                return value
            doc = (imp.implication_document() if kind != "cross" else cross.document())
            if terms:
                doc = observed(doc, zero=terms == "zero")
            fixture = cross.Harness if kind == "cross" else imp.Harness
            with patch.object(cross, "config", seconds(cross.config)), \
                 patch.object(cross, "fill_policy_v3", new_policy):
                h = fixture(root, fills=(EDGE,), step=step, doc=doc,
                            **({"mode": kind} if kind != "cross" else {}),
                            **({"scales": {"kalshi": ("2", "2")}} if step == "0.01" else {}), **kwargs)
        self.addCleanup(h.close)
        if not populate:
            return h
        h.window(end=40 * 10**9)
        # Quiet books are six seconds old when the second leg appears. Both
        # modes buy all legs; keep native bid below ask to avoid self-crossing.
        for p in h.initial["plans"]:
            if p["venue"] == "kalshi" and p["instrument"] == "kalshi:series":
                ladder(h, 10 * 10**9, p["instrument"], p["orientation"],
                       bids=((60, quantity * 10**int(p["quantity_scale"])),),
                       asks=((70, quantity * 10**int(p["quantity_scale"])),))
        for p in h.initial["plans"]:
            if p["venue"] == "kalshi" and p["instrument"] != "kalshi:series":
                ladder(h, 16 * 10**9, p["instrument"], p["orientation"],
                       bids=((60, quantity * 10**int(p["quantity_scale"])),),
                       asks=((70, quantity * 10**int(p["quantity_scale"])),))
            elif p["venue"] == "polymarket":
                ladder(h, 16 * 10**9, p["instrument"], p["orientation"],
                       bids=((100, quantity * M),), asks=((400, quantity * M),))
        return h

    def exit_case(self, kind="cross", orientation="outcome", *, step="1", exit_bids=((30, 10),),
                  unusable=False, terms=False, max_levels=64):
        h = self.harness(kind, step=step, populate=False, terms=terms, max_levels=max_levels)
        h.window(end=40 * 10**9)
        acquired = ("kalshi:series", orientation)
        other = {"cross": ("polymarket:987" if orientation == "outcome" else "polymarket:123", "outcome"),
                 "multi": ("kalshi:beta", "outcome"),
                 "same_venue": ("kalshi:sweep", "complement"),
                 "cross_venue": ("polymarket:654", "outcome")}[kind]
        unit = 100 if step == "0.01" else 1
        own = tuple((p, q * unit) for p, q in exit_bids)
        source = ((60, 10 * unit),)
        def pair(at, own_bids=own, invalid=None):
            kalshi_books(h, at, yes=own_bids if orientation == "outcome" else source,
                         no=source if orientation == "outcome" else own_bids, unusable=invalid)
        pair(16_000 * MS)
        if other[0].startswith("kalshi:"):
            kalshi_books(h, 16_000 * MS, other[0],
                         yes=((60 if other[1] == "complement" else 30, 10),),
                         no=((60 if other[1] == "outcome" else 30, 10),))
            kalshi_books(h, 16_050 * MS, other[0],
                         yes=((60, 6),) if other[1] == "complement" else ((30, 10),),
                         no=((60, 6),) if other[1] == "outcome" else ((30, 10),))
        else:
            ladder(h, 16_000 * MS, *other, bids=((100, 10 * M),), asks=((400, 10 * M),))
            ladder(h, 16_050 * MS, *other, bids=((100, 10 * M),), asks=((400, 6 * M),))
        if unusable:
            pair(16_075 * MS, invalid=orientation)
        entity = next(e for e in h.strategy.entities.values() if e.admission is None
                      and tuple((d["instrument"], d["orientation"]) for d in e.descriptor["legs"])
                      == tuple(sorted((acquired, other))))
        return h, entity

    def exit_rows(self, h, entity):
        h.finish()  # Real writer and the strategy's independent semantic reader.
        episode = next(r for r in h.records("episodes.ndjson") if r["kind"] == "net"
                       and r["entity"] == h.strategy.index[entity.id] and r["start_ns"] == str(16_000 * MS))
        rows = [r for r in h.records("probes.ndjson") if r["episode_id"] == episode["episode_id"]]
        self.assertEqual(len(rows), 60)
        row = next(r for r in rows if r["sizing"] == "t10" and r["confirm_ms"] == 0
                   and r["arrival_ms"] == 100 and r["sequencing"] == "simultaneous")
        return rows, row

    def test_bids_only_kalshi_yes_and_no_excess_use_acquired_native_bids(self):
        for orientation in ("outcome", "complement"):
            for step, unit in (("1", 1), ("0.01", 100)):
                with self.subTest(orientation=orientation, step=step):
                    h, entity = self.exit_case(orientation=orientation, step=step)
                    _, row = self.exit_rows(h, entity)
                    leg = row["legs"][0]
                    self.assertEqual((row["status"], row["matched_steps"]),
                                     ("partial", "6" if step == "1" else "600"))
                    self.assertEqual(leg["excess_atoms"], str(4 * unit))
                    actual = (leg["exit_consumed"], leg["exit_proceeds"], row["leg_loss_gross"])
                    self.assertEqual(actual, ([["30", str(4 * unit)]], str(120 * unit), str(4 * 10**35)))

    def test_bids_only_kalshi_insufficient_empty_and_unusable_exit_depth(self):
        for bids, unusable, walk, proceeds, loss in (
                (((30, 1), (20, 1)), False, [["30", "1"], ["20", "1"]], "50", 11 * 10**35),
                ((), False, [], "0", 16 * 10**35),
                (((30, 10),), True, [], "0", 16 * 10**35)):
            with self.subTest(bids=bids, unusable=unusable):
                h, entity = self.exit_case(exit_bids=bids, unusable=unusable)
                _, row = self.exit_rows(h, entity)
                self.assertEqual(row["legs"][0]["excess_atoms"], "4")
                self.assertEqual((row["legs"][0]["exit_consumed"], row["legs"][0]["exit_proceeds"],
                                  row["leg_loss_gross"]), (walk, proceeds, str(loss)))

    def test_bids_only_kalshi_all_four_factories_read_all_60_scenarios(self):
        # Whole-file hashes recorded after §2 but before §8: bounded exits
        # must not change any bought-leg row that fits within the cap.
        expected = {
            "cross": "60b498b7356d5985490bb08a4411c4c942877db0d677aa45dea55a75822a4a16",
            "multi": "947762d0a94cf9719634bed0a6d35ec728eb2c162173430f6fabd88e0bfc36c9",
            "same_venue": "120dd68a7363fd071292b6e346bbfe49d3d88ca4e23e5b0d9bd29603792f7570",
            "cross_venue": "ecdc18b2c0d8f49e43342406293007ba43f9d69aea264f4a5f9fe86155d6d61d"}
        for kind, sha in expected.items():
            with self.subTest(kind=kind):
                h, entity = self.exit_case(kind)
                _, row = self.exit_rows(h, entity)
                index = next(i for i, d in enumerate(entity.descriptor["legs"])
                             if d["instrument"] == "kalshi:series")
                leg = row["legs"][index]
                self.assertEqual((leg["excess_atoms"], leg["exit_consumed"], leg["exit_proceeds"],
                                  row["leg_loss_gross"]),
                                 ("4", [["30", "4"]], "120", str(4 * 10**35)))
                self.assertEqual(hashlib.sha256((h.output / "probes.ndjson").read_bytes()).hexdigest(), sha)

    def test_bids_only_kalshi_thin_exit_stops_at_cap_without_failing_attempt(self):
        h, entity = self.exit_case(exit_bids=((30, 1), (20, 1), (10, 10)), max_levels=2)
        _, row = self.exit_rows(h, entity)
        self.assertEqual((row["status"], row["matched_steps"]), ("partial", "6"))
        self.assertEqual((row["legs"][0]["excess_atoms"], row["legs"][0]["exit_consumed"],
                          row["legs"][0]["exit_proceeds"], row["leg_loss_gross"]),
                         ("4", [["30", "1"], ["20", "1"]], "50", str(11 * 10**35)))

    def test_bids_only_exit_uses_final_same_time_arrival_not_decision_or_later_view(self):
        h, entity = self.exit_case(exit_bids=((20, 10),))
        kalshi_books(h, 16_100 * MS, yes=((31, 10),), no=((60, 10),))
        kalshi_books(h, 16_100 * MS, yes=((30, 10),), no=((60, 10),))
        kalshi_books(h, 16_200 * MS, yes=((10, 10),), no=((60, 10),))
        _, row = self.exit_rows(h, entity)
        self.assertEqual(row["legs"][0]["arrival_ns"], str(16_100 * MS))
        self.assertEqual(row["legs"][0]["exit_consumed"], [["30", "4"]])

    def test_probe_exit_bind_failures_are_exact(self):
        from replay.economic_sdk import FillSpec
        for failure, message in (("plan", "probe exit book not planned"),
                                 ("builder", "probe exit book not planned"),
                                 ("ladder", "probe exit ladder not retained"),
                                 ("sold", "probe exit unsupported for Kalshi sold leg")):
            with self.subTest(failure=failure):
                h = self.harness("cross", populate=False, defer_initial=True)
                strategy = h.strategy.strategy
                pair = (("kalshi:series", "outcome"), ("polymarket:987", "outcome"))
                basket = next(b for b in strategy.baskets(strategy.snapshot, strategy.policy, 0)
                              if b.admission is None and
                              tuple((d["instrument"], d["orientation"]) for d in b.descriptor["legs"]) == pair)
                key = pair[0]
                if failure == "plan":
                    del h.strategy.plans[key]
                elif failure == "builder":
                    del h.strategy.builders[key]
                elif failure == "ladder":
                    h.strategy.builders[key].ladders = ("kalshi_complement_ask",)
                else:
                    original = strategy.fill_spec
                    strategy.fill_spec = lambda e: FillSpec(("bid", "ask"), original(e).units)
                    h.strategy.builders[basket.legs[0]].ladders += ("bid",)
                with patch.object(strategy, "baskets", return_value=(basket,)):
                    with self.assertRaises(ProtocolError) as error:
                        h.send("initial", h.initial)
                self.assertEqual(str(error.exception), message)

    def test_pm_unknown_default_is_1000ms_but_observed_zero_wins(self):
        for observed_zero in (False, True):
            with self.subTest(observed_zero=observed_zero):
                h, entity = self.exit_case(terms="zero" if observed_zero else False)
                _, row = self.exit_rows(h, entity)
                self.assertEqual((row["legs"][1]["venue_delay_ms"], row["legs"][1]["delay_source"]),
                                 (0, "terms") if observed_zero else (1000, "default"))

    def non_kalshi_case(self):
        # Hold the configured delay fixed across R12, independently of its new
        # reference default. Kalshi plans exist, but cannot open an episode.
        fixed = {**PROBES, "unknown_taker_delay_ms": {"kalshi": 0, "limitless": 0, "polymarket": 3000}}
        with patch(__name__ + ".PROBES", fixed):
            h = self.harness("cross", populate=False, limitless=True)
        h.window(end=40 * 10**9)
        kalshi_books(h, 16_000 * MS)
        ladder(h, 16_000 * MS, "limitless:slug", bids=((250, 10 * M),), asks=((400, 10 * M),))
        ladder(h, 16_000 * MS, "polymarket:987", bids=((100, 10 * M),), asks=((400, 10 * M),))
        ladder(h, 16_050 * MS, "polymarket:987", bids=((100, 10 * M),), asks=((400, 6 * M),))
        h.finish()
        return h

    def test_non_kalshi_realism_probes_match_base_bytes(self):
        h = self.non_kalshi_case()
        self.assertEqual(hashlib.sha256((h.output / "probes.ndjson").read_bytes()).hexdigest(),
                         "89f36166ac2f8b3df7a4e05e461d79d555179976cc9d2add79d7933e754036d0")

    def test_all_realism_requirements_read_only_kalshi_bids(self):
        for kind in ("cross", "multi", "same_venue", "cross_venue"):
            with self.subTest(kind=kind):
                h = self.harness(kind, populate=False)
                requirements = h.strategy.strategy.requirements(h.strategy.snapshot, h.strategy.experiment.policy)
                for key, requirement in requirements.books.items():
                    if key[0].startswith("kalshi:"):
                        self.assertEqual(requirement.sides, ("bid",))
                        self.assertEqual(requirement.transforms, ("kalshi_complement_ask",))
                        self.assertEqual(set(requirement.ladders), {"bid", "kalshi_complement_ask"})

    def rehash(self, h, manifest, name, rows):
        raw = b"".join(encoded(row) + b"\n" for row in rows)
        (h.output / name).write_bytes(raw)
        result = copy.deepcopy(manifest)
        result["files"][name] = {"sha256": hashlib.sha256(raw).hexdigest(),
                                "byte_length": len(raw), "records": len(rows)}
        return result

    def test_all_four_reference_policies_integrate_reader_flags_delays_t10_and_60_rows(self):
        for kind in ("cross", "multi", "same_venue", "cross_venue"):
            for terms in (False, True):
                with self.subTest(kind=kind, terms=terms):
                    h = self.harness(kind, terms=terms)
                    result = h.finish()
                    episodes = [r for r in h.records("episodes.ndjson") if r["kind"] == "net"]
                    self.assertTrue(episodes)
                    probes = h.records("probes.ndjson")
                    self.assertEqual(len(probes), 60 * len(episodes))
                    self.assertTrue(all([r["name"] for r in ep["fill"]["results"]] == ["edge", "t10"]
                                        for ep in episodes))
                    self.assertTrue(all(ep["flags"]["settlement_review"] is
                        (ep["flags"]["pairing_class"] != "series_moneyline") for ep in episodes))
                    self.assertTrue(any(ep["flags"]["stale_legs"] for ep in episodes))
                    self.assertTrue(all("flags" not in r for r in h.records("episodes.ndjson")
                                        if r["kind"] != "net"))
                    self.assertTrue(all(set(r["decision"]) == {"time_ns", "steps", "limits", "ages", "value", "legs"}
                                        for r in probes if r["decision"] is not None))
                    self.assertTrue(any(r["confirm_ms"] == 1000 and r["status"] == "filled" for r in probes))
                    for row in probes:
                        if row["decision"] is None:
                            continue
                        # Market terms are shared by all PM markets in this fixture.
                        pm = [l for l in row["legs"] if l["venue_delay_ms"] > 0]
                        self.assertTrue(all(l["delay_source"] == ("terms" if terms else "default") for l in pm))
                    self.assertTrue(result["summary"]["rows"])
                    # The terms change at 16.5s: d=16s uses 1s while d=16.5/17s
                    # uses 2s, including slow-first sends later than d.
                    if terms:
                        for row in probes:
                            if row["decision"] is not None:
                                expected = 1000 if int(row["decision"]["time_ns"]) < 16_500_000_000 else 2000
                                self.assertTrue(all(leg["venue_delay_ms"] == expected for leg in row["legs"]
                                                    if leg["delay_source"] == "terms"))

    def test_exact_fractional_step_and_recording_short_target_survive_reading(self):
        h = self.harness("cross", step="0.01", quantity=5)
        h.finish()
        episodes = [r for r in h.records("episodes.ndjson") if r["kind"] == "net"]
        self.assertTrue(episodes)
        for episode in episodes:
            edge, t10 = episode["fill"]["results"]
            self.assertEqual((edge["steps"], t10["steps"]), ("500", "500"))
            self.assertFalse(t10["tradeable"])
            self.assertEqual(t10["value"], edge["value"])
            # Steps are hundredths; native Kalshi quantities are hundredths
            # while PM quantities are millionths, with no floating arithmetic.
            self.assertEqual(episode["fill"]["units"], ["1", "10000"])
        rows = h.records("probes.ndjson")
        self.assertTrue(any(r["sizing"] == "t10" and r["confirm_ms"] == 0
                            and r["status"] == "filled" and r["decision"]["steps"] == "500" for r in rows))
        self.assertTrue(all(r["status"] == "decision_untradeable" for r in rows
                            if r["sizing"] == "t10" and r["confirm_ms"] > 0))

    def assert_kalshi_ask_only_recovery(self, kind, update):
        from dataclasses import replace
        from replay.strategies.cross_venue_arbitrage.strategy import CrossVenueArbitrage
        from replay.strategies.same_venue_multi_market.strategy import SameVenueMultiMarket
        owner = SameVenueMultiMarket if kind == "multi" else CrossVenueArbitrage
        original = owner.requirements
        def synthetic_asks(strategy, snapshot, policy):
            result = original(strategy, snapshot, policy)
            return replace(result, books={key: replace(req, sides=("bid", "ask"))
                           if key[0].startswith("kalshi:") else req for key, req in result.books.items()})
        # R10 remains a defensive declared-input test, not a normalizer shape.
        # R12 production requirements never read Kalshi asks. Explicitly opt
        # into them only for this pre-existing synthetic crossed fixture.
        with patch.object(owner, "requirements", synthetic_asks):
            h = self.harness(kind)
        entity = next(e for e in h.strategy.entities.values()
                      if e.admission is None and len(e.legs) == 2
                      and any(k[0] == "kalshi:series" for k in e.legs))
        key = next(k for k in entity.legs if k[0] == "kalshi:series")
        # The final state at 16s is crossed before this route can open a fill.
        # Its unchanged native bid still projects to a 0.40 acquisition ask.
        ladder(h, 16 * 10**9, *key, bids=((60, 20),), asks=((50, 20),))
        h.group(17 * 10**9)
        self.assertEqual(h.strategy.current[entity.id][0].status, "SELF_CROSSED_LEG")
        prior = h.strategy.views[key]
        self.assertEqual(prior.validity, "usable")
        self.assertTrue(prior.crossed)
        if update == "snapshot":
            ladder(h, 18 * 10**9, *key, bids=((60, 20),), asks=((70, 20),))
        else:
            plan = h.initial["plans"][h.plan_index(*key)]
            transition = h.transition(plan, 18 * 10**9)
            transition["decision"] = {"kind": "operations", "operations": [
                {"instrument": key[0], "orientation": key[1], "side": "ask",
                 "price": {"atoms": str(price), "scale": plan["price_scale"],
                           "unit": "quote_per_contract"},
                 "change": ({"kind": "delete"} if price == 50 else
                            {"kind": "set", "value": {"atoms": "20",
                             "scale": plan["quantity_scale"], "unit": "contracts"}}),
                 "book_hash": None} for price in (50, 70)]}
            ref = h.ref(18 * 10**9)
            h.send("cut", {"origin": {"kind": "group", "pin": h.pin,
                "first": ref["address"], "last": ref["address"], "visible_ns": str(18 * 10**9)},
                "market_events": [], "book_transitions": [transition]})
        h.group(19 * 10**9)
        current = h.strategy.views[key]
        self.assertFalse(current.crossed)
        self.assertIs(current.fills["bid"][1], prior.fills["bid"][1])
        self.assertIs(current.transformed["kalshi_complement_ask"][1],
                      prior.transformed["kalshi_complement_ask"][1])
        # Verify the market is economically positive independently of memo reuse.
        from replay.economic_sdk.types import Context
        fresh = h.strategy.strategy.evaluate(entity, tuple(h.strategy.views[k] for k in entity.legs),
            Context(19 * 10**9, h.seq, 0, h.strategy.experiment.experiment_sha256))
        self.assertIn("net", fresh.predicates)
        self.assertEqual(int(fresh.payload["gap_gross"]), 2 * 10**35)
        # finish() runs the real writer and independent semantic reader before
        # checking that persisted exclusion time ends at the ask-only recovery.
        h.finish()
        index = h.strategy.index[entity.id]
        denominator = next(r for r in h.records("denominators.ndjson")
                           if r["scope"] == 0 and r["entity"] == index)
        self.assertEqual(denominator["status_ns"]["SELF_CROSSED_LEG"], str(2 * 10**9))
        self.assertEqual(denominator["status_ns"]["DEPTH_SUFFICIENT"], str(22 * 10**9))
        episodes = [r for r in h.records("episodes.ndjson")
                    if r["scope"] == 0 and r["entity"] == index and r["kind"] == "net"]
        self.assertEqual([(r["start_ns"], r["end_ns"]) for r in episodes],
                         [(str(18 * 10**9), str(40 * 10**9))])
        probes = [r for r in h.records("probes.ndjson") if r["episode_id"] == episodes[0]["episode_id"]]
        self.assertEqual(len(probes), 60)
        self.assertTrue(all(r["status"] == "filled" for r in probes))

    def test_kalshi_ask_only_operations_uncross_recovers_all_four_policies(self):
        for kind in ("cross", "multi", "same_venue", "cross_venue"):
            with self.subTest(kind=kind):
                self.assert_kalshi_ask_only_recovery(kind, "operations")

    def test_kalshi_same_bid_snapshot_uncross_recovers_all_four_policies(self):
        for kind in ("cross", "multi", "same_venue", "cross_venue"):
            with self.subTest(kind=kind):
                self.assert_kalshi_ask_only_recovery(kind, "snapshot")

    def test_new_policy_sparse_and_full_cuts_preserve_probe_bytes_and_risk_numbering(self):
        outputs = []
        for full in (False, True):
            h = self.harness("cross", terms=True)
            risk = h.seq - 1
            def send(kind, body):
                nonlocal risk
                if kind == "cut":
                    risk += 1
                    body = {**body, "risk_sequence": str(risk)}
                elif kind == "terminal":
                    body = {**body, "risk_cuts": str(risk)}
                cut = h.decoder.apply(fields({"version": "4", "run_id": "coverage-test",
                    "attempt_id": h.context["attempt_id"], "sequence": str(h.seq),
                    "kind": kind, "body": body}))
                h.seq += 1
                h.strategy(cut)
                return cut
            h.send = send
            for millis in (16100, 16250, 16500, 17000, 18000, 20000, 22000):
                if full:
                    ref = h.ref(millis * MS)
                    h.send("cut", {"origin": {"kind": "group", "pin": h.pin,
                        "first": ref["address"], "last": ref["address"], "visible_ns": str(millis * MS)},
                        "market_events": [], "book_transitions": []})
                else:
                    risk += 1
            h.finish()
            self.assertEqual(h.strategy.risk_sequence, risk + 1)
            self.assertEqual(h.initial["retain_boundaries_ns"], ["10000000000", "40000000000"])
            self.assertEqual(h.initial["retain_shift_offsets_ns"], [])
            outputs.append((h.output / "probes.ndjson").read_bytes())
        self.assertEqual(*outputs)

    def test_new_decision_evidence_is_bounded_before_retention(self):
        from replay.economic_sdk import bounds
        from replay.tests.test_economic_sdk import deep_size
        h = self.harness("cross")
        ref = h.ref(17_000_000_000)
        h.send("cut", {"origin": {"kind": "group", "pin": h.pin,
            "first": ref["address"], "last": ref["address"], "visible_ns": "17000000000"},
            "market_events": [], "book_transitions": []})
        manager = h.strategy.probes
        pending = {id(entry[-1]): entry[-1] for entry in manager.heap if not entry[-1].finished}
        self.assertTrue(pending)
        self.assertTrue(any(probe.decision is not None and "legs" in probe.decision for probe in pending.values()))
        for probe in pending.values():
            self.assertGreaterEqual(probe.cost, deep_size(probe))
        h.finish()
        self.assertEqual(manager.pending, 0)
        h = self.harness("cross")
        with patch.object(bounds, "MAX_PENDING_PROBES", 59):
            with self.assertRaisesRegex(ProtocolError, "pending probe bound"):
                h.finish()
        self.assertEqual(h.strategy.probes.pending, 59)

    def test_rehashed_flags_and_decision_economics_are_reader_checked(self):
        from replay.strategies.cross_venue_arbitrage.output import validate_content
        h = self.harness("cross", terms=True)
        result = h.finish()
        original = h.records("episodes.ndjson")
        for field, bad in (("pairing_class", "unknown"), ("settlement_review", True),
                           ("stale_legs", []), ("stale_legs", [True])):
            rows = copy.deepcopy(original)
            next(r for r in rows if r["kind"] == "net")["flags"][field] = bad
            manifest = self.rehash(h, result["manifest"], "episodes.ndjson", rows)
            with self.subTest(field=field), self.assertRaisesRegex(ProtocolError, "fill flags"):
                validate_content(h.output, h.strategy.snapshot, manifest, h.strategy.strategy.bridge)
        self.rehash(h, result["manifest"], "episodes.ndjson", original)
        original = h.records("probes.ndjson")
        for field, change in (("value", lambda d: d.update(value="1")),
                              ("cost", lambda d: d["legs"][0].update(cost="1")),
                              ("unknown", lambda d: d.update(extra=True))):
            rows = copy.deepcopy(original)
            decision = next(r["decision"] for r in rows if r["confirm_ms"] == 500 and r["decision"] is not None)
            change(decision)
            manifest = self.rehash(h, result["manifest"], "probes.ndjson", rows)
            with self.subTest(field=field), self.assertRaises(ProtocolError):
                validate_content(h.output, h.strategy.snapshot, manifest, h.strategy.strategy.bridge)


class DecisionEvidenceTests(unittest.TestCase):
    def test_zero_step_recording_decision_carries_null_value_and_empty_walks(self):
        from replay.tests.test_execution_delay_probes import DelayedPair, ProbeTests, probes
        from replay.tests.test_economic_sdk_fills import LEG0, LEG1
        from replay.streams.protocol import require
        class EvidencePair(DelayedPair):
            probe_decision_evidence = True
            def fill_flags(self, entity, ages):
                return {"pairing_class": "unknown", "settlement_review": True, "stale_legs": []}
            def check_fill_flags(self, value, entity, ages):
                require(value == self.fill_flags(entity, ages), "synthetic flags")
        case = ProbeTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        governing = {"name": "two", "role": "governs", "target_contracts": "2"}
        recording = {"name": "record", "role": "records", "edge": True}
        h = case.harness(probes(sizings=["record"], arrival_ms=[0]), governing, recording,
                         strategy_class=EvidencePair)
        h.strategy.strategy.fill_value = lambda entity, steps, legs, time: 1 if steps >= 2 else 0
        case.book(h, 12, LEG0, (400, M), (410, M))
        case.book(h, 12, LEG1, (500, M), (510, M))
        row = case.row(h)
        self.assertIsNone(row["decision"]["value"])
        self.assertEqual(row["decision"]["steps"], "0")
        self.assertEqual(row["decision"]["legs"], [{"atoms": "0", "cost": "0", "taken": [], "consumed": []}] * 2)
        self.assertEqual((row["status"], row["value"]), ("empty", "0"))


class LegacyBytesTests(unittest.TestCase):
    def test_a1_native_probe_matches_base_bytes(self):
        from replay.tests.test_execution_delay_probes import ProbeTests, probes
        from replay.tests.test_economic_sdk_fills import LEG1
        case = ProbeTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        two = {"name": "two", "role": "governs", "target_contracts": "2"}
        h = case.harness(probes(sizings=["two"]), two)
        case.open(h)
        case.book(h, 15, LEG1, (500, M))
        h.finish()
        self.assertEqual(hashlib.sha256((h.output / "probes.ndjson").read_bytes()).hexdigest(),
                         "f26f5060f2ad6bc37cf2df4aab40fe2816519baeb4c403c40d7b1eaf739a6900")

    def test_every_old_policy_output_matches_the_dependency_base_bytes(self):
        # Recorded independently from 902871166496aa130fc493685112ba947a08f5e1,
        # before production changes. Hash every output, including receipts and
        # the explicit old-policy probe fixture from PR78.
        from replay.tests import test_cross_venue_arbitrage as cross
        from replay.tests import test_same_venue_multi_market as multi
        from replay.tests import test_implication_cover as imp
        from replay.tests.test_execution_probe_integration import ProbeIntegrationTests
        expected = json.loads((Path(__file__).parent / "fixtures/strategy_execution_realism_legacy.json").read_bytes())
        for kind in expected:
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as tmp:
                if kind == "cross":
                    h = cross.Harness(Path(tmp), fills=(EDGE,))
                    h.populate()
                elif kind == "multi":
                    h = multi.Harness(Path(tmp))
                    h.window()
                    for market in ("series", "beta"):
                        for orientation in ("outcome", "complement"):
                            h.kalshi_ask(12, market, orientation, (40, 3))
                elif kind == "old_probes":
                    case = ProbeIntegrationTests()
                    case.setUp()
                    self.addCleanup(case.doCleanups)
                    h = case.cross(observed=True)
                else:
                    h = imp.Harness(Path(tmp), mode=kind, fills=(EDGE,))
                    h.populate()
                self.addCleanup(h.close)
                h.finish()
                actual = {str(p.relative_to(h.output)): hashlib.sha256(p.read_bytes()).hexdigest()
                          for p in h.output.rglob("*") if p.is_file()}
                self.assertEqual(actual, expected[kind])


if __name__ == "__main__":
    unittest.main()
