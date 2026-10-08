"""Offline contract-shaped masks, native books and fee witnesses."""

import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from replay.strategies.cross_venue_arbitrage.strategy import build
from replay.strategies.cross_venue_arbitrage.contract import baskets, Inputs, UNIT, SCALE, asset_row, policy_config
from replay.strategies.cross_venue_arbitrage.output import read_provisional, read_completed, validate_content
from replay.economic_sdk.entities import resolve, scale_admission
from replay.economic_sdk.outcomes import outcome_scope
from replay.fees.artifacts import build_catalog
from replay.fees.schedules import Catalog, Kalshi, KalshiKind
from replay.fees.domain import Multiplier
from dataclasses import replace
from replay.preparation import prepare, build_snapshot, encoded
from replay.streams.protocol import ProtocolError
from replay.tests.test_bundle_coverage import Harness as BaseHarness
from replay.tests.test_preparation import config, detail
from replay.tests.test_preparation_outcomes import document
from replay.tests.test_same_venue_complement import strategy_config
from replay.tests.economic_scenarios import ladder, v2_policy
from replay.economic_sdk.types import Context


_PREPARE = prepare


def fixture_detail(limitless=False):
    d = detail()
    if limitless:
        d["context"]["markets"].append({"target_id": "limitless:internal", "venue": "limitless", "selected": True})
        d["context"]["targets"].append({"target_id": "limitless:internal", "venue": "limitless",
            "subscription_ids": ["slug"], "canonical_class": "esports.series_moneyline", "source_ref": "/test"})
        for f in ("markets", "targets"):
            d["context"][f].sort(key=lambda row: row["target_id"])
    return d


EDGE = {"name": "edge", "role": "governs", "edge": True}


def fill_policy_v3(policy, sizings, step="1"):
    """Policy 3: the policy 2 fields without the size sweep or controls, plus fills."""
    value = {k: v for k, v in policy.items() if k not in (
        "sizes_contracts", "headline_size_contracts", "controls", "controls_episodes",
        "controls_slices", "time_shift_ring_entries")}
    value.update(version=3, fills={"version": 1, "step_contracts": step, "max_levels": 64,
                                   "sizings": list(sizings)})
    return value


class Harness(BaseHarness):
    def __init__(self, root, *, doc=True, native=True, fees="zero", limitless=False, policy=None, valuation=True, scales=None,
                 fills=None, step="1", **kwargs):
        self.root = root
        d = fixture_detail(limitless)
        def prep_config():
            c = config(d)
            if native:
                for a in c["authorities"]:
                    if a["venue"] == "kalshi":
                        a.update(price_scale="2", quantity_scale="0")
            if scales:
                for a in c["authorities"]:
                    if a["venue"] in scales:
                        a["price_scale"], a["quantity_scale"] = scales[a["venue"]]
            return c
        def prepared(c, directory, **kw):
            if doc == "legacy":
                with patch("replay.preparation.build_snapshot", side_effect=lambda c, e, o=None: build_snapshot(c,e)):
                    return _PREPARE(c, directory, **kw)
            if doc is not True:
                return _PREPARE(c, directory, **kw, outcomes=lambda _: doc)
            return _PREPARE(c, directory, **kw, outcomes=lambda _: document())
        def factory(context):
            cfg = strategy_config(dict(context["config"]), root, known=True)
            cfg["policy"].update(v2_policy())
            if policy:
                cfg["policy"].update(policy)
            if fills is not None:
                cfg["policy"] = fill_policy_v3(cfg["policy"], fills, step)
            from replay.fees.artifacts import load_catalog, source_from_bytes
            cat = load_catalog(Path(cfg["fees"]["catalog_directory"]))
            # Synthetic evidenced multiplier zero is supported; ZeroFee is not a Kalshi model.
            cat = Catalog.build(tuple(replace(s, model=Kalshi(KalshiKind.QUADRATIC,
                                     Multiplier(1 if fees == "collateral" else 0, 0)))
                                      if s.scope.venue.value == "kalshi" else s for s in cat.schedules))
            raw = b"synthetic zero-fee contract, not live evidence"
            source = source_from_bytes("https://example.invalid/synthetic", 0, raw)
            directory = build_catalog(root / "supported-fees", cat, {source.sha256: raw})
            cfg["fees"].update(catalog_directory=str(directory), catalog_identity=cat.identity)
            if fees == "missing":
                catalog = Catalog.build(())
                directory = build_catalog(root / "missing-fees", catalog, {})
                cfg["fees"].update(catalog_directory=str(directory), catalog_identity=catalog.identity)
            if fees == "token":
                # Limitless CLOB's configured 3% received-token fee, no public schedule needed.
                from replay.fees.artifacts import load_catalog
                cat = load_catalog(Path(cfg["fees"]["catalog_directory"]))
                cat = Catalog.build(tuple(s for s in cat.schedules if s.scope.venue.value != "limitless"))
                source = __import__("replay.tests.test_same_venue_complement", fromlist=["x"]).source_from_bytes(
                    "https://example.invalid/synthetic", 0, b"synthetic zero-fee contract, not live evidence")
                directory = build_catalog(root / "token-fees", cat, {source.sha256: b"synthetic zero-fee contract, not live evidence"})
                cfg["fees"].update(catalog_directory=str(directory), catalog_identity=cat.identity)
            assets = sorted(cfg["fees"]["assets"].values(), key=encoded)
            cfg["valuation"] = {"version": 1, "kind": "PARITY_SCENARIO", "unit": "research_dollar", "assets": assets} if valuation else None
            self.cross_config = cfg
            return build({**context, "config": cfg})
        with patch("replay.tests.test_bundle_coverage.detail", side_effect=lambda: copy.deepcopy(d)), \
             patch("replay.tests.test_bundle_coverage.config", side_effect=prep_config), \
             patch("replay.tests.test_bundle_coverage.prepare", side_effect=prepared), \
             patch("replay.tests.test_bundle_coverage.build", side_effect=factory):
            super().__init__(root, mixed=True, **kwargs)

    def plan_index(self, instrument, orientation="outcome"):
        return next(i for i,p in enumerate(self.initial["plans"]) if (p["instrument"],p["orientation"]) == (instrument, orientation))

    def populate(self, *, pm_home=650, pm_away=580, kalshi_yes=70, kalshi_no=60, quantity=3):
        self.window()
        for orientation, price in (("outcome", kalshi_yes), ("complement", kalshi_no)):
            plan = self.initial["plans"][self.plan_index("kalshi:series", orientation)]
            scale = 10 ** int(plan["quantity_scale"])
            p = price if int(plan["price_scale"]) == 2 else price * 10
            ladder(self, 12, "kalshi:series", orientation, bids=((p, quantity*scale),))
        for token, price in (("123", pm_home), ("987", pm_away)):
            plan = self.initial["plans"][self.plan_index("polymarket:" + token)]
            ps, qs = int(plan["price_scale"]), int(plan["quantity_scale"])
            price_atoms = price * 10 ** (ps - 3) if ps >= 3 else price // 10 ** (3 - ps)
            bid_atoms = 100 * 10 ** (ps - 3) if ps >= 3 else 100 // 10 ** (3 - ps)
            ladder(self, 12, "polymarket:"+token, bids=((bid_atoms, quantity*10**qs),), asks=((price_atoms, quantity*10**qs),))

    def records(self, file):
        from replay.tests.sdk_tables import records
        return records(self.output, file, len(self.snapshot["scopes"]))

    def finish(self):
        self.terminal(); self.decoder.finish(); self.strategy.finish()
        return read_provisional(self.output, self.root/"context", expected_sha256=self.sha,
                                bridge=self.strategy.strategy.bridge)

    def close(self):
        for writer in self.strategy.writers.values():
            writer.stream.close()


class CrossVenueTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def harness(self, **kw):
        h = Harness(self.root, **kw)
        self.addCleanup(h.close)
        return h

    def observe(self, h, acquired):
        e = next(e for e in h.strategy.entities.values() if
                 tuple((d["instrument"], d["orientation"]) for d in e.descriptor["legs"]) == acquired
                 and e.descriptor["size_contracts"] == "1" and e.cls == "real")
        return h.strategy.strategy.evaluate(e, tuple(h.strategy.views[k] for k in e.legs),
             Context(12, 8, 0, h.strategy.experiment.experiment_sha256)), e

    def test_integrated_native_scales_both_directions_reader_and_determinism(self):
        manifests = []
        for attempt in ("a"*32, "b"*32):
            with tempfile.TemporaryDirectory() as tmp:
                h = Harness(Path(tmp), attempt=attempt, policy={"audit_intervals": True})
                h.populate()
                result = h.finish(); h.close()
                self.assertEqual(result["manifest"]["settlement_model"], "normal_resolution_only")
                self.assertEqual(result["manifest"]["outcomes_provider"], "universe")
                episodes = h.records("episodes.ndjson")
                vals = [r["open"]["values"] for r in episodes if r["kind"] == "net"]
                self.assertEqual(sorted(int(v["gap_net"]) for v in vals), sorted([UNIT*2//100, UNIT*5//100, UNIT*6//100, UNIT*15//100]))
                self.assertTrue(all(v["valuation"]["kind"] == "PARITY_SCENARIO" for v in vals))
                self.assertTrue(any(r["status_ns"].get("UNSUPPORTED_SHAPE") == "30" for r in result["summary"]["rows"]))
                self.assertTrue(any(r["status_ns"].get("NOT_CAPTURED") == "30" for r in result["summary"]["rows"]))
                manifests.append((h.output/"manifest.json").read_bytes())
                # Rehashed payload corruption must fail arithmetic, not just a file hash.
                strategy = h.strategy.strategy
                e = next(e for e in h.strategy.entities.values() if e.admission is None)
                value = copy.deepcopy(vals[0]); value["gap_net"] = str(int(value["gap_net"])+1)
                with self.assertRaises(ProtocolError):
                    strategy.open_facts(value, e, "net")
        self.assertEqual(*manifests)

    def test_positive_zero_negative_gross_and_exact_depth_boundary(self):
        h = self.harness()
        h.populate(pm_home=700, pm_away=600, kalshi_yes=70, kalshi_no=60, quantity=1)
        away = (("kalshi:series", "outcome"), ("polymarket:987", "outcome"))
        obs, _ = self.observe(h, away)
        self.assertEqual((obs.value_class, obs.payload["gap_gross"]), ("GROSS_NONPOSITIVE", "0"))
        ladder(h, 13, "polymarket:987", asks=((601, 10**6),))
        obs, _ = self.observe(h, away)
        self.assertEqual(int(obs.payload["gap_gross"]), -UNIT//1000)
        ladder(h, 14, "polymarket:987", asks=((599, 10**6),))
        obs, _ = self.observe(h, away)
        self.assertEqual(int(obs.payload["gap_net"]), UNIT//1000)
        result = h.finish()
        self.assertTrue(any(r["status_ns"].get("DEPTH_LIMITED") for r in result["summary"]["rows"] if r["size_contracts"] == "3"))

    def test_token_fee_reduces_payout_zero_and_negative_net(self):
        h = self.harness(limitless=True, fees="token")
        h.populate(pm_away=580)
        ladder(h, 12, "limitless:slug", bids=((100,3*10**6),), asks=((400,3*10**6),))
        pair = (("limitless:slug", "outcome"), ("polymarket:987", "outcome"))
        obs, _ = self.observe(h, pair)
        self.assertEqual((int(obs.payload["gap_gross"]), int(obs.payload["gap_net"])), (UNIT*2//100, -UNIT//100))
        self.assertEqual(int(obs.payload["payout_floor_net_e36"]), UNIT*97//100)
        ladder(h, 13, "polymarket:987", asks=((570,3*10**6),))
        obs, _ = self.observe(h, pair)
        self.assertEqual((obs.value_class, obs.payload["gap_net"]), ("NET_NONPOSITIVE", "0"))
        ladder(h, 14, "polymarket:987", asks=((560,3*10**6),))
        obs, _ = self.observe(h, pair)
        self.assertEqual(int(obs.payload["gap_net"]), UNIT//100)
        h.finish()

    def test_missing_fees_valuation_economics_and_untrusted_depth(self):
        h = self.harness(fees="missing")
        h.populate()
        pair = (("kalshi:series", "outcome"), ("polymarket:987", "outcome"))
        obs, _ = self.observe(h, pair)
        self.assertEqual((obs.value_class, obs.payload["gap_net"]), ("FEE_UNKNOWN", None))
        self.assertTrue(obs.reasons)
        s = h.strategy.strategy
        s.valuation = None
        self.assertEqual(self.observe(h, pair)[0].status, "VALUATION_UNKNOWN")
        s.valuation = h.cross_config["valuation"]
        with patch.object(s.bridge, "economics", return_value=None):
            self.assertEqual(self.observe(h, pair)[0].status, "ECONOMICS_UNKNOWN")
        ladder(h, 13, "polymarket:987", why={"kind": "connection_closed"})
        self.assertEqual(self.observe(h, pair)[0].status, "UNUSABLE")
        h.finish()

    def test_unavailable_void_overlap_incomplete_gap_and_mixed_masks(self):
        base = build_snapshot(config(), [{"provider":"universe", "detail":detail()}],
                              {"provider":"universe", "document":document()})
        h = self.harness()
        p = h.cross_config["policy"]
        for mode in ("legacy", "unavailable", "void", "incomplete", "gap", "mixed"):
            s = copy.deepcopy(base)
            if mode == "legacy":
                s = build_snapshot(config(), [{"provider":"universe", "detail":detail()}])
            elif mode == "unavailable":
                s["outcomes"] = {"provider":None,"unavailable":"universe_outcomes_unavailable"}
            elif mode == "void":
                for row in s["scopes"][0]["outcome_books"]:
                    if row["instrument"].startswith("kalshi:"):
                        row["status"] = "VOID_UNSUPPORTED"
            elif mode == "incomplete":
                s["outcomes"]["document"]["spaces"][0]["coverage"] = "INCOMPLETE_COVERAGE"
            elif mode == "gap":
                for row in s["scopes"][0]["outcome_books"]:
                    if row["instrument"].startswith("polymarket:"):
                        original = next(c for c in s["outcomes"]["document"]["claims"] if c["claim_id"] == row["claim_id"])
                        new = {**original, "claim_id": row["instrument"], "outcome_keys": original["outcome_keys"][:1]}
                        s["outcomes"]["document"]["claims"].append(new)
                        row["claim_id"] = new["claim_id"]
            elif mode == "mixed":
                shape = copy.deepcopy(s["outcomes"]["document"]["spaces"][0]); shape["space_shape_id"]="f"*64
                s["outcomes"]["document"]["spaces"].append(shape)
                for row in s["scopes"][0]["outcome_books"]:
                    if row["instrument"].startswith("polymarket:"):
                        row["space_shape_id"]="f"*64
            bs = baskets(s,p,0)
            self.assertFalse(any(b.admission is None for b in bs), mode)
            self.assertTrue(any(b.admission_reasons for b in bs))
        bs = baskets(base,p,0)
        self.assertEqual(sum(b.admission is None for b in bs),4)
        self.assertTrue(any('overlap' in r for b in bs for r in b.admission_reasons))
        self.assertTrue(outcome_scope(base,0).leg(("kalshi:series","complement")).negated)

    def test_scale_opt_in_and_pair_limit_and_control_policy(self):
        h = self.harness(policy={"controls":[{"kind":"time_shift","shift_ns":["5"]}],
                                 "controls_episodes":True,"controls_slices":True})
        h.populate(); result=h.finish()
        self.assertIn("time_shift_5",result["summary"]["controls"])
        plans=h.strategy.plans
        legs=(("kalshi:series","outcome"),("polymarket:987","outcome"))
        self.assertEqual(scale_admission(legs,plans),"UNSUPPORTED_SCALE")
        self.assertIsNone(scale_admission(legs,plans,native_scales=True))
        with patch("replay.strategies.cross_venue_arbitrage.contract.MAX_PAIRS",1), self.assertRaisesRegex(ProtocolError,"pair limit"):
            baskets(h.strategy.snapshot,h.cross_config["policy"],0)
        p=copy.deepcopy(h.cross_config["policy"]); p["controls"]=[{"kind":"cyclic_neighbor"}]
        with self.assertRaisesRegex(ProtocolError,"time_shift"):
            policy_config(p)
        with self.assertRaisesRegex(ProtocolError,"SUCCESS"):
            read_completed(self.root,"coverage")

    def test_malformed_masks_fail_preparation(self):
        doc=document(); doc["claims"][0]["outcome_keys"].pop()
        with self.assertRaises(ProtocolError):
            prepare(config(),self.root/"bad",universe=lambda *_:detail(),outcomes=lambda _:doc)

    def test_collateral_fee_enters_cash_once_and_net_boundary(self):
        h = self.harness(fees="collateral")
        h.populate(pm_away=570, kalshi_no=60)
        pair=(("kalshi:series","outcome"),("polymarket:987","outcome"))
        obs, _=self.observe(h,pair)
        # .40 BUY incurs .02 Kalshi fee on the non-direct cent grid.
        self.assertEqual(int(obs.payload["gap_gross"]), UNIT*3//100)
        self.assertEqual(int(obs.payload["gap_net"]), UNIT//100)
        self.assertEqual(int(obs.payload["native_legs"][0]["quote_delta_e36"]),-UNIT*42//100)
        ladder(h,13,"polymarket:987",asks=((580,3*10**6),))
        self.assertEqual(self.observe(h,pair)[0].payload["gap_net"],"0")
        ladder(h,14,"polymarket:987",asks=((590,3*10**6),))
        obs,_=self.observe(h,pair)
        self.assertEqual((obs.value_class,int(obs.payload["gap_net"])),("NET_NONPOSITIVE",-UNIT//100))
        h.finish()

    def test_one_sided_self_crossed_and_partial_final_level(self):
        h=self.harness()
        h.populate()
        pair=(("kalshi:series","outcome"),("polymarket:987","outcome"))
        ladder(h,13,"polymarket:987",bids=((100,10**6),),asks=())
        self.assertEqual(self.observe(h,pair)[0].status,"ONE_SIDED")
        ladder(h,14,"polymarket:987",bids=((700,10**6),),asks=((500,10**6),))
        self.assertEqual(self.observe(h,pair)[0].status,"SELF_CROSSED_LEG")
        ladder(h,15,"polymarket:987",bids=((100,10**6),),asks=((400,400_000),(500,900_000)))
        obs,_=self.observe(h,pair)
        self.assertEqual(int(obs.payload["native_legs"][1]["cost_e36"]),UNIT*46//100)
        self.assertEqual(obs.quotes[1],((400,400_000),(500,900_000)))
        h.finish()

    def test_price_quantity_scale_18_exact_rational_cost(self):
        h=self.harness(scales={"polymarket":("18","18")})
        h.window()
        ladder(h,12,"kalshi:series","complement",bids=((60,3),))
        price=580_000_000_000_000_000
        ladder(h,12,"polymarket:987",asks=((price,10**18),))
        pair=(("kalshi:series","outcome"),("polymarket:987","outcome"))
        obs,_=self.observe(h,pair)
        self.assertEqual(int(obs.payload["gap_net"]),UNIT*2//100)
        # A one-atom partial level has a 36-digit rational notional. Gross is exact;
        # the Fee SDK's quote scale 18 cannot represent this declared partition.
        ladder(h,13,"polymarket:987",asks=((price,1),(price+1,10**18-1)))
        obs,_=self.observe(h,pair)
        self.assertEqual(int(obs.payload["gap_gross"]),UNIT*2//100-(10**18-1))
        self.assertEqual((obs.value_class,obs.payload["gap_net"]),("FEE_UNKNOWN",None))
        self.assertIn("not exactly representable",obs.reasons[0])
        h.finish()

    def test_integrated_unavailable_context_versions_and_partial_status(self):
        for doc in (None,"legacy"):
            with tempfile.TemporaryDirectory() as tmp:
                h=Harness(Path(tmp),doc=doc)
                h.populate(); result=h.finish(); h.close()
                self.assertIsNone(result["manifest"]["outcomes_provider"])
                self.assertTrue(any(r.get("kind")=="outcomes_unavailable" for r in result["summary"]["reasons"]))
                self.assertFalse(h.records("episodes.ndjson"))
        doc=document()
        doc["markets"][0].update(mask_status="VOID_UNSUPPORTED",claims=[],tokens=[])
        h=self.harness(doc=doc,limitless=True)
        h.populate()
        ladder(h,12,"limitless:slug",bids=((100,3*10**6),),asks=((400,3*10**6),))
        result=h.finish()
        self.assertTrue(any(r.get("kind")=="VOID_UNSUPPORTED" for r in result["summary"]["reasons"]))
        self.assertTrue(h.records("episodes.ndjson"),"healthy siblings remain evaluable")

    def test_polymarket_token_negation_has_both_partition_directions(self):
        doc=document()
        pm=doc["markets"][-1]
        home=next(c for c in pm["claims"] if c["claim_key"]=="claim=0")
        pm["claims"]=[home]; pm["outcome_labels"]=["Yes","No"]
        pm["tokens"][1].update(claim_key="claim=0",negated=True)
        doc["claims"]=[c for c in doc["claims"] if c["claim_id"]==home["claim_id"]]
        h=self.harness(doc=doc)
        h.populate(); result=h.finish()
        bs=baskets(h.strategy.snapshot,h.cross_config["policy"],0)
        self.assertEqual(sum(b.admission is None for b in bs),4)
        self.assertTrue(any(l["negated"] for b in bs for l in b.descriptor["mask_legs"] if b.admission is None))
        self.assertEqual(sum(r["kind"]=="net" for r in h.records("episodes.ndjson")),4)

    def test_reader_rejects_rehashed_native_fee_mask_label_and_quote_tampering(self):
        h=self.harness(); h.populate(); result=h.finish()
        episodes=h.records("episodes.ndjson")
        manifest=result["manifest"]
        import hashlib
        from replay.preparation import digest
        original=(h.output/"episodes.ndjson").read_bytes()
        for mutation in ("net", "native", "quote", "label"):
            records=copy.deepcopy(episodes)
            v=records[0]["open"]["values"]
            if mutation=="net": v["gap_net"]=str(int(v["gap_net"])+1)
            if mutation=="native": v["native_legs"][0]["received_e36"]="0"
            if mutation=="quote": records[0]["open"]["quotes"][0][0][0]="1"
            if mutation=="label": v["settlement_model"]="unconditional"
            raw=b''.join(encoded(row)+b'\n' for row in records)
            (h.output/"episodes.ndjson").write_bytes(raw)
            m=copy.deepcopy(manifest)
            m["files"]["episodes.ndjson"]={"sha256":hashlib.sha256(raw).hexdigest(),"byte_length":len(raw),"records":len(records)}
            with self.assertRaises(ProtocolError):
                validate_content(h.output,h.strategy.snapshot,m)
        (h.output/"episodes.ndjson").write_bytes(original)
        for key, new in (("outcomes_provider",None),("settlement_model","unconditional")):
            m=copy.deepcopy(manifest); m[key]=new
            with self.assertRaises(ProtocolError): validate_content(h.output,h.strategy.snapshot,m)

    def test_real_completed_reader_with_offline_supervisor_attestations(self):
        from replay import supervisor
        from replay.tests.test_supervisor import config as supervisor_config, metadata_pin
        pin=metadata_pin()
        h=self.harness(pin=pin,native=False)
        c=supervisor_config()
        c["transport"].update(run_id="coverage-test",groups=["coverage"],plans=h.initial["plans"],
                               start_ns="10",end_ns="40",inputs=[pin])
        c["strategies"]={"coverage":{"factory":"replay.strategies.cross_venue_arbitrage:build","revision":"synthetic-test",
                                        "config":h.cross_config}}
        identity=supervisor.identity(c)
        h.strategy.binding["identity"]=identity
        h.populate(); h.finish()
        run=self.root/"run"; attempt=h.context["attempt_id"]
        participant=run/attempt/"coverage"; participant.mkdir(parents=True)
        h.output.rename(participant/"output")
        supervisor.write_json_durable(run/"run.json",c)
        terminal=h.seq
        supervisor.write_json_durable(participant/"complete.json",{"version":1,"identity":identity,"attempt":attempt,
                                      "group":"coverage","terminal":terminal})
        supervisor.write_json_durable(participant.parent/"result.json",{"version":1,"identity":identity,"attempt":attempt,
                  "outcome":"success","fatal":False,"progress":terminal,"terminal":terminal,
                  "participants":{"publisher":0,"coverage":0}})
        supervisor.write_json_durable(run/"SUCCESS.json",{"version":1,"identity":identity,"attempt":attempt,
                            "terminal":terminal,"outputs":{"coverage":attempt+"/coverage/output"}})
        # This seam replaces only the external Rust metadata preflight, not SUCCESS validation.
        with patch.object(supervisor,"_strict_metadata_preflight"):
            result=read_completed(run,"coverage")
            self.assertEqual(result["receipt"]["identity"],identity)
            path=participant/"output/content_receipt.json"
            receipt=json.loads(path.read_bytes()); receipt["attempt_id"]="b"*32; path.write_bytes(encoded(receipt))
            with self.assertRaisesRegex(ProtocolError,"supervisor/content binding"):
                read_completed(run,"coverage")


class CrossVenueFillTests(unittest.TestCase):
    """Policy 3: the 1-contract net check triggers one priced fill per episode."""

    YES_THEN_PM = (("kalshi:series", "outcome"), ("polymarket:987", "outcome"))

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def harness(self, *sizings, **kw):
        h = Harness(self.root, fills=sizings or (EDGE,), **kw)
        self.addCleanup(h.close)
        return h

    def fills(self, h):
        return [r for r in h.records("episodes.ndjson") if r["kind"] == "net"]

    def test_one_entity_per_route_and_the_edge_walk_stops_where_the_edge_is_gone(self):
        h = self.harness()
        h.populate()
        # Kalshi YES asks 0.40 x 3 then 0.50 x 10; Polymarket 0.580 x 1 then 0.595 x 5.
        ladder(h, 12, "kalshi:series", "complement", bids=((60, 3), (50, 10)))
        ladder(h, 12, "polymarket:987", bids=((100, 3 * 10**6),), asks=((580, 10**6), (595, 5 * 10**6)))
        # Kalshi's projected ask reaches the 0.41 kill price; the 1-contract trigger
        # (0.41 + 0.58) stays on, so the fill ends on its kill price.
        ladder(h, 20, "kalshi:series", "complement", bids=((59, 3), (50, 10)))
        result = h.finish()
        admitted = [e for e in h.strategy.entities.values() if e.admission is None and e.cls == "real"]
        self.assertEqual({e.descriptor["size_contracts"] for e in admitted}, {"1"})
        self.assertEqual(len(admitted), 2)   # the two partitions of this fixture's masks
        first = next(r for r in self.fills(h) if r["start_ns"] == "12"
                     and r["fill"]["results"][0]["legs"][1]["taken"][0] == ["580", "1000000"])
        (edge,) = first["fill"]["results"]
        # Marginal sets: 0.40 + 0.580, then 0.40 + 0.595 twice; 0.50 + 0.595 loses.
        self.assertEqual((edge["steps"], edge["stop"], edge["value"]), ("3", "edge", str(UNIT * 3 // 100)))
        self.assertEqual(edge["legs"][0]["taken"], [["40", "3"]])
        self.assertEqual(edge["after"], [["50", "10"], ["595", "3000000"]])
        # Three contracts at one price: 3k + 1.77 <= 3 on Kalshi, 3k + 1.20 <= 3 on Polymarket.
        self.assertEqual(edge["kill_prices"], ["41", "600"])
        self.assertEqual((first["end_ns"], first["end_reason"]), ("20", "KILL_PRICE"))
        self.assertEqual(first["fill"]["kill_leg"], 0)
        self.assertTrue(any("fill_ns" in row for row in result["summary"]["rows"]))

    def test_fill_value_prices_fees_like_the_trigger(self):
        h = self.harness({"name": "one", "role": "governs", "target_contracts": "1"}, fees="collateral")
        h.populate(pm_away=570, kalshi_no=60)
        strategy = h.strategy.strategy
        entity = next(e for e in h.strategy.entities.values() if e.cls == "real" and e.admission is None
                      and tuple((d["instrument"], d["orientation"]) for d in e.descriptor["legs"])
                      == self.YES_THEN_PM)
        obs = strategy.evaluate(entity, tuple(h.strategy.views[k] for k in entity.legs),
                                Context(12, 8, 0, h.strategy.experiment.experiment_sha256))
        # 0.40 BUY pays 0.02 Kalshi fee: the trigger's net and the fill's value agree.
        self.assertEqual(int(obs.payload["gap_net"]), UNIT // 100)
        h.finish()
        (row,) = [r for r in self.fills(h) if r["fill"]["results"][0]["legs"][1]["taken"][0][0] == "570"]
        self.assertEqual(row["fill"]["results"][0]["value"], str(UNIT // 100))

    def test_a_governing_target_the_book_cannot_serve_opens_no_fill(self):
        h = self.harness({"name": "t5", "role": "governs", "target_contracts": "5"})
        h.populate()   # three contracts of depth everywhere
        result = h.finish()
        self.assertEqual(self.fills(h), [])
        rows = [r for r in result["summary"]["rows"] if "fill_ns" in r]
        self.assertTrue(rows and all(set(r["fill_ns"]) == {"FILL_DEPTH_SHORT"} for r in rows))

    def test_reader_needs_the_fee_catalog_and_rechecks_fill_values(self):
        h = self.harness()
        h.populate()
        result = h.finish()
        manifest = result["manifest"]
        self.assertEqual(manifest["policy"]["version"], 3)
        with self.assertRaisesRegex(ProtocolError, "require the fee catalog"):
            read_provisional(h.output, h.root / "context", expected_sha256=h.sha)
        import hashlib
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

    def test_a_fractional_step_takes_a_partly_displayed_contract(self):
        h = self.harness(scales={"kalshi": ("2", "2")}, step="0.01")
        h.window()
        # Kalshi YES asks 0.40 x 3.16 contracts; Polymarket 0.580 x 5.
        ladder(h, 12, "kalshi:series", "complement", bids=((60, 316),))
        ladder(h, 12, "kalshi:series", "outcome", bids=((70, 300),))
        ladder(h, 12, "polymarket:987", bids=((100, 5 * 10**6),), asks=((580, 5 * 10**6),))
        ladder(h, 12, "polymarket:123", bids=((100, 5 * 10**6),), asks=((650, 5 * 10**6),))
        h.finish()
        (row,) = [r for r in self.fills(h) if r["fill"]["sources"] == ["kalshi_complement_ask", "ask"]
                  and r["fill"]["results"][0]["legs"][1]["taken"][0][0] == "580"]
        (edge,) = row["fill"]["results"]
        self.assertEqual((edge["steps"], edge["stop"]), ("316", "book_exhausted"))
        self.assertEqual(edge["legs"][0]["taken"], [["40", "316"]])
        # Gross 3.16 x 0.02; Kalshi's non-direct cash rounding takes 1.264 to 1.27.
        self.assertEqual(edge["value"], str(UNIT * (2 * 316 - 60) // 10_000))
        self.assertEqual(edge["beyond"][0], [])

    def test_a_step_finer_than_a_legs_quantity_unit_fails_at_construction(self):
        with self.assertRaisesRegex(ProtocolError, "whole number of quantity atoms"):
            self.harness(step="0.01")   # Kalshi quantities are whole contracts here

    def test_policy_3_is_closed_and_rejects_controls_and_the_size_sweep(self):
        h = self.harness()
        policy = h.cross_config["policy"]
        for bad in ({**policy, "sizes_contracts": ["1"]}, {**policy, "controls": []},
                    {**policy, "fills": {**policy["fills"], "sizings": []}}):
            with self.subTest(bad=sorted(set(bad) ^ set(policy))), self.assertRaises(ProtocolError):
                policy_config(bad)
