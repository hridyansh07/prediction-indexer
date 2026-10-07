"""Default-sweep regression with identity-valid offline outcomes and 20 books."""
import copy
import itertools
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from analysis.claims import claim_id
from replay.strategies.cross_venue_arbitrage.strategy import build
from replay.strategies.cross_venue_arbitrage.output import read_provisional
from replay.economic_sdk.entities import resolve
from replay.economic_sdk.bounds import MAX_METADATA
from replay.preparation import encoded, prepare
from replay.tests.test_bundle_coverage import Harness as BaseHarness
from replay.tests.test_cross_venue_arbitrage import Harness
from replay.tests.test_preparation import config, detail
from replay.tests.test_preparation_outcomes import document
from replay.tests.test_same_venue_complement import strategy_config
from replay.tests.economic_scenarios import ladder, v2_policy

SIZES = ["1", "10", "25", "50", "100", "250", "500", "1000"]


def wide_fixture(scope_count=14):
    d, doc = detail(), document()
    shape = doc["spaces"][0]["space_shape_id"]
    keys = doc["spaces"][0]["outcome_keys"]
    masks = list(itertools.islice(itertools.combinations(keys, 3), 10))
    claims, markets, targets, models = {}, [], [], []
    for venue in ("limitless", "polymarket"):
        for i in range(10 if venue == "limitless" else 5):
            mid = venue + ":" + f"market-{i:064d}"
            indexes = [i] if venue == "limitless" else [i*2, i*2+1]
            subs = [f"{j+1:064d}" for j in indexes]
            markets.append({"target_id":mid, "venue":venue, "selected":True})
            targets.append({"target_id":mid, "venue":venue, "subscription_ids":subs,
                            "canonical_class":"esports.series_moneyline", "source_ref":"/synthetic"})
            model = {**doc["markets"][0], "market_id":mid, "venue":venue,
                     "subscription_ids":subs, "outcome_labels":[f"claim-{j}" for j in indexes],
                     "claims":[], "tokens":[]}
            for n, j in enumerate(indexes):
                mask = sorted(masks[j] if venue == "limitless" else set(keys)-set(masks[j]))
                cid = claim_id(mask, shape)
                claims[cid] = {"claim_id":cid, "space_shape_id":shape, "outcome_keys":mask}
                model["claims"].append({"claim_key":f"claim={n}", "claim_id":cid})
                model["tokens"].append({"subscription_id":subs[n], "claim_key":f"claim={n}", "negated":False})
            models.append(model)
    markets.append({"target_id":"kalshi:missing", "venue":"kalshi", "selected":False})
    d["context"].update(markets=sorted(markets,key=lambda r:r["target_id"]),
                         targets=sorted(targets,key=lambda r:r["target_id"]))
    doc.update(claims=sorted(claims.values(),key=lambda r:r["claim_id"]),
               markets=sorted(models,key=lambda r:r["market_id"]))
    c=config(d)
    c["occurrences"]=[{**c["occurrences"][0], "start_ns":str(10+2*i),
                        "end_ns":str(12+2*i if i<scope_count-1 else max(40,10+2*scope_count))} for i in range(scope_count)]
    c["end_ns"]=c["occurrences"][-1]["end_ns"]
    return d, doc, c


class WideHarness(Harness):
    def __init__(self, root, *, sizes=SIZES, controls=False, pin=None, scope_count=14, changing=False):
        self.root=root
        d, doc, c=wide_fixture(scope_count)
        if pin:
            c["pins"]=[{k:pin[k] for k in ("derivative_address","receipt_sha256")}]
            for a in c["authorities"]: a.update(price_scale="2",quantity_scale="0")
        later = copy.deepcopy(d)
        if changing:
            from replay.tests.test_preparation import R1,R2,G2
            later["run_id"],later["generated_at"]=R2,G2
            later["source"]={k:v.replace(R1,R2) for k,v in d["source"].items()}
            later["source"]["report_sha256"]="c"*64
            later["origin"]={**later["source"],"run_id":R2,"generated_at":G2}
            for i,target in enumerate(d["context"]["targets"]):
                if target["venue"]=="polymarket":
                    target["subscription_ids"]=sorted(target["subscription_ids"]+[f"{100+i:064d}"])
            for occurrence in c["occurrences"][scope_count//2:]:
                occurrence.update(run_id=R2,source=later["source"])
        def source(occurrence,_):
            return later if changing and occurrence["run_id"]==later["run_id"] else d
        def factory(context):
            cfg=strategy_config(dict(context["config"]),root,known=True)
            from replay.fees.artifacts import build_catalog
            from replay.fees.schedules import Catalog
            cat=Catalog.build(())
            cfg["fees"].update(catalog_directory=str(build_catalog(root/"empty-fees",cat,{})),catalog_identity=cat.identity)
            cfg["policy"].update(v2_policy(sizes_contracts=sizes,headline_size_contracts="1",audit_intervals=True))
            if controls:
                cfg["policy"].update(controls=[{"kind":"time_shift","shift_ns":["5"]}],
                                     controls_episodes=True,controls_slices=True)
            cfg["valuation"]={"version":1,"kind":"PARITY_SCENARIO","unit":"research_dollar",
                              "assets":sorted(cfg["fees"]["assets"].values(),key=encoded)}
            self.cross_config=cfg
            return build({**context,"config":cfg})
        with patch("replay.tests.test_bundle_coverage.detail",side_effect=lambda:copy.deepcopy(d)), \
             patch("replay.tests.test_bundle_coverage.config",side_effect=lambda:copy.deepcopy(c)), \
             patch("replay.tests.test_bundle_coverage.prepare",side_effect=lambda c,p,**kw:prepare(c,p,universe=source,outcomes=lambda _:doc)), \
             patch("replay.tests.test_bundle_coverage.build",side_effect=factory):
            BaseHarness.__init__(self,root,mixed=True,pin=pin)

    def populate(self):
        self.window()
        for p in self.initial["plans"]:
            ps,qs=int(p["price_scale"]),int(p["quantity_scale"])
            ladder(self,12,p["instrument"],bids=((10**ps//10,2000*10**qs),),
                   asks=((4*10**ps//10,2000*10**qs),))
        # Break and restore every admitted route, exercising episode equality.
        for t,price in ((18,7),(22,4)):
            for p in self.initial["plans"]:
                if p["venue"]=="polymarket":
                    ladder(self,t,p["instrument"],bids=((10**int(p["price_scale"])//10,2000*10**int(p["quantity_scale"])),),
                           asks=((price*10**int(p["price_scale"])//10,2000*10**int(p["quantity_scale"])),))


class MetadataRegressionTests(unittest.TestCase):
    def test_default_sweep_finishes_with_full_scoped_rejections(self):
        with tempfile.TemporaryDirectory() as tmp:
            h=WideHarness(Path(tmp))
            try:
                h.populate()
                result=h.finish()
                table=h.records("entities.json")[0]
                self.assertEqual(len(table["scopes"]),14)
                for scope,rows in enumerate(table["scopes"]):
                    self.assertEqual(sum(r["descriptor"]["admission"]=="UNSUPPORTED_SHAPE" for r in rows),90)
                    self.assertEqual(sum(r["descriptor"]["admission"]=="NOT_CAPTURED" for r in rows),1)
                    self.assertEqual(sum(r["descriptor"]["admission"] is None for r in rows),80)
                for denominator in h.records("denominators.ndjson"):
                    scope=denominator["scope"]
                    interval=h.snapshot["scopes"][scope]
                    self.assertEqual(sum(map(int,denominator["status_ns"].values())),
                                     int(interval["end_ns"])-int(interval["start_ns"]))
                    d=table["scopes"][scope][denominator["entity"]]["descriptor"]
                    self.assertEqual(d["size_contracts"] is None,d["admission"] is not None)
                self.assertLessEqual((h.output/"entities.json").stat().st_size,MAX_METADATA)
                self.assertTrue(result["summary"]["rows"])
            finally: h.close()

    def test_oversized_table_is_rejected_before_output_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            from replay.streams.protocol import ProtocolError
            with self.assertRaisesRegex(ProtocolError,"entity metadata preflight.*entities.json"):
                WideHarness(root,scope_count=70)
            self.assertEqual(list((root/"output").iterdir()),[])

    def test_eight_and_four_sizes_have_identical_shared_route_time_and_episodes(self):
        summaries=[]
        for sizes in (SIZES,["1","10","100","1000"]):
            with tempfile.TemporaryDirectory() as tmp:
                h=WideHarness(Path(tmp),sizes=sizes,controls=True)
                try:
                    h.populate(); result=h.finish()
                    summaries.append(result["summary"])
                    self.assertEqual(h.strategy.entity_table_bytes["entities.json"],
                                     (h.output/"entities.json").stat().st_size)
                    self.assertEqual(h.strategy.entity_table_bytes["controls/time_shift_5/entities.json"],
                                     (h.output/"controls/time_shift_5/entities.json").stat().st_size)
                    # Each static rejected pair is present once in the isolated control.
                    for rows in h.records("controls/time_shift_5/entities.json")[0]["scopes"]:
                        self.assertEqual(sum(r["descriptor"]["size_contracts"] is None for r in rows),90)
                finally: h.close()
        def common(rows):
            return [r for r in rows if r["size_contracts"] in {None,"1","10","100","1000"}]
        self.assertEqual(common(summaries[0]["rows"]),common(summaries[1]["rows"]))
        self.assertEqual(common(summaries[0]["controls"]["time_shift_5"]["rows"]),
                         common(summaries[1]["controls"]["time_shift_5"]["rows"]))
        self.assertTrue(any(int(r["gross_positive_ns"])>0 and r["episode_count"]["gross"]>0
                            for r in common(summaries[0]["rows"])))

    def test_route_rejected_then_admitted_keeps_separate_scoped_time(self):
        with tempfile.TemporaryDirectory() as tmp:
            h=WideHarness(Path(tmp),changing=True,controls=True)
            try:
                h.populate(); result=h.finish()
                rows=result["summary"]["rows"]
                table=h.records("entities.json")[0]["scopes"]
                admitted=next(r["descriptor"] for r in table[-1] if r["descriptor"]["admission"] is None)
                route=admitted["route_id"]
                selected=[r for r in rows if r["route_id"]==route]
                self.assertEqual(len(selected),9)
                static=next(r for r in selected if r["size_contracts"] is None)
                self.assertEqual(static["status_ns"],{"UNSUPPORTED_SHAPE":"14"})
                for row in selected:
                    if row["size_contracts"] is not None:
                        self.assertEqual(sum(map(int,row["status_ns"].values())),16)
                        self.assertEqual(int(row["gross_positive_ns"]),16)
                self.assertTrue(any(r.get("kind")=="SUBSCRIPTION_MISMATCH" for r in result["summary"]["reasons"]))
            finally: h.close()

    def test_deterministic_order_and_admitted_descriptors_are_unchanged(self):
        from replay.strategies.cross_venue_arbitrage.contract import baskets
        from replay.preparation import build_snapshot,digest
        d,doc,c=wide_fixture()
        s=build_snapshot(c,[{"provider":"universe","detail":d}]*14,{"provider":"universe","document":doc})
        policy={"sizes_contracts":SIZES}
        actual=baskets(s,policy,0)
        reversed_snapshot=copy.deepcopy(s)
        reversed_snapshot["scopes"][0]["members"].reverse()
        for member in reversed_snapshot["scopes"][0]["members"]: member["books"].reverse()
        self.assertEqual(actual,baskets(reversed_snapshot,policy,0))
        # An admitted identity remains the digest of precisely the original fields.
        for basket in actual:
            if basket.admission is None:
                desc=basket.descriptor
                self.assertEqual(basket.order[-1],int(desc["size_contracts"]))
                self.assertEqual(desc["route_id"],digest(desc["legs"]))
                self.assertEqual(set(desc),{"venue","basket_kind","market_id","route_id","legs","mask_legs",
                    "ask_sources","admission","direction","size_contracts","settlement_model","outcomes_provider"})

    def test_completed_reader_and_old_contract_is_rejected(self):
        from replay import supervisor
        from replay.strategies.cross_venue_arbitrage.output import read_completed,check_manifest
        from replay.tests.test_supervisor import config as supervisor_config,metadata_pin
        from replay.streams.protocol import ProtocolError
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); pin=metadata_pin(); h=WideHarness(root,pin=pin)
            try:
                c=supervisor_config()
                c["transport"].update(run_id="coverage-test",groups=["coverage"],plans=h.initial["plans"],
                                      start_ns="10",end_ns="40",inputs=[pin])
                c["strategies"]={"coverage":{"factory":"replay.strategies.cross_venue_arbitrage:build",
                               "revision":"synthetic-test","config":h.cross_config}}
                identity=supervisor.identity(c); h.strategy.binding["identity"]=identity
                h.populate(); result=h.finish()
                old=copy.deepcopy(result["manifest"]); old["version"]=1
                with self.assertRaises(ProtocolError): check_manifest(old,h.snapshot,True)
                run=root/"run"; attempt=h.context["attempt_id"]
                participant=run/attempt/"coverage"; participant.mkdir(parents=True)
                h.output.rename(participant/"output")
                supervisor.write_json_durable(run/"run.json",c)
                terminal=h.seq
                supervisor.write_json_durable(participant/"complete.json",{"version":1,"identity":identity,
                          "attempt":attempt,"group":"coverage","terminal":terminal})
                supervisor.write_json_durable(participant.parent/"result.json",{"version":1,"identity":identity,
                          "attempt":attempt,"outcome":"success","fatal":False,"progress":terminal,"terminal":terminal,
                          "participants":{"publisher":0,"coverage":0}})
                supervisor.write_json_durable(run/"SUCCESS.json",{"version":1,"identity":identity,
                          "attempt":attempt,"terminal":terminal,"outputs":{"coverage":attempt+"/coverage/output"}})
                with patch.object(supervisor,"_strict_metadata_preflight"):
                    self.assertEqual(read_completed(run,"coverage")["manifest"],result["manifest"])
            finally: h.close()


class SDKMetadataBoundaryTests(unittest.TestCase):
    def make_strategy(self, root, *, big_group="", over=0):
        from replay.economic_sdk.types import Basket
        from replay.economic_sdk.output import group_of
        from replay.economic_sdk.entity_tables import entity_rows
        h=Harness(root,policy={"controls":[{"kind":"time_shift","shift_ns":["5"]}] if big_group else []})
        s=h.strategy.strategy
        snapshot={**s.snapshot,"scopes":[s.snapshot["scopes"][0],s.snapshot["scopes"][0]]}
        desc={"legs":[],"admission":"UNSUPPORTED_SHAPE","padding":""}
        # Keep the nontarget table small. Each target row has a fixed-width digest.
        def bs(*_): return (Basket(desc,(),("route",),"UNSUPPORTED_SHAPE",control_leg=0),)
        s.baskets=bs
        if big_group:
            def control(b,c,r,a): return {**b.descriptor,"padding":padding[0],"control":{"kind":"time_shift","leg":0,"shift_ns":"5"}}
            s.control_descriptor=control
        padding=[""]
        entities=resolve(s,snapshot,0,s.plans)
        raw=encoded({"scopes":[entity_rows(entities,big_group),entity_rows(entities,big_group)]})+b"\n"
        # Two equal rows: use an extra one-byte field to cover odd remainders.
        target=MAX_METADATA+over
        extra=(target-len(raw))%2
        if extra:
            # A trailing scope adds a comma plus an empty array (3 bytes).
            snapshot["scopes"].append({**snapshot["scopes"][0],"empty":True})
            s.baskets=lambda snapshot,policy,index: () if index==2 else bs()
            entities=resolve(s,snapshot,0,s.plans)
            raw=encoded({"scopes":[entity_rows(entities,big_group),entity_rows(entities,big_group),[]]})+b"\n"
        text="x"*((target-len(raw))//2)
        if big_group: padding[0]=text
        else: desc["padding"]=text
        return h,s,snapshot

    def test_exact_real_and_control_bound_and_one_byte_over(self):
        from replay.economic_sdk.entity_tables import preflight,table_chunks,write_table,entity_rows
        from replay.streams.protocol import ProtocolError
        import hashlib
        for group in ("","controls/time_shift_5/"):
            for over in (0,1):
                with self.subTest(group=group,over=over), tempfile.TemporaryDirectory() as tmp:
                    h,s,snapshot=self.make_strategy(Path(tmp),big_group=group,over=over)
                    try:
                        rows=[entity_rows(resolve(s,snapshot,i,s.plans),group) for i in range(len(snapshot["scopes"]))]
                        canonical=encoded({"scopes":rows})+b"\n"
                        self.assertEqual(len(canonical),MAX_METADATA+over)
                        self.assertEqual(b"".join(table_chunks(s,snapshot,s.plans,group)),canonical)
                        if over:
                            with self.assertRaisesRegex(ProtocolError,"entity metadata preflight.*"+group+"entities.json"):
                                preflight(s,snapshot,s.plans)
                            from replay.economic_sdk.runtime import Runtime
                            output=Path(tmp)/"empty-output"; output.mkdir()
                            s.snapshot=snapshot
                            with self.assertRaisesRegex(ProtocolError,"entity metadata preflight"):
                                Runtime(s,{**h.context,"output_directory":str(output)})
                            self.assertEqual(list(output.iterdir()),[])
                        else:
                            self.assertEqual(preflight(s,snapshot,s.plans)[group+"entities.json"],MAX_METADATA)
                            output=Path(tmp)/"tables"; (output/group).mkdir(parents=True)
                            identity=write_table(output,group+"entities.json",table_chunks(s,snapshot,s.plans,group))
                            self.assertEqual((output/(group+"entities.json")).read_bytes(),canonical)
                            self.assertEqual(identity,{"sha256":hashlib.sha256(canonical).hexdigest(),"byte_length":MAX_METADATA,"records":1})
                    finally: h.close()
