"""Offline implication witnesses through preparation, Decoder, SDK and readers."""

import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from analysis.claims import claim_id
from replay.strategies.cross_venue_arbitrage.contract import UNIT
from replay.preparation import build_snapshot, encoded
from replay.streams.protocol import ProtocolError
from replay.tests.economic_scenarios import ladder
from replay.tests.test_cross_venue_arbitrage import (
    EDGE, Harness as QuoteHarness, fixture_detail, document,
)
from replay.tests.test_preparation import config

from replay.strategies._shared.implication_cover.contract import baskets
from replay.strategies._shared.implication_cover.output import read_provisional, read_completed, validate_content
from replay.strategies.same_venue_implication_cover.strategy import build as same_build
from replay.strategies.cross_venue_implication_cover.strategy import build as cross_build


def implication_detail(limitless=False):
    d = fixture_detail(limitless)
    for venue, subs in (("kalshi", ["sweep"]), ("polymarket", ["456", "654"])):
        mid = venue + ":sweep"
        d["context"]["markets"].append({"target_id": mid, "venue": venue, "selected": True})
        d["context"]["targets"].append({"target_id": mid, "venue": venue,
            "subscription_ids": subs, "canonical_class": "esports.series_correct_score",
            "source_ref": "/synthetic"})
    for field in ("markets", "targets"):
        d["context"][field].sort(key=lambda r: r["target_id"])
    return d


def implication_document():
    doc = document()
    shape = doc["spaces"][0]["space_shape_id"]
    cid = claim_id(["seq:HH"], shape)
    doc["claims"].append({"claim_id": cid, "space_shape_id": shape, "outcome_keys": ["seq:HH"]})
    for venue, subs in (("kalshi", ["sweep"]), ("polymarket", ["456", "654"])):
        doc["markets"].append({"market_id": venue + ":sweep", "venue": venue,
            "market_type": "series_correct_score", "market_status": "open",
            "subscription_ids": subs, "outcome_labels": ["Yes", "No"] if len(subs) == 2 else ["Alpha"],
            "mask_status": "MASKED", "reason": None,
            "claims": [{"claim_key": "claim=0", "claim_id": cid}],
            "tokens": [{"subscription_id": sub, "claim_key": "claim=0", "negated": i == 1}
                       for i, sub in enumerate(subs)]})
    doc["claims"].sort(key=lambda r: r["claim_id"])
    doc["markets"].sort(key=lambda r: r["market_id"])
    return doc


class Harness(QuoteHarness):
    def __init__(self, root, *, mode="same_venue", **kwargs):
        self.mode = mode
        with patch("replay.tests.test_cross_venue_arbitrage.fixture_detail", side_effect=implication_detail), \
             patch("replay.tests.test_cross_venue_arbitrage.document", side_effect=implication_document), \
             patch("replay.tests.test_cross_venue_arbitrage.build", same_build if mode == "same_venue" else cross_build):
            super().__init__(root, **kwargs)

    def populate(self, **kwargs):
        super().populate(pm_home=580, pm_away=850, kalshi_yes=15, kalshi_no=42, **kwargs)
        ladder(self, 12, "kalshi:sweep", "outcome", bids=((61, 3),))
        ladder(self, 12, "kalshi:sweep", "complement", bids=((15, 3),))
        ladder(self, 12, "polymarket:456", bids=((100, 3 * 10**6),), asks=((850, 3 * 10**6),))
        ladder(self, 12, "polymarket:654", bids=((100, 3 * 10**6),), asks=((390, 3 * 10**6),))

    def finish(self):
        self.terminal(); self.decoder.finish(); self.strategy.finish()
        return read_provisional(self.output, self.root / "context", expected_sha256=self.sha,
                                mode=self.mode, bridge=self.strategy.strategy.bridge)

    def entity(self, pair, size="1"):
        return next(e for e in self.strategy.entities.values() if e.cls == "real"
                    and e.descriptor["size_contracts"] == size
                    and tuple((d["instrument"], d["orientation"]) for d in e.descriptor["legs"]) == pair)

    def entity_index(self, entity):
        rows = self.records("entities.json")[0]["scopes"][0]
        return next(i for i, row in enumerate(rows) if row["hash"] == entity.id)


SAME = (("polymarket:123", "outcome"), ("polymarket:654", "outcome"))
CROSS = (("kalshi:series", "outcome"), ("polymarket:654", "outcome"))


class ImplicationTests(unittest.TestCase):
    def harness(self, **kwargs):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        h = Harness(Path(tmp.name), **kwargs)
        self.addCleanup(h.close)
        return h

    def test_shared_core_same_and_cross_hand_calculated_floor_and_middle(self):
        for mode, pair in (("same_venue", SAME), ("cross_venue", CROSS)):
            with self.subTest(mode=mode):
                h = self.harness(mode=mode, policy={"audit_intervals": True})
                h.populate(); result = h.finish()
                e = h.entity(pair)
                rows = [r for r in h.records("episodes.ndjson") if r["entity"] == h.entity_index(e)
                        and r["kind"] == "net"]
                self.assertEqual(len(rows), 1)
                value = rows[0]["open"]["values"]
                self.assertEqual(int(value["gap_net"]), 3 * UNIT // 100)
                self.assertEqual(int(value["payout_floor_gross_e36"]), UNIT)
                self.assertEqual(int(value["payout_middle_gross_e36"]), 2 * UNIT)
                self.assertEqual(int(value["payout_middle_extra_net_e36"]), UNIT)
                self.assertEqual(sorted(map(int, value["payoffs_net_e36"])), [UNIT] * 4 + [2 * UNIT] * 2)
                self.assertEqual(result["manifest"]["venue_mode"], mode)
                self.assertEqual(result["summary"]["settlement_model"], "normal_resolution_only")

    def test_route_proof_direction_equal_masks_gap_and_deterministic_order(self):
        d, doc = implication_detail(), implication_document()
        snapshot = build_snapshot(config(d), [{"provider": "universe", "detail": d}],
                                  {"provider": "universe", "document": doc})
        h = self.harness()
        policy = h.cross_config["policy"]
        for mode in ("same_venue", "cross_venue"):
            bs = baskets(snapshot, policy, 0, mode)
            admitted = [b for b in bs if b.admission is None]
            self.assertTrue(admitted)
            for b in admitted:
                proof = b.descriptor["implication"]
                self.assertLess(set(proof["antecedent_keys"]), set(proof["consequent_keys"]))
                self.assertEqual(set(proof["payout_per_contract"]), {1, 2})
                venues = {k[0].split(":")[0] for k in b.legs}
                self.assertEqual(len(venues), 1 if mode == "same_venue" else 2)
            reordered = copy.deepcopy(snapshot)
            reordered["scopes"][0]["members"].reverse()
            self.assertEqual([b.descriptor for b in bs],
                             [b.descriptor for b in baskets(reordered, policy, 0, mode)])
            self.assertTrue(any("gap" in r for b in bs for r in b.admission_reasons))
            if mode == "cross_venue":
                self.assertTrue(any("identity_cover" in r for b in bs for r in b.admission_reasons))

    def test_unknown_and_incomplete_inputs_stay_visible(self):
        for mutation in ("unavailable", "void", "incomplete"):
            d, doc = implication_detail(), implication_document()
            if mutation == "void":
                for row in doc["markets"]:
                    row.update(mask_status="VOID_UNSUPPORTED", claims=[], tokens=[])
                doc["claims"] = []
            elif mutation == "incomplete":
                doc["spaces"][0]["coverage"] = "INCOMPLETE_COVERAGE"
            outcomes = {"provider": "universe", "document": doc}
            if mutation == "unavailable":
                outcomes = {"provider": None, "unavailable": "universe_outcomes_unavailable"}
            s = build_snapshot(config(d), [{"provider": "universe", "detail": d}], outcomes)
            h = self.harness()
            for mode in ("same_venue", "cross_venue"):
                bs = baskets(s, h.cross_config["policy"], 0, mode)
                self.assertFalse(any(b.admission is None for b in bs))
                self.assertTrue(all(b.descriptor["size_contracts"] is None for b in bs))
                self.assertTrue(any(b.admission == "NOT_CAPTURED" for b in bs))
        with patch("replay.strategies._shared.implication_cover.contract.MAX_PAIRS", 1), self.assertRaisesRegex(ProtocolError, "pair limit"):
            baskets(h.strategy.snapshot, h.cross_config["policy"], 0, "same_venue")

    def test_token_fee_recomputes_each_state_and_removes_floor_edge(self):
        h = self.harness(mode="cross_venue", limitless=True, fees="token")
        h.populate()
        ladder(h, 12, "limitless:slug", asks=((590, 3 * 10**6),))
        pair = (("limitless:slug", "outcome"), ("polymarket:654", "outcome"))
        e = h.entity(pair)
        result = h.finish()
        gross = [r for r in h.records("episodes.ndjson") if r["kind"] == "gross"
                 and r["entity"] == h.entity_index(e)][0]["open"]["values"]
        self.assertEqual(int(gross["gap_gross"]), UNIT * 2 // 100)
        self.assertEqual(int(gross["payout_floor_net_e36"]), UNIT * 97 // 100)
        self.assertEqual(int(gross["payout_middle_net_e36"]), UNIT * 197 // 100)
        self.assertEqual(int(gross["gap_net"]), -UNIT // 100)
        self.assertFalse(any(r["kind"] == "net" and r["entity"] == h.entity_index(e)
                             for r in h.records("episodes.ndjson")))
        self.assertTrue(result["summary"]["rows"])

    def test_fill_checks_and_reader_reject_rehashed_middle_payoff(self):
        h = self.harness(mode="cross_venue", fills=(EDGE,))
        h.populate(); result = h.finish()
        e = h.entity(CROSS)
        rows = h.records("episodes.ndjson")
        row = next(r for r in rows if r["kind"] == "net" and r["entity"] == h.entity_index(e))
        self.assertEqual(row["fill"]["results"][0]["value"], str(9 * UNIT // 100))
        row["open"]["values"]["payout_middle_net_e36"] = str(3 * UNIT)
        raw = b"".join(encoded(r) + b"\n" for r in rows)
        (h.output / "episodes.ndjson").write_bytes(raw)
        m = copy.deepcopy(result["manifest"])
        m["files"]["episodes.ndjson"] = {"sha256": hashlib.sha256(raw).hexdigest(),
                                         "byte_length": len(raw), "records": len(rows)}
        with self.assertRaisesRegex(ProtocolError, "middle"):
            validate_content(h.output, h.strategy.snapshot, m, h.strategy.strategy.bridge)
        with self.assertRaisesRegex(ProtocolError, "SUCCESS"):
            read_completed(h.root, "coverage", mode="cross_venue")

    def test_no_probability_model_never_treats_middle_as_floor(self):
        h = self.harness(fees="missing")
        h.populate()
        ladder(h, 20, "polymarket:654", asks=((430, 3 * 10**6),))
        result = h.finish()
        e = h.entity(SAME)
        row = next(r for r in result["summary"]["rows"] if r["route_id"] == e.descriptor["route_id"]
                   and r["size_contracts"] == "1")
        self.assertEqual(row["class_ns"]["FEE_UNKNOWN"], "8")
        self.assertEqual(row["class_ns"]["GROSS_NONPOSITIVE"], "20")
        self.assertFalse(any(r["kind"] == "net" for r in h.records("episodes.ndjson")))

    def test_same_time_restore_scope_end_controls_and_retry_identity(self):
        manifests = []
        for attempt in ("a" * 32, "b" * 32):
            h = self.harness(attempt=attempt, scopes=True,
                policy={"audit_intervals": True, "controls": [{"kind": "time_shift", "shift_ns": ["5"]}],
                        "controls_episodes": True, "controls_slices": True})
            h.populate()
            ladder(h, 18, "polymarket:654", asks=((800, 3 * 10**6),))
            ladder(h, 18, "polymarket:654", bids=((100, 3 * 10**6),), asks=((390, 3 * 10**6),))
            result = h.finish()
            route = h.entity(SAME).descriptor["route_id"]
            tables = h.records("entities.json")[0]["scopes"]
            rows = [r for r in h.records("episodes.ndjson") if r["kind"] == "net"
                    and tables[r["scope"]][r["entity"]]["descriptor"]["route_id"] == route
                    and tables[r["scope"]][r["entity"]]["descriptor"]["size_contracts"] == "1"]
            self.assertEqual([(r["start_ns"], r["end_ns"], r["end_reason"]) for r in rows],
                             [("12", "23", "SCOPE_END"), ("23", "40", "RUN_END")])
            self.assertIn("time_shift_5", result["summary"]["controls"])
            manifests.append((h.output / "manifest.json").read_bytes())
        self.assertEqual(*manifests)

    def test_gross_break_even_depth_usability_and_valuation_gates(self):
        from replay.economic_sdk.types import Context
        h = self.harness()
        h.populate()
        e = h.entity(SAME)
        def observation():
            return h.strategy.strategy.evaluate(e, tuple(h.strategy.views[k] for k in e.legs),
                Context(12, 8, 0, h.strategy.experiment.experiment_sha256))
        ladder(h, 13, "polymarket:654", asks=((420, 3 * 10**6),))
        self.assertEqual(observation().value_class, "GROSS_NONPOSITIVE")
        ladder(h, 14, "polymarket:654", asks=((390, 999_999),))
        self.assertEqual(observation().status, "DEPTH_LIMITED")
        ladder(h, 15, "polymarket:654", asks=())
        self.assertEqual(observation().status, "ONE_SIDED")
        ladder(h, 16, "polymarket:654", bids=((600, 3 * 10**6),), asks=((390, 3 * 10**6),))
        self.assertEqual(observation().status, "SELF_CROSSED_LEG")
        ladder(h, 17, "polymarket:654", why={"kind": "connection_closed"})
        self.assertEqual(observation().status, "UNUSABLE")
        ladder(h, 18, "polymarket:654", asks=((390, 3 * 10**6),))
        strategy = h.strategy.strategy
        valuation = strategy.valuation
        strategy.valuation = None
        self.assertEqual(observation().status, "VALUATION_UNKNOWN")
        strategy.valuation = valuation
        h.finish()

    def test_reader_rejects_rehashed_proof_vector_and_variant_swap(self):
        h = self.harness()
        h.populate(); result = h.finish()
        original = (h.output / "episodes.ndjson").read_bytes()
        for field in ("payoffs_gross_e36", "payoffs_net_e36"):
            rows = h.records("episodes.ndjson")
            rows[0]["open"]["values"][field][0] = "0"
            raw = b"".join(encoded(r) + b"\n" for r in rows)
            (h.output / "episodes.ndjson").write_bytes(raw)
            m = copy.deepcopy(result["manifest"])
            m["files"]["episodes.ndjson"] = {"sha256": hashlib.sha256(raw).hexdigest(),
                                             "byte_length": len(raw), "records": len(rows)}
            with self.assertRaisesRegex(ProtocolError, "outcome payoffs"):
                validate_content(h.output, h.strategy.snapshot, m)
            (h.output / "episodes.ndjson").write_bytes(original)
        with self.assertRaisesRegex(ProtocolError, "mode binding"):
            read_provisional(h.output, h.root / "context", expected_sha256=h.sha, mode="cross_venue")

    def test_both_completed_readers_bind_real_success_and_factory(self):
        from replay import supervisor
        from replay.strategies._shared.implication_cover.contract import FACTORIES
        from replay.tests.test_supervisor import config as supervisor_config, metadata_pin
        for mode in ("same_venue", "cross_venue"):
            pin = metadata_pin()
            h = self.harness(mode=mode, pin=pin, native=False)
            c = supervisor_config()
            c["transport"].update(run_id="coverage-test", groups=["coverage"], plans=h.initial["plans"],
                                  start_ns="10", end_ns="40", inputs=[pin])
            c["strategies"] = {"coverage": {"factory": FACTORIES[mode], "revision": "synthetic-test",
                                             "config": h.cross_config}}
            identity = supervisor.identity(c)
            h.strategy.binding["identity"] = identity
            h.window()
            ladder(h, 12, "kalshi:series", "complement", bids=((42, 3),))
            if mode == "same_venue":
                ladder(h, 12, "kalshi:sweep", "outcome", bids=((61, 3),))
            else:
                ladder(h, 12, "polymarket:654", asks=((39, 3),))
            h.finish()
            run, attempt = h.root / "run", h.context["attempt_id"]
            participant = run / attempt / "coverage"
            participant.mkdir(parents=True)
            h.output.rename(participant / "output")
            supervisor.write_json_durable(run / "run.json", c)
            terminal = h.seq
            supervisor.write_json_durable(participant / "complete.json", {"version": 1, "identity": identity,
                "attempt": attempt, "group": "coverage", "terminal": terminal})
            supervisor.write_json_durable(participant.parent / "result.json", {"version": 1, "identity": identity,
                "attempt": attempt, "outcome": "success", "fatal": False, "progress": terminal,
                "terminal": terminal, "participants": {"publisher": 0, "coverage": 0}})
            supervisor.write_json_durable(run / "SUCCESS.json", {"version": 1, "identity": identity,
                "attempt": attempt, "terminal": terminal, "outputs": {"coverage": attempt + "/coverage/output"}})
            with patch.object(supervisor, "_strict_metadata_preflight"):
                result = read_completed(run, "coverage", mode=mode)
                self.assertEqual(result["receipt"]["identity"], identity)
                self.assertTrue(any(int(row["class_ns"].get("NET_POSITIVE", "0"))
                                    for row in result["summary"]["rows"]))
                path = participant / "output/content_receipt.json"
                receipt = json.loads(path.read_bytes())
                receipt["attempt_id"] = "b" * 32
                path.write_bytes(encoded(receipt))
                with self.assertRaisesRegex(ProtocolError, "supervisor/content binding"):
                    read_completed(run, "coverage", mode=mode)


if __name__ == "__main__":
    unittest.main()
