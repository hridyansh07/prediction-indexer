"""Offline validation of the reviewed reference corpus and its semantic key."""

import copy
import json
import tempfile
import unittest
from pathlib import Path

from replay.corpus.__main__ import corpus_key
from replay.corpus.spec import load
from replay.economic_sdk.profile_policy import profile_policy


REFERENCE = Path(__file__).resolve().parents[2] / "configs/corpus/execution-realism.example.json"
VERSIONS = {"cross": 4, "multi": 2, "cover_same": 4, "cover_cross": 4}


class ReferenceCorpusTests(unittest.TestCase):
    def reference(self):
        return json.loads(REFERENCE.read_bytes())

    def resolved(self, root):
        spec = self.reference()
        for key in ("runner_config", "canonical_store", "work_root", "results_root", "ledger", "materializer"):
            spec[key] = str(root / key)
        for key in ("runner_config", "materializer"):
            Path(spec[key]).write_text("{}")
        Path(spec["canonical_store"]).mkdir()
        spec["universe_base_url"] = "http://127.0.0.1:1"
        spec["image"]["build_context"] = str(root)
        spec["fees"]["template_catalog"] = str(root / "reviewed-catalog")
        spec["fees"]["sources"] = str(root / "sources")
        Path(spec["fees"]["sources"]).mkdir()
        (Path(spec["fees"]["sources"]) / "public-evidence.txt").write_text("offline contract evidence")
        return spec

    def test_reference_has_exact_policies_and_sixty_probe_scenarios(self):
        groups = {g["name"]: g for g in self.reference()["groups"]}
        self.assertEqual(set(groups), {"coverage", "complement", "profile", *VERSIONS})
        for name, version in VERSIONS.items():
            with self.subTest(group=name):
                policy = groups[name]["config"]["policy"]
                self.assertEqual(policy["version"], version)
                self.assertEqual(policy["fills"]["sizings"], [
                    {"name": "edge", "role": "governs", "edge": True},
                    {"name": "t10", "role": "records", "target_contracts": "10"}])
                self.assertEqual(policy["flags"], {"version": 1, "stale_ms": 5000,
                                                  "settlement_allowlist": ["series_moneyline"]})
                self.assertEqual(policy["fills"]["probes"], {
                    "version": 1, "sizings": ["edge", "t10"], "confirm_ms": [0, 500, 1000],
                    "arrival_ms": [100, 250, 500, 1000, 2000],
                    "sequencing": ["simultaneous", "slow_first"],
                    "unknown_taker_delay_ms": {"kalshi": 0, "limitless": 0, "polymarket": 3000}})
                probes = policy["fills"]["probes"]
                self.assertEqual(len(probes["sizings"]) * len(probes["confirm_ms"]) *
                                 len(probes["arrival_ms"]) * len(probes["sequencing"]), 60)
                if name == "multi":
                    from replay.strategies.same_venue_multi_market.contract import policy_config
                else:
                    from replay.strategies.cross_venue_arbitrage.contract import policy_config
                self.assertEqual(policy_config(policy), policy)
        complement = groups["complement"]["config"]["policy"]
        self.assertEqual(complement["version"], 2)
        self.assertEqual(complement["sizes_contracts"], ["1", "10", "100"])
        self.assertNotIn("fills", complement)
        profile = groups["profile"]["config"]["policy"]
        self.assertTrue({"volatility", "transitions", "levels"} <= set(profile["groups"]))
        self.assertEqual(profile_policy(profile, standalone=True), profile)
        self.assertEqual(profile["volatility"], {
            "version": 1, "sample_ms": 250, "horizons_ms": [250, 500, 1000, 3000, 10000],
            "bucket_ms": 60000, "jump_atoms": [100, 200, 500], "gap_pairs": "outcome_complements"})

    def test_resolved_template_passes_closed_reader_and_settings_bind_key(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = self.resolved(root)
            path = root / "spec.json"
            path.write_text(json.dumps(spec))
            self.assertEqual(load(path), spec)
            args = dict(source={"commit": "offline-contract", "dirty": False},
                        materializer_describe=b"offline producer", runner_config=b"{}")
            base = corpus_key(spec, **args)
            for group in spec["groups"]:
                if group["name"] not in VERSIONS:
                    continue
                index = spec["groups"].index(group)
                for field in ("t10", "probes", "flags"):
                    changed = copy.deepcopy(spec)
                    policy = changed["groups"][index]["config"]["policy"]
                    if field == "t10":
                        policy["fills"]["sizings"][1]["target_contracts"] = "11"
                    elif field == "probes":
                        policy["fills"]["probes"]["arrival_ms"][0] = 101
                    else:
                        policy["flags"]["stale_ms"] = 5001
                    with self.subTest(group=group["name"], field=field):
                        self.assertNotEqual(corpus_key(changed, **args), base)
            changed = copy.deepcopy(spec)
            changed["groups"][-1]["config"]["policy"]["volatility"]["sample_ms"] = 500
            self.assertNotEqual(corpus_key(changed, **args), base)
            changed = copy.deepcopy(spec)
            changed["work_root"] = str(root / "other-scratch")
            self.assertEqual(corpus_key(changed, **args), base)
            spec["unknown"] = True
            path.write_text(json.dumps(spec))
            with self.assertRaisesRegex(ValueError, "corpus spec"):
                load(path)


if __name__ == "__main__":
    unittest.main()
