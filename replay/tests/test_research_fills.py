import copy
import tempfile
import unittest
from pathlib import Path

from replay.research.verify.fills import check_episodes
from replay.tests.test_cross_venue_arbitrage import EDGE, Harness
from replay.tests.economic_scenarios import ladder
from replay.tests.test_research_verify import commit_rows


class ResearchFillTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.h = Harness(Path(self.tmp.name), fills=(EDGE,), fees="collateral")
        self.addCleanup(self.h.close)
        self.h.populate()
        ladder(self.h, 12, "kalshi:series", "complement", bids=((60, 3), (50, 10)))
        ladder(self.h, 12, "polymarket:987", bids=((100, 3000000),), asks=((570, 1000000), (595, 5000000)))
        self.h.finish()
        import json
        self.manifest = json.loads((self.h.output / "manifest.json").read_text())
        self.episodes = self.h.records("episodes.ndjson")

    def check(self, tamper=None):
        manifest = copy.deepcopy(self.manifest)
        if tamper:
            episodes = copy.deepcopy(self.episodes)
            tamper(next(r for r in episodes if r.get("fill")))
            manifest["files"]["episodes.ndjson"] = commit_rows(self.h.output, "episodes.ndjson", episodes)
        return check_episodes(self.h.output, self.h.snapshot, manifest, self.h.cross_config)

    def test_fee_catalog_not_fixed_per_contract_and_all_upstream_episodes_preserved(self):
        result = self.check()
        self.assertEqual(result["episodes"], len(self.episodes))
        self.assertGreater(result["fills"], 0)

    def test_self_consistent_identity_cannot_hide_wrong_value(self):
        def tamper(row):
            row["fill"]["results"][0]["value"] = str(int(row["fill"]["results"][0]["value"]) + 1)
        with self.assertRaisesRegex(ValueError, "fee.*value"):
            self.check(tamper)

    def test_kill_one_atom_boundary_and_false_edge_witness(self):
        def kill(row):
            row["fill"]["results"][0]["kill_prices"][0] = "0"
        with self.assertRaisesRegex(ValueError, "kill"):
            self.check(kill)

        def edge(row):
            result = row["fill"]["results"][0]
            result["stop"] = "edge"
            result["beyond"] = [[], []]
            result["after"] = [None, None]
        with self.assertRaisesRegex(ValueError, "after|edge"):
            self.check(edge)
