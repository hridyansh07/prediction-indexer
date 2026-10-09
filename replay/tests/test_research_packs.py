import tempfile
import unittest
from pathlib import Path

from replay.research.charts import bucket_series, build_charts
from replay.research.io import document
from replay.tests.test_research_verify import commit_rows


class ResearchChartTests(unittest.TestCase):
    def test_asymmetric_bucket_integrals_null_gap_and_empty_last(self):
        spans = [(0, 17, 43, 0, 37, "usable"), (0, None, None, 37, 169, "unusable:gap"),
                 (0, 19, 41, 169, 200, "usable")]
        result = bucket_series(spans, [(0, 173, 11)], 0, 300, 100, 1)
        cells = result["books"][0]["buckets"]
        self.assertEqual(cells[0], {"bid": ["17", "17", None], "ask": ["43", "43", None],
                                    "usable_ppm": 370000, "trades": 0, "trade_qty": "0"})
        self.assertEqual(cells[1]["bid"], ["19", "19", "19"])
        self.assertEqual(cells[1]["usable_ppm"], 310000)
        self.assertEqual(cells[1]["trade_qty"], "11")
        self.assertEqual(cells[2]["bid"], None)

    def test_raw_tile_opening_state_and_scope_reentry_are_not_forward_filled(self):
        second = 10**9
        key = {"instrument": "polymarket:a", "orientation": "outcome"}
        snapshot = {"plans": [{**key, "venue": "polymarket", "price_scale": "2", "quantity_scale": "0"}],
                    "scopes": [{"start_ns": "0", "end_ns": str(620 * second), "members": [{"market_id": "m", "books": [key]}]},
                               {"start_ns": str(1200 * second), "end_ns": str(1260 * second), "members": [{"market_id": "m", "books": [key]}]}]}
        top = [{"type": "top", "scope": s, "book": 0, "cut": s, "t_ns": str(t), "cause": "open",
                "validity": "usable", "bid": [str(p), "3"], "ask": ["71", "5"]}
               for s, t, p in ((0, 0, 17), (1, 1200 * second, 29))]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            identity = commit_rows(root, "transitions.ndjson.zst", top)
            out = root / "pack"
            build_charts(out, root, snapshot, identity)
            raw = document(out / "tiles/raw/10.json")
            self.assertEqual(raw["rows"], [])
            self.assertEqual(raw["state_at_start"], [top[0]])
            raw = document(out / "tiles/raw/20.json")
            self.assertEqual(raw["state_at_start"], [])
            self.assertEqual(raw["rows"], [top[1]])
            self.assertEqual(raw["scopes"][0]["scope"], 1)
