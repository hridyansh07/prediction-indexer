"""Independent research checks: asymmetric contract shapes, never live responses."""
import hashlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from encoder import encode_stream
from replay.research.verify.profile import check_profile


def commit_rows(root, name, rows):
    payload = b"".join((json.dumps(r, sort_keys=True, separators=(",", ":")) + "\n").encode() for r in rows)
    if name.endswith(".zst"):
        with (root / name).open("wb") as sink:
            result = encode_stream(io.BytesIO(payload), sink)
        return {"logical": {"sha256": hashlib.sha256(payload).hexdigest(),
                            "byte_length": len(payload), "records": len(rows)},
                "stored": result.stored.as_record()}
    (root / name).write_bytes(payload)
    return {"sha256": hashlib.sha256(payload).hexdigest(), "byte_length": len(payload), "records": len(rows)}


class ResearchProfileTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.key = {"instrument": "polymarket:a", "orientation": "outcome"}
        self.snapshot = {"plans": [{**self.key, "venue": "polymarket", "price_scale": "2",
                                    "quantity_scale": "0", "lane": "polymarket"}],
                         "scopes": [{"start_ns": "100", "end_ns": "200", "run_id": "run",
                                     "bundle_id": "bundle", "members": [{"market_id": "polymarket:m",
                                                                          "books": [self.key]}]}]}
        self.top = [self.row("top", 100, 0, cause="open", validity="usable", bid=["17", "3"], ask=["43", "7"]),
                    self.row("top", 137, 1, cause="invalidation", validity="unusable:gap", bid=None, ask=None),
                    self.row("top", 169, 2, cause="snapshot", validity="usable", bid=["19", "5"], ask=["41", "9"]),
                    self.row("trade", 173, 3, price="31", qty="11", aggressor="buy", disposition="applied")]
        self.profile = [{**self.key, "scope": 0, "start_ns": "100", "end_ns": "200",
                         "state": {"usable_ns": "68", "not_initialized_ns": "0", "unusable_ns": "32",
                                   "unusable_ns_by_reason": {"gap": "32"}},
                         "activity": {"trades": {"applied": 1, "observed": 0, "invalidated": 0,
                                                 "not_authority": 0, "duplicate": 5},
                                      "trades_scale_mismatch": 0}}]
        book_id = "book:" + hashlib.sha256(json.dumps(self.key, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        self.availability = [{"kind": "book", "entity": book_id, "scope": 0, "start_ns": str(a),
                              "end_ns": str(b), "state": s, "reason": None if reason is None else {"kind": reason}}
                             for a, b, s, reason in ((100, 137, "usable", None), (137, 169, "unusable", "gap"),
                                                     (169, 200, "usable", None))]

    def row(self, kind, time, cut, **fields):
        return {"type": kind, "scope": 0, "book": 0, "t_ns": str(time), "cut": cut, **fields}

    def run_check(self, levels=None):
        files = {name: commit_rows(self.root, name, rows) for name, rows in
                 (("transitions.ndjson.zst", self.top), ("profile.ndjson", self.profile),
                  ("availability.ndjson", self.availability))}
        if levels is not None:
            files["levels.ndjson.zst"] = commit_rows(self.root, "levels.ndjson.zst", levels)
        return check_profile(self.root, self.snapshot, files)

    def test_durations_partition_and_nonduplicate_trade_count(self):
        result = self.run_check()
        self.assertEqual(result["trades"], 1)
        self.assertEqual(result["top_rows"], 3)

    def test_missing_trade_is_not_hidden_by_duplicates(self):
        self.top.pop()
        with self.assertRaisesRegex(ValueError, "trade count"):
            self.run_check()

    def test_availability_overlap_and_duration_tamper_fail(self):
        self.availability[1]["start_ns"] = "136"
        with self.assertRaisesRegex(ValueError, "partition"):
            self.run_check()
        self.availability[1]["start_ns"] = "137"
        self.profile[0]["state"]["usable_ns"] = "69"
        with self.assertRaisesRegex(ValueError, "duration"):
            self.run_check()

    def test_replayed_ladder_is_checked_not_merely_hashed(self):
        levels = [self.row("ladder", 100, 0, cause="open", validity="usable", bids=[["17", "3"]], asks=[["43", "7"]]),
                  self.row("ladder", 137, 1, cause="invalidation", validity="unusable:gap", bids=[], asks=[]),
                  self.row("ladder", 169, 2, cause="snapshot", validity="usable", bids=[["19", "5"]], asks=[["41", "9"]])]
        self.assertEqual(self.run_check(levels)["trades"], 1)
        levels[-1]["asks"][0][1] = "8"
        with self.assertRaisesRegex(ValueError, "ladder.*top"):
            self.run_check(levels)
