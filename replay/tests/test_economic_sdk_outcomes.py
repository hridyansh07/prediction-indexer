import copy
import unittest
from itertools import combinations

from analysis.claims import claim_id
from replay.economic_sdk.outcomes import outcome_scope
from replay.preparation import build_snapshot
from replay.tests.test_preparation import config, detail
from replay.tests.test_preparation_outcomes import document


class OutcomeSDKTests(unittest.TestCase):
    def snapshot(self):
        return build_snapshot(
            config(),
            [{"provider": "universe", "detail": detail()}],
            {"provider": "universe", "document": document()},
        )

    def test_payoff_partition_negation_and_cache(self):
        snapshot = self.snapshot()
        view = outcome_scope(snapshot, 0)
        home = ("kalshi:series", "outcome")
        no = ("kalshi:series", "complement")
        pm = ("polymarket:123", "outcome")
        other = ("polymarket:987", "outcome")
        self.assertIs(view, outcome_scope(snapshot, 0))
        self.assertTrue(view.available)
        self.assertTrue(view.is_partition([home, no]))
        self.assertTrue(view.is_partition([pm, other]))
        self.assertFalse(view.is_partition([home, pm]))
        self.assertFalse(view.is_partition([home]))
        self.assertFalse(view.is_partition([home, ("unknown:x", "outcome")]))
        self.assertFalse(view.is_partition([]))
        self.assertEqual(next(iter(view.payoff([home, no]).values())), (1,) * 6)
        self.assertEqual(
            next(iter(view.payoff([home, home, no]).values())),
            tuple(
                2 if k in view.leg(home).keys else 1
                for k in next(iter(view.spaces.values())).keys
            ),
        )
        self.assertEqual(view.implications([home, pm, no, other]), [])
        with self.assertRaises(ValueError):
            view.payoff([("unknown:x", "outcome")])
        self.assertEqual(view.payoff([]), {})

    def test_exact_cover_expected_sets_and_limits(self):
        snapshot = self.snapshot()
        view = outcome_scope(snapshot, 0)
        books = list(view._legs)
        expected = sorted(
            tuple(sorted(pair))
            for pair in combinations(books, 2)
            if view.is_partition(pair)
        )
        self.assertEqual(len(expected), 4)
        self.assertEqual(view.complete_sets(reversed(books)), expected)
        self.assertEqual(view.complete_sets(books + books), expected)
        self.assertEqual(view.complete_sets(books, max_legs=1), [])
        self.assertEqual(view.complete_sets(books, limit=4), expected)
        with self.assertRaisesRegex(ValueError, "limit"):
            view.complete_sets(books, limit=3)

    def test_map_claim_implications_and_complete_sets(self):
        doc, d = document(), detail()
        shape = doc["spaces"][0]["space_shape_id"]
        # Actual Bo3 map-2 winner masks, read by position in the six sequences.
        for native, keys in [
            ("map2-home", ["seq:AHA", "seq:AHH", "seq:HH"]),
            ("map2-away", ["seq:AA", "seq:HAA", "seq:HAH"]),
        ]:
            cid = claim_id(keys, shape)
            doc["claims"].append(
                {"claim_id": cid, "space_shape_id": shape, "outcome_keys": keys}
            )
            mid = "kalshi:" + native
            doc["markets"].append(
                {
                    "market_id": mid,
                    "venue": "kalshi",
                    "market_type": "map_winner",
                    "market_status": "open",
                    "subscription_ids": [native],
                    "outcome_labels": ["Alpha"],
                    "mask_status": "MASKED",
                    "reason": None,
                    "claims": [{"claim_key": "claim=0", "claim_id": cid}],
                    "tokens": [
                        {
                            "subscription_id": native,
                            "claim_key": "claim=0",
                            "negated": False,
                        }
                    ],
                }
            )
            d["context"]["markets"].append(
                {"target_id": mid, "venue": "kalshi", "selected": True}
            )
            d["context"]["targets"].append(
                {
                    "target_id": mid,
                    "venue": "kalshi",
                    "canonical_class": "esports.map_winner",
                    "subscription_ids": [native],
                    "source_ref": "/test",
                }
            )
        doc["claims"].sort(key=lambda c: c["claim_id"])
        doc["markets"].sort(key=lambda m: m["market_id"])
        for field in ("markets", "targets"):
            d["context"][field].sort(key=lambda m: m["target_id"])
        snapshot = build_snapshot(
            config(d),
            [{"provider": "universe", "detail": d}],
            {"provider": "universe", "document": doc},
        )
        view = outcome_scope(snapshot, 0)
        mh, ma = ("kalshi:map2-home", "outcome"), ("kalshi:map2-away", "outcome")
        sh, sa = ("kalshi:series", "outcome"), ("kalshi:series", "complement")
        self.assertEqual(view.leg(mh).keys, frozenset({"seq:AHA", "seq:AHH", "seq:HH"}))
        self.assertEqual(view.complete_sets([mh, ma, sh, sa]), [(ma, mh), (sa, sh)])
        # Neither a map-2 result nor a series result strictly implies the other.
        self.assertEqual(view.implications([mh, ma, sh, sa]), [])
        self.assertFalse(view.is_partition([mh, sa]))
        singleton = ["seq:HH"]
        cid = claim_id(singleton, shape)
        doc["claims"].append(
            {"claim_id": cid, "space_shape_id": shape, "outcome_keys": singleton}
        )
        snapshot["scopes"][0]["outcome_books"].append(
            {
                "instrument": "kalshi:sweep",
                "orientation": "outcome",
                "market_id": "kalshi:sweep",
                "status": "MASKED",
                "reason": None,
                "space_shape_id": shape,
                "claim_id": cid,
                "negated": False,
            }
        )
        # Rebuild a distinct view: production snapshots are deeply immutable.
        snapshot = copy.deepcopy(snapshot)
        view = outcome_scope(snapshot, 0)
        sweep = ("kalshi:sweep", "outcome")
        self.assertEqual(
            view.implications([sweep, mh, sh, sa]), [(sweep, mh), (sweep, sh)]
        )

    def test_incomplete_mixed_and_unavailable(self):
        snapshot = self.snapshot()
        snapshot["outcomes"]["document"]["spaces"][0]["coverage"] = (
            "INCOMPLETE_COVERAGE"
        )
        view = outcome_scope(snapshot, 0)
        pair = [("kalshi:series", "outcome"), ("kalshi:series", "complement")]
        self.assertFalse(view.is_partition(pair))
        self.assertEqual(view.complete_sets(pair), [])
        snapshot = self.snapshot()
        second = copy.deepcopy(snapshot["outcomes"]["document"]["spaces"][0])
        second["space_shape_id"] = "f" * 64
        snapshot["outcomes"]["document"]["spaces"].append(second)
        claim = copy.deepcopy(snapshot["outcomes"]["document"]["claims"][0])
        claim.update(claim_id="e" * 64, space_shape_id="f" * 64)
        snapshot["outcomes"]["document"]["claims"].append(claim)
        snapshot["scopes"][0]["outcome_books"][0].update(
            space_shape_id="f" * 64, claim_id="e" * 64
        )
        view = outcome_scope(snapshot, 0)
        self.assertFalse(view.is_partition(pair))
        with self.assertRaisesRegex(ValueError, "one outcome space"):
            view.payoff(pair)
        legacy = build_snapshot(
            config(), [{"provider": "universe", "detail": detail()}]
        )
        self.assertFalse(outcome_scope(legacy, 0).available)
        self.assertEqual(outcome_scope(legacy, 0).unavailable, "outcomes_unavailable")

    def test_cache_is_bounded(self):
        from replay.economic_sdk.outcomes import _CACHE

        for _ in range(20):
            outcome_scope(self.snapshot(), 0)
        self.assertLessEqual(len(_CACHE), 8)
