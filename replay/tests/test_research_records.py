import random
import unittest
from replay.research.records import decimal, game_document, shared_sides
from replay.research.layout import effective_claim


class ResearchRecordTests(unittest.TestCase):
    def test_duplicate_property_matches_all_pairs_with_asymmetric_intervals(self):
        randomizer = random.Random(631)
        for _ in range(50):
            records = []
            for i in range(25):
                start = randomizer.randrange(60)
                end = start + randomizer.randrange(1, 20)
                sides = {(str(randomizer.randrange(4)), "outcome", randomizer.choice(("bid", "ask"))) for _ in range(3)}
                records.append({"lens": "cross", "episode_id": str(i), "start_ns": str(start), "end_ns": str(end),
                                "sides": sides, "shares_leg_with": 0})
            expected = [sum(1 for j, b in enumerate(records) if i != j and a["sides"] & b["sides"]
                            and max(int(a["start_ns"]), int(b["start_ns"])) < min(int(a["end_ns"]), int(b["end_ns"])))
                        for i, a in enumerate(records)]
            shared_sides(records)
            self.assertEqual([r["shares_leg_with"] for r in records], expected)
        self.assertEqual(decimal(9007199254740993001, 9), "9007199254.740993001")

    def test_game_windows_are_source_based_and_only_settlement_is_exact(self):
        game = {"state": "ok", "timeline": {"event_id": "event:d1:" + "a" * 64, "scheduled_start_ns": 10**9,
                "match_end_ns": 20 * 10**9, "maps": [{"index": 1, "derived_start_ns": 5 * 10**9,
                "close_ns": 17 * 10**9, "settlement_ns": 19 * 10**9,
                "scores": {"home": {"value": 1}, "away": {"value": 0}}, "winner_market": "m"}]}}
        windows = {k: {"before_ms": 2000, "after_ms": 5000} for k in
                   ("kalshi_market_close", "close_minus_duration", "kalshi_milestone_end")}
        windows["close_minus_duration"]["after_ms"] = 65000
        d = game_document(game, windows, game["timeline"]["event_id"])
        start = next(r for r in d["events"] if r["kind"] == "map_start")
        self.assertEqual((start["earliest_ns"], start["known_at_ns"]), ("3000000000", "70000000000"))
        self.assertFalse(start["exact"])
        self.assertEqual([r["kind"] for r in d["events"] if r["exact"]], ["settlement"])
        self.assertNotIn("game_clock", str(d))

    def test_shared_native_sides_half_open_boundaries_and_same_chart_book_not_same_side(self):
        rows = [{"lens": "cross", "episode_id": str(i), "start_ns": str(a), "end_ns": str(b),
                 "sides": sides, "shares_leg_with": 0} for i, a, b, sides in
                ((0, 10, 30, {("kalshi:x", "complement", "bid")}),
                 (1, 20, 40, {("kalshi:x", "complement", "bid")}),
                 (2, 30, 50, {("kalshi:x", "outcome", "bid")}),
                 (3, 40, 60, {("kalshi:x", "complement", "bid")}))]
        shared_sides(rows)
        self.assertEqual([r["shares_leg_with"] for r in rows], [1, 1, 0, 0])

    def test_base_claim_does_not_merge_negated_and_nonnegated_books(self):
        doc = {"claims": [{"claim_id": "base", "outcome_keys": ["a"]}],
               "spaces": [{"space_shape_id": "shape", "outcome_keys": ["a", "b", "c"]}]}
        snapshot = {"outcomes": {"provider": "universe", "document": doc}, "scopes": [{"outcome_books":
                    [{"instrument": "x", "orientation": o, "status": "MASKED", "claim_id": "base",
                      "space_shape_id": "shape", "negated": n} for o, n in (("outcome", False), ("complement", True))]}]}
        a = effective_claim(snapshot, 0, ("x", "outcome"))
        b = effective_claim(snapshot, 0, ("x", "complement"))
        self.assertNotEqual(a["effective_claim_id"], b["effective_claim_id"])
        self.assertEqual(b["outcome_keys"], ["b", "c"])
