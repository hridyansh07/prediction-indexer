from __future__ import annotations

import unittest

from analysis.outcome_space import (
    COVERAGE_EXHAUSTIVE,
    COVERAGE_INCOMPLETE,
    SCOPE_SERIES,
    build_score_space,
    build_series_space,
    build_state_timeline,
    parse_best_of,
    parse_score_ticker,
    reachable_keys,
    series_sequences,
    settled_outcome_key,
)


class ScoreTickerTests(unittest.TestCase):
    def test_parses_team_codes_and_goals(self) -> None:
        self.assertEqual(
            parse_score_ticker("KXWCSCORE-26JUL19ESPARG-ESP1ARG0"),
            ("ESP", 1, "ARG", 0),
        )

    def test_multi_digit_and_zero_scores(self) -> None:
        self.assertEqual(parse_score_ticker("X-Y-ESP0ARG10"), ("ESP", 0, "ARG", 10))

    def test_rejects_non_score_suffix(self) -> None:
        self.assertIsNone(parse_score_ticker("KXWCGAME-26JUL19ESPARG-TIE"))


class ScoreSpaceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.space = build_score_space(
            "final",
            [
                "K-E-ESP0ARG0",
                "K-E-ESP1ARG0",
                "K-E-ESP0ARG1",
                "K-E-ESP2ARG1",
            ],
        )

    def test_outcomes_carry_derived_fields(self) -> None:
        outcome = self.space.outcome("score:2-1")
        self.assertEqual(outcome.payload["total_goals"], 3)
        self.assertEqual(outcome.payload["goal_difference"], 1)

    def test_a_listed_ladder_is_never_marked_exhaustive(self) -> None:
        """Uncovered scorelines are out of Omega, so no lock claim is possible."""
        self.assertEqual(self.space.coverage, COVERAGE_INCOMPLETE)

    def test_select_filters_on_payload(self) -> None:
        draws = self.space.select(lambda p: p["goal_difference"] == 0)
        self.assertEqual(draws, frozenset({"score:0-0"}))

    def test_settled_key_requires_membership(self) -> None:
        self.assertEqual(
            settled_outcome_key(self.space, home_goals=2, away_goals=1), "score:2-1"
        )
        self.assertIsNone(settled_outcome_key(self.space, home_goals=9, away_goals=9))


class SeriesSpaceTests(unittest.TestCase):
    def test_bo3_has_six_reachable_sequences(self) -> None:
        self.assertEqual(
            set(series_sequences(3)), {"HH", "HAH", "HAA", "AHH", "AHA", "AA"}
        )

    def test_bo5_has_twenty(self) -> None:
        self.assertEqual(len(series_sequences(5)), 20)

    def test_sequences_stop_at_the_clinch(self) -> None:
        self.assertTrue(all(len(s) <= 3 for s in series_sequences(3)))

    def test_even_best_of_rejected(self) -> None:
        with self.assertRaises(ValueError):
            series_sequences(4)

    def test_series_space_is_exhaustive_by_construction(self) -> None:
        space = build_series_space("m", best_of=3, home="A", away="B")
        self.assertEqual(space.coverage, COVERAGE_EXHAUSTIVE)
        self.assertEqual(len(space.outcomes), 6)

    def test_best_of_parsed_from_title(self) -> None:
        self.assertEqual(parse_best_of("Dota 2: X vs Y (BO5) - Playoffs"), 5)
        self.assertEqual(parse_best_of("something BO3 else"), 3)
        self.assertIsNone(parse_best_of("no format here"))


class ReachabilityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.space = build_series_space("m", best_of=3, home="A", away="B")

    def test_state_removes_outcomes_never_adds(self) -> None:
        """The whole point: Omega is fixed, the reachable set only shrinks."""
        pre = reachable_keys(self.space, "")
        after = reachable_keys(self.space, "H")
        self.assertEqual(len(pre), 6)
        self.assertTrue(after < pre)
        self.assertEqual(after, {"seq:HH", "seq:HAH", "seq:HAA"})

    def test_identity_emerges_only_after_a_transition(self) -> None:
        """idea.md's worked example: under-2.5-maps and 'home wins map 2' are
        distinct pre-match and identical once home takes map 1."""
        under_25 = self.space.select(lambda p: p["maps_played"] < 2.5)
        home_map2 = self.space.select(
            lambda p: len(p["sequence"]) > 1 and p["sequence"][1] == "H"
        )
        self.assertNotEqual(under_25, home_map2)

        universe = reachable_keys(self.space, "H")
        self.assertEqual(under_25 & universe, home_map2 & universe)


class StateTimelineTests(unittest.TestCase):
    def test_timeline_builds_prefix_from_settled_maps(self) -> None:
        timeline = build_state_timeline(
            [
                {"map_index": 2, "winner": "PARI", "settled_at_ms": 200},
                {"map_index": 1, "winner": "BB", "settled_at_ms": 100},
                {"map_index": 3, "winner": "PARI", "settled_at_ms": 300},
            ],
            home="BB",
        )
        self.assertEqual([t.prefix for t in timeline], ["H", "HA", "HAA"])
        self.assertEqual([t.at_ms for t in timeline], [100, 200, 300])

    def test_empty_timeline_is_pre_match(self) -> None:
        self.assertEqual(build_state_timeline([], home="BB"), ())


if __name__ == "__main__":
    unittest.main()
