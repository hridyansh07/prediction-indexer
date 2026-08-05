from __future__ import annotations

import unittest

from analysis.masks import (
    IDENTITY,
    IMPLICATION,
    MUTUAL_EXCLUSION,
    OVERLAP,
    STATUS_DERIVABLE,
    STATUS_DIFFERENT_SCOPE,
    STATUS_NOT_A_FUNCTION,
    STATUS_UNSUPPORTED,
    compile_mask,
    derive_relationships,
    is_partition,
    relationship,
    state_conditioned_relationships,
)
from analysis.outcome_space import (
    SCOPE_FIRST_HALF,
    build_score_space,
    build_series_space,
    reachable_keys,
)


def _score_space():
    # A 3x3 grid: enough to separate every soccer resolver.
    tickers = [f"K-E-ESP{h}ARG{a}" for h in range(3) for a in range(3)]
    return build_score_space("final", tickers)


def _market(**kwargs):
    base = {"venue": "kalshi", "event_key": "final"}
    base.update(kwargs)
    return base


class SoccerResolverTests(unittest.TestCase):
    def setUp(self) -> None:
        self.space = _score_space()

    def test_correct_score_is_a_single_outcome(self) -> None:
        mask = compile_mask(
            _market(market_type="correct_score", ticker="K-E-ESP2ARG1"), self.space
        )
        self.assertEqual(mask.status, STATUS_DERIVABLE)
        self.assertEqual(mask.outcome_keys, {"score:2-1"})

    def test_moneyline_partitions_by_goal_difference(self) -> None:
        home = compile_mask(
            _market(market_type="moneyline_3way", ticker="K-E-ESP",
                    yes_label="Reg Time: Spain"), self.space)
        draw = compile_mask(
            _market(market_type="moneyline_3way", ticker="K-E-TIE",
                    yes_label="Reg Time: Tie"), self.space)
        away = compile_mask(
            _market(market_type="moneyline_3way", ticker="K-E-ARG",
                    yes_label="Reg Time: Argentina"), self.space)
        self.assertEqual(home.outcome_keys, {"score:1-0", "score:2-0", "score:2-1"})
        self.assertEqual(draw.outcome_keys, {"score:0-0", "score:1-1", "score:2-2"})
        self.assertEqual(len(away.outcome_keys), 3)
        verdict = is_partition([home, draw, away], self.space)
        self.assertTrue(verdict["is_partition"])

    def test_total_goals_over_and_under(self) -> None:
        over = compile_mask(
            _market(market_type="total_goals", ticker="K-E-3",
                    yes_label="Reg Time: Over 2.5 goals scored"), self.space)
        self.assertEqual(
            over.outcome_keys, {"score:1-2", "score:2-1", "score:2-2"}
        )

    def test_both_teams_to_score(self) -> None:
        mask = compile_mask(
            _market(market_type="both_teams_to_score", ticker="K-E-BTTS",
                    yes_label="Reg Time: Both Teams To Score"), self.space)
        self.assertTrue(all(k not in mask.outcome_keys
                            for k in ("score:0-0", "score:1-0", "score:0-2")))
        self.assertIn("score:1-1", mask.outcome_keys)

    def test_team_total_uses_the_named_side(self) -> None:
        mask = compile_mask(
            _market(market_type="team_total_goals", ticker="K-E-ESP2",
                    yes_label="Reg Time: Spain over 1.5 goals"), self.space)
        self.assertTrue(all(k.startswith("score:2-") for k in mask.outcome_keys))

    def test_spread_is_strict(self) -> None:
        mask = compile_mask(
            _market(market_type="spread", ticker="K-E-ESP2",
                    yes_label="Goal Diff Reg Time: Spain wins by more than 1"),
            self.space)
        self.assertEqual(mask.outcome_keys, {"score:2-0"})


class ScopeClassificationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.space = _score_space()

    def test_goal_order_market_is_not_a_function_of_final_score(self) -> None:
        mask = compile_mask(
            _market(market_type="first_team_to_score", ticker="K-E-ESP"), self.space)
        self.assertEqual(mask.status, STATUS_NOT_A_FUNCTION)
        self.assertFalse(mask.derivable)

    def test_method_of_victory_is_not_a_function(self) -> None:
        """Extra time and penalties both look like a regulation draw."""
        mask = compile_mask(
            _market(market_type="method_of_victory", ticker="K-E-ESPPEN"), self.space)
        self.assertEqual(mask.status, STATUS_NOT_A_FUNCTION)

    def test_first_half_market_rejected_by_fulltime_space(self) -> None:
        mask = compile_mask(
            _market(market_type="first_half_total_goals", ticker="K-E-1",
                    yes_label="Over 0.5"), self.space)
        self.assertEqual(mask.status, STATUS_DIFFERENT_SCOPE)

    def test_unknown_type_is_unsupported_not_guessed(self) -> None:
        mask = compile_mask(_market(market_type="mystery", ticker="K-E-X"), self.space)
        self.assertEqual(mask.status, STATUS_UNSUPPORTED)

    def test_non_derivable_masks_are_excluded_from_relationships(self) -> None:
        good = compile_mask(
            _market(market_type="moneyline_3way", ticker="K-E-TIE",
                    yes_label="Reg Time: Tie"), self.space)
        bad = compile_mask(
            _market(market_type="first_team_to_score", ticker="K-E-ESP"), self.space)
        self.assertIsNone(relationship(good, bad))
        self.assertEqual(derive_relationships([good, bad]), [])


class SeriesResolverTests(unittest.TestCase):
    def setUp(self) -> None:
        self.space = build_series_space("m", best_of=3, home="PARI", away="TY")

    def test_map_winner_indexes_the_sequence(self) -> None:
        mask = compile_mask(
            _market(venue="polymarket", market_type="map_winner",
                    group_item_title="Game 1 Winner PARI", ticker="x"), self.space)
        self.assertEqual(mask.outcome_keys, {"seq:HH", "seq:HAH", "seq:HAA"})

    def test_map_two_excludes_sequences_that_ended_early(self) -> None:
        mask = compile_mask(
            _market(venue="polymarket", market_type="map_winner", map_index=3,
                    group_item_title="Game 3 Winner PARI", ticker="x"), self.space)
        # Only three-map sequences reach a decider.
        self.assertTrue(all(len(k) == len("seq:HAH") for k in mask.outcome_keys))

    def test_total_maps_over_under(self) -> None:
        over = compile_mask(
            _market(venue="polymarket", market_type="total_maps",
                    group_item_title="O/U 2.5 Games Over", ticker="x"), self.space)
        self.assertEqual(over.outcome_keys, {"seq:HAH", "seq:HAA", "seq:AHH", "seq:AHA"})

    def test_series_moneyline(self) -> None:
        mask = compile_mask(
            _market(venue="polymarket", market_type="series_moneyline",
                    group_item_title="PARI", ticker="x"), self.space)
        self.assertEqual(mask.outcome_keys, {"seq:HH", "seq:HAH", "seq:AHH"})


class RelationshipTests(unittest.TestCase):
    def setUp(self) -> None:
        self.space = build_series_space("m", best_of=3, home="PARI", away="TY")
        self.under = compile_mask(
            _market(venue="polymarket", market_type="total_maps",
                    group_item_title="O/U 2.5 Games Under", ticker="u"), self.space)
        self.map2 = compile_mask(
            _market(venue="polymarket", market_type="map_winner", map_index=2,
                    group_item_title="Game 2 Winner PARI", ticker="m2"), self.space)
        self.series = compile_mask(
            _market(venue="polymarket", market_type="series_moneyline",
                    group_item_title="PARI", ticker="s"), self.space)

    def test_subset_is_an_implication(self) -> None:
        clinch = compile_mask(
            _market(venue="polymarket", market_type="map_handicap",
                    group_item_title="Game Handicap: PARI (-1.5)", ticker="h"),
            self.space)
        self.assertEqual(relationship(clinch, self.series), IMPLICATION)

    def test_disjoint_masks_are_mutually_exclusive(self) -> None:
        away = compile_mask(
            _market(venue="polymarket", market_type="series_moneyline",
                    group_item_title="TY", ticker="s2"), self.space)
        self.assertEqual(relationship(self.series, away), MUTUAL_EXCLUSION)

    def test_identity_appears_only_under_the_conditioned_universe(self) -> None:
        self.assertNotEqual(relationship(self.under, self.map2), IDENTITY)
        universe = reachable_keys(self.space, "H")
        self.assertEqual(
            relationship(self.under, self.map2, universe=universe), IDENTITY
        )

    def test_state_conditioning_surfaces_new_identities(self) -> None:
        rows = state_conditioned_relationships(
            [self.under, self.map2, self.series], self.space, ["", "H"]
        )
        pre = {r["relationship"] for r in rows if r["state_prefix"] == "(pre-match)"}
        post = {r["relationship"] for r in rows if r["state_prefix"] == "H"}
        self.assertNotIn(IDENTITY, pre)
        self.assertIn(IDENTITY, post)


class PartitionTests(unittest.TestCase):
    def test_incomplete_ladder_reports_uncovered_outcomes(self) -> None:
        space = _score_space()
        one = compile_mask(
            _market(market_type="correct_score", ticker="K-E-ESP0ARG0"), space)
        verdict = is_partition([one], space)
        self.assertFalse(verdict["is_partition"])
        self.assertEqual(len(verdict["uncovered_outcomes"]), 8)

    def test_overlapping_masks_are_not_a_partition(self) -> None:
        space = _score_space()
        over05 = compile_mask(
            _market(market_type="total_goals", ticker="K-E-1",
                    yes_label="Reg Time: Over 0.5 goals scored"), space)
        over15 = compile_mask(
            _market(market_type="total_goals", ticker="K-E-2",
                    yes_label="Reg Time: Over 1.5 goals scored"), space)
        verdict = is_partition([over05, over15], space)
        self.assertFalse(verdict["is_partition"])
        self.assertTrue(verdict["overlapping_outcomes"])


if __name__ == "__main__":
    unittest.main()
