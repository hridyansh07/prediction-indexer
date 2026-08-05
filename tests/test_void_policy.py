from __future__ import annotations

import unittest

from analysis.void_policy import (
    PAYOFF_FAIR_MARKET_PRICE,
    PAYOFF_FIFTY_FIFTY,
    PAYOFF_RESOLVE_NO,
    PAYOFF_RESOLVE_YES,
    PAYOFF_RESOLVES_TO_WINNER,
    TRIGGER_CANCELLED_PRE_PLAY,
    TRIGGER_FORFEIT_POST_PLAY,
    TRIGGER_NOT_COMPLETED_OPPONENT_FORFEIT,
    TRIGGER_POSTPONED_BEYOND_WINDOW,
    classify_basket_lock,
    parse_void_policy,
)

# Verbatim excerpts from the rules text stored in this repository.
KALSHI_DOTA = (
    "If the relevant match is postponed and not completed within 48 hours of its "
    "originally scheduled start time, the market will resolve to the fair market "
    "price. If the relevant match is cancelled before play begins, the market will "
    "resolve to the fair market price. If the relevant match is forfeited before any "
    "play occurs, the market will resolve to the fair market price. If the relevant "
    "match begins and is subsequently forfeited, the market will resolve according to "
    "the official result published by the organizer."
)
KALSHI_SOCCER = (
    "If France and Spain both score a goal in the 1st Half, then the market resolves "
    "to Yes. If the game is cancelled or rescheduled to over two weeks away, the "
    "market will resolve to a fair price in accordance with the rules."
)
KALSHI_NO_CLAUSE = (
    "If the score at the end of the 1st Half is France 0, Spain 0 in the France vs "
    "Spain professional FIFA World Cup soccer game, then the market resolves to Yes."
)
PM_ESPORTS_MAP = (
    "If Game 1 is not completed for any reason, this market will resolve 50-50. If "
    "the match is canceled (not played at all) or is delayed beyond 7 days from the "
    "scheduled date without play beginning, this market will resolve 50-50."
)
PM_SERIES_MONEYLINE = (
    "If the match is canceled (not played at all), ends in a tie, or is delayed "
    "beyond 7 days from the scheduled date without a winner determined, this market "
    "will resolve to 50-50. If the match begins but is not completed, and one team "
    "wins due to the opponent's forfeiture, disqualification, or walkover, this "
    "market will resolve to the team who wins."
)
PM_TOTAL_MAPS = (
    "Games won by forfeit, disqualification, walkover, or default are counted "
    "towards the total, provided that the match is completed. If the match is "
    "canceled (not played at all for any reason), ends in a tie, or is delayed "
    "beyond 7 days from the scheduled date without a winner determined, this market "
    "will resolve to 50-50. If the match begins but is not completed, and one team "
    "wins due to the opponent's match forfeiture, disqualification, or walkover, "
    "this market will resolve to 50-50."
)
PM_SOCCER_TEAM = (
    "If France wins, this market will resolve to \"Yes\". Otherwise, this market "
    "will resolve to \"No\". If the game is postponed, this market will remain open "
    "until the game has been completed. If the game is canceled entirely, with no "
    "make-up game, this market will resolve \"No\"."
)
PM_SOCCER_DRAW = (
    "If the game ends in a draw, this market will resolve to \"Yes\". Otherwise, "
    "this market will resolve to \"No\". If the game is postponed, this market will "
    "remain open until the game has been completed. If the game is canceled "
    "entirely, with no make-up game, this market will resolve to \"Yes\"."
)


def _policy(text, venue="kalshi", market_id="M"):
    return parse_void_policy(text, venue=venue, market_id=market_id)


class KalshiParsingTests(unittest.TestCase):
    def test_esports_triggers_and_window(self) -> None:
        policy = _policy(KALSHI_DOTA)
        self.assertTrue(policy.parsed)
        postponed = policy.rule_for(TRIGGER_POSTPONED_BEYOND_WINDOW)
        self.assertEqual(postponed.payoff, PAYOFF_FAIR_MARKET_PRICE)
        self.assertEqual(postponed.window_hours, 48.0)

    def test_forfeit_after_play_is_a_normal_outcome_not_a_branch(self) -> None:
        """The distinction that keeps a forfeit mid-series out of OTHER."""
        policy = _policy(KALSHI_DOTA)
        rule = policy.rule_for(TRIGGER_FORFEIT_POST_PLAY)
        self.assertFalse(rule.opens_branch)
        self.assertEqual(rule.payoff_per_contract, 1.0)
        self.assertNotIn(TRIGGER_FORFEIT_POST_PLAY, policy.branch_triggers())

    def test_soccer_two_week_window_parses_from_words(self) -> None:
        policy = _policy(KALSHI_SOCCER)
        rule = policy.rule_for(TRIGGER_POSTPONED_BEYOND_WINDOW)
        self.assertEqual(rule.window_hours, 336.0)

    def test_market_with_no_clause_is_unparsed_not_defaulted(self) -> None:
        """Silence is a risk state; it must never read as 'settles normally'."""
        policy = _policy(KALSHI_NO_CLAUSE)
        self.assertFalse(policy.parsed)
        self.assertEqual(policy.unmatched_reason, "no_recognised_clause")
        self.assertEqual(policy.rules, ())


class PolymarketParsingTests(unittest.TestCase):
    def test_esports_pays_fifty_fifty_with_seven_day_window(self) -> None:
        policy = _policy(PM_ESPORTS_MAP, venue="polymarket")
        rule = policy.rule_for(TRIGGER_CANCELLED_PRE_PLAY)
        self.assertEqual(rule.payoff, PAYOFF_FIFTY_FIFTY)
        self.assertEqual(rule.window_hours, 168.0)

    def test_same_trigger_diverges_between_two_siblings(self) -> None:
        """Match abandoned after an opponent forfeits: Match Winner settles
        normally, Games Total pays 50-50. Same venue, same event, same trigger."""
        winner = _policy(PM_SERIES_MONEYLINE, venue="polymarket")
        totals = _policy(PM_TOTAL_MAPS, venue="polymarket")
        trigger = TRIGGER_NOT_COMPLETED_OPPONENT_FORFEIT
        self.assertEqual(winner.rule_for(trigger).payoff, PAYOFF_RESOLVES_TO_WINNER)
        self.assertFalse(winner.rule_for(trigger).opens_branch)
        self.assertEqual(totals.rule_for(trigger).payoff, PAYOFF_FIFTY_FIFTY)
        self.assertTrue(totals.rule_for(trigger).opens_branch)

    def test_soccer_legs_carry_opposite_cancellation_payoffs(self) -> None:
        team = _policy(PM_SOCCER_TEAM, venue="polymarket")
        draw = _policy(PM_SOCCER_DRAW, venue="polymarket")
        self.assertEqual(
            team.rule_for(TRIGGER_CANCELLED_PRE_PLAY).payoff, PAYOFF_RESOLVE_NO
        )
        self.assertEqual(
            draw.rule_for(TRIGGER_CANCELLED_PRE_PLAY).payoff, PAYOFF_RESOLVE_YES
        )


class BasketLockTests(unittest.TestCase):
    def test_polymarket_soccer_three_way_survives_cancellation(self) -> None:
        """Two legs forced to No plus a draw leg forced to Yes still pays $1."""
        legs = [
            _policy(PM_SOCCER_TEAM, venue="polymarket", market_id="home"),
            _policy(PM_SOCCER_DRAW, venue="polymarket", market_id="draw"),
            _policy(PM_SOCCER_TEAM, venue="polymarket", market_id="away"),
        ]
        verdict = classify_basket_lock(legs)
        self.assertEqual(verdict["status"], "LOCKED")
        branch = next(
            b for b in verdict["branches"] if b["trigger"] == TRIGGER_CANCELLED_PRE_PLAY
        )
        self.assertAlmostEqual(branch["payoff_per_contract"], 1.0)

    def test_three_leg_fifty_fifty_basket_is_not_locked(self) -> None:
        """n * $0.50 only equals $1 at n == 2; a 3-way is long a cancellation."""
        legs = [
            _policy(PM_ESPORTS_MAP, venue="polymarket", market_id=str(i))
            for i in range(3)
        ]
        verdict = classify_basket_lock(legs)
        branch = next(
            b for b in verdict["branches"] if b["trigger"] == TRIGGER_CANCELLED_PRE_PLAY
        )
        self.assertAlmostEqual(branch["payoff_per_contract"], 1.5)
        self.assertAlmostEqual(branch["shortfall"], 0.5)

    def test_two_leg_fifty_fifty_basket_is_locked(self) -> None:
        legs = [
            _policy(PM_ESPORTS_MAP, venue="polymarket", market_id=str(i))
            for i in range(2)
        ]
        self.assertEqual(classify_basket_lock(legs)["status"], "LOCKED")

    def test_kalshi_fair_price_is_indeterminate_not_assumed_par(self) -> None:
        legs = [_policy(KALSHI_DOTA, market_id=str(i)) for i in range(2)]
        verdict = classify_basket_lock(legs)
        self.assertEqual(verdict["status"], "LOCKED_CONDITIONAL")
        self.assertIn(TRIGGER_CANCELLED_PRE_PLAY, verdict["indeterminate_triggers"])

    def test_leg_without_policy_blocks_a_lock_claim(self) -> None:
        legs = [
            _policy(PM_ESPORTS_MAP, venue="polymarket", market_id="a"),
            _policy(KALSHI_NO_CLAUSE, market_id="silent"),
        ]
        verdict = classify_basket_lock(legs)
        self.assertEqual(verdict["status"], "LOCKED_CONDITIONAL")
        self.assertEqual(verdict["legs_without_policy"], ["silent"])

    def test_mixed_venue_basket_inherits_the_weaker_leg(self) -> None:
        legs = [
            _policy(PM_ESPORTS_MAP, venue="polymarket", market_id="pm"),
            _policy(KALSHI_DOTA, market_id="kalshi"),
        ]
        verdict = classify_basket_lock(legs)
        self.assertEqual(verdict["status"], "LOCKED_CONDITIONAL")


if __name__ == "__main__":
    unittest.main()
