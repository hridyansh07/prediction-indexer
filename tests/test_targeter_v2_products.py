"""Product parameters come from a market's own words, never from the team names."""

import unittest

from targeter.v2.parsing import products


class TotalsDirectionTests(unittest.TestCase):
    def test_a_team_name_containing_under_does_not_flip_an_over_market(self) -> None:
        # Kalshi titles name both teams; "ThunderTalk" and "Sunderland" contain "under".
        cases = [
            ("total_maps", "Invictus Gaming vs ThunderTalk Gaming: Total Maps", "Over 3.5 maps",
             ("Invictus Gaming", "ThunderTalk Gaming"), 3.5),
            ("total_goals", "Sunderland vs Arsenal: Total Goals", "Over 2.5 goals",
             ("Sunderland", "Arsenal"), 2.5),
            ("total_maps", "Ground Zero vs THUNDER dOWNUNDER: Total Maps", "Over 2.5 maps",
             ("Ground Zero", "THUNDER dOWNUNDER"), 2.5),
        ]
        for market_type, title, yes_label, participants, line in cases:
            with self.subTest(title=title):
                parameters = products.market_parameters(
                    market_type, title=title, participants=participants,
                    outcome_labels=(yes_label, yes_label), raw={"yes_sub_title": yes_label},
                )
                self.assertEqual(parameters["direction"], "over")
                self.assertEqual(parameters["line"], line)

    def test_a_team_name_containing_over_adds_no_direction(self) -> None:
        # A Polymarket O/U market states no direction; "Overtake" must not supply one.
        def parameters(home: str) -> dict:
            return products.market_parameters(
                "total_maps", title=f"{home} vs Voca - Total Maps O/U 2.5", participants=(home, "Voca"),
                outcome_labels=("Over", "Under"), raw={"groupItemTitle": "O/U 2.5"},
            )

        self.assertEqual(parameters("Overtake"), parameters("Alpha"))
        self.assertNotIn("direction", parameters("Overtake"))

    def test_the_market_words_still_decide_under_and_over(self) -> None:
        under = products.market_parameters(
            "total_goals", title="Sunderland vs Hull City: Total Goals", participants=("Sunderland", "Hull City"),
            outcome_labels=("Under 2.5 goals",), raw={"yes_sub_title": "Under 2.5 goals"},
        )
        self.assertEqual((under["direction"], under["line"]), ("under", 2.5))
        or_more = products.market_parameters(
            "total_goals", title="Sunderland vs Fulham: 3 or more goals", participants=("Sunderland", "Fulham"),
            outcome_labels=("3 or more goals",), raw={"yes_sub_title": "3 or more goals"},
        )
        self.assertEqual((or_more["direction"], or_more["line"], or_more["published_line"]), ("over", 2.5, 3))


if __name__ == "__main__":
    unittest.main()
