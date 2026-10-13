"""Universe projection v4: Kalshi totals direction re-derived from the market's words.

Contract-shaped, offline. See docs/specs/UNIVERSE_TOTALS_DIRECTION_V1.md.
"""

import copy
import hashlib
import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path

from analysis.outcome_space import build_series_space
from archive.storage import INDEPENDENT, LocalObjectStore
from targeter.v2.relationships import derive_bundle_relationships
from tests.test_event_universe_store import (
    G1,
    R1,
    _catalog_rows,
    _publish_run,
    _selection_report,
)
from universe.claims.claim_projection import project_claims, rebuild_bundle
from universe.derive.market_projection import project_market_universe
from universe.ingest.sync import UniverseSync
from universe.store import UniverseStore

KALSHI_TOTAL = "kalshi:KXLOLTOTALMAPS-26AUG280500IGTT-4"
PM_TOTAL = "polymarket:totals-bo5"
RULE_ID = "kalshi-totals-direction-v1"


def _digest(value) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _thundertalk(stored_direction="under"):
    """An IG vs ThunderTalk BO5 bundle whose stored Kalshi direction is wrong.

    The report's relationships are derived from the stored (buggy) parameters,
    exactly as the targeter recorded them at report time.
    """
    report = _selection_report(R1, G1)
    candidate = report["candidates"][0]
    candidate.update(
        game="league_of_legends",
        participants=["IG", "ThunderTalk"],
        participant_keys=["ig", "thundertalk"],
    )
    candidate["market_ids"] = [
        "kalshi:series",
        "polymarket:series",
        KALSHI_TOTAL,
        PM_TOTAL,
    ]
    events, markets = _catalog_rows(report)
    markets[:] = [m for m in markets if m["target_id"] in candidate["market_ids"]]
    for event in events:
        event.update(
            game="league_of_legends",
            participants=["IG", "ThunderTalk"],
            participant_keys=["ig", "thundertalk"],
            format="5",
        )
    for market in markets:
        market["subscription_ids"] = (
            [market["venue_market_id"]]
            if market["venue"] == "kalshi"
            else ["pm-a", "pm-b"]
        )
        market["outcome_labels"] = (
            ["IG"] if market["venue"] == "kalshi" else ["IG", "ThunderTalk"]
        )
    base = markets[0]
    kalshi = copy.deepcopy(base)
    kalshi.update(
        target_id=KALSHI_TOTAL,
        venue="kalshi",
        venue_market_id=KALSHI_TOTAL.split(":", 1)[1],
        venue_event_id="event-a",
        canonical_class="esports.total_maps",
        market_type="total_maps",
        title="IG vs ThunderTalk: Over 3.5 maps",
        subscription_ids=[KALSHI_TOTAL.split(":", 1)[1]],
        outcome_labels=["Over 3.5 maps", "Over 3.5 maps"],
        parameters={"line": 3.5, "direction": stored_direction},
    )
    polymarket = copy.deepcopy(base)
    polymarket.update(
        target_id=PM_TOTAL,
        venue="polymarket",
        venue_market_id=PM_TOTAL.split(":", 1)[1],
        venue_event_id="event-b",
        canonical_class="esports.total_maps",
        market_type="total_maps",
        title="Games Total: O/U 3.5",
        subscription_ids=["pm-over", "pm-under"],
        outcome_labels=["Over", "Under"],
        parameters={"line": 3.5},
    )
    markets[:] = [
        m for m in markets if m["target_id"] not in {KALSHI_TOTAL, PM_TOTAL}
    ]
    markets.extend([kalshi, polymarket])
    candidate["relationship_analysis"]["relationships"] = []
    projected = project_market_universe(
        report, catalog_events=events, catalog_markets=markets
    )
    # Put the report's stored parameters back so the recorded relationships are
    # those of the targeter at report time, whatever the projection does.
    stored = {m["target_id"]: m["parameters"] for m in markets}
    for row in projected["venue_markets"]:
        row["parameters"] = stored[f"{row['venue']}:{row['venue_market_id']}"]
    bundle = rebuild_bundle(
        projected["events"][0], projected["venue_events"], projected["venue_markets"]
    )
    candidate["relationship_analysis"]["relationships"] = [
        r.as_record() for r in derive_bundle_relationships(bundle).relationships
    ]
    return report, events, markets


def _over_keys() -> set[str]:
    space = build_series_space("b", best_of=5, home="IG", away="ThunderTalk")
    keys = {o.key for o in space.outcomes if o.payload["maps_played"] > 3.5}
    assert keys
    return keys


class TotalsDirectionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.db = UniverseStore(root / "universe.db")
        self.db.initialize()
        self.store = LocalObjectStore(
            root / "archive", store_id="test", durability=INDEPENDENT
        )

    def _ingest(self, report, events, markets):
        _publish_run(self.store, report, catalog_rows=(events, markets))
        result = UniverseSync(self.db, self.store).sync(
            now=datetime(2026, 1, 1, 0, 5, tzinfo=UTC)
        )
        self.assertEqual(result.ingested, 1, result.as_record())

    def test_kalshi_over_market_is_not_read_as_under_from_a_team_name(self):
        report, events, markets = _thundertalk()
        self.assertTrue(
            any(
                r["relationship"] == "MUTUAL_EXCLUSION"
                for r in report["candidates"][0]["relationship_analysis"][
                    "relationships"
                ]
            ),
            "fixture must record relations consistent with the wrong direction",
        )
        self._ingest(report, events, markets)

        doc = self.db.bundle_outcomes("bundle-1")
        claims = {c["claim_id"]: set(c["outcome_keys"]) for c in doc["claims"]}
        by_market = {m["market_id"]: m for m in doc["markets"]}
        kalshi_keys = claims[by_market[KALSHI_TOTAL]["claims"][0]["claim_id"]]
        self.assertEqual(kalshi_keys, _over_keys())

        with closing(sqlite3.connect(self.db.path)) as c:
            corrections = c.execute(
                """SELECT venue, venue_market_id, field, report_value,
                          projected_value, rule_id
                   FROM universe_run_projection_corrections"""
            ).fetchall()
            stored = json.loads(
                c.execute(
                    "SELECT parameters_json FROM venue_markets WHERE venue_market_id = ?",
                    (KALSHI_TOTAL.split(":", 1)[1],),
                ).fetchone()[0]
            )
        self.assertEqual(
            corrections,
            [
                (
                    "kalshi",
                    KALSHI_TOTAL.split(":", 1)[1],
                    "direction",
                    "under",
                    "over",
                    RULE_ID,
                )
            ],
        )
        self.assertEqual(stored, {"line": 3.5, "direction": "over"})
        # Cross-venue equivalence: Polymarket's Over is the same claim.
        self.assertEqual(
            by_market[KALSHI_TOTAL]["claims"][0]["claim_id"],
            by_market[PM_TOTAL]["claims"][0]["claim_id"],
        )

    def test_a_corrected_market_gets_a_different_canonical_id(self):
        report, events, markets = _thundertalk()
        projection = project_market_universe(
            report, catalog_events=events, catalog_markets=markets
        )
        row = next(
            r for r in projection["venue_markets"]
            if r["venue_market_id"] == KALSHI_TOTAL.split(":", 1)[1]
        )
        self.assertEqual(row["parameters"]["direction"], "over")
        self.assertEqual(projection["projection_version"], 4)
        self.assertEqual(len(projection["corrections"]), 1)

    def test_missing_direction_removes_the_field_and_makes_no_claim(self):
        report, events, markets = _thundertalk()
        kalshi = next(m for m in markets if m["target_id"] == KALSHI_TOTAL)
        kalshi["title"] = "IG vs ThunderTalk: 3.5 maps"
        kalshi["outcome_labels"] = ["Yes", "No"]
        projection = project_market_universe(
            report, catalog_events=events, catalog_markets=markets
        )
        row = next(
            r for r in projection["venue_markets"]
            if r["venue_market_id"] == KALSHI_TOTAL.split(":", 1)[1]
        )
        self.assertNotIn("direction", row["parameters"])
        self.assertEqual(projection["corrections"][0]["projected_value"], None)
        self.assertEqual(projection["corrections"][0]["report_value"], "under")

    def test_polymarket_market_with_the_same_words_is_left_as_reported(self):
        report, events, markets = _thundertalk()
        pm = next(m for m in markets if m["target_id"] == PM_TOTAL)
        pm["title"] = "IG vs ThunderTalk Over 3.5 maps"
        pm["parameters"] = {"line": 3.5, "direction": "under"}
        projection = project_market_universe(
            report, catalog_events=events, catalog_markets=markets
        )
        row = next(
            r for r in projection["venue_markets"] if r["venue"] == "polymarket"
            and r["venue_market_id"] == PM_TOTAL.split(":", 1)[1]
        )
        self.assertEqual(row["parameters"], {"line": 3.5, "direction": "under"})
        self.assertEqual(
            [c["venue"] for c in projection["corrections"]], ["kalshi"]
        )

    def test_an_unaffected_run_projects_as_v3_did(self):
        report = _selection_report(R1, G1)
        events, markets = _catalog_rows(report)
        projection = project_market_universe(
            report, catalog_events=events, catalog_markets=markets
        )
        self.assertEqual(projection["corrections"], [])
        stable = {
            key: value
            for key, value in projection.items()
            if key not in {"projection_version", "corrections"}
        }
        self.assertEqual(_digest(stable), _V3_PROJECTION_DIGEST)
        claims = project_claims(projection)
        self.assertEqual(_digest(claims), _V3_CLAIMS_DIGEST)


_V3_PROJECTION_DIGEST = "3372d8a47e584810976488185de84228ad249d5875bcf27adc6176c3166714cd"
_V3_CLAIMS_DIGEST = "399171369fb4349451415016b35f3b8deb628e71e3af615e0c4d723259311969"


if __name__ == "__main__":
    unittest.main()
