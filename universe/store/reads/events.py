"""Umbrella event reads."""

from __future__ import annotations

from contextlib import closing
from typing import Any

from universe.store import limits
from universe.store.records import (
    _bounded_detail,
    _canonical_market_record,
    _current_era,
    _ensure_detail_rows,
    _event_record,
    _event_refs_sql,
    _limit,
    _row_record,
)


class EventReads:
    def list_events(
        self,
        *,
        after: tuple[int, str] | None = None,
        limit: int = 100,
    ) -> tuple[list[dict[str, Any]], bool]:
        bounded = _limit(limit)
        where = ""
        parameters: list[Any] = []
        if after is not None:
            where = "WHERE (activation_at_ns, event_id) < (?, ?)"
            parameters.extend(after)
        with closing(self.connect(readonly=True)) as connection:
            rows = connection.execute(
                f"""WITH page AS (
                         SELECT * FROM umbrella_events
                         {where}
                         ORDER BY activation_at_ns DESC, event_id DESC
                         LIMIT ?
                     )
                     SELECT page.*,
                            {_event_refs_sql('page')} AS event_refs_json,
                            (SELECT COUNT(DISTINCT venue.venue)
                             FROM venue_events venue
                             WHERE venue.event_id = page.event_id) AS venue_count,
                            (SELECT COUNT(*)
                             FROM canonical_markets market
                             WHERE market.event_id = page.event_id) AS market_count,
                            (SELECT COUNT(DISTINCT selected.run_id)
                             FROM selected_market_occurrences selected
                             WHERE selected.event_id = page.event_id) AS selected_run_count
                     FROM page
                     ORDER BY page.activation_at_ns DESC, page.event_id DESC""",
                (*parameters, bounded + 1),
            ).fetchall()
        return [_event_record(row) for row in rows[:bounded]], len(rows) > bounded

    def event_detail(self, event_id: str) -> dict[str, Any] | None:
        with closing(self.connect(readonly=True)) as connection:
            event = connection.execute(
                f"""SELECT event.*,
                          {_event_refs_sql('event')} AS event_refs_json
                   FROM umbrella_events event WHERE event_id = ?""",
                (event_id,),
            ).fetchone()
            if event is None:
                return None
            venue_events = connection.execute(
                """SELECT venue, venue_event_id, title, league, status,
                          source_ref, format, fragment_type, first_seen_run_id,
                          last_seen_run_id
                   FROM venue_events WHERE event_id = ?
                   ORDER BY venue, venue_event_id
                   LIMIT ?""",
                (event_id, limits.DETAIL_ROW_LIMIT + 1),
            ).fetchall()
            markets = connection.execute(
                """SELECT canonical.*,
                          COUNT(venue.venue_market_id) AS venue_market_count,
                          GROUP_CONCAT(DISTINCT venue.venue) AS venues
                   FROM canonical_markets canonical
                   LEFT JOIN venue_markets venue
                     ON venue.market_id = canonical.market_id
                    AND venue.market_template_version = canonical.market_template_version
                    AND venue.outcome_space_version = canonical.outcome_space_version
                   WHERE canonical.event_id = ?
                   GROUP BY canonical.market_id, canonical.market_template_version,
                            canonical.outcome_space_version
                   ORDER BY canonical.canonical_class, canonical.market_id
                   LIMIT ?""",
                (event_id, limits.DETAIL_ROW_LIMIT + 1),
            ).fetchall()
            relations = connection.execute(
                f"""SELECT DISTINCT related.relation_type,
                          related.left_claim_id, related.right_claim_id,
                          related.space_shape_id,
                          antecedent.scope AS antecedent_scope,
                          antecedent.coverage AS antecedent_coverage,
                          consequent.scope AS consequent_scope,
                          consequent.coverage AS consequent_coverage
                   FROM market_claims left_member
                   JOIN claim_relations related
                     ON related.left_claim_id = left_member.claim_id
                   JOIN market_claims right_member
                     ON right_member.claim_id = related.right_claim_id
                    AND right_member.event_id = left_member.event_id
                   JOIN claim_classes antecedent
                     ON antecedent.claim_id = related.left_claim_id
                   JOIN claim_classes consequent
                     ON consequent.claim_id = related.right_claim_id
                   WHERE left_member.event_id = ?
                     AND {_current_era('left_member')}
                     AND {_current_era('right_member')}
                   ORDER BY related.relation_type, related.left_claim_id,
                            related.right_claim_id
                   LIMIT ?""",
                (event_id, limits.DETAIL_ROW_LIMIT + 1),
            ).fetchall()
            claims = connection.execute(
                f"""SELECT claim.claim_id, claim.space_shape_id, claim.scope,
                          claim.coverage, claim.outcome_key_count,
                          claim.first_seen_run_id, claim.last_seen_run_id,
                          COUNT(*) AS market_count,
                          COUNT(DISTINCT member.venue) AS venue_count
                   FROM claim_classes claim
                   JOIN market_claims member USING (claim_id)
                   WHERE member.event_id = ?
                     AND {_current_era('member')}
                   GROUP BY claim.claim_id
                   ORDER BY venue_count DESC, claim.claim_id
                   LIMIT ?""",
                (event_id, limits.DETAIL_ROW_LIMIT + 1),
            ).fetchall()
            observations = connection.execute(
                """SELECT observed.run_id, run.generated_at, observed.bundle_id,
                          observed.observed_activation_at
                   FROM event_observations observed
                   JOIN targeter_runs run USING (run_id)
                   WHERE observed.event_id = ?
                   ORDER BY run.generated_at_ns, observed.run_id, observed.bundle_id
                   LIMIT ?""",
                (event_id, limits.DETAIL_ROW_LIMIT + 1),
            ).fetchall()
        _ensure_detail_rows(
            (venue_events, markets, relations, claims, observations), "event detail"
        )
        return _bounded_detail(
            {
                "event": _event_record(event),
                "venue_events": [_row_record(row) for row in venue_events],
                "markets": [_canonical_market_record(row) for row in markets],
                "claims": [_row_record(row) for row in claims],
                "relations": [_row_record(row) for row in relations],
                "observations": [_row_record(row) for row in observations],
            },
            "event detail",
        )
