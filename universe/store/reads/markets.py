"""Canonical market reads."""

from __future__ import annotations

from contextlib import closing
from typing import Any

from universe.store import limits
from universe.store.records import (
    _bounded_detail,
    _canonical_market_record,
    _current_era,
    _ensure_detail_rows,
    _row_record,
    _venue_market_record,
)


class MarketReads:
    def market_detail(
        self,
        market_id: str,
        *,
        market_template_version: int | None = None,
        outcome_space_version: int | None = None,
    ) -> dict[str, Any] | None:
        predicates = ["canonical.market_id = ?"]
        parameters: list[Any] = [market_id]
        for field, value in (
            ("market_template_version", market_template_version),
            ("outcome_space_version", outcome_space_version),
        ):
            if value is not None:
                if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                    raise ValueError(f"{field} must be a positive integer")
                predicates.append(f"canonical.{field} = ?")
                parameters.append(value)
        with closing(self.connect(readonly=True)) as connection:
            market = connection.execute(
                f"""SELECT canonical.*,
                          COUNT(venue.venue_market_id) AS venue_market_count,
                          GROUP_CONCAT(DISTINCT venue.venue) AS venues
                    FROM canonical_markets canonical
                    LEFT JOIN venue_markets venue
                      ON venue.market_id = canonical.market_id
                     AND venue.market_template_version = canonical.market_template_version
                     AND venue.outcome_space_version = canonical.outcome_space_version
                    WHERE {' AND '.join(predicates)}
                    GROUP BY canonical.market_id, canonical.market_template_version,
                             canonical.outcome_space_version
                    ORDER BY canonical.market_template_version DESC,
                             canonical.outcome_space_version DESC LIMIT 1""",
                parameters,
            ).fetchone()
            if market is None:
                return None
            key = (
                market["market_id"], market["market_template_version"],
                market["outcome_space_version"],
            )
            venue_markets = connection.execute(
                """SELECT * FROM venue_markets
                   WHERE market_id = ? AND market_template_version = ?
                     AND outcome_space_version = ?
                   ORDER BY venue, venue_market_id
                   LIMIT ?""",
                (*key, limits.DETAIL_ROW_LIMIT + 1),
            ).fetchall()
            selections = connection.execute(
                """SELECT selected.run_id, run.generated_at, selected.bundle_id,
                          selected.venue, selected.venue_market_id,
                          selected.continuity_score, selected.selection_reason,
                          selected.origin_run_id
                   FROM selected_market_occurrences selected
                   JOIN targeter_runs run USING (run_id)
                   WHERE selected.market_id = ?
                     AND selected.market_template_version = ?
                     AND selected.outcome_space_version = ?
                   ORDER BY run.generated_at_ns, selected.run_id,
                            selected.venue, selected.venue_market_id
                   LIMIT ?""",
                (*key, limits.DETAIL_ROW_LIMIT + 1),
            ).fetchall()
            # Scoped to the market's own event, like the claim summaries below.
            # `claim_relations` is global, so joining on claim alone would
            # return every edge the claim takes part in anywhere.
            relations = connection.execute(
                f"""SELECT DISTINCT related.relation_type,
                          related.left_claim_id, related.right_claim_id,
                          related.space_shape_id,
                          antecedent.scope AS antecedent_scope,
                          antecedent.coverage AS antecedent_coverage,
                          consequent.scope AS consequent_scope,
                          consequent.coverage AS consequent_coverage
                   FROM market_claims member
                   JOIN claim_relations related
                     ON related.left_claim_id = member.claim_id
                     OR related.right_claim_id = member.claim_id
                   JOIN market_claims counterpart
                     ON counterpart.event_id = member.event_id
                    AND counterpart.claim_id = CASE
                          WHEN related.left_claim_id = member.claim_id
                          THEN related.right_claim_id
                          ELSE related.left_claim_id
                        END
                   JOIN claim_classes antecedent
                     ON antecedent.claim_id = related.left_claim_id
                   JOIN claim_classes consequent
                     ON consequent.claim_id = related.right_claim_id
                   WHERE member.event_id = ?
                     AND member.claim_id IN (
                         SELECT scoped.claim_id
                         FROM venue_markets venue
                         JOIN market_claims scoped
                           ON scoped.venue = venue.venue
                          AND scoped.venue_market_id = venue.venue_market_id
                         WHERE venue.market_id = ?
                           AND venue.market_template_version = ?
                           AND venue.outcome_space_version = ?
                           AND {_current_era('scoped')}
                     )
                     AND {_current_era('member')}
                     AND {_current_era('counterpart')}
                   ORDER BY related.relation_type, related.left_claim_id,
                            related.right_claim_id
                   LIMIT ?""",
                (market["event_id"], *key, limits.DETAIL_ROW_LIMIT + 1),
            ).fetchall()
            # Counts are event-scoped in both this response and event detail,
            # so one claim summary means the same thing wherever it appears.
            claims = connection.execute(
                f"""SELECT claim.claim_id, claim.space_shape_id, claim.scope,
                          claim.coverage, claim.outcome_key_count,
                          claim.first_seen_run_id, claim.last_seen_run_id,
                          COUNT(*) AS market_count,
                          COUNT(DISTINCT scoped.venue) AS venue_count
                   FROM claim_classes claim
                   JOIN market_claims scoped USING (claim_id)
                   WHERE scoped.event_id = ?
                     AND claim.claim_id IN (
                         SELECT member.claim_id
                         FROM venue_markets venue
                         JOIN market_claims member
                           ON member.venue = venue.venue
                          AND member.venue_market_id = venue.venue_market_id
                         WHERE venue.market_id = ?
                           AND venue.market_template_version = ?
                           AND venue.outcome_space_version = ?
                           AND {_current_era('member')}
                     )
                     AND {_current_era('scoped')}
                   GROUP BY claim.claim_id
                   ORDER BY venue_count DESC, claim.claim_id
                   LIMIT ?""",
                (market["event_id"], *key, limits.DETAIL_ROW_LIMIT + 1),
            ).fetchall()
        _ensure_detail_rows(
            (venue_markets, selections, relations, claims), "market detail"
        )
        return _bounded_detail(
            {
                "market": _canonical_market_record(market),
                "venue_markets": [_venue_market_record(row) for row in venue_markets],
                "selections": [_row_record(row) for row in selections],
                "claims": [_row_record(row) for row in claims],
                "relations": [_row_record(row) for row in relations],
            },
            "market detail",
        )
