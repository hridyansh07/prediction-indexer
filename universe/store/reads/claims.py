"""Claim reads."""

from __future__ import annotations

from contextlib import closing
from typing import Any

from universe.store import limits
from universe.store.records import (
    _bounded_detail,
    _claim_identifier,
    _current_era,
    _ensure_detail_rows,
    _limit,
    _row_record,
)


class ClaimReads:
    def claim_detail(self, claim_id: str) -> dict[str, Any] | None:
        """One claim: how far it reaches, and what it relates to.

        A claim is global, so the markets expressing it grow with the market
        universe -- every best-of-3 event contributes its own moneyline markets
        to one "home wins a best-of-3" claim. That set is unbounded in the same
        way the per-run observation list was, just on a slower axis, so it is
        counted here and paged through ``claim_markets`` rather than inlined.

        Bounds are targeter observation bounds, not lifecycle. A market's
        ``last_seen_run_id`` is the last run in which the targeter observed it
        expressing this claim; absence afterwards may mean the market settled,
        was delisted, or simply stopped being a candidate, and Universe cannot
        distinguish those without the venue.
        """
        identifier = _claim_identifier(claim_id)
        with closing(self.connect(readonly=True)) as connection:
            claim = connection.execute(
                "SELECT * FROM claim_classes WHERE claim_id = ?", (identifier,)
            ).fetchone()
            if claim is None:
                return None
            counts = connection.execute(
                f"""SELECT COUNT(*) AS market_count,
                          COUNT(DISTINCT member.venue) AS venue_count,
                          COUNT(DISTINCT member.event_id) AS event_count
                   FROM market_claims member
                   WHERE member.claim_id = ? AND {_current_era('member')}""",
                (identifier,),
            ).fetchone()
            relations = connection.execute(
                """SELECT space_shape_id, left_claim_id, right_claim_id,
                          relation_type
                   FROM claim_relations
                   WHERE left_claim_id = ? OR right_claim_id = ?
                   ORDER BY relation_type, left_claim_id, right_claim_id
                   LIMIT ?""",
                (identifier, identifier, limits.DETAIL_ROW_LIMIT + 1),
            ).fetchall()
        _ensure_detail_rows((relations,), "claim detail")
        return _bounded_detail(
            {
                "claim": _row_record(claim),
                "counts": {
                    "markets": int(counts["market_count"]),
                    "venues": int(counts["venue_count"]),
                    "events": int(counts["event_count"]),
                },
                "relations": [_row_record(row) for row in relations],
            },
            "claim detail",
        )

    def claim_exists(self, claim_id: str) -> bool:
        """Whether a claim is known, without building its detail.

        The paged markets route needs only this; calling `claim_detail` would
        re-run its counts aggregate and relations query on every page.
        """
        identifier = _claim_identifier(claim_id)
        with closing(self.connect(readonly=True)) as connection:
            return (
                connection.execute(
                    "SELECT 1 FROM claim_classes WHERE claim_id = ?", (identifier,)
                ).fetchone()
                is not None
            )

    def claim_markets(
        self,
        claim_id: str,
        *,
        after: tuple[str, str, str] | None = None,
        limit: int = 100,
    ) -> tuple[list[dict[str, Any]], bool]:
        """The markets expressing one claim, paged.

        Only each market's current era: a market whose semantics changed opens a
        new era rather than rewriting the old row, and only the newest describes
        it now.
        """
        identifier = _claim_identifier(claim_id)
        bounded = _limit(limit)
        where = ""
        parameters: list[Any] = []
        if after is not None:
            where = "AND (member.venue, member.venue_market_id, member.claim_key) > (?, ?, ?)"
            parameters.extend(after)
        with closing(self.connect(readonly=True)) as connection:
            rows = connection.execute(
                f"""SELECT member.venue, member.venue_market_id, member.claim_key,
                          member.event_id, member.first_seen_run_id,
                          member.last_seen_run_id, venue.market_id,
                          venue.market_template_version, venue.outcome_space_version,
                          venue.canonical_class, venue.title
                   FROM market_claims member
                   JOIN venue_markets venue USING (venue, venue_market_id)
                   WHERE member.claim_id = ? {where}
                     AND {_current_era('member')}
                   ORDER BY member.venue, member.venue_market_id, member.claim_key
                   LIMIT ?""",
                (identifier, *parameters, bounded + 1),
            ).fetchall()
        return [_row_record(row) for row in rows[:bounded]], len(rows) > bounded
