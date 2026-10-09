"""Bundle, selection, context and outcome reads."""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from typing import Any

from universe.store import limits
from universe.store.records import (
    BundleEventConflict,
    DetailTooLarge,
    EvidenceConflict,
    _SELECTION_QUERY,
    _bounded_detail,
    _canonical_json_value,
    _ensure_detail_rows,
    _limit,
    _nonempty,
    _range,
    _row_record,
    _selection_record,
)


class BundleReads:
    def bundle_outcomes(self, bundle_id: str) -> dict[str, Any] | None:
        """One read transaction; semantic masks never alter catalogue evidence."""
        from universe.claims.outcomes import bundle_document

        with closing(self.connect(readonly=True)) as connection:
            connection.execute("BEGIN")
            # Resolve through selections, never event_observations: that table
            # holds every candidate of every run and has no bundle_id index, so
            # a cold scan outlasts the edge's upstream timeout. Each selection's
            # origin run decided the bundle, and the decision row names its
            # umbrella event by primary key. Only selected bundles resolve,
            # which is every bundle preparation can pin.
            ids = connection.execute(
                """SELECT DISTINCT decision.event_id
                   FROM selection_occurrences occurrence
                   JOIN candidate_decisions decision
                     ON decision.run_id = occurrence.origin_run_id
                    AND decision.bundle_id = occurrence.bundle_id
                   WHERE occurrence.bundle_id = ?
                   LIMIT 2""",
                (bundle_id,),
            ).fetchall()
            if not ids:
                return None
            if len(ids) != 1:
                raise BundleEventConflict("bundle maps to multiple events")
            event_id = ids[0]["event_id"]
            event = dict(connection.execute(
                "SELECT * FROM umbrella_events WHERE event_id = ?", (event_id,)
            ).fetchone())
            event["source_bundle_id"] = bundle_id
            for field in ("participants", "participant_keys"):
                event[field] = json.loads(event.pop(field + "_json"))
            venue_events = [dict(row) for row in connection.execute(
                "SELECT * FROM venue_events WHERE event_id = ? ORDER BY venue, venue_event_id LIMIT ?",
                (event_id, limits.DETAIL_ROW_LIMIT + 1),
            )]
            markets = [dict(row) for row in connection.execute(
                "SELECT * FROM venue_markets WHERE event_id = ? ORDER BY venue, venue_market_id LIMIT ?",
                (event_id, limits.DETAIL_ROW_LIMIT + 1),
            )]
            _ensure_detail_rows((venue_events, markets), "bundle outcomes")
            for market in markets:
                for field in ("parameters", "subscription_ids", "outcome_labels"):
                    market[field] = json.loads(market.pop(field + "_json"))
                market["accepting_orders"] = bool(market["accepting_orders"])

            def matches(venue, native, claim_key, claim_id):
                # EXISTS is indexed and bounded even when a market has many eras.
                row = connection.execute(
                    """SELECT
                       EXISTS(SELECT 1 FROM market_claims WHERE venue=? AND venue_market_id=? AND claim_key=?) AS recorded,
                       EXISTS(SELECT 1 FROM market_claims WHERE venue=? AND venue_market_id=? AND claim_key=? AND claim_id=?) AS matches""",
                    (venue, native, claim_key, venue, native, claim_key, claim_id),
                ).fetchone()
                return not row["recorded"] or bool(row["matches"])

            return _bounded_detail(bundle_document(
                event, venue_events, markets, recorded_claim_matches=matches,
            ), "bundle outcomes")

    def list_selections(
        self,
        *,
        run_id: str | None = None,
        bundle_id: str | None = None,
        venue: str | None = None,
        activation_start_ns: int | None = None,
        activation_end_ns: int | None = None,
        selected_start_ns: int | None = None,
        selected_end_ns: int | None = None,
        sort: str = "activation",
        after: tuple[int, str, str] | None = None,
        limit: int = 100,
    ) -> tuple[list[dict[str, Any]], bool]:
        _range(activation_start_ns, activation_end_ns, "activation")
        _range(selected_start_ns, selected_end_ns, "selected")
        if sort not in {"activation", "selected"}:
            raise ValueError("sort must be activation or selected")
        predicates: list[str] = []
        parameters: list[Any] = []
        for expression, value in (
            ("o.run_id = ?", run_id),
            ("o.bundle_id = ?", bundle_id),
        ):
            if value is not None:
                predicates.append(expression)
                parameters.append(_nonempty(value, expression.split()[0]))
        if venue is not None:
            predicates.append(
                """EXISTS (
                       SELECT 1 FROM context_targets selected
                       WHERE selected.context_sha256 = o.context_sha256
                         AND selected.venue = ?
                   )"""
            )
            parameters.append(_nonempty(venue, "venue"))
        for expression, value in (
            ("c.activation_at_ns >= ?", activation_start_ns),
            ("c.activation_at_ns < ?", activation_end_ns),
            ("r.generated_at_ns >= ?", selected_start_ns),
            ("r.generated_at_ns < ?", selected_end_ns),
        ):
            if value is not None:
                predicates.append(expression)
                parameters.append(value)
        sort_expression = (
            "c.activation_at_ns" if sort == "activation" else "r.generated_at_ns"
        )
        if after is not None:
            predicates.append(f"({sort_expression}, o.run_id, o.bundle_id) > (?, ?, ?)")
            parameters.extend(after)
        where = f"WHERE {' AND '.join(predicates)}" if predicates else ""
        bounded = _limit(limit)
        with closing(self.connect(readonly=True)) as connection:
            rows = connection.execute(
                f"""{_SELECTION_QUERY} {where}
                    ORDER BY {sort_expression}, o.run_id, o.bundle_id LIMIT ?""",
                (*parameters, bounded + 1),
            ).fetchall()
        output = [_selection_record(row) for row in rows[:bounded]]
        return output, len(rows) > bounded

    def list_bundles(
        self,
        *,
        after: tuple[int, str] | None = None,
        limit: int = 100,
    ) -> tuple[list[dict[str, Any]], bool]:
        """Return one newest-context summary per historical bundle."""
        bounded = _limit(limit)
        having = ""
        parameters: list[Any] = []
        if after is not None:
            having = "HAVING (MAX(r.generated_at_ns), o.bundle_id) < (?, ?)"
            parameters.extend(after)
        with closing(self.connect(readonly=True)) as connection:
            rows = connection.execute(
                f"""SELECT
                        o.bundle_id,
                        MIN(r.generated_at) AS first_selected_at,
                        MAX(r.generated_at) AS last_selected_at,
                        MAX(r.generated_at_ns) AS last_selected_at_ns,
                        COUNT(*) AS occurrence_count,
                        EXISTS(
                            SELECT 1 FROM bundle_retirements retired
                            WHERE retired.bundle_id = o.bundle_id
                        ) AS retired,
                        (
                            SELECT newest.context_sha256
                            FROM selection_occurrences newest
                            JOIN targeter_runs newest_run
                              ON newest_run.run_id = newest.run_id
                            WHERE newest.bundle_id = o.bundle_id
                            ORDER BY newest_run.generated_at_ns DESC,
                                     newest.run_id DESC
                            LIMIT 1
                        ) AS context_sha256,
                        (
                            SELECT newest.run_id
                            FROM selection_occurrences newest
                            JOIN targeter_runs newest_run
                              ON newest_run.run_id = newest.run_id
                            WHERE newest.bundle_id = o.bundle_id
                            ORDER BY newest_run.generated_at_ns DESC,
                                     newest.run_id DESC
                            LIMIT 1
                        ) AS latest_run_id
                    FROM selection_occurrences o
                    JOIN targeter_runs r ON r.run_id = o.run_id
                    GROUP BY o.bundle_id
                    {having}
                    ORDER BY last_selected_at_ns DESC, o.bundle_id DESC
                    LIMIT ?""",
                (*parameters, bounded + 1),
            ).fetchall()
            output = []
            for row in rows[:bounded]:
                context = self._context(connection, str(row["context_sha256"]))
                output.append(
                    {
                        "bundle_id": row["bundle_id"],
                        "latest_run_id": row["latest_run_id"],
                        "sport": context["sport"],
                        "game": context["game"],
                        "topology": context["topology"],
                        "participants": context["participants"],
                        "activation_at": context["activation_at"],
                        "capture_start_at": context["capture_start_at"],
                        "first_selected_at": row["first_selected_at"],
                        "last_selected_at": row["last_selected_at"],
                        "occurrence_count": row["occurrence_count"],
                        "venues": sorted(
                            {target["venue"] for target in context["targets"]}
                        ),
                        "target_count": len(context["targets"]),
                        "lifecycle": "retired" if row["retired"] else "active",
                    }
                )
        return output, len(rows) > bounded

    def selection_detail(self, run_id: str, bundle_id: str) -> dict[str, Any] | None:
        with closing(self.connect(readonly=True)) as connection:
            row = connection.execute(
                f"{_SELECTION_QUERY} WHERE o.run_id = ? AND o.bundle_id = ?",
                (run_id, bundle_id),
            ).fetchone()
            if row is None:
                return None
            context = self._context(connection, row["context_sha256"])
        return _bounded_detail(
            {**_selection_record(row), "context": context}, "selection detail"
        )

    def _context(
        self,
        connection: sqlite3.Connection,
        context_sha256: str,
        *,
        bounded: bool = True,
    ) -> dict[str, Any]:
        """Rebuild one bundle context, optionally under the API response limits.

        The row and byte limits exist so a single HTTP response stays inside the
        response budget; they are presentation limits. Ingestion must never fail
        because a record would be awkward to serve, so the write path reads the
        whole context with ``bounded=False`` and only the API keeps the bound.
        """
        # SQLite treats a negative LIMIT as unbounded.
        row_limit = limits.DETAIL_ROW_LIMIT + 1 if bounded else -1
        context = connection.execute(
            "SELECT * FROM bundle_contexts WHERE context_sha256 = ?",
            (context_sha256,),
        ).fetchone()
        if context is None:
            raise EvidenceConflict(f"bundle context {context_sha256} is absent")
        participants = connection.execute(
            """SELECT position, name, participant_key FROM context_participants
               WHERE context_sha256 = ? ORDER BY position LIMIT ?""",
            (context_sha256, row_limit),
        ).fetchall()
        events = connection.execute(
            """SELECT event_ref FROM context_events
               WHERE context_sha256 = ? ORDER BY event_ref LIMIT ?""",
            (context_sha256, row_limit),
        ).fetchall()
        markets = connection.execute(
            """SELECT target_id, venue, selected FROM context_markets
               WHERE context_sha256 = ? ORDER BY target_id LIMIT ?""",
            (context_sha256, row_limit),
        ).fetchall()
        targets = connection.execute(
            """SELECT target_id, venue, canonical_class, source_ref
               FROM context_targets WHERE context_sha256 = ?
               ORDER BY venue, target_id LIMIT ?""",
            (context_sha256, row_limit),
        ).fetchall()
        assets = connection.execute(
            """SELECT target_id, asset_id FROM context_target_assets
               WHERE context_sha256 = ? ORDER BY target_id, asset_id LIMIT ?""",
            (context_sha256, row_limit),
        ).fetchall()
        assets_by_target: dict[str, list[str]] = {}
        for asset in assets:
            assets_by_target.setdefault(str(asset["target_id"]), []).append(
                str(asset["asset_id"])
            )
        target_records: list[dict[str, Any]] = []
        for target in targets:
            target_records.append(
                {
                    **_row_record(target),
                    "subscription_ids": assets_by_target.get(
                        str(target["target_id"]), []
                    ),
                }
            )
        relationships = connection.execute(
            """SELECT left_market AS left, right_market AS right,
                      relationship, scope, left_venue, right_venue, coverage
               FROM context_relationships WHERE context_sha256 = ?
               ORDER BY relationship_index LIMIT ?""",
            (context_sha256, row_limit),
        ).fetchall()
        if bounded:
            _ensure_detail_rows(
                (participants, events, markets, targets, assets, relationships),
                "bundle context",
            )
        record = {
            "bundle_id": context["bundle_id"],
            "sport": context["sport"],
            "game": context["game"],
            "topology": context["topology"],
            "participants": [value["name"] for value in participants],
            "participant_keys": [value["participant_key"] for value in participants],
            "activation_at": context["activation_at"],
            "capture_start_at": context["capture_start_at"],
            "event_refs": [value["event_ref"] for value in events],
            "markets": [
                {
                    "target_id": value["target_id"],
                    "venue": value["venue"],
                    "selected": bool(value["selected"]),
                }
                for value in markets
            ],
            "targets": target_records,
            "relationships": [_row_record(value) for value in relationships],
        }
        if bounded and (
            len(_canonical_json_value(record).encode("utf-8"))
            > limits.EVENT_UNIVERSE_RESPONSE_BUDGET_BYTES
        ):
            raise DetailTooLarge("bundle context exceeds the byte limit")
        return record

    def origin_context(
        self,
        *,
        run_id: str,
        bundle_id: str,
        manifest_key: str,
        manifest_sha256: str,
        report_sha256: str,
    ) -> dict[str, Any] | None:
        with closing(self.connect(readonly=True)) as connection:
            row = connection.execute(
                """SELECT o.context_sha256, o.occurrence_kind, o.origin_run_id,
                          r.manifest_key, r.manifest_sha256, r.report_sha256
                   FROM selection_occurrences o
                   JOIN targeter_runs r USING (run_id)
                   WHERE o.run_id = ? AND o.bundle_id = ?""",
                (run_id, bundle_id),
            ).fetchone()
            if row is None:
                return None
            if (
                row["occurrence_kind"] != "complete"
                or row["origin_run_id"] != run_id
                or row["manifest_key"] != manifest_key
                or row["manifest_sha256"] != manifest_sha256
                or row["report_sha256"] != report_sha256
            ):
                raise EvidenceConflict(
                    f"bundle {bundle_id} origin {run_id} conflicts with indexed evidence"
                )
            return self._context(connection, row["context_sha256"], bounded=False)
