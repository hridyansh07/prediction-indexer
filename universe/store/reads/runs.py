"""Targeter run reads: status, details, audits and listings."""

from __future__ import annotations

import json
import sqlite3
import time
from contextlib import closing
from typing import Any

from universe.derive.projection import PROJECTION_VERSION
from universe.store import limits
from universe.store.records import (
    DetailTooLarge,
    _bounded_detail,
    _context_sha256,
    _ensure_detail_rows,
    _event_record,
    _event_refs_sql,
    _isoformat_ns,
    _limit,
    _range,
    _records_sha256,
    _row_record,
    _run_record,
    _run_summary,
)


class RunReads:
    def audit_run(self, run_id: str) -> dict[str, Any] | None:
        with closing(self.connect(readonly=True)) as connection:
            exists = connection.execute(
                "SELECT 1 FROM targeter_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            return None if exists is None else self._audit_run(connection, run_id)

    def _audit_run(
        self, connection: sqlite3.Connection, run_id: str
    ) -> dict[str, Any]:
        run = connection.execute(
            """SELECT projection_version, projection_sha256,
                      projection_row_count
               FROM targeter_runs WHERE run_id = ?""",
            (run_id,),
        ).fetchone()
        assert run is not None
        rows = connection.execute(
            """SELECT bundle_id, context_sha256, occurrence_kind,
                      origin_run_id, continuity_selected,
                      continuity_disposition
               FROM selection_occurrences
               WHERE run_id = ? ORDER BY bundle_id""",
            (run_id,),
        ).fetchall()
        retirement_rows = connection.execute(
            """SELECT bundle_id, context_sha256, origin_run_id, disposition
               FROM bundle_retirements
               WHERE run_id = ? ORDER BY bundle_id""",
            (run_id,),
        ).fetchall()
        entries: list[dict[str, Any]] = []
        context_ok = True
        for row in rows:
            context = self._context(connection, row["context_sha256"])
            actual_context_sha256 = _context_sha256(context)
            context_ok = context_ok and actual_context_sha256 == row["context_sha256"]
            entries.append(
                {
                    "bundle_id": row["bundle_id"],
                    "context_sha256": actual_context_sha256,
                    "occurrence_kind": row["occurrence_kind"],
                    "origin_run_id": row["origin_run_id"],
                    "continuity_selected": bool(row["continuity_selected"]),
                    "continuity_disposition": row["continuity_disposition"],
                }
            )
        for row in retirement_rows:
            context = self._context(connection, row["context_sha256"])
            actual_context_sha256 = _context_sha256(context)
            context_ok = context_ok and actual_context_sha256 == row["context_sha256"]
            entries.append(
                {
                    "bundle_id": row["bundle_id"],
                    "context_sha256": actual_context_sha256,
                    "origin_run_id": row["origin_run_id"],
                    "retirement_disposition": row["disposition"],
                }
            )
        actual_sha256 = _records_sha256(entries)
        ok = (
            run["projection_version"] == PROJECTION_VERSION
            and run["projection_row_count"] == len(entries)
            and run["projection_sha256"] == actual_sha256
            and context_ok
        )
        return {
            "run_id": run_id,
            "ok": ok,
            "projection_version": run["projection_version"],
            "stored_sha256": run["projection_sha256"],
            "actual_sha256": actual_sha256,
            "stored_row_count": run["projection_row_count"],
            "actual_row_count": len(entries),
            "selection_row_count": len(rows),
            "retirement_row_count": len(retirement_rows),
            "contexts_ok": context_ok,
        }

    def targeter_status_snapshot(
        self, *, limit: int = 5, now_ns: int | None = None
    ) -> dict[str, Any]:
        """Return the bounded newest-run projection used by live UI views."""
        _limit(limit)
        observed_ns = now_ns if now_ns is not None else time.time_ns()
        with closing(self.connect(readonly=True)) as connection:
            latest = connection.execute(
                """SELECT * FROM targeter_runs
                   ORDER BY generated_at_ns DESC, run_id DESC LIMIT 1"""
            ).fetchone()
            complete = connection.execute(
                """SELECT * FROM targeter_runs WHERE input_complete = 1
                   ORDER BY generated_at_ns DESC, run_id DESC LIMIT 1"""
            ).fetchone()

        latest_record = _run_summary(latest) if latest is not None else None
        age_seconds = (
            max(0, (observed_ns - int(latest["generated_at_ns"])) // 1_000_000_000)
            if latest is not None
            else None
        )
        current_record = _run_summary(complete) if complete is not None else None
        with closing(self.connect(readonly=True)) as connection:
            summary = (
                connection.execute(
                    """SELECT COUNT(DISTINCT bundle_id) AS selected_bundles,
                              COUNT(*) AS selected_targets
                       FROM selected_market_occurrences WHERE run_id = ?""",
                    (complete["run_id"],),
                ).fetchone()
                if complete is not None
                else None
            )
            venues = (
                connection.execute(
                    """SELECT DISTINCT venue FROM selected_market_occurrences
                       WHERE run_id = ? ORDER BY venue""",
                    (complete["run_id"],),
                ).fetchall()
                if complete is not None
                else []
            )
        return {
            "status_projection_version": 1,
            "observed_at": _isoformat_ns(observed_ns),
            "freshness": {
                "state": (
                    "unavailable"
                    if latest is None
                    else "late"
                    if age_seconds is not None
                    and age_seconds >= limits.TARGETER_RUN_INTERVAL_SECONDS * 2
                    else "current"
                ),
                "expected_run_seconds": limits.TARGETER_RUN_INTERVAL_SECONDS,
                "latest_run_age_seconds": age_seconds,
                "latest_indexed_at": (
                    latest_record["indexed_at"] if latest_record is not None else None
                ),
            },
            "latest_run": latest_record,
            "current_complete_run": current_record,
            "current_complete_summary": {
                "selected_bundles": int(summary["selected_bundles"] or 0) if summary else 0,
                "selected_targets": int(summary["selected_targets"] or 0) if summary else 0,
                "venues": [row["venue"] for row in venues],
            },
        }

    def targeter_run_detail(self, run_id: str) -> dict[str, Any] | None:
        """Return bounded run decisions with references to normalized detail."""
        with closing(self.connect(readonly=True)) as connection:
            run = connection.execute(
                "SELECT * FROM targeter_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if run is None:
                return None
            decision_bytes = connection.execute(
                """SELECT COALESCE(SUM(
                          length(CAST(event_id AS BLOB)) +
                          length(CAST(bundle_id AS BLOB)) +
                          length(CAST(score_components_json AS BLOB)) +
                          length(CAST(rejection_reasons_json AS BLOB)) +
                          length(CAST(COALESCE(allocation_rejection, '') AS BLOB)) +
                          length(CAST(admission_json AS BLOB)) +
                          length(CAST(market_exclusions_json AS BLOB)) +
                          length(CAST(eligible_market_ids_json AS BLOB))
                       ), 0)
                   FROM candidate_decisions WHERE run_id = ?""",
                (run_id,),
            ).fetchone()[0]
            if int(decision_bytes) > limits.EVENT_UNIVERSE_RESPONSE_BUDGET_BYTES:
                raise DetailTooLarge("targeter run detail exceeds the byte limit")
            decisions = connection.execute(
                """SELECT event_id, bundle_id, eligible, selected, score,
                          score_components_json, rejection_reasons_json,
                          allocation_rejection, admission_json,
                          market_exclusions_json, eligible_market_ids_json
                   FROM candidate_decisions WHERE run_id = ? ORDER BY bundle_id
                   LIMIT ?""",
                (run_id, limits.DETAIL_ROW_LIMIT + 1),
            ).fetchall()
            selected = connection.execute(
                """SELECT s.event_id, s.bundle_id, s.venue, s.venue_market_id,
                          s.market_id, s.market_template_version,
                          s.outcome_space_version, s.canonical_class,
                          s.continuity_score, s.selection_reason, s.origin_run_id
                   FROM selected_market_occurrences s
                   JOIN venue_markets v USING (venue, venue_market_id)
                   WHERE s.run_id = ?
                   ORDER BY s.bundle_id, s.venue, s.venue_market_id
                   LIMIT ?""",
                (run_id, limits.DETAIL_ROW_LIMIT + 1),
            ).fetchall()
            events = connection.execute(
                f"""SELECT DISTINCT event.*,
                          {_event_refs_sql('event')} AS event_refs_json
                   FROM umbrella_events event
                   JOIN event_observations observed USING (event_id)
                   WHERE observed.run_id = ?
                   ORDER BY event.activation_at_ns, event.event_id
                   LIMIT ?""",
                (run_id, limits.DETAIL_ROW_LIMIT + 1),
            ).fetchall()
        _ensure_detail_rows(
            (decisions, selected, events), "targeter run detail"
        )
        return _bounded_detail(
            {
                "run": _run_summary(run),
                "source": {
                    "manifest_key": run["manifest_key"],
                    "manifest_sha256": run["manifest_sha256"],
                    "report_key": run["report_key"],
                    "report_sha256": run["report_sha256"],
                },
                "counts": {
                    "candidates": len(decisions),
                    "eligible": sum(bool(row["eligible"]) for row in decisions),
                    "selected_events": len({row["event_id"] for row in selected}),
                    "selected_markets": len(selected),
                },
                "decisions": [
                    {
                        "event_id": row["event_id"],
                        "bundle_id": row["bundle_id"],
                        "eligible": bool(row["eligible"]),
                        "selected": bool(row["selected"]),
                        "score": row["score"],
                        "score_components": json.loads(row["score_components_json"]),
                        "rejection_reasons": json.loads(row["rejection_reasons_json"]),
                        "allocation_rejection": row["allocation_rejection"],
                        "admission": json.loads(row["admission_json"]),
                        "market_exclusions": json.loads(row["market_exclusions_json"]),
                        "eligible_market_ids": json.loads(
                            row["eligible_market_ids_json"]
                        ),
                    }
                    for row in decisions
                ],
                "events": [_event_record(row) for row in events],
                "selected_markets": [_row_record(row) for row in selected],
            },
            "targeter run detail",
        )

    def list_runs(
        self,
        *,
        generated_start_ns: int | None = None,
        generated_end_ns: int | None = None,
        input_complete: bool | None = None,
        after: tuple[int, str] | None = None,
        limit: int = 100,
    ) -> tuple[list[dict[str, Any]], bool]:
        _range(generated_start_ns, generated_end_ns, "generated")
        predicates: list[str] = []
        parameters: list[Any] = []
        if generated_start_ns is not None:
            predicates.append("generated_at_ns >= ?")
            parameters.append(generated_start_ns)
        if generated_end_ns is not None:
            predicates.append("generated_at_ns < ?")
            parameters.append(generated_end_ns)
        if input_complete is not None:
            if not isinstance(input_complete, bool):
                raise ValueError("input_complete must be boolean")
            predicates.append("input_complete = ?")
            parameters.append(int(input_complete))
        if after is not None:
            predicates.append("(generated_at_ns, run_id) > (?, ?)")
            parameters.extend(after)
        where = f"WHERE {' AND '.join(predicates)}" if predicates else ""
        bounded = _limit(limit)
        with closing(self.connect(readonly=True)) as connection:
            rows = connection.execute(
                f"""SELECT * FROM targeter_runs {where}
                    ORDER BY generated_at_ns, run_id LIMIT ?""",
                (*parameters, bounded + 1),
            ).fetchall()
        output = [_run_record(row) for row in rows[:bounded]]
        return output, len(rows) > bounded

    def run_detail(self, run_id: str) -> dict[str, Any] | None:
        with closing(self.connect(readonly=True)) as connection:
            row = connection.execute(
                "SELECT * FROM targeter_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if row is None:
                return None
            record = _run_record(row)
            record["audit"] = self._audit_run(connection, run_id)
            return record
