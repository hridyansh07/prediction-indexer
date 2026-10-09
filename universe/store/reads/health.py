"""Health and freshness status."""

from __future__ import annotations

import time
from contextlib import closing
from typing import Any

from universe.store import limits


class HealthReads:
    def status(self, *, now_ns: int | None = None) -> dict[str, Any]:
        observed_ns = (
            now_ns
            if now_ns is not None
            else time.time_ns()
        )
        with closing(self.connect(readonly=True)) as connection:
            version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            counts = {
                table: int(
                    connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                )
                for table in (
                    "targeter_runs",
                    "selection_occurrences",
                    "bundle_retirements",
                    "bundle_contexts",
                    "context_targets",
                    "umbrella_events",
                    "canonical_markets",
                    "venue_markets",
                    "claim_classes",
                )
            }
            latest = connection.execute(
                """SELECT run_id, generated_at, generated_at_ns,
                          indexed_at_ns, input_complete
                   FROM targeter_runs
                   ORDER BY generated_at_ns DESC, run_id DESC LIMIT 1"""
            ).fetchone()
            # Claim coverage the recomputation did not reach. Reported beside
            # pending failures so a false negative stays visible rather than
            # looking like ordinary absence, per AGENTS.md's no-silent-loss rule.
            claims_row = connection.execute(
                """SELECT COALESCE(SUM(claim_relation_shortfall), 0),
                          COALESCE(SUM(unreconstructed_bundles), 0),
                          COUNT(*) FILTER (
                              WHERE claim_relation_shortfall > 0
                                 OR unreconstructed_bundles > 0
                          )
                   FROM universe_run_projections"""
            ).fetchone()
            pending_failures = int(
                connection.execute(
                    "SELECT COUNT(*) FROM universe_sync_failures"
                ).fetchone()[0]
            )
        latest_record = None
        if latest is not None:
            age_seconds = max(
                0, (observed_ns - int(latest["generated_at_ns"])) // 1_000_000_000
            )
            latest_record = {
                "run_id": latest["run_id"],
                "generated_at": latest["generated_at"],
                "indexed_at_ns": latest["indexed_at_ns"],
                "input_complete": bool(latest["input_complete"]),
                "age_seconds": age_seconds,
                "stale_after_seconds": limits.STALE_AFTER_SECONDS,
                "stale": age_seconds >= limits.STALE_AFTER_SECONDS,
            }
        return {
            "status": "degraded" if pending_failures else "ok",
            "schema_version": version,
            "latest_run": latest_record,
            "counts": counts,
            "sync": {"pending_failures": pending_failures},
            "claim_coverage": {
                "relation_shortfall": int(claims_row[0]),
                "unreconstructed_bundles": int(claims_row[1]),
                "runs_with_shortfall": int(claims_row[2]),
            },
        }
