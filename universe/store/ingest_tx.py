"""The admitted-run write transaction and sync bookkeeping."""

from __future__ import annotations

import sqlite3
import time
from contextlib import closing
from typing import Any, Iterable, Mapping

from archive.storage.base import normalize_key
from universe.claims.claim_projection import project_claims
from universe.derive.event_identity import EventIdentityError, resolve_market_projection
from universe.derive.market_projection import MARKET_PROJECTION_VERSION, MarketProjectionError
from universe.derive.projection import PROJECTION_VERSION
from universe.store.connection import REBUILD_INSTRUCTION
from universe.store.records import (
    EvidenceConflict,
    _canonical_json_value,
    _canonical_timestamp,
    _context_sha256,
    _market_projection,
    _market_projection_identity,
    _nonempty,
    _occurrence,
    _projection_entry,
    _records_sha256,
    _require_columns,
    _retirement,
    _retirement_projection_entry,
    _row_record,
    _run_is_newer,
    _seen_run_ids,
    _sha256,
    _sync_failure_range,
    _timestamp_ns,
    _venue_prefix,
)


class IngestTransactions:
    def known_manifest(self, key: str, sha256: str) -> bool:
        with closing(self.connect(readonly=True)) as connection:
            row = connection.execute(
                "SELECT manifest_sha256 FROM targeter_runs WHERE manifest_key = ?",
                (key,),
            ).fetchone()
        if row is None:
            return False
        if row["manifest_sha256"] != sha256:
            raise EvidenceConflict(f"immutable manifest {key!r} changed identity")
        return True

    def latest_run(self) -> dict[str, Any] | None:
        with closing(self.connect(readonly=True)) as connection:
            row = connection.execute(
                """SELECT run_id, generated_at, generated_at_ns, input_complete,
                          manifest_key, manifest_sha256
                   FROM targeter_runs
                   ORDER BY generated_at_ns DESC, run_id DESC LIMIT 1"""
            ).fetchone()
        if row is None:
            return None
        record = _row_record(row)
        record["input_complete"] = bool(record["input_complete"])
        return record

    def run_source(self, run_id: str) -> dict[str, Any] | None:
        """Return source identity without constructing or auditing run detail."""
        with closing(self.connect(readonly=True)) as connection:
            row = connection.execute(
                """SELECT run_id, input_complete, manifest_key, manifest_sha256,
                          report_key, report_sha256
                   FROM targeter_runs WHERE run_id = ?""",
                (run_id,),
            ).fetchone()
        if row is None:
            return None
        record = _row_record(row)
        record["input_complete"] = bool(record["input_complete"])
        return record

    def ingest_run(
        self,
        *,
        run_id: str,
        generated_at: str,
        input_complete: bool,
        report_version: int,
        strategy_version: int,
        manifest_key: str,
        manifest_sha256: str,
        manifest_byte_length: int,
        report_key: str,
        report_sha256: str,
        report_byte_length: int,
        report_decoded_sha256: str,
        report_decoded_byte_length: int,
        market_projection: Mapping[str, Any],
        occurrences: Iterable[Mapping[str, Any]],
        retirements: Iterable[Mapping[str, Any]],
        identity_backfill: bool = False,
    ) -> str:
        """Atomically append one verified run and its lifecycle projection."""
        run_id = _nonempty(run_id, "run_id")
        generated_at = _canonical_timestamp(generated_at, "generated_at")
        if not isinstance(input_complete, bool):
            raise EvidenceConflict("input_complete must be boolean")
        if report_version != 3:
            raise EvidenceConflict("Event Universe accepts only Targeter report v3")
        if (
            not isinstance(strategy_version, int)
            or isinstance(strategy_version, bool)
            or strategy_version <= 0
        ):
            raise EvidenceConflict("strategy_version must be a positive integer")
        for key, label in (
            (manifest_key, "manifest_key"),
            (report_key, "report_key"),
        ):
            try:
                normalize_key(key)
            except ValueError as error:
                raise EvidenceConflict(f"{label} is invalid") from error
        for digest, label in (
            (manifest_sha256, "manifest_sha256"),
            (report_sha256, "report_sha256"),
            (report_decoded_sha256, "report_decoded_sha256"),
        ):
            _sha256(digest, label)
        for length, label in (
            (manifest_byte_length, "manifest_byte_length"),
            (report_byte_length, "report_byte_length"),
            (report_decoded_byte_length, "report_decoded_byte_length"),
        ):
            if not isinstance(length, int) or isinstance(length, bool) or length <= 0:
                raise EvidenceConflict(f"{label} must be a positive integer")

        normalized = [
            _occurrence(dict(value), expected_run_id=run_id) for value in occurrences
        ]
        normalized.sort(key=lambda value: value["bundle_id"])
        normalized_retirements = [
            _retirement(dict(value), expected_run_id=run_id) for value in retirements
        ]
        normalized_retirements.sort(key=lambda value: value["bundle_id"])
        bundle_ids = [value["bundle_id"] for value in normalized]
        retired_bundle_ids = [
            value["bundle_id"] for value in normalized_retirements
        ]
        if len(bundle_ids) != len(set(bundle_ids)):
            raise EvidenceConflict(f"run {run_id} repeats a selected bundle")
        if len(retired_bundle_ids) != len(set(retired_bundle_ids)):
            raise EvidenceConflict(f"run {run_id} repeats a retired bundle")
        if set(bundle_ids) & set(retired_bundle_ids):
            raise EvidenceConflict(f"run {run_id} selects and retires the same bundle")
        if not input_complete and (normalized or normalized_retirements):
            raise EvidenceConflict("incomplete Targeter runs cannot admit lifecycle rows")
        projection_entries = [
            *(_projection_entry(value) for value in normalized),
            *(
                _retirement_projection_entry(value)
                for value in normalized_retirements
            ),
        ]
        projection_sha256 = _records_sha256(projection_entries)
        generated_at_ns = _timestamp_ns(generated_at)
        market_projection = _market_projection(
            market_projection, expected_run_id=run_id, expected_generated_at=generated_at
        )
        expected = {
            "run_id": run_id,
            "generated_at": generated_at,
            "generated_at_ns": generated_at_ns,
            "input_complete": int(input_complete),
            "report_version": report_version,
            "strategy_version": strategy_version,
            "manifest_key": manifest_key,
            "manifest_sha256": manifest_sha256,
            "manifest_byte_length": manifest_byte_length,
            "report_key": report_key,
            "report_sha256": report_sha256,
            "report_byte_length": report_byte_length,
            "report_decoded_sha256": report_decoded_sha256,
            "report_decoded_byte_length": report_decoded_byte_length,
            "projection_version": PROJECTION_VERSION,
            "projection_sha256": projection_sha256,
            "projection_row_count": len(projection_entries),
        }

        with self.write_transaction() as connection:
            lineage = connection.execute(
                "SELECT state FROM event_identity_lineage WHERE singleton = 1"
            ).fetchone()
            if (
                lineage is not None
                and lineage["state"] == "running"
                and not identity_backfill
            ):
                raise EvidenceConflict(
                    "canonical event-identity backfill is running; "
                    "incremental ingestion is blocked"
                )
            try:
                resolved_market_projection = resolve_market_projection(
                    connection, market_projection
                )
            except EventIdentityError as error:
                raise EvidenceConflict(str(error)) from error
            market_sha256, market_row_count = _market_projection_identity(
                resolved_market_projection
            )
            existing = connection.execute(
                "SELECT * FROM targeter_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if existing is not None:
                if any(existing[field] != value for field, value in expected.items()):
                    raise EvidenceConflict(
                        f"Targeter run {run_id} conflicts with prior ingestion"
                    )
                projected = connection.execute(
                    """SELECT projection_version, projection_sha256,
                              projection_row_count
                       FROM universe_run_projections WHERE run_id = ?""",
                    (run_id,),
                ).fetchone()
                if projected is None or (
                    projected["projection_version"] != MARKET_PROJECTION_VERSION
                    or projected["projection_sha256"] != market_sha256
                    or projected["projection_row_count"] != market_row_count
                ):
                    raise EvidenceConflict(
                        f"Targeter run {run_id} market projection conflicts with prior ingestion"
                    )
                return "skipped"

            indexed_at_ns = time.time_ns()
            connection.execute(
                """INSERT INTO targeter_runs(
                       run_id, generated_at, generated_at_ns, input_complete,
                       report_version, strategy_version, manifest_key,
                       manifest_sha256, manifest_byte_length, report_key,
                       report_sha256, report_byte_length,
                       report_decoded_sha256, report_decoded_byte_length,
                       projection_version, projection_sha256,
                       projection_row_count, indexed_at_ns
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    run_id,
                    generated_at,
                    generated_at_ns,
                    int(input_complete),
                    report_version,
                    strategy_version,
                    manifest_key,
                    manifest_sha256,
                    manifest_byte_length,
                    report_key,
                    report_sha256,
                    report_byte_length,
                    report_decoded_sha256,
                    report_decoded_byte_length,
                    PROJECTION_VERSION,
                    projection_sha256,
                    len(projection_entries),
                    indexed_at_ns,
                ),
            )
            self._insert_market_projection(
                connection,
                run_id=run_id,
                projection=resolved_market_projection,
                raw_projection=market_projection,
                projection_sha256=market_sha256,
                projection_row_count=market_row_count,
                occurrences=normalized,
            )
            for value in normalized:
                context = value["context"]
                context_sha256 = value["context_sha256"]
                self._insert_context(connection, context_sha256, context)
                if value["occurrence_kind"] == "retained":
                    origin = connection.execute(
                        """SELECT context_sha256, occurrence_kind, origin_run_id
                           FROM selection_occurrences
                           WHERE run_id = ? AND bundle_id = ?""",
                        (value["origin_run_id"], value["bundle_id"]),
                    ).fetchone()
                    if (
                        origin is None
                        or origin["occurrence_kind"] != "complete"
                        or origin["origin_run_id"] != value["origin_run_id"]
                        or origin["context_sha256"] != context_sha256
                    ):
                        raise EvidenceConflict(
                            f"retained bundle {value['bundle_id']} has no exact complete origin"
                        )
                connection.execute(
                    """INSERT INTO selection_occurrences(
                           run_id, bundle_id, context_sha256, occurrence_kind,
                           origin_run_id, continuity_selected,
                           continuity_disposition
                       ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        run_id,
                        value["bundle_id"],
                        context_sha256,
                        value["occurrence_kind"],
                        value["origin_run_id"],
                        int(value["continuity_selected"]),
                        value["continuity_disposition"],
                    ),
                )
            for value in normalized_retirements:
                context = value["context"]
                context_sha256 = value["context_sha256"]
                self._insert_context(connection, context_sha256, context)
                origin = connection.execute(
                    """SELECT context_sha256, occurrence_kind, origin_run_id
                       FROM selection_occurrences
                       WHERE run_id = ? AND bundle_id = ?""",
                    (value["origin_run_id"], value["bundle_id"]),
                ).fetchone()
                if (
                    origin is None
                    or origin["occurrence_kind"] != "complete"
                    or origin["origin_run_id"] != value["origin_run_id"]
                    or origin["context_sha256"] != context_sha256
                ):
                    raise EvidenceConflict(
                        f"retired bundle {value['bundle_id']} has no exact complete origin"
                    )
                connection.execute(
                    """INSERT INTO bundle_retirements(
                           run_id, bundle_id, origin_run_id, context_sha256,
                           disposition
                       ) VALUES (?, ?, ?, ?, ?)""",
                    (
                        run_id,
                        value["bundle_id"],
                        value["origin_run_id"],
                        context_sha256,
                        value["disposition"],
                    ),
                )
        return "ingested"

    def _insert_market_projection(
        self,
        connection: sqlite3.Connection,
        *,
        run_id: str,
        projection: Mapping[str, Any],
        raw_projection: Mapping[str, Any],
        projection_sha256: str,
        projection_row_count: int,
        occurrences: Iterable[Mapping[str, Any]],
    ) -> None:
        for event in projection["events"]:
            existing = connection.execute(
                "SELECT * FROM umbrella_events WHERE event_id = ?",
                (event["event_id"],),
            ).fetchone()
            semantic = {
                "identity_version": event["identity_version"],
                "identity_activation_date": event["identity_activation_date"],
                "identity_ordinal": event["identity_ordinal"],
                "sport": event["sport"],
                "game": event["game"],
                "topology": event["topology"],
                "participant_keys_json": _canonical_json_value(
                    event["participant_keys"]
                ),
            }
            if existing is None:
                connection.execute(
                    """INSERT INTO umbrella_events(
                           event_id, identity_version,
                           identity_activation_date, identity_ordinal,
                           sport, game, topology, activation_at,
                           activation_at_ns, participants_json,
                           participant_keys_json,
                           first_seen_run_id, last_seen_run_id
                       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        event["event_id"],
                        semantic["identity_version"],
                        semantic["identity_activation_date"],
                        semantic["identity_ordinal"],
                        semantic["sport"],
                        semantic["game"],
                        semantic["topology"],
                        event["activation_at"],
                        _timestamp_ns(event["activation_at"]),
                        _canonical_json_value(event["participants"]),
                        semantic["participant_keys_json"],
                        run_id,
                        run_id,
                    ),
                )
            else:
                _require_columns(existing, semantic, f"event {event['event_id']}")
                first_seen, last_seen = _seen_run_ids(connection, existing, run_id)
                connection.execute(
                    """UPDATE umbrella_events
                       SET activation_at = CASE WHEN ? = ? THEN ? ELSE activation_at END,
                           activation_at_ns = CASE WHEN ? = ? THEN ? ELSE activation_at_ns END,
                           participants_json = CASE WHEN ? = ? THEN ? ELSE participants_json END,
                           first_seen_run_id = ?, last_seen_run_id = ?
                       WHERE event_id = ?""",
                    (
                        first_seen, run_id, event["activation_at"],
                        first_seen, run_id, _timestamp_ns(event["activation_at"]),
                        first_seen, run_id, _canonical_json_value(event["participants"]),
                        first_seen, last_seen, event["event_id"],
                    ),
                )
            connection.execute(
                """INSERT INTO event_observations(
                       run_id, event_id, bundle_id,
                       observed_activation_at, observed_activation_at_ns
                   ) VALUES (?, ?, ?, ?, ?)""",
                (
                    run_id, event["event_id"], event["source_bundle_id"],
                    event["activation_at"], _timestamp_ns(event["activation_at"]),
                ),
            )

        for event in projection["venue_events"]:
            existing = connection.execute(
                """SELECT event_id, first_seen_run_id, last_seen_run_id
                   FROM venue_events WHERE venue = ? AND venue_event_id = ?""",
                (event["venue"], event["venue_event_id"]),
            ).fetchone()
            if existing is not None and existing["event_id"] != event["event_id"]:
                raise EvidenceConflict(
                    f"venue event {event['venue']}:{event['venue_event_id']} "
                    "was assigned to a different umbrella event"
                )
            first_seen, last_seen = (
                (run_id, run_id)
                if existing is None
                else _seen_run_ids(connection, existing, run_id)
            )
            if existing is not None and _run_is_newer(
                connection, run_id, existing["last_seen_run_id"]
            ):
                connection.execute(
                    """UPDATE venue_events SET title = ?, league = ?, status = ?,
                              source_ref = ?, format = ?, fragment_type = ?
                       WHERE venue = ? AND venue_event_id = ?""",
                    (
                        event["title"], event["league"], event["status"],
                        event["source_ref"], event["format"], event["fragment_type"],
                        event["venue"], event["venue_event_id"],
                    ),
                )
            connection.execute(
                """INSERT INTO venue_events(
                       venue, venue_event_id, event_id, title, league, status,
                       source_ref, format, fragment_type, first_seen_run_id,
                       last_seen_run_id
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(venue, venue_event_id) DO UPDATE SET
                       first_seen_run_id = excluded.first_seen_run_id,
                       last_seen_run_id = excluded.last_seen_run_id""",
                (
                    event["venue"],
                    event["venue_event_id"],
                    event["event_id"],
                    event["title"],
                    event["league"],
                    event["status"],
                    event["source_ref"],
                    event["format"],
                    event["fragment_type"],
                    first_seen,
                    last_seen,
                ),
            )

        for market in projection["markets"]:
            key = (
                market["market_id"],
                market["market_template_version"],
                market["outcome_space_version"],
            )
            parameters_json = _canonical_json_value(market["parameters"])
            existing = connection.execute(
                """SELECT * FROM canonical_markets
                   WHERE market_id = ? AND market_template_version = ?
                     AND outcome_space_version = ?""",
                key,
            ).fetchone()
            semantic = {
                "event_id": market["event_id"],
                "canonical_class": market["canonical_class"],
                "market_type": market["market_type"],
                "scope": market["scope"],
                "parameters_json": parameters_json,
            }
            if existing is None:
                connection.execute(
                    """INSERT INTO canonical_markets(
                           market_id, market_template_version,
                           outcome_space_version, event_id, canonical_class,
                           market_type, scope, parameters_json,
                           first_seen_run_id, last_seen_run_id
                       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (*key, *semantic.values(), run_id, run_id),
                )
            else:
                _require_columns(existing, semantic, f"market {market['market_id']}")
                first_seen, last_seen = _seen_run_ids(connection, existing, run_id)
                connection.execute(
                    """UPDATE canonical_markets
                       SET first_seen_run_id = ?, last_seen_run_id = ?
                       WHERE market_id = ? AND market_template_version = ?
                         AND outcome_space_version = ?""",
                    (first_seen, last_seen, *key),
                )

        for market in projection["venue_markets"]:
            existing = connection.execute(
                """SELECT event_id, venue_event_id, market_id,
                          market_template_version, outcome_space_version,
                          first_seen_run_id, last_seen_run_id
                   FROM venue_markets WHERE venue = ? AND venue_market_id = ?""",
                (market["venue"], market["venue_market_id"]),
            ).fetchone()
            if existing is not None and (
                existing["event_id"] != market["event_id"]
                or existing["venue_event_id"] != market["venue_event_id"]
                or existing["market_id"] != market["market_id"]
                or existing["market_template_version"]
                != market["market_template_version"]
                or existing["outcome_space_version"]
                != market["outcome_space_version"]
            ):
                raise EvidenceConflict(
                    f"venue market {market['venue']}:{market['venue_market_id']} "
                    "was assigned to a different event or canonical market"
                )
            first_seen, last_seen = (
                (run_id, run_id)
                if existing is None
                else _seen_run_ids(connection, existing, run_id)
            )
            if existing is not None and _run_is_newer(
                connection, run_id, existing["last_seen_run_id"]
            ):
                connection.execute(
                    """UPDATE venue_markets SET canonical_class = ?, market_type = ?,
                              scope = ?, title = ?, parameters_json = ?, subscription_ids_json = ?,
                              outcome_labels_json = ?, status = ?, accepting_orders = ?,
                              rules_hash = ?, rule_template_id = ?, source_ref = ?, created_at = ?,
                              volume_24h = ?, volume_total = ?, volume_total_usd = ?, liquidity = ?
                       WHERE venue = ? AND venue_market_id = ?""",
                    (
                        market["canonical_class"],
                        market["market_type"], market["scope"], market["title"],
                        _canonical_json_value(market["parameters"]),
                        _canonical_json_value(market["subscription_ids"]),
                        _canonical_json_value(market["outcome_labels"]), market["status"],
                        int(market["accepting_orders"]), market["rules_hash"],
                        market["rule_template_id"], market["source_ref"], market["created_at"],
                        market["volume_24h"], market["volume_total"], market["volume_total_usd"],
                        market["liquidity"], market["venue"], market["venue_market_id"],
                    ),
                )
            connection.execute(
                """INSERT INTO venue_markets(
                       venue, venue_market_id, venue_event_id, event_id,
                       market_id, market_template_version,
                       outcome_space_version, canonical_class, market_type,
                       scope, title, parameters_json, subscription_ids_json,
                       outcome_labels_json, status, accepting_orders,
                       rules_hash, rule_template_id, source_ref, created_at,
                       volume_24h, volume_total, volume_total_usd, liquidity,
                       first_seen_run_id, last_seen_run_id
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                             ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(venue, venue_market_id) DO UPDATE SET
                       first_seen_run_id = excluded.first_seen_run_id,
                       last_seen_run_id = excluded.last_seen_run_id""",
                (
                    market["venue"], market["venue_market_id"],
                    market["venue_event_id"], market["event_id"],
                    market["market_id"], market["market_template_version"],
                    market["outcome_space_version"], market["canonical_class"],
                    market["market_type"], market["scope"], market["title"],
                    _canonical_json_value(market["parameters"]),
                    _canonical_json_value(market["subscription_ids"]),
                    _canonical_json_value(market["outcome_labels"]),
                    market["status"], int(market["accepting_orders"]),
                    market["rules_hash"], market["rule_template_id"],
                    market["source_ref"], market["created_at"],
                    market["volume_24h"], market["volume_total"],
                    market["volume_total_usd"], market["liquidity"],
                    first_seen, last_seen,
                ),
            )

        for decision in projection["decisions"]:
            connection.execute(
                """INSERT INTO candidate_decisions(
                       run_id, event_id, bundle_id, eligible, selected, score,
                       score_components_json, rejection_reasons_json,
                       allocation_rejection, admission_json,
                       market_exclusions_json, eligible_market_ids_json
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    run_id, decision["event_id"], decision["bundle_id"],
                    int(decision["eligible"]), int(decision["selected"]),
                    decision["score"],
                    _canonical_json_value(decision["score_components"]),
                    _canonical_json_value(decision["rejection_reasons"]),
                    decision["allocation_rejection"],
                    _canonical_json_value(decision["admission"]),
                    _canonical_json_value(decision["market_exclusions"]),
                    _canonical_json_value(decision["eligible_market_ids"]),
                ),
            )

        for selected in projection["selected_markets"]:
            connection.execute(
                """INSERT INTO selected_market_occurrences(
                       run_id, event_id, bundle_id, venue, venue_market_id,
                       market_id, market_template_version, outcome_space_version,
                       canonical_class, continuity_score, selection_reason,
                       origin_run_id
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    run_id, selected["event_id"], selected["bundle_id"],
                    selected["venue"], selected["venue_market_id"],
                    selected["market_id"], selected["market_template_version"],
                    selected["outcome_space_version"],
                    selected["canonical_class"], selected["continuity_score"],
                    selected["selection_reason"], selected["origin_run_id"],
                ),
            )

        projected_selected_bundles = {
            item["bundle_id"] for item in projection["selected_markets"]
        }
        for occurrence in occurrences:
            if occurrence["occurrence_kind"] != "retained":
                continue
            bundle_id = occurrence["bundle_id"]
            if bundle_id in projected_selected_bundles:
                continue
            origin_rows = connection.execute(
                """SELECT event_id, venue, venue_market_id, market_id,
                          market_template_version, outcome_space_version,
                          canonical_class,
                          continuity_score
                   FROM selected_market_occurrences
                   WHERE run_id = ? AND bundle_id = ?
                   ORDER BY venue, venue_market_id""",
                (occurrence["origin_run_id"], bundle_id),
            ).fetchall()
            if not origin_rows:
                raise EvidenceConflict(
                    f"retained bundle {bundle_id} has no market-universe origin"
                )
            for origin in origin_rows:
                connection.execute(
                    """INSERT INTO selected_market_occurrences(
                           run_id, event_id, bundle_id, venue, venue_market_id,
                           market_id, market_template_version, outcome_space_version,
                           canonical_class, continuity_score, selection_reason,
                           origin_run_id
                       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'retained', ?)""",
                    (
                        run_id, origin["event_id"], bundle_id, origin["venue"],
                        origin["venue_market_id"], origin["market_id"],
                        origin["market_template_version"], origin["outcome_space_version"],
                        origin["canonical_class"],
                        origin["continuity_score"], occurrence["origin_run_id"],
                    ),
                )
            activation_at = occurrence["context"]["activation_at"]
            connection.execute(
                """INSERT OR IGNORE INTO event_observations(
                       run_id, event_id, bundle_id,
                       observed_activation_at, observed_activation_at_ns
                   ) VALUES (?, ?, ?, ?, ?)""",
                (
                    run_id, origin_rows[0]["event_id"], bundle_id,
                    activation_at, _timestamp_ns(activation_at),
                ),
            )

        shortfall, unreconstructed = self._insert_claims(
            connection, run_id, raw_projection, projection
        )

        connection.execute(
            """INSERT INTO universe_run_projections(
                   run_id, projection_version, projection_sha256,
                   projection_row_count, claim_relation_shortfall,
                   unreconstructed_bundles
               ) VALUES (?, ?, ?, ?, ?, ?)""",
            (
                run_id, MARKET_PROJECTION_VERSION, projection_sha256,
                projection_row_count, shortfall, unreconstructed,
            ),
        )

    def _insert_claims(
        self,
        connection: sqlite3.Connection,
        run_id: str,
        raw_projection: Mapping[str, Any],
        resolved_projection: Mapping[str, Any],
    ) -> tuple[int, int]:
        """Record which claim each market expresses, and how claims relate.

        Nothing here is keyed by run: a claim is content-addressed by its
        outcome subset and a claim relation names no event, run, or venue, so a
        run that observes what earlier runs already observed writes no new rows
        and only moves last-seen markers. That is what removes the per-run
        relation growth the pairwise model had.
        """
        # Claims are grouped per candidate bundle, which the raw projection's
        # event rows are one-to-one with; the resolved projection supplies the
        # umbrella event each bundle was assigned so stored rows reference it.
        event_id_for_bundle = {
            row["source_bundle_id"]: row["event_id"]
            for row in resolved_projection["events"]
        }
        try:
            claims = project_claims(
                raw_projection, event_id_for_bundle=event_id_for_bundle
            )
        except MarketProjectionError as error:
            raise EvidenceConflict(str(error)) from error

        for claim in claims["claims"]:
            # Bounds resolve against generated time, never ingestion order:
            # sync drains retries before the date walk and bootstraps
            # newest-first, so a run seen second is often the older one.
            existing = connection.execute(
                "SELECT first_seen_run_id, last_seen_run_id FROM claim_classes "
                "WHERE claim_id = ?",
                (claim["claim_id"],),
            ).fetchone()
            first_seen, last_seen = (
                (run_id, run_id)
                if existing is None
                else _seen_run_ids(connection, existing, run_id)
            )
            connection.execute(
                """INSERT INTO claim_classes(
                       claim_id, space_shape_id, scope, coverage,
                       outcome_key_count, claim_identity_version,
                       first_seen_run_id, last_seen_run_id
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(claim_id) DO UPDATE SET
                       first_seen_run_id = excluded.first_seen_run_id,
                       last_seen_run_id = excluded.last_seen_run_id""",
                (
                    claim["claim_id"], claim["space_shape_id"], claim["scope"],
                    claim["coverage"], claim["outcome_key_count"],
                    claim["claim_identity_version"], first_seen, last_seen,
                ),
            )
        for relation in claims["claim_relations"]:
            connection.execute(
                """INSERT INTO claim_relations(
                       space_shape_id, left_claim_id, right_claim_id,
                       relation_type, algebra_version
                   ) VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(space_shape_id, left_claim_id, right_claim_id)
                   DO NOTHING""",
                (
                    relation["space_shape_id"], relation["left_claim_id"],
                    relation["right_claim_id"], relation["relation_type"],
                    relation["algebra_version"],
                ),
            )
            stored = connection.execute(
                """SELECT relation_type FROM claim_relations
                   WHERE space_shape_id = ? AND left_claim_id = ?
                     AND right_claim_id = ?""",
                (
                    relation["space_shape_id"], relation["left_claim_id"],
                    relation["right_claim_id"],
                ),
            ).fetchone()
            if stored is not None and stored["relation_type"] != relation["relation_type"]:
                # A claim relation is a function of the two outcome subsets, so
                # it cannot change. Disagreement means the subsets did.
                raise EvidenceConflict(
                    f"claim relation {relation['left_claim_id'][:12]}/"
                    f"{relation['right_claim_id'][:12]} conflicts with prior ingestion"
                )
        for member in claims["market_claims"]:
            existing = connection.execute(
                """SELECT first_seen_run_id, last_seen_run_id FROM market_claims
                   WHERE venue = ? AND venue_market_id = ? AND claim_key = ?
                     AND claim_id = ?""",
                (
                    member["venue"], member["venue_market_id"],
                    member["claim_key"], member["claim_id"],
                ),
            ).fetchone()
            first_seen, last_seen = (
                (run_id, run_id)
                if existing is None
                else _seen_run_ids(connection, existing, run_id)
            )
            connection.execute(
                """INSERT INTO market_claims(
                       venue, venue_market_id, claim_key, claim_id, event_id,
                       first_seen_run_id, last_seen_run_id
                   ) VALUES (?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(venue, venue_market_id, claim_key, claim_id)
                   DO UPDATE SET
                       first_seen_run_id = excluded.first_seen_run_id,
                       last_seen_run_id = excluded.last_seen_run_id""",
                (
                    member["venue"], member["venue_market_id"], member["claim_key"],
                    member["claim_id"], member["event_id"], first_seen, last_seen,
                ),
            )
        return (
            int(claims["relation_shortfall"]),
            int(claims["unreconstructed_bundles"]),
        )

    def _insert_context(
        self,
        connection: sqlite3.Connection,
        context_sha256: str,
        context: Mapping[str, Any],
    ) -> None:
        existing = connection.execute(
            "SELECT 1 FROM bundle_contexts WHERE context_sha256 = ?",
            (context_sha256,),
        ).fetchone()
        if existing is not None:
            stored = self._context(connection, context_sha256, bounded=False)
            if _context_sha256(stored) != context_sha256:
                raise EvidenceConflict(
                    f"bundle context {context_sha256} failed its content identity"
                )
            return
        connection.execute(
            """INSERT INTO bundle_contexts(
                   context_sha256, bundle_id, sport, game, topology,
                   activation_at, activation_at_ns, capture_start_at,
                   capture_start_at_ns
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                context_sha256,
                context["bundle_id"],
                context["sport"],
                context["game"],
                context["topology"],
                context["activation_at"],
                _timestamp_ns(context["activation_at"]),
                context["capture_start_at"],
                _timestamp_ns(context["capture_start_at"]),
            ),
        )
        connection.executemany(
            """INSERT INTO context_participants(
                   context_sha256, position, name, participant_key
               ) VALUES (?, ?, ?, ?)""",
            (
                (
                    context_sha256,
                    position,
                    name,
                    context["participant_keys"][position],
                )
                for position, name in enumerate(context["participants"])
            ),
        )
        connection.executemany(
            """INSERT INTO context_events(context_sha256, event_ref, venue)
               VALUES (?, ?, ?)""",
            (
                (context_sha256, event_ref, _venue_prefix(event_ref))
                for event_ref in context["event_refs"]
            ),
        )
        connection.executemany(
            """INSERT INTO context_markets(
                   context_sha256, target_id, venue, selected
               ) VALUES (?, ?, ?, ?)""",
            (
                (
                    context_sha256,
                    market["target_id"],
                    market["venue"],
                    int(market["selected"]),
                )
                for market in context["markets"]
            ),
        )
        for target in context["targets"]:
            connection.execute(
                """INSERT INTO context_targets(
                       context_sha256, target_id, venue, canonical_class,
                       source_ref
                   ) VALUES (?, ?, ?, ?, ?)""",
                (
                    context_sha256,
                    target["target_id"],
                    target["venue"],
                    target["canonical_class"],
                    target["source_ref"],
                ),
            )
            connection.executemany(
                """INSERT INTO context_target_assets(
                       context_sha256, target_id, asset_id
                   ) VALUES (?, ?, ?)""",
                (
                    (context_sha256, target["target_id"], asset_id)
                    for asset_id in target["subscription_ids"]
                ),
            )
        connection.executemany(
            """INSERT INTO context_relationships(
                   context_sha256, relationship_index, left_market,
                   right_market, relationship, scope, left_venue,
                   right_venue, coverage
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                (
                    context_sha256,
                    position,
                    item["left"],
                    item["right"],
                    item["relationship"],
                    item["scope"],
                    item["left_venue"],
                    item["right_venue"],
                    item["coverage"],
                )
                for position, item in enumerate(context["relationships"])
            ),
        )

    def set_checkpoint(self, name: str, cursor: str) -> None:
        now_ns = time.time_ns()
        with self.write_transaction() as connection:
            connection.execute(
                """INSERT INTO checkpoints(name, cursor, updated_at_ns)
                   VALUES (?, ?, ?)
                   ON CONFLICT(name) DO UPDATE SET
                     cursor = excluded.cursor,
                     updated_at_ns = excluded.updated_at_ns""",
                (name, cursor, now_ns),
            )

    def checkpoint(self, name: str) -> str | None:
        with closing(self.connect(readonly=True)) as connection:
            row = connection.execute(
                "SELECT cursor FROM checkpoints WHERE name = ?", (name,)
            ).fetchone()
        return None if row is None else str(row["cursor"])

    def begin_event_identity_backfill(
        self, generated_start: str, generated_end: str
    ) -> None:
        """Claim the one canonical oldest-first identity-allocation lineage."""

        generated_start = _canonical_timestamp(generated_start, "generated_start")
        generated_end = _canonical_timestamp(generated_end, "generated_end")
        with self.write_transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM event_identity_lineage WHERE singleton = 1"
            ).fetchone()
            if existing is not None:
                if (
                    existing["generated_start"] != generated_start
                    or existing["generated_end"] != generated_end
                ):
                    raise EvidenceConflict(
                        "canonical event-identity backfill must resume with its "
                        "original generated-time range"
                    )
                return
            event_count = int(
                connection.execute("SELECT COUNT(*) FROM umbrella_events").fetchone()[0]
            )
            if event_count:
                raise EvidenceConflict(
                    "canonical event-identity backfill requires an identity-empty database; "
                    f"{REBUILD_INSTRUCTION}"
                )
            connection.execute(
                """INSERT INTO event_identity_lineage(
                       singleton, generated_start, generated_end, state
                   ) VALUES (1, ?, ?, 'running')""",
                (generated_start, generated_end),
            )

    def complete_event_identity_backfill(
        self, generated_start: str, generated_end: str
    ) -> None:
        generated_start = _canonical_timestamp(generated_start, "generated_start")
        generated_end = _canonical_timestamp(generated_end, "generated_end")
        with self.write_transaction() as connection:
            cursor = connection.execute(
                """UPDATE event_identity_lineage SET state = 'complete'
                   WHERE singleton = 1 AND generated_start = ?
                     AND generated_end = ?""",
                (generated_start, generated_end),
            )
            if cursor.rowcount != 1:
                raise EvidenceConflict("canonical event-identity backfill lineage is missing")

    def event_identity_backfill_running(self) -> bool:
        with closing(self.connect(readonly=True)) as connection:
            row = connection.execute(
                "SELECT state FROM event_identity_lineage WHERE singleton = 1"
            ).fetchone()
        return row is not None and row["state"] == "running"

    def record_sync_failure(
        self, manifest_key: str, error: str, *, now_ns: int
    ) -> None:
        message = str(error)[:4096]
        with self.write_transaction() as connection:
            existing = connection.execute(
                "SELECT attempts, first_failed_at_ns FROM universe_sync_failures "
                "WHERE manifest_key = ?",
                (manifest_key,),
            ).fetchone()
            attempts = 1 if existing is None else int(existing["attempts"]) + 1
            first = now_ns if existing is None else int(existing["first_failed_at_ns"])
            retry_seconds = min(60 * (2 ** min(attempts - 1, 10)), 86_400)
            connection.execute(
                """INSERT INTO universe_sync_failures(
                       manifest_key, first_failed_at_ns, last_failed_at_ns,
                       next_retry_at_ns, attempts, error
                   ) VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT(manifest_key) DO UPDATE SET
                       last_failed_at_ns = excluded.last_failed_at_ns,
                       next_retry_at_ns = excluded.next_retry_at_ns,
                       attempts = excluded.attempts,
                       error = excluded.error""",
                (
                    manifest_key,
                    first,
                    now_ns,
                    now_ns + retry_seconds * 1_000_000_000,
                    attempts,
                    message,
                ),
            )

    def clear_sync_failure(self, manifest_key: str) -> None:
        with self.write_transaction() as connection:
            connection.execute(
                "DELETE FROM universe_sync_failures WHERE manifest_key = ?",
                (manifest_key,),
            )

    def due_sync_failures(
        self,
        *,
        now_ns: int,
        limit: int,
        key_start: str | None = None,
        key_end: str | None = None,
    ) -> list[str]:
        if limit <= 0:
            raise ValueError("sync failure retry limit must be positive")
        where, parameters = _sync_failure_range(key_start, key_end)
        with closing(self.connect(readonly=True)) as connection:
            rows = connection.execute(
                f"""SELECT manifest_key FROM universe_sync_failures
                    WHERE next_retry_at_ns <= ? {where}
                    ORDER BY next_retry_at_ns, manifest_key LIMIT ?""",
                (now_ns, *parameters, limit),
            ).fetchall()
        return [str(row["manifest_key"]) for row in rows]

    def sync_failure_count(
        self, *, key_start: str | None = None, key_end: str | None = None
    ) -> int:
        where, parameters = _sync_failure_range(key_start, key_end, conjunction=False)
        with closing(self.connect(readonly=True)) as connection:
            return int(
                connection.execute(
                    f"SELECT COUNT(*) FROM universe_sync_failures {where}", parameters
                ).fetchone()[0]
            )

    def has_sync_failure(self, manifest_key: str) -> bool:
        with closing(self.connect(readonly=True)) as connection:
            return connection.execute(
                "SELECT 1 FROM universe_sync_failures WHERE manifest_key = ?",
                (manifest_key,),
            ).fetchone() is not None

    def known_sync_failure_keys(self, manifest_keys: list[str]) -> set[str]:
        known: set[str] = set()
        with closing(self.connect(readonly=True)) as connection:
            for offset in range(0, len(manifest_keys), 500):
                chunk = manifest_keys[offset : offset + 500]
                if not chunk:
                    continue
                placeholders = ",".join("?" for _key in chunk)
                rows = connection.execute(
                    "SELECT manifest_key FROM universe_sync_failures "
                    f"WHERE manifest_key IN ({placeholders})",
                    chunk,
                ).fetchall()
                known.update(str(row["manifest_key"]) for row in rows)
        return known
