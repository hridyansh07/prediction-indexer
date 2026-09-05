"""Durable SQLite projection of committed Targeter v3 selection history."""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import sqlite3
import time
from contextlib import closing, contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping

from archive.common.durable import fsync_directory
from archive.storage.base import normalize_key
from targeter.v2.models import isoformat, parse_timestamp
from universe.event_identity import EventIdentityError, resolve_market_projection
from universe.claim_projection import project_claims
from universe.market_projection import MARKET_PROJECTION_VERSION, MarketProjectionError
from universe.projection import PROJECTION_VERSION

SCHEMA_VERSION = 6
STALE_AFTER_SECONDS = 3_600
TARGETER_RUN_INTERVAL_SECONDS = 600
EVENT_UNIVERSE_RESPONSE_BUDGET_BYTES = 1_750_000
DETAIL_ROW_LIMIT = 1000


class DetailTooLarge(ValueError):
    """A detail document would require more child rows than the API permits."""
SQLITE_CONTENT_TYPE = "application/vnd.sqlite3"
SCHEMA_PATH = Path(__file__).with_name("schema") / "schema.sql"
REBUILD_INSTRUCTION = (
    "remove the rebuildable SQLite file and run backfill from the immutable archive"
)


class EvidenceConflict(ValueError):
    """Immutable source evidence or its SQL projection is inconsistent."""


class UniverseStore:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self.connect()) as connection:
            version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            if version == 0:
                objects = self._schema_objects(connection)
                if not objects:
                    self._execute_schema_transaction(connection, SCHEMA_PATH)
                else:
                    raise EvidenceConflict(
                        f"Event Universe schema v{SCHEMA_VERSION} requires a fresh database; "
                        f"{REBUILD_INSTRUCTION}"
                    )
            elif version != SCHEMA_VERSION:
                raise EvidenceConflict(
                    f"unsupported Event Universe schema version {version}; "
                    f"{REBUILD_INSTRUCTION}"
                )
            self._validate_schema(connection)

    @staticmethod
    def _schema_objects(connection: sqlite3.Connection) -> dict[tuple[str, str], str]:
        return {
            (str(row[0]), str(row[1])): " ".join(str(row[2]).split())
            for row in connection.execute(
                "SELECT type, name, sql FROM sqlite_master "
                "WHERE type IN ('table', 'index', 'view', 'trigger') AND sql IS NOT NULL "
                "AND name NOT LIKE 'sqlite_%'"
            )
        }

    @classmethod
    def _expected_schema(cls) -> dict[tuple[str, str], str]:
        with closing(sqlite3.connect(":memory:")) as expected:
            expected.execute("PRAGMA foreign_keys = ON")
            expected.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
            return cls._schema_objects(expected)

    @classmethod
    def _validate_schema(cls, connection: sqlite3.Connection) -> None:
        actual = cls._schema_objects(connection)
        expected = cls._expected_schema()
        if actual != expected:
            raise EvidenceConflict(
                f"database contains an invalid Event Universe schema v{SCHEMA_VERSION}; "
                f"{REBUILD_INSTRUCTION}"
            )

    @staticmethod
    def _execute_statements(connection: sqlite3.Connection, path: Path) -> None:
        for statement in path.read_text(encoding="utf-8").split(";"):
            if statement.strip():
                connection.execute(statement)

    @classmethod
    def _execute_schema_transaction(
        cls, connection: sqlite3.Connection, *paths: Path
    ) -> None:
        try:
            connection.execute("BEGIN IMMEDIATE")
            for path in paths:
                cls._execute_statements(connection, path)
            connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
            connection.commit()
        except Exception:
            connection.rollback()
            raise

    def connect(self, *, readonly: bool = False) -> sqlite3.Connection:
        if readonly:
            connection = sqlite3.connect(
                f"file:{self.path}?mode=ro", uri=True, timeout=30.0
            )
            connection.execute("PRAGMA query_only = ON")
        else:
            connection = sqlite3.connect(self.path, timeout=30.0)
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
            connection.execute("PRAGMA wal_autocheckpoint = 1000")
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
        return connection

    @contextmanager
    def write_transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

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

        self._insert_claims(connection, run_id, projection)

        connection.execute(
            """INSERT INTO universe_run_projections(
                   run_id, projection_version, projection_sha256,
                   projection_row_count
               ) VALUES (?, ?, ?, ?)""",
            (
                run_id, MARKET_PROJECTION_VERSION, projection_sha256,
                projection_row_count,
            ),
        )

    def _insert_claims(
        self,
        connection: sqlite3.Connection,
        run_id: str,
        projection: Mapping[str, Any],
    ) -> None:
        """Record which claim each market expresses, and how claims relate.

        Nothing here is keyed by run: a claim is content-addressed by its
        outcome subset and a claim relation names no event, run, or venue, so a
        run that observes what earlier runs already observed writes no new rows
        and only moves last-seen markers. That is what removes the per-run
        relation growth the pairwise model had.
        """
        try:
            claims = project_claims(projection)
        except MarketProjectionError as error:
            raise EvidenceConflict(str(error)) from error

        for claim in claims["claims"]:
            connection.execute(
                """INSERT INTO claim_classes(
                       claim_id, space_shape_id, scope, coverage,
                       outcome_key_count, claim_identity_version,
                       first_seen_run_id, last_seen_run_id
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(claim_id) DO UPDATE SET
                       last_seen_run_id = excluded.last_seen_run_id""",
                (
                    claim["claim_id"], claim["space_shape_id"], claim["scope"],
                    claim["coverage"], claim["outcome_key_count"],
                    claim["claim_identity_version"], run_id, run_id,
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
            connection.execute(
                """INSERT INTO market_claims(
                       venue, venue_market_id, claim_key, claim_id, event_id,
                       first_seen_run_id, last_seen_run_id
                   ) VALUES (?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(venue, venue_market_id, claim_key, claim_id)
                   DO UPDATE SET last_seen_run_id = excluded.last_seen_run_id""",
                (
                    member["venue"], member["venue_market_id"], member["claim_key"],
                    member["claim_id"], member["event_id"], run_id, run_id,
                ),
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
                "stale_after_seconds": STALE_AFTER_SECONDS,
                "stale": age_seconds >= STALE_AFTER_SECONDS,
            }
        return {
            "status": "degraded" if pending_failures else "ok",
            "schema_version": version,
            "latest_run": latest_record,
            "counts": counts,
            "sync": {"pending_failures": pending_failures},
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
                    and age_seconds >= TARGETER_RUN_INTERVAL_SECONDS * 2
                    else "current"
                ),
                "expected_run_seconds": TARGETER_RUN_INTERVAL_SECONDS,
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
            if int(decision_bytes) > EVENT_UNIVERSE_RESPONSE_BUDGET_BYTES:
                raise DetailTooLarge("targeter run detail exceeds the byte limit")
            decisions = connection.execute(
                """SELECT event_id, bundle_id, eligible, selected, score,
                          score_components_json, rejection_reasons_json,
                          allocation_rejection, admission_json,
                          market_exclusions_json, eligible_market_ids_json
                   FROM candidate_decisions WHERE run_id = ? ORDER BY bundle_id
                   LIMIT ?""",
                (run_id, DETAIL_ROW_LIMIT + 1),
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
                (run_id, DETAIL_ROW_LIMIT + 1),
            ).fetchall()
            events = connection.execute(
                f"""SELECT DISTINCT event.*,
                          {_event_refs_sql('event')} AS event_refs_json
                   FROM umbrella_events event
                   JOIN event_observations observed USING (event_id)
                   WHERE observed.run_id = ?
                   ORDER BY event.activation_at_ns, event.event_id
                   LIMIT ?""",
                (run_id, DETAIL_ROW_LIMIT + 1),
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
                (event_id, DETAIL_ROW_LIMIT + 1),
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
                (event_id, DETAIL_ROW_LIMIT + 1),
            ).fetchall()
            relations = connection.execute(
                """SELECT DISTINCT related.relation_type,
                          related.left_claim_id, related.right_claim_id,
                          related.space_shape_id, claim.scope, claim.coverage
                   FROM market_claims left_member
                   JOIN claim_relations related
                     ON related.left_claim_id = left_member.claim_id
                   JOIN market_claims right_member
                     ON right_member.claim_id = related.right_claim_id
                    AND right_member.event_id = left_member.event_id
                   JOIN claim_classes claim
                     ON claim.claim_id = related.left_claim_id
                   WHERE left_member.event_id = ?
                   ORDER BY related.relation_type, related.left_claim_id,
                            related.right_claim_id
                   LIMIT ?""",
                (event_id, DETAIL_ROW_LIMIT + 1),
            ).fetchall()
            claims = connection.execute(
                """SELECT claim.claim_id, claim.space_shape_id, claim.scope,
                          claim.coverage, claim.outcome_key_count,
                          claim.first_seen_run_id, claim.last_seen_run_id,
                          COUNT(*) AS market_count,
                          COUNT(DISTINCT member.venue) AS venue_count
                   FROM claim_classes claim
                   JOIN market_claims member USING (claim_id)
                   WHERE member.event_id = ?
                   GROUP BY claim.claim_id
                   ORDER BY venue_count DESC, claim.claim_id
                   LIMIT ?""",
                (event_id, DETAIL_ROW_LIMIT + 1),
            ).fetchall()
            observations = connection.execute(
                """SELECT observed.run_id, run.generated_at, observed.bundle_id,
                          observed.observed_activation_at
                   FROM event_observations observed
                   JOIN targeter_runs run USING (run_id)
                   WHERE observed.event_id = ?
                   ORDER BY run.generated_at_ns, observed.run_id, observed.bundle_id
                   LIMIT ?""",
                (event_id, DETAIL_ROW_LIMIT + 1),
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
                (*key, DETAIL_ROW_LIMIT + 1),
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
                (*key, DETAIL_ROW_LIMIT + 1),
            ).fetchall()
            relations = connection.execute(
                """SELECT DISTINCT related.relation_type,
                          related.left_claim_id, related.right_claim_id,
                          related.space_shape_id, claim.scope, claim.coverage
                   FROM venue_markets venue
                   JOIN market_claims member
                     ON member.venue = venue.venue
                    AND member.venue_market_id = venue.venue_market_id
                   JOIN claim_relations related
                     ON related.left_claim_id = member.claim_id
                     OR related.right_claim_id = member.claim_id
                   JOIN claim_classes claim
                     ON claim.claim_id = related.left_claim_id
                   WHERE venue.market_id = ?
                     AND venue.market_template_version = ?
                     AND venue.outcome_space_version = ?
                   ORDER BY related.relation_type, related.left_claim_id,
                            related.right_claim_id
                   LIMIT ?""",
                (*key, DETAIL_ROW_LIMIT + 1),
            ).fetchall()
            claims = connection.execute(
                """SELECT DISTINCT claim.claim_id, claim.space_shape_id,
                          claim.scope, claim.coverage, claim.outcome_key_count,
                          claim.first_seen_run_id, claim.last_seen_run_id
                   FROM venue_markets venue
                   JOIN market_claims member
                     ON member.venue = venue.venue
                    AND member.venue_market_id = venue.venue_market_id
                   JOIN claim_classes claim USING (claim_id)
                   WHERE venue.market_id = ?
                     AND venue.market_template_version = ?
                     AND venue.outcome_space_version = ?
                   ORDER BY claim.claim_id
                   LIMIT ?""",
                (*key, DETAIL_ROW_LIMIT + 1),
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

    def claim_detail(self, claim_id: str) -> dict[str, Any] | None:
        """One claim: which markets express it, and what it relates to.

        Members carry targeter observation bounds, not lifecycle. A market's
        ``last_seen_run_id`` is the last run in which the targeter observed it
        expressing this claim; absence afterwards may mean the market settled,
        was delisted, or simply stopped being a candidate, and Universe cannot
        distinguish those without the venue.

        The old per-run observation list grew one row per run forever and
        overran the response bound once a claim outlived ~1,000 runs. These rows
        are bounded by the markets that express the claim.
        """
        identifier = str(claim_id)
        if not identifier or len(identifier) != 64 or not _is_hex(identifier):
            raise ValueError("claim id must be a sha256 digest")
        with closing(self.connect(readonly=True)) as connection:
            claim = connection.execute(
                "SELECT * FROM claim_classes WHERE claim_id = ?", (identifier,)
            ).fetchone()
            if claim is None:
                return None
            members = connection.execute(
                """SELECT member.venue, member.venue_market_id, member.claim_key,
                          member.event_id, member.first_seen_run_id,
                          member.last_seen_run_id, venue.market_id,
                          venue.market_template_version, venue.outcome_space_version,
                          venue.canonical_class, venue.title
                   FROM market_claims member
                   JOIN venue_markets venue USING (venue, venue_market_id)
                   WHERE member.claim_id = ?
                   ORDER BY member.venue, member.venue_market_id, member.claim_key
                   LIMIT ?""",
                (identifier, DETAIL_ROW_LIMIT + 1),
            ).fetchall()
            relations = connection.execute(
                """SELECT space_shape_id, left_claim_id, right_claim_id,
                          relation_type
                   FROM claim_relations
                   WHERE left_claim_id = ? OR right_claim_id = ?
                   ORDER BY relation_type, left_claim_id, right_claim_id
                   LIMIT ?""",
                (identifier, identifier, DETAIL_ROW_LIMIT + 1),
            ).fetchall()
        _ensure_detail_rows((members, relations), "claim detail")
        return _bounded_detail(
            {
                "claim": _row_record(claim),
                "members": [_row_record(row) for row in members],
                "relations": [_row_record(row) for row in relations],
            },
            "claim detail",
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
        row_limit = DETAIL_ROW_LIMIT + 1 if bounded else -1
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
            > EVENT_UNIVERSE_RESPONSE_BUDGET_BYTES
        ):
            raise DetailTooLarge("bundle context exceeds the byte limit")
        return record

    def backup(self, destination: Path) -> Path:
        destination = Path(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_name(
            f".{destination.name}.{os.getpid()}.{secrets.token_hex(4)}.open"
        )
        replaced = False
        try:
            with closing(self.connect(readonly=True)) as source, closing(
                sqlite3.connect(temporary)
            ) as sink:
                source.backup(sink)
                integrity = sink.execute("PRAGMA integrity_check").fetchone()
                if integrity is None or integrity[0] != "ok":
                    raise EvidenceConflict("SQLite backup failed its integrity check")
                sink.commit()
            with temporary.open("rb") as handle:
                os.fsync(handle.fileno())
            os.replace(temporary, destination)
            replaced = True
            fsync_directory(destination.parent)
        except Exception:
            temporary.unlink(missing_ok=True)
            if replaced:
                destination.unlink(missing_ok=True)
            raise
        return destination


_SELECTION_QUERY = """SELECT
    o.run_id, o.bundle_id, o.context_sha256, o.occurrence_kind,
    o.origin_run_id, o.continuity_selected, o.continuity_disposition,
    c.sport, c.game, c.topology, c.activation_at, c.activation_at_ns,
    c.capture_start_at, c.capture_start_at_ns,
    r.generated_at, r.generated_at_ns, r.manifest_key, r.manifest_sha256,
    r.report_key, r.report_sha256,
    origin.generated_at AS origin_generated_at,
    origin.manifest_key AS origin_manifest_key,
    origin.manifest_sha256 AS origin_manifest_sha256,
    origin.report_key AS origin_report_key,
    origin.report_sha256 AS origin_report_sha256,
    retired.run_id AS retirement_run_id,
    retired.generated_at AS retired_at,
    retirement.disposition AS retirement_disposition,
    retired.manifest_key AS retirement_manifest_key,
    retired.manifest_sha256 AS retirement_manifest_sha256,
    retired.report_key AS retirement_report_key,
    retired.report_sha256 AS retirement_report_sha256
FROM selection_occurrences o
JOIN bundle_contexts c USING (context_sha256)
JOIN targeter_runs r ON r.run_id = o.run_id
JOIN targeter_runs origin ON origin.run_id = o.origin_run_id
LEFT JOIN bundle_retirements retirement
    ON retirement.bundle_id = o.bundle_id
   AND retirement.run_id = (
       SELECT candidate.run_id
       FROM bundle_retirements candidate
       JOIN targeter_runs candidate_run ON candidate_run.run_id = candidate.run_id
       WHERE candidate.bundle_id = o.bundle_id
       ORDER BY candidate_run.generated_at_ns, candidate.run_id
       LIMIT 1
   )
LEFT JOIN targeter_runs retired ON retired.run_id = retirement.run_id"""


def file_sha256(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    length = 0
    with Path(path).open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
            length += len(chunk)
    return digest.hexdigest(), length


def _occurrence(record: dict[str, Any], *, expected_run_id: str) -> dict[str, Any]:
    expected = {
        "run_id",
        "bundle_id",
        "occurrence_kind",
        "origin_run_id",
        "continuity_selected",
        "continuity_disposition",
        "context",
    }
    if set(record) != expected or record.get("run_id") != expected_run_id:
        raise EvidenceConflict("selected occurrence fields disagree with its run")
    bundle_id = _nonempty(record["bundle_id"], "bundle_id")
    kind = record["occurrence_kind"]
    origin_run_id = _nonempty(record["origin_run_id"], "origin_run_id")
    selected = record["continuity_selected"]
    disposition = record["continuity_disposition"]
    if not isinstance(selected, bool) or kind not in {"complete", "retained"}:
        raise EvidenceConflict(f"selected occurrence {bundle_id} is invalid")
    if kind == "complete":
        if origin_run_id != expected_run_id or disposition not in {
            None,
            "held_current_candidate",
        } or selected != (disposition is not None):
            raise EvidenceConflict(f"complete occurrence {bundle_id} provenance is invalid")
    elif (
        origin_run_id == expected_run_id
        or not selected
        or disposition != "retained"
    ):
        raise EvidenceConflict(f"retained occurrence {bundle_id} provenance is invalid")
    context = _context_record(record["context"], expected_bundle_id=bundle_id)
    return {
        **record,
        "bundle_id": bundle_id,
        "origin_run_id": origin_run_id,
        "context": context,
        "context_sha256": _context_sha256(context),
    }


def _retirement(record: dict[str, Any], *, expected_run_id: str) -> dict[str, Any]:
    expected = {
        "run_id",
        "bundle_id",
        "origin_run_id",
        "disposition",
        "terminal_observed",
        "context",
    }
    if set(record) != expected or record.get("run_id") != expected_run_id:
        raise EvidenceConflict("bundle retirement fields disagree with its run")
    bundle_id = _nonempty(record["bundle_id"], "bundle_id")
    origin_run_id = _nonempty(record["origin_run_id"], "origin_run_id")
    disposition = record["disposition"]
    terminal_observed = record["terminal_observed"]
    if (
        origin_run_id == expected_run_id
        or disposition not in {"all_markets_terminal", "terminal_clamp_elapsed"}
        or not isinstance(terminal_observed, bool)
        or terminal_observed != (disposition == "all_markets_terminal")
    ):
        raise EvidenceConflict(f"retired bundle {bundle_id} provenance is invalid")
    context = _context_record(record["context"], expected_bundle_id=bundle_id)
    return {
        **record,
        "bundle_id": bundle_id,
        "origin_run_id": origin_run_id,
        "context": context,
        "context_sha256": _context_sha256(context),
    }


def _context_record(value: Any, *, expected_bundle_id: str) -> dict[str, Any]:
    expected = {
        "bundle_id",
        "sport",
        "game",
        "topology",
        "participants",
        "participant_keys",
        "activation_at",
        "capture_start_at",
        "event_refs",
        "markets",
        "targets",
        "relationships",
    }
    if not isinstance(value, Mapping) or set(value) != expected:
        raise EvidenceConflict("bundle context fields are invalid")
    record = dict(value)
    if record.get("bundle_id") != expected_bundle_id:
        raise EvidenceConflict("bundle context has the wrong bundle_id")
    sport = _nonempty(record["sport"], "sport")
    optional = {
        field: _optional_text(record[field], field) for field in ("game", "topology")
    }
    participants = _text_list(record["participants"], "participants")
    participant_keys = _text_list(record["participant_keys"], "participant_keys")
    if len(participants) != 2 or len(participant_keys) != 2:
        raise EvidenceConflict("bundle context must have two participants")
    activation_at = _canonical_timestamp(record["activation_at"], "activation_at")
    capture_start_at = _canonical_timestamp(
        record["capture_start_at"], "capture_start_at"
    )
    if _timestamp_ns(capture_start_at) >= _timestamp_ns(activation_at):
        raise EvidenceConflict("bundle capture_start_at must precede activation_at")
    event_refs = sorted(_text_list(record["event_refs"], "event_refs"))
    for reference in event_refs:
        _venue_prefix(reference)

    markets: list[dict[str, Any]] = []
    market_ids: set[str] = set()
    selected_market_ids: set[str] = set()
    if not isinstance(record["markets"], list) or not record["markets"]:
        raise EvidenceConflict("bundle markets must be a non-empty array")
    for raw in record["markets"]:
        if not isinstance(raw, Mapping) or set(raw) != {"target_id", "venue", "selected"}:
            raise EvidenceConflict("bundle market fields are invalid")
        target_id = _nonempty(raw["target_id"], "market target_id")
        venue = _nonempty(raw["venue"], "market venue")
        selected = raw["selected"]
        if (
            not isinstance(selected, bool)
            or _venue_prefix(target_id) != venue
            or target_id in market_ids
        ):
            raise EvidenceConflict(f"bundle market {target_id} is invalid")
        market_ids.add(target_id)
        if selected:
            selected_market_ids.add(target_id)
        markets.append({"target_id": target_id, "venue": venue, "selected": selected})
    markets.sort(key=lambda item: item["target_id"])

    targets: list[dict[str, Any]] = []
    target_ids: set[str] = set()
    if not isinstance(record["targets"], list) or not record["targets"]:
        raise EvidenceConflict("bundle targets must be a non-empty array")
    target_fields = {
        "venue",
        "target_id",
        "canonical_class",
        "subscription_ids",
        "source_ref",
    }
    for raw in record["targets"]:
        if not isinstance(raw, Mapping) or set(raw) != target_fields:
            raise EvidenceConflict("bundle target fields are invalid")
        target_id = _nonempty(raw["target_id"], "target_id")
        venue = _nonempty(raw["venue"], "target venue")
        if (
            _venue_prefix(target_id) != venue
            or target_id in target_ids
            or target_id not in selected_market_ids
        ):
            raise EvidenceConflict(f"bundle target {target_id} is invalid")
        target_ids.add(target_id)
        targets.append(
            {
                "venue": venue,
                "target_id": target_id,
                "canonical_class": _nonempty(
                    raw["canonical_class"], "canonical_class"
                ),
                "subscription_ids": sorted(
                    _text_list(raw["subscription_ids"], "subscription_ids")
                ),
                "source_ref": _nonempty(raw["source_ref"], "source_ref"),
            }
        )
    if target_ids != selected_market_ids:
        raise EvidenceConflict("selected markets and targets disagree")
    targets.sort(key=lambda item: (item["venue"], item["target_id"]))

    relationships: list[dict[str, str]] = []
    relationship_fields = {
        "left",
        "right",
        "relationship",
        "scope",
        "left_venue",
        "right_venue",
        "coverage",
    }
    if not isinstance(record["relationships"], list):
        raise EvidenceConflict("bundle relationships must be an array")
    for raw in record["relationships"]:
        if not isinstance(raw, Mapping) or set(raw) != relationship_fields:
            raise EvidenceConflict("bundle relationship fields are invalid")
        relationships.append(
            {field: _nonempty(raw[field], f"relationship {field}") for field in relationship_fields}
        )
    relationships.sort(
        key=lambda item: (
            item["left"],
            item["right"],
            item["relationship"],
            item["scope"],
        )
    )
    return {
        "bundle_id": expected_bundle_id,
        "sport": sport,
        **optional,
        "participants": participants,
        "participant_keys": participant_keys,
        "activation_at": activation_at,
        "capture_start_at": capture_start_at,
        "event_refs": event_refs,
        "markets": markets,
        "targets": targets,
        "relationships": relationships,
    }


def _projection_entry(value: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "bundle_id": value["bundle_id"],
        "context_sha256": value["context_sha256"],
        "occurrence_kind": value["occurrence_kind"],
        "origin_run_id": value["origin_run_id"],
        "continuity_selected": value["continuity_selected"],
        "continuity_disposition": value["continuity_disposition"],
    }


def _retirement_projection_entry(value: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "bundle_id": value["bundle_id"],
        "context_sha256": value["context_sha256"],
        "origin_run_id": value["origin_run_id"],
        "retirement_disposition": value["disposition"],
    }


def _context_sha256(context: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical_json(context).encode("utf-8")).hexdigest()


def _records_sha256(records: Iterable[Mapping[str, Any]]) -> str:
    digest = hashlib.sha256()
    for record in records:
        digest.update((_canonical_json(record) + "\n").encode("utf-8"))
    return digest.hexdigest()


def _canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _run_record(row: sqlite3.Row) -> dict[str, Any]:
    record = _row_record(row)
    record["input_complete"] = bool(record["input_complete"])
    return record


def _run_summary(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "run_id": row["run_id"],
        "generated_at": row["generated_at"],
        "input_complete": bool(row["input_complete"]),
        "indexed_at": _isoformat_ns(int(row["indexed_at_ns"])),
    }


def _selection_record(row: sqlite3.Row) -> dict[str, Any]:
    retirement = None
    if row["retirement_run_id"] is not None:
        retirement = {
            "retired_at": row["retired_at"],
            "disposition": row["retirement_disposition"],
            "terminal_observed_at": (
                row["retired_at"]
                if row["retirement_disposition"] == "all_markets_terminal"
                else None
            ),
            "source": {
                "run_id": row["retirement_run_id"],
                "manifest_key": row["retirement_manifest_key"],
                "manifest_sha256": row["retirement_manifest_sha256"],
                "report_key": row["retirement_report_key"],
                "report_sha256": row["retirement_report_sha256"],
            },
        }
    return {
        "run_id": row["run_id"],
        "generated_at": row["generated_at"],
        "bundle_id": row["bundle_id"],
        "occurrence_kind": row["occurrence_kind"],
        "continuity_selected": bool(row["continuity_selected"]),
        "continuity_disposition": row["continuity_disposition"],
        "sport": row["sport"],
        "game": row["game"],
        "topology": row["topology"],
        "activation_at": row["activation_at"],
        "capture_start_at": row["capture_start_at"],
        "retirement": retirement,
        "source": {
            "manifest_key": row["manifest_key"],
            "manifest_sha256": row["manifest_sha256"],
            "report_key": row["report_key"],
            "report_sha256": row["report_sha256"],
        },
        "origin": {
            "run_id": row["origin_run_id"],
            "generated_at": row["origin_generated_at"],
            "manifest_key": row["origin_manifest_key"],
            "manifest_sha256": row["origin_manifest_sha256"],
            "report_key": row["origin_report_key"],
            "report_sha256": row["origin_report_sha256"],
        },
    }


def _is_hex(value: str) -> bool:
    return all(character in "0123456789abcdef" for character in value)


def _row_record(row: sqlite3.Row) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}


def _market_projection(
    value: Mapping[str, Any], *, expected_run_id: str, expected_generated_at: str
) -> dict[str, Any]:
    fields = {
        "projection_version",
        "run_id",
        "generated_at",
        "events",
        "venue_events",
        "markets",
        "venue_markets",
        "decisions",
        "selected_markets",
        "relations",
    }
    if not isinstance(value, Mapping) or set(value) != fields:
        raise EvidenceConflict("market projection fields are invalid")
    if (
        value.get("projection_version") != MARKET_PROJECTION_VERSION
        or value.get("run_id") != expected_run_id
        or value.get("generated_at") != expected_generated_at
    ):
        raise EvidenceConflict("market projection disagrees with its Targeter run")
    record = dict(value)
    for field in fields - {"projection_version", "run_id", "generated_at"}:
        if not isinstance(record[field], list) or any(
            not isinstance(item, Mapping) for item in record[field]
        ):
            raise EvidenceConflict(f"market projection {field} must be object rows")
    try:
        json.dumps(record, allow_nan=False, sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError) as error:
        raise EvidenceConflict("market projection must be finite JSON") from error
    return record


def _market_projection_identity(value: Mapping[str, Any]) -> tuple[str, int]:
    encoded = _canonical_json_value(value)
    rows = sum(
        len(value[field])
        for field in (
            "events",
            "venue_events",
            "markets",
            "venue_markets",
            "decisions",
            "selected_markets",
            "relations",
        )
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest(), rows


def _canonical_json_value(value: Any) -> str:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _require_columns(
    row: sqlite3.Row, expected: Mapping[str, Any], label: str
) -> None:
    if any(row[field] != value for field, value in expected.items()):
        raise EvidenceConflict(f"{label} conflicts with its prior projection")


def _seen_run_ids(
    connection: sqlite3.Connection, existing: sqlite3.Row, run_id: str
) -> tuple[str, str]:
    run_ids = {
        str(existing["first_seen_run_id"]),
        str(existing["last_seen_run_id"]),
        run_id,
    }
    rows = connection.execute(
        f"""SELECT run_id, generated_at_ns FROM targeter_runs
            WHERE run_id IN ({','.join('?' for _ in run_ids)})""",
        tuple(sorted(run_ids)),
    ).fetchall()
    if len(rows) != len(run_ids):
        raise EvidenceConflict("first/last-seen run provenance is incomplete")
    ordered = sorted(rows, key=lambda row: (row["generated_at_ns"], row["run_id"]))
    return str(ordered[0]["run_id"]), str(ordered[-1]["run_id"])


def _run_is_newer(
    connection: sqlite3.Connection, run_id: str, other_run_id: str
) -> bool:
    rows = connection.execute(
        """SELECT run_id, generated_at_ns FROM targeter_runs
           WHERE run_id IN (?, ?)""",
        (run_id, other_run_id),
    ).fetchall()
    if len(rows) != 2:
        raise EvidenceConflict("venue projection observation is incomplete")
    timestamps = {str(row["run_id"]): int(row["generated_at_ns"]) for row in rows}
    return (timestamps[run_id], run_id) > (timestamps[other_run_id], other_run_id)


def _event_record(row: sqlite3.Row) -> dict[str, Any]:
    keys = set(row.keys())
    record = {
        "event_id": row["event_id"],
        "identity_version": row["identity_version"],
        "identity_activation_date": row["identity_activation_date"],
        "identity_ordinal": row["identity_ordinal"],
        "sport": row["sport"],
        "game": row["game"],
        "topology": row["topology"],
        "activation_at": row["activation_at"],
        "participants": json.loads(row["participants_json"]),
        "participant_keys": json.loads(row["participant_keys_json"]),
        "event_refs": json.loads(row["event_refs_json"]),
        "first_seen_run_id": row["first_seen_run_id"],
        "last_seen_run_id": row["last_seen_run_id"],
    }
    for field in ("venue_count", "market_count", "selected_run_count"):
        if field in keys:
            record[field] = int(row[field])
    return record


def _event_refs_sql(event_alias: str) -> str:
    return f"""COALESCE((
        SELECT json_group_array(alias.event_ref)
        FROM (
            SELECT venue || ':' || venue_event_id AS event_ref
            FROM venue_events
            WHERE event_id = {event_alias}.event_id
            ORDER BY venue, venue_event_id
        ) alias
    ), '[]')"""


def _canonical_market_record(row: sqlite3.Row) -> dict[str, Any]:
    keys = set(row.keys())
    record = {
        "market_id": row["market_id"],
        "market_template_version": row["market_template_version"],
        "outcome_space_version": row["outcome_space_version"],
        "event_id": row["event_id"],
        "canonical_class": row["canonical_class"],
        "market_type": row["market_type"],
        "scope": row["scope"],
        "parameters": json.loads(row["parameters_json"]),
        "first_seen_run_id": row["first_seen_run_id"],
        "last_seen_run_id": row["last_seen_run_id"],
    }
    if "venue_market_count" in keys:
        record["venue_market_count"] = int(row["venue_market_count"])
        record["venues"] = sorted(
            item for item in str(row["venues"] or "").split(",") if item
        )
    return record


def _venue_market_record(row: sqlite3.Row) -> dict[str, Any]:
    record = _row_record(row)
    record["parameters"] = json.loads(record.pop("parameters_json"))
    record["subscription_ids"] = json.loads(record.pop("subscription_ids_json"))
    record["outcome_labels"] = json.loads(record.pop("outcome_labels_json"))
    record["accepting_orders"] = bool(record["accepting_orders"])
    return record


def _range(start: int | None, end: int | None, label: str) -> None:
    for value in (start, end):
        if value is not None and (
            not isinstance(value, int) or isinstance(value, bool) or value < 0
        ):
            raise ValueError(f"{label} bounds must be non-negative integers")
    if start is not None and end is not None and start >= end:
        raise ValueError(f"{label}_start must be before {label}_end")


def _limit(value: int) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError("limit must be a positive integer")
    return min(value, 1000)


def _ensure_detail_rows(groups: tuple[list[Any], ...], label: str) -> None:
    if any(len(rows) > DETAIL_ROW_LIMIT for rows in groups):
        raise DetailTooLarge(f"{label} exceeds the child-row limit")


def _bounded_detail(record: dict[str, Any], label: str) -> dict[str, Any]:
    if (
        len(_canonical_json_value(record).encode("utf-8"))
        > EVENT_UNIVERSE_RESPONSE_BUDGET_BYTES
    ):
        raise DetailTooLarge(f"{label} exceeds the byte limit")
    return record


def _sync_failure_range(
    key_start: str | None,
    key_end: str | None,
    *,
    conjunction: bool = True,
) -> tuple[str, tuple[str, str] | tuple[()]]:
    if (key_start is None) != (key_end is None):
        raise ValueError("sync failure key bounds must both be set")
    if key_start is None:
        return "", ()
    operator = "AND" if conjunction else "WHERE"
    return f"{operator} manifest_key >= ? AND manifest_key < ?", (key_start, key_end)


def _nonempty(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise EvidenceConflict(f"{label} must be non-empty text")
    return value


def _optional_text(value: Any, label: str) -> str | None:
    if value is None:
        return None
    return _nonempty(value, label)


def _text_list(value: Any, label: str) -> list[str]:
    if (
        not isinstance(value, list)
        or not all(isinstance(item, str) and item for item in value)
        or len(value) != len(set(value))
    ):
        raise EvidenceConflict(f"{label} must contain unique non-empty text")
    return list(value)


def _canonical_timestamp(value: Any, label: str) -> str:
    parsed = parse_timestamp(value)
    if parsed is None:
        raise EvidenceConflict(f"{label} must be a UTC timestamp")
    canonical = isoformat(parsed)
    if value != canonical:
        raise EvidenceConflict(f"{label} must use canonical UTC form")
    return canonical


def _timestamp_ns(value: str) -> int:
    parsed = parse_timestamp(value)
    if parsed is None:
        raise EvidenceConflict("timestamp must be valid")
    delta = parsed - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (
        (delta.days * 86_400 + delta.seconds) * 1_000_000_000
        + delta.microseconds * 1_000
    )


def _isoformat_ns(value: int) -> str:
    return isoformat(
        datetime.fromtimestamp(value / 1_000_000_000, tz=timezone.utc)
    )


def _sha256(value: Any, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or value != value.lower()
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise EvidenceConflict(f"{label} is invalid")
    return value


def _venue_prefix(reference: str) -> str:
    venue, separator, identifier = reference.partition(":")
    if not separator or not venue or not identifier:
        raise EvidenceConflict(f"reference has no venue prefix: {reference!r}")
    return venue
