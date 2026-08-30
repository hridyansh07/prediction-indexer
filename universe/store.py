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
from universe.cadence import CADENCE_PROJECTION_VERSION, validate_cadence_projection
from universe.projection import PROJECTION_VERSION

SCHEMA_VERSION = 2
STALE_AFTER_SECONDS = 3_600
TARGETER_CADENCE_SECONDS = 600
SQLITE_CONTENT_TYPE = "application/vnd.sqlite3"
SCHEMA_PATH = Path(__file__).with_name("schema") / "v1.sql"
SCHEMA_V2_PATH = Path(__file__).with_name("schema") / "v2.sql"
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
                    self._validate_recoverable_schema(connection)
                version = 1
            if version == 1:
                self._validate_recoverable_schema(connection)
                try:
                    connection.execute("BEGIN IMMEDIATE")
                    cadence = connection.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'cadence_runs'"
                    ).fetchone()
                    if cadence is None:
                        self._execute_statements(connection, SCHEMA_V2_PATH)
                    else:
                        connection.execute(
                            "CREATE INDEX IF NOT EXISTS cadence_runs_generated "
                            "ON cadence_runs(generated_at_ns DESC, run_id DESC)"
                        )
                    connection.execute("PRAGMA user_version = 2")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
            elif version != SCHEMA_VERSION:
                raise EvidenceConflict(
                    f"unsupported Event Universe schema version {version}; "
                    "initialize a fresh v1 database"
                )

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
    def _expected_schema(cls, *paths: Path) -> dict[tuple[str, str], str]:
        with closing(sqlite3.connect(":memory:")) as expected:
            expected.execute("PRAGMA foreign_keys = ON")
            for path in paths:
                expected.executescript(path.read_text(encoding="utf-8"))
            return cls._schema_objects(expected)

    @classmethod
    def _validate_recoverable_schema(cls, connection: sqlite3.Connection) -> None:
        actual = cls._schema_objects(connection)
        expected_v1 = cls._expected_schema(SCHEMA_PATH)
        expected_v2 = cls._expected_schema(SCHEMA_PATH, SCHEMA_V2_PATH)
        cadence_keys = {
            ("table", "cadence_runs"),
            ("index", "cadence_runs_generated"),
        }
        if any(actual.get(key) != definition for key, definition in expected_v1.items()):
            raise EvidenceConflict("database contains an invalid Event Universe v1 schema")
        extras = set(actual) - set(expected_v1)
        if not extras <= cadence_keys:
            raise EvidenceConflict("database contains unexpected Event Universe schema objects")
        cadence_table = ("table", "cadence_runs")
        cadence_index = ("index", "cadence_runs_generated")
        if cadence_index in actual and cadence_table not in actual:
            raise EvidenceConflict("cadence index exists without its table")
        for key in extras:
            if actual[key] != expected_v2[key]:
                raise EvidenceConflict("existing cadence_runs table has an invalid schema")

    @staticmethod
    def _execute_statements(connection: sqlite3.Connection, path: Path) -> None:
        for statement in path.read_text(encoding="utf-8").split(";"):
            if statement.strip():
                connection.execute(statement)

    @classmethod
    def _execute_schema_transaction(
        cls, connection: sqlite3.Connection, path: Path
    ) -> None:
        try:
            connection.execute("BEGIN IMMEDIATE")
            cls._execute_statements(connection, path)
            connection.execute("PRAGMA user_version = 1")
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
                """SELECT r.manifest_sha256,
                          EXISTS(
                              SELECT 1 FROM cadence_runs c WHERE c.run_id = r.run_id
                          ) AS has_cadence,
                          r.run_id IN (
                              SELECT run_id FROM targeter_runs
                              ORDER BY generated_at_ns DESC, run_id DESC LIMIT 5
                          ) AS needs_cadence
                   FROM targeter_runs r WHERE r.manifest_key = ?""",
                (key,),
            ).fetchone()
        if row is None:
            return False
        if row["manifest_sha256"] != sha256:
            raise EvidenceConflict(f"immutable manifest {key!r} changed identity")
        return bool(row["has_cadence"]) or not bool(row["needs_cadence"])

    def missing_cadence_manifest_keys(self) -> list[str]:
        with closing(self.connect(readonly=True)) as connection:
            rows = connection.execute(
                """WITH newest AS (
                       SELECT run_id, manifest_key, generated_at_ns
                       FROM targeter_runs
                       ORDER BY generated_at_ns DESC, run_id DESC LIMIT 5
                   )
                   SELECT r.manifest_key FROM newest r
                   LEFT JOIN cadence_runs c USING (run_id)
                   WHERE c.run_id IS NULL
                   ORDER BY r.generated_at_ns DESC, r.run_id DESC"""
            ).fetchall()
        return [str(row["manifest_key"]) for row in rows]

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
        cadence: Mapping[str, Any],
        occurrences: Iterable[Mapping[str, Any]],
        retirements: Iterable[Mapping[str, Any]],
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
        cadence = validate_cadence_projection(cadence)
        cadence_json = json.dumps(
            cadence, ensure_ascii=False, separators=(",", ":"), sort_keys=True
        )
        cadence_sha256 = hashlib.sha256(cadence_json.encode("utf-8")).hexdigest()
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
            existing = connection.execute(
                "SELECT * FROM targeter_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if existing is not None:
                if any(existing[field] != value for field, value in expected.items()):
                    raise EvidenceConflict(
                        f"Targeter run {run_id} conflicts with prior ingestion"
                    )
                audit = self._audit_run(connection, run_id)
                if not audit["ok"]:
                    raise EvidenceConflict(
                        f"Targeter run {run_id} SQL projection failed audit"
                    )
                cached = connection.execute(
                    "SELECT payload_sha256 FROM cadence_runs WHERE run_id = ?", (run_id,)
                ).fetchone()
                if cached is not None and cached["payload_sha256"] != cadence_sha256:
                    raise EvidenceConflict(
                        f"Targeter run {run_id} cadence projection conflicts with prior ingestion"
                    )
                if cached is None:
                    connection.execute(
                        """INSERT INTO cadence_runs(
                               run_id, generated_at_ns, projection_version,
                               payload_json, payload_sha256
                           ) VALUES (?, ?, ?, ?, ?)""",
                        (
                            run_id,
                            generated_at_ns,
                            CADENCE_PROJECTION_VERSION,
                            cadence_json,
                            cadence_sha256,
                        ),
                    )
                    _trim_cadence(connection)
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
            connection.execute(
                """INSERT INTO cadence_runs(
                       run_id, generated_at_ns, projection_version,
                       payload_json, payload_sha256
                   ) VALUES (?, ?, ?, ?, ?)""",
                (
                    run_id,
                    generated_at_ns,
                    CADENCE_PROJECTION_VERSION,
                    cadence_json,
                    cadence_sha256,
                ),
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
            _trim_cadence(connection)
        return "ingested"

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
            if _context_sha256(self._context(connection, context_sha256)) != context_sha256:
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
            return self._context(connection, row["context_sha256"])

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
                )
            }
            latest = connection.execute(
                """SELECT run_id, generated_at, generated_at_ns,
                          indexed_at_ns, input_complete
                   FROM targeter_runs
                   ORDER BY generated_at_ns DESC, run_id DESC LIMIT 1"""
            ).fetchone()
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
            "status": "ok",
            "schema_version": version,
            "latest_run": latest_record,
            "counts": counts,
        }

    def cadence_snapshot(
        self, *, limit: int = 5, now_ns: int | None = None
    ) -> dict[str, Any]:
        """Return the bounded Targeter cadence view for the dashboard."""
        bounded = min(_limit(limit), 5)
        with closing(self.connect(readonly=True)) as connection:
            rows = connection.execute(
                """SELECT r.*, c.payload_json, c.payload_sha256
                   FROM cadence_runs c JOIN targeter_runs r USING (run_id)
                   ORDER BY c.generated_at_ns DESC, c.run_id DESC LIMIT ?""",
                (bounded,),
            ).fetchall()
            runs = []
            for row in rows:
                payload_json = str(row["payload_json"])
                if (
                    hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
                    != row["payload_sha256"]
                ):
                    raise EvidenceConflict(
                        f"Targeter run {row['run_id']} cadence projection failed its identity"
                    )
                payload = validate_cadence_projection(json.loads(payload_json))
                selections = self._selection_details(connection, str(row["run_id"]))
                run = _run_record(row)
                run.pop("payload_json", None)
                run.pop("payload_sha256", None)
                runs.append({**payload, **run, "selections": selections})
        observed_ns = now_ns if now_ns is not None else time.time_ns()
        latest = runs[0] if runs else None
        age_seconds = (
            max(0, (observed_ns - _timestamp_ns(latest["generated_at"])) // 1_000_000_000)
            if latest is not None
            else None
        )
        return {
            "cadence_projection_version": 1,
            "observed_at": _isoformat_ns(observed_ns),
            "freshness": {
                "state": (
                    "unavailable"
                    if latest is None
                    else "late"
                    if age_seconds is not None
                    and age_seconds >= TARGETER_CADENCE_SECONDS * 2
                    else "current"
                ),
                "expected_run_seconds": TARGETER_CADENCE_SECONDS,
                "latest_run_age_seconds": age_seconds,
                "latest_indexed_at": (
                    _isoformat_ns(latest["indexed_at_ns"]) if latest is not None else None
                ),
            },
            "runs": runs,
        }

    def _selection_details(
        self, connection: sqlite3.Connection, run_id: str
    ) -> list[dict[str, Any]]:
        rows = connection.execute(
            f"{_SELECTION_QUERY} WHERE o.run_id = ? ORDER BY o.bundle_id", (run_id,)
        ).fetchall()
        return [
            {
                **_selection_record(row),
                "context": self._context(connection, row["context_sha256"]),
            }
            for row in rows
        ]

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
        return {**_selection_record(row), "context": context}

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
        self, connection: sqlite3.Connection, context_sha256: str
    ) -> dict[str, Any]:
        context = connection.execute(
            "SELECT * FROM bundle_contexts WHERE context_sha256 = ?",
            (context_sha256,),
        ).fetchone()
        if context is None:
            raise EvidenceConflict(f"bundle context {context_sha256} is absent")
        participants = connection.execute(
            """SELECT position, name, participant_key FROM context_participants
               WHERE context_sha256 = ? ORDER BY position""",
            (context_sha256,),
        ).fetchall()
        events = connection.execute(
            """SELECT event_ref FROM context_events
               WHERE context_sha256 = ? ORDER BY event_ref""",
            (context_sha256,),
        ).fetchall()
        markets = connection.execute(
            """SELECT target_id, venue, selected FROM context_markets
               WHERE context_sha256 = ? ORDER BY target_id""",
            (context_sha256,),
        ).fetchall()
        targets = connection.execute(
            """SELECT target_id, venue, canonical_class, source_ref
               FROM context_targets WHERE context_sha256 = ?
               ORDER BY venue, target_id""",
            (context_sha256,),
        ).fetchall()
        target_records: list[dict[str, Any]] = []
        for target in targets:
            assets = connection.execute(
                """SELECT asset_id FROM context_target_assets
                   WHERE context_sha256 = ? AND target_id = ? ORDER BY asset_id""",
                (context_sha256, target["target_id"]),
            ).fetchall()
            target_records.append(
                {
                    **_row_record(target),
                    "subscription_ids": [asset["asset_id"] for asset in assets],
                }
            )
        relationships = connection.execute(
            """SELECT left_market AS left, right_market AS right,
                      relationship, scope, left_venue, right_venue, coverage
               FROM context_relationships WHERE context_sha256 = ?
               ORDER BY relationship_index""",
            (context_sha256,),
        ).fetchall()
        return {
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


def _row_record(row: sqlite3.Row) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}


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


def _trim_cadence(connection: sqlite3.Connection) -> None:
    connection.execute(
        """DELETE FROM cadence_runs WHERE run_id NOT IN (
               SELECT run_id FROM cadence_runs
               ORDER BY generated_at_ns DESC, run_id DESC LIMIT 5
           )"""
    )


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
