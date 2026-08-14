"""SQLite-backed Event Universe evidence index.

SQLite is a derived, durable query surface; immutable Targeter manifests and
segment universe receipts remain the evidence authorities.  Every ingestion is
one ``BEGIN IMMEDIATE`` transaction and every source identity is recorded, so a
retry is a no-op while a changed immutable source fails closed.
"""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import sqlite3
from contextlib import closing, contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping

from archive.archiver.universe import SegmentUniverseReceipt
from archive.common.durable import fsync_directory
from targeter.targets import Target, target_digest

SCHEMA_VERSION = 1
SQLITE_CONTENT_TYPE = "application/vnd.sqlite3"


class EvidenceConflict(ValueError):
    """An immutable identity or indexed evidence disagrees with prior state."""


class UniverseStore:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self.connect()) as connection:
            version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            if version > SCHEMA_VERSION:
                raise EvidenceConflict(
                    f"universe schema {version} is newer than supported {SCHEMA_VERSION}"
                )
            if version == 0:
                connection.executescript(_SCHEMA_V1)
                connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
                connection.commit()
            elif version != SCHEMA_VERSION:
                raise EvidenceConflict(f"unsupported universe schema version {version}")

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

    def source_is_complete(self, source_key: str, identity_sha256: str) -> bool:
        with closing(self.connect(readonly=True)) as connection:
            row = connection.execute(
                "SELECT identity_sha256 FROM ingest_sources WHERE source_key = ?",
                (source_key,),
            ).fetchone()
        if row is None:
            return False
        if row["identity_sha256"] != identity_sha256:
            raise EvidenceConflict(
                f"source {source_key!r} changed identity from "
                f"{row['identity_sha256']} to {identity_sha256}"
            )
        return True

    def ingest_targeter_run(
        self,
        *,
        source_key: str,
        source_sha256: str,
        run_id: str,
        generated_at: str,
        input_complete: bool,
        events: Iterable[Mapping[str, Any]],
        markets: Iterable[Mapping[str, Any]],
        report: Mapping[str, Any],
        target_records: Iterable[Mapping[str, Any]] = (),
    ) -> str:
        """Index one manifest-committed Targeter run, idempotently."""
        event_rows = [dict(item) for item in events]
        market_rows = [dict(item) for item in markets]
        target_record_rows = [dict(item) for item in target_records]
        report_record = dict(report)
        generated_ns = _timestamp_ns(generated_at)
        with self.write_transaction() as connection:
            if not _begin_source(
                connection, source_key, "targeter_run", source_sha256
            ):
                return "skipped"
            connection.execute(
                """INSERT INTO targeter_runs(
                       run_id, source_key, generated_at, generated_at_ns,
                       input_complete, report_json
                   ) VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    run_id,
                    source_key,
                    generated_at,
                    generated_ns,
                    int(input_complete),
                    _canonical_json(report_record),
                ),
            )

            known_events: set[str] = set()
            for record in event_rows:
                venue = _text(record, "venue", "catalog event")
                venue_event_id = _text(record, "venue_event_id", "catalog event")
                event_ref = f"{venue}:{venue_event_id}"
                if event_ref in known_events:
                    raise EvidenceConflict(f"run {run_id} repeats event {event_ref}")
                known_events.add(event_ref)
                connection.execute(
                    """INSERT INTO catalog_events(
                           run_id, event_ref, venue, venue_event_id, title, sport,
                           league, game, activation_at, record_json
                       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        run_id,
                        event_ref,
                        venue,
                        venue_event_id,
                        _optional_text(record.get("title")),
                        _optional_text(record.get("sport")),
                        _optional_text(record.get("league")),
                        _optional_text(record.get("game")),
                        _optional_text(record.get("activation_at")),
                        _canonical_json(record),
                    ),
                )

            seen_markets: set[str] = set()
            known_markets: set[str] = set()
            market_venues: dict[str, str] = {}
            for record in market_rows:
                venue = _text(record, "venue", "catalog market")
                venue_event_id = _text(record, "venue_event_id", "catalog market")
                venue_market_id = _text(record, "venue_market_id", "catalog market")
                target_id = _text(record, "target_id", "catalog market")
                if target_id != f"{venue}:{venue_market_id}":
                    raise EvidenceConflict(f"catalog market {target_id} has inconsistent identity")
                if target_id in seen_markets:
                    raise EvidenceConflict(f"run {run_id} repeats market {target_id}")
                seen_markets.add(target_id)
                event_ref = f"{venue}:{venue_event_id}"
                if event_ref not in known_events:
                    _issue(
                        connection,
                        source_key,
                        "market_event_missing",
                        target_id,
                        event_ref,
                    )
                    continue
                known_markets.add(target_id)
                market_venues[target_id] = venue
                subscription_ids = _text_list(
                    record.get("subscription_ids"), "catalog market subscription_ids"
                )
                connection.execute(
                    """INSERT INTO catalog_markets(
                           run_id, target_id, event_ref, venue, venue_market_id,
                           canonical_class, market_type, scope, title,
                           subscription_ids_json, record_json
                       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        run_id,
                        target_id,
                        event_ref,
                        venue,
                        venue_market_id,
                        _optional_text(record.get("canonical_class")),
                        _optional_text(record.get("market_type")),
                        _optional_text(record.get("scope")),
                        _optional_text(record.get("title")),
                        _canonical_json(subscription_ids),
                        _canonical_json(record),
                    ),
                )

            for record in target_record_rows:
                target_id = _text(record, "target_id", "target record")
                venue = _text(record, "venue", "target record")
                if record.get("run_id") != run_id:
                    raise EvidenceConflict(
                        f"target record {target_id} names another Targeter run"
                    )
                if target_id not in known_markets:
                    _issue(
                        connection,
                        source_key,
                        "target_record_market_missing",
                        target_id,
                        venue,
                    )
                    continue
                if market_venues[target_id] != venue:
                    raise EvidenceConflict(
                        f"target record {target_id} is grouped under venue {venue}"
                    )
                subscription_ids = _text_list(
                    record.get("subscription_ids"),
                    "target record subscription_ids",
                    allow_empty=True,
                )
                raw_record = record.get("record")
                if not isinstance(raw_record, dict):
                    raise EvidenceConflict(
                        f"target record {target_id} raw record is not an object"
                    )
                record_sha256 = _text(record, "record_sha256", "target record")
                computed_sha256 = hashlib.sha256(
                    _canonical_json(raw_record).encode("utf-8")
                ).hexdigest()
                if record_sha256 != computed_sha256:
                    raise EvidenceConflict(
                        f"target record {target_id} content hash is invalid"
                    )
                connection.execute(
                    """INSERT INTO target_records(
                           run_id, target_id, venue, observed_at,
                           subscription_ids_json, record_sha256, record_json
                       ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        run_id,
                        target_id,
                        venue,
                        _optional_text(record.get("observed_at")),
                        _canonical_json(subscription_ids),
                        record_sha256,
                        _canonical_json(record),
                    ),
                )

            candidates = report_record.get("candidates")
            if not isinstance(candidates, list):
                raise EvidenceConflict("Targeter report candidates must be an array")
            selection = report_record.get("selection")
            if not isinstance(selection, dict):
                raise EvidenceConflict("Targeter report selection must be an object")
            selected_bundle_ids = set(
                _text_list(selection.get("bundle_ids"), "selected bundle ids", allow_empty=True)
            )
            known_bundles: set[str] = set()
            for candidate in candidates:
                if not isinstance(candidate, dict):
                    raise EvidenceConflict("Targeter candidate is not an object")
                bundle_id = _text(candidate, "bundle_id", "Targeter candidate")
                if bundle_id in known_bundles:
                    raise EvidenceConflict(f"run {run_id} repeats bundle {bundle_id}")
                known_bundles.add(bundle_id)
                connection.execute(
                    """INSERT INTO event_bundles(
                           run_id, bundle_id, selected, activation_at, game,
                           confidence, record_json
                       ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        run_id,
                        bundle_id,
                        int(bundle_id in selected_bundle_ids),
                        _optional_text(candidate.get("activation_at")),
                        _optional_text(candidate.get("game")),
                        _optional_text(candidate.get("confidence")),
                        _canonical_json(candidate),
                    ),
                )
                for event_ref in _text_list(
                    candidate.get("event_refs"), "candidate event_refs", allow_empty=True
                ):
                    if event_ref not in known_events:
                        _issue(
                            connection,
                            source_key,
                            "bundle_event_missing",
                            bundle_id,
                            event_ref,
                        )
                        continue
                    connection.execute(
                        "INSERT INTO bundle_events(run_id, bundle_id, event_ref) VALUES (?, ?, ?)",
                        (run_id, bundle_id, event_ref),
                    )
                for target_id in _text_list(
                    candidate.get("market_ids"), "candidate market_ids", allow_empty=True
                ):
                    if target_id not in known_markets:
                        _issue(
                            connection,
                            source_key,
                            "bundle_market_missing",
                            bundle_id,
                            target_id,
                        )
                        continue
                    connection.execute(
                        "INSERT INTO bundle_markets(run_id, bundle_id, target_id) VALUES (?, ?, ?)",
                        (run_id, bundle_id, target_id),
                    )

            for missing in sorted(selected_bundle_ids - known_bundles):
                _issue(
                    connection,
                    source_key,
                    "selected_bundle_missing",
                    missing,
                    "selection names no candidate",
                )

            targets = selection.get("targets")
            if not isinstance(targets, dict):
                raise EvidenceConflict("Targeter selection targets must be an object")
            assets_by_venue: dict[str, set[str]] = {}
            for venue, raw_targets in sorted(targets.items()):
                if not isinstance(venue, str) or not isinstance(raw_targets, list):
                    raise EvidenceConflict("Targeter selection target group is invalid")
                for position, raw_target in enumerate(raw_targets):
                    if not isinstance(raw_target, dict):
                        raise EvidenceConflict(
                            f"Targeter selection target {venue}[{position}] is not an object"
                        )
                    target_id = _text(raw_target, "target_id", "selection target")
                    bundle_id = _text(raw_target, "bundle_id", "selection target")
                    subscription_ids = _text_list(
                        raw_target.get("subscription_ids"),
                        "selection target subscription_ids",
                    )
                    if target_id not in known_markets:
                        _issue(
                            connection,
                            source_key,
                            "selected_market_missing",
                            target_id,
                            bundle_id,
                        )
                        continue
                    if market_venues[target_id] != venue:
                        raise EvidenceConflict(
                            f"selection target {target_id} is grouped under venue {venue}"
                        )
                    if bundle_id not in known_bundles:
                        _issue(
                            connection,
                            source_key,
                            "selected_target_bundle_missing",
                            target_id,
                            bundle_id,
                        )
                        continue
                    connection.execute(
                        """INSERT INTO target_selections(
                               run_id, venue, target_id, bundle_id,
                               subscription_ids_json
                           ) VALUES (?, ?, ?, ?, ?)""",
                        (
                            run_id,
                            venue,
                            target_id,
                            bundle_id,
                            _canonical_json(subscription_ids),
                        ),
                    )
                    assets_by_venue.setdefault(venue, set()).update(subscription_ids)

            for venue, assets in sorted(assets_by_venue.items()):
                frozen = tuple(Target(asset_id=item) for item in sorted(assets))
                digest = target_digest(venue, frozen)
                connection.execute(
                    """INSERT INTO subscription_sets(
                           run_id, venue, target_digest, asset_count
                       ) VALUES (?, ?, ?, ?)""",
                    (run_id, venue, digest, len(assets)),
                )
                connection.executemany(
                    """INSERT INTO subscription_assets(
                           run_id, venue, asset_id
                       ) VALUES (?, ?, ?)""",
                    ((run_id, venue, item) for item in sorted(assets)),
                )

            _finish_source(
                connection, source_key, "targeter_run", source_sha256
            )
        return "ingested"

    def ingest_control_receipt(
        self,
        receipt: SegmentUniverseReceipt,
        envelopes: Iterable[Mapping[str, Any]],
        *,
        source_sha256: str,
    ) -> str:
        """Index one verified control sidecar and refold its complete lane."""
        source_key = receipt.key
        with self.write_transaction() as connection:
            if not _begin_source(
                connection, source_key, "segment_control", source_sha256
            ):
                return "skipped"
            connection.execute(
                """INSERT INTO segment_receipts(
                       receipt_key, lane_id, segment_id, segment_index,
                       window_start_ns, window_end_ns, data_key, control_key,
                       control_sha256, control_byte_length, control_line_count,
                       published_at_ns
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    receipt.key,
                    receipt.lane_id,
                    receipt.segment_id,
                    receipt.segment_index,
                    receipt.window_start_ns,
                    receipt.window_end_ns,
                    receipt.data_key,
                    receipt.control.key,
                    receipt.control.logical.sha256,
                    receipt.control.logical.byte_length,
                    receipt.control.logical.line_count,
                    receipt.published_at_ns,
                ),
            )
            previous_delivery: int | None = None
            first_delivery: int | None = None
            record_count = 0
            for raw_envelope in envelopes:
                envelope = dict(raw_envelope)
                parsed = _control_envelope(envelope)
                if previous_delivery is not None and parsed["delivery_index"] <= previous_delivery:
                    raise EvidenceConflict(
                        f"control sidecar {receipt.control.key} is not in delivery order"
                    )
                if first_delivery is None:
                    first_delivery = parsed["delivery_index"]
                previous_delivery = parsed["delivery_index"]
                values = (
                    receipt.lane_id,
                    parsed["delivery_index"],
                    parsed["record_id"],
                    parsed["visible_ns"],
                    parsed["monotonic_ns"],
                    parsed["venue"],
                    parsed["connection_epoch"],
                    parsed["local_counter"],
                    parsed["event"],
                    parsed["target_digest"],
                    parsed["target_metadata_digest"],
                    _canonical_json(parsed["detail"]),
                    _canonical_json(envelope),
                    receipt.key,
                )
                try:
                    connection.execute(
                        """INSERT INTO control_records(
                               lane_id, delivery_index, record_id, visible_ns,
                               monotonic_ns, venue, connection_epoch, local_counter,
                               event, target_digest, target_metadata_digest,
                               detail_json, envelope_json, receipt_key
                           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        values,
                    )
                except sqlite3.IntegrityError as error:
                    existing = connection.execute(
                        """SELECT envelope_json, receipt_key FROM control_records
                           WHERE lane_id = ? AND delivery_index = ?""",
                        (receipt.lane_id, parsed["delivery_index"]),
                    ).fetchone()
                    if existing is None or existing["envelope_json"] != values[12]:
                        raise EvidenceConflict(
                            f"lane {receipt.lane_id} delivery "
                            f"{parsed['delivery_index']} conflicts with prior evidence"
                        ) from error
                    raise EvidenceConflict(
                        f"control record is committed by two segments: "
                        f"{existing['receipt_key']} and {receipt.key}"
                    ) from error
                record_count += 1
            if record_count != receipt.control.logical.line_count:
                raise EvidenceConflict(
                    f"receipt says {receipt.control.logical.line_count} controls, "
                    f"decoded {record_count}"
                )
            if (
                first_delivery != receipt.control.first_delivery_index
                or previous_delivery != receipt.control.last_delivery_index
            ):
                raise EvidenceConflict(
                    f"control sidecar {receipt.control.key} delivery bounds disagree "
                    "with its receipt"
                )
            self._refold_lane(connection, receipt.lane_id)
            _finish_source(
                connection, source_key, "segment_control", source_sha256
            )
        return "ingested"

    def _refold_lane(self, connection: sqlite3.Connection, lane_id: str) -> None:
        connection.execute("DELETE FROM connection_epochs WHERE lane_id = ?", (lane_id,))
        rows = connection.execute(
            """SELECT * FROM control_records
               WHERE lane_id = ? ORDER BY delivery_index""",
            (lane_id,),
        ).fetchall()
        by_epoch: dict[str, list[sqlite3.Row]] = {}
        for row in rows:
            by_epoch.setdefault(row["connection_epoch"], []).append(row)
        ordered = sorted(
            by_epoch.items(), key=lambda item: item[1][0]["delivery_index"]
        )
        predecessor: str | None = None
        for epoch, controls in ordered:
            opened = next((row for row in controls if row["event"] == "connection_opened"), None)
            sent = next((row for row in controls if row["event"] == "subscription_sent"), None)
            accepted = next(
                (
                    row
                    for row in controls
                    if row["event"] in {"subscription_accepted", "venue_subscription_accepted"}
                ),
                None,
            )
            closed = next(
                (row for row in reversed(controls) if row["event"] == "connection_closed"),
                None,
            )
            digests = {
                row["target_digest"] for row in controls if row["target_digest"] is not None
            }
            digest = next(iter(digests)) if len(digests) == 1 else None
            digest_status = (
                "observed" if len(digests) == 1 else "unknown" if not digests else "ambiguous"
            )
            metadata_digests = {
                row["target_metadata_digest"]
                for row in controls
                if row["target_metadata_digest"] is not None
            }
            metadata_digest = (
                next(iter(metadata_digests)) if len(metadata_digests) == 1 else None
            )
            first, last = controls[0], controls[-1]
            connection.execute(
                """INSERT INTO connection_epochs(
                       lane_id, connection_epoch, venue, predecessor_epoch,
                       first_delivery_index, last_delivery_index,
                       observed_start_ns, observed_end_ns,
                       socket_status, socket_opened_delivery_index,
                       send_status, send_completed_delivery_index,
                       venue_acceptance_status, venue_acceptance_delivery_index,
                       close_status, closed_delivery_index,
                       target_digest, target_digest_status, target_metadata_digest
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    lane_id,
                    epoch,
                    first["venue"],
                    predecessor,
                    first["delivery_index"],
                    last["delivery_index"],
                    opened["visible_ns"] if opened is not None else first["visible_ns"],
                    closed["visible_ns"] if closed is not None else None,
                    "socket_opened" if opened is not None else "unknown",
                    opened["delivery_index"] if opened is not None else None,
                    "subscription_send_completed" if sent is not None else "unknown",
                    sent["delivery_index"] if sent is not None else None,
                    "venue_acceptance_observed" if accepted is not None else "unknown",
                    accepted["delivery_index"] if accepted is not None else None,
                    "closed_observed" if closed is not None else "unknown",
                    closed["delivery_index"] if closed is not None else None,
                    digest,
                    digest_status,
                    metadata_digest,
                ),
            )
            predecessor = epoch

    def set_checkpoint(self, name: str, cursor: str) -> None:
        now_ns = int(datetime.now(timezone.utc).timestamp() * 1_000_000_000)
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

    def status(self) -> dict[str, Any]:
        with closing(self.connect(readonly=True)) as connection:
            version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            counts = {
                table: int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
                for table in (
                    "ingest_sources",
                    "targeter_runs",
                    "event_bundles",
                    "catalog_markets",
                    "target_records",
                    "segment_receipts",
                    "control_records",
                    "connection_epochs",
                    "evidence_issues",
                )
            }
        return {"status": "ok", "schema_version": version, "counts": counts}

    def list_bundles(self, *, limit: int = 100) -> list[dict[str, Any]]:
        bounded = max(1, min(int(limit), 1000))
        with closing(self.connect(readonly=True)) as connection:
            rows = connection.execute(
                """WITH latest AS (
                       SELECT eb.bundle_id, MAX(tr.generated_at_ns) AS generated_at_ns
                       FROM event_bundles eb
                       JOIN targeter_runs tr USING (run_id)
                       GROUP BY eb.bundle_id
                   )
                   SELECT eb.bundle_id, eb.run_id, eb.selected, eb.activation_at,
                          eb.game, eb.confidence, tr.generated_at
                   FROM event_bundles eb
                   JOIN targeter_runs tr USING (run_id)
                   JOIN latest l ON l.bundle_id = eb.bundle_id
                                AND l.generated_at_ns = tr.generated_at_ns
                   ORDER BY tr.generated_at_ns DESC, eb.bundle_id
                   LIMIT ?""",
                (bounded,),
            ).fetchall()
        return [_row_record(row) for row in rows]

    def bundle_detail(self, bundle_id: str) -> dict[str, Any] | None:
        with closing(self.connect(readonly=True)) as connection:
            bundle = connection.execute(
                """SELECT eb.*, tr.generated_at, tr.input_complete
                   FROM event_bundles eb JOIN targeter_runs tr USING (run_id)
                   WHERE eb.bundle_id = ?
                   ORDER BY tr.generated_at_ns DESC LIMIT 1""",
                (bundle_id,),
            ).fetchone()
            if bundle is None:
                return None
            run_id = bundle["run_id"]
            events = connection.execute(
                """SELECT ce.record_json FROM bundle_events be
                   JOIN catalog_events ce USING (run_id, event_ref)
                   WHERE be.run_id = ? AND be.bundle_id = ?
                   ORDER BY ce.event_ref""",
                (run_id, bundle_id),
            ).fetchall()
            markets = connection.execute(
                """SELECT cm.record_json, tr.record_json AS target_record_json,
                          CASE WHEN ts.target_id IS NULL THEN 0 ELSE 1 END AS selected
                   FROM bundle_markets bm
                   JOIN catalog_markets cm USING (run_id, target_id)
                   LEFT JOIN target_records tr
                     ON tr.run_id = bm.run_id AND tr.target_id = bm.target_id
                   LEFT JOIN target_selections ts
                     ON ts.run_id = bm.run_id AND ts.target_id = bm.target_id
                   WHERE bm.run_id = ? AND bm.bundle_id = ?
                   ORDER BY cm.target_id""",
                (run_id, bundle_id),
            ).fetchall()
            subscriptions = self._bundle_subscriptions(connection, run_id, bundle_id)
            issues = connection.execute(
                """SELECT code, subject, detail FROM evidence_issues
                   WHERE source_key = (SELECT source_key FROM targeter_runs WHERE run_id = ?)
                     AND (subject = ? OR detail = ?)
                   ORDER BY code, subject""",
                (run_id, bundle_id, bundle_id),
            ).fetchall()
        record = json.loads(bundle["record_json"])
        return {
            "bundle": record,
            "run": {
                "run_id": run_id,
                "generated_at": bundle["generated_at"],
                "input_complete": bool(bundle["input_complete"]),
            },
            "events": [json.loads(row["record_json"]) for row in events],
            "markets": [
                {
                    **json.loads(row["record_json"]),
                    "selected": bool(row["selected"]),
                    "target_record": (
                        json.loads(row["target_record_json"])
                        if row["target_record_json"] is not None
                        else None
                    ),
                }
                for row in markets
            ],
            "subscriptions": subscriptions,
            "issues": [_row_record(row) for row in issues],
        }

    def _bundle_subscriptions(
        self, connection: sqlite3.Connection, run_id: str, bundle_id: str
    ) -> list[dict[str, Any]]:
        sets = connection.execute(
            """SELECT DISTINCT ss.venue, ss.target_digest, ss.asset_count
               FROM target_selections ts
               JOIN subscription_sets ss USING (run_id, venue)
               WHERE ts.run_id = ? AND ts.bundle_id = ?
               ORDER BY ss.venue""",
            (run_id, bundle_id),
        ).fetchall()
        output: list[dict[str, Any]] = []
        for item in sets:
            candidate_count = int(
                connection.execute(
                    """SELECT COUNT(*) FROM subscription_sets
                       WHERE venue = ? AND target_digest = ?""",
                    (item["venue"], item["target_digest"]),
                ).fetchone()[0]
            )
            epochs = connection.execute(
                """SELECT lane_id, connection_epoch, predecessor_epoch,
                          observed_start_ns, observed_end_ns, socket_status,
                          send_status, venue_acceptance_status, close_status,
                          target_digest_status, target_metadata_digest
                   FROM connection_epochs
                   WHERE venue = ? AND target_digest = ?
                   ORDER BY observed_start_ns, lane_id""",
                (item["venue"], item["target_digest"]),
            ).fetchall()
            output.append(
                {
                    "venue": item["venue"],
                    "target_digest": item["target_digest"],
                    "asset_count": item["asset_count"],
                    "selection_status": "selected",
                    "historical_link_status": (
                        "exact" if candidate_count == 1 else "ambiguous"
                    ),
                    "candidate_run_count": candidate_count,
                    "epochs": [_row_record(row) for row in epochs],
                }
            )
        return output

    def overlapping_segments(
        self,
        *,
        start_ns: int,
        end_ns: int,
        lane_id: str | None = None,
    ) -> list[dict[str, Any]]:
        if start_ns >= end_ns:
            raise ValueError("start_ns must be before end_ns")
        query = """SELECT * FROM segment_receipts
                   WHERE window_end_ns > ? AND window_start_ns < ?"""
        parameters: list[Any] = [start_ns, end_ns]
        if lane_id is not None:
            query += " AND lane_id = ?"
            parameters.append(lane_id)
        query += " ORDER BY window_start_ns, lane_id, segment_index"
        with closing(self.connect(readonly=True)) as connection:
            rows = connection.execute(query, parameters).fetchall()
        return [_row_record(row) for row in rows]

    def overlapping_epochs(
        self,
        *,
        start_ns: int,
        end_ns: int,
        lane_id: str | None = None,
    ) -> list[dict[str, Any]]:
        if start_ns >= end_ns:
            raise ValueError("start_ns must be before end_ns")
        query = """SELECT * FROM connection_epochs
                   WHERE (observed_end_ns IS NULL OR observed_end_ns > ?)
                     AND observed_start_ns < ?"""
        parameters: list[Any] = [start_ns, end_ns]
        if lane_id is not None:
            query += " AND lane_id = ?"
            parameters.append(lane_id)
        query += " ORDER BY observed_start_ns, lane_id, first_delivery_index"
        with closing(self.connect(readonly=True)) as connection:
            rows = connection.execute(query, parameters).fetchall()
        return [_row_record(row) for row in rows]

    def backup(self, destination: Path) -> Path:
        """Create a consistent SQLite-native backup through an atomic rename."""
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
                sink.execute("PRAGMA wal_checkpoint(TRUNCATE)")
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


def file_sha256(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    length = 0
    with Path(path).open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
            length += len(chunk)
    return digest.hexdigest(), length


def _begin_source(
    connection: sqlite3.Connection,
    source_key: str,
    source_type: str,
    identity_sha256: str,
) -> bool:
    row = connection.execute(
        "SELECT source_type, identity_sha256 FROM ingest_sources WHERE source_key = ?",
        (source_key,),
    ).fetchone()
    if row is None:
        return True
    if row["source_type"] != source_type or row["identity_sha256"] != identity_sha256:
        raise EvidenceConflict(f"immutable source {source_key!r} conflicts with prior ingestion")
    return False


def _finish_source(
    connection: sqlite3.Connection,
    source_key: str,
    source_type: str,
    identity_sha256: str,
) -> None:
    now_ns = int(datetime.now(timezone.utc).timestamp() * 1_000_000_000)
    connection.execute(
        """INSERT INTO ingest_sources(
               source_key, source_type, identity_sha256, ingested_at_ns
           ) VALUES (?, ?, ?, ?)""",
        (source_key, source_type, identity_sha256, now_ns),
    )


def _issue(
    connection: sqlite3.Connection,
    source_key: str,
    code: str,
    subject: str,
    detail: str,
) -> None:
    connection.execute(
        """INSERT OR IGNORE INTO evidence_issues(
               source_key, code, subject, detail
           ) VALUES (?, ?, ?, ?)""",
        (source_key, code, subject, detail),
    )


def _control_envelope(envelope: Mapping[str, Any]) -> dict[str, Any]:
    if envelope.get("kind") != "control":
        raise EvidenceConflict("control sidecar contains a non-control envelope")
    required_text = ("record_id", "venue", "connection_epoch")
    for field in required_text:
        _text(envelope, field, "control envelope")
    integers: dict[str, int | None] = {}
    for field in ("delivery_index", "visible_ns", "local_counter"):
        value = envelope.get(field)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise EvidenceConflict(f"control envelope {field} is invalid")
        integers[field] = value
    monotonic = envelope.get("monotonic_ns")
    if monotonic is not None and (
        not isinstance(monotonic, int) or isinstance(monotonic, bool) or monotonic < 0
    ):
        raise EvidenceConflict("control envelope monotonic_ns is invalid")
    raw_payload = envelope.get("raw_payload")
    if not isinstance(raw_payload, str):
        raise EvidenceConflict("control envelope raw_payload is not text")
    try:
        detail = json.loads(raw_payload)
    except json.JSONDecodeError as error:
        raise EvidenceConflict("control envelope payload is invalid JSON") from error
    if not isinstance(detail, dict):
        raise EvidenceConflict("control envelope payload is not an object")
    event = _text(detail, "event", "control payload")
    return {
        **integers,
        "monotonic_ns": monotonic,
        "record_id": envelope["record_id"],
        "venue": envelope["venue"],
        "connection_epoch": envelope["connection_epoch"],
        "event": event,
        "target_digest": _optional_text(detail.get("target_digest")),
        "target_metadata_digest": _optional_text(detail.get("target_metadata_digest")),
        "detail": detail,
    }


def _timestamp_ns(value: str) -> int:
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as error:
        raise EvidenceConflict(f"invalid timestamp {value!r}") from error
    if parsed.tzinfo is None:
        raise EvidenceConflict(f"timestamp {value!r} has no timezone")
    return int(parsed.timestamp() * 1_000_000_000)


def _text(record: Mapping[str, Any], field: str, label: str) -> str:
    value = record.get(field)
    if not isinstance(value, str) or not value:
        raise EvidenceConflict(f"{label} {field} must be non-empty text")
    return value


def _optional_text(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _text_list(value: Any, label: str, *, allow_empty: bool = False) -> list[str]:
    if not isinstance(value, (list, tuple)) or (
        not allow_empty and not value
    ) or any(not isinstance(item, str) or not item for item in value):
        raise EvidenceConflict(f"{label} must be an array of non-empty strings")
    if len(value) != len(set(value)):
        raise EvidenceConflict(f"{label} contains duplicates")
    return list(value)


def _canonical_json(value: Any) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
            allow_nan=False,
        )
    except (TypeError, ValueError) as error:
        raise EvidenceConflict(f"evidence is not finite JSON: {error}") from error


def _row_record(row: sqlite3.Row) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}


_SCHEMA_V1 = """
CREATE TABLE ingest_sources (
    source_key TEXT PRIMARY KEY,
    source_type TEXT NOT NULL,
    identity_sha256 TEXT NOT NULL,
    ingested_at_ns INTEGER NOT NULL
) STRICT;

CREATE TABLE checkpoints (
    name TEXT PRIMARY KEY,
    cursor TEXT NOT NULL,
    updated_at_ns INTEGER NOT NULL
) STRICT;

CREATE TABLE targeter_runs (
    run_id TEXT PRIMARY KEY,
    source_key TEXT NOT NULL UNIQUE REFERENCES ingest_sources(source_key) DEFERRABLE INITIALLY DEFERRED,
    generated_at TEXT NOT NULL,
    generated_at_ns INTEGER NOT NULL,
    input_complete INTEGER NOT NULL CHECK(input_complete IN (0, 1)),
    report_json TEXT NOT NULL
) STRICT;

CREATE TABLE catalog_events (
    run_id TEXT NOT NULL REFERENCES targeter_runs(run_id) ON DELETE CASCADE,
    event_ref TEXT NOT NULL,
    venue TEXT NOT NULL,
    venue_event_id TEXT NOT NULL,
    title TEXT,
    sport TEXT,
    league TEXT,
    game TEXT,
    activation_at TEXT,
    record_json TEXT NOT NULL,
    PRIMARY KEY(run_id, event_ref)
) STRICT;

CREATE TABLE catalog_markets (
    run_id TEXT NOT NULL,
    target_id TEXT NOT NULL,
    event_ref TEXT NOT NULL,
    venue TEXT NOT NULL,
    venue_market_id TEXT NOT NULL,
    canonical_class TEXT,
    market_type TEXT,
    scope TEXT,
    title TEXT,
    subscription_ids_json TEXT NOT NULL,
    record_json TEXT NOT NULL,
    PRIMARY KEY(run_id, target_id),
    FOREIGN KEY(run_id, event_ref) REFERENCES catalog_events(run_id, event_ref) ON DELETE CASCADE
) STRICT;

CREATE TABLE event_bundles (
    run_id TEXT NOT NULL REFERENCES targeter_runs(run_id) ON DELETE CASCADE,
    bundle_id TEXT NOT NULL,
    selected INTEGER NOT NULL CHECK(selected IN (0, 1)),
    activation_at TEXT,
    game TEXT,
    confidence TEXT,
    record_json TEXT NOT NULL,
    PRIMARY KEY(run_id, bundle_id)
) STRICT;

CREATE TABLE target_records (
    run_id TEXT NOT NULL,
    target_id TEXT NOT NULL,
    venue TEXT NOT NULL,
    observed_at TEXT,
    subscription_ids_json TEXT NOT NULL,
    record_sha256 TEXT,
    record_json TEXT NOT NULL,
    PRIMARY KEY(run_id, target_id),
    FOREIGN KEY(run_id, target_id) REFERENCES catalog_markets(run_id, target_id) ON DELETE CASCADE
) STRICT;

CREATE TABLE bundle_events (
    run_id TEXT NOT NULL,
    bundle_id TEXT NOT NULL,
    event_ref TEXT NOT NULL,
    PRIMARY KEY(run_id, bundle_id, event_ref),
    FOREIGN KEY(run_id, bundle_id) REFERENCES event_bundles(run_id, bundle_id) ON DELETE CASCADE,
    FOREIGN KEY(run_id, event_ref) REFERENCES catalog_events(run_id, event_ref) ON DELETE CASCADE
) STRICT;

CREATE TABLE bundle_markets (
    run_id TEXT NOT NULL,
    bundle_id TEXT NOT NULL,
    target_id TEXT NOT NULL,
    PRIMARY KEY(run_id, bundle_id, target_id),
    FOREIGN KEY(run_id, bundle_id) REFERENCES event_bundles(run_id, bundle_id) ON DELETE CASCADE,
    FOREIGN KEY(run_id, target_id) REFERENCES catalog_markets(run_id, target_id) ON DELETE CASCADE
) STRICT;

CREATE TABLE target_selections (
    run_id TEXT NOT NULL,
    venue TEXT NOT NULL,
    target_id TEXT NOT NULL,
    bundle_id TEXT NOT NULL,
    subscription_ids_json TEXT NOT NULL,
    PRIMARY KEY(run_id, target_id),
    FOREIGN KEY(run_id, target_id) REFERENCES catalog_markets(run_id, target_id) ON DELETE CASCADE,
    FOREIGN KEY(run_id, bundle_id) REFERENCES event_bundles(run_id, bundle_id) ON DELETE CASCADE
) STRICT;

CREATE TABLE subscription_sets (
    run_id TEXT NOT NULL REFERENCES targeter_runs(run_id) ON DELETE CASCADE,
    venue TEXT NOT NULL,
    target_digest TEXT NOT NULL,
    asset_count INTEGER NOT NULL,
    PRIMARY KEY(run_id, venue)
) STRICT;
CREATE INDEX subscription_sets_digest ON subscription_sets(venue, target_digest);

CREATE TABLE subscription_assets (
    run_id TEXT NOT NULL,
    venue TEXT NOT NULL,
    asset_id TEXT NOT NULL,
    PRIMARY KEY(run_id, venue, asset_id),
    FOREIGN KEY(run_id, venue) REFERENCES subscription_sets(run_id, venue) ON DELETE CASCADE
) STRICT;

CREATE TABLE segment_receipts (
    receipt_key TEXT PRIMARY KEY,
    lane_id TEXT NOT NULL,
    segment_id TEXT NOT NULL,
    segment_index INTEGER NOT NULL,
    window_start_ns INTEGER NOT NULL,
    window_end_ns INTEGER NOT NULL,
    data_key TEXT NOT NULL UNIQUE,
    control_key TEXT NOT NULL UNIQUE,
    control_sha256 TEXT NOT NULL,
    control_byte_length INTEGER NOT NULL,
    control_line_count INTEGER NOT NULL,
    published_at_ns INTEGER NOT NULL,
    UNIQUE(lane_id, segment_id)
) STRICT;
CREATE INDEX segment_interval ON segment_receipts(window_start_ns, window_end_ns, lane_id);

CREATE TABLE control_records (
    lane_id TEXT NOT NULL,
    delivery_index INTEGER NOT NULL,
    record_id TEXT NOT NULL,
    visible_ns INTEGER NOT NULL,
    monotonic_ns INTEGER,
    venue TEXT NOT NULL,
    connection_epoch TEXT NOT NULL,
    local_counter INTEGER NOT NULL,
    event TEXT NOT NULL,
    target_digest TEXT,
    target_metadata_digest TEXT,
    detail_json TEXT NOT NULL,
    envelope_json TEXT NOT NULL,
    receipt_key TEXT NOT NULL REFERENCES segment_receipts(receipt_key),
    PRIMARY KEY(lane_id, delivery_index),
    UNIQUE(record_id)
) STRICT;
CREATE INDEX controls_epoch ON control_records(lane_id, connection_epoch, delivery_index);

CREATE TABLE connection_epochs (
    lane_id TEXT NOT NULL,
    connection_epoch TEXT NOT NULL,
    venue TEXT NOT NULL,
    predecessor_epoch TEXT,
    first_delivery_index INTEGER NOT NULL,
    last_delivery_index INTEGER NOT NULL,
    observed_start_ns INTEGER NOT NULL,
    observed_end_ns INTEGER,
    socket_status TEXT NOT NULL,
    socket_opened_delivery_index INTEGER,
    send_status TEXT NOT NULL,
    send_completed_delivery_index INTEGER,
    venue_acceptance_status TEXT NOT NULL,
    venue_acceptance_delivery_index INTEGER,
    close_status TEXT NOT NULL,
    closed_delivery_index INTEGER,
    target_digest TEXT,
    target_digest_status TEXT NOT NULL,
    target_metadata_digest TEXT,
    PRIMARY KEY(lane_id, connection_epoch)
) STRICT;
CREATE INDEX epochs_digest ON connection_epochs(venue, target_digest);
CREATE INDEX epochs_interval ON connection_epochs(observed_start_ns, observed_end_ns, lane_id);

CREATE TABLE evidence_issues (
    source_key TEXT NOT NULL,
    code TEXT NOT NULL,
    subject TEXT NOT NULL,
    detail TEXT NOT NULL,
    PRIMARY KEY(source_key, code, subject, detail)
) STRICT;
"""
