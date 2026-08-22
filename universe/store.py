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
from targeter.v2.selected_bundles import SELECTED_BUNDLE_INDEX_VERSION

SCHEMA_VERSION = 3
STALE_AFTER_SECONDS = 3_600
SQLITE_CONTENT_TYPE = "application/vnd.sqlite3"
SCHEMA_PATH = Path(__file__).with_name("schema") / "v3.sql"


class EvidenceConflict(ValueError):
    """An immutable identity or indexed evidence disagrees with prior state."""


class UniverseStore:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self.connect()) as connection:
            version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            if version == 0:
                connection.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
                connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
                connection.commit()
            elif version != SCHEMA_VERSION:
                raise EvidenceConflict(
                    f"unsupported universe schema version {version}; "
                    "initialize a fresh v3 database"
                )

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

    def replace_active_snapshot(
        self,
        *,
        manifest_key: str,
        manifest_sha256: str,
        report_key: str,
        report_sha256: str,
        report_byte_length: int,
        index_key: str,
        index_sha256: str,
        index_byte_length: int,
        run_id: str,
        generated_at: str,
        input_complete: bool,
        rows: Iterable[Mapping[str, Any]],
    ) -> str:
        """Atomically replace active bundles with one complete archived v3 run."""
        if input_complete is not True:
            raise EvidenceConflict("active snapshot input must be complete")
        records = [_active_bundle_row(dict(item)) for item in rows]
        strategy_versions = {record["strategy_version"] for record in records}
        if len(strategy_versions) > 1:
            raise EvidenceConflict("selected bundle index mixes strategy versions")
        seen_bundles: set[str] = set()
        for record in records:
            bundle_id = record["bundle_id"]
            if bundle_id in seen_bundles:
                raise EvidenceConflict(f"run {run_id} repeats bundle {bundle_id}")
            seen_bundles.add(bundle_id)
            if record["run_id"] != run_id or record["generated_at"] != generated_at:
                raise EvidenceConflict(
                    f"selected bundle {bundle_id} disagrees with its run manifest"
                )
        generated_ns = _timestamp_ns(generated_at)
        indexed_at_ns = int(datetime.now(timezone.utc).timestamp() * 1_000_000_000)
        with self.write_transaction() as connection:
            existing = connection.execute("SELECT * FROM active_snapshot").fetchone()
            if existing is not None:
                if existing["run_id"] == run_id:
                    if (
                        existing["manifest_key"] != manifest_key
                        or existing["manifest_sha256"] != manifest_sha256
                        or existing["report_key"] != report_key
                        or existing["report_sha256"] != report_sha256
                        or existing["index_key"] != index_key
                        or existing["index_sha256"] != index_sha256
                    ):
                        raise EvidenceConflict(
                            f"active run {run_id} changed immutable identities"
                        )
                    return "skipped"
                if existing["run_id"] > run_id:
                    return "stale"

            connection.execute("DELETE FROM active_bundles")
            connection.execute("DELETE FROM subscription_sets")
            connection.execute("DELETE FROM active_snapshot")
            connection.execute(
                """INSERT INTO active_snapshot(
                       singleton, run_id, generated_at, generated_at_ns,
                       indexed_at_ns, strategy_version, manifest_key,
                       manifest_sha256, report_key, report_sha256,
                       report_byte_length, index_key, index_sha256,
                       index_byte_length
                   ) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    run_id,
                    generated_at,
                    generated_ns,
                    indexed_at_ns,
                    next(iter(strategy_versions), None),
                    manifest_key,
                    manifest_sha256,
                    report_key,
                    report_sha256,
                    report_byte_length,
                    index_key,
                    index_sha256,
                    index_byte_length,
                ),
            )

            assets_by_venue: dict[str, set[str]] = {}
            for record in records:
                bundle_id = record["bundle_id"]
                connection.execute(
                    """INSERT INTO active_bundles(
                           bundle_id, origin_run_id, origin_generated_at,
                           origin_generated_at_ns, origin_manifest_key,
                           origin_manifest_sha256, origin_report_key,
                           origin_report_sha256, origin_report_byte_length,
                           origin_index_key, origin_index_sha256,
                           origin_index_byte_length, continuity_selected,
                           continuity_disposition, sport, game, topology,
                           activation_at, activation_at_ns, capture_start_at,
                           capture_start_at_ns, planned_capture_end_at,
                           planned_capture_end_at_ns,
                           post_start_retention_seconds
                       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                                 ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        bundle_id,
                        record["origin_run_id"],
                        record["origin_generated_at"],
                        _timestamp_ns(record["origin_generated_at"]),
                        record["origin_manifest_key"],
                        record["origin_manifest_sha256"],
                        record["origin_report_key"],
                        record["origin_report_sha256"],
                        record["origin_report_byte_length"],
                        record["origin_index_key"],
                        record["origin_index_sha256"],
                        record["origin_index_byte_length"],
                        int(record["continuity_selected"]),
                        record["continuity_disposition"],
                        record["sport"],
                        record["game"],
                        record["topology"],
                        record["activation_at"],
                        _timestamp_ns(record["activation_at"]),
                        record["capture_start_at"],
                        _timestamp_ns(record["capture_start_at"]),
                        record["planned_capture_end_at"],
                        _timestamp_ns(record["planned_capture_end_at"]),
                        record["post_start_retention_seconds"],
                    ),
                )
                connection.executemany(
                    """INSERT INTO bundle_participants(
                           bundle_id, position, name, participant_key
                       ) VALUES (?, ?, ?, ?)""",
                    (
                        (
                            bundle_id,
                            position,
                            name,
                            record["participant_keys"][position],
                        )
                        for position, name in enumerate(record["participants"])
                    ),
                )
                connection.executemany(
                    """INSERT INTO bundle_events(bundle_id, event_ref, venue)
                       VALUES (?, ?, ?)""",
                    (
                        (bundle_id, event_ref, _venue_prefix(event_ref))
                        for event_ref in record["event_refs"]
                    ),
                )
                connection.executemany(
                    """INSERT INTO bundle_markets(
                           bundle_id, target_id, venue, selected
                       ) VALUES (?, ?, ?, ?)""",
                    (
                        (
                            bundle_id,
                            market["target_id"],
                            market["venue"],
                            int(market["selected"]),
                        )
                        for market in record["markets"]
                    ),
                )
                for target in record["targets"]:
                    connection.execute(
                        """INSERT INTO selected_targets(
                               bundle_id, target_id, venue, canonical_class,
                               source_ref
                           ) VALUES (?, ?, ?, ?, ?)""",
                        (
                            bundle_id,
                            target["target_id"],
                            target["venue"],
                            target["canonical_class"],
                            target["source_ref"],
                        ),
                    )
                    connection.executemany(
                        """INSERT INTO selected_target_assets(target_id, asset_id)
                           VALUES (?, ?)""",
                        (
                            (target["target_id"], asset_id)
                            for asset_id in target["subscription_ids"]
                        ),
                    )
                    assets_by_venue.setdefault(target["venue"], set()).update(
                        target["subscription_ids"]
                    )
                connection.executemany(
                    """INSERT INTO bundle_relationships(
                           bundle_id, relationship_index, left_market,
                           right_market, relationship, scope, left_venue,
                           right_venue, coverage
                       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        (
                            bundle_id,
                            position,
                            relationship["left"],
                            relationship["right"],
                            relationship["relationship"],
                            relationship["scope"],
                            relationship["left_venue"],
                            relationship["right_venue"],
                            relationship["coverage"],
                        )
                        for position, relationship in enumerate(
                            record["relationships"]
                        )
                    ),
                )

            for venue, assets in sorted(assets_by_venue.items()):
                frozen = tuple(Target(asset_id=item) for item in sorted(assets))
                digest = target_digest(venue, frozen)
                connection.execute(
                    """INSERT INTO subscription_sets(
                           venue, target_digest, asset_count
                       ) VALUES (?, ?, ?)""",
                    (venue, digest, len(assets)),
                )
                connection.executemany(
                    """INSERT INTO subscription_assets(venue, asset_id)
                       VALUES (?, ?)""",
                    ((venue, item) for item in sorted(assets)),
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
                    hashlib.sha256(_canonical_json(envelope).encode("utf-8")).hexdigest(),
                    parsed["visible_ns"],
                    parsed["monotonic_ns"],
                    parsed["venue"],
                    parsed["connection_epoch"],
                    parsed["local_counter"],
                    parsed["event"],
                    parsed["target_digest"],
                    parsed["target_metadata_digest"],
                    parsed["target_count"],
                    receipt.key,
                )
                try:
                    connection.execute(
                        """INSERT INTO control_records(
                               lane_id, delivery_index, record_id, envelope_sha256,
                               visible_ns, monotonic_ns, venue, connection_epoch,
                               local_counter, event, target_digest,
                               target_metadata_digest, target_count, receipt_key
                           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        values,
                    )
                except sqlite3.IntegrityError as error:
                    existing = connection.execute(
                        """SELECT envelope_sha256, receipt_key FROM control_records
                           WHERE lane_id = ? AND delivery_index = ?""",
                        (receipt.lane_id, parsed["delivery_index"]),
                    ).fetchone()
                    if existing is None or existing["envelope_sha256"] != values[3]:
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

    def status(self, *, now_ns: int | None = None) -> dict[str, Any]:
        observed_ns = (
            now_ns
            if now_ns is not None
            else int(datetime.now(timezone.utc).timestamp() * 1_000_000_000)
        )
        with closing(self.connect(readonly=True)) as connection:
            version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            counts = {
                table: int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
                for table in (
                    "ingest_sources",
                    "active_bundles",
                    "bundle_markets",
                    "selected_targets",
                    "segment_receipts",
                    "control_records",
                    "connection_epochs",
                )
            }
            snapshot = connection.execute("SELECT * FROM active_snapshot").fetchone()
        active = None
        if snapshot is not None:
            age_seconds = max(
                0, (observed_ns - int(snapshot["generated_at_ns"])) // 1_000_000_000
            )
            active = {
                "run_id": snapshot["run_id"],
                "generated_at": snapshot["generated_at"],
                "indexed_at_ns": snapshot["indexed_at_ns"],
                "age_seconds": age_seconds,
                "stale_after_seconds": STALE_AFTER_SECONDS,
                "stale": age_seconds >= STALE_AFTER_SECONDS,
            }
        return {
            "status": "ok",
            "schema_version": version,
            "active_snapshot": active,
            "counts": counts,
        }

    def list_bundles(
        self,
        *,
        activation_start_ns: int | None = None,
        activation_end_ns: int | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        for field, value in (
            ("activation_start_ns", activation_start_ns),
            ("activation_end_ns", activation_end_ns),
        ):
            if value is not None and (
                not isinstance(value, int) or isinstance(value, bool) or value < 0
            ):
                raise ValueError(f"{field} must be a non-negative integer")
        if (
            activation_start_ns is not None
            and activation_end_ns is not None
            and activation_start_ns >= activation_end_ns
        ):
            raise ValueError("activation_start_ns must be before activation_end_ns")
        bounded = max(1, min(int(limit), 1000))
        predicates: list[str] = []
        parameters: list[Any] = []
        if activation_start_ns is not None:
            predicates.append("ab.activation_at_ns >= ?")
            parameters.append(activation_start_ns)
        if activation_end_ns is not None:
            predicates.append("ab.activation_at_ns < ?")
            parameters.append(activation_end_ns)
        where = f"WHERE {' AND '.join(predicates)}" if predicates else ""
        with closing(self.connect(readonly=True)) as connection:
            rows = connection.execute(
                f"""SELECT ab.bundle_id, snapshot.run_id, ab.origin_run_id,
                          ab.continuity_selected, ab.continuity_disposition,
                          ab.sport, ab.game, ab.topology, ab.activation_at,
                          ab.capture_start_at, ab.planned_capture_end_at,
                          snapshot.generated_at
                   FROM active_bundles ab
                   CROSS JOIN active_snapshot snapshot
                   {where}
                   ORDER BY ab.activation_at_ns, ab.bundle_id
                   LIMIT ?""",
                (*parameters, bounded),
            ).fetchall()
        return [
            {
                **_row_record(row),
                "continuity_selected": bool(row["continuity_selected"]),
            }
            for row in rows
        ]

    def bundle_detail(self, bundle_id: str) -> dict[str, Any] | None:
        with closing(self.connect(readonly=True)) as connection:
            bundle = connection.execute(
                """SELECT ab.*, snapshot.run_id, snapshot.generated_at,
                          snapshot.indexed_at_ns, snapshot.manifest_key,
                          snapshot.manifest_sha256, snapshot.report_key,
                          snapshot.report_sha256, snapshot.report_byte_length,
                          snapshot.index_key, snapshot.index_sha256,
                          snapshot.index_byte_length
                   FROM active_bundles ab
                   CROSS JOIN active_snapshot snapshot
                   WHERE ab.bundle_id = ?""",
                (bundle_id,),
            ).fetchone()
            if bundle is None:
                return None
            participants = connection.execute(
                """SELECT position, name, participant_key FROM bundle_participants
                   WHERE bundle_id = ? ORDER BY position""",
                (bundle_id,),
            ).fetchall()
            events = connection.execute(
                """SELECT event_ref, venue FROM bundle_events
                   WHERE bundle_id = ? ORDER BY event_ref""",
                (bundle_id,),
            ).fetchall()
            markets = connection.execute(
                """SELECT bm.target_id, bm.venue, bm.selected,
                          st.canonical_class, st.source_ref
                   FROM bundle_markets bm
                   LEFT JOIN selected_targets st
                     ON st.target_id = bm.target_id
                   WHERE bm.bundle_id = ?
                   ORDER BY bm.target_id""",
                (bundle_id,),
            ).fetchall()
            relationships = connection.execute(
                """SELECT left_market, right_market, relationship, scope,
                          left_venue, right_venue, coverage
                   FROM bundle_relationships
                   WHERE bundle_id = ?
                   ORDER BY relationship_index""",
                (bundle_id,),
            ).fetchall()
            subscriptions = self._bundle_subscriptions(connection, bundle_id)
            market_records: list[dict[str, Any]] = []
            for market in markets:
                assets = connection.execute(
                    """SELECT asset_id FROM selected_target_assets
                       WHERE target_id = ? ORDER BY asset_id""",
                    (market["target_id"],),
                ).fetchall()
                market_records.append(
                    {
                        **_row_record(market),
                        "selected": bool(market["selected"]),
                        "subscription_ids": [item["asset_id"] for item in assets],
                    }
                )
        return {
            "bundle": {
                key: bundle[key]
                for key in (
                    "bundle_id",
                    "sport",
                    "game",
                    "topology",
                    "activation_at",
                    "capture_start_at",
                    "planned_capture_end_at",
                    "post_start_retention_seconds",
                )
            },
            "run": {
                "run_id": bundle["run_id"],
                "generated_at": bundle["generated_at"],
                "indexed_at_ns": bundle["indexed_at_ns"],
            },
            "continuity": {
                "selected": bool(bundle["continuity_selected"]),
                "disposition": bundle["continuity_disposition"],
            },
            "participants": [_row_record(row) for row in participants],
            "events": [_row_record(row) for row in events],
            "markets": market_records,
            "relationships": [_row_record(row) for row in relationships],
            "subscriptions": subscriptions,
            "source": {
                "manifest_key": bundle["manifest_key"],
                "manifest_sha256": bundle["manifest_sha256"],
                "report_key": bundle["report_key"],
                "report_sha256": bundle["report_sha256"],
                "report_byte_length": bundle["report_byte_length"],
                "index_key": bundle["index_key"],
                "index_sha256": bundle["index_sha256"],
                "index_byte_length": bundle["index_byte_length"],
            },
            "origin": {
                key.removeprefix("origin_"): bundle[key]
                for key in (
                    "origin_run_id",
                    "origin_generated_at",
                    "origin_manifest_key",
                    "origin_manifest_sha256",
                    "origin_report_key",
                    "origin_report_sha256",
                    "origin_report_byte_length",
                    "origin_index_key",
                    "origin_index_sha256",
                    "origin_index_byte_length",
                )
            },
        }

    def _bundle_subscriptions(
        self, connection: sqlite3.Connection, bundle_id: str
    ) -> list[dict[str, Any]]:
        sets = connection.execute(
            """SELECT DISTINCT ss.venue, ss.target_digest, ss.asset_count
               FROM selected_targets st
               JOIN subscription_sets ss USING (venue)
               WHERE st.bundle_id = ?
               ORDER BY ss.venue""",
            (bundle_id,),
        ).fetchall()
        output: list[dict[str, Any]] = []
        for item in sets:
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

    def segments_for_bundle(
        self, bundle_id: str, *, lane_id: str | None = None
    ) -> list[dict[str, Any]] | None:
        """Return archived segments overlapping the active selected bundle window."""
        with closing(self.connect(readonly=True)) as connection:
            bounds = connection.execute(
                """SELECT capture_start_at_ns, planned_capture_end_at_ns
                   FROM active_bundles WHERE bundle_id = ?""",
                (bundle_id,),
            ).fetchone()
        if bounds is None:
            return None
        return self.overlapping_segments(
            start_ns=bounds["capture_start_at_ns"],
            end_ns=bounds["planned_capture_end_at_ns"],
            lane_id=lane_id,
        )

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


def _active_bundle_row(record: dict[str, Any]) -> dict[str, Any]:
    expected = {
        "selected_bundle_index_version",
        "occurrence_kind",
        "run_id",
        "generated_at",
        "input_complete",
        "strategy_version",
        "post_start_retention_seconds",
        "bundle_id",
        "origin_run_id",
        "origin_generated_at",
        "origin_manifest_key",
        "origin_manifest_sha256",
        "origin_report_key",
        "origin_report_sha256",
        "origin_report_byte_length",
        "origin_index_key",
        "origin_index_sha256",
        "origin_index_byte_length",
        "continuity_selected",
        "continuity_disposition",
        "sport",
        "game",
        "topology",
        "participants",
        "participant_keys",
        "activation_at",
        "capture_start_at",
        "planned_capture_end_at",
        "event_refs",
        "markets",
        "targets",
        "relationships",
    }
    if set(record) != expected:
        raise EvidenceConflict("selected bundle index row fields are invalid")
    if record["selected_bundle_index_version"] != SELECTED_BUNDLE_INDEX_VERSION:
        raise EvidenceConflict("unsupported selected bundle index version")
    if record["occurrence_kind"] not in {"complete", "retained"}:
        raise EvidenceConflict("selected bundle occurrence kind is invalid")
    for field in (
        "run_id",
        "generated_at",
        "bundle_id",
        "origin_run_id",
        "origin_generated_at",
        "origin_manifest_key",
        "origin_manifest_sha256",
        "origin_report_key",
        "origin_report_sha256",
        "origin_index_key",
        "origin_index_sha256",
        "sport",
        "activation_at",
        "capture_start_at",
        "planned_capture_end_at",
    ):
        _text(record, field, "selected bundle")
    if record["input_complete"] is not True:
        raise EvidenceConflict("active selected bundle input must be complete")
    if not isinstance(record["continuity_selected"], bool):
        raise EvidenceConflict("selected bundle continuity flag must be boolean")
    disposition = record["continuity_disposition"]
    if record["occurrence_kind"] == "retained":
        if not record["continuity_selected"] or disposition != "retained":
            raise EvidenceConflict("retained bundle continuity provenance is invalid")
    elif disposition not in {None, "held_current_candidate"} or (
        record["continuity_selected"] != (disposition is not None)
    ):
        raise EvidenceConflict("complete bundle continuity provenance is invalid")
    for field in (
        "strategy_version",
        "post_start_retention_seconds",
        "origin_report_byte_length",
        "origin_index_byte_length",
    ):
        value = record[field]
        minimum = 1 if field in {"strategy_version", "post_start_retention_seconds"} else 0
        if (
            not isinstance(value, int)
            or isinstance(value, bool)
            or value < minimum
        ):
            raise EvidenceConflict(f"selected bundle {field} is invalid")
    for field in ("game", "topology"):
        value = record[field]
        if value is not None and (not isinstance(value, str) or not value):
            raise EvidenceConflict(f"selected bundle {field} is invalid")
    participants = _text_list(record["participants"], "selected bundle participants")
    participant_keys = _text_list(
        record["participant_keys"], "selected bundle participant_keys"
    )
    if len(participants) != 2 or len(participant_keys) != 2:
        raise EvidenceConflict("selected bundle must contain two participants")
    event_refs = _text_list(
        record["event_refs"], "selected bundle event_refs"
    )

    markets = record["markets"]
    if not isinstance(markets, list) or not markets:
        raise EvidenceConflict("selected bundle markets must be a non-empty array")
    parsed_markets: list[dict[str, Any]] = []
    market_ids: set[str] = set()
    selected_market_ids: set[str] = set()
    for market in markets:
        if not isinstance(market, dict) or set(market) != {
            "target_id",
            "venue",
            "selected",
        }:
            raise EvidenceConflict("selected bundle market fields are invalid")
        target_id = _text(market, "target_id", "selected bundle market")
        venue = _text(market, "venue", "selected bundle market")
        if _venue_prefix(target_id) != venue or target_id in market_ids:
            raise EvidenceConflict(f"selected bundle market {target_id} is invalid")
        if not isinstance(market["selected"], bool):
            raise EvidenceConflict(f"selected bundle market {target_id} selected is invalid")
        market_ids.add(target_id)
        if market["selected"]:
            selected_market_ids.add(target_id)
        parsed_markets.append(
            {"target_id": target_id, "venue": venue, "selected": market["selected"]}
        )

    targets = record["targets"]
    if not isinstance(targets, list) or not targets:
        raise EvidenceConflict("selected bundle targets must be a non-empty array")
    parsed_targets: list[dict[str, Any]] = []
    target_ids: set[str] = set()
    for target in targets:
        if not isinstance(target, dict) or set(target) != {
            "venue",
            "target_id",
            "canonical_class",
            "subscription_ids",
            "activation_at",
            "capture_start_at",
            "source_ref",
        }:
            raise EvidenceConflict("selected bundle target fields are invalid")
        target_id = _text(target, "target_id", "selected bundle target")
        venue = _text(target, "venue", "selected bundle target")
        canonical_class = _text(
            target, "canonical_class", "selected bundle target"
        )
        source_ref = _text(target, "source_ref", "selected bundle target")
        if (
            target.get("activation_at") != record["activation_at"]
            or target.get("capture_start_at") != record["capture_start_at"]
        ):
            raise EvidenceConflict(
                f"selected bundle target {target_id} timing disagrees with its bundle"
            )
        assets = _text_list(
            target["subscription_ids"], "selected bundle target subscription_ids"
        )
        if (
            _venue_prefix(target_id) != venue
            or target_id in target_ids
            or target_id not in selected_market_ids
        ):
            raise EvidenceConflict(f"selected bundle target {target_id} is invalid")
        target_ids.add(target_id)
        parsed_targets.append(
            {
                "venue": venue,
                "target_id": target_id,
                "canonical_class": canonical_class,
                "subscription_ids": assets,
                "activation_at": target["activation_at"],
                "capture_start_at": target["capture_start_at"],
                "source_ref": source_ref,
            }
        )
    if target_ids != selected_market_ids:
        raise EvidenceConflict("selected bundle markets and targets disagree")

    relationships = record["relationships"]
    if not isinstance(relationships, list):
        raise EvidenceConflict("selected bundle relationships must be an array")
    relationship_fields = {
        "left",
        "right",
        "relationship",
        "scope",
        "left_venue",
        "right_venue",
        "coverage",
    }
    parsed_relationships: list[dict[str, str]] = []
    for relationship in relationships:
        if not isinstance(relationship, dict) or set(relationship) != relationship_fields:
            raise EvidenceConflict("selected bundle relationship fields are invalid")
        parsed_relationships.append(
            {
                field: _text(relationship, field, "selected bundle relationship")
                for field in relationship_fields
            }
        )

    activation_ns = _timestamp_ns(record["activation_at"])
    start_ns = _timestamp_ns(record["capture_start_at"])
    end_ns = _timestamp_ns(record["planned_capture_end_at"])
    if start_ns >= activation_ns or end_ns <= activation_ns:
        raise EvidenceConflict("selected bundle capture bounds are invalid")
    if end_ns - activation_ns != record["post_start_retention_seconds"] * 1_000_000_000:
        raise EvidenceConflict("selected bundle planned end disagrees with retention policy")
    _timestamp_ns(record["origin_generated_at"])
    for field in (
        "origin_manifest_sha256",
        "origin_report_sha256",
        "origin_index_sha256",
    ):
        value = record[field]
        if (
            len(value) != 64
            or value != value.lower()
            or any(character not in "0123456789abcdef" for character in value)
        ):
            raise EvidenceConflict(f"selected bundle {field} is invalid")
    return {
        **record,
        "participants": participants,
        "participant_keys": participant_keys,
        "event_refs": event_refs,
        "markets": parsed_markets,
        "targets": parsed_targets,
        "relationships": parsed_relationships,
    }


def _venue_prefix(reference: str) -> str:
    venue, separator, identifier = reference.partition(":")
    if not separator or not venue or not identifier:
        raise EvidenceConflict(f"reference has no venue prefix: {reference!r}")
    return venue


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
    target_count = detail.get("target_count")
    if target_count is not None and (
        not isinstance(target_count, int)
        or isinstance(target_count, bool)
        or target_count < 0
    ):
        raise EvidenceConflict("control target_count must be a non-negative integer")
    return {
        **integers,
        "monotonic_ns": monotonic,
        "record_id": envelope["record_id"],
        "venue": envelope["venue"],
        "connection_epoch": envelope["connection_epoch"],
        "event": event,
        "target_digest": _optional_text(detail.get("target_digest")),
        "target_metadata_digest": _optional_text(detail.get("target_metadata_digest")),
        "target_count": target_count,
    }


def _timestamp_ns(value: str) -> int:
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as error:
        raise EvidenceConflict(f"invalid timestamp {value!r}") from error
    if parsed.tzinfo is None:
        raise EvidenceConflict(f"timestamp {value!r} has no timezone")
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = parsed.astimezone(timezone.utc) - epoch
    return (
        delta.days * 86_400_000_000_000
        + delta.seconds * 1_000_000_000
        + delta.microseconds * 1_000
    )


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
