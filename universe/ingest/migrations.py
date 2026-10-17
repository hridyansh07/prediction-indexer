"""Offline, copy-based Event Universe migrations. Never called by server/jobs."""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import subprocess
import sys
import time
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from typing import Any, Callable
from urllib.parse import quote

from archive.storage.base import ObjectStore, ObjectStoreError
from archive.storage.factory import build_store
from archive.storage.verification import verify_metadata_objects
from universe.derive.market_projection import (
    TOTALS_DIRECTION_VENUES, _corrected_totals_parameters, _event_participants,
    _relation_member, canonical_market_id,
)
from universe.ingest.sync import UniverseSync, UniverseSyncError, _execution_term_items, _logical_size
from universe.store import EvidenceConflict, SCHEMA_PATH, UniverseStore
from universe.store.records import _canonical_json_value, _seen_run_ids, _venue_market_record


M1 = "m1-totals-direction"
M2 = "m2-execution-terms"
M2_CHECKPOINT = "m2_execution_terms_checkpoint"
M2_CHECKPOINT_SQL = (
    "CREATE TABLE m2_execution_terms_checkpoint ("
    "run_id TEXT PRIMARY KEY, state_json TEXT NOT NULL CHECK(json_valid(state_json))) STRICT"
)


def _schema_sql(name: str, kind: str = "table") -> str:
    # Validation normalizes whitespace, but executable SQL must preserve newlines
    # terminating -- comments. SQLite itself parses the resource, not a splitter.
    with closing(sqlite3.connect(":memory:")) as c:
        c.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        return c.execute("SELECT sql FROM sqlite_master WHERE type = ? AND name = ?", (kind, name)).fetchone()[0]


def _open_readonly(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(f"file:{quote(str(path.resolve()))}?mode=ro", uri=True)
    connection.execute("PRAGMA query_only = ON")
    connection.row_factory = sqlite3.Row
    return connection


def _validate_v6(c: sqlite3.Connection) -> None:
    if c.execute("PRAGMA user_version").fetchone()[0] != 6:
        raise EvidenceConflict("m1 requires user_version 6")
    if c.execute("SELECT 1 FROM universe_run_projections WHERE projection_version <> 3 LIMIT 1").fetchone():
        raise EvidenceConflict("m1 requires every market projection row at version 3")
    expected = UniverseStore._expected_schema(7)
    del expected["table", "universe_migrations"]
    del expected["table", "universe_run_projection_corrections"]
    key = ("table", "universe_run_projections")
    expected[key] = expected[key].replace("CHECK(projection_version IN (3, 4))", "CHECK(projection_version = 3)")
    if UniverseStore._schema_objects(c) != expected:
        raise EvidenceConflict("m1 requires the exact Event Universe v6 schema")


def _contexts(c: sqlite3.Connection, row: sqlite3.Row) -> list[str]:
    return [r[0] for r in c.execute(
        """SELECT DISTINCT m.context_sha256 FROM context_markets m
           JOIN context_events e USING(context_sha256)
           WHERE m.target_id = ? AND e.event_ref = ? ORDER BY 1""",
        (f"{row['venue']}:{row['venue_market_id']}", f"{row['venue']}:{row['venue_event_id']}"),
    )]


def _participants(c: sqlite3.Connection, row: sqlite3.Row,
                  contexts: list[str]) -> tuple[list[str], str]:
    market = f"{row['venue']}:{row['venue_market_id']}"
    umbrella = c.execute("SELECT participants_json FROM umbrella_events WHERE event_id = ?",
                         (row["event_id"],)).fetchone()
    sources = [("context", [r[0] for r in c.execute(
        "SELECT name FROM context_participants WHERE context_sha256 = ? ORDER BY position", (context,),
    )]) for context in contexts]
    if umbrella is not None:
        sources.append(("umbrella", json.loads(umbrella[0])))
    available = [(source, _event_participants(None, names)) for source, names in sources if names]
    if not available or any(len(names) != 2 or any(not isinstance(n, str) or not n for n in names)
                            for _, names in available):
        raise EvidenceConflict(f"cannot reconstruct participants for {market}")
    if any(names != available[0][1] for _, names in available):
        raise EvidenceConflict(f"participants disagree for {market}")
    return available[0][1], available[0][0]


def _merge_spans(c: sqlite3.Connection, members: list[sqlite3.Row]) -> tuple[str, str]:
    first, last = members[0]["first_seen_run_id"], members[0]["last_seen_run_id"]
    for member in members[1:]:
        for run_id in (member["first_seen_run_id"], member["last_seen_run_id"]):
            first, last = _seen_run_ids(c, {"first_seen_run_id": first, "last_seen_run_id": last}, run_id)
    return first, last


def _candidate_projection(c: sqlite3.Connection, decision: sqlite3.Row,
                          corrections: list[dict[str, Any]]) -> dict[str, Any]:
    """Unselected candidates have decisions and markets, but no saved report context."""
    umbrella = c.execute("SELECT * FROM umbrella_events WHERE event_id = ?",
                         (decision["event_id"],)).fetchone()
    event = {
        "event_id": decision["event_id"], "source_bundle_id": decision["bundle_id"],
        **{key: umbrella[key] for key in ("sport", "game", "topology", "activation_at")},
        "participants": _event_participants(None, json.loads(umbrella["participants_json"])),
        "participant_keys": json.loads(umbrella["participant_keys_json"]),
    }
    observation = c.execute(
        "SELECT observed_activation_at FROM event_observations WHERE run_id = ? AND bundle_id = ? AND event_id = ?",
        (decision["run_id"], decision["bundle_id"], decision["event_id"]),
    ).fetchone()
    if observation is not None:
        event["activation_at"] = observation[0]
    # v6 retains only bounds, not a complete market list for every candidate.
    # Never extend a market's claims outside its stored observation span.
    markets = [_venue_market_record(row) for row in c.execute(
        """SELECT m.* FROM venue_markets m
           JOIN targeter_runs first ON first.run_id = m.first_seen_run_id
           JOIN targeter_runs last ON last.run_id = m.last_seen_run_id
           JOIN targeter_runs r ON r.run_id = ?
           WHERE m.event_id = ? AND (r.generated_at_ns, r.run_id)
               BETWEEN (first.generated_at_ns, first.run_id) AND (last.generated_at_ns, last.run_id)
           ORDER BY m.venue, m.venue_market_id""", (decision["run_id"], decision["event_id"]),
    )]
    references = {(row["venue"], row["venue_event_id"]) for row in markets}
    events = [dict(row) for row in c.execute(
        "SELECT * FROM venue_events WHERE event_id = ? ORDER BY venue, venue_event_id", (decision["event_id"],),
    ) if (row["venue"], row["venue_event_id"]) in references]
    return {
        "events": [event], "venue_events": events, "venue_markets": markets,
        "decisions": [{"event_id": decision["event_id"], "market_exclusions": json.loads(decision["market_exclusions_json"])}],
        # None means unavailable, unlike an explicitly empty report relation list.
        "relations": None, "corrections": corrections,
    }


def _bundle_projection(c: sqlite3.Connection, db: UniverseStore, decision: sqlite3.Row,
                       corrections: list[dict[str, Any]]) -> dict[str, Any]:
    occurrence = c.execute(
        "SELECT context_sha256 FROM selection_occurrences WHERE run_id = ? AND bundle_id = ? "
        "AND occurrence_kind = 'complete'", (decision["run_id"], decision["bundle_id"]),
    ).fetchone()
    if occurrence is None:
        return _candidate_projection(c, decision, corrections)
    context = db._context(c, occurrence[0], bounded=False)
    event = {
        "event_id": decision["event_id"], "source_bundle_id": decision["bundle_id"],
        **{key: context[key] for key in ("sport", "game", "topology", "participants", "participant_keys", "activation_at")},
    }
    events = []
    for ref in context["event_refs"]:
        venue, native = ref.split(":", 1)
        row = c.execute("SELECT * FROM venue_events WHERE venue = ? AND venue_event_id = ?", (venue, native)).fetchone()
        if row is None or row["event_id"] != decision["event_id"]:
            raise EvidenceConflict(f"cannot reconstruct event {ref} in bundle {decision['bundle_id']}")
        events.append(dict(row))
    markets = []
    for member in context["markets"]:
        venue, native = member["target_id"].split(":", 1)
        row = c.execute("SELECT * FROM venue_markets WHERE venue = ? AND venue_market_id = ?", (venue, native)).fetchone()
        if row is None or row["event_id"] != decision["event_id"]:
            raise EvidenceConflict(f"cannot reconstruct market {member['target_id']} in bundle {decision['bundle_id']}")
        markets.append(_venue_market_record(row))
    relations = [{
        "relation_type": r["relationship"],
        "members": [_relation_member(r, "left"), _relation_member(r, "right")],
    } for r in context["relationships"]]
    return {
        "events": [event], "venue_events": events, "venue_markets": markets,
        "decisions": [{"event_id": decision["event_id"], "market_exclusions": json.loads(decision["market_exclusions_json"])}],
        "relations": relations, "corrections": corrections,
    }


def _correction_runs(c: sqlite3.Connection, row: sqlite3.Row) -> tuple[set[str], bool]:
    name = f"{row['venue']}:{row['venue_market_id']}"
    runs: set[str] = set()
    fallback = False
    for decision in c.execute(
        """SELECT d.*, s.context_sha256, m.target_id FROM candidate_decisions d
           JOIN targeter_runs r USING(run_id)
           JOIN targeter_runs first ON first.run_id = ?
           JOIN targeter_runs last ON last.run_id = ?
           LEFT JOIN selection_occurrences s ON s.run_id = d.run_id AND s.bundle_id = d.bundle_id
               AND s.occurrence_kind = 'complete'
           LEFT JOIN context_markets m ON m.context_sha256 = s.context_sha256 AND m.target_id = ?
           WHERE d.event_id = ? AND (r.generated_at_ns, r.run_id)
               BETWEEN (first.generated_at_ns, first.run_id) AND (last.generated_at_ns, last.run_id)
           ORDER BY d.run_id, d.bundle_id""",
        (row["first_seen_run_id"], row["last_seen_run_id"], name, row["event_id"]),
    ):
        if decision["context_sha256"] is not None:
            if decision["target_id"] is not None:
                runs.add(decision["run_id"])
        else:
            known = set(json.loads(decision["eligible_market_ids_json"]) or ()) | set(json.loads(decision["market_exclusions_json"]) or ())
            runs.add(decision["run_id"])
            fallback |= name not in known
    return runs, fallback


def _m1(c: sqlite3.Connection, db: UniverseStore) -> dict[str, Any]:
    _validate_v6(c)
    c.execute("ALTER TABLE universe_run_projections RENAME TO old_run_projections")
    c.execute(_schema_sql("universe_run_projections"))
    c.execute("INSERT INTO universe_run_projections SELECT * FROM old_run_projections")
    c.execute("DROP TABLE old_run_projections")
    c.execute(_schema_sql("universe_run_projection_corrections"))
    c.execute(_schema_sql("universe_migrations"))

    examined = {venue: 0 for venue in sorted(TOTALS_DIRECTION_VENUES)}
    changed = dict(examined)
    corrections: dict[tuple[str, str, str], dict[str, Any]] = {}
    affected_events: set[str] = set()
    old_claims: set[str] = set()
    old_markets: set[tuple[str, int, int]] = set()
    before_members: set[tuple[Any, ...]] = set()
    before_relations: set[tuple[Any, ...]] = set()
    before_canonical_counts: dict[tuple[str, str], int] = {}
    row_count_deltas: dict[str, int] = {}
    affected_shapes: set[str] = set()
    participants_source = {"context": 0, "umbrella": 0}
    relations_not_rederived: set[str] = set()
    protected_claims: set[str] = set()
    correction_count = 0
    fallback_markets = []
    rows = c.execute(
        "SELECT * FROM venue_markets WHERE venue IN (%s) AND market_type IN ('total_maps', 'total_goals') "
        "ORDER BY venue, venue_market_id" % ",".join("?" for _ in examined), tuple(examined),
    ).fetchall()
    for row in rows:
        venue, native = row["venue"], row["venue_market_id"]
        name = f"{venue}:{native}"
        examined[venue] += 1
        contexts = _contexts(c, row)
        participants, origin = _participants(c, row, contexts)
        participants_source[origin] += 1
        parameters = json.loads(row["parameters_json"])
        source = {**dict(row), "target_id": name, "outcome_labels": json.loads(row["outcome_labels_json"])}
        corrected = _corrected_totals_parameters(
            source, parameters, participants=participants, corrections=corrections,
        )
        if corrected == parameters:
            continue
        changed[venue] += 1
        if row["event_id"] not in affected_events:
            for decision in c.execute(
                "SELECT * FROM candidate_decisions WHERE event_id = ? ORDER BY run_id, bundle_id",
                (row["event_id"],),
            ).fetchall():
                before = _bundle_projection(c, db, decision, [])
                if before["relations"] is None:
                    relations_not_rederived.add(row["event_id"])
                before_canonical_counts[decision["run_id"], decision["bundle_id"]] = len({
                    market["market_id"] for market in before["venue_markets"]
                })
            if row["event_id"] in relations_not_rederived:
                # Relations are global claim edges. Retain both endpoints of
                # every existing edge touching this event, even if a corrected
                # market was the last market referencing its old claim.
                for relation in c.execute(
                    """SELECT left_claim_id, right_claim_id FROM claim_relations r
                       WHERE EXISTS (SELECT 1 FROM market_claims m WHERE m.event_id = ?
                           AND m.claim_id IN (r.left_claim_id, r.right_claim_id))""", (row["event_id"],),
                ):
                    protected_claims.update(relation)
            before_members.update(tuple(r) for r in c.execute(
                "SELECT * FROM market_claims WHERE event_id = ?", (row["event_id"],),
            ))
            shapes = {r[0] for r in c.execute(
                "SELECT DISTINCT space_shape_id FROM claim_classes JOIN market_claims USING(claim_id) "
                "WHERE event_id = ?", (row["event_id"],),
            )} - affected_shapes
            for shape in sorted(shapes):
                before_relations.update(tuple(r) for r in c.execute(
                    "SELECT * FROM claim_relations WHERE space_shape_id = ?", (shape,),
                ))
            affected_shapes.update(shapes)
        affected_events.add(row["event_id"])
        old_markets.add((row["market_id"], row["market_template_version"], row["outcome_space_version"]))
        old_claims.update(r[0] for r in c.execute(
            "SELECT claim_id FROM market_claims WHERE venue = ? AND venue_market_id = ?", (venue, native),
        ))
        c.execute("DELETE FROM market_claims WHERE venue = ? AND venue_market_id = ?", (venue, native))

        market_id = canonical_market_id(
            event_id=row["event_id"], canonical_class=row["canonical_class"], market_type=row["market_type"],
            scope=row["scope"], parameters=corrected, market_template_version=row["market_template_version"],
            outcome_space_version=row["outcome_space_version"],
        )
        first_seen, last_seen = row["first_seen_run_id"], row["last_seen_run_id"]
        existing = c.execute(
            "SELECT first_seen_run_id, last_seen_run_id FROM canonical_markets "
            "WHERE market_id = ? AND market_template_version = ? AND outcome_space_version = ?",
            (market_id, row["market_template_version"], row["outcome_space_version"]),
        ).fetchone()
        if existing is not None:
            first_seen, last_seen = _merge_spans(c, [row, existing])
        c.execute(
            """INSERT INTO canonical_markets VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(market_id, market_template_version, outcome_space_version) DO UPDATE SET
                   first_seen_run_id = excluded.first_seen_run_id,
                   last_seen_run_id = excluded.last_seen_run_id""",
            (market_id, row["market_template_version"], row["outcome_space_version"], row["event_id"],
             row["canonical_class"], row["market_type"], row["scope"], _canonical_json_value(corrected),
             first_seen, last_seen),
        )
        c.execute("UPDATE venue_markets SET parameters_json = ?, market_id = ? WHERE venue = ? AND venue_market_id = ?",
                  (_canonical_json_value(corrected), market_id, venue, native))
        c.execute("UPDATE selected_market_occurrences SET market_id = ? WHERE venue = ? AND venue_market_id = ?",
                  (market_id, venue, native))
        c.execute(
            """DELETE FROM canonical_markets WHERE market_id = ?
               AND NOT EXISTS (SELECT 1 FROM venue_markets v WHERE v.market_id = canonical_markets.market_id)
               AND NOT EXISTS (SELECT 1 FROM selected_market_occurrences s WHERE s.market_id = canonical_markets.market_id)""",
            (row["market_id"],),
        )
        run_ids, fallback = _correction_runs(c, row)
        if fallback:
            fallback_markets.append(name)
        correction = corrections[venue, native, "direction"]
        for run_id in sorted(run_ids):
            c.execute("INSERT INTO universe_run_projection_corrections VALUES (?, ?, ?, ?, ?, ?, ?)",
                      (run_id, venue, native, "direction", correction["report_value"], correction["projected_value"], correction["rule_id"]))
            correction_count += 1
            row_count_deltas[run_id] = row_count_deltas.get(run_id, 0) + 1

    # The wrong canonical/claim may still be shared by a genuinely under
    # market. Its bounds must lose the corrected market, not retain its span.
    for key in sorted(old_markets):
        members = c.execute(
            """SELECT first_seen_run_id, last_seen_run_id FROM venue_markets
               WHERE market_id = ? AND market_template_version = ? AND outcome_space_version = ?
               UNION SELECT run_id, run_id FROM selected_market_occurrences
               WHERE market_id = ? AND market_template_version = ? AND outcome_space_version = ?""",
            (*key, *key),
        ).fetchall()
        if members:
            c.execute(
                "UPDATE canonical_markets SET first_seen_run_id = ?, last_seen_run_id = ? "
                "WHERE market_id = ? AND market_template_version = ? AND outcome_space_version = ?",
                (*_merge_spans(c, members), *key),
            )

    # Retire only old subsets no other market expresses. Shared, content-addressed
    # claims and their algebra remain valid in unaffected bundles.
    for claim_id in sorted(old_claims):
        if claim_id not in protected_claims and not c.execute("SELECT 1 FROM market_claims WHERE claim_id = ? LIMIT 1", (claim_id,)).fetchone():
            c.execute("DELETE FROM claim_relations WHERE left_claim_id = ? OR right_claim_id = ?", (claim_id, claim_id))
            c.execute("DELETE FROM claim_classes WHERE claim_id = ?", (claim_id,))
    for event_id in sorted(affected_events):
        decisions = c.execute(
            "SELECT d.* FROM candidate_decisions d JOIN targeter_runs r USING(run_id) "
            "WHERE event_id = ? ORDER BY r.generated_at_ns, d.run_id, d.bundle_id", (event_id,),
        ).fetchall()
        if not decisions:
            raise EvidenceConflict(f"cannot reconstruct claims for event {event_id}")
        for decision in decisions:
            projection = _bundle_projection(c, db, decision, list(corrections.values()))
            if event_id in relations_not_rederived:
                projection["relations"] = None
            # Only corrections and canonical deduplication change the market
            # projection's row count. Its original v3 hash/version still stand.
            run_id = decision["run_id"]
            row_count_deltas[run_id] = row_count_deltas.get(run_id, 0) + len({
                market["market_id"] for market in projection["venue_markets"]
            }) - before_canonical_counts[run_id, decision["bundle_id"]]
            _, unreconstructed = db._insert_claims(c, decision["run_id"], projection, projection)
            if unreconstructed:
                raise EvidenceConflict(f"cannot reconstruct claims for bundle {decision['bundle_id']}")

    for run_id, delta in sorted(row_count_deltas.items()):
        c.execute("UPDATE universe_run_projections SET projection_row_count = projection_row_count + ? WHERE run_id = ?",
                  (delta, run_id))

    for claim_id in sorted(old_claims):
        members = c.execute(
            "SELECT first_seen_run_id, last_seen_run_id FROM market_claims WHERE claim_id = ?",
            (claim_id,),
        ).fetchall()
        if members:
            c.execute("UPDATE claim_classes SET first_seen_run_id = ?, last_seen_run_id = ? WHERE claim_id = ?",
                      (*_merge_spans(c, members), claim_id))

    after_members = {tuple(r) for event_id in sorted(affected_events) for r in c.execute(
        "SELECT * FROM market_claims WHERE event_id = ?", (event_id,),
    )}
    after_relations = {tuple(r) for shape in sorted(affected_shapes) for r in c.execute(
        "SELECT * FROM claim_relations WHERE space_shape_id = ?", (shape,),
    )}

    return {
        "migration_id": M1, "from_user_version": 6, "to_user_version": 7,
        "markets_examined": examined, "markets_changed": changed,
        "participants_source": participants_source,
        "relations_not_rederived": sorted(relations_not_rederived),
        "correction_rows_written": correction_count,
        "claims_rewritten": len(before_members ^ after_members),
        "relations_rewritten": len(before_relations ^ after_relations),
        "projection_row_count_deltas": {run: delta for run, delta in sorted(row_count_deltas.items()) if delta},
        "correction_membership_fallback": fallback_markets,
        "changed": [{"market": f"{value['venue']}:{value['venue_market_id']}",
                     "report_value": value["report_value"], "projected_value": value["projected_value"]}
                    for _, value in sorted(corrections.items())],
    }


def _stage_m2(c: sqlite3.Connection, db: UniverseStore, objects: ObjectStore,
              binding: dict[str, Any], *, resume: bool) -> None:
    if resume:
        schema = UniverseStore._schema_objects(c)
        expected = UniverseStore._expected_schema(7)
        expected["table", M2_CHECKPOINT] = M2_CHECKPOINT_SQL
        if schema != expected:
            raise EvidenceConflict("resume requires the exact m2 checkpoint schema")
        header = c.execute(f"SELECT state_json FROM {M2_CHECKPOINT} WHERE run_id = ''").fetchone()
        if header is None or json.loads(header[0]) != binding:
            raise EvidenceConflict("resume identity disagrees with source, target, or code revision")
    else:
        UniverseStore._validate_schema(c, 7)
        with c:
            c.execute("BEGIN IMMEDIATE")
            c.execute(M2_CHECKPOINT_SQL)
            c.execute(f"INSERT INTO {M2_CHECKPOINT} VALUES ('', ?)", (_canonical_json_value(binding),))
    reader = UniverseSync(db, objects)
    for run in c.execute(
        f"""SELECT r.* FROM targeter_runs r WHERE input_complete = 1
            AND NOT EXISTS (SELECT 1 FROM {M2_CHECKPOINT} p WHERE p.run_id = r.run_id)
            ORDER BY generated_at_ns, run_id"""
    ).fetchall():
        key = run["manifest_key"]
        try:
            manifest, stored = reader._read_run_manifest(key, expected_sha256=run["manifest_sha256"])
            if (stored.byte_length != run["manifest_byte_length"] or manifest.run_id != run["run_id"]
                    or manifest.generated_at != run["generated_at"] or not manifest.input_complete):
                raise EvidenceConflict(f"run manifest {key} disagrees with stored run identity")
            items = _execution_term_items(manifest)
            metadata = verify_metadata_objects(objects, (item.expectation() for item in items))
            bound = {
                item.key: replace(item.expectation(), provider_checksum=observed.provider_checksum,
                                  provider_checksum_algorithm=observed.provider_checksum_algorithm)
                for item, observed in zip(items, metadata, strict=True)
            }
            selected = {}
            # Use each run's saved subscriptions, not venue_markets' latest row.
            for occurrence in c.execute(
                "SELECT context_sha256 FROM selection_occurrences WHERE run_id = ? ORDER BY bundle_id",
                (run["run_id"],),
            ):
                for target in db._context(c, occurrence[0], bounded=False)["targets"]:
                    selected[target["target_id"]] = target
            terms = reader._read_execution_terms(manifest, bound, selected)
            present = {item.file.removeprefix("target_records_").split(".", 1)[0] for item in items}
            state = {"terms": terms, "missing_venues": sorted({"kalshi", "limitless", "polymarket"} - present),
                     "artifact_objects": len(items), "decoded_bytes": sum(_logical_size(item) for item in items)}
        except (UniverseSyncError, ObjectStoreError) as error:
            raise EvidenceConflict(f"{key}: {error}") from error
        # Preparatory evidence, not the versioned projection. Each fully verified
        # run is durable; no partial artifact or partially processed run is saved.
        with c:
            c.execute(f"INSERT INTO {M2_CHECKPOINT} VALUES (?, ?)",
                      (run["run_id"], _canonical_json_value(state)))


def _m2(c: sqlite3.Connection, db: UniverseStore) -> dict[str, Any]:
    c.execute(_schema_sql("market_execution_terms"))
    c.execute(_schema_sql("market_execution_terms_market", "index"))
    c.execute(_schema_sql("selected_market_occurrences_bundle", "index"))
    missing = {venue: 0 for venue in ("kalshi", "limitless", "polymarket")}
    counts = {venue: {"observed": 0, "null": 0} for venue in missing}
    total = runs = artifact_objects = decoded_bytes = 0
    for checkpoint in c.execute(f"SELECT * FROM {M2_CHECKPOINT} WHERE run_id != '' ORDER BY run_id"):
        run_id, state = checkpoint["run_id"], json.loads(checkpoint["state_json"])
        # Bundle entries (unlike market-projection hashes) are fully retained.
        # Verify the old identity before including execution terms in its hash.
        if not db._audit_run(c, run_id)["ok"]:
            raise EvidenceConflict(f"cannot reconstruct bundle projection for run {run_id}")
        for term in state["terms"]:
            c.execute("INSERT INTO market_execution_terms VALUES (?, ?, ?, ?, ?)",
                      (run_id, term["venue"], term["venue_market_id"], term["taker_delay_ms"], term["observed_at"]))
            counts[term["venue"]]["observed"] += 1
            counts[term["venue"]]["null"] += term["taker_delay_ms"] is None
            total += 1
        audit = db._audit_run(c, run_id)
        c.execute("UPDATE targeter_runs SET projection_sha256 = ?, projection_row_count = ? WHERE run_id = ?",
                  (audit["actual_sha256"], audit["actual_row_count"], run_id))
        for venue in state["missing_venues"]:
            missing[venue] += 1
        runs += 1
        artifact_objects += state["artifact_objects"]
        decoded_bytes += state["decoded_bytes"]
    c.execute(f"DROP TABLE {M2_CHECKPOINT}")
    return {"migration_id": M2, "from_user_version": 7, "to_user_version": 8,
            "runs_processed": runs, "rows_written": total, "runs_lacking_artifact": missing,
            "by_venue": counts, "artifact_objects": artifact_objects, "decoded_bytes": decoded_bytes}


def migrate(source: Path, destination: Path, *, to: int, code_revision: str,
            inputs: dict[str, Path] | None = None,
            objects: ObjectStore | None = None, resume: bool = False,
            progress: Callable[[dict[str, Any]], None] | None = None) -> list[dict[str, Any]]:
    source, destination = Path(source), Path(destination)
    if not source.is_file() or source.resolve() == destination.resolve():
        raise EvidenceConflict("SOURCE must be an existing file distinct from DEST")
    if not code_revision:
        raise EvidenceConflict("migration requires code_revision")
    inputs = inputs or {}
    if inputs:
        raise EvidenceConflict("m1 declares no inputs")
    with closing(_open_readonly(source)) as original:
        version = source_version = original.execute("PRAGMA user_version").fetchone()[0]
        if version not in (6, 7) or to not in (7, 8) or to <= version:
            raise EvidenceConflict("only consecutive upward migrations from v6 or v7 to v7 or v8 are supported")
        if resume and (to != 8 or not destination.is_file()):
            raise EvidenceConflict("--resume requires an existing m2 destination and --to 8")
        if destination.exists() and not resume:
            raise EvidenceConflict("DEST already exists; refusing to overwrite it")
        if not resume:
            destination.parent.mkdir(parents=True, exist_ok=True)
            with destination.open("xb"):
                pass
        with closing(sqlite3.connect(destination)) as c:
            if not resume:
                original.backup(c)
            version = c.execute("PRAGMA user_version").fetchone()[0]
            if resume and version != 7:
                raise EvidenceConflict("--resume requires a v7 m2 destination")
            c.row_factory = sqlite3.Row
            c.execute("PRAGMA journal_mode = DELETE")
            c.execute("PRAGMA synchronous = FULL")
            c.execute("PRAGMA foreign_keys = ON")
            if version == 6:
                _validate_v6(c)
            if c.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                raise EvidenceConflict("SQLite backup failed integrity_check")
            results = []
            db = UniverseStore(destination)
            while version < to:
                if version == 7:
                    _stage_m2(c, db, objects if objects is not None else build_store((destination.parent,)),
                              {"source": str(source.resolve()), "source_user_version": source_version,
                               "to_user_version": to, "code_revision": code_revision}, resume=resume)
                try:
                    c.execute("BEGIN IMMEDIATE")
                    if version == 6:
                        summary = _m1(c, db)
                    else:
                        summary = _m2(c, db)
                    c.execute("INSERT INTO universe_migrations VALUES (?, ?, ?, ?, ?, ?, ?)",
                              (summary["migration_id"], version, version + 1, code_revision, "{}",
                               _canonical_json_value(summary), time.time_ns()))
                    UniverseStore._validate_schema(c, version + 1)
                    if c.execute("PRAGMA foreign_key_check").fetchone():
                        raise EvidenceConflict("migration failed foreign_key_check")
                    c.execute(f"PRAGMA user_version = {version + 1}")
                    c.commit()
                except BaseException:
                    c.rollback()
                    raise
                version += 1
                results.append(summary)
                if progress:
                    progress(summary)
            return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m universe migrate")
    parser.add_argument("--to", type=int, required=True)
    parser.add_argument("source", type=Path)
    parser.add_argument("destination", type=Path)
    parser.add_argument("--input", action="append", default=[])
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args(argv)
    try:
        inputs = {}
        for value in args.input:
            name, separator, path = value.partition("=")
            if not separator or not name or not path or name in inputs:
                raise EvidenceConflict("--input requires unique NAME=PATH entries")
            inputs[name] = Path(path)
        revision = os.environ.get("UNIVERSE_CODE_REVISION")
        if not revision:
            try:
                revision = subprocess.run(
                    ["git", "rev-parse", "HEAD"], cwd=Path(__file__).resolve().parents[2],
                    capture_output=True, text=True, check=True,
                ).stdout.strip()
            except (OSError, subprocess.SubprocessError) as error:
                raise EvidenceConflict("migration image requires UNIVERSE_CODE_REVISION (full git commit)") from error
        if not re.fullmatch(r"[0-9a-f]{40}", revision):
            raise EvidenceConflict("UNIVERSE_CODE_REVISION must be a full git commit")
        migrate(args.source, args.destination, to=args.to, inputs=inputs, code_revision=revision, resume=args.resume,
                progress=lambda row: print(_canonical_json_value(row), flush=True))
    except (EvidenceConflict, ObjectStoreError, OSError, sqlite3.Error, subprocess.SubprocessError) as error:
        print(str(error), file=sys.stderr)
        return 1
    return 0
