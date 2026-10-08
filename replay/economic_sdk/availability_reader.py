"""Independent, streaming reader for availability intervals (bundle coverage rows).

The same rules validate bundle coverage's ``intervals.ndjson`` and the market
profile's ``availability.ndjson``: both are the output of one interval engine
(``replay.economic_sdk.availability``), but this module does not import it.
It re-derives the expected entities from the pinned snapshot and checks, one
bounded line at a time, closed schemas, provenance, a complete gap-free
partition per entity, the file identity and (through a disposable disk index)
the member and bundle arithmetic against the books active at every instant.
"""

from __future__ import annotations

import hashlib
import itertools
import sqlite3
import tempfile
from pathlib import Path

from replay.preparation import digest, encoded
from replay.strategy_sdk import plain
from replay.streams.protocol import choice, decode, obj, origin, pin, reason, require, uint

MAX_BYTES = 256 * 1024 * 1024
MAX_RECORDS = 1_000_000
MAX_LINE = 64 * 1024
MAX_ENTITIES = 32768
STATES = "AVAILABLE_UNDER_POLICY PARTIAL UNAVAILABLE NOT_CAPTURED"


def book_id(book):
    return "book:" + digest(plain(book))


def entities(snapshot):
    result = {}
    for index, scope in enumerate(snapshot["scopes"]):
        require(scope["members"], "empty requested denominator")
        result[index, "bundle"] = ("bundle", scope)
        for member in scope["members"]:
            result[index, "member:" + member["market_id"]] = ("member", member)
            for book in member["books"]:
                result[index, book_id(book)] = ("book", book)
        require(len(result) <= MAX_ENTITIES, "coverage context entity budget")
    return result


def _row(row, snapshot, expected, pins):
    common = "version scope entity start_ns end_ns vendor_completeness kind state"
    require(type(row) is dict)
    if row.get("kind") == "book":
        obj(row, common + " evidence reason source evidence_source")
    else:
        obj(row, common + " usable_books required_books uncaptured_members")
    require(type(row["version"]) is int and row["version"] == 1)
    require(type(row["scope"]) is int and type(row["entity"]) is str)
    identity = row["scope"], row["entity"]
    require(
        identity in expected and row["kind"] == expected[identity][0], "unknown entity"
    )
    scope = snapshot["scopes"][row["scope"]]
    start, end = uint(row["start_ns"]), uint(row["end_ns"])
    require(
        int(scope["start_ns"]) <= start < end <= int(scope["end_ns"]), "interval bounds"
    )
    require(row["vendor_completeness"] == "NOT_PROVEN")
    if row["kind"] == "book":
        choice(row["state"], "not_initialized usable unusable")
        choice(
            row["evidence"],
            "unknown lane_missing lane_invalid lane_not_expected visible_clock_regression",
        )
        if row["state"] == "not_initialized":
            require(row["reason"] is None and row["source"] is None)
        else:
            require(row["source"] is not None)
            origin(row["source"], pins)
            source_time = row["source"].get("visible_ns", row["source"].get("start_ns"))
            require(uint(source_time) <= start, "future status provenance")
            if row["state"] == "usable":
                require(row["reason"] is None and row["source"]["kind"] == "group")
            else:
                reason(row["reason"])
        if row["evidence"] == "unknown":
            require(row["evidence_source"] is None)
        else:
            source = row["evidence_source"]
            origin(source, pins)
            require(
                source["kind"] == "window"
                and uint(source["start_ns"]) <= start
                and end <= uint(source["end_ns"]),
                "evidence interval",
            )
            # Window evidence persists even when a later group fault replaces
            # the book's current reason. Both facts must remain visible.
            require(
                row["state"] == "unusable",
                "evidence/state mismatch",
            )
    else:
        choice(row["state"], STATES)
        for field in ("usable_books", "required_books", "uncaptured_members"):
            require(type(row[field]) is int and 0 <= row[field] <= MAX_ENTITIES)
    return identity, start, end


def _check_active(active, scope):
    """Recompute the denominator from requested members, not reported aggregates."""
    total = usable = outside = 0
    for member in scope["members"]:
        n = len(member["books"])
        u = sum(active[book_id(b)]["state"] == "usable" for b in member["books"])
        o = int(not member["capture_selected"])
        _check_counts(active["member:" + member["market_id"]], u, n, o)
        total += n
        usable += u
        outside += o
    _check_counts(active["bundle"], usable, total, outside)


def _check_counts(row, usable, required, outside):
    # Deliberately independent of the engine's availability helper.
    if required == 0:
        state = "NOT_CAPTURED"
    elif usable == 0:
        state = "UNAVAILABLE"
    elif usable < required or outside:
        state = "PARTIAL"
    else:
        state = "AVAILABLE_UNDER_POLICY"
    require(
        (
            row["usable_books"],
            row["required_books"],
            row["uncaptured_members"],
            row["state"],
        )
        == (usable, required, outside, state),
        "aggregate/member arithmetic",
    )


def validate_intervals(path, snapshot, identity):
    """Validate one interval file against the snapshot and its recorded identity.

    One bounded line at a time; the temporal cross-check uses a disposable disk
    index, not a retained tape. Returns ``{(scope, entity): {state: ns}}``.
    """
    snapshot = plain(snapshot)
    expected = entities(snapshot)
    pins = {pin(p) for p in snapshot["config"]["pins"]}
    cursors = {k: int(snapshot["scopes"][k[0]]["start_ns"]) for k in expected}
    durations = {k: {} for k in expected}
    path = Path(path)
    require(path.is_file() and not path.is_symlink(), "regular intervals required")
    checksum = hashlib.sha256()
    size = count = 0
    with tempfile.TemporaryDirectory(prefix="coverage-validate-") as tmp:
        db = sqlite3.connect(str(Path(tmp) / "events.db"))
        try:
            db.execute("PRAGMA cache_size=-2048")
            db.execute("PRAGMA temp_store=FILE")
            db.execute("PRAGMA max_page_count=262144")  # at most 1 GiB scratch
            db.execute(
                "CREATE TABLE events(scope INTEGER, time TEXT, opening INTEGER, entity TEXT, payload BLOB)"
            )
            with path.open("rb") as stream:
                while payload := stream.readline(MAX_LINE + 1):
                    require(
                        len(payload) <= MAX_LINE and payload.endswith(b"\n"),
                        "interval line/truncation",
                    )
                    size += len(payload)
                    count += 1
                    require(size <= MAX_BYTES and count <= MAX_RECORDS, "output budget")
                    checksum.update(payload)
                    row = decode(payload, MAX_LINE)
                    key, start, end = _row(row, snapshot, expected, pins)
                    require(start == cursors[key], "interval gap/overlap/order")
                    cursors[key] = end
                    totals = durations[key]
                    totals[row["state"]] = totals.get(row["state"], 0) + end - start
                    # Keep only aggregate-relevant data in the disk sweep.
                    compact = {
                        k: row[k]
                        for k in (
                            "state",
                            "usable_books",
                            "required_books",
                            "uncaptured_members",
                        )
                        if k in row
                    }
                    db.executemany(
                        "INSERT INTO events VALUES(?,?,?,?,?)",
                        [
                            (
                                key[0],
                                f"{start:020d}",
                                1,
                                key[1],
                                encoded(compact),
                            ),
                            (key[0], f"{end:020d}", 0, key[1], b"{}"),
                        ],
                    )
            require(
                {"sha256": checksum.hexdigest(), "byte_length": size, "records": count}
                == identity,
                "interval identity",
            )
            require(
                all(
                    end == int(snapshot["scopes"][k[0]]["end_ns"])
                    for k, end in cursors.items()
                ),
                "incomplete intervals",
            )
            db.execute("CREATE INDEX temporal ON events(scope,time,opening,entity)")
            db.commit()
            rows = db.execute(
                "SELECT scope,time,opening,entity,payload FROM events ORDER BY scope,time,opening,entity"
            )
            active = {}
            for (index, time), changes in itertools.groupby(
                rows, key=lambda r: (r[0], r[1])
            ):
                for _, _, opening, entity, payload in changes:
                    if opening:
                        require(entity not in active, "overlapping active entity")
                        active[entity] = decode(payload, MAX_LINE)
                    else:
                        require(entity in active, "missing active entity")
                        del active[entity]
                scope = snapshot["scopes"][index]
                if int(time) == int(scope["end_ns"]):
                    require(not active, "unclosed scope")
                else:
                    require(
                        len(active) == sum(k[0] == index for k in expected),
                        "incomplete active denominator",
                    )
                    _check_active(active, scope)
        finally:
            db.close()
    return durations


def duration_rows(durations):
    """Deterministic summary form of ``validate_intervals`` durations."""
    return [
        {
            "scope": s,
            "entity": e,
            "state_ns": {k: str(v) for k, v in sorted(t.items())},
        }
        for (s, e), t in sorted(durations.items())
    ]
