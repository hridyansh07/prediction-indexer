"""Independent, streaming coverage content reader; completion is a separate fact."""

import hashlib
import itertools
import sqlite3
import tempfile
from pathlib import Path

from replay.preparation import digest, encoded, load_snapshot, sha
from replay.strategy_sdk import PreparedInput, plain
from replay.streams.protocol import (
    choice,
    decode,
    freeze,
    obj,
    origin,
    pin,
    reason,
    require,
    uint,
)
from replay.supervisor import initial, read_success
from replay.supervisor import read as read_run

MAX_BYTES = 256 * 1024 * 1024
MAX_RECORDS = 1_000_000
MAX_LINE = 64 * 1024
MAX_METADATA = 16 * 1024 * 1024
MAX_ENTITIES = 32768
DISPOSITIONS = "observed applied duplicate not_authority invalidated"
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


def read_json(path):
    require(not path.is_symlink() and path.is_file(), "regular output required")
    with path.open("rb") as stream:
        return decode(stream.read(MAX_METADATA + 1), MAX_METADATA)


def _manifest(value, complete):
    obj(
        value,
        "version strategy snapshot_sha256 intervals trades nonduplicate_trade_observations"
        + (" summary_sha256" if complete else ""),
    )
    require(type(value["version"]) is int and value["version"] == 1)
    require(value["strategy"] == "bundle_coverage_v1")
    sha(value["snapshot_sha256"])
    if complete:
        sha(value["summary_sha256"])
    identity = obj(value["intervals"], "sha256 byte_length records")
    sha(identity["sha256"])
    for field, cap in (("byte_length", MAX_BYTES), ("records", MAX_RECORDS)):
        require(type(identity[field]) is int and 0 < identity[field] <= cap)
    obj(value["trades"], DISPOSITIONS)
    require(
        all(type(v) is int and 0 <= v <= 2**64 - 1 for v in value["trades"].values())
    )
    count = value["nonduplicate_trade_observations"]
    require(
        type(count) is int
        and count == sum(v for k, v in value["trades"].items() if k != "duplicate"),
        "trade count mismatch",
    )


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
            require(
                row["state"] == "unusable" and row["reason"]["kind"] == row["evidence"],
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
    # Deliberately independent of the strategy's availability helper.
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


def validate_content(directory, snapshot, manifest):
    """Validate provisional intervals; never asserts supervisor completion.

    One bounded line at a time; temporal cross-check uses a disposable disk index,
    not a retained tape. Returns a deterministic summary derived from intervals.
    """
    _manifest(manifest, "summary_sha256" in manifest)
    snapshot = plain(snapshot)
    expected = entities(snapshot)
    pins = {pin(p) for p in snapshot["config"]["pins"]}
    cursors = {k: int(snapshot["scopes"][k[0]]["start_ns"]) for k in expected}
    durations = {k: {} for k in expected}
    path = Path(directory) / "intervals.ndjson"
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
                    identity, start, end = _row(row, snapshot, expected, pins)
                    require(start == cursors[identity], "interval gap/overlap/order")
                    cursors[identity] = end
                    totals = durations[identity]
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
                                identity[0],
                                f"{start:020d}",
                                1,
                                identity[1],
                                encoded(compact),
                            ),
                            (identity[0], f"{end:020d}", 0, identity[1], b"{}"),
                        ],
                    )
            require(
                {"sha256": checksum.hexdigest(), "byte_length": size, "records": count}
                == manifest["intervals"],
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
    summary = {
        "version": 1,
        "snapshot_sha256": manifest["snapshot_sha256"],
        "membership_basis": snapshot["membership_basis"],
        "history_complete": snapshot["history_complete"],
        "vendor_completeness": "NOT_PROVEN",
        "trades": manifest["trades"],
        "nonduplicate_trade_observations": manifest["nonduplicate_trade_observations"],
        "durations": [
            {
                "scope": s,
                "entity": e,
                "state_ns": {k: str(v) for k, v in sorted(t.items())},
            }
            for (s, e), t in sorted(durations.items())
        ],
    }
    require(len(encoded(summary)) <= MAX_METADATA, "summary budget")
    return summary


def read_provisional(directory, snapshot_directory, *, expected_sha256):
    """Content validation only. This API deliberately returns no committed flag."""
    root = Path(directory)
    snapshot = load_snapshot(snapshot_directory, expected_sha256=expected_sha256)
    manifest = read_json(root / "manifest.json")
    _manifest(manifest, True)
    require(manifest["snapshot_sha256"] == expected_sha256, "snapshot binding")
    receipt = obj(
        read_json(root / "content_receipt.json"),
        "version semantic_sha256 run_id attempt_id group identity terminal",
    )
    require(type(receipt["version"]) is int and receipt["version"] == 1)
    sha(receipt["identity"])
    sha(receipt["semantic_sha256"])
    require(receipt["semantic_sha256"] == digest(manifest), "semantic identity")
    for field in ("run_id", "attempt_id", "group"):
        require(type(receipt[field]) is str and 0 < len(receipt[field]) <= 128)
    require(type(receipt["terminal"]) is int and receipt["terminal"] >= 2)
    summary = validate_content(root, snapshot, manifest)
    require(
        encoded(read_json(root / "summary.json")) == encoded(summary)
        and manifest["summary_sha256"] == digest(summary),
        "summary identity/schema",
    )
    return {"receipt": receipt, "manifest": manifest, "summary": summary}


def read_completed(run_directory, group):
    """Only this reader combines content identity with whole-attempt success."""
    root = Path(run_directory)
    success = read_success(root)
    config = read_run(root / "run.json")
    require(group in success["outputs"], "unknown strategy group")
    spec = config["strategies"][group]
    require(
        spec["factory"] == "replay.bundle_coverage:build", "coverage factory binding"
    )
    strategy_config = obj(spec["config"], "version snapshot_directory snapshot_sha256")
    PreparedInput(strategy_config).bind(freeze(initial(config)))
    result = read_provisional(
        root / success["outputs"][group],
        strategy_config["snapshot_directory"],
        expected_sha256=strategy_config["snapshot_sha256"],
    )
    receipt = result["receipt"]
    require(
        {
            k: receipt[k]
            for k in ("identity", "attempt_id", "group", "run_id", "terminal")
        }
        == {
            "identity": success["identity"],
            "attempt_id": success["attempt"],
            "group": group,
            "run_id": config["transport"]["run_id"],
            "terminal": success["terminal"],
        },
        "supervisor/content binding",
    )
    return result
