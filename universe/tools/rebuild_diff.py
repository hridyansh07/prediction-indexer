"""Compare two Event Universe SQLite files, read-only.

    python -m universe.tools.rebuild_diff OLD.sqlite NEW.sqlite

Prints one deterministic JSON report (sorted keys): venue markets whose
parameters or canonical market ID differ, markets present in only one file,
market claims that moved, the bundles those touch, and the corrections the new
file recorded, migration metadata, execution-term observations, and differences
in every table. --equivalence checks a named half-open short range with only the
spec's exclusions. It never validates the schema or writes: both files
open with ``mode=ro`` and ``PRAGMA query_only = ON``.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote


def _open(path: Path) -> sqlite3.Connection:
    if not path.is_file():
        raise SystemExit(f"not a file: {path}")
    connection = sqlite3.connect(f"file:{quote(str(path.resolve()))}?mode=ro", uri=True)
    connection.execute("PRAGMA query_only = ON")
    connection.row_factory = sqlite3.Row
    return connection


def _has(connection: sqlite3.Connection, table: str) -> bool:
    return (
        connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)
        ).fetchone()
        is not None
    )


def _markets(connection: sqlite3.Connection) -> dict[tuple[str, str], dict[str, Any]]:
    rows = connection.execute(
        """SELECT venue, venue_market_id, market_id, parameters_json, event_id
           FROM venue_markets"""
    )
    return {
        (row["venue"], row["venue_market_id"]): {
            "market_id": row["market_id"],
            "parameters": json.loads(row["parameters_json"]),
            "event_id": row["event_id"],
        }
        for row in rows
    }


def _claims(connection: sqlite3.Connection) -> dict[tuple[str, str, str], dict[str, Any]]:
    out: dict[tuple[str, str, str], dict[str, Any]] = {}
    rows = connection.execute(
        """SELECT m.venue, m.venue_market_id, m.claim_key, m.claim_id,
                  c.outcome_key_count, c.space_shape_id
           FROM market_claims m JOIN claim_classes c USING(claim_id)
           ORDER BY m.venue, m.venue_market_id, m.claim_key, m.claim_id"""
    )
    for row in rows:
        entry = out.setdefault(
            (row["venue"], row["venue_market_id"], row["claim_key"]), {"claims": {}}
        )
        entry["claims"][row["claim_id"]] = {
            "outcome_key_count": row["outcome_key_count"],
            "space_shape_id": row["space_shape_id"],
        }
    return out


def _bundles(connection: sqlite3.Connection) -> dict[tuple[str, str], set[str]]:
    out: dict[tuple[str, str], set[str]] = {}
    for row in connection.execute(
        """SELECT v.venue, v.venue_market_id, d.bundle_id
           FROM venue_markets v JOIN candidate_decisions d ON d.event_id = v.event_id"""
    ):
        out.setdefault((row["venue"], row["venue_market_id"]), set()).add(row["bundle_id"])
    return out


def _corrections(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    if not _has(connection, "universe_run_projection_corrections"):
        return []
    return [
        dict(row)
        for row in connection.execute(
            """SELECT run_id, venue, venue_market_id, field, report_value,
                      projected_value, rule_id
               FROM universe_run_projection_corrections
               ORDER BY run_id, venue, venue_market_id, field"""
        )
    ]


def _versions(connection: sqlite3.Connection) -> dict[str, Any]:
    return {
        "user_version": connection.execute("PRAGMA user_version").fetchone()[0],
        "projection_versions": [
            row[0]
            for row in connection.execute(
                "SELECT DISTINCT projection_version FROM universe_run_projections "
                "ORDER BY 1"
            )
        ],
    }


def _identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _tables(connection: sqlite3.Connection) -> dict[str, list[str]]:
    return {
        row[0]: [column[1] for column in connection.execute(f"PRAGMA table_info({_identifier(row[0])})")]
        for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
    }


def _migrations(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    if not _has(connection, "universe_migrations"):
        return []
    return [dict(row) for row in connection.execute(
        "SELECT * FROM universe_migrations ORDER BY to_user_version, migration_id"
    )]


def _execution_terms(connection: sqlite3.Connection) -> dict[str, Any]:
    # Discover the isolated table by its contract, without inventing the absent
    # EXECUTION_DELAY_SDK_V1 §1.2 table name. All tables are compared regardless.
    result = {}
    for table, columns in _tables(connection).items():
        if not {"venue", "venue_market_id", "taker_delay_ms", "observed_at"} <= set(columns):
            continue
        by_venue = {venue: {"rows": 0, "observed": 0, "unavailable": 0}
                    for venue in ("kalshi", "limitless", "polymarket")}
        for row in connection.execute(
            f"SELECT venue, COUNT(*), SUM(taker_delay_ms IS NOT NULL), SUM(taker_delay_ms IS NULL) "
            f"FROM {_identifier(table)} GROUP BY venue ORDER BY venue"
        ):
            by_venue[row[0]] = dict(zip(("rows", "observed", "unavailable"), row[1:]))
        result[table] = {"by_venue": by_venue}
    return result


def _row_key(row: tuple[Any, ...]) -> tuple[Any, ...]:
    # SQLite's storage-class sort order, so nulls and mixed STRICT ANY values
    # compare the same way as each cursor's ORDER BY, without buffering tables.
    return tuple((0, 0) if value is None else
                 (1, value) if isinstance(value, (int, float)) else
                 (2, value) if isinstance(value, str) else (3, value) for value in row)


def _table_differences(old: sqlite3.Connection, new: sqlite3.Connection, *, equivalence: bool) -> dict[str, Any]:
    inventories = [_tables(c) for c in (old, new)]
    result = {}
    for table in sorted(inventories[0].keys() | inventories[1].keys()):
        if equivalence and table == "universe_migrations":
            continue
        ignored = {"indexed_at_ns"} if equivalence else set()
        if equivalence and table == "universe_run_projections":
            ignored |= {"projection_version", "projection_sha256"}
        columns = [[col for col in inventory.get(table, []) if col not in ignored] for inventory in inventories]
        cursors = []
        for c, cols in zip((old, new), columns):
            fields = ", ".join(_identifier(col) for col in cols)
            cursors.append(iter(c.execute(f"SELECT {fields} FROM {_identifier(table)} ORDER BY {fields}")) if cols else iter(()))
        report = {"columns_old": columns[0], "columns_new": columns[1],
                  "only_in_old": 0, "only_in_new": 0, "sample_old": [], "sample_new": []}

        def record(side: int, row: sqlite3.Row) -> None:
            label = ("old", "new")[side]
            report[f"only_in_{label}"] += 1
            sample = report[f"sample_{label}"]
            if len(sample) < 20:
                sample.append({key: value.hex() if isinstance(value, bytes) else value
                               for key, value in zip(columns[side], row)})

        before, after = (next(cursor, None) for cursor in cursors)
        while before is not None or after is not None:
            if before is not None and after is not None and tuple(before) == tuple(after) and columns[0] == columns[1]:
                before, after = (next(cursor, None) for cursor in cursors)
            elif before is not None and (after is None or _row_key(tuple(before)) <= _row_key(tuple(after))):
                record(0, before)
                before = next(cursors[0], None)
            else:
                record(1, after)
                after = next(cursors[1], None)
        if columns[0] != columns[1] or report["only_in_old"] or report["only_in_new"]:
            result[table] = report
    return result


def _bound_ns(value: str) -> int:
    instant = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if instant.tzinfo is None:
        raise ValueError("range bounds require a timezone")
    delta = instant - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return ((delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds) * 1000


def _name(key: tuple[str, ...]) -> str:
    return ":".join(key[:2]) + ("#" + key[2] if len(key) > 2 and key[2] else "")


def diff(old: sqlite3.Connection, new: sqlite3.Connection) -> dict[str, Any]:
    old_markets, new_markets = _markets(old), _markets(new)
    changed_markets = {}
    for key in sorted(old_markets.keys() & new_markets.keys()):
        before, after = old_markets[key], new_markets[key]
        if before["parameters"] != after["parameters"] or before["market_id"] != after["market_id"]:
            changed_markets[_name(key)] = {
                "market_id": {"old": before["market_id"], "new": after["market_id"]},
                "parameters": {"old": before["parameters"], "new": after["parameters"]},
            }

    old_claims, new_claims = _claims(old), _claims(new)
    changed_claims = {}
    for key in sorted(old_claims.keys() & new_claims.keys()):
        before, after = old_claims[key]["claims"], new_claims[key]["claims"]
        if set(before) != set(after):
            changed_claims[_name(key)] = {"old": before, "new": after}

    old_bundles, new_bundles = _bundles(old), _bundles(new)
    touched_markets = (
        set(changed_markets_keys(changed_markets))
        | set(old_markets.keys() ^ new_markets.keys())
        | {key[:2] for key in old_claims.keys() & new_claims.keys() if _name(key) in changed_claims}
        | {key[:2] for key in old_claims.keys() ^ new_claims.keys()}
    )
    bundles = sorted(
        {
            bundle
            for key in touched_markets
            for bundle in old_bundles.get(key, set()) | new_bundles.get(key, set())
        }
    )
    return {
        "old": _versions(old),
        "new": _versions(new),
        "venue_markets": {
            "changed": changed_markets,
            "only_in_old": sorted(_name(k) for k in old_markets.keys() - new_markets.keys()),
            "only_in_new": sorted(_name(k) for k in new_markets.keys() - old_markets.keys()),
        },
        "market_claims": {
            "changed": changed_claims,
            "only_in_old": {
                _name(k): old_claims[k]["claims"]
                for k in sorted(old_claims.keys() - new_claims.keys())
            },
            "only_in_new": {
                _name(k): new_claims[k]["claims"]
                for k in sorted(new_claims.keys() - old_claims.keys())
            },
        },
        "bundles_affected": bundles,
        "new_corrections": _corrections(new),
        "universe_migrations": {"old": _migrations(old), "new": _migrations(new)},
        "execution_terms": {"old": _execution_terms(old), "new": _execution_terms(new)},
    }


def changed_markets_keys(changed: dict[str, Any]) -> list[tuple[str, str]]:
    return [tuple(name.split(":", 1)) for name in changed]  # type: ignore[misc]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m universe.tools.rebuild_diff")
    parser.add_argument("old", type=Path)
    parser.add_argument("new", type=Path)
    parser.add_argument("--equivalence", action="store_true")
    parser.add_argument("--generated-start")
    parser.add_argument("--generated-end")
    parser.add_argument("--accept-correction-membership-fallback", action="store_true")
    args = parser.parse_args(argv)
    try:
        if args.equivalence:
            if not args.generated_start or not args.generated_end:
                raise ValueError("--equivalence requires --generated-start and --generated-end")
            start, end = _bound_ns(args.generated_start), _bound_ns(args.generated_end)
            if start >= end:
                raise ValueError("range must be nonempty and half-open [start, end)")
        elif args.generated_start or args.generated_end or args.accept_correction_membership_fallback:
            raise ValueError("range/fallback options require --equivalence")
        with closing(_open(args.old)) as old, closing(_open(args.new)) as new:
            report = diff(old, new)
            differences = report["table_differences"] = _table_differences(old, new, equivalence=args.equivalence)
            if args.equivalence:
                ranges = []
                for c in (old, new):
                    inside = [row[0] for row in c.execute(
                        "SELECT run_id FROM targeter_runs WHERE generated_at_ns >= ? AND generated_at_ns < ? ORDER BY run_id", (start, end),
                    )]
                    outside = [row[0] for row in c.execute(
                        "SELECT run_id FROM targeter_runs WHERE generated_at_ns < ? OR generated_at_ns >= ? ORDER BY run_id", (start, end),
                    )]
                    if not inside:
                        raise ValueError("both files must contain runs in the requested range")
                    ranges.append({"inside": inside, "outside": outside})
                accepted = ["universe_run_projection_corrections"] if args.accept_correction_membership_fallback and set(differences) == {"universe_run_projection_corrections"} else []
                report["equivalence"] = {
                    "equal": not (set(differences) - set(accepted)) and ranges[0] == ranges[1],
                    "generated_start": args.generated_start, "generated_end": args.generated_end,
                    "runs_in_range": len(ranges[0]["inside"]),
                    "outside_range_run_ids": {"old": ranges[0]["outside"], "new": ranges[1]["outside"]},
                    "accepted_differences": accepted,
                }
    except (ValueError, sqlite3.Error) as error:
        print(str(error), file=sys.stderr)
        return 2
    print(json.dumps(report, sort_keys=True, indent=2, ensure_ascii=False))
    return int(args.equivalence and not report["equivalence"]["equal"])


if __name__ == "__main__":
    raise SystemExit(main())
