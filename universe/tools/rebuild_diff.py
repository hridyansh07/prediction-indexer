"""Compare two Event Universe SQLite files, read-only.

    python -m universe.tools.rebuild_diff OLD.sqlite NEW.sqlite

Prints one deterministic JSON report (sorted keys): venue markets whose
parameters or canonical market ID differ, markets present in only one file,
market claims that moved, the bundles those touch, and the corrections the new
file recorded, plus execution-terms coverage in the new file. Market comparison
uses columns that schema v6/projection v3 and schema v7/projection v4 share;
optional new tables are read only when present. It never validates the schema or writes: both files
open with ``mode=ro`` and ``PRAGMA query_only = ON``.
"""

from __future__ import annotations

import json
import sqlite3
import sys
from contextlib import closing
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


def _execution_terms(connection: sqlite3.Connection) -> dict[str, Any]:
    markets = set(_markets(connection))
    if _has(connection, "market_execution_terms"):
        observed = {
            (row["venue"], row["venue_market_id"])
            for row in connection.execute(
                "SELECT DISTINCT venue, venue_market_id FROM market_execution_terms"
            )
        }
        counts = {
            row["venue"]: row["rows"]
            for row in connection.execute(
                "SELECT venue, COUNT(*) AS rows FROM market_execution_terms GROUP BY venue ORDER BY venue"
            )
        }
    else:
        observed, counts = set(), {}
    return {
        "rows_by_venue": counts,
        "observed": sorted(_name(key) for key in markets & observed),
        "unavailable": sorted(_name(key) for key in markets - observed),
    }


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
        "execution_terms": _execution_terms(new),
    }


def changed_markets_keys(changed: dict[str, Any]) -> list[tuple[str, str]]:
    return [tuple(name.split(":", 1)) for name in changed]  # type: ignore[misc]


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    if len(args) != 2:
        print("usage: python -m universe.tools.rebuild_diff OLD.sqlite NEW.sqlite", file=sys.stderr)
        return 2
    with closing(_open(Path(args[0]))) as old, closing(_open(Path(args[1]))) as new:
        report = diff(old, new)
    print(json.dumps(report, sort_keys=True, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
