#!/usr/bin/env python3
"""The claim model's real-data gate, against a built Event Universe database.

```sh
python scripts/verify_claim_model.py --database /srv/event-universe/build/universe-v5.sqlite3
python scripts/verify_claim_model.py --database <path> --limit 50   # sample runs
```

`docs/UNIVERSE_RELATION_SCHEMA_REMEDIATION_PLAN.md` makes this a gate: nothing in
the schema moves until the claim model is shown to reproduce, on real venue data,
every relation the selection scorer acts on. Synthetic fixtures prove the model is
sound; only the archive proves it matches what the venues actually publish.

Four things, read-only, against a schema v5 database:

```text
1  size the dead weight the pairwise model stores (same-venue and OVERLAP)
2  count the claim classes the whole build holds, which is what would be stored
3  partition masks into claims from the recorded IDENTITY edges
4  check every partition is a complete clique, as set equality requires
5  check every relation between two claims agrees once direction is normalized
```

Steps 2-4 need no mask recompilation and no object store, so this runs against a
partial build. A non-zero exit means the model is not safe to build on: read the
reported defects rather than proceeding.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from analysis.claims import (  # noqa: E402
    UNINFORMATIVE_RELATIONS,
    classes_from_identity_edges,
    identity_clique_defects,
    relation_agreement_defects,
    relation_members_to_edges,
)


def _connect(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def _dead_weight(connection: sqlite3.Connection) -> dict[str, object]:
    """How much of the stored relation set the producer already discarded."""
    rows = connection.execute(
        """SELECT relation_type, pairing, COUNT(*) AS relations FROM (
               SELECT r.relation_id, r.relation_type,
                      CASE WHEN COUNT(DISTINCT m.venue) = 1 THEN 'same' ELSE 'cross' END
                          AS pairing
               FROM relations r JOIN relation_members m USING (relation_id)
               GROUP BY r.relation_id
           ) GROUP BY relation_type, pairing"""
    ).fetchall()
    total = sum(int(row["relations"]) for row in rows)
    signal = sum(
        int(row["relations"])
        for row in rows
        if row["pairing"] == "cross" and row["relation_type"] not in UNINFORMATIVE_RELATIONS
    )
    return {
        "distinct_relations": total,
        "scorer_relevant": signal,
        "dead_weight_fraction": round(1 - signal / total, 4) if total else None,
        "breakdown": [
            {
                "relation_type": row["relation_type"],
                "pairing": row["pairing"],
                "relations": int(row["relations"]),
            }
            for row in sorted(rows, key=lambda item: -int(item["relations"]))
        ],
    }


def _observation_pressure(connection: sqlite3.Connection) -> dict[str, object]:
    """The two API row caps the pairwise model walks into."""
    per_run = connection.execute(
        "SELECT MAX(n) FROM (SELECT COUNT(*) n FROM relation_observations GROUP BY run_id)"
    ).fetchone()[0]
    per_relation = connection.execute(
        "SELECT MAX(n) FROM (SELECT COUNT(*) n FROM relation_observations GROUP BY relation_id)"
    ).fetchone()[0]
    return {
        "max_relations_in_one_run": per_run,
        "max_observations_of_one_relation": per_relation,
        "detail_row_limit": 1000,
        "run_detail_already_unservable": bool(per_run and per_run > 1000),
        "relation_detail_already_unservable": bool(per_relation and per_relation > 1000),
    }


def _claims_per_run(connection: sqlite3.Connection, limit: int | None) -> dict[str, object]:
    """Partition each run's masks into claims, and check the partition holds."""
    runs = connection.execute(
        "SELECT run_id FROM targeter_runs ORDER BY generated_at_ns, run_id"
        + (f" LIMIT {int(limit)}" if limit else "")
    ).fetchall()

    defects: list[str] = []
    pairwise_total = 0
    claim_total = 0
    checked = 0

    for run in runs:
        run_id = str(run["run_id"])
        members = connection.execute(
            """SELECT o.relation_id, m.venue, m.venue_market_id, m.claim_key,
                      m.role, r.relation_type
               FROM relation_observations o
               JOIN relations r USING (relation_id)
               JOIN relation_members m USING (relation_id)
               WHERE o.run_id = ?""",
            (run_id,),
        ).fetchall()
        if not members:
            continue
        checked += 1

        by_relation: dict[int, dict[str, object]] = defaultdict(
            lambda: {"type": None, "members": [], "roles": []}
        )
        for row in members:
            entry = by_relation[int(row["relation_id"])]
            entry["type"] = row["relation_type"]
            key = f"{row['venue']}:{row['venue_market_id']}"
            if row["claim_key"]:
                key = f"{key}#{row['claim_key']}"
            if key not in entry["members"]:
                entry["members"].append(key)
                entry["roles"].append(str(row["role"]))

        pairwise_total += len(by_relation)
        identity_rows = [
            {
                "relation_id": relation_id,
                "venue": member.split(":", 1)[0],
                "venue_market_id": member.split(":", 1)[1].split("#", 1)[0],
                "claim_key": member.split("#", 1)[1] if "#" in member else "",
            }
            for relation_id, entry in by_relation.items()
            if entry["type"] == "IDENTITY"
            for member in entry["members"]
        ]
        edges = relation_members_to_edges(identity_rows)
        every_member = {
            member for entry in by_relation.values() for member in entry["members"]
        }
        components = classes_from_identity_edges(edges, members=every_member)
        claim_total += len(components)

        for defect in identity_clique_defects(components, edges):
            defects.append(f"{run_id}: {defect}")

        # Every relation between one pair of claims must reduce to one
        # descriptor: the relation is a function of the two subsets, so a
        # genuine disagreement falsifies the model. Direction is normalized
        # first, because a directed relation written from its other end is the
        # same fact and comparing raw labels would report a conflict that is
        # not there.
        owner = {
            member: f"claim-{index}"
            for index, component in enumerate(components)
            for member in component
        }
        recorded: list[tuple[str, list[tuple[str, str]]]] = []
        for entry in by_relation.values():
            pairs = [
                (role, owner[member])
                for member, role in zip(entry["members"], entry["roles"])
                if member in owner
            ]
            if len(pairs) == 2:
                recorded.append((str(entry["type"]), pairs))
        for defect in relation_agreement_defects(recorded):
            defects.append(f"{run_id}: {defect}")

    return {
        "runs_checked": checked,
        "pairwise_relations": pairwise_total,
        "claims": claim_total,
        "collapse_ratio": (
            round(pairwise_total / claim_total, 2) if claim_total else None
        ),
        "defects": defects[:50],
        "defect_count": len(defects),
    }


def _global_claims(connection: sqlite3.Connection) -> dict[str, object]:
    """Distinct claim classes across the whole build, not summed per run.

    ``relations`` is keyed by canonical hash, so its IDENTITY rows are a global
    edge set: partitioning them once gives the number of claim classes the whole
    database holds, which is what a `claim_classes` table would store.

    This counts *event-scoped* classes, since a mask is keyed by its venue market
    and two events' "home wins" are different markets. Deduplicating further by
    outcome key set — the global identity in `analysis.claims` — needs mask
    recompilation and so is measured by the Path A spike, not here. Treat this as
    the upper bound.
    """
    rows = connection.execute(
        """SELECT m.relation_id, m.venue, m.venue_market_id, m.claim_key
           FROM relation_members m
           JOIN relations r USING (relation_id)
           WHERE r.relation_type = 'IDENTITY'"""
    ).fetchall()
    edges = relation_members_to_edges([dict(row) for row in rows])
    every = connection.execute(
        """SELECT DISTINCT venue, venue_market_id, claim_key FROM relation_members"""
    ).fetchall()
    members = {
        f"{row['venue']}:{row['venue_market_id']}"
        + (f"#{row['claim_key']}" if row["claim_key"] else "")
        for row in every
    }
    components = classes_from_identity_edges(edges, members=members)
    observations = connection.execute(
        "SELECT COUNT(*) FROM relation_observations"
    ).fetchone()[0]
    return {
        "masks": len(members),
        "claim_classes_upper_bound": len(components),
        "cross_venue_classes": sum(
            1
            for component in components
            if len({member.split(":", 1)[0] for member in component}) > 1
        ),
        "relation_observations": observations,
        "rows_removed_ratio": (
            round(observations / len(components), 1) if components else None
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument(
        "--limit", type=int, default=None, help="check only the oldest N runs"
    )
    arguments = parser.parse_args()

    if not arguments.database.exists():
        print(f"no such database: {arguments.database}", file=sys.stderr)
        return 2

    with _connect(arguments.database) as connection:
        report = {
            "database": str(arguments.database),
            "dead_weight": _dead_weight(connection),
            "observation_pressure": _observation_pressure(connection),
            "global_claims": _global_claims(connection),
            "claim_partition": _claims_per_run(connection, arguments.limit),
        }

    print(json.dumps(report, indent=2, sort_keys=True))
    return 1 if report["claim_partition"]["defect_count"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
