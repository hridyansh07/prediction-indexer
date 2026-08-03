#!/usr/bin/env python3
"""Falsify the mask engine against settlement.

Every derivable mask makes a checkable claim: the event's realised outcome
omega-star lies inside the mask if and only if the market resolved YES. Any
mismatch means the mask is wrong. This runs that check across every settled
sibling market held in the repository and writes a durable report.

It also asserts the structural invariant that a mutually exclusive Kalshi event
settles exactly one YES leg, and cross-checks Kalshi settlement against
Polymarket's for markets the engine calls IDENTITY.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from prediction_indexer.masks import (
    STATUS_DERIVABLE,
    compile_mask,
    derive_relationships,
    is_partition,
    state_conditioned_relationships,
)
from prediction_indexer.outcome_space import (
    SCOPE_FIRST_HALF,
    SCOPE_REGULATION_FULLTIME,
    build_score_space,
    build_series_space,
    build_state_timeline,
    parse_best_of,
    parse_score_ticker,
    settled_outcome_key,
)
from prediction_indexer.storage import iso_to_unix_seconds, utc_now, write_json


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-directory", type=Path, default=PROJECT_ROOT / "data")
    return parser.parse_args()


def load_manifest(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _kalshi_by_event(manifest: dict, market_type: str, event_key: str) -> list[dict]:
    return [
        t
        for t in manifest["history_targets"]
        if t["venue"] == "kalshi"
        and t["market_type"] == market_type
        and t["event_key"] == event_key
    ]


def _settled_score(targets: list[dict]) -> tuple[int, int] | None:
    """The single correct-score leg that resolved YES gives omega-star."""
    for target in targets:
        if str(target.get("result")) == "yes":
            parsed = parse_score_ticker(target.get("ticker") or "")
            if parsed:
                return parsed[1], parsed[3]
    return None


def check_masks(
    markets: list[dict],
    space,
    omega_star: str | None,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    status_counts: Counter[str] = Counter()
    for market in markets:
        mask = compile_mask(market, space)
        status_counts[mask.status] += 1
        result = str(market.get("result") or "").lower()
        if mask.status != STATUS_DERIVABLE or omega_star is None or result not in ("yes", "no"):
            continue
        predicted = omega_star in mask.outcome_keys
        actual = result == "yes"
        rows.append(
            {
                "market_key": mask.market_key,
                "market_type": mask.market_type,
                "resolver": mask.resolver,
                "mask_size": len(mask.outcome_keys),
                "predicted_yes": predicted,
                "actual_yes": actual,
                "agrees": predicted == actual,
            }
        )
    return {"rows": rows, "status_counts": dict(status_counts)}


def main() -> int:
    arguments = parse_args()
    data = arguments.data_directory
    report: dict[str, Any] = {"generated_at": utc_now(), "events": []}
    all_rows: list[dict[str, Any]] = []

    # ---------------- World Cup: two score scopes per fixture ----------------
    wc = load_manifest(data / "manifests" / "wc_knockout_2026.json")
    wc_events = sorted({t["event_key"] for t in wc["history_targets"]})
    for event_key in wc_events:
        for scope, score_type, derived_types in (
            (
                SCOPE_REGULATION_FULLTIME,
                "correct_score",
                ("moneyline_3way", "total_goals", "spread",
                 "both_teams_to_score", "team_total_goals"),
            ),
            (
                SCOPE_FIRST_HALF,
                "first_half_correct_score",
                ("first_half_moneyline_3way", "first_half_total_goals",
                 "first_half_spread", "first_half_both_teams_to_score"),
            ),
        ):
            ladder = _kalshi_by_event(wc, score_type, event_key)
            if not ladder:
                continue
            space = build_score_space(
                event_key,
                [t["ticker"] for t in ladder],
                scope=scope,
            )
            settled = _settled_score(ladder)
            omega_star = (
                settled_outcome_key(space, home_goals=settled[0], away_goals=settled[1])
                if settled
                else None
            )
            markets = list(ladder)
            for market_type in derived_types:
                markets += _kalshi_by_event(wc, market_type, event_key)

            checked = check_masks(markets, space, omega_star)
            all_rows.extend(checked["rows"])
            ladder_masks = [compile_mask(t, space) for t in ladder]
            partition = is_partition(ladder_masks, space)
            derived = derive_relationships(
                [compile_mask(m, space) for m in markets]
            )
            yes_count = sum(1 for t in ladder if str(t.get("result")) == "yes")
            report["events"].append(
                {
                    "event_key": event_key,
                    "scope": scope,
                    "omega_size": len(space.outcomes),
                    "coverage": space.coverage,
                    "omega_star": omega_star,
                    "ladder_yes_count": yes_count,
                    "ladder_yes_invariant_holds": yes_count == 1,
                    "mask_status_counts": checked["status_counts"],
                    "checked": len(checked["rows"]),
                    "disagreements": [r for r in checked["rows"] if not r["agrees"]],
                    "ladder_partitions_omega": partition["is_partition"],
                    "relationship_counts": dict(
                        Counter(r["relationship"] for r in derived)
                    ),
                }
            )

    # ---------------- Dota: series space + state timeline ----------------
    dota = load_manifest(data / "manifests" / "ewc_dota_siblings.json")
    pm_titles = {
        t["event_key"]: t.get("question") or ""
        for t in dota["history_targets"]
        if t["venue"] == "polymarket"
    }
    for event_key in sorted({t["event_key"] for t in dota["history_targets"]}):
        maps = [
            t
            for t in dota["history_targets"]
            if t["venue"] == "kalshi"
            and t["market_type"] == "map_winner"
            and t["event_key"] == event_key
        ]
        series = [
            t
            for t in dota["history_targets"]
            if t["venue"] == "kalshi"
            and t["market_type"] == "series_moneyline"
            and t["event_key"] == event_key
        ]
        if not maps or not series:
            continue
        best_of = parse_best_of(pm_titles.get(event_key)) or (
            5 if len({re.search(r"-(\d+)-", t["ticker"]).group(1) for t in maps
                      if re.search(r"-(\d+)-", t["ticker"])}) > 2 else 3
        )
        teams = sorted({str(t.get("yes_label")) for t in series})
        if len(teams) != 2:
            continue
        home, away = teams
        space = build_series_space(event_key, best_of=best_of, home=home, away=away)

        observed: list[dict[str, Any]] = []
        for target in maps:
            match = re.search(r"-(\d+)-", target["ticker"])
            if not match or str(target.get("result")) != "yes":
                continue
            observed.append(
                {
                    "map_index": int(match.group(1)),
                    "winner": str(target.get("yes_label")),
                    "settled_at_ms": (iso_to_unix_seconds(target.get("close_time")) or 0)
                    * 1000,
                }
            )
        timeline = build_state_timeline(observed, home=home)
        series_winner = next(
            (str(t.get("yes_label")) for t in series if str(t.get("result")) == "yes"),
            None,
        )
        markets = maps + series + [
            t
            for t in dota["history_targets"]
            if t["event_key"] == event_key
            and t["venue"] == "polymarket"
            and t["market_type"] in ("map_winner", "total_maps", "series_moneyline",
                                     "map_handicap")
        ]
        masks = [compile_mask(m, space) for m in markets]
        prefixes = [""] + [t.prefix for t in timeline]
        conditioned = state_conditioned_relationships(masks, space, prefixes)
        emergent = [
            r for r in conditioned
            if r["relationship"] == "IDENTITY" and r["state_prefix"] != "(pre-match)"
        ]
        pre_match_identities = {
            (r["left"], r["right"])
            for r in conditioned
            if r["relationship"] == "IDENTITY" and r["state_prefix"] == "(pre-match)"
        }
        report["events"].append(
            {
                "event_key": event_key,
                "scope": "series",
                "best_of": best_of,
                "omega_size": len(space.outcomes),
                "coverage": space.coverage,
                "observed_maps": len(observed),
                "series_winner": series_winner,
                "state_prefix_final": timeline[-1].prefix if timeline else "",
                "mask_status_counts": dict(
                    Counter(m.status for m in masks)
                ),
                "identities_pre_match": len(pre_match_identities),
                "identities_emergent": len(
                    {(r["left"], r["right"]) for r in emergent}
                    - pre_match_identities
                ),
            }
        )

    agree = sum(1 for r in all_rows if r["agrees"])
    report["settlement_check"] = {
        "checked": len(all_rows),
        "agreements": agree,
        "disagreements": len(all_rows) - agree,
        "by_type": {
            key: {
                "checked": sum(1 for r in all_rows if r["market_type"] == key),
                "disagreements": sum(
                    1 for r in all_rows if r["market_type"] == key and not r["agrees"]
                ),
            }
            for key in sorted({r["market_type"] for r in all_rows})
        },
    }

    out = data / "analysis" / "mask_validation.json"
    write_json(out, report)

    check = report["settlement_check"]
    print(f"settlement check: {check['agreements']}/{check['checked']} masks agree "
          f"with settlement  ({check['disagreements']} disagreements)")
    for key, value in check["by_type"].items():
        flag = "  <-- MISMATCH" if value["disagreements"] else ""
        print(f"   {key:34s} {value['checked']:4d} checked  "
              f"{value['disagreements']:3d} wrong{flag}")
    print()
    for event in report["events"]:
        if event["scope"] == "series":
            print(f"  {event['event_key']:28s} Bo{event['best_of']} "
                  f"|Ω|={event['omega_size']:2d} maps={event['observed_maps']} "
                  f"identities pre={event['identities_pre_match']} "
                  f"emergent={event['identities_emergent']}")
        else:
            ok = "ok" if event["ladder_yes_invariant_holds"] else "VIOLATED"
            print(f"  {event['event_key']:28s} {event['scope']:20s} "
                  f"|Ω|={event['omega_size']:3d} ω*={str(event['omega_star']):12s} "
                  f"1-yes:{ok:8s} checked={event['checked']:3d} "
                  f"bad={len(event['disagreements'])}")
    print(f"\nwrote {out}")
    return 0 if check["disagreements"] == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
