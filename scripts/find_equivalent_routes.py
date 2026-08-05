#!/usr/bin/env python3
"""Price the same bet across every contract that expresses it.

Builds the outcome space per event, compiles every tradable position to a mask,
groups positions whose masks coincide over the reachable outcomes at each state,
and reports the price spread across those equivalent routes.

Prices come from the free layer: Kalshi one-minute candlestick mid, Polymarket
CLOB price. That answers whether equivalent contracts diverge. It does not
answer whether a divergence was fillable — that needs depth, and the spread
reported here is therefore an upper bound on any realisable edge.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from analysis.equivalence import Instrument, analyse_state_intervals
from analysis.masks import compile_mask
from analysis.ndjson_sink import safe_name
from analysis.outcome_space import (
    build_series_space,
    build_state_timeline,
    parse_best_of,
    reachable_keys,
)
from analysis.storage import iso_to_unix_seconds, utc_now, write_json

BAR = 60


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-directory", type=Path, default=PROJECT_ROOT / "data")
    parser.add_argument("--minimum-bars", type=int, default=10)
    return parser.parse_args()


def kalshi_mid_series(path: Path) -> dict[int, float]:
    """Bar -> mid of the top-of-book yes bid/ask."""
    out: dict[int, float] = {}
    if not path.exists():
        return out
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            row = json.loads(line)
            bid = (row.get("yes_bid") or {}).get("close_dollars")
            ask = (row.get("yes_ask") or {}).get("close_dollars")
            if bid is None or ask is None:
                continue
            bid_f, ask_f = float(bid), float(ask)
            # A 0/1 quote is an empty book, not a price.
            if ask_f - bid_f > 0.5:
                continue
            out[int(row["end_period_ts"]) // BAR * BAR] = (bid_f + ask_f) / 2
    return out


def polymarket_series(path: Path) -> dict[int, float]:
    out: dict[int, float] = {}
    if not path.exists():
        return out
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            row = json.loads(line)
            out[int(row["timestamp_seconds"]) // BAR * BAR] = float(row["price"])
    return out


def main() -> int:
    arguments = parse_args()
    data = arguments.data_directory
    manifest = json.loads(
        (data / "manifests" / "ewc_dota_siblings.json").read_text(encoding="utf-8")
    )
    coverage = json.loads(
        (data / "history" / "ewc_dota_siblings_coverage.json").read_text(encoding="utf-8")
    )
    kalshi_job = Path(coverage["kalshi"]["job_directory"])
    polymarket_job = Path(coverage["polymarket"]["job_directory"])

    by_event: dict[str, list[dict]] = defaultdict(list)
    for target in manifest["history_targets"]:
        by_event[target["event_key"]].append(target)

    report: dict[str, Any] = {"generated_at": utc_now(), "events": []}
    all_classes: list[dict[str, Any]] = []

    for event_key in sorted(by_event):
        targets = by_event[event_key]
        series = [
            t for t in targets
            if t["venue"] == "kalshi" and t["market_type"] == "series_moneyline"
        ]
        maps = [
            t for t in targets
            if t["venue"] == "kalshi" and t["market_type"] == "map_winner"
        ]
        if len(series) != 2 or not maps:
            continue
        teams = sorted({str(t.get("yes_label")) for t in series})
        home, away = teams
        pm_title = next(
            (t.get("question") or "" for t in targets if t["venue"] == "polymarket"), ""
        )
        best_of = parse_best_of(pm_title)
        if best_of is None:
            indices = {
                int(m.group(1))
                for t in maps
                if (m := re.search(r"-(\d+)-", t["ticker"]))
            }
            best_of = 5 if max(indices, default=2) > 2 else 3
        space = build_series_space(event_key, best_of=best_of, home=home, away=away)

        # ---- instruments: Kalshi yes/no legs + Polymarket tokens ----
        instruments: list[Instrument] = []
        for target in targets:
            if target["venue"] != "kalshi":
                continue
            mask = compile_mask(target, space)
            if not mask.derivable:
                continue
            prices = kalshi_mid_series(
                kalshi_job / safe_name(target["ticker"]) / "candlesticks.ndjson"
            )
            if not prices:
                continue
            instruments.append(
                Instrument(
                    instrument_id=f"kalshi:{target['ticker']}:yes",
                    venue="kalshi",
                    market_key=target["ticker"],
                    market_type=target["market_type"],
                    position="yes",
                    outcome_label=str(target.get("yes_label") or ""),
                    mask=mask.outcome_keys,
                    prices=prices,
                )
            )
            instruments.append(
                Instrument(
                    instrument_id=f"kalshi:{target['ticker']}:no",
                    venue="kalshi",
                    market_key=target["ticker"],
                    market_type=target["market_type"],
                    position="no",
                    outcome_label=f"NOT {target.get('yes_label')}",
                    mask=space.keys - mask.outcome_keys,
                    prices={bar: 1.0 - price for bar, price in prices.items()},
                )
            )

        for target in targets:
            if target["venue"] != "polymarket":
                continue
            for token in target.get("outcome_tokens") or []:
                asset_id = str(token.get("asset_id") or "")
                outcome = str(token.get("outcome") or "")
                if not asset_id:
                    continue
                # Compile per token: the tradable claim is "this outcome". The
                # outcome is passed separately rather than folded into the
                # title, because a handicap title names both teams and would
                # otherwise give both tokens of one condition the same mask.
                probe = dict(target)
                probe["outcome_label"] = outcome
                mask = compile_mask(probe, space)
                if not mask.derivable:
                    continue
                prices = polymarket_series(
                    polymarket_job
                    / safe_name(str(target["market_id"]))
                    / safe_name(asset_id)
                    / "prices.ndjson"
                )
                if not prices:
                    continue
                instruments.append(
                    Instrument(
                        instrument_id=f"polymarket:{target['market_id'][:10]}:{outcome}",
                        venue="polymarket",
                        market_key=str(target["market_id"]),
                        market_type=target["market_type"],
                        position=asset_id,
                        outcome_label=outcome,
                        mask=mask.outcome_keys,
                        prices=prices,
                    )
                )

        # ---- state intervals from settled map markets ----
        observed = []
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

        intervals: list[tuple[str, frozenset[str], tuple[int, int] | None]] = []
        previous = 0
        for transition in timeline:
            at = transition.at_ms // 1000
            prefix_before = transition.prefix[:-1]
            intervals.append(
                (prefix_before, reachable_keys(space, prefix_before), (previous, at))
            )
            previous = at
        intervals.append(
            (
                timeline[-1].prefix if timeline else "",
                reachable_keys(space, timeline[-1].prefix if timeline else ""),
                (previous, 2**31),
            )
        )

        classes = analyse_state_intervals(
            instruments, intervals, minimum_bars=arguments.minimum_bars
        )
        for record in classes:
            record["event_key"] = event_key
            record["best_of"] = best_of
        all_classes.extend(classes)
        report["events"].append(
            {
                "event_key": event_key,
                "best_of": best_of,
                "instruments": len(instruments),
                "state_intervals": len(intervals),
                "equivalence_classes": len(classes),
            }
        )

    all_classes.sort(key=lambda record: -record["spread_median"])
    report["classes"] = all_classes
    report["totals"] = {
        "events": len(report["events"]),
        "equivalence_classes": len(all_classes),
        "bars": sum(record["bars"] for record in all_classes),
        "classes_median_over_1c": sum(
            1 for record in all_classes if record["spread_median"] > 0.01
        ),
        "classes_median_over_2c": sum(
            1 for record in all_classes if record["spread_median"] > 0.02
        ),
    }

    out = data / "analysis" / "equivalent_routes.json"
    write_json(out, report)

    import statistics as _st
    for composition in ("single_venue", "mixed_venue"):
        subset = [c for c in all_classes if c["venue_composition"] == composition]
        if not subset:
            continue
        med = _st.median([c["spread_median"] for c in subset])
        report.setdefault("by_composition", {})[composition] = {
            "classes": len(subset),
            "bars": sum(c["bars"] for c in subset),
            "median_of_class_medians": med,
            "classes_over_1c": sum(1 for c in subset if c["spread_median"] > 0.01),
            "classes_over_2c": sum(1 for c in subset if c["spread_median"] > 0.02),
        }

    totals = report["totals"]
    print(
        f"events={totals['events']}  equivalence classes={totals['equivalence_classes']}  "
        f"bars={totals['bars']:,}"
    )
    print(
        f"classes with median spread >1c: {totals['classes_median_over_1c']}   "
        f">2c: {totals['classes_median_over_2c']}\n"
    )
    for composition, stats in (report.get("by_composition") or {}).items():
        print(f"  {composition:14s} classes={stats['classes']:3d} bars={stats['bars']:6,d} "
              f"median-of-medians={stats['median_of_class_medians']:.4f} "
              f">1c={stats['classes_over_1c']:3d} >2c={stats['classes_over_2c']:3d}")
    print()
    header = f"{'event':26s} {'state':7s} {'routes':>6s} {'bars':>5s} {'med':>7s} {'p90':>7s} {'max':>7s}"
    print(header)
    print("-" * len(header))
    for record in all_classes[:18]:
        types = "+".join(sorted({r["market_type"] for r in record["routes"]}))
        print(
            f"{record['event_key'][:25]:26s} {record['state_prefix'][:7]:7s} "
            f"{record['route_count']:6d} {record['bars']:5d} "
            f"{record['spread_median']:7.4f} {record['spread_p90']:7.4f} "
            f"{record['spread_max']:7.4f}  {record['venue_composition'][:6]:6s} {types}"
        )
    print(f"\nwrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
