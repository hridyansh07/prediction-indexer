#!/usr/bin/env python3
"""Do equivalent routes ever cross — buy one side below where another bids?

The earlier run compared mid prices and found large cross-venue gaps. With real
Polymarket books in hand those gaps are not automatically tradeable: an esports
book here can be tens of cents wide, and a mid inside a 50c spread is not a
price anyone can transact at.

So this measures the only thing that matters economically. For every set of
contracts expressing the same bet at the same moment, take the best ask across
all routes and the best bid across all routes. If the best bid exceeds the best
ask, the two are crossed and the difference is a gross, executable edge before
fees. If not, the venues merely disagree inside their spreads, which is not an
opportunity.

Quotes only, both sides:
  Kalshi     candlestick yes_bid / yes_ask (top of book)
  Polymarket Oddpool orderbook best_bid / best_ask
"""

from __future__ import annotations

import argparse
import bisect
import json
import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from prediction_indexer.masks import compile_mask
from prediction_indexer.ndjson_sink import safe_name
from prediction_indexer.outcome_space import (
    build_series_space,
    build_state_timeline,
    parse_best_of,
    reachable_keys,
)
from prediction_indexer.storage import iso_to_unix_seconds, utc_now, write_json

BAR = 60
MAX_SNAPSHOT_AGE = 60  # seconds; older than this and the quote is stale

# Kalshi charges ceil(rate * contracts * P * (1-P)); Polymarket charged no CLOB
# trading fee over this period. A cross-venue trade therefore pays a Kalshi fee
# only on the leg that sits on Kalshi.
KALSHI_FEE_RATE = 0.07


def leg_fee(instrument_id: str, price: float) -> float:
    if not instrument_id.startswith("kalshi:"):
        return 0.0
    return KALSHI_FEE_RATE * price * (1.0 - price)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-directory", type=Path, default=PROJECT_ROOT / "data")
    parser.add_argument("--depth-job", type=Path, required=True)
    parser.add_argument("--minimum-bars", type=int, default=10)
    return parser.parse_args()


def kalshi_quotes(path: Path) -> dict[int, tuple[float, float]]:
    """Bar -> (bid, ask) for the YES side."""
    out: dict[int, tuple[float, float]] = {}
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
            if ask_f <= bid_f:
                continue  # crossed or empty book, not a quote
            out[int(row["end_period_ts"]) // BAR * BAR] = (bid_f, ask_f)
    return out


def polymarket_quotes(path: Path) -> dict[str, dict[int, tuple[float, float, int]]]:
    """asset_id -> bar -> (bid, ask, snapshot_age_seconds).

    Uses the latest snapshot at or before each bar boundary — never a later one.
    """
    raw: dict[str, list[tuple[int, float, float]]] = defaultdict(list)
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            row = json.loads(line)
            asset = str(row.get("asset_id") or "")
            bid, ask = row.get("best_bid"), row.get("best_ask")
            if not asset or bid is None or ask is None:
                continue
            bid_f, ask_f = float(bid), float(ask)
            if ask_f <= bid_f:
                continue
            raw[asset].append((int(row["timestamp"]) // 1000, bid_f, ask_f))

    out: dict[str, dict[int, tuple[float, float, int]]] = {}
    for asset, points in raw.items():
        points.sort()
        stamps = [p[0] for p in points]
        series: dict[int, tuple[float, float, int]] = {}
        first_bar = (stamps[0] // BAR) * BAR
        last_bar = (stamps[-1] // BAR + 1) * BAR
        for bar in range(first_bar, last_bar + BAR, BAR):
            index = bisect.bisect_right(stamps, bar) - 1
            if index < 0:
                continue
            stamp, bid, ask = points[index]
            age = bar - stamp
            if age > MAX_SNAPSHOT_AGE:
                continue
            series[bar] = (bid, ask, age)
        out[asset] = series
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
    depth_run = json.loads((arguments.depth_job / "run.json").read_text(encoding="utf-8"))
    depth_paths = {
        str(t["market_id"]): Path(t["snapshots_path"])
        for t in depth_run.get("targets", [])
        if t.get("complete")
    }
    # The original EWC pull holds depth for the Match Winner conditions.
    prior_job = data / "oddpool" / "orderbooks" / "27bd23c87b2408a6"
    if (prior_job / "run.json").exists():
        prior = json.loads((prior_job / "run.json").read_text(encoding="utf-8"))
        for target in prior.get("targets", []):
            if target.get("complete") and target.get("venue") == "polymarket":
                depth_paths.setdefault(
                    str(target["market_id"]), Path(target["snapshots_path"])
                )

    by_event: dict[str, list[dict]] = defaultdict(list)
    for target in manifest["history_targets"]:
        by_event[target["event_key"]].append(target)

    records: list[dict[str, Any]] = []
    surviving: list[dict[str, Any]] = []
    events_covered: list[str] = []

    for event_key in sorted(by_event):
        targets = by_event[event_key]
        pm_conditions = {
            str(t["market_id"])
            for t in targets
            if t["venue"] == "polymarket"
            and t["market_type"] in ("map_winner", "total_maps", "series_moneyline")
        }
        if not pm_conditions or not pm_conditions <= set(depth_paths):
            continue  # only fully-covered events; a partial class is untestable
        events_covered.append(event_key)

        series_targets = [
            t for t in targets
            if t["venue"] == "kalshi" and t["market_type"] == "series_moneyline"
        ]
        maps = [
            t for t in targets
            if t["venue"] == "kalshi" and t["market_type"] == "map_winner"
        ]
        teams = sorted({str(t.get("yes_label")) for t in series_targets})
        if len(teams) != 2:
            continue
        home, away = teams
        pm_title = next(
            (t.get("question") or "" for t in targets if t["venue"] == "polymarket"), ""
        )
        best_of = parse_best_of(pm_title) or 3
        space = build_series_space(event_key, best_of=best_of, home=home, away=away)

        # ---- quoted instruments ----
        instruments: list[dict[str, Any]] = []
        for target in targets:
            if target["venue"] != "kalshi":
                continue
            mask = compile_mask(target, space)
            if not mask.derivable:
                continue
            quotes = kalshi_quotes(
                kalshi_job / safe_name(target["ticker"]) / "candlesticks.ndjson"
            )
            if not quotes:
                continue
            instruments.append({
                "id": f"kalshi:{target['ticker']}:yes", "venue": "kalshi",
                "market": target["ticker"], "type": target["market_type"],
                "mask": mask.outcome_keys,
                "quotes": {b: (q[0], q[1], 0) for b, q in quotes.items()},
            })
            instruments.append({
                "id": f"kalshi:{target['ticker']}:no", "venue": "kalshi",
                "market": target["ticker"], "type": target["market_type"],
                "mask": space.keys - mask.outcome_keys,
                "quotes": {b: (1 - q[1], 1 - q[0], 0) for b, q in quotes.items()},
            })

        for target in targets:
            if target["venue"] != "polymarket":
                continue
            path = depth_paths.get(str(target["market_id"]))
            if path is None:
                continue
            books = polymarket_quotes(path)
            for token in target.get("outcome_tokens") or []:
                asset = str(token.get("asset_id") or "")
                outcome = str(token.get("outcome") or "")
                series = books.get(asset)
                if not asset or not series:
                    continue
                probe = dict(target)
                probe["outcome_label"] = outcome
                mask = compile_mask(probe, space)
                if not mask.derivable:
                    continue
                instruments.append({
                    "id": f"polymarket:{str(target['market_id'])[:10]}:{outcome}",
                    "venue": "polymarket", "market": str(target["market_id"]),
                    "type": target["market_type"], "mask": mask.outcome_keys,
                    "quotes": series,
                })

        # ---- state intervals ----
        observed = []
        for target in maps:
            match = re.search(r"-(\d+)-", target["ticker"])
            if not match or str(target.get("result")) != "yes":
                continue
            observed.append({
                "map_index": int(match.group(1)),
                "winner": str(target.get("yes_label")),
                "settled_at_ms": (iso_to_unix_seconds(target.get("close_time")) or 0) * 1000,
            })
        timeline = build_state_timeline(observed, home=home)
        intervals = []
        previous = 0
        for transition in timeline:
            at = transition.at_ms // 1000
            before = transition.prefix[:-1]
            intervals.append((before, reachable_keys(space, before), (previous, at)))
            previous = at
        tail = timeline[-1].prefix if timeline else ""
        intervals.append((tail, reachable_keys(space, tail), (previous, 2**31)))

        # ---- crossing test ----
        for prefix, universe, (start, end) in intervals:
            groups: dict[frozenset[str], list[dict]] = defaultdict(list)
            for instrument in instruments:
                restricted = instrument["mask"] & universe
                if not restricted or restricted == universe:
                    continue
                groups[restricted].append(instrument)

            for outcome_keys, members in groups.items():
                if len({m["market"] for m in members}) < 2:
                    continue
                bars = set(members[0]["quotes"])
                for member in members[1:]:
                    bars &= set(member["quotes"])
                bars = {b for b in bars if start <= b < end}
                if len(bars) < arguments.minimum_bars:
                    continue

                rows = []
                for bar in sorted(bars):
                    quotes = {m["id"]: m["quotes"][bar] for m in members}
                    venue_of = {m["id"]: m["venue"] for m in members}
                    width_of = {m["id"]: m["quotes"][bar][1] - m["quotes"][bar][0]
                                for m in members}
                    best_bid_id = max(quotes, key=lambda k: quotes[k][0])
                    best_ask_id = min(quotes, key=lambda k: quotes[k][1])
                    best_bid = quotes[best_bid_id][0]
                    best_ask = quotes[best_ask_id][1]
                    skew = max(q[2] for q in quotes.values())
                    gross = best_bid - best_ask
                    fees = leg_fee(best_ask_id, best_ask) + leg_fee(best_bid_id, best_bid)
                    rows.append({
                        "bar": bar,
                        "crossing": gross,
                        "net_crossing": gross - fees,
                        "fees": fees,
                        "buy": best_ask_id, "sell": best_bid_id,
                        "same_market": best_bid_id.rsplit(":", 1)[0]
                                       == best_ask_id.rsplit(":", 1)[0],
                        # Same-venue crossings are the Sigma-bid>1 partition
                        # condition already measured and found unprofitable;
                        # only a genuine two-venue crossing is new information.
                        "same_venue": venue_of[best_bid_id] == venue_of[best_ask_id],
                        # Buying at the ask of a very wide book is not a price
                        # anyone transacts at, so the width of the leg being
                        # bought is carried through as a quality control.
                        "buy_leg_width": width_of[best_ask_id],
                        "skew": skew,
                        "mid_spread": max(
                            (q[0] + q[1]) / 2 for q in quotes.values()
                        ) - min((q[0] + q[1]) / 2 for q in quotes.values()),
                        "widest_book": max(q[1] - q[0] for q in quotes.values()),
                    })

                for row in rows:
                    if (row["net_crossing"] > 0 and not row["same_venue"]
                            and row["skew"] <= 5 and row["buy_leg_width"] <= 0.05):
                        surviving.append({
                            "event_key": event_key,
                            "state_prefix": prefix or "(pre-match)",
                            "types": sorted({m["type"] for m in members}),
                            "buy": row["buy"], "sell": row["sell"],
                            "gross": round(row["crossing"], 4),
                            "fees": round(row["fees"], 4),
                            "net_crossing": round(row["net_crossing"], 4),
                            "buy_leg_width": round(row["buy_leg_width"], 4),
                            "bar": row.get("bar"),
                        })
                crossings = [r["crossing"] for r in rows]
                positive = [r for r in rows if r["crossing"] > 0]
                net_positive = [r for r in rows if r["net_crossing"] > 0]
                fresh = [r for r in rows if r["skew"] <= 5]
                fresh_net = [r for r in fresh if r["net_crossing"] > 0]
                records.append({
                    "event_key": event_key,
                    "state_prefix": prefix or "(pre-match)",
                    "outcome_keys": sorted(outcome_keys),
                    "routes": len(members),
                    "venues": sorted({m["venue"] for m in members}),
                    "types": sorted({m["type"] for m in members}),
                    "bars": len(rows),
                    "crossing_median": statistics.median(crossings),
                    "crossing_max": max(crossings),
                    "bars_crossed": len(positive),
                    "bars_net_crossed": len(net_positive),
                    "net_crossing_max": max(r["net_crossing"] for r in rows),
                    "bars_fresh": len(fresh),
                    "bars_fresh_net_crossed": len(fresh_net),
                    "bars_crossed_over_1c": sum(1 for r in rows if r["crossing"] > 0.01),
                    "cross_venue_crossings": sum(
                        1 for r in positive if not r["same_venue"]
                    ),
                    "same_venue_crossings": sum(
                        1 for r in positive if r["same_venue"]
                    ),
                    "xv_net_crossings": sum(
                        1 for r in net_positive if not r["same_venue"]
                    ),
                    "xv_net_fresh_crossings": sum(
                        1 for r in fresh_net if not r["same_venue"]
                    ),
                    "xv_net_fresh_tight_crossings": sum(
                        1 for r in fresh_net
                        if not r["same_venue"] and r["buy_leg_width"] <= 0.05
                    ),
                    "median_mid_spread": statistics.median(
                        [r["mid_spread"] for r in rows]
                    ),
                    "median_widest_book": statistics.median(
                        [r["widest_book"] for r in rows]
                    ),
                    "max_skew_seconds": max(r["skew"] for r in rows),
                })

    surviving.sort(key=lambda r: -r["net_crossing"])
    records.sort(key=lambda r: -r["crossing_max"])
    total_bars = sum(r["bars"] for r in records)
    total_crossed = sum(r["bars_crossed"] for r in records)
    report = {
        "generated_at": utc_now(),
        "events_covered": events_covered,
        "classes": len(records),
        "bars": total_bars,
        "bars_crossed": total_crossed,
        "bars_net_crossed": sum(r["bars_net_crossed"] for r in records),
        "bars_fresh": sum(r["bars_fresh"] for r in records),
        "bars_fresh_net_crossed": sum(r["bars_fresh_net_crossed"] for r in records),
        "xv_net": sum(r["xv_net_crossings"] for r in records),
        "xv_net_fresh": sum(r["xv_net_fresh_crossings"] for r in records),
        "xv_net_fresh_tight": sum(r["xv_net_fresh_tight_crossings"] for r in records),
        "same_venue_crossings": sum(r["same_venue_crossings"] for r in records),
        "bars_crossed_over_1c": sum(r["bars_crossed_over_1c"] for r in records),
        "crossing_rate": (total_crossed / total_bars) if total_bars else 0.0,
        "median_mid_spread": statistics.median(
            [r["median_mid_spread"] for r in records]
        ) if records else None,
        "median_widest_book": statistics.median(
            [r["median_widest_book"] for r in records]
        ) if records else None,
        "surviving_bars": surviving,
        "records": records,
    }
    out = data / "analysis" / "cross_venue_crossing.json"
    write_json(out, report)

    print(f"events with full depth both sides: {len(events_covered)}")
    print(f"equivalence classes: {report['classes']}   bars: {total_bars:,}")
    print(f"bars where routes CROSS (best bid > best ask): {total_crossed} "
          f"({report['crossing_rate']*100:.3f}%)")
    print(f"  of which > 1c: {report['bars_crossed_over_1c']}")
    print(f"bars still crossed AFTER Kalshi fees: {report['bars_net_crossed']} "
          f"({report['bars_net_crossed']/total_bars*100:.3f}%)")
    print(f"  restricted to quotes <=5s old: {report['bars_fresh_net_crossed']} "
          f"of {report['bars_fresh']} fresh bars")
    print(f"\n  of the crossings, same-venue (Sigma-bid>1, already known): "
          f"{report['same_venue_crossings']}")
    print(f"  genuinely cross-venue, net of fees:            {report['xv_net']}")
    print(f"    ...and quote <=5s old:                       {report['xv_net_fresh']}")
    print(f"    ...and bought leg's book <=5c wide:          {report['xv_net_fresh_tight']}")
    print(f"\nmedian mid-to-mid disagreement across routes: "
          f"{report['median_mid_spread']:.4f}")
    print(f"median widest book in a class:                 "
          f"{report['median_widest_book']:.4f}")
    print(f"\n{'event':26s} {'st':5s} {'bars':>5s} {'crossMax':>9s} {'crossed':>8s} "
          f"{'midGap':>7s} {'widest':>7s}")
    for record in records[:15]:
        print(f"{record['event_key'][:25]:26s} {record['state_prefix'][:5]:5s} "
              f"{record['bars']:5d} {record['crossing_max']:9.4f} "
              f"{record['bars_crossed']:8d} {record['median_mid_spread']:7.4f} "
              f"{record['median_widest_book']:7.4f}")
    print(f"\nwrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
