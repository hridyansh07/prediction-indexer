#!/usr/bin/env python3
"""Validate a free-source sibling history pull and report per-event coverage.

Checks that every manifest target produced rows, that timestamps fall inside
the market's declared life, and that each event's sibling market types are
present on the venues that list them. Writes a machine-readable coverage file
next to the manifest's history jobs.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from analysis.ndjson_sink import safe_name
from analysis.storage import iso_to_unix_seconds, utc_now, write_json


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--data-directory", type=Path, default=PROJECT_ROOT / "data")
    return parser.parse_args()


def _read_checkpoint(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _count_rows(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open(encoding="utf-8") as handle:
        return sum(1 for line in handle if line.strip())


def inspect_kalshi(target: dict[str, Any], job_directory: Path) -> dict[str, Any]:
    directory = job_directory / safe_name(str(target["ticker"]))
    candles = _read_checkpoint(directory / "checkpoint.json")
    trades = _read_checkpoint(directory / "trades_checkpoint.json")
    open_seconds = iso_to_unix_seconds(target.get("open_time"))
    close_seconds = iso_to_unix_seconds(target.get("close_time"))

    out_of_window = 0
    path = directory / "candlesticks.ndjson"
    if path.exists() and open_seconds is not None and close_seconds is not None:
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                timestamp = json.loads(line).get("end_period_ts")
                if timestamp is None:
                    continue
                if not open_seconds <= int(timestamp) <= close_seconds + 60:
                    out_of_window += 1

    return {
        "target_id": target["target_id"],
        "ticker": target["ticker"],
        "event_key": target["event_key"],
        "market_type": target["market_type"],
        "candlestick_rows": _count_rows(directory / "candlesticks.ndjson"),
        "candlestick_complete": bool(candles.get("complete")),
        "trade_rows": _count_rows(directory / "trades.ndjson"),
        "trade_complete": bool(trades.get("complete")),
        "candlesticks_out_of_window": out_of_window,
    }


def inspect_polymarket(target: dict[str, Any], job_directory: Path) -> list[dict[str, Any]]:
    condition_directory = job_directory / safe_name(str(target["market_id"]))
    rows: list[dict[str, Any]] = []
    for token in target.get("outcome_tokens") or []:
        asset_id = str(token.get("asset_id") or "")
        if not asset_id:
            continue
        directory = condition_directory / safe_name(asset_id)
        checkpoint = _read_checkpoint(directory / "checkpoint.json")
        rows.append(
            {
                "target_id": target["target_id"],
                "event_key": target["event_key"],
                "market_type": target["market_type"],
                "asset_id": asset_id,
                "outcome": token.get("outcome"),
                "price_rows": _count_rows(directory / "prices.ndjson"),
                "complete": bool(checkpoint.get("complete")),
            }
        )
    return rows


def main() -> int:
    arguments = parse_args()
    manifest = json.loads(arguments.manifest.read_text(encoding="utf-8"))
    dataset = str(manifest.get("dataset_name") or arguments.manifest.stem)
    coverage_path = arguments.data_directory / "history" / f"{dataset}_coverage.json"
    if not coverage_path.exists():
        print(f"missing {coverage_path}; run scripts/pull_free_history.py first")
        return 1
    coverage = json.loads(coverage_path.read_text(encoding="utf-8"))

    kalshi_rows: list[dict[str, Any]] = []
    polymarket_rows: list[dict[str, Any]] = []
    kalshi_job = Path(coverage.get("kalshi", {}).get("job_directory", ""))
    polymarket_job = Path(coverage.get("polymarket", {}).get("job_directory", ""))

    for target in manifest["history_targets"]:
        if target["venue"] == "kalshi" and kalshi_job.exists():
            kalshi_rows.append(inspect_kalshi(target, kalshi_job))
        elif target["venue"] == "polymarket" and polymarket_job.exists():
            polymarket_rows.extend(inspect_polymarket(target, polymarket_job))

    empty_kalshi = [r for r in kalshi_rows if r["candlestick_rows"] == 0]
    empty_polymarket = [r for r in polymarket_rows if r["price_rows"] == 0]
    incomplete = [r for r in kalshi_rows if not r["candlestick_complete"]]
    out_of_window = [r for r in kalshi_rows if r["candlesticks_out_of_window"] > 0]

    events: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"kalshi_types": defaultdict(int), "polymarket_types": defaultdict(int),
                 "candlestick_rows": 0, "trade_rows": 0, "price_rows": 0}
    )
    for row in kalshi_rows:
        bucket = events[row["event_key"]]
        bucket["kalshi_types"][row["market_type"]] += 1
        bucket["candlestick_rows"] += row["candlestick_rows"]
        bucket["trade_rows"] += row["trade_rows"]
    for row in polymarket_rows:
        bucket = events[row["event_key"]]
        bucket["polymarket_types"][row["market_type"]] += 1
        bucket["price_rows"] += row["price_rows"]

    print(f"=== {dataset} ===")
    print(
        f"kalshi targets: {len(kalshi_rows)}  "
        f"complete: {len(kalshi_rows) - len(incomplete)}  empty: {len(empty_kalshi)}"
    )
    print(
        f"polymarket tokens: {len(polymarket_rows)}  empty: {len(empty_polymarket)}"
    )
    print()
    for event_key in sorted(events):
        bucket = events[event_key]
        print(
            f"  {event_key:28s} candles={bucket['candlestick_rows']:7,d} "
            f"trades={bucket['trade_rows']:7,d} prices={bucket['price_rows']:8,d}"
        )
        print(
            f"      kalshi     {dict(sorted(bucket['kalshi_types'].items()))}"
        )
        print(
            f"      polymarket {dict(sorted(bucket['polymarket_types'].items()))}"
        )

    if empty_kalshi:
        print(f"\nkalshi targets with no candlesticks ({len(empty_kalshi)}):")
        for row in empty_kalshi[:20]:
            print(f"  {row['ticker']} [{row['market_type']}]")
    if empty_polymarket:
        print(f"\npolymarket tokens with no prices ({len(empty_polymarket)}):")
        for row in empty_polymarket[:20]:
            print(f"  {row['target_id']} {row['outcome']}")
    if out_of_window:
        print(f"\ntargets with candlesticks outside their market life: {len(out_of_window)}")

    report = {
        "dataset_name": dataset,
        "generated_at": utc_now(),
        "manifest_path": str(arguments.manifest),
        "kalshi_target_count": len(kalshi_rows),
        "kalshi_empty_count": len(empty_kalshi),
        "kalshi_incomplete_count": len(incomplete),
        "polymarket_token_count": len(polymarket_rows),
        "polymarket_empty_count": len(empty_polymarket),
        "out_of_window_target_count": len(out_of_window),
        "events": {
            key: {
                "kalshi_types": dict(sorted(value["kalshi_types"].items())),
                "polymarket_types": dict(sorted(value["polymarket_types"].items())),
                "candlestick_rows": value["candlestick_rows"],
                "trade_rows": value["trade_rows"],
                "price_rows": value["price_rows"],
            }
            for key, value in sorted(events.items())
        },
        "kalshi_targets": kalshi_rows,
        "polymarket_tokens": polymarket_rows,
    }
    report_path = (
        arguments.data_directory / "history" / f"{dataset}_validation.json"
    )
    write_json(report_path, report)
    print(f"\nwrote {report_path}")
    return 0 if not (empty_kalshi or incomplete or out_of_window) else 2


if __name__ == "__main__":
    raise SystemExit(main())
