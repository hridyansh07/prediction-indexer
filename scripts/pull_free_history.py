"""Pull sibling market history from free venue APIs only.

Kalshi supplies one-minute candlesticks and the public trade tape; Polymarket
supplies CLOB price series. Oddpool is never contacted, so this script consumes
no metered request budget.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from analysis.durable_http import RetryingJsonClient
from analysis.kalshi_history import pull_kalshi_history
from analysis.polymarket_history import pull_polymarket_history
from analysis.storage import write_json


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--data-directory", default=str(PROJECT_ROOT / "data"))
    parser.add_argument("--period-minutes", type=int, default=1)
    parser.add_argument("--skip-trades", action="store_true")
    parser.add_argument("--skip-polymarket", action="store_true")
    parser.add_argument("--skip-kalshi", action="store_true")
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=120.0,
        help="Socket timeout; large candlestick and trade pages exceed the 30s default.",
    )
    arguments = parser.parse_args()

    manifest_path = Path(arguments.manifest)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    data_directory = Path(arguments.data_directory)
    client = RetryingJsonClient(
        cache_root=data_directory / "cache" / "http",
        timeout_seconds=arguments.timeout_seconds,
    )

    coverage: dict[str, object] = {
        "dataset_name": manifest.get("dataset_name"),
        "manifest_path": str(manifest_path),
    }

    if not arguments.skip_kalshi:
        summary = pull_kalshi_history(
            client,
            data_directory=data_directory,
            manifest=manifest,
            period_minutes=arguments.period_minutes,
            include_trades=not arguments.skip_trades,
        )
        coverage["kalshi"] = {
            "job_id": summary["job_id"],
            "job_directory": summary["job_directory"],
            "target_count": summary["target_count"],
            "targets_complete": summary["targets_complete"],
            "candlestick_records": summary["candlestick_records"],
            "trade_records": summary["trade_records"],
        }
        print(
            f"kalshi: {summary['target_count']} markets, "
            f"{summary['candlestick_records']:,} candlesticks, "
            f"{summary['trade_records']:,} trades -> {summary['job_directory']}"
        )

    if not arguments.skip_polymarket:
        summary = pull_polymarket_history(
            client,
            data_directory=data_directory,
            manifest=manifest,
        )
        coverage["polymarket"] = {
            "job_id": summary["job_id"],
            "job_directory": summary["job_directory"],
            "token_count": summary["token_count"],
            "tokens_with_data": summary["tokens_with_data"],
            "records": summary["records"],
        }
        print(
            f"polymarket: {summary['tokens_with_data']}/{summary['token_count']} "
            f"tokens with data, {summary['records']:,} points "
            f"-> {summary['job_directory']}"
        )

    coverage["http"] = {
        "cache_hits": client.cache_hits,
        "network_requests": client.network_requests,
    }
    dataset = str(manifest.get("dataset_name") or manifest_path.stem)
    coverage_path = data_directory / "history" / f"{dataset}_coverage.json"
    write_json(coverage_path, coverage)
    print(
        f"http: cache_hits={client.cache_hits} "
        f"network_requests={client.network_requests} -> {coverage_path}"
    )


if __name__ == "__main__":
    main()
