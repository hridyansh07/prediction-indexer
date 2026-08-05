"""Discover sibling markets for the World Cup knockout and EWC Dota datasets.

Both datasets are described here so a rerun is reproducible from one command.
Discovery only touches the free Kalshi and Polymarket Gamma APIs.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from analysis.durable_http import DurableJsonClient
from analysis.sibling_markets import (
    DOTA_SIBLING_SERIES,
    WORLD_CUP_STRUCTURAL_SERIES,
    build_sibling_manifest,
    discover_kalshi_siblings,
    discover_polymarket_siblings,
)
from analysis.storage import write_json


PROJECT_ROOT = Path(__file__).resolve().parents[1]

WORLD_CUP_FIXTURES = {
    "wc-2026-07-14-fra-esp": "26JUL14FRAESP",
    "wc-2026-07-15-eng-arg": "26JUL15ENGARG",
    "wc-2026-07-19-esp-arg": "26JUL19ESPARG",
}
WORLD_CUP_SLUGS = {
    "wc-2026-07-14-fra-esp": "fifwc-fra-esp-2026-07-14",
    "wc-2026-07-15-eng-arg": "fifwc-eng-arg-2026-07-15",
    "wc-2026-07-19-esp-arg": "fifwc-esp-arg-2026-07-19",
}
WORLD_CUP_OUTCOMES = {
    "wc-2026-07-14-fra-esp": ("France", "Spain"),
    "wc-2026-07-15-eng-arg": ("England", "Argentina"),
    "wc-2026-07-19-esp-arg": ("Spain", "Argentina"),
}

DOTA_FIXTURES = {
    "dota2-ngx-bb4-2026-07-16": "26JUL160700BBNGX",
    "dota2-flc-vg-2026-07-16": "26JUL161030VGFLC",
    "dota2-ty-ts8-2026-07-17": "26JUL170700TSTY",
    "dota2-pari-re-2026-07-17": "26JUL171030REPARI",
    "dota2-bb4-vg-2026-07-18": "26JUL180700VGBB",
    "dota2-ty-pari-2026-07-18": "26JUL181030PARITY",
    "dota2-vg-ty-2026-07-19": "26JUL190600TYVG",
    "dota2-bb4-pari-2026-07-19": "26JUL190930PARIBB",
}
DOTA_SLUGS = {key: key for key in DOTA_FIXTURES}

# Structural Dota types only: the novelty markets in these events (first blood,
# rampage, total kills, Roshan) resolve on in-game incidents rather than on the
# series outcome space, so they cannot share a mask with the map winners.
DOTA_MARKET_TYPES = (
    "series_moneyline",
    "map_winner",
    "total_maps",
    "map_handicap",
)


def read_ndjson(path: Path) -> list[dict]:
    rows: list[dict] = []
    if not path.exists():
        return rows
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                rows.append(json.loads(line))
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-directory", default=str(PROJECT_ROOT / "data"))
    parser.add_argument(
        "--dataset",
        choices=("world_cup", "dota", "both"),
        default="both",
    )
    parser.add_argument("--refresh", action="store_true")
    arguments = parser.parse_args()

    data_directory = Path(arguments.data_directory)
    client = DurableJsonClient(
        cache_root=data_directory / "cache" / "http",
        force_refresh=arguments.refresh,
    )

    if arguments.dataset in ("world_cup", "both"):
        kalshi = discover_kalshi_siblings(
            client,
            data_directory=data_directory,
            dataset_name="wc_knockout_2026",
            series_tickers=WORLD_CUP_STRUCTURAL_SERIES,
            fixture_codes=WORLD_CUP_FIXTURES,
        )
        polymarket = discover_polymarket_siblings(
            client,
            data_directory=data_directory,
            dataset_name="wc_knockout_2026",
            event_slugs=WORLD_CUP_SLUGS,
            moneyline_outcomes=WORLD_CUP_OUTCOMES,
        )
        manifest = build_sibling_manifest(
            dataset_name="wc_knockout_2026",
            kalshi_markets=read_ndjson(Path(kalshi["job_directory"]) / "markets.ndjson"),
            polymarket_markets=read_ndjson(
                Path(polymarket["job_directory"]) / "markets.ndjson"
            ),
        )
        path = data_directory / "manifests" / "wc_knockout_2026.json"
        write_json(path, manifest)
        print(
            f"world_cup: kalshi={kalshi['market_count']} "
            f"polymarket={polymarket['market_count']} "
            f"targets={manifest['history_target_count']} -> {path}"
        )
        if kalshi["series_without_fixture"]:
            print(f"  series with no fixture event: {kalshi['series_without_fixture']}")

    if arguments.dataset in ("dota", "both"):
        kalshi = discover_kalshi_siblings(
            client,
            data_directory=data_directory,
            dataset_name="ewc_dota_siblings",
            series_tickers=DOTA_SIBLING_SERIES,
            fixture_codes=DOTA_FIXTURES,
        )
        polymarket = discover_polymarket_siblings(
            client,
            data_directory=data_directory,
            dataset_name="ewc_dota_siblings",
            event_slugs=DOTA_SLUGS,
        )
        manifest = build_sibling_manifest(
            dataset_name="ewc_dota_siblings",
            kalshi_markets=read_ndjson(Path(kalshi["job_directory"]) / "markets.ndjson"),
            polymarket_markets=read_ndjson(
                Path(polymarket["job_directory"]) / "markets.ndjson"
            ),
            include_market_types=DOTA_MARKET_TYPES,
        )
        path = data_directory / "manifests" / "ewc_dota_siblings.json"
        write_json(path, manifest)
        print(
            f"dota: kalshi={kalshi['market_count']} "
            f"polymarket={polymarket['market_count']} "
            f"targets={manifest['history_target_count']} -> {path}"
        )
        print(f"  polymarket types: {polymarket['market_type_counts']}")

    print(
        f"http: cache_hits={client.cache_hits} "
        f"network_requests={client.network_requests}"
    )


if __name__ == "__main__":
    main()
