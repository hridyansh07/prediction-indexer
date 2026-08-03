"""Free historical price series from Polymarket's CLOB.

``https://clob.polymarket.com/prices-history`` returns ``{"t": unix_seconds,
"p": price}`` points for one CLOB token. Passing ``startTs``/``endTs`` with
``fidelity=1`` yields minute-resolution points and returns the whole range in a
single response; the ``interval`` form (``max``, ``1d`` ...) downsamples
heavily and is not used here.

The series carries a single price per point — not a bid/ask pair and not depth.
Polymarket's ``/book`` endpoint serves live books only and 404s once a market
settles, so depth for closed Polymarket markets is available exclusively from a
paid archive. Instruments built from these rows are price-only.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from analysis.durable_http import DurableJsonClient, HttpRequestError
from analysis.ndjson_sink import (
    append_rows,
    existing_fingerprints,
    row_fingerprint,
    safe_name,
)
from analysis.storage import (
    iso_to_unix_seconds,
    stable_job_id,
    utc_now,
    write_json,
)


CLOB_BASE_URL = "https://clob.polymarket.com"


def parse_price_points(
    payload: Any,
    *,
    condition_id: str,
    asset_id: str,
) -> list[dict[str, Any]]:
    """Normalize a prices-history payload into stable rows."""
    if not isinstance(payload, dict):
        raise ValueError("Polymarket prices-history payload is not an object")
    history = payload.get("history")
    if not isinstance(history, list):
        raise ValueError("Polymarket prices-history payload has no history list")

    rows: list[dict[str, Any]] = []
    for point in history:
        if not isinstance(point, dict):
            continue
        timestamp = point.get("t")
        price = point.get("p")
        if timestamp is None or price is None:
            continue
        rows.append(
            {
                "condition_id": condition_id,
                "asset_id": asset_id,
                "timestamp_seconds": int(timestamp),
                "price": float(price),
            }
        )
    rows.sort(key=lambda row: row["timestamp_seconds"])
    return rows


def pull_price_history_token(
    client: DurableJsonClient,
    *,
    target: Mapping[str, Any],
    asset_id: str,
    target_directory: Path,
    fidelity_minutes: int = 1,
) -> dict[str, Any]:
    """Pull the full price series for one CLOB token."""
    condition_id = str(target["market_id"])
    start_seconds = iso_to_unix_seconds(target.get("start_time"))
    end_seconds = iso_to_unix_seconds(target.get("end_time"))
    if start_seconds is None or end_seconds is None:
        raise ValueError(f"{condition_id} is missing start_time or end_time")

    rows_path = Path(target_directory) / "prices.ndjson"
    checkpoint_path = Path(target_directory) / "checkpoint.json"
    checkpoint: dict[str, Any] = {}
    if checkpoint_path.exists():
        checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    if checkpoint.get("complete"):
        return checkpoint

    request_parameters = {
        "market": asset_id,
        "startTs": start_seconds,
        "endTs": end_seconds,
        "fidelity": fidelity_minutes,
    }
    try:
        response = client.get_json(
            CLOB_BASE_URL,
            "/prices-history",
            params=request_parameters,
        )
    except HttpRequestError as error:
        checkpoint = {
            **checkpoint,
            "condition_id": condition_id,
            "asset_id": asset_id,
            "complete": False,
            "error": str(error),
            "updated_at": utc_now(),
        }
        write_json(checkpoint_path, checkpoint)
        raise

    parsed = parse_price_points(
        response.data,
        condition_id=condition_id,
        asset_id=asset_id,
    )
    seen = existing_fingerprints(rows_path)
    new_rows: list[dict[str, Any]] = []
    for row in parsed:
        fingerprint = row_fingerprint(row)
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        new_rows.append(
            {
                **row,
                "_provenance": {
                    "source": "polymarket_clob",
                    "endpoint": "/prices-history",
                    "event_key": target.get("event_key"),
                    "market_type": target.get("market_type"),
                    "outcome": target.get("outcome"),
                    "request_parameters": request_parameters,
                    "fetched_at": response.fetched_at,
                    "from_cache": response.from_cache,
                },
            }
        )
    append_rows(rows_path, new_rows)

    checkpoint = {
        "condition_id": condition_id,
        "asset_id": asset_id,
        "event_key": target.get("event_key"),
        "market_type": target.get("market_type"),
        "outcome": target.get("outcome"),
        "fidelity_minutes": fidelity_minutes,
        "records_written": len(seen),
        "points_returned": len(parsed),
        "first_timestamp_seconds": parsed[0]["timestamp_seconds"] if parsed else None,
        "last_timestamp_seconds": parsed[-1]["timestamp_seconds"] if parsed else None,
        "complete": True,
        "rows_path": str(rows_path),
        "last_request_url": response.url,
        "updated_at": utc_now(),
    }
    write_json(checkpoint_path, checkpoint)
    return checkpoint


def _token_ids(target: Mapping[str, Any]) -> list[tuple[str, str | None]]:
    tokens = target.get("outcome_tokens")
    if isinstance(tokens, Sequence) and not isinstance(tokens, (str, bytes)):
        pairs = [
            (str(token.get("asset_id")), token.get("outcome"))
            for token in tokens
            if isinstance(token, Mapping) and token.get("asset_id")
        ]
        if pairs:
            return pairs
    asset_id = target.get("asset_id")
    return [(str(asset_id), target.get("outcome"))] if asset_id else []


def pull_polymarket_history(
    client: DurableJsonClient,
    *,
    data_directory: Path,
    manifest: Mapping[str, Any],
    fidelity_minutes: int = 1,
) -> dict[str, Any]:
    """Pull price series for every Polymarket token in the manifest."""
    targets = [
        target
        for target in manifest.get("history_targets") or []
        if target.get("venue") == "polymarket"
    ]
    specification = {
        "source": "polymarket_clob_prices",
        "dataset_name": manifest.get("dataset_name"),
        "manifest_version": manifest.get("version"),
        "fidelity_minutes": fidelity_minutes,
        "condition_ids": sorted(str(target["market_id"]) for target in targets),
    }
    job_id = stable_job_id(specification)
    job_directory = Path(data_directory) / "history" / "polymarket" / job_id
    write_json(job_directory / "request.json", specification)

    results: list[dict[str, Any]] = []
    for target in sorted(targets, key=lambda item: str(item["market_id"])):
        condition_id = str(target["market_id"])
        for asset_id, outcome in _token_ids(target):
            target_directory = (
                job_directory / safe_name(condition_id) / safe_name(asset_id)
            )
            checkpoint = pull_price_history_token(
                client,
                target={**target, "outcome": outcome or target.get("outcome")},
                asset_id=asset_id,
                target_directory=target_directory,
                fidelity_minutes=fidelity_minutes,
            )
            results.append(
                {
                    "condition_id": condition_id,
                    "asset_id": asset_id,
                    "event_key": target.get("event_key"),
                    "market_type": target.get("market_type"),
                    "outcome": outcome,
                    "records": checkpoint.get("records_written", 0),
                    "complete": bool(checkpoint.get("complete")),
                }
            )

    summary = {
        "job_id": job_id,
        "generated_at": utc_now(),
        "specification": specification,
        "job_directory": str(job_directory),
        "token_count": len(results),
        "records": sum(item["records"] for item in results),
        "tokens_with_data": sum(1 for item in results if item["records"] > 0),
        "tokens": results,
        "http": {
            "cache_hits": client.cache_hits,
            "network_requests": client.network_requests,
        },
    }
    write_json(job_directory / "run.json", summary)
    return summary
