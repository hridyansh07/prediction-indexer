from __future__ import annotations

import json
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from analysis.durable_http import DurableJsonClient, HttpRequestError
from analysis.ndjson_sink import (
    append_rows as _append_rows,
    existing_fingerprints as _existing_fingerprints,
    row_fingerprint as _snapshot_fingerprint,
    safe_name as _safe_name,
)
from analysis.storage import (
    parse_iso8601,
    stable_job_id,
    utc_now,
    write_json,
)


ODDPOOL_BASE_URL = "https://api.oddpool.com"


def load_oddpool_api_key(env_path: Path) -> str:
    environment_value = os.environ.get("ODDPOOL_API_KEY")
    if environment_value:
        return environment_value

    path = Path(env_path)
    if not path.exists():
        raise ValueError(f"Missing {path}; set ODDPOOL_API_KEY there or in the environment")
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        name, value = stripped.split("=", 1)
        if name.strip() != "ODDPOOL_API_KEY":
            continue
        api_key = value.strip().strip("\"'")
        if api_key:
            return api_key
    raise ValueError("ODDPOOL_API_KEY is missing or empty")


def _unix_milliseconds(value: str | None) -> int | None:
    parsed = parse_iso8601(value)
    return round(parsed.timestamp() * 1000) if parsed else None


def pull_orderbook_target(
    client: DurableJsonClient,
    *,
    api_key: str,
    target: Mapping[str, Any],
    target_directory: Path,
    granularity: str,
    page_limit: int,
    max_pages: int | None = None,
    max_network_requests: int | None = None,
) -> dict[str, Any]:
    venue = str(target.get("venue") or "")
    if venue not in {"kalshi", "polymarket"}:
        raise ValueError(f"Unsupported Oddpool venue {venue!r}")
    market_id = str(target.get("market_id") or "")
    if not market_id:
        raise ValueError("Oddpool history target is missing market_id")

    snapshots_path = target_directory / "snapshots.ndjson"
    checkpoint_path = target_directory / "checkpoint.json"
    checkpoint: dict[str, Any] = {}
    if checkpoint_path.exists():
        checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    if checkpoint.get("complete"):
        return checkpoint

    seen = _existing_fingerprints(snapshots_path)
    pagination_key = checkpoint.get("next_pagination_key")
    pages_completed = int(checkpoint.get("pages_completed", 0))
    records_written = int(checkpoint.get("records_written", len(seen)))
    cursors_seen = set(checkpoint.get("cursors_seen") or [])
    endpoint = f"/historical/{venue}/orderbook"
    request_parameters = {
        "market_id": market_id,
        "asset_id": target.get("asset_id"),
        "start_time": _unix_milliseconds(target.get("start_time")),
        "end_time": _unix_milliseconds(target.get("end_time")),
        "granularity": granularity,
        "limit": page_limit,
    }
    stopped_reason: str | None = None

    while True:
        if max_pages is not None and pages_completed >= max_pages:
            stopped_reason = "max_pages_reached"
            break
        if (
            max_network_requests is not None
            and client.network_requests >= max_network_requests
        ):
            stopped_reason = "network_request_budget_reached"
            break

        params = {**request_parameters, "pagination_key": pagination_key}
        try:
            response = client.get_json(
                ODDPOOL_BASE_URL,
                endpoint,
                params=params,
                headers={"X-API-Key": api_key},
            )
        except HttpRequestError as error:
            checkpoint = {
                **checkpoint,
                "target_id": target.get("target_id"),
                "venue": venue,
                "market_id": market_id,
                "complete": False,
                "error": str(error),
                "updated_at": utc_now(),
            }
            write_json(checkpoint_path, checkpoint)
            raise

        payload = response.data
        if not isinstance(payload, dict):
            raise ValueError(f"Unexpected Oddpool response for {response.url}")
        snapshots = payload.get("snapshots") or []
        pagination = payload.get("pagination") or {}
        if not isinstance(snapshots, list) or not isinstance(pagination, dict):
            raise ValueError(f"Unexpected Oddpool payload shape for {response.url}")

        new_rows: list[dict[str, Any]] = []
        page_number = pages_completed + 1
        for snapshot in snapshots:
            if not isinstance(snapshot, dict):
                continue
            fingerprint = _snapshot_fingerprint(snapshot)
            if fingerprint in seen:
                continue
            seen.add(fingerprint)
            row = dict(snapshot)
            row["_provenance"] = {
                "source": "oddpool",
                "endpoint": endpoint,
                "event_key": target.get("event_key"),
                "target_id": target.get("target_id"),
                "request_parameters": request_parameters,
                "page_number": page_number,
                "fetched_at": response.fetched_at,
                "from_cache": response.from_cache,
            }
            new_rows.append(row)
        _append_rows(snapshots_path, new_rows)
        records_written += len(new_rows)
        pages_completed += 1

        has_more = bool(pagination.get("has_more"))
        next_key = pagination.get("pagination_key") if has_more else None
        if next_key is not None:
            next_key = str(next_key)
            if next_key in cursors_seen:
                raise RuntimeError(f"Oddpool repeated a pagination cursor for {market_id}")
            cursors_seen.add(next_key)

        checkpoint = {
            "target_id": target.get("target_id"),
            "event_key": target.get("event_key"),
            "venue": venue,
            "market_id": market_id,
            "asset_id": target.get("asset_id"),
            "granularity": granularity,
            "page_limit": page_limit,
            "pages_completed": pages_completed,
            "records_written": records_written,
            "next_pagination_key": next_key,
            "cursors_seen": sorted(cursors_seen),
            "complete": not has_more,
            "stopped_reason": None,
            "snapshots_path": str(snapshots_path),
            "last_request_url": response.url,
            "updated_at": utc_now(),
        }
        write_json(checkpoint_path, checkpoint)
        if not has_more:
            return checkpoint
        pagination_key = next_key

    checkpoint = {
        **checkpoint,
        "target_id": target.get("target_id"),
        "event_key": target.get("event_key"),
        "venue": venue,
        "market_id": market_id,
        "granularity": granularity,
        "page_limit": page_limit,
        "pages_completed": pages_completed,
        "records_written": records_written,
        "next_pagination_key": pagination_key,
        "cursors_seen": sorted(cursors_seen),
        "complete": False,
        "stopped_reason": stopped_reason,
        "snapshots_path": str(snapshots_path),
        "updated_at": utc_now(),
    }
    write_json(checkpoint_path, checkpoint)
    return checkpoint


def pull_manifest_history(
    client: DurableJsonClient,
    *,
    api_key: str,
    manifest: Mapping[str, Any],
    output_root: Path,
    granularity: str = "1m",
    page_limit: int = 200,
    max_pages_per_target: int | None = None,
    max_network_requests: int | None = 900,
) -> dict[str, Any]:
    targets = manifest.get("history_targets") or []
    if not isinstance(targets, list):
        raise ValueError("Manifest history_targets must be a list")
    specification = {
        "manifest_version": manifest.get("version"),
        "event_keys": sorted(
            str(match.get("event_key"))
            for match in manifest.get("matches") or []
            if isinstance(match, dict)
        ),
        "target_ids": sorted(
            str(target.get("target_id"))
            for target in targets
            if isinstance(target, dict)
        ),
        "granularity": granularity,
        "page_limit": page_limit,
    }
    job_id = stable_job_id(specification)
    job_directory = Path(output_root) / job_id
    write_json(job_directory / "request.json", specification)

    target_results: list[dict[str, Any]] = []
    run_summary: dict[str, Any] = {}
    for target in targets:
        if not isinstance(target, dict):
            continue
        venue = str(target.get("venue") or "")
        market_id = str(target.get("market_id") or "")
        target_directory = (
            job_directory / venue / _safe_name(market_id)
        )
        result = pull_orderbook_target(
            client,
            api_key=api_key,
            target=target,
            target_directory=target_directory,
            granularity=granularity,
            page_limit=page_limit,
            max_pages=max_pages_per_target,
            max_network_requests=max_network_requests,
        )
        target_results.append(result)
        run_summary = {
            "job_id": job_id,
            "generated_at": utc_now(),
            "complete": (
                len(target_results) == len(targets)
                and all(result.get("complete") for result in target_results)
            ),
            "target_count": len(targets),
            "targets_processed": len(target_results),
            "targets_complete": sum(
                1 for result in target_results if result.get("complete")
            ),
            "records_written": sum(
                int(result.get("records_written", 0)) for result in target_results
            ),
            "http": {
                "cache_hits": client.cache_hits,
                "network_requests": client.network_requests,
            },
            "job_directory": str(job_directory),
            "targets": target_results,
        }
        write_json(job_directory / "run.json", run_summary)
        if result.get("stopped_reason") == "network_request_budget_reached":
            break

    return run_summary
