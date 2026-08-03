"""Free historical pullers for Kalshi's own API.

Two public, market-wide sources are used here:

``/series/{series}/markets/{ticker}/candlesticks``
    One-minute top-of-book OHLC (``yes_bid`` / ``yes_ask``) plus traded price,
    volume and open interest. The endpoint has no cursor: it returns every
    candlestick in the requested window, but rejects windows longer than
    ``MAX_CANDLESTICK_PERIODS`` periods, so long-lived markets are chunked.

``/markets/trades``
    The public trade tape: executed size (``count_fp``), price, and taker
    direction. Cursor paginated.

Neither source carries book depth. Kalshi exposes no historical orderbook
endpoint, so instruments built from these rows are top-of-book only and must
not be fed to a size-adjusted VWAP walk. The account-scoped ``/historical``
endpoints (fills, orders, positions) return only the authenticated member's own
activity and are deliberately not used.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
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


KALSHI_BASE_URL = "https://external-api.kalshi.com/trade-api/v2"

# The API rejects windows longer than 5000 periods; verified empirically at
# period_interval=1 (5000 minutes succeeds, 5001 returns HTTP 400).
MAX_CANDLESTICK_PERIODS = 5000


def candlestick_windows(
    start_seconds: int,
    end_seconds: int,
    *,
    period_minutes: int,
    max_periods: int = MAX_CANDLESTICK_PERIODS,
) -> list[tuple[int, int]]:
    """Split a request range into windows the candlestick endpoint accepts."""
    if end_seconds < start_seconds:
        raise ValueError("end_seconds precedes start_seconds")
    if period_minutes <= 0:
        raise ValueError("period_minutes must be positive")
    span = period_minutes * max_periods * 60
    windows: list[tuple[int, int]] = []
    cursor = start_seconds
    while cursor < end_seconds:
        windows.append((cursor, min(cursor + span, end_seconds)))
        cursor += span
    if not windows:
        windows.append((start_seconds, end_seconds))
    return windows


def _target_directory(root: Path, target: Mapping[str, Any]) -> Path:
    return Path(root) / safe_name(str(target["ticker"]))


def pull_candlestick_target(
    client: DurableJsonClient,
    *,
    target: Mapping[str, Any],
    target_directory: Path,
    period_minutes: int = 1,
) -> dict[str, Any]:
    """Pull every candlestick for one market, resuming from a checkpoint."""
    ticker = str(target["ticker"])
    series_ticker = str(target["series_ticker"])
    start_seconds = iso_to_unix_seconds(target.get("open_time"))
    end_seconds = iso_to_unix_seconds(target.get("close_time"))
    if start_seconds is None or end_seconds is None:
        raise ValueError(f"{ticker} is missing open_time or close_time")

    rows_path = Path(target_directory) / "candlesticks.ndjson"
    checkpoint_path = Path(target_directory) / "checkpoint.json"
    checkpoint: dict[str, Any] = {}
    if checkpoint_path.exists():
        checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    if checkpoint.get("complete"):
        return checkpoint

    seen = existing_fingerprints(rows_path)
    windows = candlestick_windows(
        start_seconds,
        end_seconds,
        period_minutes=period_minutes,
    )
    completed = int(checkpoint.get("windows_completed", 0))
    records_written = int(checkpoint.get("records_written", len(seen)))
    path = f"/series/{series_ticker}/markets/{ticker}/candlesticks"

    for index in range(completed, len(windows)):
        window_start, window_end = windows[index]
        request_parameters = {
            "start_ts": window_start,
            "end_ts": window_end,
            "period_interval": period_minutes,
        }
        try:
            response = client.get_json(
                KALSHI_BASE_URL,
                path,
                params=request_parameters,
            )
        except HttpRequestError as error:
            checkpoint = {
                **checkpoint,
                "ticker": ticker,
                "complete": False,
                "error": str(error),
                "updated_at": utc_now(),
            }
            write_json(checkpoint_path, checkpoint)
            raise

        payload = response.data
        if not isinstance(payload, dict):
            raise ValueError(f"Unexpected Kalshi response for {response.url}")
        candlesticks = payload.get("candlesticks")
        if not isinstance(candlesticks, list):
            raise ValueError(f"Kalshi returned no candlestick list for {response.url}")

        new_rows: list[dict[str, Any]] = []
        for candlestick in candlesticks:
            if not isinstance(candlestick, dict):
                continue
            row = dict(candlestick)
            row["ticker"] = ticker
            fingerprint = row_fingerprint(row)
            if fingerprint in seen:
                continue
            seen.add(fingerprint)
            row["_provenance"] = {
                "source": "kalshi",
                "endpoint": path,
                "event_key": target.get("event_key"),
                "market_type": target.get("market_type"),
                "request_parameters": request_parameters,
                "window_index": index,
                "fetched_at": response.fetched_at,
                "from_cache": response.from_cache,
            }
            new_rows.append(row)

        append_rows(rows_path, new_rows)
        records_written += len(new_rows)
        checkpoint = {
            "ticker": ticker,
            "series_ticker": series_ticker,
            "event_key": target.get("event_key"),
            "market_type": target.get("market_type"),
            "period_minutes": period_minutes,
            "window_count": len(windows),
            "windows_completed": index + 1,
            "records_written": records_written,
            "complete": index + 1 == len(windows),
            "rows_path": str(rows_path),
            "last_request_url": response.url,
            "updated_at": utc_now(),
        }
        write_json(checkpoint_path, checkpoint)

    return checkpoint


def pull_trades_target(
    client: DurableJsonClient,
    *,
    target: Mapping[str, Any],
    target_directory: Path,
    page_limit: int = 1000,
    max_pages: int | None = None,
) -> dict[str, Any]:
    """Pull the public trade tape for one market, resuming from a checkpoint."""
    ticker = str(target["ticker"])
    start_seconds = iso_to_unix_seconds(target.get("open_time"))
    end_seconds = iso_to_unix_seconds(target.get("close_time"))

    rows_path = Path(target_directory) / "trades.ndjson"
    checkpoint_path = Path(target_directory) / "trades_checkpoint.json"
    checkpoint: dict[str, Any] = {}
    if checkpoint_path.exists():
        checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    if checkpoint.get("complete"):
        return checkpoint

    seen = existing_fingerprints(rows_path)
    cursor = checkpoint.get("next_cursor")
    pages_completed = int(checkpoint.get("pages_completed", 0))
    records_written = int(checkpoint.get("records_written", len(seen)))
    cursors_seen = set(checkpoint.get("cursors_seen") or [])
    request_parameters = {
        "ticker": ticker,
        "min_ts": start_seconds,
        "max_ts": end_seconds,
        "limit": page_limit,
    }

    while True:
        if max_pages is not None and pages_completed >= max_pages:
            break
        params = {**request_parameters, "cursor": cursor}
        try:
            response = client.get_json(KALSHI_BASE_URL, "/markets/trades", params=params)
        except HttpRequestError as error:
            checkpoint = {
                **checkpoint,
                "ticker": ticker,
                "complete": False,
                "error": str(error),
                "updated_at": utc_now(),
            }
            write_json(checkpoint_path, checkpoint)
            raise

        payload = response.data
        if not isinstance(payload, dict):
            raise ValueError(f"Unexpected Kalshi response for {response.url}")
        trades = payload.get("trades")
        if not isinstance(trades, list):
            raise ValueError(f"Kalshi returned no trade list for {response.url}")

        new_rows: list[dict[str, Any]] = []
        for trade in trades:
            if not isinstance(trade, dict):
                continue
            fingerprint = row_fingerprint(trade)
            if fingerprint in seen:
                continue
            seen.add(fingerprint)
            row = dict(trade)
            row["_provenance"] = {
                "source": "kalshi",
                "endpoint": "/markets/trades",
                "event_key": target.get("event_key"),
                "market_type": target.get("market_type"),
                "request_parameters": request_parameters,
                "page_number": pages_completed + 1,
                "fetched_at": response.fetched_at,
                "from_cache": response.from_cache,
            }
            new_rows.append(row)

        append_rows(rows_path, new_rows)
        records_written += len(new_rows)
        pages_completed += 1

        next_cursor = payload.get("cursor") or None
        if next_cursor is not None:
            next_cursor = str(next_cursor)
            if next_cursor in cursors_seen:
                raise RuntimeError(f"Kalshi repeated a trade cursor for {ticker}")
            cursors_seen.add(next_cursor)

        # An empty page is the tape's terminator even when a cursor echoes back.
        complete = next_cursor is None or not trades
        checkpoint = {
            "ticker": ticker,
            "event_key": target.get("event_key"),
            "market_type": target.get("market_type"),
            "page_limit": page_limit,
            "pages_completed": pages_completed,
            "records_written": records_written,
            "next_cursor": None if complete else next_cursor,
            "cursors_seen": sorted(cursors_seen),
            "complete": complete,
            "rows_path": str(rows_path),
            "last_request_url": response.url,
            "updated_at": utc_now(),
        }
        write_json(checkpoint_path, checkpoint)
        if complete:
            break
        cursor = next_cursor

    return checkpoint


def pull_kalshi_history(
    client: DurableJsonClient,
    *,
    data_directory: Path,
    manifest: Mapping[str, Any],
    period_minutes: int = 1,
    include_trades: bool = True,
    page_limit: int = 1000,
) -> dict[str, Any]:
    """Pull candlesticks (and optionally trades) for every Kalshi manifest target."""
    targets = [
        target
        for target in manifest.get("history_targets") or []
        if target.get("venue") == "kalshi"
    ]
    # `include_trades` and `page_limit` steer how much of the job runs now, not
    # which dataset it is, so they stay out of the job identity. That lets a
    # candlesticks-first pass and a later trades pass share one job directory.
    specification = {
        "source": "kalshi_history",
        "dataset_name": manifest.get("dataset_name"),
        "manifest_version": manifest.get("version"),
        "period_minutes": period_minutes,
        "tickers": sorted(str(target["ticker"]) for target in targets),
    }
    job_id = stable_job_id(specification)
    job_directory = Path(data_directory) / "history" / "kalshi" / job_id
    write_json(job_directory / "request.json", specification)

    results: list[dict[str, Any]] = []
    for target in sorted(targets, key=lambda item: str(item["ticker"])):
        target_directory = _target_directory(job_directory, target)
        candles = pull_candlestick_target(
            client,
            target=target,
            target_directory=target_directory,
            period_minutes=period_minutes,
        )
        entry = {
            "ticker": str(target["ticker"]),
            "event_key": target.get("event_key"),
            "market_type": target.get("market_type"),
            "candlestick_records": candles.get("records_written", 0),
            "candlestick_complete": bool(candles.get("complete")),
        }
        if include_trades:
            trades = pull_trades_target(
                client,
                target=target,
                target_directory=target_directory,
                page_limit=page_limit,
            )
            entry["trade_records"] = trades.get("records_written", 0)
            entry["trade_complete"] = bool(trades.get("complete"))
        results.append(entry)

    summary = {
        "job_id": job_id,
        "generated_at": utc_now(),
        "specification": specification,
        "include_trades": include_trades,
        "page_limit": page_limit,
        "job_directory": str(job_directory),
        "target_count": len(results),
        "candlestick_records": sum(item["candlestick_records"] for item in results),
        "trade_records": sum(item.get("trade_records", 0) for item in results),
        "targets_complete": sum(1 for item in results if item["candlestick_complete"]),
        "targets": results,
        "http": {
            "cache_hits": client.cache_hits,
            "network_requests": client.network_requests,
        },
    }
    write_json(job_directory / "run.json", summary)
    return summary
