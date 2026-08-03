from __future__ import annotations

import hashlib
import json
import math
import statistics
from collections import Counter, defaultdict
from dataclasses import asdict
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence

from analysis.durable_http import DurableJsonClient, HttpRequestError
from analysis.partition_sum import (
    PARTITION_CROSS_VENUE,
    PARTITION_KALSHI_EVENT,
    BookSnapshot,
    FeeSchedule,
    InstrumentSeries,
    bar_snapshots,
    build_partition_definitions,
    fee_schedule_at,
    ladder_issues,
    level_count_diagnostics,
    measure_partition,
    normalize_kalshi_snapshot,
    normalize_polymarket_snapshot,
    polymarket_complement_diagnostics,
    validate_top_of_book,
)
from analysis.storage import parse_iso8601, write_json


KALSHI_BASE_URL = "https://external-api.kalshi.com/trade-api/v2"
FEE_EXECUTION_ASSUMPTIONS = {
    "execution": (
        "Each basket leg is a marketable taker order for the same number of "
        "contracts. It walks the displayed ladder without price improvement, "
        "maker rebates, or impact beyond displayed depth."
    ),
    "raw_fee_formula": (
        "sum_over_fills(rate * multiplier * quantity * "
        "(price * (1 - price)) ** exponent) + fixed_execution_cost"
    ),
    "basket_net_formula": (
        "net_gap_per_contract = 1 - sum(leg_costs) / N "
        "- sum(leg_fees) / N"
    ),
    "rounding": (
        "Fees are aggregated across fills within each leg. Normal rounding "
        "uses the venue/config increment; conservative rounding always rounds "
        "each leg fee upward to its conservative increment."
    ),
}


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_json(value: Any) -> str:
    return sha256_bytes(canonical_json(value).encode("utf-8"))


def relative_or_absolute(path: Path, project_root: Path) -> str:
    try:
        return str(path.resolve().relative_to(project_root.resolve()))
    except ValueError:
        return str(path.resolve())


def resolve_config_paths(
    config: Mapping[str, Any],
    project_root: Path,
) -> dict[str, Any]:
    resolved = json.loads(canonical_json(config))
    for key in (
        "manifest_path",
        "history_job_directory",
        "http_cache_root",
        "output_root",
    ):
        path = Path(str(resolved[key]))
        if not path.is_absolute():
            path = project_root / path
        resolved[key] = str(path.resolve())
    return resolved


def iso_to_milliseconds(value: str | None) -> int | None:
    parsed = parse_iso8601(value)
    return round(parsed.timestamp() * 1000) if parsed else None


def read_ndjson(path: Path) -> Iterator[dict[str, Any]]:
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            value = json.loads(line)
            if not isinstance(value, dict):
                raise ValueError(f"{path}:{line_number} is not a JSON object")
            yield value


def _walk_dicts(value: Any) -> Iterator[Mapping[str, Any]]:
    if isinstance(value, dict):
        yield value
        for nested in value.values():
            yield from _walk_dicts(nested)
    elif isinstance(value, list):
        for nested in value:
            yield from _walk_dicts(nested)


def resolve_polymarket_fees(
    manifest: Mapping[str, Any],
    cache_root: Path,
    fee_config: Mapping[str, Any],
) -> tuple[dict[str, FeeSchedule], dict[str, Any], list[str]]:
    condition_ids = {
        str(target["market_id"])
        for target in manifest.get("history_targets") or []
        if target.get("venue") == "polymarket"
    }
    records: dict[str, list[tuple[dict[str, Any], Path]]] = defaultdict(list)
    gamma_root = cache_root / "gamma-api.polymarket.com"
    for path in sorted(gamma_root.glob("*.json")):
        if path.name.endswith(".meta.json"):
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        for item in _walk_dicts(payload):
            condition_id = str(item.get("conditionId") or "")
            if condition_id not in condition_ids:
                continue
            if "feesEnabled" not in item and "feeSchedule" not in item:
                continue
            record = {
                "condition_id": condition_id,
                "created_at": item.get("createdAt"),
                "updated_at": item.get("updatedAt"),
                "fees_enabled": bool(item.get("feesEnabled")),
                "fee_type": item.get("feeType"),
                "fee_schedule": item.get("feeSchedule"),
                "version": item.get("version"),
            }
            records[condition_id].append((record, path))

    schedules: dict[str, FeeSchedule] = {}
    metadata_conditions: list[dict[str, Any]] = []
    errors: list[str] = []
    for condition_id in sorted(condition_ids):
        candidates = records.get(condition_id) or []
        def fee_identity(record: Mapping[str, Any]) -> dict[str, Any]:
            return {
                key: value
                for key, value in record.items()
                if key not in {"created_at", "updated_at"}
            }

        unique = {
            canonical_json(fee_identity(record)): fee_identity(record)
            for record, _ in candidates
        }
        if not unique:
            errors.append(f"missing_polymarket_fee_metadata:{condition_id}")
            continue
        if len(unique) != 1:
            errors.append(f"conflicting_polymarket_fee_metadata:{condition_id}")
            continue
        record = {
            **next(iter(unique.values())),
            "created_at": min(
                (
                    str(candidate["created_at"])
                    for candidate, _ in candidates
                    if candidate.get("created_at")
                ),
                default=None,
            ),
            "updated_at": max(
                (
                    str(candidate["updated_at"])
                    for candidate, _ in candidates
                    if candidate.get("updated_at")
                ),
                default=None,
            ),
        }
        source_paths = sorted(
            {
                str(path.resolve())
                for candidate, path in candidates
                if fee_identity(candidate) == fee_identity(record)
            }
        )
        fee_schedule = record.get("fee_schedule")
        if record["fees_enabled"]:
            if not isinstance(fee_schedule, dict):
                errors.append(f"missing_polymarket_fee_schedule:{condition_id}")
                continue
            rate = float(fee_schedule.get("rate"))
            exponent = float(fee_schedule.get("exponent", 1))
        else:
            rate = 0.0
            exponent = 1.0
        schedule = FeeSchedule(
            venue="polymarket",
            fee_type=str(record.get("fee_type") or "no_fee"),
            rate=rate,
            exponent=exponent,
            normal_rounding_dollars=float(
                fee_config["polymarket_normal_rounding_dollars"]
            ),
            conservative_rounding_dollars=float(
                fee_config["polymarket_conservative_rounding_dollars"]
            ),
            fixed_execution_cost_dollars=float(
                fee_config["fixed_execution_costs_dollars"]["polymarket"]
            ),
            effective_at_ms=iso_to_milliseconds(record.get("created_at")),
        )
        schedules[f"polymarket:{condition_id}"] = schedule
        metadata_conditions.append(
            {
                **record,
                "source_files": [
                    {
                        "path": path,
                        "sha256": sha256_file(Path(path)),
                    }
                    for path in source_paths
                ],
                "resolved_schedule": asdict(schedule),
            }
        )
    return (
        schedules,
        {
            "source": "cached_gamma_market_metadata",
            "conditions": metadata_conditions,
        },
        errors,
    )


# Kalshi fee types whose *taker* charge is the quadratic
# rate * multiplier * quantity * p * (1 - p).
#
# `quadratic_with_maker_fees` additionally charges resting makers. Every leg
# here is a marketable taker order that walks displayed depth (see the
# execution assumptions recorded in fee_audit_manifest), so the maker component
# never applies and the taker arithmetic is identical to plain `quadratic`.
# Treating the two as equivalent is therefore correct for this model but wrong
# for any future maker-side analysis, so the distinction is preserved in the
# emitted metadata rather than collapsed.
SUPPORTED_KALSHI_FEE_TYPES = frozenset({"quadratic", "quadratic_with_maker_fees"})
KALSHI_MAKER_FEE_TYPES = frozenset({"quadratic_with_maker_fees"})


def resolve_kalshi_fee(
    client: DurableJsonClient,
    fee_config: Mapping[str, Any],
    *,
    offline: bool,
) -> tuple[dict[str, FeeSchedule], dict[str, Any], list[str]]:
    series_ticker = str(fee_config["kalshi_series_ticker"])
    errors: list[str] = []
    try:
        series_response = client.get_json(
            KALSHI_BASE_URL,
            f"/series/{series_ticker}",
        )
        changes_response = client.get_json(
            KALSHI_BASE_URL,
            "/series/fee_changes",
            params={
                "series_ticker": series_ticker,
                "show_historical": True,
            },
        )
    except HttpRequestError as error:
        mode = "offline_cache_missing" if offline else "metadata_request_failed"
        return {}, {"source": mode, "error": str(error)}, [mode]

    series = (
        series_response.data.get("series")
        if isinstance(series_response.data, dict)
        else None
    )
    changes = (
        changes_response.data.get("series_fee_change_arr")
        if isinstance(changes_response.data, dict)
        else None
    )
    if not isinstance(series, dict):
        errors.append("invalid_kalshi_series_metadata")
        return {}, {}, errors
    if not isinstance(changes, list):
        errors.append("invalid_kalshi_fee_changes")
        changes = []

    fee_type = str(series.get("fee_type") or "")
    multiplier = float(series.get("fee_multiplier", 1))
    if fee_type not in SUPPORTED_KALSHI_FEE_TYPES:
        errors.append(f"unsupported_kalshi_fee_type:{fee_type}")
    base_effective_at = str(fee_config["kalshi_base_rate_effective_at"])
    base_effective_at_ms = iso_to_milliseconds(base_effective_at)
    if base_effective_at_ms is None:
        errors.append("invalid_kalshi_base_rate_effective_at")
    initial_multiplier = (
        float(fee_config.get("kalshi_default_fee_multiplier", 1.0))
        if changes
        else multiplier
    )

    def make_schedule(
        schedule_multiplier: float,
        effective_at_ms: int | None,
    ) -> FeeSchedule:
        return FeeSchedule(
            venue="kalshi",
            fee_type=fee_type,
            rate=float(fee_config["kalshi_base_taker_rate"]),
            multiplier=schedule_multiplier,
            exponent=1.0,
            normal_rounding_dollars=float(
                fee_config["kalshi_normal_rounding_dollars"]
            ),
            conservative_rounding_dollars=float(
                fee_config["kalshi_conservative_rounding_dollars"]
            ),
            fixed_execution_cost_dollars=float(
                fee_config["fixed_execution_costs_dollars"]["kalshi"]
            ),
            effective_at_ms=effective_at_ms,
        )

    timeline_entries: list[tuple[FeeSchedule, dict[str, Any]]] = [
        (
            make_schedule(initial_multiplier, base_effective_at_ms),
            {
                "effective_at": base_effective_at,
                "fee_multiplier": initial_multiplier,
                "source": "configured_base_schedule",
            },
        )
    ]
    seen_change_timestamps: dict[int, float] = {}
    for change in sorted(
        changes,
        key=lambda item: (
            str(item.get("scheduled_ts") or "")
            if isinstance(item, dict)
            else ""
        ),
    ):
        if not isinstance(change, dict):
            errors.append("invalid_kalshi_fee_change_record")
            continue
        change_series = str(change.get("series_ticker") or series_ticker)
        if change_series != series_ticker:
            errors.append(
                f"kalshi_fee_change_series_mismatch:{change_series}"
            )
            continue
        scheduled_at = change.get("scheduled_ts")
        scheduled_at_ms = iso_to_milliseconds(
            str(scheduled_at) if scheduled_at else None
        )
        try:
            change_multiplier = float(change["fee_multiplier"])
        except (KeyError, TypeError, ValueError):
            errors.append("invalid_kalshi_fee_change_multiplier")
            continue
        if scheduled_at_ms is None:
            errors.append("invalid_kalshi_fee_change_timestamp")
            continue
        previous_multiplier = seen_change_timestamps.get(scheduled_at_ms)
        if (
            previous_multiplier is not None
            and previous_multiplier != change_multiplier
        ):
            errors.append(
                f"conflicting_kalshi_fee_changes:{scheduled_at_ms}"
            )
            continue
        seen_change_timestamps[scheduled_at_ms] = change_multiplier
        timeline_entries.append(
            (
                make_schedule(change_multiplier, scheduled_at_ms),
                {
                    "effective_at": scheduled_at,
                    "fee_multiplier": change_multiplier,
                    "source": f"fee_change:{change.get('id') or 'unknown'}",
                },
            )
        )
    timeline_entries.sort(
        key=lambda value: (
            value[0].effective_at_ms
            if value[0].effective_at_ms is not None
            else -1
        )
    )
    timeline = [entry[0] for entry in timeline_entries]
    series_updated_at_ms = iso_to_milliseconds(series.get("last_updated_ts"))
    schedule_at_series_update = (
        fee_schedule_at(timeline, series_updated_at_ms)
        if series_updated_at_ms is not None
        else None
    )
    if (
        changes
        and schedule_at_series_update is not None
        and not math.isclose(
            schedule_at_series_update.multiplier,
            multiplier,
            rel_tol=0,
            abs_tol=1e-12,
        )
    ):
        errors.append("kalshi_current_multiplier_conflicts_with_fee_changes")

    schedule = make_schedule(multiplier, series_updated_at_ms)
    metadata_timeline = [
        {
            **entry_metadata,
            "resolved_schedule": asdict(schedule_value),
        }
        for schedule_value, entry_metadata in timeline_entries
    ]
    metadata = {
        "source": "kalshi_series_and_fee_changes",
        "series_ticker": series_ticker,
        "series": {
            "fee_type": fee_type,
            "fee_multiplier": multiplier,
            "last_updated_ts": series.get("last_updated_ts"),
            "charges_maker_fees": fee_type in KALSHI_MAKER_FEE_TYPES,
            "maker_fee_modelled": False,
            "maker_fee_note": (
                "Taker-only model: every leg is a marketable order walking "
                "displayed depth, so the maker component of this fee type is "
                "not charged and is not modelled."
            ),
        },
        "historical_changes": changes,
        "resolved_timeline": metadata_timeline,
        "base_rate": {
            "rate": fee_config["kalshi_base_taker_rate"],
            "effective_at": base_effective_at,
            "source": fee_config["kalshi_base_rate_source"],
        },
        "series_response": {
            "url": series_response.url,
            "cache_sha256": sha256_file(series_response.cache_path),
        },
        "fee_changes_response": {
            "url": changes_response.url,
            "cache_sha256": sha256_file(changes_response.cache_path),
        },
        "resolved_schedule": asdict(schedule),
    }
    return {f"kalshi:{series_ticker}": tuple(timeline)}, metadata, errors


def resolve_fee_metadata(
    manifest: Mapping[str, Any],
    config: Mapping[str, Any],
    *,
    offline: bool,
) -> tuple[
    dict[str, FeeSchedule],
    dict[str, Any],
    list[str],
    dict[str, int],
]:
    cache_root = Path(config["http_cache_root"])
    fee_config = config["fees"]
    client = DurableJsonClient(
        cache_root,
        transport=(
            (
                lambda request, timeout: (_ for _ in ()).throw(
                    HttpRequestError(
                        f"Offline mode has no cached response for {request.full_url}"
                    )
                )
            )
            if offline
            else None
        ),
    )
    poly_schedules, poly_metadata, poly_errors = resolve_polymarket_fees(
        manifest,
        cache_root,
        fee_config,
    )
    kalshi_schedules, kalshi_metadata, kalshi_errors = resolve_kalshi_fee(
        client,
        fee_config,
        offline=offline,
    )
    schedules = {**poly_schedules, **kalshi_schedules}
    metadata = {
        "kalshi": kalshi_metadata,
        "polymarket": poly_metadata,
        "execution_assumptions": FEE_EXECUTION_ASSUMPTIONS,
    }
    request_stats = {
        "cache_hits": client.cache_hits,
        "network_requests": client.network_requests,
    }
    return schedules, metadata, [*poly_errors, *kalshi_errors], request_stats


def _target_run_map(run: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    return {
        str(target["target_id"]): target
        for target in run.get("targets") or []
    }


def _event_outcomes(manifest: Mapping[str, Any]) -> dict[str, list[str]]:
    output: dict[str, list[str]] = defaultdict(list)
    for target in manifest.get("history_targets") or []:
        event_key = str(target["event_key"])
        if target.get("venue") == "kalshi" and target.get("outcome"):
            output[event_key].append(str(target["outcome"]))
    return {
        event_key: sorted(set(outcomes))
        for event_key, outcomes in output.items()
    }


def load_and_validate_books(
    manifest: Mapping[str, Any],
    history_run: Mapping[str, Any],
    config: Mapping[str, Any],
) -> tuple[dict[str, InstrumentSeries], dict[str, Any]]:
    validation_config = config["validation"]
    price_tolerance = float(validation_config["price_tolerance"])
    size_tolerance = float(validation_config["size_tolerance"])
    run_targets = _target_run_map(history_run)
    outcomes_by_event = _event_outcomes(manifest)
    instruments: dict[str, InstrumentSeries] = {}
    hard_errors: list[str] = []
    target_reports: list[dict[str, Any]] = []
    level_counts: dict[str, list[int]] = defaultdict(list)

    for target in manifest.get("history_targets") or []:
        target_id = str(target["target_id"])
        venue = str(target["venue"])
        event_key = str(target["event_key"])
        market_id = str(target["market_id"])
        run_target = run_targets.get(target_id)
        if run_target is None:
            hard_errors.append(f"missing_history_target:{target_id}")
            continue
        if not run_target.get("complete"):
            hard_errors.append(f"incomplete_history_target:{target_id}")

        snapshots_path = Path(str(run_target.get("snapshots_path") or ""))
        expected_assets = {
            str(token["asset_id"])
            for token in target.get("outcome_tokens") or []
        }
        rows: list[dict[str, Any]] = (
            list(read_ndjson(snapshots_path))
            if snapshots_path.exists()
            else []
        )
        row_errors: Counter[str] = Counter()
        normalized_by_asset: dict[str, list[BookSnapshot]] = defaultdict(list)
        normalized_yes: list[BookSnapshot] = []
        normalized_no: list[BookSnapshot] = []

        for row in rows:
            provenance = row.get("_provenance") or {}
            if (
                provenance.get("source") != "oddpool"
                or provenance.get("target_id") != target_id
            ):
                row_errors["invalid_provenance"] += 1
            if str(row.get("market_id") or "") != market_id:
                row_errors["wrong_market_id"] += 1

            if venue == "kalshi":
                for side in ("yes_bids", "no_bids"):
                    raw_levels = row.get(side) or []
                    level_counts[f"kalshi:{side}"].append(len(raw_levels))
                    for issue in ladder_issues(
                        raw_levels,
                        expected_descending=True,
                        price_tolerance=price_tolerance,
                    ):
                        row_errors[f"{side}:{issue}"] += 1
                try:
                    if not validate_top_of_book(
                        row,
                        venue=venue,
                        price_tolerance=price_tolerance,
                    ):
                        row_errors["top_of_book_mismatch"] += 1
                    yes, no = normalize_kalshi_snapshot(row)
                    normalized_yes.append(yes)
                    normalized_no.append(no)
                except (KeyError, TypeError, ValueError):
                    row_errors["normalization_error"] += 1
            elif venue == "polymarket":
                asset_id = str(row.get("asset_id") or "")
                if asset_id not in expected_assets:
                    row_errors["unexpected_asset_id"] += 1
                    continue
                for side, descending in (("bids", True), ("asks", False)):
                    raw_levels = row.get(side) or []
                    level_counts[f"polymarket:{side}"].append(len(raw_levels))
                    for issue in ladder_issues(
                        raw_levels,
                        expected_descending=descending,
                        price_tolerance=price_tolerance,
                    ):
                        row_errors[f"{side}:{issue}"] += 1
                try:
                    if not validate_top_of_book(
                        row,
                        venue=venue,
                        price_tolerance=price_tolerance,
                    ):
                        row_errors["top_of_book_mismatch"] += 1
                    normalized_by_asset[asset_id].append(
                        normalize_polymarket_snapshot(row)
                    )
                except (KeyError, TypeError, ValueError):
                    row_errors["normalization_error"] += 1
            else:
                row_errors["unsupported_venue"] += 1

        if row_errors:
            hard_errors.extend(
                f"{target_id}:{name}:{count}"
                for name, count in sorted(row_errors.items())
            )

        end_time_ms = iso_to_milliseconds(target.get("end_time"))
        if venue == "kalshi" and (normalized_yes or normalized_no):
            outcome = str(target.get("outcome") or "")
            other = [
                candidate
                for candidate in outcomes_by_event.get(event_key, [])
                if candidate != outcome
            ]
            no_outcome = other[0] if len(other) == 1 else f"NOT {outcome}"
            common = {
                "venue": venue,
                "event_key": event_key,
                "market_id": market_id,
                "asset_id": None,
                "fee_key": f"kalshi:{config['fees']['kalshi_series_ticker']}",
                "end_time_ms": end_time_ms,
            }
            instruments[f"kalshi:{market_id}:yes"] = InstrumentSeries(
                instrument_id=f"kalshi:{market_id}:yes",
                outcome=outcome,
                position="yes",
                snapshots=sorted(
                    normalized_yes,
                    key=lambda snapshot: snapshot.timestamp_ms,
                ),
                **common,
            )
            instruments[f"kalshi:{market_id}:no"] = InstrumentSeries(
                instrument_id=f"kalshi:{market_id}:no",
                outcome=no_outcome,
                position="no",
                snapshots=sorted(
                    normalized_no,
                    key=lambda snapshot: snapshot.timestamp_ms,
                ),
                **common,
            )
        elif venue == "polymarket":
            labels = {
                str(token["asset_id"]): str(token["outcome"])
                for token in target.get("outcome_tokens") or []
            }
            for asset_id, snapshots in normalized_by_asset.items():
                instrument_id = f"polymarket:{market_id}:{asset_id}"
                instruments[instrument_id] = InstrumentSeries(
                    instrument_id=instrument_id,
                    venue=venue,
                    event_key=event_key,
                    market_id=market_id,
                    outcome=labels[asset_id],
                    position="token",
                    asset_id=asset_id,
                    fee_key=f"polymarket:{market_id}",
                    end_time_ms=end_time_ms,
                    snapshots=sorted(
                        snapshots,
                        key=lambda snapshot: snapshot.timestamp_ms,
                    ),
                )

        target_reports.append(
            {
                "target_id": target_id,
                "event_key": event_key,
                "venue": venue,
                "market_id": market_id,
                "complete": bool(run_target.get("complete")),
                "row_count": len(rows),
                "confirmed_zero": bool(run_target.get("complete")) and not rows,
                "row_errors": dict(sorted(row_errors.items())),
                "observed_asset_ids": sorted(normalized_by_asset),
            }
        )

    level_diagnostics: dict[str, Any] = {}
    for side, counts in sorted(level_counts.items()):
        diagnostic = level_count_diagnostics(
            counts,
            minimum_distinct_nonzero_counts=int(
                validation_config["minimum_distinct_nonzero_level_counts"]
            ),
            maximum_share_at_cap=float(
                validation_config["maximum_share_at_observed_level_cap"]
            ),
        )
        level_diagnostics[side] = diagnostic
        if not diagnostic["valid"]:
            hard_errors.append(f"ladder_depth_validation_failed:{side}")

    complement_reports: list[dict[str, Any]] = []
    minimum_rate = float(
        validation_config["minimum_polymarket_complement_rate"]
    )
    for match in manifest.get("matches") or []:
        event_key = str(match["event_key"])
        condition_id = str(match.get("polymarket", {}).get("condition_id") or "")
        token_ids = [
            f"polymarket:{condition_id}:{token['asset_id']}"
            for token in match.get("polymarket", {}).get("outcome_tokens") or []
        ]
        available = [
            instruments[token_id] for token_id in token_ids if token_id in instruments
        ]
        if not available:
            complement_reports.append(
                {
                    "event_key": event_key,
                    "condition_id": condition_id,
                    "status": "confirmed_zero_or_unavailable",
                }
            )
            continue
        if len(available) != 2:
            hard_errors.append(f"incomplete_polymarket_token_pair:{condition_id}")
            continue
        diagnostic = polymarket_complement_diagnostics(
            available[0].snapshots,
            available[1].snapshots,
            pair_tolerance_ms=int(
                validation_config["polymarket_pair_tolerance_ms"]
            ),
            size_tolerance=size_tolerance,
        )
        passed = bool(
            diagnostic["match_rate"] is not None
            and diagnostic["match_rate"] >= minimum_rate
        )
        complement_reports.append(
            {
                "event_key": event_key,
                "condition_id": condition_id,
                "status": "passed" if passed else "failed",
                **diagnostic,
            }
        )
        if not passed:
            hard_errors.append(
                f"polymarket_complement_validation_failed:{condition_id}"
            )

    validation = {
        "valid": not hard_errors,
        "hard_errors": sorted(hard_errors),
        "target_count": len(target_reports),
        "instrument_count": len(instruments),
        "snapshot_count": sum(
            len(series.snapshots) for series in instruments.values()
        ),
        "targets": target_reports,
        "level_count_diagnostics": level_diagnostics,
        "polymarket_complement_diagnostics": complement_reports,
    }
    return instruments, validation


def _import_pyarrow():
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as error:
        raise RuntimeError(
            "pyarrow is required; install the project with "
            "`python3 -m pip install -e .`"
        ) from error
    return pa, pq


def normalized_book_schema():
    pa, _ = _import_pyarrow()
    level = pa.struct(
        [
            pa.field("price", pa.float64(), nullable=False),
            pa.field("size", pa.float64(), nullable=False),
        ]
    )
    return pa.schema(
        [
            pa.field("instrument_id", pa.string(), nullable=False),
            pa.field("venue", pa.string(), nullable=False),
            pa.field("event_key", pa.string(), nullable=False),
            pa.field("market_id", pa.string(), nullable=False),
            pa.field("asset_id", pa.string()),
            pa.field("outcome", pa.string(), nullable=False),
            pa.field("position", pa.string(), nullable=False),
            pa.field("fee_key", pa.string(), nullable=False),
            pa.field("end_time_ms", pa.int64()),
            pa.field("snapshot_timestamp_ms", pa.int64(), nullable=False),
            pa.field("bids", pa.list_(level), nullable=False),
            pa.field("asks", pa.list_(level), nullable=False),
            pa.field("provenance_json", pa.string(), nullable=False),
        ]
    )


def observation_schema():
    pa, _ = _import_pyarrow()
    return pa.schema(
        [
            pa.field("economic_partition_id", pa.string(), nullable=False),
            pa.field("representation_id", pa.string(), nullable=False),
            pa.field("partition_class", pa.string(), nullable=False),
            pa.field("partition_status", pa.string(), nullable=False),
            pa.field("event_key", pa.string(), nullable=False),
            pa.field("bar_timestamp_ms", pa.int64(), nullable=False),
            pa.field("direction", pa.string(), nullable=False),
            pa.field("diagnostic_only", pa.bool_(), nullable=False),
            pa.field("size_contracts", pa.int64(), nullable=False),
            pa.field("leg_instrument_ids", pa.list_(pa.string()), nullable=False),
            pa.field("leg_market_ids", pa.list_(pa.string()), nullable=False),
            pa.field("leg_asset_ids", pa.list_(pa.string()), nullable=False),
            pa.field("leg_positions", pa.list_(pa.string()), nullable=False),
            pa.field(
                "leg_snapshot_timestamps_ms",
                pa.list_(pa.int64()),
                nullable=False,
            ),
            pa.field(
                "leg_snapshot_ages_seconds",
                pa.list_(pa.float64()),
                nullable=False,
            ),
            pa.field("leg_skew_seconds", pa.float64(), nullable=False),
            pa.field("skew_bucket", pa.string(), nullable=False),
            pa.field("time_to_resolution_seconds", pa.float64()),
            pa.field(
                "minimum_leg_available_depth_contracts",
                pa.float64(),
                nullable=False,
            ),
            pa.field("leg_vwap_prices", pa.list_(pa.float64()), nullable=False),
            pa.field(
                "leg_filled_contracts",
                pa.list_(pa.float64()),
                nullable=False,
            ),
            pa.field(
                "leg_costs_dollars",
                pa.list_(pa.float64()),
                nullable=False,
            ),
            pa.field(
                "leg_fee_raw_dollars",
                pa.list_(pa.float64()),
                nullable=False,
            ),
            pa.field(
                "leg_fee_normal_dollars",
                pa.list_(pa.float64()),
                nullable=False,
            ),
            pa.field(
                "leg_fee_conservative_dollars",
                pa.list_(pa.float64()),
                nullable=False,
            ),
            pa.field("cost_or_proceeds_per_contract", pa.float64()),
            pa.field("gross_gap_per_contract", pa.float64()),
            pa.field("net_gap_normal_per_contract", pa.float64()),
            pa.field("net_gap_conservative_per_contract", pa.float64()),
            pa.field("profit_normal_dollars", pa.float64()),
            pa.field("profit_conservative_dollars", pa.float64()),
            pa.field("fee_raw_total_dollars", pa.float64()),
            pa.field("fee_normal_total_dollars", pa.float64()),
            pa.field("fee_conservative_total_dollars", pa.float64()),
            pa.field("valid", pa.bool_(), nullable=False),
            pa.field("fee_complete", pa.bool_(), nullable=False),
            pa.field("depth_limited", pa.bool_(), nullable=False),
            pa.field("max_fillable_contracts", pa.float64(), nullable=False),
            pa.field("one_sided_legs", pa.int64(), nullable=False),
            pa.field("exclusion_reason", pa.string()),
            pa.field(
                "selected_best_representation",
                pa.bool_(),
                nullable=False,
            ),
        ]
    )


def write_normalized_books(
    path: Path,
    instruments: Mapping[str, InstrumentSeries],
    *,
    batch_size: int = 5000,
) -> int:
    pa, pq = _import_pyarrow()
    schema = normalized_book_schema()
    path.parent.mkdir(parents=True, exist_ok=True)
    writer = pq.ParquetWriter(path, schema, compression="zstd")
    rows: list[dict[str, Any]] = []
    count = 0
    try:
        for instrument_id in sorted(instruments):
            series = instruments[instrument_id]
            for snapshot in series.snapshots:
                rows.append(
                    {
                        "instrument_id": series.instrument_id,
                        "venue": series.venue,
                        "event_key": series.event_key,
                        "market_id": series.market_id,
                        "asset_id": series.asset_id,
                        "outcome": series.outcome,
                        "position": series.position,
                        "fee_key": series.fee_key,
                        "end_time_ms": series.end_time_ms,
                        "snapshot_timestamp_ms": snapshot.timestamp_ms,
                        "bids": [
                            {"price": level.price, "size": level.size}
                            for level in snapshot.bids
                        ],
                        "asks": [
                            {"price": level.price, "size": level.size}
                            for level in snapshot.asks
                        ],
                        "provenance_json": canonical_json(snapshot.provenance),
                    }
                )
                if len(rows) >= batch_size:
                    writer.write_table(pa.Table.from_pylist(rows, schema=schema))
                    count += len(rows)
                    rows = []
        if rows:
            writer.write_table(pa.Table.from_pylist(rows, schema=schema))
            count += len(rows)
    finally:
        writer.close()
    return count


class SummaryAccumulator:
    def __init__(self, *, bar_seconds: int) -> None:
        self.bar_ms = bar_seconds * 1000
        self.groups: dict[tuple[str, str, int, str], dict[str, Any]] = {}
        self.gate_events: dict[tuple[str, str], dict[str, Any]] = {}
        self.positive_times: dict[tuple[str, str, int, str], list[int]] = (
            defaultdict(list)
        )
        self.selected_row_count = 0

    def observe(self, row: Mapping[str, Any], gate: Mapping[str, Any]) -> None:
        if not row["selected_best_representation"]:
            return
        self.selected_row_count += 1
        group_key = (
            str(row["partition_class"]),
            str(row["direction"]),
            int(row["size_contracts"]),
            str(row["skew_bucket"]),
        )
        group = self.groups.setdefault(
            group_key,
            {
                "selected_count": 0,
                "valid_count": 0,
                "depth_limited_count": 0,
                "one_sided_count": 0,
                "gross_positive_count": 0,
                "net_positive_count": 0,
                "gross_gaps": [],
                "net_gaps": [],
                "profits": [],
            },
        )
        group["selected_count"] += 1
        group["depth_limited_count"] += int(bool(row["depth_limited"]))
        group["one_sided_count"] += int(int(row["one_sided_legs"]) > 0)
        if row["valid"]:
            group["valid_count"] += 1
            gross_gap = float(row["gross_gap_per_contract"])
            net_gap = float(row["net_gap_conservative_per_contract"])
            profit = float(row["profit_conservative_dollars"])
            group["gross_gaps"].append(gross_gap)
            group["net_gaps"].append(net_gap)
            group["profits"].append(profit)
            group["gross_positive_count"] += int(gross_gap > 0)
            group["net_positive_count"] += int(net_gap > 0)
            if row["direction"] == "long" and net_gap > 0:
                run_key = (
                    str(row["partition_class"]),
                    str(row["economic_partition_id"]),
                    int(row["size_contracts"]),
                    str(row["skew_bucket"]),
                )
                self.positive_times[run_key].append(
                    int(row["bar_timestamp_ms"])
                )

        is_gate_row = (
            row["direction"] == gate["direction"]
            and int(row["size_contracts"]) == int(gate["ticket_size_contracts"])
            and row["skew_bucket"] == "lt_5s"
            and row["partition_class"]
            in {gate["same_venue_class"], gate["cross_venue_class"]}
        )
        if is_gate_row:
            event_key = (
                str(row["partition_class"]),
                str(row["event_key"]),
            )
            event = self.gate_events.setdefault(
                event_key,
                {
                    "valid_count": 0,
                    "positive_count": 0,
                },
            )
            if row["valid"] and row["fee_complete"]:
                event["valid_count"] += 1
                event["positive_count"] += int(
                    float(row["net_gap_conservative_per_contract"]) > 0
                )

    @staticmethod
    def _distribution(values: Sequence[float]) -> dict[str, float | None]:
        if not values:
            return {"minimum": None, "median": None, "maximum": None}
        return {
            "minimum": min(values),
            "median": statistics.median(values),
            "maximum": max(values),
        }

    def finalize(self, gate: Mapping[str, Any]) -> dict[str, Any]:
        groups: list[dict[str, Any]] = []
        for key, value in sorted(self.groups.items()):
            valid = int(value["valid_count"])
            groups.append(
                {
                    "partition_class": key[0],
                    "direction": key[1],
                    "size_contracts": key[2],
                    "skew_bucket": key[3],
                    "selected_count": value["selected_count"],
                    "valid_count": valid,
                    "depth_limited_count": value["depth_limited_count"],
                    "one_sided_count": value["one_sided_count"],
                    "gross_positive_count": value["gross_positive_count"],
                    "net_positive_count": value["net_positive_count"],
                    "gross_positive_rate": (
                        value["gross_positive_count"] / valid if valid else None
                    ),
                    "net_positive_rate": (
                        value["net_positive_count"] / valid if valid else None
                    ),
                    "gross_gap_distribution": self._distribution(
                        value["gross_gaps"]
                    ),
                    "net_gap_distribution": self._distribution(
                        value["net_gaps"]
                    ),
                    "profit_distribution": self._distribution(value["profits"]),
                }
            )

        event_rows: list[dict[str, Any]] = []
        minimum_count = int(
            gate["minimum_valid_low_skew_observations_per_event"]
        )
        minimum_rate = float(gate["minimum_positive_rate"])
        for (partition_class, event_key), value in sorted(
            self.gate_events.items()
        ):
            valid_count = int(value["valid_count"])
            rate = (
                value["positive_count"] / valid_count if valid_count else None
            )
            qualifies = valid_count >= minimum_count
            event_rows.append(
                {
                    "partition_class": partition_class,
                    "event_key": event_key,
                    "valid_low_skew_count": valid_count,
                    "positive_count": value["positive_count"],
                    "positive_rate": rate,
                    "qualifies_sample_size": qualifies,
                    "passes_rate": bool(
                        qualifies and rate is not None and rate >= minimum_rate
                    ),
                }
            )

        run_lengths: list[dict[str, Any]] = []
        for key, timestamps in sorted(self.positive_times.items()):
            ordered = sorted(set(timestamps))
            lengths: list[int] = []
            current = 0
            previous: int | None = None
            for timestamp in ordered:
                if previous is not None and timestamp == previous + self.bar_ms:
                    current += 1
                else:
                    if current:
                        lengths.append(current)
                    current = 1
                previous = timestamp
            if current:
                lengths.append(current)
            run_lengths.append(
                {
                    "partition_class": key[0],
                    "economic_partition_id": key[1],
                    "size_contracts": key[2],
                    "skew_bucket": key[3],
                    "positive_observation_count": len(ordered),
                    "run_count": len(lengths),
                    "median_run_bars": (
                        statistics.median(lengths) if lengths else None
                    ),
                    "maximum_run_bars": max(lengths) if lengths else None,
                }
            )
        return {
            "selected_row_count": self.selected_row_count,
            "groups": groups,
            "gate_events": event_rows,
            "positive_run_lengths": run_lengths,
        }


def determine_terminal_status(
    summary: Mapping[str, Any],
    validation: Mapping[str, Any],
    fee_errors: Sequence[str],
    gate: Mapping[str, Any],
) -> tuple[str, dict[str, Any]]:
    event_rows = summary.get("gate_events") or []
    passing: dict[str, list[str]] = defaultdict(list)
    qualifying: dict[str, list[str]] = defaultdict(list)
    for row in event_rows:
        partition_class = str(row["partition_class"])
        if row["qualifies_sample_size"]:
            qualifying[partition_class].append(str(row["event_key"]))
        if row["passes_rate"]:
            passing[partition_class].append(str(row["event_key"]))
    required = int(gate["minimum_passing_events"])
    same_class = str(gate["same_venue_class"])
    cross_class = str(gate["cross_venue_class"])

    detail = {
        "fee_errors": sorted(fee_errors),
        "qualifying_events_by_class": {
            key: sorted(value) for key, value in sorted(qualifying.items())
        },
        "passing_events_by_class": {
            key: sorted(value) for key, value in sorted(passing.items())
        },
    }
    if not validation.get("valid") or fee_errors:
        return "DATA_INVALID", detail
    if len(passing.get(same_class, [])) >= required:
        return "EWC_ECONOMIC_PASS", detail
    if len(passing.get(cross_class, [])) >= required:
        return "EWC_CONDITIONAL_CROSS_VENUE_PASS", detail
    if max(
        len(qualifying.get(same_class, [])),
        len(qualifying.get(cross_class, [])),
    ) < required:
        return "INSUFFICIENT_LOW_SKEW_DATA", detail
    return "EWC_NO_SIGNAL", detail


def _markdown_percent(value: float | None) -> str:
    return "—" if value is None else f"{value:.2%}"


def _markdown_money(value: float | None) -> str:
    return "—" if value is None else f"${value:.4f}"


def build_report(
    *,
    run_id: str,
    terminal_status: str,
    validation: Mapping[str, Any],
    fee_metadata: Mapping[str, Any],
    fee_errors: Sequence[str],
    definitions: Sequence[Mapping[str, Any]],
    summary: Mapping[str, Any],
    gate: Mapping[str, Any],
    dataset_name: str = "ewc_dota_playoffs",
) -> str:
    lines = [
        f"# Partition-Sum Test — {dataset_name}",
        "",
        f"- Run ID: `{run_id}`",
        f"- Terminal status: **{terminal_status}**",
        f"- Normalized instruments: {validation.get('instrument_count', 0)}",
        f"- Normalized snapshots: {validation.get('snapshot_count', 0):,}",
        f"- Partition definitions: {len(definitions)}",
        f"- Selected observation rows: {summary.get('selected_row_count', 0):,}",
        "- Oddpool requests made by this pipeline: **0**",
        "",
        "## Interpretation",
        "",
    ]
    interpretations = {
        "DATA_INVALID": (
            "A hard book, complement, provenance, or fee gate failed. Gross "
            "diagnostics may exist, but no economic conclusion is valid."
        ),
        "INSUFFICIENT_LOW_SKEW_DATA": (
            "The data is internally valid, but fewer than two events meet the "
            "pre-registered low-skew denominator."
        ),
        "EWC_ECONOMIC_PASS": (
            "A separate-market same-venue partition passed the pre-registered "
            "size, frequency, and replication thresholds."
        ),
        "EWC_CONDITIONAL_CROSS_VENUE_PASS": (
            "Only cross-venue price gaps passed. They remain conditional on "
            "rules and fungibility review."
        ),
        "EWC_NO_SIGNAL": (
            f"The {dataset_name} data is valid and sufficiently sampled, but "
            "no eligible economic class passed. Replicate on another dataset "
            "before rejecting the thesis."
        ),
    }
    lines.extend([interpretations[terminal_status], ""])

    lines.extend(
        [
            "## Gate",
            "",
            (
                f"Long baskets at {gate['ticket_size_contracts']} contracts; "
                f"at least {gate['minimum_valid_low_skew_observations_per_event']} "
                "valid `<5s` observations; net-positive rate of at least "
                f"{float(gate['minimum_positive_rate']):.2%} in "
                f"{gate['minimum_passing_events']} events."
            ),
            "",
            "| Class | Event | Valid low-skew bars | Positive bars | Rate | Qualifies | Passes |",
            "|---|---|---:|---:|---:|---|---|",
        ]
    )
    for row in summary.get("gate_events") or []:
        lines.append(
            "| {partition_class} | {event_key} | {valid_low_skew_count} | "
            "{positive_count} | {rate} | {qualifies} | {passes} |".format(
                **row,
                rate=_markdown_percent(row["positive_rate"]),
                qualifies="yes" if row["qualifies_sample_size"] else "no",
                passes="yes" if row["passes_rate"] else "no",
            )
        )

    lines.extend(
        [
            "",
            "## Low-skew long size curves",
            "",
            "| Class | Size | Valid | Net-positive rate | Median gross gap | Median net gap | Median profit |",
            "|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    low_skew_groups = [
        row
        for row in summary.get("groups") or []
        if row["direction"] == "long" and row["skew_bucket"] == "lt_5s"
    ]
    for row in low_skew_groups:
        lines.append(
            "| {partition_class} | {size_contracts} | {valid_count} | "
            "{positive_rate} | {gross} | {net} | {profit} |".format(
                **row,
                positive_rate=_markdown_percent(row["net_positive_rate"]),
                gross=_markdown_money(
                    row["gross_gap_distribution"]["median"]
                ),
                net=_markdown_money(row["net_gap_distribution"]["median"]),
                profit=_markdown_money(row["profit_distribution"]["median"]),
            )
        )

    ticket_size = int(gate["ticket_size_contracts"])
    lines.extend(
        [
            "",
            f"## Skew falsification at {ticket_size} contracts",
            "",
            "| Class | Skew | Valid | Net-positive rate | Median net gap |",
            "|---|---|---:|---:|---:|",
        ]
    )
    for row in summary.get("groups") or []:
        if row["direction"] != "long" or row["size_contracts"] != ticket_size:
            continue
        lines.append(
            "| {partition_class} | {skew_bucket} | {valid_count} | "
            "{positive_rate} | {net} |".format(
                **row,
                positive_rate=_markdown_percent(row["net_positive_rate"]),
                net=_markdown_money(row["net_gap_distribution"]["median"]),
            )
        )

    lines.extend(
        [
            "",
            f"## Depth and fillability at {ticket_size} contracts",
            "",
            "| Class | Direction | Skew | Selected | Valid | Depth-limited | One-sided | Fill rate |",
            "|---|---|---|---:|---:|---:|---:|---:|",
        ]
    )
    for row in summary.get("groups") or []:
        if row["size_contracts"] != ticket_size:
            continue
        selected_count = int(row["selected_count"])
        valid_count = int(row["valid_count"])
        fill_rate = (
            valid_count / selected_count if selected_count else None
        )
        lines.append(
            "| {partition_class} | {direction} | {skew_bucket} | "
            "{selected_count} | {valid_count} | {depth_limited_count} | "
            "{one_sided_count} | {fill_rate} |".format(
                **row,
                fill_rate=_markdown_percent(fill_rate),
            )
        )

    lines.extend(
        [
            "",
            f"## Positive run lengths at {ticket_size} contracts",
            "",
            "| Class | Economic partition | Skew | Positive bars | Runs | Median run | Maximum run |",
            "|---|---|---|---:|---:|---:|---:|",
        ]
    )
    ticket_run_rows = [
        row
        for row in summary.get("positive_run_lengths") or []
        if int(row["size_contracts"]) == ticket_size
    ]
    if ticket_run_rows:
        for row in ticket_run_rows:
            lines.append(
                "| {partition_class} | {economic_partition_id} | "
                "{skew_bucket} | {positive_observation_count} | "
                "{run_count} | {median_run_bars} | "
                "{maximum_run_bars} |".format(**row)
            )
    else:
        lines.append("| — | — | — | 0 | 0 | — | — |")

    lines.extend(["", "## Validation", ""])
    if validation.get("hard_errors"):
        lines.extend(
            [f"- `{error}`" for error in validation["hard_errors"]]
        )
    else:
        lines.append("- All book, provenance, level-cap, and complement gates passed.")
    if fee_errors:
        lines.extend([f"- Fee error: `{error}`" for error in fee_errors])
    else:
        kalshi = fee_metadata.get("kalshi", {}).get("resolved_schedule", {})
        poly_count = len(
            fee_metadata.get("polymarket", {}).get("conditions", [])
        )
        lines.append(
            "- Kalshi fee model: "
            f"`{kalshi.get('fee_type')}`, base rate "
            f"`{kalshi.get('rate')}`, multiplier `{kalshi.get('multiplier')}`."
        )
        lines.append(
            f"- Polymarket condition-level fee schedules resolved: {poly_count}."
        )

    lines.extend(
        [
            "",
            "## Scope hold",
            "",
            "This report does not emit correlation candidates. The EWC manifest "
            "contains match-winner moneylines only; correlation remains blocked "
            "until the economic gate and sibling-market dataset requirements in "
            "`analysis/PIPELINE_SPEC.md` are satisfied.",
            "",
        ]
    )
    return "\n".join(lines)


def code_fingerprint(project_root: Path) -> dict[str, str]:
    paths = [
        *sorted((project_root / "analysis").glob("*.py")),
        project_root / "scripts" / "run_partition_sum_test.py",
        project_root / "analysis" / "PIPELINE_SPEC.md",
    ]
    return {
        relative_or_absolute(path, project_root): sha256_file(path)
        for path in paths
        if path.exists()
    }


def fee_audit_manifest(
    fee_metadata: Mapping[str, Any],
    fee_path: Path,
) -> dict[str, Any]:
    kalshi = fee_metadata.get("kalshi") or {}
    polymarket = fee_metadata.get("polymarket") or {}
    conditions = polymarket.get("conditions") or []
    source_hashes = sorted(
        {
            str(source["sha256"])
            for condition in conditions
            for source in condition.get("source_files") or []
        }
    )
    return {
        "fee_metadata_file_sha256": sha256_file(fee_path),
        "execution_assumptions": fee_metadata.get(
            "execution_assumptions",
            FEE_EXECUTION_ASSUMPTIONS,
        ),
        "effective_schedules": {
            "kalshi": {
                "base_rate_effective_at": (
                    (kalshi.get("base_rate") or {}).get("effective_at")
                ),
                "series_metadata_updated_at": (
                    (kalshi.get("series") or {}).get("last_updated_ts")
                ),
                "historical_changes": kalshi.get("historical_changes") or [],
                "resolved_schedule": kalshi.get("resolved_schedule"),
            },
            "polymarket": [
                {
                    "condition_id": condition.get("condition_id"),
                    "effective_at": condition.get("created_at"),
                    "effective_at_basis": (
                        "condition-specific feeSchedule recorded on market "
                        "metadata; applied from condition creation"
                    ),
                    "metadata_updated_at": condition.get("updated_at"),
                    "version": condition.get("version"),
                    "resolved_schedule": condition.get("resolved_schedule"),
                }
                for condition in conditions
            ],
        },
        "raw_metadata_hashes": {
            "kalshi_series_response": (
                (kalshi.get("series_response") or {}).get("cache_sha256")
            ),
            "kalshi_fee_changes_response": (
                (kalshi.get("fee_changes_response") or {}).get("cache_sha256")
            ),
            "polymarket_source_files": source_hashes,
        },
    }


def write_stage_manifest(
    output_directory: Path,
    *,
    number: int,
    name: str,
    status: str,
    inputs: Mapping[str, str],
    outputs: Sequence[Path],
    metrics: Mapping[str, Any],
    project_root: Path,
) -> Path:
    output_path = output_directory / "stages" / f"{number:02d}_{name}.json"
    write_json(
        output_path,
        {
            "stage": name,
            "status": status,
            "inputs": dict(sorted(inputs.items())),
            "outputs": [
                {
                    "path": relative_or_absolute(path, project_root),
                    "sha256": sha256_file(path),
                }
                for path in outputs
                if path.exists()
            ],
            "metrics": metrics,
        },
    )
    return output_path


def run_partition_pipeline(
    config_path: Path,
    *,
    project_root: Path,
    offline: bool = False,
) -> dict[str, Any]:
    raw_config = json.loads(config_path.read_text(encoding="utf-8"))
    config = resolve_config_paths(raw_config, project_root)
    manifest_path = Path(config["manifest_path"])
    history_directory = Path(config["history_job_directory"])
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    history_run_path = history_directory / "run.json"
    history_coverage_path = history_directory / "coverage.json"
    history_run = json.loads(history_run_path.read_text(encoding="utf-8"))

    fee_schedules, fee_metadata, fee_errors, metadata_request_stats = (
        resolve_fee_metadata(
        manifest,
        config,
        offline=offline,
        )
    )
    fingerprints = {
        "config": sha256_file(config_path),
        "manifest": sha256_file(manifest_path),
        "history_run": sha256_file(history_run_path),
        "history_coverage": (
            sha256_file(history_coverage_path)
            if history_coverage_path.exists()
            else ""
        ),
        "fees": sha256_json(fee_metadata),
        "code": sha256_json(code_fingerprint(project_root)),
    }
    run_id = sha256_json(fingerprints)[:16]
    output_directory = Path(config["output_root"]) / run_id
    output_directory.mkdir(parents=True, exist_ok=True)

    fee_path = output_directory / "fee_metadata.json"
    write_json(
        fee_path,
        {
            "valid": not fee_errors,
            "errors": sorted(fee_errors),
            "metadata": fee_metadata,
        },
    )
    write_stage_manifest(
        output_directory,
        number=1,
        name="resolve_inputs_and_fees",
        status="passed" if not fee_errors else "failed",
        inputs=fingerprints,
        outputs=[fee_path],
        metrics={
            "fee_schedule_count": len(fee_schedules),
            "fee_error_count": len(fee_errors),
            "oddpool_network_requests": 0,
        },
        project_root=project_root,
    )

    instruments, validation = load_and_validate_books(
        manifest,
        history_run,
        config,
    )
    missing_fee_keys = sorted(
        {
            series.fee_key
            for series in instruments.values()
            if series.fee_key not in fee_schedules
        }
    )
    fee_errors = [
        *fee_errors,
        *(f"missing_observed_fee_schedule:{key}" for key in missing_fee_keys),
    ]
    validation_path = output_directory / "validation.json"
    write_json(validation_path, validation)

    normalized_path = output_directory / "normalized_books.parquet"
    normalized_row_count = write_normalized_books(
        normalized_path,
        instruments,
    )
    write_stage_manifest(
        output_directory,
        number=2,
        name="normalize_and_validate",
        status="passed" if validation["valid"] else "failed",
        inputs={
            "manifest": fingerprints["manifest"],
            "history_run": fingerprints["history_run"],
        },
        outputs=[validation_path, normalized_path],
        metrics={
            "instrument_count": len(instruments),
            "normalized_row_count": normalized_row_count,
            "hard_error_count": len(validation["hard_errors"]),
        },
        project_root=project_root,
    )

    book_validation_failed = not validation["valid"]
    definitions = (
        []
        if book_validation_failed
        else build_partition_definitions(manifest, instruments)
    )
    definition_rows = [
        {
            "economic_partition_id": definition.economic_partition_id,
            "partition_class": definition.partition_class,
            "event_key": definition.event_key,
            "partition_status": definition.partition_status,
            "rules_hashes": list(definition.rules_hashes),
            "resolution_sources": list(definition.resolution_sources),
            "representations": [
                {
                    "representation_id": representation.representation_id,
                    "leg_instrument_ids": list(
                        representation.leg_instrument_ids
                    ),
                }
                for representation in definition.representations
            ],
        }
        for definition in definitions
    ]
    definitions_path = output_directory / "partition_definitions.json"
    write_json(
        definitions_path,
        {
            "partition_count": len(definition_rows),
            "partitions": definition_rows,
        },
    )
    class_counts = Counter(
        definition.partition_class for definition in definitions
    )
    write_stage_manifest(
        output_directory,
        number=3,
        name="build_partitions",
        status=(
            "skipped_data_invalid"
            if book_validation_failed
            else ("passed" if definitions else "failed")
        ),
        inputs={
            "normalized_books": sha256_file(normalized_path),
            "manifest": fingerprints["manifest"],
        },
        outputs=[definitions_path],
        metrics={
            "partition_count": len(definitions),
            "partition_class_counts": dict(sorted(class_counts.items())),
        },
        project_root=project_root,
    )

    bars = {
        instrument_id: bar_snapshots(
            series.snapshots,
            bar_seconds=int(config["bar_seconds"]),
            max_age_seconds=int(config["max_snapshot_age_seconds"]),
        )
        for instrument_id, series in instruments.items()
    }
    pa, pq = _import_pyarrow()
    observations_path = output_directory / "partition_observations.parquet"
    schema = observation_schema()
    writer = pq.ParquetWriter(observations_path, schema, compression="zstd")
    accumulator = SummaryAccumulator(bar_seconds=int(config["bar_seconds"]))
    observation_count = 0
    try:
        for definition in definitions:
            rows = measure_partition(
                definition,
                instruments=instruments,
                bars=bars,
                fee_schedules=fee_schedules,
                sizes=[int(value) for value in config["sizes_contracts"]],
                skew_edges=[
                    float(value)
                    for value in config["skew_bucket_edges_seconds"]
                ],
            )
            for row in rows:
                accumulator.observe(row, config["gate"])
            if rows:
                writer.write_table(pa.Table.from_pylist(rows, schema=schema))
                observation_count += len(rows)
    finally:
        writer.close()

    partial_summary = accumulator.finalize(config["gate"])
    write_stage_manifest(
        output_directory,
        number=4,
        name="measure",
        status="skipped_data_invalid" if book_validation_failed else "passed",
        inputs={
            "definitions": sha256_file(definitions_path),
            "normalized_books": sha256_file(normalized_path),
            "fees": sha256_file(fee_path),
        },
        outputs=[observations_path],
        metrics={
            "observation_row_count": observation_count,
            "selected_row_count": partial_summary["selected_row_count"],
        },
        project_root=project_root,
    )

    terminal_status, gate_detail = determine_terminal_status(
        partial_summary,
        validation,
        fee_errors,
        config["gate"],
    )
    summary = {
        "run_id": run_id,
        "dataset_name": config["dataset_name"],
        "terminal_status": terminal_status,
        "gate": config["gate"],
        "gate_detail": gate_detail,
        "validation_valid": validation["valid"],
        "fee_metadata_valid": not fee_errors,
        "oddpool_network_requests": 0,
        "observation_row_count": observation_count,
        **partial_summary,
    }
    summary_path = output_directory / "summary.json"
    write_json(summary_path, summary)
    report_path = output_directory / "report.md"
    report_path.write_text(
        build_report(
            run_id=run_id,
            terminal_status=terminal_status,
            validation=validation,
            fee_metadata=fee_metadata,
            fee_errors=fee_errors,
            definitions=definition_rows,
            summary=summary,
            gate=config["gate"],
            dataset_name=str(config["dataset_name"]),
        ),
        encoding="utf-8",
    )
    write_stage_manifest(
        output_directory,
        number=5,
        name="gate_and_report",
        status="completed",
        inputs={
            "observations": sha256_file(observations_path),
            "validation": sha256_file(validation_path),
        },
        outputs=[summary_path, report_path],
        metrics={
            "terminal_status": terminal_status,
            "qualifying_events_by_class": gate_detail[
                "qualifying_events_by_class"
            ],
            "passing_events_by_class": gate_detail[
                "passing_events_by_class"
            ],
        },
        project_root=project_root,
    )

    manifest_output_path = output_directory / "run_manifest.json"
    write_json(
        manifest_output_path,
        {
            "run_id": run_id,
            "dataset_name": config["dataset_name"],
            "terminal_status": terminal_status,
            "config": raw_config,
            "fingerprints": fingerprints,
            "code_files": code_fingerprint(project_root),
            "inputs": {
                "config": relative_or_absolute(config_path, project_root),
                "manifest": relative_or_absolute(manifest_path, project_root),
                "history_job_directory": relative_or_absolute(
                    history_directory,
                    project_root,
                ),
            },
            "outputs": {
                path.name: {
                    "path": relative_or_absolute(path, project_root),
                    "sha256": sha256_file(path),
                }
                for path in (
                    fee_path,
                    validation_path,
                    normalized_path,
                    definitions_path,
                    observations_path,
                    summary_path,
                    report_path,
                )
            },
            "network": {
                "oddpool_requests": 0,
                "fee_metadata_is_durably_cached": True,
            },
            "fee_model": fee_audit_manifest(fee_metadata, fee_path),
        },
    )
    return {
        "run_id": run_id,
        "terminal_status": terminal_status,
        "output_directory": str(output_directory),
        "report_path": str(report_path),
        "summary_path": str(summary_path),
        "observation_row_count": observation_count,
        "normalized_row_count": normalized_row_count,
        "oddpool_network_requests": 0,
        "metadata_network_requests": metadata_request_stats["network_requests"],
        "metadata_cache_hits": metadata_request_stats["cache_hits"],
    }
