from __future__ import annotations

import bisect
import math
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Sequence


SMOKE_PM_CONDITION = "SMOKE_PM_CONDITION"
SMOKE_KALSHI_CONTRACT = "SMOKE_KALSHI_CONTRACT"
PARTITION_KALSHI_EVENT = "PARTITION_KALSHI_EVENT"
PARTITION_CROSS_VENUE = "PARTITION_CROSS_VENUE"


@dataclass(frozen=True)
class Level:
    price: float
    size: float


@dataclass(frozen=True)
class BookSnapshot:
    timestamp_ms: int
    bids: tuple[Level, ...]
    asks: tuple[Level, ...]
    provenance: Mapping[str, Any] = field(default_factory=dict)


@dataclass
class InstrumentSeries:
    instrument_id: str
    venue: str
    event_key: str
    market_id: str
    outcome: str
    position: str
    asset_id: str | None
    fee_key: str
    end_time_ms: int | None
    snapshots: list[BookSnapshot]


@dataclass(frozen=True)
class PartitionRepresentation:
    representation_id: str
    leg_instrument_ids: tuple[str, ...]


@dataclass(frozen=True)
class PartitionDefinition:
    economic_partition_id: str
    partition_class: str
    event_key: str
    partition_status: str
    rules_hashes: tuple[str, ...]
    resolution_sources: tuple[str, ...]
    representations: tuple[PartitionRepresentation, ...]


@dataclass(frozen=True)
class FillResult:
    requested_contracts: float
    filled_contracts: float
    cost: float
    vwap: float | None
    depth_limited: bool
    fills: tuple[tuple[float, float], ...]


@dataclass(frozen=True)
class FeeSchedule:
    venue: str
    fee_type: str
    rate: float
    multiplier: float = 1.0
    exponent: float = 1.0
    normal_rounding_dollars: float = 0.0
    conservative_rounding_dollars: float = 0.0
    fixed_execution_cost_dollars: float = 0.0
    effective_at_ms: int | None = None


def _as_float(value: Any) -> float:
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError(f"Non-finite numeric value {value!r}")
    return parsed


def parse_levels(
    values: Iterable[Mapping[str, Any]],
    *,
    sort_descending: bool,
) -> tuple[Level, ...]:
    levels = tuple(
        Level(price=_as_float(value["price"]), size=_as_float(value["size"]))
        for value in values
    )
    return tuple(
        sorted(
            levels,
            key=lambda level: level.price,
            reverse=sort_descending,
        )
    )


def complement_levels(
    levels: Sequence[Level],
    *,
    sort_descending: bool,
) -> tuple[Level, ...]:
    return tuple(
        sorted(
            (Level(price=1.0 - level.price, size=level.size) for level in levels),
            key=lambda level: level.price,
            reverse=sort_descending,
        )
    )


def normalize_kalshi_snapshot(
    row: Mapping[str, Any],
) -> tuple[BookSnapshot, BookSnapshot]:
    yes_bids = parse_levels(row.get("yes_bids") or [], sort_descending=True)
    no_bids = parse_levels(row.get("no_bids") or [], sort_descending=True)
    timestamp_ms = int(row["timestamp"])
    provenance = row.get("_provenance") or {}
    yes = BookSnapshot(
        timestamp_ms=timestamp_ms,
        bids=yes_bids,
        asks=complement_levels(no_bids, sort_descending=False),
        provenance=provenance,
    )
    no = BookSnapshot(
        timestamp_ms=timestamp_ms,
        bids=no_bids,
        asks=complement_levels(yes_bids, sort_descending=False),
        provenance=provenance,
    )
    return yes, no


def normalize_polymarket_snapshot(row: Mapping[str, Any]) -> BookSnapshot:
    return BookSnapshot(
        timestamp_ms=int(row["timestamp"]),
        bids=parse_levels(row.get("bids") or [], sort_descending=True),
        asks=parse_levels(row.get("asks") or [], sort_descending=False),
        provenance=row.get("_provenance") or {},
    )


def ladder_issues(
    raw_levels: Sequence[Mapping[str, Any]],
    *,
    expected_descending: bool,
    price_tolerance: float,
) -> list[str]:
    issues: list[str] = []
    parsed: list[Level] = []
    for raw in raw_levels:
        try:
            level = Level(_as_float(raw["price"]), _as_float(raw["size"]))
        except (KeyError, TypeError, ValueError):
            issues.append("invalid_level")
            continue
        if level.price < -price_tolerance or level.price > 1 + price_tolerance:
            issues.append("price_out_of_bounds")
        if level.size < 0:
            issues.append("negative_size")
        parsed.append(level)

    prices = [level.price for level in parsed]
    if len(set(prices)) != len(prices):
        issues.append("duplicate_price")
    expected = sorted(prices, reverse=expected_descending)
    if prices != expected:
        issues.append("wrong_order")
    return sorted(set(issues))


def price_matches(
    observed: Any,
    expected: float | None,
    *,
    tolerance: float,
) -> bool:
    if observed is None and expected is None:
        return True
    if observed is None or expected is None:
        return False
    return math.isclose(float(observed), expected, abs_tol=tolerance, rel_tol=0)


def validate_top_of_book(
    row: Mapping[str, Any],
    *,
    venue: str,
    price_tolerance: float,
) -> bool:
    if venue == "kalshi":
        yes, _ = normalize_kalshi_snapshot(row)
        bid = yes.bids[0].price if yes.bids else None
        ask = yes.asks[0].price if yes.asks else None
        return price_matches(
            row.get("best_yes_bid"), bid, tolerance=price_tolerance
        ) and price_matches(
            row.get("best_yes_ask"), ask, tolerance=price_tolerance
        )
    if venue == "polymarket":
        book = normalize_polymarket_snapshot(row)
        bid = book.bids[0].price if book.bids else None
        ask = book.asks[0].price if book.asks else None
        return price_matches(
            row.get("best_bid"), bid, tolerance=price_tolerance
        ) and price_matches(
            row.get("best_ask"), ask, tolerance=price_tolerance
        )
    raise ValueError(f"Unsupported venue {venue!r}")


def walk_ladder(levels: Sequence[Level], contracts: float) -> FillResult:
    if contracts <= 0:
        raise ValueError("contracts must be positive")
    remaining = float(contracts)
    cost = 0.0
    fills: list[tuple[float, float]] = []
    for level in levels:
        if level.size <= 0:
            continue
        take = min(remaining, level.size)
        if take <= 0:
            continue
        cost += take * level.price
        fills.append((level.price, take))
        remaining -= take
        if remaining <= 1e-12:
            remaining = 0.0
            break
    filled = float(contracts) - remaining
    return FillResult(
        requested_contracts=float(contracts),
        filled_contracts=filled,
        cost=cost,
        vwap=(cost / filled if filled > 0 else None),
        depth_limited=remaining > 0,
        fills=tuple(fills),
    )


def _round_increment(value: float, increment: float, *, up: bool) -> float:
    if increment <= 0:
        return value
    units = value / increment
    if up:
        return math.ceil(units - 1e-12) * increment
    return round(units) * increment


def fee_for_fills(
    schedule: FeeSchedule,
    fills: Sequence[tuple[float, float]],
) -> tuple[float, float, float]:
    raw = 0.0
    for price, quantity in fills:
        probability_term = max(0.0, price * (1.0 - price))
        raw += (
            schedule.rate
            * schedule.multiplier
            * quantity
            * (probability_term**schedule.exponent)
        )
    raw += schedule.fixed_execution_cost_dollars
    normal = _round_increment(
        raw,
        schedule.normal_rounding_dollars,
        up=(schedule.venue == "kalshi"),
    )
    conservative = _round_increment(
        raw,
        schedule.conservative_rounding_dollars,
        up=True,
    )
    return raw, normal, conservative


def fee_schedule_at(
    value: FeeSchedule | Sequence[FeeSchedule] | None,
    timestamp_ms: int,
) -> FeeSchedule | None:
    if value is None:
        return None
    if isinstance(value, FeeSchedule):
        return (
            value
            if value.effective_at_ms is None
            or value.effective_at_ms <= timestamp_ms
            else None
        )
    eligible = [
        schedule
        for schedule in value
        if schedule.effective_at_ms is None
        or schedule.effective_at_ms <= timestamp_ms
    ]
    if not eligible:
        return None
    return max(
        eligible,
        key=lambda schedule: (
            schedule.effective_at_ms
            if schedule.effective_at_ms is not None
            else -1
        ),
    )


def skew_bucket(skew_seconds: float, edges: Sequence[float]) -> str:
    if len(edges) != 3:
        raise ValueError("skew bucket edges must contain exactly three values")
    if skew_seconds < edges[0]:
        return "lt_5s"
    if skew_seconds < edges[1]:
        return "5_to_15s"
    if skew_seconds <= edges[2]:
        return "15_to_60s"
    return "gt_60s"


def bar_snapshots(
    snapshots: Sequence[BookSnapshot],
    *,
    bar_seconds: int,
    max_age_seconds: int,
) -> dict[int, BookSnapshot]:
    if not snapshots:
        return {}
    if bar_seconds <= 0 or max_age_seconds < 0:
        raise ValueError("invalid bar configuration")
    ordered = sorted(snapshots, key=lambda snapshot: snapshot.timestamp_ms)
    bar_ms = bar_seconds * 1000
    maximum_age_ms = max_age_seconds * 1000
    first_bar = ((ordered[0].timestamp_ms + bar_ms - 1) // bar_ms) * bar_ms
    last_bar = (
        (ordered[-1].timestamp_ms + maximum_age_ms) // bar_ms
    ) * bar_ms
    output: dict[int, BookSnapshot] = {}
    index = -1
    for boundary in range(first_bar, last_bar + 1, bar_ms):
        while (
            index + 1 < len(ordered)
            and ordered[index + 1].timestamp_ms <= boundary
        ):
            index += 1
        if index < 0:
            continue
        selected = ordered[index]
        if boundary - selected.timestamp_ms <= maximum_age_ms:
            output[boundary] = selected
    return output


def level_count_diagnostics(
    counts: Sequence[int],
    *,
    minimum_distinct_nonzero_counts: int,
    maximum_share_at_cap: float,
) -> dict[str, Any]:
    nonzero = [count for count in counts if count > 0]
    if not nonzero:
        return {
            "valid": False,
            "reason": "no_nonzero_ladders",
            "observed_max": 0,
            "distinct_nonzero_counts": 0,
            "share_at_observed_max": None,
        }
    observed_max = max(nonzero)
    frequencies = Counter(nonzero)
    share_at_max = frequencies[observed_max] / len(nonzero)
    distinct = len(frequencies)
    valid = (
        distinct >= minimum_distinct_nonzero_counts
        and share_at_max <= maximum_share_at_cap
    )
    return {
        "valid": valid,
        "reason": None if valid else "possible_fixed_level_cap",
        "observed_max": observed_max,
        "distinct_nonzero_counts": distinct,
        "share_at_observed_max": share_at_max,
        "most_common_counts": frequencies.most_common(10),
    }


def _level_maps(
    levels: Sequence[Level],
    *,
    complement: bool,
) -> dict[float, float]:
    return {
        round(1.0 - level.price if complement else level.price, 9): level.size
        for level in levels
    }


def levels_match(
    first: Sequence[Level],
    second: Sequence[Level],
    *,
    complement_second: bool,
    size_tolerance: float,
) -> bool:
    left = _level_maps(first, complement=False)
    right = _level_maps(second, complement=complement_second)
    if left.keys() != right.keys():
        return False
    return all(
        math.isclose(
            left[price],
            right[price],
            abs_tol=size_tolerance,
            rel_tol=1e-9,
        )
        for price in left
    )


def polymarket_complement_diagnostics(
    first: Sequence[BookSnapshot],
    second: Sequence[BookSnapshot],
    *,
    pair_tolerance_ms: int,
    size_tolerance: float,
) -> dict[str, Any]:
    if not first or not second:
        return {
            "valid": False,
            "paired": 0,
            "matched": 0,
            "mismatched": 0,
            "unpaired": len(first),
            "match_rate": None,
            "maximum_pair_skew_ms": None,
        }
    second_ordered = sorted(second, key=lambda snapshot: snapshot.timestamp_ms)
    second_times = [snapshot.timestamp_ms for snapshot in second_ordered]
    matched = 0
    mismatched = 0
    unpaired = 0
    skews: list[int] = []
    for snapshot in sorted(first, key=lambda value: value.timestamp_ms):
        index = bisect.bisect_left(second_times, snapshot.timestamp_ms)
        candidates = second_ordered[max(0, index - 1) : min(len(second), index + 1)]
        if not candidates:
            unpaired += 1
            continue
        paired = min(
            candidates,
            key=lambda value: abs(value.timestamp_ms - snapshot.timestamp_ms),
        )
        skew = abs(paired.timestamp_ms - snapshot.timestamp_ms)
        if skew > pair_tolerance_ms:
            unpaired += 1
            continue
        skews.append(skew)
        is_match = levels_match(
            snapshot.asks,
            paired.bids,
            complement_second=True,
            size_tolerance=size_tolerance,
        ) and levels_match(
            snapshot.bids,
            paired.asks,
            complement_second=True,
            size_tolerance=size_tolerance,
        )
        if is_match:
            matched += 1
        else:
            mismatched += 1
    paired_count = matched + mismatched
    return {
        "valid": paired_count > 0,
        "paired": paired_count,
        "matched": matched,
        "mismatched": mismatched,
        "unpaired": unpaired,
        "match_rate": matched / paired_count if paired_count else None,
        "maximum_pair_skew_ms": max(skews) if skews else None,
    }


def _rules_map(match: Mapping[str, Any]) -> dict[str, str]:
    market_ids = match.get("kalshi", {}).get("market_ids") or []
    hashes = match.get("kalshi", {}).get("rules_hashes") or []
    return {
        str(market_id): str(rule_hash)
        for market_id, rule_hash in zip(market_ids, hashes)
        if rule_hash
    }


def build_partition_definitions(
    manifest: Mapping[str, Any],
    instruments: Mapping[str, InstrumentSeries],
) -> list[PartitionDefinition]:
    definitions: list[PartitionDefinition] = []
    for match in manifest.get("matches") or []:
        event_key = str(match["event_key"])
        targets = match.get("history_targets") or []
        kalshi_targets = [
            target for target in targets if target.get("venue") == "kalshi"
        ]
        polymarket_targets = [
            target for target in targets if target.get("venue") == "polymarket"
        ]
        rules_by_market = _rules_map(match)
        poly_rules_hash = str(
            match.get("polymarket", {}).get("rules_hash") or ""
        )
        poly_resolution = str(
            match.get("polymarket", {}).get("resolution_source") or ""
        )

        for target in kalshi_targets:
            market_id = str(target["market_id"])
            yes_id = f"kalshi:{market_id}:yes"
            no_id = f"kalshi:{market_id}:no"
            if yes_id not in instruments or no_id not in instruments:
                continue
            definitions.append(
                PartitionDefinition(
                    economic_partition_id=f"smoke-kalshi:{market_id}",
                    partition_class=SMOKE_KALSHI_CONTRACT,
                    event_key=event_key,
                    partition_status="MECHANICAL_COMPLEMENT",
                    rules_hashes=(rules_by_market.get(market_id, ""),),
                    resolution_sources=("kalshi_same_contract",),
                    representations=(
                        PartitionRepresentation(
                            representation_id=f"{market_id}:yes+no",
                            leg_instrument_ids=(yes_id, no_id),
                        ),
                    ),
                )
            )

        if polymarket_targets:
            target = polymarket_targets[0]
            condition_id = str(target["market_id"])
            token_ids = [
                f"polymarket:{condition_id}:{token['asset_id']}"
                for token in target.get("outcome_tokens") or []
            ]
            if len(token_ids) == 2 and all(
                token_id in instruments for token_id in token_ids
            ):
                definitions.append(
                    PartitionDefinition(
                        economic_partition_id=f"smoke-pm:{condition_id}",
                        partition_class=SMOKE_PM_CONDITION,
                        event_key=event_key,
                        partition_status="MECHANICAL_COMPLEMENT",
                        rules_hashes=(poly_rules_hash,),
                        resolution_sources=(poly_resolution,),
                        representations=(
                            PartitionRepresentation(
                                representation_id=f"{condition_id}:token-pair",
                                leg_instrument_ids=tuple(token_ids),
                            ),
                        ),
                    )
                )

        if (
            len(kalshi_targets) >= 2
            and bool(match.get("kalshi", {}).get("mutually_exclusive"))
        ):
            yes_ids = tuple(
                f"kalshi:{target['market_id']}:yes" for target in kalshi_targets
            )
            no_ids = tuple(
                f"kalshi:{target['market_id']}:no" for target in kalshi_targets
            )
            event_representations: list[PartitionRepresentation] = []
            # Exactly one outcome of a mutually exclusive event resolves YES, so
            # one contract of every YES leg pays exactly $1 for any leg count.
            if all(instrument_id in instruments for instrument_id in yes_ids):
                event_representations.append(
                    PartitionRepresentation(
                        representation_id=f"{event_key}:all-yes",
                        leg_instrument_ids=yes_ids,
                    )
                )
            # The all-NO basket pays $(n-1), so it is only a unit-payout
            # partition in the two-outcome case, where NO_A is simply YES_B.
            if len(kalshi_targets) == 2 and all(
                instrument_id in instruments for instrument_id in no_ids
            ):
                event_representations.append(
                    PartitionRepresentation(
                        representation_id=f"{event_key}:all-no",
                        leg_instrument_ids=no_ids,
                    )
                )
            if event_representations:
                definitions.append(
                    PartitionDefinition(
                        economic_partition_id=f"kalshi-event:{event_key}",
                        partition_class=PARTITION_KALSHI_EVENT,
                        event_key=event_key,
                        partition_status="RULES_REVIEW_REQUIRED",
                        rules_hashes=tuple(
                            rules_by_market.get(str(target["market_id"]), "")
                            for target in kalshi_targets
                        ),
                        resolution_sources=("kalshi_event",),
                        representations=tuple(event_representations),
                    )
                )

        if len(kalshi_targets) == 2 and polymarket_targets:
            target = polymarket_targets[0]
            condition_id = str(target["market_id"])
            poly_by_outcome = {
                str(token["outcome"]): (
                    f"polymarket:{condition_id}:{token['asset_id']}"
                )
                for token in target.get("outcome_tokens") or []
            }
            representations: list[PartitionRepresentation] = []
            for kalshi_target in kalshi_targets:
                market_id = str(kalshi_target["market_id"])
                outcome = str(kalshi_target.get("outcome") or "")
                other_outcomes = [
                    candidate
                    for candidate in poly_by_outcome
                    if candidate != outcome
                ]
                same_poly = poly_by_outcome.get(outcome)
                if len(other_outcomes) != 1 or same_poly is None:
                    continue
                other_poly = poly_by_outcome[other_outcomes[0]]
                yes_id = f"kalshi:{market_id}:yes"
                no_id = f"kalshi:{market_id}:no"
                if yes_id in instruments and other_poly in instruments:
                    representations.append(
                        PartitionRepresentation(
                            representation_id=(
                                f"{event_key}:{market_id}:yes+poly:"
                                f"{other_outcomes[0]}"
                            ),
                            leg_instrument_ids=(yes_id, other_poly),
                        )
                    )
                if no_id in instruments and same_poly in instruments:
                    representations.append(
                        PartitionRepresentation(
                            representation_id=(
                                f"{event_key}:{market_id}:no+poly:{outcome}"
                            ),
                            leg_instrument_ids=(no_id, same_poly),
                        )
                    )
            if representations:
                definitions.append(
                    PartitionDefinition(
                        economic_partition_id=f"cross-venue:{event_key}",
                        partition_class=PARTITION_CROSS_VENUE,
                        event_key=event_key,
                        partition_status="NEAR_PARTITION_RULES_REVIEW_REQUIRED",
                        rules_hashes=tuple(
                            [
                                *(
                                    rules_by_market.get(
                                        str(kalshi_target["market_id"]), ""
                                    )
                                    for kalshi_target in kalshi_targets
                                ),
                                poly_rules_hash,
                            ]
                        ),
                        resolution_sources=("kalshi_event", poly_resolution),
                        representations=tuple(
                            sorted(
                                representations,
                                key=lambda representation: (
                                    representation.representation_id
                                ),
                            )
                        ),
                    )
                )
    return sorted(
        definitions,
        key=lambda definition: (
            definition.partition_class,
            definition.economic_partition_id,
        ),
    )


def measure_partition(
    definition: PartitionDefinition,
    *,
    instruments: Mapping[str, InstrumentSeries],
    bars: Mapping[str, Mapping[int, BookSnapshot]],
    fee_schedules: Mapping[
        str,
        FeeSchedule | Sequence[FeeSchedule],
    ],
    sizes: Sequence[int],
    skew_edges: Sequence[float],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for representation in definition.representations:
        leg_series = [
            instruments[instrument_id]
            for instrument_id in representation.leg_instrument_ids
        ]
        common_bars: set[int] | None = None
        for instrument_id in representation.leg_instrument_ids:
            available = set(bars.get(instrument_id, {}))
            common_bars = available if common_bars is None else common_bars & available
        for bar_timestamp in sorted(common_bars or set()):
            snapshots = [
                bars[instrument_id][bar_timestamp]
                for instrument_id in representation.leg_instrument_ids
            ]
            snapshot_times = [snapshot.timestamp_ms for snapshot in snapshots]
            ages_seconds = [
                (bar_timestamp - snapshot_time) / 1000
                for snapshot_time in snapshot_times
            ]
            leg_skew_seconds = (
                max(snapshot_times) - min(snapshot_times)
            ) / 1000
            bucket = skew_bucket(leg_skew_seconds, skew_edges)
            end_times = [
                series.end_time_ms
                for series in leg_series
                if series.end_time_ms is not None
            ]
            time_to_resolution_seconds = (
                (min(end_times) - bar_timestamp) / 1000
                if end_times
                else None
            )

            for direction in ("long", "short"):
                sides = [
                    snapshot.asks if direction == "long" else snapshot.bids
                    for snapshot in snapshots
                ]
                one_sided_legs = sum(1 for levels in sides if not levels)
                minimum_available_depth = min(
                    (sum(level.size for level in levels) for levels in sides),
                    default=0.0,
                )
                for size in sizes:
                    fills = [walk_ladder(levels, size) for levels in sides]
                    depth_limited = any(fill.depth_limited for fill in fills)
                    max_fillable = min(
                        (fill.filled_contracts for fill in fills),
                        default=0.0,
                    )
                    exclusion_reason: str | None = None
                    if one_sided_legs:
                        exclusion_reason = "one_sided"
                    elif depth_limited:
                        exclusion_reason = "depth_limited"

                    leg_fee_schedules = [
                        fee_schedule_at(
                            fee_schedules.get(series.fee_key),
                            bar_timestamp,
                        )
                        for series in leg_series
                    ]
                    fees_available = all(
                        schedule is not None
                        for schedule in leg_fee_schedules
                    )
                    if exclusion_reason is None and not fees_available:
                        exclusion_reason = "fee_metadata_missing"

                    gross_gap: float | None = None
                    net_gap_normal: float | None = None
                    net_gap_conservative: float | None = None
                    profit_normal: float | None = None
                    profit_conservative: float | None = None
                    fee_raw_total: float | None = None
                    fee_normal_total: float | None = None
                    fee_conservative_total: float | None = None
                    leg_fee_raw: list[float | None] = []
                    leg_fee_normal: list[float | None] = []
                    leg_fee_conservative: list[float | None] = []
                    cost_per_contract: float | None = None

                    if exclusion_reason is None:
                        fee_values = [
                            fee_for_fills(schedule, fill.fills)
                            for schedule, fill in zip(leg_fee_schedules, fills)
                            if schedule is not None
                        ]
                        leg_fee_raw = [value[0] for value in fee_values]
                        leg_fee_normal = [value[1] for value in fee_values]
                        leg_fee_conservative = [value[2] for value in fee_values]
                        fee_raw_total = sum(value[0] for value in fee_values)
                        fee_normal_total = sum(value[1] for value in fee_values)
                        fee_conservative_total = sum(
                            value[2] for value in fee_values
                        )
                        cost_per_contract = sum(fill.cost for fill in fills) / size
                        if direction == "long":
                            gross_gap = 1.0 - cost_per_contract
                        else:
                            gross_gap = cost_per_contract - 1.0
                        net_gap_normal = gross_gap - fee_normal_total / size
                        net_gap_conservative = (
                            gross_gap - fee_conservative_total / size
                        )
                        profit_normal = net_gap_normal * size
                        profit_conservative = net_gap_conservative * size
                    else:
                        leg_fee_raw = [None] * len(fills)
                        leg_fee_normal = [None] * len(fills)
                        leg_fee_conservative = [None] * len(fills)

                    rows.append(
                        {
                            "economic_partition_id": (
                                definition.economic_partition_id
                            ),
                            "representation_id": (
                                representation.representation_id
                            ),
                            "partition_class": definition.partition_class,
                            "partition_status": definition.partition_status,
                            "event_key": definition.event_key,
                            "bar_timestamp_ms": bar_timestamp,
                            "direction": direction,
                            "diagnostic_only": direction == "short",
                            "size_contracts": int(size),
                            "leg_instrument_ids": [
                                series.instrument_id for series in leg_series
                            ],
                            "leg_market_ids": [
                                series.market_id for series in leg_series
                            ],
                            "leg_asset_ids": [
                                series.asset_id or "" for series in leg_series
                            ],
                            "leg_positions": [
                                series.position for series in leg_series
                            ],
                            "leg_snapshot_timestamps_ms": snapshot_times,
                            "leg_snapshot_ages_seconds": ages_seconds,
                            "leg_skew_seconds": leg_skew_seconds,
                            "skew_bucket": bucket,
                            "time_to_resolution_seconds": (
                                time_to_resolution_seconds
                            ),
                            "minimum_leg_available_depth_contracts": (
                                minimum_available_depth
                            ),
                            "leg_vwap_prices": [
                                fill.vwap for fill in fills
                            ],
                            "leg_filled_contracts": [
                                fill.filled_contracts for fill in fills
                            ],
                            "leg_costs_dollars": [
                                fill.cost for fill in fills
                            ],
                            "leg_fee_raw_dollars": leg_fee_raw,
                            "leg_fee_normal_dollars": leg_fee_normal,
                            "leg_fee_conservative_dollars": (
                                leg_fee_conservative
                            ),
                            "cost_or_proceeds_per_contract": cost_per_contract,
                            "gross_gap_per_contract": gross_gap,
                            "net_gap_normal_per_contract": net_gap_normal,
                            "net_gap_conservative_per_contract": (
                                net_gap_conservative
                            ),
                            "profit_normal_dollars": profit_normal,
                            "profit_conservative_dollars": (
                                profit_conservative
                            ),
                            "fee_raw_total_dollars": fee_raw_total,
                            "fee_normal_total_dollars": fee_normal_total,
                            "fee_conservative_total_dollars": (
                                fee_conservative_total
                            ),
                            "valid": exclusion_reason is None,
                            "fee_complete": fees_available,
                            "depth_limited": depth_limited,
                            "max_fillable_contracts": max_fillable,
                            "one_sided_legs": one_sided_legs,
                            "exclusion_reason": exclusion_reason,
                            "selected_best_representation": False,
                        }
                    )

    selection_groups: dict[tuple[int, str, int], list[dict[str, Any]]] = {}
    for row in rows:
        key = (
            row["bar_timestamp_ms"],
            row["direction"],
            row["size_contracts"],
        )
        selection_groups.setdefault(key, []).append(row)
    for candidates in selection_groups.values():
        valid = [row for row in candidates if row["valid"]]
        if not valid:
            continue
        selected = max(
            valid,
            key=lambda row: (
                row["net_gap_conservative_per_contract"],
                -len(row["representation_id"]),
                row["representation_id"],
            ),
        )
        selected["selected_best_representation"] = True
    return sorted(
        rows,
        key=lambda row: (
            row["bar_timestamp_ms"],
            row["direction"],
            row["size_contracts"],
            row["representation_id"],
        ),
    )
