"""Exact VWAP economics, symbolic payout LP, and matched-leg placebo null."""

from __future__ import annotations

import itertools
import statistics
from dataclasses import dataclass
from decimal import ROUND_CEILING, Decimal
from fractions import Fraction
from typing import Any, Iterable, Mapping, Sequence

from replay.catalog import FeeTerms, InstrumentMetadata, MetadataCatalogue
from replay.events import (
    ConnectionOpened,
    FullBook,
    MetadataChanged,
    ReplayEvent,
)
from replay.trust import MarketTrust, TrustAudit, Verdict

DEFAULT_SIZES = (1, 10, 25, 50, 100, 250, 500, 1000)
HEADLINE_SIZE = 100
CONSERVATIVE_FEE_ROUNDING = Decimal("0.0001")


@dataclass(frozen=True)
class Fill:
    requested: Decimal
    filled: Decimal
    notional: Decimal
    vwap: Decimal | None
    depth_limited: bool
    levels: tuple[tuple[Decimal, Decimal], ...]


@dataclass(frozen=True)
class CoverSolution:
    feasible: bool
    minimum_cost: Decimal | None
    weights: tuple[Decimal, ...]

    def as_record(self) -> dict[str, object]:
        return {
            "feasible": self.feasible,
            "minimum_cost": _text(self.minimum_cost),
            "weights": [_text(value) for value in self.weights],
        }


@dataclass(frozen=True)
class EconomicAudit:
    rows: tuple[dict[str, Any], ...]
    summary: dict[str, Any]
    placebo_summary: dict[str, Any]
    exclusions: dict[str, int]


@dataclass(frozen=True)
class _CapturedBook:
    market_id: str
    asset_id: str
    receive_ns: int
    source_time: str
    metadata_digest: str | None
    metadata: InstrumentMetadata
    bids: tuple[tuple[Decimal, Decimal], ...]
    asks: tuple[tuple[Decimal, Decimal], ...]


def walk_ladder(
    levels: Sequence[tuple[Decimal, Decimal]], contracts: Decimal
) -> Fill:
    if contracts <= 0:
        raise ValueError("contracts must be positive")
    remaining = contracts
    notional = Decimal(0)
    fills: list[tuple[Decimal, Decimal]] = []
    for price, available in levels:
        if available <= 0:
            continue
        take = min(remaining, available)
        if take <= 0:
            continue
        fills.append((price, take))
        notional += price * take
        remaining -= take
        if remaining == 0:
            break
    filled = contracts - remaining
    return Fill(
        requested=contracts,
        filled=filled,
        notional=notional,
        vwap=notional / filled if filled else None,
        depth_limited=remaining > 0,
        levels=tuple(fills),
    )


def conservative_fee(terms: FeeTerms, fill: Fill) -> Decimal:
    raw = Decimal(0)
    for price, quantity in fill.levels:
        probability = max(Decimal(0), price * (Decimal(1) - price))
        raw += terms.rate * quantity * _power(probability, terms.exponent)
    if raw == 0:
        return raw
    units = (raw / CONSERVATIVE_FEE_ROUNDING).to_integral_value(
        rounding=ROUND_CEILING
    )
    return units * CONSERVATIVE_FEE_ROUNDING


def solve_cover_lp(
    outcomes: Sequence[str],
    masks: Sequence[frozenset[str]],
    unit_costs: Sequence[Decimal],
) -> CoverSolution:
    """Solve min c·x, A·x>=1, x>=0 by exact vertex enumeration.

    This is intentionally a small symbolic LP for event baskets, not a generic
    optimizer. It has no floating-point tolerance and no external dependency.
    """
    if not outcomes or len(masks) != len(unit_costs) or not masks:
        return CoverSolution(False, None, ())
    variable_count = len(masks)
    constraints: list[tuple[list[Fraction], Fraction]] = []
    for outcome in outcomes:
        constraints.append(
            (
                [
                    Fraction(1) if outcome in mask else Fraction(0)
                    for mask in masks
                ],
                Fraction(1),
            )
        )
    for index in range(variable_count):
        row = [Fraction(0)] * variable_count
        row[index] = Fraction(1)
        constraints.append((row, Fraction(0)))

    best_cost: Fraction | None = None
    best_weights: tuple[Fraction, ...] = ()
    costs = tuple(Fraction(value) for value in unit_costs)
    for active in itertools.combinations(constraints, variable_count):
        solution = _solve_square(
            [list(row) for row, _ in active],
            [right for _, right in active],
        )
        if solution is None or any(value < 0 for value in solution):
            continue
        if any(
            sum(
                (Fraction(1) if outcome in mask else Fraction(0)) * weight
                for mask, weight in zip(masks, solution)
            )
            < 1
            for outcome in outcomes
        ):
            continue
        cost = sum(value * weight for value, weight in zip(costs, solution))
        if (
            best_cost is None
            or cost < best_cost
            or (cost == best_cost and tuple(solution) < best_weights)
        ):
            best_cost = cost
            best_weights = tuple(solution)
    if best_cost is None:
        return CoverSolution(False, None, ())
    return CoverSolution(
        True,
        _fraction_decimal(best_cost),
        tuple(_fraction_decimal(value) for value in best_weights),
    )


def audit_binary_economics(
    events: Iterable[ReplayEvent],
    catalogue: MetadataCatalogue,
    trust: TrustAudit,
    *,
    sizes: Sequence[int] = DEFAULT_SIZES,
) -> EconomicAudit:
    grouped: dict[
        tuple[str, str], dict[str, list[_CapturedBook]]
    ] = {}
    active_digest: dict[tuple[str, str, str], str | None] = {}
    completed: list[tuple[_CapturedBook, _CapturedBook]] = []
    market_metadata: dict[str, dict[str, InstrumentMetadata]] = {}

    for event in events:
        connection_key = (event.venue, event.lane, event.epoch)
        if isinstance(event, ConnectionOpened):
            active_digest[connection_key] = event.target_metadata_digest
            continue
        if isinstance(event, MetadataChanged):
            active_digest[connection_key] = event.to_metadata_digest
            continue
        if not (
            isinstance(event, FullBook)
            and event.venue == "polymarket"
            and event.independent_snapshot
            and event.market_id is not None
        ):
            continue
        digest = active_digest.get(connection_key)
        metadata = catalogue.by_asset("polymarket", event.asset_id, digest)
        if metadata is None:
            continue
        book = _CapturedBook(
            market_id=event.market_id,
            asset_id=event.asset_id,
            receive_ns=event.order_ns,
            source_time=str(event.source_time),
            metadata_digest=digest,
            metadata=metadata,
            bids=tuple(
                sorted(
                    (
                        (level.price, level.size / metadata.size_scale)
                        for level in event.bids
                        if level.size > 0
                    ),
                    reverse=True,
                )
            ),
            asks=tuple(
                sorted(
                    (
                        (level.price, level.size / metadata.size_scale)
                        for level in event.asks
                        if level.size > 0
                    )
                )
            ),
        )
        market_metadata.setdefault(event.market_id, {})[event.asset_id] = metadata
        key = (event.market_id, book.source_time)
        slot = grouped.setdefault(key, {})
        slot.setdefault(event.asset_id, []).append(book)
        expected_assets = {
            item.subscription_asset_id
            for item in market_metadata[event.market_id].values()
            if item.outcome_index in (0, 1)
        }
        if len(expected_assets) != 2 or not all(slot.get(asset) for asset in expected_assets):
            continue
        ordered_assets = sorted(
            expected_assets,
            key=lambda asset: (
                market_metadata[event.market_id][asset].outcome_index
                if market_metadata[event.market_id][asset].outcome_index
                is not None
                else 99,
                asset,
            ),
        )
        completed.append(
            (slot[ordered_assets[0]].pop(0), slot[ordered_assets[1]].pop(0))
        )
        if not any(slot.values()):
            grouped.pop(key, None)

    trust_by_market = {
        market.market_id: market
        for market in trust.markets
        if market.venue == "polymarket"
    }
    rows: list[dict[str, Any]] = []
    exclusions: dict[str, int] = {}
    for first, second in completed:
        market_trust = trust_by_market.get(first.market_id)
        for size in sizes:
            for direction in ("long", "short"):
                row = _measure_pair(
                    first,
                    second,
                    Decimal(size),
                    direction,
                    market_trust,
                )
                reason = row["exclusion_reason"]
                if reason is not None:
                    exclusions[reason] = exclusions.get(reason, 0) + 1
                rows.append(row)

    rows = _attach_placebos(rows)
    long_rows = [row for row in rows if row["direction"] == "long"]
    headline = [
        row
        for row in long_rows
        if row["size_contracts"] == HEADLINE_SIZE and row["headline_eligible"]
    ]
    positive = [
        row
        for row in headline
        if Decimal(row["net_gap_conservative_per_contract"]) > 0
    ]
    summary = {
        "scope": "same_condition_binary_long_baskets",
        "cross_venue_baskets": 0,
        "cross_venue_reason": (
            "no exact condition identity with matching resolution source, "
            "observation method, and fixing time exists in this fixture"
        ),
        "sizes_contracts": list(sizes),
        "headline_size_contracts": HEADLINE_SIZE,
        "observed_binary_pairs": len(completed),
        "rows": len(rows),
        "headline_eligible_rows": len(headline),
        "headline_positive_rows": len(positive),
        "headline_positive_rate_percentage": _percentage(
            len(positive), len(headline)
        ),
        "headline_median_net_gap": _median_text(
            [
                Decimal(row["net_gap_conservative_per_contract"])
                for row in headline
            ]
        ),
        "headline_max_net_gap": _max_text(
            [
                Decimal(row["net_gap_conservative_per_contract"])
                for row in headline
            ]
        ),
        "fixture_economic_observation": (
            "POSITIVE_DEPLOYABLE_QUOTES_OBSERVED"
            if positive
            else "NO_POSITIVE_DEPLOYABLE_QUOTES_OBSERVED"
        ),
    }
    null_rows = [
        row
        for row in long_rows
        if row.get("placebo_status") == "MATCHED"
        and row.get("placebo_net_gap_conservative_per_contract") is not None
    ]
    placebo_summary = {
        "construction": (
            "replace leg 2 with the nearest-time different-condition leg having "
            "the same venue, size, direction, and skew stratum; preserve the "
            "two-state incidence matrix"
        ),
        "matched_rows": len(null_rows),
        "positive_rows": sum(
            Decimal(row["placebo_net_gap_conservative_per_contract"]) > 0
            for row in null_rows
        ),
        "median_net_gap": _median_text(
            [
                Decimal(row["placebo_net_gap_conservative_per_contract"])
                for row in null_rows
            ]
        ),
    }
    return EconomicAudit(
        rows=tuple(rows),
        summary=summary,
        placebo_summary=placebo_summary,
        exclusions=dict(sorted(exclusions.items())),
    )


def _measure_pair(
    first: _CapturedBook,
    second: _CapturedBook,
    size: Decimal,
    direction: str,
    market_trust: MarketTrust | None,
) -> dict[str, Any]:
    books = (first, second)
    sides = tuple(book.asks if direction == "long" else book.bids for book in books)
    fills = tuple(walk_ladder(side, size) for side in sides)
    observation_ns = max(book.receive_ns for book in books)
    skew_ns = observation_ns - min(book.receive_ns for book in books)
    verdict = _verdict_at(market_trust, observation_ns)
    one_sided = sum(not side for side in sides)
    minimums = tuple(book.metadata.minimum_order_size for book in books)
    fee_terms = tuple(book.metadata.fee_terms for book in books)
    exclusion: str | None = None
    if verdict != Verdict.TRUSTED:
        exclusion = f"trust_{verdict.value.lower()}"
    elif one_sided:
        exclusion = "one_sided"
    elif any(fill.depth_limited for fill in fills):
        exclusion = "depth_limited"
    elif any(minimum is not None and size < minimum for minimum in minimums):
        exclusion = "below_minimum_order"
    elif any(value is None for value in fee_terms):
        exclusion = "fee_metadata_missing"

    gross_gap: Decimal | None = None
    net_gap: Decimal | None = None
    fees: tuple[Decimal | None, ...] = tuple(None for _ in books)
    unit_costs: tuple[Decimal, ...] = ()
    cover = CoverSolution(False, None, ())
    if all(fill.vwap is not None and not fill.depth_limited for fill in fills):
        total = sum(fill.notional for fill in fills) / size
        gross_gap = Decimal(1) - total if direction == "long" else total - Decimal(1)
        if all(value is not None for value in fee_terms):
            fees = tuple(
                conservative_fee(terms, fill)
                for terms, fill in zip(fee_terms, fills)
                if terms is not None
            )
            net_gap = gross_gap - sum(
                value for value in fees if value is not None
            ) / size
            if direction == "long":
                unit_costs = tuple(
                    fill.notional / size + (fee or Decimal(0)) / size
                    for fill, fee in zip(fills, fees)
                )
                cover = solve_cover_lp(
                    ("OUTCOME_0", "OUTCOME_1"),
                    (
                        frozenset({"OUTCOME_0"}),
                        frozenset({"OUTCOME_1"}),
                    ),
                    unit_costs,
                )

    return {
        "basket_id": f"polymarket:{first.market_id}:binary",
        "venue": "polymarket",
        "market_id": first.market_id,
        "source_time": first.source_time,
        "observation_ns": observation_ns,
        "direction": direction,
        "size_contracts": int(size),
        "leg_asset_ids": [book.asset_id for book in books],
        "leg_outcomes": [book.metadata.outcome for book in books],
        "leg_receive_ns": [book.receive_ns for book in books],
        "leg_skew_ns": skew_ns,
        "leg_skew_stratum": _skew_stratum(skew_ns),
        "trust_verdict": verdict.value,
        "fee_source_hashes": [
            value.source_record_hash if value is not None else None
            for value in fee_terms
        ],
        "leg_vwap": [_text(fill.vwap) for fill in fills],
        "leg_filled_contracts": [_text(fill.filled) for fill in fills],
        "leg_fee_conservative_dollars": [_text(value) for value in fees],
        "gross_gap_per_contract": _text(gross_gap),
        "net_gap_conservative_per_contract": _text(net_gap),
        "profit_conservative_dollars": _text(
            net_gap * size if net_gap is not None else None
        ),
        "depth_limited": any(fill.depth_limited for fill in fills),
        "max_fillable_contracts": _text(min(fill.filled for fill in fills)),
        "one_sided_legs": one_sided,
        "exclusion_reason": exclusion,
        "headline_eligible": exclusion is None and direction == "long",
        "subset_cover_lp": cover.as_record(),
        "_unit_costs": [_text(value) for value in unit_costs],
    }


def _attach_placebos(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    eligible = [
        row
        for row in rows
        if row["direction"] == "long"
        and row["exclusion_reason"] is None
        and row["subset_cover_lp"]["feasible"]
        and len(row["_unit_costs"]) == 2
    ]
    output: list[dict[str, Any]] = []
    for row in rows:
        result = dict(row)
        if row not in eligible:
            result["placebo_status"] = "NOT_ELIGIBLE"
            result["placebo_market_id"] = None
            result["placebo_net_gap_conservative_per_contract"] = None
        else:
            candidates = [
                candidate
                for candidate in eligible
                if candidate["market_id"] != row["market_id"]
                and candidate["size_contracts"] == row["size_contracts"]
                and candidate["direction"] == row["direction"]
                and candidate["leg_skew_stratum"] == row["leg_skew_stratum"]
            ]
            if not candidates:
                result["placebo_status"] = "NO_MATCH"
                result["placebo_market_id"] = None
                result["placebo_net_gap_conservative_per_contract"] = None
            else:
                matched = min(
                    candidates,
                    key=lambda candidate: (
                        abs(candidate["observation_ns"] - row["observation_ns"]),
                        candidate["market_id"],
                        candidate["observation_ns"],
                    ),
                )
                costs = (
                    Decimal(row["_unit_costs"][0]),
                    Decimal(matched["_unit_costs"][1]),
                )
                cover = solve_cover_lp(
                    ("OUTCOME_0", "OUTCOME_1"),
                    (
                        frozenset({"OUTCOME_0"}),
                        frozenset({"OUTCOME_1"}),
                    ),
                    costs,
                )
                result["placebo_status"] = "MATCHED"
                result["placebo_market_id"] = matched["market_id"]
                result["placebo_net_gap_conservative_per_contract"] = _text(
                    Decimal(1) - cover.minimum_cost
                    if cover.minimum_cost is not None
                    else None
                )
        result.pop("_unit_costs", None)
        output.append(result)
    return output


def _verdict_at(market: MarketTrust | None, time_ns: int) -> Verdict:
    if market is None:
        return Verdict.UNKNOWN
    for interval in market.intervals:
        if interval.start_ns <= time_ns < interval.end_ns:
            return interval.verdict
    return Verdict.UNKNOWN


def _solve_square(
    matrix: list[list[Fraction]], right: list[Fraction]
) -> list[Fraction] | None:
    size = len(matrix)
    augmented = [row + [value] for row, value in zip(matrix, right)]
    for column in range(size):
        pivot = next(
            (
                row
                for row in range(column, size)
                if augmented[row][column] != 0
            ),
            None,
        )
        if pivot is None:
            return None
        augmented[column], augmented[pivot] = augmented[pivot], augmented[column]
        divisor = augmented[column][column]
        augmented[column] = [value / divisor for value in augmented[column]]
        for row in range(size):
            if row == column:
                continue
            factor = augmented[row][column]
            if factor:
                augmented[row] = [
                    value - factor * pivot_value
                    for value, pivot_value in zip(
                        augmented[row], augmented[column]
                    )
                ]
    return [augmented[row][-1] for row in range(size)]


def _fraction_decimal(value: Fraction) -> Decimal:
    return Decimal(value.numerator) / Decimal(value.denominator)


def _power(value: Decimal, exponent: Decimal) -> Decimal:
    if exponent == exponent.to_integral_value():
        return value ** int(exponent)
    return Decimal(str(float(value) ** float(exponent)))


def _skew_stratum(value_ns: int) -> str:
    if value_ns < 5_000_000_000:
        return "lt_5s"
    if value_ns < 15_000_000_000:
        return "5_to_15s"
    if value_ns <= 60_000_000_000:
        return "15_to_60s"
    return "gt_60s"


def _percentage(numerator: int, denominator: int) -> str:
    return (
        f"{numerator * 100 / denominator:.6f}"
        if denominator
        else "0.000000"
    )


def _median_text(values: Sequence[Decimal]) -> str | None:
    return _text(statistics.median(values)) if values else None


def _max_text(values: Sequence[Decimal]) -> str | None:
    return _text(max(values)) if values else None


def _text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value.normalize(), "f")
