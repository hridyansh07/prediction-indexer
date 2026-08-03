"""Find contracts that express the same bet, and price the difference.

This is the measurement the whole project points at. Two markets are the *same
bet* when their masks are equal over the currently reachable outcomes — not
equal in general, but equal given where the event has got to. A Bo3 after the
home side takes map 1 is the canonical case:

    reachable            = {HH, HAH, HAA}
    "under 2.5 maps"     = {HH}
    "home wins map 2"    = {HH}
    "home -1.5 handicap" = {HH}

Three separate contracts, one bet. Whoever holds the conviction that the home
side sweeps can express it through whichever of the three is cheapest, and the
gap between them is an edge that requires the relationship graph to see. None of
these are the same contract, so a venue-level matcher cannot find them.

An instrument here is a tradable *position*, not a market: a Kalshi ticker
contributes YES and NO legs whose masks are complements, and each Polymarket
token contributes one leg.
"""

from __future__ import annotations

import statistics
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from analysis.masks import Mask


@dataclass(frozen=True)
class Instrument:
    """One tradable position with a mask and a price series."""

    instrument_id: str
    venue: str
    market_key: str
    market_type: str
    position: str
    outcome_label: str
    mask: frozenset[str]
    prices: Mapping[int, float]

    def restricted(self, universe: frozenset[str]) -> frozenset[str]:
        return self.mask & universe


def complement_mask(mask: frozenset[str], universe: frozenset[str]) -> frozenset[str]:
    return universe - mask


def equivalence_classes(
    instruments: Sequence[Instrument],
    universe: frozenset[str],
    *,
    minimum_routes: int = 2,
) -> list[list[Instrument]]:
    """Group instruments whose masks coincide over ``universe``.

    Masks that are empty or cover the whole universe are dropped: those are
    already-settled positions worth 0 or 1, and grouping them would swamp the
    output with trivially equal legs.
    """
    groups: dict[frozenset[str], list[Instrument]] = defaultdict(list)
    for instrument in instruments:
        restricted = instrument.restricted(universe)
        if not restricted or restricted == universe:
            continue
        groups[restricted].append(instrument)

    classes: list[list[Instrument]] = []
    for members in groups.values():
        distinct_markets = {member.market_key for member in members}
        if len(members) >= minimum_routes and len(distinct_markets) >= minimum_routes:
            classes.append(sorted(members, key=lambda item: item.instrument_id))
    return sorted(classes, key=lambda group: group[0].instrument_id)


def compare_routes(
    members: Sequence[Instrument],
    *,
    bar_range: tuple[int, int] | None = None,
) -> list[dict[str, Any]]:
    """Per-bar price spread across routes expressing the identical bet."""
    if len(members) < 2:
        return []
    common = set(members[0].prices)
    for member in members[1:]:
        common &= set(member.prices)
    if bar_range is not None:
        start, end = bar_range
        common = {bar for bar in common if start <= bar < end}

    rows: list[dict[str, Any]] = []
    for bar in sorted(common):
        quotes = {member.instrument_id: member.prices[bar] for member in members}
        cheapest = min(quotes, key=lambda key: quotes[key])
        dearest = max(quotes, key=lambda key: quotes[key])
        rows.append(
            {
                "bar_timestamp_seconds": bar,
                "prices": quotes,
                "cheapest_instrument": cheapest,
                "cheapest_price": quotes[cheapest],
                "dearest_instrument": dearest,
                "dearest_price": quotes[dearest],
                "spread": quotes[dearest] - quotes[cheapest],
            }
        )
    return rows


def summarize_routes(
    members: Sequence[Instrument],
    rows: Sequence[Mapping[str, Any]],
    *,
    state_prefix: str,
    universe_size: int,
) -> dict[str, Any] | None:
    """Collapse per-bar comparisons into one record per equivalence class."""
    if not rows:
        return None
    spreads = [float(row["spread"]) for row in rows]
    cheapest_counts: dict[str, int] = defaultdict(int)
    for row in rows:
        cheapest_counts[str(row["cheapest_instrument"])] += 1

    ordered = sorted(spreads)
    def quantile(fraction: float) -> float:
        if not ordered:
            return 0.0
        index = min(len(ordered) - 1, int(fraction * len(ordered)))
        return ordered[index]

    venues = sorted({member.venue for member in members})
    return {
        "state_prefix": state_prefix or "(pre-match)",
        "universe_size": universe_size,
        "venues": venues,
        # Kalshi candlesticks are a live top-of-book mid; Polymarket's CLOB
        # series is a traded price that goes stale in thin markets. A mixed
        # class therefore compares a quote against a possibly hours-old print,
        # which inflates the spread exactly the way sampling skew did in the
        # partition test. Single-venue classes are the clean comparison.
        "venue_composition": "single_venue" if len(venues) == 1 else "mixed_venue",
        "outcome_keys": sorted(members[0].restricted(frozenset(members[0].mask))),
        "route_count": len(members),
        "routes": [
            {
                "instrument_id": member.instrument_id,
                "venue": member.venue,
                "market_type": member.market_type,
                "outcome_label": member.outcome_label,
                "cheapest_bars": cheapest_counts.get(member.instrument_id, 0),
            }
            for member in members
        ],
        "bars": len(rows),
        "spread_median": statistics.median(spreads),
        "spread_mean": statistics.fmean(spreads),
        "spread_p90": quantile(0.90),
        "spread_max": max(spreads),
        "bars_with_spread_over_1c": sum(1 for value in spreads if value > 0.01),
        "bars_with_spread_over_2c": sum(1 for value in spreads if value > 0.02),
        "bars_with_spread_over_5c": sum(1 for value in spreads if value > 0.05),
        # Within-venue control. Every route here expresses the same bet, so a
        # single-venue subset compares two live quotes with no cross-venue
        # staleness in between. If the full-class spread is far wider than the
        # same-venue subset, the difference is a data artifact, not an edge.
        "subset_spreads": _subset_spreads(members, rows),
    }


def _subset_spreads(
    members: Sequence[Instrument],
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for venue in sorted({member.venue for member in members}):
        ids = [m.instrument_id for m in members if m.venue == venue]
        if len(ids) < 2:
            continue
        values = []
        for row in rows:
            quotes = [row["prices"][i] for i in ids if i in row["prices"]]
            if len(quotes) >= 2:
                values.append(max(quotes) - min(quotes))
        if values:
            out[venue] = {
                "routes": len(ids),
                "bars": len(values),
                "spread_median": statistics.median(values),
                "spread_max": max(values),
            }
    return out


def analyse_state_intervals(
    instruments: Sequence[Instrument],
    intervals: Sequence[tuple[str, frozenset[str], tuple[int, int] | None]],
    *,
    minimum_bars: int = 10,
) -> list[dict[str, Any]]:
    """Run the comparison over each state interval of an event.

    ``intervals`` is ``(state_prefix, reachable_universe, bar_range)``. The
    universe shrinks as the event progresses, which is what turns distinct
    masks into equivalent routes partway through.
    """
    results: list[dict[str, Any]] = []
    for prefix, universe, bar_range in intervals:
        for members in equivalence_classes(instruments, universe):
            rows = compare_routes(members, bar_range=bar_range)
            if len(rows) < minimum_bars:
                continue
            summary = summarize_routes(
                members,
                rows,
                state_prefix=prefix,
                universe_size=len(universe),
            )
            if summary is not None:
                summary["outcome_keys"] = sorted(members[0].restricted(universe))
                results.append(summary)
    return results
