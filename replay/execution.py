"""Displayed-depth lifetimes and a deliberately non-fill execution estimator."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Iterable, Sequence

from replay.books import ReplayedBookState
from replay.catalog import MetadataCatalogue
from replay.economics import HEADLINE_SIZE, EconomicAudit, walk_ladder
from replay.trust import MarketTrust, TrustAudit, Verdict

ESTIMATOR_NAME = "DISPLAYED_DEPTH_SURVIVAL_100MS"
MINIMUM_SURVIVAL_NS = 100_000_000


@dataclass(frozen=True)
class DepthEpisode:
    venue: str
    market_id: str
    asset_id: str
    direction: str
    size_contracts: int
    start_ns: int
    end_ns: int
    vwap: Decimal | None
    depth_limited: bool
    fingerprint: tuple[tuple[str, str], ...]
    trust_verdict: Verdict
    right_censored: bool

    @property
    def lifetime_ns(self) -> int:
        return self.end_ns - self.start_ns

    def as_record(self) -> dict[str, object]:
        return {
            "venue": self.venue,
            "market_id": self.market_id,
            "asset_id": self.asset_id,
            "direction": self.direction,
            "size_contracts": self.size_contracts,
            "start_ns": self.start_ns,
            "end_ns": self.end_ns,
            "lifetime_ns": self.lifetime_ns,
            "vwap": _text(self.vwap),
            "depth_limited": self.depth_limited,
            "trust_verdict": self.trust_verdict.value,
            "right_censored": self.right_censored,
        }


@dataclass(frozen=True)
class ExecutionAudit:
    episodes: tuple[DepthEpisode, ...]
    lifetime_summary: tuple[dict[str, Any], ...]
    candidate_estimates: tuple[dict[str, Any], ...]
    detected_candidates: int


@dataclass
class _OpenEpisode:
    state: ReplayedBookState
    direction: str
    start_ns: int
    vwap: Decimal | None
    depth_limited: bool
    fingerprint: tuple[tuple[str, str], ...]


def audit_execution(
    states: Iterable[ReplayedBookState],
    catalogue: MetadataCatalogue,
    trust: TrustAudit,
    economics: EconomicAudit,
    *,
    size_contracts: int = HEADLINE_SIZE,
) -> ExecutionAudit:
    active: dict[tuple[str, str, str], _OpenEpisode] = {}
    raw_episodes: list[DepthEpisode] = []
    last_seen: dict[tuple[str, str], int] = {}
    market_trust = {
        (market.venue, market.market_id): market for market in trust.markets
    }

    for state in states:
        metadata = catalogue.by_asset(state.venue, state.asset_id)
        scale = metadata.size_scale if metadata is not None else Decimal(1)
        last_seen[(state.venue, state.market_id)] = max(
            state.order_ns,
            last_seen.get((state.venue, state.market_id), state.order_ns),
        )
        for direction, levels in (
            ("long", state.asks),
            ("short", state.bids),
        ):
            normalized = tuple(
                (level.price, level.size / scale) for level in levels
            )
            fill = walk_ladder(normalized, Decimal(size_contracts))
            fingerprint = tuple(
                (_text(price) or "", _text(quantity) or "")
                for price, quantity in fill.levels
            )
            key = (state.venue, state.asset_id, direction)
            previous = active.get(key)
            if previous is not None and (
                previous.fingerprint != fingerprint
                or previous.depth_limited != fill.depth_limited
            ):
                if state.order_ns > previous.start_ns:
                    raw_episodes.append(
                        _close_episode(
                            previous,
                            state.order_ns,
                            Verdict.UNKNOWN,
                            right_censored=False,
                            size_contracts=size_contracts,
                        )
                    )
                previous = None
            if previous is None:
                active[key] = _OpenEpisode(
                    state=state,
                    direction=direction,
                    start_ns=state.order_ns,
                    vwap=fill.vwap,
                    depth_limited=fill.depth_limited,
                    fingerprint=fingerprint,
                )

    for value in active.values():
        trust_model = market_trust.get(
            (value.state.venue, value.state.market_id)
        )
        end_ns = (
            trust_model.intervals[-1].end_ns
            if trust_model is not None and trust_model.intervals
            else last_seen.get(
                (value.state.venue, value.state.market_id),
                value.start_ns,
            )
        )
        if end_ns > value.start_ns:
            raw_episodes.append(
                _close_episode(
                    value,
                    end_ns,
                    Verdict.UNKNOWN,
                    right_censored=True,
                    size_contracts=size_contracts,
                )
            )

    episodes = tuple(
        part
        for episode in raw_episodes
        for part in _split_by_trust(
            episode,
            market_trust.get((episode.venue, episode.market_id)),
        )
    )
    candidate_rows = [
        row
        for row in economics.rows
        if row["direction"] == "long"
        and row["size_contracts"] == size_contracts
        and row["headline_eligible"]
        and row["net_gap_conservative_per_contract"] is not None
        and Decimal(row["net_gap_conservative_per_contract"]) > 0
    ]
    estimates = estimate_candidates(candidate_rows, episodes)
    return ExecutionAudit(
        episodes=episodes,
        lifetime_summary=_summaries(episodes),
        candidate_estimates=tuple(estimates),
        detected_candidates=len(candidate_rows),
    )


def estimate_candidates(
    candidates: Sequence[dict[str, Any]],
    episodes: Sequence[DepthEpisode],
) -> list[dict[str, Any]]:
    by_asset: dict[str, list[DepthEpisode]] = {}
    for episode in episodes:
        if (
            episode.direction == "long"
            and episode.size_contracts == HEADLINE_SIZE
        ):
            by_asset.setdefault(episode.asset_id, []).append(episode)
    estimates: list[dict[str, Any]] = []
    for row in candidates:
        detection_ns = int(row["observation_ns"])
        matched: list[DepthEpisode] = []
        for asset_id in row["leg_asset_ids"]:
            episode = next(
                (
                    item
                    for item in by_asset.get(asset_id, ())
                    if item.start_ns <= detection_ns < item.end_ns
                    and item.trust_verdict == Verdict.TRUSTED
                    and not item.depth_limited
                ),
                None,
            )
            if episode is None:
                matched = []
                break
            matched.append(episode)
        if matched:
            remaining_ns = min(
                episode.end_ns - detection_ns for episode in matched
            )
            status = (
                "DISPLAYED_DEPTH_SURVIVED_MINIMUM"
                if remaining_ns >= MINIMUM_SURVIVAL_NS
                else "DISPLAYED_DEPTH_DID_NOT_SURVIVE_MINIMUM"
            )
        else:
            remaining_ns = None
            status = "NO_MATCHING_TRUSTED_DEPTH_EPISODE"
        estimates.append(
            {
                "basket_id": row["basket_id"],
                "market_id": row["market_id"],
                "observation_ns": detection_ns,
                "size_contracts": row["size_contracts"],
                "net_gap_conservative_per_contract": row[
                    "net_gap_conservative_per_contract"
                ],
                "estimator": ESTIMATOR_NAME,
                "minimum_survival_ns": MINIMUM_SURVIVAL_NS,
                "observed_remaining_lifetime_ns": remaining_ns,
                "status": status,
                "not_a_fill_claim": True,
            }
        )
    return estimates


def _close_episode(
    value: _OpenEpisode,
    end_ns: int,
    verdict: Verdict,
    *,
    right_censored: bool,
    size_contracts: int,
) -> DepthEpisode:
    return DepthEpisode(
        venue=value.state.venue,
        market_id=value.state.market_id,
        asset_id=value.state.asset_id,
        direction=value.direction,
        size_contracts=size_contracts,
        start_ns=value.start_ns,
        end_ns=end_ns,
        vwap=value.vwap,
        depth_limited=value.depth_limited,
        fingerprint=value.fingerprint,
        trust_verdict=verdict,
        right_censored=right_censored,
    )


def _split_by_trust(
    episode: DepthEpisode, market: MarketTrust | None
) -> tuple[DepthEpisode, ...]:
    if market is None:
        return (episode,)
    output: list[DepthEpisode] = []
    covered: list[tuple[int, int]] = []
    for interval in market.intervals:
        start_ns = max(episode.start_ns, interval.start_ns)
        end_ns = min(episode.end_ns, interval.end_ns)
        if end_ns <= start_ns:
            continue
        covered.append((start_ns, end_ns))
        output.append(
            DepthEpisode(
                venue=episode.venue,
                market_id=episode.market_id,
                asset_id=episode.asset_id,
                direction=episode.direction,
                size_contracts=episode.size_contracts,
                start_ns=start_ns,
                end_ns=end_ns,
                vwap=episode.vwap,
                depth_limited=episode.depth_limited,
                fingerprint=episode.fingerprint,
                trust_verdict=interval.verdict,
                right_censored=(
                    episode.right_censored and end_ns == episode.end_ns
                ),
            )
        )
    if not output:
        return (episode,)
    return tuple(output)


def _summaries(
    episodes: Sequence[DepthEpisode],
) -> tuple[dict[str, Any], ...]:
    groups: dict[tuple[str, str, str], list[DepthEpisode]] = {}
    for episode in episodes:
        groups.setdefault(
            (
                episode.venue,
                episode.direction,
                episode.trust_verdict.value,
            ),
            [],
        ).append(episode)
    output: list[dict[str, Any]] = []
    for (venue, direction, verdict), values in sorted(groups.items()):
        observed = [
            item.lifetime_ns
            for item in values
            if not item.right_censored and item.lifetime_ns > 0
        ]
        output.append(
            {
                "venue": venue,
                "direction": direction,
                "trust_verdict": verdict,
                "size_contracts": values[0].size_contracts,
                "episodes": len(values),
                "uncensored_episodes": len(observed),
                "right_censored_episodes": sum(
                    item.right_censored for item in values
                ),
                "p50_lifetime_ns": _quantile(observed, 0.50),
                "p90_lifetime_ns": _quantile(observed, 0.90),
                "p99_lifetime_ns": _quantile(observed, 0.99),
            }
        )
    return tuple(output)


def _quantile(values: Sequence[int], probability: float) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int(probability * len(ordered) - 1e-12)))
    return ordered[index]


def _text(value: Decimal | None) -> str | None:
    return format(value.normalize(), "f") if value is not None else None
