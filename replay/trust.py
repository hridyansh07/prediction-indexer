"""Venue-specific interval trust over normalized replay events."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from typing import Iterable

from replay.books import AnchorCheck
from replay.events import (
    BookDelta,
    ConnectionClosed,
    ConnectionOpened,
    FullBook,
    ReplayEvent,
)


class Verdict(str, Enum):
    TRUSTED = "TRUSTED"
    UNTRUSTED = "UNTRUSTED"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class TrustInterval:
    venue: str
    market_id: str
    start_ns: int
    end_ns: int
    verdict: Verdict
    reasons: tuple[str, ...]

    @property
    def duration_ns(self) -> int:
        return self.end_ns - self.start_ns

    def as_record(self) -> dict[str, object]:
        return {
            "venue": self.venue,
            "market_id": self.market_id,
            "start_ns": self.start_ns,
            "end_ns": self.end_ns,
            "duration_ns": self.duration_ns,
            "verdict": self.verdict.value,
            "reasons": list(self.reasons),
        }


@dataclass(frozen=True)
class MarketTrust:
    venue: str
    market_id: str
    asset_ids: tuple[str, ...]
    native_model: str
    intervals: tuple[TrustInterval, ...]
    anchor_matches: int
    anchor_total: int

    @property
    def total_ns(self) -> int:
        return sum(interval.duration_ns for interval in self.intervals)

    def duration(self, verdict: Verdict) -> int:
        return sum(
            interval.duration_ns
            for interval in self.intervals
            if interval.verdict == verdict
        )

    @property
    def trusted_percentage(self) -> str:
        return _percentage(self.duration(Verdict.TRUSTED), self.total_ns)

    @property
    def hash_match_percentage(self) -> str:
        return _percentage(self.anchor_matches, self.anchor_total)

    @property
    def hash_match_status(self) -> str:
        """`NOT_PRESENT` where the venue offers no hash, never a rate of zero.

        `_percentage(0, 0)` renders "0.000000", which reads as "every hash
        failed" for a venue that has no hash mechanism at all. Limitless has
        none. The top-level evidence already distinguishes these; the per-market
        record did not, so the same conflation the capture layer refuses
        everywhere — "not applicable" shown as "total failure" — survived into
        the report a reader actually scans.
        """
        return "MEASURED" if self.anchor_total else "NOT_PRESENT"

    def as_record(self) -> dict[str, object]:
        return {
            "venue": self.venue,
            "market_id": self.market_id,
            "asset_ids": list(self.asset_ids),
            "native_model": self.native_model,
            "duration_ns": {
                verdict.value.lower(): self.duration(verdict) for verdict in Verdict
            },
            "trusted_percentage": self.trusted_percentage,
            "polymarket_hash_match": {
                "status": self.hash_match_status,
                "matched": self.anchor_matches,
                "total": self.anchor_total,
                "rate_percentage": self.hash_match_percentage,
            },
            "intervals": [interval.as_record() for interval in self.intervals],
        }


@dataclass(frozen=True)
class TrustAudit:
    markets: tuple[MarketTrust, ...]
    polymarket_anchor_checks: tuple[AnchorCheck, ...]

    @property
    def intervals(self) -> tuple[TrustInterval, ...]:
        return tuple(
            interval for market in self.markets for interval in market.intervals
        )

    @property
    def polymarket_matches(self) -> int:
        return sum(check.matched for check in self.polymarket_anchor_checks)

    @property
    def polymarket_total(self) -> int:
        return len(self.polymarket_anchor_checks)

    @property
    def polymarket_hash_match_percentage(self) -> str:
        return _percentage(self.polymarket_matches, self.polymarket_total)


@dataclass(frozen=True)
class _Signal:
    time_ns: int
    priority: int
    action: str
    verdict: Verdict | None
    reason: str


def audit_trust(
    events: Iterable[ReplayEvent], anchor_checks: tuple[AnchorCheck, ...]
) -> TrustAudit:
    signals: dict[tuple[str, str], list[_Signal]] = defaultdict(list)
    asset_market: dict[tuple[str, str], str] = {}
    epoch_assets: dict[tuple[str, str, str], tuple[str, ...]] = {}
    limitless_versions: dict[str, int] = {}

    for event in events:
        if isinstance(event, ConnectionOpened):
            is_primary = (
                (event.venue == "polymarket" and event.delivers_deltas)
                or (event.venue == "limitless" and event.lane == "limitless")
            )
            if not is_primary:
                continue
            assets = tuple(event.asset_ids)
            epoch_assets[(event.venue, event.lane, event.epoch)] = assets
            for asset_id in assets:
                if event.venue == "limitless":
                    asset_market[(event.venue, asset_id)] = asset_id
                signals[(event.venue, asset_id)].append(
                    _Signal(
                        event.order_ns,
                        0,
                        "OPEN",
                        Verdict.UNKNOWN,
                        "connection_opened_without_snapshot_proof",
                    )
                )
            continue

        if isinstance(event, ConnectionClosed):
            assets = epoch_assets.get((event.venue, event.lane, event.epoch), ())
            for asset_id in assets:
                signals[(event.venue, asset_id)].append(
                    _Signal(
                        event.order_ns,
                        4,
                        "CLOSE",
                        None,
                        "connection_closed",
                    )
                )
            continue

        if isinstance(event, (FullBook, BookDelta)) and event.market_id is not None:
            asset_market[(event.venue, event.asset_id)] = event.market_id

        if isinstance(event, FullBook):
            key = (event.venue, event.asset_id)
            if event.venue == "polymarket" and not event.independent_snapshot:
                signals[key].append(
                    _Signal(
                        event.order_ns,
                        1,
                        "STATE",
                        Verdict.TRUSTED,
                        "venue_full_book",
                    )
                )
            elif event.venue == "limitless":
                signals[key].append(
                    _Signal(
                        event.order_ns,
                        1,
                        "STATE",
                        Verdict.UNKNOWN,
                        "full_book_exact_but_drop_completeness_unprovable",
                    )
                )
                version = _integer_version(event.source_version)
                previous = limitless_versions.get(event.asset_id)
                if version is not None:
                    if previous is not None and version < previous:
                        signals[key].append(
                            _Signal(
                                event.order_ns,
                                2,
                                "STATE",
                                Verdict.UNTRUSTED,
                                "limitless_version_not_monotonic",
                            )
                        )
                    limitless_versions[event.asset_id] = max(
                        version, previous if previous is not None else version
                    )

    checks_by_asset: dict[str, list[AnchorCheck]] = defaultdict(list)
    for check in anchor_checks:
        checks_by_asset[check.asset_id].append(check)
    for asset_id, checks in checks_by_asset.items():
        key = ("polymarket", asset_id)
        previous_proof_ns = min(
            (signal.time_ns for signal in signals.get(key, ())),
            default=min(check.snapshot_receive_ns for check in checks),
        )
        # Every instant at which the reconstruction was reset to venue truth. A
        # venue `book` frame overwrites the working book outright, so corruption
        # cannot predate the most recent one — walking back past it would condemn
        # a window the chain does not actually depend on.
        reanchors = sorted(
            signal.time_ns
            for signal in signals.get(key, ())
            if signal.reason == "venue_full_book"
        )
        for check in sorted(checks, key=lambda item: item.snapshot_receive_ns):
            if not check.matched:
                # An anchor compares a book *reconstructed from deltas* against an
                # independently polled one. A mismatch therefore says the chain was
                # already wrong when that state was reached, not that it went wrong
                # at the moment of detection — the deltas that produced it arrived
                # earlier and are exactly what is in doubt.
                #
                # So distrust runs from the last point the chain was proven, not
                # from the detection point. The optimistic reading left the window
                # between last proof and detection labelled TRUSTED, which is where
                # candidates are drawn from; better to understate the edge than to
                # count a candidate whose book cannot be vouched for.
                #
                # This walk-back is specific to delta-derived state. A snapshot-only
                # feed carries no chain — each frame stands alone, so a bad one
                # condemns itself and nothing before it. Limitless is handled that
                # way elsewhere and never reaches this branch.
                detection_ns = check.stream_order_ns
                candidates = [previous_proof_ns]
                if detection_ns is not None:
                    candidates.extend(
                        value for value in reanchors if value <= detection_ns
                    )
                failure_ns = max(candidates)
                if detection_ns is not None:
                    failure_ns = min(failure_ns, detection_ns)
                signals[key].append(
                    _Signal(
                        failure_ns,
                        # Highest priority so that at an exact tie with the proof
                        # it is anchored to, distrust wins. `failure_ns` is the
                        # instant of the last proof, and the proof is genuine —
                        # the book *was* right then — so the untrusted span is
                        # really (last_proof, recovery]. Intervals are built from
                        # signal points and cannot express a half-open start, so
                        # ordering after the proof at the same timestamp is how
                        # that is spelled. At priority 2 the verification sorted
                        # last instead and relabelled the whole window TRUSTED,
                        # which is the failure this change exists to remove.
                        5,
                        "STATE",
                        Verdict.UNTRUSTED,
                        check.reason,
                    )
                )
            signals[key].append(
                _Signal(
                    check.snapshot_receive_ns,
                    3,
                    "STATE",
                    Verdict.TRUSTED,
                    (
                        "independent_snapshot_recovery"
                        if not check.matched
                        else "independent_snapshot_verified"
                    ),
                )
            )
            previous_proof_ns = check.snapshot_receive_ns

    asset_intervals = {
        key: _build_asset_intervals(key[0], key[1], value)
        for key, value in signals.items()
    }
    market_assets: dict[tuple[str, str], list[str]] = defaultdict(list)
    for (venue, asset_id), intervals in asset_intervals.items():
        if not intervals:
            continue
        market_id = asset_market.get((venue, asset_id), f"asset:{asset_id}")
        market_assets[(venue, market_id)].append(asset_id)

    # Keyed by (venue, market_id) to match `market_assets`. Keyed by market alone,
    # a venue whose identifier happened to collide with a Polymarket condition id
    # would inherit its anchor counts and report a hash rate for a mechanism it
    # does not have.
    check_counts: dict[tuple[str, str], list[int]] = defaultdict(lambda: [0, 0])
    for check in anchor_checks:
        market_key = (
            "polymarket",
            asset_market.get(("polymarket", check.asset_id), f"asset:{check.asset_id}"),
        )
        check_counts[market_key][1] += 1
        check_counts[market_key][0] += int(check.matched)

    markets: list[MarketTrust] = []
    for (venue, market_id), assets in sorted(market_assets.items()):
        intervals = _combine_market(
            venue,
            market_id,
            tuple(sorted(set(assets))),
            asset_intervals,
        )
        counts = check_counts[(venue, market_id)]
        markets.append(
            MarketTrust(
                venue=venue,
                market_id=market_id,
                asset_ids=tuple(sorted(set(assets))),
                native_model=(
                    "polymarket_hash_plus_independent_snapshot_recovery"
                    if venue == "polymarket"
                    else "limitless_monotonic_version_without_drop_proof"
                ),
                intervals=intervals,
                anchor_matches=counts[0],
                anchor_total=counts[1],
            )
        )
    return TrustAudit(
        markets=tuple(markets),
        polymarket_anchor_checks=anchor_checks,
    )


def _build_asset_intervals(
    venue: str, asset_id: str, signals: list[_Signal]
) -> tuple[TrustInterval, ...]:
    del venue, asset_id
    ordered = sorted(signals, key=lambda item: (item.time_ns, item.priority, item.reason))
    intervals: list[TrustInterval] = []
    active = False
    start_ns: int | None = None
    verdict = Verdict.UNKNOWN
    reason = "not_observed"
    for signal in ordered:
        if signal.action == "OPEN":
            if active and start_ns is not None and signal.time_ns > start_ns:
                intervals.append(
                    _asset_interval(start_ns, signal.time_ns, verdict, reason)
                )
            active = True
            start_ns = signal.time_ns
            verdict = signal.verdict or Verdict.UNKNOWN
            reason = signal.reason
            continue
        if not active:
            active = True
            start_ns = signal.time_ns
        if signal.action == "CLOSE":
            if start_ns is not None and signal.time_ns > start_ns:
                intervals.append(
                    _asset_interval(start_ns, signal.time_ns, verdict, reason)
                )
            active = False
            start_ns = None
            continue
        if start_ns is not None and signal.time_ns > start_ns:
            intervals.append(_asset_interval(start_ns, signal.time_ns, verdict, reason))
        start_ns = signal.time_ns
        verdict = signal.verdict or Verdict.UNKNOWN
        reason = signal.reason
    return tuple(intervals)


def _asset_interval(
    start_ns: int, end_ns: int, verdict: Verdict, reason: str
) -> TrustInterval:
    # Venue and market are assigned during market aggregation.
    return TrustInterval("", "", start_ns, end_ns, verdict, (reason,))


def _combine_market(
    venue: str,
    market_id: str,
    assets: tuple[str, ...],
    asset_intervals: dict[tuple[str, str], tuple[TrustInterval, ...]],
) -> tuple[TrustInterval, ...]:
    per_asset = {asset: asset_intervals[(venue, asset)] for asset in assets}
    boundaries = sorted(
        {
            value
            for intervals in per_asset.values()
            for interval in intervals
            for value in (interval.start_ns, interval.end_ns)
        }
    )
    combined: list[TrustInterval] = []
    for start_ns, end_ns in zip(boundaries, boundaries[1:]):
        if start_ns == end_ns:
            continue
        verdicts: list[Verdict] = []
        reasons: set[str] = set()
        for asset_id, intervals in per_asset.items():
            current = next(
                (
                    interval
                    for interval in intervals
                    if interval.start_ns <= start_ns and interval.end_ns >= end_ns
                ),
                None,
            )
            if current is None:
                verdicts.append(Verdict.UNKNOWN)
                reasons.add(f"{asset_id}:not_captured")
            else:
                verdicts.append(current.verdict)
                reasons.update(f"{asset_id}:{reason}" for reason in current.reasons)
        verdict = (
            Verdict.UNTRUSTED
            if Verdict.UNTRUSTED in verdicts
            else Verdict.UNKNOWN
            if Verdict.UNKNOWN in verdicts
            else Verdict.TRUSTED
        )
        candidate = TrustInterval(
            venue=venue,
            market_id=market_id,
            start_ns=start_ns,
            end_ns=end_ns,
            verdict=verdict,
            reasons=tuple(sorted(reasons)),
        )
        if (
            combined
            and combined[-1].end_ns == candidate.start_ns
            and combined[-1].verdict == candidate.verdict
            and combined[-1].reasons == candidate.reasons
        ):
            previous = combined[-1]
            combined[-1] = TrustInterval(
                venue,
                market_id,
                previous.start_ns,
                candidate.end_ns,
                candidate.verdict,
                candidate.reasons,
            )
        else:
            combined.append(candidate)
    return tuple(combined)


def _integer_version(value: str | int | None) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _percentage(numerator: int, denominator: int) -> str:
    if denominator <= 0:
        return "0.000000"
    return f"{numerator * 100 / denominator:.6f}"
