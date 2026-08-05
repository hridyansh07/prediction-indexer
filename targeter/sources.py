"""Venue discovery: catalogue in, subscribable targets out.

One source per venue. A source knows how to ask a venue what exists and how to
decide which of it we want; it knows nothing about files, digests, scheduling, or
what a splice does with the answer.

**Every source returns its rejections.** A candidate that was considered and
dropped is data — without the reason, a selector bug looks exactly like a venue
not listing the market, and those have very different fixes. Sources therefore
return `(targets, rejections)` rather than just the targets, and the rejection is
never optional.

The selection rules here are the ones verified against each live catalogue, not
guesses: Limitless answers 400 above `limit=25`; Kalshi ladders live under
`event_ticker` and must be taken whole; Polymarket returns `clobTokenIds` as a
JSON-encoded string rather than an array.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Protocol

from targeter.coverage import created_at_of
from targeter.targets import Target, raw_resolution_evidence

USER_AGENT = "prediction-indexer/0.1"

KALSHI_API = "https://api.elections.kalshi.com/trade-api/v2"
POLYMARKET_GAMMA = "https://gamma-api.polymarket.com"
LIMITLESS_API = "https://api.limitless.exchange"

#: Limitless rejects a page size above this with HTTP 400, so widening the search
#: means paging rather than asking for more.
LIMITLESS_PAGE_LIMIT = 25

#: Slug fragments identifying each Limitless horizon. The venue encodes it in the
#: slug (`btc-up-or-down-5-min-<epoch>`), so there is nothing else to match on.
LIMITLESS_HORIZONS = {
    "5min": "-5-min-",
    "15min": "-15-min-",
    "hourly": "-hourly-",
    "daily": "-daily-",
}


class DiscoveryError(RuntimeError):
    """A venue could not be queried. Never fatal — one venue's outage is its own."""


@dataclass
class Discovery:
    """What one source found in one cycle."""

    venue: str
    targets: list[Target] = field(default_factory=list)
    rejections: list[dict[str, Any]] = field(default_factory=list)
    groups: list[dict[str, Any]] = field(default_factory=list)
    error: str | None = None
    #: `asset_id -> ISO-8601 creation time`, where the venue publishes one. Feeds
    #: the coverage ledger, which is the only way discovery lag becomes a measured
    #: number instead of an assumption.
    created_at: dict[str, str] = field(default_factory=dict)

    def reject(self, **detail: Any) -> None:
        self.rejections.append(detail)

    def note_created(self, asset_id: str, record: dict[str, Any]) -> None:
        stamp = created_at_of(record)
        if stamp is not None:
            self.created_at[asset_id] = stamp


class DiscoverySource(Protocol):
    venue: str

    def discover(self, selector: dict[str, Any]) -> Discovery: ...


def get_json(url: str, *, timeout: float = 30.0) -> Any:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.loads(response.read())
    except urllib.error.HTTPError as error:
        raise DiscoveryError(f"{url} -> HTTP {error.code}") from error
    except urllib.error.URLError as error:
        raise DiscoveryError(f"{url} -> {error.reason}") from error
    except json.JSONDecodeError as error:
        raise DiscoveryError(f"{url} -> response was not JSON: {error}") from error


# ---------------------------------------------------------------------------
# Kalshi
# ---------------------------------------------------------------------------


class KalshiSource:
    """Kalshi strike ladders. Needs no credentials — the catalogue is public.

    Selects whole ladders and never fragments. A Kalshi event such as
    `KXBTCD-26JUL2904` holds every strike for one settlement hour, and the P0
    experiment is a monotonicity test across that chain: P(BTC > 72,299) must
    never exceed P(BTC > 71,299). Half a ladder cannot answer that, so a cap
    truncates at an event boundary and drops the partial rather than subscribing
    to a fragment that costs a slot and answers nothing.
    """

    venue = "kalshi"

    def discover(self, selector: dict[str, Any]) -> Discovery:
        found = Discovery(venue=self.venue)
        series_tickers = list(selector.get("series") or [])
        max_events = int(selector.get("max_events_per_series", 1))
        min_strikes = int(selector.get("min_strikes", 5))
        max_targets = int(selector.get("max_targets", 400))

        for series_ticker in series_tickers:
            try:
                markets = self._open_markets(series_ticker)
            except DiscoveryError as error:
                # A failed series makes this selector incomplete. Returning an
                # ordinary rejection would let the targeter cache and publish the
                # remaining ladders as a successful result, unsubscribing the
                # failed series from a live splice.
                found.error = str(error)
                return found

            by_event: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for market in markets:
                event = market.get("event_ticker") or ""
                if not event:
                    found.reject(ticker=market.get("ticker"), reason="no event_ticker")
                elif _kalshi_strike(market) is None:
                    found.reject(ticker=market.get("ticker"), reason="no strike")
                else:
                    by_event[event].append(market)

            # Nearest expiry first: the live ladder is the one being priced, and
            # later hours are thin enough that a null result there says nothing.
            ordered = sorted(
                by_event.items(),
                key=lambda item: (min(str(m.get("close_time") or "") for m in item[1]), item[0]),
            )

            for event, event_markets in ordered[:max_events]:
                if len(event_markets) < min_strikes:
                    found.reject(event=event, reason=f"only {len(event_markets)} strikes")
                    continue
                if len(found.targets) + len(event_markets) > max_targets:
                    found.reject(
                        event=event,
                        reason=f"{len(event_markets)} strikes would exceed max_targets {max_targets}",
                    )
                    continue

                event_markets.sort(key=lambda market: _kalshi_strike(market) or 0.0)
                for market in event_markets:
                    found.note_created(str(market["ticker"]), market)
                    found.targets.append(
                        Target(
                            asset_id=str(market["ticker"]),
                            market_id=str(market.get("market_id") or "") or None,
                            condition_id=event,
                            note=f"{series_ticker} strike={_kalshi_strike(market)} "
                                 f"close={str(market.get('close_time') or '')[:19]}",
                            resolution=raw_resolution_evidence(self.venue, market),
                        )
                    )
                found.groups.append(
                    {
                        "series": series_ticker,
                        "event_ticker": event,
                        "members": len(event_markets),
                        "strike_min": _kalshi_strike(event_markets[0]),
                        "strike_max": _kalshi_strike(event_markets[-1]),
                        "close_time": event_markets[0].get("close_time"),
                    }
                )

            for event, _ in ordered[max_events:]:
                found.reject(event=event, reason="beyond max_events_per_series")

        return found

    def _open_markets(self, series_ticker: str) -> list[dict[str, Any]]:
        markets: list[dict[str, Any]] = []
        cursor = ""
        while True:
            suffix = f"&cursor={cursor}" if cursor else ""
            page = get_json(
                f"{KALSHI_API}/markets?series_ticker={series_ticker}&status=open&limit=1000{suffix}"
            )
            markets.extend(page.get("markets") or [])
            cursor = page.get("cursor") or ""
            if not cursor:
                return markets


def _kalshi_strike(market: dict[str, Any]) -> float | None:
    """Directional markets carry `floor_strike`; ranges carry a cap, floor, or both.

    The floor orders the ladder either way, and a market with neither is not a
    strike market at all.
    """
    for field_name in ("floor_strike", "cap_strike"):
        value = market.get(field_name)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return float(value)
    return None


# ---------------------------------------------------------------------------
# Polymarket
# ---------------------------------------------------------------------------


class PolymarketSource:
    venue = "polymarket"

    def discover(self, selector: dict[str, Any]) -> Discovery:
        found = Discovery(venue=self.venue)
        max_targets = int(selector.get("max_targets", 40))
        slug_contains = selector.get("slug_contains")
        min_liquidity = float(selector.get("min_liquidity", 0.0))

        params = [
            "closed=false",
            f"limit={int(selector.get('limit', 200))}",
            "order=volume24hr",
            "ascending=false",
        ]
        if selector.get("tag"):
            params.append(f"tag_slug={selector['tag']}")

        try:
            payload = get_json(f"{POLYMARKET_GAMMA}/markets?" + "&".join(params))
        except DiscoveryError as error:
            found.error = str(error)
            return found
        if isinstance(payload, dict):
            payload = payload.get("data") or payload.get("markets") or []
        markets = payload if isinstance(payload, list) else []

        for market in markets:
            slug = str(market.get("slug") or "")
            if slug_contains and slug_contains not in slug:
                found.reject(slug=slug, reason=f"slug does not contain {slug_contains!r}")
                continue
            try:
                liquidity = float(market.get("liquidityNum") or market.get("liquidity") or 0.0)
            except (TypeError, ValueError):
                liquidity = 0.0
            if liquidity < min_liquidity:
                found.reject(slug=slug, reason=f"liquidity {liquidity:.0f} below {min_liquidity:.0f}")
                continue
            tokens = _polymarket_tokens(market)
            if not tokens:
                found.reject(slug=slug, reason="no clobTokenIds")
                continue
            # A market's tokens are complements — taking one without the other
            # would leave the pair unpriceable, so the cap applies to the pair.
            if len(found.targets) + len(tokens) > max_targets:
                found.reject(slug=slug, reason=f"would exceed max_targets {max_targets}")
                continue

            for index, token in enumerate(tokens):
                found.note_created(token, market)
                found.targets.append(
                    Target(
                        asset_id=token,
                        market_id=str(market.get("id")) if market.get("id") is not None else None,
                        condition_id=str(market.get("conditionId") or "") or None,
                        note=f"{slug}#{index}",
                        resolution=raw_resolution_evidence(self.venue, market),
                    )
                )
            found.groups.append({"slug": slug, "members": len(tokens), "liquidity": liquidity})

        return found


def _polymarket_tokens(market: dict[str, Any]) -> list[str]:
    """Gamma returns `clobTokenIds` as a JSON-encoded string, not an array."""
    raw = market.get("clobTokenIds")
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return []
    return [str(value) for value in raw] if isinstance(raw, list) else []


# ---------------------------------------------------------------------------
# Limitless
# ---------------------------------------------------------------------------


class LimitlessSource:
    """Limitless subscribes by market *slug*, so `asset_id` holds a slug here.

    The splice does not know or care — it takes the identifiers it is given — but
    deciding what counts as one market is a targeter's job, so the difference
    lives on this side.
    """

    venue = "limitless"

    def discover(self, selector: dict[str, Any]) -> Discovery:
        found = Discovery(venue=self.venue)
        horizons = list(selector.get("horizons") or ["5min", "15min", "hourly"])
        wanted = tuple(LIMITLESS_HORIZONS[name] for name in horizons if name in LIMITLESS_HORIZONS)
        underlyings = tuple(str(u).lower() for u in (selector.get("underlyings") or []))
        pages = int(selector.get("pages", 4))
        max_targets = int(selector.get("max_targets", 60))

        markets: list[dict[str, Any]] = []
        for page in range(1, pages + 1):
            try:
                payload = get_json(
                    f"{LIMITLESS_API}/markets/active?page={page}&limit={LIMITLESS_PAGE_LIMIT}"
                )
            except DiscoveryError as error:
                # Every requested page participates in the selector. Page one
                # succeeding does not make pages 2..N optional: publishing those
                # partial results would silently narrow the subscription.
                found.error = str(error)
                return found
            if isinstance(payload, dict):
                payload = payload.get("data") or payload.get("markets") or []
            markets.extend(payload if isinstance(payload, list) else [])

        for market in markets:
            slug = str(market.get("slug") or "")
            if not slug:
                found.reject(reason="no slug")
                continue
            if wanted and not any(token in slug for token in wanted):
                found.reject(slug=slug, reason=f"horizon not in {horizons}")
                continue
            if underlyings and not any(slug.lower().startswith(u) for u in underlyings):
                found.reject(slug=slug, reason=f"underlying not in {list(underlyings)}")
                continue
            if str(market.get("tradeType") or "").lower() != "clob":
                # An AMM market has no order book, so `orderbookUpdate` never
                # arrives and it would sit in the subscription looking exactly
                # like a market that had gone quiet.
                found.reject(slug=slug, reason=f"tradeType={market.get('tradeType')!r} is not clob")
                continue
            if len(found.targets) >= max_targets:
                found.reject(slug=slug, reason=f"would exceed max_targets {max_targets}")
                continue

            found.note_created(slug, market)
            found.targets.append(
                Target(
                    asset_id=slug,
                    market_id=str(market.get("id")) if market.get("id") is not None else None,
                    condition_id=str(market.get("conditionId") or "") or None,
                    note=str(market.get("title") or "")[:120] or None,
                    resolution=raw_resolution_evidence(self.venue, market),
                )
            )

        return found


SOURCES: dict[str, DiscoverySource] = {
    "kalshi": KalshiSource(),
    "polymarket": PolymarketSource(),
    "limitless": LimitlessSource(),
}
