"""Public Targeter v2 sports adapter API."""

from __future__ import annotations

from typing import Any

from targeter.v2.registry import MarketClassRegistry, Strategy

from .clients import durable_client
from .kalshi import KalshiSportsAdapter
from .limitless import LimitlessSportsAdapter
from .polymarket import PolymarketSportsAdapter


def live_adapters(
    strategy: Strategy,
    *,
    max_kalshi_series: int | None = None,
    max_kalshi_pages: int | None = None,
    max_polymarket_pages: int | None = None,
    max_limitless_pages: int | None = None,
) -> tuple[Any, ...]:
    registry = MarketClassRegistry(strategy)
    return (
        KalshiSportsAdapter(
            registry,
            max_series=max_kalshi_series,
            max_pages=max_kalshi_pages,
        ),
        PolymarketSportsAdapter(
            registry,
            max_pages_per_tag=max_polymarket_pages,
        ),
        LimitlessSportsAdapter(registry, max_pages=max_limitless_pages),
    )


__all__ = [
    "KalshiSportsAdapter",
    "PolymarketSportsAdapter",
    "LimitlessSportsAdapter",
    "durable_client",
    "live_adapters",
]
