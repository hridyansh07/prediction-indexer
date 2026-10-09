"""Response and freshness limits, read as module attributes so tests can patch them."""

from __future__ import annotations


STALE_AFTER_SECONDS = 3_600
TARGETER_RUN_INTERVAL_SECONDS = 600
EVENT_UNIVERSE_RESPONSE_BUDGET_BYTES = 1_750_000
DETAIL_ROW_LIMIT = 1000
