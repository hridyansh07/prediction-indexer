"""Response and freshness limits, read as module attributes so tests can patch them."""

from __future__ import annotations


STALE_AFTER_SECONDS = 3_600
TARGETER_RUN_INTERVAL_SECONDS = 600
EVENT_UNIVERSE_RESPONSE_BUDGET_BYTES = 1_750_000
DETAIL_ROW_LIMIT = 1000
# A bundle context is bounded by the response byte budget. This row bound only
# caps how many rows one read loads: a context child row serializes to at least
# about 90 bytes, so 20,000 rows cannot fit the budget anyway.
CONTEXT_ROW_LIMIT = 20_000
