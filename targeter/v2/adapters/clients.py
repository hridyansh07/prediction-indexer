"""Construction of durable HTTP clients for live adapters."""

from __future__ import annotations

from typing import Any

from analysis.durable_http import DurableJsonClient, RetryingJsonClient


def durable_client(
    cache_root: Any,
    *,
    force_refresh: bool = False,
    persist_responses: bool = True,
) -> DurableJsonClient:
    return RetryingJsonClient(
        cache_root,
        force_refresh=force_refresh,
        persist_responses=persist_responses,
        compress_responses=True,
    )
