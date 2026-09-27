"""Bounded in-process rate limiting for requests received through Caddy."""

from __future__ import annotations

import ipaddress
import math
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Callable

from universe.auth import AuthStore
from universe.config import RateLimitConfig


@dataclass
class _Bucket:
    tokens: float
    updated_at: float


class RateLimiter:
    def __init__(
        self,
        config: RateLimitConfig,
        auth: AuthStore,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._config = config
        self._auth = auth
        self._clock = clock
        self._trusted = frozenset(config.trusted_proxy_addresses)
        self._buckets: OrderedDict[str, _Bucket] = OrderedDict()
        self._lock = threading.Lock()

    def retry_after(self, peer_address: str, headers: Any) -> int | None:
        """Return seconds to wait, or None when this request may proceed.

        Direct internal callers are exempt. Forwarded addresses are considered
        only for an exact trusted proxy peer, and only the final address Caddy
        appended is used.
        """
        try:
            peer = str(ipaddress.ip_address(peer_address))
        except ValueError:
            return None
        if peer not in self._trusted:
            return None

        session = self._auth.authenticated_session_key(headers)
        if session is None:
            key = "ip:" + _last_forwarded_address(headers, fallback=peer)
            capacity = self._config.unauthenticated_requests
            window = self._config.unauthenticated_window_seconds
        else:
            key = "session:" + session
            capacity = self._config.authenticated_requests
            window = self._config.authenticated_window_seconds

        now = self._clock()
        with self._lock:
            bucket = self._buckets.pop(key, None)
            if bucket is None:
                if len(self._buckets) >= self._config.max_buckets:
                    self._buckets.popitem(last=False)
                bucket = _Bucket(float(capacity), now)
            else:
                elapsed = max(0.0, now - bucket.updated_at)
                bucket.tokens = min(
                    float(capacity),
                    bucket.tokens + elapsed * capacity / window,
                )
                bucket.updated_at = now
            self._buckets[key] = bucket
            if bucket.tokens >= 1.0:
                bucket.tokens -= 1.0
                return None
            seconds = (1.0 - bucket.tokens) * window / capacity
            return max(1, math.ceil(seconds))

    def __len__(self) -> int:
        with self._lock:
            return len(self._buckets)


def _last_forwarded_address(headers: Any, *, fallback: str) -> str:
    values = headers.get_all("X-Forwarded-For", []) if hasattr(headers, "get_all") else []
    if not values and isinstance(headers, dict):
        values = [
            value
            for key, value in headers.items()
            if str(key).lower() == "x-forwarded-for"
        ]
    if not values or not isinstance(values[-1], str):
        return fallback
    candidate = values[-1].rsplit(",", 1)[-1].strip()
    try:
        return str(ipaddress.ip_address(candidate))
    except ValueError:
        return fallback
