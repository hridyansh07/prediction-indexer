from __future__ import annotations

from pathlib import Path
from typing import Any

from analysis.durable_http import RetryingJsonClient


class RetryingOddpoolClient(RetryingJsonClient):
    """Durable client with conservative Oddpool pacing and transient-error backoff."""

    def __init__(
        self,
        cache_root: Path,
        *,
        rate_limit_retries: int = 5,
        **kwargs: Any,
    ) -> None:
        minimum_intervals = {
            "api.oddpool.com": 2.0,
            **(kwargs.pop("min_interval_seconds", {}) or {}),
        }
        super().__init__(
            cache_root,
            min_interval_seconds=minimum_intervals,
            transient_retries=rate_limit_retries,
            **kwargs,
        )

    @property
    def rate_limit_retries(self) -> int:
        return self.transient_retries

    @property
    def rate_limit_retry_attempts(self) -> int:
        return self.transient_retry_attempts
