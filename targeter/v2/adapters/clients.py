"""Construction of durable HTTP clients for live adapters."""

from __future__ import annotations

from typing import Any
from urllib.request import Request

import requests

from analysis.durable_http import (
    HttpRequestError,
    RetryingJsonClient,
    TransientHttpError,
)


class _PooledJsonClient(RetryingJsonClient):
    """One venue worker's persistent, compression-aware HTTP session.

    The durable client still owns caching, request slots and retries. The
    session performs no hidden retries and is never shared between workers.
    HTTP content encoding is independent of the persisted Zstandard format.
    """

    def __init__(self, cache_root: Any, **kwargs: Any) -> None:
        self._session = requests.Session()
        self._session.headers["Accept-Encoding"] = "gzip, deflate"
        super().__init__(cache_root, transport=self._transport, **kwargs)

    def _transport(
        self, request: Request, timeout_seconds: float
    ) -> tuple[int, dict[str, str], bytes]:
        try:
            with self._session.get(
                request.full_url,
                headers=dict(request.header_items()),
                timeout=timeout_seconds,
            ) as response:
                body = response.content
                if not 200 <= response.status_code < 300:
                    detail = body.decode("utf-8", errors="replace")[:500]
                    raise HttpRequestError(
                        f"HTTP {response.status_code} for {request.full_url}: {detail}"
                    )
                return response.status_code, dict(response.headers), body
        except requests.RequestException as error:
            raise TransientHttpError(
                f"Network error for {request.full_url}: {error}"
            ) from error

    def close(self) -> None:
        self._session.close()


def durable_client(
    cache_root: Any,
    *,
    force_refresh: bool = False,
    persist_responses: bool = True,
) -> _PooledJsonClient:
    return _PooledJsonClient(
        cache_root,
        force_refresh=force_refresh,
        persist_responses=persist_responses,
        compress_responses=True,
    )
