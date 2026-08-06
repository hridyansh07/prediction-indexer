from __future__ import annotations

import fcntl
import hashlib
import http.client
import json
import os
import re
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Sequence

from archive.common.durable import fsync_directory
from analysis.storage import read_json_zstd, write_json_zstd
from encoder import CodecError, LogicalIdentity, StoredIdentity


DEFAULT_MIN_INTERVAL_SECONDS = {
    # Oddpool's free tier allows one request per second. The small buffer avoids
    # boundary jitter between our clock and the API gateway's clock.
    "api.oddpool.com": 1.10,
    "external-api.kalshi.com": 0.25,
    "gamma-api.polymarket.com": 0.25,
    "clob.polymarket.com": 0.25,
}


class HttpRequestError(RuntimeError):
    """Raised when a durable HTTP request does not return valid JSON."""


class TransientHttpError(HttpRequestError):
    """Raised for transport failures worth retrying: timeouts, resets, DNS.

    Kept distinct from ``HttpRequestError`` so retry policy is a type check
    rather than a match on the message text.
    """


@dataclass(frozen=True)
class JsonResponse:
    data: Any
    url: str
    cache_path: Path
    from_cache: bool
    fetched_at: str


Transport = Callable[
    [urllib.request.Request, float],
    tuple[int, Mapping[str, str], bytes],
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        dir=path.parent,
        prefix=f".{path.name}.",
        delete=False,
    ) as handle:
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())
        temporary_path = Path(handle.name)
    os.replace(temporary_path, path)
    fsync_directory(path.parent)


@contextmanager
def _cache_entry_lock(response_path: Path) -> Iterator[None]:
    """Serialize readers and two-file commits for one response cache key."""
    lock_path = response_path.with_suffix(".cache.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def _default_transport(
    request: urllib.request.Request,
    timeout_seconds: float,
) -> tuple[int, Mapping[str, str], bytes]:
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            return response.status, dict(response.headers.items()), response.read()
    except urllib.error.HTTPError as error:
        body = error.read().decode("utf-8", errors="replace")
        raise HttpRequestError(
            f"HTTP {error.code} for {request.full_url}: {body[:500]}"
        ) from error
    except urllib.error.URLError as error:
        raise TransientHttpError(
            f"Network error for {request.full_url}: {error.reason}"
        ) from error


class DurableJsonClient:
    """JSON HTTP client with content-addressed caching and durable rate limits."""

    def __init__(
        self,
        cache_root: Path,
        *,
        force_refresh: bool = False,
        persist_responses: bool = True,
        compress_responses: bool = False,
        timeout_seconds: float = 30.0,
        min_interval_seconds: Mapping[str, float] | None = None,
        transport: Transport | None = None,
        user_agent: str = "prediction-indexer/0.1",
    ) -> None:
        self.cache_root = Path(cache_root)
        self.force_refresh = force_refresh
        self.persist_responses = persist_responses
        self.compress_responses = compress_responses
        self.timeout_seconds = timeout_seconds
        self.min_interval_seconds = {
            **DEFAULT_MIN_INTERVAL_SECONDS,
            **(min_interval_seconds or {}),
        }
        self.transport = transport or _default_transport
        self.user_agent = user_agent
        self.cache_hits = 0
        self.network_requests = 0

    @staticmethod
    def build_url(
        base_url: str,
        path: str,
        params: Mapping[str, Any] | None = None,
    ) -> str:
        url = urllib.parse.urljoin(base_url.rstrip("/") + "/", path.lstrip("/"))
        if not params:
            return url

        query_items: list[tuple[str, str]] = []
        for key in sorted(params):
            value = params[key]
            if value is None:
                continue
            values: Sequence[Any]
            if isinstance(value, (list, tuple)):
                values = value
            else:
                values = (value,)
            for item in values:
                if isinstance(item, bool):
                    encoded = "true" if item else "false"
                else:
                    encoded = str(item)
                query_items.append((key, encoded))
        return f"{url}?{urllib.parse.urlencode(query_items)}"

    def get_json(
        self,
        base_url: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> JsonResponse:
        url = self.build_url(base_url, path, params)
        parsed = urllib.parse.urlparse(url)
        host = parsed.hostname or "unknown-host"
        request_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()
        host_directory = re.sub(r"[^a-zA-Z0-9._-]", "_", host)
        response_suffix = ".json.zst" if self.compress_responses else ".json"
        response_path = self.cache_root / host_directory / f"{request_hash}{response_suffix}"
        metadata_path = response_path.with_suffix(".meta.json")
        cache_miss = object()

        if self.persist_responses and not self.force_refresh:
            with _cache_entry_lock(response_path):
                if response_path.exists():
                    try:
                        metadata = (
                            json.loads(metadata_path.read_text(encoding="utf-8"))
                            if metadata_path.exists()
                            else {}
                        )
                    except (OSError, json.JSONDecodeError):
                        metadata = {}
                    if self.compress_responses:
                        try:
                            data = read_json_zstd(
                                response_path,
                                expected_logical=LogicalIdentity.from_record(
                                    metadata.get("decoded")
                                ),
                                expected_stored=StoredIdentity.from_record(
                                    metadata.get("stored")
                                ),
                            )
                        except (CodecError, OSError, ValueError):
                            # Body plus metadata is one cache commit. An
                            # interrupted or mismatched pair is an uncommitted
                            # cache miss, not a durable discovery failure.
                            data = cache_miss
                    else:
                        data = json.loads(response_path.read_text(encoding="utf-8"))
                    if data is not cache_miss:
                        self.cache_hits += 1
                        return JsonResponse(
                            data=data,
                            url=url,
                            cache_path=response_path,
                            from_cache=True,
                            fetched_at=metadata.get("fetched_at", "unknown"),
                        )

        request_headers = {
            "Accept": "application/json",
            "User-Agent": self.user_agent,
            **(headers or {}),
        }
        request = urllib.request.Request(url, headers=request_headers)

        with self._request_slot(host):
            status, response_headers, body = self.transport(
                request,
                self.timeout_seconds,
            )
            self.network_requests += 1

        if status < 200 or status >= 300:
            raise HttpRequestError(f"HTTP {status} for {url}")

        try:
            data = json.loads(body)
        except json.JSONDecodeError as error:
            raise HttpRequestError(f"Invalid JSON returned by {url}") from error

        fetched_at = _utc_now()
        if self.persist_responses:
            with _cache_entry_lock(response_path):
                cache_identity = None
                if self.compress_responses:
                    cache_identity = write_json_zstd(response_path, data)
                else:
                    _atomic_write(
                        response_path,
                        json.dumps(
                            data,
                            ensure_ascii=False,
                            separators=(",", ":"),
                        ).encode("utf-8"),
                    )
                metadata = {
                    "url": url,
                    "fetched_at": fetched_at,
                    "status": status,
                    "response_headers": dict(response_headers),
                }
                if cache_identity is not None:
                    metadata.update(
                        {
                            "content_encoding": "zstd",
                            "decoded": cache_identity.logical.as_record(),
                            "stored": cache_identity.stored.as_record(),
                        }
                    )
                _atomic_write(
                    metadata_path,
                    json.dumps(
                        metadata,
                        ensure_ascii=False,
                        indent=2,
                        sort_keys=True,
                    ).encode("utf-8"),
                )
        return JsonResponse(
            data=data,
            url=url,
            cache_path=response_path,
            from_cache=False,
            fetched_at=fetched_at,
        )

    @contextmanager
    def _request_slot(self, host: str) -> Iterator[None]:
        minimum_interval = max(0.0, self.min_interval_seconds.get(host, 0.0))
        rate_directory = self.cache_root / "_rate_limits"
        rate_directory.mkdir(parents=True, exist_ok=True)
        safe_host = re.sub(r"[^a-zA-Z0-9._-]", "_", host)
        lock_path = rate_directory / f"{safe_host}.lock"
        state_path = rate_directory / f"{safe_host}.json"

        with lock_path.open("a+", encoding="utf-8") as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            last_started_at = 0.0
            if state_path.exists():
                try:
                    state = json.loads(state_path.read_text(encoding="utf-8"))
                    last_started_at = float(state.get("last_started_at", 0.0))
                except (ValueError, json.JSONDecodeError):
                    last_started_at = 0.0

            delay = minimum_interval - (time.time() - last_started_at)
            if delay > 0:
                time.sleep(delay)

            started_at = time.time()
            _atomic_write(
                state_path,
                json.dumps(
                    {
                        "host": host,
                        "last_started_at": started_at,
                        "minimum_interval_seconds": minimum_interval,
                    },
                    indent=2,
                    sort_keys=True,
                ).encode("utf-8"),
            )
            try:
                yield
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


class RetryingJsonClient(DurableJsonClient):
    """Durable client that retries transient transport and rate-limit failures.

    Large historical payloads (5,000 candlesticks, 1,000 trades) occasionally
    exceed the socket read timeout, and venues rate-limit under sustained
    pulls. Both are transient: the cache makes a retry free when the earlier
    attempt actually succeeded, and checkpoints make it free when it did not.
    """

    def __init__(
        self,
        cache_root: Path,
        *,
        transient_retries: int = 5,
        backoff_seconds: float = 5.0,
        maximum_backoff_seconds: float = 30.0,
        **kwargs: Any,
    ) -> None:
        super().__init__(cache_root, **kwargs)
        self.transient_retries = transient_retries
        self.backoff_seconds = backoff_seconds
        self.maximum_backoff_seconds = maximum_backoff_seconds
        self.transient_retry_attempts = 0

    @staticmethod
    def _is_retryable(error: BaseException) -> bool:
        if isinstance(error, TransientHttpError):
            return True
        if isinstance(error, HttpRequestError):
            # A 4xx other than 429 means the request itself is wrong, so
            # retrying only burns budget. Rate limits and 5xx are transient.
            message = str(error)
            return any(
                marker in message
                for marker in ("HTTP 429", "HTTP 500", "HTTP 502", "HTTP 503", "HTTP 504")
            )
        return True

    def get_json(self, *args: Any, **kwargs: Any) -> JsonResponse:
        retry_number = 0
        while True:
            try:
                return super().get_json(*args, **kwargs)
            except (
                HttpRequestError,
                http.client.IncompleteRead,
                TimeoutError,
                OSError,
            ) as error:
                if (
                    not self._is_retryable(error)
                    or retry_number >= self.transient_retries
                ):
                    raise
                retry_number += 1
                self.transient_retry_attempts += 1
                time.sleep(
                    min(
                        self.maximum_backoff_seconds,
                        self.backoff_seconds * (2 ** (retry_number - 1)),
                    )
                )
