from __future__ import annotations

import asyncio
import heapq
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any

from .config import REST_BASE, settings


class PaginationLimitExceeded(RuntimeError):
    def __init__(self, rows: list[dict[str, Any]], cursor: str, pages: int) -> None:
        self.rows = rows
        self.cursor = cursor
        self.pages = pages
        super().__init__(f"Pagination exceeded configured maximum of {pages} pages")


class PublicKalshiAPI:
    """Public, GET-only Kalshi API client with one shared request budget."""

    def __init__(self, min_interval: float | None = None) -> None:
        self.min_interval = (
            settings.rest_min_interval_seconds
            if min_interval is None
            else min_interval
        )
        self._next_request_at = 0.0
        self._lock = asyncio.Lock()
        self._gate = asyncio.Condition()
        self._request_ticket = 0
        self._waiting_requests: list[tuple[int, int, object]] = []
        self.requests = 0
        self.attempts = 0
        self.errors = 0
        self.rate_limit_responses = 0
        self.rate_limit_backoff_count = 0
        self.rate_limit_backoff_seconds = 0.0
        self.rate_limit_backoff_scheduled_seconds = 0.0
        self.last_rate_limit_path: str | None = None
        self.last_rate_limit_utc: str | None = None
        self._rate_limit_backoff_level = 0
        self._last_rate_limit_monotonic: float | None = None
        self.last_error: str | None = None
        self.last_request_rtt_ms: float | None = None

    @staticmethod
    def _priority(path: str, params: dict[str, Any] | None) -> int:
        """Static GET-only scheduling: prices/opening first, bulk inventory last."""
        if "orderbook" in path:
            return 0
        if path == "/markets" and params and params.get("tickers"):
            return 1
        if path == "/markets" and params and params.get("status") in {"unopened", "open", "active"}:
            return 2
        if path == "/markets/trades":
            return 3
        return 5

    async def _throttle(self, priority: int = 5) -> None:
        # Preserve the observed 2-second rate budget while allowing a request
        # that arrived later to pass bulk inventory work waiting in the queue.
        marker = object()
        async with self._gate:
            self._request_ticket += 1
            ticket = (priority, self._request_ticket, marker)
            heapq.heappush(self._waiting_requests, ticket)
            try:
                while True:
                    if self._waiting_requests[0] != ticket:
                        await self._gate.wait()
                        continue
                    remaining = self._next_request_at - time.monotonic()
                    if remaining > 0:
                        try:
                            await asyncio.wait_for(self._gate.wait(), timeout=remaining)
                        except TimeoutError:
                            pass
                        continue
                    heapq.heappop(self._waiting_requests)
                    self._next_request_at = time.monotonic() + self.min_interval
                    self._gate.notify_all()
                    return
            finally:
                if ticket in self._waiting_requests:
                    self._waiting_requests.remove(ticket)
                    heapq.heapify(self._waiting_requests)
                    self._gate.notify_all()

    async def _schedule_rate_limit_backoff(self, path: str) -> None:
        """Throttle all REST requests after 429 while preserving scheduling priority."""
        async with self._gate:
            now = time.monotonic()
            if (
                self._last_rate_limit_monotonic is None
                or now - self._last_rate_limit_monotonic >= 60.0
            ):
                self._rate_limit_backoff_level = 0
            delay = min(8.0, 0.5 * (2**self._rate_limit_backoff_level))
            self._rate_limit_backoff_level = min(self._rate_limit_backoff_level + 1, 5)
            self._last_rate_limit_monotonic = now
            self._next_request_at = max(self._next_request_at, now + delay)
            self.rate_limit_backoff_count += 1
            self.rate_limit_backoff_seconds = delay
            self.rate_limit_backoff_scheduled_seconds += delay
            self.last_rate_limit_path = path
            self.last_rate_limit_utc = datetime.now(timezone.utc).isoformat()
            self._gate.notify_all()

    def _get_sync(self, path: str, params: dict[str, Any] | None) -> dict[str, Any]:
        if not path.startswith("/") or ".." in path or path.startswith("//"):
            raise ValueError("Only fixed relative GET API paths are permitted")
        query_values: dict[str, Any] = {}
        for key, value in (params or {}).items():
            if value is None or value == "":
                continue
            if isinstance(value, bool):
                query_values[key] = str(value).lower()
            else:
                query_values[key] = value
        query = urllib.parse.urlencode(query_values, doseq=True)
        url = f"{REST_BASE}{path}"
        if query:
            url = f"{url}?{query}"
        request = urllib.request.Request(
            url,
            method="GET",
            headers={"Accept": "application/json", "User-Agent": "kalshi-15m-native/0.1"},
        )
        with urllib.request.urlopen(request, timeout=25) as response:
            if response.status != 200:
                raise RuntimeError(f"Kalshi GET returned HTTP {response.status}")
            raw = response.read(settings.max_response_bytes + 1)
            if len(raw) > settings.max_response_bytes:
                raise RuntimeError("Kalshi response exceeded configured 64 MiB limit")
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            raise RuntimeError("Kalshi response was not a JSON object")
        return payload

    async def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        self.requests += 1
        for retry in range(4):
            await self._throttle(self._priority(path, params))
            self.attempts += 1
            request_started = time.perf_counter()
            try:
                return await asyncio.to_thread(self._get_sync, path, params)
            except urllib.error.HTTPError as exc:
                if exc.code == 429:
                    self.rate_limit_responses += 1
                    await self._schedule_rate_limit_backoff(path)
                    if retry < 3:
                        continue
                elif 500 <= exc.code < 600 and retry < 3:
                    await asyncio.sleep(min(3.0, 0.25 * (2 ** retry)))
                    continue
                self.errors += 1
                self.last_error = f"{type(exc).__name__}: HTTP {exc.code}"
                raise
            except (urllib.error.URLError, TimeoutError) as exc:
                if retry < 3:
                    await asyncio.sleep(min(3.0, 0.25 * (2 ** retry)))
                    continue
                self.errors += 1
                self.last_error = f"{type(exc).__name__}: {exc}"
                raise
            except (ValueError, RuntimeError, json.JSONDecodeError) as exc:
                self.errors += 1
                self.last_error = f"{type(exc).__name__}: {exc}"
                raise
            finally:
                self.last_request_rtt_ms = round(
                    max(0.0, time.perf_counter() - request_started) * 1000,
                    3,
                )
        raise RuntimeError("GET retry loop ended unexpectedly")

    async def pages(
        self,
        path: str,
        result_key: str,
        params: dict[str, Any] | None = None,
        max_pages: int = 100,
    ) -> list[dict[str, Any]]:
        found: list[dict[str, Any]] = []
        cursor: str | None = None
        for _ in range(max_pages):
            query = dict(params or {})
            if cursor:
                query["cursor"] = cursor
            payload = await self.get(path, query)
            rows = payload.get(result_key, [])
            if not isinstance(rows, list):
                raise RuntimeError(f"Expected '{result_key}' to be an array")
            found.extend(row for row in rows if isinstance(row, dict))
            cursor = payload.get("cursor") or None
            if not cursor:
                return found
        raise PaginationLimitExceeded(found, cursor, max_pages)
