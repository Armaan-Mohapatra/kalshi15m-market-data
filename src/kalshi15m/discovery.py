from __future__ import annotations

import csv
import json
import logging
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from .api import PaginationLimitExceeded, PublicKalshiAPI
from .classify import candidate_series, duration_seconds, parse_time, rules_confirm_15m
from .config import settings

LOG = logging.getLogger(__name__)
FIFTEEN_FREQUENCIES = {"fifteen_min", "fifteen_minutes", "15_minutes"}
SCHEDULED_RECONCILIATION_BATCH_SIZE = 25
ROLLOVER_SERIES_SCAN_BATCH_SIZE = 4
SERIES_DURATION_PATTERNS = (
    re.compile(r"\b15\s*(?:-\s*)?(?:minutes?|mins?)\b", re.IGNORECASE),
    re.compile(r"\b900\s+seconds?\b", re.IGNORECASE),
    re.compile(r"\bfifteen[-\s]+minutes?\b", re.IGNORECASE),
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _series_metadata_strings(value: Any) -> Iterable[str]:
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for key, nested in value.items():
            if str(key).lower() not in {"ticker", "series_ticker"}:
                yield from _series_metadata_strings(nested)
    elif isinstance(value, (list, tuple)):
        for nested in value:
            yield from _series_metadata_strings(nested)


def series_catalog_candidate(series: dict[str, Any]) -> bool:
    """Select candidates from explicit frequency, tags, or duration metadata.

    Ticker text is deliberately ignored. Market rules and 900-second timing
    still determine whether a discovered contract is confirmed 15-minute.
    """
    if candidate_series(series):
        return True
    return any(
        pattern.search(text)
        for key, value in series.items()
        if key not in {"ticker", "series_ticker"}
        for text in _series_metadata_strings(value)
        for pattern in SERIES_DURATION_PATTERNS
    )


def _candidate_confirmed(series: dict[str, Any]) -> bool:
    """Whether series metadata independently confirms a 15-minute window."""
    return series_catalog_candidate(series)


def candidate_series_tickers(series_rows: Iterable[dict[str, Any]]) -> list[str]:
    """Return the stable candidate ticker set from series metadata."""
    return sorted(
        {
            str(row.get("ticker") or row.get("series_ticker"))
            for row in series_rows
            if (row.get("ticker") or row.get("series_ticker")) and series_catalog_candidate(row)
        }
    )


def series_catalog_changes(
    snapshot: dict[str, Any], series_rows: Iterable[dict[str, Any]]
) -> tuple[list[str], list[str]]:
    """Compare fresh candidate-series metadata with the cached inventory."""
    prior = {
        str(row.get("series_ticker") or row.get("ticker"))
        for row in snapshot.get("series") or []
        if row.get("series_ticker") or row.get("ticker")
    }
    current = set(candidate_series_tickers(series_rows))
    return sorted(current - prior), sorted(prior - current)


def _market_api_reconciliation_set(
    events_by_series: dict[str, list[dict[str, Any]]],
    now: datetime | None = None,
    settlement_lookback_seconds: int = 1800,
) -> list[str]:
    """Choose live, nearest unopened, and recent unsettled markets per series.

    Event responses provide active and upcoming inventory. Recent closed rows
    are reconciled through the market API so settlement results are captured
    after a restart as well as during a continuously running session.
    """
    now = now or datetime.now(timezone.utc)
    chosen: set[str] = set()
    for events in events_by_series.values():
        upcoming: list[tuple[datetime, str]] = []
        for event in events:
            event_status = str(event.get("_query_status", "")).lower()
            for market in event.get("markets") or []:
                if not isinstance(market, dict) or not market.get("ticker"):
                    continue
                ticker = str(market["ticker"])
                status = str(market.get("status") or event_status).lower()
                if status in {"active", "open"} or event_status == "open":
                    chosen.add(ticker)
                    continue
                if event_status in {"closed", "settled"}:
                    closed = parse_time(market.get("close_time"))
                    if (
                        closed is not None
                        and 0 <= (now - closed).total_seconds() <= settlement_lookback_seconds
                        and str(market.get("result") or "").lower() not in {"yes", "no"}
                    ):
                        chosen.add(ticker)
                    continue
                if status not in {"initialized", "unopened"} and event_status != "unopened":
                    continue
                raw_open = market.get("open_time")
                if not raw_open:
                    continue
                try:
                    parsed = datetime.fromisoformat(str(raw_open).replace("Z", "+00:00"))
                    if parsed.tzinfo is None:
                        parsed = parsed.replace(tzinfo=timezone.utc)
                    upcoming.append((parsed.astimezone(timezone.utc), ticker))
                except (TypeError, ValueError):
                    continue
        if upcoming:
            upcoming.sort(key=lambda item: (item[0], item[1]))
            chosen.add(upcoming[0][1])
    return sorted(chosen)


def _dedupe_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Merge duplicate status-query results by event and nested market ticker."""
    merged: dict[tuple[str, bool], dict[str, Any]] = {}
    status_rank = {
        "settled": 5,
        "finalized": 5,
        "closed": 4,
        "active": 3,
        "open": 3,
        "initialized": 2,
        "unopened": 2,
    }
    query_status_rank = {"open": 5, "unopened": 4, "settled": 3, "closed": 2}
    for event in events:
        event_ticker = str(event.get("event_ticker") or "")
        key = (event_ticker, bool(event.get("_is_mve")))
        if key not in merged:
            item = dict(event)
            unique_markets: dict[str, dict[str, Any]] = {}
            for market in event.get("markets") or []:
                if isinstance(market, dict) and market.get("ticker"):
                    unique_markets[str(market["ticker"])] = dict(market)
            item["markets"] = list(unique_markets.values())
            merged[key] = item
            continue
        prior = merged[key]
        incoming_query_status = str(event.get("_query_status") or "").lower()
        prior_query_status = str(prior.get("_query_status") or "").lower()
        if query_status_rank.get(incoming_query_status, 0) > query_status_rank.get(prior_query_status, 0):
            prior["_query_status"] = incoming_query_status
        for field, value in event.items():
            if prior.get(field) in (None, "") and value not in (None, ""):
                prior[field] = value
        nested = {str(market.get("ticker")): market for market in prior.get("markets", [])}
        for market in event.get("markets") or []:
            if not isinstance(market, dict) or not market.get("ticker"):
                continue
            ticker = str(market["ticker"])
            existing = nested.get(ticker)
            if existing is None or status_rank.get(str(market.get("status", "")).lower(), 0) > status_rank.get(
                str(existing.get("status", "")).lower(), 0
            ):
                nested[ticker] = dict(market)
        prior["markets"] = list(nested.values())
    return list(merged.values())


def _scheduled_market_reconciliation_set(
    markets: list[dict[str, Any]],
    now: datetime,
    near_open_seconds: int = 90,
    settlement_lookback_seconds: int = 1800,
) -> list[str]:
    """Choose markets whose scheduled window is current, near, or next per series."""
    chosen: set[str] = set()
    next_by_series: dict[str, tuple[datetime, str]] = {}
    for market in markets:
        ticker = str(market.get("ticker") or "")
        if not ticker:
            continue
        opened = parse_time(market.get("open_time"))
        closed = parse_time(market.get("close_time"))
        status = str(market.get("market_status") or "").lower()
        if status in {"active", "open"} and (closed is None or closed >= now):
            chosen.add(ticker)
        if opened is None:
            continue
        seconds_until_open = (opened - now).total_seconds()
        if (seconds_until_open <= 0 and (closed is None or closed > now)) or (
            0 < seconds_until_open <= near_open_seconds
        ):
            chosen.add(ticker)
        if (
            closed is not None
            and 0 <= (now - closed).total_seconds() <= settlement_lookback_seconds
            and not market.get("result")
        ):
            chosen.add(ticker)
        if seconds_until_open > 0:
            series = str(market.get("series_ticker") or "")
            prior = next_by_series.get(series)
            candidate = (opened, ticker)
            if prior is None or candidate < prior:
                next_by_series[series] = candidate
    chosen.update(ticker for _, ticker in next_by_series.values())
    return sorted(chosen)


async def refresh_known_market_rows(
    api: PublicKalshiAPI,
    snapshot: dict[str, Any],
    now: datetime | None = None,
    near_open_seconds: int = 90,
    settlement_lookback_seconds: int = 1800,
    ticker_override: Iterable[str] | None = None,
    ticker_batch_size: int | None = None,
) -> list[str]:
    """Reconcile scheduled rows, or a supplied high-priority subset, in cached inventory."""
    now = now or datetime.now(timezone.utc)
    markets = snapshot.get("markets") or []
    by_ticker = {str(row.get("ticker")): row for row in markets if row.get("ticker")}
    tickers = (
        _scheduled_market_reconciliation_set(
            markets, now, near_open_seconds, settlement_lookback_seconds
        )
        if ticker_override is None
        else sorted({str(ticker) for ticker in ticker_override if str(ticker) in by_ticker})
    )
    prior_results = {
        ticker: str(row.get("result") or "").lower()
        for ticker, row in by_ticker.items()
    }
    market_api_rows: dict[str, dict[str, Any]] = {}
    batch_size = max(1, ticker_batch_size or SCHEDULED_RECONCILIATION_BATCH_SIZE)
    for start in range(0, len(tickers), batch_size):
        batch = tickers[start : start + batch_size]
        try:
            rows = await api.pages(
                "/markets",
                "markets",
                {"tickers": ",".join(batch), "limit": 1000},
                max_pages=2,
            )
            for row in rows:
                if row.get("ticker"):
                    market_api_rows[str(row["ticker"])] = row
        except Exception as exc:
            LOG.warning("scheduled market reconciliation failed: %s", type(exc).__name__)

    refresh_fields = (
        "status", "open_time", "close_time", "expected_expiration_time",
        "latest_expiration_time", "result", "settlement_ts", "settlement_value_dollars",
        "yes_bid_dollars", "yes_bid_size_fp", "no_bid_dollars", "no_bid_size_fp",
        "volume_fp", "volume_24h_fp", "updated_time", "fractional_trading_enabled",
        "rules_primary", "rules_secondary", "market_type",
    )
    for ticker, fresh in market_api_rows.items():
        cached = by_ticker.get(ticker)
        if cached is None:
            continue
        for field in refresh_fields:
            if field in fresh:
                cached[field] = fresh[field]
        # Preserve the authoritative exchange row independently of our
        # derived event schedule/classification projection.
        cached["market_api_raw"] = dict(fresh)
        for field in ("floor_strike", "cap_strike", "custom_strike", "expiration_value", "expiration_value_dollars"):
            if field in fresh:
                cached[field] = fresh[field]
        cached["market_status"] = str(fresh.get("status") or cached.get("market_status") or "unknown").lower()
        cached["market_api_reconciled"] = True
        cached["provenance"] = "Kalshi public REST: cached event schedule plus current market API reconciliation"

    newly_settled = sum(
        1
        for ticker, fresh in market_api_rows.items()
        if str(fresh.get("result") or "").lower() in {"yes", "no"}
        and prior_results.get(ticker) not in {"yes", "no"}
    )

    snapshot["market_api_reconciliation_tickers"] = tickers
    snapshot["market_api_reconciliation_returned_tickers"] = sorted(market_api_rows)
    snapshot["market_api_reconciliation_rows"] = len(market_api_rows)
    snapshot["api_requests"] = api.requests
    snapshot["api_attempts"] = api.attempts
    snapshot["api_rate_limit_responses"] = api.rate_limit_responses
    snapshot["api_errors"] = api.errors
    snapshot["api_last_error"] = api.last_error
    snapshot["active_15m_market_rows"] = sum(
        1 for row in markets
        if str(row.get("market_status", "")).lower() in {"active", "open"}
        and row.get("is_15m_confirmed")
        and (parse_time(row.get("open_time")) is None or parse_time(row.get("open_time")) <= now)
        and (parse_time(row.get("close_time")) is None or parse_time(row.get("close_time")) > now)
    )
    snapshot["rest_settlements_observed"] = int(snapshot.get("rest_settlements_observed", 0)) + newly_settled
    snapshot["open_or_upcoming_tickers"] = tickers
    return tickers


async def refresh_upcoming_series_markets(
    api: PublicKalshiAPI,
    snapshot: dict[str, Any],
    now: datetime | None = None,
    near_open_seconds: int = 300,
    scan_cursor: int = 0,
    scan_batch_size: int = ROLLOVER_SERIES_SCAN_BATCH_SIZE,
) -> tuple[int, int]:
    """Discover imminent contracts by rotating through confirmed 15-minute series.

    Cached full discovery snapshots age for hours, so scheduled market-row
    reconciliation alone cannot see a newly published event. Each pass checks
    a small series batch, enriches only imminent results through the event API,
    and keeps the normal metadata, rules, fee, and outcome-count gates.
    """
    now = now or datetime.now(timezone.utc)
    series_rows = []
    for row in snapshot.get("series") or []:
        series_ticker = str(row.get("series_ticker") or row.get("ticker") or "")
        if series_ticker and _candidate_confirmed(row):
            series_rows.append({**row, "ticker": series_ticker})
    if not series_rows:
        return 0, 0

    batch_size = max(1, min(scan_batch_size, len(series_rows)))
    start = scan_cursor % len(series_rows)
    scan_rows = [series_rows[(start + offset) % len(series_rows)] for offset in range(batch_size)]
    next_cursor = (start + batch_size) % len(series_rows)
    market_rows_by_ticker: dict[str, dict[str, Any]] = {}
    series_by_ticker = {str(row["ticker"]): row for row in series_rows}
    errors = 0
    for series in scan_rows:
        series_ticker = str(series["ticker"])
        rows: list[dict[str, Any]] = []
        for market_status in ("unopened", "open"):
            try:
                rows.extend(await api.pages(
                    "/markets",
                    "markets",
                    {
                        "series_ticker": series_ticker,
                        "status": market_status,
                        "limit": 200,
                    },
                    max_pages=1,
                ))
            except PaginationLimitExceeded as exc:
                rows.extend(exc.rows)
                errors += 1  # Pagination boundary prevents exhaustive coverage.
            except Exception as exc:
                errors += 1
                LOG.info("opening market scan failed for %s/%s: %s", series_ticker, market_status, type(exc).__name__)
                continue
        for row in rows:
            ticker = str(row.get("ticker") or "")
            event_ticker = str(row.get("event_ticker") or "")
            opened, closed = parse_time(row.get("open_time")), parse_time(row.get("close_time"))
            if not ticker or not event_ticker or opened is None:
                continue
            seconds_until_open = (opened - now).total_seconds()
            if not -180 <= seconds_until_open <= near_open_seconds:
                continue
            if closed is not None and closed <= now:
                continue
            market_rows_by_ticker[ticker] = dict(row)

    existing_tickers = {
        str(row.get("ticker")) for row in snapshot.get("markets") or [] if row.get("ticker")
    }
    imminent_new = {
        str(row.get("event_ticker") or "")
        for ticker, row in market_rows_by_ticker.items()
        if ticker not in existing_tickers and row.get("event_ticker")
    }
    if not imminent_new:
        snapshot["rollover_series_scan"] = {
            "checked_series": [str(row["ticker"]) for row in scan_rows],
            "checked_series_count": len(scan_rows),
            "imminent_event_count": 0,
            "new_market_rows": 0,
            "error_count": errors,
            "checked_utc": now.isoformat(),
        }
        return next_cursor, 0

    try:
        events = await api.pages(
            "/events",
            "events",
            {
                "tickers": ",".join(sorted(imminent_new)),
                "with_nested_markets": True,
                "limit": 200,
            },
            max_pages=2,
        )
    except Exception as exc:
        errors += 1
        LOG.info("upcoming event enrichment failed: %s", type(exc).__name__)
        snapshot["rollover_series_scan"] = {
            "checked_series": [str(row["ticker"]) for row in scan_rows],
            "checked_series_count": len(scan_rows),
            "imminent_event_count": len(imminent_new),
            "new_market_rows": 0,
            "error_count": errors,
            "checked_utc": now.isoformat(),
        }
        return next_cursor, 0

    by_ticker = {str(row.get("ticker")): row for row in snapshot.get("markets") or [] if row.get("ticker")}
    added = 0
    reconciled_tickers = set(snapshot.get("market_api_reconciliation_tickers") or [])
    open_upcoming_tickers = set(snapshot.get("open_or_upcoming_tickers") or [])
    for event in events:
        series_ticker = str(event.get("series_ticker") or "")
        series = series_by_ticker.get(series_ticker)
        event_ticker = str(event.get("event_ticker") or "")
        if series is None or event_ticker not in imminent_new:
            continue
        nested_markets = [
            market for market in event.get("markets") or []
            if isinstance(market, dict) and market.get("ticker")
        ]
        outcome_count = len({str(market["ticker"]) for market in nested_markets})
        for nested in nested_markets:
            ticker = str(nested["ticker"])
            market = {**nested, **market_rows_by_ticker.get(ticker, {})}
            duration = duration_seconds(market)
            rules_confirm = rules_confirm_15m(market)
            metadata_confirms = _candidate_confirmed(series)
            if duration is not None:
                confirmed = duration == 900 or (rules_confirm and metadata_confirms)
                basis = (
                    "open_time_to_close_time=900s"
                    if duration == 900
                    else "series_metadata_and_market_rules"
                    if confirmed
                    else "duration_metadata_mismatch"
                )
            else:
                confirmed = rules_confirm and metadata_confirms
                basis = "series_metadata_and_market_rules" if confirmed else "duration_unavailable"
            market_type = str(market.get("market_type") or "unknown").lower()
            if not confirmed:
                compatibility = "UNCONFIRMED_DURATION"
            elif market_type != "binary":
                compatibility = "UNSUPPORTED_NON_BINARY"
            elif outcome_count > 1:
                compatibility = "MULTI_OUTCOME_EVENT_REVIEW"
            else:
                compatibility = "BINARY_SINGLE_MARKET"
            row = {
                "ticker": ticker,
                "series_ticker": series_ticker,
                "event_ticker": event_ticker,
                "event_title": event.get("title"),
                "event_status_query": "open_or_unopened",
                "is_multivariate_event": False,
                "market_status": str(market.get("status") or "unopened").lower(),
                "market_api_reconciled": ticker in market_rows_by_ticker,
                "market_type": market_type,
                "market_api_raw": dict(market),
                "floor_strike": market.get("floor_strike"),
                "cap_strike": market.get("cap_strike"),
                "custom_strike": market.get("custom_strike"),
                "expiration_value": market.get("expiration_value"),
                "expiration_value_dollars": market.get("expiration_value_dollars"),
                "duration_seconds": duration,
                "duration_basis": basis,
                "rules_confirm_15m": rules_confirm,
                "is_15m_confirmed": confirmed,
                "event_market_count": outcome_count,
                "paper_compatibility": compatibility,
                "open_time": market.get("open_time"),
                "close_time": market.get("close_time"),
                "expected_expiration_time": market.get("expected_expiration_time"),
                "latest_expiration_time": market.get("latest_expiration_time"),
                "result": market.get("result"),
                "settlement_ts": market.get("settlement_ts"),
                "settlement_value_dollars": market.get("settlement_value_dollars"),
                "yes_bid_dollars": market.get("yes_bid_dollars"),
                "yes_bid_size_fp": market.get("yes_bid_size_fp"),
                "no_bid_dollars": market.get("no_bid_dollars"),
                "no_bid_size_fp": market.get("no_bid_size_fp"),
                "volume_fp": market.get("volume_fp"),
                "volume_24h_fp": market.get("volume_24h_fp"),
                "price_level_structure": market.get("price_level_structure"),
                "price_ranges": market.get("price_ranges"),
                "can_close_early": market.get("can_close_early"),
                "rules_primary": market.get("rules_primary"),
                "rules_secondary": market.get("rules_secondary"),
                "fractional_trading_enabled": market.get("fractional_trading_enabled"),
                "fee_type": series.get("fee_type"),
                "fee_multiplier": series.get("fee_multiplier"),
                "fee_type_override": event.get("fee_type_override"),
                "fee_multiplier_override": event.get("fee_multiplier_override"),
                "settlement_sources": (
                    series.get("settlement_sources")
                    or event.get("settlement_sources")
                    or []
                ),
                "underlying_feed_status": "NOT_CONNECTED_NO_EXTERNAL_SOURCE_ASSUMED",
                "provenance": "Kalshi public REST: rotating series market scan plus event rule enrichment",
            }
            prior = by_ticker.get(ticker)
            if prior is None:
                snapshot.setdefault("markets", []).append(row)
                by_ticker[ticker] = row
                added += 1
            else:
                prior.update(row)
            reconciled_tickers.add(ticker)
            open_upcoming_tickers.add(ticker)
    snapshot["market_api_reconciliation_tickers"] = sorted(reconciled_tickers)
    snapshot["open_or_upcoming_tickers"] = sorted(open_upcoming_tickers)
    snapshot["rollover_series_scan"] = {
        "checked_series": [str(row["ticker"]) for row in scan_rows],
        "checked_series_count": len(scan_rows),
        "imminent_event_count": len(imminent_new),
        "new_market_rows": added,
        "error_count": errors,
        "checked_utc": now.isoformat(),
    }
    return next_cursor, added


async def _series_events(
    api: PublicKalshiAPI,
    series_ticker: str,
    status: str,
    path: str = "/events",
    min_close_ts: int | None = None,
    max_pages: int = 100,
) -> list[dict[str, Any]]:
    params: dict[str, Any] = {
        "series_ticker": series_ticker,
        "with_nested_markets": True,
        "limit": 200,
    }
    if path == "/events":
        params["status"] = status
    if min_close_ts is not None:
        params["min_close_ts"] = min_close_ts
    return await api.pages(path, "events", params, max_pages=max_pages)


async def discover(
    api: PublicKalshiAPI,
    output_dir: Path | None = None,
    *,
    series_rows_override: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Enumerate 15-minute metadata and reconcile current, upcoming, and recent settlements."""
    fetched_at = utc_now()
    # The current Series List has no cursor/limit parameters. pages() still
    # honors a cursor if that API adds one; the one-page request is bounded.
    series_all = (
        await api.pages("/series", "series", max_pages=1)
        if series_rows_override is None
        else list(series_rows_override)
    )
    selected = [s for s in series_all if series_catalog_candidate(s)]
    events_by_series: dict[str, list[dict[str, Any]]] = defaultdict(list)
    market_by_ticker: dict[str, dict[str, Any]] = {}
    mve_truncated_series: list[str] = []
    event_pagination_truncated: list[dict[str, Any]] = []
    coverage_errors: list[dict[str, str]] = []
    now = datetime.now(timezone.utc)
    recent_close_min_ts = int(now.timestamp()) - 1800

    for series in selected:
        ticker = str(series.get("ticker", ""))
        if not ticker:
            continue
        for status in ("open", "unopened", "closed", "settled"):
            min_close_ts = recent_close_min_ts if status in {"closed", "settled"} else None
            try:
                events = await _series_events(
                    api, ticker, status, min_close_ts=min_close_ts
                )
            except PaginationLimitExceeded as exc:
                events = exc.rows
                event_pagination_truncated.append(
                    {"series_ticker": ticker, "status": status, "pages": str(exc.pages)}
                )
                coverage_errors.append(
                    {
                        "source": "events",
                        "series_ticker": ticker,
                        "status": status,
                        "reason": "PAGINATION_LIMIT_REACHED_PARTIAL_ROWS_RETAINED",
                    }
                )
            except Exception as exc:
                LOG.warning("events discovery failed for a candidate series: %s", type(exc).__name__)
                coverage_errors.append(
                    {
                        "source": "events",
                        "series_ticker": ticker,
                        "status": status,
                        "reason": type(exc).__name__,
                    }
                )
                events = []
            for event in events:
                event_copy = dict(event)
                event_copy["_query_status"] = status
                events_by_series[ticker].append(event_copy)
                for market in event.get("markets") or []:
                    if isinstance(market, dict) and market.get("ticker"):
                        market_by_ticker[str(market["ticker"])] = dict(market)

        # Standard /events excludes multivariate events.
        try:
            mve_page = await api.get(
                "/events/multivariate",
                {
                    "series_ticker": ticker,
                    "with_nested_markets": True,
                    "limit": 200,
                },
            )
            mve_events = mve_page.get("events", [])
            if mve_page.get("cursor"):
                # MVE events are kept in a separate review class. Retain a
                # bounded newest page and make the remaining history gap visible.
                mve_truncated_series.append(ticker)
            if not isinstance(mve_events, list):
                mve_events = []
        except Exception as exc:
            LOG.info("multivariate event discovery unavailable: %s", type(exc).__name__)
            coverage_errors.append(
                {
                    "source": "multivariate_events",
                    "series_ticker": ticker,
                    "status": "all",
                    "reason": type(exc).__name__,
                }
            )
            mve_events = []
        for event in mve_events:
            nested_markets = event.get("markets") or []
            if not nested_markets:
                continue
            statuses = {str(m.get("status", "")).lower() for m in nested_markets if isinstance(m, dict)}
            if not statuses.intersection({"active", "open", "initialized", "unopened"}):
                continue
            event_copy = dict(event)
            event_copy["_query_status"] = "multivariate"
            event_copy["_is_mve"] = True
            events_by_series[ticker].append(event_copy)
            for market in nested_markets:
                if isinstance(market, dict) and market.get("ticker"):
                    market_by_ticker[str(market["ticker"])] = dict(market)

    for ticker, related_events in list(events_by_series.items()):
        events_by_series[ticker] = _dedupe_events(related_events)
    market_by_ticker = {
        str(market["ticker"]): dict(market)
        for events in events_by_series.values()
        for event in events
        for market in event.get("markets") or []
        if isinstance(market, dict) and market.get("ticker")
    }

    tickers = sorted(market_by_ticker)
    reconcile_tickers = _market_api_reconciliation_set(events_by_series)
    market_api_rows: dict[str, dict[str, Any]] = {}
    # The tickers filter has a short query-string limit. Five comma-separated
    # tickers stays comfortably below it while still consolidating requests.
    for start in range(0, len(reconcile_tickers), 5):
        batch = reconcile_tickers[start : start + 5]
        try:
            rows = await api.pages(
                "/markets",
                "markets",
                {"tickers": ",".join(batch), "limit": 1000},
                max_pages=2,
            )
            for row in rows:
                if row.get("ticker"):
                    market_api_rows[str(row["ticker"])] = row
        except Exception as exc:
            LOG.warning("market API reconciliation failed: %s", type(exc).__name__)
            coverage_errors.append(
                {
                    "source": "markets_reconciliation",
                    "series_ticker": "",
                    "status": "current_upcoming_or_recent_unsettled",
                    "reason": type(exc).__name__,
                }
            )
            if isinstance(exc, PaginationLimitExceeded):
                for row in exc.rows:
                    if row.get("ticker"):
                        market_api_rows[str(row["ticker"])] = row

    schedule: dict[str, Any] = {}
    try:
        schedule = await api.get("/exchange/schedule")
    except Exception as exc:
        LOG.info("exchange schedule unavailable: %s", type(exc).__name__)

    inventory: list[dict[str, Any]] = []
    active_markets: list[dict[str, Any]] = []
    all_market_rows: list[dict[str, Any]] = []
    open_or_upcoming_market_tickers: set[str] = set()
    for series in selected:
        series_ticker = str(series.get("ticker", ""))
        related_events = events_by_series.get(series_ticker, [])
        local_markets: list[dict[str, Any]] = []
        event_counts: dict[str, int] = defaultdict(int)
        for event in related_events:
            event_ticker = str(event.get("event_ticker", ""))
            for market in event.get("markets") or []:
                if isinstance(market, dict) and market.get("ticker"):
                    event_counts[event_ticker] += 1
        for event in related_events:
            event_ticker = str(event.get("event_ticker", ""))
            for nested in event.get("markets") or []:
                if not isinstance(nested, dict) or not nested.get("ticker"):
                    continue
                ticker = str(nested["ticker"])
                market_api_reconciled = ticker in market_api_rows
                market = market_api_rows.get(ticker, nested)
                duration = duration_seconds(market)
                status = str(market.get("status") or nested.get("status") or "unknown").lower()
                metadata_confirms = _candidate_confirmed(series)
                rules_confirm = rules_confirm_15m(market)
                if duration is not None:
                    confirmed = duration == 900 or (rules_confirm and metadata_confirms)
                    basis = (
                        "open_time_to_close_time=900s"
                        if duration == 900
                        else "series_metadata_and_market_rules"
                        if confirmed
                        else "duration_metadata_mismatch"
                    )
                else:
                    confirmed = rules_confirm and metadata_confirms
                    basis = "series_metadata_and_market_rules" if confirmed else "duration_unavailable"
                market_type = str(market.get("market_type") or "unknown").lower()
                outcomes = event_counts[event_ticker]
                if not confirmed:
                    compatibility = "UNCONFIRMED_DURATION"
                elif market_type != "binary":
                    compatibility = "UNSUPPORTED_NON_BINARY"
                elif event.get("_is_mve"):
                    compatibility = "MULTIVARIATE_EVENT_REVIEW"
                elif outcomes > 1:
                    compatibility = "MULTI_OUTCOME_EVENT_REVIEW"
                else:
                    compatibility = "BINARY_SINGLE_MARKET"
                row = {
                    "ticker": ticker,
                    "series_ticker": series_ticker,
                    "event_ticker": event_ticker,
                    "event_title": event.get("title"),
                    "event_status_query": event.get("_query_status"),
                    "is_multivariate_event": bool(event.get("_is_mve")),
                    "market_status": status,
                    "market_api_reconciled": market_api_reconciled,
                    "market_type": market_type,
                    "market_api_raw": dict(market),
                    "floor_strike": market.get("floor_strike"),
                    "cap_strike": market.get("cap_strike"),
                    "custom_strike": market.get("custom_strike"),
                    "expiration_value": market.get("expiration_value"),
                    "expiration_value_dollars": market.get("expiration_value_dollars"),
                    "duration_seconds": duration,
                    "duration_basis": basis,
                    "rules_confirm_15m": rules_confirm,
                    "is_15m_confirmed": confirmed,
                    "event_market_count": outcomes,
                    "paper_compatibility": compatibility,
                    "open_time": market.get("open_time"),
                    "close_time": market.get("close_time"),
                    "expected_expiration_time": market.get("expected_expiration_time"),
                    "latest_expiration_time": market.get("latest_expiration_time"),
                    "result": market.get("result"),
                    "settlement_ts": market.get("settlement_ts"),
                    "settlement_value_dollars": market.get("settlement_value_dollars"),
                    "yes_bid_dollars": market.get("yes_bid_dollars"),
                    "yes_bid_size_fp": market.get("yes_bid_size_fp"),
                    "no_bid_dollars": market.get("no_bid_dollars"),
                    "no_bid_size_fp": market.get("no_bid_size_fp"),
                    "volume_fp": market.get("volume_fp"),
                    "volume_24h_fp": market.get("volume_24h_fp"),
                    "price_level_structure": market.get("price_level_structure"),
                    "price_ranges": market.get("price_ranges"),
                    "can_close_early": market.get("can_close_early"),
                    "rules_primary": market.get("rules_primary"),
                    "rules_secondary": market.get("rules_secondary"),
                    "fractional_trading_enabled": market.get("fractional_trading_enabled"),
                    "fee_type": series.get("fee_type"),
                    "fee_multiplier": series.get("fee_multiplier"),
                    "fee_type_override": event.get("fee_type_override"),
                    "fee_multiplier_override": event.get("fee_multiplier_override"),
                    "settlement_sources": series.get("settlement_sources") or event.get("settlement_sources") or [],
                    "underlying_feed_status": "NOT_CONNECTED_NO_EXTERNAL_SOURCE_ASSUMED",
                    "provenance": (
                        "Kalshi public REST: paginated event and market API reconciliation"
                        if market_api_reconciled
                        else "Kalshi public REST: paginated event with nested market metadata"
                    ),
                }
                local_markets.append(row)
                all_market_rows.append(row)
                opened = parse_time(row.get("open_time"))
                closes = parse_time(row.get("close_time"))
                is_current = status in {"active", "open"} and (
                    opened is None or opened <= now
                ) and (closes is None or closes > now)
                is_upcoming = status in {"initialized", "unopened"} and (
                    opened is None or opened > now
                ) and (closes is None or closes > now)
                if is_current or is_upcoming:
                    open_or_upcoming_market_tickers.add(ticker)
                if is_current and confirmed:
                    active_markets.append(row)

        source_names = [item.get("name") for item in (series.get("settlement_sources") or [])]
        frequency = str(series.get("frequency", ""))
        inventory.append(
            {
                "series_ticker": series_ticker,
                "title": series.get("title"),
                "category": series.get("category"),
                "categories": series.get("categories") or [],
                "frequency": frequency,
                "tags": series.get("tags") or [],
                "duration_confirmed_from_series_metadata": _candidate_confirmed(series),
                "fee_type": series.get("fee_type"),
                "fee_multiplier": series.get("fee_multiplier"),
                "settlement_sources": series.get("settlement_sources") or [],
                "source_names": source_names,
                "series_last_updated_ts": series.get("last_updated_ts"),
                "event_count_open_or_upcoming": len(
                    {
                        str(event.get("event_ticker") or "")
                        for event in related_events
                        if str(event.get("_query_status") or "").lower()
                        in {"open", "unopened", "multivariate"}
                    }
                    - {""}
                ),
                "market_rows_open_or_upcoming": sum(
                    1 for row in local_markets if row["ticker"] in open_or_upcoming_market_tickers
                ),
                "recent_closed_or_settled_market_rows": sum(
                    1 for row in local_markets
                    if str(row.get("event_status_query") or "").lower() in {"closed", "settled"}
                ),
                "active_market_rows_15m": sum(
                    1 for row in local_markets
                    if row["market_status"] in {"active", "open"} and row["is_15m_confirmed"]
                ),
                "multi_outcome_event_rows": sum(
                    1 for row in local_markets if row["event_market_count"] > 1 or row["is_multivariate_event"]
                ),
                "markets": local_markets,
            }
        )

    snapshot = {
        "snapshot_utc": fetched_at,
        "api_base": "https://external-api.kalshi.com/trade-api/v2",
        "discovery_method": "series metadata; paginated open/unopened events; recent closed/settled events with min_close_ts; newest multivariate page per candidate series; current, nearest-upcoming, and unsettled-market reconciliation",
        "discovery_coverage_state": (
            "PARTIAL"
            if coverage_errors or mve_truncated_series
            else "COMPLETE_CURRENT_AND_NEAR_TERM"
        ),
        "discovery_coverage_errors": coverage_errors,
        "event_pagination_truncated": event_pagination_truncated,
        "recent_closed_settled_lookback_seconds": 1800,
        "recent_closed_settled_min_close_ts": recent_close_min_ts,
        "series_api_rows": len(series_all),
        "fifteen_min_series": len(inventory),
        "series_catalog_candidate_tickers": candidate_series_tickers(series_all),
        "series_catalog_fingerprint_checked_utc": fetched_at,
        "event_api_rows": sum(len(value) for value in events_by_series.values()),
        "mve_latest_page_truncated_series": mve_truncated_series,
        "market_event_rows": len(all_market_rows),
        "active_15m_market_rows": len(active_markets),
        "all_open_or_upcoming_market_tickers": sorted(open_or_upcoming_market_tickers),
        "open_or_upcoming_tickers": sorted(open_or_upcoming_market_tickers),
        "market_api_reconciliation_tickers": reconcile_tickers,
        "market_api_reconciliation_rows": len(market_api_rows),
        "series": inventory,
        "markets": all_market_rows,
        "exchange_schedule": schedule,
        "api_requests": api.requests,
        "api_attempts": api.attempts,
        "api_rate_limit_responses": api.rate_limit_responses,
        "api_errors": api.errors,
        "api_last_error": api.last_error,
        "strategy_state": "PAPER_CONFIG_FROZEN_SEPARATELY",
        "strategy_block_reason": "Discovery does not execute the paper evaluator; active REFLEX config provenance is recorded separately.",
        "underlying_data_state": "MISSING_OR_NOT_CONNECTED; no underlying prices synthesized",
    }
    if output_dir:
        output_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        snapshot_path = output_dir / f"series_inventory_{stamp}.json"
        series_csv = output_dir / f"series_inventory_{stamp}.csv"
        market_csv = output_dir / f"active_markets_{stamp}.csv"
        snapshot["artifact_paths"] = {
            "json": str(snapshot_path),
            "series_csv": str(series_csv),
            "markets_csv": str(market_csv),
        }
        columns = [
            "series_ticker", "title", "category", "frequency", "tags",
            "duration_confirmed_from_series_metadata", "fee_type", "fee_multiplier",
            "source_names", "event_count_open_or_upcoming", "market_rows_open_or_upcoming",
            "active_market_rows_15m", "multi_outcome_event_rows",
        ]
        with series_csv.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
            writer.writeheader()
            for item in inventory:
                writer.writerow(
                    {
                        **item,
                        "tags": json.dumps(item["tags"], ensure_ascii=False),
                        "source_names": json.dumps(item["source_names"], ensure_ascii=False),
                    }
                )
        with market_csv.open("w", newline="", encoding="utf-8") as handle:
            columns = list(active_markets[0]) if active_markets else ["ticker"]
            writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(active_markets)
        snapshot_path.write_text(json.dumps(snapshot, indent=2, ensure_ascii=False), encoding="utf-8")
    return snapshot

