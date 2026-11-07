"""Bounded per-market opening-window observation audit.

This is an *observation* ledger, not a proof of continuous historical
order-book completeness. Public REST snapshots are sampled states.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any


def _parse(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.astimezone(timezone.utc) if parsed.tzinfo else None
    except (ValueError, TypeError):
        return None


class OpeningCoverage:
    """Track at most the current and recently opened 15-minute market groups."""

    def __init__(self) -> None:
        self.records: dict[str, dict[str, Any]] = {}

    def track(self, ticker: str, open_time: str | None, observed_utc: str) -> None:
        opening = _parse(open_time)
        observed = _parse(observed_utc)
        if not ticker or opening is None or observed is None:
            return
        seconds = (observed - opening).total_seconds()
        if not -300 <= seconds <= 600:
            return
        current = self.records.get(ticker)
        if current is not None and current["scheduled_open_utc"] == opening.isoformat():
            return
        self.records[ticker] = {
            "ticker": ticker,
            "scheduled_open_utc": opening.isoformat(),
            "first_tracked_utc": observed.isoformat(),
            "rest_valid_samples_by_minute": [0, 0, 0],
            "rest_executable_quote_samples_by_minute": [0, 0, 0],
            "rest_missing_or_invalid_polls_by_minute": [0, 0, 0],
            "rest_trade_rows_by_minute": [0, 0, 0],
            "first_rest_book_observed_utc": None,
            "first_executable_quote_observed_utc": None,
            "first_trade_exchange_utc": None,
            "first_ws_synced_book_utc": None,
            "finalized": False,
        }

    def _bucket(self, ticker: str, observed_utc: str) -> tuple[dict[str, Any] | None, int | None]:
        record = self.records.get(ticker)
        observed = _parse(observed_utc)
        if record is None or observed is None or record["finalized"]:
            return None, None
        open_at = _parse(record["scheduled_open_utc"])
        if open_at is None:
            return None, None
        secs = (observed - open_at).total_seconds()
        if not (0 <= secs < 180):
            return record, None
        return record, int(secs // 60)

    def rest_book(self, ticker: str, observed_utc: str, *, valid: bool, executable: bool) -> None:
        record, bucket = self._bucket(ticker, observed_utc)
        if record is None or bucket is None:
            return
        if not valid:
            record["rest_missing_or_invalid_polls_by_minute"][bucket] += 1
            return
        record["rest_valid_samples_by_minute"][bucket] += 1
        if record["first_rest_book_observed_utc"] is None:
            record["first_rest_book_observed_utc"] = observed_utc
        if executable:
            record["rest_executable_quote_samples_by_minute"][bucket] += 1
            if record["first_executable_quote_observed_utc"] is None:
                record["first_executable_quote_observed_utc"] = observed_utc

    def rest_trade(self, ticker: str, exchange_utc: str | None, observed_utc: str) -> None:
        record = self.records.get(ticker)
        if record is None or record["finalized"]:
            return
        # Trade's authoritative timestamp determines which window it belongs
        # to; observed time is recorded in the durable raw trade journal.
        record, bucket = self._bucket(ticker, exchange_utc or "")
        if record is None or bucket is None:
            return
        record["rest_trade_rows_by_minute"][bucket] += 1
        if record["first_trade_exchange_utc"] is None:
            record["first_trade_exchange_utc"] = exchange_utc

    def ws_book(self, ticker: str, observed_utc: str) -> None:
        record, bucket = self._bucket(ticker, observed_utc)
        if record is not None and bucket is not None and record["first_ws_synced_book_utc"] is None:
            record["first_ws_synced_book_utc"] = observed_utc

    def finalize_due(self, observed_utc: str) -> list[dict[str, Any]]:
        now = _parse(observed_utc)
        if now is None:
            return []
        output = []
        expired = []
        for ticker, record in self.records.items():
            opening = _parse(record["scheduled_open_utc"])
            if opening is None:
                continue
            age = (now - opening).total_seconds()
            if age >= 180 and not record["finalized"]:
                samples = record["rest_valid_samples_by_minute"]
                missing = record["rest_missing_or_invalid_polls_by_minute"]
                state = (
                    "REST_SAMPLED_EACH_MINUTE_NOT_CONTINUOUS"
                    if all(samples) and not any(missing)
                    else "REST_PARTIAL_OR_INVALID"
                    if any(samples) or any(missing)
                    else "NO_REST_BOOK_OBSERVATIONS"
                )
                result = {
                    key: value for key, value in record.items() if key != "finalized"
                }
                result.update({
                    "finalized_utc": observed_utc,
                    "opening_window_seconds": 180,
                    "rest_sample_coverage_state": state,
                    "wss_continuity_verified": False,
                    "rest_book_history_complete": False,
                    "underlying_reference_verification": "NOT_CONNECTED",
                    "limitation": "REST books are sampled; missed intermediate deltas cannot be reconstructed",
                })
                output.append(result)
                record["finalized"] = True
            if record["finalized"] and age >= 900:
                expired.append(ticker)
        for ticker in expired:
            self.records.pop(ticker, None)
        return output
