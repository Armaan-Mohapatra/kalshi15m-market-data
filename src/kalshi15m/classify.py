from __future__ import annotations

import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any


FIFTEEN_MIN_FREQUENCIES = {"fifteen_min", "fifteen_minutes", "15_minutes"}
FIFTEEN_MIN_TAGS = {"15 min", "15-minute", "15 minute", "fifteen minutes"}
DURATION_PATTERNS = (
    re.compile(r"\b15\s*[- ]?\s*minutes?\b", re.IGNORECASE),
    re.compile(r"\b900\s+seconds?\b", re.IGNORECASE),
)


def candidate_series(series: dict[str, Any]) -> bool:
    frequency = str(series.get("frequency", "")).strip().lower()
    tags = {str(tag).strip().lower() for tag in (series.get("tags") or [])}
    return frequency in FIFTEEN_MIN_FREQUENCIES or bool(tags & FIFTEEN_MIN_TAGS)


def parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (ValueError, TypeError):
        return None


def duration_seconds(market: dict[str, Any]) -> int | None:
    opened = parse_time(market.get("open_time"))
    closed = parse_time(market.get("close_time"))
    if not opened or not closed:
        return None
    return int((closed - opened).total_seconds())


def rules_confirm_15m(market: dict[str, Any]) -> bool:
    text = " ".join(
        str(market.get(key, ""))
        for key in ("rules_primary", "rules_secondary", "title", "subtitle")
    )
    return any(pattern.search(text) for pattern in DURATION_PATTERNS)


def source_window_confirmed(series: dict[str, Any], market: dict[str, Any]) -> bool:
    return (
        str(series.get("frequency", "")).strip().lower() in FIFTEEN_MIN_FREQUENCIES
        or (duration_seconds(market) == 900 and rules_confirm_15m(market))
    )


def decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def executable_quotes(book: dict[str, Any]) -> dict[str, Any]:
    """Derive binary asks from the opposite side's best bid using Decimal."""
    fp = book.get("orderbook_fp") or book.get("orderbook") or book
    yes_levels = fp.get("yes_dollars_fp") or fp.get("yes_dollars") or fp.get("yes") or []
    no_levels = fp.get("no_dollars_fp") or fp.get("no_dollars") or fp.get("no") or []

    def best(levels: list[Any]) -> tuple[Decimal | None, Decimal | None]:
        parsed: list[tuple[Decimal, Decimal]] = []
        for level in levels:
            if not isinstance(level, (list, tuple)) or len(level) < 2:
                continue
            price, size = decimal(level[0]), decimal(level[1])
            if price is not None and size is not None and size > 0:
                parsed.append((price, size))
        return max(parsed, key=lambda item: item[0]) if parsed else (None, None)

    yes_bid, yes_bid_size = best(yes_levels)
    no_bid, no_bid_size = best(no_levels)
    one = Decimal("1.0000")
    return {
        "yes_bid": str(yes_bid) if yes_bid is not None else None,
        "yes_bid_size": str(yes_bid_size) if yes_bid_size is not None else None,
        "yes_ask": str(one - no_bid) if no_bid is not None else None,
        "yes_ask_size": str(no_bid_size) if no_bid_size is not None else None,
        "no_bid": str(no_bid) if no_bid is not None else None,
        "no_bid_size": str(no_bid_size) if no_bid_size is not None else None,
        "no_ask": str(one - yes_bid) if yes_bid is not None else None,
        "no_ask_size": str(yes_bid_size) if yes_bid_size is not None else None,
    }

