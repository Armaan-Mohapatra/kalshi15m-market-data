from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING
from pathlib import Path
from typing import Any

from .book import OrderBook
from .classify import decimal, parse_time

D = Decimal
PROFILE_ID = "REFLEX_VNEXT_PAPER"
CONFIG_RELATIVE_PATH = Path("strategy") / "reflex_vnext_frozen_config.json"
SOURCE_CONFIG_RELATIVE_PATH = Path("strategy") / "reflex_vnext_source_snapshot.json"
AUTHORITY_RELATIVE_PATH = Path("strategy") / "authority_status.json"


class FrozenStrategyError(RuntimeError):
    pass


def load_frozen_strategy(project_root: Path | None = None) -> tuple[dict[str, Any], str]:
    root = project_root or Path(__file__).resolve().parents[2]
    config_path = root / CONFIG_RELATIVE_PATH
    source_path = root / SOURCE_CONFIG_RELATIVE_PATH
    authority_path = root / AUTHORITY_RELATIVE_PATH
    try:
        raw = config_path.read_bytes()
        source_raw = source_path.read_bytes()
        authority = json.loads(authority_path.read_text(encoding="utf-8"))
        config = json.loads(raw)
        source_config = json.loads(source_raw)
    except (OSError, json.JSONDecodeError) as exc:
        raise FrozenStrategyError(f"frozen strategy configuration unavailable: {type(exc).__name__}") from exc
    digest = hashlib.sha256(raw).hexdigest()
    expected = str(authority.get("frozen_config", {}).get("sha256", "")).lower()
    source_digest = hashlib.sha256(source_raw).hexdigest()
    expected_source = str(authority.get("source_config", {}).get("sha256", "")).lower()
    if not expected or digest != expected or not expected_source or source_digest != expected_source:
        raise FrozenStrategyError("frozen strategy configuration hash does not match authority manifest")
    if not authority.get("evaluator_enabled"):
        raise FrozenStrategyError("paper evaluator is disabled by authority manifest")
    if config.get("execution", {}).get("mode") != "PAPER" or config.get("execution", {}).get("live_authorized"):
        raise FrozenStrategyError("standalone strategy copy must remain PAPER and live-disabled")
    if (
        config.get("strategy") != source_config.get("strategy")
        or config.get("market_data") != source_config.get("market_data")
        or config.get("revision") != source_config.get("revision")
    ):
        raise FrozenStrategyError("paper execution wrapper changed the frozen active strategy rules")
    return config, digest


def intended_quantity(config: dict[str, Any]) -> Decimal:
    sizing = config["strategy"]["sizing"]
    quantity = D(str(sizing["target_quantity"]))
    if sizing.get("integer_only") or sizing.get("quantity_policy") == "WHOLE_CONTRACTS":
        quantity = quantity.to_integral_value(rounding=ROUND_CEILING)
    if quantity <= 0:
        raise FrozenStrategyError("frozen strategy has a nonpositive target quantity")
    return quantity


def _iso_utc(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("strategy timestamps must be timezone-aware")
    return value.astimezone(timezone.utc).isoformat()


def evaluate_market(
    config: dict[str, Any],
    market: dict[str, Any],
    orderbook: dict[str, Any] | None,
    *,
    as_of: datetime,
    book_received_at: datetime | None,
    submitted_attempt: bool = False,
    has_position: bool = False,
    config_sha256: str,
) -> dict[str, Any]:
    """Apply the frozen active REFLEX vNext entry rules as a pure paper decision."""
    if as_of.tzinfo is None:
        raise ValueError("as_of must be timezone-aware")
    strategy = config["strategy"]
    market_data = config["market_data"]
    ticker = str(market.get("ticker") or "")
    snapshot_ticker = str((orderbook or {}).get("ticker") or "")
    now = as_of.astimezone(timezone.utc)
    close_time = parse_time(market.get("close_time"))
    seconds_remaining = D(str((close_time - now).total_seconds())) if close_time else None
    quantity = intended_quantity(config)
    book = OrderBook()
    book_valid = bool(
        orderbook
        and snapshot_ticker == ticker
        and isinstance(orderbook.get("orderbook_fp"), dict)
    )
    if book_valid:
        book.snapshot(orderbook)
    quotes = book.quotes() if book_valid else {}
    yes_ask = decimal(quotes.get("yes_ask"))
    no_ask = decimal(quotes.get("no_ask"))
    yes_depth = decimal(quotes.get("yes_ask_size")) or D(0)
    no_depth = decimal(quotes.get("no_ask_size")) or D(0)

    reason: str | None = None
    selected_side: str | None = None
    selected_price: Decimal | None = None
    eligible = False
    if not strategy.get("enabled"):
        reason = "STRATEGY_DISABLED"
    elif (
        strategy.get("market_series_scope") == "VALIDATED_BINARY_900S_NON_BTC"
        and ticker.startswith("KXBTC15M-")
    ):
        reason = "MARKET_RESERVED_FOR_FROZEN_BTC_PROFILE"
    elif (
        strategy.get("market_series_scope") == "VALIDATED_BINARY_900S_NON_BTC"
        and (
            not market.get("is_15m_confirmed")
            or market.get("paper_compatibility") != "BINARY_SINGLE_MARKET"
        )
    ):
        reason = "MARKET_STRUCTURE_NOT_ELIGIBLE"
    elif (
        strategy.get("market_series_scope") != "VALIDATED_BINARY_900S_NON_BTC"
        and not ticker.startswith(f"{strategy['market_series']}-")
    ):
        reason = "MARKET_SERIES_NOT_ELIGIBLE"
    elif not market.get("is_15m_confirmed") or market.get("paper_compatibility") != "BINARY_SINGLE_MARKET":
        reason = "MARKET_STRUCTURE_NOT_ELIGIBLE"
    elif str(market.get("market_status", "")).lower() not in {"open", "active"}:
        reason = "MARKET_NOT_OPEN"
    elif has_position:
        reason = "POSITION_ALREADY_OPEN"
    elif submitted_attempt:
        reason = "MARKET_ENTRY_ATTEMPT_ALREADY_SUBMITTED"
    elif close_time is None:
        reason = "MARKET_CLOSE_TIME_UNAVAILABLE"
    elif not book_valid:
        reason = "ORDERBOOK_INVALID_OR_TICKER_MISMATCH"
    elif book_received_at is None:
        reason = "ORDERBOOK_TIMESTAMP_UNAVAILABLE"
    elif book_received_at.tzinfo is None:
        raise ValueError("book_received_at must be timezone-aware")
    else:
        book_age = (now - book_received_at.astimezone(timezone.utc)).total_seconds()
        if book_age < 0 or D(str(book_age)) > D(str(market_data["max_orderbook_age_seconds"])):
            reason = "ORDERBOOK_STALE"
        elif market_data.get("standard_brti_strategy_gate"):
            reason = "BRTI_UNAVAILABLE_OR_STALE"
        elif market_data.get("optional_brti_5hz_required"):
            reason = "BRTI_5HZ_UNAVAILABLE_OR_STALE"
        elif seconds_remaining is None or not (
            D(str(strategy["entry"]["latest_seconds_remaining"]))
            <= seconds_remaining
            <= D(str(strategy["entry"]["earliest_seconds_remaining"]))
        ):
            reason = "OUTSIDE_ENTRY_WINDOW"
        else:
            candidates: list[tuple[str, Decimal, Decimal]] = []
            saw_price = False
            saw_depth_shortfall = False
            low = D(str(strategy["entry"]["min_price"]))
            high = D(str(strategy["entry"]["max_price"]))
            for side, enabled, price, depth in (
                ("YES", strategy["buy_yes"], yes_ask, yes_depth),
                ("NO", strategy["buy_no"], no_ask, no_depth),
            ):
                if not enabled or price is None or not low <= price <= high:
                    continue
                saw_price = True
                if depth < quantity:
                    saw_depth_shortfall = True
                    continue
                candidates.append((side, price, depth))
            if len(candidates) == 1:
                selected_side, selected_price, _ = candidates[0]
                eligible = True
            elif len(candidates) > 1:
                policy = strategy["entry"].get("simultaneous_candidate_policy", "SKIP_AMBIGUOUS")
                if policy == "FIRST_OBSERVED":
                    reason = "SKIP_AMBIGUOUS"
                else:
                    reason = "SKIP_AMBIGUOUS"
            elif saw_depth_shortfall:
                reason = "INSUFFICIENT_EXECUTABLE_DEPTH"
            elif saw_price:
                reason = "INVALID_PRICE_GRID"
            else:
                reason = "NO_QUALIFYING_EXECUTABLE_PRICE"

    result = {
        "decision_id": str(uuid.uuid4()),
        "strategy_version": config["revision"],
        "config_revision": config["revision"],
        "config_sha256": config_sha256,
        "market_ticker": ticker,
        "decision_utc": _iso_utc(as_of),
        "book_received_utc": _iso_utc(book_received_at) if book_received_at else None,
        "seconds_remaining": str(seconds_remaining) if seconds_remaining is not None else None,
        "candidate_direction": selected_side,
        "executable_acquisition_price": str(selected_price) if selected_price is not None else None,
        "intended_quantity": str(quantity),
        "eligible": eligible,
        "rejection_reason": None if eligible else reason,
        "book_valid": book_valid and book.synchronized,
        "executable_quotes": quotes,
        "market_status": market.get("market_status"),
        "paper_compatibility": market.get("paper_compatibility"),
        "market_rules_primary": market.get("rules_primary"),
    }
    return result
