from __future__ import annotations

from decimal import Decimal, ROUND_CEILING
from typing import Any, Iterable

from .classify import decimal

CENTICENT = Decimal("0.0001")
TAKER_QUADRATIC_RATE = Decimal("0.07")
MAKER_QUADRATIC_RATE = Decimal("0.0175")
FEE_SCHEDULE_URL = "https://kalshi.com/docs/kalshi-fee-schedule.pdf"
FEE_SCHEDULE_EFFECTIVE_DATE = "2026-07-07"
FINAL_OFFICIAL_MARKET_STATUSES = frozenset({"finalized", "settled"})


def has_final_official_settlement(market: dict[str, Any]) -> bool:
    """Require a final exchange market state before realizing paper P&L."""
    status = str(
        market.get("official_market_status") or market.get("status") or ""
    ).strip().lower()
    result = str(
        market.get("official_market_result") or market.get("result") or ""
    ).strip().lower()
    return status in FINAL_OFFICIAL_MARKET_STATUSES and result in {"yes", "no"}


def crossmarket_performance_summary(
    records: Iterable[dict[str, Any]],
    series_categories: dict[str, str],
) -> dict[str, Any]:
    """Aggregate persisted cross-market attempts, fills, and final settlements.

    Asset classes come only from the discovered Kalshi series catalog. Tickers
    without a matching series remain in the UNKNOWN class instead of being
    classified from their names.
    """
    metrics_by_market: dict[str, dict[str, Any]] = {}
    pnl_samples_by_category: dict[str, list[Decimal]] = {}

    def category_for(ticker: str) -> str:
        matches = [
            series_ticker
            for series_ticker in series_categories
            if ticker.startswith(f"{series_ticker}-")
        ]
        if not matches:
            return "UNKNOWN"
        return str(series_categories[max(matches, key=len)] or "UNKNOWN")

    def market_metrics(ticker: str) -> dict[str, Any]:
        return metrics_by_market.setdefault(
            ticker,
            {
                "asset_class": category_for(ticker),
                "attempts": 0,
                "fills": 0,
                "settlements": 0,
                "wins": 0,
                "losses": 0,
                "unresolved_fee_settlements": 0,
                "net_realized_pnl_dollars": Decimal("0"),
                "_net_pnl_samples": [],
            },
        )

    for record in records:
        if not isinstance(record, dict):
            continue
        payload = record.get("payload")
        if not isinstance(payload, dict):
            continue
        ticker = str(payload.get("market_ticker") or "")
        if not ticker:
            continue
        record_type = str(record.get("record_type") or "")
        if record_type == "ENTRY_ATTEMPT":
            market_metrics(ticker)["attempts"] += 1
        elif record_type == "PAPER_FILL":
            filled = decimal(payload.get("filled_quantity"))
            if filled is not None and filled > 0:
                market_metrics(ticker)["fills"] += 1
        elif record_type in {"PAPER_SETTLEMENT", "PAPER_SETTLEMENT_FINAL"}:
            if not has_final_official_settlement(payload):
                continue
            market = market_metrics(ticker)
            market["settlements"] += 1
            side = str(payload.get("side") or "").strip().lower()
            result = str(
                payload.get("settlement_result")
                or payload.get("official_market_result")
                or payload.get("result")
                or ""
            ).strip().lower()
            if side in {"yes", "no"} and result in {"yes", "no"}:
                market["wins" if side == result else "losses"] += 1
            pnl = decimal(payload.get("net_pnl_dollars"))
            if pnl is None:
                market["unresolved_fee_settlements"] += 1
            else:
                market["net_realized_pnl_dollars"] += pnl
                market["_net_pnl_samples"].append(pnl)
                category = str(market["asset_class"])
                pnl_samples_by_category.setdefault(category, []).append(pnl)

    def pnl_statistics(samples: list[Decimal]) -> dict[str, Any]:
        count = len(samples)
        if not count:
            return {
                "net_pnl_observations": 0,
                "known_net_pnl_settlement_coverage": "0.0000",
                "net_pnl_mean_per_known_settlement_dollars": None,
                "net_pnl_sample_stddev_dollars": None,
                "net_pnl_standard_error_dollars": None,
                "known_net_pnl_max_drawdown_dollars": None,
            }

        mean = sum(samples, Decimal("0")) / Decimal(count)
        sample_stddev = None
        standard_error = None
        if count > 1:
            variance = sum(
                ((sample - mean) ** 2 for sample in samples), Decimal("0")
            ) / Decimal(count - 1)
            sample_stddev = variance.sqrt()
            standard_error = sample_stddev / Decimal(count).sqrt()

        cumulative = Decimal("0")
        high_water_mark = Decimal("0")
        maximum_drawdown = Decimal("0")
        for sample in samples:
            cumulative += sample
            high_water_mark = max(high_water_mark, cumulative)
            maximum_drawdown = max(maximum_drawdown, high_water_mark - cumulative)

        def formatted(value: Decimal | None) -> str | None:
            return str(value.quantize(Decimal("0.000001"))) if value is not None else None

        return {
            "net_pnl_observations": count,
            "known_net_pnl_settlement_coverage": None,
            "net_pnl_mean_per_known_settlement_dollars": formatted(mean),
            "net_pnl_sample_stddev_dollars": formatted(sample_stddev),
            "net_pnl_standard_error_dollars": formatted(standard_error),
            "known_net_pnl_max_drawdown_dollars": formatted(maximum_drawdown),
        }

    def wilson_interval(wins: int, resolved: int) -> tuple[str, str] | None:
        if resolved <= 0:
            return None
        z = Decimal("1.95996398454005423552")
        n = Decimal(resolved)
        p = Decimal(wins) / n
        z_squared = z * z
        denominator = Decimal("1") + z_squared / n
        center = (p + z_squared / (Decimal("2") * n)) / denominator
        half_width = (
            z
            * (
                p * (Decimal("1") - p) / n
                + z_squared / (Decimal("4") * n * n)
            ).sqrt()
            / denominator
        )
        return (
            str(max(Decimal("0"), center - half_width).quantize(Decimal("0.0001"))),
            str(min(Decimal("1"), center + half_width).quantize(Decimal("0.0001"))),
        )

    def render(metrics: dict[str, Any]) -> dict[str, Any]:
        wins = int(metrics["wins"])
        losses = int(metrics["losses"])
        resolved = wins + losses
        win_rate = (
            str((Decimal(wins) / Decimal(resolved)).quantize(Decimal("0.0001")))
            if resolved
            else None
        )
        interval = wilson_interval(wins, resolved)
        rendered = {
            key: (str(value) if isinstance(value, Decimal) else value)
            for key, value in metrics.items()
            if not key.startswith("_")
        } | {"resolved_win_rate": win_rate}
        rendered.update(
            {
                "resolved_outcome_count": resolved,
                "resolved_win_rate_wilson_95": (
                    {"lower": interval[0], "upper": interval[1]} if interval else None
                ),
                **pnl_statistics(metrics["_net_pnl_samples"]),
            }
        )
        settlements = int(metrics["settlements"])
        pnl_observations = int(rendered["net_pnl_observations"])
        rendered["known_net_pnl_settlement_coverage"] = (
            str(
                (Decimal(pnl_observations) / Decimal(settlements)).quantize(
                    Decimal("0.0001")
                )
            )
            if settlements
            else "0.0000"
        )
        return rendered

    by_market = {
        ticker: render(metrics)
        for ticker, metrics in sorted(metrics_by_market.items())
    }
    grouped: dict[str, dict[str, Any]] = {}
    for metrics in metrics_by_market.values():
        category = str(metrics["asset_class"])
        total = grouped.setdefault(
            category,
            {
                "markets": 0,
                "attempts": 0,
                "fills": 0,
                "settlements": 0,
                "wins": 0,
                "losses": 0,
                "unresolved_fee_settlements": 0,
                "net_realized_pnl_dollars": Decimal("0"),
                "_net_pnl_samples": [],
            },
        )
        total["markets"] += 1
        for key in (
            "attempts",
            "fills",
            "settlements",
            "wins",
            "losses",
            "unresolved_fee_settlements",
        ):
            total[key] += int(metrics[key])
        total["net_realized_pnl_dollars"] += metrics["net_realized_pnl_dollars"]

    for category, metrics in grouped.items():
        metrics["_net_pnl_samples"] = pnl_samples_by_category.get(category, [])

    return {
        "by_asset_class": {
            category: render(metrics) for category, metrics in sorted(grouped.items())
        },
        "by_market": by_market,
    }


def resolve_fee_terms(market: dict[str, Any]) -> tuple[str | None, Decimal | None]:
    """Resolve event-level fee overrides over series fee metadata."""
    fee_type = market.get("fee_type_override") or market.get("fee_type")
    raw_multiplier = market.get("fee_multiplier_override")
    if raw_multiplier is None:
        raw_multiplier = market.get("fee_multiplier")
    multiplier = decimal(raw_multiplier)
    if fee_type is None:
        return None, multiplier
    return str(fee_type).strip().lower(), multiplier


def fee_for_single_price_fill(
    price_dollars: Any,
    quantity: Any,
    fee_type: str | None,
    fee_multiplier: Any = None,
    liquidity_role: str = "taker",
) -> Decimal | None:
    """Calculate one matched fill using the published quadratic schedule.

    Returns None for fee types with no confirmed formula in this runtime.
    The returned fee is rounded upward to $0.0001 for the whole fill.
    """
    price = decimal(price_dollars)
    contracts = decimal(quantity)
    role = str(liquidity_role).strip().lower()
    if price is None or contracts is None:
        raise ValueError("price and quantity must be decimal values")
    if not Decimal("0") < price < Decimal("1") or contracts <= 0:
        raise ValueError("price must be between 0 and 1 and quantity must be positive")
    if role not in {"maker", "taker"}:
        raise ValueError("liquidity_role must be maker or taker")
    kind = str(fee_type or "").strip().lower()
    if kind not in {"quadratic", "quadratic_with_maker_fees"}:
        return None
    if role == "maker" and kind == "quadratic":
        return Decimal("0")
    if fee_multiplier is None:
        if role == "maker":
            return None
        multiplier = Decimal("1")
    else:
        multiplier = decimal(fee_multiplier)
        if multiplier is None or multiplier < 0:
            raise ValueError("fee multiplier must be a nonnegative decimal value")
    rate = MAKER_QUADRATIC_RATE if role == "maker" else TAKER_QUADRATIC_RATE
    unrounded = multiplier * rate * contracts * price * (Decimal("1") - price)
    position_cost = price * contracts
    rounded_total = (position_cost + unrounded).quantize(CENTICENT, rounding=ROUND_CEILING)
    return rounded_total - position_cost


def simulate_taker_ioc(
    orderbook: dict[str, Any],
    side: str,
    limit_price_dollars: Any,
    requested_quantity: Any,
    fee_type: str | None,
    fee_multiplier: Any = None,
) -> dict[str, Any]:
    """Simulate a marketable limit IOC against a post-latency public book snapshot."""
    normalized_side = str(side).strip().lower()
    if normalized_side not in {"yes", "no"}:
        raise ValueError("side must be yes or no")
    limit_price = decimal(limit_price_dollars)
    requested = decimal(requested_quantity)
    if limit_price is None or requested is None or not Decimal("0") < limit_price < Decimal("1") or requested <= 0:
        raise ValueError("valid limit price and positive requested quantity are required")
    book = orderbook.get("orderbook_fp") or orderbook.get("orderbook") or orderbook
    if not isinstance(book, dict):
        raise ValueError("orderbook snapshot is malformed")
    opposing_key = "no" if normalized_side == "yes" else "yes"
    raw_levels = (
        book.get(f"{opposing_key}_dollars_fp")
        or book.get(f"{opposing_key}_dollars")
        or book.get(opposing_key)
        or []
    )
    levels: list[tuple[Decimal, Decimal]] = []
    for item in raw_levels:
        if not isinstance(item, (tuple, list)) or len(item) < 2:
            continue
        bid_price, depth = decimal(item[0]), decimal(item[1])
        if bid_price is None or depth is None or not Decimal("0") < bid_price < Decimal("1") or depth <= 0:
            continue
        ask_price = Decimal("1") - bid_price
        if ask_price <= limit_price:
            levels.append((ask_price, depth))
    levels.sort(key=lambda level: level[0])

    remaining = requested
    total_cost = Decimal("0")
    total_fees = Decimal("0")
    fees_known = True
    fills: list[dict[str, str | None]] = []
    for price, available in levels:
        if remaining <= 0:
            break
        quantity = min(remaining, available)
        cost = price * quantity
        fee = fee_for_single_price_fill(price, quantity, fee_type, fee_multiplier, "taker")
        total_cost += cost
        if fee is None:
            fees_known = False
        else:
            total_fees += fee
        fills.append(
            {
                "price_dollars": str(price),
                "quantity": str(quantity),
                "cost_dollars": str(cost),
                "taker_fee_dollars": str(fee) if fee is not None else None,
            }
        )
        remaining -= quantity

    filled = requested - remaining
    average_price = total_cost / filled if filled else None
    state = "NO_FILL" if filled == 0 else "FULL_FILL" if remaining == 0 else "PARTIAL_FILL"
    return {
        "execution_model": "TAKER_IOC_TOP_OF_BOOK_DEPTH_AT_ARRIVAL",
        "liquidity_role": "TAKER",
        "time_in_force": "IMMEDIATE_OR_CANCEL",
        "side": normalized_side,
        "limit_price_dollars": str(limit_price),
        "requested_quantity": str(requested),
        "filled_quantity": str(filled),
        "remaining_quantity_cancelled": str(remaining),
        "average_fill_price_dollars": str(average_price) if average_price is not None else None,
        "total_cost_dollars": str(total_cost),
        "entry_fee_dollars": str(total_fees) if fees_known else None,
        "fee_state": "CALCULATED" if fees_known else "FEE_TYPE_UNSUPPORTED_OR_UNKNOWN",
        "fee_type": fee_type,
        "fee_multiplier": str(fee_multiplier) if fee_multiplier is not None else None,
        "fill_state": state,
        "fills": fills,
    }


def settle_binary_paper_fill(
    side: str,
    entry_price_dollars: Any,
    quantity: Any,
    entry_fee_dollars: Any,
    result: str | None,
) -> dict[str, Any]:
    """Settle a caller-provided binary paper fill; this function creates no intent."""
    normalized_side = str(side).strip().lower()
    if normalized_side not in {"yes", "no"}:
        raise ValueError("side must be yes or no")
    price = decimal(entry_price_dollars)
    contracts = decimal(quantity)
    fee = decimal(entry_fee_dollars) if entry_fee_dollars is not None else None
    if price is None or contracts is None or not Decimal("0") <= price <= Decimal("1") or contracts <= 0:
        raise ValueError("entry price and positive quantity are required")
    normalized_result = str(result or "").strip().lower()
    if normalized_result not in {"yes", "no"}:
        return {
            "settlement_state": "UNRESOLVED",
            "side": normalized_side,
            "entry_price_dollars": str(price),
            "quantity": str(contracts),
            "settlement_result": normalized_result or None,
            "payout_dollars": None,
            "gross_pnl_dollars": None,
            "entry_fee_dollars": str(fee) if fee is not None else None,
            "settlement_fee_dollars": "0",
            "net_pnl_dollars": None,
        }
    payout = contracts if normalized_side == normalized_result else Decimal("0")
    cost = price * contracts
    gross = payout - cost
    net = gross - fee if fee is not None else None
    return {
        "settlement_state": "SETTLED",
        "side": normalized_side,
        "entry_price_dollars": str(price),
        "quantity": str(contracts),
        "settlement_result": normalized_result,
        "payout_dollars": str(payout),
        "cost_dollars": str(cost),
        "gross_pnl_dollars": str(gross),
        "entry_fee_dollars": str(fee) if fee is not None else None,
        "settlement_fee_dollars": "0",
        "net_pnl_dollars": str(net) if net is not None else None,
        "net_pnl_state": "CALCULATED" if net is not None else "FEE_UNRESOLVED",
    }
