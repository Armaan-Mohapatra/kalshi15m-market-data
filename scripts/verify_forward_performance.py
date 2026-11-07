from __future__ import annotations

import argparse
import json
import math
import sqlite3
import statistics
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from kalshi15m.config import settings
from kalshi15m.paper import has_final_official_settlement


SERIES_CLASS = {
    "KXBTC15M": "BTC",
    "KXAUDUSD15M": "FX",
    "KXEURUSD15M": "FX",
    "KXGBPUSD15M": "FX",
    "KXUSDCAD15M": "FX",
    "KXUSDJPY15M": "FX",
    "KXBNB15M": "non-BTC crypto",
    "KXDOGE15M": "non-BTC crypto",
    "KXETH15M": "non-BTC crypto",
    "KXHYPE15M": "non-BTC crypto",
    "KXNEAR15M": "non-BTC crypto",
    "KXSOL15M": "non-BTC crypto",
    "KXXRP15M": "non-BTC crypto",
    "KXZEC15M": "non-BTC crypto",
    "KXCOPPER15M": "commodities",
    "KXGOLD15M": "commodities",
    "KXNATGAS15M": "commodities",
    "KXPALLADIUM15M": "commodities",
    "KXPLATINUM15M": "commodities",
    "KXSILVER15M": "commodities",
    "KXWTI15M": "commodities",
    # The active Crypto Lead contracts are MVE legs, not standalone binary
    # books. Keep them visible in coverage while the classifier blocks them.
    "KXCRYPTOLEAD15M": "unsupported multi-outcome",
}
PROFILE_PREFIXES = {
    "BTC": "REFLEX_VNEXT_PAPER:",
    "cross-market": "REFLEX_CROSSMARKET_PAPER:",
}


def _status(url: str) -> dict[str, Any]:
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=5) as response:
        if response.status != 200:
            raise RuntimeError(f"status endpoint returned HTTP {response.status}")
        return json.loads(response.read())


def _ledger(path: Path) -> tuple[str, list[dict[str, Any]]]:
    uri = path.resolve().as_uri() + "?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=15)
    connection.row_factory = sqlite3.Row
    try:
        integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
        connection.execute("BEGIN")
        rows = connection.execute(
            """SELECT profile_id, created_utc, idempotency_key, record_type, payload_json
               FROM paper_ledger
               WHERE profile_id LIKE 'REFLEX_VNEXT_PAPER:%'
                  OR profile_id LIKE 'REFLEX_CROSSMARKET_PAPER:%'
               ORDER BY created_utc, idempotency_key"""
        ).fetchall()
        records = []
        for row in rows:
            payload = json.loads(row["payload_json"])
            payload.update(
                {
                    "_profile_id": str(row["profile_id"]),
                    "_created_utc": str(row["created_utc"]),
                    "_idempotency_key": str(row["idempotency_key"]),
                    "_record_type": str(row["record_type"]),
                }
            )
            records.append(payload)
        return integrity, records
    finally:
        connection.close()


def _wilson(wins: int, sample: int) -> tuple[float | None, float | None]:
    if sample == 0:
        return None, None
    z = 1.959963984540054
    rate = wins / sample
    denominator = 1 + z * z / sample
    center = (rate + z * z / (2 * sample)) / denominator
    margin = z * math.sqrt(rate * (1 - rate) / sample + z * z / (4 * sample * sample)) / denominator
    return round(max(0.0, center - margin), 4), round(min(1.0, center + margin), 4)


def _series_key(ticker: Any) -> str:
    return str(ticker or "").split("-", 1)[0]


def _family(profile_id: str) -> str:
    return "BTC" if profile_id.startswith(PROFILE_PREFIXES["BTC"]) else "cross-market"


def _drawdown(items: list[dict[str, Any]]) -> Decimal:
    running = Decimal("0")
    peak = Decimal("0")
    maximum = Decimal("0")
    for item in sorted(items, key=lambda row: (row.get("settlement_observed_utc") or "", row.get("market_ticker") or "")):
        pnl = Decimal(str(item.get("net_pnl_dollars") or "0"))
        running += pnl
        peak = max(peak, running)
        maximum = max(maximum, peak - running)
    return maximum


def _latency(values: list[float]) -> dict[str, Any]:
    ordered = sorted(values)
    if not ordered:
        return {"n": 0, "mean": None, "median": None, "p95_nearest_rank": None, "min": None, "max": None}
    p95 = ordered[max(0, math.ceil(0.95 * len(ordered)) - 1)]
    return {
        "n": len(ordered),
        "mean": round(statistics.mean(ordered), 3),
        "median": round(statistics.median(ordered), 3),
        "p95_nearest_rank": round(p95, 3),
        "min": round(ordered[0], 3),
        "max": round(ordered[-1], 3),
    }


def _metrics(items: list[dict[str, Any]]) -> dict[str, Any]:
    pnl = [Decimal(str(row["net_pnl_dollars"])) for row in items if row.get("net_pnl_dollars") is not None]
    wins = sum(value > 0 for value in pnl)
    lower, upper = _wilson(wins, len(pnl))
    return {
        "settlements": len(pnl),
        "wins": wins,
        "losses": sum(value < 0 for value in pnl),
        "breakeven": sum(value == 0 for value in pnl),
        "win_rate": round(wins / len(pnl), 4) if pnl else None,
        "win_rate_wilson_95_ci": [lower, upper],
        "net_pnl_dollars": str(sum(pnl, Decimal("0")).quantize(Decimal("0.0001"))),
        "max_drawdown_dollars": str(_drawdown(items).quantize(Decimal("0.0001"))),
    }


def audit(source_db: Path, status_url: str) -> dict[str, Any]:
    status = _status(status_url)
    integrity, records = _ledger(source_db)
    series: dict[str, dict[str, Any]] = {}
    for ticker, asset_class in SERIES_CLASS.items():
        series[ticker] = {
            "series_ticker": ticker,
            "asset_class": asset_class,
            "entry_attempts": 0,
            "no_fill_results": 0,
            "full_fills": 0,
            "partial_fills": 0,
            "final_settlements": [],
        }

    attempts: Counter[str] = Counter()
    fill_states: Counter[str] = Counter()
    no_fill_states: Counter[str] = Counter()
    fill_records: list[dict[str, Any]] = []
    filled_position_keys: set[tuple[str, str]] = set()
    execution_records: list[dict[str, Any]] = []
    eligible_decisions = 0
    attempt_times: list[str] = []
    final_settlements: dict[tuple[str, str], dict[str, Any]] = {}
    settlement_priority: dict[tuple[str, str], int] = {}
    unmapped_series: set[str] = set()
    recorded_exchange_writes = 0

    for record in records:
        kind = record["_record_type"]
        ticker = _series_key(record.get("market_ticker"))
        profile = _family(record["_profile_id"])
        if ticker and ticker not in series:
            unmapped_series.add(ticker)
        if kind == "DECISION" and record.get("eligible"):
            eligible_decisions += 1
        if kind == "ENTRY_ATTEMPT":
            attempts[profile] += 1
            attempt_times.append(record["_created_utc"])
            if ticker in series:
                series[ticker]["entry_attempts"] += 1
        if kind == "PAPER_ORDER_RESULT" and record.get("fill_state") == "NO_FILL":
            no_fill_states[str(record.get("fill_state"))] += 1
            if ticker in series:
                series[ticker]["no_fill_results"] += 1
        if kind in {"PAPER_FILL", "PAPER_ORDER_RESULT"} and record.get("simulated_latency_proxy_ms") is not None:
            execution_records.append(record)
        if kind == "PAPER_FILL":
            state = str(record.get("fill_state") or "UNKNOWN")
            fill_states[state] += 1
            fill_records.append(record)
            identity = str(
                record.get("decision_id")
                or record.get("entry_decision_id")
                or record.get("market_ticker")
                or ""
            )
            filled_position_keys.add((record["_profile_id"], identity))
            if ticker in series:
                series[ticker]["full_fills"] += int(state == "FULL_FILL")
                series[ticker]["partial_fills"] += int(state == "PARTIAL_FILL")
        if kind in {"PAPER_SETTLEMENT", "PAPER_SETTLEMENT_FINAL"} and has_final_official_settlement(record):
            identity = str(record.get("entry_decision_id") or record.get("market_ticker") or "")
            key = (record["_profile_id"], identity)
            priority = 2 if kind == "PAPER_SETTLEMENT_FINAL" else 1
            if priority >= settlement_priority.get(key, 0):
                final_settlements[key] = record
                settlement_priority[key] = priority
        recorded_exchange_writes += int(record.get("exchange_write_calls") or 0)

    settlement_values = list(final_settlements.values())
    for record in settlement_values:
        ticker = _series_key(record.get("market_ticker"))
        if ticker in series:
            series[ticker]["final_settlements"].append(record)
    for item in series.values():
        item.update(_metrics(item.pop("final_settlements")))

    classes: dict[str, dict[str, Any]] = {}
    for stat in series.values():
        cls = stat["asset_class"]
        target = classes.setdefault(
            cls,
            {"series_count": 0, "entry_attempts": 0, "no_fill_results": 0, "full_fills": 0, "partial_fills": 0, "settlements": []},
        )
        target["series_count"] += 1
        for key in ("entry_attempts", "no_fill_results", "full_fills", "partial_fills"):
            target[key] += stat[key]
    for record in settlement_values:
        ticker = _series_key(record.get("market_ticker"))
        if ticker in SERIES_CLASS:
            classes[SERIES_CLASS[ticker]]["settlements"].append(record)
    for item in classes.values():
        item.update(_metrics(item.pop("settlements")))

    order_records = execution_records
    latency_values = [float(row["simulated_latency_proxy_ms"]) for row in order_records]
    arrival_values = [float(row["decision_to_arrival_observation_ms"]) for row in order_records if row.get("decision_to_arrival_observation_ms") is not None]
    net_by_profile: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    fees = Decimal("0")
    settlements_by_profile: Counter[str] = Counter()
    for record in settlement_values:
        profile = _family(record["_profile_id"])
        net = Decimal(str(record.get("net_pnl_dollars") or "0"))
        net_by_profile[profile] += net
        fees += Decimal(str(record.get("entry_fee_dollars") or "0"))
        fees += Decimal(str(record.get("settlement_fee_dollars") or "0"))
        settlements_by_profile[profile] += 1

    open_positions = int(status.get("paper_open_positions", 0)) + int(status.get("crossmarket_paper_open_positions", 0))
    status_net = Decimal(str(status.get("paper_realized_net_pnl_dollars", "0"))) + Decimal(
        str(status.get("crossmarket_paper_realized_net_pnl_dollars", "0"))
    )
    ledger_net = sum(net_by_profile.values(), Decimal("0"))
    total_attempts = sum(attempts.values())
    total_fills = len(fill_records)
    total_settlements = len(settlement_values)
    derived_open_positions = len(filled_position_keys - set(final_settlements))
    status_open_positions = int(status.get("paper_open_positions", 0)) + int(
        status.get("crossmarket_paper_open_positions", 0)
    )
    passed = (
        integrity == "ok"
        and not unmapped_series
        and total_attempts == int(status.get("paper_entry_attempts", 0)) + int(status.get("crossmarket_paper_entry_attempts", 0))
        and total_fills == int(status.get("paper_fill_records", 0)) + int(status.get("crossmarket_paper_fill_records", 0))
        and total_settlements == int(status.get("paper_settlement_records", 0)) + int(status.get("crossmarket_paper_settlement_records", 0))
        and derived_open_positions == status_open_positions
        and ledger_net == status_net
        and recorded_exchange_writes == 0
        and int(status.get("exchange_write_calls", 0)) == 0
    )
    now = datetime.now(timezone.utc).isoformat()
    return {
        "schema_version": 1,
        "as_of_utc": now,
        "source": "Read-only standalone paper ledger plus loopback runtime status endpoint",
        "source_db": str(source_db.resolve()),
        "sqlite_integrity": integrity,
        "passed": passed,
        "profile_config_hashes": {
            "btc": status.get("strategy_config_sha256"),
            "crossmarket": status.get("crossmarket_strategy_config_sha256"),
        },
        "runtime": {
            "state": status.get("service_state"),
            "mode": status.get("mode"),
            "pid": status.get("pid"),
            "rest_state": status.get("rest_state"),
            "websocket_state": status.get("websocket_state"),
            "exchange_write_calls": status.get("exchange_write_calls"),
            "errors": status.get("error_count"),
            "api_errors": status.get("market_api_errors"),
            "api_429": status.get("market_api_rate_limit_responses"),
            "active_evaluators": status.get("strategy_all_profiles_evaluators_active"),
            "decisions_persisted": int(status.get("strategy_decisions_persisted", 0)) + int(status.get("crossmarket_decisions_persisted", 0)),
            "feed_ages_seconds": status.get("feed_ages_seconds"),
            "sequence_continuity": "UNKNOWN_NOT_CONNECTED" if status.get("websocket_state") != "CONNECTED" else "REQUIRES_LIVE_GAP_AUDIT",
        },
        "sample": {
            "first_entry_attempt_utc": min(attempt_times) if attempt_times else None,
            "last_entry_attempt_utc": max(attempt_times) if attempt_times else None,
            "entry_attempts": total_attempts,
            "eligible_decisions": eligible_decisions,
            "no_fill_results": sum(no_fill_states.values()),
            "fill_states": dict(fill_states),
            "fills": total_fills,
            "final_official_settlements": total_settlements,
            "open_positions": open_positions,
            "ledger_derived_open_positions": derived_open_positions,
            "partial_fills": fill_states.get("PARTIAL_FILL", 0),
            "total_fees_dollars": str(fees.quantize(Decimal("0.0001"))),
            "btc_realized_net_pnl_dollars": str(net_by_profile["BTC"].quantize(Decimal("0.0001"))),
            "crossmarket_realized_net_pnl_dollars": str(net_by_profile["cross-market"].quantize(Decimal("0.0001"))),
            "combined_realized_net_pnl_dollars": str(ledger_net.quantize(Decimal("0.0001"))),
            "profitable_claim": "NOT_SUPPORTED_SMALL_SAMPLE",
            "simulated_latency_proxy_ms": _latency(latency_values),
            "decision_to_arrival_observation_ms": _latency(arrival_values),
        },
        "by_asset_class": classes,
        "by_series": sorted(series.values(), key=lambda row: row["series_ticker"]),
        "current_market_classification": {
            "active": status.get("market_tickers", []),
            "eligible_crossmarket": status.get("crossmarket_eligible_market_tickers", []),
            "excluded": status.get("crossmarket_excluded_market_tickers", []),
            "blocked_reasons": status.get("crossmarket_blocked_market_reasons", {}),
        },
        "unmapped_series": sorted(unmapped_series),
        "decision_replay": {
            "script": "scripts/verify_decision_replay.py",
            "run_separately": True,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Audit paper fills, final settlements, class/series P&L, and current runtime status read-only."
    )
    parser.add_argument("--source-db", type=Path, default=settings.ledger_path)
    parser.add_argument("--status-url", default="http://127.0.0.1:8768/api/v1/status")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    report = audit(args.source_db, args.status_url)
    rendered = json.dumps(report, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
