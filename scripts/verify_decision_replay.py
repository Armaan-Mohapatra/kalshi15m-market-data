from __future__ import annotations

import json
import sqlite3
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from kalshi15m.classify import parse_time
from kalshi15m.config import settings
from kalshi15m.crossmarket import load_crossmarket_strategy
from kalshi15m.strategy import evaluate_market, load_frozen_strategy


COMPARE_FIELDS = (
    "strategy_version",
    "config_revision",
    "config_sha256",
    "market_ticker",
    "decision_utc",
    "book_received_utc",
    "seconds_remaining",
    "candidate_direction",
    "executable_acquisition_price",
    "intended_quantity",
    "eligible",
    "rejection_reason",
    "book_valid",
    "executable_quotes",
    "market_status",
    "paper_compatibility",
    "market_rules_primary",
)


def _equal(field: str, expected: Any, actual: Any) -> bool:
    if field == "seconds_remaining":
        if expected is None or actual is None:
            return expected is actual
        return abs(Decimal(str(expected)) - Decimal(str(actual))) <= Decimal("0.000001")
    return expected == actual


def _replay(
    record: dict[str, Any], configs: dict[str, tuple[dict[str, Any], str]]
) -> dict[str, Any]:
    profile_id = str(record.get("profile_id") or "")
    if profile_id.startswith("REFLEX_VNEXT_PAPER:"):
        config, config_hash = configs["btc"]
        is_btc = True
    elif profile_id.startswith("REFLEX_CROSSMARKET_PAPER:"):
        config, config_hash = configs["crossmarket"]
        is_btc = False
    else:
        raise ValueError("unrecognized paper profile")

    ticker = str(record.get("market_ticker") or "")
    decision_at = parse_time(record.get("decision_utc"))
    book_received_at = parse_time(record.get("book_received_utc"))
    if decision_at is None:
        raise ValueError("decision timestamp is unavailable")
    remaining = record.get("seconds_remaining")
    close_time = None
    if remaining is not None:
        micros = int((Decimal(str(remaining)) * Decimal(1_000_000)).to_integral_value())
        close_time = decision_at + timedelta(microseconds=micros)

    market = {
        "ticker": ticker,
        "market_status": record.get("market_status"),
        "is_15m_confirmed": ticker.startswith("KXBTC15M-") if is_btc else True,
        "paper_compatibility": record.get("paper_compatibility"),
        "close_time": close_time.isoformat() if close_time else None,
        "rules_primary": record.get("market_rules_primary"),
    }
    raw_book = record.get("research_orderbook_snapshot")
    book_input = {"ticker": ticker, **raw_book} if isinstance(raw_book, dict) else None
    replayed = evaluate_market(
        config,
        market,
        book_input,
        as_of=decision_at,
        book_received_at=book_received_at,
        submitted_attempt=record.get("rejection_reason")
        == "MARKET_ENTRY_ATTEMPT_ALREADY_SUBMITTED",
        has_position=record.get("rejection_reason") == "POSITION_ALREADY_OPEN",
        config_sha256=config_hash,
    )
    return replayed


def main() -> int:
    btc_config, btc_hash = load_frozen_strategy(Path(__file__).resolve().parents[1])
    cross_config, cross_hash = load_crossmarket_strategy(Path(__file__).resolve().parents[1])
    configs = {"btc": (btc_config, btc_hash), "crossmarket": (cross_config, cross_hash)}
    uri = settings.ledger_path.resolve().as_uri() + "?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=15)
    connection.row_factory = sqlite3.Row
    try:
        integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
        connection.execute("BEGIN")
        rows = connection.execute(
            """SELECT profile_id, record_type, payload_json
               FROM paper_ledger
               WHERE record_type = 'DECISION'
               ORDER BY created_utc, idempotency_key"""
        ).fetchall()
    finally:
        connection.close()

    replayable: list[dict[str, Any]] = []
    skipped_scope = 0
    skipped_missing_context = 0
    for row in rows:
        record = json.loads(row["payload_json"])
        if str(record.get("decision_id") or "").startswith("scope:"):
            skipped_scope += 1
            continue
        if "research_orderbook_snapshot" not in record:
            skipped_missing_context += 1
            continue
        record["profile_id"] = row["profile_id"]
        replayable.append(record)

    profile_counts = {"btc": 0, "crossmarket": 0}
    mismatch_count = 0
    mismatches: list[dict[str, Any]] = []
    eligible_count = 0
    for record in replayable:
        profile = "crossmarket" if str(record["profile_id"]).startswith("REFLEX_CROSSMARKET_PAPER:") else "btc"
        profile_counts[profile] += 1
        eligible_count += int(bool(record.get("eligible")))
        try:
            replayed = _replay(record, configs)
        except Exception as exc:
            mismatch_count += 1
            if len(mismatches) < 20:
                mismatches.append(
                    {
                        "market_ticker": record.get("market_ticker"),
                        "decision_id": record.get("decision_id"),
                        "error": type(exc).__name__,
                    }
                )
            continue
        for field in COMPARE_FIELDS:
            expected = record.get(field)
            actual = replayed.get(field)
            if not _equal(field, expected, actual):
                mismatch_count += 1
                if len(mismatches) < 20:
                    mismatches.append(
                        {
                            "market_ticker": record.get("market_ticker"),
                            "decision_id": record.get("decision_id"),
                            "field": field,
                            "stored": expected,
                            "replayed": actual,
                        }
                    )

    report = {
        "ledger_integrity": integrity,
        "btc_config_sha256": btc_hash,
        "crossmarket_config_sha256": cross_hash,
        "decision_rows": len(rows),
        "scope_classification_rows_skipped": skipped_scope,
        "rows_missing_replay_context": skipped_missing_context,
        "replayed_decisions": len(replayable),
        "replayed_by_profile": profile_counts,
        "stored_eligible_decisions": eligible_count,
        "field_comparisons_per_decision": len(COMPARE_FIELDS),
        "mismatch_count": mismatch_count,
        "mismatches": mismatches,
        "passed": integrity == "ok" and skipped_missing_context == 0 and mismatch_count == 0,
    }
    print(json.dumps(report, indent=2))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
