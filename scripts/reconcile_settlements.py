"""Read-only settlement reconciliation of the paper ledger against Kalshi public REST.

For every PAPER_SETTLEMENT_FINAL record: re-fetch the market, compare result and
status with what the ledger recorded, and recompute payout/gross/net from the
recorded quantity, entry price and fees. Anything that cannot be confirmed is
reported UNRESOLVED; nothing is inferred from prices. Never writes to the ledger.

usage: python scripts/reconcile_settlements.py [--ledger PATH] [--out PATH] [--sleep 0.4]
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from kalshi15m.config import REST_BASE  # noqa: E402

D = Decimal


def fetch_market(ticker: str) -> dict | None:
    request = urllib.request.Request(
        f"{REST_BASE}/markets/{ticker}", headers={"Accept": "application/json", "User-Agent": "k15m-reconcile/1"}
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            return json.load(response).get("market")
    except (urllib.error.URLError, TimeoutError, ValueError):
        return None


def recompute(p: dict) -> dict:
    qty, cost = D(p["quantity"]), D(p["cost_dollars"])
    won = p["side"] == p["settlement_result"]
    payout = qty if won else D(0)
    gross = payout - cost
    net = gross - D(p["entry_fee_dollars"]) - D(p["settlement_fee_dollars"])
    return {"payout": payout, "gross": gross, "net": net}


FINAL_STATUSES = ("finalized", "settled")


def load_official_cache(path: Path | None) -> dict[str, dict]:
    try:
        data = json.loads(path.read_text("utf-8")) if path else {}
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def reconcile(ledger: Path, sleep: float, cache_path: Path | None = None) -> dict:
    """cache_path: official outcomes of finalized/settled markets never change, so once
    fetched they are kept there and not re-requested every run (less load on public REST).
    Only final outcomes are cached; anything else is fetched again next time."""
    official = load_official_cache(cache_path)
    known = len(official)
    fetched_new = 0
    db = sqlite3.connect("file:" + ledger.as_posix() + "?mode=ro", uri=True, timeout=30)
    rows = db.execute(
        "select profile_id, idempotency_key, payload_json from paper_ledger "
        "where record_type = 'PAPER_SETTLEMENT_FINAL' order by created_utc"
    ).fetchall()
    db.close()  # release the file handle (Windows keeps it locked otherwise)
    out, cache = [], {}
    for profile, key, raw in rows:
        p = json.loads(raw)
        ticker = p["market_ticker"]
        if ticker not in cache:
            if ticker in official:
                cache[ticker] = {"status": official[ticker]["status"], "result": official[ticker]["result"]}
            else:
                cache[ticker] = fetch_market(ticker)
                fetched_new += 1
                time.sleep(sleep)
                market = cache[ticker]
                if market and market.get("status") in FINAL_STATUSES and market.get("result") in ("yes", "no"):
                    official[ticker] = {"status": market["status"], "result": market["result"],
                                        "fetched_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
        market = cache[ticker]
        calc = recompute(p)
        issues = []
        if market is None:
            status = "UNRESOLVED_OFFICIAL_OUTCOME_UNAVAILABLE"
        else:
            if market.get("status") not in ("finalized", "settled"):
                issues.append(f"official_status={market.get('status')}")
            if market.get("result") != p["settlement_result"]:
                issues.append(f"result ledger={p['settlement_result']} official={market.get('result')}")
            status = "MATCH"
        for name, field in (("payout", "payout_dollars"), ("gross", "gross_pnl_dollars"), ("net", "net_pnl_dollars")):
            if calc[name] != D(p[field]):
                issues.append(f"{name} recorded={p[field]} recomputed={calc[name]}")
        if issues:
            status = "DISCREPANCY" if market is not None else status
        out.append({"profile": profile, "ticker": ticker, "side": p["side"], "status": status,
                    "issues": issues, "net_pnl_recorded": p["net_pnl_dollars"], "key": key})
    if cache_path is not None and len(official) > known:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = cache_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(official, indent=1, sort_keys=True), encoding="utf-8")
        tmp.replace(cache_path)
    counts: dict[str, int] = {}
    for item in out:
        counts[item["status"]] = counts.get(item["status"], 0) + 1
    by_profile: dict[str, str] = {}
    for item in out:
        by_profile[item["profile"]] = str(D(by_profile.get(item["profile"], "0")) + D(item["net_pnl_recorded"]))
    return {"checked_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "settlements": len(out),
            "counts": counts, "net_pnl_recorded_by_profile": by_profile,
            "official_outcomes_fetched_this_run": fetched_new,
            "unmatched": [i for i in out if i["status"] != "MATCH"]}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--home", default=r"E:\PhoenixLab\runtime\k15m-native")
    ap.add_argument("--ledger", default=None, help="default: the runtime's configured ledger (Settings.ledger_path)")
    ap.add_argument("--out", default=None)
    ap.add_argument("--sleep", type=float, default=0.4)
    args = ap.parse_args()
    from kalshi15m.config import Settings
    cfg = Settings(home=Path(args.home))
    ledger = Path(args.ledger) if args.ledger else cfg.ledger_path
    out_path = args.out or str(cfg.home / "reports" / "settlement_reconciliation.json")
    report = reconcile(ledger, args.sleep, cfg.home / "reports" / "official_outcomes_cache.json")
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    Path(out_path).write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({k: report[k] for k in ("checked_utc", "settlements", "counts", "net_pnl_recorded_by_profile")}))
    for item in report["unmatched"]:
        print("UNMATCHED", item["ticker"], item["status"], item["issues"])
    return 0 if not report["unmatched"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
