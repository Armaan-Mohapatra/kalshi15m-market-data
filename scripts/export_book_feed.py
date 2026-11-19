"""Read-only export of one market's replayable book feed (NDJSON) for the Rust adapter.

Only WebSocket snapshots and deltas are exported, in journal arrival order. REST
snapshots are a separate book lineage in the collector and must NOT rebase the delta
replay (validated: replaying WS snapshot + deltas through the collector's own OrderBook
reproduces every recorded quote). Each line carries the native executable_quotes as the
parity oracle. Never writes to the ledger (mode=ro).
usage: python scripts/export_book_feed.py --ticker KXBTC15M-... --out feed.ndjson [--ledger PATH]
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from kalshi15m.storage import decode_journal_payload  # noqa: E402

QUOTE_KEYS = ("yes_bid", "no_bid", "yes_ask", "no_ask")


def export(ledger: Path, ticker: str, out: Path) -> dict:
    db = sqlite3.connect("file:" + ledger.as_posix() + "?mode=ro", uri=True, timeout=60)
    try:
        rows = db.execute(
            "select kind, channel, raw_json from market_events where market_ticker = ? "
            "and channel = 'websocket_orderbook' and kind in ('orderbook_snapshot','orderbook_delta') "
            "order by received_utc, rowid",
            (ticker,),
        ).fetchall()
    finally:
        db.close()
    counts = {"snapshot": 0, "delta": 0, "skipped_unparsable": 0}
    with out.open("w", encoding="utf-8", newline="\n") as handle:
        for kind, _, raw in rows:
            try:
                payload = json.loads(decode_journal_payload(raw))
                outer = payload.get("raw") or {}
                msg = outer.get("msg") or {}
                oracle = payload.get("executable_quotes") or {}
                line = {"seq": outer.get("seq"), "sid": outer.get("sid"), "ticker": ticker,
                        "oracle": {k: oracle.get(k) for k in QUOTE_KEYS}}
                if kind == "orderbook_snapshot":
                    line.update(t="snap", yes=msg.get("yes_dollars_fp") or [], no=msg.get("no_dollars_fp") or [])
                else:
                    line.update(t="delta", side=msg["side"], price=msg["price_dollars"], delta=msg["delta_fp"])
            except (KeyError, ValueError, TypeError):
                counts["skipped_unparsable"] += 1
                continue
            handle.write(json.dumps(line, separators=(",", ":")) + "\n")
            counts["snapshot" if kind == "orderbook_snapshot" else "delta"] += 1
    return counts


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ledger", default=r"E:\PhoenixLab\runtime\k15m-native\data\paper_ledger.sqlite3")
    ap.add_argument("--ticker", required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    print(json.dumps(export(Path(a.ledger), a.ticker, Path(a.out))))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
