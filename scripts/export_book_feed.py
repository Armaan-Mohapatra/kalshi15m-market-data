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
import os
import sqlite3
import tempfile
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from kalshi15m.storage import decode_journal_payload  # noqa: E402

QUOTE_KEYS = ("yes_bid", "no_bid", "yes_ask", "no_ask")


def export(ledger: Path, ticker: str, out: Path) -> dict:
    """Stream from a read-only SQLite snapshot and publish only a complete feed.

    A skipped corrupt event could make an apparently parity-clean Rust replay
    omit exactly the event that changes the book. Never certify a partial feed.
    The existing destination is preserved until the complete temp is verified.
    """
    db = sqlite3.connect("file:" + ledger.as_posix() + "?mode=ro", uri=True, timeout=60)
    partial: Path | None = None
    counts = {"snapshot": 0, "delta": 0, "skipped_unparsable": 0}
    try:
        cursor = db.execute(
            "select kind, channel, raw_json from market_events where market_ticker = ? "
            "and channel = 'websocket_orderbook' and kind in ('orderbook_snapshot','orderbook_delta') "
            "order by received_utc, rowid",
            (ticker,),
        )
        with tempfile.NamedTemporaryFile(
            "w", encoding="utf-8", newline="\n", dir=out.parent,
            prefix="." + out.name + ".", suffix=".partial", delete=False,
        ) as handle:
            partial = Path(handle.name)
            for kind, _, raw in cursor:
                try:
                    payload = json.loads(decode_journal_payload(raw))
                    outer = payload.get("raw") or {}
                    msg = outer.get("msg") or {}
                    oracle = payload.get("executable_quotes") or {}
                    seq, sid = outer.get("seq"), outer.get("sid")
                    if type(seq) is not int or type(sid) is not int:
                        raise ValueError("missing or invalid source sequence identity")
                    line = {"seq": seq, "sid": sid, "ticker": ticker,
                            "oracle": {k: oracle.get(k) for k in QUOTE_KEYS}}
                    if kind == "orderbook_snapshot":
                        line.update(t="snap", yes=msg.get("yes_dollars_fp") or [],
                                    no=msg.get("no_dollars_fp") or [])
                    else:
                        line.update(t="delta", side=msg["side"],
                                    price=msg["price_dollars"], delta=msg["delta_fp"])
                except (KeyError, ValueError, TypeError, AttributeError):
                    counts["skipped_unparsable"] += 1
                    continue
                handle.write(json.dumps(line, separators=(",", ":")) + "\n")
                counts["snapshot" if kind == "orderbook_snapshot" else "delta"] += 1
        if counts["skipped_unparsable"]:
            raise ValueError(
                f"refusing partial book feed: {counts['skipped_unparsable']} corrupt source event(s)"
            )
        if counts["snapshot"] == 0:
            raise ValueError("refusing book feed without a verified WebSocket snapshot")
        os.replace(partial, out)
        partial = None
        return counts
    finally:
        db.close()
        if partial is not None:
            partial.unlink(missing_ok=True)

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--home", default=r"E:\PhoenixLab\runtime\k15m-native")
    ap.add_argument("--ledger", default=None, help="default: the runtime's configured ledger")
    ap.add_argument("--ticker", required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    from kalshi15m.config import Settings

    ledger = Path(a.ledger) if a.ledger else Settings(home=Path(a.home)).ledger_path
    print(json.dumps(export(ledger, a.ticker, Path(a.out))))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
