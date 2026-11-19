import json
import sqlite3
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from scripts import export_book_feed as ex

QUOTES = {"yes_bid": "0.6000", "no_bid": None, "yes_ask": None, "no_ask": "0.4000"}


def _row(db, key, received, kind, channel, payload):
    db.execute("insert into market_events values (?,?,?,?,?,?,?,?)",
               (key, received, None, channel, kind, "KXT15M-X", json.dumps(payload), "h"))


class ExportBookFeedTests(unittest.TestCase):
    def test_exports_ws_snapshot_and_deltas_in_arrival_order_and_excludes_rest(self):
        with TemporaryDirectory() as tmp:
            ledger = Path(tmp) / "l.sqlite3"
            db = sqlite3.connect(ledger)
            db.execute("create table market_events (dedupe_key text primary key, received_utc text, exchange_ts text, "
                       "channel text, kind text, market_ticker text, raw_json text, payload_sha256 text)")
            ws = lambda t, msg, seq: {"raw": {"seq": seq, "sid": 2, "msg": msg}, "executable_quotes": QUOTES}
            _row(db, "a", "2026-11-19T10:00:00.1", "orderbook_snapshot", "websocket_orderbook",
                 ws("s", {"yes_dollars_fp": [["0.6000", "10.00"]], "no_dollars_fp": []}, 1))
            # REST snapshot (separate lineage) between the WS rows must not appear
            _row(db, "r", "2026-11-19T10:00:00.2", "orderbook_snapshot", "rest_orderbook",
                 {"orderbook_fp": {"yes_dollars": [["0.5000", "1.00"]], "no_dollars": []}, "executable_quotes": QUOTES})
            _row(db, "b", "2026-11-19T10:00:00.3", "orderbook_delta", "websocket_orderbook",
                 ws("d", {"side": "yes", "price_dollars": "0.6000", "delta_fp": "2.00"}, 2))
            _row(db, "bad", "2026-11-19T10:00:00.4", "orderbook_delta", "websocket_orderbook", {"raw": {"msg": {}}})
            db.commit()
            db.close()
            out = Path(tmp) / "feed.ndjson"
            counts = ex.export(ledger, "KXT15M-X", out)
            self.assertEqual(counts, {"snapshot": 1, "delta": 1, "skipped_unparsable": 1})
            lines = [json.loads(x) for x in out.read_text(encoding="utf-8").splitlines()]
            self.assertEqual([x["t"] for x in lines], ["snap", "delta"])
            self.assertEqual(lines[0]["yes"], [["0.6000", "10.00"]])
            self.assertEqual((lines[1]["side"], lines[1]["price"], lines[1]["delta"]), ("yes", "0.6000", "2.00"))
            self.assertEqual(lines[1]["oracle"], QUOTES)


if __name__ == "__main__":
    unittest.main()
