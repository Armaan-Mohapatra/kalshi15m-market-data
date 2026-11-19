import json
import sqlite3
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from scripts import research_cycle as rc


def _home(directory, nets):
    home = Path(directory)
    (home / "data").mkdir()
    db = sqlite3.connect(home / "data" / "paper_ledger.sqlite3")
    db.execute("create table paper_ledger (idempotency_key text primary key, created_utc text, "
               "profile_id text, record_type text, payload_json text)")
    add_settlements(db, nets, 0)
    db.commit()
    db.close()
    return home


def add_settlements(db, nets, start):
    for i, net in enumerate(nets, start):
        db.execute("insert into paper_ledger values (?, ?, 'P', 'PAPER_SETTLEMENT_FINAL', ?)",
                   (f"k{i}", f"2026-11-19T00:{i % 60:02d}:00Z",
                    json.dumps({"market_ticker": f"M{i}", "net_pnl_dollars": net})))


class ResearchCycleTests(unittest.TestCase):
    def test_process_then_noop_then_incremental_resume(self):
        with TemporaryDirectory() as tmp:
            home = _home(tmp, ["0.5", "-0.25", "0.5", "-1.0", "0.5", "0.5"])
            first = rc.cycle(home)
            self.assertEqual((first["result"], first["new_settlements"]), ("PROCESSED", 6))
            self.assertEqual(first["profiles"]["P"]["net_pnl"], 0.75)
            self.assertEqual(first["profiles"]["P"]["verdict"], "NO_DEMONSTRATED_EDGE_KEEP_PAPER_ONLY")
            self.assertEqual(rc.cycle(home)["result"], "NO_NEW_FINALIZED_MARKETS")
            db = sqlite3.connect(home / "data" / "paper_ledger.sqlite3")
            add_settlements(db, ["0.1", "0.1"], 6)
            db.commit()
            db.close()
            third = rc.cycle(home)
            self.assertEqual((third["result"], third["new_settlements"]), ("PROCESSED", 2))
            self.assertEqual(third["profiles"]["P"]["settlements"], 8)

    def test_too_few_events_gives_no_interval(self):
        self.assertIsNone(rc.cluster_ci([[1.0], [2.0]], 1))


if __name__ == "__main__":
    unittest.main()
