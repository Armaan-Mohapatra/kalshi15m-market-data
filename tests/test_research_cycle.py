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
            self.assertEqual(first["profiles"]["P"]["verdict"], "INSUFFICIENT_DATA_KEEP_PAPER_ONLY")  # 6 markets
            self.assertEqual(rc.cycle(home)["result"], "NO_NEW_FINALIZED_MARKETS")
            db = sqlite3.connect(home / "data" / "paper_ledger.sqlite3")
            add_settlements(db, ["0.1", "0.1"], 6)
            db.commit()
            db.close()
            third = rc.cycle(home)
            self.assertEqual((third["result"], third["new_settlements"]), ("PROCESSED", 2))
            self.assertEqual(third["profiles"]["P"]["settlements"], 8)

    def test_verdict_is_derived_from_the_interval_and_never_promotes(self):
        self.assertEqual(rc.verdict(10, (1.0, 2.0)), "INSUFFICIENT_DATA_KEEP_PAPER_ONLY")  # too few markets
        self.assertEqual(rc.verdict(100, None), "INSUFFICIENT_DATA_KEEP_PAPER_ONLY")
        self.assertEqual(rc.verdict(100, (-3.0, 4.0)), "NO_DEMONSTRATED_EDGE_KEEP_PAPER_ONLY")  # interval spans 0
        self.assertEqual(rc.verdict(100, (-9.0, -1.0)), "NEGATIVE_NET_RETURN_KEEP_PAPER_ONLY")
        positive = rc.verdict(100, (0.5, 8.0))
        self.assertTrue(positive.startswith("POSITIVE_FORWARD_PAPER"))
        self.assertTrue(positive.endswith("KEEP_PAPER_ONLY"))  # a good number still needs protected OOS

    def test_too_few_events_gives_no_interval(self):
        self.assertIsNone(rc.cluster_ci([[1.0], [2.0]], 1))


if __name__ == "__main__":
    unittest.main()


class HypothesisIntegrationTests(unittest.TestCase):
    def test_the_cycle_reports_the_sealed_h1_experiment_and_a_failure_cannot_break_it(self):
        with TemporaryDirectory() as tmp:
            home = _home(tmp, ["0.5", "-0.25", "0.5", "-1.0", "0.5", "0.5"])
            report = rc.cycle(home)
            h1 = report["hypotheses"]["H1_inversion"]
            self.assertIn("partitions", h1)  # no fills in this fixture: empty partitions, but present and sealed
            self.assertTrue((home / "reports" / "oos_seal.json").exists())
            self.assertEqual(h1["verdict"], "H1_INSUFFICIENT_DATA")
        import hypothesis_inversion as hi
        from unittest.mock import patch

        with TemporaryDirectory() as tmp:
            home = _home(tmp, ["0.5", "-0.25"])
            with patch.object(hi, "evaluate", side_effect=RuntimeError("boom")):
                report = rc.cycle(home)
            self.assertEqual(report["result"], "PROCESSED")  # the cycle itself still completed
            self.assertEqual(report["hypotheses"]["H1_inversion"]["verdict"], "ERROR")
