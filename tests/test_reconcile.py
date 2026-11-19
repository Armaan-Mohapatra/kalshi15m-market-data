import json
import sqlite3
import unittest
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from scripts import reconcile_settlements as rs


def _payload(**over):
    base = {"market_ticker": "KXTEST15M-X", "side": "yes", "settlement_result": "yes", "quantity": "2",
            "cost_dollars": "1.4000", "entry_fee_dollars": "0.0294", "settlement_fee_dollars": "0",
            "payout_dollars": "2", "gross_pnl_dollars": "0.6000", "net_pnl_dollars": "0.5706"}
    base.update(over)
    return base


class ReconcileTests(unittest.TestCase):
    def test_recompute_win_and_loss(self):
        self.assertEqual(rs.recompute(_payload())["net"], Decimal("0.5706"))
        lost = rs.recompute(_payload(settlement_result="no"))
        self.assertEqual((lost["payout"], lost["gross"]), (Decimal(0), Decimal("-1.4000")))

    def _ledger(self, directory, payload):
        path = Path(directory) / "l.sqlite3"
        db = sqlite3.connect(path)
        db.execute("create table paper_ledger (idempotency_key text primary key, created_utc text, "
                   "profile_id text, record_type text, payload_json text)")
        db.execute("insert into paper_ledger values ('k','2026-11-19T00:00:00Z','p','PAPER_SETTLEMENT_FINAL',?)",
                   (json.dumps(payload),))
        db.commit()
        db.close()
        return path

    def test_flags_result_mismatch_and_unavailable_outcome_never_invents(self):
        with TemporaryDirectory() as tmp:
            path = self._ledger(tmp, _payload())
            with patch.object(rs, "fetch_market", return_value={"status": "finalized", "result": "no"}):
                report = rs.reconcile(path, 0)
            self.assertEqual(report["counts"], {"DISCREPANCY": 1})
            with patch.object(rs, "fetch_market", return_value=None):
                report = rs.reconcile(path, 0)
            self.assertEqual(report["counts"], {"UNRESOLVED_OFFICIAL_OUTCOME_UNAVAILABLE": 1})
            with patch.object(rs, "fetch_market", return_value={"status": "finalized", "result": "yes"}):
                self.assertEqual(rs.reconcile(path, 0)["counts"], {"MATCH": 1})

    def test_final_outcomes_are_cached_and_not_refetched_but_open_ones_are(self):
        with TemporaryDirectory() as tmp:
            path = self._ledger(tmp, _payload())
            cache = Path(tmp) / "official.json"
            with patch.object(rs, "fetch_market", return_value={"status": "finalized", "result": "yes"}) as fetch:
                first = rs.reconcile(path, 0, cache)
            self.assertEqual((fetch.call_count, first["official_outcomes_fetched_this_run"]), (1, 1))
            with patch.object(rs, "fetch_market") as fetch:
                second = rs.reconcile(path, 0, cache)
            self.assertEqual(fetch.call_count, 0)  # served from the saved official outcome
            self.assertEqual(second["counts"], {"MATCH": 1})
            # a cached outcome that disagrees with the ledger is still reported, not hidden
            bad = self._ledger(Path(tmp) / "x" if (Path(tmp) / "x").mkdir() is None else tmp,
                               _payload(settlement_result="no", payout_dollars="0", gross_pnl_dollars="-1.4000",
                                        net_pnl_dollars="-1.4294"))
            self.assertEqual(rs.reconcile(bad, 0, cache)["counts"], {"DISCREPANCY": 1})
            open_cache = Path(tmp) / "open.json"
            with patch.object(rs, "fetch_market", return_value={"status": "active", "result": ""}):
                rs.reconcile(path, 0, open_cache)
            self.assertFalse(open_cache.exists())  # a market that is not final is never cached


if __name__ == "__main__":
    unittest.main()
