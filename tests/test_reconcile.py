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


if __name__ == "__main__":
    unittest.main()
