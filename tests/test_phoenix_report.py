import json
import sqlite3
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from scripts import phoenix_report as pr


def _ledger(home: Path, pointer: Path | None = None) -> Path:
    path = pointer or home / "data" / "paper_ledger.sqlite3"
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path)
    db.execute("create table paper_ledger (idempotency_key text primary key, created_utc text, "
               "profile_id text, record_type text, payload_json text)")
    rows = [
        ("d1", "P:X", "DECISION", {"eligible": False, "rejection_reason": "OUTSIDE_ENTRY_WINDOW"}),
        ("d2", "P:X", "DECISION", {"eligible": False, "rejection_reason": "OUTSIDE_ENTRY_WINDOW"}),
        ("d3", "P:X", "DECISION", {"eligible": False}),                       # silent no-trade: must be flagged
        ("d4", "P:X", "DECISION", {"eligible": True, "candidate_direction": "YES"}),
        ("f1", "P:X", "PAPER_FILL", {"market_ticker": "M1", "side": "yes"}),
    ]
    for i, (key, profile, kind, payload) in enumerate(rows):
        db.execute("insert into paper_ledger values (?,?,?,?,?)",
                   (key, f"2026-11-19T00:00:{i:02d}Z", profile, kind, json.dumps(payload)))
    db.commit()
    db.close()
    return path


class PhoenixReportTests(unittest.TestCase):
    def test_decision_invariant_counts_explained_and_missing_reasons(self):
        with TemporaryDirectory() as tmp:
            home = Path(tmp)
            _ledger(home)
            decisions = pr.ledger(home)["decisions"]["P"]
            self.assertEqual((decisions["eligible"], decisions["ineligible"]), (1, 3))
            self.assertEqual(decisions["missing_reason"], 1)  # the silent one is surfaced, not hidden
            self.assertEqual(decisions["reasons"]["OUTSIDE_ENTRY_WINDOW"], 2)

    def test_report_follows_the_ledger_pointer_and_renders_html(self):
        with TemporaryDirectory() as tmp:
            home = Path(tmp) / "home"
            fast = Path(tmp) / "fast" / "ledger.sqlite3"
            _ledger(home, pointer=fast)
            (home / "runtime").mkdir(parents=True, exist_ok=True)
            (home / "runtime" / "ledger_path.txt").write_text(str(fast), encoding="utf-8")
            report = pr.build(home, port=1)  # nothing listens on port 1: falls back, never raises
            self.assertEqual(report["status_source"], "UNAVAILABLE")
            self.assertEqual(report["ledger"]["counts"]["P/DECISION"], 4)
            page = pr.render(report)
            self.assertIn("WITHOUT reason", page)
            self.assertIn("OUTSIDE_ENTRY_WINDOW=2", page)


class AlertTests(unittest.TestCase):
    def _report(self, **over):
        base = {"status_source": "status_api", "persisted_event_age_s": 10,
                "status": {"mode": "PAPER_ONLY", "exchange_write_calls": 0, "orders_submitted_to_exchange": 0,
                           "queue_depth": 100, "ledger_volume_state": "OK",
                           "archive_integrity_state": "VERIFIED_COMPLETE"},
                "ledger": {"decisions": {"P": {"missing_reason": 0}}},
                "settlement_reconciliation": {"settlements": 3, "counts": {"MATCH": 3}}}
        base.update(over)
        return base

    def test_healthy_report_has_no_alerts(self):
        self.assertEqual(pr.alerts(self._report()), [])

    def test_each_fault_raises_a_specific_alert(self):
        r = self._report()
        r["status"] = dict(r["status"], exchange_write_calls=1)
        self.assertTrue(any("paper-only invariant" in a for a in pr.alerts(r)))
        r = self._report(persisted_event_age_s=500)
        self.assertTrue(any("500s old" in a for a in pr.alerts(r)))
        r = self._report(ledger={"decisions": {"P": {"missing_reason": 2}}})
        self.assertTrue(any("without a reason" in a for a in pr.alerts(r)))
        r = self._report(settlement_reconciliation={"settlements": 3, "counts": {"MATCH": 2, "DISCREPANCY": 1}})
        self.assertTrue(any("mismatches" in a for a in pr.alerts(r)))
        r = self._report(status_source="UNAVAILABLE", status={})
        self.assertTrue(any("status unavailable" in a for a in pr.alerts(r)))  # unknown is an alarm
        self.assertFalse(any("500s" in a for a in pr.alerts(r)))


if __name__ == "__main__":
    unittest.main()
