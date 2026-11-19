"""Run the scheduled scripts exactly the way Task Scheduler does: `python -I script.py`.

Unit tests import these modules with PYTHONPATH set, which hid an import regression
(ModuleNotFoundError for kalshi15m under isolated mode) that silently broke the report's
ledger section and the research cycle.
"""
import json
import os
import sqlite3
import subprocess
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

ROOT = Path(__file__).resolve().parents[1]


def _isolated(script: str, *args: str) -> subprocess.CompletedProcess:
    env = {k: v for k, v in os.environ.items() if k not in ("PYTHONPATH",)}
    env["PHOENIX_NO_TOAST"] = "1"  # these runs raise real CRITICAL alerts (no worker on the port): no desktop toasts
    return subprocess.run([sys.executable, "-I", str(ROOT / "scripts" / script), *args],
                          capture_output=True, text=True, timeout=120, env=env, cwd=os.environ.get("TEMP"))


def _home(tmp: str) -> Path:
    home = Path(tmp)
    (home / "data").mkdir()
    db = sqlite3.connect(home / "data" / "paper_ledger.sqlite3")
    db.execute("create table paper_ledger (idempotency_key text primary key, created_utc text, "
               "profile_id text, record_type text, payload_json text)")
    db.execute("insert into paper_ledger values ('d','2026-11-19T00:00:00Z','P:X','DECISION',?)",
               (json.dumps({"eligible": False, "rejection_reason": "OUTSIDE_ENTRY_WINDOW"}),))
    db.execute("insert into paper_ledger values ('s','2026-11-19T00:00:01Z','P:X','PAPER_SETTLEMENT_FINAL',?)",
               (json.dumps({"market_ticker": "M1", "net_pnl_dollars": "0.5"}),))
    db.commit()
    db.close()
    return home


class ScheduledScriptTests(unittest.TestCase):
    def test_report_runs_isolated_and_loads_the_ledger(self):
        with TemporaryDirectory() as tmp:
            home = _home(tmp)
            result = _isolated("phoenix_report.py", "--home", str(home), "--port", "1")
            self.assertEqual(result.returncode, 0, result.stderr)
            report = json.loads((home / "reports" / "report.json").read_text(encoding="utf-8"))
            self.assertNotIn("error", report["ledger"], report["ledger"])  # the ModuleNotFoundError case
            self.assertEqual(report["ledger"]["decisions"]["P"]["ineligible"], 1)
            self.assertTrue(any("status unavailable" in a for a in report["alerts"]))  # port 1: unknown = alarm

    def test_research_cycle_runs_isolated_and_is_a_noop_the_second_time(self):
        with TemporaryDirectory() as tmp:
            home = _home(tmp)
            first = _isolated("research_cycle.py", "--home", str(home))
            self.assertEqual(first.returncode, 0, first.stderr)
            self.assertEqual(json.loads(first.stdout)["result"], "PROCESSED")
            second = _isolated("research_cycle.py", "--home", str(home))
            self.assertEqual(json.loads(second.stdout)["result"], "NO_NEW_FINALIZED_MARKETS")


if __name__ == "__main__":
    unittest.main()
