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

    def test_stale_or_never_run_hourly_jobs_alarm_even_if_task_result_would_be_zero(self):
        fresh = {"settlement_reconcile": 600.0, "research_cycle": 1200.0}
        self.assertEqual(pr.alerts(self._report(job_ages_s=fresh)), [])
        stale = pr.alerts(self._report(job_ages_s={"settlement_reconcile": 600.0, "research_cycle": 20000.0}))
        self.assertEqual(len(stale), 1)
        self.assertIn("research_cycle", stale[0])
        never = pr.alerts(self._report(job_ages_s={"settlement_reconcile": None, "research_cycle": 5.0}))
        self.assertIn("ever", never[0])

    def test_job_ages_reads_file_mtimes(self):
        with TemporaryDirectory() as tmp:
            home = Path(tmp)
            (home / "reports").mkdir()
            (home / "reports" / "research_last_run.txt").write_text("x", encoding="utf-8")
            ages = pr.job_ages(home)
            self.assertLess(ages["research_cycle"], 60)
            self.assertIsNone(ages["settlement_reconcile"])  # never ran

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


class NotifyTests(unittest.TestCase):
    class _Run:
        def __init__(self, code=0, boom=False):
            self.calls, self.code, self.boom = [], code, boom

        def __call__(self, args, **kw):
            if self.boom:
                raise OSError("no powershell")
            self.calls.append((args, kw))
            return type("R", (), {"returncode": self.code})()

    def test_only_new_critical_alerts_raise_a_toast_and_text_travels_in_the_environment(self):
        import os
        from unittest.mock import patch

        # independent of the caller's environment: toasts must be enabled for this check
        with patch.dict(os.environ):
            os.environ.pop("PHOENIX_NO_TOAST", None)
            self._check_toast_behaviour()

    def _check_toast_behaviour(self):
        run = self._Run()
        self.assertTrue(pr.notify(["WARNING writer queue at 12000", "CRITICAL archive integrity UNKNOWN"], run=run))
        self.assertEqual(len(run.calls), 1)
        args, kw = run.calls[0]
        self.assertIn("archive integrity UNKNOWN", kw["env"]["PHOENIX_TOAST_TEXT"])
        self.assertNotIn("WARNING", kw["env"]["PHOENIX_TOAST_TEXT"])
        self.assertNotIn("archive integrity", " ".join(args))  # never interpolated into the script
        warning_only = self._Run()
        self.assertFalse(pr.notify(["WARNING writer queue at 12000"], run=warning_only))
        self.assertEqual(warning_only.calls, [])
        self.assertFalse(pr.notify([], run=warning_only))

    def test_toast_app_id_keeps_its_backslashes(self):
        # a normal (non-raw) literal turned  into a vertical tab and broke the toast identity
        self.assertIn(chr(92).join(["}", "WindowsPowerShell", "v1.0", "powershell.exe"]), pr.TOAST_SCRIPT)

    def test_opt_out_and_failures_never_break_the_report(self):
        import os
        from unittest.mock import patch

        with patch.dict(os.environ, {"PHOENIX_NO_TOAST": "1"}):
            quiet = self._Run()
            self.assertFalse(pr.notify(["CRITICAL x"], run=quiet))
            self.assertEqual(quiet.calls, [])
        self.assertFalse(pr.notify(["CRITICAL x"], run=self._Run(boom=True)))  # swallowed
        self.assertFalse(pr.notify(["CRITICAL x"], run=self._Run(code=1)))


class ResourceLogTests(unittest.TestCase):
    def test_process_memory_is_read_for_a_live_pid_and_none_for_a_dead_one(self):
        import os
        import sys

        if sys.platform != "win32":
            self.skipTest("psapi is Windows only")
        mine = pr.process_rss_mb(os.getpid())
        self.assertIsNotNone(mine)
        self.assertGreater(mine, 10.0)  # a running Python interpreter is well over 10 MB
        self.assertIsNone(pr.process_rss_mb(None))
        self.assertIsNone(pr.process_rss_mb(0))
        self.assertIsNone(pr.process_rss_mb(4_194_000))  # no such process

    def test_each_run_appends_one_row_and_the_header_is_written_once(self):
        with TemporaryDirectory() as tmp:
            out = Path(tmp)
            report = {"generated_utc": "t1", "status": {"pid": None, "queue_depth": 7}, "persisted_event_age_s": 2.5}
            row = pr.resource_row(report, out)
            self.assertEqual((row["queue_depth"], row["persisted_lag_s"], row["worker_rss_mb"]), (7, 2.5, None))
            pr.append_resource_log(out, row)
            pr.append_resource_log(out, dict(row, utc="t2"))
            lines = (out / "resource_log.csv").read_text(encoding="utf-8").strip().splitlines()
            self.assertEqual(len(lines), 3)  # header + 2 rows
            self.assertTrue(lines[0].startswith("utc,worker_pid,worker_rss_mb"))
            self.assertEqual([l.split(",")[0] for l in lines[1:]], ["t1", "t2"])

    def test_a_logging_failure_never_breaks_the_report(self):
        pr.append_resource_log(Path("Z:/no/such/folder/for/this/test"), {"utc": "t"})  # swallowed


class StatusWhitelistTests(unittest.TestCase):
    def test_every_status_key_the_alerts_read_is_passed_through_to_the_report(self):
        import inspect
        import re

        source = inspect.getsource(pr.alerts)
        read = set(re.findall(r"""\bs\.get\(["'](\w+)["']""", source))
        self.assertIn("ledger_volume_state", read)  # the case that was silently dropped
        self.assertEqual(sorted(read - set(pr.STATUS_KEYS)), [])  # an alert on a dropped key can never fire

    def test_build_carries_process_identity_and_the_volume_state_through(self):
        from unittest.mock import patch

        live = {"pid": 4242, "journal_process_pid": 4343, "ledger_volume_state": "CRITICAL", "mode": "PAPER_ONLY",
                "exchange_write_calls": 0, "orders_submitted_to_exchange": 0, "queue_depth": 0,
                "archive_integrity_state": "VERIFIED_COMPLETE"}
        with TemporaryDirectory() as tmp, patch.object(pr, "get_status", return_value=(live, "status_api")):
            report = pr.build(Path(tmp), 1)
        self.assertEqual((report["status"]["pid"], report["status"]["journal_process_pid"]), (4242, 4343))
        self.assertTrue(any("journal volume CRITICAL" in a for a in report["alerts"]))  # now actually fires


if __name__ == "__main__":
    unittest.main()
