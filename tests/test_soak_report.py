import json
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from scripts import soak_report as sr


def _facts(**over):
    base = {"mode": "PAPER_ONLY", "exchange_write_calls": 0, "orders_submitted_to_exchange": 0,
            "critical_alerts_in_window": [], "parquet_dropped_rows": 0, "archive_integrity_state": "VERIFIED_COMPLETE",
            "restarts_in_window": 0, "commits_seen_in_window": 1, "uptime_seconds": 25 * 3600,
            "service_state": "RUNNING", "journal_process_alive": True,
            "source_commit": "abc1234def", "source_dirty": False, "heartbeat_age_s": 5,
            "missing_evidence_files": [], "resource_samples": 144,
            "first_resource_offset_s": 600, "last_resource_age_s": 120, "max_resource_gap_s": 600}
    base.update(over)
    return base


class JudgeTests(unittest.TestCase):
    def test_complete_only_after_24_hours_with_everything_clean(self):
        self.assertEqual(sr.judge(_facts()), ("SOAK_COMPLETE", []))
        verdict, why = sr.judge(_facts(uptime_seconds=23.9 * 3600))
        self.assertEqual(verdict, "SOAK_IN_PROGRESS")  # one minute short is not a pass
        self.assertIn("remain", why[0])
        self.assertEqual(sr.judge(_facts(uptime_seconds=24 * 3600))[0], "SOAK_COMPLETE")

    def test_every_safety_or_integrity_problem_fails_even_after_24_hours(self):
        for field, value, expected in (
            ("mode", "LIVE", "mode"), ("exchange_write_calls", 1, "exchange"), ("orders_submitted_to_exchange", 2, "exchange"),
            ("critical_alerts_in_window", ["x"], "CRITICAL"), ("parquet_dropped_rows", 5, "dropped"),
            ("archive_integrity_state", "INCOMPLETE", "integrity"), ("restarts_in_window", 1, "restart"),
            ("commits_seen_in_window", 2, "commit"),
        ):
            verdict, why = sr.judge(_facts(**{field: value}))
            self.assertEqual(verdict, "SOAK_FAILED", field)
            self.assertTrue(any(expected in w for w in why), (field, why))

    def test_a_clean_stop_checkpoint_integrity_state_is_accepted(self):
        self.assertEqual(sr.judge(_facts(archive_integrity_state="VERIFIED_COMPLETE_CLEAN_STOP_CHECKPOINT"))[0], "SOAK_COMPLETE")

    def test_missing_monitoring_evidence_cannot_pass_a_full_day(self):
        for field, bad in (
            ("missing_evidence_files", ["alerts.log"]),
            ("resource_samples", 3),
            ("first_resource_offset_s", 8000),
            ("last_resource_age_s", 2000),
            ("max_resource_gap_s", 1900),
        ):
            verdict, reasons = sr.judge(_facts(**{field: bad}))
            self.assertEqual(verdict, "SOAK_FAILED", field)
            self.assertTrue(reasons, field)

    def test_hung_worker_or_child_never_passes_even_if_uptime_is_long(self):
        for field, bad in (
            ("service_state", "RUNNING_BUT_STALE"),
            ("journal_process_alive", False),
            ("source_dirty", True),
            ("source_commit", None),
            ("heartbeat_age_s", None),
            ("heartbeat_age_s", 650),
        ):
            verdict, reasons = sr.judge(_facts(**{field: bad}))
            self.assertEqual(verdict, "SOAK_FAILED", field)
            self.assertTrue(reasons)

    def test_partial_window_does_not_need_a_full_day_of_resource_samples(self):
        verdict, _ = sr.judge(_facts(uptime_seconds=2 * 3600, resource_samples=10))
        self.assertEqual(verdict, "SOAK_IN_PROGRESS")

    def test_unavailable_status_produces_explicit_failed_verdict_not_crash(self):
        import io
        from contextlib import redirect_stdout

        with patch.object(sr.sys, "argv", ["soak_report.py"]), \
             patch.object(sr, "collect", side_effect=OSError("status unreachable")):
            with redirect_stdout(io.StringIO()) as out:
                code = sr.main()
        self.assertEqual(code, 2)
        self.assertIn("SOAK_FAILED", out.getvalue())
        self.assertIn("OSError", out.getvalue())


class CollectTests(unittest.TestCase):
    def _home(self, tmp, started, now):
        home = Path(tmp)
        (home / "reports").mkdir()
        (home / "logs").mkdir()
        (home / "reports" / "alerts.log").write_text(
            f"{(started - timedelta(hours=1)).isoformat()} NEW CRITICAL before the window\n"
            f"{(started + timedelta(hours=2)).isoformat()} NEW WARNING queue\n"
            f"{(started + timedelta(hours=3)).isoformat()} NEW CRITICAL worker status unavailable\n", encoding="utf-8")
        (home / "logs" / "phoenix-supervisor.log").write_text(
            f"{(started - timedelta(minutes=2)).strftime('%Y-%m-%dT%H:%M:%S.0000000Z')} WORKER_ABSENT_STARTING attempt=1\n"
            f"{(started + timedelta(seconds=90)).strftime('%Y-%m-%dT%H:%M:%S.0000000Z')} RESTART_COMPLETE seconds=90\n", encoding="utf-8")
        (home / "reports" / "resource_log.csv").write_text(
            "utc,worker_pid,worker_rss_mb,journal_pid,journal_rss_mb,free_ram_mb,free_c_gb,free_e_gb,queue_depth,persisted_lag_s\n"
            f"{(started - timedelta(hours=1)).isoformat()},1,999,2,999,1,1,1,0,1\n"
            f"{(started + timedelta(hours=1)).isoformat()},1,130,2,170,1500,23,480,0,4\n", encoding="utf-8")
        return home

    def test_the_restart_that_opened_the_window_and_pre_window_alerts_do_not_count(self):
        now = datetime(2026, 11, 20, 12, 0, tzinfo=timezone.utc)
        started = now - timedelta(hours=4)
        status = {"pid": 7, "started_utc": started.isoformat(), "source_commit": "abc1234def", "source_dirty": False,
                  "mode": "PAPER_ONLY", "exchange_write_calls": 0, "orders_submitted_to_exchange": 0,
                  "parquet_dropped_rows": 0, "archive_integrity_state": "VERIFIED_COMPLETE", "reconnect_count": 3,
                  "sequence_gap_count": 0}
        with TemporaryDirectory() as tmp:
            home = self._home(tmp, started, now)

            class Response:
                def read(self):
                    return json.dumps(status).encode()

                def __enter__(self):
                    return self

                def __exit__(self, *a):
                    return False

            with patch.object(sr.urllib.request, "urlopen", return_value=Response()):
                facts = sr.collect(home, now=now)
        self.assertEqual(facts["restarts_in_window"], 0)  # WORKER_ABSENT_STARTING predates started; RESTART_COMPLETE ignored
        self.assertEqual(len(facts["critical_alerts_in_window"]), 1)  # only the one inside the window
        self.assertEqual(facts["worker_rss_mb"]["max"], 130.0)  # the pre-window 999 MB sample is excluded
        self.assertEqual(sr.judge(facts)[0], "SOAK_FAILED")  # a CRITICAL inside the window fails the soak
        self.assertAlmostEqual(facts["uptime_seconds"], 4 * 3600)


if __name__ == "__main__":
    unittest.main()
