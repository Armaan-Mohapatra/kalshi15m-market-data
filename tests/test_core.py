import asyncio
import hashlib
import io
import csv
import json
import os
import sqlite3
import subprocess
import sys
import time
import urllib.error
import unittest

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, Mock, patch

from kalshi15m.api import PublicKalshiAPI
from kalshi15m.api import PaginationLimitExceeded
from kalshi15m.auth import ws_auth_headers
from kalshi15m.book import OrderBook
from kalshi15m.classify import duration_seconds, executable_quotes, rules_confirm_15m
from kalshi15m.config import Settings
from kalshi15m.crossmarket import (
    crossmarket_eligibility,
    load_crossmarket_strategy,
)
from kalshi15m.discovery import (
    SCHEDULED_RECONCILIATION_BATCH_SIZE,
    _dedupe_events,
    _market_api_reconciliation_set,
    _scheduled_market_reconciliation_set,
    candidate_series_tickers,
    discover,
    refresh_known_market_rows,
    refresh_upcoming_series_markets,
    series_catalog_changes,
    series_catalog_candidate,
)
from kalshi15m.paper import (
    crossmarket_performance_summary,
    fee_for_single_price_fill,
    has_final_official_settlement,
    resolve_fee_terms,
    settle_binary_paper_fill,
    simulate_taker_ioc,
)
from kalshi15m.strategy import evaluate_market, load_frozen_strategy
from kalshi15m.storage import Storage, decode_journal_payload, encode_journal_payload
from kalshi15m.worker import Collector, RuntimeStatus


class RuntimeStatusTests(unittest.TestCase):
    def test_status_exposes_its_resolved_storage_paths(self):
        with TemporaryDirectory() as root:
            cfg = Settings(home=Path(root))
            status = RuntimeStatus(cfg)

            snapshot = status.snapshot()

            self.assertEqual(snapshot["runtime_home"], str(cfg.home.resolve()))
            self.assertEqual(snapshot["data_dir"], str(cfg.data_dir.resolve()))
            self.assertEqual(snapshot["paper_ledger_path"], str(cfg.ledger_path.resolve()))
            self.assertEqual(snapshot["parquet_dir"], str(cfg.parquet_dir.resolve()))

    def test_status_file_replace_retries_a_transient_windows_lock(self):
        with TemporaryDirectory() as root:
            status = RuntimeStatus(Settings(home=Path(root)))
            original_replace = Path.replace
            attempts = 0

            def replace_after_one_lock(path, target):
                nonlocal attempts
                attempts += 1
                if attempts == 1:
                    raise PermissionError("simulated transient Windows file lock")
                return original_replace(path, target)

            with patch.object(Path, "replace", new=replace_after_one_lock):
                status.update(service_state="RUNNING")

            self.assertEqual(attempts, 2)
            saved = json.loads((Path(root) / "runtime" / "status.json").read_text())
            self.assertEqual(saved["service_state"], "RUNNING")

    def test_high_frequency_status_updates_are_coalesced_on_disk_but_live_in_memory(self):
        with TemporaryDirectory() as root:
            status = RuntimeStatus(Settings(home=Path(root)))
            original_replace = Path.replace
            attempts = 0

            def count_replace(path, target):
                nonlocal attempts
                attempts += 1
                return original_replace(path, target)

            status.update(queue_depth=1)
            with patch.object(Path, "replace", new=count_replace):
                status.update(queue_depth=2)
                self.assertEqual(attempts, 0)
                self.assertEqual(status.snapshot()["queue_depth"], 2)
                status.last_status_file_write_monotonic -= (
                    status.status_file_write_interval_seconds + 1
                )
                status.update(queue_depth=3)

            self.assertEqual(attempts, 1)
            saved = json.loads((Path(root) / "runtime" / "status.json").read_text())
            self.assertEqual(saved["queue_depth"], 3)

    def test_persistent_status_file_lock_does_not_stop_runtime_updates(self):
        with TemporaryDirectory() as root:
            status = RuntimeStatus(Settings(home=Path(root)))
            with patch.object(Path, "replace", side_effect=PermissionError("locked")):
                status.update(service_state="RUNNING")

            self.assertEqual(status.snapshot()["service_state"], "RUNNING")
            self.assertFalse((Path(root) / "runtime" / "status.json").exists())

    def test_market_reconciliation_cadence_covers_15m_rollovers(self):
        self.assertEqual(Settings().rest_min_interval_seconds, 2.0)
        self.assertEqual(Settings().reconcile_interval_seconds, 15)
        self.assertEqual(Settings().book_poll_interval_seconds, 15)
        self.assertEqual(Settings().rollover_preopen_seconds, 300)
        self.assertEqual(Settings().rollover_preflight_lead_seconds, 180)
        self.assertEqual(Settings().rollover_trade_quiet_before_seconds, 45)
        self.assertEqual(Settings().rollover_trade_quiet_after_seconds, 35)
        self.assertEqual(Settings().series_catalog_refresh_seconds, 900)
        self.assertEqual(Settings().rollover_priority_batch_size, 50)

    def test_series_catalog_change_detection_uses_confirmed_frequency_metadata(self):
        prior = {"series": [{"series_ticker": "KEEP"}, {"series_ticker": "REMOVE"}]}
        fresh = [
            {"ticker": "KEEP", "frequency": "fifteen_min"},
            {"ticker": "ADD", "tags": ["15-minute"]},
            {"ticker": "ADD_TITLE", "title": "What will gold do every 15 minutes?"},
            {"ticker": "IGNORE", "frequency": "daily"},
            {
                "ticker": "IGNORE_TICKER_TEXT",
                "series_ticker": "KXGOLD15M",
                "title": "Gold price",
            },
        ]

        self.assertTrue(series_catalog_candidate(fresh[2]))
        self.assertFalse(series_catalog_candidate(fresh[4]))
        self.assertTrue(series_catalog_candidate({"title": "A market window of 900 seconds"}))
        self.assertTrue(series_catalog_candidate({"description": "Moves every fifteen minutes"}))
        self.assertEqual(candidate_series_tickers(fresh), ["ADD", "ADD_TITLE", "KEEP"])
        self.assertEqual(series_catalog_changes(prior, fresh), (["ADD", "ADD_TITLE"], ["REMOVE"]))

    def test_public_trade_poll_yields_shared_rest_budget_around_rollover(self):
        with TemporaryDirectory() as root:
            collector = Collector(Settings(home=Path(root)))
            now = datetime(2026, 10, 14, 21, 29, 22, tzinfo=timezone.utc)
            opening = now + timedelta(seconds=38)
            collector.markets = {
                "KXBTC15M-NEXT": {
                    "ticker": "KXBTC15M-NEXT",
                    "market_status": "initialized",
                    "is_15m_confirmed": True,
                    "open_time": opening.isoformat(),
                    "close_time": (opening + timedelta(minutes=15)).isoformat(),
                },
                "KXUNVERIFIED15M-NEXT": {
                    "ticker": "KXUNVERIFIED15M-NEXT",
                    "market_status": "initialized",
                    "is_15m_confirmed": False,
                    "open_time": (now + timedelta(seconds=5)).isoformat(),
                },
            }

            delay = collector._public_trade_poll_rollover_deferral(now)

            self.assertEqual(delay, (73.0, opening))
            self.assertIsNone(
                collector._public_trade_poll_rollover_deferral(
                    opening + timedelta(seconds=36)
                )
            )

    def test_rollover_preflight_schedules_a_boundary_reconcile(self):
        with TemporaryDirectory() as root:
            collector = Collector(Settings(home=Path(root), rollover_preopen_seconds=180))
            now = datetime.now(timezone.utc)
            collector.markets = {
                "KXBTC15M-NEXT": {
                    "ticker": "KXBTC15M-NEXT",
                    "market_status": "initialized",
                    "is_15m_confirmed": True,
                    "open_time": (now + timedelta(seconds=40)).isoformat(),
                    "close_time": (now + timedelta(minutes=15, seconds=40)).isoformat(),
                },
                "KXETH15M-NEXT": {
                    "ticker": "KXETH15M-NEXT",
                    "market_status": "active",
                    "is_15m_confirmed": True,
                    "open_time": (now + timedelta(seconds=40)).isoformat(),
                    "close_time": (now + timedelta(minutes=15, seconds=40)).isoformat(),
                },
                "KXUNVERIFIED15M-NEXT": {
                    "ticker": "KXUNVERIFIED15M-NEXT",
                    "market_status": "initialized",
                    "is_15m_confirmed": False,
                    "open_time": (now + timedelta(seconds=40)).isoformat(),
                    "close_time": (now + timedelta(minutes=15, seconds=40)).isoformat(),
                },
                "KXBTC15M-LATER": {
                    "ticker": "KXBTC15M-LATER",
                    "market_status": "initialized",
                    "is_15m_confirmed": True,
                    "open_time": (now + timedelta(minutes=15, seconds=40)).isoformat(),
                    "close_time": (now + timedelta(minutes=30, seconds=40)).isoformat(),
                },
            }
            delay = collector._next_inventory_reconcile_delay()
            self.assertAlmostEqual(delay, 40, delta=0.1)
            self.assertEqual(len(collector.rollover_preflighted_groups), 1)
            self.assertIsNotNone(collector.pending_rollover_check)
            assert collector.pending_rollover_check is not None
            self.assertEqual(
                collector.pending_rollover_check[0],
                collector.pending_rollover_check[1].isoformat(),
            )
            self.assertEqual(
                collector.pending_rollover_tickers,
                {"KXBTC15M-NEXT", "KXETH15M-NEXT"},
            )

    def test_rollover_preflight_begins_before_rotating_series_scan_deadline(self):
        with TemporaryDirectory() as root:
            collector = Collector(
                Settings(
                    home=Path(root),
                    reconcile_interval_seconds=15,
                    rollover_preflight_lead_seconds=180,
                )
            )
            now = datetime.now(timezone.utc)
            opening = now + timedelta(seconds=120)
            collector.markets = {
                "KXBTC15M-NEXT": {
                    "ticker": "KXBTC15M-NEXT",
                    "market_status": "initialized",
                    "is_15m_confirmed": True,
                    "open_time": opening.isoformat(),
                    "close_time": (opening + timedelta(minutes=15)).isoformat(),
                }
            }

            delay = collector._next_inventory_reconcile_delay()

            self.assertAlmostEqual(delay, 120, delta=0.1)
            self.assertEqual(
                collector.pending_rollover_tickers,
                {"KXBTC15M-NEXT"},
            )
            self.assertEqual(collector.status.snapshot()["rollover_priority_reconcile_state"], "PREPARED")

    def test_postopen_reconcile_defers_only_one_upcoming_series_scan(self):
        with TemporaryDirectory() as root:
            collector = Collector(Settings(home=Path(root)))
            collector.rollover_postopen_reconcile_pending = True

            self.assertTrue(collector._skip_upcoming_scan_for_postopen_reconcile())
            self.assertFalse(collector._skip_upcoming_scan_for_postopen_reconcile())

    def test_rollover_detection_uses_new_open_window_and_survives_empty_inventory(self):
        with TemporaryDirectory() as root:
            collector = Collector(Settings(home=Path(root)))
            prior_open = datetime(2026, 10, 14, 20, 45, tzinfo=timezone.utc)
            next_open = datetime(2026, 10, 14, 21, 0, tzinfo=timezone.utc)
            collector.previous_current_open_windows = {prior_open}

            self.assertEqual(
                collector._observe_rollover_transition(set(), next_open),
                (0, set(), None, None),
            )
            self.assertEqual(collector.previous_current_open_windows, {prior_open})

            collector.markets = {
                "KXBTC15M-NEXT": {
                    "ticker": "KXBTC15M-NEXT",
                    "event_ticker": "KXBTC15M-NEXT-EVENT",
                    "open_time": next_open.isoformat(),
                }
            }
            observed_at = next_open + timedelta(seconds=14)
            count, tickers, scheduled, latency = collector._observe_rollover_transition(
                {"KXBTC15M-NEXT"}, observed_at
            )

            self.assertEqual(count, 1)
            self.assertEqual(tickers, {"KXBTC15M-NEXT"})
            self.assertEqual(scheduled, next_open)
            self.assertEqual(latency, 14.0)
            self.assertEqual(collector.previous_current_open_windows, {next_open})

            self.assertEqual(
                collector._observe_rollover_transition({"KXBTC15M-NEXT"}, observed_at),
                (0, set(), None, None),
            )

    def test_preflighted_first_window_is_counted_without_prior_active_window(self):
        with TemporaryDirectory() as root:
            collector = Collector(Settings(home=Path(root)))
            next_open = datetime(2026, 10, 15, 9, 0, tzinfo=timezone.utc)
            collector.status.update(
                rollover_preflight_group_open_utc=next_open.isoformat()
            )
            collector.markets = {
                "KXBTC15M-NEXT": {
                    "ticker": "KXBTC15M-NEXT",
                    "event_ticker": "KXBTC15M-NEXT-EVENT",
                    "open_time": next_open.isoformat(),
                }
            }

            count, tickers, scheduled, latency = collector._observe_rollover_transition(
                {"KXBTC15M-NEXT"}, next_open + timedelta(seconds=14)
            )

            self.assertEqual(count, 1)
            self.assertEqual(tickers, {"KXBTC15M-NEXT"})
            self.assertEqual(scheduled, next_open)
            self.assertEqual(latency, 14.0)

    def test_inventory_refresh_status_separates_cached_snapshot_and_reconcile_time(self):
        with TemporaryDirectory() as root:
            collector = Collector(Settings(home=Path(root), reconcile_interval_seconds=15))
            snapshot = {
                "snapshot_utc": "2026-10-14T18:00:00+00:00",
                "series": [{"ticker": "KXBTC15M", "category": "Crypto"}],
                "markets": [],
                "fifteen_min_series": 1,
                "series_api_rows": 1,
                "active_15m_market_rows": 0,
                "api_errors": 0,
                "discovery_coverage_state": "COMPLETE_CURRENT_AND_NEAR_TERM",
                "discovery_coverage_errors": [],
                "event_pagination_truncated": [],
                "mve_latest_page_truncated_series": [],
                "rest_settlements_observed": 0,
                "market_event_rows": 0,
            }
            with patch("kalshi15m.worker.discover", new=AsyncMock(return_value=snapshot)):
                asyncio.run(collector.refresh_inventory())

            status = collector.status.snapshot()
            self.assertEqual(status["inventory_snapshot_utc"], snapshot["snapshot_utc"])
            self.assertGreaterEqual(
                datetime.fromisoformat(status["inventory_last_reconciled_utc"]),
                datetime.fromisoformat(snapshot["snapshot_utc"]),
            )
            self.assertEqual(status["inventory_reconcile_interval_seconds"], 15)
            self.assertGreaterEqual(status["inventory_reconcile_duration_seconds"], 0)
            self.assertEqual(status["inventory_reconcile_request_count"], 0)
            self.assertEqual(status["inventory_reconcile_error_count"], 0)

    def test_empty_trade_poll_is_fresh_without_fabricating_trade_activity(self):
        with TemporaryDirectory() as root:
            status = RuntimeStatus(Settings(home=Path(root)))
            status.mark_rest_poll(
                "rest_public_trades",
                started_utc="2026-10-14T18:00:00+00:00",
                request_count=26,
                successful_request_count=26,
                requested_market_count=26,
                markets_with_data=0,
                rows_received=0,
                error_count=0,
            )
            status.refresh_ages()

            snapshot = status.snapshot()
            self.assertEqual(snapshot["rest_poll_health"]["rest_public_trades"], "READY")
            self.assertEqual(snapshot["last_rest_poll_by_channel"]["rest_public_trades"]["rows_received"], 0)
            self.assertLess(snapshot["rest_poll_ages_seconds"]["rest_public_trades"], 1.0)
            self.assertNotIn("rest_public_trades", snapshot["last_by_channel"])

    def test_rest_poll_errors_mark_channel_degraded(self):
        with TemporaryDirectory() as root:
            status = RuntimeStatus(Settings(home=Path(root)))
            status.mark_rest_poll(
                "rest_orderbook",
                started_utc="2026-10-14T18:00:00+00:00",
                request_count=2,
                successful_request_count=1,
                requested_market_count=26,
                markets_with_data=13,
                rows_received=13,
                error_count=1,
            )
            snapshot = status.snapshot()
            self.assertEqual(snapshot["rest_poll_health"]["rest_orderbook"], "DEGRADED")
            self.assertEqual(snapshot["last_rest_poll_by_channel"]["rest_orderbook"]["error_count"], 1)

    def test_rest_orderbook_poll_health_marks_stale_after_two_intervals(self):
        with TemporaryDirectory() as root:
            status = RuntimeStatus(
                Settings(home=Path(root), book_poll_interval_seconds=15)
            )
            status.mark_rest_poll(
                "rest_orderbook",
                started_utc="2026-10-14T18:00:00+00:00",
                request_count=1,
                successful_request_count=1,
                requested_market_count=26,
                markets_with_data=26,
                rows_received=26,
                error_count=0,
            )
            old_completion = datetime.now(timezone.utc) - timedelta(seconds=31)
            status.data["last_rest_poll_by_channel"]["rest_orderbook"][
                "completed_utc"
            ] = old_completion.isoformat()

            status.refresh_ages()

            snapshot = status.snapshot()
            self.assertEqual(snapshot["rest_poll_health"]["rest_orderbook"], "STALE")
            self.assertEqual(
                snapshot["rest_poll_stale_thresholds_seconds"]["rest_orderbook"], 30
            )

    def test_strategy_book_health_marks_stale_only_while_poll_is_due(self):
        with TemporaryDirectory() as root:
            status = RuntimeStatus(
                Settings(home=Path(root), strategy_poll_interval_seconds=2)
            )
            status.mark_rest_poll(
                "rest_strategy_orderbook",
                started_utc=datetime.now(timezone.utc).isoformat(),
                request_count=1,
                successful_request_count=1,
                requested_market_count=1,
                markets_with_data=1,
                rows_received=1,
                error_count=0,
            )
            status.data["strategy_rest_poll_health"] = "READY"
            status.data["last_rest_poll_by_channel"]["rest_strategy_orderbook"][
                "completed_utc"
            ] = (datetime.now(timezone.utc) - timedelta(seconds=6)).isoformat()

            status.refresh_ages()
            stale = status.snapshot()
            self.assertEqual(stale["rest_poll_health"]["rest_strategy_orderbook"], "STALE")
            self.assertEqual(stale["strategy_rest_poll_health"], "STALE")

            status.mark_rest_poll_not_due(
                "rest_strategy_orderbook",
                "NOT_DUE_OUTSIDE_ENTRY_WINDOW",
                profile_health_field="strategy_rest_poll_health",
            )
            idle = status.snapshot()
            self.assertEqual(
                idle["rest_poll_health"]["rest_strategy_orderbook"],
                "NOT_DUE_OUTSIDE_ENTRY_WINDOW",
            )
            self.assertEqual(idle["strategy_rest_poll_health"], "NOT_DUE_OUTSIDE_ENTRY_WINDOW")


class StackSamplingTests(unittest.TestCase):
    def test_config_parsing_is_strict(self):
        from kalshi15m.worker import parse_stack_sample_config as parse

        self.assertEqual(parse("1 240\n"), (1.0, 240.0))
        self.assertEqual(parse("  0.5   60 "), (0.5, 60.0))
        for bad in ("", "x y", "1", "0.01 60", "1 0", "100 60", "1 99999"):
            self.assertIsNone(parse(bad), bad)

    def test_disabled_without_the_file_and_bounded_when_enabled(self):
        import faulthandler

        from kalshi15m.worker import start_stack_sampling

        with TemporaryDirectory() as tmp:
            runtime, logs = Path(tmp) / "runtime", Path(tmp) / "logs"
            runtime.mkdir()
            logs.mkdir()
            self.assertFalse(start_stack_sampling(runtime, logs))
            self.assertFalse((logs / "stack_samples.log").exists())
            (runtime / "stack_sample.txt").write_text("0.2 1", encoding="utf-8")
            try:
                self.assertTrue(start_stack_sampling(runtime, logs))
                self.assertFalse((runtime / "stack_sample.txt").exists())  # request consumed
                self.assertTrue((runtime / "stack_sample.started.txt").exists())
                self.assertFalse(start_stack_sampling(runtime, logs))  # a running tick does not restart it
                time.sleep(0.7)
            finally:
                faulthandler.cancel_dump_traceback_later()
            self.assertIn("most recent call first", (logs / "stack_samples.log").read_text(encoding="utf-8"))


class GilSwitchIntervalTests(unittest.TestCase):
    def test_tuning_shortens_the_switch_interval_and_collector_applies_it(self):
        from kalshi15m.worker import GIL_SWITCH_INTERVAL_SECONDS, tune_gil_switch_interval

        before = sys.getswitchinterval()
        try:
            sys.setswitchinterval(0.005)
            tune_gil_switch_interval()
            self.assertAlmostEqual(sys.getswitchinterval(), GIL_SWITCH_INTERVAL_SECONDS, places=6)
            self.assertLessEqual(GIL_SWITCH_INTERVAL_SECONDS, 0.0005)  # 1 ms was NOT enough when measured
            sys.setswitchinterval(0.005)
            with TemporaryDirectory() as tmp:
                Collector(Settings(home=Path(tmp), min_free_bytes=0))
            self.assertAlmostEqual(sys.getswitchinterval(), GIL_SWITCH_INTERVAL_SECONDS, places=6)
        finally:
            sys.setswitchinterval(before)


class ManifestHeadCacheTests(unittest.TestCase):
    def test_appends_and_checkpoints_do_not_reread_the_manifest(self):
        with TemporaryDirectory() as tmp:
            cfg = Settings(home=Path(tmp), min_free_bytes=0)
            store, _ = _flushed_store(cfg, [2])  # first flush loads the cache once
            real = store._manifest_read
            reads = []
            store._manifest_read = lambda: (reads.append(1), real())[1]
            for start in (2, 3, 4):
                store.append_batch([_trade(start)])
                store.flush_parquet(reason="TEST")
            self.assertEqual(reads, [])  # no full re-read per flush
            self.assertTrue(store.verify_archive(full=True)["ok"])  # on-disk chain is intact
            record = json.loads(store.archive_checkpoint_path.read_text(encoding="utf-8"))
            self.assertEqual(record["manifest_seq"], len(real()[0]))
            self.assertEqual(record["manifest_head"], real()[0][-1]["hash"])
            store.close()

    def test_failed_append_drops_the_cache_and_the_chain_stays_valid(self):
        with TemporaryDirectory() as tmp:
            cfg = Settings(home=Path(tmp), min_free_bytes=0)
            store, _ = _flushed_store(cfg, [2])
            store.append_batch([_trade(2)])
            real_fsync, calls = os.fsync, []

            def second_fsync_fails(fd):  # 1st = the Parquet file, 2nd = the manifest line
                calls.append(1)
                if len(calls) == 2:
                    raise OSError(5, "I/O error")
                return real_fsync(fd)

            with patch("kalshi15m.storage.os.fsync", side_effect=second_fsync_fails):
                store.flush_parquet(reason="TEST")
            self.assertEqual(store.archive_manifest_failures, 1)
            self.assertIsNone(store._manifest_cache)  # unknown state: next use re-reads the file
            store.append_batch([_trade(3)])
            store.flush_parquet(reason="TEST")  # re-reads, appends; checkpoint stays frozen
            self.assertIsNotNone(store._manifest_cache)
            self.assertTrue(store.verify_archive(full=True)["ok"])
            self.assertEqual(sorted(_archived_keys(cfg)), _expected(4))
            store.close()


class AdaptiveFlushThresholdTests(unittest.TestCase):
    def test_slow_flushes_double_the_threshold_to_a_cap_and_fast_ones_decay_to_base(self):
        with TemporaryDirectory() as tmp:
            store = Storage(Settings(home=Path(tmp), min_free_bytes=0))
            self.assertEqual(store.parquet_flush_rows, 2000)
            seen = []
            for _ in range(5):
                store._adapt_flush_rows(5.0)  # slow flush
                seen.append(store.parquet_flush_rows)
            self.assertEqual(seen, [4000, 8000, 16000, 16000, 16000])  # capped
            for _ in range(5):
                store._adapt_flush_rows(0.05)  # fast flush
            self.assertEqual(store.parquet_flush_rows, 2000)  # back to base, not below
            store._adapt_flush_rows(0.5)  # in between: unchanged
            self.assertEqual(store.parquet_flush_rows, 2000)
            store.parquet_flush_rows = 1  # an explicit smaller value is left alone by fast flushes
            store._adapt_flush_rows(0.05)
            self.assertEqual(store.parquet_flush_rows, 1)

    def test_real_flush_reports_threshold_in_status(self):
        with TemporaryDirectory() as tmp:
            store = Storage(Settings(home=Path(tmp), min_free_bytes=0))
            store.open()
            store.append_batch([_trade(i) for i in range(2100)])
            self.assertIn("parquet_flush_rows_threshold", store.connection_status())
            store.close()


class WriterPhaseTimingTests(unittest.TestCase):
    def test_phases_are_recorded_and_exposed_in_status(self):
        with TemporaryDirectory() as tmp:
            store = Storage(Settings(home=Path(tmp), min_free_bytes=0))
            store.open()
            store.append_batch([_trade(i) for i in range(2100)])  # crosses the 2000-row flush threshold
            phases = store.connection_status()["writer_phase_ms_ema"]
            for name in ("append_prepare", "append_insert", "append_commit", "append_tail",
                         "append_parquet_buffer_and_flush", "flush_write_files", "flush_manifest",
                         "flush_wal_barrier", "flush_checkpoint_file"):
                self.assertIn(name, phases)
                self.assertGreaterEqual(phases[name], 0.0)
            store.close()


class LedgerVolumeTests(unittest.TestCase):
    def test_ledger_path_override_and_default(self):
        with TemporaryDirectory() as tmp:
            self.assertEqual(Settings(home=Path(tmp)).ledger_path, Path(tmp) / "data" / "paper_ledger.sqlite3")
            elsewhere = Path(tmp) / "fast" / "j.sqlite3"
            cfg = Settings(home=Path(tmp) / "h", ledger_override=str(elsewhere))
            cfg.ensure_dirs()
            self.assertEqual(cfg.ledger_path, elsewhere)
            self.assertTrue(elsewhere.parent.is_dir())
            store = Storage(cfg)
            store.open()
            store.append_batch([_trade(1)])
            self.assertTrue(elsewhere.exists())
            self.assertEqual(store.sqlite_open_path, elsewhere.resolve())
            store.close()

    def test_pointer_file_relocates_the_ledger_and_env_wins(self):
        with TemporaryDirectory() as tmp:
            home = Path(tmp) / "h"
            cfg = Settings(home=home)
            self.assertEqual(cfg.ledger_path, home / "data" / "paper_ledger.sqlite3")
            (home / "runtime").mkdir(parents=True)
            target = Path(tmp) / "fast" / "ledger.sqlite3"
            (home / "runtime" / "ledger_path.txt").write_text(f"{target}\n", encoding="utf-8")
            self.assertEqual(Settings(home=home).ledger_path, target)  # every consumer follows it
            other = Path(tmp) / "env.sqlite3"
            self.assertEqual(Settings(home=home, ledger_override=str(other)).ledger_path, other)
            (home / "runtime" / "ledger_path.txt").write_text("  \n", encoding="utf-8")  # blank = unset
            self.assertEqual(Settings(home=home).ledger_path, home / "data" / "paper_ledger.sqlite3")

    def test_volume_state_thresholds(self):
        cfg = Settings(ledger_min_free_bytes=5000, ledger_critical_free_bytes=1000)
        self.assertEqual(
            [cfg.ledger_volume_state(v) for v in (999, 1000, 4999, 5000)],
            ["CRITICAL", "LOW", "LOW", "OK"],
        )


class JournalDurabilityPolicyTests(unittest.TestCase):
    def test_events_use_configured_mode_but_paper_records_commit_full(self):
        with TemporaryDirectory() as temporary:
            store = Storage(Settings(home=Path(temporary), min_free_bytes=0, event_synchronous="NORMAL"))
            store.open()
            self.assertEqual(store.connection.execute("PRAGMA synchronous").fetchone()[0], 1)  # NORMAL
            modes = []
            real = store._set_synchronous
            store._set_synchronous = lambda mode: (modes.append(mode), real(mode))[1]
            store.append_batch([_trade(1)])
            self.assertEqual(modes, [])  # event batches never raise the policy
            store.append_paper_record("k1", "p", "DECISION", {"x": 1})
            self.assertEqual(modes, ["FULL", "NORMAL"])
            self.assertEqual(store.connection.execute("PRAGMA synchronous").fetchone()[0], 1)
            store.close()

    def test_checkpoint_waits_for_the_journal_durability_barrier(self):
        with TemporaryDirectory() as temporary:
            cfg = Settings(home=Path(temporary), min_free_bytes=0)
            store = Storage(cfg)
            store.open()
            store.append_batch([_trade(1)])
            with patch.object(store, "_make_journal_durable", return_value=False):
                store.flush_parquet(reason="TEST")
            self.assertFalse(store.archive_checkpoint_path.exists())
            self.assertEqual(store.archive_checkpoint_write_failures, 1)
            store.append_batch([_trade(2)])
            store.flush_parquet(reason="TEST")
            self.assertEqual(
                json.loads(store.archive_checkpoint_path.read_text(encoding="utf-8"))["through_rowid"], 2
            )
            store.close()

    def test_unsupported_mode_is_rejected(self):
        with TemporaryDirectory() as temporary:
            with self.assertRaises(ValueError):
                Storage(Settings(home=Path(temporary), event_synchronous="OFF")).open()


class SourceFingerprintTests(unittest.TestCase):
    def test_status_reports_git_head_or_explicit_unknown(self):
        from kalshi15m.worker import source_fingerprint

        fingerprint = source_fingerprint()
        commit = fingerprint["source_commit"]
        self.assertTrue(commit == "UNKNOWN" or (len(commit) == 40 and int(commit, 16) >= 0))
        with patch("subprocess.run", side_effect=FileNotFoundError):
            self.assertEqual(source_fingerprint(), {"source_commit": "UNKNOWN", "source_dirty": None})
        with TemporaryDirectory() as temporary:
            status = RuntimeStatus(Settings(home=Path(temporary)))
            self.assertIn("source_commit", status.snapshot())


class WebSocketAuthTests(unittest.TestCase):
    def test_generic_exchange_credentials_do_not_enable_market_data_socket(self):
        with patch.dict(
            os.environ,
            {
                "KALSHI_API_KEY_ID": "another-runtime-key",
                "KALSHI_PRIVATE_KEY_PATH": "C:\\not-a-real-key.pem",
                "KALSHI15M_MARKETDATA_API_KEY_ID": "",
                "KALSHI15M_MARKETDATA_PRIVATE_KEY_PATH": "",
            },
        ):
            self.assertIsNone(ws_auth_headers())


class ClassificationTests(unittest.TestCase):
    def test_duration_comes_from_times_and_rules_not_ticker(self):
        market = {
            "ticker": "anything",
            "open_time": "2026-10-14T10:00:00Z",
            "close_time": "2026-10-14T10:15:00Z",
            "rules_primary": "The index value over a 15-minute window determines the result.",
        }
        self.assertEqual(duration_seconds(market), 900)
        self.assertTrue(rules_confirm_15m(market))
        market["ticker"] = "KXABC15M"
        market["rules_primary"] = "The index value at expiration determines the result."
        self.assertFalse(rules_confirm_15m(market))


class FrozenStrategyTests(unittest.TestCase):
    def setUp(self):
        self.config, self.config_hash = load_frozen_strategy(Path(__file__).resolve().parents[1])
        self.now = datetime.now(timezone.utc)
        self.market = {
            "ticker": "KXBTC15M-SAMPLE",
            "market_status": "active",
            "is_15m_confirmed": True,
            "paper_compatibility": "BINARY_SINGLE_MARKET",
            "open_time": (self.now - timedelta(minutes=14, seconds=30)).isoformat(),
            "close_time": (self.now + timedelta(seconds=30)).isoformat(),
            "rules_primary": "15-minute BTC market",
        }
        self.orderbook = {
            "ticker": self.market["ticker"],
            "orderbook_fp": {
                "yes_dollars_fp": [["0.30", "3.00"]],
                "no_dollars_fp": [["0.20", "3.00"]],
            },
        }

    def evaluate(self, market=None, orderbook=None, received=None):
        return evaluate_market(
            self.config,
            market or self.market,
            orderbook or self.orderbook,
            as_of=self.now,
            book_received_at=received or self.now,
            config_sha256=self.config_hash,
        )

    def test_active_configuration_eligible_side_and_depth_are_frozen(self):
        decision = self.evaluate()
        self.assertTrue(decision["eligible"])
        self.assertEqual(decision["candidate_direction"], "YES")
        self.assertEqual(decision["executable_acquisition_price"], "0.8000")
        self.assertEqual(decision["intended_quantity"], "2")
        self.assertEqual(decision["config_sha256"], self.config_hash)

    def test_simultaneous_yes_and_no_candidates_skip_ambiguously(self):
        both_sides = {
            **self.orderbook,
            "orderbook_fp": {
                "yes_dollars_fp": [["0.20", "3.00"]],
                "no_dollars_fp": [["0.20", "3.00"]],
            },
        }
        decision = self.evaluate(orderbook=both_sides)
        self.assertFalse(decision["eligible"])
        self.assertEqual(decision["rejection_reason"], "SKIP_AMBIGUOUS")

    def test_stale_book_and_non_btc_scope_fail_closed(self):
        stale = self.evaluate(received=self.now - timedelta(seconds=6))
        other_asset = self.evaluate(market={**self.market, "ticker": "KXETH15M-SAMPLE"})
        self.assertEqual(stale["rejection_reason"], "ORDERBOOK_STALE")
        self.assertEqual(other_asset["rejection_reason"], "MARKET_SERIES_NOT_ELIGIBLE")

    def test_worker_persists_scope_gates_and_runs_configured_btc_evaluator(self):
        async def scenario():
            with TemporaryDirectory() as root:
                collector = Collector(Settings(home=Path(root)))
                now = datetime.now(timezone.utc)
                common = {
                    "is_15m_confirmed": True,
                    "paper_compatibility": "BINARY_SINGLE_MARKET",
                    "market_status": "active",
                    "open_time": (now - timedelta(minutes=10)).isoformat(),
                    "close_time": (now + timedelta(seconds=30)).isoformat(),
                }
                collector.markets = {
                    ticker: {**common, "ticker": ticker}
                    for ticker in ("KXBTC15M-TEST", "KXETH15M-TEST", "KXALT15M-TEST")
                }
                stored = []

                async def append_record(key, kind, payload):
                    stored.append((key, kind, payload))
                    return True

                collector._append_paper_record = append_record

                class FakeAPI:
                    last_request_rtt_ms = 4.0

                    async def get(self, path, params=None):
                        assert path == "/markets/orderbooks"
                        assert params["tickers"] == ["KXBTC15M-TEST"]
                        return {
                            "orderbooks": [
                                {
                                    "ticker": "KXBTC15M-TEST",
                                    "orderbook_fp": {
                                        "yes_dollars_fp": [["0.48", "3.00"]],
                                        "no_dollars_fp": [["0.49", "3.00"]],
                                    },
                                }
                            ]
                        }

                collector.api = FakeAPI()
                await collector.collect_strategy_inputs()
                await collector.collect_strategy_inputs()
                return collector.status.snapshot(), stored

        status, stored = asyncio.run(scenario())
        decisions = [payload for _, kind, payload in stored if kind == "DECISION"]
        scope_records = [key for key, kind, _ in stored if kind == "DECISION" and key.startswith("scope-decision|")]
        self.assertEqual(status["strategy_evaluators_active"], 1)
        self.assertEqual(status["strategy_in_scope_evaluators_active"], 1)
        self.assertEqual(status["strategy_candidate_markets_out_of_scope"], 2)
        self.assertEqual(len(decisions), 4)
        self.assertEqual(len(scope_records), 2)
        self.assertEqual(
            {row["market_ticker"] for row in decisions},
            {"KXBTC15M-TEST", "KXETH15M-TEST", "KXALT15M-TEST"},
        )
        self.assertFalse(
            next(row for row in decisions if row["market_ticker"] == "KXBTC15M-TEST")["eligible"]
        )

    def test_scope_decision_window_cache_restores_from_durable_ledger(self):
        async def scenario():
            with TemporaryDirectory() as root:
                collector = Collector(Settings(home=Path(root)))
                now = datetime.now(timezone.utc)
                opened = now - timedelta(minutes=1)
                closed = now + timedelta(minutes=14)
                ticker = "KXETH15M-RESTORED"
                market = {
                    "ticker": ticker,
                    "event_ticker": "KXETH15M-RESTORED-EVENT",
                    "market_type": "binary",
                    "market_status": "active",
                    "paper_compatibility": "BINARY_SINGLE_MARKET",
                    "is_15m_confirmed": True,
                    "open_time": opened.isoformat(),
                    "close_time": closed.isoformat(),
                    "rules_primary": "If the verified reference rises, then the market resolves to Yes.",
                    "fee_type": "quadratic",
                    "fee_multiplier": 1,
                    "result": "",
                }
                records = [
                    {
                        "idempotency_key": f"scope-decision|{profile}|{ticker}|{opened.isoformat()}",
                        "profile_id": profile,
                        "record_type": "DECISION",
                        "payload": {"market_ticker": ticker},
                    }
                    for profile in (collector.strategy_profile_id, collector.crossmarket_profile_id)
                ]
                collector.storage.paper_records = lambda profile: [
                    row for row in records if row["profile_id"] == profile
                ]
                await collector.restore_paper_state()
                await collector.restore_crossmarket_paper_state()
                collector.markets = {ticker: market}
                appended = []

                async def append_record(key, kind, payload, *, profile_id=None):
                    appended.append((key, kind, payload, profile_id))
                    return True

                collector._append_paper_record = append_record
                await collector.collect_strategy_inputs()
                await collector.collect_crossmarket_strategy_inputs()
                return collector.paper_scope_decision_windows, collector.crossmarket_scope_decision_windows, appended

        btc_windows, cross_windows, appended = asyncio.run(scenario())
        self.assertIn("KXETH15M-RESTORED", btc_windows)
        self.assertIn("KXETH15M-RESTORED", cross_windows)
        self.assertEqual(appended, [])

    def test_crossmarket_strategy_is_separately_hashed_and_inherits_frozen_entry_rules(self):
        root = Path(__file__).resolve().parents[1]
        btc, btc_hash = load_frozen_strategy(root)
        cross, cross_hash = load_crossmarket_strategy(root)
        self.assertEqual(btc_hash, "7ecd13ea603092bb010149002f98d399cfa924d557445725fdaadff504f142ad")
        self.assertEqual(cross["revision"], "REFLEX_CROSSMARKET_PAPER_V1")
        self.assertNotEqual(cross_hash, btc_hash)
        self.assertEqual(cross["strategy"]["entry"], btc["strategy"]["entry"])
        self.assertEqual(cross["strategy"]["sizing"], btc["strategy"]["sizing"])
        self.assertEqual(cross["execution"]["mode"], "PAPER")
        self.assertFalse(cross["execution"]["live_authorized"])

    def test_crossmarket_classification_requires_verified_binary_rules_fees_and_settlement_shape(self):
        now = datetime.now(timezone.utc)
        market = {
            "ticker": "KXETH15M-SAMPLE",
            "market_status": "active",
            "market_type": "binary",
            "paper_compatibility": "BINARY_SINGLE_MARKET",
            "is_15m_confirmed": True,
            "open_time": (now - timedelta(minutes=14)).isoformat(),
            "close_time": (now + timedelta(minutes=1)).isoformat(),
            "rules_primary": "If index rises, then the market resolves to Yes.",
            "fee_type": "quadratic",
            "fee_multiplier": 1,
            "result": "",
        }
        self.assertEqual(crossmarket_eligibility(market)[0], "ELIGIBLE")
        self.assertEqual(
            crossmarket_eligibility({**market, "ticker": "KXBTC15M-SAMPLE"})[1],
            "RESERVED_FOR_FROZEN_BTC_PROFILE",
        )
        self.assertEqual(
            crossmarket_eligibility({**market, "paper_compatibility": "MULTI_OUTCOME_EVENT_REVIEW"})[0],
            "BLOCKED",
        )
        self.assertEqual(
            crossmarket_eligibility({**market, "rules_primary": "Settlement rules unavailable"})[1],
            "BINARY_YES_RESOLUTION_RULE_NOT_VERIFIED",
        )
        self.assertEqual(
            crossmarket_eligibility({**market, "fee_type": "unknown"})[1],
            "FEE_SCHEDULE_NOT_VERIFIED_FOR_MARKET",
        )

    def test_crossmarket_evaluator_applies_inherited_rule_to_allowed_binary_ticker(self):
        root = Path(__file__).resolve().parents[1]
        config, config_hash = load_crossmarket_strategy(root)
        now = datetime.now(timezone.utc)
        market = {
            "ticker": "KXETH15M-SAMPLE",
            "market_status": "active",
            "is_15m_confirmed": True,
            "paper_compatibility": "BINARY_SINGLE_MARKET",
            "open_time": (now - timedelta(minutes=14, seconds=30)).isoformat(),
            "close_time": (now + timedelta(seconds=30)).isoformat(),
        }
        orderbook = {
            "ticker": market["ticker"],
            "orderbook_fp": {
                "yes_dollars_fp": [["0.05", "2.00"]],
                "no_dollars_fp": [["0.20", "2.00"]],
            },
        }
        decision = evaluate_market(
            config,
            market,
            orderbook,
            as_of=now,
            book_received_at=now,
            config_sha256=config_hash,
        )
        self.assertTrue(decision["eligible"])
        self.assertEqual(decision["candidate_direction"], "YES")
        self.assertEqual(decision["executable_acquisition_price"], "0.8000")

    def test_crossmarket_worker_persists_eligible_excluded_and_blocked_evaluators(self):
        async def scenario():
            with TemporaryDirectory() as root:
                collector = Collector(Settings(home=Path(root)))
                now = datetime.now(timezone.utc)
                common = {
                    "market_status": "active",
                    "is_15m_confirmed": True,
                    "open_time": (now - timedelta(minutes=14, seconds=30)).isoformat(),
                    "close_time": (now + timedelta(seconds=30)).isoformat(),
                    "market_type": "binary",
                    "rules_primary": "If the index rises, then the market resolves to Yes.",
                    "fee_type": "quadratic",
                    "fee_multiplier": 1,
                    "result": "",
                }
                collector.markets = {
                    "KXETH15M-TEST": {
                        **common,
                        "ticker": "KXETH15M-TEST",
                        "paper_compatibility": "BINARY_SINGLE_MARKET",
                    },
                    "KXBTC15M-TEST": {
                        **common,
                        "ticker": "KXBTC15M-TEST",
                        "paper_compatibility": "BINARY_SINGLE_MARKET",
                    },
                    "KXCRYPTOLEAD15M-TEST": {
                        **common,
                        "ticker": "KXCRYPTOLEAD15M-TEST",
                        "paper_compatibility": "MULTI_OUTCOME_EVENT_REVIEW",
                    },
                }
                stored = []

                async def append_record(key, kind, payload, *, profile_id=None):
                    stored.append((key, kind, payload, profile_id))
                    return True

                collector._append_paper_record = append_record

                class FakeAPI:
                    last_request_rtt_ms = 4.0

                    async def get(self, path, params=None):
                        self.request = (path, params)
                        return {
                            "orderbooks": [
                                {
                                    "ticker": "KXETH15M-TEST",
                                    "orderbook_fp": {
                                        "yes_dollars_fp": [["0.50", "3.00"]],
                                        "no_dollars_fp": [["0.50", "3.00"]],
                                    },
                                }
                            ]
                        }

                collector.api = FakeAPI()
                await collector.collect_crossmarket_strategy_inputs()
                await collector.collect_crossmarket_strategy_inputs()
                return collector.status.snapshot(), stored

        status, stored = asyncio.run(scenario())
        scope = status["crossmarket_last_scope_decisions"]
        self.assertEqual(status["crossmarket_evaluators_active"], 1)
        self.assertEqual(status["crossmarket_eligible_market_tickers"], ["KXETH15M-TEST"])
        self.assertEqual(status["crossmarket_excluded_market_tickers"], ["KXBTC15M-TEST"])
        self.assertEqual(
            status["crossmarket_blocked_market_reasons"],
            {"KXCRYPTOLEAD15M-TEST": "UNSUPPORTED_MULTI_OUTCOME_OR_MARKET_STRUCTURE"},
        )
        self.assertEqual(scope["KXETH15M-TEST"]["market_eligibility_state"], "ELIGIBLE")
        self.assertEqual(scope["KXBTC15M-TEST"]["market_eligibility_state"], "EXCLUDED")
        self.assertEqual(scope["KXCRYPTOLEAD15M-TEST"]["market_eligibility_state"], "BLOCKED")
        self.assertEqual(
            sum(1 for row in stored if row[1] == "DECISION" and row[0].startswith("scope-decision|")),
            3,
        )
        self.assertTrue(all(row[3] == "REFLEX_CROSSMARKET_PAPER:REFLEX_CROSSMARKET_PAPER_V1" for row in stored))
        self.assertTrue(any(row[2].get("input_source") == "KALSHI_PUBLIC_REST_FALLBACK" for row in stored))

    def test_worker_warms_inventory_from_recent_persisted_discovery(self):
        with TemporaryDirectory() as root:
            home = Path(root)
            reports = home / "reports"
            reports.mkdir()
            snapshot = {
                "snapshot_utc": datetime.now(timezone.utc).isoformat(),
                "series": [{"series_ticker": "KXBTC15M"}],
                "markets": [{"ticker": "KXBTC15M-CACHED"}],
            }
            (reports / "series_inventory_test.json").write_text(
                json.dumps(snapshot), encoding="utf-8"
            )

            collector = Collector(Settings(home=home))

            self.assertEqual(collector.inventory_snapshot["markets"][0]["ticker"], "KXBTC15M-CACHED")
            self.assertGreater(collector.last_full_inventory_monotonic, 0)
            self.assertEqual(
                collector.status.snapshot()["inventory_cache_state"],
                "WARMED_FROM_PERSISTED_DISCOVERY",
            )

    def test_market_reconciliation_keeps_all_active_and_nearest_upcoming(self):
        selected = _market_api_reconciliation_set(
            {
                "SERIES_A": [
                    {
                        "_query_status": "open",
                        "markets": [
                            {"ticker": "A_ACTIVE_1", "status": "active"},
                            {"ticker": "A_ACTIVE_2", "status": "active"},
                        ],
                    },
                    {
                        "_query_status": "unopened",
                        "markets": [
                            {"ticker": "A_NEXT", "status": "initialized", "open_time": "2026-10-14T10:15:00Z"},
                            {"ticker": "A_LATER", "status": "initialized", "open_time": "2026-10-14T10:30:00Z"},
                        ],
                    },
                ],
                "SERIES_B": [
                    {
                        "_query_status": "unopened",
                        "markets": [
                            {"ticker": "B_NEXT", "status": "unopened", "open_time": "2026-10-14T10:45:00Z"},
                        ],
                    },
                ],
            }
        )
        self.assertEqual(selected, ["A_ACTIVE_1", "A_ACTIVE_2", "A_NEXT", "B_NEXT"])

    def test_duplicate_status_results_merge_without_double_counting_markets(self):
        events = _dedupe_events(
            [
                {
                    "event_ticker": "EVENT_A",
                    "_query_status": "open",
                    "markets": [{"ticker": "A1", "status": "active"}],
                },
                {
                    "event_ticker": "EVENT_A",
                    "_query_status": "unopened",
                    "markets": [
                        {"ticker": "A1", "status": "initialized"},
                        {"ticker": "A2", "status": "initialized"},
                    ],
                },
            ]
        )
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["_query_status"], "open")
        self.assertEqual({row["ticker"] for row in events[0]["markets"]}, {"A1", "A2"})
        self.assertEqual(next(row["status"] for row in events[0]["markets"] if row["ticker"] == "A1"), "active")


class StrategyOrderbookInputTests(unittest.IsolatedAsyncioTestCase):
    async def test_fresh_synchronized_websocket_book_is_the_primary_input(self):
        with TemporaryDirectory() as root:
            collector = Collector(Settings(home=Path(root)))
            ticker = "KXBTC15M-STREAM"
            book = OrderBook()
            book.snapshot(
                {
                    "yes_dollars_fp": [["0.20", "3.00"]],
                    "no_dollars_fp": [["0.18", "4.00"]],
                }
            )
            collector.books[ticker] = book
            collector.book_received_monotonic[ticker] = time.monotonic() - 0.5
            collector.api.get = AsyncMock()

            inputs, request_count, errors = await collector._load_entry_orderbooks(
                [{"ticker": ticker}],
                max_age_seconds=5.0,
                rest_channel="rest_strategy_orderbook",
                profile_health_field="strategy_rest_poll_health",
                status_prefix="strategy",
            )

        raw_book, received_at, source = inputs[ticker]
        self.assertEqual(source, "KALSHI_AUTHENTICATED_WSS")
        self.assertLessEqual((datetime.now(timezone.utc) - received_at).total_seconds(), 5.0)
        self.assertEqual(raw_book["orderbook_fp"]["yes_dollars_fp"], [["0.20", "3.00"]])
        self.assertEqual(raw_book["orderbook_fp"]["no_dollars_fp"], [["0.18", "4.00"]])
        self.assertEqual(request_count, 0)
        self.assertEqual(errors, 0)
        collector.api.get.assert_not_awaited()

    async def test_rest_is_only_a_throttled_fallback_and_recent_fallback_is_cached(self):
        with TemporaryDirectory() as root:
            collector = Collector(Settings(home=Path(root)))
            ticker = "KXETH15M-REST-FALLBACK"
            collector.api.get = AsyncMock(
                return_value={
                    "orderbooks": [
                        {
                            "ticker": ticker,
                            "orderbook_fp": {
                                "yes_dollars_fp": [["0.30", "2.00"]],
                                "no_dollars_fp": [["0.25", "2.00"]],
                            },
                        }
                    ]
                }
            )
            kwargs = {
                "max_age_seconds": 5.0,
                "rest_channel": "rest_crossmarket_orderbook",
                "profile_health_field": "crossmarket_rest_poll_health",
                "status_prefix": "crossmarket",
            }

            first, first_requests, first_errors = await collector._load_entry_orderbooks(
                [{"ticker": ticker}], **kwargs
            )
            second, second_requests, second_errors = await collector._load_entry_orderbooks(
                [{"ticker": ticker}], **kwargs
            )

            self.assertEqual(first[ticker][2], "KALSHI_PUBLIC_REST_FALLBACK")
            self.assertEqual(second[ticker][2], "KALSHI_PUBLIC_REST_FALLBACK_CACHE")
            self.assertEqual(first_requests, 1)
            self.assertEqual(second_requests, 0)
            self.assertEqual(first_errors, 0)
            self.assertEqual(second_errors, 0)
            self.assertEqual(collector.api.get.await_count, 1)


class ApiTests(unittest.TestCase):
    def test_rate_limit_response_retries_and_recovers(self):
        api = PublicKalshiAPI(min_interval=0)
        rate_limited = urllib.error.HTTPError(
            "https://example.invalid", 429, "too many requests", {}, io.BytesIO(b"429")
        )
        api._get_sync = Mock(side_effect=[rate_limited, {"ok": True}])
        with patch("kalshi15m.api.asyncio.sleep", new=AsyncMock()):
            result = asyncio.run(api.get("/exchange/schedule"))
        self.assertEqual(result, {"ok": True})
        self.assertEqual(api.requests, 1)
        self.assertEqual(api.attempts, 2)
        self.assertEqual(api.rate_limit_responses, 1)
        self.assertEqual(api.errors, 0)
        self.assertEqual(api.rate_limit_backoff_count, 1)
        self.assertEqual(api.rate_limit_backoff_seconds, 0.5)
        self.assertEqual(api.last_rate_limit_path, "/exchange/schedule")

    def test_rate_limit_backoff_is_shared_and_exponential(self):
        api = PublicKalshiAPI(min_interval=0)

        async def apply_three_backoffs():
            for _ in range(3):
                await api._schedule_rate_limit_backoff("/markets")

        asyncio.run(apply_three_backoffs())

        self.assertEqual(api.rate_limit_backoff_count, 3)
        self.assertEqual(api.rate_limit_backoff_scheduled_seconds, 3.5)
        self.assertEqual(api.rate_limit_backoff_seconds, 2.0)
        self.assertGreater(api._next_request_at, time.monotonic())

    def test_pagination_limit_preserves_partial_rows_and_cursor(self):
        api = PublicKalshiAPI(min_interval=0)
        api.get = AsyncMock(return_value={"events": [{"event_ticker": "A"}], "cursor": "next"})

        with self.assertRaises(PaginationLimitExceeded) as raised:
            asyncio.run(api.pages("/events", "events", max_pages=1))

        self.assertEqual(raised.exception.rows, [{"event_ticker": "A"}])
        self.assertEqual(raised.exception.cursor, "next")
        self.assertEqual(raised.exception.pages, 1)

    def test_scheduled_reconciliation_uses_current_and_next_window(self):
        now = datetime(2026, 10, 14, 10, 14, 0, tzinfo=timezone.utc)
        selected = _scheduled_market_reconciliation_set(
            [
                {
                    "ticker": "A_CURRENT",
                    "series_ticker": "SERIES_A",
                    "market_status": "active",
                    "open_time": "2026-10-14T10:00:00Z",
                    "close_time": "2026-10-14T10:15:00Z",
                },
                {
                    "ticker": "A_NEXT",
                    "series_ticker": "SERIES_A",
                    "market_status": "initialized",
                    "open_time": "2026-10-14T10:15:00Z",
                    "close_time": "2026-10-14T10:30:00Z",
                },
                {
                    "ticker": "A_LATER",
                    "series_ticker": "SERIES_A",
                    "market_status": "initialized",
                    "open_time": "2026-10-14T10:30:00Z",
                    "close_time": "2026-10-14T10:45:00Z",
                },
            ],
            now,
        )
        self.assertEqual(selected, ["A_CURRENT", "A_NEXT"])

    def test_scheduled_reconciliation_keeps_recent_closed_market_for_settlement(self):
        now = datetime(2026, 10, 14, 10, 16, 0, tzinfo=timezone.utc)
        selected = _scheduled_market_reconciliation_set(
            [
                {
                    "ticker": "A_CLOSED",
                    "series_ticker": "SERIES_A",
                    "market_status": "active",
                    "open_time": "2026-10-14T10:00:00Z",
                    "close_time": "2026-10-14T10:15:00Z",
                    "result": None,
                },
                {
                    "ticker": "A_CURRENT",
                    "series_ticker": "SERIES_A",
                    "market_status": "initialized",
                    "open_time": "2026-10-14T10:15:00Z",
                    "close_time": "2026-10-14T10:30:00Z",
                },
                {
                    "ticker": "A_NEXT",
                    "series_ticker": "SERIES_A",
                    "market_status": "initialized",
                    "open_time": "2026-10-14T10:30:00Z",
                    "close_time": "2026-10-14T10:45:00Z",
                },
            ],
            now,
        )
        self.assertEqual(selected, ["A_CLOSED", "A_CURRENT", "A_NEXT"])

    def test_light_refresh_queries_and_updates_only_scheduled_tickers(self):
        class FakeAPI:
            requests = 0
            attempts = 0
            rate_limit_responses = 0
            errors = 0
            last_error = None

            async def pages(self, path, result_key, params, max_pages):
                self.requests += 1
                return [
                    {"ticker": ticker, "status": "active", "yes_bid_dollars": "0.4100"}
                    for ticker in params["tickers"].split(",")
                ]

        now = datetime(2026, 10, 14, 10, 14, 0, tzinfo=timezone.utc)
        snapshot = {
            "markets": [
                {
                    "ticker": "A_CURRENT", "series_ticker": "SERIES_A",
                    "market_status": "active", "is_15m_confirmed": True,
                    "open_time": "2026-10-14T10:00:00Z", "close_time": "2026-10-14T10:15:00Z",
                },
                {
                    "ticker": "A_NEXT", "series_ticker": "SERIES_A",
                    "market_status": "initialized", "is_15m_confirmed": True,
                    "open_time": "2026-10-14T10:15:00Z", "close_time": "2026-10-14T10:30:00Z",
                },
                {
                    "ticker": "A_LATER", "series_ticker": "SERIES_A",
                    "market_status": "initialized", "is_15m_confirmed": True,
                    "open_time": "2026-10-14T10:30:00Z", "close_time": "2026-10-14T10:45:00Z",
                },
            ]
        }
        api = FakeAPI()
        selected = asyncio.run(refresh_known_market_rows(api, snapshot, now))
        self.assertEqual(selected, ["A_CURRENT", "A_NEXT"])
        self.assertEqual(api.requests, 1)
        self.assertEqual(snapshot["markets"][0]["yes_bid_dollars"], "0.4100")
        self.assertEqual(snapshot["markets"][0]["market_api_reconciled"], True)
        self.assertEqual(snapshot["markets"][2].get("market_api_reconciled"), None)

    def test_scheduled_market_refresh_batches_at_most_25_tickers(self):
        self.assertEqual(SCHEDULED_RECONCILIATION_BATCH_SIZE, 25)
        now = datetime(2026, 10, 14, 10, 14, 0, tzinfo=timezone.utc)

        class FakeAPI:
            def __init__(self):
                self.calls = []
                self.requests = 0
                self.attempts = 0
                self.rate_limit_responses = 0
                self.errors = 0
                self.last_error = None

            async def pages(self, path, result_key, params, max_pages):
                batch = params["tickers"].split(",")
                self.calls.append(batch)
                self.requests += 1
                return [{"ticker": ticker, "status": "active"} for ticker in batch]

        snapshot = {
            "markets": [
                {
                    "ticker": f"MKT_{index:02d}",
                    "series_ticker": f"SERIES_{index:02d}",
                    "market_status": "active",
                    "open_time": "2026-10-14T09:00:00Z",
                    "close_time": "2026-10-14T10:15:00Z",
                }
                for index in range(26)
            ]
        }
        api = FakeAPI()

        asyncio.run(refresh_known_market_rows(api, snapshot, now))

        self.assertEqual([len(batch) for batch in api.calls], [25, 1])

    def test_rollover_priority_refresh_queries_only_prepared_group(self):
        now = datetime(2026, 10, 14, 10, 15, 2, tzinfo=timezone.utc)

        class FakeAPI:
            def __init__(self):
                self.calls = []
                self.requests = 0
                self.attempts = 0
                self.rate_limit_responses = 0
                self.errors = 0
                self.last_error = None

            async def pages(self, path, result_key, params, max_pages):
                self.calls.append(params["tickers"].split(","))
                self.requests += 1
                return [
                    {"ticker": ticker, "status": "active", "market_type": "binary"}
                    for ticker in params["tickers"].split(",")
                ]

        snapshot = {
            "markets": [
                {
                    "ticker": "OPENING_A",
                    "market_status": "initialized",
                    "is_15m_confirmed": True,
                    "open_time": "2026-10-14T10:15:00Z",
                    "close_time": "2026-10-14T10:30:00Z",
                },
                {
                    "ticker": "OPENING_B",
                    "market_status": "initialized",
                    "is_15m_confirmed": True,
                    "open_time": "2026-10-14T10:15:00Z",
                    "close_time": "2026-10-14T10:30:00Z",
                },
                {
                    "ticker": "OTHER_ACTIVE",
                    "market_status": "active",
                    "is_15m_confirmed": True,
                    "open_time": "2026-10-14T10:00:00Z",
                    "close_time": "2026-10-14T10:15:00Z",
                },
                {
                    "ticker": "NEXT_SERIES",
                    "market_status": "initialized",
                    "is_15m_confirmed": True,
                    "open_time": "2026-10-14T10:30:00Z",
                    "close_time": "2026-10-14T10:45:00Z",
                },
            ] + [
                {
                    "ticker": f"OPENING_{index:02d}",
                    "market_status": "initialized",
                    "is_15m_confirmed": True,
                    "open_time": "2026-10-14T10:15:00Z",
                    "close_time": "2026-10-14T10:30:00Z",
                }
                for index in range(24)
            ],
        }
        api = FakeAPI()
        prepared_tickers = {"OPENING_A", "OPENING_B"} | {
            f"OPENING_{index:02d}" for index in range(24)
        }

        selected = asyncio.run(
            refresh_known_market_rows(
                api,
                snapshot,
                now,
                ticker_override=prepared_tickers,
                ticker_batch_size=Settings().rollover_priority_batch_size,
            )
        )

        self.assertEqual(selected, sorted(prepared_tickers))
        self.assertEqual(api.calls, [sorted(prepared_tickers)])
        self.assertEqual(
            snapshot["market_api_reconciliation_returned_tickers"],
            sorted(prepared_tickers),
        )
        self.assertTrue(snapshot["markets"][0]["market_api_reconciled"])
        self.assertTrue(snapshot["markets"][1]["market_api_reconciled"])
        self.assertIsNone(snapshot["markets"][2].get("market_api_reconciled"))
        self.assertIsNone(snapshot["markets"][3].get("market_api_reconciled"))

    def test_upcoming_series_scan_discovers_and_enriches_imminent_contracts(self):
        now = datetime(2026, 10, 14, 10, 14, 0, tzinfo=timezone.utc)
        opened = (now + timedelta(seconds=60)).isoformat()
        closed = (now + timedelta(seconds=960)).isoformat()

        class FakeAPI:
            def __init__(self):
                self.market_series_calls = []
                self.event_calls = []

            async def pages(self, path, result_key, params, max_pages):
                if path == "/markets":
                    series_ticker = params["series_ticker"]
                    self.market_series_calls.append(series_ticker)
                    ticker = f"{series_ticker}-NEXT"
                    event_ticker = f"{series_ticker}-EVENT"
                    return [{
                        "ticker": ticker,
                        "event_ticker": event_ticker,
                        "status": "initialized",
                        "market_type": "binary",
                        "open_time": opened,
                        "close_time": closed,
                        "rules_primary": "If the value holds, the market resolves to Yes.",
                    }]
                self.event_calls.append(params["tickers"])
                found = []
                for event_ticker in params["tickers"].split(","):
                    series_ticker = event_ticker.removesuffix("-EVENT")
                    found.append({
                        "event_ticker": event_ticker,
                        "series_ticker": series_ticker,
                        "title": event_ticker,
                        "markets": [{
                            "ticker": f"{series_ticker}-NEXT",
                            "status": "initialized",
                            "market_type": "binary",
                            "open_time": opened,
                            "close_time": closed,
                            "rules_primary": "If the value holds, the market resolves to Yes.",
                        }],
                    })
                return found

        snapshot = {
            "series": [
                {
                    "series_ticker": f"SERIES_{index}",
                    "frequency": "fifteen_minutes",
                    "fee_type": "quadratic",
                    "fee_multiplier": "1",
                    "settlement_sources": [],
                }
                for index in range(9)
            ],
            "markets": [],
            "market_api_reconciliation_tickers": [],
            "open_or_upcoming_tickers": [],
        }
        api = FakeAPI()

        next_cursor, added = asyncio.run(
            refresh_upcoming_series_markets(
                api,
                snapshot,
                now,
                near_open_seconds=180,
                scan_cursor=0,
                scan_batch_size=4,
            )
        )

        self.assertEqual(next_cursor, 4)
        self.assertEqual(added, 4)
        self.assertEqual(len(api.market_series_calls), 4)
        self.assertEqual(len(api.event_calls), 1)
        self.assertEqual(len(snapshot["markets"]), 4)
        self.assertTrue(all(row["duration_seconds"] == 900 for row in snapshot["markets"]))
        self.assertTrue(all(row["paper_compatibility"] == "BINARY_SINGLE_MARKET" for row in snapshot["markets"]))
        self.assertEqual(snapshot["rollover_series_scan"]["checked_series_count"], 4)

    def test_full_discovery_includes_recent_unsettled_rows_and_limits_active_csv(self):
        now = datetime.now(timezone.utc)

        def market(ticker, status, opened, closed, result=None):
            return {
                "ticker": ticker,
                "status": status,
                "market_type": "binary",
                "open_time": opened.isoformat(),
                "close_time": closed.isoformat(),
                "result": result,
                "rules_primary": "The result is determined over a 15-minute window.",
            }

        current = market("A_CURRENT", "active", now - timedelta(minutes=5), now + timedelta(minutes=10))
        upcoming = market("A_UPCOMING", "initialized", now + timedelta(minutes=10), now + timedelta(minutes=25))
        closed = market("A_CLOSED", "closed", now - timedelta(minutes=15), now - timedelta(minutes=1))
        settled = market("A_SETTLED", "settled", now - timedelta(minutes=30), now - timedelta(minutes=15), "yes")
        events_by_status = {
            "open": [{"event_ticker": "EVENT_CURRENT", "markets": [current]}],
            "unopened": [{"event_ticker": "EVENT_UPCOMING", "markets": [upcoming]}],
            "closed": [{"event_ticker": "EVENT_CLOSED", "markets": [closed]}],
            "settled": [{"event_ticker": "EVENT_SETTLED", "markets": [settled]}],
        }

        class FakeAPI:
            requests = 0
            attempts = 0
            rate_limit_responses = 0
            errors = 0
            last_error = None

            def __init__(self):
                self.series_requests = 0

            async def pages(self, path, result_key, params=None, max_pages=100):
                if path == "/series":
                    self.series_requests += 1
                    return [{"ticker": "SERIES_A", "frequency": "fifteen_min", "tags": []}]
                if path == "/events":
                    self.assert_close_filter(path, params)
                    return events_by_status[params["status"]]
                if path == "/markets":
                    selected = set(params["tickers"].split(","))
                    return [row for row in (current, upcoming, closed, settled) if row["ticker"] in selected]
                raise AssertionError(path)

            async def get(self, path, params=None):
                if path == "/events/multivariate":
                    return {"events": [], "cursor": ""}
                if path == "/exchange/schedule":
                    return {}
                raise AssertionError(path)

            @staticmethod
            def assert_close_filter(path, params):
                if params["status"] in {"closed", "settled"}:
                    assert isinstance(params.get("min_close_ts"), int)
                else:
                    assert params.get("min_close_ts") is None

        with TemporaryDirectory() as root, patch("kalshi15m.discovery.candidate_series", return_value=True):
            api = FakeAPI()
            snapshot = asyncio.run(discover(api, Path(root)))
            self.assertEqual(api.series_requests, 1)
            self.assertEqual(snapshot["fifteen_min_series"], 1)
            self.assertEqual(snapshot["discovery_coverage_state"], "COMPLETE_CURRENT_AND_NEAR_TERM")
            self.assertEqual(snapshot["recent_closed_settled_lookback_seconds"], 1800)
            self.assertEqual(snapshot["active_15m_market_rows"], 1)
            self.assertEqual(snapshot["market_event_rows"], 4)
            series = snapshot["series"][0]
            self.assertEqual(series["event_count_open_or_upcoming"], 2)
            self.assertEqual(series["market_rows_open_or_upcoming"], 2)
            self.assertEqual(series["recent_closed_or_settled_market_rows"], 2)
            self.assertIn("A_CLOSED", snapshot["market_api_reconciliation_tickers"])
            self.assertNotIn("A_SETTLED", snapshot["market_api_reconciliation_tickers"])
            with Path(snapshot["artifact_paths"]["markets_csv"]).open(newline="", encoding="utf-8") as handle:
                active_rows = list(csv.DictReader(handle))
            self.assertEqual([row["ticker"] for row in active_rows], ["A_CURRENT"])

            override_api = FakeAPI()
            overridden = asyncio.run(
                discover(
                    override_api,
                    Path(root),
                    series_rows_override=[
                        {"ticker": "SERIES_A", "frequency": "fifteen_min", "tags": []}
                    ],
                )
            )
            self.assertEqual(override_api.series_requests, 0)
            self.assertEqual(overridden["series_catalog_candidate_tickers"], ["SERIES_A"])


class BookTests(unittest.TestCase):
    def test_yes_no_asks_derive_from_opposite_side_bids(self):
        quotes = executable_quotes(
            {
                "orderbook_fp": {
                    "yes_dollars": [["0.40", "7.00"], ["0.42", "3.00"]],
                    "no_dollars": [["0.50", "6.00"], ["0.56", "2.00"]],
                }
            }
        )
        self.assertEqual(quotes["yes_ask"], "0.4400")
        self.assertEqual(quotes["no_ask"], "0.5800")
        self.assertEqual(quotes["yes_ask_size"], "2.00")
        self.assertEqual(quotes["no_ask_size"], "3.00")

    def test_current_fixed_point_snapshot_shape_is_parsed(self):
        payload = {
            "ticker": "sample",
            "orderbook_fp": {
                "yes_dollars_fp": [["0.40", "7.00"]],
                "no_dollars_fp": [["0.50", "6.00"]],
            },
        }
        quotes = executable_quotes(payload)
        book = OrderBook()
        book.snapshot(payload)

        self.assertTrue(book.synchronized)
        self.assertEqual(quotes["yes_ask"], "0.5000")
        self.assertEqual(quotes["no_ask"], "0.6000")

    def test_websocket_root_snapshot_and_strict_sequence_gap_invalidation(self):
        class FakeWebSocket:
            def __init__(self, messages):
                self.messages = messages
                self.closed = False

            def __aiter__(self):
                async def iterate():
                    for message in self.messages:
                        yield json.dumps(message)
                return iterate()

            async def close(self, code=None, reason=None):
                self.closed = True

        with TemporaryDirectory() as root:
            collector = Collector(Settings(home=Path(root)))
            collector.open_tickers.add("SAMPLE")
            collector.subscription_channel_by_sid[8] = "orderbook_delta"
            collector.subscription_membership[8] = {"SAMPLE"}
            ws = FakeWebSocket(
                [
                    {
                        "type": "orderbook_snapshot", "sid": 8, "seq": 10,
                        "msg": {
                            "market_ticker": "SAMPLE",
                            "yes_dollars_fp": [["0.40", "2.00"]],
                            "no_dollars_fp": [["0.50", "3.00"]],
                        },
                    },
                    {
                        "type": "orderbook_delta", "sid": 8, "seq": 11,
                        "msg": {
                            "market_ticker": "SAMPLE", "side": "yes",
                            "price_dollars": "0.40", "delta_fp": "1.00",
                        },
                    },
                    {
                        "type": "orderbook_delta", "sid": 8, "seq": 13,
                        "msg": {
                            "market_ticker": "SAMPLE", "side": "yes",
                            "price_dollars": "0.40", "delta_fp": "1.00",
                        },
                    },
                ]
            )
            asyncio.run(collector._receive_ws(ws))

            self.assertTrue(ws.closed)
            self.assertEqual(collector.last_seq[8], 11)
            self.assertEqual(collector.status.snapshot()["sequence_gap_count"], 1)
            self.assertEqual(collector.status.snapshot()["orderbook_sequence_state"], "INVALIDATED")
            self.assertEqual(
                collector.status.snapshot()["orderbook_quote_freshness_by_ticker"]["SAMPLE"],
                "INVALIDATED",
            )
            self.assertFalse(collector.books["SAMPLE"].synchronized)

    def test_orderbook_status_requires_snapshot_and_contiguous_delta_and_reports_freshness(self):
        class FakeWebSocket:
            def __init__(self, messages):
                self.messages = messages

            def __aiter__(self):
                async def iterate():
                    for message in self.messages:
                        yield json.dumps(message)
                return iterate()

        with TemporaryDirectory() as root:
            collector = Collector(Settings(home=Path(root)))
            collector.open_tickers.add("SAMPLE")
            collector.subscription_channel_by_sid[5] = "orderbook_delta"
            collector.subscription_membership[5] = {"SAMPLE"}
            ws = FakeWebSocket(
                [
                    {
                        "type": "orderbook_snapshot", "sid": 5, "seq": 30,
                        "msg": {
                            "market_ticker": "SAMPLE",
                            "yes_dollars_fp": [["0.40", "2.00"]],
                            "no_dollars_fp": [["0.50", "3.00"]],
                        },
                    },
                    {
                        "type": "orderbook_delta", "sid": 5, "seq": 31,
                        "msg": {
                            "market_ticker": "SAMPLE", "side": "yes",
                            "price_dollars": "0.40", "delta_fp": "1.00",
                        },
                    },
                ]
            )
            asyncio.run(collector._receive_ws(ws))

            status = collector.status.snapshot()
            self.assertEqual(status["orderbook_sequence_state"], "VERIFIED_CONTIGUOUS")
            self.assertEqual(status["orderbook_sequence_snapshot_count"], 1)
            self.assertEqual(status["orderbook_sequence_delta_count"], 1)
            self.assertEqual(status["orderbook_sequence_verified_subscription_count"], 1)
            self.assertEqual(status["orderbook_quote_freshness_by_ticker"]["SAMPLE"], "FRESH")

            old = (datetime.now(timezone.utc) - timedelta(seconds=10)).isoformat()
            collector.status.update(
                orderbook_quote_last_update_utc_by_ticker={"SAMPLE": old},
                orderbook_quote_freshness_by_ticker={"SAMPLE": "FRESH"},
            )
            collector.status.refresh_ages()
            stale = collector.status.snapshot()
            self.assertEqual(stale["orderbook_quote_freshness_by_ticker"]["SAMPLE"], "STALE")
            self.assertGreater(stale["orderbook_quote_age_seconds_by_ticker"]["SAMPLE"], 5.0)

            collector._invalidate_websocket_books()
            disconnected = collector.status.snapshot()
            self.assertEqual(disconnected["orderbook_sequence_state"], "UNKNOWN_NOT_CONNECTED")
            self.assertEqual(disconnected["orderbook_sequence_last_session_state"], "VERIFIED_CONTIGUOUS")
            self.assertEqual(disconnected["orderbook_quote_freshness_by_ticker"]["SAMPLE"], "DISCONNECTED")

    def test_websocket_reconnect_generates_fresh_auth_headers(self):
        class FakeWebSocket:
            async def send(self, _message):
                return None

            def __aiter__(self):
                async def iterate():
                    if False:
                        yield ""

                return iterate()

            async def close(self, code=None, reason=None):
                return None

        class FakeConnection:
            def __init__(self, headers):
                self.headers = headers

            async def __aenter__(self):
                observed_headers.append(self.headers)
                if len(observed_headers) == 2:
                    collector.stopping.set()
                    raise ConnectionError("simulated reconnect failure")
                return FakeWebSocket()

            async def __aexit__(self, _type, _value, _traceback):
                return False

        with TemporaryDirectory() as root:
            collector = Collector(Settings(home=Path(root)))
            observed_headers = []

            def fake_connect(_url, **kwargs):
                return FakeConnection(kwargs["additional_headers"])

            with (
                patch(
                    "kalshi15m.worker.ws_auth_headers",
                    side_effect=[{"timestamp": "first"}, {"timestamp": "fresh-on-reconnect"}],
                ) as sign_headers,
                patch("kalshi15m.worker.websockets.connect", side_effect=fake_connect),
            ):
                asyncio.run(collector.websocket_loop())

            self.assertEqual(sign_headers.call_count, 2)
            self.assertEqual(
                observed_headers,
                [{"timestamp": "first"}, {"timestamp": "fresh-on-reconnect"}],
            )

    def test_delta_before_snapshot_fails_closed(self):
        class FakeWebSocket:
            def __init__(self, message):
                self.message = json.dumps(message)
                self.closed = False

            def __aiter__(self):
                async def iterate():
                    yield self.message
                return iterate()

            async def close(self, code=None, reason=None):
                self.closed = True

        with TemporaryDirectory() as root:
            collector = Collector(Settings(home=Path(root)))
            collector.open_tickers.add("SAMPLE")
            collector.subscription_channel_by_sid[4] = "orderbook_delta"
            collector.subscription_membership[4] = {"SAMPLE"}
            ws = FakeWebSocket(
                {
                    "type": "orderbook_delta", "sid": 4, "seq": 1,
                    "msg": {
                        "market_ticker": "SAMPLE", "side": "yes",
                        "price_dollars": "0.40", "delta_fp": "1.00",
                    },
                }
            )
            asyncio.run(collector._receive_ws(ws))

            self.assertTrue(ws.closed)
            self.assertFalse(collector.books.get("SAMPLE", OrderBook()).synchronized)
            self.assertEqual(collector.status.snapshot()["sequence_gap_count"], 1)

    def test_websocket_snapshot_sequence_gap_invalidates_every_market_on_sid(self):
        class FakeWebSocket:
            def __init__(self, messages):
                self.messages = messages
                self.closed = False

            def __aiter__(self):
                async def iterate():
                    for message in self.messages:
                        yield json.dumps(message)
                return iterate()

            async def close(self, code=None, reason=None):
                self.closed = True

        with TemporaryDirectory() as root:
            collector = Collector(Settings(home=Path(root)))
            collector.open_tickers.update({"A", "B"})
            collector.subscription_channel_by_sid[12] = "orderbook_delta"
            collector.subscription_membership[12] = {"A", "B"}
            collector.books["A"] = OrderBook()
            collector.books["B"] = OrderBook()
            collector.books["A"].snapshot({"yes_dollars_fp": [["0.4", "1"]]})
            collector.books["B"].snapshot({"yes_dollars_fp": [["0.5", "1"]]})
            collector.last_seq[12] = 20
            ws = FakeWebSocket(
                [
                    {
                        "type": "orderbook_snapshot", "sid": 12, "seq": 22,
                        "msg": {"market_ticker": "A", "yes_dollars_fp": [["0.4", "2"]]},
                    }
                ]
            )

            asyncio.run(collector._receive_ws(ws))

            self.assertTrue(ws.closed)
            self.assertFalse(collector.books["A"].synchronized)
            self.assertFalse(collector.books["B"].synchronized)
            self.assertEqual(collector.status.snapshot()["sequence_gap_count"], 1)

    def test_subscription_acknowledgements_track_channel_membership_and_rollover(self):
        with TemporaryDirectory() as root:
            collector = Collector(Settings(home=Path(root)))
            loop = asyncio.new_event_loop()
            try:
                for channel, command_id, sid in (
                    ("orderbook_delta", 2, 10),
                    ("trade", 3, 11),
                    ("ticker", 4, 12),
                ):
                    collector.pending_subscriptions[command_id] = (channel, {"OLD"})
                    collector.pending_subscription_futures[command_id] = loop.create_future()
                    collector.initial_subscription_commands.add(command_id)
                    self.assertTrue(
                        collector._handle_subscription_response(
                            {
                                "type": "subscribed", "id": command_id,
                                "msg": {"channel": channel, "sid": sid},
                            }
                        )
                    )
                self.assertEqual(collector.subscribed_tickers, {"OLD"})
                self.assertTrue(collector.ws_subscriptions_ready.is_set())
                update_future = loop.create_future()
                collector.pending_updates[20] = (10, "delete_markets", {"OLD"})
                collector.pending_update_futures[20] = update_future
                collector._handle_subscription_response(
                    {"type": "ok", "id": 20, "sid": 10, "msg": {}}
                )
                self.assertNotIn("OLD", collector.subscribed_tickers)
                self.assertTrue(update_future.done())
            finally:
                loop.close()

    def test_websocket_rollover_deletes_retired_market_and_adds_new_market(self):
        class AutoAckWebSocket:
            def __init__(self, collector):
                self.collector = collector
                self.sent = []

            async def send(self, raw):
                command = json.loads(raw)
                self.sent.append(command)
                if command["cmd"] == "update_subscription":
                    self.collector._handle_subscription_response(
                        {
                            "type": "ok",
                            "id": command["id"],
                            "sid": command["params"]["sids"][0],
                            "msg": {"market_tickers": command["params"]["market_tickers"]},
                        }
                    )
                else:
                    channel = command["params"]["channels"][0]
                    sid = 100 + len(self.sent)
                    self.collector._handle_subscription_response(
                        {
                            "type": "subscribed",
                            "id": command["id"],
                            "msg": {"channel": channel, "sid": sid},
                        }
                    )

        async def scenario():
            with TemporaryDirectory() as root:
                collector = Collector(Settings(home=Path(root)))
                for channel, sid in (("orderbook_delta", 10), ("trade", 11), ("ticker", 12)):
                    collector.subscription_sids[channel] = {sid}
                    collector.subscription_channel_by_sid[sid] = channel
                    collector.subscription_membership[sid] = {"OLD"}
                collector._recompute_subscribed_tickers()
                collector.open_tickers = {"NEW"}
                stale_book = OrderBook()
                stale_book.snapshot({"yes_dollars_fp": [["0.4", "1"]]})
                collector.books["OLD"] = stale_book
                collector.book_received_monotonic["OLD"] = 1.0
                collector.subscription_updates.put_nowait({"NEW"})
                ws = AutoAckWebSocket(collector)
                updater = asyncio.create_task(collector._update_ws_subscriptions(ws))
                await asyncio.wait_for(collector.subscription_updates.join(), timeout=2)
                collector.stopping.set()
                updater.cancel()
                await asyncio.gather(updater, return_exceptions=True)
                return collector, ws.sent

        collector, commands = asyncio.run(scenario())
        self.assertEqual(collector.subscribed_tickers, {"NEW"})
        self.assertNotIn("OLD", collector.books)
        updates = [command["params"] for command in commands]
        self.assertEqual(sum(1 for item in updates if item["action"] == "delete_markets"), 3)
        self.assertEqual(sum(1 for item in updates if item["action"] == "add_markets"), 3)
        self.assertNotIn("OLD", collector.book_received_monotonic)

    def test_snapshot_and_delta_update_book(self):
        book = OrderBook()
        book.snapshot({"orderbook_fp": {"yes_dollars": [["0.4", "2"]], "no_dollars": [["0.5", "1"]]}})
        book.delta({"side": "no", "price_dollars": "0.5", "delta_fp": "-1"})
        self.assertTrue(book.synchronized)
        self.assertIsNone(book.quotes()["yes_ask"])


class StorageTests(unittest.TestCase):
    def test_parquet_size_reconciles_files_added_after_storage_open(self):
        import pyarrow as pa
        import pyarrow.parquet as pq

        with TemporaryDirectory() as temporary:
            cfg = Settings(home=Path(temporary))
            store = Storage(cfg)
            store.open()
            store.parquet_flush_rows = 1
            late_partition = cfg.parquet_dir / "date=2026-10-15" / "kind=public_trade"
            late_partition.mkdir(parents=True)
            pq.write_table(
                pa.table({"dedupe_key": ["already-archived"]}),
                late_partition / "part-external.parquet",
                compression="zstd",
            )
            store.append_batch(
                [
                    {
                        "dedupe_key": "size-reconcile-event",
                        "received_utc": "2026-10-15T06:00:00+00:00",
                        "channel": "test",
                        "kind": "public_trade",
                        "market_ticker": "sample",
                        "payload": {"trade_id": "size-reconcile-event"},
                    }
                ]
            )

            actual_bytes = sum(
                path.stat().st_size for path in cfg.parquet_dir.rglob("*.parquet")
            )
            store.parquet_size_reconcile_interval_seconds = 0
            status = store.connection_status()
            self.assertEqual(store.parquet_bytes, actual_bytes)
            self.assertEqual(status["parquet_bytes"], actual_bytes)
            self.assertEqual(status["parquet_flush_count"], 1)
            self.assertEqual(status["parquet_last_flush_rows"], 1)
            self.assertEqual(status["parquet_last_flush_reason"], "ROW_THRESHOLD")
            store.close()

    def test_storage_status_tracks_open_database_path_and_durable_record_watermarks(self):
        with TemporaryDirectory() as temporary:
            cfg = Settings(home=Path(temporary))
            store = Storage(cfg)
            store.open()
            opened = store.connection_status()

            self.assertEqual(
                opened["storage_configured_ledger_path"], str(cfg.ledger_path.resolve())
            )
            self.assertEqual(
                opened["storage_open_database_path"], str(cfg.ledger_path.resolve())
            )
            self.assertTrue(opened["storage_database_path_matches_config"])
            self.assertEqual(
                opened["storage_path_file_identity_state"], "UNCHANGED_SINCE_OPEN"
            )
            self.assertEqual(opened["storage_paper_ledger_record_count"], 0)
            self.assertIsNone(opened["storage_latest_paper_record_utc"])
            self.assertIsNone(opened["storage_latest_market_event_received_utc"])
            self.assertIsNone(opened["storage_latest_disk_paper_record_utc"])
            self.assertIsNone(opened["storage_latest_disk_market_event_received_utc"])
            self.assertIsNone(opened["storage_latest_disk_paper_record_rowid"])
            self.assertIsNone(opened["storage_latest_disk_market_event_rowid"])
            self.assertTrue(opened["storage_paper_watermark_matches_disk"])
            self.assertTrue(opened["storage_market_event_watermark_matches_disk"])
            self.assertEqual(opened["sqlite_bytes"], store._sqlite_size())

            store.append_batch(
                [
                    {
                        "dedupe_key": "identity-event-1",
                        "received_utc": "2026-10-15T06:00:00+00:00",
                        "channel": "test",
                        "kind": "public_trade",
                        "market_ticker": "sample",
                        "payload": {"trade_id": "identity-event-1"},
                    }
                ]
            )
            store.append_paper_record(
                "identity-paper-1", "test-profile", "DECISION", {"eligible": False}
            )
            store.persistence_audit_last_monotonic = 0.0
            current = store.connection_status()

            self.assertEqual(current["sqlite_bytes"], store._sqlite_size())
            self.assertEqual(
                current["storage_latest_market_event_received_utc"],
                "2026-10-15T06:00:00+00:00",
            )
            self.assertEqual(current["storage_paper_ledger_record_count"], 1)
            self.assertIsNotNone(current["storage_latest_paper_record_utc"])
            self.assertEqual(
                current["storage_latest_disk_market_event_received_utc"],
                "2026-10-15T06:00:00+00:00",
            )
            self.assertEqual(current["storage_latest_disk_market_event_rowid"], 1)
            self.assertEqual(current["storage_latest_disk_paper_record_rowid"], 1)
            self.assertEqual(
                current["storage_latest_disk_paper_record_utc"],
                current["storage_latest_paper_record_utc"],
            )
            self.assertTrue(current["storage_paper_watermark_matches_disk"])
            self.assertTrue(current["storage_market_event_watermark_matches_disk"])
            self.assertEqual(
                current["storage_path_file_identity_state"], "UNCHANGED_SINCE_OPEN"
            )
            store.close()

    def test_storage_status_detects_runtime_watermarks_missing_from_disk(self):
        import sqlite3

        with TemporaryDirectory() as temporary:
            cfg = Settings(home=Path(temporary))
            store = Storage(cfg)
            store.open()
            store.append_batch(
                [
                    {
                        "dedupe_key": "watermark-gap-event",
                        "received_utc": "2026-10-15T06:10:00+00:00",
                        "channel": "test",
                        "kind": "public_trade",
                        "market_ticker": "sample",
                        "payload": {"trade_id": "watermark-gap-event"},
                    }
                ]
            )
            store.append_paper_record(
                "watermark-gap-paper", "test-profile", "DECISION", {"eligible": False}
            )

            external = sqlite3.connect(cfg.ledger_path, timeout=5)
            external.execute("DELETE FROM market_events")
            external.execute("DELETE FROM paper_ledger")
            external.commit()
            external.close()

            current = store.connection_status()
            self.assertIsNone(current["storage_latest_disk_market_event_received_utc"])
            self.assertIsNone(current["storage_latest_disk_paper_record_utc"])
            self.assertFalse(current["storage_market_event_watermark_matches_disk"])
            self.assertFalse(current["storage_paper_watermark_matches_disk"])
            store.close()

    def test_event_journal_is_idempotent_and_parquet_reopens(self):
        with TemporaryDirectory() as temporary:
            cfg = Settings(home=Path(temporary))
            store = Storage(cfg)
            with ThreadPoolExecutor(max_workers=1) as writer:
                writer.submit(store.open).result()
                event = {
                    "dedupe_key": "trade-1",
                    "received_utc": "2026-10-14T00:00:00+00:00",
                    "channel": "test",
                    "kind": "public_trade",
                    "market_ticker": "sample",
                    "payload": {"trade_id": "trade-1"},
                }
                self.assertEqual(writer.submit(store.append_batch, [event]).result(), 1)
                self.assertEqual(len(store.parquet_buffer), 1)
                self.assertEqual(writer.submit(store.append_batch, [event]).result(), 0)
                self.assertTrue(writer.submit(store.append_paper_record, "signal-1", "blocked", "signal", {"x": 1}).result())
                self.assertFalse(writer.submit(store.append_paper_record, "signal-1", "blocked", "signal", {"x": 1}).result())
                paper_rows = writer.submit(store.paper_records, "blocked").result()
                self.assertEqual(len(paper_rows), 1)
                self.assertEqual(paper_rows[0]["payload"], {"x": 1})
                self.assertEqual(writer.submit(store.integrity_check).result(), "ok")
                with patch("kalshi15m.storage.os.fsync", wraps=os.fsync) as fsync:
                    writer.submit(store.close).result()
                    # parquet file, manifest listing, checkpoint: each durable in order
                    self.assertEqual(fsync.call_count, 3)
            checkpoint = json.loads(store.archive_checkpoint_path.read_text(encoding="utf-8"))
            self.assertEqual(checkpoint["through_rowid"], 1)
            self.assertEqual(checkpoint["through_dedupe_key"], "trade-1")
            parquet_files = list(cfg.parquet_dir.rglob("*.parquet"))
            self.assertEqual(len(parquet_files), 1)
            self.assertEqual(list(cfg.parquet_dir.rglob("*.partial")), [])
            import pyarrow.parquet as pq

            self.assertEqual(pq.ParquetFile(parquet_files[0]).read().num_rows, 1)
            self.assertEqual(pq.ParquetFile(parquet_files[0]).metadata.row_group(0).column(0).compression, "ZSTD")

    def test_existing_deduplicated_events_skip_payload_normalization(self):
        with TemporaryDirectory() as temporary:
            store = Storage(Settings(home=Path(temporary)))
            store.open()
            event = {
                "dedupe_key": "repeat-websocket-event",
                "received_utc": "2026-10-15T06:00:00+00:00",
                "channel": "websocket_orderbook",
                "kind": "orderbook_delta",
                "market_ticker": "KXBTC15M-SAMPLE",
                "payload": {"seq": 7, "delta": "0.01"},
            }
            self.assertEqual(store.append_batch([event]), 1)

            with patch.object(
                store,
                "_normalized",
                side_effect=AssertionError("duplicate payload was normalized"),
            ):
                self.assertEqual(store.append_batch([event]), 0)

            self.assertEqual(store.events_duplicate, 1)
            self.assertEqual(len(store.parquet_buffer), 1)
            store.close()

    def test_duplicates_are_ignored_after_restart_without_recent_key_window(self):
        with TemporaryDirectory() as temporary:
            cfg = Settings(home=Path(temporary), min_free_bytes=0)
            first = Storage(cfg)
            first.open()
            self.assertEqual(first.append_batch([_trade(1), _trade(2)]), 2)
            first.close()
            second = Storage(cfg)  # empty recent-key window, like a restarted worker
            second.open()
            self.assertEqual(second.append_batch([_trade(2), _trade(3), _trade(3)]), 1)
            self.assertEqual(second.events_duplicate, 2)
            self.assertEqual(
                second.connection.execute("SELECT count(*) FROM market_events").fetchone()[0], 3
            )
            self.assertEqual([r["dedupe_key"] for r in second.parquet_buffer], ["trade-3"])
            second.close()

    def test_unclean_start_recovers_missing_parquet_rows_from_sqlite(self):
        with TemporaryDirectory() as temporary:
            cfg = Settings(home=Path(temporary))
            store = Storage(cfg)
            store.open()
            events = [
                {
                    "dedupe_key": f"trade-{index}",
                    "received_utc": f"2026-10-14T00:00:0{index}+00:00",
                    "channel": "test",
                    "kind": "public_trade",
                    "market_ticker": "sample",
                    "payload": (
                        {"trade_id": f"trade-{index}"}
                        if index == 1
                        else {
                            "trade_id": f"trade-{index}",
                            "market_evidence": "repeatable captured payload " * 150,
                        }
                    ),
                }
                for index in (1, 2)
            ]
            self.assertEqual(store.append_batch(events), 2)
            store.close()
            for path in cfg.parquet_dir.rglob("*.parquet"):
                path.unlink()
            cfg.runtime_dir.mkdir(parents=True, exist_ok=True)
            (cfg.runtime_dir / "status.json").write_text(
                json.dumps({"service_state": "RUNNING"}), encoding="utf-8"
            )

            recovered = Storage(cfg)
            recovered.open()

            self.assertEqual(recovered.parquet_recovery_state, "RECOVERED_MISSING_ROWS")
            self.assertEqual(recovered.parquet_recovery_rows, 2)
            parquet_files = list(cfg.parquet_dir.rglob("*.parquet"))
            self.assertEqual(len(parquet_files), 1)
            import pyarrow.parquet as pq

            archived = pq.read_table(parquet_files[0]).to_pylist()
            self.assertEqual({row["dedupe_key"] for row in archived}, {"trade-1", "trade-2"})
            self.assertTrue(all(isinstance(row["raw_json"], str) for row in archived))
            sqlite_payloads = dict(
                recovered.connection.execute(
                    "SELECT dedupe_key, raw_json FROM market_events ORDER BY dedupe_key"
                ).fetchall()
            )
            self.assertIsInstance(sqlite_payloads["trade-1"], str)
            self.assertIsInstance(sqlite_payloads["trade-2"], bytes)
            self.assertIn('"market_evidence"', decode_journal_payload(sqlite_payloads["trade-2"]))
            self.assertEqual(recovered.integrity_check(), "ok")
            recovered.close()

    def test_stopped_but_incomplete_capture_recovers_missing_parquet_rows(self):
        with TemporaryDirectory() as temporary:
            cfg = Settings(home=Path(temporary), min_free_bytes=0)
            store = Storage(cfg)
            store.open()
            self.assertEqual(
                store.append_batch(
                    [
                        {
                            "dedupe_key": "stopped-incomplete-event",
                            "received_utc": "2026-10-15T18:00:00+00:00",
                            "channel": "test",
                            "kind": "public_trade",
                            "market_ticker": "sample",
                            "payload": {"trade_id": "stopped-incomplete-event"},
                        }
                    ]
                ),
                1,
            )
            store.close()
            for path in cfg.parquet_dir.rglob("*.parquet"):
                path.unlink()

            cfg.runtime_dir.mkdir(parents=True, exist_ok=True)
            (cfg.runtime_dir / "status.json").write_text(
                json.dumps(
                    {
                        "service_state": "STOPPED",
                        "raw_capture_state": "THROTTLED_BUDGET",
                        "parquet_dropped_rows": 1145,
                    }
                ),
                encoding="utf-8",
            )

            recovered = Storage(cfg)
            recovered.open()

            self.assertEqual(recovered.parquet_recovery_state, "RECOVERED_MISSING_ROWS")
            self.assertEqual(recovered.parquet_recovery_rows, 1)
            self.assertEqual(recovered.parquet_dropped_rows, 1145)
            self.assertTrue(recovered.raw_capture_enabled)
            self.assertEqual(recovered.integrity_check(), "ok")
            recovered.close()

    def test_clean_stopped_capture_skips_parquet_recovery(self):
        with TemporaryDirectory() as temporary:
            cfg = Settings(home=Path(temporary), min_free_bytes=0)
            cfg.runtime_dir.mkdir(parents=True, exist_ok=True)
            (cfg.runtime_dir / "status.json").write_text(
                json.dumps(
                    {
                        "service_state": "STOPPED",
                        "raw_capture_state": "ENABLED",
                        "parquet_dropped_rows": 0,
                    }
                ),
                encoding="utf-8",
            )

            store = Storage(cfg)
            store.open()

            self.assertEqual(store.parquet_recovery_state, "SKIPPED_PREVIOUS_RUN_STOPPED")
            self.assertEqual(store.parquet_recovery_rows, 0)
            store.close()

    def test_raw_capture_resumes_after_disk_pressure_and_repairs_archive_first(self):
        with TemporaryDirectory() as temporary:
            cfg = Settings(home=Path(temporary), min_free_bytes=0)
            store = Storage(cfg)
            store.open()
            store.raw_capture_enabled = False
            event = {
                "dedupe_key": "recovered-before-capture-resume",
                "received_utc": "2026-10-15T18:10:00+00:00",
                "channel": "test",
                "kind": "public_trade",
                "market_ticker": "sample",
                "payload": {"trade_id": "recovered-before-capture-resume"},
            }
            self.assertEqual(store.append_batch([event]), 1)
            self.assertEqual(store.parquet_buffer, [])
            self.assertEqual(store.parquet_dropped_rows, 1)

            self.assertTrue(store.resume_raw_capture_if_safe())

            self.assertTrue(store.raw_capture_enabled)
            self.assertEqual(store.parquet_recovery_state, "RECOVERED_MISSING_ROWS")
            self.assertEqual(store.parquet_recovery_rows, 1)
            self.assertEqual(store.parquet_buffer, [])
            import pyarrow.parquet as pq

            archived = [
                row
                for path in cfg.parquet_dir.rglob("*.parquet")
                for row in pq.read_table(path).to_pylist()
            ]
            self.assertEqual(
                [row["dedupe_key"] for row in archived],
                ["recovered-before-capture-resume"],
            )
            store.close()

    def test_raw_capture_does_not_resume_when_parquet_budget_is_exhausted(self):
        with TemporaryDirectory() as temporary:
            cfg = Settings(
                home=Path(temporary), min_free_bytes=0, raw_data_budget_bytes=0
            )
            store = Storage(cfg)
            store.open()
            store.raw_capture_enabled = False
            try:
                self.assertEqual(
                    store.append_batch(
                        [
                            {
                                "dedupe_key": "budget-throttled-event",
                                "received_utc": "2026-10-15T18:20:00+00:00",
                                "channel": "test",
                                "kind": "public_trade",
                                "market_ticker": "sample",
                                "payload": {"trade_id": "budget-throttled-event"},
                            }
                        ]
                    ),
                    1,
                )
                self.assertEqual(store.parquet_dropped_rows, 1)
                self.assertFalse(store.resume_raw_capture_if_safe())
                self.assertFalse(store.raw_capture_enabled)
                self.assertEqual(store.parquet_recovery_state, "SKIPPED_RAW_BUDGET")
            finally:
                store.close()

    def test_process_crash_recovers_committed_journal_rows_not_yet_in_parquet(self):
        with TemporaryDirectory() as temporary:
            home = Path(temporary)
            child_script = "\n".join(
                [
                    "import json, os, sys",
                    "from pathlib import Path",
                    "from kalshi15m.config import Settings",
                    "from kalshi15m.storage import Storage",
                    "cfg = Settings(home=Path(sys.argv[1]))",
                    "cfg.runtime_dir.mkdir(parents=True, exist_ok=True)",
                    "(cfg.runtime_dir / 'status.json').write_text(",
                    "    json.dumps({'service_state': 'RUNNING'}), encoding='utf-8'",
                    ")",
                    "store = Storage(cfg)",
                    "store.open()",
                    "store.append_batch([{",
                    "    'dedupe_key': 'crash-pending-event',",
                    "    'received_utc': '2026-10-15T00:00:00+00:00',",
                    "    'channel': 'test', 'kind': 'public_trade',",
                    "    'market_ticker': 'sample',",
                    "    'payload': {'trade_id': 'crash-pending-event'},",
                    "}])",
                    "assert len(store.parquet_buffer) == 1",
                    "os._exit(93)",
                ]
            )
            environment = os.environ.copy()
            environment["PYTHONPATH"] = str(Path(__file__).resolve().parents[1] / "src")
            crashed = subprocess.run(
                [sys.executable, "-c", child_script, str(home)],
                cwd=Path(__file__).resolve().parents[1],
                env=environment,
                capture_output=True,
                text=True,
                timeout=15,
                check=False,
            )
            self.assertEqual(crashed.returncode, 93, crashed.stderr)

            recovered = Storage(Settings(home=home))
            recovered.open()

            self.assertEqual(recovered.parquet_recovery_state, "RECOVERED_MISSING_ROWS")
            self.assertEqual(recovered.parquet_recovery_rows, 1)
            self.assertEqual(recovered.integrity_check(), "ok")
            parquet_files = list(recovered.cfg.parquet_dir.rglob("*.parquet"))
            self.assertEqual(len(parquet_files), 1)
            import pyarrow.parquet as pq

            archived = pq.read_table(parquet_files[0]).to_pylist()
            self.assertEqual(
                [row["dedupe_key"] for row in archived],
                ["crash-pending-event"],
            )
            recovered.close()

    def test_unclean_start_recovers_conflicting_event_generation_variant(self):
        with TemporaryDirectory() as temporary:
            cfg = Settings(home=Path(temporary))
            canonical_payload = {"trade_id": "canonical"}
            canonical_raw = json.dumps(canonical_payload, sort_keys=True, separators=(",", ":"))
            canonical_hash = hashlib.sha256(canonical_raw.encode("utf-8")).hexdigest()
            store = Storage(cfg)
            store.open()
            store.append_batch(
                [
                    {
                        "dedupe_key": "versioned-event",
                        "received_utc": "2026-10-15T06:00:00+00:00",
                        "exchange_ts": "2026-10-15T06:00:00Z",
                        "channel": "rest_public_trades",
                        "kind": "public_trade",
                        "market_ticker": "sample",
                        "payload": canonical_payload,
                    }
                ]
            )
            store.close()

            alternate_payload = {"trade_id": "alternate"}
            alternate_raw = json.dumps(alternate_payload, sort_keys=True, separators=(",", ":"))
            alternate_hash = hashlib.sha256(alternate_raw.encode("utf-8")).hexdigest()
            connection = sqlite3.connect(cfg.ledger_path)
            try:
                connection.execute(
                    """CREATE TABLE market_event_versions (
                        source_generation TEXT NOT NULL,
                        row_sha256 TEXT NOT NULL,
                        captured_utc TEXT NOT NULL,
                        dedupe_key TEXT NOT NULL,
                        received_utc TEXT NOT NULL,
                        exchange_ts TEXT,
                        channel TEXT NOT NULL,
                        kind TEXT NOT NULL,
                        market_ticker TEXT,
                        raw_json BLOB NOT NULL,
                        payload_sha256 TEXT NOT NULL,
                        PRIMARY KEY (source_generation, row_sha256)
                    ) WITHOUT ROWID"""
                )
                connection.execute(
                    "INSERT INTO market_event_versions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        "older-generation",
                        "alternate-row-hash",
                        "2026-10-15T06:01:00+00:00",
                        "versioned-event",
                        "2026-10-15T06:01:00+00:00",
                        "2026-10-15T06:00:00Z",
                        "rest_public_trades",
                        "public_trade",
                        "sample",
                        alternate_raw,
                        alternate_hash,
                    ),
                )
                connection.commit()
            finally:
                connection.close()

            cfg.runtime_dir.mkdir(parents=True, exist_ok=True)
            (cfg.runtime_dir / "status.json").write_text(
                json.dumps({"service_state": "RUNNING"}), encoding="utf-8"
            )
            recovered = Storage(cfg)
            recovered.open()

            self.assertEqual(recovered.parquet_recovery_state, "RECOVERED_MISSING_ROWS")
            self.assertEqual(recovered.parquet_recovery_rows, 1)
            import pyarrow.parquet as pq

            archive_rows = [
                row
                for path in cfg.parquet_dir.rglob("*.parquet")
                for row in pq.read_table(path).to_pylist()
            ]
            self.assertEqual(len(archive_rows), 2)
            self.assertEqual(
                {row["payload_sha256"] for row in archive_rows},
                {canonical_hash, alternate_hash},
            )
            from scripts.verify_storage_archive import _compare, _journal_rows

            integrity, journal = _journal_rows(cfg.ledger_path)
            comparison = _compare(journal, cfg.parquet_dir)
            self.assertEqual(integrity, "ok")
            self.assertEqual(comparison["missing_from_parquet"], 0)
            self.assertEqual(comparison["extra_in_parquet"], 0)
            self.assertEqual(comparison["parquet_duplicates"], 0)
            self.assertEqual(comparison["payload_hash_mismatches"], 0)
            self.assertEqual(recovered.integrity_check(), "ok")
            recovered.close()

    def test_sqlite_journal_zstd_codec_preserves_small_and_compresses_large_payloads(self):
        small = '{"trade_id":"x"}'
        self.assertEqual(encode_journal_payload(small), small)

        large = json.dumps({"book": "same-level-data " * 400}, separators=(",", ":"))
        encoded = encode_journal_payload(large)
        self.assertIsInstance(encoded, bytes)
        self.assertLess(len(encoded), len(large.encode("utf-8")))
        self.assertEqual(decode_journal_payload(encoded), large)


class WriterLoopTests(unittest.IsolatedAsyncioTestCase):
    async def test_emit_backpressures_when_bounded_ingestion_queue_is_full(self):
        with TemporaryDirectory() as temporary:
            cfg = Settings(home=Path(temporary))
            collector = Collector.__new__(Collector)
            collector.queue = asyncio.Queue(maxsize=1)
            collector.queue_dropped = 0
            collector.status = RuntimeStatus(cfg)
            collector.status.update(mode="PAPER_ONLY", exchange_write_calls=0)
            collector.queue.put_nowait({"kind": "already_queued"})

            pending = asyncio.create_task(
                collector.emit("test", "trade", {"trade_id": "preserved"})
            )
            await asyncio.sleep(0)

            self.assertFalse(pending.done())
            self.assertEqual(collector.queue_dropped, 0)
            collector.queue.get_nowait()
            await asyncio.wait_for(pending, timeout=1)

            event = collector.queue.get_nowait()
            self.assertEqual(event["payload"], {"trade_id": "preserved"})
            self.assertEqual(collector.queue_dropped, 0)
            self.assertEqual(collector.status.snapshot()["mode"], "PAPER_ONLY")
            self.assertEqual(collector.status.snapshot()["exchange_write_calls"], 0)

    async def test_writer_marks_storage_healthy_after_first_successful_batch(self):
        with TemporaryDirectory() as temporary:
            cfg = Settings(home=Path(temporary))
            store = Storage(cfg)
            pool = ThreadPoolExecutor(max_workers=1)
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(pool, store.open)
            collector = Collector.__new__(Collector)
            collector.stopping = asyncio.Event()
            collector.queue = asyncio.Queue(maxsize=10)
            collector.storage = store
            collector.writer_pool = pool
            collector.status = RuntimeStatus(cfg)
            collector.queue.put_nowait(
                {
                    "dedupe_key": "first-successful-journal-event",
                    "received_utc": "2026-10-15T06:00:00+00:00",
                    "channel": "test",
                    "kind": "public_trade",
                    "market_ticker": "sample",
                    "payload": {"trade_id": "first-successful-journal-event"},
                }
            )
            writer = asyncio.create_task(collector.writer_loop())
            try:
                await asyncio.wait_for(collector.queue.join(), timeout=4)
                self.assertEqual(collector.status.data["storage_write_state"], "HEALTHY")
                self.assertEqual(collector.status.data["storage_pending_batch_events"], 0)
                self.assertEqual(collector.status.data["storage_last_write_retry_count"], 0)
            finally:
                collector.stopping.set()
                await asyncio.wait_for(writer, timeout=4)
                await loop.run_in_executor(pool, store.close)
                pool.shutdown(wait=True)

    async def test_writer_group_commits_a_full_queue_in_one_batch(self):
        with TemporaryDirectory() as temporary:
            cfg = Settings(home=Path(temporary), min_free_bytes=0)
            store = Storage(cfg)
            pool = ThreadPoolExecutor(max_workers=1)
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(pool, store.open)
            collector = Collector.__new__(Collector)
            collector.stopping = asyncio.Event()
            collector.queue = asyncio.Queue(maxsize=2000)
            collector.storage = store
            collector.writer_pool = pool
            collector.status = RuntimeStatus(cfg)
            for index in range(1200):
                collector.queue.put_nowait(_trade(index))
            sizes = []
            real_append = store.append_batch

            def recording_append(batch):
                sizes.append(len(batch))
                return real_append(batch)

            store.append_batch = recording_append
            writer = asyncio.create_task(collector.writer_loop())
            try:
                await asyncio.wait_for(collector.queue.join(), timeout=10)
                # One drain, one fsync: not three 500-event commits.
                self.assertEqual(sizes, [1200])
                self.assertEqual(store.events_inserted, 1200)
                await asyncio.sleep(1.0)  # let the cycle finish its status update
                data = collector.status.data
                self.assertEqual(data["writer_batch_events_ema"], 1200.0)
                self.assertGreaterEqual(data["writer_append_ms_ema"], 0.0)
                self.assertGreaterEqual(data["writer_overhead_ms_ema"], 0.0)
            finally:
                collector.stopping.set()
                await asyncio.wait_for(writer, timeout=4)
                await loop.run_in_executor(pool, store.close)
                pool.shutdown(wait=True)

    async def test_writer_retains_batch_until_transient_sqlite_failure_recovers(self):
        with TemporaryDirectory() as temporary:
            cfg = Settings(home=Path(temporary))
            store = Storage(cfg)
            pool = ThreadPoolExecutor(max_workers=1)
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(pool, store.open)
            original_append = store.append_batch
            attempts = 0

            def fail_once_then_append(events):
                nonlocal attempts
                attempts += 1
                if attempts == 1:
                    raise OSError("transient journal I/O error")
                return original_append(events)

            store.append_batch = fail_once_then_append
            collector = Collector.__new__(Collector)
            collector.stopping = asyncio.Event()
            collector.queue = asyncio.Queue(maxsize=10)
            collector.storage = store
            collector.writer_pool = pool
            collector.status = RuntimeStatus(cfg)
            event = {
                "dedupe_key": "retry-journal-event",
                "received_utc": "2026-10-15T06:00:00+00:00",
                "channel": "test",
                "kind": "public_trade",
                "market_ticker": "sample",
                "payload": {"trade_id": "retry-journal-event"},
            }
            collector.queue.put_nowait(event)
            writer = asyncio.create_task(collector.writer_loop())
            try:
                await asyncio.wait_for(collector.queue.join(), timeout=4)
                self.assertEqual(attempts, 2)
                self.assertEqual(
                    await loop.run_in_executor(
                        pool,
                        lambda: store.connection.execute(
                            "SELECT count(*) FROM market_events WHERE dedupe_key = ?",
                            ("retry-journal-event",),
                        ).fetchone()[0],
                    ),
                    1,
                )
                self.assertEqual(collector.status.data["storage_write_state"], "HEALTHY")
                self.assertEqual(collector.status.data["storage_pending_batch_events"], 0)
            finally:
                collector.stopping.set()
                await asyncio.wait_for(writer, timeout=4)
                await loop.run_in_executor(pool, store.close)
                pool.shutdown(wait=True)

    async def test_paper_record_retries_until_idempotent_ledger_commit(self):
        with TemporaryDirectory() as temporary:
            cfg = Settings(home=Path(temporary))
            store = Storage(cfg)
            pool = ThreadPoolExecutor(max_workers=1)
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(pool, store.open)
            original_append = store.append_paper_record
            attempts = 0

            def fail_once_then_append(*args):
                nonlocal attempts
                attempts += 1
                if attempts == 1:
                    raise OSError("transient ledger I/O error")
                return original_append(*args)

            store.append_paper_record = fail_once_then_append
            collector = Collector.__new__(Collector)
            collector.writer_pool = pool
            collector.storage = store
            collector.status = RuntimeStatus(cfg)
            collector.strategy_profile_id = "paper-profile"
            collector.crossmarket_profile_id = None
            try:
                inserted = await collector._append_paper_record(
                    "retry-paper-record", "DECISION", {"candidate": "sample"}
                )
                self.assertTrue(inserted)
                self.assertEqual(attempts, 2)
                self.assertEqual(
                    await loop.run_in_executor(
                        pool,
                        lambda: store.connection.execute(
                            "SELECT count(*) FROM paper_ledger WHERE idempotency_key = ?",
                            ("retry-paper-record",),
                        ).fetchone()[0],
                    ),
                    1,
                )
            finally:
                await loop.run_in_executor(pool, store.close)
                pool.shutdown(wait=True)

    async def test_aged_parquet_buffer_flushes_when_writer_queue_is_idle(self):
        with TemporaryDirectory() as temporary:
            cfg = Settings(home=Path(temporary))
            store = Storage(cfg)
            store.open()
            event = {
                "dedupe_key": "idle-flush-event",
                "received_utc": "2026-10-15T06:00:00+00:00",
                "channel": "test",
                "kind": "public_trade",
                "market_ticker": "sample",
                "payload": {"trade_id": "idle-flush-event"},
            }
            self.assertEqual(store.append_batch([event]), 1)
            store.parquet_buffer_started_monotonic = (
                time.monotonic() - store.parquet_flush_interval_seconds - 1
            )

            collector = Collector.__new__(Collector)
            collector.stopping = asyncio.Event()
            collector.queue = asyncio.Queue()
            collector.storage = store
            collector.writer_pool = ThreadPoolExecutor(max_workers=1)
            collector.status = RuntimeStatus(cfg)
            writer = asyncio.create_task(collector.writer_loop())
            try:
                deadline = asyncio.get_running_loop().time() + 3
                while (store.parquet_buffer or collector.status.data.get("parquet_last_flush_reason") is None) and asyncio.get_running_loop().time() < deadline:
                    await asyncio.sleep(0.05)

                self.assertEqual(store.parquet_buffer, [])
                self.assertEqual(collector.status.data["parquet_last_flush_reason"], "IDLE_BUFFER_AGE")
                self.assertEqual(collector.status.data["parquet_last_flush_rows"], 1)
                parquet_files = list(cfg.parquet_dir.rglob("*.parquet"))
                self.assertEqual(len(parquet_files), 1)
                self.assertFalse(list(cfg.parquet_dir.rglob("*.partial")))
                import pyarrow.parquet as pq

                archived = pq.read_table(parquet_files[0]).to_pylist()
                self.assertEqual([row["dedupe_key"] for row in archived], ["idle-flush-event"])
            finally:
                collector.stopping.set()
                await asyncio.wait_for(writer, timeout=2)
                store.close()
                collector.writer_pool.shutdown(wait=True)

    async def test_aged_parquet_buffer_flushes_while_writer_queue_is_busy(self):
        with TemporaryDirectory() as temporary:
            cfg = Settings(home=Path(temporary))
            store = Storage(cfg)
            initial = {
                "dedupe_key": "busy-flush-initial",
                "received_utc": "2026-10-15T06:00:00+00:00",
                "channel": "test",
                "kind": "public_trade",
                "market_ticker": "sample",
                "payload": {"trade_id": "busy-flush-initial"},
            }
            dedupe, digest, raw = store._normalized(initial)
            store.parquet_buffer = [
                {
                    "dedupe_key": dedupe,
                    "received_utc": initial["received_utc"],
                    "exchange_ts": None,
                    "channel": initial["channel"],
                    "kind": initial["kind"],
                    "market_ticker": initial["market_ticker"],
                    "payload_sha256": digest,
                    "raw_json": raw,
                }
            ]
            store.parquet_buffer_bytes_estimate = len(raw) + 192
            store.parquet_buffer_started_monotonic = (
                time.monotonic() - store.parquet_flush_interval_seconds - 1
            )

            collector = Collector.__new__(Collector)
            collector.stopping = asyncio.Event()
            collector.queue = asyncio.Queue()
            collector.storage = store
            collector.writer_pool = ThreadPoolExecutor(max_workers=1)
            collector.status = RuntimeStatus(cfg)
            collector.WRITER_BATCH_MAX = 500  # this test needs two batches (500 + 1)
            for index in range(501):
                event_id = f"busy-flush-{index}"
                collector.queue.put_nowait(
                    {
                        "dedupe_key": event_id,
                        "received_utc": "2026-10-15T06:00:01+00:00",
                        "channel": "test",
                        "kind": "public_trade",
                        "market_ticker": "sample",
                        "payload": {"trade_id": event_id},
                    }
                )

            writer = asyncio.create_task(collector.writer_loop())
            try:
                deadline = asyncio.get_running_loop().time() + 5
                while (
                    collector.status.data.get("parquet_flush_count", 0) == 0
                    and asyncio.get_running_loop().time() < deadline
                ):
                    await asyncio.sleep(0.02)

                self.assertEqual(
                    collector.status.data.get("parquet_last_flush_reason"),
                    "BUSY_BUFFER_AGE",
                    f"status={collector.status.data!r}; buffer={len(store.parquet_buffer)}; queue={collector.queue.qsize()}",
                )
                self.assertEqual(collector.status.data["parquet_last_flush_rows"], 501)
                self.assertEqual(collector.status.data["parquet_flush_trigger_queue_depth"], 1)
            finally:
                collector.stopping.set()
                await asyncio.wait_for(writer, timeout=5)
                await asyncio.get_running_loop().run_in_executor(
                    collector.writer_pool, store.close
                )
                collector.writer_pool.shutdown(wait=True)

            import pyarrow.parquet as pq

            rows = [
                row
                for path in cfg.parquet_dir.rglob("*.parquet")
                for row in pq.read_table(path, columns=["dedupe_key"]).to_pylist()
            ]
            self.assertEqual(len(rows), 502)
            self.assertEqual(len({row["dedupe_key"] for row in rows}), 502)


class PaperAccountingTests(unittest.TestCase):
    def test_crossmarket_performance_is_grouped_only_by_discovered_series_and_final_settlements(self):
        records = [
            {
                "record_type": "ENTRY_ATTEMPT",
                "payload": {"market_ticker": "KXBTC15M-26OCT1501"},
            },
            {
                "record_type": "PAPER_FILL",
                "payload": {
                    "market_ticker": "KXBTC15M-26OCT1501",
                    "filled_quantity": "2",
                },
            },
            {
                "record_type": "PAPER_SETTLEMENT_FINAL",
                "payload": {
                    "market_ticker": "KXBTC15M-26OCT1501",
                    "official_market_status": "finalized",
                    "official_market_result": "yes",
                    "side": "yes",
                    "net_pnl_dollars": "0.2368",
                },
            },
            {
                "record_type": "PAPER_SETTLEMENT",
                "payload": {
                    "market_ticker": "KXBTC15M-26OCT1502",
                    "official_market_status": "determined",
                    "official_market_result": "no",
                    "side": "no",
                    "net_pnl_dollars": "99",
                },
            },
            {
                "record_type": "ENTRY_ATTEMPT",
                "payload": {"market_ticker": "UNMAPPED-26OCT1501"},
            },
        ]

        summary = crossmarket_performance_summary(
            records, {"KXBTC15M": "Crypto"}
        )

        self.assertEqual(summary["by_market"]["KXBTC15M-26OCT1501"]["asset_class"], "Crypto")
        self.assertEqual(summary["by_market"]["KXBTC15M-26OCT1501"]["attempts"], 1)
        self.assertEqual(summary["by_market"]["KXBTC15M-26OCT1501"]["fills"], 1)
        self.assertEqual(summary["by_market"]["KXBTC15M-26OCT1501"]["settlements"], 1)
        self.assertEqual(summary["by_market"]["KXBTC15M-26OCT1501"]["wins"], 1)
        self.assertEqual(
            summary["by_market"]["KXBTC15M-26OCT1501"]["net_realized_pnl_dollars"],
            "0.2368",
        )
        self.assertNotIn("KXBTC15M-26OCT1502", summary["by_market"])
        self.assertEqual(summary["by_market"]["UNMAPPED-26OCT1501"]["asset_class"], "UNKNOWN")
        self.assertEqual(summary["by_asset_class"]["Crypto"]["settlements"], 1)
        self.assertEqual(summary["by_asset_class"]["UNKNOWN"]["attempts"], 1)

    def test_crossmarket_performance_reports_sample_sizes_uncertainty_and_drawdown(self):
        ticker = "KXBTC15M-26OCT1501"
        outcomes = [
            (ticker, "yes", "yes", "0.20"),
            ("KXETH15M-26OCT1501", "no", "yes", "-0.30"),
            (ticker, "yes", "yes", "0.10"),
            (ticker, "no", "no", None),
        ]
        records = [
            {
                "record_type": "PAPER_SETTLEMENT_FINAL",
                "payload": {
                    "market_ticker": settlement_ticker,
                    "official_market_status": "finalized",
                    "official_market_result": result,
                    "side": side,
                    "net_pnl_dollars": pnl,
                },
            }
            for settlement_ticker, side, result, pnl in outcomes
        ]

        summary = crossmarket_performance_summary(
            records, {"KXBTC15M": "Crypto", "KXETH15M": "Crypto"}
        )
        market = summary["by_market"][ticker]
        asset_class = summary["by_asset_class"]["Crypto"]

        self.assertEqual(market["settlements"], 3)
        self.assertEqual(market["resolved_outcome_count"], 3)
        self.assertEqual(market["net_pnl_observations"], 2)
        self.assertEqual(market["known_net_pnl_settlement_coverage"], "0.6667")
        self.assertEqual(market["resolved_win_rate"], "1.0000")
        market_interval = market["resolved_win_rate_wilson_95"]
        self.assertLess(Decimal(market_interval["lower"]), Decimal("1"))
        self.assertEqual(market_interval["upper"], "1.0000")
        self.assertEqual(market["net_pnl_mean_per_known_settlement_dollars"], "0.150000")
        self.assertEqual(market["net_pnl_sample_stddev_dollars"], "0.070711")
        self.assertEqual(market["known_net_pnl_max_drawdown_dollars"], "0.000000")
        self.assertEqual(asset_class["settlements"], 4)
        self.assertEqual(asset_class["resolved_win_rate"], "0.7500")
        class_interval = asset_class["resolved_win_rate_wilson_95"]
        self.assertLess(Decimal(class_interval["lower"]), Decimal("0.75"))
        self.assertGreater(Decimal(class_interval["upper"]), Decimal("0.75"))
        self.assertEqual(asset_class["net_pnl_observations"], 3)
        self.assertEqual(asset_class["known_net_pnl_max_drawdown_dollars"], "0.300000")

    def test_market_result_is_provisional_until_final_official_status(self):
        self.assertFalse(
            has_final_official_settlement(
                {"status": "determined", "result": "yes", "settlement_ts": None}
            )
        )
        self.assertFalse(
            has_final_official_settlement({"status": "closed", "result": "no"})
        )
        self.assertTrue(
            has_final_official_settlement(
                {"status": "finalized", "result": "no", "settlement_ts": "2026-10-14T21:00:00Z"}
            )
        )

    def test_quadratic_fee_rounds_whole_fill_up_to_centicent(self):
        self.assertEqual(fee_for_single_price_fill("0.50", "1", "quadratic", "1"), Decimal("0.0175"))
        self.assertEqual(fee_for_single_price_fill("0.30", "0.10", "quadratic", "1"), Decimal("0.0015"))
        self.assertEqual(fee_for_single_price_fill("0.30", "1", "quadratic", "1", "maker"), Decimal("0"))
        self.assertIsNone(fee_for_single_price_fill("0.30", "1", "flat", "1"))
        # Kalshi rounds the combined position cost and fee upward to a centicent.
        self.assertEqual(fee_for_single_price_fill("0.7611", "0.01", "quadratic", "1"), Decimal("0.000189"))

    def test_taker_ioc_consumes_price_levels_and_records_partial_remainder(self):
        result = simulate_taker_ioc(
            {
                "ticker": "SAMPLE",
                "orderbook_fp": {
                    "yes_dollars_fp": [["0.22", "1.00"]],
                    "no_dollars_fp": [["0.24", "1.20"], ["0.20", "0.50"]],
                },
            },
            "yes",
            "0.80",
            "2",
            "quadratic",
            "1",
        )

        self.assertEqual(result["fill_state"], "PARTIAL_FILL")
        self.assertEqual(result["filled_quantity"], "1.70")
        self.assertEqual(result["remaining_quantity_cancelled"], "0.30")
        self.assertEqual(result["fills"][0]["price_dollars"], "0.76")
        self.assertEqual(result["fills"][1]["price_dollars"], "0.80")
        self.assertIsNotNone(result["entry_fee_dollars"])

    def test_event_fee_override_and_binary_settlement_are_deterministic(self):
        terms = resolve_fee_terms(
            {"fee_type": "quadratic", "fee_multiplier": "1", "fee_type_override": "quadratic_with_maker_fees", "fee_multiplier_override": "0.5"}
        )
        self.assertEqual(terms, ("quadratic_with_maker_fees", Decimal("0.5")))
        result = settle_binary_paper_fill("yes", "0.76", "1", "0.0032", "yes")
        self.assertEqual(result["payout_dollars"], "1")
        self.assertEqual(result["net_pnl_dollars"], "0.2368")
        self.assertEqual(result["settlement_fee_dollars"], "0")
        unresolved = settle_binary_paper_fill("no", "0.20", "1", None, None)
        self.assertEqual(unresolved["settlement_state"], "UNRESOLVED")
        self.assertIsNone(unresolved["net_pnl_dollars"])


class PaperRuntimeSettlementTests(unittest.TestCase):
    def _collector_for_market_status(self, status, home):
        collector = Collector(Settings(home=Path(home)))
        ticker = "KXETH15M-PAPER-TEST"
        collector.crossmarket_profile_id = "REFLEX_CROSSMARKET_PAPER:TEST"
        collector.crossmarket_strategy_config = {"revision": "REFLEX_CROSSMARKET_PAPER_V1"}
        collector.crossmarket_positions[ticker] = {
            "market_ticker": ticker,
            "side": "no",
            "average_fill_price_dollars": "0.80",
            "filled_quantity": "2",
            "entry_fee_dollars": "0.02",
            "strategy_revision": "REFLEX_CROSSMARKET_PAPER_V1",
            "decision_id": "decision-test",
        }
        collector.api.pages = AsyncMock(
            return_value=[
                {
                    "ticker": ticker,
                    "status": status,
                    "result": "no",
                    "settlement_ts": None if status == "determined" else "2026-10-14T21:00:00Z",
                }
            ]
        )
        collector._append_paper_record = AsyncMock(return_value=True)
        return collector, ticker

    def test_determined_result_keeps_position_open_and_persists_provisional_state(self):
        async def run():
            temporary = TemporaryDirectory()
            try:
                collector, ticker = self._collector_for_market_status("determined", temporary.name)
                await collector.settle_paper_positions(crossmarket=True)
                self.assertIn(ticker, collector.crossmarket_positions)
                self.assertIn(ticker, collector.crossmarket_provisional_outcome_tickers)
                self.assertEqual(collector.crossmarket_settlement_count, 0)
                self.assertEqual(collector.crossmarket_realized_net_pnl, Decimal("0"))
                args = collector._append_paper_record.await_args.args
                self.assertEqual(args[1], "PAPER_RESULT_PROVISIONAL")
            finally:
                temporary.cleanup()

        asyncio.run(run())

    def test_finalized_result_closes_position_and_persists_final_settlement(self):
        async def run():
            temporary = TemporaryDirectory()
            try:
                collector, ticker = self._collector_for_market_status("finalized", temporary.name)
                await collector.settle_paper_positions(crossmarket=True)
                self.assertNotIn(ticker, collector.crossmarket_positions)
                self.assertNotIn(ticker, collector.crossmarket_provisional_outcome_tickers)
                self.assertEqual(collector.crossmarket_settlement_count, 1)
                self.assertEqual(collector.crossmarket_realized_net_pnl, Decimal("0.38"))
                args = collector._append_paper_record.await_args.args
                self.assertEqual(args[1], "PAPER_SETTLEMENT_FINAL")
            finally:
                temporary.cleanup()

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()




class JournalDeltaAllowlistTests(unittest.IsolatedAsyncioTestCase):
    async def _emit(self, prefixes, ticker, kind="orderbook_delta"):
        collector = Collector.__new__(Collector)
        collector.cfg = Settings(home=Path("unused"), journal_delta_series_prefixes=prefixes)
        collector.queue = asyncio.Queue()
        await collector.emit("websocket_orderbook", kind, {"x": 1}, market_ticker=ticker)
        return collector

    async def test_default_journals_every_delta(self):
        c = await self._emit((), "KXGOLD15M-26NOV181500-00")
        self.assertEqual(c.queue.qsize(), 1)

    async def test_out_of_scope_delta_skipped_and_counted(self):
        c = await self._emit(("KXBTC15M", "KXETH15M"), "KXGOLD15M-26NOV181500-00")
        self.assertEqual(c.queue.qsize(), 0)
        self.assertEqual(c.journal_delta_skipped, 1)

    async def test_in_scope_delta_and_other_kinds_kept(self):
        c = await self._emit(("KXBTC15M",), "KXBTC15M-26NOV181500-00")
        self.assertEqual(c.queue.qsize(), 1)
        c = await self._emit(("KXBTC15M",), "KXGOLD15M-26NOV181500-00", kind="trade")
        self.assertEqual(c.queue.qsize(), 1)


class JournalTickerAllowlistTests(unittest.IsolatedAsyncioTestCase):
    async def _emit(self, prefixes, ticker, kind="ticker"):
        collector = Collector.__new__(Collector)
        collector.cfg = Settings(home=Path("unused"), journal_ticker_series_prefixes=prefixes)
        collector.queue = asyncio.Queue()
        await collector.emit("websocket_ticker", kind, {"x": 1}, market_ticker=ticker)
        return collector

    async def test_default_journals_every_ticker_row(self):
        self.assertEqual((await self._emit((), "KXGOLD15M-26NOV181500-00")).queue.qsize(), 1)

    async def test_out_of_scope_ticker_skipped_and_counted_in_scope_kept(self):
        skipped = await self._emit(("KXBTC15M",), "KXGOLD15M-26NOV181500-00")
        self.assertEqual((skipped.queue.qsize(), skipped.journal_ticker_skipped), (0, 1))
        self.assertEqual((await self._emit(("KXBTC15M",), "KXBTC15M-26NOV181500-00")).queue.qsize(), 1)

    async def test_ticker_allowlist_never_filters_trades_or_deltas(self):
        for kind in ("public_trade", "orderbook_delta"):
            kept = await self._emit(("KXBTC15M",), "KXGOLD15M-26NOV181500-00", kind=kind)
            self.assertEqual(kept.queue.qsize(), 1)


def _trade(index):
    return {
        "dedupe_key": f"trade-{index}",
        "received_utc": "2026-10-15T06:00:00+00:00",
        "channel": "test",
        "kind": "public_trade",
        "market_ticker": "sample",
        "payload": {"trade_id": f"trade-{index}"},
    }


def _archived_keys(cfg):
    import pyarrow.parquet as pq

    keys = []
    for path in cfg.parquet_dir.rglob("*.parquet"):
        keys.extend(pq.read_table(path, columns=["dedupe_key"]).column(0).to_pylist())
    return keys


def _mark_unclean(cfg):
    cfg.runtime_dir.mkdir(parents=True, exist_ok=True)
    (cfg.runtime_dir / "status.json").write_text(
        json.dumps({"service_state": "RUNNING"}), encoding="utf-8"
    )


class ArchiveCheckpointRecoveryTests(unittest.TestCase):
    def test_crash_recovers_only_rows_after_checkpoint_without_duplicates(self):
        with TemporaryDirectory() as temporary:
            cfg = Settings(home=Path(temporary), min_free_bytes=0)
            store = Storage(cfg)
            store.open()
            store.append_batch([_trade(i) for i in range(5)])
            store.flush_parquet(reason="TEST")
            # Committed to SQLite, still only in the memory buffer at the "crash".
            store.append_batch([_trade(i) for i in range(5, 8)])
            store.connection.close()
            store.connection = None
            _mark_unclean(cfg)

            recovered = Storage(cfg)
            recovered.open()

            self.assertEqual(recovered.parquet_recovery_mode, "INCREMENTAL_FROM_ROWID_5")
            self.assertEqual(recovered.parquet_recovery_rows, 3)
            keys = _archived_keys(cfg)
            self.assertEqual(sorted(keys), sorted(f"trade-{i}" for i in range(8)))
            checkpoint = json.loads(recovered.archive_checkpoint_path.read_text(encoding="utf-8"))
            self.assertEqual(checkpoint["through_rowid"], 8)
            recovered.close()

    def test_crash_after_publish_before_checkpoint_does_not_duplicate(self):
        with TemporaryDirectory() as temporary:
            cfg = Settings(home=Path(temporary), min_free_bytes=0)
            store = Storage(cfg)
            store.open()
            store.append_batch([_trade(i) for i in range(3)])
            store.flush_parquet(reason="TEST")
            store.append_batch([_trade(i) for i in range(3, 6)])
            # Files publish, then the process dies before the checkpoint moves.
            with patch.object(store, "_write_archive_checkpoint"):
                store.flush_parquet(reason="TEST")
            store.connection.close()
            store.connection = None
            _mark_unclean(cfg)

            recovered = Storage(cfg)
            recovered.open()

            self.assertEqual(recovered.parquet_recovery_mode, "INCREMENTAL_FROM_ROWID_3")
            self.assertEqual(recovered.parquet_recovery_rows, 0)
            self.assertEqual(sorted(_archived_keys(cfg)), sorted(f"trade-{i}" for i in range(6)))
            recovered.close()

    def test_mismatched_or_shrunken_checkpoint_falls_back_to_full_scan(self):
        with TemporaryDirectory() as temporary:
            cfg = Settings(home=Path(temporary), min_free_bytes=0)
            store = Storage(cfg)
            store.open()
            store.append_batch([_trade(i) for i in range(4)])
            store.close()
            path = store.archive_checkpoint_path
            record = json.loads(path.read_text(encoding="utf-8"))
            record["through_dedupe_key"] = "not-this-row"
            path.write_text(json.dumps(record), encoding="utf-8")
            _mark_unclean(cfg)

            recovered = Storage(cfg)
            recovered.open()
            self.assertEqual(recovered.parquet_recovery_mode, "FULL_SCAN")
            self.assertEqual(recovered.parquet_recovery_rows, 0)
            recovered.close()

            for parquet in cfg.parquet_dir.rglob("*.parquet"):
                parquet.unlink()
            _mark_unclean(cfg)
            rebuilt = Storage(cfg)
            rebuilt.open()
            self.assertEqual(rebuilt.parquet_recovery_mode, "FULL_SCAN")
            self.assertEqual(rebuilt.parquet_recovery_rows, 4)
            rebuilt.close()

    def test_checkpoint_does_not_advance_past_dropped_rows(self):
        with TemporaryDirectory() as temporary:
            cfg = Settings(home=Path(temporary), min_free_bytes=0)
            store = Storage(cfg)
            store.open()
            store.append_batch([_trade(0)])
            store.flush_parquet(reason="TEST")
            store.raw_capture_enabled = False
            store.append_batch([_trade(1)])
            store.raw_capture_enabled = True
            store.append_batch([_trade(2)])
            store.flush_parquet(reason="TEST")
            record = json.loads(store.archive_checkpoint_path.read_text(encoding="utf-8"))
            self.assertEqual(record["through_rowid"], 1)
            store.connection.close()
            store.connection = None
            _mark_unclean(cfg)

            recovered = Storage(cfg)
            recovered.open()
            self.assertEqual(sorted(_archived_keys(cfg)), ["trade-0", "trade-1", "trade-2"])
            recovered.close()


    def test_recovery_low_disk_refuses_to_advance_checkpoint(self):
        # The positive recovery tests above use tiny synthetic fixtures and
        # independently exercise recovery. This test checks the real disk guard.
        with TemporaryDirectory() as temporary:
            cfg = Settings(home=Path(temporary), min_free_bytes=1 << 62)
            store = Storage(cfg)
            store.open()
            store.append_batch([_trade(0)])
            store.connection.close()
            store.connection = None
            _mark_unclean(cfg)

            recovered = Storage(cfg)
            recovered.open()
            self.assertEqual(recovered.parquet_recovery_state, "SKIPPED_LOW_DISK")
            self.assertEqual(recovered.parquet_recovery_mode, "SKIPPED_LOW_DISK")
            self.assertTrue(recovered.archive_gap_pending)
            self.assertEqual(_archived_keys(cfg), [])
            recovered.connection.close()
            recovered.connection = None

def _flushed_store(cfg, sizes):
    """Journal and flush one Parquet file per size; leave the store open."""
    store = Storage(cfg)
    store.open()
    start = 0
    for size in sizes:
        store.append_batch([_trade(i) for i in range(start, start + size)])
        store.flush_parquet(reason="TEST")
        start += size
    return store, start


def _crash(store, cfg):
    store.connection.close()
    store.connection = None
    _mark_unclean(cfg)


def _expected(count):
    return sorted(f"trade-{i}" for i in range(count))


class JournalRetentionTests(unittest.TestCase):
    def _store(self, tmp, retention=3600):
        cfg = Settings(home=Path(tmp), min_free_bytes=0, journal_retention_seconds=retention)
        store = Storage(cfg)
        store.open()
        store.PRUNE_BATCH_ROWS = 3
        store.PRUNE_MIN_INTERVAL_SECONDS = 0
        return cfg, store

    def test_only_archived_old_rows_are_pruned_and_a_restart_still_verifies(self):
        with TemporaryDirectory() as tmp:
            cfg, store = self._store(tmp)
            store.append_batch([_trade(i) for i in range(10)])
            store.flush_parquet(reason="TEST")  # checkpoint through row 10, prunes rows 1-3
            while store.prune_journal():
                pass
            count = lambda: store.connection.execute("SELECT count(*) FROM market_events").fetchone()[0]
            self.assertEqual(count(), 1)  # the checkpoint row itself is always kept
            self.assertEqual(store.journal_pruned_rows, 9)
            self.assertEqual(sorted(_archived_keys(cfg)), _expected(10))  # nothing lost
            store.append_batch([_trade(10)])
            self.assertEqual(store.latest_event[0], 11)  # rowids keep counting, none reused
            store.flush_parquet(reason="TEST")
            store.close()
            again = Storage(cfg)
            again.open()
            self.assertEqual(again.archive_integrity_state, "VERIFIED_COMPLETE_CLEAN_STOP_CHECKPOINT")
            self.assertTrue(again.verify_archive(full=True)["ok"])
            again.close()

    def test_nothing_is_pruned_while_a_gap_is_pending_or_retention_is_off(self):
        with TemporaryDirectory() as tmp:
            cfg, store = self._store(tmp)
            store.append_batch([_trade(i) for i in range(10)])
            store.archive_gap_pending = True  # e.g. rows dropped before reaching Parquet
            store.flush_parquet(reason="TEST")
            self.assertEqual(store.prune_journal(), 0)
            self.assertEqual(store.journal_prune_blocked, "NO_CHECKPOINT_THIS_RUN")
            store.checkpoint_rowid_this_run = 10  # even with a stale checkpoint in hand
            self.assertEqual(store.prune_journal(), 0)
            self.assertEqual(store.journal_prune_blocked, "ARCHIVE_GAP_OR_MANIFEST_FAILURE")
            store.close()
        with TemporaryDirectory() as tmp:
            cfg, store = self._store(tmp, retention=0)
            store.append_batch([_trade(i) for i in range(10)])
            store.flush_parquet(reason="TEST")
            self.assertEqual(store.prune_journal(), 0)
            self.assertEqual(store.connection.execute("SELECT count(*) FROM market_events").fetchone()[0], 10)
            store.close()

    def test_rows_inside_the_retention_window_are_kept(self):
        with TemporaryDirectory() as tmp:
            cfg, store = self._store(tmp, retention=3600)
            fresh = datetime.now(timezone.utc).isoformat()
            store.append_batch([dict(_trade(i), received_utc=fresh) for i in range(10)])
            store.flush_parquet(reason="TEST")
            self.assertEqual(store.prune_journal(), 0)
            self.assertEqual(store.connection.execute("SELECT count(*) FROM market_events").fetchone()[0], 10)
            store.close()


class ArchiveManifestIntegrityTests(unittest.TestCase):
    def test_same_size_corruption_of_newest_file_forces_full_scan_without_loss_or_dupes(self):
        with TemporaryDirectory() as temporary:
            cfg = Settings(home=Path(temporary), min_free_bytes=0)
            store, count = _flushed_store(cfg, [3, 3, 3])
            newest = cfg.parquet_dir / store._manifest_read()[0][-1]["path"]
            _crash(store, cfg)
            data = bytearray(newest.read_bytes())
            data[len(data) // 2] ^= 0xFF  # flip a byte: size unchanged
            newest.write_bytes(bytes(data))

            recovered = Storage(cfg)
            recovered.open()

            self.assertEqual(recovered.parquet_recovery_mode, "FULL_SCAN")
            self.assertTrue(recovered.parquet_recovery_reason.startswith("MANIFEST_FILE_HASH_MISMATCH"))
            self.assertEqual(recovered.archive_quarantined_files, 1)
            self.assertEqual(len(list(cfg.parquet_dir.rglob("*.parquet.corrupt-*"))), 1)
            self.assertEqual(sorted(_archived_keys(cfg)), _expected(count))
            self.assertEqual(recovered.archive_integrity_state, "VERIFIED_COMPLETE")
            self.assertTrue(recovered.verify_archive(full=True)["ok"])
            self.assertEqual(len(list(cfg.parquet_dir.glob("_archive_manifest.jsonl.superseded-*"))), 1)
            recovered.close()

    def test_file_replacement_is_detected(self):
        with TemporaryDirectory() as temporary:
            cfg = Settings(home=Path(temporary), min_free_bytes=0)
            store, count = _flushed_store(cfg, [2, 5])
            entries = store._manifest_read()[0]
            small = cfg.parquet_dir / entries[0]["path"]
            large = cfg.parquet_dir / entries[1]["path"]
            _crash(store, cfg)
            small.write_bytes(large.read_bytes())  # a different, valid Parquet file

            recovered = Storage(cfg)
            recovered.open()

            self.assertEqual(recovered.parquet_recovery_mode, "FULL_SCAN")
            self.assertTrue(recovered.parquet_recovery_reason.startswith("MANIFEST_FILE_SIZE_MISMATCH"))
            # The replacement's rows are duplicates of the other file; the original
            # file's rows come back from the journal.
            self.assertEqual(set(_archived_keys(cfg)), set(_expected(count)))
            self.assertEqual(recovered.archive_integrity_state, "VERIFIED_COMPLETE")
            recovered.close()

    def test_missing_archive_part_is_rebuilt_from_journal(self):
        with TemporaryDirectory() as temporary:
            cfg = Settings(home=Path(temporary), min_free_bytes=0)
            store, count = _flushed_store(cfg, [2, 2, 2])
            oldest = cfg.parquet_dir / store._manifest_read()[0][0]["path"]
            _crash(store, cfg)
            oldest.unlink()

            recovered = Storage(cfg)
            recovered.open()

            self.assertEqual(recovered.parquet_recovery_mode, "FULL_SCAN")
            self.assertTrue(recovered.parquet_recovery_reason.startswith("MANIFEST_FILE_MISSING"))
            self.assertEqual(recovered.parquet_recovery_rows, 2)
            self.assertEqual(sorted(_archived_keys(cfg)), _expected(count))
            recovered.close()

    def test_old_same_size_corruption_is_only_caught_by_full_verification(self):
        # Documented limit: startup hashes the newest MANIFEST_TAIL_HASH_FILES only.
        with TemporaryDirectory() as temporary:
            cfg = Settings(home=Path(temporary), min_free_bytes=0)
            with patch.object(Storage, "MANIFEST_TAIL_HASH_FILES", 1):
                store, count = _flushed_store(cfg, [2, 2, 2])
                oldest = cfg.parquet_dir / store._manifest_read()[0][0]["path"]
                _crash(store, cfg)
                original = oldest.stat()
                data = bytearray(oldest.read_bytes())
                data[len(data) // 2] ^= 0xFF
                oldest.write_bytes(bytes(data))
                os.utime(oldest, ns=(original.st_atime_ns, original.st_mtime_ns))

                recovered = Storage(cfg)
                recovered.open()
                self.assertTrue(recovered.parquet_recovery_mode.startswith("INCREMENTAL_FROM_ROWID_"))
                self.assertTrue(recovered.verify_archive()["ok"])
                audit = recovered.verify_archive(full=True)
                self.assertFalse(audit["ok"])
                self.assertTrue(audit["violations"][0].startswith("MANIFEST_FILE_HASH_MISMATCH"))
                recovered.connection.close()
                recovered.connection = None

    def test_interrupted_checkpoint_write_is_counted_and_recovered_idempotently(self):
        with TemporaryDirectory() as temporary:
            cfg = Settings(home=Path(temporary), min_free_bytes=0)
            store, count = _flushed_store(cfg, [3])
            store.append_batch([_trade(i) for i in range(3, 6)])
            with patch.object(store, "_write_archive_checkpoint_file", side_effect=OSError("disk")):
                store.flush_parquet(reason="TEST")
            self.assertEqual(store.archive_checkpoint_write_failures, 1)
            record = json.loads(store.archive_checkpoint_path.read_text(encoding="utf-8"))
            self.assertEqual(record["through_rowid"], 3)  # older checkpoint kept
            _crash(store, cfg)

            recovered = Storage(cfg)
            recovered.open()
            self.assertEqual(recovered.parquet_recovery_mode, "INCREMENTAL_FROM_ROWID_3")
            self.assertEqual(recovered.parquet_recovery_rows, 0)
            self.assertEqual(sorted(_archived_keys(cfg)), _expected(6))
            recovered.close()

    def test_failed_parquet_fsync_publishes_nothing_and_keeps_buffer(self):
        with TemporaryDirectory() as temporary:
            cfg = Settings(home=Path(temporary), min_free_bytes=0)
            store, _ = _flushed_store(cfg, [2])
            store.append_batch([_trade(2), _trade(3)])
            with patch("kalshi15m.storage.os.fsync", side_effect=OSError(5, "I/O error")):
                with self.assertRaises(OSError):
                    store.flush_parquet(reason="TEST")
            self.assertEqual(len(store.parquet_buffer), 2)
            self.assertEqual(list(cfg.parquet_dir.rglob("*.partial")), [])
            record = json.loads(store.archive_checkpoint_path.read_text(encoding="utf-8"))
            self.assertEqual(record["through_rowid"], 2)
            self.assertEqual(store.flush_parquet(reason="TEST"), 2)
            self.assertEqual(sorted(_archived_keys(cfg)), _expected(4))
            store.close()

    def test_failed_manifest_fsync_freezes_checkpoint_and_next_start_adopts_files(self):
        with TemporaryDirectory() as temporary:
            cfg = Settings(home=Path(temporary), min_free_bytes=0)
            store, _ = _flushed_store(cfg, [2])
            store.append_batch([_trade(2), _trade(3)])
            with patch.object(store, "_manifest_append", side_effect=OSError(5, "I/O error")):
                store.flush_parquet(reason="TEST")
            self.assertEqual(store.archive_manifest_failures, 1)
            self.assertTrue(store.archive_gap_pending)
            self.assertEqual(store.archive_integrity_state, "INCOMPLETE")
            store.append_batch([_trade(4)])
            store.flush_parquet(reason="TEST")  # must not advance past the unlisted file
            record = json.loads(store.archive_checkpoint_path.read_text(encoding="utf-8"))
            self.assertEqual(record["through_rowid"], 2)
            _crash(store, cfg)

            recovered = Storage(cfg)
            recovered.open()
            self.assertEqual(recovered.parquet_recovery_mode, "INCREMENTAL_FROM_ROWID_2")
            self.assertEqual(recovered.parquet_recovery_rows, 0)
            self.assertEqual(sorted(_archived_keys(cfg)), _expected(5))
            self.assertEqual(recovered.archive_integrity_state, "VERIFIED_COMPLETE")
            self.assertTrue(recovered.verify_archive(full=True)["ok"])
            recovered.close()

    def test_torn_manifest_tail_is_ignored_then_truncated_by_next_append(self):
        with TemporaryDirectory() as temporary:
            cfg = Settings(home=Path(temporary), min_free_bytes=0)
            store, _ = _flushed_store(cfg, [2])
            with store.archive_manifest_path.open("ab") as handle:
                handle.write(b'{"seq":2,"prev":"abc"')  # crash mid-append
            self.assertEqual(len(store._manifest_read()[0]), 1)
            self.assertFalse(store.verify_archive()["ok"])
            store.append_batch([_trade(2)])
            store.flush_parquet(reason="TEST")
            self.assertTrue(store.verify_archive(full=True)["ok"])
            self.assertEqual(len(store._manifest_read()[0]), 2)
            store.close()

    def test_partial_recovery_by_index_cap_never_reports_complete_and_resumes_idempotently(self):
        with TemporaryDirectory() as temporary:
            cfg = Settings(home=Path(temporary), min_free_bytes=0, recovery_index_max_bytes=8192)
            # Below parquet_flush_rows (2000): 1500 archived rows, then 10 journal-only.
            store, _ = _flushed_store(cfg, [1500])
            store.append_batch([_trade(i) for i in range(1500, 1510)])
            _crash(store, cfg)
            # Only archive-side fingerprints grow the index (the journal pass just
            # probes), so a full scan of 1500 archived rows exceeds the 8 KiB cap.
            store.archive_checkpoint_path.unlink()

            partial = Storage(cfg)
            partial.open()
            self.assertEqual(partial.parquet_recovery_state, "PARTIAL_RECOVERY_INDEX_BUDGET")
            self.assertTrue(partial.archive_gap_pending)
            self.assertEqual(partial.archive_integrity_state, "INCOMPLETE")
            self.assertFalse(partial.archive_checkpoint_path.exists())
            partial.connection.close()
            partial.connection = None
            _mark_unclean(cfg)

            full = Storage(Settings(home=Path(temporary), min_free_bytes=0))
            full.open()
            self.assertEqual(full.archive_integrity_state, "VERIFIED_COMPLETE")
            self.assertEqual(sorted(_archived_keys(cfg)), _expected(1510))  # no duplicates
            full.close()

    def test_restart_after_recovery_is_idempotent(self):
        with TemporaryDirectory() as temporary:
            cfg = Settings(home=Path(temporary), min_free_bytes=0)
            store, count = _flushed_store(cfg, [3])
            store.append_batch([_trade(3), _trade(4)])
            _crash(store, cfg)
            for expected_rows in (2, 0, 0):
                recovered = Storage(cfg)
                recovered.open()
                self.assertEqual(recovered.parquet_recovery_rows, expected_rows)
                self.assertEqual(sorted(_archived_keys(cfg)), _expected(5))
                recovered.connection.close()
                recovered.connection = None
                _mark_unclean(cfg)

    def test_recovery_index_never_uses_temp_and_leaves_no_scratch(self):
        with TemporaryDirectory() as temporary:
            cfg = Settings(home=Path(temporary), min_free_bytes=0)
            store = Storage(cfg)
            store.open()
            store.append_batch([_trade(0)])
            _crash(store, cfg)
            with patch("kalshi15m.storage.sqlite3.connect", wraps=sqlite3.connect) as connect:
                recovered = Storage(cfg)
                recovered.open()
            targets = [str(call.args[0]) for call in connect.call_args_list]
            self.assertNotIn("", targets)
            self.assertTrue(
                any(str(cfg.recovery_workspace_dir) in target for target in targets), targets
            )
            self.assertEqual(list(cfg.recovery_workspace_dir.glob("archive-fingerprints-*")), [])
            recovered.close()

    def test_clean_stop_with_stale_checkpoint_runs_recovery_instead_of_assuming(self):
        with TemporaryDirectory() as temporary:
            cfg = Settings(home=Path(temporary), min_free_bytes=0)
            store, _ = _flushed_store(cfg, [2])
            store.append_batch([_trade(2)])
            with patch.object(store, "_write_archive_checkpoint_file", side_effect=OSError("disk")):
                store.close()  # shutdown flush published the file; checkpoint write failed
            (cfg.runtime_dir).mkdir(parents=True, exist_ok=True)
            (cfg.runtime_dir / "status.json").write_text(
                json.dumps({"service_state": "STOPPED", "raw_capture_state": "ENABLED"}),
                encoding="utf-8",
            )
            recovered = Storage(cfg)
            recovered.open()
            self.assertNotEqual(recovered.parquet_recovery_state, "SKIPPED_PREVIOUS_RUN_STOPPED")
            self.assertEqual(recovered.archive_integrity_state, "VERIFIED_COMPLETE")
            self.assertEqual(sorted(_archived_keys(cfg)), _expected(3))
            recovered.close()


class WebSocketReconnectDedupeTests(unittest.TestCase):
    def test_reconnect_reusing_sid_and_seq_journals_new_events(self):
        class FakeWebSocket:
            def __init__(self, messages):
                self.messages = messages

            def __aiter__(self):
                async def iterate():
                    for message in self.messages:
                        yield json.dumps(message)
                return iterate()

        messages = [
            {
                "type": "orderbook_snapshot", "sid": 2, "seq": 1,
                "msg": {
                    "market_ticker": "SAMPLE",
                    "yes_dollars_fp": [["0.40", "2.00"]],
                    "no_dollars_fp": [["0.50", "3.00"]],
                },
            },
            {
                "type": "orderbook_delta", "sid": 2, "seq": 2,
                "msg": {"market_ticker": "SAMPLE", "side": "yes", "price_dollars": "0.40", "delta_fp": "1.00"},
            },
        ]
        with TemporaryDirectory() as root:
            collector = Collector(Settings(home=Path(root)))
            collector.open_tickers.add("SAMPLE")
            keys = []
            for connection in ("first", "second"):
                collector.ws_connection_id = connection
                collector.last_seq.clear()
                collector.subscription_channel_by_sid[2] = "orderbook_delta"
                collector.subscription_membership[2] = {"SAMPLE"}
                asyncio.run(collector._receive_ws(FakeWebSocket(messages)))
                while not collector.queue.empty():
                    keys.append(collector.queue.get_nowait()["dedupe_key"])
            self.assertEqual(len(keys), 4)
            self.assertEqual(len(set(keys)), 4)


class JournalBudgetTests(unittest.IsolatedAsyncioTestCase):
    async def test_deltas_stop_at_budget_but_other_kinds_persist(self):
        collector = Collector.__new__(Collector)
        collector.cfg = Settings(home=Path("unused"), journal_budget_bytes=100)
        collector.queue = asyncio.Queue()

        class FakeStorage:
            sqlite_bytes = 100

        collector.storage = FakeStorage()
        await collector.emit("websocket_orderbook", "orderbook_delta", {"x": 1}, market_ticker="KXBTC15M-X")
        await collector.emit("websocket_orderbook", "orderbook_snapshot", {"x": 1}, market_ticker="KXBTC15M-X")
        self.assertEqual(collector.queue.qsize(), 1)
        self.assertEqual(collector.journal_delta_budget_skipped, 1)
        collector.storage.sqlite_bytes = 99
        await collector.emit("websocket_orderbook", "orderbook_delta", {"x": 1}, market_ticker="KXBTC15M-X")
        self.assertEqual(collector.queue.qsize(), 2)


class DiskFullFixtureTests(unittest.TestCase):
    def test_enospc_on_parquet_keeps_journal_buffer_and_checkpoint(self):
        import errno

        with TemporaryDirectory() as temporary:
            cfg = Settings(home=Path(temporary))
            store = Storage(cfg)
            store.open()
            store.append_batch([_trade(0)])
            store.flush_parquet(reason="TEST")
            store.append_batch([_trade(1), _trade(2)])
            with patch(
                "kalshi15m.storage.pq.write_table",
                side_effect=OSError(errno.ENOSPC, "No space left on device"),
            ):
                with self.assertRaises(OSError):
                    store.flush_parquet(reason="TEST")
            self.assertEqual(len(store.parquet_buffer), 2)
            self.assertEqual(list(cfg.parquet_dir.rglob("*.partial")), [])
            record = json.loads(store.archive_checkpoint_path.read_text(encoding="utf-8"))
            self.assertEqual(record["through_rowid"], 1)
            self.assertEqual(store.integrity_check(), "ok")
            self.assertEqual(store.flush_parquet(reason="TEST"), 2)
            record = json.loads(store.archive_checkpoint_path.read_text(encoding="utf-8"))
            self.assertEqual(record["through_rowid"], 3)
            self.assertEqual(sorted(_archived_keys(cfg)), ["trade-0", "trade-1", "trade-2"])
            store.close()


class ProcessJournalStorageTests(unittest.TestCase):
    def test_child_process_owns_the_journal_and_archive_while_parent_keeps_paper_ledger(self):
        from kalshi15m.journal_process import ProcessJournalStorage

        with TemporaryDirectory() as tmp:
            cfg = Settings(home=Path(tmp), min_free_bytes=0)
            store = ProcessJournalStorage(cfg)
            store.open()
            try:
                self.assertEqual(store.append_batch([_trade(i) for i in range(5)]), 5)
                self.assertEqual(store.append_batch([_trade(i) for i in range(3, 8)]), 5)  # 2 duplicates; pipelined: returns the batch size
                self.assertTrue(store.append_paper_record("p1", "PROFILE", "DECISION", {"eligible": False}))
                self.assertEqual(store.flush_parquet(reason="TEST"), 8)
                self.assertEqual(sorted(_archived_keys(cfg)), _expected(8))
                time.sleep(1.5)  # one state interval
                status = store.connection_status()
                self.assertTrue(status["journal_process_alive"])
                self.assertNotEqual(status["journal_process_pid"], os.getpid())
                self.assertEqual(store.events_inserted, 8)
                self.assertEqual(store.events_duplicate, 2)
                self.assertEqual(status["storage_paper_ledger_record_count"], 1)
                self.assertTrue(status["storage_paper_watermark_matches_disk"])
                self.assertEqual(len(store.paper_records("PROFILE")), 1)
                store.raw_capture_enabled = False
                time.sleep(1.5)
                self.assertFalse(store.raw_capture_enabled)
            finally:
                store.close()
            self.assertFalse(store.child_alive)
            again = Storage(cfg)
            again.open()
            self.assertEqual(again.archive_integrity_state, "VERIFIED_COMPLETE_CLEAN_STOP_CHECKPOINT")
            self.assertEqual(again.connection.execute("SELECT count(*) FROM market_events").fetchone()[0], 8)
            again.close()

    def test_a_second_journal_process_cannot_share_the_journal(self):
        from kalshi15m.journal_process import ProcessJournalStorage

        with TemporaryDirectory() as tmp:
            cfg = Settings(home=Path(tmp), min_free_bytes=0)
            first = ProcessJournalStorage(cfg)
            first.open()
            try:
                second = ProcessJournalStorage(cfg)
                with self.assertRaises(RuntimeError) as caught:
                    second.open()
                self.assertIn("ALREADY_RUNNING", str(caught.exception))
            finally:
                first.close()

    def test_losing_the_child_makes_appends_fail_instead_of_dropping_events(self):
        from kalshi15m.journal_process import ProcessJournalStorage

        with TemporaryDirectory() as tmp:
            cfg = Settings(home=Path(tmp), min_free_bytes=0)
            store = ProcessJournalStorage(cfg)
            store.open()
            store._process.kill()
            store._process.join(10)
            time.sleep(0.5)
            with self.assertRaises(RuntimeError):
                store.append_batch([_trade(1)])
            self.assertFalse(store.connection_status()["journal_process_alive"])
            store.close()


class AsyncFlushTests(unittest.TestCase):
    def _store(self, tmp, **kw):
        cfg = Settings(home=Path(tmp), min_free_bytes=0, **kw)
        store = Storage(cfg)
        store.open()
        store.async_flush = True
        store.parquet_flush_rows = 5
        return cfg, store

    def test_appends_continue_while_a_flush_is_stuck_and_everything_lands(self):
        import threading

        with TemporaryDirectory() as tmp:
            cfg, store = self._store(tmp)
            gate, started = threading.Event(), threading.Event()
            real = store._write_partitioned_parquet

            def slow(rows):
                started.set()
                gate.wait(10)
                return real(rows)

            store._write_partitioned_parquet = slow
            store.append_batch([_trade(i) for i in range(5)])  # reaches the threshold: flush starts
            self.assertTrue(started.wait(5))
            began = time.monotonic()
            store.append_batch([_trade(i) for i in range(5, 9)])  # must not wait for the stuck flush
            self.assertLess(time.monotonic() - began, 2.0)
            self.assertTrue(store.connection_status()["parquet_flush_in_flight"])
            gate.set()
            store._write_partitioned_parquet = real
            store.flush_parquet(reason="TEST")  # drains the background flush, then writes the rest
            self.assertEqual(sorted(_archived_keys(cfg)), _expected(9))
            record = json.loads(store.archive_checkpoint_path.read_text(encoding="utf-8"))
            self.assertEqual(record["through_rowid"], 9)  # checkpoints stayed ordered
            self.assertTrue(store.verify_archive(full=True)["ok"])
            store.close()

    def test_failed_background_flush_keeps_its_rows_and_they_are_archived_later(self):
        with TemporaryDirectory() as tmp:
            cfg, store = self._store(tmp)
            real, calls = store._write_partitioned_parquet, []

            def fail_once(rows):
                calls.append(1)
                if len(calls) == 1:
                    raise OSError(5, "I/O error")
                return real(rows)

            store._write_partitioned_parquet = fail_once
            store.append_batch([_trade(i) for i in range(5)])
            store._flush_future.exception(timeout=10)  # wait for the failure without draining
            store.append_batch([_trade(5)])
            store.flush_parquet(reason="TEST")  # drains: requeues the failed rows, writes all
            self.assertEqual(store.archive_async_flush_failures, 1)
            self.assertEqual(sorted(_archived_keys(cfg)), _expected(6))
            store.close()
            again = Storage(cfg)
            again.open()
            self.assertEqual(again.archive_integrity_state, "VERIFIED_COMPLETE_CLEAN_STOP_CHECKPOINT")
            again.close()

    def test_background_flush_prunes_with_its_own_connection(self):
        with TemporaryDirectory() as tmp:
            cfg, store = self._store(tmp, journal_retention_seconds=3600)
            store.PRUNE_BATCH_ROWS = 2
            store.PRUNE_MIN_INTERVAL_SECONDS = 0
            store.append_batch([_trade(i) for i in range(12)])
            store.flush_parquet(reason="TEST")
            store.flush_parquet(reason="TEST")
            remaining = store.connection.execute("SELECT count(*) FROM market_events").fetchone()[0]
            self.assertLessEqual(remaining, 2)  # less than one batch plus the checkpoint row is left
            self.assertEqual(store.journal_pruned_rows, 12 - remaining)
            self.assertEqual(sorted(_archived_keys(cfg)), _expected(12))
            store.close()


class JournalPipeTimingTests(unittest.TestCase):
    def test_status_reports_where_a_batch_spends_its_time(self):
        from kalshi15m.journal_process import ProcessJournalStorage

        with TemporaryDirectory() as tmp:
            store = ProcessJournalStorage(Settings(home=Path(tmp), min_free_bytes=0))
            store.open()
            try:
                store.append_batch([_trade(i) for i in range(20)])
                store.append_batch([_trade(i) for i in range(20, 40)])
                timing = store.connection_status()["journal_pipe_ms_ema"]
                self.assertTrue({"rpc", "send", "gap", "child_wall", "child_cpu"} <= set(timing))
            finally:
                store.close()


class PipelinedAppendTests(unittest.TestCase):
    """The proxy sends batch N before collecting N-1's ack; failed batches are re-sent in order."""

    def _stub(self, outcomes):
        from concurrent.futures import Future
        from kalshi15m.journal_process import ProcessJournalStorage

        class Stub(ProcessJournalStorage):
            def _request(self, kind, *payload):
                self.sent.append((kind, payload[0] if payload else None))
                future = Future()
                if self.outcomes.pop(0) == "ok":
                    future.set_result(1)
                else:
                    future.set_exception(RuntimeError("child failed"))
                return future

        with TemporaryDirectory() as tmp:
            stub = Stub(Settings(home=Path(tmp), min_free_bytes=0))
        stub.sent, stub.outcomes = [], list(outcomes)
        stub.child_alive = True
        return stub

    def test_send_precedes_the_previous_ack_and_settle_collects_the_last(self):
        stub = self._stub(["ok", "ok"])
        stub.append_batch(["b1"])
        self.assertEqual(stub._inflight[1], ["b1"])  # returned without collecting its own ack
        stub.append_batch(["b2"])
        self.assertEqual([p for _, p in stub.sent], [["b1"], ["b2"]])
        stub._settle()
        self.assertIsNone(stub._inflight)
        self.assertEqual(stub._failed, [])

    def test_a_failed_batch_is_resent_before_any_new_batch_in_order(self):
        stub = self._stub(["fail", "ok", "ok", "ok"])
        stub.append_batch(["b1"])
        stub.append_batch(["b2"])  # collects b1's failure
        self.assertEqual(stub._failed, [["b1"]])
        stub.append_batch(["b3"])  # re-sends b1 first
        self.assertEqual([p for _, p in stub.sent], [["b1"], ["b2"], ["b1"], ["b3"]])
        self.assertEqual(stub._failed, [])

    def test_a_batch_that_keeps_failing_raises_and_stays_queued_and_blocks_new_sends(self):
        stub = self._stub(["fail", "ok", "fail"])
        stub.append_batch(["b1"])
        stub.append_batch(["b2"])
        with self.assertRaises(RuntimeError):
            stub.append_batch(["b3"])  # re-send of b1 fails
        self.assertEqual(stub._failed, [["b1"]])  # kept for the next attempt, nothing dropped
        self.assertEqual([p for _, p in stub.sent], [["b1"], ["b2"], ["b1"]])  # b3 never sent



class NamedStackSamplingTests(unittest.TestCase):
    def test_a_named_request_is_consumed_and_logged_separately(self):
        import faulthandler
        from kalshi15m.worker import start_stack_sampling

        with TemporaryDirectory() as tmp:
            runtime, logs = Path(tmp) / "runtime", Path(tmp) / "logs"
            runtime.mkdir()
            logs.mkdir()
            (runtime / "journal_stack_sample.txt").write_text("0.2 1", encoding="utf-8")
            (runtime / "stack_sample.txt").write_text("0.2 1", encoding="utf-8")  # the worker's own request
            try:
                self.assertTrue(start_stack_sampling(runtime, logs, "journal_stack_sample", "journal_stack_samples.log"))
                self.assertFalse((runtime / "journal_stack_sample.txt").exists())
                self.assertTrue((runtime / "journal_stack_sample.started.txt").exists())
                self.assertTrue((runtime / "stack_sample.txt").exists())  # the other process's request is untouched
                self.assertTrue((logs / "journal_stack_samples.log").exists())
            finally:
                faulthandler.cancel_dump_traceback_later()


class WebSocketCloseDetailTests(unittest.TestCase):
    def test_close_detail_names_code_reason_and_session_length(self):
        from kalshi15m.worker import Collector

        class FakeSocket:
            close_code = 1001
            close_reason = "going away"

        detail = Collector._close_detail(FakeSocket(), time.monotonic() - 12.0)
        self.assertIn("close_code=1001", detail)
        self.assertIn("going away", detail)
        self.assertRegex(detail, r"session_s=1[12]\.\d")
        self.assertIn("close_code=None", Collector._close_detail(object(), time.monotonic()))  # no attribute: still safe


class LoopLagMonitorTests(unittest.TestCase):
    def test_a_blocked_event_loop_shows_up_as_lag_and_old_samples_expire(self):
        import asyncio
        from kalshi15m.worker import LoopLagMonitor

        async def scenario():
            monitor = LoopLagMonitor()
            task = asyncio.create_task(monitor.run())
            await asyncio.sleep(0.2)
            time.sleep(0.4)  # block the loop the way a long synchronous call would
            await asyncio.sleep(0.2)
            task.cancel()
            return monitor

        monitor = asyncio.run(scenario())
        self.assertGreaterEqual(monitor.max_ms(30.0), 300)
        self.assertEqual(monitor.max_ms(30.0, now=time.monotonic() + 1000), 0.0)  # window passed: nothing

    def test_samples_older_than_two_minutes_are_dropped(self):
        from kalshi15m.worker import LoopLagMonitor

        monitor = LoopLagMonitor()
        monitor.record(5.0, now=0.0)
        monitor.record(0.01, now=200.0)
        self.assertEqual(len(monitor.samples), 1)
        self.assertEqual(monitor.max_ms(30.0, now=200.0), 10.0)


class StallWatchdogAndCatalogScanTests(unittest.TestCase):
    def test_watchdog_names_the_call_that_blocks_the_event_loop_once_per_stall(self):
        import asyncio
        import logging
        from kalshi15m.worker import LoopLagMonitor

        def blocking_call_for_test():
            time.sleep(1.4)

        async def scenario(monitor):
            task = asyncio.create_task(monitor.run())
            await asyncio.sleep(0.2)
            monitor.start_watchdog(__import__("threading").get_ident(), threshold_seconds=0.5,
                                   log=logging.getLogger("stall-test"))
            blocking_call_for_test()
            await asyncio.sleep(0.4)
            task.cancel()
            monitor.watchdog_stop.set()

        monitor = LoopLagMonitor()
        with self.assertLogs("stall-test", level="WARNING") as captured:
            asyncio.run(scenario(monitor))
        text = "\n".join(captured.output)
        self.assertIn("blocking_call_for_test", text)
        self.assertEqual(len(captured.records), 1)  # one report per stall, not one per check

    def test_combined_duration_pattern_matches_exactly_what_the_three_patterns_did(self):
        from kalshi15m.discovery import SERIES_DURATION_PATTERN, SERIES_DURATION_PATTERNS, series_catalog_candidate

        for text in ("every 15 minutes", "15-min contract", "settles each 900 seconds", "fifteen minutes",
                     "Fifteen-Minute", "150 minutes", "no duration here", "1500 seconds", "15mins"):
            self.assertEqual(bool(SERIES_DURATION_PATTERN.search(text)),
                             any(p.search(text) for p in SERIES_DURATION_PATTERNS), text)
        self.assertTrue(series_catalog_candidate({"ticker": "X", "title": "BTC price every 15 minutes"}))
        self.assertFalse(series_catalog_candidate({"ticker": "X15M", "title": "daily close"}))  # ticker text ignored

    def test_catalog_changes_with_precomputed_candidates_is_identical_and_scans_once(self):
        from unittest.mock import patch
        import kalshi15m.discovery as discovery

        prior = {"series": [{"series_ticker": "KEEP"}, {"series_ticker": "REMOVE"}]}
        fresh = [{"ticker": "KEEP", "title": "every 15 minutes"}, {"ticker": "ADD", "title": "every 15 minutes"}]
        candidates = discovery.candidate_series_tickers(fresh)
        with patch.object(discovery, "candidate_series_tickers") as scan:
            self.assertEqual(discovery.series_catalog_changes(prior, fresh, candidates), (["ADD"], ["REMOVE"]))
            scan.assert_not_called()  # the caller's scan is reused
        self.assertEqual(discovery.series_catalog_changes(prior, fresh), (["ADD"], ["REMOVE"]))


class EventLoopCpuDietTests(unittest.TestCase):
    def test_cached_best_prices_match_a_brute_force_scan_over_random_books(self):
        import random

        rng = random.Random(7)
        book = OrderBook()
        book.snapshot({"yes_dollars_fp": [["0.40", "5"], ["0.35", "2"]], "no_dollars_fp": [["0.55", "3"]]})
        for step in range(3000):
            side = rng.choice(["yes", "no"])
            levels = book.yes if side == "yes" else book.no
            price = Decimal(rng.randrange(1, 99)) / 100
            have = levels.get(price, Decimal(0))
            delta = -have if (have and rng.random() < 0.45) else Decimal(rng.randrange(1, 5))
            book.delta({"side": side, "price_dollars": str(price), "delta_fp": str(delta)})
            if step % 500 == 0:  # a fresh snapshot resets everything
                book.snapshot({"yes_dollars_fp": [[str(price), "1"]], "no_dollars_fp": []})
            quotes = book.quotes()
            best_yes, best_no = max(book.yes, default=None), max(book.no, default=None)
            self.assertEqual(quotes["yes_bid"], str(best_yes) if best_yes is not None else None)
            self.assertEqual(quotes["no_bid"], str(best_no) if best_no is not None else None)
            self.assertEqual(quotes["yes_ask"], str(Decimal("1.0000") - best_no) if best_no is not None else None)
            self.assertEqual(quotes["no_ask_size"], str(book.yes[best_yes]) if best_yes is not None else None)

    def test_book_recovers_if_the_level_dicts_are_replaced_behind_its_back(self):
        book = OrderBook()
        book.snapshot({"yes_dollars_fp": [["0.40", "5"]], "no_dollars_fp": [["0.55", "3"]]})
        book.yes = {Decimal("0.60"): Decimal("1")}  # someone assigned a new dict
        self.assertEqual(book.quotes()["yes_bid"], "0.60")
        book.no = {}
        self.assertIsNone(book.quotes()["no_bid"])

    def test_background_status_writes_do_not_block_updates_and_the_latest_wins(self):
        import threading

        with TemporaryDirectory() as root:
            status = RuntimeStatus(Settings(home=Path(root)))
            status.async_file_writes = True
            status.status_file_write_interval_seconds = 0.0
            gate = threading.Event()
            original = status._write_payload_locked

            def slow(payload):
                gate.wait(5)
                return original(payload)

            status._write_payload_locked = slow
            began = time.monotonic()
            for value in range(1, 6):
                status.update(queue_depth=value)  # the write thread is stuck; none of these may wait
            self.assertLess(time.monotonic() - began, 1.0)
            gate.set()
            deadline = time.monotonic() + 5
            path = Path(root) / "runtime" / "status.json"
            while time.monotonic() < deadline:
                if path.exists() and json.loads(path.read_text())["queue_depth"] == 5:
                    break
                time.sleep(0.05)
            self.assertEqual(json.loads(path.read_text())["queue_depth"], 5)

    def test_forced_status_writes_stay_synchronous_even_when_background_writes_are_on(self):
        with TemporaryDirectory() as root:
            status = RuntimeStatus(Settings(home=Path(root)))
            status.async_file_writes = True
            status.update(service_state="STOPPED")  # service_state forces a write
            saved = json.loads((Path(root) / "runtime" / "status.json").read_text())
            self.assertEqual(saved["service_state"], "STOPPED")  # on disk before update() returned

    def test_mark_quote_updates_the_three_maps_in_place_like_the_copying_version(self):
        with TemporaryDirectory() as root:
            status = RuntimeStatus(Settings(home=Path(root)))
            status.update(
                orderbook_quote_last_update_utc_by_ticker={"OLD": "t0"},
                orderbook_quote_freshness_by_ticker={"OLD": "STALE"},
                orderbook_quote_age_seconds_by_ticker={"OLD": 9.0},
            )
            ages = status.data["orderbook_quote_age_seconds_by_ticker"]
            status.mark_quote("NEW", "FRESH", "t1")
            snap = status.snapshot()
            self.assertEqual(snap["orderbook_quote_last_update_utc_by_ticker"], {"OLD": "t0", "NEW": "t1"})
            self.assertEqual(snap["orderbook_quote_freshness_by_ticker"], {"OLD": "STALE", "NEW": "FRESH"})
            self.assertEqual(snap["orderbook_quote_age_seconds_by_ticker"], {"OLD": 9.0, "NEW": 0.0})
            self.assertIs(status.data["orderbook_quote_age_seconds_by_ticker"], ages)  # no per-message copy
            self.assertEqual(snap["heartbeat_utc"], "t1")

    def test_sequence_state_publishes_on_transitions_but_not_on_every_delta(self):
        with TemporaryDirectory() as root:
            collector = Collector(Settings(home=Path(root)))
            published = []
            real = collector._publish_orderbook_sequence_state
            collector._publish_orderbook_sequence_state = lambda **kw: (published.append(1), real(**kw))[1]
            collector._record_orderbook_sequence(5, 1, snapshot=True)    # snapshot: publishes at once
            collector._record_orderbook_sequence(5, 2, snapshot=False)   # first delta: state changes
            before = len(published)
            for seq in range(3, 203):
                collector._record_orderbook_sequence(5, seq, snapshot=False)  # steady state
            self.assertLessEqual(len(published) - before, 1)  # at most one time-based refresh in a fast loop
            self.assertEqual(collector.orderbook_sequence_by_sid[5]["delta_count"], 201)  # counters are exact
            published_before_snapshot = len(published)
            collector._record_orderbook_sequence(5, 203, snapshot=True)
            self.assertEqual(len(published), published_before_snapshot + 1)

    def test_websocket_compression_defaults_to_deflate_and_can_be_disabled(self):
        def connect_args(**settings):
            seen = {}

            class FakeConnection:
                async def __aenter__(self):
                    collector.stopping.set()
                    raise ConnectionError("stop after capturing the connect arguments")

                async def __aexit__(self, *exc):
                    return False

            with TemporaryDirectory() as root:
                collector = Collector(Settings(home=Path(root), **settings))

                def fake_connect(_url, **kwargs):
                    seen.update(kwargs)
                    return FakeConnection()

                with (
                    patch("kalshi15m.worker.ws_auth_headers", return_value={"k": "v"}),
                    patch("kalshi15m.worker.websockets.connect", side_effect=fake_connect),
                ):
                    asyncio.run(collector.websocket_loop())
            return seen

        self.assertEqual(connect_args()["compression"], "deflate")  # measured: "none" doubled reconnects
        self.assertIsNone(connect_args(ws_compression="none")["compression"])


class RecentKeysWindowTests(unittest.TestCase):
    def test_window_is_bounded_ordered_and_trims_in_constant_time(self):
        from collections import OrderedDict

        with TemporaryDirectory() as tmp:
            store = Storage(Settings(home=Path(tmp), min_free_bytes=0))
            store.open()
            try:
                store.RECENT_KEYS_MAX = 1000
                self.assertIsInstance(store._recent_keys, OrderedDict)
                for start in range(0, 6000, 500):
                    store.append_batch([_trade(i) for i in range(start, start + 500)])
                self.assertEqual(len(store._recent_keys), 1000)
                kept = list(store._recent_keys)
                self.assertEqual(kept[0], "trade-5000")   # oldest kept = 1000 newest
                self.assertEqual(kept[-1], "trade-5999")
                self.assertEqual(store.append_batch([_trade(5999), _trade(6000)]), 1)  # recent duplicate skipped
            finally:
                store.close()

    def test_directory_size_reconcile_is_hourly_not_every_five_minutes(self):
        with TemporaryDirectory() as tmp:
            store = Storage(Settings(home=Path(tmp), min_free_bytes=0))
            self.assertEqual(store.parquet_size_reconcile_interval_seconds, 3600.0)
