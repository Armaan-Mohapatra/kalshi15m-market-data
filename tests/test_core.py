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

    def test_rest_orderbook_partial_success_is_degraded(self):
        with TemporaryDirectory() as root:
            status = RuntimeStatus(Settings(home=Path(root)))
            status.mark_rest_poll(
                "rest_orderbook",
                started_utc=datetime.now(timezone.utc).isoformat(),
                request_count=1,
                successful_request_count=1,
                requested_market_count=3,
                markets_with_data=2,
                rows_received=2,
                error_count=0,
            )
            snapshot = status.snapshot()
            self.assertEqual(snapshot["rest_poll_health"]["rest_orderbook"], "DEGRADED")
            self.assertEqual(
                snapshot["last_rest_poll_by_channel"]["rest_orderbook"]["missing_market_count"],
                1,
            )

    def test_degraded_rest_book_poll_becomes_stale(self):
        with TemporaryDirectory() as root:
            status = RuntimeStatus(Settings(home=Path(root), book_poll_interval_seconds=15))
            status.mark_rest_poll(
                "rest_orderbook",
                started_utc=datetime.now(timezone.utc).isoformat(),
                request_count=1,
                successful_request_count=1,
                requested_market_count=3,
                markets_with_data=2,
                rows_received=2,
                error_count=0,
            )
            status.data["last_rest_poll_by_channel"]["rest_orderbook"][
                "completed_utc"
            ] = (datetime.now(timezone.utc) - timedelta(seconds=31)).isoformat()
            status.refresh_ages()
            self.assertEqual(
                status.snapshot()["rest_poll_health"]["rest_orderbook"], "STALE"
            )

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
        source_root = Path(__file__).resolve().parents[1]
        if not (source_root / "strategy" / "reflex_vnext_frozen_config.json").is_file():
            self.skipTest("requires private frozen strategy authority omitted from sanitized GitHub snapshot")
        self.config, self.config_hash = load_frozen_strategy(source_root)
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
        self.assertEqual(len(api.market_series_calls), 8)
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
            self.assertEqual(store.parquet_bytes, actual_bytes)
            status = store.connection_status()
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
                    self.assertEqual(fsync.call_count, 1)
            parquet_files = list(cfg.parquet_dir.rglob("*.parquet"))
            self.assertEqual(len(parquet_files), 1)
            self.assertEqual(list(cfg.parquet_dir.rglob("*.partial")), [])
            import pyarrow.parquet as pq

            self.assertEqual(pq.ParquetFile(parquet_files[0]).read().num_rows, 1)
            self.assertEqual(pq.ParquetFile(parquet_files[0]).metadata.row_group(0).column(0).compression, "ZSTD")

    def test_existing_deduplicated_events_are_hash_checked(self):
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

            with patch.object(store, "_normalized", wraps=store._normalized) as normalized:
                self.assertEqual(store.append_batch([event]), 0)
            self.assertGreater(normalized.call_count, 0)

            self.assertEqual(store.events_duplicate, 1)
            self.assertEqual(len(store.parquet_buffer), 1)
            store.close()

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

    def test_status_only_does_not_skip_archive_recovery(self):
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

            self.assertEqual(store.parquet_recovery_state, "VERIFIED_NO_GAP")
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
                    """CREATE TABLE IF NOT EXISTS market_event_versions (
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
                while (
                    (store.parquet_buffer or collector.status.data.get("parquet_last_flush_reason") != "IDLE_BUFFER_AGE")
                    and asyncio.get_running_loop().time() < deadline
                ):
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


