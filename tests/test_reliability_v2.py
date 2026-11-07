"""Isolated regression tests for collector reliability work. No network or live paths."""
from __future__ import annotations

import asyncio
import io
import json
import sqlite3
import urllib.error
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, Mock, patch

from kalshi15m.api import PublicKalshiAPI
from kalshi15m.book import OrderBook
from kalshi15m.config import Settings
from kalshi15m.coverage import OpeningCoverage
from kalshi15m.storage import Storage
from kalshi15m.worker import Collector, classify_rest_reconciliation


def event(key, payload):
    return {
        "dedupe_key": key,
        "channel": "test",
        "kind": "observation",
        "market_ticker": "TEST",
        "received_utc": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }


class OpeningCoverageTests(unittest.TestCase):
    def test_three_minute_rest_samples_are_labeled_noncontinuous(self):
        coverage = OpeningCoverage()
        open_at = datetime(2026, 11, 7, 12, 15, tzinfo=timezone.utc)
        at = lambda seconds: (open_at + timedelta(seconds=seconds)).isoformat()
        coverage.track("K15M", at(0), at(-60))
        coverage.rest_book("K15M", at(12), valid=True, executable=False)
        coverage.rest_book("K15M", at(70), valid=True, executable=True)
        coverage.rest_book("K15M", at(136), valid=True, executable=True)
        coverage.rest_trade("K15M", at(20), at(90))
        summaries = coverage.finalize_due(at(181))
        self.assertEqual(len(summaries), 1)
        item = summaries[0]
        self.assertEqual(item["rest_valid_samples_by_minute"], [1, 1, 1])
        self.assertEqual(item["rest_executable_quote_samples_by_minute"], [0, 1, 1])
        self.assertEqual(item["rest_trade_rows_by_minute"], [1, 0, 0])
        self.assertEqual(item["rest_sample_coverage_state"], "REST_SAMPLED_EACH_MINUTE_NOT_CONTINUOUS")
        self.assertFalse(item["rest_book_history_complete"])
        self.assertEqual(coverage.finalize_due(at(190)), [])

    def test_failed_polls_do_not_count_as_valid_and_book_may_be_quiet(self):
        coverage = OpeningCoverage()
        open_at = datetime(2026, 11, 7, 12, 15, tzinfo=timezone.utc)
        at = lambda seconds: (open_at + timedelta(seconds=seconds)).isoformat()
        coverage.track("ONE", at(0), at(0))
        coverage.rest_book("ONE", at(14), valid=False, executable=False)
        coverage.rest_book("ONE", at(35), valid=True, executable=False)
        coverage.rest_book("ONE", at(91), valid=False, executable=False)
        item = coverage.finalize_due(at(180))[0]
        self.assertEqual(item["rest_missing_or_invalid_polls_by_minute"], [1, 1, 0])
        self.assertEqual(item["rest_valid_samples_by_minute"], [1, 0, 0])
        self.assertEqual(item["rest_sample_coverage_state"], "REST_PARTIAL_OR_INVALID")


class StorageReliabilityTests(unittest.TestCase):
    def test_same_key_different_payload_retains_version_and_manifest(self):
        with TemporaryDirectory() as directory:
            store = Storage(Settings(home=Path(directory), min_free_bytes=0))
            store.open()
            inserted = store.append_batch([
                event("one-key", {"price": 1}),
                event("one-key", {"price": 2}),
                event("one-key", {"price": 2}),
            ])
            self.assertEqual(inserted, 2)
            self.assertEqual(store.events_key_conflict, 1)
            self.assertEqual(store.events_duplicate, 1)
            self.assertEqual(store.connection.execute("SELECT count(*) FROM market_events").fetchone()[0], 1)
            self.assertEqual(store.connection.execute("SELECT count(*) FROM market_event_versions").fetchone()[0], 1)
            store.close()
            self.assertTrue(list((Path(directory) / "data" / "market_data").rglob("*.parquet")))

    def test_hot_writer_flush_and_watermark_audit_avoid_full_archive_scan(self):
        # The archive can contain thousands of prior parts on a slow HDD.
        # Each new writer flush must account incrementally, never recursively
        # enumerate historical parts. Restart still scans the archive once.
        with TemporaryDirectory() as directory:
            cfg = Settings(home=Path(directory), min_free_bytes=0)
            store = Storage(cfg)
            store.open()
            store.parquet_flush_rows = 1
            store.persistence_audit_last_monotonic = 0
            with patch.object(
                store, "_parquet_directory_size",
                side_effect=AssertionError("full archive walk during hot ingest"),
            ):
                self.assertEqual(store.append_batch([event("fast-1", {"price": "0.40"})]), 1)
                status = store.connection_status()
                self.assertTrue(status["storage_market_event_watermark_matches_disk"])
                self.assertEqual(store.append_batch([event("fast-2", {"price": "0.41"})]), 1)
            exact_bytes = sum(p.stat().st_size for p in cfg.parquet_dir.rglob("*.parquet"))
            self.assertGreater(exact_bytes, 0)
            self.assertEqual(store.parquet_bytes, exact_bytes)
            self.assertEqual(store.parquet_flush_count, 2)
            store.close()
            reopened = Storage(cfg)
            reopened.open()
            self.assertEqual(reopened.parquet_bytes, exact_bytes)
            reopened.close()

    def test_invalid_payload_quarantined_but_valid_event_preserved(self):
        with TemporaryDirectory() as directory:
            store = Storage(Settings(home=Path(directory), min_free_bytes=0))
            store.open()
            inserted = store.append_batch_resilient([
                event("bad-event", {"nonserializable": object()}),
                event("good-event", {"price": "0.45"}),
            ])
            self.assertEqual(inserted, 1)
            self.assertEqual(store.events_quarantined, 1)
            self.assertEqual(store.connection.execute(
                "SELECT count(*) FROM quarantined_market_events"
            ).fetchone()[0], 1)
            self.assertEqual(store.connection.execute(
                "SELECT count(*) FROM market_events"
            ).fetchone()[0], 1)
            store.close()

    def test_unverified_stopped_status_does_not_skip_recovery(self):
        with TemporaryDirectory() as directory:
            cfg = Settings(home=Path(directory), min_free_bytes=0)
            store = Storage(cfg)
            store.open()
            store.raw_capture_enabled = False
            store.append_batch([event("unarchived", {"id": 3})])
            store.close()
            (cfg.runtime_dir / "status.json").write_text(
                json.dumps({"service_state": "STOPPED", "raw_capture_state": "ENABLED"}),
                encoding="utf-8",
            )
            reopened = Storage(cfg)
            reopened.open()
            self.assertEqual(reopened.parquet_recovery_rows, 1)
            self.assertEqual(reopened.parquet_recovery_state, "RECOVERED_MISSING_ROWS")
            reopened.close()

    def test_emergency_spool_replays_idempotently_after_storage_recovers(self):
        with TemporaryDirectory() as directory:
            cfg = Settings(home=Path(directory), min_free_bytes=0)
            store = Storage(cfg)
            store.open()
            event_payload = event("spooled-event", {"id": 7})
            original = store.spool_batch([event_payload])
            self.assertTrue(original.exists())
            self.assertEqual(store.replay_spooled_batches(), 1)
            self.assertFalse(original.exists())
            self.assertEqual(store.replay_spooled_batches(), 0)
            self.assertEqual(store.connection.execute(
                "SELECT COUNT(*) FROM market_events WHERE dedupe_key = 'spooled-event'"
            ).fetchone()[0], 1)
            store.close()

    def test_corrupt_parquet_is_reported_without_deleting_source(self):
        with TemporaryDirectory() as directory:
            cfg = Settings(home=Path(directory), min_free_bytes=0)
            corrupt = cfg.parquet_dir / "date=2026-11-07" / "kind=observation" / "bad.parquet"
            corrupt.parent.mkdir(parents=True, exist_ok=True)
            corrupt.write_bytes(b"not valid parquet")
            store = Storage(cfg)
            store.open()
            self.assertTrue(store.corrupt_parquet_parts)
            self.assertTrue(corrupt.exists())
            store.close()


class RuntimeHealthReliabilityTests(unittest.TestCase):
    def test_active_market_without_book_is_degraded_even_if_worker_runs(self):
        from kalshi15m.worker import RuntimeStatus
        with TemporaryDirectory() as directory:
            status = RuntimeStatus(Settings(home=Path(directory)))
            status.update(service_state="RUNNING", market_tickers=["ONE"])
            status.mark_rest_poll(
                "rest_orderbook",
                started_utc=datetime.now(timezone.utc).isoformat(),
                request_count=1, successful_request_count=1, requested_market_count=1,
                markets_with_data=1, rows_received=1, error_count=0,
            )
            status.refresh_ages()
            self.assertEqual(status.snapshot()["service_state"], "RUNNING")
            self.assertEqual(status.snapshot()["rest_book_health_by_ticker"]["ONE"], "NEVER_OBSERVED")
            self.assertEqual(
                status.snapshot()["collection_quality_state"],
                "DEGRADED_PARTIAL_REST_COVERAGE",
            )

    def test_quiet_recent_valid_book_is_sampled_healthy(self):
        from kalshi15m.worker import RuntimeStatus
        with TemporaryDirectory() as directory:
            status = RuntimeStatus(Settings(home=Path(directory)))
            now = datetime.now(timezone.utc).isoformat()
            status.update(
                service_state="RUNNING", market_tickers=["ONE"],
                rest_book_last_observed_utc_by_ticker={"ONE": now},
            )
            status.mark_rest_poll(
                "rest_orderbook",
                started_utc=now,
                request_count=1, successful_request_count=1,
                requested_market_count=1, markets_with_data=1,
                rows_received=1, error_count=0,
            )
            status.refresh_ages()
            self.assertEqual(
                status.snapshot()["collection_quality_state"],
                "REST_SAMPLED_RECENT_NOT_CONTINUOUS",
            )


class BookReliabilityTests(unittest.TestCase):
    def test_empty_snapshot_is_valid_but_missing_sides_are_invalid(self):
        book = OrderBook()
        book.snapshot({"orderbook_fp": {"yes_dollars_fp": [], "no_dollars_fp": []}})
        self.assertTrue(book.synchronized)
        self.assertIsNone(book.quotes()["yes_bid"])
        with self.assertRaises(ValueError):
            book.snapshot({"orderbook_fp": {}})
        self.assertFalse(book.synchronized)

    def test_invalid_price_and_zero_quantity(self):
        book = OrderBook()
        with self.assertRaises(ValueError):
            book.snapshot({"yes_dollars_fp": [["55", "3"]]})
        book.snapshot({"yes_dollars_fp": [["0.55", "3"]]})
        book.delta({"side": "yes", "price_dollars": "0.55", "delta_fp": "-3"})
        self.assertIsNone(book.quotes()["yes_bid"])
        with self.assertRaises(ValueError):
            book.delta({"side": "yes", "price_dollars": "0.55", "delta_fp": "-1"})


    def test_legacy_cent_denominated_snapshot_and_delta_are_normalized(self):
        book = OrderBook()
        book.snapshot({"yes": [[55, 2]], "no": [[35, 3]]})
        self.assertEqual(book.quotes()["yes_bid"], "0.55")
        self.assertEqual(Decimal(book.quotes()["yes_ask"]), Decimal("0.65"))
        book.delta({"side": "yes", "price": 55, "delta": -2})
        self.assertIsNone(book.quotes()["yes_bid"])


class CaptureReliabilityTests(unittest.TestCase):
    def test_missing_rest_book_is_degraded_and_observed_milestone_is_durable(self):
        with TemporaryDirectory() as directory:
            collector = Collector(Settings(home=Path(directory)))
            collector.open_tickers = {"ONE", "TWO"}
            collector.markets = {
                "ONE": {"open_time": datetime.now(timezone.utc).isoformat()},
                "TWO": {"open_time": datetime.now(timezone.utc).isoformat()},
            }
            collector.api.get = AsyncMock(return_value={
                "orderbooks": [{
                    "ticker": "ONE",
                    "orderbook_fp": {"yes_dollars_fp": [["0.44", "1"]], "no_dollars_fp": []},
                }]
            })
            asyncio.run(collector.collect_rest_depth())
            snapshot = collector.status.snapshot()
            self.assertEqual(snapshot["rest_poll_health"]["rest_orderbook"], "DEGRADED")
            self.assertEqual(snapshot["last_rest_poll_by_channel"]["rest_orderbook"]["missing_market_count"], 1)
            messages = list(collector.queue._queue)
            self.assertTrue(any(m["kind"] == "first_book_observed" for m in messages))
            self.assertTrue(any(m["kind"] == "first_executable_quote_observed" for m in messages))
            self.assertTrue(any(m["kind"] == "rest_poll_result" and m["payload"]["coverage"] == "INCOMPLETE" for m in messages))
            collector.writer_pool.shutdown(wait=False)

    def test_two_trade_pages_and_watermark_are_complete(self):
        with TemporaryDirectory() as directory:
            collector = Collector(Settings(home=Path(directory)))
            collector.active_tickers = {"ONE"}
            now = datetime.now(timezone.utc)
            collector.markets = {"ONE": {"open_time": (now - timedelta(minutes=5)).isoformat()}}
            collector.api.get = AsyncMock(side_effect=[
                {"trades": [{"trade_id": "T1", "created_time": now.isoformat()}], "cursor": "page2"},
                {"trades": [{"trade_id": "T2", "created_time": now.isoformat()}], "cursor": ""},
            ])
            asyncio.run(collector.collect_recent_trades())
            messages = list(collector.queue._queue)
            self.assertEqual(len([m for m in messages if m["kind"] == "public_trade"]), 2)
            self.assertTrue(any(m["kind"] == "first_trade_observed" for m in messages))
            self.assertTrue(any(m["kind"] == "rest_poll_result" and m["payload"]["complete"] for m in messages))
            self.assertIn("ONE", collector.trade_poll_since_by_ticker)
            self.assertEqual(collector.api.get.await_count, 2)
            collector.writer_pool.shutdown(wait=False)

    def test_websocket_sid_sequence_reuse_has_distinct_session_keys(self):
        class FakeSocket:
            def __init__(self, message):
                self.message = message
                self.sent = False
            def __aiter__(self):
                return self
            async def __anext__(self):
                if self.sent:
                    raise StopAsyncIteration
                self.sent = True
                return json.dumps(self.message)
        with TemporaryDirectory() as directory:
            collector = Collector(Settings(home=Path(directory)))
            msg = {"type": "trade", "sid": 1, "seq": 2, "msg": {"market_ticker": "ONE", "trade_id": "T1"}}
            collector.websocket_session_id = "old-session"
            asyncio.run(collector._receive_ws(FakeSocket(msg)))
            collector.websocket_session_id = "new-session"
            asyncio.run(collector._receive_ws(FakeSocket(msg)))
            keys = [m["dedupe_key"] for m in list(collector.queue._queue)]
            self.assertEqual(len(keys), 2)
            self.assertNotEqual(keys[0], keys[1])
            collector.writer_pool.shutdown(wait=False)


    def test_websocket_delta_counters_distinguish_filtered_from_enqueued(self):
        class FakeSocket:
            def __init__(self, frame):
                self.frame = frame
                self.delivered = False

            def __aiter__(self):
                return self

            async def __anext__(self):
                if self.delivered:
                    raise StopAsyncIteration
                self.delivered = True
                return json.dumps(self.frame)

        with TemporaryDirectory() as directory:
            collector = Collector(Settings(home=Path(directory)))
            collector.websocket_session_id = "staging-test-session"
            collector.subscription_channel_by_sid[9] = "orderbook_delta"
            collector.last_seq[9] = 2
            frame = {
                "type": "orderbook_delta", "sid": 9, "seq": 3,
                "msg": {
                    "market_ticker": "ONE", "side": "yes",
                    "price_dollars": "0.50", "delta_fp": "1",
                },
            }
            # Valid sequence, but market is not currently in our capture scope.
            asyncio.run(collector._receive_ws(FakeSocket(frame)))
            status = collector.status.snapshot()
            self.assertEqual(status["websocket_delta_received_count"], 1)
            self.assertEqual(status["websocket_delta_filtered_out_of_scope_count"], 1)
            self.assertEqual(status["websocket_delta_enqueued_count"], 0)
            self.assertEqual(
                status["last_by_channel"]["websocket_orderbook_delta"]["exchange_ts"],
                None,
            )
            self.assertEqual(len(collector.queue._queue), 0)
            self.assertEqual(status["orderbook_sequence_delta_count"], 1)

            collector.open_tickers = {"ONE"}
            book = OrderBook()
            book.snapshot({"yes_dollars_fp": [["0.50", "2"]], "no_dollars_fp": []})
            collector.books["ONE"] = book
            frame["seq"] = 4
            asyncio.run(collector._receive_ws(FakeSocket(frame)))
            status = collector.status.snapshot()
            self.assertEqual(status["websocket_delta_received_count"], 2)
            self.assertEqual(status["websocket_delta_filtered_out_of_scope_count"], 1)
            self.assertEqual(status["websocket_delta_enqueued_count"], 1)
            self.assertIsNotNone(status["websocket_delta_last_enqueued_utc"])
            self.assertEqual(status["orderbook_sequence_delta_count"], 2)
            self.assertEqual(
                len([e for e in collector.queue._queue if e["kind"] == "orderbook_delta"]),
                1,
            )
            # Enqueued does not imply journaled or finalized in Parquet.
            collector.writer_pool.shutdown(wait=False)


class RestHealthClassificationTests(unittest.TestCase):
    def test_transient_prior_api_error_does_not_permanently_degrade_rest(self):
        # The API lifetime error total may remain >0 after recovery.
        # Only errors during the current reconcile should determine freshness.
        self.assertEqual(
            classify_rest_reconciliation(
                interval_errors=0,
                coverage_state="COMPLETE_CURRENT_AND_NEAR_TERM",
            ),
            ("READY", "RECENT_RECONCILIATION_SUCCEEDED"),
        )
        self.assertEqual(
            classify_rest_reconciliation(
                interval_errors=1,
                coverage_state="COMPLETE_CURRENT_AND_NEAR_TERM",
            ),
            ("DEGRADED", "CURRENT_RECONCILIATION_API_ERRORS"),
        )

    def test_cached_partial_discovery_does_not_report_rest_ready(self):
        self.assertEqual(
            classify_rest_reconciliation(
                interval_errors=0, coverage_state="PARTIAL",
            ),
            ("DEGRADED", "DISCOVERY_COVERAGE_NOT_CONFIRMED_COMPLETE"),
        )
        self.assertEqual(
            classify_rest_reconciliation(
                interval_errors=0, coverage_state="UNKNOWN",
            ),
            ("DEGRADED", "DISCOVERY_COVERAGE_NOT_CONFIRMED_COMPLETE"),
        )


class ApiReliabilityTests(unittest.TestCase):
    def test_public_http_date_metadata_is_diagnostic_only(self):
        class Response:
            status = 200
            headers = {"Date": "Sat, 07 Nov 2026 12:00:00 GMT"}
            def __enter__(self):
                return self
            def __exit__(self, *_args):
                return False
            def read(self, _limit):
                return b'{"markets":[]}'
        with patch("kalshi15m.api.urllib.request.urlopen", return_value=Response()):
            api = PublicKalshiAPI(min_interval=0)
            result = api._get_sync("/markets", None)
        self.assertEqual(result["markets"], [])
        self.assertEqual(result.response_metadata["server_date_header"], Response.headers["Date"])
        self.assertEqual(result.response_metadata["server_date_offset_quality"], "COARSE_HTTP_DATE_DIAGNOSTIC")

    def test_http_503_get_is_retried_but_not_trading(self):
        api = PublicKalshiAPI(min_interval=0)
        transient = urllib.error.HTTPError(
            "https://example.invalid", 503, "service unavailable", {}, io.BytesIO(b"503")
        )
        api._get_sync = Mock(side_effect=[transient, {"ok": True}])
        with patch("kalshi15m.api.asyncio.sleep", new=AsyncMock()):
            result = asyncio.run(api.get("/markets"))
        self.assertEqual(result, {"ok": True})
        self.assertEqual(api.attempts, 2)
        self.assertEqual(api.errors, 0)


if __name__ == "__main__":
    unittest.main()
