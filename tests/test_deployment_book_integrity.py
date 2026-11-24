"""Regression coverage for fail-closed native order-book parsing."""
import asyncio
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from kalshi15m.book import OrderBook
from kalshi15m.config import Settings
from kalshi15m.worker import Collector


class DeploymentBookIntegrityTests(unittest.TestCase):
    def book(self):
        b = OrderBook()
        b.snapshot({"yes_dollars_fp": [["0.40", "3"]], "no_dollars_fp": [["0.55", "4"]]})
        return b

    def test_malformed_snapshot_never_replaces_either_side(self):
        for bad in (
            {"yes": [["1.01", "1"]]},
            {"yes": [["0.5", "-1"]]},
            {"yes": [["0.5", "1"], ["0.50", "2"]]},
            {"yes": [["0.5", "1"]], "no": [["NaN", "1"]]},
            {"yes_dollars_fp": "", "yes": [["0.5", "1"]]},
            {"orderbook_fp": []},
            {"yes": [["0.5", "0"], ["0.5", "1"]]},
            {"yes_dollars_fp": None},
        ):
            with self.subTest(bad=bad):
                b = self.book()
                old = b.quotes()
                with self.assertRaises(ValueError):
                    b.snapshot(bad)
                self.assertFalse(b.synchronized)
                self.assertEqual(b.quotes(), old)

    def test_bad_delta_invalidates_until_resnapshot(self):
        for price, delta in (("NaN", "1"), ("Infinity", "1"), ("0.4", "NaN"),
                             ("0.4", "Infinity"), ("1.01", "1"), ("0", "1"),
                             ("0.4", "-4")):
            with self.subTest(price=price, delta=delta):
                b = self.book()
                old = b.quotes()
                with self.assertRaises(ValueError):
                    b.delta({"side": "yes", "price": price, "delta": delta})
                self.assertFalse(b.synchronized)
                self.assertEqual(b.quotes(), old)
                with self.assertRaises(ValueError):
                    b.delta({"side": "yes", "price": "0.4", "delta": "1"})
                b.snapshot({"yes": [["0.4", "3"]]})
                b.delta({"side": "yes", "price": "0.4", "delta": "1"})
                self.assertTrue(b.synchronized)
                self.assertEqual(b.quotes()["yes_bid_size"], "4")


class WorkerPathBookIntegrityTests(unittest.TestCase):
    """Malformed native payloads on the real receive loop: close, invalidate, record why."""

    class FakeWebSocket:
        def __init__(self, messages):
            self.messages, self.closed = messages, False

        def __aiter__(self):
            async def iterate():
                for message in self.messages:
                    yield json.dumps(message)
            return iterate()

        async def close(self, code=None, reason=None):
            self.closed = True

    def run_messages(self, messages):
        root = TemporaryDirectory()
        self.addCleanup(root.cleanup)
        collector = Collector(Settings(home=Path(root.name)))
        collector.open_tickers.add("SAMPLE")
        collector.subscription_channel_by_sid[8] = "orderbook_delta"
        collector.subscription_membership[8] = {"SAMPLE"}
        ws = self.FakeWebSocket(messages)
        asyncio.run(collector._receive_ws(ws))
        return collector, ws

    snapshot = {"type": "orderbook_snapshot", "sid": 8, "seq": 10, "msg": {
        "market_ticker": "SAMPLE", "yes_dollars_fp": [["0.40", "2.00"]], "no_dollars_fp": [["0.50", "3.00"]]}}

    def assert_invalidated(self, collector, ws, fragment):
        status = collector.status.snapshot()
        self.assertTrue(ws.closed)
        self.assertFalse(collector.books["SAMPLE"].synchronized)
        self.assertEqual(status["websocket_state"], "RESYNC_REQUIRED")
        self.assertEqual(status["orderbook_sequence_state"], "INVALIDATED")
        self.assertIn(fragment, status["last_error"])

    def test_malformed_delta_closes_socket_and_invalidates(self):
        bad = {"type": "orderbook_delta", "sid": 8, "seq": 11, "msg": {
            "market_ticker": "SAMPLE", "side": "yes", "price_dollars": "1.0100", "delta_fp": "1.00"}}
        collector, ws = self.run_messages([self.snapshot, bad])
        self.assert_invalidated(collector, ws, "invalid orderbook delta")

    def test_malformed_snapshot_closes_socket_and_invalidates(self):
        bad = {**self.snapshot, "msg": {**self.snapshot["msg"], "no_dollars_fp": [["0.50", "-1"]]}}
        collector, ws = self.run_messages([bad])
        self.assert_invalidated(collector, ws, "invalid orderbook snapshot")


if __name__ == "__main__":
    unittest.main()
