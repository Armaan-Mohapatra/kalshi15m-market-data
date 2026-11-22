"""Invalid or missing source records must never be promoted into a synchronized book."""
import unittest
from decimal import Decimal

from kalshi15m.book import OrderBook

GOOD = {"yes": [["0.40", "5"]], "no": [["0.55", "5"]]}


class BookIntegrityTests(unittest.TestCase):
    def synced(self):
        b = OrderBook()
        b.snapshot(GOOD)
        return b

    def test_delta_cannot_revive_an_invalidated_book(self):
        b = self.synced()
        b.invalidate()
        with self.assertRaises(ValueError):
            b.delta({"side": "yes", "price": "0.41", "delta": "1"})
        self.assertFalse(b.synchronized)

    def test_delta_before_any_snapshot_is_refused(self):
        b = OrderBook()
        with self.assertRaises(ValueError):
            b.delta({"side": "yes", "price": "0.41", "delta": "1"})
        self.assertFalse(b.synchronized)

    def test_malformed_snapshot_levels_are_rejected_not_dropped(self):
        for bad in ([["0.40", "5"], ["garbage", "x"]], [["0.40", "5"], ["0.45"]], [["0.40", "5"], "oops"],
                    [["NaN", "5"]], [["0.40", "Infinity"]]):
            with self.subTest(bad=bad):
                b = OrderBook()
                with self.assertRaises(ValueError):
                    b.snapshot({"yes": bad, "no": [["0.55", "5"]]})
                self.assertFalse(b.synchronized)

    def test_failed_snapshot_unsyncs_a_previously_good_book(self):
        b = self.synced()
        with self.assertRaises(ValueError):
            b.snapshot({"yes": [["bad", "1"]], "no": []})
        self.assertFalse(b.synchronized)

    def test_empty_and_zero_size_levels_remain_legal(self):
        b = OrderBook()
        b.snapshot({"yes": [["0.40", "0"]], "no": []})
        self.assertTrue(b.synchronized)
        self.assertEqual(b.yes, {})

    def test_fresh_snapshot_restores_deltas(self):
        b = self.synced()
        b.invalidate()
        b.snapshot(GOOD)
        b.delta({"side": "yes", "price": "0.41", "delta": "2"})
        self.assertEqual(b.quotes()["yes_bid"], str(Decimal("0.41")))


if __name__ == "__main__":
    unittest.main()
