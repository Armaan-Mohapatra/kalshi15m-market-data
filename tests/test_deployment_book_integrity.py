"""Regression coverage for fail-closed native order-book parsing."""
import unittest
from kalshi15m.book import OrderBook


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


if __name__ == "__main__":
    unittest.main()
