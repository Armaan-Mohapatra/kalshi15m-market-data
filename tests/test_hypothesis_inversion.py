import json
import sqlite3
import unittest
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

from scripts import hypothesis_inversion as hi

D = Decimal


def _fill(decision, market, side, qty, yes_bids, no_bids, utc):
    return {
        "decision_id": decision, "market_ticker": market, "side": side, "filled_quantity": str(qty),
        "decision_utc": utc, "fee_type": "quadratic", "fee_multiplier": "1",
        "arrival_orderbook_snapshot": {"orderbook_fp": {"yes_dollars": yes_bids, "no_dollars": no_bids}},
    }


def _settlement(decision, market, side, result, net):
    return {"entry_decision_id": decision, "market_ticker": market, "side": side, "settlement_result": result,
            "net_pnl_dollars": str(net), "settlement_fee_dollars": "0"}


def _ledger(directory, rows):
    path = Path(directory) / "l.sqlite3"
    db = sqlite3.connect(path)
    db.execute("create table paper_ledger (idempotency_key text primary key, created_utc text, "
               "profile_id text, record_type text, payload_json text)")
    for i, (kind, payload) in enumerate(rows):
        db.execute("insert into paper_ledger values (?,?,?,?,?)",
                   (f"k{i}", f"2026-11-20T00:00:{i:02d}Z", "P", kind, json.dumps(payload)))
    db.commit()
    db.close()
    return path


class InversionPairTests(unittest.TestCase):
    def test_inverse_is_priced_from_the_same_arrival_book_and_settled_on_the_official_result(self):
        # BUY YES consumes NO bids: ask 1-0.30 = 0.70.  The inverse BUY NO consumes YES bids: ask 1-0.60 = 0.40.
        book = dict(yes_bids=[["0.6000", "10"]], no_bids=[["0.3000", "10"]])
        with TemporaryDirectory() as tmp:
            path = _ledger(tmp, [
                ("PAPER_FILL", _fill("d1", "M1", "yes", 2, utc="2026-11-20T01:00:00+00:00", **book)),
                ("PAPER_SETTLEMENT_FINAL", _settlement("d1", "M1", "yes", "yes", "0.5000")),
            ])
            (pair,) = hi.load_pairs(path)
        self.assertEqual(pair["actual_net"], D("0.5000"))  # taken from the real settlement record
        # inverse buys 2 NO at 0.40 (cost 0.80) plus a small fee, and loses it all because YES won
        self.assertLess(pair["inverse_net"], D("-0.80"))
        self.assertGreater(pair["inverse_net"], D("-0.90"))

    def test_the_inverse_wins_when_the_opposite_outcome_settles(self):
        book = dict(yes_bids=[["0.6000", "10"]], no_bids=[["0.3000", "10"]])
        with TemporaryDirectory() as tmp:
            path = _ledger(tmp, [
                ("PAPER_FILL", _fill("d1", "M1", "yes", 2, utc="2026-11-20T01:00:00+00:00", **book)),
                ("PAPER_SETTLEMENT_FINAL", _settlement("d1", "M1", "yes", "no", "-1.4500")),
            ])
            (pair,) = hi.load_pairs(path)
        self.assertGreater(pair["inverse_net"], D("1.0"))  # pays 2.00 for ~0.80 + fee

    def test_unfillable_inverse_and_unsettled_fills_are_excluded_not_invented(self):
        thin = dict(yes_bids=[["0.6000", "1"]], no_bids=[["0.3000", "10"]])  # only 1 contract of inverse depth
        with TemporaryDirectory() as tmp:
            path = _ledger(tmp, [
                ("PAPER_FILL", _fill("d1", "M1", "yes", 2, utc="2026-11-20T01:00:00+00:00", **thin)),
                ("PAPER_SETTLEMENT_FINAL", _settlement("d1", "M1", "yes", "yes", "0.5")),
                ("PAPER_FILL", _fill("d2", "M2", "yes", 2, utc="2026-11-20T01:01:00+00:00", **thin)),  # never settled
            ])
            pairs = hi.load_pairs(path)
        self.assertEqual(len(pairs), 1)  # the unsettled fill has no outcome and is not paired
        self.assertIsNone(pairs[0]["inverse_net"])  # partial inverse fill: excluded from the comparison
        self.assertEqual(hi.summarise(pairs)["excluded_inverse_unfillable"], 1)


class ProtocolTests(unittest.TestCase):
    def test_the_oos_boundary_is_sealed_once_and_never_moves(self):
        with TemporaryDirectory() as tmp:
            home = Path(tmp)
            first = hi.seal(home, "2026-11-20T08:00:00+00:00")
            self.assertEqual(hi.seal(home, "2027-01-01T00:00:00+00:00"), first)  # a later run cannot re-seal

    def test_partitions_are_chronological_with_oos_after_the_seal_only(self):
        pairs = [{"decision_utc": f"2026-11-20T0{i}:00:00+00:00"} for i in range(9)]
        parts = hi.partition(pairs, "2026-11-20T06:00:00+00:00")
        self.assertEqual([len(parts[k]) for k in ("discovery", "validation", "oos")], [4, 2, 3])
        self.assertLess(parts["discovery"][-1]["decision_utc"], parts["validation"][0]["decision_utc"])
        self.assertTrue(all(p["decision_utc"] >= "2026-11-20T06:00:00+00:00" for p in parts["oos"]))

    def test_verdict_needs_both_pre_oos_partitions_then_confirmation_and_never_promotes(self):
        above = {"paired": 40, "markets": 40, "diff_ci95_cluster_bootstrap": (1.0, 9.0)}
        spans = {"paired": 40, "markets": 40, "diff_ci95_cluster_bootstrap": (-3.0, 9.0)}
        empty = {"paired": 0, "markets": 0}
        few = {"paired": 2, "markets": 2}
        self.assertEqual(hi.verdict({"discovery": spans, "validation": spans, "oos": empty}), "H1_NOT_SUPPORTED")
        self.assertEqual(hi.verdict({"discovery": above, "validation": spans, "oos": empty}), "H1_NOT_SUPPORTED")
        self.assertEqual(hi.verdict({"discovery": few, "validation": few, "oos": empty}), "H1_INSUFFICIENT_DATA")
        needs_oos = hi.verdict({"discovery": above, "validation": above, "oos": empty})
        self.assertEqual(needs_oos, "H1_SUPPORTED_NEEDS_OOS_KEEP_PAPER_ONLY")
        confirmed = hi.verdict({"discovery": above, "validation": above, "oos": above})
        self.assertEqual(confirmed, "H1_CONFIRMED_OOS_STILL_PAPER_ONLY")
        self.assertTrue(needs_oos.endswith("PAPER_ONLY") and confirmed.endswith("PAPER_ONLY"))
        small_oos = dict(above, markets=5)  # an OOS interval from 5 markets does not confirm anything
        self.assertEqual(hi.verdict({"discovery": above, "validation": above, "oos": small_oos}),
                         "H1_SUPPORTED_NEEDS_OOS_KEEP_PAPER_ONLY")


if __name__ == "__main__":
    unittest.main()
