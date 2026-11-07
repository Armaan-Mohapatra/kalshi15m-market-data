from __future__ import annotations

from collections import defaultdict
from decimal import Decimal, InvalidOperation
from typing import Any


class OrderBook:
    """Local YES/NO bid book; asks are derived from the complementary bids."""

    def __init__(self) -> None:
        self.yes: dict[Decimal, Decimal] = {}
        self.no: dict[Decimal, Decimal] = {}
        self.synchronized = False
        self.sequence: int | None = None

    @staticmethod
    def _levels(values: list[Any], *, legacy_cents: bool = False) -> dict[Decimal, Decimal]:
        if not isinstance(values, list):
            raise ValueError("orderbook levels must be an array")
        result: dict[Decimal, Decimal] = {}
        for item in values:
            if not isinstance(item, (tuple, list)) or len(item) < 2:
                raise ValueError("malformed orderbook price level")
            try:
                price, size = Decimal(str(item[0])), Decimal(str(item[1]))
            except (InvalidOperation, TypeError) as exc:
                raise ValueError("non-numeric orderbook price or size") from exc
            if legacy_cents and price > 1:
                price = price / 100
            if not (price.is_finite() and size.is_finite() and 0 < price < 1 and size >= 0):
                raise ValueError("out-of-range or non-finite orderbook price/size")
            if size > 0:
                result[price] = size
        return result

    @staticmethod
    def _side(book: dict[str, Any], side: str) -> tuple[list[Any], bool]:
        for field in (f"{side}_dollars_fp", f"{side}_dollars", side):
            if field in book:
                value = book[field]
                if not isinstance(value, list):
                    raise ValueError(f"{field} must be an array")
                return value, field == side
        return [], False

    def snapshot(self, msg: dict[str, Any]) -> None:
        nested = msg.get("orderbook_fp", msg.get("orderbook"))
        book = nested if nested is not None else msg
        if not isinstance(book, dict):
            self.invalidate()
            raise ValueError("orderbook snapshot must be an object")
        if not any(k in book for k in (
            "yes_dollars_fp", "no_dollars_fp", "yes_dollars", "no_dollars", "yes", "no"
        )):
            self.invalidate()
            raise ValueError("orderbook snapshot has no recognized bid sides")
        try:
            yes_rows, yes_legacy = self._side(book, "yes")
            no_rows, no_legacy = self._side(book, "no")
            yes = self._levels(yes_rows, legacy_cents=yes_legacy)
            no = self._levels(no_rows, legacy_cents=no_legacy)
        except ValueError:
            self.invalidate()
            raise
        self.yes, self.no = yes, no
        self.synchronized = True

    def invalidate(self) -> None:
        """Keep captured levels for diagnosis but prevent their use as a live book."""
        self.synchronized = False

    def delta(self, msg: dict[str, Any]) -> None:
        if not self.synchronized:
            raise ValueError("cannot apply orderbook delta before valid snapshot")
        side = str(msg.get("side", "")).lower()
        levels = self.yes if side == "yes" else self.no if side == "no" else None
        if levels is None:
            raise ValueError("unknown orderbook side")
        try:
            price = Decimal(str(msg.get("price_dollars", msg.get("price"))))
            delta = Decimal(str(msg.get("delta_fp", msg.get("delta"))))
            if "price_dollars" not in msg and price > 1:
                price = price / 100
        except InvalidOperation as exc:
            raise ValueError("invalid price or delta") from exc
        if not (price.is_finite() and delta.is_finite() and 0 < price < 1):
            raise ValueError("invalid orderbook delta price or size")
        size = levels.get(price, Decimal(0)) + delta
        if size < 0:
            raise ValueError("negative orderbook quantity")
        if size == 0:
            levels.pop(price, None)
        else:
            levels[price] = size
        self.synchronized = True

    def quotes(self) -> dict[str, str | None]:
        best_yes = max(self.yes, default=None)
        best_no = max(self.no, default=None)
        one = Decimal("1.0000")
        return {
            "yes_bid": str(best_yes) if best_yes is not None else None,
            "yes_bid_size": str(self.yes[best_yes]) if best_yes is not None else None,
            "yes_ask": str(one - best_no) if best_no is not None else None,
            "yes_ask_size": str(self.no[best_no]) if best_no is not None else None,
            "no_bid": str(best_no) if best_no is not None else None,
            "no_bid_size": str(self.no[best_no]) if best_no is not None else None,
            "no_ask": str(one - best_yes) if best_yes is not None else None,
            "no_ask_size": str(self.yes[best_yes]) if best_yes is not None else None,
        }
