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
    def _levels(values: list[Any]) -> dict[Decimal, Decimal]:
        result: dict[Decimal, Decimal] = {}
        for item in values or []:
            if not isinstance(item, (tuple, list)) or len(item) < 2:
                continue
            try:
                price, size = Decimal(str(item[0])), Decimal(str(item[1]))
            except InvalidOperation:
                continue
            if price > 0 and size > 0:
                result[price] = size
        return result

    def snapshot(self, msg: dict[str, Any]) -> None:
        nested = msg.get("orderbook_fp") or msg.get("orderbook")
        book = nested if isinstance(nested, dict) else msg
        self.yes = self._levels(
            book.get("yes_dollars_fp") or book.get("yes_dollars") or book.get("yes") or []
        )
        self.no = self._levels(
            book.get("no_dollars_fp") or book.get("no_dollars") or book.get("no") or []
        )
        self.synchronized = True

    def invalidate(self) -> None:
        """Keep captured levels for diagnosis but prevent their use as a live book."""
        self.synchronized = False

    def delta(self, msg: dict[str, Any]) -> None:
        side = str(msg.get("side", "")).lower()
        levels = self.yes if side == "yes" else self.no if side == "no" else None
        if levels is None:
            raise ValueError("unknown orderbook side")
        try:
            price = Decimal(str(msg.get("price_dollars", msg.get("price"))))
            delta = Decimal(str(msg.get("delta_fp", msg.get("delta"))))
        except InvalidOperation as exc:
            raise ValueError("invalid price or delta") from exc
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
