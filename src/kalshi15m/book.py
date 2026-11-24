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
        # Best bid per side, kept incrementally: quotes() runs on every delta and a max() over
        # hundreds of Decimal keys was ~15% of the event loop's busy time.
        self._best_yes: Decimal | None = None
        self._best_no: Decimal | None = None

    @staticmethod
    def _levels(values: list[Any]) -> dict[Decimal, Decimal]:
        result: dict[Decimal, Decimal] = {}
        seen: set[Decimal] = set()
        if not isinstance(values, list):
            raise ValueError("orderbook snapshot levels must be a list")
        for item in values:
            # A malformed level must never be dropped silently: that would publish a
            # "synchronized" book with understated depth and wrong derived asks.
            if not isinstance(item, (tuple, list)) or len(item) < 2:
                raise ValueError("malformed orderbook snapshot level")
            try:
                price, size = Decimal(str(item[0])), Decimal(str(item[1]))
            except InvalidOperation as exc:
                raise ValueError("unparseable orderbook snapshot level") from exc
            if not (price.is_finite() and size.is_finite()):
                raise ValueError("non-finite orderbook snapshot level")
            if not (Decimal(0) < price <= Decimal(1)) or size < 0:
                raise ValueError("out-of-range orderbook snapshot level")
            if price in seen:  # zero-size repeats too: an ambiguous level list is not a book
                raise ValueError("duplicate orderbook snapshot price")
            seen.add(price)
            if size > 0:
                result[price] = size
        return result

    def snapshot(self, msg: dict[str, Any]) -> None:
        self.synchronized = False  # stays unsynchronized unless the whole snapshot parses
        book = msg
        for key in ("orderbook_fp", "orderbook"):
            if key in msg:
                book = msg[key]
                if not isinstance(book, dict):
                    raise ValueError("malformed orderbook snapshot envelope")
                break

        def side_levels(side: str) -> dict[Decimal, Decimal]:
            for key in (f"{side}_dollars_fp", f"{side}_dollars", side):
                if key in book:
                    return self._levels(book[key])
            return {}

        # Parse both sides before replacing either; preserve the old invalidated book on failure.
        yes, no = side_levels("yes"), side_levels("no")
        self.yes, self.no = yes, no
        self._best_yes = max(self.yes, default=None)
        self._best_no = max(self.no, default=None)
        self.synchronized = True

    def invalidate(self) -> None:
        """Keep captured levels for diagnosis but prevent their use as a live book."""
        self.synchronized = False

    def delta(self, msg: dict[str, Any]) -> None:
        if not self.synchronized:
            # An invalidated or never-snapshotted book can only be revived by a fresh snapshot.
            raise ValueError("delta on an unsynchronized orderbook")
        # A malformed delta invalidates the entire lineage until a new snapshot.
        self.synchronized = False
        side = str(msg.get("side", "")).lower()
        levels = self.yes if side == "yes" else self.no if side == "no" else None
        if levels is None:
            raise ValueError("unknown orderbook side")
        try:
            price = Decimal(str(msg.get("price_dollars", msg.get("price"))))
            delta = Decimal(str(msg.get("delta_fp", msg.get("delta"))))
        except InvalidOperation as exc:
            raise ValueError("invalid price or delta") from exc
        if not (price.is_finite() and delta.is_finite()):
            raise ValueError("non-finite orderbook price or delta")
        if not Decimal(0) < price <= Decimal(1):
            raise ValueError("out-of-range orderbook delta price")
        try:
            size = levels.get(price, Decimal(0)) + delta
        except ArithmeticError as exc:
            raise ValueError("invalid resulting orderbook quantity") from exc
        if not size.is_finite() or size < 0:
            raise ValueError("negative orderbook quantity")
        is_yes = levels is self.yes
        best = self._best_yes if is_yes else self._best_no
        if size == 0:
            levels.pop(price, None)
            if price == best:  # the best level went away: only now is a scan needed
                best = max(levels, default=None)
        else:
            levels[price] = size
            if best is None or price > best:
                best = price
        if is_yes:
            self._best_yes = best
        else:
            self._best_no = best
        self.synchronized = True

    def _best(self) -> tuple[Decimal | None, Decimal | None]:
        """Cached best bids, re-derived if the level dicts were replaced behind our back."""
        yes, no = self._best_yes, self._best_no
        if (yes is None) == bool(self.yes) or (yes is not None and yes not in self.yes):
            yes = self._best_yes = max(self.yes, default=None)
        if (no is None) == bool(self.no) or (no is not None and no not in self.no):
            no = self._best_no = max(self.no, default=None)
        return yes, no

    def quotes(self) -> dict[str, str | None]:
        best_yes, best_no = self._best()
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
