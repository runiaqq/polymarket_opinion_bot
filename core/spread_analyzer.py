from __future__ import annotations

from core.models import OrderBook
from exchanges.orderbook_manager import OrderbookManager


class SpreadAnalyzer:
    """Evaluates cross-exchange spreads."""

    def __init__(self):
        self.orderbooks = OrderbookManager()

    async def compute_spread(self, primary: OrderBook, secondary: OrderBook) -> float:
        best_bid = await self.orderbooks.best_bid(secondary)
        best_ask = await self.orderbooks.best_ask(primary)
        if not best_bid or not best_ask:
            return 0.0
        return best_bid.price - best_ask.price

    async def is_profitable(self, spread: float, fees: float, min_spread: float) -> bool:
        adjusted = spread - fees
        return adjusted >= min_spread

