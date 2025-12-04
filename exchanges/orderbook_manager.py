from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from core.models import OrderBook, OrderBookEntry, OrderSide


class OrderbookManager:
    """Utility for working with exchange orderbooks."""

    async def best_bid(self, orderbook: OrderBook) -> Optional[OrderBookEntry]:
        return orderbook.bids[0] if orderbook.bids else None

    async def best_ask(self, orderbook: OrderBook) -> Optional[OrderBookEntry]:
        return orderbook.asks[0] if orderbook.asks else None

    async def combined(self, ob_a: OrderBook, ob_b: OrderBook) -> Dict[str, OrderBook]:
        return {"primary": ob_a, "secondary": ob_b}

    def build(
        self,
        market_id: str,
        bids: List[Dict[str, float]],
        asks: List[Dict[str, float]],
    ) -> OrderBook:
        return OrderBook(
            market_id=market_id,
            bids=[
                OrderBookEntry(
                    price=float(level["price"]),
                    size=float(level.get("size") or level.get("amount") or 0.0),
                )
                for level in bids
            ],
            asks=[
                OrderBookEntry(
                    price=float(level["price"]),
                    size=float(level.get("size") or level.get("amount") or 0.0),
                )
                for level in asks
            ],
        )

    def parse_orderbook(
        self,
        market_id: str,
        bids: List[Dict[str, float]],
        asks: List[Dict[str, float]],
    ) -> OrderBook:
        return self.build(market_id, bids, asks)

    def get_best_price_for_size(
        self,
        orderbook: OrderBook,
        side: OrderSide,
        size: float,
    ) -> Optional[float]:
        depth = orderbook.asks if side == OrderSide.BUY else orderbook.bids
        remaining = size
        for level in depth:
            if level.size >= remaining:
                return level.price
            remaining -= level.size
        return None

    def estimate_slippage(
        self,
        orderbook: OrderBook,
        side: OrderSide,
        size: float,
    ) -> Tuple[float, float]:
        depth = orderbook.asks if side == OrderSide.BUY else orderbook.bids
        if not depth:
            return 0.0, 0.0

        remaining = size
        filled_value = 0.0
        accumulated = 0.0
        for level in depth:
            take = min(level.size, remaining)
            filled_value += take * level.price
            accumulated += take
            remaining -= take
            if remaining <= 0:
                break

        if accumulated == 0:
            return 0.0, 0.0

        average_price = filled_value / accumulated
        top_price = depth[0].price
        slippage = average_price - top_price if side == OrderSide.BUY else top_price - average_price
        return average_price, slippage

