from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from core.models import ExchangeName, OrderSide, OrderStatus, OrderType


@dataclass(slots=True)
class MockOrder:
    order_id: str
    client_order_id: str
    market_id: str
    price: float
    size: float
    side: OrderSide
    exchange: ExchangeName
    created_at: datetime


class MockExchange:
    """Lightweight in-memory exchange mock for tests."""

    def __init__(self, name: ExchangeName, base_price: float = 0.5, latency: float = 0.0001):
        self.name = name
        self.base_price = base_price
        self.latency = latency
        self._order_counter = 0
        self.limit_orders = 0
        self.market_orders = 0
        self.cancels = 0

    async def place_limit_order(
        self,
        market_id: str,
        side: OrderSide,
        price: float,
        size: float,
        client_order_id: Optional[str] = None,
    ) -> MockOrder:
        await asyncio.sleep(self.latency)
        self._order_counter += 1
        order_id = client_order_id or f"{self.name.value}-order-{self._order_counter}"
        self.limit_orders += 1
        return MockOrder(
            order_id=order_id,
            client_order_id=order_id,
            market_id=market_id,
            price=price,
            size=size,
            side=side,
            exchange=self.name,
            created_at=datetime.now(tz=timezone.utc),
        )

    async def place_market_order(
        self,
        market_id: str,
        side: OrderSide,
        size: float,
        client_order_id: Optional[str] = None,
    ) -> MockOrder:
        await asyncio.sleep(self.latency)
        self._order_counter += 1
        order_id = client_order_id or f"{self.name.value}-market-{self._order_counter}"
        self.market_orders += 1
        return MockOrder(
            order_id=order_id,
            client_order_id=order_id,
            market_id=market_id,
            price=self.base_price,
            size=size,
            side=side,
            exchange=self.name,
            created_at=datetime.now(tz=timezone.utc),
        )

    async def cancel_order(self, order_id: str) -> bool:
        await asyncio.sleep(self.latency)
        self.cancels += 1
        return True

    async def get_orderbook(self, market_id: str):
        await asyncio.sleep(self.latency)
        return {
            "market_id": market_id,
            "bids": [{"price": self.base_price - 0.01, "size": 100}],
            "asks": [{"price": self.base_price + 0.01, "size": 100}],
        }

    async def get_balances(self):
        return {"USDC": 1_000_000}

