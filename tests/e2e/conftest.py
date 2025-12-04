import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import AsyncIterator, Dict

import pytest
from aioresponses import aioresponses


@pytest.fixture
def http_mocks():
    with aioresponses() as mock:
        yield mock


class DummyDB:
    async def update_order_fill(self, *args, **kwargs):
        return None

    async def save_order(self, *args, **kwargs):
        return None

    async def update_order_status(self, *args, **kwargs):
        return None


class DummyTracker:
    async def add_fill(self, *args, **kwargs):
        return None


class DummyRisk:
    async def check_limits(self, *args, **kwargs):
        return None

    async def check_balance(self, *args, **kwargs):
        return None

    async def check_slippage(self, *args, **kwargs):
        return None


class DummyHedger:
    def __init__(self):
        self.calls = []

    async def hedge(self, *args, **kwargs):
        self.calls.append(kwargs)


@dataclass
class MockExchangeClient:
    fills: asyncio.Queue

    async def place_limit_order(self, *args, **kwargs):
        return type("Order", (), {"order_id": kwargs.get("client_order_id", "order-1")})

    async def get_orderbook(self, market_id):
        return {
            "market_id": market_id,
            "bids": [{"price": 0.48, "size": 100}],
            "asks": [{"price": 0.52, "size": 100}],
        }

    async def place_market_order(self, market_id, side, size, client_order_id=None):
        return type("Order", (), {"order_id": f"hedge-{market_id}"})

    async def fetch_user_trades(self, since=None):
        trades = []
        while not self.fills.empty():
            trades.append(await self.fills.get())
        return trades

    async def listen_fills(self, handler):
        while not self.fills.empty():
            payload = await self.fills.get()
            await handler({"data": payload})

