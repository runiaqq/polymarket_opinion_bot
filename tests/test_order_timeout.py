import asyncio
from datetime import datetime, timezone

import pytest

from core.models import ExchangeName, Order, OrderSide, OrderStatus, OrderType
from core.order_manager import OrderManager
from utils.config_loader import MarketPairConfig


class StubExchange:
    def __init__(self, name: ExchangeName):
        self.name = name
        self.cancelled = []

    async def place_limit_order(self, market_id, side, price, size, client_order_id=None):
        return Order(
            order_id=f"{self.name.value}-1",
            client_order_id=client_order_id or f"{self.name.value}-cid",
            market_id=market_id,
            exchange=self.name,
            side=side,
            order_type=OrderType.LIMIT,
            price=price,
            size=size,
            filled_size=0.0,
            status=OrderStatus.OPEN,
            created_at=datetime.now(tz=timezone.utc),
        )

    async def cancel_order(self, order_id):
        self.cancelled.append(order_id)
        return True

    async def get_balances(self):
        return {"USDC": 1_000_000}


class DummyDB:
    async def save_order(self, *_args, **_kwargs):
        return None

    async def update_order_status(self, *_args, **_kwargs):
        return None

    async def update_order_fill(self, *_args, **_kwargs):
        return None

    async def save_double_limit_pair(self, *_args, **_kwargs):
        return None

    async def get_double_limit_by_order(self, *_args, **_kwargs):
        return None

    async def log_order_event(self, *_args, **_kwargs):
        return None

    async def record_incident(self, *_args, **_kwargs):
        return None


class DummyRisk:
    def __init__(self):
        self.exposure = 0

    async def check_limits(self, *_):
        self.exposure += 10

    async def check_balance(self, *_):
        return None

    async def decrement(self, *_):
        self.exposure -= 10


class DummyTracker:
    async def add_fill(self, *_args, **_kwargs):
        return None


class DummyHedger:
    pass


@pytest.mark.asyncio
async def test_order_auto_cancel_after_timeout():
    exchanges = {
        ExchangeName.OPINION: StubExchange(ExchangeName.OPINION),
        ExchangeName.POLYMARKET: StubExchange(ExchangeName.POLYMARKET),
    }
    manager = OrderManager(
        exchanges,
        DummyDB(),
        DummyTracker(),
        DummyHedger(),
        DummyRisk(),
        dry_run=False,
        event_id="evt-timeout",
        market_map={
            ExchangeName.OPINION: "opinion_market",
            ExchangeName.POLYMARKET: "poly_market",
        },
        double_limit_enabled=False,
        cancel_after_ms=20,
    )
    manager.set_routing(ExchangeName.OPINION, ExchangeName.POLYMARKET)
    await manager.place_primary_limit(
        ExchangeName.OPINION,
        "opinion_market",
        OrderSide.BUY,
        price=0.5,
        size=10,
    )
    await asyncio.sleep(0.05)
    assert exchanges[ExchangeName.OPINION].cancelled, "order should be auto-cancelled after timeout"





