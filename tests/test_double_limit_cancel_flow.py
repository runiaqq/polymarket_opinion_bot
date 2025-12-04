import asyncio
from datetime import datetime, timezone

import pytest

from core.models import ExchangeName, Fill, Order, OrderSide, OrderStatus, OrderType
from core.order_manager import OrderManager


class StubExchange:
    def __init__(self, name: ExchangeName):
        self.name = name
        self.orders = []
        self.cancelled = []
        self.fail_cancel_times = 0

    async def place_limit_order(self, market_id, side, price, size, client_order_id=None):
        order = Order(
            order_id=f"{self.name.value}-{len(self.orders)+1}",
            client_order_id=client_order_id or f"cid-{len(self.orders)+1}",
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
        self.orders.append(order)
        return order

    async def cancel_order(self, order_id):
        self.cancelled.append(order_id)
        if self.fail_cancel_times > 0:
            self.fail_cancel_times -= 1
            raise RuntimeError("cancel failure")
        return True

    async def get_balances(self):
        return {"USDC": 1_000_000}


class DoubleLimitDB:
    def __init__(self):
        self.double_limits = {}
        self.order_index = {}
        self.events = []

    async def save_order(self, *_args, **_kwargs):
        return None

    async def update_order_fill(self, *_args, **_kwargs):
        return None

    async def update_order_status(self, *_args, **_kwargs):
        return None

    async def save_double_limit_pair(
        self,
        record_id,
        pair_key,
        primary_order_ref,
        secondary_order_ref,
        primary_exchange,
        secondary_exchange,
        primary_client_order_id,
        secondary_client_order_id,
        state=None,
    ):
        record = {
            "id": record_id,
            "order_a_ref": primary_order_ref,
            "order_b_ref": secondary_order_ref,
            "order_a_exchange": primary_exchange,
            "order_b_exchange": secondary_exchange,
            "client_order_id_a": primary_client_order_id,
            "client_order_id_b": secondary_client_order_id,
            "state": "ACTIVE",
        }
        self.double_limits[record_id] = record
        for value in [
            primary_order_ref,
            secondary_order_ref,
            primary_client_order_id,
            secondary_client_order_id,
        ]:
            self.order_index[value] = record_id

    async def get_double_limit_by_order(self, order_ref):
        record_id = self.order_index.get(order_ref)
        if not record_id:
            return None
        return self.double_limits.get(record_id)

    async def update_double_limit_state(self, record_id, state, **kwargs):
        record = self.double_limits.get(record_id)
        if record:
            record["state"] = state.value if hasattr(state, "value") else state

    async def log_order_event(self, order_id, stage, payload):
        self.events.append((order_id, stage, payload))

    async def record_incident(self, *_args, **_kwargs):
        return None


class DummyTracker:
    async def add_fill(self, *_args, **_kwargs):
        return None


class DummyHedger:
    def __init__(self):
        self.calls = []

    async def hedge(self, *_, **kwargs):
        self.calls.append(kwargs)
        return [{"order_id": "hedge"}]


class DummyRisk:
    async def check_limits(self, *_):
        return None

    async def check_balance(self, *_):
        return None


@pytest.mark.asyncio
async def test_double_limit_fill_cancels_counterpart_and_hedges():
    exchanges = {
        ExchangeName.OPINION: StubExchange(ExchangeName.OPINION),
        ExchangeName.POLYMARKET: StubExchange(ExchangeName.POLYMARKET),
    }
    db = DoubleLimitDB()
    tracker = DummyTracker()
    hedger = DummyHedger()
    risk = DummyRisk()
    manager = OrderManager(
        exchanges,
        db,
        tracker,
        hedger,
        risk,
        dry_run=False,
        market_map={
            ExchangeName.OPINION: "opinion_market",
            ExchangeName.POLYMARKET: "poly_market",
        },
        double_limit_enabled=True,
    )
    manager.set_routing(ExchangeName.OPINION, ExchangeName.POLYMARKET)
    await manager.place_double_limit(
        account="pair-1",
        pair=None,
        price_a=0.5,
        size_a=10,
        price_b=0.51,
        size_b=10,
    )
    primary_order = exchanges[ExchangeName.OPINION].orders[0]
    fill = Fill(
        order_id=primary_order.order_id,
        market_id="opinion_market",
        exchange=ExchangeName.OPINION,
        side=OrderSide.BUY,
        price=0.5,
        size=4,
        fee=0.0,
        timestamp=datetime.now(tz=timezone.utc),
    )
    await manager.handle_fill(ExchangeName.OPINION, fill)
    assert exchanges[ExchangeName.POLYMARKET].cancelled == [exchanges[ExchangeName.POLYMARKET].orders[0].order_id]
    assert len(hedger.calls) == 1
    assert hedger.calls[0]["size"] == pytest.approx(4)


@pytest.mark.asyncio
async def test_cancel_failure_still_triggers_hedge():
    exchanges = {
        ExchangeName.OPINION: StubExchange(ExchangeName.OPINION),
        ExchangeName.POLYMARKET: StubExchange(ExchangeName.POLYMARKET),
    }
    exchanges[ExchangeName.POLYMARKET].fail_cancel_times = 2
    db = DoubleLimitDB()
    tracker = DummyTracker()
    hedger = DummyHedger()
    risk = DummyRisk()
    manager = OrderManager(
        exchanges,
        db,
        tracker,
        hedger,
        risk,
        dry_run=False,
        market_map={
            ExchangeName.OPINION: "opinion_market",
            ExchangeName.POLYMARKET: "poly_market",
        },
        double_limit_enabled=True,
    )
    manager.cancel_retry_attempts = 2
    manager.set_routing(ExchangeName.OPINION, ExchangeName.POLYMARKET)
    await manager.place_double_limit(
        account="pair-1",
        pair=None,
        price_a=0.5,
        size_a=5,
        price_b=0.51,
        size_b=5,
    )
    primary_order = exchanges[ExchangeName.OPINION].orders[0]
    fill = Fill(
        order_id=primary_order.order_id,
        market_id="opinion_market",
        exchange=ExchangeName.OPINION,
        side=OrderSide.BUY,
        price=0.5,
        size=2,
        fee=0.0,
        timestamp=datetime.now(tz=timezone.utc),
    )
    await manager.handle_fill(ExchangeName.OPINION, fill)
    assert len(hedger.calls) == 1
    assert hedger.calls[0]["size"] == pytest.approx(2)

