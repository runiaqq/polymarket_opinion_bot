from datetime import datetime, timezone

import pytest

from decimal import Decimal

from core.models import ExchangeName, Fill, OrderSide
from core.order_manager import OrderManager


class DummyDB:
    def __init__(self):
        self.updated = []

    async def update_order_fill(self, order_id, increment, fill):
        self.updated.append((order_id, increment, fill))

    async def save_order(self, *_args, **_kwargs):
        return None

    async def update_order_status(self, *_args, **_kwargs):
        return None


class DummyTracker:
    def __init__(self):
        self.fills = []

    async def add_fill(self, event_id, size, price, side):
        self.fills.append((event_id, size, price, side))


class DummyHedger:
    def __init__(self):
        self.calls = []

    async def hedge(self, *args, **kwargs):
        self.calls.append(kwargs)


class DummyRiskManager:
    async def check_limits(self, *_):
        return None

    async def check_balance(self, *_):
        return None


class DummyMapper:
    def __init__(self, poly_to_op=None, op_to_poly=None):
        self.poly_to_op = poly_to_op or {}
        self.op_to_poly = op_to_poly or {}

    def find_opinion_for_polymarket(self, poly_id):
        return self.poly_to_op.get(poly_id)

    def find_polymarket_for_opinion(self, op_id):
        return self.op_to_poly.get(op_id)


@pytest.mark.asyncio
async def test_handle_fill_routes_to_secondary_market():
    exchanges = {
        ExchangeName.POLYMARKET: object(),
        ExchangeName.OPINION: object(),
    }
    db = DummyDB()
    tracker = DummyTracker()
    hedger = DummyHedger()
    risk = DummyRiskManager()
    mapper = DummyMapper(poly_to_op={"poly-market": "opinion-token"})
    manager = OrderManager(
        exchanges,
        db,
        tracker,
        hedger,
        risk,
        dry_run=True,
        event_id="event-42",
        market_map={
            ExchangeName.POLYMARKET: "poly-market",
            ExchangeName.OPINION: "opinion-token",
        },
        mapper=mapper,
    )
    manager.set_routing(ExchangeName.POLYMARKET, ExchangeName.OPINION)

    message = {
        "data": {
            "order_id": "order123",
            "market_id": "poly-market",
            "token_id": "poly-market",
            "side": "BUY",
            "price": 0.5,
            "filled_size": 25,
            "fee": 0.0,
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        }
    }
    fill = manager.normalize_fill(ExchangeName.POLYMARKET, message)
    assert fill
    await manager.handle_fill(ExchangeName.POLYMARKET, fill)

    assert db.updated, "order fill should be recorded"
    assert tracker.fills[0][0] == "event-42"
    assert hedger.calls
    assert hedger.calls[0]["legs"][0].market_id == "opinion-token"


class PollingExchange:
    def __init__(self, fills):
        self.queue = fills

    async def fetch_fills(self, since=None):
        if self.queue:
            return self.queue.pop(0)
        return []


@pytest.mark.asyncio
async def test_poll_fills_triggers_hedge():
    fill = Fill(
        order_id="poll-1",
        market_id="poly-market",
        exchange=ExchangeName.POLYMARKET,
        side=OrderSide.BUY,
        price=0.45,
        size=10,
        fee=0.0,
        timestamp=datetime.now(tz=timezone.utc),
    )
    exchanges = {
        ExchangeName.POLYMARKET: PollingExchange([[fill]]),
        ExchangeName.OPINION: object(),
    }
    db = DummyDB()
    tracker = DummyTracker()
    hedger = DummyHedger()
    risk = DummyRiskManager()
    mapper = DummyMapper(poly_to_op={"poly-market": "opinion-token"})
    manager = OrderManager(
        exchanges,
        db,
        tracker,
        hedger,
        risk,
        dry_run=True,
        event_id="event-99",
        market_map={
            ExchangeName.POLYMARKET: "poly-market",
            ExchangeName.OPINION: "opinion-token",
        },
        mapper=mapper,
    )
    manager.set_routing(ExchangeName.POLYMARKET, ExchangeName.OPINION)
    await manager.handle_fill(ExchangeName.POLYMARKET, fill)

    assert db.updated, "order fill should be recorded"
    assert hedger.calls, "hedger should run for fill"

