from datetime import datetime, timezone

import pytest

from decimal import Decimal

from core.models import (
    ExchangeName,
    Fill,
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
)
from core.order_manager import OrderManager
from utils.config_loader import MarketPairConfig


class DummyDB:
    def __init__(self):
        self.updated = []
        self.events = []

    async def update_order_fill(self, order_id, increment, fill):
        self.updated.append((order_id, increment, fill))

    async def save_order(self, *_args, **_kwargs):
        return None

    async def update_order_status(self, *_args, **_kwargs):
        return None

    async def log_order_event(self, order_id, stage, payload):
        self.events.append((order_id, stage, payload))


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


class StubExchange:
    def __init__(self, name: ExchangeName):
        self.name = name
        self.orders = []
        self.cancelled = []
        self._counter = 0

    async def place_limit_order(
        self,
        market_id: str,
        side: OrderSide,
        price: float,
        size: float,
        client_order_id: str | None = None,
    ) -> Order:
        self._counter += 1
        order_id = f"{self.name.value.lower()}-{self._counter}"
        order = Order(
            order_id=order_id,
            client_order_id=client_order_id or order_id,
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

    async def cancel_order(self, order_id: str | None = None, client_order_id: str | None = None) -> bool:
        identifier = order_id or client_order_id
        self.cancelled.append(identifier)
        return True


class DoubleLimitDB(DummyDB):
    def __init__(self):
        super().__init__()
        self.double_limits = {}
        self._order_index = {}

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
            "pair_key": pair_key,
            "order_a_ref": primary_order_ref,
            "order_b_ref": secondary_order_ref,
            "order_a_exchange": primary_exchange,
            "order_b_exchange": secondary_exchange,
            "client_order_id_a": primary_client_order_id,
            "client_order_id_b": secondary_client_order_id,
            "state": state.value if state else "ACTIVE",
        }
        self.double_limits[record_id] = record
        self._index(primary_order_ref, record_id)
        self._index(secondary_order_ref, record_id)
        self._index(primary_client_order_id, record_id)
        self._index(secondary_client_order_id, record_id)

    async def get_double_limit_by_order(self, order_ref):
        record_id = self._order_index.get(order_ref)
        if not record_id:
            return None
        return self.double_limits.get(record_id)

    async def update_double_limit_state(self, record_id, state, triggered_order_id=None, cancelled_order_id=None):
        record = self.double_limits.get(record_id)
        if not record:
            return
        record["state"] = state.value
        if triggered_order_id:
            record["triggered_order_id"] = triggered_order_id
        if cancelled_order_id:
            record["cancelled_order_id"] = cancelled_order_id

    def _index(self, key, record_id):
        if key:
            self._order_index[key] = record_id


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
        dry_run=False,
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
        dry_run=False,
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


@pytest.mark.asyncio
async def test_place_double_limit_persists_records():
    exchanges = {
        ExchangeName.OPINION: StubExchange(ExchangeName.OPINION),
        ExchangeName.POLYMARKET: StubExchange(ExchangeName.POLYMARKET),
    }
    db = DoubleLimitDB()
    tracker = DummyTracker()
    hedger = DummyHedger()
    risk = DummyRiskManager()
    mapper = DummyMapper()
    manager = OrderManager(
        exchanges,
        db,
        tracker,
        hedger,
        risk,
        dry_run=False,
        event_id="event-dl",
        market_map={
            ExchangeName.OPINION: "opinion-token",
            ExchangeName.POLYMARKET: "poly-market",
        },
        mapper=mapper,
        double_limit_enabled=True,
    )
    manager.set_routing(ExchangeName.OPINION, ExchangeName.POLYMARKET)
    pair = MarketPairConfig(
        event_id="event-dl",
        primary_market_id="opinion-token",
        secondary_market_id="poly-market",
    )
    primary_id, secondary_id = await manager.place_double_limit(
        account="acct-1",
        pair=pair,
        price_a=0.5,
        size_a=10,
        price_b=0.51,
        size_b=5,
        side_a=OrderSide.BUY,
        side_b=OrderSide.BUY,
    )
    assert primary_id != secondary_id
    assert db.double_limits, "double limit record should be stored"


@pytest.mark.asyncio
async def test_double_limit_primary_fill_cancels_secondary_once():
    exchanges = {
        ExchangeName.OPINION: StubExchange(ExchangeName.OPINION),
        ExchangeName.POLYMARKET: StubExchange(ExchangeName.POLYMARKET),
    }
    db = DoubleLimitDB()
    tracker = DummyTracker()
    hedger = DummyHedger()
    risk = DummyRiskManager()
    manager = OrderManager(
        exchanges,
        db,
        tracker,
        hedger,
        risk,
        dry_run=False,
        event_id="event-dl",
        market_map={
            ExchangeName.OPINION: "opinion-token",
            ExchangeName.POLYMARKET: "poly-market",
        },
        double_limit_enabled=True,
    )
    manager.set_routing(ExchangeName.OPINION, ExchangeName.POLYMARKET)
    pair = MarketPairConfig(
        event_id="event-dl",
        primary_market_id="opinion-token",
        secondary_market_id="poly-market",
    )
    await manager.place_double_limit(
        account="acct-1",
        pair=pair,
        price_a=0.5,
        size_a=10,
        price_b=0.51,
        size_b=10,
        side_a=OrderSide.BUY,
        side_b=OrderSide.BUY,
    )
    primary_order_id = exchanges[ExchangeName.OPINION].orders[0].order_id
    secondary_order_id = exchanges[ExchangeName.POLYMARKET].orders[0].order_id
    fill = Fill(
        order_id=primary_order_id,
        market_id="opinion-token",
        exchange=ExchangeName.OPINION,
        side=OrderSide.BUY,
        price=0.5,
        size=2,
        fee=0.0,
        timestamp=datetime.now(tz=timezone.utc),
    )
    await manager.handle_fill(ExchangeName.OPINION, fill)
    assert exchanges[ExchangeName.POLYMARKET].cancelled == [secondary_order_id]
    assert len(hedger.calls) == 1


@pytest.mark.asyncio
async def test_double_limit_secondary_fill_cancels_primary_once():
    exchanges = {
        ExchangeName.OPINION: StubExchange(ExchangeName.OPINION),
        ExchangeName.POLYMARKET: StubExchange(ExchangeName.POLYMARKET),
    }
    db = DoubleLimitDB()
    tracker = DummyTracker()
    hedger = DummyHedger()
    risk = DummyRiskManager()
    manager = OrderManager(
        exchanges,
        db,
        tracker,
        hedger,
        risk,
        dry_run=False,
        event_id="event-dl",
        market_map={
            ExchangeName.OPINION: "opinion-token",
            ExchangeName.POLYMARKET: "poly-market",
        },
        double_limit_enabled=True,
    )
    manager.set_routing(ExchangeName.OPINION, ExchangeName.POLYMARKET)
    pair = MarketPairConfig(
        event_id="event-dl",
        primary_market_id="opinion-token",
        secondary_market_id="poly-market",
    )
    await manager.place_double_limit(
        account="acct-1",
        pair=pair,
        price_a=0.5,
        size_a=10,
        price_b=0.51,
        size_b=10,
        side_a=OrderSide.BUY,
        side_b=OrderSide.BUY,
    )
    primary_order_id = exchanges[ExchangeName.OPINION].orders[0].order_id
    secondary_order_id = exchanges[ExchangeName.POLYMARKET].orders[0].order_id
    fill = Fill(
        order_id=secondary_order_id,
        market_id="poly-market",
        exchange=ExchangeName.POLYMARKET,
        side=OrderSide.BUY,
        price=0.51,
        size=4,
        fee=0.0,
        timestamp=datetime.now(tz=timezone.utc),
    )
    await manager.handle_fill(ExchangeName.POLYMARKET, fill)
    assert exchanges[ExchangeName.OPINION].cancelled == [primary_order_id]
    assert len(hedger.calls) == 1

