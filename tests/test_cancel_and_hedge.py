from datetime import datetime, timezone

import pytest

from core.hedger import Hedger, HedgeLegRequest
from core.models import (
    ExchangeName,
    Fill,
    Order,
    OrderBook,
    OrderBookEntry,
    OrderSide,
    OrderStatus,
    OrderType,
)
from core.order_manager import OrderManager
from core.risk_manager import RiskManager
from exchanges.orderbook_manager import OrderbookManager
from utils.config_loader import MarketHedgeConfig, MarketPairConfig


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
        return [{"order_id": "hedge-1"}]


class DummyRiskManager:
    async def check_limits(self, *_):
        return None

    async def check_balance(self, *_):
        return None

    async def check_slippage(self, *_):
        return None


class SequenceDB:
    def __init__(self):
        self.double_limits = {}
        self._order_index = {}
        self.events = []

    async def update_order_fill(self, *_args, **_kwargs):
        return None

    async def save_order(self, *_args, **_kwargs):
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
            "pair_key": pair_key,
            "order_a_ref": primary_order_ref,
            "order_b_ref": secondary_order_ref,
            "order_a_exchange": primary_exchange,
            "order_b_exchange": secondary_exchange,
            "client_order_id_a": primary_client_order_id,
            "client_order_id_b": secondary_client_order_id,
            "state": "ACTIVE",
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
        record["state"] = state.value if hasattr(state, "value") else state
        if triggered_order_id:
            record["triggered_order_id"] = triggered_order_id
        if cancelled_order_id:
            record["cancelled_order_id"] = cancelled_order_id

    async def record_incident(self, *_args, **_kwargs):
        return None

    async def log_order_event(self, order_id, stage, payload):
        self.events.append((order_id, stage, payload))

    def _index(self, key, record_id):
        if key:
            self._order_index[key] = record_id


class StubExchange:
    def __init__(self, name: ExchangeName):
        self.name = name
        self.orders = []
        self.cancelled = []
        self.cancel_attempts = 0

    async def place_limit_order(self, market_id, side, price, size, client_order_id=None):
        order = Order(
            order_id=f"{self.name.value.lower()}-{len(self.orders)+1}",
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
        self.cancel_attempts += 1
        self.cancelled.append(order_id)
        raise RuntimeError("cancel failure")

    async def get_balances(self):
        return {"USDC": 1_000_000}


@pytest.mark.asyncio
async def test_cancel_failure_still_triggers_hedge():
    exchanges = {
        ExchangeName.OPINION: StubExchange(ExchangeName.OPINION),
        ExchangeName.POLYMARKET: StubExchange(ExchangeName.POLYMARKET),
    }
    # allow secondary cancel to fail
    exchanges[ExchangeName.POLYMARKET].cancel_order = exchanges[ExchangeName.POLYMARKET].cancel_order

    db = SequenceDB()
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
    manager.cancel_retry_attempts = 2
    manager._cancel_backoff_base = 0  # speed up tests

    pair = MarketPairConfig(
        event_id="event-dl",
        primary_market_id="opinion-token",
        secondary_market_id="poly-market",
    )
    await manager.place_double_limit(
        account="acct-1",
        pair=pair,
        price_a=0.5,
        size_a=5,
        price_b=0.51,
        size_b=5,
        side_a=OrderSide.BUY,
        side_b=OrderSide.BUY,
    )
    primary_order_id = exchanges[ExchangeName.OPINION].orders[0].order_id
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
    assert len(hedger.calls) == 1, "hedger should execute even when cancel fails"
    assert exchanges[ExchangeName.POLYMARKET].cancel_attempts == manager.cancel_retry_attempts
    assert any(stage == "hedge" for _, stage, _ in db.events)


class DummyNotifier:
    def __init__(self):
        self.messages = []
        self.enabled = True

    async def send_message(self, msg: str) -> bool:
        self.messages.append(msg)
        return True


class HedgerDB:
    def __init__(self):
        self.incidents = []

    async def begin_transaction(self):
        return object()

    async def commit_transaction(self, _conn):
        return None

    async def rollback_transaction(self, _conn):
        return None

    async def save_trade(self, *_args, **_kwargs):
        return None

    async def record_incident(self, _level, message, details):
        self.incidents.append((message, details))


class ShallowOrderbookClient:
    def __init__(self, orderbook: OrderBook):
        self.orderbook = orderbook

    async def get_orderbook(self, _market_id):
        return self.orderbook


@pytest.mark.asyncio
async def test_ultra_safe_slippage_blocks_hedge():
    config = MarketHedgeConfig(
        enabled=True,
        hedge_ratio=1.0,
        max_slippage_market_hedge=0.01,
        min_spread_for_entry=0.0,
        max_position_size_per_market=100,
        max_position_size_per_event=200,
        cancel_unfilled_after_ms=60000,
        allow_partial_fill_hedge=True,
        hedge_strategy="FULL",
        max_slippage_percent=0.05,
        min_quote_size=0.0,
        exposure_tolerance=0.0,
        ultra_safe=True,
    )
    risk_manager = RiskManager(config)
    orderbook = OrderBook(
        market_id="m",
        bids=[],
        asks=[
            OrderBookEntry(price=0.50, size=1),
            OrderBookEntry(price=0.70, size=10),
        ],
    )
    client = ShallowOrderbookClient(orderbook)
    db = HedgerDB()
    notifier = DummyNotifier()
    hedger = Hedger(
        config,
        risk_manager,
        OrderbookManager(),
        db,
        notifier,
        logger=None,
        dry_run=True,
    )
    request = HedgeLegRequest(
        client=client,
        exchange=ExchangeName.OPINION,
        market_id="m",
    )
    with pytest.raises(Exception):
        await hedger.hedge(
            legs=[request],
            event_id="evt",
            side=OrderSide.BUY,
            size=5,
            reference_price=0.5,
            entry_order_id="entry",
            entry_exchange=ExchangeName.POLYMARKET,
        )
    assert notifier.messages, "ultra-safe skip should send telemetry"
    assert any("Ultra Safe" in msg for msg in notifier.messages)

