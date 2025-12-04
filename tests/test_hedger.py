import asyncio
from types import SimpleNamespace

import pytest

from core.hedger import Hedger, HedgeLegRequest
from core.models import ExchangeName, OrderBook, OrderBookEntry, OrderSide, Trade
from core.risk_manager import RiskManager
from exchanges.orderbook_manager import OrderbookManager
from utils.config_loader import MarketHedgeConfig
from utils.logger import BotLogger


class DummyDatabase:
    def __init__(self):
        self.trades: list[Trade] = []
        self.begin_called = False
        self.commit_called = False
        self.rollback_called = False

    async def begin_transaction(self):
        self.begin_called = True
        return None

    async def commit_transaction(self, conn=None):
        self.commit_called = True

    async def rollback_transaction(self, conn=None):
        self.rollback_called = True

    async def save_trade(self, trade: Trade, tx_conn=None):
        self.trades.append(trade)

    async def record_incident(self, *args, **kwargs):
        return None


class DummyNotifier:
    def __init__(self):
        self.messages = []

    async def send_message(self, msg: str):
        self.messages.append(msg)
        return True


class DummyExchange:
    def __init__(self, orderbook: OrderBook):
        self.orderbook = orderbook
        self.orders = []

    async def get_orderbook(self, market_id: str):
        assert market_id == self.orderbook.market_id
        return self.orderbook

    async def place_market_order(self, market_id: str, side: OrderSide, size: float, client_order_id=None):
        self.orders.append({"market_id": market_id, "side": side, "size": size})
        return SimpleNamespace(order_id="hedge-order")


@pytest.mark.asyncio
async def test_hedger_reduces_size_on_slippage():
    config = MarketHedgeConfig(
        enabled=True,
        hedge_ratio=1.0,
        max_slippage_market_hedge=0.01,
        min_spread_for_entry=0.002,
        max_position_size_per_market=1000,
        max_position_size_per_event=5000,
        cancel_unfilled_after_ms=1000,
        allow_partial_fill_hedge=True,
        hedge_strategy="FULL",
    )
    risk_manager = RiskManager(config, BotLogger("test"))
    orderbook = OrderBook(
        market_id="secondary",
        bids=[],
        asks=[
            OrderBookEntry(price=0.50, size=40),
            OrderBookEntry(price=0.60, size=60),
        ],
    )
    exchange = DummyExchange(orderbook)
    db = DummyDatabase()
    notifier = DummyNotifier()
    hedger = Hedger(
        config,
        risk_manager,
        OrderbookManager(),
        db,
        notifier,
        BotLogger("hedger-test"),
        dry_run=False,
    )

    await hedger.hedge(
        legs=[HedgeLegRequest(exchange=ExchangeName.OPINION, client=exchange, market_id="secondary")],
        event_id="event-1",
        side=OrderSide.BUY,
        size=100,
        reference_price=0.45,
        entry_order_id="order-1",
        entry_exchange=ExchangeName.POLYMARKET,
    )

    assert exchange.orders, "hedge order was not placed"
    assert exchange.orders[0]["size"] < 100, "size should be reduced due to slippage"
    assert db.trades, "trade record should be persisted"
    assert notifier.messages, "notifier should receive update"
    assert db.commit_called and not db.rollback_called

