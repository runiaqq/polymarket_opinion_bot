from __future__ import annotations

from datetime import datetime, timezone

import pytest

from core.exceptions import HedgingError
from core.hedger import Hedger, HedgeLegRequest, HedgeStrategy
from core.models import ExchangeName, OrderBook, OrderBookEntry, OrderSide, Trade
from core.risk_manager import RiskManager
from exchanges.orderbook_manager import OrderbookManager
from utils.config_loader import MarketHedgeConfig
from utils.logger import BotLogger


class DummyExchange:
    def __init__(self, market_id: str, levels: list[float]):
        self.market_id = market_id
        self.orderbook = OrderBook(
            market_id=market_id,
            bids=[],
            asks=[OrderBookEntry(price=price, size=100) for price in levels],
        )
        self.orders = []

    async def get_orderbook(self, market_id: str):
        assert market_id == self.market_id
        return self.orderbook

    async def place_market_order(self, market_id: str, side: OrderSide, size: float, client_order_id=None):
        self.orders.append({"market_id": market_id, "side": side, "size": size})
        return type("Order", (), {"order_id": f"{market_id}-order"})


class DummyDB:
    def __init__(self):
        self.trades: list[Trade] = []
        self.begin_called = False
        self.commit_called = False
        self.rollback_called = False
        self.incidents = []

    async def begin_transaction(self):
        self.begin_called = True
        return None

    async def commit_transaction(self, conn=None):
        self.commit_called = True

    async def rollback_transaction(self, conn=None):
        self.rollback_called = True

    async def save_trade(self, trade: Trade, tx_conn=None):
        self.trades.append(trade)

    async def record_incident(self, level, message, details):
        self.incidents.append((level, message, details))


class DummyNotifier:
    def __init__(self):
        self.messages = []

    async def send_message(self, msg):
        self.messages.append(msg)


def build_config(strategy: str) -> MarketHedgeConfig:
    return MarketHedgeConfig(
        enabled=True,
        hedge_ratio=1.0,
        max_slippage_market_hedge=0.02,
        min_spread_for_entry=0.0,
        max_position_size_per_market=1000,
        max_position_size_per_event=1000,
        cancel_unfilled_after_ms=1000,
        allow_partial_fill_hedge=True,
        hedge_strategy=strategy,
    )


@pytest.mark.asyncio
async def test_multi_leg_atomic_success():
    config = build_config("FULL")
    hedger = Hedger(
        config,
        risk_manager=RiskManager(config, BotLogger("risk")),
        orderbook_manager=OrderbookManager(),
        database=DummyDB(),
        notifier=DummyNotifier(),
        logger=BotLogger("hedger"),
        dry_run=False,
    )

    leg_a = HedgeLegRequest(DummyExchange("m-a", [0.5, 0.52]), ExchangeName.OPINION, "m-a", weight=1)
    leg_b = HedgeLegRequest(DummyExchange("m-b", [0.51, 0.53]), ExchangeName.OPINION, "m-b", weight=1)

    await hedger.hedge(
        legs=[leg_a, leg_b],
        event_id="evt",
        side=OrderSide.BUY,
        size=100,
        reference_price=0.45,
        entry_order_id="entry-1",
        entry_exchange=ExchangeName.POLYMARKET,
    )

    assert len(leg_a.client.orders) == 1
    assert len(leg_b.client.orders) == 1
    assert hedger.db.trades, "trade not saved"
    assert hedger.db.commit_called and not hedger.db.rollback_called


@pytest.mark.asyncio
async def test_multi_leg_failure_rolls_back_and_incident():
    config = build_config("FULL")
    db = DummyDB()
    notifier = DummyNotifier()
    hedger = Hedger(
        config,
        risk_manager=RiskManager(config, BotLogger("risk")),
        orderbook_manager=OrderbookManager(),
        database=db,
        notifier=notifier,
        logger=BotLogger("hedger"),
        dry_run=False,
    )

    class FailingExchange(DummyExchange):
        async def place_market_order(self, *args, **kwargs):
            raise HedgingError("execution rejected")

    leg_good = HedgeLegRequest(DummyExchange("m-good", [0.5]), ExchangeName.OPINION, "m-good", weight=1)
    leg_bad = HedgeLegRequest(FailingExchange("m-bad", [0.5]), ExchangeName.OPINION, "m-bad", weight=1)

    with pytest.raises(HedgingError):
        await hedger.hedge(
            legs=[leg_good, leg_bad],
            event_id="evt",
            side=OrderSide.BUY,
            size=100,
            reference_price=0.45,
            entry_order_id="entry-2",
            entry_exchange=ExchangeName.POLYMARKET,
        )

    assert db.rollback_called, "transaction should rollback on failure"
    assert db.incidents, "incident should be recorded"
    assert notifier.messages, "failure notification expected"

