from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest

from core.models import ExchangeName, Fill, OrderSide
from exchanges.reconciliation import Reconciler


class DummyDB:
    async def fetch_fill_keys(self):
        return set()


class DummyWSClient:
    def __init__(self, events):
        self.events = events

    async def listen_fills(self, handler):
        for event in self.events:
            await handler(event)


class DummyPollClient:
    def __init__(self, batches):
        self.batches = batches

    async def fetch_user_trades(self, since=None):
        return self.batches.pop(0) if self.batches else []


def build_fill(order_id: str, exchange: ExchangeName) -> Fill:
    return Fill(
        order_id=order_id,
        market_id="m-1",
        exchange=exchange,
        side=OrderSide.BUY,
        price=0.5,
        size=10,
        fee=0.0,
        timestamp=datetime.now(tz=timezone.utc),
    )


@pytest.mark.asyncio
async def test_reconciler_deduplicates_ws_and_poll():
    fill = build_fill("ord-1", ExchangeName.OPINION)
    processed = []

    async def consumer(f):
        processed.append(f.order_id)

    reconciler = Reconciler(DummyDB(), consumer)
    ws_client = DummyWSClient([fill])
    poll_client = DummyPollClient([[fill]])

    reconciler.subscribe_ws(ws_client, lambda message: message)
    reconciler.register_poller(poll_client, 0.05)

    await reconciler.start()
    await asyncio.sleep(0.1)
    await reconciler.stop()

    assert processed == ["ord-1"]


@pytest.mark.asyncio
async def test_reconciler_skips_duplicates():
    fill = build_fill("dup", ExchangeName.OPINION)
    processed = []

    async def consumer(f):
        processed.append(f.order_id)

    reconciler = Reconciler(DummyDB(), consumer)
    ws_client = DummyWSClient([fill, fill])

    reconciler.subscribe_ws(ws_client, lambda message: message)
    await reconciler.start()
    await asyncio.sleep(0.01)
    await reconciler.stop()

    assert processed == ["dup"]
