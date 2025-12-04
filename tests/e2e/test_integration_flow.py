import asyncio
from datetime import datetime, timezone

import pytest

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from core.models import ExchangeName
from tests.e2e.conftest import MockExchangeClient, DummyDB, DummyTracker, DummyRisk, DummyHedger
from core.order_manager import OrderManager


@pytest.mark.asyncio
async def test_partial_fill_triggers_hedge(monkeypatch):
    fills = asyncio.Queue()
    await fills.put(
        {
            "order_id": "order-1",
            "market_id": "poly-market",
            "token_id": "poly-market",
            "side": "BUY",
            "price": 0.5,
            "filled_size": 10,
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        }
    )
    poly = MockExchangeClient(fills=fills)
    opinion = MockExchangeClient(fills=asyncio.Queue())

    hedges = []

    class DummyHedger:
        async def hedge(self, legs, **kwargs):
            hedges.append((legs, kwargs))

    manager = OrderManager(
        {
            ExchangeName.POLYMARKET: poly,
            ExchangeName.OPINION: opinion,
        },
        database=DummyDB(),
        position_tracker=DummyTracker(),
        hedger=DummyHedger(),
        risk_manager=DummyRisk(),
        dry_run=True,
        event_id="evt",
        market_map={
            ExchangeName.POLYMARKET: "poly-market",
            ExchangeName.OPINION: "opinion-market",
        },
    )
    manager.set_routing(ExchangeName.POLYMARKET, ExchangeName.OPINION)

    payload = {
        "data": {
            "order_id": "order-1",
            "market_id": "poly-market",
            "side": "BUY",
            "price": 0.5,
            "filled_size": 10,
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        }
    }
    fill = manager.normalize_fill(ExchangeName.POLYMARKET, payload)
    await manager.handle_fill(ExchangeName.POLYMARKET, fill)
    assert hedges, "hedge not triggered"

