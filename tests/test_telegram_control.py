from __future__ import annotations

import asyncio
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import aiosqlite
import pytest

from core.healthcheck import HealthcheckService
from core.models import ExchangeName
from core.spread_analyzer import SpreadAnalyzer
from exchanges.orderbook_manager import OrderbookManager
from telegram.commands import TelegramCommandRouter
from utils.config_loader import (
    DatabaseConfig,
    ExchangeConnectivity,
    ExchangeRoutingConfig,
    FeeConfig,
    GoogleSheetsConfig,
    MarketHedgeConfig,
    MarketPairConfig,
    RateLimitConfig,
    Settings,
    TelegramConfig,
    WebhookConfig,
)
from utils.db import Database
from utils.db_migrations import apply_migrations
from utils.logger import BotLogger


class DummyNotifier:
    def __init__(self):
        self.enabled = True
        self.sent: list[tuple[str | None, str]] = []

    async def send_message(self, msg: str, chat_id: str | None = None) -> bool:  # noqa: D401
        self.sent.append((chat_id, msg))
        return True


class DummyPairController:
    def __init__(self, pairs):
        self._pairs = pairs

    async def list_pairs(self):
        return self._pairs

    async def snapshot(self):
        return {"count": len(self._pairs), "pairs": []}


class DummyReconciler:
    def __init__(self):
        self.metrics = {"processed": 0, "duplicates": 0, "poll_events": 0}


class DummyDB:
    def __init__(self):
        self.records = []

    async def record_simulated_run(self, pair_id: str, size: float, plan, expected_pnl, notes=None):
        self.records.append({"pair_id": pair_id, "size": size, "plan": plan, "expected_pnl": expected_pnl})
        return f"sim-{len(self.records)}"

    def status_snapshot(self):
        return {"connected": True, "last_write": None, "backend": "sqlite"}


class PassingClient:
    def __init__(self):
        ob = OrderbookManager()
        self.orderbook = ob.build(
            "m1",
            bids=[{"price": 0.61, "size": 100}],
            asks=[{"price": 0.39, "size": 100}],
        )

    async def get_orderbook(self, market_id: str):
        return self.orderbook


class FailingClient:
    async def get_orderbook(self, market_id: str):
        raise RuntimeError("boom")


def _settings() -> Settings:
    return Settings(
        market_hedge_mode=MarketHedgeConfig(
            enabled=True,
            hedge_ratio=1.0,
            max_slippage_market_hedge=0.1,
            min_spread_for_entry=0.0,
            max_position_size_per_market=10,
            max_position_size_per_event=20,
            cancel_unfilled_after_ms=1000,
            allow_partial_fill_hedge=True,
        ),
        double_limit_enabled=True,
        exchanges=ExchangeRoutingConfig(primary=ExchangeName.OPINION, secondary=ExchangeName.POLYMARKET),
        fees={
            ExchangeName.OPINION: FeeConfig(),
            ExchangeName.POLYMARKET: FeeConfig(),
        },
        google_sheets=GoogleSheetsConfig(),
        webhook=WebhookConfig(),
        dry_run=True,
        telegram=TelegramConfig(enabled=True, token="t", chat_id="123", heartbeat_enabled=False, heartbeat_interval_sec=900),
        database=DatabaseConfig(backend="sqlite", dsn="sqlite+aiosqlite:///./data/test.db"),
        rate_limits={},
        market_pairs=[
            MarketPairConfig(
                event_id="pair-1",
                primary_market_id="m1",
                secondary_market_id="m2",
                primary_exchange=ExchangeName.OPINION,
                secondary_exchange=ExchangeName.POLYMARKET,
            )
        ],
        connectivity={
            ExchangeName.OPINION: ExchangeConnectivity(use_websocket=False, poll_interval=1.0),
            ExchangeName.POLYMARKET: ExchangeConnectivity(use_websocket=False, poll_interval=1.0),
        },
        scheduler_policy="round_robin",
    )


@pytest.mark.asyncio
async def test_simulate_command_records_plan():
    settings = _settings()
    pair = settings.market_pairs[0]
    notifier = DummyNotifier()
    db = DummyDB()
    spread_analyzer = SpreadAnalyzer()
    router = TelegramCommandRouter(
        settings=settings,
        pair_controller=DummyPairController([pair]),
        db=db,
        reconciler=DummyReconciler(),
        spread_analyzer=spread_analyzer,
        notifier=notifier,
        healthcheck=None,  # not needed for this test
        account_pools={
            ExchangeName.OPINION: [],
            ExchangeName.POLYMARKET: [],
        },
        clients_by_id={
            "acc-a": PassingClient(),
            "acc-b": PassingClient(),
        },
        account_index={},
        logger=BotLogger("test_simulate"),
    )
    # map pools to clients
    router.account_pools[ExchangeName.OPINION] = [type("A", (), {"account_id": "acc-a", "exchange": ExchangeName.OPINION})()]
    router.account_pools[ExchangeName.POLYMARKET] = [type("B", (), {"account_id": "acc-b", "exchange": ExchangeName.POLYMARKET})()]

    await router.handle_update({"message": {"chat": {"id": "123"}, "text": "/simulate pair-1 2"}})

    assert db.records, "simulation should be recorded"
    assert "/simulate" not in notifier.sent[0][1]  # ensure a summary was sent


@pytest.mark.asyncio
async def test_healthcheck_handles_failures():
    settings = _settings()
    pair = settings.market_pairs[0]
    account_pools = {
        ExchangeName.OPINION: [type("A", (), {"account_id": "acc-a", "exchange": ExchangeName.OPINION})()],
        ExchangeName.POLYMARKET: [type("B", (), {"account_id": "acc-b", "exchange": ExchangeName.POLYMARKET})()],
    }
    clients = {"acc-a": FailingClient(), "acc-b": PassingClient()}
    service = HealthcheckService(
        spread_analyzer=SpreadAnalyzer(),
        orderbook_manager=OrderbookManager(),
        account_pools=account_pools,
        clients_by_id=clients,
        fees=settings.fees,
        logger=BotLogger("healthcheck_test"),
    )

    results = await service.run([pair], size=1.0)
    assert results[0].primary_status == "FAIL"
    assert results[0].secondary_status in {"OK", "FAIL"}


@pytest.mark.asyncio
async def test_record_simulated_runs_persists(tmp_path):
    db_file = tmp_path / "sim.db"
    config = DatabaseConfig(backend="sqlite", dsn=f"sqlite+aiosqlite:///{db_file}")
    project_root = Path(__file__).resolve().parent.parent
    await apply_migrations(config, base_path=project_root)
    db = Database(config)
    await db.init()
    await db.record_simulated_run("pair-1", 1.0, {"plan": True}, 0.1, notes="test")

    async with aiosqlite.connect(db_file) as conn:
        cursor = await conn.execute("SELECT COUNT(*) FROM simulated_runs")
        count_row = await cursor.fetchone()
        assert count_row[0] == 1
    await db.close()


