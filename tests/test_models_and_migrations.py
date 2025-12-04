from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from pathlib import Path

import aiosqlite
import pytest

from models import canonical
from utils.config_loader import DatabaseConfig
from utils.db import Database
from utils.db_migrations import apply_migrations


@pytest.mark.asyncio
async def test_migrations_and_crud(tmp_path):
    db_file = tmp_path / "test.db"
    config = DatabaseConfig(
        backend="sqlite",
        dsn=f"sqlite+aiosqlite:///{db_file}",
    )
    project_root = Path(__file__).resolve().parent.parent
    await apply_migrations(config, base_path=project_root)

    db = Database(config)
    await db.init()

    order = canonical.Order(
        client_order_id="client-1",
        exchange="Opinion",
        order_id="order-1",
        market_id="m-1",
        side="BUY",
        price=Decimal("0.55"),
        size=Decimal("100"),
        ts=datetime.utcnow(),
    )
    await db.save_order(order)

    fill = canonical.Fill(
        order_id="order-1",
        exchange="Opinion",
        fill_id="fill-1",
        size=Decimal("25"),
        price=Decimal("0.55"),
        side="BUY",
        ts=datetime.utcnow(),
    )
    await db.update_order_fill("order-1", Decimal("25"), fill)

    trade = canonical.Trade(
        entry_order_id="order-1",
        hedge_order_id="order-hedge",
        entry_exchange="Opinion",
        hedge_exchange="Polymarket",
        size=Decimal("25"),
        price_entry=Decimal("0.55"),
        price_hedge=Decimal("0.50"),
        fees=Decimal("0.10"),
        pnl_estimated=Decimal("1.25"),
        ts=datetime.utcnow(),
    )
    await db.save_trade(trade)

    remaining = await db.get_unhedged_size("m-1")
    assert remaining == Decimal("75")

    async with aiosqlite.connect(db_file) as conn:
        cursor = await conn.execute("SELECT filled_size FROM orders WHERE order_id = ?", ("order-1",))
        row = await cursor.fetchone()
        assert Decimal(str(row[0])) == Decimal("25")

        cursor = await conn.execute("SELECT COUNT(*) FROM fills")
        row = await cursor.fetchone()
        assert row[0] == 1

        cursor = await conn.execute("SELECT COUNT(*) FROM trades")
        row = await cursor.fetchone()
        assert row[0] == 1

    await db.close()

