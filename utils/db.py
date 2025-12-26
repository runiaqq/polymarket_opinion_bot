from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, List
from urllib.parse import urlparse

import aiosqlite
import asyncpg

from core.models import (
    DoubleLimitState,
    Order as LegacyOrder,
    OrderStatus,
    Trade as LegacyTrade,
    Fill as LegacyFill,
)
from models import canonical as canon
from utils.config_loader import DatabaseConfig
from utils.logger import BotLogger


class Database:
    """Async persistence interface."""

    def __init__(self, config: DatabaseConfig, logger: BotLogger | None = None):
        self.config = config
        self.logger = logger or BotLogger(__name__)
        self.backend = config.backend.lower()
        self._conn: Optional[aiosqlite.Connection] = None
        self._pool: Optional[asyncpg.Pool] = None
        self._lock = asyncio.Lock()
        self._in_transaction = False
        self.last_write_ts: Optional[datetime] = None
        self.connected: bool = False

    async def init(self) -> None:
        if self.backend.startswith("sqlite"):
            path = self._sqlite_path(self.config.dsn)
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            self._conn = await aiosqlite.connect(path)
            self._conn.row_factory = aiosqlite.Row
        elif self.backend in {"postgres", "postgresql"}:
            self._pool = await asyncpg.create_pool(self.config.dsn)
        else:
            raise ValueError(f"Unsupported database backend {self.backend}")
        self.logger.info("database initialized", backend=self.backend)
        self.connected = True

    async def close(self) -> None:
        if self._conn:
            await self._conn.close()
        if self._pool:
            await self._pool.close()

    async def save_order(self, order: canon.Order | LegacyOrder) -> None:
        order = _coerce_order(order)
        sql = """
        INSERT INTO orders (
            client_order_id, exchange, order_id, market_id, side,
            price, size, filled_size, status, ts
        ) VALUES (
            :client_order_id, :exchange, :order_id, :market_id, :side,
            :price, :size, :filled_size, :status, :ts
        )
        """
        await self._execute(
            sql,
            {
                "client_order_id": order.client_order_id,
                "exchange": order.exchange,
                "order_id": order.order_id,
                "market_id": order.market_id,
                "side": order.side,
                "price": _decimal_or_none(order.price),
                "size": str(order.size),
                "filled_size": str(order.filled_size),
                "status": order.status,
                "ts": order.ts.isoformat(),
            },
        )

    async def log_order_event(
        self,
        order_id: str,
        stage: str,
        payload: Dict[str, Any] | None = None,
    ) -> None:
        payload = payload or {}
        await self._execute(
            """
            INSERT INTO order_events (order_id, stage, payload)
            VALUES (:order_id, :stage, :payload)
            """,
            {
                "order_id": order_id,
                "stage": stage,
                "payload": json.dumps(payload, default=str),
            },
        )
    async def save_double_limit_pair(
        self,
        record_id: str,
        pair_key: str,
        primary_order_ref: str,
        secondary_order_ref: str,
        primary_exchange: str,
        secondary_exchange: str,
        primary_client_order_id: str,
        secondary_client_order_id: str,
        state: DoubleLimitState = DoubleLimitState.ACTIVE,
    ) -> None:
        now = datetime.now(tz=timezone.utc).isoformat()
        sql = """
        INSERT INTO double_limits (
            id,
            pair_key,
            order_a_ref,
            order_b_ref,
            order_a_exchange,
            order_b_exchange,
            client_order_id_a,
            client_order_id_b,
            state,
            created_at,
            updated_at
        ) VALUES (
            :id,
            :pair_key,
            :order_a_ref,
            :order_b_ref,
            :order_a_exchange,
            :order_b_exchange,
            :client_order_id_a,
            :client_order_id_b,
            :state,
            :created_at,
            :updated_at
        )
        """
        await self._execute(
            sql,
            {
                "id": record_id,
                "pair_key": pair_key,
                "order_a_ref": primary_order_ref,
                "order_b_ref": secondary_order_ref,
                "order_a_exchange": primary_exchange,
                "order_b_exchange": secondary_exchange,
                "client_order_id_a": primary_client_order_id,
                "client_order_id_b": secondary_client_order_id,
                "state": state.value,
                "created_at": now,
                "updated_at": now,
            },
        )

    async def get_double_limit_by_order(self, order_ref: str) -> Optional[Dict[str, Any]]:
        row = await self._fetchone(
            """
            SELECT *
            FROM double_limits
            WHERE order_a_ref = :order_ref
               OR order_b_ref = :order_ref
               OR client_order_id_a = :order_ref
               OR client_order_id_b = :order_ref
            LIMIT 1
            """,
            {"order_ref": order_ref},
        )
        return row

    async def update_double_limit_state(
        self,
        record_id: str,
        state: DoubleLimitState,
        triggered_order_id: Optional[str] = None,
        cancelled_order_id: Optional[str] = None,
    ) -> None:
        await self._execute(
            """
            UPDATE double_limits
            SET
                state = :state,
                triggered_order_id = CASE
                    WHEN :triggered_order_id IS NOT NULL THEN :triggered_order_id
                    ELSE triggered_order_id
                END,
                cancelled_order_id = CASE
                    WHEN :cancelled_order_id IS NOT NULL THEN :cancelled_order_id
                    ELSE cancelled_order_id
                END,
                updated_at = :updated_at
            WHERE id = :id
            """,
            {
                "id": record_id,
                "state": state.value,
                "triggered_order_id": triggered_order_id,
                "cancelled_order_id": cancelled_order_id,
                "updated_at": datetime.now(tz=timezone.utc).isoformat(),
            },
        )

    async def update_order_fill(
        self,
        order_id: str,
        filled_increment: Decimal,
        fill_record: canon.Fill | LegacyFill,
    ) -> None:
        fill = _coerce_fill(fill_record)
        await self._execute(
            """
            UPDATE orders
            SET filled_size = filled_size + :inc
            WHERE (order_id = :order_id OR client_order_id = :order_id)
            """,
            {"inc": str(filled_increment), "order_id": order_id},
        )
        await self._execute(
            """
            INSERT INTO fills (
                order_id, exchange, fill_id, size, price, side, ts
            ) VALUES (
                :order_id, :exchange, :fill_id, :size, :price, :side, :ts
            )
            """,
            {
                "order_id": fill.order_id,
                "exchange": fill.exchange,
                "fill_id": fill.fill_id,
                "size": str(fill.size),
                "price": str(fill.price),
                "side": fill.side,
                "ts": fill.ts.isoformat(),
            },
        )

    async def begin_transaction(self):
        if self.backend.startswith("sqlite"):
            assert self._conn is not None
            await self._conn.execute("BEGIN")
            self._in_transaction = True
            return None
        else:
            assert self._pool is not None
            conn = await self._pool.acquire()
            await conn.execute("BEGIN")
            return conn

    async def commit_transaction(self, conn=None):
        if self.backend.startswith("sqlite"):
            assert self._conn is not None
            await self._conn.commit()
            self._in_transaction = False
        else:
            assert conn is not None
            await conn.execute("COMMIT")
            await self._pool.release(conn)

    async def rollback_transaction(self, conn=None):
        if self.backend.startswith("sqlite"):
            assert self._conn is not None
            await self._conn.rollback()
            self._in_transaction = False
        else:
            assert conn is not None
            await conn.execute("ROLLBACK")
            await self._pool.release(conn)

    async def save_trade(self, trade: canon.Trade | LegacyTrade, tx_conn=None) -> None:
        trade = _coerce_trade(trade)
        sql = """
        INSERT INTO trades (
            entry_order_id, hedge_order_id, entry_exchange, hedge_exchange,
            size, price_entry, price_hedge, fees, pnl_estimated, ts
        ) VALUES (
            :entry_order_id, :hedge_order_id, :entry_exchange, :hedge_exchange,
            :size, :price_entry, :price_hedge, :fees, :pnl_estimated, :ts
        )
        """
        params = {
            "entry_order_id": trade.entry_order_id,
            "hedge_order_id": trade.hedge_order_id,
            "entry_exchange": trade.entry_exchange,
            "hedge_exchange": trade.hedge_exchange,
            "size": str(trade.size),
            "price_entry": str(trade.price_entry),
            "price_hedge": str(trade.price_hedge),
            "fees": str(trade.fees),
            "pnl_estimated": str(trade.pnl_estimated),
            "ts": trade.ts.isoformat(),
        }
        if tx_conn:
            await tx_conn.execute(sql, params)
        else:
            await self._execute(sql, params)

    async def get_unhedged_size(self, market_id: str) -> Decimal:
        row = await self._fetchone(
            "SELECT COALESCE(SUM(size - filled_size), 0) AS remaining FROM orders WHERE market_id = :market_id",
            {"market_id": market_id},
        )
        return Decimal(str(row["remaining"])) if row else Decimal("0")

    async def fill_exists(self, order_id: str, ts: datetime) -> bool:
        row = await self._fetchone(
            "SELECT 1 FROM fills WHERE order_id=:order_id AND ts=:ts LIMIT 1",
            {"order_id": order_id, "ts": ts.isoformat()},
        )
        return row is not None

    async def list_fill_records(self):
        return await self._fetchall("SELECT order_id, fill_id, ts, size FROM fills", {})

    async def update_order_status(self, order_id: str, status: OrderStatus) -> None:
        await self._execute(
            "UPDATE orders SET status=:status WHERE order_id=:order_id OR client_order_id=:order_id",
            {"status": status.value, "order_id": order_id},
        )

    async def record_incident(self, level: str, message: str, details: Dict[str, Any]) -> None:
        await self._execute(
            """
            INSERT INTO incidents (level, message, details)
            VALUES (:level, :message, :details)
            """,
            {"level": level, "message": message, "details": json.dumps(details)},
        )

    async def record_simulated_run(
        self,
        pair_id: str,
        size: float,
        plan: Dict[str, Any],
        expected_pnl: Optional[float],
        notes: str | None = None,
    ) -> str:
        record_id = uuid.uuid4().hex
        await self._execute(
            """
            INSERT INTO simulated_runs (id, ts, pair_id, size, plan_json, expected_pnl, notes)
            VALUES (:id, :ts, :pair_id, :size, :plan_json, :expected_pnl, :notes)
            """,
            {
                "id": record_id,
                "ts": datetime.now(tz=timezone.utc).isoformat(),
                "pair_id": pair_id,
                "size": size,
                "plan_json": json.dumps(plan, default=str),
                "expected_pnl": expected_pnl,
                "notes": notes,
            },
        )
        return record_id

    def status_snapshot(self) -> Dict[str, Any]:
        return {
            "backend": self.backend,
            "connected": self.connected,
            "last_write": self.last_write_ts.isoformat() if self.last_write_ts else None,
        }

    async def fetch_fill_keys(self) -> set[str]:
        rows = await self._fetchall(
            """
            SELECT exchange, COALESCE(fill_id, order_id) AS key_part, ts
            FROM fills
            """,
            {},
        )
        keys: set[str] = set()
        for row in rows:
            exchange = row.get("exchange") or ""
            key_part = row.get("key_part") or ""
            ts = row.get("ts") or ""
            keys.add(f"{exchange}:{key_part}:{ts}")
        return keys

    async def _execute(self, sql: str, params: Dict[str, Any]) -> None:
        async with self._lock:
            if self.backend.startswith("sqlite"):
                assert self._conn is not None
                await self._conn.execute(sql, params)
                if not self._in_transaction:
                    await self._conn.commit()
                self.last_write_ts = datetime.now(tz=timezone.utc)
            else:
                assert self._pool is not None
                formatted, values = self._format_pg(sql, params)
                async with self._pool.acquire() as conn:
                    await conn.execute(formatted, *values)
                self.last_write_ts = datetime.now(tz=timezone.utc)

    async def _fetchone(self, sql: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        async with self._lock:
            if self.backend.startswith("sqlite"):
                assert self._conn is not None
                cursor = await self._conn.execute(sql, params)
                row = await cursor.fetchone()
                return dict(row) if row else None
            else:
                assert self._pool is not None
                formatted, values = self._format_pg(sql, params)
                async with self._pool.acquire() as conn:
                    row = await conn.fetchrow(formatted, *values)
                    return dict(row) if row else None

    async def _fetchall(self, sql: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        async with self._lock:
            if self.backend.startswith("sqlite"):
                assert self._conn is not None
                cursor = await self._conn.execute(sql, params)
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
            else:
                assert self._pool is not None
                formatted, values = self._format_pg(sql, params)
                async with self._pool.acquire() as conn:
                    rows = await conn.fetch(formatted, *values)
                    return [dict(row) for row in rows]

    def _format_pg(self, sql: str, params: Dict[str, Any]) -> Tuple[str, Tuple[Any, ...]]:
        mapping = []
        formatted = ""
        idx = 1
        i = 0
        while i < len(sql):
            if sql[i] == ":":
                j = i + 1
                while j < len(sql) and (sql[j].isalnum() or sql[j] == "_"):
                    j += 1
                key = sql[i + 1 : j]
                formatted += f"${idx}"
                mapping.append(key)
                idx += 1
                i = j
            else:
                formatted += sql[i]
                i += 1
        values = tuple(params.get(key) for key in mapping)
        return formatted, values

    def _sqlite_path(self, dsn: str) -> str:
        if dsn.startswith("sqlite"):
            parsed = urlparse(dsn)
            path = parsed.path or "market_hedge.db"
            if path.startswith("/") and len(path) > 2 and path[2] == ":":
                path = path[1:]
            return path
        return dsn


def _coerce_order(order: canon.Order | LegacyOrder) -> canon.Order:
    if isinstance(order, canon.Order):
        return order
    return canon.Order(
        client_order_id=order.client_order_id,
        exchange=order.exchange.value,
        order_id=order.order_id,
        market_id=order.market_id,
        side=order.side.value,
        price=Decimal(str(order.price)) if order.price is not None else None,
        size=Decimal(str(order.size)),
        filled_size=Decimal(str(order.filled_size)),
        status=order.status.value,
        ts=order.created_at,
    )


def _coerce_fill(fill: canon.Fill | LegacyFill) -> canon.Fill:
    if isinstance(fill, canon.Fill):
        return fill
    return canon.Fill(
        order_id=fill.order_id,
        exchange=fill.exchange.value,
        fill_id=None,
        size=Decimal(str(fill.size)),
        price=Decimal(str(fill.price)),
        side=fill.side.value,
        ts=fill.timestamp,
    )


def _coerce_trade(trade: canon.Trade | LegacyTrade) -> canon.Trade:
    if isinstance(trade, canon.Trade):
        return trade
    return canon.Trade(
        entry_order_id=trade.entry_order_id,
        hedge_order_id=trade.hedge_order_id,
        entry_exchange=trade.entry_exchange.value,
        hedge_exchange=trade.hedge_exchange.value,
        size=Decimal(str(trade.size)),
        price_entry=Decimal(str(trade.entry_price)),
        price_hedge=Decimal(str(trade.hedge_price)),
        fees=Decimal("0"),
        pnl_estimated=Decimal(str(trade.pnl_estimate)),
        ts=trade.timestamp,
    )


def _decimal_or_none(value: Optional[Decimal]) -> Optional[str]:
    if value is None:
        return None
    return str(value)

