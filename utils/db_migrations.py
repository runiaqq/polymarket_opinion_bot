from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Iterable, List
from urllib.parse import urlparse

import aiosqlite
import asyncpg

from utils.config_loader import DatabaseConfig
from utils.logger import BotLogger


async def apply_migrations(
    config: DatabaseConfig,
    base_path: Path | None = None,
    logger: BotLogger | None = None,
) -> None:
    logger = logger or BotLogger("migrations")
    root = base_path or Path(__file__).resolve().parent.parent
    backend = config.backend.lower()
    if backend.startswith("sqlite"):
        await _apply_sqlite(config, root / "migrations" / "sqlite", logger)
    elif backend in {"postgres", "postgresql"}:
        await _apply_postgres(config, root / "migrations" / "postgres", logger)
    else:
        logger.warn("no migrations applied for backend", backend=backend)


async def _apply_sqlite(config: DatabaseConfig, path: Path, logger: BotLogger) -> None:
    db_path = _sqlite_path(config.dsn)
    if not path.exists():
        logger.warn("sqlite migrations path missing", path=str(path))
        return
    async with aiosqlite.connect(db_path) as conn:
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations (version TEXT PRIMARY KEY, applied_at TEXT NOT NULL)"
        )
        applied = await _fetch_applied_sqlite(conn)
        for file in _ordered_sql(path):
            version = file.stem
            if version in applied:
                continue
            sql = file.read_text(encoding="utf-8")
            await conn.executescript(sql)
            await conn.execute(
                "INSERT INTO schema_migrations(version, applied_at) VALUES (?, datetime('now'))",
                (version,),
            )
        await conn.commit()
    logger.info("sqlite migrations applied", path=str(path))


async def _apply_postgres(config: DatabaseConfig, path: Path, logger: BotLogger) -> None:
    if not path.exists():
        logger.warn("postgres migrations path missing", path=str(path))
        return
    conn = await asyncpg.connect(config.dsn)
    try:
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations (version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW())"
        )
        applied_rows = await conn.fetch("SELECT version FROM schema_migrations")
        applied = {row["version"] for row in applied_rows}
        for file in _ordered_sql(path):
            version = file.stem
            if version in applied:
                continue
            sql = file.read_text(encoding="utf-8")
            for statement in _split_statements(sql):
                if statement.strip():
                    await conn.execute(statement)
            await conn.execute(
                "INSERT INTO schema_migrations(version) VALUES($1)",
                version,
            )
    finally:
        await conn.close()
    logger.info("postgres migrations applied", path=str(path))


async def _fetch_applied_sqlite(conn: aiosqlite.Connection) -> set[str]:
    cursor = await conn.execute("SELECT version FROM schema_migrations")
    rows = await cursor.fetchall()
    return {row[0] for row in rows}


def _ordered_sql(path: Path) -> List[Path]:
    return sorted([p for p in path.glob("*.sql") if p.is_file()], key=lambda p: p.name)


def _split_statements(sql: str) -> Iterable[str]:
    buffer: List[str] = []
    for line in sql.splitlines():
        buffer.append(line)
        if line.strip().endswith(";"):
            yield "\n".join(buffer).strip()
            buffer = []
    if buffer:
        yield "\n".join(buffer).strip()


def _sqlite_path(dsn: str) -> str:
    if dsn.startswith("sqlite"):
        parsed = urlparse(dsn)
        path = parsed.path or "market_hedge.db"
        if path.startswith("/") and len(path) > 2 and path[2] == ":":
            path = path[1:]
        return path
    return dsn

