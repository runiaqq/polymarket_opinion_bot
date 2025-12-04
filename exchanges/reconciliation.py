from __future__ import annotations

import asyncio
import contextlib
from typing import Awaitable, Callable, List, Optional, Tuple

from core.models import Fill
from utils.logger import BotLogger


FillDecoder = Callable[[object], Awaitable[Optional[Fill]] | Optional[Fill]]
FillHandler = Callable[[Fill], Awaitable[None]]


class Reconciler:
    """Cross-check websocket and polling feeds, deduplicate fills, and dispatch them."""

    def __init__(self, database, handler: FillHandler, logger: BotLogger | None = None):
        self.db = database
        self.handler = handler
        self.logger = logger or BotLogger(__name__)
        self._ws_sources: List[Tuple[object, FillDecoder]] = []
        self._poll_sources: List[Tuple[object, float]] = []
        self._tasks: List[asyncio.Task] = []
        self._stop = asyncio.Event()
        self._seen: set[str] = set()
        self.metrics = {"ws_events": 0, "poll_events": 0, "duplicates": 0, "processed": 0}

    def subscribe_ws(self, exchange_client, decoder: FillDecoder) -> None:
        self._ws_sources.append((exchange_client, decoder))

    def register_poller(self, exchange_client, interval_seconds: float) -> None:
        self._poll_sources.append((exchange_client, interval_seconds))

    async def start(self) -> None:
        self._seen = await self.db.fetch_fill_keys()
        for client, decoder in self._ws_sources:
            task = asyncio.create_task(self._run_ws(client, decoder))
            self._tasks.append(task)
        for client, interval in self._poll_sources:
            task = asyncio.create_task(self._run_poller(client, interval))
            self._tasks.append(task)

    async def stop(self) -> None:
        self._stop.set()
        for task in self._tasks:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    async def _run_ws(self, client, decoder: FillDecoder) -> None:
        async def handler(message):
            fill = await self._decode(decoder, message)
            await self._process_fill(fill, source="ws")

        await client.listen_fills(handler)

    async def _run_poller(self, client, interval: float) -> None:
        backoff = interval
        since = None
        while not self._stop.is_set():
            try:
                trades = await client.fetch_user_trades(since=since)
                for fill in trades or []:
                    await self._process_fill(fill, source="poll")
                    if fill and fill.timestamp:
                        since = max(since or fill.timestamp.timestamp(), fill.timestamp.timestamp())
                await asyncio.wait_for(self._stop.wait(), timeout=interval)
                backoff = interval
            except asyncio.TimeoutError:
                continue
            except Exception as exc:
                self.logger.warn("poller failure", error=str(exc))
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, interval * 5)

    async def _decode(self, decoder: FillDecoder, payload) -> Optional[Fill]:
        result = decoder(payload)
        if asyncio.iscoroutine(result):
            result = await result
        return result

    async def _process_fill(self, fill: Optional[Fill], source: str) -> None:
        if fill is None:
            return
        key = self._fill_key(fill)
        if key in self._seen:
            self.metrics["duplicates"] += 1
            return
        self._seen.add(key)
        self.metrics["processed"] += 1
        await self.handler(fill)

    def _fill_key(self, fill: Fill) -> str:
        ts = fill.timestamp.isoformat() if fill.timestamp else ""
        key_part = getattr(fill, "fill_id", None) or fill.order_id
        return f"{fill.exchange.value}:{key_part}:{ts}"

