from __future__ import annotations

import asyncio
import json
import random
from typing import Awaitable, Callable, List, Optional

import aiohttp

from utils.logger import BotLogger


class WebSocketManager:
    """Reliable websocket helper with exponential backoff and reconnection."""

    def __init__(
        self,
        url: str,
        session: aiohttp.ClientSession,
        logger: BotLogger | None = None,
        proxy: str | None = None,
        ping_interval: float = 20.0,
        max_retries: int = 5,
    ):
        self.url = url
        self.session = session
        self.proxy = proxy
        self.logger = logger or BotLogger(__name__)
        self.ping_interval = ping_interval
        self.max_retries = max_retries
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._subscriptions: List[dict] = []
        self._handler: Optional[Callable[[dict], Awaitable[None]]] = None
        self._running = False
        self._closing = False

    def set_handler(self, handler: Callable[[dict], Awaitable[None]]) -> None:
        self._handler = handler

    async def connect(self) -> None:
        backoff = 1.0
        attempt = 0
        while not self._closing:
            try:
                self._ws = await self.session.ws_connect(self.url, proxy=self.proxy)
                self.logger.info("websocket connected", url=self.url)
                for payload in self._subscriptions:
                    await self.subscribe(payload)
                return
            except Exception as exc:  # pragma: no cover - connection errors
                attempt += 1
                self.logger.warn(
                    "websocket connect failed",
                    url=self.url,
                    attempt=attempt,
                    error=str(exc),
                )
                if attempt >= self.max_retries:
                    raise
                await asyncio.sleep(backoff + random.random())
                backoff = min(backoff * 2, 30)

    async def subscribe(self, payload: dict) -> None:
        if payload not in self._subscriptions:
            self._subscriptions.append(payload)
        if self._ws is None:
            return
        await self._ws.send_json(payload)
        self.logger.debug("websocket subscribed", payload=json.dumps(payload))

    async def listen(self) -> None:
        if self._running:
            return
        self._running = True
        while not self._closing:
            if not self._ws or self._ws.closed:
                await self.connect()
            try:
                msg = await self._ws.receive(timeout=self.ping_interval)
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    if self._handler:
                        await self._handler(data)
                elif msg.type == aiohttp.WSMsgType.PONG:
                    continue
                elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED):
                    await self._ws.close()
                    self._ws = None
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    raise msg.data
            except asyncio.TimeoutError:
                if self._ws:
                    await self._ws.ping()
            except Exception as exc:  # pragma: no cover - network errors
                self.logger.warn("websocket listen error", error=str(exc))
                await asyncio.sleep(2)
                self._ws = None
        self._running = False

    async def close(self) -> None:
        self._closing = True
        if self._ws and not self._ws.closed:
            await self._ws.close()

