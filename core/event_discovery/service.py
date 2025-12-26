from __future__ import annotations

import asyncio
from contextlib import suppress
from typing import Awaitable, Callable, Iterable, List, Optional

import aiohttp

from . import DiscoveredEvent, MatchedEventPair, SOURCE_OPINION, SOURCE_POLYMARKET
from .matcher import match_events
from .filters import apply_filters
from .normalizer import normalize_events
from .opinion_discovery import OpinionDiscovery
from .polymarket_discovery import PolymarketDiscovery
from .registry import EventDiscoveryRegistry
from utils.logger import BotLogger
from utils.config_loader import EventDiscoveryConfig


Fetcher = Callable[[], Awaitable[List[DiscoveredEvent]]]


class EventDiscoveryService:
    """Periodic discovery and matching loop (read-only, dry-run)."""

    def __init__(
        self,
        config: EventDiscoveryConfig,
        registry: EventDiscoveryRegistry,
        logger: BotLogger,
        opinion_api_key: str | None,
        stop_event: asyncio.Event,
        poll_interval_sec: Optional[int] = None,
        polymarket_fetcher: Fetcher | None = None,
        opinion_fetcher: Fetcher | None = None,
        proxy: str | None = None,
    ):
        self.config = config
        self.registry = registry
        self.logger = logger
        self.opinion_api_key = opinion_api_key
        self.stop_event = stop_event
        self.poll_interval = poll_interval_sec or getattr(config, "poll_interval_sec", 300) or 300
        self._task: asyncio.Task | None = None
        self._session: aiohttp.ClientSession | None = None
        self._polymarket_fetcher = polymarket_fetcher
        self._opinion_fetcher = opinion_fetcher
        self.proxy = proxy

    async def start(self) -> None:
        if not self.config.enabled:
            self.logger.info("event discovery disabled in config; skipping start")
            return
        if not self.opinion_api_key:
            self.logger.warn("event discovery disabled: missing Opinion API key")
            return
        if self._task and not self._task.done():
            return
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            with suppress(asyncio.CancelledError):  # type: ignore[name-defined]
                await self._task
        if self._session:
            await self._session.close()

    async def _run_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                await self.run_once()
            except Exception as exc:  # pragma: no cover - defensive
                self.logger.warn("event discovery iteration failed", error=str(exc))
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=self.poll_interval)
            except asyncio.TimeoutError:
                continue

    async def run_once(self) -> None:
        if not self.config.enabled:
            return
        if not self._session or self._session.closed:
            self._session = aiohttp.ClientSession()
        polymarket_events = await self._fetch_polymarket()
        opinion_events = await self._fetch_opinion()
        filtered_pm = apply_filters(polymarket_events, self.config, SOURCE_POLYMARKET)
        filtered_op = apply_filters(opinion_events, self.config, SOURCE_OPINION)
        matches: List[MatchedEventPair] = match_events(filtered_op, filtered_pm, threshold=0.85)
        self.registry.update(filtered_op, filtered_pm, matches)
        summary = self.registry.summary()
        self.logger.info(
            "event discovery updated",
            opinion=len(opinion_events),
            polymarket=len(polymarket_events),
            filtered_op=len(filtered_op),
            filtered_pm=len(filtered_pm),
            matches=len(matches),
        )

    async def _fetch_polymarket(self) -> List[DiscoveredEvent]:
        if self._polymarket_fetcher:
            return await self._polymarket_fetcher()
        assert self._session
        discovery = PolymarketDiscovery(session=self._session, proxy=self.proxy)
        return await discovery.discover()

    async def _fetch_opinion(self) -> List[DiscoveredEvent]:
        if self._opinion_fetcher:
            return await self._opinion_fetcher()
        assert self._session
        discovery = OpinionDiscovery(session=self._session, api_key=self.opinion_api_key or "", proxy=self.proxy)
        return await discovery.discover()


__all__ = ["EventDiscoveryService"]

