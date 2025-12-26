from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

import aiohttp

from utils.polymarket_discovery import check_clob_orderbook, extract_token_ids, paginate_markets
from . import DiscoveredEvent, SOURCE_POLYMARKET


def _parse_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except (OverflowError, ValueError):
            return None
    if isinstance(value, str):
        try:
            normalized = value.replace("Z", "+00:00")
            return datetime.fromisoformat(normalized)
        except ValueError:
            return None
    return None


class PolymarketDiscovery:
    """Fetches and validates Polymarket events against CLOB availability."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        logger=None,
        gamma_url: str = "https://gamma-api.polymarket.com",
        clob_url: str = "https://clob.polymarket.com",
        proxy: str | None = None,
        max_pages: int | None = None,
        concurrency: int = 10,
    ):
        self.session = session
        self.logger = logger
        self.gamma_url = gamma_url.rstrip("/")
        self.clob_url = clob_url.rstrip("/")
        self.proxy = proxy
        self.max_pages = max_pages
        self.concurrency = max(1, concurrency)

    async def discover(self) -> List[DiscoveredEvent]:
        markets = await paginate_markets(
            session=self.session,
            proxy=self.proxy,
            base_url=self.gamma_url,
            page_size=100,
            max_pages=self.max_pages,
            extra_params={"closed": "false"},
        )
        results: List[DiscoveredEvent] = []
        sem = asyncio.Semaphore(self.concurrency)

        async def _handle(market: Dict[str, Any]) -> None:
            if not self._is_candidate(market):
                return
            token_ids = extract_token_ids(market)
            if not token_ids:
                return
            validated_token = await self._validate_tokens(token_ids, sem)
            if not validated_token:
                return
            results.append(self._build_event(market, token_ids))

        await asyncio.gather(*(_handle(mkt) for mkt in markets))
        return results

    def _is_candidate(self, market: Dict[str, Any]) -> bool:
        status = str(market.get("status") or market.get("state") or "").lower()
        if status in {"resolved", "closed", "settled"}:
            return False
        if market.get("resolved") or market.get("closed"):
            return False
        if market.get("paused") or market.get("freeze"):
            return False
        if str(market.get("active", True)).lower() in {"false", "0"}:
            return False
        if str(market.get("acceptingOrders", True)).lower() in {"false", "0"}:
            return False
        tokens = extract_token_ids(market)
        return bool(tokens)

    async def _validate_tokens(self, token_ids: Sequence[str], sem: asyncio.Semaphore) -> Optional[str]:
        for token_id in token_ids:
            async with sem:
                ok, status = await check_clob_orderbook(
                    self.session, token_id, proxy=self.proxy, base_url=self.clob_url
                )
            if ok:
                return token_id
            if self.logger:
                self.logger.debug("polymarket token skipped", token_id=token_id, status=status)
        return None

    def _parse_end_time(self, market: Dict[str, Any]) -> Optional[datetime]:
        candidates = [
            market.get("endDate"),
            market.get("endDateIso"),
            market.get("closeDate"),
            market.get("closeTime"),
            market.get("resolutionTime"),
            market.get("resolvedTime"),
        ]
        for candidate in candidates:
            parsed = _parse_datetime(candidate)
            if parsed:
                return parsed
        return None

    def _build_event(self, market: Dict[str, Any], token_ids: Sequence[str]) -> DiscoveredEvent:
        contract_type = "categorical" if len(token_ids) > 2 else "binary"
        yes_token_id = token_ids[0] if token_ids else None
        no_token_id = token_ids[1] if len(token_ids) > 1 else None
        end_time = self._parse_end_time(market)
        metadata = {
            "slug": market.get("slug") or market.get("marketSlug"),
            "condition_id": market.get("conditionId") or market.get("condition_id"),
            "category": market.get("category"),
            "volume": market.get("volume"),
            "clob_token_ids": list(token_ids),
        }
        description = market.get("description") or market.get("question")
        return DiscoveredEvent(
            source=SOURCE_POLYMARKET,
            event_id=str(market.get("id") or market.get("market_id")),
            title=market.get("question") or market.get("title") or "",
            description=description,
            end_time=end_time,
            contract_type=contract_type,
            yes_token_id=yes_token_id,
            no_token_id=no_token_id,
            metadata=metadata,
        )


__all__ = ["PolymarketDiscovery"]

