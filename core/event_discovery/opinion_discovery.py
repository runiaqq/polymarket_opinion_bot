from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import aiohttp

from . import DiscoveredEvent, SOURCE_OPINION


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
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


class OpinionDiscovery:
    """Fetch activated Opinion markets via the OpenAPI endpoint."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        api_key: str,
        logger=None,
        base_url: str = "https://openapi.opinion.trade/openapi/market",
        proxy: str | None = None,
        page_size: int = 50,
    ):
        self.session = session
        self.api_key = api_key
        self.logger = logger
        self.base_url = base_url
        self.proxy = proxy
        self.page_size = min(100, max(1, page_size))

    async def discover(self) -> List[DiscoveredEvent]:
        page = 1
        results: List[DiscoveredEvent] = []
        while True:
            markets = await self._fetch_page(page)
            if not markets:
                break
            for market in markets:
                if not self._is_active(market):
                    continue
                results.append(self._build_event(market))
            if len(markets) < self.page_size:
                break
            page += 1
        return results

    async def _fetch_page(self, page: int) -> List[Dict[str, Any]]:
        params = {"page": page, "limit": self.page_size, "status": "activated", "marketType": 2}
        headers = {"apikey": self.api_key}
        async with self.session.get(self.base_url, headers=headers, params=params, proxy=self.proxy, timeout=30) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise RuntimeError(f"opinion discovery failed ({resp.status}): {text}")
            payload = await resp.json()
        result = payload.get("result") or payload.get("data") or {}
        return result.get("list", []) if isinstance(result, dict) else []

    def _is_active(self, market: Dict[str, Any]) -> bool:
        status = str(market.get("statusEnum") or market.get("status") or "").lower()
        return status == "activated"

    def _build_event(self, market: Dict[str, Any]) -> DiscoveredEvent:
        yes_token = market.get("yesTokenId") or market.get("yes_token_id")
        no_token = market.get("noTokenId") or market.get("no_token_id")
        contract_type = "categorical" if market.get("childMarkets") else "binary"
        end_time = _parse_datetime(market.get("expireTime") or market.get("endTime") or market.get("settleTime"))
        metadata = {
            "child_markets": market.get("childMarkets") or [],
            "volume": market.get("volume"),
        }
        return DiscoveredEvent(
            source=SOURCE_OPINION,
            event_id=str(market.get("marketId") or market.get("market_id")),
            title=market.get("marketTitle") or market.get("topic_title") or "",
            description=market.get("description"),
            end_time=end_time,
            contract_type=contract_type,
            yes_token_id=yes_token,
            no_token_id=no_token,
            metadata=metadata,
        )


__all__ = ["OpinionDiscovery"]

