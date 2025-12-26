"""
One-off diagnostic to check event discovery health.
Safe: read-only, no trading, no writes to market_pairs.
Run: python scripts/diag_event_discovery.py
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import aiohttp

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from core.event_discovery.approvals import EventApprovalStore
from core.event_discovery.registry import EventDiscoveryRegistry
from core.event_discovery.service import EventDiscoveryService
from core.event_discovery.polymarket_discovery import PolymarketDiscovery
from core.event_discovery.opinion_discovery import OpinionDiscovery
from utils.config_loader import ConfigLoader
from utils.logger import BotLogger


async def main() -> None:
    loader = ConfigLoader()
    settings = loader.load_settings()
    accounts = loader.load_accounts()

    if not settings.event_discovery.enabled:
        print("event discovery disabled in config; enable event_discovery.enabled=true")
        return

    opinion_key = None
    for acc in accounts:
        if acc.exchange.value == "Opinion" and acc.api_key:
            opinion_key = acc.api_key
            break
    if not opinion_key:
        print("missing Opinion API key in accounts.json for exchange=Opinion")
        return

    approvals = EventApprovalStore("data/event_approvals_diag.json")
    registry = EventDiscoveryRegistry(approvals)

    # Use constrained fetchers to keep diagnostics quick.
    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        pm = PolymarketDiscovery(session=session, max_pages=1)
        op = OpinionDiscovery(session=session, api_key=opinion_key)

        async def fetch_pm():
            return await pm.discover()

        async def fetch_op():
            return await op.discover()

        svc = EventDiscoveryService(
            config=settings.event_discovery,
            registry=registry,
            logger=BotLogger("diag_discovery"),
            opinion_api_key=opinion_key,
            stop_event=asyncio.Event(),
            poll_interval_sec=1,
            polymarket_fetcher=fetch_pm,
            opinion_fetcher=fetch_op,
        )

        try:
            await svc.run_once()
        except Exception as exc:  # pragma: no cover - diagnostic
            print("error during discovery:", exc)
        await svc.stop()

    summary = registry.summary()
    print("summary:", summary)
    matches = registry.get_candidates()[:5]
    if not matches:
        print("no matched candidates (try relaxing filters or lower threshold).")
        return
    print("top candidates:")
    for m in matches:
        match_id = registry.match_id(m)
        print(
            f"- {match_id} | score {m.confidence_score:.2f} | "
            f"Opinion: {m.opinion_event.title}  | Polymarket: {m.polymarket_event.title}"
        )


if __name__ == "__main__":
    asyncio.run(main())

