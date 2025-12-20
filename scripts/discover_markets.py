#!/usr/bin/env python
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Dict, List

from aiohttp import ClientResponseError, ContentTypeError

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.models import ExchangeName
from exchanges.opinion_api import OpinionAPI
from exchanges.polymarket_api import PolymarketAPI
from exchanges.rate_limiter import RateLimiter
from utils.config_loader import ConfigLoader, RateLimitConfig
from utils.logger import BotLogger
from utils.proxy_handler import ProxyHandler


async def build_client(account, session, rate_cfg: RateLimitConfig, logger: BotLogger):
    limiter = RateLimiter(
        requests_per_minute=rate_cfg.requests_per_minute,
        burst=rate_cfg.burst,
    )
    if account.exchange == ExchangeName.POLYMARKET:
        return PolymarketAPI(
            session=session,
            api_key=account.api_key,
            secret=account.secret_key,
            passphrase=account.passphrase or account.metadata.get("passphrase"),
            wallet_address=account.wallet_address or account.metadata.get("wallet_address"),
            rate_limit=limiter,
            logger=logger,
            proxy=account.proxy,
        )
    return OpinionAPI(
        session=session,
        api_key=account.api_key,
        secret=account.secret_key,
        rate_limit=limiter,
        logger=logger,
        proxy=account.proxy,
    )


def _normalize_title(title: str) -> str:
    return " ".join(title.lower().split())


async def _fetch_opinion_markets(client: OpinionAPI, limit: int) -> List[Dict[str, str]]:
    try:
        markets = await client.get_markets()
    except Exception as exc:
        print(f"[opinion] discovery failed: {exc}")
        return []
    rows = []
    for m in markets[:limit]:
        rows.append(
            {
                "exchange": "Opinion",
                "market_id": m.market_id,
                "title": m.name,
                "status": m.status,
                "normalized_title": _normalize_title(m.name or ""),
            }
        )
    return rows


async def _fetch_polymarket_markets(client: PolymarketAPI, limit: int) -> List[Dict[str, str]]:
    try:
        markets = await client.fetch_markets()
    except Exception as exc:
        print(f"[polymarket] discovery failed: {exc}")
        return []
    rows = []
    for m in markets[:limit]:
        rows.append(
            {
                "exchange": "Polymarket",
                "market_id": m.market_id,
                "title": m.name,
                "status": m.status,
                "normalized_title": _normalize_title(m.name or ""),
            }
        )
    return rows


async def discover(limit: int) -> Dict[str, List[Dict[str, str]]]:
    loader = ConfigLoader()
    settings = loader.load_settings()
    accounts = loader.load_accounts()
    logger = BotLogger("discover")
    proxy_handler = ProxyHandler(logger)

    clients = {}
    for acc in accounts:
        session = await proxy_handler.get_session(acc)
        rate_cfg = settings.rate_limits.get(
            acc.exchange.value,
            RateLimitConfig(requests_per_minute=60, burst=5),
        )
        clients[acc.exchange] = await build_client(acc, session, rate_cfg, logger)

    results: Dict[str, List[Dict[str, str]]] = {}

    if ExchangeName.OPINION in clients:
        results["opinion"] = await _fetch_opinion_markets(clients[ExchangeName.OPINION], limit)
    if ExchangeName.POLYMARKET in clients:
        results["polymarket"] = await _fetch_polymarket_markets(clients[ExchangeName.POLYMARKET], limit)

    await proxy_handler.close()
    for client in set(clients.values()):
        close = getattr(client, "close", None)
        if close:
            await close()
    return results


def print_results(data: Dict[str, List[Dict[str, str]]]) -> None:
    print(json.dumps(data, indent=2, ensure_ascii=False))
    print("\n# Ready-to-copy market_pairs (edit IDs as needed):")
    for row in data.get("opinion", [])[:5]:
        title = (row.get("title") or "").replace("\"", "")
        event_id = title[:40]
        for poly in data.get("polymarket", [])[:5]:
            print("- event_id: \"{}\"".format(event_id))
            print("  primary_market_id: \"{}\"".format(row.get("market_id")))
            print("  secondary_market_id: \"{}\"".format(poly.get("market_id")))
            print("  contract_type: \"BINARY\"")
            print("  strategy_direction: \"AUTO\"\n")


async def main():
    parser = argparse.ArgumentParser(description="Discover active markets (read-only)")
    parser.add_argument("--limit", type=int, default=20, help="Max markets per exchange to show")
    args = parser.parse_args()
    data = await discover(limit=args.limit)
    print_results(data)


if __name__ == "__main__":
    asyncio.run(main())

