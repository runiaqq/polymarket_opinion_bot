#!/usr/bin/env python
from __future__ import annotations

import argparse
import asyncio
import json
from typing import Dict, List, Optional, Tuple

import aiohttp

from core.models import ExchangeName
from utils.config_loader import ConfigLoader, RateLimitConfig
from utils.logger import BotLogger
from utils.proxy_handler import ProxyHandler


OPENAPI_BASE = "https://openapi.opinion.trade/openapi"
API_BASE = "https://api.opinion.trade"


def _build_headers(api_key: str) -> Dict[str, str]:
    return {"apikey": api_key}


async def fetch_markets(session: aiohttp.ClientSession, api_key: str, proxy: Optional[str], status: str) -> List[Dict]:
    url = f"{OPENAPI_BASE}/market"
    headers = _build_headers(api_key)
    params = {"page": 1, "limit": 50, "status": status, "marketType": 2}
    async with session.get(url, headers=headers, params=params, proxy=proxy, timeout=20) as resp:
        if resp.status != 200:
            text = await resp.text()
            raise RuntimeError(f"market list failed {resp.status}: {text}")
        data = await resp.json()
    result = data.get("result") or data.get("data") or {}
    return result.get("list", []) if isinstance(result, dict) else []


async def try_orderbook(
    session: aiohttp.ClientSession, api_key: str, proxy: Optional[str], endpoint: str, ident: str
) -> Tuple[bool, int]:
    if endpoint == "openapi_token":
        url = f"{OPENAPI_BASE}/token/orderbook"
        params = {"token_id": ident}
        headers = _build_headers(api_key)
    elif endpoint == "openapi_orderbook_path":
        url = f"{OPENAPI_BASE}/orderbook/{ident}"
        params = None
        headers = _build_headers(api_key)
    else:  # api_rest
        url = f"{API_BASE}/orderbook/{ident}"
        params = None
        headers = {}
    async with session.get(url, headers=headers, params=params, proxy=proxy, timeout=10) as resp:
        status = resp.status
        if status != 200:
            return False, status
        try:
            payload = await resp.json()
        except Exception:
            return False, status
        bids = payload.get("bids") or payload.get("data", {}).get("bids") or []
        asks = payload.get("asks") or payload.get("data", {}).get("asks") or []
        if isinstance(bids, list) or isinstance(asks, list):
            return True, status
        return False, status


def build_identifiers(market: Dict) -> List[str]:
    candidates = []
    for key in ("marketId", "market_id", "topic_id"):
        val = market.get(key)
        if val:
            candidates.append(str(val))
    for key in ("yesTokenId", "noTokenId", "yes_token_id", "no_token_id"):
        val = market.get(key)
        if val:
            candidates.append(str(val))
    for child in market.get("childMarkets") or []:
        cid = child.get("marketId") or child.get("topic_id")
        if cid:
            candidates.append(str(cid))
        for tk in ("yesTokenId", "noTokenId"):
            cv = child.get(tk)
            if cv:
                candidates.append(str(cv))
    # ensure uniqueness preserving order
    seen = set()
    uniq = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            uniq.append(c)
    return uniq


def yaml_snippet(event_id: str, opinion_id: str, poly_id: str) -> str:
    return (
        f"- event_id: \"{event_id}\"\n"
        f"  primary_market_id: \"{opinion_id}\"\n"
        f"  secondary_market_id: \"{poly_id}\"\n"
        f"  contract_type: \"BINARY\"\n"
        f"  strategy_direction: \"AUTO\"\n"
    )


async def main():
    parser = argparse.ArgumentParser(description="Discover Opinion orderbook identifiers.")
    parser.add_argument("--status", default="activated", help="Market status filter")
    parser.add_argument("--limit", type=int, default=20, help="Max markets to test")
    args = parser.parse_args()

    loader = ConfigLoader()
    settings = loader.load_settings()
    accounts = loader.load_accounts()
    logger = BotLogger("opinion-discovery")
    proxy_handler = ProxyHandler(logger)

    opinion_account = next(a for a in accounts if a.exchange == ExchangeName.OPINION)
    session = await proxy_handler.get_session(opinion_account)
    markets = await fetch_markets(session, opinion_account.api_key, opinion_account.proxy, args.status)
    markets = markets[: args.limit]

    endpoints = ["openapi_token", "openapi_orderbook_path", "api_rest"]
    working: List[Dict[str, str]] = []

    for m in markets:
        title = m.get("marketTitle") or m.get("topic_title") or ""
        idents = build_identifiers(m)
        for ident in idents:
            for ep in endpoints:
                ok, status = await try_orderbook(session, opinion_account.api_key, opinion_account.proxy, ep, ident)
                if ok:
                    working.append(
                        {
                            "title": title,
                            "identifier": ident,
                            "endpoint": ep,
                            "status": status,
                        }
                    )
                    print(
                        json.dumps(
                            {
                                "title": title,
                                "identifier": ident,
                                "endpoint": ep,
                                "status": status,
                                "yaml": yaml_snippet(title[:48].replace('\"', ''), ident, "<poly_token_id_here>"),
                            },
                            ensure_ascii=False,
                        )
                    )
                    break
            else:
                continue
            break

    print("\n=== SUMMARY ===")
    print(json.dumps(working, indent=2, ensure_ascii=False))

    await proxy_handler.close()


if __name__ == "__main__":
    asyncio.run(main())
