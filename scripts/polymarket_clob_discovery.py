#!/usr/bin/env python
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Dict, List, Tuple

from aiohttp import ClientSession

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.models import ExchangeName  # noqa: E402
from exchanges.opinion_api import OpinionAPI  # noqa: E402
from exchanges.rate_limiter import RateLimiter  # noqa: E402
from utils.config_loader import ConfigLoader, RateLimitConfig  # noqa: E402
from utils.logger import BotLogger  # noqa: E402
from utils.polymarket_discovery import (  # noqa: E402
    CLOB_URL,
    DEFAULT_GAMMA_URL,
    build_yaml_snippet,
    fetch_valid_clob_markets,
    score_title_match,
    slugify,
    write_csv,
)
from utils.proxy_handler import ProxyHandler  # noqa: E402


def _pick_account(accounts, exchange: ExchangeName):
    for acc in accounts:
        if acc.exchange == exchange:
            return acc
    raise RuntimeError(f"no account configured for {exchange.value}")


async def _build_opinion_client(account, session: ClientSession, rate_cfg: RateLimitConfig, logger: BotLogger):
    limiter = RateLimiter(
        requests_per_minute=rate_cfg.requests_per_minute,
        burst=rate_cfg.burst,
    )
    return OpinionAPI(
        session=session,
        api_key=account.api_key,
        secret=account.secret_key,
        rate_limit=limiter,
        logger=logger,
        proxy=account.proxy,
    )


async def fetch_opinion_markets(
    client: OpinionAPI,
    limit: int,
    logger: BotLogger,
) -> List[Dict[str, str]]:
    try:
        markets = await client.get_markets()
    except Exception as exc:
        logger.error("opinion discovery failed", error=str(exc))
        return []
    rows = []
    for m in markets[:limit]:
        rows.append(
            {
                "exchange": "Opinion",
                "market_id": m.market_id,
                "title": m.name,
                "status": m.status,
                "normalized_title": normalize_title(m.name or ""),
            }
        )
    return rows


def map_opinion_to_clob(
    opinion_markets: List[Dict[str, str]],
    clob_markets: List[Dict[str, str]],
    min_score: float = 0.45,
) -> List[Dict[str, object]]:
    """Return best Polymarket match for each Opinion market."""
    matches: List[Dict[str, object]] = []
    for op in opinion_markets:
        best: Tuple[float, Dict[str, str]] | None = None
        for pm in clob_markets:
            title = pm.get("question") or pm.get("title") or pm.get("name") or ""
            score = score_title_match(op["title"], title)
            if pm.get("acceptingOrders") is False:
                score -= 0.1
            if not best or score > best[0]:
                best = (score, pm)
        if best and best[0] >= min_score:
            pm = best[1]
            event_id = f"{slugify(op['title'])[:48]}-{op['market_id']}"
            poly_id = pm.get("validated_token_id") or pm.get("primary_token_id") or pm.get("id")
            matches.append(
                {
                    "event_id": event_id,
                    "opinion_market_id": op["market_id"],
                    "opinion_title": op["title"],
                    "polymarket_market_id": str(poly_id),
                    "polymarket_title": pm.get("question") or pm.get("title") or "",
                    "polymarket_accepting_orders": pm.get("acceptingOrders", pm.get("enableOrderBook")),
                    "score": round(best[0], 4),
                }
            )
    return matches


def render_yaml(matches: List[Dict[str, object]]) -> str:
    snippets = []
    for match in matches:
        snippets.append(
            build_yaml_snippet(
                event_id=match["event_id"],
                opinion_market_id=str(match["opinion_market_id"]),
                polymarket_market_id=str(match["polymarket_market_id"]),
            ).rstrip()
        )
    return "\n".join(snippets)


async def main():
    parser = argparse.ArgumentParser(description="Discover Polymarket CLOB markets and map to Opinion.")
    parser.add_argument("--limit", type=int, default=50, help="Max Opinion markets to consider")
    parser.add_argument("--page-size", type=int, default=200, help="CLOB pagination size")
    parser.add_argument("--max-pages", type=int, default=None, help="Optional maximum CLOB pages")
    parser.add_argument("--concurrency", type=int, default=20, help="Parallel orderbook checks")
    parser.add_argument(
        "--source",
        choices=["clob", "gamma"],
        default="gamma",
        help="Source endpoint for market listings (gamma includes clobTokenIds).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / "data" / "clob_markets_valid.csv",
        help="Where to write the validated CLOB markets CSV",
    )
    args = parser.parse_args()

    loader = ConfigLoader()
    settings = loader.load_settings()
    accounts = loader.load_accounts()
    logger = BotLogger("clob-discovery")
    proxy_handler = ProxyHandler(logger)

    poly_account = _pick_account(accounts, ExchangeName.POLYMARKET)
    opinion_account = _pick_account(accounts, ExchangeName.OPINION)

    # Opinion client setup
    opinion_session = await proxy_handler.get_session(opinion_account)
    opinion_rate_cfg = settings.rate_limits.get(
        opinion_account.exchange.value,
        RateLimitConfig(requests_per_minute=60, burst=5),
    )
    opinion_client = await _build_opinion_client(opinion_account, opinion_session, opinion_rate_cfg, logger)

    # Polymarket CLOB discovery (no auth required)
    poly_session = await proxy_handler.get_session(poly_account)
    query_params = {"sort": "volume24h:desc", "closed": "false", "archived": "false", "active": "true"}
    if args.source != "gamma":
        query_params = None
    base_url = DEFAULT_GAMMA_URL if args.source == "gamma" else CLOB_URL

    valid_markets, excluded = await fetch_valid_clob_markets(
        session=poly_session,
        proxy=poly_account.proxy,
        base_url=base_url,
        orderbook_base_url=CLOB_URL,
        page_size=args.page_size,
        max_pages=args.max_pages,
        concurrency=args.concurrency,
        source=args.source,
        query_params=query_params,
    )

    write_csv(valid_markets, args.output)

    opinion_markets = await fetch_opinion_markets(opinion_client, limit=args.limit, logger=logger)
    matches = map_opinion_to_clob(opinion_markets, valid_markets)

    summary = {
        "opinion_markets_considered": len(opinion_markets),
        "polymarket_clob_valid": len(valid_markets),
        "excluded": {str(status): count for status, count in _status_counts(excluded).items()},
        "matches_found": len(matches),
    }
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    print("\n# Ready-to-use YAML market_pairs (Polymarket CLOB only):")
    print(render_yaml(matches))

    if excluded:
        print("\n# Excluded Polymarket markets (non-200 orderbook):")
        for market, status in excluded[:10]:
            title = market.get("question") or market.get("title") or market.get("name") or ""
            print(f"- id={market.get('id')} status={status} title={title}")

    await proxy_handler.close()
    close = getattr(opinion_client, "close", None)
    if close:
        await close()


def _status_counts(excluded: List[Tuple[Dict[str, object], int]]) -> Dict[int, int]:
    counts: Dict[int, int] = {}
    for _, status in excluded:
        counts[status] = counts.get(status, 0) + 1
    return counts


if __name__ == "__main__":
    asyncio.run(main())


