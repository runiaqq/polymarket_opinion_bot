#!/usr/bin/env python
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

import aiohttp

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from utils.polymarket_discovery import paginate_markets, write_csv  # noqa: E402
from utils.logger import BotLogger  # noqa: E402
from utils.proxy_handler import ProxyHandler  # noqa: E402
from utils.config_loader import ConfigLoader  # noqa: E402


async def main() -> None:
    parser = argparse.ArgumentParser(description="Download Polymarket markets into CSV cache (read-only).")
    parser.add_argument("--out", default="data/polymarket_markets.csv", help="Output CSV path")
    parser.add_argument("--page-size", type=int, default=100, help="Page size for pagination (default 100)")
    parser.add_argument("--max-pages", type=int, default=0, help="Max pages to fetch (0 = until empty)")
    parser.add_argument("--insecure", action="store_true", help="Disable SSL verification (use only if cert fails)")
    args = parser.parse_args()

    logger = BotLogger("poly_update")
    loader = ConfigLoader()
    accounts = loader.load_accounts()
    proxy_handler = ProxyHandler(logger)

    # pick proxy from first Polymarket account if provided
    proxy = None
    for acc in accounts:
        if acc.exchange.value.lower() == "polymarket" and acc.proxy:
            proxy = acc.proxy
            break

    connector = aiohttp.TCPConnector(ssl=not args.insecure)
    async with aiohttp.ClientSession(connector=connector) as session:
        try:
            markets = await paginate_markets(
                session,
                proxy=proxy,
                page_size=args.page_size,
                max_pages=args.max_pages if args.max_pages > 0 else None,
            )
        except Exception as exc:
            logger.error("market download failed", error=str(exc))
            return
    if not markets:
        logger.warn("no markets retrieved")
        return
    out_path = write_csv(markets, args.out)
    logger.info("polymarket markets cached", count=len(markets), path=str(out_path))
    await proxy_handler.close()


if __name__ == "__main__":
    asyncio.run(main())

