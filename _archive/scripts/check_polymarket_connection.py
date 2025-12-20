from __future__ import annotations

import asyncio
import sys
from pathlib import Path

ROOT_PATH = Path(__file__).resolve().parent.parent
if str(ROOT_PATH) not in sys.path:
    sys.path.insert(0, str(ROOT_PATH))

from core.models import ExchangeName
from exchanges.polymarket_api import PolymarketAPI
from exchanges.rate_limiter import RateLimiter
from utils.config_loader import ConfigLoader, RateLimitConfig
from utils.logger import BotLogger
from utils.proxy_handler import ProxyHandler


async def _check_polymarket() -> None:
    loader = ConfigLoader()
    settings = loader.load_settings()
    accounts = loader.load_accounts()
    poly_account = next((a for a in accounts if a.exchange == ExchangeName.POLYMARKET), None)
    if poly_account is None:
        raise RuntimeError("No Polymarket account configured in config/accounts.json")

    rate_cfg = settings.rate_limits.get(
        ExchangeName.POLYMARKET.value,
        RateLimitConfig(requests_per_minute=60, burst=5),
    )
    limiter = RateLimiter(
        requests_per_minute=rate_cfg.requests_per_minute,
        burst=rate_cfg.burst,
    )

    proxy_handler = ProxyHandler(BotLogger("proxy_check"))
    session = await proxy_handler.get_session(poly_account)
    client = PolymarketAPI(
        session=session,
        api_key=poly_account.api_key,
        secret=poly_account.secret_key,
        passphrase=poly_account.passphrase or poly_account.metadata.get("passphrase"),
        wallet_address=poly_account.wallet_address or poly_account.metadata.get("wallet_address"),
        rate_limit=limiter,
        logger=BotLogger("poly_check"),
        proxy=poly_account.proxy,
    )
    try:
        balances = await client.get_balances()
        print("Polymarket balances:", balances)
        positions = await client.get_positions()
        print("Polymarket positions:", len(positions))
    finally:
        await proxy_handler.close()
        await client.close()


def main() -> None:
    asyncio.run(_check_polymarket())


if __name__ == "__main__":
    main()

