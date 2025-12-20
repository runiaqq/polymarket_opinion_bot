from __future__ import annotations

import asyncio
from contextlib import suppress
from typing import Dict, List, Optional

from aiohttp import web
from core.hedger import Hedger
from core.market_mapper import MarketMapper
from core.models import AccountCredentials, ExchangeName
from core.pair_controller import PairController
from core.position_tracker import PositionTracker
from core.risk_manager import RiskManager
from core.spread_analyzer import SpreadAnalyzer
from exchanges.opinion_api import OpinionAPI
from exchanges.orderbook_manager import OrderbookManager
from exchanges.polymarket_api import PolymarketAPI
from exchanges.rate_limiter import RateLimiter
from exchanges.reconciliation import Reconciler
from telegram.notifier import TelegramNotifier
from utils.config_loader import (
    ConfigLoader,
    ExchangeConnectivity,
    RateLimitConfig,
    Settings,
)
from utils.db import Database
from utils.db_migrations import apply_migrations
from utils.google_sheets import GoogleSheetsClient, GoogleSheetsSync, MarketPairStore
from utils.logger import BotLogger
from utils.proxy_handler import ProxyHandler


async def build_client(
    account: AccountCredentials,
    session,
    rate_cfg: RateLimitConfig,
    logger: BotLogger,
):
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


async def main() -> None:
    loader = ConfigLoader()
    settings = loader.load_settings()
    accounts = loader.load_accounts()

    logger = BotLogger("market_hedge")
    pair_store = MarketPairStore(settings.market_pairs)
    sheet_sync_for_web = GoogleSheetsSync(settings.google_sheets, logger) if settings.google_sheets.enabled else None
    await apply_migrations(settings.database)
    db = Database(settings.database, logger=logger)
    await db.init()
    proxy_handler = ProxyHandler(logger)
    notifier = TelegramNotifier(
        token=settings.telegram.token,
        chat_id=settings.telegram.chat_id,
        enabled=settings.telegram.enabled,
    )

    if not settings.market_hedge_mode.enabled:
        logger.warn("market hedge mode disabled in settings.yaml; exiting")
        return

    risk_manager = RiskManager(settings.market_hedge_mode, logger)
    orderbook_manager = OrderbookManager()
    spread_analyzer = SpreadAnalyzer()
    position_tracker = PositionTracker(db, logger)
    hedger = Hedger(
        settings.market_hedge_mode,
        risk_manager,
        orderbook_manager,
        db,
        notifier,
        logger,
        dry_run=settings.dry_run,
    )
    mapper = MarketMapper()

    clients_by_id: Dict[str, object] = {}
    account_index: Dict[str, AccountCredentials] = {acc.account_id: acc for acc in accounts}

    for account in accounts:
        session = await proxy_handler.get_session(account)
        rate_cfg = settings.rate_limits.get(
            account.exchange.value,
            RateLimitConfig(requests_per_minute=60, burst=5),
        )
        clients_by_id[account.account_id] = await build_client(
            account,
            session,
            rate_cfg,
            logger,
        )

    account_pools: Dict[ExchangeName, List[AccountCredentials]] = {
        ExchangeName.POLYMARKET: [
            account for account in accounts if account.exchange == ExchangeName.POLYMARKET
        ],
        ExchangeName.OPINION: [
            account for account in accounts if account.exchange == ExchangeName.OPINION
        ],
    }
    for exchange_name, pool in account_pools.items():
        if not pool:
            raise RuntimeError(f"at least one account required for {exchange_name.value}")

    stop_event = asyncio.Event()
    pair_controller = PairController(
        settings=settings,
        db=db,
        position_tracker=position_tracker,
        hedger=hedger,
        risk_manager=risk_manager,
        logger=logger,
        stop_event=stop_event,
        spread_analyzer=spread_analyzer,
        orderbook_manager=orderbook_manager,
        mapper=mapper,
        notifier=notifier,
        account_pools=account_pools,
        clients_by_id=clients_by_id,
    )

    if not settings.market_pairs:
        logger.warn("no market pairs configured; engine will idle")

    for pair in settings.market_pairs:
        if pair.primary_market_id and pair.secondary_market_id:
            await pair_controller.start_pair(pair, source="static")

    sheet_client: Optional[GoogleSheetsClient] = None
    sheet_task: Optional[asyncio.Task] = None
    if settings.google_sheets.enabled:
        sheet_client = GoogleSheetsClient(settings.google_sheets, logger=logger)
        poll_interval = max(5, settings.google_sheets.poll_interval_sec)

        async def _sheet_loop():
            while not stop_event.is_set():
                try:
                    specs = await sheet_client.fetch_specs()
                    await pair_controller.sync_sheet_pairs(specs)
                    await pair_store.update_pairs([spec.pair_cfg for spec in specs.values()])
                except Exception as exc:  # pragma: no cover - defensive logging
                    logger.warn("sheet sync failed", error=str(exc))
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=poll_interval)
                except asyncio.TimeoutError:
                    continue

        sheet_task = asyncio.create_task(_sheet_loop())

    webhook_runner = None
    if settings.webhook.enabled:
        from scripts.webhook_server import create_app

        app = await create_app(pair_store, sheet_sync_for_web, settings.webhook.admin_token or "")
        webhook_runner = web.AppRunner(app)
        await webhook_runner.setup()
        site = web.TCPSite(webhook_runner, settings.webhook.host, settings.webhook.port)
        await site.start()
        logger.info(
            "webhook server started",
            host=settings.webhook.host,
            port=settings.webhook.port,
        )

    reconciler = Reconciler(db, pair_controller.dispatch_fill, logger)
    connectivity_defaults = ExchangeConnectivity(use_websocket=False, poll_interval=5.0)

    for account_id, client in clients_by_id.items():
        exchange = account_index[account_id].exchange
        cfg = settings.connectivity.get(exchange, connectivity_defaults)
        reconciler.register_poller(client, cfg.poll_interval)

    await reconciler.start()

    wait_forever = asyncio.Future()
    try:
        await wait_forever
    except KeyboardInterrupt:
        stop_event.set()
        wait_forever.cancel()
        logger.info("shutting down...")
    finally:
        await pair_controller.shutdown()
        if sheet_task:
            sheet_task.cancel()
            with suppress(asyncio.CancelledError):
                await sheet_task
        await reconciler.stop()
        if webhook_runner:
            await webhook_runner.cleanup()
        if sheet_client:
            await sheet_client.close()
        for client in set(clients_by_id.values()):
            close = getattr(client, "close", None)
            if close:
                await close()
        await notifier.close()
        await proxy_handler.close()
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())




