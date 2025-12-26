from __future__ import annotations

import asyncio
from contextlib import suppress
from typing import Dict, List, Optional

from aiohttp import web
from core.event_discovery.approvals import EventApprovalStore
from core.event_discovery.registry import EventDiscoveryRegistry
from core.event_discovery.service import EventDiscoveryService
from core.hedger import Hedger
from core.healthcheck import HealthcheckService
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
from telegram.commands import TelegramBotRunner, TelegramCommandRouter
from telegram.event_review import EventReviewHandler
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
    approvals_store = EventApprovalStore()
    discovery_registry = EventDiscoveryRegistry(approvals_store)

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

    healthcheck = HealthcheckService(
        spread_analyzer=spread_analyzer,
        orderbook_manager=orderbook_manager,
        account_pools=account_pools,
        clients_by_id=clients_by_id,
        fees=settings.fees,
        logger=logger,
    )
    opinion_key: Optional[str] = None
    for acc in account_pools[ExchangeName.OPINION]:
        if acc.api_key:
            opinion_key = acc.api_key
            break

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
    telegram_runner: Optional[TelegramBotRunner] = None
    heartbeat_task: Optional[asyncio.Task] = None

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

    event_discovery_service = EventDiscoveryService(
        config=settings.event_discovery,
        registry=discovery_registry,
        logger=logger,
        opinion_api_key=opinion_key,
        stop_event=stop_event,
        poll_interval_sec=settings.event_discovery.poll_interval_sec,
    )
    await event_discovery_service.start()

    event_review_handler = EventReviewHandler(
        registry=discovery_registry,
        approvals=approvals_store,
        notifier=notifier,
        logger=logger,
    )

    command_router = TelegramCommandRouter(
        settings=settings,
        pair_controller=pair_controller,
        db=db,
        reconciler=reconciler,
        spread_analyzer=spread_analyzer,
        notifier=notifier,
        healthcheck=healthcheck,
        account_pools=account_pools,
        clients_by_id=clients_by_id,
        account_index=account_index,
        logger=logger,
        event_review_handler=event_review_handler,
    )
    telegram_runner = TelegramBotRunner(
        notifier=notifier,
        router=command_router,
        stop_event=stop_event,
        logger=logger,
        poll_interval=5,
    )

    async def _heartbeat_loop():
        interval = max(60, settings.telegram.heartbeat_interval_sec)
        while not stop_event.is_set():
            try:
                msg = await command_router.build_heartbeat()
                await notifier.send_message(msg)
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.warn("heartbeat send failed", error=str(exc))
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval)
            except asyncio.TimeoutError:
                continue

    if settings.telegram.enabled and notifier.enabled:
        await telegram_runner.start()
        startup_msg = (
            f"Market-hedge bot started | dry_run={settings.dry_run} | pairs={len(settings.market_pairs)}\n"
            "Commands: /status /health /simulate <pair_id> [size]"
        )
        await notifier.send_message(startup_msg)
        if settings.telegram.heartbeat_enabled:
            heartbeat_task = asyncio.create_task(_heartbeat_loop())

    wait_forever = asyncio.Future()
    try:
        await wait_forever
    except KeyboardInterrupt:
        stop_event.set()
        wait_forever.cancel()
        logger.info("shutting down...")
    finally:
        await pair_controller.shutdown()
        if heartbeat_task:
            heartbeat_task.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat_task
        if telegram_runner:
            await telegram_runner.stop()
        if sheet_task:
            sheet_task.cancel()
            with suppress(asyncio.CancelledError):
                await sheet_task
        await reconciler.stop()
        if webhook_runner:
            await webhook_runner.cleanup()
        if sheet_client:
            await sheet_client.close()
        await event_discovery_service.stop()
        for client in set(clients_by_id.values()):
            close = getattr(client, "close", None)
            if close:
                await close()
        await notifier.close()
        await proxy_handler.close()
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())




