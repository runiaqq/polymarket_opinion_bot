from __future__ import annotations

import asyncio
from contextlib import suppress
from typing import Dict, List

from core.hedger import Hedger
from core.market_mapper import MarketMapper
from core.models import AccountCredentials, ExchangeName, OrderSide
from core.order_manager import OrderManager
from core.position_tracker import PositionTracker
from core.risk_manager import RiskManager
from core.spread_analyzer import SpreadAnalyzer
from exchanges.opinion_api import OpinionAPI
from exchanges.orderbook_manager import OrderbookManager
from exchanges.polymarket_api import PolymarketAPI
from exchanges.rate_limiter import RateLimiter
from exchanges.reconciliation import Reconciler
from exchanges.reconciliation import Reconciler
from telegram.notifier import TelegramNotifier
from utils.config_loader import (
    ConfigLoader,
    ExchangeConnectivity,
    MarketPairConfig,
    RateLimitConfig,
    Settings,
)
from utils.db import Database
from utils.db_migrations import apply_migrations
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


async def run_pair(
    pair_id: str,
    pair_cfg: MarketPairConfig,
    settings: Settings,
    primary_client,
    secondary_client,
    order_manager: OrderManager,
    spread_analyzer: SpreadAnalyzer,
    orderbook_manager: OrderbookManager,
    stop_event: asyncio.Event,
    logger: BotLogger,
):
    min_spread = settings.market_hedge_mode.min_spread_for_entry
    fees = 0.001
    max_size = settings.market_hedge_mode.max_position_size_per_market or 10.0
    size = max(0.01, min(10.0, max_size))

    async def evaluate_once():
        primary_book = await primary_client.get_orderbook(pair_cfg.primary_market_id)
        secondary_book = await secondary_client.get_orderbook(pair_cfg.secondary_market_id)
        spread = await spread_analyzer.compute_spread(primary_book, secondary_book)
        profitable = await spread_analyzer.is_profitable(spread, fees, min_spread)
        if not profitable:
            return
        best_ask = await orderbook_manager.best_ask(primary_book)
        if not best_ask:
            return
        await order_manager.place_primary_limit(
            settings.exchanges.primary,
            pair_cfg.primary_market_id,
            OrderSide.BUY,
            best_ask.price,
            size,
        )

    while not stop_event.is_set():
        try:
            await evaluate_once()
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("pair loop error", pair=pair_id, error=str(exc))
            await asyncio.sleep(5)
        await asyncio.sleep(1)


async def main() -> None:
    loader = ConfigLoader()
    settings = loader.load_settings()
    accounts = loader.load_accounts()

    logger = BotLogger("market_hedge")
    await apply_migrations(settings.database)
    db = Database(settings.database, logger=logger)
    await db.init()
    proxy_handler = ProxyHandler(logger)
    notifier = TelegramNotifier(
        token=settings.telegram.token,
        chat_id=settings.telegram.chat_id,
        enabled=settings.telegram.enabled,
    )

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

    clients: Dict[str, object] = {}

    for account in accounts:
        session = await proxy_handler.get_session(account)
        rate_cfg = settings.rate_limits.get(
            account.exchange.value,
            RateLimitConfig(requests_per_minute=60, burst=5),
        )
        clients[account.account_id] = await build_client(
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
    tasks: List[asyncio.Task] = []
    order_managers: List[OrderManager] = []
    exchange_clients: Dict[ExchangeName, object] = {
        ExchangeName.POLYMARKET: clients[account_pools[ExchangeName.POLYMARKET][0].account_id],
        ExchangeName.OPINION: clients[account_pools[ExchangeName.OPINION][0].account_id],
    }
    def resolve_account(
        exchange: ExchangeName,
        desired_id: str | None,
    ) -> AccountCredentials:
        pool = account_pools[exchange]
        if desired_id:
            for account in pool:
                if account.account_id == desired_id:
                    return account
            raise RuntimeError(f"account {desired_id} not found for exchange {exchange.value}")
        return pool[0]

    if not settings.market_pairs:
        logger.warn("no market pairs configured; engine will idle")

    for idx, pair in enumerate(settings.market_pairs):
        if not pair.primary_market_id or not pair.secondary_market_id:
            continue
        primary_exchange = settings.exchanges.primary
        secondary_exchange = settings.exchanges.secondary
        primary_account = resolve_account(primary_exchange, pair.primary_account_id)
        secondary_account = resolve_account(secondary_exchange, pair.secondary_account_id)
        primary_client = clients[primary_account.account_id]
        secondary_client = clients[secondary_account.account_id]
        exchange_map = {
            primary_exchange: primary_client,
            secondary_exchange: secondary_client,
        }
        order_manager = OrderManager(
            exchange_map,
            db,
            position_tracker,
            hedger,
            risk_manager,
            logger,
            settings.dry_run,
            event_id=pair.event_id,
            market_map={
                primary_exchange: pair.primary_market_id,
                secondary_exchange: pair.secondary_market_id,
            },
            mapper=mapper,
        )
        order_manager.set_routing(settings.exchanges.primary, settings.exchanges.secondary)
        order_managers.append(order_manager)
        tasks.append(
            asyncio.create_task(
                run_pair(
                    f"pair-{idx}",
                    pair,
                    settings,
                    primary_client,
                    secondary_client,
                    order_manager,
                    spread_analyzer,
                    orderbook_manager,
                    stop_event,
                    logger,
                )
            )
        )

    async def dispatch_fill(fill):
        for manager in order_managers:
            if fill.market_id in manager.market_map.values():
                await manager.handle_fill(fill.exchange, fill)
                return
        logger.warn("fill without order manager", market_id=fill.market_id, exchange=fill.exchange.value)

    reconciler = Reconciler(db, dispatch_fill, logger)
    connectivity_defaults = ExchangeConnectivity(use_websocket=True, poll_interval=5.0)

    opinion_cfg = settings.connectivity.get(ExchangeName.OPINION, connectivity_defaults)
    poly_cfg = settings.connectivity.get(ExchangeName.POLYMARKET, connectivity_defaults)

    if opinion_cfg.use_websocket:
        reconciler.subscribe_ws(
            exchange_clients[ExchangeName.OPINION],
            lambda payload: OrderManager.normalize_fill(ExchangeName.OPINION, payload),
        )
    else:
        reconciler.register_poller(exchange_clients[ExchangeName.OPINION], opinion_cfg.poll_interval)

    if poly_cfg.use_websocket:
        reconciler.subscribe_ws(
            exchange_clients[ExchangeName.POLYMARKET],
            lambda payload: OrderManager.normalize_fill(ExchangeName.POLYMARKET, payload),
        )
    else:
        reconciler.register_poller(exchange_clients[ExchangeName.POLYMARKET], poly_cfg.poll_interval)

    await reconciler.start()

    wait_forever = asyncio.Future()
    try:
        await wait_forever
    except KeyboardInterrupt:
        stop_event.set()
        wait_forever.cancel()
        logger.info("shutting down...")
    finally:
        for om in order_managers:
            om.stop()
        await reconciler.stop()
        for task in tasks:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        for client in set(clients.values()):
            close = getattr(client, "close", None)
            if close:
                await close()
        await notifier.close()
        await proxy_handler.close()
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())

