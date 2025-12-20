from __future__ import annotations

import asyncio
import json
from contextlib import suppress
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional

from core.models import AccountCredentials, ExchangeName
from core.order_manager import OrderManager
from core.spread_analyzer import SpreadAnalyzer
from exchanges.orderbook_manager import OrderbookManager
from utils.account_pool import AccountPool
from utils.config_loader import FeeConfig, MarketPairConfig, Settings
from utils.google_sheets import SheetPairSpec
from utils.logger import BotLogger


@dataclass(slots=True)
class PairRuntime:
    pair_id: str
    config: MarketPairConfig
    order_manager: OrderManager
    stop_event: asyncio.Event
    task: asyncio.Task
    source: str
    size_override: Optional[float]
    fingerprint: str


class PairController:
    """Supervises pair execution tasks and dynamic sheet-driven changes."""

    def __init__(
        self,
        settings: Settings,
        db,
        position_tracker,
        hedger,
        risk_manager,
        logger: BotLogger,
        stop_event: asyncio.Event,
        spread_analyzer: SpreadAnalyzer,
        orderbook_manager: OrderbookManager,
        mapper,
        notifier,
        account_pools: Dict[ExchangeName, List[AccountCredentials]],
        clients_by_id: Dict[str, object],
    ):
        self.settings = settings
        self.db = db
        self.position_tracker = position_tracker
        self.hedger = hedger
        self.risk_manager = risk_manager
        self.logger = logger
        self.stop_event = stop_event
        self.spread_analyzer = spread_analyzer
        self.orderbook_manager = orderbook_manager
        self.mapper = mapper
        self.notifier = notifier
        self.account_pools = account_pools
        self.clients_by_id = clients_by_id
        self._pairs: Dict[str, PairRuntime] = {}
        self._lock = asyncio.Lock()
        self._account_rr: Dict[ExchangeName, int] = {}

    def list_order_managers(self) -> Iterable[OrderManager]:
        return (runtime.order_manager for runtime in self._pairs.values())

    async def start_pair(
        self,
        pair_cfg: MarketPairConfig,
        source: str = "static",
        size_override: Optional[float] = None,
        fingerprint: str = "",
    ):
        pair_id = pair_cfg.event_id
        if not pair_id:
            raise ValueError("pair must provide event_id")
        async with self._lock:
            if pair_id in self._pairs:
                self.logger.warn("pair already running", pair_id=pair_id)
                return
            try:
                runtime = await self._spawn_pair(pair_cfg, size_override, source, fingerprint)
            except Exception as exc:
                self.logger.error("failed to start pair", pair_id=pair_id, error=str(exc))
                return
            self._pairs[pair_id] = runtime

    async def _spawn_pair(
        self,
        pair_cfg: MarketPairConfig,
        size_override: Optional[float],
        source: str,
        fingerprint: str,
    ) -> PairRuntime:
        primary_exchange = pair_cfg.primary_exchange or self.settings.exchanges.primary
        secondary_exchange = pair_cfg.secondary_exchange or self.settings.exchanges.secondary
        primary_account = self._resolve_account(primary_exchange, pair_cfg.primary_account_id)
        secondary_account = self._resolve_account(secondary_exchange, pair_cfg.secondary_account_id)
        primary_client = self.clients_by_id[primary_account.account_id]
        secondary_client = self.clients_by_id[secondary_account.account_id]
        exchange_map = {
            primary_exchange: primary_client,
            secondary_exchange: secondary_client,
        }

        await self._assert_polymarket_orderbook(
            primary_exchange, secondary_exchange, pair_cfg, primary_client, secondary_client
        )

        order_manager = OrderManager(
            exchange_map,
            self.db,
            self.position_tracker,
            self.hedger,
            self.risk_manager,
            self.logger,
            self.settings.dry_run,
            event_id=pair_cfg.event_id,
            market_map={
                primary_exchange: pair_cfg.primary_market_id,
                secondary_exchange: pair_cfg.secondary_market_id,
            },
            mapper=self.mapper,
            double_limit_enabled=self.settings.double_limit_enabled,
            cancel_after_ms=self.settings.market_hedge_mode.cancel_unfilled_after_ms,
        )
        order_manager.set_routing(primary_exchange, secondary_exchange)
        pair_stop_event = asyncio.Event()
        task = asyncio.create_task(
            run_pair_loop(
                pair_cfg=pair_cfg,
                settings=self.settings,
                primary_client=primary_client,
                secondary_client=secondary_client,
                order_manager=order_manager,
                spread_analyzer=self.spread_analyzer,
                orderbook_manager=self.orderbook_manager,
                stop_event=self.stop_event,
                pair_stop_event=pair_stop_event,
                size_override=size_override,
                fees=self.settings.fees,
                logger=self.logger,
            )
        )
        return PairRuntime(
            pair_id=pair_cfg.event_id,
            config=pair_cfg,
            order_manager=order_manager,
            stop_event=pair_stop_event,
            task=task,
            source=source,
            size_override=size_override,
            fingerprint=fingerprint or _fingerprint(pair_cfg, size_override),
        )

    def _resolve_account(self, exchange: ExchangeName, preferred_id: Optional[str]) -> AccountCredentials:
        pool = self.account_pools.get(exchange)
        if not pool:
            raise RuntimeError(f"no accounts configured for {exchange.value}")
        if preferred_id:
            for account in pool:
                if account.account_id == preferred_id:
                    return account
            self.logger.warn(
                "preferred account missing; falling back",
                exchange=exchange.value,
                account_id=preferred_id,
            )
        # Round-robin selection to spread load across many accounts.
        cursor = self._account_rr.get(exchange, 0)
        account = pool[cursor % len(pool)]
        self._account_rr[exchange] = (cursor + 1) % len(pool)
        return account

    async def _assert_polymarket_orderbook(
        self,
        primary_exchange: ExchangeName,
        secondary_exchange: ExchangeName,
        pair_cfg: MarketPairConfig,
        primary_client,
        secondary_client,
    ) -> None:
        """Validate Polymarket orderbook availability once before running a pair."""
        targets = []
        if primary_exchange == ExchangeName.POLYMARKET:
            targets.append(("primary", pair_cfg.primary_market_id, primary_client))
        if secondary_exchange == ExchangeName.POLYMARKET:
            targets.append(("secondary", pair_cfg.secondary_market_id, secondary_client))
        for side, market_id, client in targets:
            try:
                await client.get_orderbook(market_id)
            except Exception as exc:
                message = f"polymarket orderbook unavailable ({side})"
                self.logger.error(message, market_id=market_id, pair=pair_cfg.event_id, error=str(exc))
                raise RuntimeError(message)

    async def stop_pair(self, pair_id: str, reason: str | None = None) -> None:
        async with self._lock:
            runtime = self._pairs.pop(pair_id, None)
        if not runtime:
            return
        runtime.stop_event.set()
        with suppress(asyncio.CancelledError):
            await runtime.task
        await runtime.order_manager.cancel_all_open_orders()
        runtime.order_manager.stop()
        if reason:
            await self._notify(f"Pair {pair_id} stopped: {reason}")

    async def shutdown(self) -> None:
        async with self._lock:
            pair_ids = list(self._pairs.keys())
        for pair_id in pair_ids:
            await self.stop_pair(pair_id, reason="shutdown")

    async def dispatch_fill(self, fill) -> None:
        async with self._lock:
            runtimes = list(self._pairs.values())
        for runtime in runtimes:
            if fill.market_id in runtime.order_manager.market_map.values():
                await runtime.order_manager.handle_fill(fill.exchange, fill)
                break

    async def sync_sheet_pairs(self, specs: Dict[str, SheetPairSpec]) -> None:
        async with self._lock:
            current_sheet = {
                pair_id: runtime
                for pair_id, runtime in self._pairs.items()
                if runtime.source == "sheet"
            }
        desired_ids = set(specs.keys())
        current_ids = set(current_sheet.keys())
        to_remove = current_ids - desired_ids
        to_add = desired_ids - current_ids
        to_check = current_ids & desired_ids

        for pair_id in to_check:
            runtime = current_sheet[pair_id]
            spec = specs[pair_id]
            if runtime.fingerprint != spec.fingerprint:
                to_remove.add(pair_id)
                to_add.add(pair_id)

        for pair_id in to_remove:
            await self.stop_pair(pair_id, reason="sheet_removed")

        for pair_id in to_add:
            spec = specs[pair_id]
            await self.start_pair(
                spec.pair_cfg,
                source="sheet",
                size_override=spec.size_limit,
                fingerprint=spec.fingerprint,
            )

    async def _notify(self, message: str) -> None:
        if not self.notifier:
            return
        try:
            await self.notifier.send_message(message)
        except Exception as exc:
            self.logger.warn("notifier failed", error=str(exc))


async def run_pair_loop(
    pair_cfg: MarketPairConfig,
    settings: Settings,
    primary_client,
    secondary_client,
    order_manager: OrderManager,
    spread_analyzer: SpreadAnalyzer,
    orderbook_manager: OrderbookManager,
    stop_event: asyncio.Event,
    pair_stop_event: asyncio.Event,
    size_override: Optional[float],
    fees: Dict[ExchangeName, FeeConfig],
    logger: BotLogger,
) -> None:
    min_spread = settings.market_hedge_mode.min_spread_for_entry
    size_limit = (
        size_override
        or pair_cfg.max_position_size_per_market
        or settings.market_hedge_mode.max_position_size_per_market
        or 10.0
    )
    size = max(0.01, size_limit)
    primary_exchange = pair_cfg.primary_exchange or settings.exchanges.primary
    secondary_exchange = pair_cfg.secondary_exchange or settings.exchanges.secondary
    primary_fees = fees.get(primary_exchange, FeeConfig())
    secondary_fees = fees.get(secondary_exchange, FeeConfig())

    async def evaluate_once():
        primary_book = await primary_client.get_orderbook(pair_cfg.primary_market_id)
        secondary_book = await secondary_client.get_orderbook(pair_cfg.secondary_market_id)
        scenario = await spread_analyzer.evaluate_opportunity(
            primary_exchange=primary_exchange,
            secondary_exchange=secondary_exchange,
            primary_book=primary_book,
            secondary_book=secondary_book,
            primary_fees=primary_fees,
            secondary_fees=secondary_fees,
            size=size,
            forced_direction=pair_cfg.strategy_direction,
        )
        if not scenario:
            return
        min_total = min_spread * size
        if scenario["net_total"] < min_total:
            return
        primary_leg = scenario["legs"].get(primary_exchange)
        secondary_leg = scenario["legs"].get(secondary_exchange)
        if not primary_leg or not secondary_leg:
            return
        if order_manager.double_limit_enabled:
            await order_manager.place_double_limit(
                account=pair_cfg.event_id,
                pair=pair_cfg,
                price_a=primary_leg["price"],
                size_a=size,
                price_b=secondary_leg["price"],
                size_b=size,
                side_a=primary_leg["side"],
                side_b=secondary_leg["side"],
            )
            return
        await order_manager.place_primary_limit(
            primary_exchange,
            pair_cfg.primary_market_id,
            primary_leg["side"],
            primary_leg["price"],
            size,
        )

    while not stop_event.is_set() and not pair_stop_event.is_set():
        try:
            await evaluate_once()
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("pair loop error", pair=pair_cfg.event_id, error=str(exc))
            await asyncio.sleep(5)
        await asyncio.sleep(1)


def _fingerprint(pair_cfg: MarketPairConfig, size_override: Optional[float]) -> str:
    return json.dumps(
        {
            "primary": pair_cfg.primary_market_id,
            "secondary": pair_cfg.secondary_market_id,
            "peq": (pair_cfg.primary_exchange.value if pair_cfg.primary_exchange else ""),
            "seq": (pair_cfg.secondary_exchange.value if pair_cfg.secondary_exchange else ""),
            "size": size_override,
        },
        sort_keys=True,
    )

