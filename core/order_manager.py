from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, Optional

from core.exceptions import HedgingError, RiskCheckError
from core.models import ExchangeName, Fill, Order, OrderSide, OrderStatus, OrderType
from core.order_fsm import OrderFSMEvent, OrderFSMState, OrderStateMachine
from core.hedger import HedgeLegRequest
from core.market_mapper import MarketMapper
from models.validators import validate_order, validate_fill
from utils.log_hooks import LogHooks
from utils.logger import BotLogger


class OrderManager:
    """Coordinates order placement and lifecycle tracking."""

    def __init__(
        self,
        exchanges: Dict[ExchangeName, object],
        database,
        position_tracker,
        hedger,
        risk_manager,
        logger: BotLogger | None = None,
        dry_run: bool = False,
        event_id: str | None = None,
        market_map: Dict[ExchangeName, str] | None = None,
        mapper: Optional[MarketMapper] = None,
    ):
        self.exchanges = exchanges
        self.db = database
        self.position_tracker = position_tracker
        self.hedger = hedger
        self.risk_manager = risk_manager
        self.logger = logger or BotLogger(__name__)
        self.dry_run = dry_run
        self._locks = {name: asyncio.Lock() for name in exchanges}
        self.primary = None
        self.secondary = None
        self.event_id = event_id
        self.market_map = market_map or {}
        self.mapper = mapper
        self._fill_lock = asyncio.Lock()
        self._processed_fills: set[str] = set()
        self._shutdown = asyncio.Event()
        self._fsms: Dict[str, OrderStateMachine] = {}
        self._order_sizes: Dict[str, float] = {}
        self._fill_progress: Dict[str, float] = {}
        self.log_hooks = LogHooks()

    def set_routing(self, primary: ExchangeName, secondary: ExchangeName) -> None:
        self.primary = primary
        self.secondary = secondary

    async def place_primary_limit(
        self,
        exchange_name: ExchangeName,
        market_id: str,
        side: OrderSide,
        price: float,
        size: float,
        client_order_id: str | None = None,
    ) -> Order | None:
        exchange = self.exchanges[exchange_name]
        async with self._locks[exchange_name]:
            await self.risk_manager.check_limits(market_id, size)
            await self.risk_manager.check_balance(exchange, price * size)
            client_order_id = client_order_id or str(uuid.uuid4())
            if self.dry_run:
                order = self._build_dry_order(
                    exchange_name, market_id, side, price, size, client_order_id
                )
            else:
                order = await exchange.place_limit_order(
                    market_id=market_id,
                    side=side,
                    price=price,
                    size=size,
                    client_order_id=client_order_id,
                )
            validate_order(order)
            await self.db.save_order(order)
            order_key = order.order_id or order.client_order_id
            self._order_sizes[order_key] = order.size
            self._fill_progress.setdefault(order_key, 0.0)
            fsm = OrderStateMachine(order_key, self.db, logger=self.logger)
            self._fsms[order_key] = fsm
            await fsm.transition(
                OrderFSMEvent.PLACE,
                payload=order,
                event_id=f"place-{order_key}",
            )
            await self.log_hooks.emit(
                "order_state",
                {
                    "order_id": order_key,
                    "state": fsm.current_state.value,
                    "market_id": market_id,
                    "exchange": exchange_name.value,
                },
            )
            self.logger.info(
                "limit order placed",
                order_id=order_key,
                market_id=market_id,
                exchange=exchange_name.value,
            )
            return order

    async def track_fills(self, exchange_name: ExchangeName) -> None:
        raise RuntimeError("track_fills is handled by Reconciler.")

    async def poll_fills(self, exchange_name: ExchangeName, interval: float) -> None:
        raise RuntimeError("poll_fills is handled by Reconciler.")

    async def cancel_limit(self, exchange_name: ExchangeName, order_id: str) -> bool:
        exchange = self.exchanges[exchange_name]
        async with self._locks[exchange_name]:
            if self.dry_run:
                return True
            fsm = self._fsms.get(order_id)
            if fsm:
                await fsm.transition(
                    OrderFSMEvent.CANCEL_REQUEST,
                    event_id=f"cancel-req-{order_id}",
                )
            await exchange.cancel_order(order_id)
            if fsm:
                await fsm.transition(
                    OrderFSMEvent.CANCEL_ACK,
                    event_id=f"cancel-ack-{order_id}",
                )
            else:
                await self.db.update_order_status(order_id, OrderStatus.CANCELED)
            self.logger.info(
                "limit order cancelled",
                exchange=exchange_name.value,
                order_id=order_id,
            )
            return True

    async def handle_fill(self, exchange_name: ExchangeName, fill: Fill) -> Optional[str]:
        validate_fill(fill)
        if not await self._mark_fill_processed(fill):
            return None
        event_id = self.event_id or fill.market_id
        await self.db.update_order_fill(fill.order_id, Decimal(str(fill.size)), fill)
        fsm = self._get_or_create_fsm(fill.order_id)
        await self.log_hooks.emit(
            "fill_consumed",
            {
                "order_id": fill.order_id,
                "exchange": exchange_name.value,
                "market_id": fill.market_id,
                "size": fill.size,
                "price": fill.price,
            },
        )
        progress = self._fill_progress.get(fill.order_id, 0.0) + fill.size
        target = self._order_sizes.get(fill.order_id)
        is_full = target is not None and progress >= target - 1e-9
        event = OrderFSMEvent.FILL_FULL if is_full else OrderFSMEvent.FILL_PARTIAL
        self._fill_progress[fill.order_id] = target if is_full and target is not None else progress
        await fsm.transition(
            event,
            payload=fill,
            event_id=f"fill-{fill.order_id}-{fill.timestamp.isoformat()}",
        )
        await self.position_tracker.add_fill(event_id, fill.size, fill.price, fill.side)
        hedge_side = OrderSide.SELL if fill.side == OrderSide.BUY else OrderSide.BUY
        hedge_exchange_name = self.secondary if exchange_name == self.primary else self.primary
        if hedge_exchange_name is None:
            self.logger.warn("hedge exchange not configured")
            return
        hedge_exchange = self.exchanges[hedge_exchange_name]
        hedge_market_id = self._resolve_market_id(exchange_name, hedge_exchange_name, fill.market_id)
        try:
            await self.hedger.hedge(
                legs=[
                    HedgeLegRequest(
                        client=hedge_exchange,
                        exchange=hedge_exchange_name,
                        market_id=hedge_market_id,
                    )
                ],
                event_id=event_id,
                side=hedge_side,
                size=fill.size,
                reference_price=fill.price,
                entry_order_id=fill.order_id,
                entry_exchange=exchange_name,
            )
            await self.log_hooks.emit(
                "hedge_requested",
                {
                    "order_id": fill.order_id,
                    "hedge_exchange": hedge_exchange_name.value,
                    "market_id": hedge_market_id,
                    "size": fill.size,
                    "side": hedge_side.value,
                },
            )
        except (RiskCheckError, HedgingError) as exc:
            self.logger.error("hedging failed", error=str(exc))
        return self._fill_key(fill)

    @staticmethod
    def normalize_fill(exchange_name: ExchangeName, message) -> Fill | None:
        if not isinstance(message, dict):
            return None
        data = message.get("data", message)
        if not isinstance(data, dict):
            return None
        order_id = str(data.get("order_id") or data.get("id"))
        if not order_id:
            return None
        price = float(data.get("price") or data.get("fill_price") or 0.0)
        size = float(
            data.get("size")
            or data.get("filled_size")
            or data.get("fill_size")
            or data.get("matchedAmount")
            or 0.0
        )
        side_raw = str(data.get("side", "BUY")).upper()
        side = OrderSide.BUY if side_raw == "BUY" else OrderSide.SELL
        timestamp = data.get("timestamp") or data.get("filled_at") or datetime.now(tz=timezone.utc)
        if isinstance(timestamp, (int, float)):
            ts_dt = datetime.fromtimestamp(
                timestamp / 1000 if timestamp > 10**12 else timestamp,
                tz=timezone.utc,
            )
        elif isinstance(timestamp, str):
            ts_dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        else:
            ts_dt = datetime.now(tz=timezone.utc)
        return Fill(
            order_id=order_id,
            market_id=str(data.get("market_id") or data.get("token_id")),
            exchange=exchange_name,
            side=side,
            price=price,
            size=size,
            fee=float(data.get("fee", 0.0)),
            timestamp=ts_dt,
        )

    async def _mark_fill_processed(self, fill: Fill) -> bool:
        key = f"{fill.order_id}:{fill.size}:{fill.timestamp.isoformat()}"
        async with self._fill_lock:
            if key in self._processed_fills:
                return False
            self._processed_fills.add(key)
            if len(self._processed_fills) > 10000:
                self._processed_fills.clear()
            return True

    def stop(self) -> None:
        self._shutdown.set()

    def _get_or_create_fsm(
        self,
        order_id: str,
        default_state: OrderFSMState = OrderFSMState.PLACED,
    ) -> OrderStateMachine:
        if order_id not in self._fsms:
            self._fsms[order_id] = OrderStateMachine(
                order_id,
                self.db,
                initial_state=default_state,
                logger=self.logger,
            )
        return self._fsms[order_id]
    def _build_dry_order(
        self,
        exchange_name: ExchangeName,
        market_id: str,
        side: OrderSide,
        price: float,
        size: float,
        client_order_id: str,
    ) -> Order:
        return Order(
            order_id=f"dry-{client_order_id}",
            client_order_id=client_order_id,
            market_id=market_id,
            exchange=exchange_name,
            side=side,
            order_type=OrderType.LIMIT,
            price=price,
            size=size,
            filled_size=0.0,
            status=OrderStatus.PENDING,
            created_at=datetime.now(tz=timezone.utc),
        )

    def _resolve_market_id(
        self,
        source_exchange: ExchangeName,
        target_exchange: ExchangeName,
        source_market_id: str,
    ) -> str:
        mapped: Optional[str] = None
        if self.mapper:
            if source_exchange == ExchangeName.POLYMARKET and target_exchange == ExchangeName.OPINION:
                mapped = self.mapper.find_opinion_for_polymarket(source_market_id)
            elif source_exchange == ExchangeName.OPINION and target_exchange == ExchangeName.POLYMARKET:
                mapped = self.mapper.find_polymarket_for_opinion(source_market_id)
        return mapped or self.market_map.get(target_exchange, source_market_id)

    def _fill_key(self, fill: Fill) -> str:
        return f"{fill.order_id}:{fill.timestamp.isoformat()}:{fill.size}"

