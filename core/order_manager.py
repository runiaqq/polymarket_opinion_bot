from __future__ import annotations

import asyncio
import uuid
from contextlib import suppress
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, Optional, Tuple

from core.exceptions import HedgingError, RiskCheckError
from core.models import (
    DoubleLimitState,
    ExchangeName,
    Fill,
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
)
from core.order_fsm import OrderFSMEvent, OrderFSMState, OrderStateMachine
from core.hedger import HedgeLegRequest
from core.market_mapper import MarketMapper
from models.validators import validate_order, validate_fill
from utils.log_hooks import LogHooks
from utils.logger import BotLogger
from utils.config_loader import MarketPairConfig

CANCEL_RETRY_ATTEMPTS = 3
CANCEL_BACKOFF_BASE = 0.5
CANCEL_FAILURE_ALERT_THRESHOLD = 3
from utils.config_loader import MarketPairConfig


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
        double_limit_enabled: bool = False,
        cancel_after_ms: Optional[int] = None,
    ):
        self.exchanges = exchanges
        self.db = database
        self.position_tracker = position_tracker
        self.hedger = hedger
        self.risk_manager = risk_manager
        self.logger = logger or BotLogger(__name__)
        self.dry_run = dry_run
        self._locks = {name: asyncio.Lock() for name in exchanges}
        self.double_limit_enabled = double_limit_enabled
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
        self._order_exchanges: Dict[str, ExchangeName] = {}
        self.log_hooks = LogHooks()
        self._double_limit_locks: Dict[str, asyncio.Lock] = {}
        self._cancel_tasks: Dict[str, asyncio.Task] = {}
        self._cancel_after_ms = cancel_after_ms
        self.cancel_retry_attempts = CANCEL_RETRY_ATTEMPTS
        self._cancel_backoff_base = CANCEL_BACKOFF_BASE
        self._cancel_failure_count = 0
        self._cancel_alert_threshold = CANCEL_FAILURE_ALERT_THRESHOLD

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
            await self.risk_manager.check_limits(self.event_id or market_id, size)
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
            self._order_exchanges[order_key] = exchange_name
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
            await self._schedule_cancel(order_key, exchange_name)
            return order

    async def place_double_limit(
        self,
        account: str | None,
        pair: MarketPairConfig | None,
        price_a: float,
        size_a: float,
        price_b: float,
        size_b: float,
        side_a: OrderSide = OrderSide.BUY,
        side_b: OrderSide = OrderSide.BUY,
    ) -> Tuple[str, str]:
        if not self.double_limit_enabled:
            raise RuntimeError("double limit placement attempted while disabled")
        if not self.primary or not self.secondary:
            raise RuntimeError("exchange routing not configured for double limit placement")
        primary_market = self._resolve_pair_market(pair, self.primary)
        secondary_market = self._resolve_pair_market(pair, self.secondary)
        if not primary_market or not secondary_market:
            raise ValueError("missing market identifiers for double limit order")
        suffix = uuid.uuid4().hex
        primary_client_id = self._build_client_order_id(self.primary, suffix)
        secondary_client_id = self._build_client_order_id(self.secondary, suffix)
        primary_order = await self.place_primary_limit(
            self.primary,
            primary_market,
            side_a,
            price_a,
            size_a,
            client_order_id=primary_client_id,
        )
        if primary_order is None:
            raise RuntimeError("primary exchange did not return order for double limit placement")
        try:
            secondary_order = await self.place_primary_limit(
                self.secondary,
                secondary_market,
                side_b,
                price_b,
                size_b,
                client_order_id=secondary_client_id,
            )
        except Exception as exc:
            await self._attempt_cancel(self.primary, primary_order)
            raise
        if secondary_order is None:
            await self._attempt_cancel(self.primary, primary_order)
            raise RuntimeError("secondary exchange did not return order for double limit placement")

        record_id = uuid.uuid4().hex
        primary_ref = primary_order.order_id or primary_order.client_order_id
        secondary_ref = secondary_order.order_id or secondary_order.client_order_id
        pair_key = self._derive_pair_key(pair)
        await self.db.save_double_limit_pair(
            record_id=record_id,
            pair_key=pair_key,
            primary_order_ref=primary_ref,
            secondary_order_ref=secondary_ref,
            primary_exchange=self.primary.value,
            secondary_exchange=self.secondary.value,
            primary_client_order_id=primary_order.client_order_id,
            secondary_client_order_id=secondary_order.client_order_id,
        )
        self._double_limit_locks.setdefault(record_id, asyncio.Lock())
        await self._promote_order_to_double_limit(primary_ref)
        await self._promote_order_to_double_limit(secondary_ref)
        await self.log_hooks.emit(
            "double_limit_placed",
            {
                "record_id": record_id,
                "pair_key": pair_key,
                "account": account,
                "primary_order_id": primary_ref,
                "secondary_order_id": secondary_ref,
            },
        )
        self.logger.info(
            "double limit orders placed",
            record_id=record_id,
            account=account,
            pair_key=pair_key,
        )
        return primary_order.client_order_id, secondary_order.client_order_id

    async def track_fills(self, exchange_name: ExchangeName) -> None:
        raise RuntimeError("track_fills is handled by Reconciler.")

    async def poll_fills(self, exchange_name: ExchangeName, interval: float) -> None:
        raise RuntimeError("poll_fills is handled by Reconciler.")

    async def cancel_limit(self, exchange_name: ExchangeName, order_id: str) -> bool:
        exchange = self.exchanges[exchange_name]
        async with self._locks[exchange_name]:
            if self.dry_run:
                await self._after_cancel(order_id)
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
            await self._after_cancel(order_id)
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
        await self._record_sequence_event(
            fill.order_id,
            "fill",
            {
                "exchange": exchange_name.value,
                "market_id": fill.market_id,
                "size": fill.size,
                "price": fill.price,
                "is_full": is_full,
            },
        )

        counter_order_id: Optional[str] = None
        counter_exchange: Optional[ExchangeName] = None
        double_record_id: Optional[str] = None
        if self.double_limit_enabled:
            counter = await self._prepare_double_limit_cancel(exchange_name, fill)
            if counter:
                counter_order_id, counter_exchange, double_record_id = counter

        cancel_summary = {
            "attempted": bool(counter_order_id and counter_exchange),
            "order_id": counter_order_id,
            "exchange": counter_exchange.value if counter_exchange else None,
        }
        if counter_order_id and counter_exchange:
            success, attempts, error = await self._cancel_with_retry(
                fill.order_id,
                counter_exchange,
                counter_order_id,
            )
            cancel_summary.update(
                {
                    "success": success,
                    "attempts": attempts,
                    "error": error,
                    "double_limit_id": double_record_id,
                }
            )
        else:
            cancel_summary["skipped"] = True
        await self._record_sequence_event(fill.order_id, "cancel_result", cancel_summary)

        hedge_side = OrderSide.SELL if fill.side == OrderSide.BUY else OrderSide.BUY
        hedge_exchange_name = self.secondary if exchange_name == self.primary else self.primary
        if hedge_exchange_name is None:
            self.logger.warn("hedge exchange not configured")
            return
        hedge_exchange = self.exchanges[hedge_exchange_name]
        hedge_market_id = self._resolve_market_id(exchange_name, hedge_exchange_name, fill.market_id)
        hedge_payload = {
            "hedge_exchange": hedge_exchange_name.value,
            "market_id": hedge_market_id,
            "size": fill.size,
            "side": hedge_side.value,
        }
        try:
            result = await self.hedger.hedge(
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
            hedge_payload.update(
                {
                    "status": "success",
                    "legs": len(result or []),
                }
            )
            if hasattr(self.risk_manager, "decrement"):
                await self.risk_manager.decrement(event_id, fill.size)
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
            hedge_payload.update(
                {
                    "status": "failed",
                    "error": str(exc),
                }
            )
        await self._record_sequence_event(fill.order_id, "hedge", hedge_payload)
        if is_full:
            await self._clear_cancel_task(fill.order_id)
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

    def _resolve_pair_market(
        self,
        pair: MarketPairConfig | None,
        exchange: ExchangeName,
    ) -> str | None:
        if pair:
            if exchange == self.primary:
                candidate = getattr(pair, "primary_market_id", None)
            elif exchange == self.secondary:
                candidate = getattr(pair, "secondary_market_id", None)
            else:
                candidate = None
            if candidate:
                return candidate
        return self.market_map.get(exchange)

    def _build_client_order_id(self, exchange: ExchangeName, suffix: str) -> str:
        prefix = exchange.value.lower()
        return f"dl-{prefix}-{suffix}"

    async def cancel_all_open_orders(self) -> None:
        cancellable_states = {
            OrderFSMState.PLACED,
            OrderFSMState.DOUBLE_LIMIT,
            OrderFSMState.PARTIALLY_FILLED,
        }
        tasks = []
        for order_id, exchange in list(self._order_exchanges.items()):
            fsm = self._fsms.get(order_id)
            if fsm and fsm.current_state in cancellable_states:
                tasks.append(self.cancel_limit(exchange, order_id))
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    self.logger.warn("cancel_all_open_orders encountered error", error=str(result))
        await self._cancel_all_timers()

    async def _attempt_cancel(self, exchange_name: ExchangeName, order: Order | None) -> None:
        if not order:
            return
        order_id = order.order_id or order.client_order_id
        if not order_id:
            return
        try:
            await self.cancel_limit(exchange_name, order_id)
        except Exception as exc:
            self.logger.warn(
                "cleanup cancel failed",
                exchange=exchange_name.value,
                order_id=order_id,
                error=str(exc),
            )

    async def cancel_all_orders(self) -> None:
        pending = list(self._order_exchanges.items())
        for order_id, exchange in pending:
            try:
                await self.cancel_limit(exchange, order_id)
            except Exception as exc:
                self.logger.warn(
                    "cancel_all_orders failed",
                    exchange=exchange.value,
                    order_id=order_id,
                    error=str(exc),
                )

    async def _promote_order_to_double_limit(self, order_key: str | None) -> None:
        if not order_key:
            return
        fsm = self._fsms.get(order_key)
        if not fsm:
            return
        await fsm.transition(
            OrderFSMEvent.DOUBLE_LINKED,
            event_id=f"double-link-{order_key}",
        )

    def _derive_pair_key(self, pair: MarketPairConfig | None) -> str:
        if pair and getattr(pair, "event_id", None):
            return pair.event_id
        if self.event_id:
            return self.event_id
        primary_market = self.market_map.get(self.primary, "")
        secondary_market = self.market_map.get(self.secondary, "")
        return f"{primary_market}:{secondary_market}"

    def _ensure_double_limit_lock(self, record_id: str) -> asyncio.Lock:
        lock = self._double_limit_locks.get(record_id)
        if lock is None:
            lock = asyncio.Lock()
            self._double_limit_locks[record_id] = lock
        return lock

    def _counterparty_from_record(
        self,
        record: Dict[str, object],
        order_ref: str,
    ) -> Tuple[Optional[str], Optional[ExchangeName]]:
        if record.get("order_a_ref") == order_ref:
            exchange_value = record.get("order_b_exchange")
            try:
                exchange = ExchangeName(str(exchange_value))
            except ValueError:
                exchange = None
            other_ref = record.get("order_b_ref")
            return (str(other_ref) if other_ref is not None else None), exchange
        if record.get("order_b_ref") == order_ref:
            exchange_value = record.get("order_a_exchange")
            try:
                exchange = ExchangeName(str(exchange_value))
            except ValueError:
                exchange = None
            other_ref = record.get("order_a_ref")
            return (str(other_ref) if other_ref is not None else None), exchange
        return None, None

    async def _prepare_double_limit_cancel(
        self,
        exchange_name: ExchangeName,
        fill: Fill,
    ) -> Optional[Tuple[str, ExchangeName, str]]:
        record = await self.db.get_double_limit_by_order(fill.order_id)
        if not record or not record.get("id"):
            return None
        record_id = str(record["id"])
        lock = self._ensure_double_limit_lock(record_id)
        async with lock:
            latest = await self.db.get_double_limit_by_order(fill.order_id)
            if not latest:
                return None
            if latest.get("state") != DoubleLimitState.ACTIVE.value:
                return None
            counter_order_id, counter_exchange = self._counterparty_from_record(latest, fill.order_id)
            if not counter_order_id or not counter_exchange:
                return None
            self.logger.debug(
                "double limit trigger",
                record_id=latest["id"],
                fill_exchange=exchange_name.value,
                trigger_order=fill.order_id,
            )
            await self.db.update_double_limit_state(
                record_id,
                DoubleLimitState.TRIGGERED,
                triggered_order_id=fill.order_id,
                cancelled_order_id=counter_order_id,
            )
            return counter_order_id, counter_exchange, record_id

    async def _cancel_with_retry(
        self,
        source_order_id: str,
        exchange_name: ExchangeName,
        cancel_order_id: str,
    ) -> Tuple[bool, int, Optional[str]]:
        attempts = 0
        delay = self._cancel_backoff_base
        last_error: Optional[str] = None
        while attempts < self.cancel_retry_attempts:
            attempts += 1
            try:
                await self.log_hooks.emit(
                    "cancel_attempt",
                    {
                        "order_id": cancel_order_id,
                        "source_order_id": source_order_id,
                        "exchange": exchange_name.value,
                        "attempt": attempts,
                    },
                )
                success = await self.cancel_limit(exchange_name, cancel_order_id)
                if success:
                    return True, attempts, None
            except Exception as exc:
                last_error = str(exc)
                self.logger.warn(
                    "cancel attempt failed",
                    exchange=exchange_name.value,
                    order_id=cancel_order_id,
                    attempt=attempts,
                    error=last_error,
                )
            if attempts < self.cancel_retry_attempts:
                await asyncio.sleep(delay)
                delay *= 2
        await self._record_cancel_failure_incident(cancel_order_id, exchange_name, last_error)
        return False, attempts, last_error

    async def _record_cancel_failure_incident(
        self,
        order_id: str,
        exchange_name: ExchangeName,
        error: Optional[str],
    ) -> None:
        self._cancel_failure_count += 1
        await self.log_hooks.emit(
            "metric",
            {
                "name": "cancel_failures",
                "value": self._cancel_failure_count,
            },
        )
        if hasattr(self.db, "record_incident"):
            await self.db.record_incident(
                "WARNING",
                "cancel_failure",
                {
                    "order_id": order_id,
                    "exchange": exchange_name.value,
                    "error": error or "unknown",
                    "attempts": self.cancel_retry_attempts,
                },
            )
        if self._cancel_failure_count >= self._cancel_alert_threshold:
            await self._notify_cancel_threshold()

    async def _notify_cancel_threshold(self) -> None:
        message = (
            f"Cancel failures exceeded threshold ({self._cancel_alert_threshold}). "
            "Investigate exchange reliability."
        )
        await self._send_alert(message)
        self._cancel_failure_count = 0

    async def _send_alert(self, message: str) -> None:
        notifier = getattr(self.hedger, "notifier", None)
        if not notifier:
            return
        try:
            await notifier.send_message(message)
        except Exception as exc:
            self.logger.warn("alert send failed", error=str(exc))

    async def _record_sequence_event(self, order_id: str, stage: str, payload: Dict[str, object]) -> None:
        log_method = getattr(self.db, "log_order_event", None)
        if not log_method:
            return
        try:
            await log_method(order_id, stage, payload)
        except Exception as exc:
            self.logger.debug(
                "sequence log failed",
                order_id=order_id,
                stage=stage,
                error=str(exc),
            )

    def _fill_key(self, fill: Fill) -> str:
        return f"{fill.order_id}:{fill.timestamp.isoformat()}:{fill.size}"

    async def _schedule_cancel(self, order_id: str, exchange: ExchangeName) -> None:
        if not self._cancel_after_ms or self.dry_run:
            return
        # avoid duplicate timers
        await self._clear_cancel_task(order_id)

        async def _wait_and_cancel():
            try:
                await asyncio.sleep(self._cancel_after_ms / 1000)
                fsm = self._fsms.get(order_id)
                if fsm and fsm.current_state in {
                    OrderFSMState.FILLED,
                    OrderFSMState.CANCELLED,
                    OrderFSMState.FAILED,
                }:
                    return
                await self._record_sequence_event(
                    order_id,
                    "cancel_timeout",
                    {"reason": "cancel_unfilled_after_ms", "ms": self._cancel_after_ms},
                )
                await self._send_alert(
                    f"Auto-cancel triggered for order {order_id} after {self._cancel_after_ms}ms"
                )
                await self.cancel_limit(exchange, order_id)
            except asyncio.CancelledError:
                return

        task = asyncio.create_task(_wait_and_cancel())
        self._cancel_tasks[order_id] = task

    async def _clear_cancel_task(self, order_id: str) -> None:
        task = self._cancel_tasks.pop(order_id, None)
        if task:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    async def _cancel_all_timers(self) -> None:
        tasks = list(self._cancel_tasks.values())
        self._cancel_tasks.clear()
        for task in tasks:
            task.cancel()
        if tasks:
            with suppress(asyncio.CancelledError):
                await asyncio.gather(*tasks, return_exceptions=True)

    async def _after_cancel(self, order_id: str) -> None:
        await self._clear_cancel_task(order_id)
        remaining = self._remaining_unfilled(order_id)
        if remaining > 0 and self.event_id and hasattr(self.risk_manager, "decrement"):
            await self.risk_manager.decrement(self.event_id, remaining)

    def _remaining_unfilled(self, order_id: str) -> float:
        size = self._order_sizes.get(order_id, 0.0)
        filled = self._fill_progress.get(order_id, 0.0)
        remaining = max(0.0, size - filled)
        return remaining

