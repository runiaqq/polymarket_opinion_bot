from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import List, Optional

from core.exceptions import HedgingError, RiskCheckError
from core.models import ExchangeName, OrderSide, Trade
from exchanges.orderbook_manager import OrderbookManager
from utils.config_loader import MarketHedgeConfig
from utils.logger import BotLogger


class HedgeStrategy(str, Enum):
    FULL = "FULL"
    PARTIAL_IF_SAFER = "PARTIAL_IF_SAFER"
    SKIP_IF_TOO_EXPENSIVE = "SKIP_IF_TOO_EXPENSIVE"


@dataclass(slots=True)
class HedgeLegRequest:
    client: object
    exchange: ExchangeName
    market_id: str
    weight: float = 1.0


class Hedger:
    """Handles hedge execution across venues."""

    def __init__(
        self,
        config: MarketHedgeConfig,
        risk_manager,
        orderbook_manager: OrderbookManager,
        database,
        notifier,
        logger: BotLogger | None = None,
        dry_run: bool = False,
    ):
        self.config = config
        self.risk_manager = risk_manager
        self.orderbooks = orderbook_manager
        self.db = database
        self.notifier = notifier
        self.logger = logger or BotLogger(__name__)
        self.dry_run = dry_run
        try:
            self.strategy = HedgeStrategy(self.config.hedge_strategy.upper())
        except Exception:
            self.strategy = HedgeStrategy.FULL

    async def hedge(
        self,
        legs: List[HedgeLegRequest],
        event_id: str,
        side: OrderSide,
        size: float,
        reference_price: float,
        entry_order_id: str,
        entry_exchange: ExchangeName,
    ):
        if size <= 0 or not legs:
            raise HedgingError("no hedge legs provided")

        target_size = size * self.config.hedge_ratio
        weight_sum = sum(max(leg.weight, 0) for leg in legs) or len(legs)
        executed = []
        tx_conn = await self.db.begin_transaction()
        try:
            for leg in legs:
                leg_weight = leg.weight if leg.weight > 0 else 1.0
                leg_size = target_size * (leg_weight / weight_sum)
                if leg_size <= 0:
                    continue
                try:
                    execution = await self._execute_leg(leg, side, leg_size, reference_price)
                    if execution:
                        executed.append(execution)
                except (RiskCheckError, HedgingError) as exc:
                    if self.strategy == HedgeStrategy.PARTIAL_IF_SAFER:
                        self.logger.warn("leg skipped", exchange=leg.exchange.value, error=str(exc))
                        continue
                    elif self.strategy == HedgeStrategy.SKIP_IF_TOO_EXPENSIVE:
                        await self.db.rollback_transaction(tx_conn)
                        await self._handle_failure("hedge skipped due to strategy", {"error": str(exc)})
                        return None
                    else:
                        raise
            if not executed:
                raise HedgingError("no hedge legs executed")

            total_hedge = sum(entry["size"] for entry in executed)
            weighted_price = (
                sum(entry["price"] * entry["size"] for entry in executed) / total_hedge
            )
            pnl_estimate = (reference_price - weighted_price) * total_hedge
            hedge_ids = ",".join(entry["order_id"] for entry in executed)
            trade = Trade(
                entry_order_id=entry_order_id,
                hedge_order_id=hedge_ids or "dry-run",
                event_id=event_id,
                entry_exchange=entry_exchange,
                hedge_exchange=executed[0]["exchange"],
                entry_price=reference_price,
                hedge_price=weighted_price,
                size=size,
                hedge_size=total_hedge,
                pnl_estimate=pnl_estimate,
                timestamp=datetime.now(tz=timezone.utc),
            )
            await self.db.save_trade(trade, tx_conn=tx_conn)
            await self.db.commit_transaction(tx_conn)
            await self.notifier.send_message(
                f"Hedged {total_hedge:.2f} units across {len(executed)} leg(s) at {weighted_price:.4f}. "
                f"Estimated PnL: {pnl_estimate:.4f}"
            )
            self.logger.info(
                "hedge completed",
                legs=len(executed),
                hedge_size=total_hedge,
                hedge_price=weighted_price,
            )
            return executed
        except Exception as exc:
            await self.db.rollback_transaction(tx_conn)
            await self._handle_failure("hedge failed", {"error": str(exc)})
            raise

    async def _execute_leg(
        self,
        request: HedgeLegRequest,
        side: OrderSide,
        leg_size: float,
        reference_price: float,
    ):
        orderbook = await request.client.get_orderbook(request.market_id)
        target_size = leg_size
        avg_price, slippage = self.orderbooks.estimate_slippage(orderbook, side, target_size)
        try:
            await self.risk_manager.check_slippage(
                abs(slippage),
                self.config.max_slippage_market_hedge,
            )
        except RiskCheckError:
            target_size = self._reduce_size(orderbook, side, target_size)
            if target_size <= 0:
                raise
            avg_price, slippage = self.orderbooks.estimate_slippage(orderbook, side, target_size)
            await self.risk_manager.check_slippage(
                abs(slippage),
                self.config.max_slippage_market_hedge,
            )
        if self.dry_run:
            order_id = "dry-run"
        else:
            order = await request.client.place_market_order(
                market_id=request.market_id,
                side=side,
                size=target_size,
                client_order_id=None,
            )
            order_id = order.order_id
        return {
            "order_id": order_id,
            "exchange": request.exchange,
            "market_id": request.market_id,
            "size": target_size,
            "price": avg_price,
        }

    async def validate_slippage(self, expected_slippage: float, max_slippage: float) -> bool:
        return abs(expected_slippage) <= max_slippage

    def _reduce_size(self, orderbook, side: OrderSide, size: float) -> float:
        step = size * 0.1
        current_size = size
        while current_size > 0:
            avg, slippage = self.orderbooks.estimate_slippage(orderbook, side, current_size)
            if abs(slippage) <= self.config.max_slippage_market_hedge:
                return current_size
            current_size -= step
        return 0.0

    async def _handle_failure(self, message: str, details: dict) -> None:
        await self.db.record_incident("ERROR", message, details)
        await self.notifier.send_message(f"[Hedge Failure] {message}: {details}")
        self.logger.error(message, **details)

