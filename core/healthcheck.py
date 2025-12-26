from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from core.models import ExchangeName, OrderBook, OrderSide
from core.spread_analyzer import SpreadAnalyzer
from exchanges.orderbook_manager import OrderbookManager
from utils.config_loader import FeeConfig, MarketPairConfig
from utils.logger import BotLogger


@dataclass(slots=True)
class HealthcheckResult:
    pair_id: str
    primary_exchange: ExchangeName
    secondary_exchange: ExchangeName
    primary_status: str
    secondary_status: str
    primary_top: Dict[str, float | None]
    secondary_top: Dict[str, float | None]
    spreads: Dict[str, Dict[str, float]]
    chosen_direction: Optional[str]
    net_total: Optional[float]
    error: Optional[str]
    checked_at: str


class HealthcheckService:
    """Read-only connectivity and pricing checks for enabled pairs."""

    def __init__(
        self,
        spread_analyzer: SpreadAnalyzer,
        orderbook_manager: OrderbookManager,
        account_pools: Dict[ExchangeName, List[Any]],
        clients_by_id: Dict[str, Any],
        fees: Dict[ExchangeName, FeeConfig],
        logger: BotLogger | None = None,
    ):
        self.spread_analyzer = spread_analyzer
        self.orderbook_manager = orderbook_manager
        self.account_pools = account_pools
        self.clients_by_id = clients_by_id
        self.fees = fees
        self.logger = logger or BotLogger(__name__)

    async def run(self, pairs: List[MarketPairConfig], size: float = 1.0) -> List[HealthcheckResult]:
        results: List[HealthcheckResult] = []
        for pair in pairs:
            try:
                result = await self._check_pair(pair, size=size)
            except Exception as exc:  # pragma: no cover - defensive catch
                self.logger.warn("healthcheck pair failure", pair_id=pair.event_id, error=str(exc))
                result = HealthcheckResult(
                    pair_id=pair.event_id,
                    primary_exchange=pair.primary_exchange or ExchangeName.POLYMARKET,
                    secondary_exchange=pair.secondary_exchange or ExchangeName.OPINION,
                    primary_status="FAIL",
                    secondary_status="FAIL",
                    primary_top={"bid": None, "ask": None},
                    secondary_top={"bid": None, "ask": None},
                    spreads={},
                    chosen_direction=None,
                    net_total=None,
                    error=str(exc),
                    checked_at=datetime.now(tz=timezone.utc).isoformat(),
                )
            results.append(result)
        return results

    async def _check_pair(self, pair: MarketPairConfig, size: float) -> HealthcheckResult:
        primary_exchange = pair.primary_exchange or ExchangeName.POLYMARKET
        secondary_exchange = pair.secondary_exchange or ExchangeName.OPINION
        primary_client = self._resolve_client(primary_exchange, pair.primary_account_id)
        secondary_client = self._resolve_client(secondary_exchange, pair.secondary_account_id)

        primary_ob: Optional[OrderBook] = None
        secondary_ob: Optional[OrderBook] = None
        primary_status = "OK"
        secondary_status = "OK"
        error: Optional[str] = None
        try:
            primary_ob = await primary_client.get_orderbook(pair.primary_market_id)
        except Exception as exc:
            primary_status = "FAIL"
            error = str(exc)
        try:
            secondary_ob = await secondary_client.get_orderbook(pair.secondary_market_id)
        except Exception as exc:
            secondary_status = "FAIL"
            error = error or str(exc)

        spreads: Dict[str, Dict[str, float]] = {}
        chosen_direction: Optional[str] = None
        net_total: Optional[float] = None
        if primary_ob and secondary_ob:
            spreads = self._compute_spreads(
                primary_ob,
                secondary_ob,
                primary_exchange,
                secondary_exchange,
                size,
                pair,
            )
            scenario = await self.spread_analyzer.evaluate_opportunity(
                primary_exchange=primary_exchange,
                secondary_exchange=secondary_exchange,
                primary_book=primary_ob,
                secondary_book=secondary_ob,
                primary_fees=self.fees.get(primary_exchange, FeeConfig()),
                secondary_fees=self.fees.get(secondary_exchange, FeeConfig()),
                size=size,
                forced_direction=pair.strategy_direction,
            )
            if scenario:
                chosen_direction = scenario.get("direction")
                net_total = float(scenario.get("net_total", 0.0))

        return HealthcheckResult(
            pair_id=pair.event_id,
            primary_exchange=primary_exchange,
            secondary_exchange=secondary_exchange,
            primary_status=primary_status,
            secondary_status=secondary_status,
            primary_top=self._top_of_book(primary_ob),
            secondary_top=self._top_of_book(secondary_ob),
            spreads=spreads,
            chosen_direction=chosen_direction,
            net_total=net_total,
            error=error,
            checked_at=datetime.now(tz=timezone.utc).isoformat(),
        )

    def _top_of_book(self, orderbook: Optional[OrderBook]) -> Dict[str, float | None]:
        if not orderbook:
            return {"bid": None, "ask": None}
        best_bid = orderbook.bids[0].price if orderbook.bids else None
        best_ask = orderbook.asks[0].price if orderbook.asks else None
        return {"bid": best_bid, "ask": best_ask}

    def _resolve_client(self, exchange: ExchangeName, preferred_id: Optional[str]):
        if preferred_id and preferred_id in self.clients_by_id:
            return self.clients_by_id[preferred_id]
        pool = self.account_pools.get(exchange, [])
        if not pool:
            raise RuntimeError(f"no accounts available for {exchange.value}")
        account = pool[0]
        client = self.clients_by_id.get(account.account_id)
        if not client:
            raise RuntimeError(f"no client for account {account.account_id}")
        return client

    def _compute_spreads(
        self,
        primary_ob: OrderBook,
        secondary_ob: OrderBook,
        primary_exchange: ExchangeName,
        secondary_exchange: ExchangeName,
        size: float,
        pair: MarketPairConfig,
    ) -> Dict[str, Dict[str, float]]:
        spreads: Dict[str, Dict[str, float]] = {}
        primary_fees = self.fees.get(primary_exchange, FeeConfig())
        secondary_fees = self.fees.get(secondary_exchange, FeeConfig())
        best_primary_ask = primary_ob.asks[0] if primary_ob.asks else None
        best_primary_bid = primary_ob.bids[0] if primary_ob.bids else None
        best_secondary_ask = secondary_ob.asks[0] if secondary_ob.asks else None
        best_secondary_bid = secondary_ob.bids[0] if secondary_ob.bids else None

        if best_primary_ask and best_secondary_bid:
            per, total = self.spread_analyzer.compute_net_spread(
                buy_price=best_primary_ask.price,
                sell_price=best_secondary_bid.price,
                size=size,
                buy_fee=primary_fees.maker,
                sell_fee=secondary_fees.maker,
            )
            spreads["primary_buy_secondary_sell"] = {"per_unit": per, "total": total}

        if best_secondary_ask and best_primary_bid:
            per, total = self.spread_analyzer.compute_net_spread(
                buy_price=best_secondary_ask.price,
                sell_price=best_primary_bid.price,
                size=size,
                buy_fee=secondary_fees.maker,
                sell_fee=primary_fees.maker,
            )
            spreads["secondary_buy_primary_sell"] = {"per_unit": per, "total": total}
        return spreads


