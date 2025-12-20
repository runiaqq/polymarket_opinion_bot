from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from core.models import ExchangeName, OrderBook, OrderSide, StrategyDirection
from exchanges.orderbook_manager import OrderbookManager


@dataclass(slots=True)
class FeeQuote:
    maker: float = 0.0
    taker: float = 0.0


class SpreadAnalyzer:
    """Evaluates cross-exchange spreads."""

    def __init__(self):
        self.orderbooks = OrderbookManager()

    async def compute_spread(self, primary: OrderBook, secondary: OrderBook) -> float:
        best_bid = await self.orderbooks.best_bid(secondary)
        best_ask = await self.orderbooks.best_ask(primary)
        if not best_bid or not best_ask:
            return 0.0
        return best_bid.price - best_ask.price

    async def is_profitable(self, spread: float, fees: float, min_spread: float) -> bool:
        adjusted = spread - fees
        return adjusted >= min_spread

    @staticmethod
    def compute_net_spread(
        buy_price: float,
        sell_price: float,
        size: float,
        buy_fee: float,
        sell_fee: float,
    ) -> tuple[float, float]:
        buy_cost = buy_price * (1.0 + buy_fee)
        sell_value = sell_price * (1.0 - sell_fee)
        net_per_unit = sell_value - buy_cost
        return net_per_unit, net_per_unit * size

    async def evaluate_opportunity(
        self,
        primary_exchange: ExchangeName,
        secondary_exchange: ExchangeName,
        primary_book: OrderBook,
        secondary_book: OrderBook,
        primary_fees: Any,
        secondary_fees: Any,
        size: float,
        forced_direction: Optional[StrategyDirection] = None,
    ) -> Optional[Dict[str, Any]]:
        best_primary_ask = await self.orderbooks.best_ask(primary_book)
        best_primary_bid = await self.orderbooks.best_bid(primary_book)
        best_secondary_ask = await self.orderbooks.best_ask(secondary_book)
        best_secondary_bid = await self.orderbooks.best_bid(secondary_book)

        scenarios: list[Dict[str, Any]] = []
        primary_maker_fee = self._fee_value(primary_fees, "maker")
        secondary_maker_fee = self._fee_value(secondary_fees, "maker")

        if best_primary_ask and best_secondary_bid:
            net_per, net_total = self.compute_net_spread(
                buy_price=best_primary_ask.price,
                sell_price=best_secondary_bid.price,
                size=size,
                buy_fee=primary_maker_fee,
                sell_fee=secondary_maker_fee,
            )
            scenarios.append(
                {
                    "direction": "primary_buy_secondary_sell",
                    "net_per_unit": net_per,
                    "net_total": net_total,
                    "legs": {
                        primary_exchange: {
                            "side": OrderSide.BUY,
                            "price": best_primary_ask.price,
                        },
                        secondary_exchange: {
                            "side": OrderSide.SELL,
                            "price": best_secondary_bid.price,
                        },
                    },
                }
            )

        if best_secondary_ask and best_primary_bid:
            net_per, net_total = self.compute_net_spread(
                buy_price=best_secondary_ask.price,
                sell_price=best_primary_bid.price,
                size=size,
                buy_fee=secondary_maker_fee,
                sell_fee=primary_maker_fee,
            )
            scenarios.append(
                {
                    "direction": "secondary_buy_primary_sell",
                    "net_per_unit": net_per,
                    "net_total": net_total,
                    "legs": {
                        primary_exchange: {
                            "side": OrderSide.SELL,
                            "price": best_primary_bid.price,
                        },
                        secondary_exchange: {
                            "side": OrderSide.BUY,
                            "price": best_secondary_ask.price,
                        },
                    },
                }
            )

        if forced_direction:
            scenarios = [s for s in scenarios if _matches_direction(s["direction"], forced_direction)]
        if not scenarios:
            return None
        return max(scenarios, key=lambda entry: entry["net_total"])

    def _fee_value(self, fees: Any, attr: str) -> float:
        if fees is None:
            return 0.0
        if isinstance(fees, dict):
            return float(fees.get(attr, 0.0))
        return float(getattr(fees, attr, 0.0))


def _matches_direction(direction: str, forced: "StrategyDirection") -> bool:
    if forced == StrategyDirection.AUTO:
        return True
    if forced == StrategyDirection.A_TO_B:
        return direction == "primary_buy_secondary_sell"
    if forced == StrategyDirection.B_TO_A:
        return direction == "secondary_buy_primary_sell"
    return True

