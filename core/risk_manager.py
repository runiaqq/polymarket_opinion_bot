from __future__ import annotations

from core.exceptions import RiskCheckError
from utils.config_loader import MarketHedgeConfig
from utils.logger import BotLogger


class RiskManager:
    """Performs pre-trade risk validation."""

    def __init__(self, config: MarketHedgeConfig, logger: BotLogger | None = None):
        self.config = config
        self.logger = logger or BotLogger(__name__)
        self._event_limits: dict[str, float] = {}

    async def check_balance(self, exchange, required: float, asset: str = "USDC") -> None:
        balances = await exchange.get_balances()
        available = float(balances.get(asset, 0))
        if available < required:
            self.logger.warn(
                "balance check failed",
                required=required,
                available=available,
                asset=asset,
                exchange=exchange.__class__.__name__,
            )
            raise RiskCheckError("insufficient balance")

    async def check_limits(self, event_id: str, size: float) -> None:
        current = self._event_limits.get(event_id, 0.0)
        if size > self.config.max_position_size_per_market:
            raise RiskCheckError("size exceeds per-market limit")
        if current + size > self.config.max_position_size_per_event:
            raise RiskCheckError("size exceeds per-event limit")
        self._event_limits[event_id] = current + size

    async def check_slippage(self, slippage: float, max_slippage: float) -> None:
        if slippage > max_slippage:
            raise RiskCheckError("slippage exceeds threshold")

    async def decrement(self, event_id: str, size: float) -> None:
        if size <= 0:
            return
        current = self._event_limits.get(event_id, 0.0)
        new_value = max(0.0, current - size)
        self._event_limits[event_id] = new_value
        self.logger.debug("exposure decremented", event_id=event_id, size=size, remaining=new_value)

