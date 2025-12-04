from __future__ import annotations

from typing import Dict

from core.models import OrderSide
from utils.logger import BotLogger


class PositionTracker:
    """Tracks net positions across events."""

    def __init__(self, database, logger: BotLogger | None = None):
        self.db = database
        self.logger = logger or BotLogger(__name__)
        self._cache: Dict[str, float] = {}

    async def add_fill(self, event_id: str, size: float, price: float, side: OrderSide) -> None:
        delta = size if side == OrderSide.BUY else -size
        net = self._cache.get(event_id, 0.0) + delta
        self._cache[event_id] = net
        await self.db.upsert_position(event_id, net, price)
        self.logger.debug(
            "position updated",
            event_id=event_id,
            net=net,
            price=price,
        )

    async def get_net_position(self, event_id: str) -> float:
        if event_id in self._cache:
            return self._cache[event_id]
        position = await self.db.get_position(event_id)
        value = position.net_position if position else 0.0
        self._cache[event_id] = value
        return value

    async def get_unhedged(self, event_id: str) -> float:
        net = await self.get_net_position(event_id)
        return net

