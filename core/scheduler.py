from __future__ import annotations

import asyncio
import random
from typing import Dict, List, Optional

from utils.account_pool import AccountPool, AccountWorker
from utils.logger import BotLogger


class TaskScheduler:
    """Dispatches tasks to healthy accounts using configurable policies."""

    def __init__(
        self,
        pool: AccountPool,
        policy: str = "round_robin",
        logger: BotLogger | None = None,
    ):
        self.pool = pool
        self.policy = policy.lower()
        self.logger = logger or BotLogger(__name__)

    async def assign(self) -> Optional[AccountWorker]:
        worker = await self.pool.acquire_worker(self.policy)
        if not worker:
            self.logger.warn("no healthy accounts available")
            return None
        can_run = await worker.rate_limiter.try_acquire()
        if not can_run:
            await self.pool.release_worker(worker)
            return None
        return worker

    async def release(self, worker: AccountWorker) -> None:
        await self.pool.release_worker(worker)






