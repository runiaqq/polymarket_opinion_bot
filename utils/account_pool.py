from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from core.models import AccountCredentials, ExchangeName
from utils.logger import BotLogger
from utils.token_bucket import AsyncTokenBucket


@dataclass(slots=True)
class AccountWorker:
    credentials: AccountCredentials
    session: Optional[object]
    rate_limiter: AsyncTokenBucket
    weight: float
    healthy: bool = True
    last_health_check: float = field(default_factory=lambda: 0.0)
    active_tasks: int = 0


class AccountPool:
    """Manages per-account sessions, rate limiting, and health state."""

    def __init__(
        self,
        accounts: List[AccountCredentials],
        create_session,
        health_check,
        logger: BotLogger | None = None,
        health_interval: float = 60.0,
    ):
        self.logger = logger or BotLogger(__name__)
        self._create_session = create_session
        self._health_check = health_check
        self._health_interval = max(health_interval, 10.0)
        self._workers: Dict[str, AccountWorker] = {}
        self._lock = asyncio.Lock()
        self._rr_order: List[str] = []
        self._rr_cursor: int = 0
        for acc in accounts:
            rate_limiter = AsyncTokenBucket(acc.tokens_per_sec, acc.burst)
            self._workers[acc.account_id] = AccountWorker(
                credentials=acc,
                session=None,
                rate_limiter=rate_limiter,
                weight=max(acc.weight, 0.0),
            )
            self._rr_order.append(acc.account_id)

    async def ensure_session(self, account_id: str):
        worker = self._workers[account_id]
        if worker.session:
            return worker.session
        session = await self._create_session(worker.credentials)
        worker.session = session
        return session

    async def schedule_health_checks(self) -> None:
        while True:
            await asyncio.sleep(self._health_interval)
            await self._run_health_checks()

    async def _run_health_checks(self) -> None:
        async with self._lock:
            workers = list(self._workers.values())
        tasks = [self._check_worker(worker) for worker in workers]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _check_worker(self, worker: AccountWorker) -> None:
        now = time.monotonic()
        if now - worker.last_health_check < self._health_interval / 2:
            return
        worker.last_health_check = now
        try:
            session = await self.ensure_session(worker.credentials.account_id)
            healthy = await self._health_check(worker.credentials, session)
            worker.healthy = healthy
        except Exception as exc:  # pragma: no cover - defensive
            worker.healthy = False
            self.logger.warn("account health check failed", account=worker.credentials.account_id, error=str(exc))

    async def acquire_worker(self, policy: str = "round_robin") -> Optional[AccountWorker]:
        async with self._lock:
            candidates = [worker for worker in self._workers.values() if worker.healthy]
            if not candidates:
                return None
            worker: Optional[AccountWorker]
            if policy == "least_loaded":
                worker = min(candidates, key=lambda w: (w.active_tasks, w.weight))
            elif policy == "random":
                worker = random.choice(candidates)
            elif policy == "weighted":
                weights = [max(0.0, w.weight) for w in candidates]
                total = sum(weights)
                if total <= 0:
                    worker = random.choice(candidates)
                else:
                    pick = random.random() * total
                    cumulative = 0.0
                    worker = candidates[0]
                    for w, weight in zip(candidates, weights):
                        cumulative += weight
                        if pick <= cumulative:
                            worker = w
                            break
            else:  # round_robin default
                worker = None
                total_accounts = len(self._rr_order)
                for _ in range(total_accounts):
                    account_id = self._rr_order[self._rr_cursor % total_accounts]
                    self._rr_cursor = (self._rr_cursor + 1) % total_accounts
                    candidate = self._workers[account_id]
                    if candidate.healthy:
                        worker = candidate
                        break
                if worker is None:
                    return None
            worker.active_tasks += 1
            return worker

    async def release_worker(self, worker: AccountWorker) -> None:
        async with self._lock:
            worker.active_tasks = max(0, worker.active_tasks - 1)

    def export_state(self) -> Dict[str, object]:
        return {
            account_id: {
                "healthy": worker.healthy,
                "active_tasks": worker.active_tasks,
                "weight": worker.weight,
            }
            for account_id, worker in self._workers.items()
        }

