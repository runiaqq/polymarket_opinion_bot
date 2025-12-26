from __future__ import annotations

import asyncio
import time
from typing import Optional


class AsyncTokenBucket:
    """Simple async token bucket for per-account rate limiting."""

    def __init__(self, tokens_per_second: float, burst: int):
        self.tokens_per_second = max(tokens_per_second, 0.0001)
        self.burst = max(burst, 1)
        self._tokens = float(self.burst)
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, amount: int = 1) -> None:
        if amount > self.burst:
            raise ValueError("request exceeds bucket capacity")
        async with self._lock:
            await self._refill_locked()
            while self._tokens < amount:
                wait_time = (amount - self._tokens) / self.tokens_per_second
                await asyncio.sleep(wait_time)
                await self._refill_locked()
            self._tokens -= amount

    async def try_acquire(self, amount: int = 1) -> bool:
        async with self._lock:
            await self._refill_locked()
            if self._tokens >= amount:
                self._tokens -= amount
                return True
            return False

    async def _refill_locked(self) -> None:
        now = time.monotonic()
        elapsed = max(0.0, now - self._last_refill)
        self._last_refill = now
        refill = elapsed * self.tokens_per_second
        if refill > 0:
            self._tokens = min(self.burst, self._tokens + refill)







