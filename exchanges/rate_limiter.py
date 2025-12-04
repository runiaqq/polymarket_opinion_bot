from __future__ import annotations

import asyncio
import time


class RateLimiter:
    """Simple asyncio rate limiter using token bucket semantics."""

    def __init__(self, requests_per_minute: int = 60, burst: int = 5):
        self.requests_per_minute = max(1, requests_per_minute)
        self.interval = 60.0 / self.requests_per_minute
        self.burst = max(1, burst)
        self._semaphore = asyncio.BoundedSemaphore(self.burst)
        self._lock = asyncio.Lock()
        self._last_request_at = 0.0

    async def acquire(self) -> None:
        await self._semaphore.acquire()
        async with self._lock:
            now = time.monotonic()
            wait_time = self.interval - (now - self._last_request_at)
            if wait_time > 0:
                await asyncio.sleep(wait_time)
            self._last_request_at = time.monotonic()

        asyncio.create_task(self._delayed_release())

    async def _delayed_release(self) -> None:
        await asyncio.sleep(self.interval)
        self._semaphore.release()

