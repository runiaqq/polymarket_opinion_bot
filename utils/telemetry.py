from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import Dict, Optional

from utils.logger import BotLogger

try:
    from prometheus_client import Counter, Histogram, start_http_server

    PROM_AVAILABLE = True
except ImportError:
    PROM_AVAILABLE = False


class Telemetry:
    """Minimal telemetry wrapper with optional Prometheus integration."""

    def __init__(self, enable_prometheus: bool = False, port: int = 9000, logger: BotLogger | None = None):
        self.logger = logger or BotLogger("telemetry")
        self.enable_prometheus = enable_prometheus and PROM_AVAILABLE
        if self.enable_prometheus:
            start_http_server(port)
            self.hedge_attempts = Counter("hedge_attempts", "Number of hedge attempts")
            self.hedge_success = Counter("hedge_success", "Number of successful hedges")
            self.hedge_failures = Counter("hedge_failures", "Number of failed hedges")
            self.slippage_histogram = Histogram("hedge_slippage", "Observed hedge slippage")
        else:
            self._counters: Dict[str, int] = defaultdict(int)
            self._slippage = []
            self._task: Optional[asyncio.Task] = None

    def start(self) -> None:
        if self.enable_prometheus:
            return
        loop = asyncio.get_running_loop()
        self._task = loop.create_task(self._logger_loop())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task

    def inc_attempt(self):
        if self.enable_prometheus:
            self.hedge_attempts.inc()
        else:
            self._counters["hedge_attempts"] += 1

    def inc_success(self):
        if self.enable_prometheus:
            self.hedge_success.inc()
        else:
            self._counters["hedge_success"] += 1

    def inc_failure(self):
        if self.enable_prometheus:
            self.hedge_failures.inc()
        else:
            self._counters["hedge_failures"] += 1

    def observe_slippage(self, value: float):
        if self.enable_prometheus:
            self.slippage_histogram.observe(value)
        else:
            self._slippage.append(value)

    async def _logger_loop(self):
        while True:
            await asyncio.sleep(60)

            counts = dict(self._counters)
            if self._slippage:
                avg_slippage = sum(self._slippage) / len(self._slippage)
            else:
                avg_slippage = 0.0
            counts["avg_slippage"] = avg_slippage
            self.logger.info("telemetry snapshot", **counts)
            self._slippage.clear()


import contextlib

