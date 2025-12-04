from __future__ import annotations

from collections import defaultdict
from typing import Awaitable, Callable, Dict, List, Optional


LogCallback = Callable[[Dict[str, object]], Awaitable[None] | None]


class LogHooks:
    """Optional callback registry for detailed instrumentation."""

    def __init__(self, enabled: bool = True):
        self.enabled = enabled
        self._callbacks: Dict[str, List[LogCallback]] = defaultdict(list)

    def register(self, event: str, callback: LogCallback) -> None:
        if self.enabled:
            self._callbacks[event].append(callback)

    async def emit(self, event: str, payload: Optional[Dict[str, object]] = None) -> None:
        if not self.enabled:
            return
        payload = payload or {}
        callbacks = self._callbacks.get(event, [])
        for cb in callbacks:
            result = cb(payload)
            if result and hasattr(result, "__await__"):
                await result
