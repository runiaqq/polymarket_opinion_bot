from __future__ import annotations

import logging
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict


def _build_logger(
    name: str,
    level: int,
    log_file: Path | None,
) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(level)
    handler: logging.Handler
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s | %(context)s"
    )
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handler = RotatingFileHandler(log_file, maxBytes=5_000_000, backupCount=5)
    else:
        handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    class ContextFilter(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:  # noqa: D401
            if not hasattr(record, "context"):
                record.context = "{}"
            return True

    handler.addFilter(ContextFilter())
    logger.addHandler(handler)
    return logger


class BotLogger:
    """Structured logger with optional context binding."""

    def __init__(
        self,
        name: str = "market_hedge",
        level: int = logging.INFO,
        log_file: Path | None = None,
    ):
        self._logger = _build_logger(name, level, log_file)
        self._sink = None
        self._sink_min_interval = 60.0
        self._sink_last_ts = 0.0

    def _log(self, level: int, msg: str, **context: Any) -> None:
        extra = {"context": context} if context else {"context": "{}"}
        self._logger.log(level, msg, extra=extra)
        if self._sink and level >= logging.WARNING:
            now = time.monotonic()
            if now - self._sink_last_ts >= self._sink_min_interval:
                self._sink_last_ts = now
                try:
                    self._sink(level, msg, context)
                except Exception:
                    pass

    def debug(self, msg: str, **context: Any) -> None:
        self._log(logging.DEBUG, msg, **context)

    def info(self, msg: str, **context: Any) -> None:
        self._log(logging.INFO, msg, **context)

    def warn(self, msg: str, **context: Any) -> None:
        self._log(logging.WARNING, msg, **context)

    def error(self, msg: str, **context: Any) -> None:
        self._log(logging.ERROR, msg, **context)

    def exception(self, msg: str, **context: Any) -> None:
        self._log(logging.ERROR, msg, **context)

    def bind_sink(self, sink, min_interval: float = 60.0) -> None:
        """Forward warnings/errors to an external sink (e.g., Telegram) with rate limiting."""
        self._sink = sink
        self._sink_min_interval = max(1.0, min_interval)
        self._sink_last_ts = 0.0

    def set_level(self, level: int) -> None:
        self._logger.setLevel(level)

