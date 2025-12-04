from __future__ import annotations

import logging
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

    def _log(self, level: int, msg: str, **context: Any) -> None:
        extra = {"context": context} if context else {"context": "{}"}
        self._logger.log(level, msg, extra=extra)

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

