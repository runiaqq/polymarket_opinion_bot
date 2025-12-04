from core.errors import (
    DatabaseError,
    ExchangeError,
    HedgingError,
    NetworkError,
    ReconciliationError,
    ValidationError,
)


class RecoverableExchangeError(ExchangeError):
    """Raised for transient exchange errors that are safe to retry."""


class FatalExchangeError(ExchangeError):
    """Raised for permanent exchange errors."""


class RiskCheckError(Exception):
    """Raised when a risk check fails."""

