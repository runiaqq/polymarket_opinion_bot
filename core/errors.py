from __future__ import annotations


class BaseWrappedError(Exception):
    """Common wrapper that preserves the original exception."""

    def __init__(self, message: str, original: Exception | None = None):
        super().__init__(message)
        self.original = original


class ExchangeError(BaseWrappedError):
    pass


class NetworkError(BaseWrappedError):
    pass


class ValidationError(BaseWrappedError):
    pass


class HedgingError(BaseWrappedError):
    pass


class ReconciliationError(BaseWrappedError):
    pass


class DatabaseError(BaseWrappedError):
    pass
class BaseError(Exception):
    def __init__(self, message: str, original: Exception | None = None):
        super().__init__(message)
        self.original = original


class ExchangeError(BaseError):
    """Generic exchange error."""


class NetworkError(BaseError):
    """Network layer failure."""


class ValidationError(BaseError):
    """Raised when canonical data validation fails."""


class HedgingError(BaseError):
    """Raised when hedging is unsafe."""


class ReconciliationError(BaseError):
    """Raised when reconciliation fails."""


class DatabaseError(BaseError):
    """Raised when database operations fail."""

