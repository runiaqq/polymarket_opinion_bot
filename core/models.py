from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional


class OrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(str, Enum):
    LIMIT = "LIMIT"
    MARKET = "MARKET"


class OrderStatus(str, Enum):
    PENDING = "PENDING"
    OPEN = "OPEN"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"


class ExchangeName(str, Enum):
    POLYMARKET = "Polymarket"
    OPINION = "Opinion"


class DoubleLimitState(str, Enum):
    ACTIVE = "ACTIVE"
    TRIGGERED = "TRIGGERED"
    COMPLETED = "COMPLETED"


@dataclass(slots=True)
class OrderBookEntry:
    price: float
    size: float


@dataclass(slots=True)
class OrderBook:
    market_id: str
    bids: List[OrderBookEntry] = field(default_factory=list)
    asks: List[OrderBookEntry] = field(default_factory=list)


@dataclass(slots=True)
class Market:
    market_id: str
    name: str
    exchange: ExchangeName
    status: str | None = None
    extra: Dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class Order:
    order_id: str
    client_order_id: str
    market_id: str
    exchange: ExchangeName
    side: OrderSide
    order_type: OrderType
    price: float
    size: float
    filled_size: float
    status: OrderStatus
    created_at: datetime


@dataclass(slots=True)
class Fill:
    order_id: str
    market_id: str
    exchange: ExchangeName
    side: OrderSide
    price: float
    size: float
    fee: float
    timestamp: datetime


@dataclass(slots=True)
class Trade:
    entry_order_id: str
    hedge_order_id: str
    event_id: str
    entry_exchange: ExchangeName
    hedge_exchange: ExchangeName
    entry_price: float
    hedge_price: float
    size: float
    hedge_size: float
    pnl_estimate: float
    timestamp: datetime


@dataclass(slots=True)
class Position:
    event_id: str
    net_position: float
    last_price: Optional[float]
    updated_at: datetime


@dataclass(slots=True)
class AccountCredentials:
    account_id: str
    exchange: ExchangeName
    api_key: str
    secret_key: str
    passphrase: Optional[str] = None
    wallet_address: Optional[str] = None
    proxy: Optional[str] = None
    metadata: Dict[str, str] = field(default_factory=dict)
    weight: float = 1.0
    tokens_per_sec: float = 5.0
    burst: int = 10

