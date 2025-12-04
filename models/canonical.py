from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Literal, Optional


@dataclass(slots=True)
class Market:
    id: str
    source: str
    symbol: str
    metadata: Dict[str, object] = field(default_factory=dict)


@dataclass(slots=True)
class Order:
    client_order_id: str
    exchange: str
    order_id: Optional[str]
    market_id: str
    side: Literal["BUY", "SELL"]
    price: Optional[Decimal]
    size: Decimal
    filled_size: Decimal = Decimal("0")
    status: str = "PENDING"
    ts: datetime = datetime.utcnow()


@dataclass(slots=True)
class Fill:
    order_id: str
    exchange: str
    fill_id: Optional[str]
    size: Decimal
    price: Decimal
    side: Literal["BUY", "SELL"]
    ts: datetime


@dataclass(slots=True)
class Trade:
    entry_order_id: str
    hedge_order_id: str
    entry_exchange: str
    hedge_exchange: str
    size: Decimal
    price_entry: Decimal
    price_hedge: Decimal
    fees: Decimal
    pnl_estimated: Decimal
    ts: datetime


@dataclass(slots=True)
class OrderBookEntry:
    price: Decimal
    size: Decimal


@dataclass(slots=True)
class OrderBook:
    market_id: str
    bids: List[OrderBookEntry]
    asks: List[OrderBookEntry]
    ts: datetime

