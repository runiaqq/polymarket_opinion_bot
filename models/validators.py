from __future__ import annotations

from decimal import Decimal

from models import canonical as canon


def validate_market(market: canon.Market) -> None:
    if not market.id or not market.symbol:
        raise ValueError("market id/symbol required")
    if not isinstance(market.metadata, dict):
        raise ValueError("market metadata must be dict")


def validate_order(order: canon.Order) -> None:
    if not order.client_order_id:
        raise ValueError("client_order_id required")
    if not order.exchange:
        raise ValueError("exchange required")
    if not order.market_id:
        raise ValueError("market_id required")
    if order.size <= Decimal("0"):
        raise ValueError("order size must be positive")
    if order.price is not None and order.price <= Decimal("0"):
        raise ValueError("order price must be positive when provided")
    if order.filled_size < Decimal("0"):
        raise ValueError("filled_size cannot be negative")


def validate_fill(fill: canon.Fill) -> None:
    if not fill.order_id:
        raise ValueError("fill order_id required")
    if fill.size <= Decimal("0"):
        raise ValueError("fill size must be positive")
    if fill.price <= Decimal("0"):
        raise ValueError("fill price must be positive")
    if fill.side not in {"BUY", "SELL"}:
        raise ValueError("fill side invalid")


def validate_trade(trade: canon.Trade) -> None:
    if not trade.entry_order_id or not trade.hedge_order_id:
        raise ValueError("trade order ids required")
    if trade.size <= Decimal("0") or trade.hedge_size <= Decimal("0"):
        raise ValueError("trade sizes must be positive")


def validate_orderbook(orderbook: canon.OrderBook) -> None:
    if not orderbook.market_id:
        raise ValueError("orderbook market_id required")
    for side in (orderbook.bids, orderbook.asks):
        for entry in side:
            if entry.price <= Decimal("0") or entry.size < Decimal("0"):
                raise ValueError("orderbook entries invalid")

