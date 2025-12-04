import pytest

from core.models import OrderSide
from exchanges.orderbook_manager import OrderbookManager


def test_estimate_slippage_buy_side():
    manager = OrderbookManager()
    orderbook = manager.parse_orderbook(
        market_id="m",
        bids=[{"price": 0.45, "size": 50}],
        asks=[
            {"price": 0.50, "size": 40},
            {"price": 0.55, "size": 60},
        ],
    )
    avg_price, slippage = manager.estimate_slippage(orderbook, OrderSide.BUY, 80)
    assert avg_price == pytest.approx(0.525, abs=0.0001)
    assert slippage == pytest.approx(0.025, abs=0.0001)


def test_get_best_price_for_size():
    manager = OrderbookManager()
    orderbook = manager.parse_orderbook(
        "m",
        bids=[{"price": 0.48, "size": 10}],
        asks=[{"price": 0.52, "size": 5}, {"price": 0.54, "size": 10}],
    )
    price = manager.get_best_price_for_size(orderbook, OrderSide.BUY, 8)
    assert price == 0.54

