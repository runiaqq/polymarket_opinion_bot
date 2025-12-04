import pytest

from core.models import OrderBook, OrderBookEntry
from core.spread_analyzer import SpreadAnalyzer


@pytest.mark.asyncio
async def test_compute_spread_positive():
    analyzer = SpreadAnalyzer()
    primary = OrderBook(
        market_id="primary",
        bids=[],
        asks=[OrderBookEntry(price=0.48, size=100)],
    )
    secondary = OrderBook(
        market_id="secondary",
        bids=[OrderBookEntry(price=0.52, size=200)],
        asks=[],
    )
    spread = await analyzer.compute_spread(primary, secondary)
    assert spread == pytest.approx(0.04, abs=0.001)


@pytest.mark.asyncio
async def test_is_profitable_considers_fees():
    analyzer = SpreadAnalyzer()
    assert await analyzer.is_profitable(spread=0.03, fees=0.01, min_spread=0.015) is True
    assert await analyzer.is_profitable(spread=0.01, fees=0.005, min_spread=0.02) is False

