import pytest

from core.models import ExchangeName, OrderBook, OrderBookEntry
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


def test_compute_net_spread_with_fees():
    per_unit, total = SpreadAnalyzer.compute_net_spread(
        buy_price=0.50,
        sell_price=0.55,
        size=10,
        buy_fee=0.001,
        sell_fee=0.002,
    )
    expected_per = (0.55 * (1 - 0.002)) - (0.50 * (1 + 0.001))
    assert per_unit == pytest.approx(expected_per, abs=1e-9)
    assert total == pytest.approx(expected_per * 10, abs=1e-9)


@pytest.mark.asyncio
async def test_evaluate_opportunity_respects_fees():
    analyzer = SpreadAnalyzer()
    primary = OrderBook(
        market_id="primary",
        bids=[OrderBookEntry(price=0.51, size=50)],
        asks=[OrderBookEntry(price=0.49, size=50)],
    )
    secondary = OrderBook(
        market_id="secondary",
        bids=[OrderBookEntry(price=0.55, size=50)],
        asks=[OrderBookEntry(price=0.53, size=50)],
    )
    scenario = await analyzer.evaluate_opportunity(
        primary_exchange=ExchangeName.OPINION,
        secondary_exchange=ExchangeName.POLYMARKET,
        primary_book=primary,
        secondary_book=secondary,
        primary_fees={"maker": 0.001},
        secondary_fees={"maker": 0.002},
        size=10,
    )
    assert scenario
    assert scenario["net_total"] > 0
    assert scenario["legs"][ExchangeName.OPINION]["side"].value == "BUY"
    assert scenario["legs"][ExchangeName.POLYMARKET]["side"].value == "SELL"


@pytest.mark.asyncio
async def test_evaluate_opportunity_returns_none_when_spread_negative():
    analyzer = SpreadAnalyzer()
    primary = OrderBook(
        market_id="primary",
        bids=[OrderBookEntry(price=0.50, size=10)],
        asks=[OrderBookEntry(price=0.52, size=10)],
    )
    secondary = OrderBook(
        market_id="secondary",
        bids=[OrderBookEntry(price=0.49, size=10)],
        asks=[OrderBookEntry(price=0.51, size=10)],
    )
    scenario = await analyzer.evaluate_opportunity(
        primary_exchange=ExchangeName.OPINION,
        secondary_exchange=ExchangeName.POLYMARKET,
        primary_book=primary,
        secondary_book=secondary,
        primary_fees={"maker": 0.001},
        secondary_fees={"maker": 0.001},
        size=10,
    )
    assert scenario["net_total"] < 0

