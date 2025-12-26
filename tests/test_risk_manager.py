import pytest

from core.risk_manager import RiskManager
from utils.config_loader import MarketHedgeConfig


def _cfg():
    return MarketHedgeConfig(
        enabled=True,
        hedge_ratio=1.0,
        max_slippage_market_hedge=0.01,
        min_spread_for_entry=0.0,
        max_position_size_per_market=100,
        max_position_size_per_event=200,
        cancel_unfilled_after_ms=1000,
        allow_partial_fill_hedge=True,
    )


@pytest.mark.asyncio
async def test_exposure_increment_and_decrement():
    rm = RiskManager(_cfg())
    await rm.check_limits("evt-1", 50)
    await rm.decrement("evt-1", 20)
    await rm.decrement("evt-1", 40)  # should floor at zero
    assert rm._event_limits["evt-1"] == 0





