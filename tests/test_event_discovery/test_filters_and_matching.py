from datetime import datetime, timedelta, timezone

from core.event_discovery import DiscoveredEvent, SOURCE_OPINION, SOURCE_POLYMARKET
from core.event_discovery.filters import apply_filters
from core.event_discovery.matcher import match_events
from utils.config_loader import DiscoveryLiquidity, EventDiscoveryConfig


def _event(
    source: str,
    event_id: str,
    title: str,
    end_days: int,
    liquidity: float,
    description: str | None = None,
) -> DiscoveredEvent:
    return DiscoveredEvent(
        source=source,
        event_id=event_id,
        title=title,
        description=description,
        end_time=datetime.now(timezone.utc) + timedelta(days=end_days),
        contract_type="binary",
        yes_token_id=f"{event_id}-yes",
        no_token_id=f"{event_id}-no",
        metadata={"liquidity": liquidity},
    )


def test_filters_remove_blocked_and_apply_horizon():
    config = EventDiscoveryConfig(
        enabled=True,
        keywords_allow=["fed", "interest"],
        keywords_block=["sports"],
        min_liquidity=DiscoveryLiquidity(polymarket=5000, opinion=3000),
        horizon_days_min=3,
        horizon_days_max=365,
    )

    opinion_events = [
        _event(SOURCE_OPINION, "op-1", "Fed interest rate decision 2025", 10, 4000),
        _event(SOURCE_OPINION, "op-2", "Sports final odds", 10, 4000),
        _event(SOURCE_OPINION, "op-3", "Fed interest rate decision 2025", 1, 4000),
    ]
    polymarket_events = [
        _event(SOURCE_POLYMARKET, "pm-1", "Will the fed cut rates in 2025?", 12, 6000),
        _event(SOURCE_POLYMARKET, "pm-2", "Unrelated market", 12, 1000),
    ]

    filtered_opinion = apply_filters(opinion_events, config, SOURCE_OPINION)
    filtered_poly = apply_filters(polymarket_events, config, SOURCE_POLYMARKET)

    assert len(filtered_opinion) == 1
    assert filtered_opinion[0].raw.event_id == "op-1"
    assert len(filtered_poly) == 1
    assert filtered_poly[0].raw.event_id == "pm-1"

    matches = match_events(filtered_opinion, filtered_poly, threshold=0.5)
    assert matches, "expected at least one candidate match"
    assert matches[0].confidence_score >= 0.5


def test_matching_threshold_blocks_low_confidence():
    config = EventDiscoveryConfig(enabled=True, min_liquidity=DiscoveryLiquidity())
    op_evt = _event(SOURCE_OPINION, "op-10", "Election 2025 outcome", 30, 10000)
    pm_evt = _event(SOURCE_POLYMARKET, "pm-10", "Completely unrelated topic", 30, 10000)

    filtered_op = apply_filters([op_evt], config, SOURCE_OPINION)
    filtered_pm = apply_filters([pm_evt], config, SOURCE_POLYMARKET)
    matches = match_events(filtered_op, filtered_pm, threshold=0.85)
    assert not matches

