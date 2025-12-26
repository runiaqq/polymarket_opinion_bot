import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from core.event_discovery import DiscoveredEvent, SOURCE_OPINION, SOURCE_POLYMARKET
from core.event_discovery.approvals import EventApprovalStore
from core.event_discovery.registry import EventDiscoveryRegistry
from core.event_discovery.service import EventDiscoveryService
from utils.config_loader import DiscoveryLiquidity, EventDiscoveryConfig
from utils.logger import BotLogger


def _event(source: str, eid: str, title: str) -> DiscoveredEvent:
    return DiscoveredEvent(
        source=source,
        event_id=eid,
        title=title,
        description=None,
        end_time=datetime.now(timezone.utc) + timedelta(days=30),
        contract_type="binary",
        yes_token_id=f"{eid}-yes",
        no_token_id=f"{eid}-no",
        metadata={"liquidity": 10000},
    )


@pytest.mark.asyncio
async def test_service_run_once_populates_registry(tmp_path):
    approvals = EventApprovalStore(tmp_path / "approvals.json")
    registry = EventDiscoveryRegistry(approvals)
    config = EventDiscoveryConfig(
        enabled=True,
        keywords_allow=["fed"],
        keywords_block=[],
        min_liquidity=DiscoveryLiquidity(polymarket=0, opinion=0),
        horizon_days_min=1,
        horizon_days_max=365,
        poll_interval_sec=1,
    )
    op_event = _event(SOURCE_OPINION, "op-1", "Fed cuts rates 2025")
    pm_event = _event(SOURCE_POLYMARKET, "pm-1", "Fed cuts rates in 2025")

    async def fake_pm():
        return [pm_event]

    async def fake_op():
        return [op_event]

    stop_event = asyncio.Event()
    service = EventDiscoveryService(
        config=config,
        registry=registry,
        logger=BotLogger("test_discovery_service"),
        opinion_api_key="dummy",
        stop_event=stop_event,
        poll_interval_sec=1,
        polymarket_fetcher=fake_pm,
        opinion_fetcher=fake_op,
    )

    await service.run_once()
    await service.stop()
    summary = registry.summary()
    assert summary["candidate_pairs"] >= 1

