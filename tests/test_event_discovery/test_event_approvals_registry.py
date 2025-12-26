import pytest
from datetime import datetime, timedelta, timezone

from core.event_discovery import DiscoveredEvent, MatchedEventPair, SOURCE_OPINION, SOURCE_POLYMARKET
from core.event_discovery.approvals import EventApprovalStore
from core.event_discovery.registry import EventDiscoveryRegistry


def _match() -> MatchedEventPair:
    now = datetime.now(tz=timezone.utc)
    op = DiscoveredEvent(
        source=SOURCE_OPINION,
        event_id="op-101",
        title="Fed rate cut 2025",
        description=None,
        end_time=now + timedelta(days=30),
        contract_type="binary",
        yes_token_id="op-yes",
        no_token_id="op-no",
        metadata={"liquidity": 7000},
    )
    pm = DiscoveredEvent(
        source=SOURCE_POLYMARKET,
        event_id="pm-202",
        title="Will Fed cut rates in 2025?",
        description=None,
        end_time=now + timedelta(days=32),
        contract_type="binary",
        yes_token_id="pm-yes",
        no_token_id="pm-no",
        metadata={"liquidity": 9000},
    )
    return MatchedEventPair(opinion_event=op, polymarket_event=pm, confidence_score=0.9)


def test_approval_persistence(tmp_path):
    store = EventApprovalStore(tmp_path / "approvals.json")
    registry = EventDiscoveryRegistry(store)
    match = _match()
    registry.update([], [], [match])
    match_id = registry.match_id(match)

    assert registry.list_pending()
    registry.mark_approved(match_id)
    assert store.is_approved(match_id)
    assert not registry.list_pending()


def test_rejection_hides_event(tmp_path):
    store = EventApprovalStore(tmp_path / "approvals.json")
    registry = EventDiscoveryRegistry(store)
    match = _match()
    registry.update([], [], [match])
    match_id = registry.match_id(match)

    registry.mark_rejected(match_id)
    assert store.is_rejected(match_id)
    assert not registry.list_pending()


def test_export_yaml_for_single_event(tmp_path):
    store = EventApprovalStore(tmp_path / "approvals.json")
    registry = EventDiscoveryRegistry(store)
    match = _match()
    registry.update([], [], [match])
    match_id = registry.match_id(match)

    snippet = registry.export_yaml(event_id=match_id)
    assert "event_id" in snippet
    assert "primary_market_id" in snippet

