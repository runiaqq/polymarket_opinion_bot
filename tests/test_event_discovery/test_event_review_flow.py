import pytest
from datetime import datetime, timedelta, timezone

from core.event_discovery import DiscoveredEvent, MatchedEventPair, SOURCE_OPINION, SOURCE_POLYMARKET
from core.event_discovery.approvals import EventApprovalStore
from core.event_discovery.registry import EventDiscoveryRegistry
from telegram.event_review import EventReviewHandler


class DummyNotifier:
    def __init__(self):
        self.sent: list[tuple] = []

    async def send_message(self, msg: str, chat_id: str | None = None, parse_mode=None, disable_web_page_preview=True, reply_markup=None):
        self.sent.append((msg, chat_id, reply_markup))
        return True


def _match() -> MatchedEventPair:
    now = datetime.now(tz=timezone.utc)
    op = DiscoveredEvent(
        source=SOURCE_OPINION,
        event_id="op-201",
        title="Election outcome 2025",
        description=None,
        end_time=now + timedelta(days=40),
        contract_type="binary",
        yes_token_id="op-yes",
        no_token_id="op-no",
        metadata={"liquidity": 6000},
    )
    pm = DiscoveredEvent(
        source=SOURCE_POLYMARKET,
        event_id="pm-303",
        title="Who wins the 2025 election?",
        description=None,
        end_time=now + timedelta(days=42),
        contract_type="binary",
        yes_token_id="pm-yes",
        no_token_id="pm-no",
        metadata={"liquidity": 8000},
    )
    return MatchedEventPair(opinion_event=op, polymarket_event=pm, confidence_score=0.88)


@pytest.mark.asyncio
async def test_event_review_flow(tmp_path):
    store = EventApprovalStore(tmp_path / "approvals.json")
    registry = EventDiscoveryRegistry(store)
    match = _match()
    registry.update([], [], [match])
    notifier = DummyNotifier()
    handler = EventReviewHandler(registry=registry, approvals=store, notifier=notifier, logger=None)

    await handler.send_pending_events("123")
    assert notifier.sent, "should send event card"
    msg, chat_id, markup = notifier.sent[0]
    assert "Найдено потенциальное событие" in msg
    assert markup and "inline_keyboard" in markup

    match_id = registry.match_id(match)
    await handler.handle_callback("123", f"event:approve:{match_id}")
    assert store.is_approved(match_id)
    assert any("подтверждено" in m[0] for m in notifier.sent)

