from __future__ import annotations

from typing import Iterable, List, Optional

import yaml

from . import MatchedEventPair, NormalizedEvent, SOURCE_OPINION, SOURCE_POLYMARKET
from .normalizer import normalize_event, slugify
from .approvals import EventApprovalStore


def _normalize_collection(events: Iterable[NormalizedEvent]) -> List[NormalizedEvent]:
    normalized: List[NormalizedEvent] = []
    for evt in events:
        normalized.append(evt if isinstance(evt, NormalizedEvent) else normalize_event(evt))
    return normalized


class EventDiscoveryRegistry:
    """In-memory store for discovered events and matched pairs."""

    def __init__(self, approvals: EventApprovalStore | None = None):
        self.opinion_events: List[NormalizedEvent] = []
        self.polymarket_events: List[NormalizedEvent] = []
        self.matches: List[MatchedEventPair] = []
        self.approvals = approvals

    def update(
        self,
        opinion_events: Iterable[NormalizedEvent],
        polymarket_events: Iterable[NormalizedEvent],
        matches: Iterable[MatchedEventPair],
    ) -> None:
        self.opinion_events = _normalize_collection(opinion_events)
        self.polymarket_events = _normalize_collection(polymarket_events)
        self.matches = sorted(list(matches), key=lambda m: m.confidence_score, reverse=True)

    def get_candidates(self, limit: Optional[int] = None) -> List[MatchedEventPair]:
        return self.matches[:limit] if limit else list(self.matches)

    def list_pending(self, limit: Optional[int] = None) -> List[MatchedEventPair]:
        """Return matches that are not approved/rejected."""
        pending: List[MatchedEventPair] = []
        for match in self.matches:
            match_id = self.match_id(match)
            if self.approvals and (self.approvals.is_approved(match_id) or self.approvals.is_rejected(match_id)):
                continue
            pending.append(match)
            if limit and len(pending) >= limit:
                break
        return pending

    def mark_approved(self, match_id: str) -> Optional[MatchedEventPair]:
        match = self.find_match(match_id)
        if not match or not self.approvals:
            return match
        self.approvals.mark_approved(
            match_id,
            opinion_event_id=match.opinion_event.event_id,
            polymarket_event_id=match.polymarket_event.event_id,
            title=match.opinion_event.title or match.polymarket_event.title,
        )
        return match

    def mark_rejected(self, match_id: str) -> Optional[MatchedEventPair]:
        match = self.find_match(match_id)
        if not match or not self.approvals:
            return match
        self.approvals.mark_rejected(
            match_id,
            opinion_event_id=match.opinion_event.event_id,
            polymarket_event_id=match.polymarket_event.event_id,
            title=match.opinion_event.title or match.polymarket_event.title,
        )
        return match

    def find_match(self, match_id: str) -> Optional[MatchedEventPair]:
        for match in self.matches:
            if self.match_id(match) == match_id:
                return match
        return None

    def summary(self) -> dict:
        return {
            "opinion_events": len(self.opinion_events),
            "polymarket_events": len(self.polymarket_events),
            "candidate_pairs": len(self.matches),
        }

    def export_yaml(self, limit: int | None = None, event_id: str | None = None) -> str:
        if event_id:
            match = self.find_match(event_id)
            if not match:
                return "# No candidates available"
            matches: List[MatchedEventPair] = [match]
        else:
            matches = self.get_candidates(limit)
        entries = []
        for match in matches:
            event_slug = slugify(match.opinion_event.title) or slugify(match.polymarket_event.title)
            entries.append(
                {
                    "event_id": event_slug,
                    "primary_market_id": match.opinion_event.event_id,
                    "secondary_market_id": match.polymarket_event.yes_token_id
                    or match.polymarket_event.event_id,
                    "contract_type": (match.opinion_event.contract_type or "BINARY").upper(),
                    "strategy_direction": "AUTO",
                }
            )
        if not entries:
            return "# No candidates available"
        return yaml.safe_dump(entries, sort_keys=False, default_flow_style=False).rstrip()

    @staticmethod
    def match_id(match: MatchedEventPair) -> str:
        return f"{match.opinion_event.event_id}::{match.polymarket_event.event_id}"


__all__ = ["EventDiscoveryRegistry"]

