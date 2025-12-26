from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Set

SOURCE_POLYMARKET = "polymarket"
SOURCE_OPINION = "opinion"


@dataclass(slots=True)
class DiscoveredEvent:
    """Lightweight representation of a market/event discovered on an exchange."""

    source: str
    event_id: str
    title: str
    description: str | None
    end_time: datetime | None
    contract_type: str  # binary | categorical
    yes_token_id: str | None
    no_token_id: str | None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class NormalizedEvent:
    """A discovered event plus normalized text to make filtering/matching deterministic."""

    raw: DiscoveredEvent
    normalized_title: str
    keywords: Set[str] = field(default_factory=set)
    slug: str = ""


@dataclass(slots=True)
class MatchedEventPair:
    """Represents a potential cross-exchange pairing produced by the matcher."""

    opinion_event: DiscoveredEvent
    polymarket_event: DiscoveredEvent
    confidence_score: float


__all__ = [
    "DiscoveredEvent",
    "NormalizedEvent",
    "MatchedEventPair",
    "SOURCE_OPINION",
    "SOURCE_POLYMARKET",
]

