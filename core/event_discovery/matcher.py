from __future__ import annotations

from difflib import SequenceMatcher
from datetime import datetime
from typing import Iterable, List

from . import MatchedEventPair, NormalizedEvent
from .normalizer import normalize_event


def _ensure_normalized(events: Iterable[NormalizedEvent]) -> List[NormalizedEvent]:
    normalized: List[NormalizedEvent] = []
    for evt in events:
        normalized.append(evt if isinstance(evt, NormalizedEvent) else normalize_event(evt))
    return normalized


def _date_score(opinion_end: datetime | None, poly_end: datetime | None) -> float:
    if not opinion_end or not poly_end:
        return 0.5
    delta_days = abs((opinion_end - poly_end).total_seconds()) / 86400
    if delta_days <= 1:
        return 1.0
    return max(0.0, 1.0 - (delta_days / 30))


def confidence_score(opinion_event: NormalizedEvent, polymarket_event: NormalizedEvent) -> float:
    title_similarity = SequenceMatcher(
        None, opinion_event.normalized_title, polymarket_event.normalized_title
    ).ratio()
    union_keywords = opinion_event.keywords | polymarket_event.keywords
    keyword_overlap = (
        len(opinion_event.keywords & polymarket_event.keywords) / len(union_keywords)
        if union_keywords
        else 0.0
    )
    date_component = _date_score(opinion_event.raw.end_time, polymarket_event.raw.end_time)
    score = (title_similarity * 0.65) + (keyword_overlap * 0.2) + (date_component * 0.15)
    return min(1.0, max(0.0, score))


def match_events(
    opinion_events: Iterable[NormalizedEvent],
    polymarket_events: Iterable[NormalizedEvent],
    threshold: float = 0.85,
) -> List[MatchedEventPair]:
    op_norm = _ensure_normalized(opinion_events)
    pm_norm = _ensure_normalized(polymarket_events)
    matches: List[MatchedEventPair] = []
    for op_evt in op_norm:
        for pm_evt in pm_norm:
            score = confidence_score(op_evt, pm_evt)
            if score >= threshold:
                matches.append(
                    MatchedEventPair(
                        opinion_event=op_evt.raw,
                        polymarket_event=pm_evt.raw,
                        confidence_score=score,
                    )
                )
    matches.sort(key=lambda m: m.confidence_score, reverse=True)
    return matches


__all__ = ["match_events", "confidence_score"]

