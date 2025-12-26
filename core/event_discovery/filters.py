from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable, List

from utils.config_loader import EventDiscoveryConfig

from . import DiscoveredEvent, NormalizedEvent, SOURCE_OPINION, SOURCE_POLYMARKET
from .normalizer import KEYWORD_CANONICAL, normalize_event


def _to_float(value) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _liquidity_threshold(config: EventDiscoveryConfig, source: str) -> float:
    if not config or not config.min_liquidity:
        return 0.0
    if source == SOURCE_POLYMARKET:
        return float(config.min_liquidity.polymarket)
    if source == SOURCE_OPINION:
        return float(config.min_liquidity.opinion)
    return 0.0


def _event_liquidity(event: NormalizedEvent) -> float:
    meta = event.raw.metadata or {}
    for key in ("liquidity", "volume", "24hVolume", "tvl"):
        if key in meta:
            val = _to_float(meta.get(key))
            if val:
                return val
    return 0.0


def _within_horizon(event: NormalizedEvent, horizon_min: int, horizon_max: int, now: datetime) -> bool:
    if event.raw.end_time is None:
        return False
    delta_days = (event.raw.end_time - now).total_seconds() / 86400
    return horizon_min <= delta_days <= horizon_max


def apply_filters(
    events: Iterable[DiscoveredEvent | NormalizedEvent],
    config: EventDiscoveryConfig | None,
    source: str,
) -> List[NormalizedEvent]:
    """Apply config-driven filters to discovered events."""
    normalized: List[NormalizedEvent] = []
    for evt in events:
        normalized.append(evt if isinstance(evt, NormalizedEvent) else normalize_event(evt))

    if not config or not config.enabled:
        return normalized

    allow = _canonicalize_keywords(config.keywords_allow)
    block = _canonicalize_keywords(config.keywords_block)
    threshold = _liquidity_threshold(config, source)
    now = datetime.now(timezone.utc)
    filtered: List[NormalizedEvent] = []
    for evt in normalized:
        keyword_set = {kw.lower() for kw in evt.keywords}
        if block and keyword_set.intersection(block):
            continue
        if allow and not keyword_set.intersection(allow):
            continue
        if threshold and _event_liquidity(evt) < threshold:
            continue
        if not _within_horizon(evt, int(config.horizon_days_min), int(config.horizon_days_max), now):
            continue
        filtered.append(evt)
    return filtered


def _canonicalize_keywords(keywords: Iterable[str] | None) -> set[str]:
    canonical: set[str] = set()
    for kw in keywords or []:
        cleaned = kw.lower().strip()
        canonical.add(KEYWORD_CANONICAL.get(cleaned, cleaned))
    return canonical


__all__ = ["apply_filters"]

