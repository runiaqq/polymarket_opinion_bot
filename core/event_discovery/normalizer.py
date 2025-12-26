from __future__ import annotations

import re
from typing import Iterable, Set, Tuple

from . import DiscoveredEvent, NormalizedEvent

MONTH_ALIASES = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}

KEYWORD_CANONICAL = {
    "fed": "federal_reserve",
    "federal": "federal_reserve",
    "rate": "interest_rate",
    "rates": "interest_rate",
    "interest": "interest_rate",
    "inflation": "inflation",
    "cpi": "inflation",
    "election": "election",
}

PUNCT_RE = re.compile(r"[^\w\s]", flags=re.UNICODE)
SPACE_RE = re.compile(r"\s+")


def slugify(text: str) -> str:
    """Simple slug that keeps titles readable while being id-safe."""
    cleaned = SPACE_RE.sub(" ", (text or "").lower()).strip()
    cleaned = re.sub(r"[^a-z0-9]+", "-", cleaned)
    cleaned = re.sub(r"-{2,}", "-", cleaned)
    return cleaned.strip("-")


def _normalize_tokens(tokens: list[str]) -> Tuple[str, Set[str]]:
    normalized_tokens: list[str] = []
    keywords: Set[str] = set()
    idx = 0
    while idx < len(tokens):
        token = tokens[idx]
        next_token = tokens[idx + 1] if idx + 1 < len(tokens) else None
        if token in MONTH_ALIASES and next_token and re.fullmatch(r"\d{4}", next_token):
            month_num = MONTH_ALIASES[token]
            normalized_tokens.append(f"{next_token}-{month_num:02d}")
            keywords.add(next_token)
            idx += 2
            continue
        canonical = KEYWORD_CANONICAL.get(token, token)
        if canonical:
            keywords.add(canonical)
            normalized_tokens.append(canonical)
        idx += 1
    normalized_title = " ".join(normalized_tokens)
    keywords.update(_extract_years(normalized_title))
    return normalized_title, keywords


def normalize_title(title: str) -> Tuple[str, Set[str]]:
    """
    Normalize titles for fuzzy matching:
    - lowercase
    - strip punctuation
    - collapse whitespace
    - normalize dates and keyword aliases
    """
    text = (title or "").lower().replace("’", "'")
    text = PUNCT_RE.sub(" ", text)
    text = SPACE_RE.sub(" ", text).strip()
    tokens = [tok for tok in text.split(" ") if tok]
    return _normalize_tokens(tokens)


def normalize_event(event: DiscoveredEvent) -> NormalizedEvent:
    normalized_title, keywords = normalize_title(event.title)
    slug = slugify(normalized_title or event.title or event.event_id)
    return NormalizedEvent(raw=event, normalized_title=normalized_title, keywords=keywords, slug=slug)


def normalize_events(events: Iterable[DiscoveredEvent]) -> list[NormalizedEvent]:
    return [normalize_event(evt) for evt in events]


def _extract_years(text: str) -> Set[str]:
    return {match.group(0) for match in re.finditer(r"\b(20\d{2})\b", text)}


__all__ = ["normalize_title", "normalize_event", "normalize_events", "slugify"]

