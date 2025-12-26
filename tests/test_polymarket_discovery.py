import json
from pathlib import Path

import pytest

from utils.polymarket_discovery import (
    build_yaml_snippet,
    load_cache,
    parse_slug_from_url,
    resolve_market,
    write_csv,
)

FIXTURE = Path("tests/fixtures/polymarket_markets_sample.json")


def test_parse_slug_from_url():
    url = "https://polymarket.com/event/fed-decision-in-january?tid=123"
    assert parse_slug_from_url(url) == "fed-decision-in-january"
    assert parse_slug_from_url("simple-slug") == "simple-slug"


def test_write_and_load_cache(tmp_path):
    markets = json.loads(FIXTURE.read_text(encoding="utf-8"))
    out = write_csv(markets, tmp_path / "cache.csv")
    assert out.exists()
    cache = load_cache(out)
    assert cache
    entry = cache[0]
    assert entry["id"] == "601697"
    assert entry["slug"] == "fed-decreases-interest-rates-by-50-bps-after-january-2026-meeting"
    assert entry["condition_id"]


def test_resolve_market_by_slug_and_id(tmp_path):
    markets = json.loads(FIXTURE.read_text(encoding="utf-8"))
    out = write_csv(markets, tmp_path / "cache.csv")
    cache = load_cache(out)
    slug = "fed-decreases-interest-rates-by-50-bps-after-january-2026-meeting"
    match = resolve_market(cache, slug=slug, market_id=None)
    assert match is not None
    assert match["id"] == "601697"
    # by id
    match2 = resolve_market(cache, slug=None, market_id="601697")
    assert match2 is not None
    assert match2["slug"] == slug


def test_build_yaml_snippet():
    snippet = build_yaml_snippet("evt", "op-1", "pm-1", contract_type="MULTI")
    assert "event_id: \"evt\"" in snippet
    assert "primary_market_id: \"op-1\"" in snippet
    assert "secondary_market_id: \"pm-1\"" in snippet
    assert "contract_type: \"MULTI\"" in snippet




