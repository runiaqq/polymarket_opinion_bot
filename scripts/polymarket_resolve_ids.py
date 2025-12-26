#!/usr/bin/env python
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from utils.polymarket_discovery import (  # noqa: E402
    build_yaml_snippet,
    load_cache,
    parse_slug_from_url,
    resolve_market,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Resolve Polymarket IDs from URL/slug using cached markets.")
    parser.add_argument("--cache", default="data/polymarket_markets.csv", help="Path to cached markets CSV")
    parser.add_argument("--url", help="Polymarket market/event URL")
    parser.add_argument("--slug", help="Polymarket slug (if URL not provided)")
    parser.add_argument("--market-id", help="Polymarket market id (optional direct)")
    parser.add_argument("--event-id", default="auto-event", help="event_id to embed into YAML snippet")
    parser.add_argument("--opinion-id", default="OPINION_MARKET_ID", help="Opinion market id placeholder")
    parser.add_argument("--contract-type", default="BINARY", help="Contract type for YAML snippet")
    args = parser.parse_args()

    cache = load_cache(args.cache)
    if not cache:
        print(f"[error] cache not found or empty: {args.cache}")
        sys.exit(1)

    slug = parse_slug_from_url(args.url) if args.url else args.slug
    match = resolve_market(cache, slug=slug, market_id=args.market_id)
    if not match:
        print(f"[error] no match found for slug={slug} market_id={args.market_id}")
        sys.exit(1)

    print("[match]")
    for k in ["id", "slug", "question", "condition_id", "clob_token_ids", "outcomes", "active", "accepting_orders"]:
        if k in match:
            print(f"{k}: {match[k]}")

    snippet = build_yaml_snippet(
        event_id=args.event_id,
        opinion_market_id=args.opinion_id,
        polymarket_market_id=str(match.get("id")),
        contract_type=args.contract_type,
    )
    print("\n# YAML snippet (settings.yaml -> market_pairs):")
    print(snippet)


if __name__ == "__main__":
    main()




