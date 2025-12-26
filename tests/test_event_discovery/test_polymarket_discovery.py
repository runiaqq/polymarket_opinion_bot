import re

import aiohttp
import pytest
from aioresponses import CallbackResult, aioresponses

from core.event_discovery.polymarket_discovery import PolymarketDiscovery


@pytest.mark.asyncio
async def test_polymarket_discovery_validates_and_filters():
    async with aiohttp.ClientSession() as session:
        with aioresponses() as mocked:
            mocked.get(
                re.compile(r"https://gamma-api\.polymarket\.com/markets.*"),
                payload={
                    "markets": [
                        {
                            "id": "m-1",
                            "question": "Will the Fed cut rates in 2025?",
                            "active": True,
                            "clobTokenIds": ["yes-1", "no-1"],
                            "endDate": "2025-06-01T00:00:00Z",
                            "volume": 12000,
                        },
                        {
                            "id": "m-2",
                            "question": "Old resolved market",
                            "active": False,
                            "clobTokenIds": ["bad-token"],
                            "resolved": True,
                        },
                    ]
                },
            )
            mocked.get(
                "https://clob.polymarket.com/book",
                callback=lambda url, **kwargs: CallbackResult(
                    status=200 if kwargs.get("params", {}).get("token_id") == "yes-1" else 404,
                    payload={"bids": [], "asks": []},
                ),
            )
            mocked.get(
                re.compile(r"https://clob\.polymarket\.com/markets/.*/orderbook"),
                callback=lambda url, **kwargs: CallbackResult(
                    status=200 if "yes-1" in str(url) else 404,
                    payload={"bids": [], "asks": []},
                ),
            )

            discovery = PolymarketDiscovery(session=session, max_pages=1)
            events = await discovery.discover()

    assert len(events) == 1
    event = events[0]
    assert event.source == "polymarket"
    assert event.yes_token_id == "yes-1"
    assert event.contract_type == "binary"

