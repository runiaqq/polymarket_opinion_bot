import re

import aiohttp
import pytest
from aioresponses import aioresponses

from core.event_discovery.opinion_discovery import OpinionDiscovery


@pytest.mark.asyncio
async def test_opinion_discovery_only_activated():
    async with aiohttp.ClientSession() as session:
        with aioresponses() as mocked:
            mocked.get(
                re.compile(r"https://openapi\.opinion\.trade/openapi/market.*"),
                payload={
                    "result": {
                        "list": [
                            {
                                "marketId": 101,
                                "marketTitle": "Fed rate cut in 2025?",
                                "statusEnum": "activated",
                                "yesTokenId": "op-yes",
                                "noTokenId": "op-no",
                                "volume": 8000,
                            },
                            {
                                "marketId": 202,
                                "marketTitle": "Draft market",
                                "statusEnum": "draft",
                            },
                        ]
                    }
                },
            )

            discovery = OpinionDiscovery(session=session, api_key="dummy")
            events = await discovery.discover()

    assert len(events) == 1
    event = events[0]
    assert event.source == "opinion"
    assert event.yes_token_id == "op-yes"
    assert event.contract_type == "binary"

