import pytest
from aiohttp.test_utils import TestClient, TestServer

from core.models import ExchangeName
from scripts.webhook_server import create_app
from utils.google_sheets import GoogleSheetsSync, MarketPairStore
from utils.config_loader import MarketPairConfig


class DummySyncer:
    def __init__(self, new_pairs):
        self.new_pairs = new_pairs
        self.triggered = False

    async def sync(self, store: MarketPairStore):
        self.triggered = True
        return await store.update_pairs(self.new_pairs)


@pytest.mark.asyncio
async def test_webhook_pairs_and_sync():
    initial = [
        MarketPairConfig(
            event_id="pair-1",
            primary_market_id="a",
            secondary_market_id="b",
        )
    ]
    store = MarketPairStore(initial)
    new_pairs = [
        MarketPairConfig(
            event_id="pair-2",
            primary_market_id="c",
            secondary_market_id="d",
            strategy="NEW",
        )
    ]
    syncer = DummySyncer(new_pairs)
    app = await create_app(store, syncer, admin_token="secret")
    server = TestServer(app)
    client = TestClient(server)
    await client.start_server()

    # unauthorized
    try:
        resp = await client.get("/pairs")
        assert resp.status == 401

        headers = {"X-ADMIN-TOKEN": "secret"}
        resp = await client.get("/pairs", headers=headers)
        assert resp.status == 200
        data = await resp.json()
        assert data["count"] == 1
        assert data["pairs"][0]["event_id"] == "pair-1"

        resp = await client.post("/sync", headers=headers)
        assert resp.status == 200
        payload = await resp.json()
        assert payload["added"] == 1
        assert payload["removed"] == 1
        assert syncer.triggered

        resp = await client.get("/pairs", headers=headers)
        data = await resp.json()
        assert data["pairs"][0]["event_id"] == "pair-2"
    finally:
        await client.close()
        await server.close()


@pytest.mark.asyncio
async def test_sync_endpoint_disabled():
    store = MarketPairStore([])
    app = await create_app(store, syncer=None, admin_token="token")
    server = TestServer(app)
    client = TestClient(server)
    await client.start_server()
    try:
        resp = await client.post("/sync", headers={"X-ADMIN-TOKEN": "token"})
        assert resp.status == 400
    finally:
        await client.close()
        await server.close()

