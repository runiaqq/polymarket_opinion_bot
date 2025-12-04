import aiohttp
import pytest

from core.models import ExchangeName, Market, OrderSide, OrderType
from exchanges.opinion_api import OpinionAPI
from exchanges.polymarket_api import PolymarketAPI
from exchanges.rate_limiter import RateLimiter
from utils.logger import BotLogger


@pytest.mark.asyncio
async def test_polymarket_limit_order_payload(monkeypatch):
    session = aiohttp.ClientSession()
    limiter = RateLimiter(100, 5)
    client = PolymarketAPI(
        session=session,
        api_key="key",
        secret="secret",
        rate_limit=limiter,
        logger=BotLogger("poly-test"),
    )

    async def fake_request(self, method, path, params=None, payload=None, headers=None, auth=True):
        fake_request.payload = payload
        return {
            "order_id": "123",
            "market_id": "m",
            "side": "buy",
            "type": "limit",
            "price": 0.5,
            "size": 10,
            "status": "open",
            "created_at": "2024-01-01T00:00:00Z",
        }

    monkeypatch.setattr(PolymarketAPI, "_request", fake_request)
    order = await client.place_limit_order("m", OrderSide.BUY, price=0.5, size=10, client_order_id="cid")
    assert fake_request.payload["client_order_id"] == "cid"
    assert order.order_id == "123"

    await session.close()


@pytest.mark.asyncio
async def test_opinion_place_order_uses_payload_fields(monkeypatch):
    session = aiohttp.ClientSession()
    limiter = RateLimiter(100, 5)
    client = OpinionAPI(
        session=session,
        api_key="key",
        secret="secret",
        rate_limit=limiter,
        logger=BotLogger("opinion-test"),
    )

    async def fake_api_request(self, method, path, params=None, payload=None, auth=True):
        fake_api_request.payload = payload
        return {
            "data": {
                "order_id": "abc",
                "token_id": "token",
                "side": "buy",
                "order_type": "limit",
                "price": 0.6,
                "size": 5,
                "status": "open",
                "createdAt": "2024-01-01T00:00:00Z",
            }
        }

    monkeypatch.setattr(OpinionAPI, "_api_request", fake_api_request)
    await client.place_order(
        market_id=1,
        token_id="token",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        price=0.6,
        quote_amount=5,
        client_order_id="cid",
    )
    payload = fake_api_request.payload
    assert payload["side"] == 0
    assert payload["orderType"] == 2
    assert float(payload["makerAmountInQuoteToken"]) == pytest.approx(5.0, abs=0.0001)
    await session.close()


@pytest.mark.asyncio
async def test_opinion_place_limit_order_fetches_market(monkeypatch):
    session = aiohttp.ClientSession()
    limiter = RateLimiter(100, 5)
    client = OpinionAPI(
        session=session,
        api_key="key",
        secret="secret",
        rate_limit=limiter,
        logger=BotLogger("opinion-limit"),
    )

    async def fake_get_market(self, token_id):
        return Market(
            market_id="77",
            name="market",
            exchange=ExchangeName.OPINION,
        )

    async def fake_api_request(self, method, path, params=None, payload=None, auth=True):
        fake_api_request.payload = payload
        return {
            "data": {
                "order_id": "limit-123",
                "token_id": "tok",
                "side": "buy",
                "order_type": "limit",
                "price": 0.5,
                "size": 10,
                "status": "open",
                "createdAt": "2024-01-01T00:00:00Z",
            }
        }

    monkeypatch.setattr(OpinionAPI, "get_market", fake_get_market)
    monkeypatch.setattr(OpinionAPI, "_api_request", fake_api_request)
    order = await client.place_limit_order("tok", OrderSide.BUY, 0.5, 10, client_order_id="cid")
    assert order.order_id == "limit-123"
    assert fake_api_request.payload["clientOrderId"] == "cid"
    assert fake_api_request.payload["makerAmountInQuoteToken"] == "10.000000"
    await session.close()


@pytest.mark.asyncio
async def test_clients_cancel_with_client_ids(monkeypatch):
    session = aiohttp.ClientSession()
    limiter = RateLimiter(100, 5)
    opinion = OpinionAPI(
        session=session,
        api_key="key",
        secret="secret",
        rate_limit=limiter,
        logger=BotLogger("opinion-cancel"),
    )
    poly = PolymarketAPI(
        session=session,
        api_key="key",
        secret="secret",
        rate_limit=limiter,
        logger=BotLogger("poly-cancel"),
    )

    paths = {"opinion": None, "poly": None}

    async def fake_opinion_request(self, method, path, params=None, payload=None, auth=True):
        paths["opinion"] = path
        return {}

    async def fake_poly_request(self, method, path, params=None, payload=None, headers=None, auth=True):
        paths["poly"] = path
        return {}

    monkeypatch.setattr(OpinionAPI, "_api_request", fake_opinion_request)
    monkeypatch.setattr(PolymarketAPI, "_request", fake_poly_request)
    await opinion.cancel_order(client_order_id="client-1")
    await poly.cancel_order(client_order_id="client-2")
    assert paths["opinion"].endswith("/client-1")
    assert paths["poly"].endswith("/client-2")
    await session.close()

