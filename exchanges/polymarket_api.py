from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import aiohttp
from eth_utils import to_checksum_address

from core.models import (
    ExchangeName,
    Fill,
    Market,
    Order,
    OrderBook,
    OrderSide,
    OrderStatus,
    OrderType,
)
from exchanges.base_client import BaseExchangeClient
from exchanges.orderbook_manager import OrderbookManager
from exchanges.rate_limiter import RateLimiter
from utils.logger import BotLogger


class PolymarketAPI(BaseExchangeClient):
    """Async interface for interacting with Polymarket endpoints."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        api_key: str,
        secret: str,
        passphrase: str | None,
        wallet_address: str | None,
        rate_limit: RateLimiter,
        logger: BotLogger | None = None,
        proxy: str | None = None,
        rest_url: str = "https://clob.polymarket.com",
        data_url: str = "https://gamma-api.polymarket.com",
    ):
        super().__init__(rest_url, session, api_key, secret, rate_limit, logger, proxy)
        if not passphrase:
            raise ValueError("Polymarket passphrase is required")
        if not wallet_address:
            raise ValueError("Polymarket wallet_address is required")
        self.passphrase = passphrase
        try:
            self.wallet_address = to_checksum_address(wallet_address)
        except ValueError as exc:
            raise ValueError("Invalid Polymarket wallet address") from exc
        self.data_url = data_url.rstrip("/")
        self.orderbooks = OrderbookManager()

    async def fetch_markets(self) -> List[Market]:
        data = await self._request_data("GET", "/markets")
        records = data.get("markets", data) if isinstance(data, dict) else data
        return [self._parse_market(entry) for entry in records]

    async def fetch_market(self, market_id: str) -> Market:
        data = await self._request_data("GET", f"/markets/{market_id}")
        payload = data.get("market", data) if isinstance(data, dict) else data
        return self._parse_market(payload)

    async def get_orderbook(self, market_id: str) -> OrderBook:
        data = await self._request_data("GET", f"/markets/{market_id}/orderbook")
        bids = [{"price": float(b["price"]), "size": float(b["amount"])} for b in data.get("bids", [])]
        asks = [{"price": float(a["price"]), "size": float(a["amount"])} for a in data.get("asks", [])]
        return self.orderbooks.parse_orderbook(market_id, bids, asks)

    async def place_limit_order(
        self,
        market_id: str,
        side: OrderSide | str,
        price: float,
        size: float,
        client_order_id: str | None = None,
    ) -> Order:
        side_value = side.value.lower() if isinstance(side, OrderSide) else str(side).lower()
        payload = {
            "market_id": market_id,
            "side": side_value,
            "type": "limit",
            "price": price,
            "size": size,
            "client_order_id": client_order_id,
        }
        data = await self._request("POST", "/orders", payload=payload)
        return self._parse_order(data)

    async def place_market_order(
        self,
        market_id: str,
        side: OrderSide | str,
        size: float,
        client_order_id: str | None = None,
    ) -> Order:
        side_value = side.value.lower() if isinstance(side, OrderSide) else str(side).lower()
        payload = {
            "market_id": market_id,
            "side": side_value,
            "type": "market",
            "size": size,
            "client_order_id": client_order_id,
        }
        data = await self._request("POST", "/orders", payload=payload)
        return self._parse_order(data)

    async def cancel_order(
        self,
        order_id: str | None = None,
        client_order_id: str | None = None,
    ) -> bool:
        identifier = order_id or client_order_id
        if not identifier:
            raise ValueError("order_id or client_order_id is required")
        await self._request("DELETE", f"/orders/{identifier}")
        return True

    async def get_order_status(self, order_id: str) -> Order:
        data = await self._request("GET", f"/orders/{order_id}")
        return self._parse_order(data)

    async def get_recent_trades(self, market_id: str) -> List[Dict[str, Any]]:
        data = await self._request("GET", "/trades", params={"market_id": market_id})
        return data.get("trades", [])

    async def get_balances(self) -> Dict[str, float]:
        data = await self._request(
            "GET",
            "/balance-allowance",
            params={"asset_type": "COLLATERAL"},
        )
        balance = float(data.get("balance", 0.0))
        allowance = float(data.get("allowance", 0.0))
        return {
            "USDC": balance,
            "USDC_allowance": allowance,
        }

    async def get_positions(self) -> List[Dict[str, Any]]:
        data = await self._request_data("GET", "/positions")
        if isinstance(data, dict):
            return data.get("positions", [])
        return data

    async def fetch_fills(self, since: Optional[float] = None) -> List[Fill]:
        params = {"since": since} if since else None
        data = await self._request("GET", "/orders/fills", params=params)
        fills = []
        for entry in data.get("fills", []):
            fills.append(self._parse_fill(entry))
        return fills

    async def fetch_user_trades(self, since: Optional[float] = None) -> List[Fill]:
        return await self.fetch_fills(since)

    async def fetch_user_trades(self, since: Optional[float] = None) -> List[Fill]:
        return await self.fetch_fills(since)

    async def listen_fills(self, handler):
        raise NotImplementedError("Polymarket client does not expose websocket fills")

    async def close(self) -> None:
        return None

    def _parse_market(self, payload: Dict[str, Any]) -> Market:
        return Market(
            market_id=str(payload.get("id") or payload.get("market_id")),
            name=payload.get("question") or payload.get("name", ""),
            exchange=ExchangeName.POLYMARKET,
            status=payload.get("status"),
            extra={
                "category": payload.get("category", ""),
                "volume": str(payload.get("volume", "")),
            },
        )

    def _parse_order(self, payload: Dict[str, Any]) -> Order:
        created_at = payload.get("created_at") or payload.get("timestamp") or time.time()
        if isinstance(created_at, str):
            created_dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        else:
            created_dt = datetime.fromtimestamp(float(created_at), tz=timezone.utc)
        raw_status = str(payload.get("status", "OPEN")).upper()
        try:
            status = OrderStatus(raw_status)
        except ValueError:
            status = OrderStatus.OPEN
        raw_side = str(payload.get("side", "BUY")).upper()
        try:
            side = OrderSide(raw_side)
        except ValueError:
            side = OrderSide.BUY
        order_type = (
            OrderType.LIMIT if str(payload.get("type", "limit")).lower() == "limit" else OrderType.MARKET
        )
        return Order(
            order_id=str(payload.get("order_id") or payload.get("id")),
            client_order_id=str(payload.get("client_order_id") or payload.get("order_id")),
            market_id=str(payload.get("market_id")),
            exchange=ExchangeName.POLYMARKET,
            side=side,
            order_type=order_type,
            price=float(payload.get("price", 0)),
            size=float(payload.get("size", 0)),
            filled_size=float(payload.get("filled_size", payload.get("fillAmount", 0))),
            status=status,
            created_at=created_dt,
        )

    def _parse_fill(self, payload: Dict[str, Any]) -> Fill:
        ts = payload.get("timestamp") or payload.get("filled_at") or time.time()
        if isinstance(ts, str):
            ts_dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        else:
            ts_dt = datetime.fromtimestamp(float(ts), tz=timezone.utc)
        try:
            side = OrderSide(payload.get("side", "BUY").upper())
        except ValueError:
            side = OrderSide.BUY
        return Fill(
            order_id=str(payload.get("order_id") or payload.get("id")),
            market_id=str(payload.get("market_id")),
            exchange=ExchangeName.POLYMARKET,
            side=side,
            price=float(payload.get("price") or 0.0),
            size=float(payload.get("size") or payload.get("filled_size") or 0.0),
            fee=float(payload.get("fee") or 0.0),
            timestamp=ts_dt,
        )

    async def _request_data(self, method: str, path: str) -> Dict[str, Any]:
        url = f"{self.data_url}/{path.lstrip('/')}"
        await self.rate_limit.acquire()
        async with self.session.request(method, url, proxy=self.proxy) as response:
            if response.status != 200:
                raise RuntimeError(f"polymarket data error {response.status}")
            return await response.json()

    def _auth_headers(
        self,
        method: str,
        path: str,
        payload: Dict[str, Any] | None = None,
        serialized_body: str | None = None,
    ) -> Dict[str, str]:
        timestamp = str(int(time.time()))
        request_path = path if path.startswith("/") else f"/{path}"
        message = f"{timestamp}{method.upper()}{request_path}"
        body_for_sig: str | Dict[str, Any] | None = serialized_body or payload
        if body_for_sig:
            if isinstance(body_for_sig, str):
                body_repr = body_for_sig
            else:
                body_repr = str(body_for_sig)
            message += body_repr.replace("'", '"')
        try:
            decoded_secret = base64.urlsafe_b64decode(self.secret)
        except Exception as exc:  # pragma: no cover - defensive conversion
            raise ValueError("Invalid Polymarket API secret; expected base64 string") from exc
        signature = base64.urlsafe_b64encode(
            hmac.new(decoded_secret, message.encode("utf-8"), hashlib.sha256).digest()
        ).decode("utf-8")
        return {
            "POLY_ADDRESS": self.wallet_address,
            "POLY_SIGNATURE": signature,
            "POLY_TIMESTAMP": timestamp,
            "POLY_API_KEY": self.api_key,
            "POLY_PASSPHRASE": self.passphrase,
        }

