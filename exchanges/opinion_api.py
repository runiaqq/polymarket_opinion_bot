from __future__ import annotations

import hashlib
import hmac
import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import aiohttp

from aiohttp import ClientResponseError, ContentTypeError

from core.exceptions import FatalExchangeError
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
from exchanges.websocket_manager import WebSocketManager
from utils.logger import BotLogger


class OpinionAPI(BaseExchangeClient):
    """Async interface for interacting with the Opinion exchange."""

    # Opinion websocket endpoint is currently unavailable; keep the flag for capability checks.
    supports_websocket = False

    def __init__(
        self,
        session: aiohttp.ClientSession,
        api_key: str,
        secret: str,
        rate_limit: RateLimiter,
        logger: BotLogger | None = None,
        proxy: str | None = None,
        rest_url: str = "https://api.opinion.trade/v1",
        ws_url: str = "wss://ws.opinion.trade/v1",
    ):
        super().__init__(rest_url, session, api_key, secret, rate_limit, logger, proxy)
        self.orderbooks = OrderbookManager()
        # WebSocketManager is retained for optional future use but is not exercised today.
        self.ws = WebSocketManager(ws_url, session, logger=self.logger, proxy=proxy)

    async def get_markets(self, page: int = 1, limit: int = 20, status: str = "activated") -> List[Market]:
        """Discovery via OpenAPI (not CLOB REST)."""
        url = "https://openapi.opinion.trade/openapi/market"
        headers = {"apikey": self.api_key}
        params = {"page": page, "limit": min(limit, 20), "status": status, "marketType": 2}
        try:
            async with self.session.get(url, headers=headers, params=params, proxy=self.proxy, timeout=30) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    raise FatalExchangeError(f"openapi discovery failed ({resp.status}): {text}")
                try:
                    payload = await resp.json()
                except ContentTypeError:
                    text = await resp.text()
                    raise FatalExchangeError(f"openapi discovery invalid content: {text}")
        except ClientResponseError as exc:
            raise FatalExchangeError(f"openapi discovery error: {exc}") from exc
        if not isinstance(payload, dict):
            raise FatalExchangeError("openapi discovery returned non-dict payload")
        result = payload.get("result") or payload.get("data") or {}
        markets = result.get("list", []) if isinstance(result, dict) else []
        return [self._parse_market(item) for item in markets]

    async def get_market(self, token_id: str) -> Market:
        data = await self._api_request("GET", f"/markets/{token_id}", auth=False)
        return self._parse_market(data.get("data", {}))

    async def get_orderbook(self, token_id: str) -> OrderBook:
        """Fetch Opinion orderbook via OpenAPI token endpoint using token_id."""
        url = "https://openapi.opinion.trade/openapi/token/orderbook"
        headers = {"apikey": self.api_key}
        params = {"token_id": token_id}
        try:
            async with self.session.get(url, headers=headers, params=params, proxy=self.proxy, timeout=30) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    raise FatalExchangeError(f"unexpected response: {text}")
                data = await resp.json()
            payload = data.get("result") or data.get("data") or data
            bids = [{"price": float(b.get("price", 0)), "size": float(b.get("amount", b.get("size", 0)))} for b in payload.get("bids", [])]
            asks = [{"price": float(a.get("price", 0)), "size": float(a.get("amount", a.get("size", 0)))} for a in payload.get("asks", [])]
            orderbook = self.orderbooks.parse_orderbook(token_id, bids, asks)
            self.last_orderbook_at = datetime.now(tz=timezone.utc)
            self.last_orderbook_error = None
            return orderbook
        except Exception as exc:
            self.last_orderbook_error = str(exc)
            raise

    async def place_limit_order(
        self,
        market_id: str,
        side: OrderSide,
        price: float,
        size: float,
        client_order_id: Optional[str] = None,
    ) -> Order:
        token_id = str(market_id)
        numeric_market_id: Optional[int] = None
        try:
            numeric_market_id = int(token_id)
        except ValueError:
            market = await self.get_market(token_id)
            try:
                numeric_market_id = int(market.market_id)
            except (TypeError, ValueError) as exc:
                raise ValueError("opinion market id must be numeric") from exc
        return await self.place_order(
            market_id=numeric_market_id,
            token_id=token_id,
            side=side,
            order_type=OrderType.LIMIT,
            price=price,
            quote_amount=size,
            client_order_id=client_order_id,
        )

    async def place_order(
        self,
        market_id: int,
        token_id: str,
        side: OrderSide,
        order_type: OrderType,
        price: Optional[float],
        quote_amount: Optional[float] = None,
        base_amount: Optional[float] = None,
        client_order_id: Optional[str] = None,
    ) -> Order:
        payload = {
            "marketId": int(market_id),
            "tokenId": token_id,
            "side": self._serialize_side(side),
            "orderType": self._serialize_order_type(order_type),
            "price": f"{price:.6f}" if price is not None else None,
            "makerAmountInQuoteToken": f"{quote_amount:.6f}" if quote_amount is not None else None,
            "makerAmountInBaseToken": f"{base_amount:.6f}" if base_amount is not None else None,
            "clientOrderId": client_order_id,
        }
        payload = {k: v for k, v in payload.items() if v is not None}
        data = await self._api_request("POST", "/orders", payload=payload, auth=True)
        return self._parse_order(data.get("data", data))

    async def cancel_order(
        self,
        order_id: str | None = None,
        client_order_id: str | None = None,
    ) -> bool:
        identifier = order_id or client_order_id
        if not identifier:
            raise ValueError("order_id or client_order_id is required")
        await self._api_request("DELETE", f"/orders/{identifier}", auth=True)
        return True

    async def get_balances(self) -> Dict[str, float]:
        data = await self._api_request("GET", "/balances", auth=True)
        return data.get("balances", {})

    async def get_positions(self) -> List[Dict[str, Any]]:
        data = await self._api_request("GET", "/positions", auth=True)
        return data.get("list", [])

    async def get_trades(self, token_id: Optional[str] = None) -> List[Dict[str, Any]]:
        params = {"tokenId": token_id} if token_id else None
        data = await self._api_request("GET", "/trades", params=params, auth=True)
        return data.get("list", [])

    async def fetch_fills(self, token_id: Optional[str] = None) -> List[Fill]:
        try:
            trades = await self.get_trades(token_id)
        except Exception as exc:
            # Defensive: missing/404 trade endpoint should not crash reconciliation polling.
            self.logger.warn("opinion trades fetch failed", error=str(exc))
            return []
        return [self._trade_to_fill(entry) for entry in trades]

    async def fetch_user_trades(self, since: Optional[float] = None) -> List[Fill]:
        """
        REST fallback for fills polling. Filters locally using the provided unix timestamp.
        """
        # Disable noisy polling (404s) for now; websockets are unavailable on Opinion.
        return []

    async def listen_fills(self, handler):
        if not self.supports_websocket:
            raise RuntimeError("Opinion websocket is unavailable; use REST polling instead")
        async def _wrapped(message: Dict[str, Any]):
            fill = self._parse_ws_fill(message)
            if fill:
                await handler(fill)

        await self.ws.subscribe({"topic": "order"})
        self.ws.set_handler(_wrapped)
        await self.ws.listen()

    async def close(self) -> None:
        await self.ws.close()

    def _parse_market(self, payload: Dict[str, Any]) -> Market:
        market_id = payload.get("marketId") or payload.get("market_id") or payload.get("topic_id")
        name = payload.get("marketTitle") or payload.get("topic_title", "")
        market_type = payload.get("marketType")
        status_enum = payload.get("statusEnum") or payload.get("status")
        yes_token = payload.get("yesTokenId")
        no_token = payload.get("noTokenId")
        child_markets = payload.get("childMarkets") or []
        return Market(
            market_id=str(market_id),
            name=name,
            exchange=ExchangeName.OPINION,
            status=str(status_enum),
            extra={
                "marketType": market_type,
                "yesTokenId": yes_token,
                "noTokenId": no_token,
                "childMarkets": child_markets,
                "volume": str(payload.get("volume")),
            },
        )

    def _parse_order(self, payload: Dict[str, Any]) -> Order:
        created = payload.get("createdAt") or payload.get("created_at") or time.time()
        if isinstance(created, str):
            created_dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
        else:
            created_dt = datetime.fromtimestamp(float(created), tz=timezone.utc)
        status_raw = str(payload.get("status", "OPEN")).upper()
        try:
            status = OrderStatus(status_raw)
        except ValueError:
            status = OrderStatus.OPEN
        side_raw = str(payload.get("side", "BUY")).upper()
        try:
            side = OrderSide(side_raw)
        except ValueError:
            side = OrderSide.BUY
        order_type_raw = str(payload.get("order_type", "limit")).upper()
        order_type = OrderType.LIMIT if order_type_raw == "LIMIT" else OrderType.MARKET
        return Order(
            order_id=str(payload.get("order_id") or payload.get("id")),
            client_order_id=str(payload.get("client_order_id") or payload.get("order_id")),
            market_id=str(payload.get("token_id") or payload.get("market_id")),
            exchange=ExchangeName.OPINION,
            side=side,
            order_type=order_type,
            price=float(payload.get("price") or payload.get("order_price") or 0.0),
            size=float(payload.get("size") or payload.get("makerAmount") or 0.0),
            filled_size=float(payload.get("filled_size") or payload.get("matchedAmount") or 0.0),
            status=status,
            created_at=created_dt,
        )

    def _auth_headers(
        self,
        method: str,
        path: str,
        payload: Dict[str, Any] | None = None,
        serialized_body: str | None = None,
    ) -> Dict[str, str]:
        timestamp = str(int(time.time() * 1000))
        body = serialized_body or json.dumps(payload or {}, separators=(",", ":"), sort_keys=True)
        signature = hmac.new(
            self.secret.encode(),
            f"{timestamp}{body}".encode(),
            hashlib.sha256,
        ).hexdigest()
        return {
            "X-OPINION-KEY": self.api_key,
            "X-OPINION-TIMESTAMP": timestamp,
            "X-OPINION-SIGNATURE": signature,
        }

    async def _api_request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        payload: Optional[Dict[str, Any]] = None,
        auth: bool = True,
    ) -> Dict[str, Any]:
        response = await super()._request(
            method,
            path,
            params=params,
            payload=payload,
            auth=auth,
        )
        errno = response.get("errno", 0)
        if errno != 0:
            raise FatalExchangeError(response.get("errmsg", "unknown opinion error"))
        return response.get("result", {})

    def _serialize_side(self, side: OrderSide) -> int:
        mapping = {OrderSide.BUY: 0, OrderSide.SELL: 1}
        return mapping[side]

    def _serialize_order_type(self, order_type: OrderType) -> int:
        mapping = {OrderType.MARKET: 1, OrderType.LIMIT: 2}
        return mapping[order_type]

    def _parse_ws_fill(self, message: Dict[str, Any]) -> Optional[Fill]:
        if message.get("topic") != "order":
            return None
        if message.get("event") not in {"filled", "partial"}:
            return None
        data = message.get("data", {})
        order = data.get("order") or data
        if not order:
            return None
        try:
            side = OrderSide(order.get("side", "BUY").upper())
        except ValueError:
            side = OrderSide.BUY
        timestamp = order.get("filledAt") or order.get("updatedAt") or time.time()
        if isinstance(timestamp, str):
            ts = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        else:
            ts = datetime.fromtimestamp(float(timestamp), tz=timezone.utc)
        return Fill(
            order_id=str(order.get("orderId") or order.get("order_id")),
            market_id=str(order.get("tokenId") or order.get("token_id")),
            exchange=ExchangeName.OPINION,
            side=side,
            price=float(order.get("price") or order.get("matchedPrice") or 0.0),
            size=float(order.get("matchedAmount") or order.get("fillSize") or 0.0),
            fee=float(order.get("fee") or 0.0),
            timestamp=ts,
        )

    def _trade_to_fill(self, trade: Dict[str, Any]) -> Fill:
        side = OrderSide.BUY if str(trade.get("side", "BUY")).upper() == "BUY" else OrderSide.SELL
        ts_raw = trade.get("matchedAt") or trade.get("createdAt") or time.time()
        if isinstance(ts_raw, str):
            ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
        else:
            ts = datetime.fromtimestamp(float(ts_raw), tz=timezone.utc)
        return Fill(
            order_id=str(trade.get("orderId") or trade.get("order_id")),
            market_id=str(trade.get("tokenId") or trade.get("token_id")),
            exchange=ExchangeName.OPINION,
            side=side,
            price=float(trade.get("price") or 0.0),
            size=float(trade.get("matchedAmount") or trade.get("size") or 0.0),
            fee=float(trade.get("fee") or 0.0),
            timestamp=ts,
        )

