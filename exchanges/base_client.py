from __future__ import annotations

import asyncio
import json
import random
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

import aiohttp

from core.exceptions import FatalExchangeError, RecoverableExchangeError
from exchanges.rate_limiter import RateLimiter
from utils.logger import BotLogger


class BaseExchangeClient(ABC):
    """Base class with request/retry helpers shared by exchange clients."""

    def __init__(
        self,
        base_url: str,
        session: aiohttp.ClientSession,
        api_key: str,
        secret: str,
        rate_limit: RateLimiter,
        logger: BotLogger | None = None,
        proxy: str | None = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.session = session
        self.api_key = api_key
        self.secret = secret
        self.rate_limit = rate_limit
        self.logger = logger or BotLogger(self.__class__.__name__)
        self.proxy = proxy
        self.max_retries = 5

    async def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        payload: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        auth: bool = True,
    ) -> Dict[str, Any]:
        url = f"{self.base_url}/{path.lstrip('/')}"
        attempt = 0
        while True:
            attempt += 1
            await self.rate_limit.acquire()
            serialized_payload: Optional[str] = None
            if payload is not None:
                serialized_payload = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
            req_headers = self._build_headers(
                method,
                path,
                payload,
                serialized_payload,
                headers,
                auth,
            )
            data_bytes = serialized_payload.encode("utf-8") if serialized_payload is not None else None
            try:
                async with self.session.request(
                    method,
                    url,
                    params=params,
                    data=data_bytes,
                    headers=req_headers,
                    proxy=self.proxy,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as response:
                    content = await self._parse_response(response)
                    return content
            except RecoverableExchangeError as exc:
                if attempt >= self.max_retries:
                    raise
                backoff = min(2 ** attempt, 30) + random.random()
                self.logger.warn(
                    "recoverable exchange error",
                    path=path,
                    attempt=attempt,
                    backoff=backoff,
                    error=str(exc),
                )
                await asyncio.sleep(backoff)
            except FatalExchangeError:
                raise

    async def _parse_response(self, response: aiohttp.ClientResponse) -> Dict[str, Any]:
        try:
            data = await response.json()
        except aiohttp.ContentTypeError:
            text = await response.text()
            raise FatalExchangeError(f"unexpected response: {text}")

        if 200 <= response.status < 300:
            return data

        error_msg = data.get("error") or data.get("message") or str(data)
        if response.status in (429, 500, 502, 503, 504):
            raise RecoverableExchangeError(error_msg)
        raise FatalExchangeError(error_msg)

    def _build_headers(
        self,
        method: str,
        path: str,
        payload: Optional[Dict[str, Any]],
        serialized_body: Optional[str],
        headers: Optional[Dict[str, str]],
        auth: bool,
    ) -> Dict[str, str]:
        combined = {"Content-Type": "application/json"}
        if headers:
            combined.update(headers)
        if auth:
            combined.update(self._auth_headers(method, path, payload, serialized_body))
        return combined

    @abstractmethod
    def _auth_headers(
        self,
        method: str,
        path: str,
        payload: Optional[Dict[str, Any]] = None,
        serialized_body: Optional[str] = None,
    ) -> Dict[str, str]:
        """Return headers required for authenticated endpoints."""

