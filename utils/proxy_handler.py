from __future__ import annotations

import asyncio
from typing import Dict, Optional

import aiohttp

from core.models import AccountCredentials
from utils.logger import BotLogger


class ProxyHandler:
    """Manages per-account HTTP sessions with optional proxy routing."""

    def __init__(self, logger: BotLogger | None = None):
        self.logger = logger or BotLogger(__name__)
        self._sessions: Dict[str, aiohttp.ClientSession] = {}
        self._proxies: Dict[str, Optional[str]] = {}
        self._locks: Dict[str, asyncio.Lock] = {}

    async def get_session(self, account: AccountCredentials) -> aiohttp.ClientSession:
        lock = self._locks.setdefault(account.account_id, asyncio.Lock())
        async with lock:
            session = self._sessions.get(account.account_id)
            if session and not session.closed:
                return session
            timeout = aiohttp.ClientTimeout(total=30)
            connector = aiohttp.TCPConnector(limit=100, ssl=False)
            session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                trust_env=False,
            )
            self._sessions[account.account_id] = session
            self._proxies[account.account_id] = account.proxy
            self.logger.debug(
                "created session",
                account_id=account.account_id,
                proxy=account.proxy or "none",
            )
            return session

    def get_proxy_for_account(self, account_id: str) -> Optional[str]:
        return self._proxies.get(account_id)

    async def close(self) -> None:
        await asyncio.gather(*(session.close() for session in self._sessions.values()), return_exceptions=True)
        self._sessions.clear()
        self._proxies.clear()
        self._locks.clear()

