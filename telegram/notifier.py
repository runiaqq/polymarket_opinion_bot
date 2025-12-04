from __future__ import annotations

import aiohttp

from utils.logger import BotLogger


class TelegramNotifier:
    """Sends updates via Telegram."""

    def __init__(self, token: str | None, chat_id: str | None, enabled: bool = False):
        self.token = token
        self.chat_id = chat_id
        self.enabled = enabled and bool(token and chat_id)
        self.logger = BotLogger(__name__)
        self._session: aiohttp.ClientSession | None = None

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def send_message(self, msg: str) -> bool:
        if not self.enabled:
            return False
        session = await self._ensure_session()
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload = {"chat_id": self.chat_id, "text": msg}
        async with session.post(url, json=payload) as response:
            if response.status != 200:
                self.logger.warn("telegram send failed", status=response.status)
                return False
        return True

    async def close(self) -> None:
        if self._session:
            await self._session.close()

