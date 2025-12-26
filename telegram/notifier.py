from __future__ import annotations

import aiohttp

from utils.logger import BotLogger


class TelegramNotifier:
    """Sends updates via Telegram."""

    def __init__(self, token: str | None, chat_id: str | None, enabled: bool = False):
        self.token = token
        self.chat_id = chat_id
        self.enabled = enabled and bool(token)
        self.logger = BotLogger(__name__)
        self._session: aiohttp.ClientSession | None = None

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def send_message(
        self,
        msg: str,
        chat_id: str | None = None,
        parse_mode: str | None = "HTML",
        disable_web_page_preview: bool = True,
        reply_markup: dict | None = None,
    ) -> bool:
        if not self.enabled:
            return False
        msg = self._format_outgoing(msg)
        target_chat = chat_id or self.chat_id
        if not target_chat:
            return False
        session = await self._ensure_session()
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload = {"chat_id": target_chat, "text": msg}
        if parse_mode:
            payload["parse_mode"] = parse_mode
        if disable_web_page_preview:
            payload["disable_web_page_preview"] = True
        if reply_markup:
            payload["reply_markup"] = reply_markup
        try:
            async with session.post(url, json=payload) as response:
                if response.status != 200:
                    self.logger.warn("telegram send failed", status=response.status)
                    return False
        except Exception as exc:  # pragma: no cover - network guard
            self.logger.warn("telegram send exception", error=str(exc))
            return False
        return True

    def _format_outgoing(self, msg: str) -> str:
        prefix = "Market-hedge bot started"
        if msg.startswith(prefix):
            bullet = "▫️"
            dry_run = None
            pairs = None
            for part in msg.split("|"):
                part = part.strip()
                if part.startswith("dry_run="):
                    dry_run = part.split("=", 1)[1].strip()
                if part.startswith("pairs="):
                    pairs = part.split("=", 1)[1].strip()
            mode = "🧪 Dry-run" if (dry_run or "").lower() == "true" else "🟢 Live"
            pair_text = pairs or "—"
            lines = [
                "🤖 <b>Market-Hedge Bot запущен</b>",
                "",
                f"{bullet} Режим: {mode}",
                f"{bullet} Активных пар: {pair_text}",
                "",
                "Используйте /status или /health",
            ]
            return "\n".join(lines)
        return msg

    async def set_commands(self, commands: list[dict[str, str]], language_code: str | None = "ru") -> bool:
        if not self.enabled or not commands:
            return False
        session = await self._ensure_session()
        url = f"https://api.telegram.org/bot{self.token}/setMyCommands"
        payload: dict[str, object] = {"commands": commands}
        if language_code:
            payload["language_code"] = language_code
        try:
            async with session.post(url, json=payload) as response:
                if response.status != 200:
                    self.logger.warn("telegram set commands failed", status=response.status)
                    return False
        except Exception as exc:  # pragma: no cover - network guard
            self.logger.warn("telegram set commands exception", error=str(exc))
            return False
        return True

    async def fetch_updates(self, offset: int | None = None, timeout: int = 25) -> list[dict]:
        if not self.enabled:
            return []
        session = await self._ensure_session()
        url = f"https://api.telegram.org/bot{self.token}/getUpdates"
        params = {"timeout": timeout}
        if offset is not None:
            params["offset"] = offset
        try:
            async with session.get(url, params=params, timeout=timeout + 5) as response:
                if response.status != 200:
                    self.logger.warn("telegram updates failed", status=response.status)
                    return []
                payload = await response.json()
        except Exception as exc:
            self.logger.warn("telegram updates exception", error=str(exc))
            return []
        result = payload.get("result")
        if not isinstance(result, list):
            return []
        return result

    async def close(self) -> None:
        if self._session:
            await self._session.close()

