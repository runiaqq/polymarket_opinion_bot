from __future__ import annotations

from datetime import datetime
from typing import List

from core.event_discovery import MatchedEventPair
from core.event_discovery.approvals import EventApprovalStore
from core.event_discovery.normalizer import normalize_title
from core.event_discovery.registry import EventDiscoveryRegistry
from utils.logger import BotLogger


class EventReviewHandler:
    """Handles Telegram presentation and approval flow for discovered events."""

    def __init__(
        self,
        registry: EventDiscoveryRegistry,
        approvals: EventApprovalStore,
        notifier,
        logger: BotLogger | None = None,
    ):
        self.registry = registry
        self.approvals = approvals
        self.notifier = notifier
        self.logger = logger or BotLogger(__name__)

    async def send_pending_events(self, chat_id: str) -> None:
        pending = self.registry.list_pending()
        if not pending:
            await self.notifier.send_message("ℹ️ Новых событий нет. Попробуйте позже.", chat_id=chat_id)
            return
        for match in pending:
            await self._send_event_card(match, chat_id)

    async def _send_event_card(self, match: MatchedEventPair, chat_id: str) -> None:
        match_id = self.registry.match_id(match)
        title_norm, _ = normalize_title(match.opinion_event.title or match.polymarket_event.title or "")
        confidence = f"{match.confidence_score * 100:.1f}%"
        end_dates = []
        if match.opinion_event.end_time:
            end_dates.append(f"Opinion: {self._fmt_date(match.opinion_event.end_time)}")
        if match.polymarket_event.end_time:
            end_dates.append(f"Polymarket: {self._fmt_date(match.polymarket_event.end_time)}")
        liquidity = self._fmt_liquidity(match)
        lines = [
            "🧠 <b>Найдено потенциальное событие</b>",
            f"Название: <b>{title_norm or '—'}</b>",
            "Биржи: Opinion ↔ Polymarket",
            "",
            "📊 Сравнение:",
            f"▫️ Opinion: {match.opinion_event.title or '—'}",
            f"▫️ Polymarket: {match.polymarket_event.title or '—'}",
            "",
            f"🔗 Match score: {confidence}",
        ]
        if end_dates:
            lines.append("📅 Даты:")
            lines.extend([f"▫️ {row}" for row in end_dates])
        if liquidity:
            lines.append(f"📈 Ликвидность: {liquidity}")
        msg = "\n".join(lines)
        buttons = [
            [
                {"text": "✅ Подтвердить", "callback_data": f"event:approve:{match_id}"},
                {"text": "❌ Отклонить", "callback_data": f"event:reject:{match_id}"},
            ],
            [{"text": "ℹ️ Подробнее", "callback_data": f"event:details:{match_id}"}],
        ]
        await self.notifier.send_message(msg, chat_id=chat_id, reply_markup={"inline_keyboard": buttons})

    async def handle_callback(self, chat_id: str, data: str) -> None:
        if not data.startswith("event:"):
            return
        parts = data.split(":", 2)
        if len(parts) != 3:
            return
        action, match_id = parts[1], parts[2]
        match = self.registry.find_match(match_id)
        if not match:
            await self.notifier.send_message("⚠️ Событие не найдено или устарело.", chat_id=chat_id)
            return
        if action == "approve":
            self.registry.mark_approved(match_id)
            snippet = self.registry.export_yaml(event_id=match_id)
            await self.notifier.send_message(
                "✅ Событие подтверждено. Готово к добавлению в торговлю.\n\n"
                "Скопируйте сниппет и добавьте вручную в market_pairs:\n"
                f"<pre>{snippet}</pre>",
                chat_id=chat_id,
                parse_mode="HTML",
            )
        elif action == "reject":
            self.registry.mark_rejected(match_id)
            await self.notifier.send_message("❌ Событие отклонено.", chat_id=chat_id)
        elif action == "details":
            await self._send_details(match, chat_id)

    async def _send_details(self, match: MatchedEventPair, chat_id: str) -> None:
        keywords_op = ", ".join(sorted(match.opinion_event.metadata.get("keywords", []))) if match.opinion_event.metadata else "—"
        keywords_pm = ", ".join(sorted(match.polymarket_event.metadata.get("keywords", []))) if match.polymarket_event.metadata else "—"
        msg = "\n".join(
            [
                "ℹ️ <b>Детали совпадения</b>",
                f"Opinion: {match.opinion_event.title} (id: {match.opinion_event.event_id})",
                f"Polymarket: {match.polymarket_event.title} (id: {match.polymarket_event.event_id})",
                "",
                f"Match score: {match.confidence_score * 100:.1f}%",
                f"Ключевые слова Opinion: {keywords_op}",
                f"Ключевые слова Polymarket: {keywords_pm}",
                "",
                "Почему предложено:",
                "▫️ Похожие названия",
                "▫️ Пересечение ключевых слов",
                "▫️ Близкие даты окончания",
            ]
        )
        await self.notifier.send_message(msg, chat_id=chat_id, parse_mode="HTML")

    def _fmt_date(self, dt: datetime) -> str:
        return dt.strftime("%Y-%m-%d")

    def _fmt_liquidity(self, match: MatchedEventPair) -> str:
        def _val(meta):
            if not meta:
                return None
            for key in ("liquidity", "volume", "24hVolume", "tvl"):
                if key in meta:
                    try:
                        return float(meta.get(key))
                    except (TypeError, ValueError):
                        return meta.get(key)
            return None

        op_liq = _val(match.opinion_event.metadata)
        pm_liq = _val(match.polymarket_event.metadata)
        parts: List[str] = []
        if op_liq is not None:
            parts.append(f"Opinion ~ {op_liq}")
        if pm_liq is not None:
            parts.append(f"Polymarket ~ {pm_liq}")
        return " | ".join(parts)


__all__ = ["EventReviewHandler"]

