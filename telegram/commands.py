from __future__ import annotations

import asyncio
import html
import logging
import time
from contextlib import suppress
from datetime import datetime
from typing import Any, Dict, List, Optional

from core.healthcheck import HealthcheckResult, HealthcheckService
from core.models import ExchangeName
from exchanges.orderbook_manager import OrderbookManager
from utils.config_loader import MarketPairConfig, Settings
from utils.logger import BotLogger

BULLET = "▫️"
SUB_BULLET = "•"

TELEGRAM_COMMANDS: list[dict[str, str]] = [
    {"command": "start", "description": "🚀 Запуск бота — проверка подключения и приветствие"},
    {"command": "status", "description": "📊 Статус системы — общий статус бота и рынков"},
    {"command": "pairs", "description": "🔗 Торговые пары — список активных пар"},
    {"command": "health", "description": "🩺 Проверка рынков — стаканы и спреды"},
    {"command": "simulate", "description": "🧪 Симуляция сделки — проверка логики без ордеров"},
    {"command": "debug", "description": "🛠 Режим отладки — включить/выключить debug"},
    {"command": "events", "description": "🧠 Найденные события — подтвердить или отклонить"},
]


def _escape(value: object) -> str:
    return html.escape(str(value))


def _fmt_bool(value: bool) -> str:
    return "✅" if value else "❌"


def _fmt_price(value: float | None) -> str:
    if value is None:
        return "—"
    return f"{value:.6f}".rstrip("0").rstrip(".")


def _fmt_time(value: Optional[str]) -> str:
    if not value:
        return "нет данных"
    return _escape(value)


class MessageBuilder:
    @staticmethod
    def startup(chat_id: str, pairs_count: int, dry_run: bool, double_limit: bool) -> str:
        mode = "🧪 Dry-run" if dry_run else "🟢 Live"
        lines = [
            "🤖 <b>Market-Hedge Bot запущен</b>",
            "",
            f"{BULLET} Режим: {mode}",
            f"{BULLET} Double-limit: {_fmt_bool(double_limit)}",
            f"{BULLET} Активных пар: {pairs_count}",
            f"{BULLET} ID чата: {_escape(chat_id)}",
            "",
            "Используйте /status или /health",
        ]
        return "\n".join(lines)

    @staticmethod
    def status(
        snapshot: Dict[str, Any],
        settings: Settings,
        orderbook_times: Dict[str, str | None],
        metrics: Dict[str, Any],
        status: Dict[str, Any],
        poll_intervals: Dict[str, float],
        account_counts: Dict[str, int],
    ) -> str:
        mode = "🧪 Dry-run" if settings.dry_run else "🟢 Live"
        accounts = " | ".join(f"{name}: {count}" for name, count in account_counts.items()) if account_counts else "—"
        poll_lines = [f"{SUB_BULLET} {name}: {int(interval)} сек" for name, interval in poll_intervals.items()] or [
            f"{SUB_BULLET} нет данных"
        ]
        ob_lines = [f"{SUB_BULLET} {name}: {_fmt_time(ts)}" for name, ts in orderbook_times.items()] or [
            f"{SUB_BULLET} нет данных"
        ]
        db_backend = status.get("backend") or "—"
        db_last_write = status.get("last_write")
        metrics_line = (
            f"{SUB_BULLET} processed: {metrics.get('processed', 0)} | dup: {metrics.get('duplicates', 0)} "
            f"| events: {metrics.get('poll_events', 0)}"
        )
        lines = [
            "📊 <b>Статус системы</b>",
            "",
            f"{BULLET} Активные пары: {snapshot.get('count', 0)}",
            f"{BULLET} Режим: {mode}",
            f"{BULLET} Double-limit: {_fmt_bool(settings.double_limit_enabled)}",
            f"{BULLET} Аккаунты: {accounts}",
            "",
            "⏱ Интервалы опроса:",
            *poll_lines,
            "",
            "📚 Ордербуки:",
            *ob_lines,
            "",
            "🔄 Реконсайлер:",
            metrics_line,
            "",
            "🗄 База данных:",
            f"{SUB_BULLET} backend: {db_backend}",
            f"{SUB_BULLET} last_write: {_fmt_time(db_last_write)}",
        ]
        return "\n".join(lines)

    @staticmethod
    def pairs(pairs: List[MarketPairConfig], settings: Settings) -> str:
        lines: List[str] = ["🔗 <b>Торговые пары</b>", ""]
        if not pairs:
            lines.append("Нет активных пар.")
            return "\n".join(lines)
        for pair in pairs:
            primary_ex = (pair.primary_exchange or settings.exchanges.primary).value
            secondary_ex = (pair.secondary_exchange or settings.exchanges.secondary).value
            strategy = pair.strategy_direction.value if pair.strategy_direction else "—"
            lines.append(f"🔹 {_escape(pair.event_id)}")
            lines.append(f"{SUB_BULLET} {primary_ex}: {_escape(pair.primary_market_id or '—')}")
            lines.append(f"{SUB_BULLET} {secondary_ex}: {_escape(pair.secondary_market_id or '—')}")
            lines.append(f"{SUB_BULLET} Направление: {_escape(strategy)}")
            lines.append("")
        return "\n".join(lines).strip()

    @staticmethod
    def health(results: List[HealthcheckResult]) -> str:
        lines: List[str] = ["🩺 <b>Проверка рынков</b>", ""]
        for row in results:
            lines.extend(MessageBuilder._health_row(row))
            lines.append("")
        return "\n".join(lines).strip()

    @staticmethod
    def _health_row(row: HealthcheckResult) -> List[str]:
        direction = row.chosen_direction or "—"
        spread_txt = MessageBuilder._format_spread(row)
        lines = [
            f"🔹 {_escape(row.pair_id)}",
            MessageBuilder._format_book_line(row.primary_exchange, row.primary_status, row.primary_top),
            MessageBuilder._format_book_line(row.secondary_exchange, row.secondary_status, row.secondary_top),
            f"{SUB_BULLET} Направление: {_escape(direction)}",
            f"{SUB_BULLET} Спред: {spread_txt}",
        ]
        if row.error:
            lines.append(f"{SUB_BULLET} Примечание: {_escape(row.error)}")
        return lines

    @staticmethod
    def _format_book_line(exchange: ExchangeName, status: str, top: Dict[str, float | None]) -> str:
        status_icon = "✅" if status == "OK" else "⚠️"
        bid = _fmt_price(top.get("bid"))
        ask = _fmt_price(top.get("ask"))
        if bid == "—" and ask == "—":
            return f"{SUB_BULLET} {exchange.value}: {status_icon} стакан пуст"
        return f"{SUB_BULLET} {exchange.value}: {status_icon} bid {bid} / ask {ask}"

    @staticmethod
    def _format_spread(row: HealthcheckResult) -> str:
        if row.net_total is not None:
            return f"{row.net_total:.6f}"
        spread_info = row.spreads.get("primary_buy_secondary_sell") or row.spreads.get("secondary_buy_primary_sell")
        if spread_info and "total" in spread_info:
            return f"{spread_info['total']:.6f}"
        return "—"

    @staticmethod
    def simulate_usage() -> str:
        lines = [
            "🧪 <b>Симуляция сделки</b>",
            "",
            "Использование:",
            f"{SUB_BULLET} /simulate &lt;pair_id&gt; [size]",
        ]
        return "\n".join(lines)

    @staticmethod
    def simulate_pair_not_found(pair_id: str) -> str:
        lines = [
            "⚠️ <b>Пара не найдена</b>",
            "",
            f"{BULLET} Запрошено: {_escape(pair_id)}",
            "Проверьте идентификатор и попробуйте снова.",
        ]
        return "\n".join(lines)

    @staticmethod
    def simulate_orderbook_error(error: Exception) -> str:
        lines = [
            "⚠️ <b>Не удалось получить стаканы</b>",
            "",
            f"{BULLET} Детали: {_escape(error)}",
            "Попробуйте позже или проверьте соединение.",
        ]
        return "\n".join(lines)

    @staticmethod
    def simulate_no_opportunity(size: float) -> str:
        lines = [
            "🧪 <b>Симуляция сделки</b>",
            "",
            "❌ Подходящая возможность не найдена",
            "",
            "Причина:",
            f"{SUB_BULLET} Спред не покрывает комиссии",
            f"{SUB_BULLET} Нет направления с положительным результатом",
            f"{SUB_BULLET} Недостаточно ликвидности для объёма {size}",
        ]
        return "\n".join(lines)

    @staticmethod
    def simulate_plan(
        pair_id: str,
        size: float,
        direction: str,
        primary_exchange: ExchangeName,
        primary_leg: Dict[str, Any],
        primary_slippage: float,
        secondary_exchange: ExchangeName,
        secondary_leg: Dict[str, Any],
        secondary_slippage: float,
        net_total: float | None,
        record_id: str,
        double_limit: bool,
    ) -> str:
        lines = [
            "🧪 <b>Симуляция сделки</b>",
            "",
            f"✅ План сохранён: {_escape(record_id)}",
            "",
            f"{BULLET} Пара: {_escape(pair_id)}",
            f"{BULLET} Размер: {size}",
            f"{BULLET} Направление: {_escape(direction)}",
            f"{BULLET} Double-limit: {_fmt_bool(double_limit)}",
            "",
            "Сделки:",
            f"{SUB_BULLET} {primary_exchange.value}: {primary_leg['side'].value.upper()} @ {primary_leg['price']:.4f} "
            f"(слиппедж ~ {primary_slippage:.6f})",
            f"{SUB_BULLET} {secondary_exchange.value}: {secondary_leg['side'].value.upper()} @ {secondary_leg['price']:.4f} "
            f"(слиппедж ~ {secondary_slippage:.6f})",
            "",
            f"Ожидаемый результат: {net_total:.6f}" if net_total is not None else "Ожидаемый результат: —",
        ]
        return "\n".join(lines)

    @staticmethod
    def debug_status(enabled: bool) -> str:
        status = "включён" if enabled else "выключен"
        return "\n".join(
            [
                "🛠 <b>Режим отладки</b>",
                "",
                f"{BULLET} Статус: {status}",
                "Используйте /debug on|off для переключения.",
            ]
        )

    @staticmethod
    def debug_log(level: int, msg: str, context: Dict[str, Any]) -> str:
        level_name = logging.getLevelName(level)
        context_txt = ", ".join(f"{_escape(k)}={_escape(v)}" for k, v in context.items()) if context else "—"
        lines = [
            f"🛠 <b>Debug {level_name}</b>",
            "",
            _escape(msg),
            "",
            f"{BULLET} Контекст: {context_txt}",
        ]
        return "\n".join(lines)

    @staticmethod
    def unknown_command() -> str:
        return "\n".join(
            [
                "ℹ️ <b>Неизвестная команда</b>",
                "",
                "Попробуйте: /status, /pairs, /health, /simulate, /debug",
            ]
        )

    @staticmethod
    def unauthorized(chat_id: str) -> str:
        return "\n".join(
            [
                "🚫 <b>Нет доступа</b>",
                "",
                "Добавьте chat_id в конфигурацию Telegram.",
                f"{BULLET} Обнаруженный chat_id: {_escape(chat_id)}",
            ]
        )

    @staticmethod
    def health_unavailable() -> str:
        return "\n".join(["🩺 <b>Проверка рынков</b>", "", "Сервис healthcheck недоступен."])

    @staticmethod
    def health_no_pairs() -> str:
        return "\n".join(["🩺 <b>Проверка рынков</b>", "", "Нет активных пар для проверки."])

    @staticmethod
    def heartbeat(
        pairs_count: int,
        settings: Settings,
        last_sample: Dict[str, Any],
        metrics: Dict[str, Any],
        status: Dict[str, Any],
    ) -> str:
        scenario = last_sample.get("scenario") if last_sample else {}
        direction = scenario.get("direction") if scenario else None
        net_total = scenario.get("net_total") if scenario else None
        lines = [
            "💓 <b>Heartbeat</b>",
            "",
            f"{BULLET} Пары: {pairs_count}",
            f"{BULLET} Режим: {'🧪 Dry-run' if settings.dry_run else '🟢 Live'}",
            f"{BULLET} Double-limit: {_fmt_bool(settings.double_limit_enabled)}",
            "",
            "📊 Последний спред:",
            f"{SUB_BULLET} Направление: {_escape(direction) if direction else '—'}",
            f"{SUB_BULLET} Net: {net_total:.6f}" if net_total is not None else f"{SUB_BULLET} Net: —",
            "",
            "🔄 Реконсайлер:",
            f"{SUB_BULLET} processed: {metrics.get('processed', 0)} | dup: {metrics.get('duplicates', 0)} | events: {metrics.get('poll_events', 0)}",
            "",
            "🗄 База данных:",
            f"{SUB_BULLET} last_write: {_fmt_time(status.get('last_write'))}",
        ]
        return "\n".join(lines)


class TelegramCommandRouter:
    """Parses Telegram bot commands and returns status/health/simulation data."""

    def __init__(
        self,
        settings: Settings,
        pair_controller,
        db,
        reconciler,
        spread_analyzer,
        notifier,
        healthcheck: HealthcheckService,
        account_pools: Dict[ExchangeName, list],
        clients_by_id: Dict[str, object],
        account_index: Dict[str, object],
        logger: BotLogger | None = None,
        event_review_handler=None,
    ):
        self.settings = settings
        self.pair_controller = pair_controller
        self.db = db
        self.reconciler = reconciler
        self.spread_analyzer = spread_analyzer
        self.notifier = notifier
        self.healthcheck = healthcheck
        self.account_pools = account_pools
        self.clients_by_id = clients_by_id
        self.account_index = account_index
        self.logger = logger or BotLogger(__name__)
        self.event_review_handler = event_review_handler
        self.debug_enabled = False
        self._debug_interval = 30.0
        self._last_debug_forward = 0.0
        self.orderbook_manager = OrderbookManager()
        self.logger.bind_sink(self._debug_sink, min_interval=self._debug_interval)

    async def handle_update(self, update: Dict[str, Any]) -> None:
        callback = update.get("callback_query")
        if callback:
            await self._handle_callback(callback)
            return

        message = update.get("message") or update.get("edited_message")
        if not message:
            return
        chat = message.get("chat") or {}
        chat_id = str(chat.get("id"))
        text = (message.get("text") or "").strip()
        if not text:
            return

        if not self._allowed_chat(chat_id, text):
            await self.notifier.send_message(MessageBuilder.unauthorized(chat_id), chat_id=chat_id)
            return

        if text.startswith("/start"):
            await self._handle_start(chat_id)
        elif text.startswith("/status"):
            await self._handle_status(chat_id)
        elif text.startswith("/pairs"):
            await self._handle_pairs(chat_id)
        elif text.startswith("/health"):
            await self._handle_health(chat_id)
        elif text.startswith("/simulate"):
            await self._handle_simulate(chat_id, text)
        elif text.startswith("/debug"):
            await self._handle_debug(chat_id, text)
        elif text.startswith("/events"):
            await self._handle_events(chat_id)
        else:
            await self.notifier.send_message(MessageBuilder.unknown_command(), chat_id=chat_id)

    def _allowed_chat(self, chat_id: str, text: str) -> bool:
        configured = str(self.settings.telegram.chat_id) if self.settings.telegram.chat_id else None
        if configured:
            return configured == chat_id
        # allow /start for discovery
        return text.startswith("/start")

    async def _handle_callback(self, callback: Dict[str, Any]) -> None:
        message = callback.get("message") or {}
        chat = message.get("chat") or {}
        chat_id = str(chat.get("id"))
        data = callback.get("data") or ""
        if not self._allowed_chat(chat_id, data):
            await self.notifier.send_message(MessageBuilder.unauthorized(chat_id), chat_id=chat_id)
            return
        if self.event_review_handler:
            await self.event_review_handler.handle_callback(chat_id, data)

    async def _handle_start(self, chat_id: str) -> None:
        pairs = await self._active_pairs()
        msg = MessageBuilder.startup(
            chat_id=chat_id,
            pairs_count=len(pairs),
            dry_run=self.settings.dry_run,
            double_limit=self.settings.double_limit_enabled,
        )
        await self.notifier.send_message(msg, chat_id=chat_id)

    async def _handle_status(self, chat_id: str) -> None:
        msg = await self._build_status_summary()
        await self.notifier.send_message(msg, chat_id=chat_id)

    async def _handle_pairs(self, chat_id: str) -> None:
        pairs = await self._active_pairs()
        msg = MessageBuilder.pairs(pairs, self.settings)
        await self.notifier.send_message(msg, chat_id=chat_id)

    async def _handle_health(self, chat_id: str) -> None:
        pairs = await self._active_pairs()
        if not pairs:
            await self.notifier.send_message(MessageBuilder.health_no_pairs(), chat_id=chat_id)
            return
        if not self.healthcheck:
            await self.notifier.send_message(MessageBuilder.health_unavailable(), chat_id=chat_id)
            return
        results = await self.healthcheck.run(pairs, size=1.0)
        table = self._format_health_table(results)
        await self.notifier.send_message(table, chat_id=chat_id)

    async def _handle_simulate(self, chat_id: str, text: str) -> None:
        parts = text.split()
        if len(parts) < 2:
            await self.notifier.send_message(MessageBuilder.simulate_usage(), chat_id=chat_id)
            return
        pair_id = parts[1]
        size_override = None
        if len(parts) >= 3:
            try:
                size_override = float(parts[2])
            except ValueError:
                size_override = None
        pair = await self._find_pair(pair_id)
        if not pair:
            await self.notifier.send_message(MessageBuilder.simulate_pair_not_found(pair_id), chat_id=chat_id)
            return
        size = (
            size_override
            or pair.max_position_size_per_market
            or self.settings.market_hedge_mode.max_position_size_per_market
            or 1.0
        )
        primary_exchange = pair.primary_exchange or self.settings.exchanges.primary
        secondary_exchange = pair.secondary_exchange or self.settings.exchanges.secondary
        try:
            primary_client = self._resolve_client(primary_exchange, pair.primary_account_id)
            secondary_client = self._resolve_client(secondary_exchange, pair.secondary_account_id)
            primary_ob = await primary_client.get_orderbook(pair.primary_market_id)
            secondary_ob = await secondary_client.get_orderbook(pair.secondary_market_id)
        except Exception as exc:
            await self.notifier.send_message(MessageBuilder.simulate_orderbook_error(exc), chat_id=chat_id)
            return

        scenario = await self.spread_analyzer.evaluate_opportunity(
            primary_exchange=primary_exchange,
            secondary_exchange=secondary_exchange,
            primary_book=primary_ob,
            secondary_book=secondary_ob,
            primary_fees=self.settings.fees.get(primary_exchange),
            secondary_fees=self.settings.fees.get(secondary_exchange),
            size=size,
            forced_direction=pair.strategy_direction,
        )
        if not scenario:
            await self.notifier.send_message(MessageBuilder.simulate_no_opportunity(size), chat_id=chat_id)
            return

        primary_leg = scenario["legs"].get(primary_exchange)
        secondary_leg = scenario["legs"].get(secondary_exchange)
        slippage_a = self.orderbook_manager.estimate_slippage(primary_ob, primary_leg["side"], size)
        slippage_b = self.orderbook_manager.estimate_slippage(secondary_ob, secondary_leg["side"], size)
        plan = {
            "pair_id": pair.event_id,
            "size": size,
            "direction": scenario["direction"],
            "double_limit": bool(self.settings.double_limit_enabled),
            "dry_run": True,
            "legs": {
                primary_exchange.value: {
                    "side": primary_leg["side"].value,
                    "price": primary_leg["price"],
                    "slippage": slippage_a[1],
                },
                secondary_exchange.value: {
                    "side": secondary_leg["side"].value,
                    "price": secondary_leg["price"],
                    "slippage": slippage_b[1],
                },
            },
            "expected_net_total": scenario.get("net_total"),
        }
        record_id = await self.db.record_simulated_run(
            pair_id=pair.event_id,
            size=size,
            plan=plan,
            expected_pnl=scenario.get("net_total"),
            notes="telegram_simulation",
        )
        summary = MessageBuilder.simulate_plan(
            pair_id=pair.event_id,
            size=size,
            direction=scenario["direction"],
            primary_exchange=primary_exchange,
            primary_leg=primary_leg,
            primary_slippage=slippage_a[1],
            secondary_exchange=secondary_exchange,
            secondary_leg=secondary_leg,
            secondary_slippage=slippage_b[1],
            net_total=scenario.get("net_total"),
            record_id=record_id,
            double_limit=bool(self.settings.double_limit_enabled),
        )
        await self.notifier.send_message(summary, chat_id=chat_id)

    async def _handle_debug(self, chat_id: str, text: str) -> None:
        toggle = text.split()
        if len(toggle) < 2 or toggle[1] not in {"on", "off"}:
            await self.notifier.send_message(MessageBuilder.debug_status(self.debug_enabled), chat_id=chat_id)
            return
        self.debug_enabled = toggle[1] == "on"
        level = logging.DEBUG if self.debug_enabled else logging.INFO
        self.logger.set_level(level)
        await self.notifier.send_message(MessageBuilder.debug_status(self.debug_enabled), chat_id=chat_id)

    async def _handle_events(self, chat_id: str) -> None:
        if not self.event_review_handler:
            await self.notifier.send_message("ℹ️ Сервис обзора событий недоступен.", chat_id=chat_id)
            return
        await self.event_review_handler.send_pending_events(chat_id)

    def _debug_sink(self, level: int, msg: str, context: Dict[str, Any]) -> None:
        if not self.debug_enabled:
            return
        now = time.monotonic()
        if now - self._last_debug_forward < self._debug_interval:
            return
        self._last_debug_forward = now
        text = MessageBuilder.debug_log(level, msg, context)
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.notifier.send_message(text))
        except RuntimeError:
            # best-effort; ignore if loop missing (e.g., during shutdown)
            pass

    async def build_heartbeat(self) -> str:
        pairs = await self._active_pairs()
        last_sample = self.spread_analyzer.last_sample or {}
        metrics = self.reconciler.metrics if self.reconciler else {}
        status = self.db.status_snapshot() if hasattr(self.db, "status_snapshot") else {}
        return MessageBuilder.heartbeat(len(pairs), self.settings, last_sample, metrics, status)

    async def _build_status_summary(self) -> str:
        snapshot = await self.pair_controller.snapshot()
        orderbook_times = self._orderbook_timestamps()
        metrics = self.reconciler.metrics if self.reconciler else {}
        status = self.db.status_snapshot() if hasattr(self.db, "status_snapshot") else {}
        poll_intervals = {name.value: cfg.poll_interval for name, cfg in self.settings.connectivity.items()}
        account_counts = {ex.value: len(pool) for ex, pool in self.account_pools.items()}
        return MessageBuilder.status(snapshot, self.settings, orderbook_times, metrics, status, poll_intervals, account_counts)

    async def _active_pairs(self) -> List[MarketPairConfig]:
        pairs = await self.pair_controller.list_pairs()
        if pairs:
            return pairs
        return self.settings.market_pairs

    async def _find_pair(self, pair_id: str) -> Optional[MarketPairConfig]:
        pairs = await self._active_pairs()
        for pair in pairs:
            if pair.event_id == pair_id or pair.pair_id == pair_id:
                return pair
        return None

    def _orderbook_timestamps(self) -> Dict[str, str | None]:
        latest: Dict[str, datetime] = {}
        for account_id, client in self.clients_by_id.items():
            account = self.account_index.get(account_id)
            if not account:
                continue
            ts = getattr(client, "last_orderbook_at", None)
            if ts is None:
                continue
            current = latest.get(account.exchange.value)
            if not current or ts > current:
                latest[account.exchange.value] = ts
        return {k: v.isoformat() if v else None for k, v in latest.items()}

    def _resolve_client(self, exchange: ExchangeName, preferred_id: Optional[str]):
        if preferred_id and preferred_id in self.clients_by_id:
            return self.clients_by_id[preferred_id]
        pool = self.account_pools.get(exchange) or []
        if not pool:
            raise RuntimeError(f"no accounts available for {exchange.value}")
        client = self.clients_by_id.get(pool[0].account_id)
        if not client:
            raise RuntimeError(f"no client bound for account {pool[0].account_id}")
        return client

    def _format_health_table(self, results: List[HealthcheckResult]) -> str:
        return MessageBuilder.health(results)


class TelegramBotRunner:
    """Lightweight polling loop to route Telegram commands."""

    def __init__(
        self,
        notifier,
        router: TelegramCommandRouter,
        stop_event: asyncio.Event,
        logger: BotLogger | None = None,
        poll_interval: int = 2,
    ):
        self.notifier = notifier
        self.router = router
        self.stop_event = stop_event
        self.logger = logger or BotLogger(__name__)
        self.poll_interval = poll_interval
        self._task: Optional[asyncio.Task] = None
        self._offset: Optional[int] = None

    async def start(self) -> None:
        if not self.notifier.enabled:
            self.logger.warn("telegram notifier disabled; command runner not started")
            return
        await self.notifier.set_commands(TELEGRAM_COMMANDS)
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task

    async def _loop(self) -> None:
        while not self.stop_event.is_set():
            updates = await self.notifier.fetch_updates(offset=self._offset, timeout=25)
            for update in updates:
                self._offset = max(self._offset or 0, int(update.get("update_id", 0)) + 1)
                try:
                    await self.router.handle_update(update)
                except Exception as exc:  # pragma: no cover - defensive catch
                    self.logger.warn("telegram command failed", error=str(exc))
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=self.poll_interval)
            except asyncio.TimeoutError:
                continue

