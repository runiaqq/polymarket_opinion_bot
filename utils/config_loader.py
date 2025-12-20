from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import yaml

from core.models import AccountCredentials, ContractType, ExchangeName, StrategyDirection


@dataclass(slots=True)
class MarketHedgeConfig:
    enabled: bool
    hedge_ratio: float
    max_slippage_market_hedge: float
    min_spread_for_entry: float
    max_position_size_per_market: float
    max_position_size_per_event: float
    cancel_unfilled_after_ms: int
    allow_partial_fill_hedge: bool
    hedge_strategy: str = "FULL"
    max_slippage_percent: float = 0.05
    min_quote_size: float = 0.0
    exposure_tolerance: float = 0.0
    ultra_safe: bool = False


@dataclass(slots=True)
class ExchangeRoutingConfig:
    primary: ExchangeName
    secondary: ExchangeName


@dataclass(slots=True)
class FeeConfig:
    maker: float = 0.0
    taker: float = 0.0


@dataclass(slots=True)
class DatabaseConfig:
    backend: str
    dsn: str


@dataclass(slots=True)
class TelegramConfig:
    enabled: bool
    token: Optional[str]
    chat_id: Optional[str]


@dataclass(slots=True)
class RateLimitConfig:
    requests_per_minute: int
    burst: int


@dataclass(slots=True)
class ExchangeConnectivity:
    use_websocket: bool
    poll_interval: float


@dataclass(slots=True)
class GoogleSheetsConfig:
    enabled: bool = False
    sheet_id: str | None = None
    range: str = "Sheet1!A1:F100"
    poll_interval_sec: int = 60
    credentials_path: str | None = None
    mode: str = "service_account"
    api_key: str | None = None


@dataclass(slots=True)
class WebhookConfig:
    enabled: bool = False
    host: str = "0.0.0.0"
    port: int = 8081
    admin_token: str = ""


@dataclass(slots=True)
class MarketPairConfig:
    event_id: str
    primary_market_id: str
    secondary_market_id: str
    primary_account_id: str | None = None
    secondary_account_id: str | None = None
    pair_id: str | None = None
    strategy: str | None = None
    max_position_size_per_market: float | None = None
    primary_exchange: ExchangeName | None = None
    secondary_exchange: ExchangeName | None = None
    contract_type: ContractType = ContractType.BINARY
    strategy_direction: StrategyDirection = StrategyDirection.AUTO


@dataclass(slots=True)
class Settings:
    market_hedge_mode: MarketHedgeConfig
    double_limit_enabled: bool
    exchanges: ExchangeRoutingConfig
    fees: Dict[ExchangeName, FeeConfig]
    google_sheets: GoogleSheetsConfig
    webhook: WebhookConfig
    scheduler_policy: str
    dry_run: bool
    telegram: TelegramConfig
    database: DatabaseConfig
    rate_limits: Dict[str, RateLimitConfig]
    market_pairs: List[MarketPairConfig]
    connectivity: Dict[ExchangeName, ExchangeConnectivity]


class ConfigLoader:
    """Loads configuration files for the bot."""

    def __init__(self, base_path: Path | None = None):
        self.base_path = base_path or Path(__file__).resolve().parent.parent
        self._config_dir = self.base_path / "config"

    def _resolve_config_file(self, filename: str, fallbacks: list[str] | None = None) -> Path:
        candidates = [self._config_dir / filename]
        if fallbacks:
            candidates.extend(self._config_dir / name for name in fallbacks)
        for path in candidates:
            if path.exists():
                return path
        searched = ", ".join(str(path) for path in candidates)
        raise FileNotFoundError(f"missing config file; searched: {searched}")

    def load_settings(self) -> Settings:
        settings_path = self._resolve_config_file(
            "settings.yaml",
            ["settings.local.yaml", "settings.example.yaml", "settings.template.yaml"],
        )
        with settings_path.open("r", encoding="utf-8") as handle:
            raw = yaml.safe_load(handle) or {}
        return self._parse_settings(raw)

    def load_accounts(self) -> List[AccountCredentials]:
        accounts_path = self._resolve_config_file(
            "accounts.json",
            ["accounts.local.json", "accounts.example.json", "accounts.template.json"],
        )
        data = json.loads(accounts_path.read_text(encoding="utf-8"))
        accounts: List[AccountCredentials] = []
        for entry in data.get("accounts", []):
            exchange = ExchangeName(entry["exchange"])
            accounts.append(
                AccountCredentials(
                    account_id=entry["account_id"],
                    exchange=exchange,
                    api_key=entry["api_key"],
                    secret_key=entry.get("secret_key", ""),
                    passphrase=entry.get("passphrase"),
                    wallet_address=entry.get("wallet_address"),
                    proxy=entry.get("proxy"),
                    metadata=entry.get("metadata", {}),
                    weight=float(entry.get("weight", 1.0)),
                    tokens_per_sec=float(entry.get("tokens_per_sec", 5.0)),
                    burst=int(entry.get("burst", 10)),
                )
            )
        return accounts

    def _parse_settings(self, raw: Dict[str, object]) -> Settings:
        market_cfg = raw.get("market_hedge_mode", {})
        exchanges_cfg = raw.get("exchanges", {})
        telegram_cfg = raw.get("telegram", {})
        db_cfg = raw.get("database", {})
        rate_cfg = raw.get("rate_limits", {})
        connectivity_cfg = raw.get("connectivity", {})

        market = MarketHedgeConfig(
            enabled=bool(market_cfg.get("enabled", True)),
            hedge_ratio=float(market_cfg.get("hedge_ratio", 1.0)),
            max_slippage_market_hedge=float(
                market_cfg.get("max_slippage_market_hedge", 0.005)
            ),
            min_spread_for_entry=float(market_cfg.get("min_spread_for_entry", 0.0)),
            max_position_size_per_market=float(
                market_cfg.get("max_position_size_per_market", 0.0)
            ),
            max_position_size_per_event=float(
                market_cfg.get("max_position_size_per_event", 0.0)
            ),
            cancel_unfilled_after_ms=int(
                market_cfg.get("cancel_unfilled_after_ms", 60000)
            ),
            allow_partial_fill_hedge=bool(
                market_cfg.get("allow_partial_fill_hedge", True)
            ),
            hedge_strategy=str(market_cfg.get("hedge_strategy", "FULL")).upper(),
            max_slippage_percent=float(market_cfg.get("max_slippage_percent", 0.05)),
            min_quote_size=float(market_cfg.get("min_quote_size", 0.0)),
            exposure_tolerance=float(market_cfg.get("exposure_tolerance", 0.0)),
            ultra_safe=bool(market_cfg.get("ultra_safe", False)),
        )

        exchanges = ExchangeRoutingConfig(
            primary=ExchangeName(exchanges_cfg.get("primary", "Opinion")),
            secondary=ExchangeName(exchanges_cfg.get("secondary", "Polymarket")),
        )

        telegram = TelegramConfig(
            enabled=bool(telegram_cfg.get("enabled", False)),
            token=telegram_cfg.get("token"),
            chat_id=telegram_cfg.get("chat_id"),
        )

        database = DatabaseConfig(
            backend=db_cfg.get("backend", "sqlite"),
            dsn=db_cfg.get("dsn", "sqlite+aiosqlite:///market_hedge.db"),
        )

        rate_limits: Dict[str, RateLimitConfig] = {}
        for name, cfg in rate_cfg.items():
            rate_limits[name] = RateLimitConfig(
                requests_per_minute=int(cfg.get("requests_per_minute", 60)),
                burst=int(cfg.get("burst", 5)),
            )

        pairs = []
        for item in raw.get("market_pairs", []):
            if {"event_id", "primary_market_id", "secondary_market_id"} - item.keys():
                continue
            primary_exchange = item.get("primary_exchange")
            secondary_exchange = item.get("secondary_exchange")
            try:
                primary_exchange_enum = ExchangeName(primary_exchange) if primary_exchange else None
            except ValueError:
                primary_exchange_enum = None
            try:
                secondary_exchange_enum = ExchangeName(secondary_exchange) if secondary_exchange else None
            except ValueError:
                secondary_exchange_enum = None
            pairs.append(
                MarketPairConfig(
                    event_id=item["event_id"],
                    primary_market_id=item["primary_market_id"],
                    secondary_market_id=item["secondary_market_id"],
                    primary_account_id=item.get("primary_account_id"),
                    secondary_account_id=item.get("secondary_account_id"),
                    primary_exchange=primary_exchange_enum,
                    secondary_exchange=secondary_exchange_enum,
                    contract_type=ContractType(
                        str(item.get("contract_type", ContractType.BINARY.value)).upper()
                    )
                    if item.get("contract_type")
                    else ContractType.BINARY,
                    strategy_direction=StrategyDirection(
                        str(item.get("strategy_direction", StrategyDirection.AUTO.value)).upper()
                    )
                    if item.get("strategy_direction")
                    else StrategyDirection.AUTO,
                )
            )

        connectivity: Dict[ExchangeName, ExchangeConnectivity] = {}
        for name, cfg in connectivity_cfg.items():
            try:
                exchange_name = ExchangeName(name)
            except ValueError:
                continue
            connectivity[exchange_name] = ExchangeConnectivity(
                use_websocket=bool(cfg.get("use_websocket", True)),
                poll_interval=float(cfg.get("poll_interval", 5.0)),
            )

        fees: Dict[ExchangeName, FeeConfig] = {}
        for name, cfg in raw.get("fees", {}).items():
            try:
                exchange_name = ExchangeName(name)
            except ValueError:
                continue
            fees[exchange_name] = FeeConfig(
                maker=float(cfg.get("maker", 0.0)),
                taker=float(cfg.get("taker", 0.0)),
            )

        sheets_cfg = raw.get("google_sheets", {})
        google_sheets = GoogleSheetsConfig(
            enabled=bool(sheets_cfg.get("enabled", False)),
            sheet_id=sheets_cfg.get("sheet_id"),
            range=str(sheets_cfg.get("range", "Sheet1!A1:F100")),
            poll_interval_sec=int(sheets_cfg.get("poll_interval_sec", 60)),
            credentials_path=sheets_cfg.get("credentials_path"),
            mode=str(sheets_cfg.get("mode", "service_account")),
            api_key=sheets_cfg.get("api_key"),
        )

        webhook_cfg = raw.get("webhook", {})
        webhook = WebhookConfig(
            enabled=bool(webhook_cfg.get("enabled", False)),
            host=str(webhook_cfg.get("host", "0.0.0.0")),
            port=int(webhook_cfg.get("port", 8081)),
            admin_token=str(webhook_cfg.get("admin_token", "")),
        )

        return Settings(
            market_hedge_mode=market,
            double_limit_enabled=bool(raw.get("double_limit_enabled", True)),
            exchanges=exchanges,
            fees=fees,
            google_sheets=google_sheets,
            webhook=webhook,
            dry_run=bool(raw.get("dry_run", True)),
            telegram=telegram,
            database=database,
            rate_limits=rate_limits,
            market_pairs=pairs,
            connectivity=connectivity,
            scheduler_policy=str(raw.get("scheduler", {}).get("policy", "round_robin")).lower(),
        )

