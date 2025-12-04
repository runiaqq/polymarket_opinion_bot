from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import yaml

from core.models import AccountCredentials, ExchangeName


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


@dataclass(slots=True)
class ExchangeRoutingConfig:
    primary: ExchangeName
    secondary: ExchangeName


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
class MarketPairConfig:
    event_id: str
    primary_market_id: str
    secondary_market_id: str
    primary_account_id: str | None = None
    secondary_account_id: str | None = None


@dataclass(slots=True)
class Settings:
    market_hedge_mode: MarketHedgeConfig
    exchanges: ExchangeRoutingConfig
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

    def load_settings(self) -> Settings:
        settings_path = self.base_path / "config" / "settings.yaml"
        if not settings_path.exists():
            raise FileNotFoundError(f"missing settings file at {settings_path}")
        with settings_path.open("r", encoding="utf-8") as handle:
            raw = yaml.safe_load(handle) or {}
        return self._parse_settings(raw)

    def load_accounts(self) -> List[AccountCredentials]:
        accounts_path = self.base_path / "config" / "accounts.json"
        if not accounts_path.exists():
            raise FileNotFoundError(f"missing accounts file at {accounts_path}")
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
                    proxy=entry.get("proxy"),
                    metadata=entry.get("metadata", {}),
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
            pairs.append(
                MarketPairConfig(
                    event_id=item["event_id"],
                    primary_market_id=item["primary_market_id"],
                    secondary_market_id=item["secondary_market_id"],
                    primary_account_id=item.get("primary_account_id"),
                    secondary_account_id=item.get("secondary_account_id"),
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

        return Settings(
            market_hedge_mode=market,
            exchanges=exchanges,
            dry_run=bool(raw.get("dry_run", True)),
            telegram=telegram,
            database=database,
            rate_limits=rate_limits,
            market_pairs=pairs,
            connectivity=connectivity,
        )

