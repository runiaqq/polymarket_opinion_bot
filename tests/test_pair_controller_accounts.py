import asyncio

from core.models import AccountCredentials, ExchangeName
from core.pair_controller import PairController
from utils.config_loader import (
    DatabaseConfig,
    ExchangeConnectivity,
    ExchangeRoutingConfig,
    GoogleSheetsConfig,
    MarketHedgeConfig,
    RateLimitConfig,
    Settings,
    TelegramConfig,
    WebhookConfig,
)
from utils.logger import BotLogger


def _build_settings() -> Settings:
    market_cfg = MarketHedgeConfig(
        enabled=True,
        hedge_ratio=1.0,
        max_slippage_market_hedge=0.01,
        min_spread_for_entry=0.0,
        max_position_size_per_market=100,
        max_position_size_per_event=200,
        cancel_unfilled_after_ms=60000,
        allow_partial_fill_hedge=True,
    )
    return Settings(
        market_hedge_mode=market_cfg,
        double_limit_enabled=True,
        exchanges=ExchangeRoutingConfig(
            primary=ExchangeName.OPINION,
            secondary=ExchangeName.POLYMARKET,
        ),
        fees={},
        google_sheets=GoogleSheetsConfig(),
        webhook=WebhookConfig(),
        dry_run=True,
        telegram=TelegramConfig(enabled=False, token=None, chat_id=None),
        database=DatabaseConfig(backend="sqlite", dsn="sqlite+aiosqlite:///./data/test.db"),
        rate_limits={
            "Opinion": RateLimitConfig(requests_per_minute=60, burst=5),
            "Polymarket": RateLimitConfig(requests_per_minute=60, burst=5),
        },
        market_pairs=[],
        connectivity={
            ExchangeName.OPINION: ExchangeConnectivity(use_websocket=False, poll_interval=1.0),
            ExchangeName.POLYMARKET: ExchangeConnectivity(use_websocket=False, poll_interval=1.0),
        },
        scheduler_policy="round_robin",
    )


def test_pair_controller_round_robin_accounts():
    settings = _build_settings()
    opinion_accounts = [
        AccountCredentials(
            account_id="acc-1",
            exchange=ExchangeName.OPINION,
            api_key="k1",
            secret_key="s1",
        ),
        AccountCredentials(
            account_id="acc-2",
            exchange=ExchangeName.OPINION,
            api_key="k2",
            secret_key="s2",
        ),
    ]
    poly_account = AccountCredentials(
        account_id="poly-1",
        exchange=ExchangeName.POLYMARKET,
        api_key="k3",
        secret_key="s3",
    )
    clients = {acc.account_id: object() for acc in opinion_accounts + [poly_account]}

    controller = PairController(
        settings=settings,
        db=object(),
        position_tracker=object(),
        hedger=object(),
        risk_manager=object(),
        logger=BotLogger("pair_controller_test"),
        stop_event=asyncio.Event(),
        spread_analyzer=object(),
        orderbook_manager=object(),
        mapper=None,
        notifier=None,
        account_pools={
            ExchangeName.OPINION: opinion_accounts,
            ExchangeName.POLYMARKET: [poly_account],
        },
        clients_by_id=clients,
    )

    first = controller._resolve_account(ExchangeName.OPINION, preferred_id=None)
    second = controller._resolve_account(ExchangeName.OPINION, preferred_id=None)
    assert {first.account_id, second.account_id} == {"acc-1", "acc-2"}





