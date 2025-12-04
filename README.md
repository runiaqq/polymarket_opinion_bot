# Market Hedge Trading Engine

Async market-hedging engine that arbitrages prediction markets between [Polymarket](https://docs.polymarket.com/developers/gamma-markets-api/overview) and [Opinion](https://docs.opinion.trade/developer-guide/api-references/models). The bot routes primary limit orders to one venue, listens for fills via websocket, and immediately hedges filled size on the opposing venue using market/IOC orders while enforcing configurable risk and slippage controls.

## Features

- **Async REST + WebSocket clients** with retry/backoff, per-account rate limiting, and proxy-aware sessions (Opinion supports realtime websockets; Polymarket currently relies on REST polling).
- **Market hedging workflow**: spread analysis, primary order placement, fill tracking, automated hedging with slippage-aware sizing, and optional dry-run mode.
- **Multi-account support**: independent API credentials & proxies per account; configurable market pairs map primary/secondary accounts per event.
- **Risk & compliance**: balance, exposure, and slippage checks plus incident logging.
- **Persistence**: async database layer supporting SQLite (default) and PostgreSQL (asyncpg) storing orders, trades, positions, and incidents.
- **Observability**: structured logger + Telegram notifier for fills, hedges, and incidents.
- **Tests**: pytest suite covering spread logic, orderbook math, hedger behavior, order manager routing, and API client integration points.

## Requirements

- Python 3.10+
- Access to the official exchange APIs and websockets (see doc links above)
- For PostgreSQL mode: running instance reachable by the bot

## Installation

```bash
python -m venv .venv
. .venv/Scripts/activate  # Windows
pip install -r requirements.txt
```

## Configuration

### `config/accounts.json`

Provide every trading account with its exchange, API keys, and proxy:

```json
{
  "accounts": [
    {
      "account_id": "opinion_acc",
      "exchange": "Opinion",
      "api_key": "OPINION_API_KEY",
      "secret_key": "OPINION_SECRET",
      "proxy": "http://user:pass@host:port"
    },
    {
      "account_id": "poly_acc",
      "exchange": "Polymarket",
      "api_key": "POLYMARKET_API_KEY",
      "secret_key": "POLYMARKET_SECRET",
      "proxy": "http://user:pass@host:port"
    }
  ]
}
```

### `config/settings.yaml`

- `market_hedge_mode`: hedge ratio, slippage caps, spread threshold, exposure limits, cancel timers, etc.
- `exchanges.primary/secondary`: choose which venue receives limit legs vs hedge legs.
- `market_pairs`: map shared event IDs to per-exchange market identifiers and (optionally) specific account IDs to use for that pair.
- `database`: `backend` (`sqlite` or `postgres`) and DSN (`sqlite+aiosqlite:///path.db` or postgres URL).
- `telegram`: enable + bot token/chat ID for notifications.
- `rate_limits`: per-exchange request ceilings.
- `connectivity`: per-exchange flags to enable websockets (`use_websocket: true`) or fall back to REST polling with `poll_interval` in seconds. By default Polymarket is polled while Opinion uses websockets.
- `dry_run`: keep logic running without sending live orders.

### API Docs

Implementation follows the public specs:

- Polymarket Gamma & CLOB: <https://docs.polymarket.com/developers/gamma-markets-api/overview>
- Opinion CLOB SDK/API: <https://docs.opinion.trade/developer-guide/api-references/models> and <https://docs.opinion.trade/developer-guide/api-references/methods>

## Usage

```bash
python main.py
```

The engine will:

1. Load settings + accounts.
2. Spin up per-account aiohttp sessions with proxies and rate limiters.
3. For each exchange, either start websocket listeners (if `use_websocket: true`) or launch REST polling loops for fills (used by Polymarket due to missing public WS feed).
4. For each configured `market_pair`, continuously evaluate spreads and place limit orders on the primary venue whenever spread ≥ `min_spread_for_entry`. Every fill triggers instantaneous hedging on the secondary venue while respecting `hedge_ratio` and slippage limits.
5. Persist all orders/trades/positions and push Telegram notifications for hedge events or incidents.

Stop the bot with `CTRL+C`. The shutdown hook drains tasks, closes websockets, sessions, and DB connections cleanly.

## Testing

```bash
make test          # runs pytest -q (fast suite, skips stress)
make test-stress   # runs pytest -q -m stress
```

Tests rely purely on mocks/fixtures (no external API calls) and cover:

- Spread math & profitability gating
- Orderbook depth/slippage calculations
- Hedger (single-leg + multi-leg) trade persistence, slippage, and failure handling
- OrderManager fill routing and hedge triggering
- API client payload normalization for both venues
- Reconciliation layer (websocket + poll dedupe)
- Webhook + Google Sheets sync endpoints
- Stress harness with 100+ mock accounts (marked `@pytest.mark.stress`)

### End-to-End Harness

The `tests/e2e` suite spins up mocked Opinion/Polymarket endpoints using `pytest-asyncio` + `aioresponses` and validates:

1. Market mapping + order placement
2. Partial fills emitted via websocket or polling
3. Hedger invocation and trade persistence

Run locally with:

```bash
pytest tests/e2e -q
```

By default it uses SQLite and `dry_run=true`; set `E2E_USE_REAL_DB=1` to point to a live Postgres DSN.

## Telemetry

`utils/telemetry.py` provides lightweight counters (`hedge_attempts`, `hedge_success`, `hedge_failures`) and a slippage histogram.

- Enable Prometheus export by setting `ENABLE_PROMETHEUS=1` (listens on port 9000 by default).
- Without Prometheus, telemetry logs snapshots every 60 seconds.

Integrations can push custom metrics by injecting the `Telemetry` instance into hedger/order manager components.

## Extending

- Add more `market_pairs` to scale coverage.
- Implement strategy modules that generate limit orders beyond the built-in spread trigger.
- Wire additional persistence sinks (e.g., Kafka) by extending `Database`.
- Introduce structured JSON logging or plug into observability stacks.

## Safety Notes

- Keep `dry_run: true` until sandbox credentials and configs are verified.
- Double-check `market_pairs` to ensure market IDs align between exchanges.
- Review per-exchange rate limits and API docs before increasing order throughput.
- **Polymarket latency warning**: when `use_websocket: false`, fills are detected via periodic REST polling. Hedging actions can lag during network congestion; size your risk limits accordingly.


