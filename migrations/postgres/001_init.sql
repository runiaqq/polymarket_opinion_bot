CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS markets (
    id SERIAL PRIMARY KEY,
    market_id TEXT NOT NULL,
    exchange TEXT NOT NULL,
    symbol TEXT NOT NULL,
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    client_order_id TEXT NOT NULL,
    exchange TEXT NOT NULL,
    order_id TEXT,
    market_id TEXT NOT NULL,
    side TEXT NOT NULL,
    price NUMERIC,
    size NUMERIC NOT NULL,
    filled_size NUMERIC NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    raw JSONB
);

CREATE TABLE IF NOT EXISTS double_limits (
    id TEXT PRIMARY KEY,
    pair_key TEXT NOT NULL,
    order_a_ref TEXT NOT NULL,
    order_b_ref TEXT NOT NULL,
    order_a_exchange TEXT NOT NULL,
    order_b_exchange TEXT NOT NULL,
    client_order_id_a TEXT NOT NULL,
    client_order_id_b TEXT NOT NULL,
    state TEXT NOT NULL,
    triggered_order_id TEXT,
    cancelled_order_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_double_limits_order_a ON double_limits(order_a_ref);
CREATE UNIQUE INDEX IF NOT EXISTS idx_double_limits_order_b ON double_limits(order_b_ref);

CREATE TABLE IF NOT EXISTS fills (
    id SERIAL PRIMARY KEY,
    order_id TEXT NOT NULL,
    exchange TEXT NOT NULL,
    fill_id TEXT,
    size NUMERIC NOT NULL,
    price NUMERIC NOT NULL,
    side TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    raw JSONB
);

CREATE TABLE IF NOT EXISTS trades (
    id SERIAL PRIMARY KEY,
    entry_order_id TEXT NOT NULL,
    hedge_order_id TEXT NOT NULL,
    entry_exchange TEXT NOT NULL,
    hedge_exchange TEXT NOT NULL,
    size NUMERIC NOT NULL,
    price_entry NUMERIC NOT NULL,
    price_hedge NUMERIC NOT NULL,
    fees NUMERIC NOT NULL,
    pnl_estimated NUMERIC NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    raw JSONB
);

CREATE TABLE IF NOT EXISTS incidents (
    id SERIAL PRIMARY KEY,
    level TEXT NOT NULL,
    message TEXT NOT NULL,
    details JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS order_events (
    id SERIAL PRIMARY KEY,
    order_id TEXT NOT NULL,
    stage TEXT NOT NULL,
    payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_order_events_order_id ON order_events(order_id);

