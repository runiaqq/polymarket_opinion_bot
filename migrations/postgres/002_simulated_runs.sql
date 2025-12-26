CREATE TABLE IF NOT EXISTS simulated_runs (
    id TEXT PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    pair_id TEXT NOT NULL,
    size NUMERIC NOT NULL,
    plan_json JSONB NOT NULL,
    expected_pnl NUMERIC,
    notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_sim_runs_pair ON simulated_runs(pair_id);


