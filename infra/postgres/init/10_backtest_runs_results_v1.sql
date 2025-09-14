-- Backtest V1 (renamed tables to *_v1 to avoid collisions with V2)

CREATE TABLE IF NOT EXISTS backtest_runs_v1 (
  run_id      bigserial PRIMARY KEY,
  label       text,
  started_at  timestamptz NOT NULL DEFAULT now(),
  start_date  date NOT NULL,
  end_date    date NOT NULL,
  tfs         text NOT NULL,
  strategies  jsonb NOT NULL,
  costs       jsonb NOT NULL
);

CREATE TABLE IF NOT EXISTS backtest_results_v1 (
  run_id     bigint REFERENCES backtest_runs_v1(run_id) ON DELETE CASCADE,
  symbol     text NOT NULL,
  strategy   text NOT NULL,
  tf_min     int  NOT NULL,
  params     jsonb NOT NULL,
  gross_pnl  double precision NOT NULL,
  net_pnl    double precision NOT NULL,
  n_trades   int NOT NULL,
  win_rate   double precision,
  max_dd     double precision,
  sharpe     double precision,
  PRIMARY KEY (run_id, symbol, strategy, tf_min, params)
);

CREATE INDEX IF NOT EXISTS bt_results_v1_rank_idx
  ON backtest_results_v1(run_id, net_pnl DESC);
