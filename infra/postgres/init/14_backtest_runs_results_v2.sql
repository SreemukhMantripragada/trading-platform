CREATE TABLE IF NOT EXISTS backtest_runs(
  run_id      bigserial PRIMARY KEY,
  created_at  timestamptz NOT NULL DEFAULT now(),
  params      jsonb NOT NULL
);

CREATE TABLE IF NOT EXISTS backtest_results(
  run_id      bigint REFERENCES backtest_runs(run_id) ON DELETE CASCADE,
  strategy    text NOT NULL,
  symbol      text NOT NULL,
  timeframe   text NOT NULL,
  metrics     jsonb NOT NULL,     -- {net_pnl, gross_pnl, max_dd, sharpe, hit_rate, trades}
  PRIMARY KEY (run_id, strategy, symbol, timeframe)
);

CREATE VIEW backtest_top5 AS
SELECT run_id, (metrics->>'net_pnl')::double precision AS net_pnl,
       strategy, timeframe
FROM backtest_results
ORDER BY run_id DESC, net_pnl DESC
LIMIT 5;
