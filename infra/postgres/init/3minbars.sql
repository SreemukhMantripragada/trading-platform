-- 3m/5m/15m bar tables like bars_1m
CREATE TABLE IF NOT EXISTS bars_3m(
  symbol text NOT NULL,
  ts timestamptz NOT NULL,
  o double precision NOT NULL,
  h double precision NOT NULL,
  l double precision NOT NULL,
  c double precision NOT NULL,
  vol bigint NOT NULL,
  n_trades bigint NOT NULL,
  PRIMARY KEY(symbol, ts)
);
CREATE INDEX IF NOT EXISTS bars_3m_ts_idx ON bars_3m(ts);

CREATE TABLE IF NOT EXISTS bars_5m (LIKE bars_3m INCLUDING ALL);
ALTER TABLE bars_5m ADD PRIMARY KEY (symbol, ts);
CREATE INDEX IF NOT EXISTS bars_5m_ts_idx ON bars_5m(ts);

CREATE TABLE IF NOT EXISTS bars_15m (LIKE bars_3m INCLUDING ALL);
ALTER TABLE bars_15m ADD PRIMARY KEY (symbol, ts);
CREATE INDEX IF NOT EXISTS bars_15m_ts_idx ON bars_15m(ts);
