CREATE TABLE IF NOT EXISTS bars_1m_golden (
  symbol     text NOT NULL,
  ts         timestamptz NOT NULL,
  o          double precision NOT NULL,
  h          double precision NOT NULL,
  l          double precision NOT NULL,
  c          double precision NOT NULL,
  vol        double precision NOT NULL,
  source     text NOT NULL DEFAULT 'Zerodha',
  loaded_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (symbol, ts)
);

CREATE INDEX IF NOT EXISTS bars_1m_golden_symbol_ts_idx
ON bars_1m_golden (symbol, ts);