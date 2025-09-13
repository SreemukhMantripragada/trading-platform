-- Snapshot of broker cash/funds pulled from Zerodha (Kite)
CREATE TABLE IF NOT EXISTS broker_cash (
  id           bigserial PRIMARY KEY,
  asof_ts      timestamptz NOT NULL DEFAULT now(),
  source       text NOT NULL DEFAULT 'zerodha',
  account_id   text,              -- e.g., user_id from profile()
  equity_used  double precision,
  equity_avail double precision,  -- free cash available to trade
  mtm          double precision,
  raw          jsonb              -- full payload for audit
);

-- Latest row fast access
CREATE OR REPLACE VIEW broker_cash_latest AS
SELECT *
FROM broker_cash
ORDER BY asof_ts DESC
LIMIT 1;
