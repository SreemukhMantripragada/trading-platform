-- Positions & Cash ledger for paper/live accounting

CREATE TABLE IF NOT EXISTS positions (
  symbol       text PRIMARY KEY,
  qty          bigint NOT NULL DEFAULT 0,
  avg_price    double precision NOT NULL DEFAULT 0, -- WAC
  realized_pnl double precision NOT NULL DEFAULT 0,
  updated_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS cash_ledger (
  id           bigserial PRIMARY KEY,
  ts           timestamptz NOT NULL DEFAULT now(),
  reason       text NOT NULL,            -- e.g., FILL_BUY, FILL_SELL, FEES, ADJUST
  amount       double precision NOT NULL, -- +credit / -debit
  balance      double precision,          -- optional running balance (filled by app)
  meta         jsonb
);

-- Optional raw fill audit (if not already elsewhere)
CREATE TABLE IF NOT EXISTS fills_audit (
  id               bigserial PRIMARY KEY,
  ts               timestamptz NOT NULL,
  symbol           text NOT NULL,
  side             text NOT NULL,         -- BUY/SELL
  qty              bigint NOT NULL,
  price            double precision NOT NULL,
  fees_inr         double precision NOT NULL DEFAULT 0,
  client_order_id  text,
  strategy         text,
  risk_bucket      text,
  raw              jsonb
);

CREATE INDEX IF NOT EXISTS fills_audit_symbol_ts_idx ON fills_audit(symbol, ts);
