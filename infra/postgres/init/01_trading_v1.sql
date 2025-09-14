-- Trading Platform Database Schema (v1)
-- NOTE: View renamed to blotter_agg to avoid clashing with v2's detailed blotter.

CREATE TABLE IF NOT EXISTS orders (
  order_id         bigserial PRIMARY KEY,
  ts               timestamptz NOT NULL DEFAULT now(),
  client_order_id  text UNIQUE,
  symbol           text        NOT NULL,
  side             text        NOT NULL CHECK (side IN ('BUY','SELL')),
  qty              integer     NOT NULL CHECK (qty > 0),
  order_type       text        NOT NULL DEFAULT 'MKT',
  limit_price      double precision,
  strategy         text        NOT NULL,
  reason           text,
  risk_bucket      text,
  status           text        NOT NULL DEFAULT 'NEW',
  extra            jsonb       NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS orders_ts_idx      ON orders(ts);
CREATE INDEX IF NOT EXISTS orders_sym_ts_idx  ON orders(symbol, ts);

CREATE TABLE IF NOT EXISTS fills (
  fill_id    bigserial PRIMARY KEY,
  order_id   bigint       NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
  ts         timestamptz  NOT NULL DEFAULT now(),
  qty        integer      NOT NULL CHECK (qty > 0),
  price      double precision NOT NULL,
  venue      text,
  extra      jsonb        NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS fills_order_idx ON fills(order_id);
CREATE INDEX IF NOT EXISTS fills_ts_idx    ON fills(ts);

CREATE TABLE IF NOT EXISTS positions (
  symbol        text PRIMARY KEY,
  qty           integer           NOT NULL DEFAULT 0,
  avg_price     double precision  NOT NULL DEFAULT 0,
  realized_pnl  double precision  NOT NULL DEFAULT 0,
  updated_at    timestamptz       NOT NULL DEFAULT now()
);

-- Renamed from 'blotter' -> 'blotter_agg' so that v2's detailed blotter can own the 'blotter' name
CREATE OR REPLACE VIEW blotter_agg AS
SELECT
  o.order_id, o.ts AS order_ts, o.client_order_id, o.symbol, o.side, o.qty AS ord_qty,
  o.order_type, o.limit_price, o.strategy, o.reason, o.risk_bucket, o.status,
  COALESCE(SUM(f.qty), 0)           AS fill_qty,
  CASE WHEN COALESCE(SUM(f.qty),0) > 0
       THEN SUM(f.qty * f.price)::double precision / SUM(f.qty)
       ELSE NULL
  END                               AS avg_fill_price,
  MAX(f.ts)                         AS last_fill_ts
FROM orders o
LEFT JOIN fills f ON f.order_id = o.order_id
GROUP BY o.order_id;
