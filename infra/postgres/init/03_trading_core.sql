-- Orders table: strategy emits NEW; risk sets APPROVED + qty; exec sets PARTIAL/FILLED/REJECTED.
CREATE TABLE IF NOT EXISTS orders(
  order_id         bigserial PRIMARY KEY,
  client_order_id  text UNIQUE NOT NULL,               -- idempotency key: strat:SYM:ts
  ts               timestamptz NOT NULL DEFAULT now(), -- event time (seconds precision ok)
  symbol           text        NOT NULL,
  side             text        NOT NULL CHECK (side IN ('BUY','SELL','EXIT')),
  qty              integer     NOT NULL DEFAULT 0 CHECK (qty >= 0),
  order_type       text        NOT NULL DEFAULT 'MKT',
  strategy         text        NOT NULL,
  reason           text        NULL,
  risk_bucket      text        NOT NULL DEFAULT 'LOW',
  status           text        NOT NULL DEFAULT 'NEW',
  extra            jsonb       NULL                     -- carries signal/risk context
);

CREATE INDEX IF NOT EXISTS orders_sym_ts_idx ON orders(symbol, ts DESC);
CREATE INDEX IF NOT EXISTS orders_status_idx ON orders(status);

-- Fills: multiple rows may map to one order (partial fills).
CREATE TABLE IF NOT EXISTS fills(
  fill_id   bigserial PRIMARY KEY,
  order_id  bigint REFERENCES orders(order_id) ON DELETE CASCADE,
  ts        timestamptz NOT NULL DEFAULT now(),
  qty       integer     NOT NULL CHECK (qty > 0),
  price     double precision NOT NULL CHECK (price > 0),
  venue     text        NOT NULL,      -- MATCHER / BROKER
  extra     jsonb       NULL           -- fees breakdown etc.
);
CREATE INDEX IF NOT EXISTS fills_order_idx ON fills(order_id);

-- Positions: net position per symbol (paper), with realized PnL; simplistic FIFO avg-price model.
CREATE TABLE IF NOT EXISTS positions(
  symbol        text PRIMARY KEY,
  qty           integer NOT NULL DEFAULT 0,                 -- signed, + long / - short
  avg_price     double precision NOT NULL DEFAULT 0,
  realized_pnl  double precision NOT NULL DEFAULT 0,
  updated_at    timestamptz NOT NULL DEFAULT now()
);

-- Blotter view: join orders/fills with per-fill fees & per-order aggregates (read-only dashboard).
CREATE OR REPLACE VIEW blotter AS
SELECT
  o.order_id, o.client_order_id, o.ts AS order_ts, o.symbol, o.side, o.qty AS order_qty,
  o.strategy, o.status, o.risk_bucket,
  f.ts AS fill_ts, f.qty AS fill_qty, f.price AS fill_price,
  (f.extra->'fees'->>'total')::double precision AS fees_total,
  f.venue
FROM orders o
LEFT JOIN fills f ON f.order_id = o.order_id
ORDER BY o.ts DESC, f.ts NULLS LAST;
