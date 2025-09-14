-- Trading Core (v2): richer constraints + detailed blotter

-- Orders table
CREATE TABLE IF NOT EXISTS orders(
  order_id         bigserial PRIMARY KEY,
  client_order_id  text UNIQUE NOT NULL,
  ts               timestamptz NOT NULL DEFAULT now(),
  symbol           text        NOT NULL,
  side             text        NOT NULL CHECK (side IN ('BUY','SELL','EXIT')),
  qty              integer     NOT NULL DEFAULT 0 CHECK (qty >= 0),
  order_type       text        NOT NULL DEFAULT 'MKT',
  strategy         text        NOT NULL,
  reason           text        NULL,
  risk_bucket      text        NOT NULL DEFAULT 'LOW',
  status           text        NOT NULL DEFAULT 'NEW',
  extra            jsonb       NULL
);
CREATE INDEX IF NOT EXISTS orders_sym_ts_idx ON orders(symbol, ts DESC);
CREATE INDEX IF NOT EXISTS orders_status_idx ON orders(status);

-- Fills table
CREATE TABLE IF NOT EXISTS fills(
  fill_id   bigserial PRIMARY KEY,
  order_id  bigint REFERENCES orders(order_id) ON DELETE CASCADE,
  ts        timestamptz NOT NULL DEFAULT now(),
  qty       integer     NOT NULL CHECK (qty > 0),
  price     double precision NOT NULL CHECK (price > 0),
  venue     text        NOT NULL,
  extra     jsonb       NULL
);
CREATE INDEX IF NOT EXISTS fills_order_idx ON fills(order_id);

-- Positions: prefer bigint and only create if absent (08_positions will own it)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema='public' AND table_name='positions'
  ) THEN
    CREATE TABLE positions(
      symbol        text PRIMARY KEY,
      qty           bigint NOT NULL DEFAULT 0,
      avg_price     double precision NOT NULL DEFAULT 0,
      realized_pnl  double precision NOT NULL DEFAULT 0,
      updated_at    timestamptz NOT NULL DEFAULT now()
    );
  END IF;
END$$;

-- Detailed blotter view (owns the 'blotter' name)
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
