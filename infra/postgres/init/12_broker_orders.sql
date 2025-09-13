CREATE TABLE IF NOT EXISTS broker_orders (
  coid            text PRIMARY KEY,        -- client_order_id
  broker_order_id text,
  status          text,
  avg_price       double precision,
  filled_qty      integer,
  raw             jsonb,
  updated_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS broker_orders_status_idx ON broker_orders(status);
