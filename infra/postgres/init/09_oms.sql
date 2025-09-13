-- Orders & OMS state
CREATE TABLE IF NOT EXISTS orders (
  client_order_id  text PRIMARY KEY,       -- idempotency key
  ts               timestamptz NOT NULL,   -- order creation (from event)
  symbol           text        NOT NULL,
  side             text        NOT NULL,   -- BUY/SELL/EXIT
  qty              bigint      NOT NULL,
  order_type       text        NOT NULL,   -- MKT/LMT/IOC etc.
  strategy         text        NOT NULL,
  risk_bucket      text        NOT NULL,
  status           text        NOT NULL,   -- NEW/ACK/PARTIAL/FILLED/CANCELED/REJECTED
  last_update      timestamptz NOT NULL DEFAULT now(),
  extra            jsonb,
  audit_hash       text        NOT NULL
);

CREATE INDEX IF NOT EXISTS orders_status_idx ON orders(status);
CREATE INDEX IF NOT EXISTS orders_symbol_idx ON orders(symbol);

-- Minimal order state transitions audit trail
CREATE TABLE IF NOT EXISTS order_audit (
  id      bigserial PRIMARY KEY,
  client_order_id text NOT NULL,
  ts      timestamptz NOT NULL DEFAULT now(),
  from_st text,
  to_st   text,
  note    text,
  meta    jsonb
);

CREATE INDEX IF NOT EXISTS order_audit_coid_idx ON order_audit(client_order_id);
