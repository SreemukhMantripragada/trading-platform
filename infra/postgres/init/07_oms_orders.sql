-- OMS Orders & Audit (augment base orders table instead of creating a duplicate)

ALTER TABLE IF EXISTS orders
  ADD COLUMN IF NOT EXISTS last_update timestamptz NOT NULL DEFAULT now();

ALTER TABLE IF EXISTS orders
  ADD COLUMN IF NOT EXISTS audit_hash text;

CREATE INDEX IF NOT EXISTS orders_status_idx ON orders(status);
CREATE INDEX IF NOT EXISTS orders_symbol_idx ON orders(symbol);

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
