-- OMS Live: live_orders + oms_fills (client-order keyed)

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.tables
                 WHERE table_schema='public' AND table_name='live_orders') THEN
    CREATE TABLE live_orders (
      client_order_id text PRIMARY KEY,
      symbol          text NOT NULL,
      side            text NOT NULL CHECK (side IN ('BUY','SELL')),
      qty             integer NOT NULL CHECK (qty > 0),
      px_ref          double precision,
      strategy        text,
      risk_bucket     text CHECK (risk_bucket IN ('LOW','MED','HIGH')),
      broker_order_id text,
      status          text NOT NULL DEFAULT 'NEW',
      extra           jsonb NOT NULL DEFAULT '{}'::jsonb,
      created_at      timestamptz NOT NULL DEFAULT now(),
      updated_at      timestamptz NOT NULL DEFAULT now()
    );
    CREATE INDEX live_orders_status_idx ON live_orders(status);
    CREATE INDEX live_orders_symbol_idx ON live_orders(symbol);
  END IF;

  -- Renamed: fills -> oms_fills (to avoid collision with trading fills)
  IF NOT EXISTS (SELECT 1 FROM information_schema.tables
                 WHERE table_schema='public' AND table_name='oms_fills') THEN
    CREATE TABLE oms_fills (
      id              bigserial PRIMARY KEY,
      client_order_id text,
      symbol          text NOT NULL,
      side            text NOT NULL CHECK (side IN ('BUY','SELL')),
      qty             integer NOT NULL CHECK (qty > 0),
      price           double precision NOT NULL,
      ts              timestamptz NOT NULL DEFAULT now(),
      strategy        text,
      risk_bucket     text
    );
    CREATE INDEX IF NOT EXISTS oms_fills_sym_ts_idx ON oms_fills(symbol, ts);
  END IF;
END$$;