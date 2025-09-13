-- Trading Platform Database Schema
-- This schema defines tables and a view for managing orders, fills, and positions in the trading system.

-- Table: orders
-- Stores all order requests with details such as symbol, side (BUY/SELL), quantity, type (market/limit), status, and metadata.
CREATE TABLE IF NOT EXISTS orders (
  order_id         bigserial PRIMARY KEY,
  ts               timestamptz NOT NULL DEFAULT now(), -- Timestamp of order creation
  client_order_id  text UNIQUE,                        -- Optional client-supplied order identifier
  symbol           text        NOT NULL,               -- Trading symbol (e.g., AAPL)
  side             text        NOT NULL CHECK (side IN ('BUY','SELL')), -- Order side
  qty              integer     NOT NULL CHECK (qty > 0),               -- Order quantity
  order_type       text        NOT NULL DEFAULT 'MKT',  -- Order type: 'MKT' (market) or 'LMT' (limit)
  limit_price      double precision,                   -- Limit price if applicable
  strategy         text        NOT NULL,               -- Trading strategy name
  reason           text,                               -- Reason for order
  risk_bucket      text,                               -- Risk classification (e.g., LOW/MED/HIGH)
  status           text        NOT NULL DEFAULT 'NEW', -- Order status (NEW, SENT, FILLED, etc.)
  extra            jsonb       NOT NULL DEFAULT '{}'::jsonb -- Additional metadata
);

-- Indexes for efficient querying by timestamp and symbol
CREATE INDEX IF NOT EXISTS orders_ts_idx      ON orders(ts);
CREATE INDEX IF NOT EXISTS orders_sym_ts_idx  ON orders(symbol, ts);

-- Table: fills
-- Records executions (fills) against orders, including fill quantity, price, and venue.
CREATE TABLE IF NOT EXISTS fills (
  fill_id    bigserial PRIMARY KEY,
  order_id   bigint       NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE, -- Linked order
  ts         timestamptz  NOT NULL DEFAULT now(), -- Fill timestamp
  qty        integer      NOT NULL CHECK (qty > 0), -- Fill quantity
  price      double precision NOT NULL,             -- Fill price
  venue      text,                                 -- Execution venue
  extra      jsonb        NOT NULL DEFAULT '{}'::jsonb -- Additional metadata
);

-- Indexes for efficient querying by order and timestamp
CREATE INDEX IF NOT EXISTS fills_order_idx ON fills(order_id);
CREATE INDEX IF NOT EXISTS fills_ts_idx    ON fills(ts);

-- Table: positions
-- Tracks current position, average price, and realized P&L per symbol.
CREATE TABLE IF NOT EXISTS positions (
  symbol        text PRIMARY KEY,                  -- Trading symbol
  qty           integer           NOT NULL DEFAULT 0, -- Net position quantity
  avg_price     double precision  NOT NULL DEFAULT 0, -- Average price of position
  realized_pnl  double precision  NOT NULL DEFAULT 0, -- Realized profit and loss
  updated_at    timestamptz       NOT NULL DEFAULT now() -- Last update timestamp
);

-- View: blotter
-- Aggregates orders and their fills, showing fill quantity, average fill price, and last fill timestamp per order.
CREATE OR REPLACE VIEW blotter AS
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