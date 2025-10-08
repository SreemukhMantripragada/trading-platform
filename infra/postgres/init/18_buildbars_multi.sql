DROP MATERIALIZED VIEW IF EXISTS bars_3m_golden;
DROP MATERIALIZED VIEW IF EXISTS bars_5m_golden;
DROP MATERIALIZED VIEW IF EXISTS bars_15m_golden;
-- 3-minute MV
CREATE MATERIALIZED VIEW bars_3m_golden AS
WITH b AS (
  SELECT
    symbol,
    (date_trunc('minute', ts)
     - make_interval(mins => (EXTRACT(MINUTE FROM ts)::int % 3))) AS bucket_ts,
    ts, o, h, l, c, vol
  FROM bars_1m_golden
)
SELECT
  symbol,
  bucket_ts AS ts,
  (ARRAY_AGG(o ORDER BY ts))[1]                   AS o,   -- open = first o in bucket
  MAX(h)                                          AS h,   -- high
  MIN(l)                                          AS l,   -- low
  (ARRAY_AGG(c ORDER BY ts DESC))[1]              AS c,   -- close = last c in bucket
  SUM(vol)                                        AS vol  -- volume
FROM b
GROUP BY symbol, bucket_ts;

-- 5-minute MV
CREATE MATERIALIZED VIEW bars_5m_golden AS
WITH b AS (
  SELECT
    symbol,
    (date_trunc('minute', ts)
     - make_interval(mins => (EXTRACT(MINUTE FROM ts)::int % 5))) AS bucket_ts,
    ts, o, h, l, c, vol
  FROM bars_1m_golden
)
SELECT
  symbol,
  bucket_ts AS ts,
  (ARRAY_AGG(o ORDER BY ts))[1]                   AS o,
  MAX(h)                                          AS h,
  MIN(l)                                          AS l,
  (ARRAY_AGG(c ORDER BY ts DESC))[1]              AS c,
  SUM(vol)                                        AS vol
FROM b
GROUP BY symbol, bucket_ts;

-- 15-minute MV
CREATE MATERIALIZED VIEW bars_15m_golden AS
WITH b AS (
  SELECT
    symbol,
    (date_trunc('minute', ts)
     - make_interval(mins => (EXTRACT(MINUTE FROM ts)::int % 15))) AS bucket_ts,
    ts, o, h, l, c, vol
  FROM bars_1m_golden
)
SELECT
  symbol,
  bucket_ts AS ts,
  (ARRAY_AGG(o ORDER BY ts))[1]                   AS o,
  MAX(h)                                          AS h,
  MIN(l)                                          AS l,
  (ARRAY_AGG(c ORDER BY ts DESC))[1]              AS c,
  SUM(vol)                                        AS vol
FROM b
GROUP BY symbol, bucket_ts;

CREATE UNIQUE INDEX bars_3m_golden_symbol_ts_idx  ON bars_3m_golden  (symbol, ts);
CREATE UNIQUE INDEX bars_5m_golden_symbol_ts_idx  ON bars_5m_golden  (symbol, ts);
CREATE UNIQUE INDEX bars_15m_golden_symbol_ts_idx ON bars_15m_golden (symbol, ts);
