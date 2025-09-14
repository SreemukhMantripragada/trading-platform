-- Pairs universe (unique (a_symbol,b_symbol) in alpha order)
CREATE TABLE IF NOT EXISTS pairs_universe (
  pair_id    bigserial PRIMARY KEY,
  a_symbol   text NOT NULL,
  b_symbol   text NOT NULL,
  beta       double precision NOT NULL DEFAULT 1.0,  -- hedge ratio A ~ beta * B
  active     boolean NOT NULL DEFAULT true,
  note       text,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS pairs_universe_ab_uniq
  ON pairs_universe(LEAST(a_symbol,b_symbol), GREATEST(a_symbol,b_symbol));

-- Nightly stats snapshot per pair
CREATE TABLE IF NOT EXISTS pairs_stats (
  pair_id     bigint REFERENCES pairs_universe(pair_id) ON DELETE CASCADE,
  corr_lookback_days int NOT NULL,
  corr        double precision,
  dist_metric double precision,               -- e.g., stdev(spread)
  updated_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (pair_id, updated_at)
);

-- Optional (if you want to persist signal history later)
-- CREATE TABLE IF NOT EXISTS pairs_signal_log(
--   ts      timestamptz NOT NULL DEFAULT now(),
--   pair_id bigint REFERENCES pairs_universe(pair_id) ON DELETE CASCADE,
--   zscore  double precision,
--   action  text,          -- ENTER_LONG_A_SHORT_B / ENTER_SHORT_A_LONG_B / EXIT
--   meta    jsonb
-- );
