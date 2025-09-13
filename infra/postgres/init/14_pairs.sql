CREATE TABLE IF NOT EXISTS pairs_candidates(
  as_of date NOT NULL,
  sym_a text NOT NULL,
  sym_b text NOT NULL,
  rho double precision,
  half_life double precision,
  PRIMARY KEY (as_of, sym_a, sym_b)
);

CREATE TABLE IF NOT EXISTS pairs_signals(
  ts timestamptz NOT NULL DEFAULT now(),
  sym_a text NOT NULL,
  sym_b text NOT NULL,
  zscore double precision NOT NULL,
  side text NOT NULL CHECK (side IN ('LONG_A_SHORT_B','SHORT_A_LONG_B','EXIT')),
  reason text
);
