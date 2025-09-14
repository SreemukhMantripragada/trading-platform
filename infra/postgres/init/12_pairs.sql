-- infra/postgres/init/12_pairs.sql
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.tables
                 WHERE table_schema='public' AND table_name='pairs_scan') THEN
    CREATE TABLE pairs_scan (
      scan_date   date           NOT NULL,
      sym1        text           NOT NULL,
      sym2        text           NOT NULL,
      pvalue      double precision NOT NULL,
      beta        double precision NOT NULL,  -- hedge ratio for (sym2 ~ beta*sym1)
      half_life_m double precision,          -- mean reversion half-life (minutes)
      zmean       double precision,          -- z-score mean used at scan
      zstd        double precision,          -- z-score std used at scan
      bars_used   integer        NOT NULL,   -- sample size
      PRIMARY KEY (scan_date, sym1, sym2)
    );
    CREATE INDEX pairs_scan_pval_idx ON pairs_scan(scan_date, pvalue);
  END IF;

  IF NOT EXISTS (SELECT 1 FROM information_schema.tables
                 WHERE table_schema='public' AND table_name='pairs_live') THEN
    CREATE TABLE pairs_live (
      trade_date  date           NOT NULL,
      sym1        text           NOT NULL,
      sym2        text           NOT NULL,
      beta        double precision NOT NULL,
      entry_z     double precision NOT NULL,
      exit_z      double precision NOT NULL,
      stop_z      double precision NOT NULL,
      window_m    integer        NOT NULL,
      chosen_at   timestamptz    NOT NULL DEFAULT now(),
      PRIMARY KEY (trade_date, sym1, sym2)
    );
  END IF;
END$$;
