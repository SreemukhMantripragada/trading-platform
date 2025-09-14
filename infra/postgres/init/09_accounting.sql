-- infra/postgres/init/09_accounting.sql
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema='public' AND table_name='pnl_intraday'
  ) THEN
    CREATE TABLE pnl_intraday(
      as_of        date NOT NULL,
      symbol       text NOT NULL,
      bucket       text NOT NULL CHECK (bucket IN ('LOW','MED','HIGH')),
      realized_inr double precision NOT NULL DEFAULT 0,
      unrealized_inr double precision NOT NULL DEFAULT 0,
      updated_at   timestamptz NOT NULL DEFAULT now(),
      PRIMARY KEY (as_of, symbol, bucket)
    );
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema='public' AND table_name='pnl_eod'
  ) THEN
    CREATE TABLE pnl_eod(
      as_of        date PRIMARY KEY,
      bucket       text NOT NULL CHECK (bucket IN ('LOW','MED','HIGH')),
      realized_inr double precision NOT NULL,
      unrealized_inr double precision NOT NULL,
      created_at   timestamptz NOT NULL DEFAULT now()
    );
  END IF;
END$$;
