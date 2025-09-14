-- infra/postgres/init/08_budget.sql
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema='public' AND table_name='budget_reservations'
  ) THEN
    CREATE TABLE budget_reservations (
      id           bigserial PRIMARY KEY,
      as_of        date         NOT NULL DEFAULT CURRENT_DATE,
      bucket       text         NOT NULL CHECK (bucket IN ('LOW','MED','HIGH')),
      notional_inr double precision NOT NULL CHECK (notional_inr >= 0),
      meta         jsonb        NOT NULL DEFAULT '{}'::jsonb,
      created_at   timestamptz  NOT NULL DEFAULT now(),
      expires_at   timestamptz  NOT NULL
    );
    CREATE INDEX budget_reservations_active_idx
      ON budget_reservations(as_of, bucket)
      WHERE expires_at > now();
  END IF;
END$$;
