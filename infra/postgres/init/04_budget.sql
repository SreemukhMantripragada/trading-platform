-- 04_budget.sql
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
  END IF;

  -- Drop the old (failing) index name if it exists from prior attempts
  IF EXISTS (
    SELECT 1 FROM pg_indexes
    WHERE schemaname='public' AND indexname='budget_reservations_active_idx'
  ) THEN
    DROP INDEX budget_reservations_active_idx;
  END IF;

  -- Use a composite btree index; queries like
  --   WHERE as_of = $1 AND bucket = $2 AND expires_at > now()
  -- can use it efficiently.
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes
    WHERE schemaname='public' AND indexname='budget_reservations_lookup_idx'
  ) THEN
    CREATE INDEX budget_reservations_lookup_idx
      ON budget_reservations(as_of, bucket, expires_at);
  END IF;
END$$;
