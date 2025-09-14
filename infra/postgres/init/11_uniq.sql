-- infra/postgres/init/11_uniq.sql
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes WHERE schemaname='public' AND indexname='fills_coid_px_qty_uidx'
  ) THEN
    CREATE UNIQUE INDEX fills_coid_px_qty_uidx ON fills(client_order_id, price, qty);
  END IF;
END$$;
