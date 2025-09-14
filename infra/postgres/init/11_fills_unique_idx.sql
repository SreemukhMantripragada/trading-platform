-- Unique triple for OMS fills (client_order_id, price, qty)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes
    WHERE schemaname='public' AND indexname='oms_fills_coid_px_qty_uidx'
  ) THEN
    CREATE UNIQUE INDEX oms_fills_coid_px_qty_uidx
      ON oms_fills(client_order_id, price, qty);
  END IF;
END$$;
