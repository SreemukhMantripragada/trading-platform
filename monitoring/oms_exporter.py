# monitoring/oms_exporter.py
from __future__ import annotations
import os, asyncio, asyncpg
from prometheus_client import start_http_server, Gauge

PG_HOST=os.getenv("POSTGRES_HOST","localhost"); PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading"); PG_USER=os.getenv("POSTGRES_USER","trader"); PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")
PORT=int(os.getenv("METRICS_PORT","8025"))

OPEN_ORDERS = Gauge("oms_open_orders", "Open orders by status", ["status"])
OPEN_POS    = Gauge("oms_open_position_qty", "Net position qty by symbol", ["symbol"])
BUCKET_EXPO = Gauge("oms_bucket_notional_inr", "Approx notional by bucket", ["bucket"])

SQL_ORD = "SELECT status, count(*) AS n FROM live_orders GROUP BY status"
SQL_POS = """
WITH p AS (
  SELECT symbol,
         SUM(CASE WHEN side='BUY'  THEN qty ELSE 0 END) -
         SUM(CASE WHEN side='SELL' THEN qty ELSE 0 END) AS net_qty
  FROM fills WHERE ts::date = CURRENT_DATE GROUP BY symbol
)
SELECT symbol, net_qty FROM p WHERE net_qty != 0
"""
SQL_EXPO = """
SELECT risk_bucket, SUM(qty * COALESCE(b.c,0)) AS notional
FROM fills f
LEFT JOIN LATERAL (
  SELECT c FROM bars_1m b WHERE b.symbol=f.symbol ORDER BY ts DESC LIMIT 1
) b ON TRUE
WHERE f.ts::date = CURRENT_DATE
GROUP BY risk_bucket
"""

async def loop():
    pool=await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
    try:
        while True:
            try:
                async with pool.acquire() as con:
                    rows=await con.fetch(SQL_ORD)
                    for r in rows:
                        OPEN_ORDERS.labels(r["status"]).set(int(r["n"]))
                    rows=await con.fetch(SQL_POS)
                    for r in rows:
                        OPEN_POS.labels(r["symbol"]).set(int(r["net_qty"]))
                    rows=await con.fetch(SQL_EXPO)
                    for r in rows:
                        BUCKET_EXPO.labels((r["risk_bucket"] or "MED")).set(float(r["notional"] or 0.0))
            except Exception as e:
                print("[oms_exporter] error:", e)
            await asyncio.sleep(5)
    finally:
        await pool.close()

if __name__=="__main__":
    start_http_server(PORT)
    import asyncio; asyncio.run(loop())
