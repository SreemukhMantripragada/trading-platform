"""
monitoring/gateway_oms_exporter.py
Prometheus exporter for OMS + Zerodha gateway health.

Metrics exposed:
- gateway_orders_24h_total{status}
- gateway_fills_24h_total
- gateway_orders_pending_now
- gateway_orders_rejected_24h
- gateway_broker_status_now{status}
- gateway_order_latency_seconds  (histogram, COMPLETE in last 1h)

Run:
  POSTGRES_HOST=localhost POSTGRES_PORT=5432 POSTGRES_DB=trading POSTGRES_USER=trader POSTGRES_PASSWORD=trader \
  METRICS_PORT=8016 python monitoring/gateway_oms_exporter.py
"""
from __future__ import annotations
import os, asyncio, asyncpg
from prometheus_client import start_http_server, Gauge, Histogram

PG_HOST=os.getenv("POSTGRES_HOST","localhost"); PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading"); PG_USER=os.getenv("POSTGRES_USER","trader"); PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")
METRICS_PORT=int(os.getenv("METRICS_PORT","8016"))

ORDERS_24H   = Gauge("gateway_orders_24h_total", "Orders in last 24h by status", ["status"])
FILLS_24H    = Gauge("gateway_fills_24h_total", "Fills in last 24h")
PENDING_NOW  = Gauge("gateway_orders_pending_now", "Pending/working orders now")
REJ_24H      = Gauge("gateway_orders_rejected_24h", "Rejected orders in last 24h")
BROKER_NOW   = Gauge("gateway_broker_status_now", "Broker order rows by status now", ["status"])
LAT_HIST     = Histogram("gateway_order_latency_seconds", "Order create -> broker COMPLETE (last 1h)",
                         buckets=(0.5,1,2,3,5,8,13,21,34,55,89))

async def scrape_once(pool):
    async with pool.acquire() as con:
        # 24h orders by status
        rows = await con.fetch("""
          SELECT coalesce(status,'UNKNOWN') AS s, count(*) AS n
          FROM orders
          WHERE ts >= now() - interval '24 hours'
          GROUP BY 1
        """)
        seen=set()
        for r in rows:
            ORDERS_24H.labels(r["s"]).set(int(r["n"])); seen.add(r["s"])
        # zero missing labels to avoid stale
        for s in ("NEW","ACK","PENDING","OPEN","FILLED","PARTIAL","CANCELLED","REJECTED","UNKNOWN"):
            if s not in seen: ORDERS_24H.labels(s).set(0)

        # pending now
        r = await con.fetchrow("""
          SELECT count(*) AS n FROM orders
          WHERE coalesce(status,'') IN ('NEW','ACK','PENDING','OPEN')
        """)
        PENDING_NOW.set(int(r["n"]))

        # rejected 24h
        r = await con.fetchrow("""
          SELECT count(*) AS n FROM orders
          WHERE ts >= now() - interval '24 hours' AND coalesce(status,'')='REJECTED'
        """)
        REJ_24H.set(int(r["n"]))

        # fills 24h
        r = await con.fetchrow("""
          SELECT count(*) AS n FROM fills
          WHERE ts >= now() - interval '24 hours'
        """)
        FILLS_24H.set(int(r["n"]))

        # broker status snapshot
        rows = await con.fetch("SELECT coalesce(status,'UNKNOWN') AS s, count(*) AS n FROM broker_orders GROUP BY 1")
        seen=set()
        for r in rows:
            BROKER_NOW.labels(r["s"]).set(int(r["n"])); seen.add(r["s"])
        for s in ("PENDING","OPEN","COMPLETE","REJECTED","CANCELLED","UNKNOWN"):
            if s not in seen: BROKER_NOW.labels(s).set(0)

        # latency histogram (last 1h), prefer orders.created_at else orders.ts
        rows = await con.fetch("""
          WITH src AS (
            SELECT o.client_order_id AS coid,
                   coalesce(o.created_at, o.ts) AS cts,
                   bo.updated_at AS uts
            FROM broker_orders bo
            JOIN orders o ON o.client_order_id = bo.coid
            WHERE bo.status='COMPLETE'
              AND bo.updated_at >= now() - interval '1 hour'
          )
          SELECT extract(epoch from (uts - cts)) AS sec FROM src WHERE uts > cts
        """)
        for r in rows:
            LAT_HIST.observe(float(r["sec"]))

async def main():
    pool = await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
    start_http_server(METRICS_PORT)
    print(f"[gwexp] listening :{METRICS_PORT}/metrics")
    try:
        while True:
            try:
                await scrape_once(pool)
            except Exception as e:
                print(f"[gwexp] scrape error: {e}")
            await asyncio.sleep(5)
    finally:
        await pool.close()

if __name__=="__main__":
    asyncio.run(main())
