"""
monitoring/oms_lifecycle_exporter.py
Prometheus exporter for OMS lifecycle health.

Metrics:
- oms_orders_by_status{status}           (current snapshot of orders table)
- oms_open_orders_now                    (NEW/ACK/PENDING/OPEN count)
- oms_orders_24h_total                   (orders last 24h)
- oms_fills_24h_total                    (fills last 24h)
- oms_cancel_rate_24h                    (cancels / orders last 24h)
- oms_reject_rate_24h                    (rejects / orders last 24h)
- oms_open_age_seconds                   (histogram of open order ages)
- oms_time_to_complete_seconds           (histogram creation->broker COMPLETE, last 1h)

Run:
  POSTGRES_HOST=localhost POSTGRES_PORT=5432 POSTGRES_DB=trading POSTGRES_USER=trader POSTGRES_PASSWORD=trader \
  METRICS_PORT=8017 python monitoring/oms_lifecycle_exporter.py
"""
from __future__ import annotations
import os, asyncio, asyncpg
from prometheus_client import start_http_server, Gauge, Histogram

PG_HOST=os.getenv("POSTGRES_HOST","localhost")
PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading")
PG_USER=os.getenv("POSTGRES_USER","trader")
PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")
METRICS_PORT=int(os.getenv("METRICS_PORT","8017"))

ORDERS_BY_STATUS = Gauge("oms_orders_by_status", "Orders by current status", ["status"])
OPEN_NOW         = Gauge("oms_open_orders_now", "Open/working orders now")
ORDERS_24H       = Gauge("oms_orders_24h_total", "Orders in last 24h")
FILLS_24H        = Gauge("oms_fills_24h_total", "Fills in last 24h")
CANCEL_RATE_24H  = Gauge("oms_cancel_rate_24h", "Cancel/Orders last 24h")
REJECT_RATE_24H  = Gauge("oms_reject_rate_24h", "Reject/Orders last 24h")

OPEN_AGE_HIST = Histogram(
    "oms_open_age_seconds", "Age of open orders (seconds)",
    buckets=(5,10,20,30,45,60,90,120,180,300,600,900,1800)
)
TTF_HIST = Histogram(
    "oms_time_to_complete_seconds", "Order create -> broker COMPLETE (seconds, last 1h)",
    buckets=(0.5,1,2,3,5,8,13,21,34,55,89,144)
)

OPEN_STATES=("NEW","ACK","PENDING","OPEN")

async def scrape(pool):
    async with pool.acquire() as con:
        rows = await con.fetch("SELECT coalesce(status,'UNKNOWN') AS s, count(*) AS n FROM orders GROUP BY 1")
        seen=set()
        for r in rows:
            ORDERS_BY_STATUS.labels(r["s"]).set(int(r["n"]))
            seen.add(r["s"])
        for s in ("NEW","ACK","PENDING","OPEN","FILLED","PARTIAL","CANCELLED","REJECTED","UNKNOWN"):
            if s not in seen:
                ORDERS_BY_STATUS.labels(s).set(0)

        r = await con.fetchrow(
            "SELECT count(*) AS n FROM orders WHERE coalesce(status,'') = ANY($1::text[])",
            list(OPEN_STATES)
        )
        OPEN_NOW.set(int(r["n"]))

        r = await con.fetchrow("SELECT count(*) AS n FROM orders WHERE ts >= now() - interval '24 hours'")
        orders_24h=int(r["n"]); ORDERS_24H.set(orders_24h)

        r = await con.fetchrow("SELECT count(*) AS n FROM fills WHERE ts >= now() - interval '24 hours'")
        fills_24h=int(r["n"]);  FILLS_24H.set(fills_24h)

        # rates
        r = await con.fetchrow("SELECT count(*) AS n FROM orders WHERE ts >= now() - interval '24 hours' AND coalesce(status,'')='CANCELLED'")
        canc=int(r["n"])
        r = await con.fetchrow("SELECT count(*) AS n FROM orders WHERE ts >= now() - interval '24 hours' AND coalesce(status,'')='REJECTED'")
        rej=int(r["n"])
        CANCEL_RATE_24H.set((canc/orders_24h) if orders_24h>0 else 0.0)
        REJECT_RATE_24H.set((rej/orders_24h) if orders_24h>0 else 0.0)

        # open order ages (sample a bunch)
        rows = await con.fetch("""
          SELECT extract(epoch from (now() - coalesce(updated_at, ts))) AS age
          FROM orders
          WHERE coalesce(status,'') = ANY($1::text[])
          LIMIT 500
        """, list(OPEN_STATES))
        for r in rows:
            age=float(r["age"] or 0.0)
            if age>0: OPEN_AGE_HIST.observe(age)

        # time-to-complete (last 1h, join broker_orders COMPLETE)
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
            TTF_HIST.observe(float(r["sec"]))

async def main():
    pool=await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
    start_http_server(METRICS_PORT)
    print(f"[oms-exp] :{METRICS_PORT}/metrics")
    try:
        while True:
            try:
                await scrape(pool)
            except Exception as e:
                print(f"[oms-exp] scrape error: {e}")
            await asyncio.sleep(5)
    finally:
        await pool.close()

if __name__=="__main__":
    asyncio.run(main())
