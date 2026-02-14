"""
execution/oms_health_exporter.py
OMS health metrics from Postgres.

Metrics:
- oms_reject_rate_24h
- oms_time_to_fill_seconds_bucket/_count/_sum (Prometheus histogram)
- oms_broker_error_total{code}

Assumptions:
- orders(client_order_id, created_at timestamptz, status, reason text)
- fills(client_order_id, ts timestamptz)

ENV: POSTGRES_*, METRICS_PORT=8026
"""
from __future__ import annotations
import os, asyncio, asyncpg, re
from datetime import timedelta
from prometheus_client import start_http_server, Gauge, Histogram, Counter

PG_HOST=os.getenv("POSTGRES_HOST","localhost"); PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading"); PG_USER=os.getenv("POSTGRES_USER","trader"); PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")
PORT=int(os.getenv("METRICS_PORT","8026"))

REJECT = Gauge("oms_reject_rate_24h", "Reject rate over last 24h")
LAT    = Histogram("oms_time_to_fill_seconds", "Time from order create to first fill", buckets=(0.2,0.5,1,2,3,5,8,13,21,34))
ERR    = Counter("oms_broker_error_total", "Broker error taxonomy counts", ["code"])

ERR_PATTERNS=[
    ("RATE_LIMIT","(rate.?limit|429)"),
    ("MARGIN","(margin|insufficient funds)"),
    ("RISK","(risk|blocked)"),
    ("INSTRUMENT","(instrument|trading symbol|symbol invalid)"),
    ("NETWORK","(timeout|network|dns)"),
    ("REJECT_OTHER",".*"),
]

async def loop():
    pool=await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
    try:
        while True:
            try:
                async with pool.acquire() as con:
                    # Reject rate (24h)
                    r = await con.fetchrow("""
                      SELECT
                        SUM(CASE WHEN lower(coalesce(status,''))='rejected' THEN 1 ELSE 0 END)::float8 AS rej,
                        COUNT(*)::float8 AS tot
                      FROM orders
                      WHERE created_at >= now() - interval '24 hours'
                    """)
                    rej=float(r["rej"] or 0.0); tot=float(r["tot"] or 0.0)
                    REJECT.set(0.0 if tot==0 else rej/tot)

                    # Fill latency (one pass per scrape – sample last 500)
                    rows = await con.fetch("""
                      WITH first_fill AS (
                        SELECT o.client_order_id, o.created_at, MIN(f.ts) AS first_fill_ts
                        FROM orders o
                        JOIN fills f USING(client_order_id)
                        WHERE o.created_at >= now() - interval '24 hours'
                        GROUP BY 1,2
                        ORDER BY o.created_at DESC
                        LIMIT 500
                      )
                      SELECT EXTRACT(EPOCH FROM (first_fill_ts - created_at)) AS secs FROM first_fill
                      WHERE first_fill_ts IS NOT NULL
                    """)
                    for r in rows:
                        LAT.observe(max(0.0, float(r["secs"])))

                    # Error taxonomy (increment since last scrape—stateless; okay to double-count across restarts)
                    rows = await con.fetch("""
                      SELECT lower(coalesce(reason,'')) AS reason
                      FROM orders
                      WHERE status='REJECTED' AND created_at >= now() - interval '24 hours'
                      ORDER BY created_at DESC LIMIT 500
                    """)
                    for rr in rows:
                        txt=rr["reason"] or ""
                        code="REJECT_OTHER"
                        for name, pat in ERR_PATTERNS:
                            if re.search(pat, txt, re.I):
                                code=name; break
                        ERR.labels(code).inc()
            except Exception as e:
                print("[oms-health] error:", e)
            await asyncio.sleep(10)
    finally:
        await pool.close()

def main():
    start_http_server(PORT)
    print(f"[oms-health] :{PORT}/metrics")
    import asyncio; asyncio.run(loop())

if __name__=="__main__":
    main()
