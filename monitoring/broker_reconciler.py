"""
monitoring/broker_reconciler.py
Intraday broker reconciliation (positions & open orders) vs local DB.

Exports Prometheus:
- recon_qty_diff{symbol}                  (signed net qty delta: broker - local)
- recon_abs_mismatch_symbols              (count of symbols with |delta| > QTY_TOL)
- recon_unknown_broker_orders_total       (broker orders we don't know locally)
- recon_open_order_delta                  (broker_open - local_open)

On breach (abs_mismatch_symbols > BREACH_SYMS or |delta| > KILL_QTY_TOL for any symbol)
and ENFORCE=1, emits kill on Kafka topic 'control.kill'.

ENV:
  POSTGRES_*            (standard)
  KITE_API_KEY, ZERODHA_TOKEN_FILE
  METRICS_PORT=8021
  QTY_TOL=0             # allowed per-symbol net difference
  BREACH_SYMS=0         # if number of mismatched symbols > this, alert
  KILL_QTY_TOL=0        # trip kill if any |delta| > this
  ENFORCE=0             # if 1 -> send kill
  KAFKA_BROKER=localhost:9092
"""
from __future__ import annotations
import os, asyncio, asyncpg, time, ujson as json
from dotenv import load_dotenv
from kiteconnect import KiteConnect, exceptions as kz
from prometheus_client import start_http_server, Gauge, Counter
from aiokafka import AIOKafkaProducer

load_dotenv(".env"); load_dotenv("infra/.env")

PG_HOST=os.getenv("POSTGRES_HOST","localhost"); PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading"); PG_USER=os.getenv("POSTGRES_USER","trader"); PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")
KAFKA=os.getenv("KAFKA_BROKER","localhost:9092")

KITE_API_KEY=os.getenv("KITE_API_KEY"); TOKEN_FILE=os.getenv("ZERODHA_TOKEN_FILE","ingestion/auth/token.json")
PORT=int(os.getenv("METRICS_PORT","8021"))
QTY_TOL=int(os.getenv("QTY_TOL","0"))
BREACH_SYMS=int(os.getenv("BREACH_SYMS","0"))
KILL_QTY_TOL=int(os.getenv("KILL_QTY_TOL","0"))
ENFORCE=bool(int(os.getenv("ENFORCE","0")))

QDIFF = Gauge("recon_qty_diff", "broker - local net qty", ["symbol"])
MISN  = Gauge("recon_abs_mismatch_symbols", "count(|delta|>tol)")
UNK   = Counter("recon_unknown_broker_orders_total","unknown broker orders")
ODEL  = Gauge("recon_open_order_delta", "broker_open - local_open")

def load_access():
    import json
    t=json.load(open(TOKEN_FILE))
    if t.get("api_key")!=KITE_API_KEY: raise SystemExit("Token file API key mismatch; re-auth")
    return t["access_token"]

async def local_net_positions(con):
    rows=await con.fetch("""
      SELECT symbol, SUM(CASE WHEN side='BUY' THEN qty ELSE -qty END)::int AS net
      FROM fills
      WHERE ts::date = CURRENT_DATE
      GROUP BY symbol
    """)
    return {r["symbol"]: int(r["net"] or 0) for r in rows}

async def local_open_orders(con):
    rows=await con.fetch("""
      SELECT client_order_id FROM orders
      WHERE coalesce(status,'') = ANY($1::text[])
    """, ["NEW","ACK","PENDING","OPEN","PARTIAL"])
    return set(r["client_order_id"] for r in rows)

async def kill_broadcast(producer, scope="ALL", reason="recon_breach", bucket=""):
    msg={"ts": time.time(), "scope": scope, "bucket": bucket, "reason": reason}
    await producer.send_and_wait("control.kill", json.dumps(msg).encode(), key=(bucket or "ALL").encode())
    print("[recon] KILL broadcast:", msg)

async def main():
    access=load_access()
    kite=KiteConnect(api_key=KITE_API_KEY); kite.set_access_token(access)
    try: kite.profile()
    except kz.TokenException: raise SystemExit("Zerodha token invalid/expired")

    pool=await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
    prod=AIOKafkaProducer(bootstrap_servers=KAFKA, acks="all", linger_ms=5)
    await prod.start()
    start_http_server(PORT)
    print(f"[recon] :{PORT}/metrics ENFORCE={int(ENFORCE)} tol={QTY_TOL} kill_tol={KILL_QTY_TOL}")
    try:
        while True:
            try:
                async with pool.acquire() as con:
                    local_pos = await local_net_positions(con)
                    local_open = await local_open_orders(con)

                # Broker positions
                bpos = {}
                try:
                    pos = kite.positions().get("net", [])
                    for p in pos:
                        sym = p.get("tradingsymbol"); q = int(p.get("quantity") or 0)
                        if sym: bpos[sym]=bpos.get(sym,0)+q
                except Exception as e:
                    print("[recon] positions error:", e); bpos={}

                # Deltas
                syms=set(local_pos.keys())|set(bpos.keys())
                mismatched=0; breach=False
                for s in syms:
                    d = int(bpos.get(s,0) - local_pos.get(s,0))
                    QDIFF.labels(s).set(d)
                    if abs(d) > QTY_TOL:
                        mismatched += 1
                    if KILL_QTY_TOL>0 and abs(d) > KILL_QTY_TOL: breach=True
                MISN.set(mismatched)

                # Broker open orders vs local
                broker_orders = []
                try:
                    broker_orders = kite.orders()
                except Exception as e:
                    print("[recon] orders error:", e)
                broker_open = [o for o in broker_orders if (o.get("status") or "").lower() not in ("complete","cancelled","rejected")]
                ODEL.set(len(broker_open) - len(local_open))

                # Unknown broker orders
                unknown=0
                for o in broker_open:
                    # If your gateway sets 'tag' to client_order_id, you can match here. Otherwise skip.
                    tag=o.get("tag") or ""
                    if tag and tag not in local_open:
                        unknown+=1
                if unknown>0: UNK.inc(unknown)

                # Enforce kill?
                if ENFORCE and (breach or mismatched > BREACH_SYMS):
                    await kill_broadcast(prod, "ALL", "recon_breach")
            except Exception as e:
                print("[recon] loop error:", e)

            await asyncio.sleep(10)
    finally:
        await prod.stop(); await pool.close()

if __name__=="__main__":
    asyncio.run(main())
