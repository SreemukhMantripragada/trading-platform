# execution/zerodha_poller.py
from __future__ import annotations
import os, asyncio, time, ujson as json
import asyncpg
from aiokafka import AIOKafkaProducer
from dotenv import load_dotenv

load_dotenv(".env"); load_dotenv("infra/.env")

BROKER=os.getenv("KAFKA_BROKER","localhost:9092")
FILL_TOPIC=os.getenv("FILL_TOPIC","fills")
POLL_SEC=int(os.getenv("POLL_SEC","4"))
DRY_RUN=int(os.getenv("DRY_RUN","1"))

PG_HOST=os.getenv("POSTGRES_HOST","localhost"); PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading"); PG_USER=os.getenv("POSTGRES_USER","trader"); PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")

KITE_API_KEY=os.getenv("KITE_API_KEY")
TOKEN_FILE=os.getenv("ZERODHA_TOKEN_FILE","ingestion/auth/token.json")

def load_kite():
    if DRY_RUN: return None
    from kiteconnect import KiteConnect, exceptions as kz_ex
    import json as _j
    t=_j.load(open(TOKEN_FILE))
    access=os.getenv("KITE_ACCESS_TOKEN") or t.get("access_token")
    k=KiteConnect(api_key=KITE_API_KEY); k.set_access_token(access)
    k.profile()  # raise if bad
    return k

STATUS_MAP={
    "OPEN":"ACK", "TRIGGER PENDING":"ACK", "PUT ORDER REQ RECEIVED":"ACK",
    "COMPLETE":"FILLED", "CANCELLED":"CANCELLED", "REJECTED":"REJECTED",
    # any unknown -> ACK
}

SQL_OPEN_ORDS = """
SELECT client_order_id, broker_order_id, symbol, side, qty, status
FROM live_orders
WHERE status IN ('PENDING','ACK') AND broker_order_id IS NOT NULL
"""

SQL_UPD_STATUS = """
UPDATE live_orders
SET status=$2, updated_at=now()
WHERE client_order_id=$1
"""

SQL_INSERT_FILL = """
INSERT INTO fills(client_order_id, symbol, side, qty, price, strategy, risk_bucket)
VALUES($1,$2,$3,$4,$5,$6,$7)
"""

SQL_FILL_EXISTS = """
SELECT 1 FROM fills WHERE client_order_id=$1 AND price=$2 AND qty=$3 LIMIT 1
"""

async def main():
    pool=await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
    prod=AIOKafkaProducer(bootstrap_servers=BROKER, acks="all", linger_ms=5)
    await prod.start()
    kite=None
    if not DRY_RUN:
        kite=load_kite()
    print(f"[poller] DRY_RUN={DRY_RUN} polling {POLL_SEC}s; publishing fills->{FILL_TOPIC}")
    try:
        while True:
            try:
                if DRY_RUN:
                    # Nothing external to poll; reconcile from our own state if needed.
                    await asyncio.sleep(POLL_SEC); continue

                # Live: fetch once per poll
                orders = { o["order_id"]: o for o in (kite.orders() or []) }
                trades = kite.trades() or []

                async with pool.acquire() as con:
                    open_rows = await con.fetch(SQL_OPEN_ORDS)
                    for r in open_rows:
                        coid=r["client_order_id"]; boid=r["broker_order_id"]; st_local=r["status"]
                        od = orders.get(boid)
                        if od:
                            st_broker = STATUS_MAP.get((od.get("status") or "").upper(), "ACK")
                            if st_broker != st_local:
                                await con.execute(SQL_UPD_STATUS, coid, st_broker)
                        # Fill reconciliation
                        for tr in trades:
                            if str(tr.get("order_id")) != str(boid): continue
                            px=float(tr.get("average_price") or tr.get("price") or 0.0)
                            q=int(tr.get("quantity") or tr.get("qty") or 0)
                            if q<=0: continue
                            exist = await con.fetchrow(SQL_FILL_EXISTS, coid, px, q)
                            if not exist:
                                # Need symbol/side from our order row
                                await con.execute(SQL_INSERT_FILL, coid, r["symbol"], r["side"], q, px, None, None)
                                fill={"client_order_id":coid,"symbol":r["symbol"],"side":r["side"],"qty":q,"price":px,"ts":int(time.time())}
                                await prod.send_and_wait(FILL_TOPIC, json.dumps(fill).encode(), key=r["symbol"].encode())
            except Exception as e:
                print("[poller] error:", e)
            await asyncio.sleep(POLL_SEC)
    finally:
        await prod.stop(); await pool.close()

if __name__=="__main__":
    asyncio.run(main())
