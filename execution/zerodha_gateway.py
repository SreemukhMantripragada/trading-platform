# execution/zerodha_gateway.py
from __future__ import annotations
import os, asyncio, ujson as json, time
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import asyncpg
from dotenv import load_dotenv
from kiteconnect import KiteConnect, exceptions as kz_ex

load_dotenv(".env"); load_dotenv("infra/.env")

BROKER=os.getenv("KAFKA_BROKER","localhost:9092")
IN_TOPIC=os.getenv("IN_TOPIC","orders.allowed")
FILL_TOPIC=os.getenv("FILL_TOPIC","fills")
GROUP_ID=os.getenv("KAFKA_GROUP","zerodha_gateway")
DRY_RUN=int(os.getenv("DRY_RUN","1"))
RATE_PER_SEC=float(os.getenv("BROKER_RATE_PER_SEC","3.0"))

PG_HOST=os.getenv("POSTGRES_HOST","localhost"); PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading"); PG_USER=os.getenv("POSTGRES_USER","trader"); PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")

KITE_API_KEY=os.getenv("KITE_API_KEY")
TOKEN_FILE=os.getenv("ZERODHA_TOKEN_FILE","ingestion/auth/token.json")

class TokenBucket:
    def __init__(self, rate_per_sec:float, burst:int=3):
        self.rate=rate_per_sec; self.cap=float(burst); self.tokens=float(burst); self.t=time.time()
    async def take(self):
        while True:
            now=time.time()
            self.tokens=min(self.cap, self.tokens+(now-self.t)*self.rate); self.t=now
            if self.tokens>=1.0:
                self.tokens-=1.0; return
            await asyncio.sleep(0.05)

def load_access_token():
    import json as _j
    if os.getenv("KITE_ACCESS_TOKEN"): return os.getenv("KITE_ACCESS_TOKEN")
    t=_j.load(open(TOKEN_FILE)); return t["access_token"]

async def upsert_order(con, o:dict, broker_order_id:str=None, status:str="NEW"):
    await con.execute("""
      INSERT INTO live_orders(client_order_id, symbol, side, qty, px_ref, strategy, risk_bucket, broker_order_id, status, extra)
      VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
      ON CONFLICT (client_order_id) DO UPDATE
      SET broker_order_id=COALESCE(EXCLUDED.broker_order_id, live_orders.broker_order_id),
          status=EXCLUDED.status,
          updated_at=now(),
          extra=COALESCE(EXCLUDED.extra, live_orders.extra)
    """, o["client_order_id"], o["symbol"], o["side"], int(o["qty"]), float((o.get("extra") or {}).get("px_ref") or 0.0),
       o.get("strategy"), o.get("risk_bucket"), broker_order_id, status, json.dumps(o.get("extra") or {}))

async def main():
    # DB + Kafka
    pool=await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
    cons=AIOKafkaConsumer(
        IN_TOPIC, bootstrap_servers=BROKER, group_id=GROUP_ID, enable_auto_commit=False, auto_offset_reset="latest",
        value_deserializer=lambda b: json.loads(b.decode()), key_deserializer=lambda b: b.decode() if b else None
    )
    prod=AIOKafkaProducer(bootstrap_servers=BROKER, acks="all", linger_ms=5)
    await cons.start(); await prod.start()

    # Rate limit
    bucket=TokenBucket(RATE_PER_SEC, burst=3)

    # Broker (or dry)
    kite=None
    if not DRY_RUN:
        access=load_access_token()
        kite=KiteConnect(api_key=KITE_API_KEY); kite.set_access_token(access)
        try: kite.profile()
        except kz_ex.TokenException: raise SystemExit("Zerodha token invalid; re-auth.")

    print(f"[gateway] consuming {IN_TOPIC} (DRY_RUN={DRY_RUN}) → fills:{FILL_TOPIC}")
    try:
        async for m in cons:
            o=m.value
            coid=o["client_order_id"]
            try:
                async with pool.acquire() as con:
                    # Idempotency: if already acknowledged to a terminal status, skip
                    r=await con.fetchrow("SELECT status FROM live_orders WHERE client_order_id=$1", coid)
                    if r and r["status"] in ("REJECTED","FILLED","CANCELLED"):
                        await cons.commit(); continue

                    # Upsert as NEW/PENDING
                    await upsert_order(con, o, status="PENDING")

                await bucket.take()

                if DRY_RUN:
                    # Simulate immediate fill at px_ref
                    px = float((o.get("extra") or {}).get("px_ref") or 0.0)
                    async with pool.acquire() as con:
                        await upsert_order(con, o, broker_order_id=f"SIM-{coid}", status="FILLED")
                        await con.execute(
                            "INSERT INTO fills(client_order_id, symbol, side, qty, price, strategy, risk_bucket) VALUES($1,$2,$3,$4,$5,$6,$7)",
                            coid, o["symbol"], o["side"], int(o["qty"]), px, o.get("strategy"), o.get("risk_bucket")
                        )
                    fill = {"client_order_id": coid, "symbol": o["symbol"], "side": o["side"], "qty": int(o["qty"]), "price": px, "ts": int(time.time())}
                    await prod.send_and_wait(FILL_TOPIC, json.dumps(fill).encode(), key=o["symbol"].encode())
                else:
                    # Real broker call (very basic MARKET example)
                    tradingsymbol=o["symbol"]; exchange="NSE"
                    qty=int(o["qty"]); side=o["side"]
                    variety="regular"; product="MIS"; order_type="MARKET"; validity="DAY"
                    if side=="BUY":
                        resp=kite.place_order(variety=variety, exchange=exchange, tradingsymbol=tradingsymbol,
                                              transaction_type="BUY", quantity=qty, product=product,
                                              order_type=order_type, validity=validity)
                    else:
                        resp=kite.place_order(variety=variety, exchange=exchange, tradingsymbol=tradingsymbol,
                                              transaction_type="SELL", quantity=qty, product=product,
                                              order_type=order_type, validity=validity)
                    broker_id=str(resp["order_id"])
                    async with pool.acquire() as con:
                        await upsert_order(con, o, broker_order_id=broker_id, status="ACK")

                await cons.commit()
            except Exception as e:
                print("[gateway] error:", e)
                await cons.commit()
    finally:
        await cons.stop(); await prod.stop(); await pool.close()

if __name__=="__main__":
    asyncio.run(main())
