"""
execution/cancel_listener.py
On control.kill, cancel open orders (scope ALL or BUCKET=<LOW|MED|HIGH>).

ENV:
  KAFKA_BROKER=localhost:9092
  CTRL_TOPIC=control.kill
  OUT_ORD_TOPIC=orders
"""
from __future__ import annotations
import os, asyncio, asyncpg, ujson as json
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

BROKER=os.getenv("KAFKA_BROKER","localhost:9092")
CTRL_TOPIC=os.getenv("CTRL_TOPIC","control.kill")
OUT_ORD_TOPIC=os.getenv("OUT_ORD_TOPIC","orders")

PG_HOST=os.getenv("POSTGRES_HOST","localhost"); PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading"); PG_USER=os.getenv("POSTGRES_USER","trader"); PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")

OPEN_STATES=("NEW","ACK","PENDING","OPEN","PARTIAL")

async def open_orders(con, bucket=None):
    if bucket:
        rows = await con.fetch("""
          SELECT client_order_id, symbol FROM orders
          WHERE coalesce(status,'') = ANY($1::text[]) AND risk_bucket=$2
        """, list(OPEN_STATES), bucket)
    else:
        rows = await con.fetch("""
          SELECT client_order_id, symbol FROM orders
          WHERE coalesce(status,'') = ANY($1::text[])
        """, list(OPEN_STATES))
    return [(r["client_order_id"], r["symbol"]) for r in rows]

async def main():
    pool=await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
    consumer=AIOKafkaConsumer(
        CTRL_TOPIC, bootstrap_servers=BROKER, enable_auto_commit=True, auto_offset_reset="latest",
        group_id="cancel_listener", value_deserializer=lambda b: json.loads(b.decode()), key_deserializer=lambda b: b.decode() if b else None
    )
    producer=AIOKafkaProducer(bootstrap_servers=BROKER, acks="all", linger_ms=5)
    await consumer.start(); await producer.start()
    print(f"[cancel] listening {CTRL_TOPIC} -> {OUT_ORD_TOPIC}")
    try:
        async for m in consumer:
            msg=m.value; scope=msg.get("scope","ALL"); bucket=(msg.get("bucket") or "").strip() or None
            async with pool.acquire() as con:
                olist = await open_orders(con, bucket=bucket if scope=="BUCKET" else None)
            print(f"[cancel] scope={scope} bucket={bucket} open={len(olist)}")
            for coid, sym in olist:
                cancel = {
                  "client_order_id": f"CANCEL:{coid}",
                  "order_type": "CANCEL",
                  "orig_client_order_id": coid,
                  "symbol": sym,
                  "status": "NEW",
                  "extra": {"reason":"kill_broadcast"}
                }
                await producer.send_and_wait(OUT_ORD_TOPIC, json.dumps(cancel).encode(), key=sym.encode())
    finally:
        await consumer.stop(); await producer.stop(); await pool.close()

if __name__=="__main__":
    asyncio.run(main())
