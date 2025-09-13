"""
Produces a couple of synthetic fills to test the ledger consumer.
"""
import os, asyncio, time, ujson as json
from aiokafka import AIOKafkaProducer

BROKER=os.getenv("KAFKA_BROKER","localhost:9092")
TOPIC=os.getenv("FILL_TOPIC","fills")

async def main():
    p=AIOKafkaProducer(bootstrap_servers=BROKER, acks="all")
    await p.start()
    try:
        now=int(time.time())
        msgs=[
          {"ts": now,   "symbol":"RELIANCE","side":"BUY","qty":50,"price":2500.0,"fees_inr":10.0,"client_order_id":"test1","strategy":"SMOKE","risk_bucket":"LOW"},
          {"ts": now+5, "symbol":"RELIANCE","side":"SELL","qty":50,"price":2520.0,"fees_inr":10.0,"client_order_id":"test2","strategy":"SMOKE","risk_bucket":"LOW"},
        ]
        for m in msgs:
            await p.send_and_wait(TOPIC, json.dumps(m).encode(), key=m["symbol"].encode())
        print(f"Sent {len(msgs)} fills to {TOPIC}")
    finally:
        await p.stop()

if __name__=="__main__":
    asyncio.run(main())
