"""
execution/shadow_mirror.py
- Consumes 'orders' (live-intent)
- Republishes to 'orders.paper' with shadow metadata
- Skips orders that are already shadows (prevents loops)

Run:
  KAFKA_BROKER=localhost:9092 IN_TOPIC=orders OUT_TOPIC=orders.paper python execution/shadow_mirror.py
"""

from __future__ import annotations
import os, asyncio, ujson as json
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

BROKER=os.getenv("KAFKA_BROKER","localhost:9092")
IN_TOPIC=os.getenv("IN_TOPIC","orders")
OUT_TOPIC=os.getenv("OUT_TOPIC","orders.paper")
GROUP_ID=os.getenv("KAFKA_GROUP","shadow_mirror")

async def main():
    consumer=AIOKafkaConsumer(
        IN_TOPIC, bootstrap_servers=BROKER, enable_auto_commit=False, auto_offset_reset="earliest",
        group_id=GROUP_ID, value_deserializer=lambda b: json.loads(b.decode()), key_deserializer=lambda b: b.decode() if b else None
    )
    producer=AIOKafkaProducer(bootstrap_servers=BROKER, acks="all", linger_ms=5)
    await consumer.start(); await producer.start()
    print(f"[shadow] {IN_TOPIC} → {OUT_TOPIC}")
    try:
        while True:
            batches=await consumer.getmany(timeout_ms=500, max_records=1000)
            for _tp, msgs in batches.items():
                for m in msgs:
                    o=m.value
                    if (o.get("extra") or {}).get("shadow", False):  # already mirrored
                        continue
                    o2=dict(o)
                    o2["extra"]=dict(o.get("extra") or {}, shadow=True, shadow_of=o.get("client_order_id"))
                    o2["client_order_id"]=f"SHADOW:{o['client_order_id']}"
                    await producer.send_and_wait(OUT_TOPIC, json.dumps(o2).encode(), key=o2["symbol"].encode())
            await consumer.commit()
    finally:
        await consumer.stop(); await producer.stop()

if __name__=="__main__":
    asyncio.run(main())
