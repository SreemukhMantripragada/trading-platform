"""
tools/sim_pairs_signal.py
Emits example pairs signals to Kafka for testing.

ENV:
  KAFKA_BROKER=localhost:9092
  TOPIC=pairs.signals
"""
from __future__ import annotations
import os, asyncio, time, ujson as json
from aiokafka import AIOKafkaProducer

BROKER=os.getenv("KAFKA_BROKER","localhost:9092")
TOPIC=os.getenv("TOPIC","pairs.signals")

async def main():
    p=AIOKafkaProducer(bootstrap_servers=BROKER, acks="all")
    await p.start()
    try:
        now=int(time.time()*1000)
        msgs=[
          {"pair_id":"REL_TCS","a_symbol":"RELIANCE","b_symbol":"TCS","beta":0.8,"z":2.2,
           "action":"ENTER_LONG_A_SHORT_B","risk_bucket":"MED","pxA":2900.0,"pxB":4100.0,"ts":now},
          {"pair_id":"REL_TCS","a_symbol":"RELIANCE","b_symbol":"TCS","beta":0.8,"z":0.3,
           "action":"EXIT","risk_bucket":"MED","ts":now+60000}
        ]
        for m in msgs:
            await p.send_and_wait(TOPIC, json.dumps(m).encode(), key=m["pair_id"].encode())
        print(f"sent {len(msgs)} to {TOPIC}")
    finally:
        await p.stop()

if __name__=="__main__":
    asyncio.run(main())
