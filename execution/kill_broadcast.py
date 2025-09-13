"""
execution/kill_broadcast.py
Trip global/tier kill and broadcast to Kafka (topic 'control.kill').

Usage:
  python execution/kill_broadcast.py --scope ALL --reason "panic"
  python execution/kill_broadcast.py --scope BUCKET --bucket MED --reason "dd breach"
"""
from __future__ import annotations
import os, argparse, asyncio, ujson as json
from aiokafka import AIOKafkaProducer
from libs.killswitch import KillSwitch

BROKER=os.getenv("KAFKA_BROKER","localhost:9092")
TOPIC=os.getenv("CTRL_TOPIC","control.kill")

def parse():
    p=argparse.ArgumentParser()
    p.add_argument("--scope", choices=["ALL","BUCKET"], required=True)
    p.add_argument("--bucket", default="")
    p.add_argument("--reason", default="manual")
    return p.parse_args()

async def main():
    a=parse()
    KS=KillSwitch()
    KS.trip(f"{a.scope}:{a.bucket}:{a.reason}")
    msg={"ts": __import__("time").time(), "scope": a.scope, "bucket": a.bucket, "reason": a.reason}
    prod=AIOKafkaProducer(bootstrap_servers=BROKER, acks="all")
    await prod.start()
    try:
        await prod.send_and_wait(TOPIC, json.dumps(msg).encode(), key=(a.bucket or "ALL").encode())
        print(f"[kill] tripped & broadcast: {msg}")
    finally:
        await prod.stop()

if __name__=="__main__":
    asyncio.run(main())
